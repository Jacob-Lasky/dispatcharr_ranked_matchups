"""Foreground tuning must keep a slot while independent workers check feeds."""
import sys
from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def capacity(monkeypatch):
    from dispatcharr_ranked_matchups import slate

    state = SimpleNamespace(used=0, locked=False, ran=0)
    redis = Mock()
    lock = redis.lock.return_value
    def acquire():
        if state.locked:
            return False
        state.locked = True
        return True
    def release():
        assert state.locked
        state.locked = False
    lock.acquire.side_effect = acquire
    lock.release.side_effect = release
    def reserve(profile, client):
        if profile.max_streams and state.used >= profile.max_streams:
            return False, state.used, 'profile_full'
        if profile.max_streams:
            state.used += 1
        return True, state.used, None
    def free(profile_id, client):
        if profile.max_streams:
            state.used -= 1
        return True
    pool = ModuleType('apps.m3u.connection_pool')
    pool.reserve_profile_slot = Mock(side_effect=reserve)
    pool.release_profile_slot = Mock(side_effect=free)
    pool.get_profile_connection_count = Mock(side_effect=lambda profile, client: state.used)
    utils = ModuleType('core.utils')
    utils.RedisClient = SimpleNamespace(get_client=lambda: redis)
    urls = ModuleType('apps.proxy.live_proxy.url_utils')
    urls.get_stream_info_for_switch = Mock(return_value={'url': 'http://127.0.0.1/test', 'user_agent': 'test'})
    models = ModuleType('apps.channels.models')
    models.Channel = Mock()
    models.Stream = Mock()
    accounts = ModuleType('apps.m3u.models')
    accounts.M3UAccount = Mock()
    accounts.M3UAccount.objects.filter.return_value.exists.return_value = False
    for name, module in [('apps.m3u.connection_pool', pool), ('core.utils', utils),
                         ('apps.proxy.live_proxy.url_utils', urls), ('apps.channels.models', models),
                         ('apps.m3u.models', accounts)]:
        monkeypatch.setitem(sys.modules, name, module)
    profile = SimpleNamespace(pk=4, max_streams=4)
    profiles = Mock()
    profiles.filter.return_value.order_by.return_value = [profile]
    state.profile = profile
    state.stream = SimpleNamespace(pk=3, m3u_account_id=4, m3u_account=SimpleNamespace(profiles=profiles))
    state.channel = SimpleNamespace(uuid='test-channel')
    state.pool, state.redis, state.slate = pool, redis, slate
    monkeypatch.setattr(slate, '_run', lambda *a, **kw: (0, b'x' * 256))
    return state


@pytest.mark.parametrize('limit', [1, 2, 4])
def test_probe_leaves_last_slot_for_a_foreground_tune(capacity, limit):
    s = capacity
    s.profile.max_streams = limit
    s.used = limit - 1
    assert s.slate.probe(s.channel, s.stream) is None, 'background probe used the last foreground slot'
    assert s.used == limit - 1
    assert s.pool.reserve_profile_slot(s.profile, s.redis)[0] is True
    assert s.profile.max_streams == limit, 'persisted provider limit was modified'


def test_probe_uses_spare_capacity_and_releases_it(capacity):
    s = capacity
    s.used = 2
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.used == 2
    assert not s.locked
    assert s.profile.max_streams == 4


def test_another_worker_cannot_overlap_a_provider_probe(capacity, monkeypatch):
    s = capacity
    def decode(*args, **kwargs):
        s.ran += 1
        if s.ran > 1:
            return 0, b'x' * 256
        assert s.used == 1
        assert s.slate.probe(s.channel, s.stream) is None, 'another worker ran a concurrent provider probe'
        return 0, b'x' * 256
    monkeypatch.setattr(s.slate, '_run', decode)
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.used == 0
    assert not s.locked


def test_unlimited_profile_stays_unlimited(capacity):
    s = capacity
    s.profile.max_streams = 0
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.used == 0
    assert not s.locked


def test_probe_releases_lock_and_slot_after_lookup_failure(capacity):
    s = capacity
    sys.modules['apps.proxy.live_proxy.url_utils'].get_stream_info_for_switch.side_effect = RuntimeError('lookup failed')
    with pytest.raises(RuntimeError, match='lookup failed'):
        s.slate.probe(s.channel, s.stream)
    assert s.used == 0
    assert not s.locked


@pytest.mark.parametrize('runtime', ['core', 'installed', 'reload'])
def test_probe_exposes_owner_and_client_during_acquire_and_release(capacity, monkeypatch, runtime):
    s = capacity
    active = []
    guard = ModuleType('apps.m3u.reservation_guard')
    @contextmanager
    def scope(owner, **identity):
        active.append((owner, identity))
        try:
            yield
        finally:
            active.pop()
    guard.scope = scope
    monkeypatch.setitem(sys.modules, 'apps.m3u.reservation_guard', guard if runtime != 'reload' else None)
    # The loader temporarily evicts the file-backed alias during a reload;
    # the installed runtime guard deliberately survives in this registry.
    if runtime == 'core':
        monkeypatch.delitem(sys.modules, '_dispatcharr_m3u_pool_guard', raising=False)
    else:
        monkeypatch.setitem(sys.modules, '_dispatcharr_m3u_pool_guard', SimpleNamespace(module=guard))
    reserve = s.pool.reserve_profile_slot.side_effect
    release = s.pool.release_profile_slot.side_effect
    observed = []
    def acquire(profile, client):
        assert active, 'reservation owner missing during loader eviction'
        observed.append(('acquire', active[0]))
        return reserve(profile, client)
    def free(profile, client):
        observed.append(('release', active[0]))
        return release(profile, client)
    s.pool.reserve_profile_slot.side_effect = acquire
    s.pool.release_profile_slot.side_effect = free
    assert s.slate.probe(s.channel, s.stream) is True
    assert [event[0] for event in observed] == ['acquire', 'release']
    for _, (owner, identity) in observed:
        assert owner == 'probe:test-channel'
        assert identity == dict(client_id='probe:test-channel', kind='task', channel='test-channel',
                                liveness_key='ranked_matchups:slate:probe')
    assert active == []
    assert s.used == 0


@pytest.fixture
def provider(capacity, monkeypatch):
    """An Xtream Codes account whose provider reports its own session count."""
    from dispatcharr_ranked_matchups import _util
    s = capacity
    s.keys = {}
    s.redis.exists.side_effect = lambda key: key in s.keys
    s.values = {}
    def setex(key, ttl, value):
        s.keys[key] = ttl
        s.values[key] = value
    s.redis.setex.side_effect = setex
    s.redis.get.side_effect = lambda key: s.values.get(key) if key in s.keys else None
    s.redis.ttl.side_effect = lambda key: s.keys[key] if key in s.keys else -2
    s.zsets = {}
    def zadd(key, mapping):
        s.zsets.setdefault(key, {}).update(mapping)
    def zremrangebyscore(key, low, high):
        z = s.zsets.get(key, {})
        for member in [m for m, score in z.items() if score <= float(high)]:
            del z[member]
    def zrangebyscore(key, low, high, withscores=False):
        assert high == '+inf' and withscores
        return sorted(((m, score) for m, score in s.zsets.get(key, {}).items() if score >= float(low)),
                      key=lambda item: item[1])
    s.redis.zadd.side_effect = zadd
    s.redis.zremrangebyscore.side_effect = zremrangebyscore
    s.redis.zrangebyscore.side_effect = zrangebyscore
    s.redis.zcount.side_effect = lambda key, low, high: len(zrangebyscore(key, low, high, withscores=True))
    s.reply = {'user_info': {'max_connections': '4', 'active_cons': '1'}}
    s.requests = []
    def fetch(requests_mod, url, **kwargs):
        s.requests.append((url, kwargs))
        return s.reply
    monkeypatch.setattr(_util, 'http_get_json', fetch)
    creds = ModuleType('apps.m3u.credentials')
    creds.get_transformed_credentials = Mock(return_value=('http://provider.test', 'user', 'secret'))
    monkeypatch.setitem(sys.modules, 'apps.m3u.credentials', creds)
    s.stream.m3u_account.account_type = 'XC'
    s.stream.m3u_account.pk = 4
    s.stream.m3u_account.get_user_agent = lambda: SimpleNamespace(user_agent='TiviMate/5')
    s.opened = 0
    def decode(*args, **kwargs):
        s.opened += 1
        return 0, b'x' * 256
    monkeypatch.setattr(s.slate, '_run', decode)
    return s


@pytest.mark.parametrize('active, expected', [('1', True), ('2', True), ('3', None), ('4', None), ('5', None)])
def test_probe_respects_sessions_the_provider_still_counts(provider, active, expected):
    """JAKE-226: the pool said 1 of 4 while the provider said 4 of 4, and each
    probe made the provider drop the live DVR feed. A probe needs one session
    for itself and must leave one for a foreground tune."""
    s = provider
    s.reply['user_info']['active_cons'] = active
    assert s.slate.probe(s.channel, s.stream) is expected
    assert s.opened == (1 if expected else 0), 'feed opened without provider headroom'
    assert s.used == 0, 'probe slot leaked on the deferral path'
    assert not s.locked
    url, kwargs = s.requests[0]
    assert url == 'http://provider.test/player_api.php'
    assert kwargs['params'] == {'username': 'user', 'password': 'secret'}
    assert kwargs['headers'] == {'User-Agent': 'TiviMate/5'}
    assert kwargs['timeout'] <= s.slate.PROBE_SECONDS


def test_full_provider_is_not_asked_again_by_every_stream(provider):
    s = provider
    s.reply['user_info']['active_cons'] = '4'
    assert s.slate.probe(s.channel, s.stream) is None
    assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 1
    assert {ttl for k, ttl in s.keys.items() if ':provider_full' in k} == {s.slate.PROVIDER_FULL_SECONDS}
    [key] = [k for k in s.keys if k.startswith('ranked_matchups:slate:provider_full:')]
    assert 'user' not in key.rsplit(':', 1)[1] and 'provider.test' not in key, 'login leaked into Redis key'
    assert 'ranked_matchups:slate:provider_full_profile:4' in s.keys
    assert s.used == 0


@pytest.mark.parametrize('reply', [
    None, {}, {'user_info': None}, {'user_info': {'max_connections': 'x'}},
    {'user_info': {'max_connections': '4'}},
    {'user_info': {'max_connections': '4', 'active_cons': None}},
    {'user_info': {'max_connections': '4', 'active_cons': ''}},
    {'user_info': {'max_connections': '4', 'active_cons': '-1'}},
])
def test_unreadable_provider_reply_defers_the_probe(provider, reply):
    s = provider
    s.reply = reply
    assert s.slate.probe(s.channel, s.stream) is None
    assert s.opened == 0
    assert s.used == 0
    assert not s.locked


def test_unresolvable_profile_credentials_defer_the_probe(provider):
    s = provider
    sys.modules['apps.m3u.credentials'].get_transformed_credentials.return_value = (None, None, None)
    assert s.slate.probe(s.channel, s.stream) is None
    assert s.requests == [] and s.opened == 0 and s.used == 0


@pytest.mark.parametrize('limit', ['0', None, ''])
def test_provider_without_a_connection_limit_is_probed(provider, limit):
    s = provider
    s.reply['user_info'] = {'max_connections': limit, 'active_cons': '9'}
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.opened == 1


def test_non_xc_account_keeps_pool_only_behaviour(provider):
    s = provider
    s.stream.m3u_account.account_type = 'STD'
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.requests == []


def test_full_line_moves_on_to_a_profile_with_its_own_login(provider):
    """Profiles can carry their own credentials: one full provider line must
    neither stop the probe nor mark the other login full."""
    s = provider
    first, second = SimpleNamespace(pk=4, max_streams=4), SimpleNamespace(pk=6, max_streams=4)
    s.stream.m3u_account.profiles.filter.return_value.order_by.return_value = [first, second]
    logins = {4: ('http://provider.test', 'full-user', 'a'), 6: ('http://provider.test', 'spare-user', 'b')}
    sys.modules['apps.m3u.credentials'].get_transformed_credentials.side_effect = lambda acct, prof: logins[prof.pk]
    def fetch(requests_mod, url, **kwargs):
        s.requests.append((url, kwargs))
        used = '4' if kwargs['params']['username'] == 'full-user' else '1'
        return {'user_info': {'max_connections': '4', 'active_cons': used}}
    from dispatcharr_ranked_matchups import _util
    _util.http_get_json = fetch
    reserved = []
    reserve = s.pool.reserve_profile_slot.side_effect
    s.pool.reserve_profile_slot.side_effect = lambda prof, client: (reserved.append(prof.pk), reserve(prof, client))[1]
    assert s.slate.probe(s.channel, s.stream) is True
    assert reserved == [4, 6]
    assert s.opened == 1 and s.used == 0
    assert sorted(k.rsplit(':', 1)[0] for k in s.keys if ':provider_full' in k) == [
        'ranked_matchups:slate:provider_full', 'ranked_matchups:slate:provider_full_profile']
    assert 'ranked_matchups:slate:provider_full_profile:4' in s.keys
    s.requests.clear()
    assert s.slate.probe(s.channel, s.stream) is True
    assert [kw['params']['username'] for _, kw in s.requests] == ['spare-user'], 'full verdict not reused, or spare login blocked'


def test_reconcile_budgets_the_provider_check_before_each_probe():
    """reconcile needs Django, so pin the call-site contract: a probe may now
    spend PROVIDER_CHECK_SECONDS on player_api.php before ffmpeg starts, and
    the 120s walk must stay inside the 180s probe lease."""
    import inspect
    from dispatcharr_ranked_matchups import slate
    assert 'time.monotonic() + PROBE_SECONDS + PROVIDER_CHECK_SECONDS > deadline' in inspect.getsource(slate.reconcile)
    assert slate.PROVIDER_CHECK_SECONDS < slate.PROBE_SECONDS


def test_provider_checks_share_one_budget_across_profiles(provider, monkeypatch):
    """A slow full line must not buy the next profile a fresh 4s check, or
    reconcile's per-stream budget (and the 180s probe lease) can be overrun."""
    s = provider
    first, second = SimpleNamespace(pk=4, max_streams=4), SimpleNamespace(pk=6, max_streams=4)
    s.stream.m3u_account.profiles.filter.return_value.order_by.return_value = [first, second]
    logins = {4: ('http://provider.test', 'slow-user', 'a'), 6: ('http://provider.test', 'other-user', 'b')}
    sys.modules['apps.m3u.credentials'].get_transformed_credentials.side_effect = lambda acct, prof: logins[prof.pk]
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(s.slate.time, 'monotonic', lambda: clock.now)
    timeouts = []
    def slow(requests_mod, url, **kwargs):
        timeouts.append(kwargs['timeout'])
        clock.now += kwargs['timeout'] - 0.5
        return {'user_info': {'max_connections': '4', 'active_cons': '4'}}
    from dispatcharr_ranked_matchups import _util
    monkeypatch.setattr(_util, 'http_get_json', slow)
    assert s.slate.probe(s.channel, s.stream) is None
    assert timeouts == [s.slate.PROVIDER_CHECK_SECONDS], 'second profile got its own provider check'
    assert s.used == 0 and s.opened == 0 and not s.locked


def test_recent_probes_count_because_the_provider_figure_lags(provider):
    """JAKE-226 follow-up: a check 5s after a probe opened still reported the
    pre-probe count, a second probe opened and the live feed dropped. Our own
    probes from the last minute are added to the provider's figure."""
    s = provider
    s.reply['user_info']['active_cons'] = '2'
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.slate.probe(s.channel, s.stream) is None, 'second probe trusted a lagging provider count'
    assert s.opened == 1 and s.used == 0 and not s.locked
    [probes] = [k for k in s.zsets if k.startswith('ranked_matchups:slate:provider_probes:')]
    assert len(s.zsets[probes]) == 1
    s.redis.expire.assert_any_call(probes, 2 * s.slate.PROVIDER_SESSION_LINGER_SECONDS)


def test_recent_probe_stops_counting_after_the_linger_window(provider, monkeypatch):
    s = provider
    clock = SimpleNamespace(now=1_000_000.0)
    monkeypatch.setattr(s.slate.time, 'time', lambda: clock.now)
    s.reply['user_info']['active_cons'] = '1'
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.slate.probe(s.channel, s.stream) is True   # 1 + 1 recent + 2 <= 4
    assert s.slate.probe(s.channel, s.stream) is None   # 1 + 2 recent + 2 > 4
    s.keys.clear()
    clock.now += s.slate.PROVIDER_SESSION_LINGER_SECONDS + 1
    assert s.slate.probe(s.channel, s.stream) is True
    assert s.opened == 3 and s.used == 0


def test_line_known_full_is_skipped_before_reserving_a_slot(provider):
    """Reconcile walks every stream of every channel; against a line already
    known full each walk must not reserve and release a pool slot (about 100
    pool log lines a minute in the first deploy)."""
    s = provider
    s.reply['user_info']['active_cons'] = '4'
    assert s.slate.probe(s.channel, s.stream) is None
    reserved = s.pool.reserve_profile_slot.call_count
    for _ in range(5):
        assert s.slate.probe(s.channel, s.stream) is None
    assert s.pool.reserve_profile_slot.call_count == reserved, 'slot churn against a full line'
    # Dispatcharr logs the credential-bearing URL at INFO on every transform.
    assert sys.modules['apps.m3u.credentials'].get_transformed_credentials.call_count == 1
    assert len(s.requests) == 1 and s.used == 0


def test_probe_note_failure_still_releases_the_slot(provider):
    s = provider
    s.redis.zadd.side_effect = RuntimeError('redis down')
    with pytest.raises(RuntimeError, match='redis down'):
        s.slate.probe(s.channel, s.stream)
    assert s.used == 0 and not s.locked and s.opened == 0


def test_second_profile_on_a_full_login_is_marked_without_asking_again(provider):
    s = provider
    first, second = SimpleNamespace(pk=4, max_streams=4), SimpleNamespace(pk=6, max_streams=4)
    s.stream.m3u_account.profiles.filter.return_value.order_by.return_value = [first, second]
    s.reply['user_info']['active_cons'] = '4'
    assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 1, 'same login asked twice'
    assert {'ranked_matchups:slate:provider_full_profile:4',
            'ranked_matchups:slate:provider_full_profile:6'} <= set(s.keys)
    assert s.used == 0 and s.opened == 0


def test_unresolvable_first_profile_falls_back_to_the_next(provider):
    s = provider
    first, second = SimpleNamespace(pk=4, max_streams=4), SimpleNamespace(pk=6, max_streams=4)
    s.stream.m3u_account.profiles.filter.return_value.order_by.return_value = [first, second]
    logins = {4: (None, None, None), 6: ('http://provider.test', 'other-user', 'b')}
    sys.modules['apps.m3u.credentials'].get_transformed_credentials.side_effect = lambda acct, prof: logins[prof.pk]
    assert s.slate.probe(s.channel, s.stream) is True
    assert [kw['params']['username'] for _, kw in s.requests] == ['other-user']
    assert s.opened == 1 and s.used == 0 and not s.locked


@pytest.fixture
def clocked(provider, monkeypatch):
    """The provider fixture with a wall clock that expires Redis keys."""
    s = provider
    s.clock = SimpleNamespace(now=1_000_000.0)
    monkeypatch.setattr(s.slate.time, 'time', lambda: s.clock.now)
    s.expires = {}
    setex = s.redis.setex.side_effect
    def timed_setex(key, ttl, value):
        assert isinstance(ttl, int) and ttl >= 1, 'Redis SETEX needs a whole number of seconds'
        s.expires[key] = s.clock.now + ttl
        setex(key, ttl, value)
    s.redis.setex.side_effect = timed_setex
    s.redis.ttl.side_effect = lambda key: (
        int(-(-(s.expires[key] - s.clock.now) // 1)) if key in s.keys and key in s.expires else -2)
    def advance(seconds):
        s.clock.now += seconds
        for key in [k for k, at in s.expires.items() if at <= s.clock.now]:
            s.keys.pop(key, None)
            del s.expires[key]
    s.advance = advance
    return s


def test_our_own_probes_defer_without_asking_the_provider_again(clocked):
    """JAKE-1309: 1,213 deferrals a day, nearly all "1 of 4 plus 2 recent
    probes", each one a player_api.php request. While our own probes keep the
    provider's last count over the limit no answer can let a probe run, so it
    is not asked until one of them ages out."""
    s = clocked
    assert s.slate.probe(s.channel, s.stream) is True    # 1 + 0 + 2 <= 4
    s.advance(7)
    assert s.slate.probe(s.channel, s.stream) is True    # 1 + 1 + 2 <= 4
    assert len(s.requests) == 2
    for _ in range(10):                                  # reconcile ticks
        s.advance(5)
        assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 2, 'provider asked while our own probes alone keep it full'
    s.advance(10)                                        # first probe is now 67 s old
    assert s.slate.probe(s.channel, s.stream) is True
    assert len(s.requests) == 3 and s.opened == 3 and s.used == 0 and not s.locked


def test_a_probe_that_fits_the_remembered_count_still_asks(clocked):
    """The remembered count may only defer. A viewer tuning in since the last
    answer must still be seen before the next probe opens."""
    s = clocked
    assert s.slate.probe(s.channel, s.stream) is True
    s.advance(5)
    s.reply['user_info']['active_cons'] = '2'            # a foreground tune
    assert s.slate.probe(s.channel, s.stream) is None    # 2 + 1 + 2 > 4
    assert len(s.requests) == 2 and s.opened == 1


def test_line_full_by_the_providers_own_count_is_asked_again_after_30s(clocked):
    """A session ending is visible only to the provider, so that hold stays
    PROVIDER_FULL_SECONDS and the remembered count does not extend it."""
    s = clocked
    s.reply['user_info']['active_cons'] = '3'
    assert s.slate.probe(s.channel, s.stream) is None
    s.advance(s.slate.PROVIDER_FULL_SECONDS - 1)
    assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 1
    s.advance(1)
    s.reply['user_info']['active_cons'] = '1'            # the viewer left
    assert s.slate.probe(s.channel, s.stream) is True
    assert len(s.requests) == 2


def test_probe_blocked_hold_ends_when_the_blocking_probe_ages_out(clocked):
    """A hold ends the first whole second (SETEX) the blocking probe stops
    counting, never later than the linger window."""
    s = clocked
    s.reply['user_info']['active_cons'] = '2'
    assert s.slate.probe(s.channel, s.stream) is True    # 2 + 0 + 2 <= 4
    s.advance(20)
    assert s.slate.probe(s.channel, s.stream) is None    # remembered 2 + 1 recent + 2 > 4
    assert len(s.requests) == 1
    assert {ttl for k, ttl in s.keys.items() if ':provider_full' in k} == {41}
    s.advance(40)
    assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 1
    s.advance(1)                                         # the probe is 61 s old
    assert s.slate.probe(s.channel, s.stream) is True
    assert len(s.requests) == 2


@pytest.mark.parametrize('active', [0, 1, 2])
def test_saturated_probe_demand_asks_once_per_probe(clocked, active):
    """Live shape: 126 matchup channels keep far more probes waiting than the
    line allows, so the probe runs on every 10 s tick. Every provider request
    must now buy a probe, and the probe rate the JAKE-226 rule allows is kept."""
    s = clocked
    s.reply['user_info']['active_cons'] = str(active)
    for _ in range(360):                                 # one hour of ticks
        s.slate.probe(s.channel, s.stream)
        s.advance(10)
    assert s.opened > 0
    assert len(s.requests) == s.opened, f'{len(s.requests)} provider requests for {s.opened} probes'
    allowed = 4 - 2 - active + 1                         # probes per linger window
    assert s.opened >= allowed * 3600 // (s.slate.PROVIDER_SESSION_LINGER_SECONDS + 10)


def test_a_viewer_leaving_drops_the_remembered_count(clocked):
    """Codex review round 1: the provider's last count must not hold a probe
    back after one of Dispatcharr's own sessions on the profile ended, which
    frees the line at once (the old code asked again 10 s later)."""
    s = clocked
    s.used = 1                                           # a viewer on the profile
    s.reply['user_info']['active_cons'] = '2'
    assert s.slate.probe(s.channel, s.stream) is True    # 2 + 0 + 2 <= 4
    s.advance(10)
    assert s.slate.probe(s.channel, s.stream) is None    # viewer still on: held, not asked
    assert len(s.requests) == 1
    s.keys.clear()
    s.expires.clear()
    s.zsets.clear()
    s.reply['user_info']['active_cons'] = '2'
    assert s.slate.probe(s.channel, s.stream) is True    # fresh start, viewer on
    s.used = 0                                           # the viewer leaves
    s.reply['user_info']['active_cons'] = '1'
    s.advance(10)
    assert s.slate.probe(s.channel, s.stream) is True    # asked again: 1 + 1 + 2 <= 4
    assert len(s.requests) == 3 and s.opened == 3 and s.used == 0


def test_a_profile_dispatcharr_does_not_count_always_asks(clocked):
    """With no pool count a session ending is invisible, so the remembered
    count is never used and every probe asks, as before JAKE-1309."""
    s = clocked
    s.profile.max_streams = 0
    s.reply['user_info']['active_cons'] = '1'
    assert s.slate.probe(s.channel, s.stream) is True
    s.advance(7)
    assert s.slate.probe(s.channel, s.stream) is True
    s.advance(5)
    assert s.slate.probe(s.channel, s.stream) is None    # 1 + 2 + 2 > 4, provider asked
    assert len(s.requests) == 3


def test_second_profile_takes_the_logins_remaining_hold(clocked):
    """Codex review round 1: a profile meeting a held login gets what is left
    of that hold, not a fresh 30 s, or it outlives the login's hold."""
    s = clocked
    first, second = SimpleNamespace(pk=4, max_streams=4), SimpleNamespace(pk=6, max_streams=4)
    profiles = s.stream.m3u_account.profiles.filter.return_value.order_by
    profiles.return_value = [first]
    s.reply['user_info']['active_cons'] = '4'
    assert s.slate.probe(s.channel, s.stream) is None
    s.advance(19)
    profiles.return_value = [first, second]
    assert s.slate.probe(s.channel, s.stream) is None
    assert len(s.requests) == 1
    assert s.keys['ranked_matchups:slate:provider_full_profile:6'] == 11
    s.advance(11)
    s.reply['user_info']['active_cons'] = '1'
    assert s.slate.probe(s.channel, s.stream) is True
    assert len(s.requests) == 2


@pytest.mark.parametrize('stamps, now, active, expected', [
    ([0.0, 10.0], 20.0, 2, (51, 2)),    # both must age out: the newer one decides
    ([0.0, 10.0], 20.0, 1, (41, 2)),    # one must age out: the older one decides
    ([0.0, 10.0], 20.0, 0, (0, 2)),     # fits now
    ([0.0], 60.0, 2, (1, 1)),           # exactly 60 s old still counts (ZCOUNT is inclusive)
    ([0.0], 60.5, 2, (0, 0)),
    ([], 5.0, 3, (None, 0)),            # the provider's own count leaves no room
])
def test_probe_room_wait(provider, stamps, now, active, expected):
    s = provider
    base = 1_000_000.0
    s.zsets['ranked_matchups:slate:provider_probes:line'] = {f'p{i}': base + t for i, t in enumerate(stamps)}
    assert s.slate._probe_room_wait(s.redis, 'line', active, 4, base + now) == expected
