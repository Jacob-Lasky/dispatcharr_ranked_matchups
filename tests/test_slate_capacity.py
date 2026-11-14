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
    s.redis.setex.side_effect = lambda key, ttl, value: s.keys.__setitem__(key, ttl)
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
    assert list(s.keys.values()) == [s.slate.PROVIDER_FULL_SECONDS]
    [key] = s.keys
    assert key.startswith('ranked_matchups:slate:provider_full:')
    assert 'user' not in key.rsplit(':', 1)[1] and 'provider.test' not in key, 'login leaked into Redis key'
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
    assert len(s.keys) == 1
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
