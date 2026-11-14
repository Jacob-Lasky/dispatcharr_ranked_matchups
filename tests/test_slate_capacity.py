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


def test_probe_exposes_owner_and_client_during_acquire_and_release(capacity, monkeypatch):
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
    monkeypatch.setitem(sys.modules, 'apps.m3u.reservation_guard', guard)
    reserve = s.pool.reserve_profile_slot.side_effect
    release = s.pool.release_profile_slot.side_effect
    observed = []
    def acquire(profile, client):
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
