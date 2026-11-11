"""W323: real ORM membership and switch decisions, with feed I/O injected."""
import importlib.util
import os
from datetime import timedelta
from unittest.mock import patch

import pytest

spec = importlib.util.spec_from_file_location('_slate_kit', os.path.join(os.path.dirname(__file__), 'test_apply_empty_slate.py'))
kit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)


def _setup(env):
    source, stream = kit._seed_source_channel(env)
    game = kit._row(401, kit._now() + timedelta(minutes=20), source.id)
    ch = kit._publish(env, [game], kit._settings())['ranked_matchups:CFB:401']
    return ch, stream, game


def _check(env, ch, game, responses, active=None, switch_ok=True):
    # On old main the real action returns Unknown action and changes no rows.
    if 'check_feeds' not in env.plugin._ACTION_HANDLERS:
        return env.plugin.Plugin.run(None, 'check_feeds', {}, {'settings': {}})
    from dispatcharr_ranked_matchups import slate
    with patch.object(slate, 'probe', side_effect=lambda c, s: responses[s.id]), \
         patch.object(slate, 'render', return_value='http://127.0.0.1:17985/' + 'a' * 64 + '.ts'), \
         patch.object(slate, 'active_stream', return_value=active), \
         patch.object(slate, 'switch', return_value=switch_ok) as switched:
        result = slate.reconcile(env.plugin, ch, game, kit._now())
        return result, switched.call_args_list


def scenario_all_streams_failing_attaches_placeholder(env):
    ch, stream, game = _setup(env)
    _check(env, ch, game, {stream.id: False})
    assert ch.streams.filter(custom_properties__ranked_matchups_slate=str(ch.uuid) if hasattr(ch,'uuid') else str(ch.pk)).exists(), 'all feeds failed but no slate was attached'
    assert ch.streams.filter(pk=stream.pk).exists(), 'provider membership was lost'


def scenario_one_recovers_switches_active_channel(env):
    ch, stream, game = _setup(env)
    _check(env, ch, game, {stream.id: False})
    sl = ch.streams.exclude(pk=stream.pk).first()
    result = _check(env, ch, game, {stream.id: True}, active={'stream_id': str(sl.pk) if sl else 'placeholder'})
    assert isinstance(result, tuple) and result[1], 'recovered feed never switched the active channel'
    assert result[1][0].args[1] == stream.pk
    assert list(ch.streams.values_list('pk', flat=True)) == [stream.pk]


def scenario_working_stream_never_gets_placeholder(env):
    ch, stream, game = _setup(env)
    _check(env, ch, game, {stream.id: True})
    assert list(ch.streams.values_list('pk', flat=True)) == [stream.pk]


def scenario_failed_switch_keeps_placeholder_for_retry(env):
    ch, stream, game = _setup(env)
    _check(env, ch, game, {stream.id: False})
    sl = ch.streams.exclude(pk=stream.pk).first()
    assert sl is not None
    _check(env, ch, game, {stream.id: True}, active={'stream_id': str(sl.pk)}, switch_ok=False)
    assert ch.streams.filter(pk=sl.pk).exists()


def scenario_unknown_capacity_is_not_a_failed_feed(env):
    ch, stream, game = _setup(env)
    _check(env, ch, game, {stream.id: None})
    assert list(ch.streams.values_list('pk', flat=True)) == [stream.pk]


def scenario_mixed_real_streams_keep_normal_failover(env):
    ch, stream, game = _setup(env)
    other = env.m.Stream.objects.create(name='Other feed',m3u_account=stream.m3u_account)
    env.m.ChannelStream.objects.create(channel=ch,stream=other,order=1)
    result = _check(env,ch,game,{stream.pk:False,other.pk:True},active={'stream_id':str(stream.pk)})
    assert set(ch.streams.values_list('pk',flat=True)) == {stream.pk,other.pk}
    assert result[1] == [], 'normal real-to-real failover must remain owned by Dispatcharr'


def scenario_finished_game_detaches_without_probing(env):
    ch, stream, game = _setup(env)
    _check(env,ch,game,{stream.pk:False})
    game['start_time_utc'] = (kit._now() - timedelta(days=2)).isoformat()
    _check(env,ch,game,{}) # probing would fail this oracle
    assert list(ch.streams.values_list('pk',flat=True)) == [stream.pk]


def scenario_apply_keeps_a_slate_out_of_provider_membership(env):
    ch, stream, game = _setup(env)
    _check(env,ch,game,{stream.pk:False})
    kit._publish(env,[game],kit._settings())
    # A later failed check must still identify the real provider and recover.
    _check(env,ch,game,{stream.pk:True})
    assert list(ch.streams.values_list('pk',flat=True)) == [stream.pk]


def scenario_apply_detached_active_slate_still_switches(env):
    ch, stream, game = _setup(env)
    _check(env,ch,game,{stream.pk:False})
    slate_stream = ch.streams.exclude(pk=stream.pk).get()
    kit._publish(env,[game],kit._settings())
    assert not ch.streams.filter(pk=slate_stream.pk).exists(), 'control: apply should replace links'
    result = _check(env,ch,game,{stream.pk:True},active={'stream_id':str(slate_stream.pk)})
    assert result[1][0].args[1] == stream.pk, 'active detached slate must switch on recovery'


def scenario_fair_scheduler_visits_later_channels(env):
    from dispatcharr_ranked_matchups import slate
    from types import SimpleNamespace,ModuleType
    from unittest.mock import Mock
    import sys
    source,stream=kit._seed_source_channel(env)
    games=[kit._row(i,kit._now()+timedelta(minutes=20),source.pk) for i in (401,402,403)]
    kit._publish(env,games,kit._settings())
    from apps.channels.models import Channel
    first_id = Channel.objects.filter(tvg_id__startswith='ranked_matchups:').order_by('pk').first().pk
    clock=[1000.0];scores={};seen=[]
    redis=Mock()
    redis.lock.return_value.acquire.return_value=True
    redis.exists.return_value=False
    redis.zscore.side_effect=lambda key,pk:scores.get(pk)
    redis.zadd.side_effect=lambda key,values:scores.update(values)
    def reconcile(plugin,ch,game,now,settings=None):
        seen.append(ch.pk);clock[0]+=24;return 'live'
    utils=ModuleType('core.utils');utils.RedisClient=SimpleNamespace(get_client=lambda:redis)
    with patch.dict(sys.modules,{'core.utils':utils}),patch.object(slate,'reconcile',side_effect=reconcile),patch.object(slate,'active_stream',side_effect=lambda ch: {'stream_id':'active'} if ch.pk == first_id else None),patch.object(slate.time,'monotonic',side_effect=lambda:clock[0]),patch.object(slate.time,'time',side_effect=lambda:clock[0]),patch.object(slate,'cache_dir',return_value=__import__('pathlib').Path('/does-not-exist-w323')):
        for _ in range(3):slate.check(env.plugin,{},scheduled=True);clock[0]+=10
    assert len(set(seen))==3, 'a slow first channel starved the remaining lineup'

SCENARIOS = kit.collect_scenarios(globals())
@pytest.fixture(scope='module')
def harness_results(tmp_path_factory):
    return kit.run_harness(tmp_path_factory, __file__, SCENARIOS)
@pytest.mark.parametrize('name', SCENARIOS)
def test_outage_slate(harness_results, name):
    assert harness_results.get(name, 'scenario did not run') is None, harness_results.get(name)


def test_caption_changes_at_kickoff():
    from dispatcharr_ranked_matchups import slate
    now = kit._now()
    game = {'home': 'NC State', 'away': 'Wake Forest', 'start_time_utc': now.isoformat()}
    assert slate.caption(game, now)[1] == 'Feed unavailable, retrying'
    assert slate.caption(game, now - timedelta(seconds=1))[1].startswith('Live at ')


def test_media_format_uses_provider_stats_and_bounded_fallbacks():
    from dispatcharr_ranked_matchups import slate
    from types import SimpleNamespace
    assert slate.media_format([]) == {'video': 'libx264', 'audio': 'ac3', 'layout': 'stereo',
        'rate': 48000, 'width': 1920, 'height': 1080}
    value = slate.media_format([SimpleNamespace(stream_stats={'video_codec':'hevc', 'audio_codec':'ac3',
        'audio_channels':'5.1', 'sample_rate':48000, 'resolution':'1280x720'})])
    assert (value['video'], value['audio'], value['layout'], value['width']) == ('libx265','ac3','5.1',1280)
    broken = slate.media_format([SimpleNamespace(stream_stats={'resolution':'999999x999999', 'audio_channels':'evil', 'audio_codec':'url'})])
    assert broken['width'] == 1920 and broken['layout'] == 'stereo' and broken['audio'] == 'ac3'


def test_no_extra_daemon_or_fetching_path():
    from dispatcharr_ranked_matchups import slate
    import ast
    tree = ast.parse(open(slate.__file__).read())
    assert not any(isinstance(n, ast.Attribute) and n.attr in {'Thread','spawn','urlopen','get','fetch_matchup_composite'}
                   and isinstance(n.value, ast.Name) and n.value.id in {'threading','gevent','requests','urllib','gamethumbs'}
                   for n in ast.walk(tree))
    assert slate.cache_dir('/data/plugins/w323') == __import__('pathlib').Path('/data/plugins/w323/slates')


def test_probe_owns_pool_slot_without_creating_a_preview(monkeypatch):
    from dispatcharr_ranked_matchups import slate
    from types import SimpleNamespace,ModuleType
    from unittest.mock import Mock
    import sys
    pool=ModuleType('apps.m3u.connection_pool')
    profile=SimpleNamespace(pk=21)
    pool.reserve_profile_slot=Mock(return_value=(True,1,None))
    pool.release_profile_slot=Mock(return_value=True)
    redis=object()
    utils=ModuleType('core.utils');utils.RedisClient=SimpleNamespace(get_client=lambda:redis)
    urls=ModuleType('apps.proxy.live_proxy.url_utils')
    urls.get_stream_info_for_switch=Mock(return_value={'url':'http://127.0.0.1/test','user_agent':'test'})
    for name,module in [('apps.m3u.connection_pool',pool),('core.utils',utils),('apps.proxy.live_proxy.url_utils',urls)]:monkeypatch.setitem(sys.modules,name,module)
    profiles=Mock();profiles.filter.return_value.order_by.return_value=[profile]
    stream=SimpleNamespace(pk=3,m3u_account=SimpleNamespace(profiles=profiles))
    # No get_stream/release_stream methods: touching a preview is a hard failure.
    monkeypatch.setattr(slate,'_run',lambda *a,**k:(0,b'x'*256))
    assert slate.probe(SimpleNamespace(uuid='test-channel'),stream) is True
    pool.release_profile_slot.assert_called_once_with(21,redis)
    pool.reserve_profile_slot.return_value=(False,1,'profile_full')
    pool.release_profile_slot.reset_mock()
    assert slate.probe(SimpleNamespace(uuid='test-channel'),stream) is None
    pool.release_profile_slot.assert_not_called()
    pool.reserve_profile_slot.return_value=(True,1,None)
    monkeypatch.setattr(slate,'_run',lambda *a,**k:(_ for _ in ()).throw(TimeoutError()))
    assert slate.probe(SimpleNamespace(uuid='test-channel'),stream) is False
    pool.release_profile_slot.assert_called_once_with(21,redis)
