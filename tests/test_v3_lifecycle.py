"""The same distribution supports 0.33 and the v3 host contract."""
import importlib
import json
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dispatcharr_ranked_matchups import storage, lifecycle, slate


def test_v3_manifest_uses_supported_schema_and_exact_disclosures():
    manifest = json.loads((Path(__file__).parents[1] / 'plugin.json').read_text())
    # Plugins v3 is the feature name. Its guarded manifest schema is version 2.
    assert manifest['manifest_version'] == 2
    assert set(manifest['capabilities']) == {
        'background_tasks', 'persistent_service', 'subprocess', 'outbound_network',
        'filesystem_write', 'proxy_internals', 'external_dependencies',
    }
    assert {a['id'] for a in manifest['actions'] if a.get('async')} == {
        *lifecycle.PIPELINE_ACTIONS, 'check_feeds',
    }


def test_v3_constructor_installs_route_without_legacy_threads(plugin, monkeypatch):
    monkeypatch.setattr(storage, 'managed_services', lambda: True)
    monkeypatch.delenv(plugin.tasks.PIPELINE_CHILD_ENV, raising=False)
    route = Mock()
    monkeypatch.setattr(slate, 'install_route', route)
    migrate = Mock()
    monkeypatch.setattr(plugin, '_migrate_stored_settings', migrate)
    monkeypatch.setattr(plugin.threading, 'Thread', Mock(side_effect=AssertionError('unmanaged thread')))
    plugin.Plugin()
    route.assert_called_once()
    migrate.assert_called_once()


def test_pipeline_child_starts_no_services_or_routes(plugin, monkeypatch):
    monkeypatch.setattr(storage, 'managed_services', lambda: True)
    monkeypatch.setenv(plugin.tasks.PIPELINE_CHILD_ENV, '1')
    route = Mock()
    monkeypatch.setattr(slate, 'install_route', route)
    migrate = Mock()
    monkeypatch.setattr(plugin, '_migrate_stored_settings', migrate)
    plugin.Plugin()
    route.assert_not_called()
    migrate.assert_not_called()


def test_leader_uses_host_service_and_stop_leaves_host_transaction_alone(plugin, monkeypatch):
    monkeypatch.setattr(storage, 'managed_services', lambda: True)
    monkeypatch.setattr(storage, 'configure', Mock())
    start, close = Mock(), Mock()
    monkeypatch.setattr(plugin, '_scheduler_close_db', close)
    instance = object.__new__(plugin.Plugin)
    instance.on_leader_acquired({'start_service': start})
    assert start.call_count == 1
    assert callable(start.call_args.args[0])
    instance.stop({})
    instance.on_leader_lost({})
    close.assert_not_called()


@pytest.mark.parametrize('action', list(lifecycle.PIPELINE_ACTIONS))
def test_v3_actions_return_completed_result_and_report_progress(plugin, monkeypatch, action):
    outcome = {'status': 'ok', 'message': 'child finished'}
    child = Mock(return_value=outcome)
    monkeypatch.setattr(plugin.tasks, 'run_pipeline_subprocess', child)
    monkeypatch.setattr(plugin, '_try_acquire_scheduler_lock', lambda *a: 'owned')
    monkeypatch.setattr(plugin, '_release_scheduler_lock', Mock())
    report = Mock()
    result = plugin.Plugin.run(None, action, {}, {'dispatch_task': Mock(), 'report_progress': report})
    assert result == outcome
    child.assert_called_once_with(lifecycle.PIPELINE_ACTIONS[action], {})
    assert report.call_args_list[0].args[0] == 5
    assert report.call_args_list[-1].args == (100, 'child finished')


def test_v3_action_lock_contention_is_visible(plugin, monkeypatch):
    monkeypatch.setattr(plugin, '_try_acquire_scheduler_lock', lambda *a: None)
    monkeypatch.setattr(plugin, '_run_lock_unavailable_reason', lambda: 'busy')
    result = plugin.Plugin.run(None, 'refresh', {}, {'dispatch_task': Mock()})
    assert result == {'status': 'partial', 'lock_busy': True, 'message': 'busy'}


def test_legacy_run_retains_existing_async_launcher(plugin, monkeypatch):
    launch = Mock(return_value='legacy-id')
    monkeypatch.setattr(plugin.tasks, 'run_refresh_background', launch)
    result = plugin.Plugin.run(None, 'refresh', {}, {'settings': {}})
    assert result['status'] == 'queued'
    assert result['task_id'] == 'legacy-id'


def test_storage_imports_once_preserves_keys_tombstones_and_existing_state(plugin, tmp_path, monkeypatch):
    old, data = tmp_path / 'old', tmp_path / 'durable'
    old.mkdir(); data.mkdir()
    (old / 'recordings_state.json').write_text('{"deleted":[17]}')
    (old / 'cache.json').write_text('old')
    (data / 'cache.json').write_text('current')
    (old / 'cfbd_api_key').write_text('fixture-key')
    (old / 'cfbd_api_key').chmod(0o600)
    (old / 'season_cache').mkdir()
    (old / 'season_cache' / 'one.gz').write_bytes(b'kept')
    monkeypatch.setattr(storage, 'CODE_DIR', old)
    monkeypatch.setattr(storage, '_root', old)
    # configure changes module path constants; monkeypatch records their originals.
    for name in ('CACHE_PATH', 'RECORDING_STATE_PATH', 'RECORDING_STOPS_PATH',
                 'NUMBERING_STATE_PATH', 'LLM_DESCRIPTIONS_CACHE_PATH',
                 'SPORTSDB_THUMB_CACHE_PATH', 'GAMETHUMBS_CACHE_PATH', 'CFBD_KEY_PATH',
                 'FD_KEY_PATH', 'ODDS_KEY_PATH', 'ANTHROPIC_KEY_PATH', 'BOXING_KEY_PATH', 'SPORTSDB_KEY_PATH'):
        monkeypatch.setattr(plugin, name, getattr(plugin, name))
    for name, attribute in [('tasks', 'LAST_RUN_PATH'), ('sources._odds', 'CACHE_PATH'),
                            ('sources._season_store', 'STORE_DIR'), ('sources._college_data', 'CACHE_DIR')]:
        module = importlib.import_module('dispatcharr_ranked_matchups.' + name)
        monkeypatch.setattr(module, attribute, getattr(module, attribute))
    storage.configure({'data_dir': str(data)})
    assert Path(plugin.CACHE_PATH).read_text() == 'current'
    assert Path(plugin.RECORDING_STATE_PATH).read_text() == '{"deleted":[17]}'
    assert (data / 'cfbd_api_key').stat().st_mode & 0o777 == 0o600
    assert (data / 'season_cache' / 'one.gz').read_bytes() == b'kept'
    assert slate.cache_dir() == data / 'slates'
    (data / 'cache.json').unlink()
    monkeypatch.setattr(storage, '_root', old)
    storage.configure({'data_dir': str(data)})
    assert not (data / 'cache.json').exists()
    assert Path(plugin.tasks.LAST_RUN_PATH).parent == data


def test_storage_legacy_context_does_not_move_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(storage, '_root', tmp_path)
    storage.configure({})
    assert storage.path('cache.json') == str(tmp_path / 'cache.json')


@pytest.mark.parametrize('name', ['recordings_state.json', 'season_cache/one.gz'])
def test_interrupted_migration_never_publishes_partial_state(monkeypatch, tmp_path, name):
    old, data = tmp_path / 'old', tmp_path / 'data'
    source = old / name
    source.parent.mkdir(parents=True)
    source.write_text('{"deleted":[17]}')
    monkeypatch.setattr(storage, 'CODE_DIR', old)
    original = storage.shutil.copy2

    def interrupted(source, target):
        Path(target).write_text('{"deleted":')
        raise OSError('interrupted copy')

    monkeypatch.setattr(storage.shutil, 'copy2', interrupted)
    with pytest.raises(OSError):
        storage._import_legacy(data)
    assert not (data / name).exists()
    assert not (data / '.legacy-imported').exists()
    monkeypatch.setattr(storage.shutil, 'copy2', original)
    storage._import_legacy(data)
    assert (data / name).read_text() == source.read_text()
    assert (data / '.legacy-imported').exists()


@pytest.mark.parametrize('ran,status,expected', [(False, 'partial', 'delete'),
                                              (True, 'ok', 'expire'), (True, 'error', None)])
def test_managed_rematch_finishes_cooldown_from_its_own_result(plugin, monkeypatch, ran, status, expected):
    outcome = {'status': status, 'message': 'this run'}
    monkeypatch.setattr(lifecycle, '_pipeline', lambda *a: (ran, outcome))
    complete = plugin._finish_m3u_rematch
    finish = Mock()
    monkeypatch.setattr(plugin, '_finish_m3u_rematch', finish)
    assert lifecycle.run(None, 'auto_pipeline', {'_m3u_rematch': True}, {}) == outcome
    finish.assert_called_once_with(ran, outcome)
    client = Mock()
    complete(ran, outcome, client)
    assert client.delete.called == (expected == 'delete')
    assert client.expire.called == (expected == 'expire')


def test_key_uses_host_identity_and_child_override(monkeypatch):
    monkeypatch.setenv(storage.KEY_ENV, 'canonical_plugin')
    assert storage.plugin_key() == 'canonical_plugin'


def test_managed_service_queues_one_slot_and_stops_cleanly(plugin, monkeypatch):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    settings = {'auto_refresh_enabled': True, 'scheduled_times': now.strftime('%H%M'), 'dry_run': True}
    event = threading.Event()
    calls = []
    ticks = []
    # Two ticks in the same slot must dispatch one pipeline, not two.
    def wait(seconds):
        ticks.append(seconds)
        if len(ticks) == 2:
            event.set()
    event.wait = wait
    monkeypatch.setattr(slate, 'check', Mock())
    monkeypatch.setattr(plugin, '_run_due_stops', Mock())
    monkeypatch.setattr(plugin, '_scheduler_close_db', Mock())
    monkeypatch.setattr(plugin, '_maybe_rematch_after_m3u', Mock())
    monkeypatch.setattr(plugin.tasks.health, 'report_refresh', Mock())
    lifecycle.serve(SimpleNamespace(), {'get_settings': lambda: settings,
                    'dispatch_scheduled_task': lambda *args: calls.append(args)}, event)
    assert len(calls) == 1
    assert calls[0][0] == 'auto_pipeline'
    assert lifecycle.SLOT_PARAM in calls[0][1]
    assert slate.check.call_count == 2
    assert plugin._scheduler_close_db.call_count == 3


def test_managed_slot_delegates_to_existing_claim_retry_path(plugin, monkeypatch):
    run = Mock()
    monkeypatch.setattr(plugin, '_run_scheduled_slot', run)
    monkeypatch.setattr(plugin.tasks, 'read_last_run', lambda: {'status': 'ok', 'message': 'scheduled'})
    settings = {'auto_refresh_enabled': True, lifecycle.SLOT_PARAM: '2026-11-10T10:00:00+00:00'}
    assert lifecycle.run(None, 'auto_pipeline', settings, {})['status'] == 'partial'
    run.assert_called_once()


def test_http_deadline_accepts_guarded_connection_constructors(monkeypatch):
    from dispatcharr_ranked_matchups import _http
    import functools
    original = _http.http.client.HTTPConnection
    calls = []

    @functools.wraps(original)
    def guarded(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(_http.http.client, 'HTTPConnection', guarded)
    connection = _http._tracked_class(original)('127.0.0.1', timeout=1)
    assert calls == [True]
    assert callable(connection._create_connection)
