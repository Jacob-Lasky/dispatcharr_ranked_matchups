"""W332: a final fixture must stop its own capture, retaining the file."""
import copy
import sys
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import ModuleType, SimpleNamespace

import pytest
from dispatcharr_ranked_matchups import _util

KICK = datetime(2026, 11, 11, tzinfo=timezone.utc)
MARKER = "ranked_matchups:CBB:cbb_374841"
GAME = {"sport_prefix": "CBB", "home": "NC State", "away": "Presbyterian",
        "start_time_utc": KICK.isoformat(), "extra": {"cbb_id": 374841}}
EVENT = {"id": "401918957", "date": "2026-11-11T00:00Z",
         "status": {"type": {"name": "STATUS_FINAL", "state": "post", "completed": True}},
         "competitions": [{"competitors": [
             {"homeAway": "home", "team": {"location": "NC State"}},
             {"homeAway": "away", "team": {"location": "Presbyterian"}}]}]}


class Rows:
    def __init__(self, rec):
        self.rec = rec
    def filter(self, **kw):
        return self
    def select_related(self, *args):
        return self
    def select_for_update(self):
        return self
    def first(self):
        return self.rec
    def __iter__(self):
        return iter([self.rec])


@pytest.mark.parametrize('scenario,expected', [
    ('final', 1), ('cancelled', 0), ('rescheduled', 0), ('other_recording', 0),
    ('user_stopped', 0), ('user_edited', 0), ('outage', 0), ('dry_run', 0),
    ('legacy_moved', 0), ('already_edited', 0), ('state_missing', 0),
])
def test_final_stops_running_recording_without_a_handoff(plugin, monkeypatch, scenario, expected):
    rec = SimpleNamespace(pk=58, id=58, task_id="dvr-recording-58", channel_id=390871,
        start_time=KICK + timedelta(minutes=5), end_time=KICK + timedelta(hours=6),
        custom_properties={"ranked_matchups_marker": MARKER, "status": "recording",
                           "program": {"start_time": (KICK-timedelta(minutes=30)).isoformat()}})
    rec.save = lambda **kwargs: None
    entry = finals.recording.written_record(rec.pk, rec.start_time, rec.end_time, rec.channel_id, KICK)
    state = {'created': {MARKER: entry}} if scenario != 'state_missing' else {'created': {}}
    monkeypatch.setattr(finals.recording, 'load_state', lambda path: state)
    if scenario == 'already_edited': rec.channel_id += 1
    event = copy.deepcopy(EVENT)
    if scenario == 'cancelled':
        event['status']['type']['name'] = 'STATUS_CANCELED'
    if scenario == 'rescheduled':
        event['date'] = '2026-11-12T00:00Z'
    if scenario == 'other_recording':
        rec.custom_properties.pop('ranked_matchups_marker')
    if scenario == 'legacy_moved':
        rec.custom_properties['program'] = {'start_time': '2026-11-10T20:30Z'}
    original = copy.deepcopy(rec)
    manager = Rows(rec)
    if scenario in {'user_stopped', 'user_edited'}:
        current = copy.deepcopy(rec)
        if scenario == 'user_stopped': current.custom_properties['status'] = 'stopped'
        else: current.channel_id += 1
        manager.first = lambda: current
    models = ModuleType("apps.channels.models")
    models.Recording = SimpleNamespace(objects=manager)
    monkeypatch.setitem(sys.modules, "apps.channels.models", models)
    from django.db import transaction
    monkeypatch.setattr(transaction, "atomic", nullcontext)
    after_commit = []
    monkeypatch.setattr(transaction, 'on_commit', after_commit.append)
    monkeypatch.setattr(plugin, "_read_stops", lambda: {})
    monkeypatch.setattr(plugin, "_read_cache", lambda: {"games": [GAME]})
    monkeypatch.setattr(_util, "http_get_json", lambda *a, **kw: None if scenario == 'outage' else {"events": [event]})
    class Lease:
        def acquire(self): return True
        def release(self): pass
    class Redis:
        def lock(self, *args, **kwargs): return Lease()
        def set(self, *args, **kwargs): return True
    core = ModuleType('core.utils')
    core.RedisClient = SimpleNamespace(get_client=lambda: Redis())
    monkeypatch.setitem(sys.modules, 'core.utils', core)
    calls = []
    monkeypatch.setitem(sys.modules, 'apps.channels.signals',
                        SimpleNamespace(revoke_task=calls.append))
    settings = {'dry_run': True} if scenario == 'dry_run' else {}
    assert plugin._run_due_stops(settings, now=KICK + timedelta(hours=3)) == expected
    assert calls == []  # teardown must wait until the stopped flag commits
    for callback in after_commit:
        assert rec.custom_properties['status'] == 'stopped'
        callback()
    assert calls == ([rec.task_id] if expected else [])
    if expected:
        assert rec.custom_properties["status"] == "stopped"
        assert plugin._run_due_stops(settings, now=KICK + timedelta(hours=3)) == 0
        assert calls == [rec.task_id]
    else:
        assert rec == original

from dispatcharr_ranked_matchups import recording_finals as finals


def test_final_stop_leaves_capture_transport_for_recorder_to_close(monkeypatch):
    """Disconnecting first makes ffmpeg reconnect into a proxy 503 (W332)."""
    rec = SimpleNamespace(pk=63, task_id='dvr-recording-63', channel_id=400633,
        start_time=KICK, end_time=KICK + timedelta(hours=4),
        custom_properties={'ranked_matchups_marker': MARKER, 'status': 'stopped'})
    monkeypatch.setitem(sys.modules, 'apps.channels.models',
                        SimpleNamespace(Recording=SimpleNamespace(objects=Rows(rec))))
    disconnected, revoked = [], []
    class StopView:
        def stop(self, request, pk=None):
            disconnected.append(pk)
            return SimpleNamespace(status_code=200)
    monkeypatch.setitem(sys.modules, 'apps.channels.api_views',
                        SimpleNamespace(RecordingViewSet=StopView))
    monkeypatch.setitem(sys.modules, 'apps.channels.signals',
                        SimpleNamespace(revoke_task=revoked.append))
    finals._finish_stop(rec.pk, finals._identity(rec))
    assert disconnected == [], 'the recorder must stop ffmpeg before its proxy connection closes'
    assert revoked == [rec.task_id]

@pytest.mark.parametrize('changed', ['missing', 'identity', 'status', 'revocation_failure'])
def test_stop_callback_rechecks_row_and_retains_committed_stop(monkeypatch, caplog, changed):
    rec = SimpleNamespace(pk=63, task_id='dvr-recording-63', channel_id=400633,
        start_time=KICK, end_time=KICK + timedelta(hours=4),
        custom_properties={'ranked_matchups_marker': MARKER, 'status': 'stopped'})
    identity = finals._identity(rec)
    if changed == 'identity':
        rec.channel_id += 1
    if changed == 'status':
        rec.custom_properties['status'] = 'recording'
    monkeypatch.setitem(sys.modules, 'apps.channels.models', SimpleNamespace(
        Recording=SimpleNamespace(objects=Rows(None if changed == 'missing' else rec))))
    revoked = []
    def revoke(task_id):
        revoked.append(task_id)
        raise RuntimeError('broker unavailable')
    monkeypatch.setitem(sys.modules, 'apps.channels.signals', SimpleNamespace(revoke_task=revoke))
    finals._finish_stop(rec.pk, identity)
    assert revoked == ([rec.task_id] if changed == 'revocation_failure' else [])
    if changed == 'revocation_failure':
        assert rec.custom_properties['status'] == 'stopped'  # recorder still sees the stop
        assert 'task revocation for recording 63 failed (RuntimeError)' in caplog.text


@pytest.mark.parametrize('status', ['STATUS_CANCELED', 'STATUS_POSTPONED', 'STATUS_SUSPENDED',
                                    'STATUS_DELAYED', 'STATUS_IN_PROGRESS', 'STATUS_SCHEDULED'])
def test_not_final_even_when_completed_is_true(status):
    event = copy.deepcopy(EVENT)
    event['status']['type']['name'] = status
    assert finals.matching_final(finals.target(GAME, MARKER), [event]) is None

@pytest.mark.parametrize('change', ['time', 'opponent', 'home_away', 'id', 'ambiguous', 'missing'])
def test_wrong_or_rescheduled_event_cannot_stop_this_recording(change):
    wanted = finals.target(GAME, MARKER)
    wanted['event_id'] = EVENT['id']
    event = copy.deepcopy(EVENT)
    events = [event]
    if change == 'time':
        event['date'] = '2026-11-12T00:00Z'
    elif change == 'opponent':
        event['competitions'][0]['competitors'][1]['team']['location'] = 'Troy'
    elif change == 'home_away':
        for c in event['competitions'][0]['competitors']:
            c['homeAway'] = 'away' if c['homeAway'] == 'home' else 'home'
    elif change == 'id':
        event['id'] = 'other-game'
    elif change == 'ambiguous':
        events.append(copy.deepcopy(event))
    else:
        events = []
    assert finals.matching_final(wanted, events) is None


def test_cbb_uses_all_division_one_games_and_eastern_date(monkeypatch):
    seen = []
    def fetch(*args, **kwargs):
        seen.append(args[1])
        return {'events': [EVENT]}
    monkeypatch.setattr(_util, 'http_get_json', fetch)
    assert finals._events(finals.target(GAME, MARKER)) == [EVENT]
    assert 'dates=20261110&limit=1000&groups=50' in seen[0]


def test_unknown_league_retains_window():
    assert finals.target({**GAME, 'sport_prefix': 'UNKNOWN'}, MARKER) is None


def test_final_checks_run_on_both_maintenance_paths(plugin):
    import inspect
    assert 'recording_finals.check' in inspect.getsource(plugin._run_due_stops)
    constructor = inspect.getsource(plugin.Plugin.__init__)
    assert 'recording_finals.check' in constructor
    assert 'reg.feed_check = check_background' in constructor
    from dispatcharr_ranked_matchups import lifecycle
    assert '_run_due_stops(settings)' in inspect.getsource(lifecycle.serve)
    assert plugin._HANDOFF_RECHECK_SECONDS <= finals.INTERVAL


@pytest.mark.parametrize('league,tag', [('NFL','nfl_'),('NBA','nba_'),('WNBA','wnba_'),('NCAAW','ncaaw_'),('CFB',''),('NCAAMSOC','espn_')])
def test_espn_markers_pin_event_id(league, tag):
    wanted = finals.target({**GAME, 'sport_prefix':league}, f'ranked_matchups:{league}:{tag}123')
    assert wanted['event_id'] == '123'


def test_response_cache_uses_same_eastern_day_as_fetch():
    import inspect
    assert "kickoff.astimezone(ZoneInfo('America/New_York')).date()" in inspect.getsource(finals.check)
