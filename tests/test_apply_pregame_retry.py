"""W322: an empty pregame failure must not discard the game recording.

Run real apply against the ORM. The failure telemetry mirrors recording 58:
capture started 30 minutes early, exhausted the DVR's outage window, and
finished with zero bytes while the event-only upstream was still offline.
"""
import importlib.util
import json
import os
from datetime import timedelta

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "_pregame_kit", os.path.join(HERE, "test_apply_empty_slate.py"))
kit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)
MARKER = "ranked_matchups:CFB:401"


def _failed(env, **props):
    ch, _ = kit._seed_source_channel(env)
    kickoff = kit._now() + timedelta(minutes=20)
    game = kit._row(401, kickoff, ch.id)
    settings = kit._settings(recorded_teams="Georgia")
    owned = kit._publish(env, [game], settings)
    rec = env.m.Recording.objects.get(channel=owned[MARKER])
    rec.custom_properties.update(
        status="interrupted", bytes_written=0, remux_success=False,
        interrupted_reason="no_stream_data: no_segments_in_15.0s",
        started_at=(kickoff - timedelta(minutes=30)).isoformat(),
        ended_at=(kickoff - timedelta(minutes=29)).isoformat(),
        file_path="/recordings/empty.mkv", _hls_dir="/recordings/.empty",
    )
    rec.custom_properties.update(props)
    rec.save()
    return rec, kickoff, settings


def _apply(env, settings):
    result = env.plugin._action_apply(settings)
    assert result["status"] == "ok", result


def scenario_empty_pregame_failure_is_scheduled_once_at_kickoff(env):
    rec, kickoff, settings = _failed(env)
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "scheduled"
    assert rec.start_time == kickoff
    assert rec.custom_properties["ranked_matchups_pregame_retry"] is True
    assert "ended_at" not in rec.custom_properties
    assert "file_path" not in rec.custom_properties
    assert rec.custom_properties["ranked_matchups_marker"] == MARKER
    assert rec.custom_properties["program"]["title"]
    # Applying again cannot move the retry back into the failed pregame span.
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.start_time == kickoff
    assert env.m.Recording.objects.count() == 1
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert json.load(f)["created"][MARKER]["start"] == kickoff.isoformat()
    # A second outage stays terminal; this is not an unbounded retry loop.
    rec.custom_properties.update(status="interrupted", bytes_written=0,
                                 remux_success=False,
                                 interrupted_reason="no_stream_data: outage")
    rec.save()
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "interrupted"


def scenario_user_stop_and_captured_data_are_preserved(env):
    rec, _, settings = _failed(env, status="stopped")
    for props in ({"status": "stopped"},
                  {"status": "interrupted", "bytes_written": 188},
                  {"status": "interrupted", "bytes_written": 0, "remux_success": True},
                  {"remux_success": False, "interrupted_reason": "stopped_by_user"},
                  {"interrupted_reason": "no_stream_data: outage", "ended_at": None}):
        rec.custom_properties.update(props)
        rec.save()
        before = dict(rec.custom_properties)
        _apply(env, settings)
        rec.refresh_from_db()
        assert rec.custom_properties == before


def scenario_user_edit_and_deletion_are_preserved(env):
    rec, _, settings = _failed(env)
    rec.start_time += timedelta(minutes=1)
    rec.save()
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "interrupted"
    rec.delete()
    _apply(env, settings)
    assert env.m.Recording.objects.count() == 0


def scenario_postkickoff_failure_is_preserved(env):
    rec, kickoff, settings = _failed(env)
    rec.custom_properties["ended_at"] = kickoff.isoformat()
    rec.save()
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "interrupted"


def scenario_retry_still_needs_a_provider_slot(env):
    rec, kickoff, settings = _failed(env)
    env.m.Recording.objects.create(
        channel=rec.channel, start_time=kickoff - timedelta(minutes=5),
        end_time=rec.end_time, custom_properties={"status": "scheduled"})
    _apply(env, {**settings, "recording_slots": 1})
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "interrupted"


def scenario_retry_follows_a_corrected_earlier_kickoff(env):
    rec, kickoff, settings = _failed(env)
    _apply(env, settings)
    games = kit._read_cache(env)["games"]
    earlier = kickoff - timedelta(minutes=10)
    games[0]["start_time_utc"] = earlier.isoformat()
    kit._write_cache(env, games)
    _apply(env, settings)
    rec.refresh_from_db()
    assert rec.start_time == earlier


def scenario_user_deletion_during_prepare_is_not_recreated(env):
    rec, _, settings = _failed(env)
    game = kit._read_cache(env)["games"][0]
    game["recorded_matched"] = ["Georgia"]
    ar = env.plugin._autorecord_prepare([game], settings)
    assert MARKER in ar.plan.planned
    channel = rec.channel
    rec.delete()
    env.plugin._autorecord_for_game(ar, MARKER, channel, "Game", "Game", False)
    assert env.m.Recording.objects.count() == 0


def scenario_user_stop_after_prepare_is_preserved(env):
    rec, _, settings = _failed(env)
    game = kit._read_cache(env)["games"][0]
    game["recorded_matched"] = ["Georgia"]
    ar = env.plugin._autorecord_prepare([game], settings)
    assert MARKER in ar.plan.planned
    rec.custom_properties["status"] = "stopped"
    rec.save()
    env.plugin._autorecord_for_game(ar, MARKER, rec.channel, "Game", "Game", False)
    rec.refresh_from_db()
    assert rec.custom_properties["status"] == "stopped"


SCENARIOS = kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_pregame_retry(harness_results, name):
    assert harness_results.get(name, "scenario did not run") is None, harness_results.get(name)
