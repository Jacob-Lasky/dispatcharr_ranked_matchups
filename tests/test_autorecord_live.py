"""Auto-record phase C (#216): stopping a RUNNING recording at a planned
handoff, via the live handoff timer in the reaper loop."""

import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dispatcharr_ranked_matchups import recording as rp

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
POST = timedelta(hours=2)


def at(hh, mm=0):
    return T0.replace(hour=hh, minute=mm)


def game(marker, kick, *, dur=timedelta(hours=2), recorded=False, favorite=False, rating=5.0):
    return rp.Candidate(marker=marker, kickoff=kick, start=kick - timedelta(minutes=30),
                        end=kick + dur + POST, sched_end=kick + dur, recorded=recorded,
                        favorite=favorite, rating=rating)


class TestPlannerCutsRunningRecordingsOnlyWithTheTimer:
    def test_a_recorded_team_stops_a_running_ranked_recording(self):
        held = {"ranked": (at(12, 30), at(17))}
        plan = rp.plan_recordings([game("ranked", at(13)), game("team", at(14), recorded=True)],
                                  slots=1, held=held, live_stops=True)
        assert plan.stops == {"ranked": at(13, 30)}
        assert plan.handoffs == {"ranked": ("team", at(13, 30))}
        assert "team" in plan.planned

    def test_a_running_recording_in_post_roll_is_stopped_for_the_next_kickoff(self):
        held = {"one": (at(12, 30), at(17))}
        plan = rp.plan_recordings([game("one", at(13)), game("four", at(16))],
                                  slots=1, held=held, live_stops=True)
        assert plan.stops == {"one": at(15, 30)}

    def test_without_the_timer_running_recordings_are_still_never_cut(self):
        held = {"ranked": (at(12, 30), at(17))}
        plan = rp.plan_recordings([game("ranked", at(13)), game("team", at(14), recorded=True)],
                                  slots=1, held=held)
        assert plan.stops == {} and plan.no_slot == ["team"]

    def test_a_running_recording_is_stopped_not_dropped_even_if_it_just_began(self):
        # Under 15 minutes in: a not-started holder would be dropped whole, a
        # running one has a file already, so it is stopped.
        held = {"ranked": (at(13, 25), at(17))}
        plan = rp.plan_recordings([game("ranked", at(13, 55)), game("team", at(14), recorded=True)],
                                  slots=1, held=held, live_stops=True)
        assert plan.stops == {"ranked": at(13, 30)}
        assert "ranked" not in plan.no_slot

    def test_a_stopped_recording_is_not_also_extended(self):
        held = {"ranked": (at(12, 30), at(15))}
        plan = rp.plan_recordings(
            [game("ranked", at(13), dur=timedelta(hours=3)), game("team", at(14), recorded=True)],
            slots=1, held=held, live_stops=True)
        assert plan.planned["ranked"][1] == at(13, 30)

    def test_a_ranked_game_still_cannot_stop_a_recorded_team(self):
        held = {"team": (at(12, 30), at(17))}
        plan = rp.plan_recordings([game("team", at(13), recorded=True), game("fav", at(14), favorite=True)],
                                  slots=1, held=held, live_stops=True, preference=rp.PREF_FAVORITES)
        assert plan.stops == {} and plan.no_slot == ["fav"]


def row(rid, marker, status="recording"):
    return SimpleNamespace(id=rid, custom_properties={rp.MARKER_KEY: marker, "status": status})


class TestStopEntries:
    def test_entries_name_the_recording_and_the_taker(self):
        plan = rp.Plan(slots=1, stops={"a": at(13, 30)}, handoffs={"a": ("b", at(13, 30))})
        out = rp.stop_entries(plan, {"a": row(7, "a")}, {"b": "NC State vs Louisville"})
        assert out == {"7": {"at": at(13, 30).isoformat(), "marker": "a", "taker": "b",
                             "taker_name": "NC State vs Louisville"}}

    def test_due_and_next(self):
        entries = {"1": {"at": at(13).isoformat()}, "2": {"at": at(15).isoformat()},
                   "3": {"at": (at(13) - timedelta(minutes=30)).isoformat()}, "4": {"at": "junk"}}
        assert [rid for rid, _ in rp.due_stops(entries, at(13, 5))] == ["1"]   # 3 is past the grace
        # entry 1 is inside its grace window: retry every minute, not at the next stop
        assert rp.next_stop_delay(entries, at(13, 5)) == rp.STOP_RETRY.total_seconds()
        assert rp.next_stop_delay({"2": entries["2"]}, at(13, 5)) == pytest.approx(timedelta(hours=1, minutes=55).total_seconds())
        assert rp.next_stop_delay({}, at(13)) is None


class TestStopWrite:
    def test_mirrors_dispatcharrs_stop(self):
        cp = rp.stopped_properties({rp.MARKER_KEY: "a", "status": "recording", "x": 1}, at(13), "b")
        assert cp["status"] == "stopped" and cp["stopped_at"] and cp["x"] == 1
        assert cp["ranked_matchups_stopped_for"] == "b"

    @pytest.mark.parametrize("cp", [
        {"status": "recording"},                              # not ours
        {rp.MARKER_KEY: "a", "status": "scheduled"},          # not running
        {rp.MARKER_KEY: "a", "status": "stopped"},            # already stopped (another worker)
        None,
    ])
    def test_refuses_anything_but_our_running_recording(self, cp):
        assert rp.stopped_properties(cp, at(13)) is None

    def test_guide_text(self):
        assert rp.strip_dot(rp.DOT + " NC State vs App State ᴸᶦᵛᵉ") == "NC State vs App State ᴸᶦᵛᵉ"
        assert rp.strip_dot("Past: X") == "Past: X"
        desc = "Recording until 3:00 PM, then NC State vs Louisville takes the slot. The big game."
        assert rp.stopped_description(desc, "3:00 PM", "NC State vs Louisville") == \
            "Recorded until 3:00 PM; the slot went to NC State vs Louisville. The big game."


@pytest.fixture(scope="module")
def src():
    with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as f:
        return f.read()


def _body(src, name):
    i = src.index(f"def {name}(")
    return src[i:src.index("\ndef ", i + 1)]


class TestWiring:
    def test_plan_is_built_with_live_stops(self, src):
        assert "live_stops=True," in _body(src, "_autorecord_prepare")

    def test_every_real_apply_rewrites_the_stops_file(self, src):
        body = _body(src, "_autorecord_finish")
        i = body.index("if not dry_run:")
        assert body.index("RECORDING_STOPS_PATH") > i
        assert "if ar.enabled else {}" in body
        assert body.index("RECORDING_STOPS_PATH") < body.index("if not ar.enabled:")

    def test_the_reaper_loop_runs_stops_before_deciding_to_reap(self, src):
        body = _body(src, "_reaper_loop")
        sleep = body.index("if _scheduler_sleep(stop_event, delay):")
        assert sleep < body.index("_run_due_stops(settings)") < body.index("_action_reap_locked(settings)")
        assert "recording_policy.next_stop_delay(_read_stops(), now)" in body
        assert "_HANDOFF_RECHECK_SECONDS" in body

    def test_the_stop_is_locked_idempotent_and_checks_the_taker_has_a_stream(self, src):
        body = _body(src, "_run_due_stops")
        assert "select_for_update().filter(pk=int(rid))" in body
        assert "recording_policy.is_in_progress(rec)" in body
        assert "ChannelStream.objects.filter(channel=taker_ch).exists()" in body
        assert 'rec.save(update_fields=["custom_properties"])' in body
        assert body.index("ChannelStream.objects") < body.index("rec.save(")
        # A stale entry (plan changed, file not rewritten) must not stop a
        # recording for a taker that is no longer set to record.
        assert "custom_properties__{recording_policy.MARKER_KEY}" in body
        assert body.index("taker_recs") < body.index("rec.save(")
        assert "rec.delete()" not in body
        assert "_invalidate_epg_output_cache()" in body


class TestStopsFileReader:
    def test_missing_or_corrupt_means_none(self, plugin, tmp_path, monkeypatch):
        p = tmp_path / "stops.json"
        monkeypatch.setattr(plugin, "RECORDING_STOPS_PATH", str(p))
        assert plugin._read_stops() == {}
        p.write_text("{nope")
        assert plugin._read_stops() == {}
        p.write_text(json.dumps({"7": {"at": "x"}}))
        assert plugin._read_stops() == {"7": {"at": "x"}}


class TestSecondOpinionOnPhaseC:
    def test_a_running_recording_never_yields_to_a_game_that_started_before_it(self):
        # Codex case: held started 13:00, the taker's window began 12:40. Its
        # stop would land before the holder's own start (an inverted window).
        held = {"late": (at(13), at(17))}
        plan = rp.plan_recordings([game("late", at(13, 30)), game("team", at(13, 10), recorded=True)],
                                  slots=1, held=held, live_stops=True)
        assert plan.stops == {}
        assert all(s <= e for s, e, _ in plan.planned.values())

    def test_description_rewrite_survives_a_period_in_a_team_name(self):
        desc = "Recording until 3:00 PM, then St. John's vs UConn takes the slot. Big East clash."
        assert rp.stopped_description(desc, "3:00 PM", "St. John's vs UConn") == \
            "Recorded until 3:00 PM; the slot went to St. John's vs UConn. Big East clash."
        assert rp.stopped_description("Recording (slot 1 of 3). Preview.", "3:00 PM", "X") == \
            "Recorded until 3:00 PM; the slot went to X. Preview."
        assert rp.stopped_description("No line here.", "3:00 PM", "X") == \
            "Recorded until 3:00 PM; the slot went to X. No line here."

    def test_stop_rereads_its_entry_after_locking_and_only_touches_current_programmes(self, src):
        body = _body(src, "_run_due_stops")
        assert body.index("select_for_update()") < body.index("_read_stops().get(rid) != entry")
        assert "end_time__gt=now" in body
