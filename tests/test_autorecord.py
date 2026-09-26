"""Auto-record for recorded teams (#216, phase A).

The policy module is pure and tested behaviourally. plugin.py's apply needs
Django, so its wiring is pinned by source contracts at the end, the same split
the rest of this suite uses.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dispatcharr_ranked_matchups import recording as rp

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
T0 = datetime(2026, 10, 4, 17, 0, tzinfo=timezone.utc)


def h(n):
    return T0 + timedelta(hours=n)


def cand(marker, kick_h, dur_h=3.0):
    k = h(kick_h)
    return rp.Candidate(marker=marker, kickoff=k, start=k - timedelta(minutes=30),
                        end=k + timedelta(hours=dur_h))


def rec(marker=None, start=None, end=None, status="scheduled", channel_id=1, rid=1):
    cp = {"status": status}
    if marker:
        cp[rp.MARKER_KEY] = marker
    return SimpleNamespace(id=rid, custom_properties=cp, start_time=start or h(0),
                           end_time=end or h(3), channel_id=channel_id)


class TestSlotBudget:
    @pytest.mark.parametrize("configured,limits,reserve,want", [
        (0, [4, 0, 0], 1, 3),        # tightest positive limit minus reserve (the live setup)
        (0, [4, 2], 1, 1),           # tightest wins
        (2, [4], 1, 2),              # a configured value may lower it
        (9, [4], 1, 3),              # but never raise it above the provider
        (0, [0, 0], 1, None),        # no provider limit, nothing configured: unlimited
        (5, [0], 1, 5),              # no provider limit: configured stands alone
        (0, [1], 1, 0),              # reserve eats the only stream: record nothing
        ("x", [4], "y", 3),          # junk settings fall back to defaults
    ])
    def test_budget(self, configured, limits, reserve, want):
        assert rp.resolve_slot_budget(configured, limits, reserve) == want


class TestPlan:
    def test_earliest_kickoff_wins_when_slots_run_out(self):
        plan = rp.plan_recordings([cand("late", 2), cand("early", 0), cand("mid", 1)], slots=2)
        assert set(plan.planned) == {"early", "mid"}
        assert plan.no_slot == ["late"]

    def test_non_overlapping_games_share_a_slot(self):
        plan = rp.plan_recordings([cand("a", 0), cand("b", 5)], slots=1)
        assert set(plan.planned) == {"a", "b"}
        assert plan.planned["a"][2] == plan.planned["b"][2] == 1

    def test_slot_numbers_are_first_fit(self):
        plan = rp.plan_recordings([cand("a", 0), cand("b", 1), cand("c", 4)], slots=2)
        assert [plan.planned[m][2] for m in ("a", "b", "c")] == [1, 2, 1]

    def test_unlimited_plans_everything(self):
        plan = rp.plan_recordings([cand(str(i), 0) for i in range(6)], slots=None)
        assert len(plan.planned) == 6 and plan.no_slot == []

    def test_the_users_own_recordings_take_capacity_first(self):
        plan = rp.plan_recordings([cand("a", 0)], slots=1, busy=[(h(-1), h(4))])
        assert plan.planned == {} and plan.no_slot == ["a"]

    def test_a_started_recording_is_never_displaced(self):
        # "late" is already recording; an earlier-kickoff candidate must not take its slot.
        held = {"late": (h(-0.5), h(3))}
        plan = rp.plan_recordings([cand("late", 0), cand("earlier", -0.2)], slots=1, held=held)
        assert "late" in plan.planned
        assert plan.no_slot == ["earlier"]

    def test_a_running_recording_for_a_dropped_team_still_holds_a_stream(self):
        held = {"gone": (h(-0.5), h(3))}
        plan = rp.plan_recordings([cand("new", 0)], slots=1, held=held)
        assert "gone" not in plan.planned
        assert plan.no_slot == ["new"]

    def test_skipped_markers_are_not_planned_and_take_no_slot(self):
        plan = rp.plan_recordings([cand("tomb", 0), cand("b", 0)], slots=1,
                                  skip={"tomb": "deleted by you"})
        assert set(plan.planned) == {"b"}
        assert plan.skipped == {"tomb": "deleted by you"}


class TestClassifyExisting:
    def test_user_deletion_is_detected_from_state(self):
        state = {"created": {"m": {"id": 5, "at": T0.isoformat()}}, "tombstones": {}}
        by_marker, skip = rp.classify_existing([], state, ["m"])
        assert by_marker == {} and skip == {"m": "deleted by you"}

    def test_tombstone_is_honoured(self):
        _, skip = rp.classify_existing([], {"created": {}, "tombstones": {"m": "x"}}, ["m"])
        assert skip == {"m": "deleted by you"}

    def test_a_recording_the_user_stopped_is_not_restarted(self):
        _, skip = rp.classify_existing([rec("m", status="stopped")], {"created": {}, "tombstones": {}}, ["m"])
        assert skip == {"m": "stopped"}

    def test_live_row_preferred_over_a_finished_one(self):
        done = rec("m", status="completed", rid=1)
        live = rec("m", status="scheduled", rid=2)
        by_marker, skip = rp.classify_existing([done, live], {"created": {}, "tombstones": {}}, ["m"])
        assert by_marker["m"].id == 2 and skip == {}

    def test_rows_without_our_marker_are_ignored(self):
        by_marker, _ = rp.classify_existing([rec(None)], {"created": {}, "tombstones": {}}, [])
        assert by_marker == {}


class TestDecide:
    P = (h(-0.5), h(4), 1)

    def test_create_when_missing(self):
        assert rp.decide(self.P, None, 7) == rp.CREATE

    def test_keep_when_identical(self):
        assert rp.decide(self.P, rec("m", h(-0.5), h(4), channel_id=7), 7) == rp.KEEP

    def test_update_when_the_game_moved(self):
        assert rp.decide(self.P, rec("m", h(-2), h(2), channel_id=7), 7) == rp.UPDATE

    def test_update_when_the_channel_changed(self):
        assert rp.decide(self.P, rec("m", h(-0.5), h(4), channel_id=8), 7) == rp.UPDATE

    def test_in_progress_only_ever_extends(self):
        running = rec("m", h(-0.5), h(3), status="recording", channel_id=7)
        assert rp.decide(self.P, running, 7) == rp.EXTEND
        shorter = (h(-0.5), h(2), 1)
        assert rp.decide(shorter, running, 7) == rp.KEEP

    def test_terminal_is_skipped(self):
        assert rp.decide(self.P, rec("m", status="completed"), 7) == rp.SKIP

    def test_unplanned_is_left_alone_here(self):
        assert rp.decide(None, rec("m"), 7) == rp.KEEP


class TestCancellable:
    def test_only_unplanned_not_started_rows_are_cancelled(self):
        ours = {
            "planned": rec("planned"), "gone": rec("gone"),
            "running": rec("running", status="recording"), "done": rec("done", status="completed"),
            "tomb": rec("tomb"),
        }
        plan = rp.Plan(slots=2, planned={"planned": (h(0), h(3), 1)}, skipped={"tomb": "x"})
        assert [rp.rec_marker(r) for r in rp.cancellable(ours, plan)] == ["gone"]


class TestDot:
    def test_overlapping_live_recording_shows_the_dot(self):
        assert rp.recording_over([rec(None, h(0), h(3))], h(1), h(2))

    def test_finished_or_stopped_recordings_do_not(self):
        rows = [rec(None, h(0), h(3), status=s) for s in rp.TERMINAL_STATUSES]
        assert not rp.recording_over(rows, h(1), h(2))

    def test_a_recording_on_another_window_does_not(self):
        assert not rp.recording_over([rec(None, h(5), h(6))], h(1), h(2))

    def test_description_line(self):
        assert rp.description_line(1, 3) == "Recording (slot 1 of 3)."
        assert rp.description_line(None, None) == "Recording."


class TestState:
    def test_roundtrip_and_corrupt_file(self, tmp_path):
        p = tmp_path / "s.json"
        st = {"created": {"m": {"id": 1, "at": T0.isoformat()}}, "tombstones": {"t": T0.isoformat()}}
        rp.save_state(str(p), st)
        assert rp.load_state(str(p)) == st
        p.write_text("{not json")
        assert rp.load_state(str(p)) == {"created": {}, "tombstones": {}}
        assert rp.load_state(str(tmp_path / "missing.json")) == {"created": {}, "tombstones": {}}

    def test_prune_keeps_live_markers_and_drops_old_ones(self):
        old = (T0 - timedelta(days=30)).isoformat()
        st = {"created": {"live": {"id": 1, "at": old}, "old": {"id": 2, "at": old},
                          "new": {"id": 3, "at": T0.isoformat()}},
              "tombstones": {"live_t": old, "old_t": (T0 - timedelta(days=200)).isoformat()}}
        out = rp.prune_state(st, {"live", "live_t"}, T0)
        assert set(out["created"]) == {"live", "new"}
        assert set(out["tombstones"]) == {"live_t"}


class TestNamingAndTitles:
    def test_recording_dot_token(self):
        from dispatcharr_ranked_matchups import naming
        assert "recording_dot" in naming.KNOWN_TOKENS
        on = naming.render_name("{recording_dot }{home_team}", naming.build_context(home="X", recording=True))
        off = naming.render_name("{recording_dot }{home_team}", naming.build_context(home="X"))
        assert on == "\U0001F534 X" and off == "X"

    def test_one_glyph_everywhere(self):
        from dispatcharr_ranked_matchups import naming
        assert naming._RECORDING_DOT == rp.DOT

    def test_default_template_has_no_dot(self):
        from dispatcharr_ranked_matchups import naming
        assert "recording_dot" not in naming.DEFAULT_NAME_TEMPLATE

    def test_programme_title_dot_on_upcoming_and_live_only(self, plugin):
        for state in ("upcoming", "live"):
            assert plugin._build_program_title(state, "A vs B", "Today", recording=True).startswith(rp.DOT + " ")
            assert not plugin._build_program_title(state, "A vs B", "Today").startswith(rp.DOT)
        assert not plugin._build_program_title("past", "A vs B", "Today", recording=True).startswith(rp.DOT)


class TestSplitApplied:
    def test_a_forced_game_past_the_cap_is_applied(self, plugin):
        rows = [{"n": i} for i in range(5)]
        rows[4]["favorites_matched"] = ["NC State"]    # a favorite playing tomorrow
        rows[3]["recorded_matched"] = ["Wrexham"]
        applied, bench = plugin._split_applied(rows, 2)
        assert [r["n"] for r in applied] == [0, 1, 3, 4]
        assert [r["n"] for r in bench] == [2]

    def test_plain_cap_when_nothing_is_forced(self, plugin):
        rows = [{"n": i} for i in range(4)]
        applied, bench = plugin._split_applied(rows, 2)
        assert [r["n"] for r in applied] == [0, 1] and [r["n"] for r in bench] == [2, 3]


class TestFavoritesOnlyGateKeepsRecordedTeams:
    def test_recorded_team_survives_strict_mode(self, plugin):
        g = lambda h_, a_: SimpleNamespace(home=h_, away=a_, extra={})
        games = [g("NC State", "Duke"), g("Wrexham", "Stoke City"), g("Arsenal", "Chelsea")]
        kept, _src, dropped = plugin._filter_favorites_only(
            games, [None] * 3, ["NC State"], "strict", always_keep=["Wrexham"])
        assert [x.home for x in kept] == ["NC State", "Wrexham"] and dropped == 1


class TestFriendliesGateSeesRecordedTeams:
    def test_union_is_passed_to_friendlies(self, plugin):
        srcs = plugin._build_sources({
            "enable_club_friendlies": True, "favorites": "Arsenal",
            "recorded_teams": "Wrexham, arsenal",
        })
        fr = [s for s in srcs if type(s).__name__ == "ClubFriendliesSource"]
        assert fr and fr[0].favorites == ["Arsenal", "Wrexham"]


class TestManifest:
    def _fields(self):
        with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as f:
            return json.load(f)["fields"]

    def test_recorded_teams_sits_right_under_favorites(self):
        ids = [f["id"] for f in self._fields()]
        assert ids[ids.index("favorites") + 1] == "recorded_teams"

    def test_recording_settings_and_defaults(self, plugin):
        by = {f["id"]: f for f in self._fields()}
        assert by[plugin.RECORDING_POST_ROLL_SETTING]["default"] == plugin.DEFAULT_RECORDING_POST_ROLL_MINUTES == 120
        assert by[plugin.RECORDING_SLOTS_SETTING]["default"] == 0
        assert by[plugin.RECORDING_RESERVE_SETTING]["default"] == 1
        assert by[plugin.RECORDED_TEAMS_SETTING]["type"] == "string"


@pytest.fixture(scope="module")
def src():
    with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as f:
        return f.read()


class TestApplyWiring:
    """_action_apply needs Django; pin the call sites and the write shapes."""

    def _body(self, src, name):
        i = src.index(f"def {name}(")
        j = src.index("\ndef ", i + 1)
        return src[i:j]

    def test_prepare_runs_after_dedupe_and_before_the_prepass(self, src):
        body = self._body(src, "_action_apply")
        assert body.index("_unique_by_marker(") < body.index("autorec = _autorecord_prepare(games, settings)") \
            < body.index("seen_markers.add(marker)")

    def test_cancel_runs_inside_the_transaction_before_the_stale_reap(self, src):
        body = self._body(src, "_action_apply")
        atomic = body.index("with transaction.atomic():")
        assert atomic < body.index("_autorecord_cancel(autorec, dry_run)") < body.index("stale = [ch for marker")

    def test_game_write_precedes_the_dot_read(self, src):
        body = self._body(src, "_action_apply")
        assert body.index("_autorecord_for_game(") < body.index("rec_dot = _autorecord_dot(vc.id, prog_start, prog_end)")
        assert 'recording=rec_dot)' in body

    def test_recorded_team_always_gets_a_placeholder(self, src):
        assert 'if score_val >= placeholder_threshold or g.get("recorded_matched"):' in src

    def test_channel_name_uses_the_plan(self, src):
        assert "recording=autorec.will_record(marker)," in src

    def test_recording_writes_fire_signals(self, src):
        body = self._body(src, "_autorecord_for_game")
        assert "Recording.objects.create(" in body
        assert "existing.save()" in body
        assert ".update(" not in body and "update_fields" not in body

    def test_recording_window_uses_the_live_block(self, src):
        body = self._body(src, "_autorecord_prepare")
        assert "_live_window(g, start_dt)" in body
        assert body.count("_live_window(") == 1
        apply = self._body(src, "_action_apply")
        assert "prog_start, prog_end = _live_window(g, start_dt)" in apply

    def test_refresh_force_includes_recorded_teams(self, src):
        body = self._body(src, "_action_refresh")
        assert "always_keep=recorded_teams" in body
        assert '"recorded_matched": recorded_by_game.get(id(game), [])' in body
        assert "_split_applied(games_payload, max_games)" in body


class TestUserAlreadyRecording:
    def test_users_own_recording_on_the_channel_covers_the_game(self):
        rows = [rec(None, h(-0.5), h(3), channel_id=7)]
        assert rp.covered_by_user(rows, 7, h(0), h(2))

    def test_ours_or_another_channel_or_finished_does_not(self):
        assert not rp.covered_by_user([rec("m", h(-0.5), h(3), channel_id=7)], 7, h(0), h(2))
        assert not rp.covered_by_user([rec(None, h(-0.5), h(3), channel_id=8)], 7, h(0), h(2))
        assert not rp.covered_by_user([rec(None, h(-0.5), h(3), status="completed", channel_id=7)], 7, h(0), h(2))

    def test_prepare_skips_a_game_the_user_is_recording(self, src):
        body = src[src.index("def _autorecord_prepare("):src.index("\ndef ", src.index("def _autorecord_prepare(") + 1)]
        assert "recording_policy.covered_by_user(live_rows, ch_id, c.start, c.end)" in body
        assert body.index("covered_by_user") < body.index("recording_policy.plan_recordings(")


class TestSecondOpinionFindings:
    """Each case below failed on the first cut of phase A (codex-dg gpt-6-sol review)."""

    def test_capacity_counts_overbooked_user_recordings(self):
        # 1 slot, the user already runs two overlapping recordings; the second
        # must still count, so a game inside its window gets no slot.
        busy = [(h(0), h(1)), (h(0.5), h(3))]
        plan = rp.plan_recordings([rp.Candidate("g", h(1.5), h(1.5), h(2.5))], slots=1, busy=busy)
        assert plan.no_slot == ["g"]

    def test_a_running_recording_is_extended_when_its_game_runs_later(self):
        held = {"m": (h(-0.5), h(3))}
        plan = rp.plan_recordings([rp.Candidate("m", h(0), h(-0.5), h(4))], slots=1, held=held)
        s, e, _ = plan.planned["m"]
        assert (s, e) == (h(-0.5), h(4))
        running = rec("m", h(-0.5), h(3), status="recording", channel_id=7)
        assert rp.decide(plan.planned["m"], running, 7) == rp.EXTEND

    def test_a_running_recording_is_never_shortened_by_the_plan(self):
        held = {"m": (h(-0.5), h(5))}
        plan = rp.plan_recordings([rp.Candidate("m", h(0), h(-0.5), h(4))], slots=1, held=held)
        assert plan.planned["m"][1] == h(5)

    def test_naive_datetimes_from_the_db_do_not_crash(self):
        naive = SimpleNamespace(id=1, custom_properties={"status": "scheduled"}, channel_id=7,
                                start_time=h(0).replace(tzinfo=None), end_time=h(3).replace(tzinfo=None))
        assert rp.occupies_stream(naive, h(-1))
        assert rp.recording_over([naive], h(1), h(2))
        assert rp.covered_by_user([naive], 7, h(1), h(2))
        mine = SimpleNamespace(**{**vars(naive), "custom_properties": {"status": "scheduled", rp.MARKER_KEY: "m"}})
        assert rp.decide((h(0), h(3), 1), mine, 7) == rp.KEEP

    def test_a_tombstone_outlives_a_long_postponement(self):
        st = {"created": {}, "tombstones": {"m": (T0 - timedelta(days=60)).isoformat()}}
        assert "m" in rp.prune_state(st, set(), T0)["tombstones"]
        st = {"created": {}, "tombstones": {"m": (T0 - timedelta(days=200)).isoformat()}}
        assert "m" not in rp.prune_state(st, set(), T0)["tombstones"]

    def test_a_recording_the_user_edited_is_left_alone(self):
        wrote = rp.written_record(5, h(-0.5), h(4), 7, T0)
        edited = rec("m", h(-0.5), h(6), channel_id=7, rid=5)     # user pushed the end out
        state = {"created": {"m": wrote}, "tombstones": {}}
        by_marker, skip = rp.classify_existing([edited], state, ["m"])
        assert skip == {"m": "you changed it"}
        plan = rp.plan_recordings([cand("m", 0)], slots=1, skip=skip)
        assert "m" not in plan.planned
        assert rp.cancellable(by_marker, plan) == []

    def test_an_edited_recording_is_not_cancelled_when_its_team_is_removed(self):
        wrote = rp.written_record(5, h(-0.5), h(4), 7, T0)
        edited = rec("m", h(1), h(4), channel_id=7, rid=5)
        by_marker, skip = rp.classify_existing([edited], {"created": {"m": wrote}, "tombstones": {}}, [])
        plan = rp.plan_recordings([], slots=1, skip=skip)
        assert rp.cancellable(by_marker, plan) == []

    def test_an_unedited_recording_is_managed(self):
        wrote = rp.written_record(5, h(-0.5), h(4), 7, T0)
        same = rec("m", h(-0.5), h(4), channel_id=7, rid=5)
        _, skip = rp.classify_existing([same], {"created": {"m": wrote}, "tombstones": {}}, ["m"])
        assert skip == {}

    def test_state_is_rewritten_after_every_write(self, src):
        body = src[src.index("def _autorecord_for_game("):src.index("\ndef ", src.index("def _autorecord_for_game(") + 1)]
        assert body.count("recording_policy.written_record(") == 3
