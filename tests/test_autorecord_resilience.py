"""Regressions from the 1.29.0 production run (#223, #224).

#224: a one-off CFBD failure emptied every CFB game from the cache and apply
cancelled a scheduled NC State recording, because "not in the slate" was read
as "no longer wanted". #223: the scheduled pipeline ran with settings read
before its multi-hour sleep, so a Recorded teams edit made during the sleep was
ignored and a recording was cancelled.
"""

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dispatcharr_ranked_matchups import recording as rp

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def rec(marker, reason=None, teams=None, league=None, status="scheduled"):
    cp = {"status": status, rp.MARKER_KEY: marker}
    if reason:
        cp[rp.REASON_KEY] = reason
    if teams is not None:
        cp[rp.TEAMS_KEY] = teams
    if league is not None:
        cp[rp.LEAGUE_KEY] = league
    return SimpleNamespace(id=1, custom_properties=cp, start_time=T0, end_time=T0 + timedelta(hours=4), channel_id=7)


EMPTY = rp.Plan(slots=3)


class TestVanishedGameIsNotCancelledOnNoEvidence:
    def test_source_outage_keeps_a_recorded_team_recording(self):
        # The #224 incident: the game is gone from the whole cache (applied and
        # bench) but NC State is still a recorded team.
        ours = {"m": rec("m", rp.REASON_TEAM, ["NC State"], "CFB")}
        holds = rp.reason_checker(["NC State"], record_ranked=False, leagues=[])
        assert rp.cancellable(ours, EMPTY, slate=set(), reason_holds=holds) == []

    def test_vanished_game_is_cancelled_once_its_team_is_removed(self):
        ours = {"m": rec("m", rp.REASON_TEAM, ["NC State"], "CFB")}
        holds = rp.reason_checker(["Duke"], record_ranked=False, leagues=[])
        assert [r.custom_properties[rp.MARKER_KEY] for r in rp.cancellable(ours, EMPTY, set(), holds)] == ["m"]

    def test_team_match_is_case_insensitive(self):
        holds = rp.reason_checker(["nc state"], record_ranked=False, leagues=[])
        assert holds(rec("m", rp.REASON_TEAM, ["NC State"]))

    def test_vanished_ranked_game_kept_while_ranked_recording_still_covers_its_league(self):
        ours = {"m": rec("m", rp.REASON_RANKED, [], "CFB")}
        assert rp.cancellable(ours, EMPTY, set(), rp.reason_checker([], True, ["cfb"])) == []
        assert rp.cancellable(ours, EMPTY, set(), rp.reason_checker([], True, [])) == []

    def test_vanished_ranked_game_cancelled_when_ranked_off_or_league_dropped(self):
        ours = {"m": rec("m", rp.REASON_RANKED, [], "CFB")}
        assert len(rp.cancellable(ours, EMPTY, set(), rp.reason_checker([], False, []))) == 1
        assert len(rp.cancellable(ours, EMPTY, set(), rp.reason_checker([], True, ["epl"]))) == 1

    def test_a_row_without_a_stored_reason_is_kept_when_its_game_vanished(self):
        # Rows created by 1.29.0 / 1.30.0 carry no reason; no evidence, no cancel.
        ours = {"m": rec("m")}
        assert rp.cancellable(ours, EMPTY, set(), rp.reason_checker([], False, [])) == []

    def test_a_game_still_in_the_slate_but_unplanned_is_cancelled_as_before(self):
        # Present in the cache, no longer planned (team removed, lost its slot):
        # that IS evidence, so the old behaviour stands.
        ours = {"m": rec("m", rp.REASON_TEAM, ["NC State"])}
        holds = rp.reason_checker(["NC State"], False, [])
        assert len(rp.cancellable(ours, EMPTY, slate={"m"}, reason_holds=holds)) == 1


class TestReasonIsStoredOnTheRecording:
    def test_reason_properties(self):
        c_team = rp.Candidate("a", T0, T0, T0 + timedelta(hours=3), recorded=True)
        c_rank = rp.Candidate("b", T0, T0, T0 + timedelta(hours=3), recorded=False)
        assert rp.reason_properties(c_team, ["NC State"], "CFB") == {
            rp.REASON_KEY: rp.REASON_TEAM, rp.TEAMS_KEY: ["NC State"], rp.LEAGUE_KEY: "CFB",
            rp.GAME_KEY: []}
        assert rp.reason_properties(c_rank, [], "EPL", ["A", "B"]) == {
            rp.REASON_KEY: rp.REASON_RANKED, rp.TEAMS_KEY: [], rp.LEAGUE_KEY: "EPL",
            rp.GAME_KEY: ["A", "B"]}


@pytest.fixture(scope="module")
def src():
    with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as f:
        return f.read()


def _body(src, name):
    i = src.index(f"def {name}(")
    return src[i:src.index("\ndef ", i + 1)]


class TestWiring:
    def test_scheduler_rereads_settings_after_waking(self, src):
        body = _body(src, "_scheduler_loop")
        sleep = body.index("if _scheduler_sleep(stop_event, sleep_s):")
        fire = body.index("tasks.run_scheduled_pipeline(settings)")
        between = body[sleep:fire]
        assert "settings = plugin_ref.get_current_settings()" in between
        assert 'settings.get("auto_refresh_enabled", False)' in between

    def test_apply_passes_the_bench_as_part_of_the_slate(self, src):
        assert "_autorecord_prepare(games, settings, bench=cache.get(\"bench\") or [])" in src

    def test_cancel_uses_the_slate_and_the_reason_check(self, src):
        body = _body(src, "_autorecord_cancel")
        assert "recording_policy.cancellable(ar.ours, ar.plan, ar.slate, ar.reason_holds)" in body

    def test_writes_store_the_reason(self, src):
        body = _body(src, "_autorecord_for_game")
        assert body.count("_reason_props(ar, marker)") >= 2      # create AND update
        assert "recording_policy.reason_properties(" in _body(src, "_reason_props")


class TestFreshRowAndBackfill:
    def test_the_row_is_reread_before_deciding(self, src):
        body = _body(src, "_autorecord_for_game")
        reread = body.index("existing = Recording.objects.select_for_update().filter(pk=existing.pk).first()")
        assert reread < body.index("recording_policy.decide(planned, existing, channel.id)")

    def test_keep_backfills_the_reason_on_old_rows(self, src):
        body = _body(src, "_autorecord_for_game")
        assert "_backfill_reason(ar, marker, existing)" in body
        fill = _body(src, "_backfill_reason")
        assert "is_in_progress(existing)" in fill and 'update_fields=["custom_properties"]' in fill


class TestSecondOpinionOnTheHotfix:
    def test_a_renamed_list_entry_that_still_matches_the_game_keeps_it(self):
        from dispatcharr_ranked_matchups.scoring import match_favorites
        r = rec("m", rp.REASON_TEAM, ["NC Sate"], "CFB")        # typo at the time
        r.custom_properties[rp.GAME_KEY] = ["NC State", "App State"]
        holds = rp.reason_checker(["NC State"], False, [], matcher=match_favorites)
        assert holds(r)
        assert not rp.reason_checker(["Duke"], False, [], matcher=match_favorites)(r)

    def test_a_stale_reason_is_detected(self):
        r = rec("m", rp.REASON_RANKED, [], "CFB")
        c = rp.Candidate("m", T0, T0, T0 + timedelta(hours=3), recorded=True)
        assert rp.reason_changed(r, rp.reason_properties(c, ["NC State"], "CFB", ["NC State", "Duke"]))
        same = rp.reason_properties(c, ["NC State"], "CFB", ["NC State", "Duke"])
        r2 = rec("m"); r2.custom_properties.update(same)
        assert not rp.reason_changed(r2, same)

    def test_rows_are_locked_before_write_and_before_cancel(self, src):
        assert "select_for_update().filter(pk=existing.pk)" in _body(src, "_autorecord_for_game")
        cancel = _body(src, "_autorecord_cancel")
        assert "select_for_update().filter(pk=rec.pk)" in cancel
        assert cancel.index("is_in_progress(rec)") < cancel.index("rec.delete()")

    def test_backfill_refreshes_a_stale_reason(self, src):
        assert "recording_policy.reason_changed(existing, props)" in _body(src, "_backfill_reason")
