"""Auto-record phase B (#216): ranked-game slots, preferences, interruption,
post-roll yield, and #221 (Apply re-reads Recorded teams)."""

import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups import recording as rp

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
POST = timedelta(hours=2)


def at(hh, mm=0):
    return T0.replace(hour=hh, minute=mm)


def game(marker, kick, *, dur=timedelta(hours=2), recorded=False, favorite=False,
         league_pos=0, rating=5.0):
    """A candidate shaped like apply builds it: 30 min pre-roll, the game's
    scheduled end, then the post-roll."""
    return rp.Candidate(
        marker=marker, kickoff=kick, start=kick - timedelta(minutes=30),
        end=kick + dur + POST, sched_end=kick + dur, recorded=recorded,
        favorite=favorite, league_pos=league_pos, rating=rating,
    )


@pytest.fixture(scope="module")
def src():
    with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as f:
        return f.read()


class TestWorkedExample:
    """The design's example (#216): Favorites NC State, record_leagues
    NCAAF(CFB), NCAAS, EPL; one slot. An EPL game is still being played
    (kicked off 1:30) when NC State's Live block starts at 3:00. An EPL game
    that had already ended would be in post-roll and yield in every mode;
    that case is TestPostRollYield."""

    def _games(self, **ncs):
        epl = game("epl", at(13, 30), league_pos=2, rating=8.0)
        ncs_kw = dict(favorite=True, league_pos=0, rating=6.0)
        ncs_kw.update(ncs)
        nc = game("ncs", at(15, 30), dur=timedelta(hours=3, minutes=30), **ncs_kw)
        return [epl, nc]

    def test_favorites_preference_hands_the_slot_to_nc_state(self):
        plan = rp.plan_recordings(self._games(), slots=1, preference=rp.PREF_FAVORITES)
        assert set(plan.planned) == {"epl", "ncs"}
        # EPL records until NC State's Live-block start, keeping what it got.
        assert plan.planned["epl"][1] == at(15, 0)
        assert plan.handoffs == {"epl": ("ncs", at(15, 0))}

    def test_league_preference_gives_the_same_result_here(self):
        plan = rp.plan_recordings(self._games(), slots=1, preference=rp.PREF_LEAGUE)
        assert plan.handoffs == {"epl": ("ncs", at(15, 0))}

    def test_rating_preference_only_if_nc_state_outrates_the_epl_game(self):
        lower = rp.plan_recordings(self._games(rating=6.0), slots=1, preference=rp.PREF_RATING)
        assert lower.no_slot == ["ncs"] and lower.handoffs == {}
        higher = rp.plan_recordings(self._games(rating=9.0), slots=1, preference=rp.PREF_RATING)
        assert higher.handoffs == {"epl": ("ncs", at(15, 0))}

    def test_earliest_never_interrupts(self):
        plan = rp.plan_recordings(self._games(), slots=1, preference=rp.PREF_EARLIEST)
        assert plan.no_slot == ["ncs"] and plan.planned["epl"][1] == at(17, 30)


class TestInterruptionRules:
    def test_a_recorded_team_takes_a_ranked_slot_even_in_earliest_mode(self):
        plan = rp.plan_recordings(
            [game("ranked", at(13)), game("team", at(14), recorded=True)],
            slots=1, preference=rp.PREF_EARLIEST)
        assert plan.handoffs == {"ranked": ("team", at(13, 30))}

    def test_a_ranked_game_never_takes_a_recorded_teams_slot(self):
        plan = rp.plan_recordings(
            [game("team", at(13), recorded=True), game("fav", at(14), favorite=True, rating=10)],
            slots=1, preference=rp.PREF_FAVORITES)
        assert plan.no_slot == ["fav"] and plan.handoffs == {}

    def test_tie_breaks_never_interrupt(self):
        # Both non-favorites under "favorites": the second only has a better
        # rating (a tier-3 tie-break), which must not bump the first.
        plan = rp.plan_recordings(
            [game("a", at(13), rating=5), game("b", at(14), rating=9)],
            slots=1, preference=rp.PREF_FAVORITES)
        assert plan.no_slot == ["b"] and plan.handoffs == {}

    def test_the_worst_keyed_holder_is_the_one_interrupted(self):
        plan = rp.plan_recordings(
            [game("mid", at(13), rating=6), game("low", at(13, 10), rating=2),
             game("top", at(14), rating=9)],
            slots=2, preference=rp.PREF_RATING)
        assert plan.handoffs == {"low": ("top", at(13, 30))}
        assert plan.planned["mid"][1] == at(13) + timedelta(hours=2) + POST

    def test_a_running_recording_is_never_cut(self):
        held = {"running": (at(12, 30), at(17))}
        plan = rp.plan_recordings(
            [game("running", at(13)), game("fav", at(14), favorite=True)],
            slots=1, held=held, preference=rp.PREF_FAVORITES)
        assert plan.no_slot == ["fav"] and plan.handoffs == {}

    def test_a_holder_that_would_keep_under_15_minutes_is_dropped_whole(self):
        plan = rp.plan_recordings(
            [game("a", at(13, 55)), game("fav", at(14), favorite=True)],
            slots=1, preference=rp.PREF_FAVORITES)
        assert "a" not in plan.planned and plan.no_slot == ["a"]
        assert "a" not in plan.handoffs and "fav" in plan.planned

    def test_a_failed_attempt_changes_nothing(self):
        # Two ranked games hold both slots until 4 PM; the user's own two
        # recordings fill both slots from 4 PM. Cutting both ranked games at
        # 1:30 still leaves no room for the favorite at 4 PM, so the attempt
        # must be rolled back: both keep their full windows, nothing handed over.
        a = rp.Candidate("a", at(12), at(11, 30), at(16), sched_end=at(14), recorded=False)
        b = rp.Candidate("b", at(12), at(11, 30), at(16), sched_end=at(14), recorded=False)
        fav = rp.Candidate("fav", at(14), at(13, 30), at(18), sched_end=at(16), recorded=False, favorite=True)
        busy = [(at(16), at(18)), (at(16), at(18))]
        plan = rp.plan_recordings([a, b, fav], slots=2, busy=busy, preference=rp.PREF_FAVORITES)
        assert plan.no_slot == ["fav"]
        assert plan.handoffs == {}
        assert plan.planned["a"][:2] == (at(11, 30), at(16))
        assert plan.planned["b"][:2] == (at(11, 30), at(16))


class TestAcrossApplies:
    def test_a_cut_recording_that_started_is_not_stretched_back_over_its_taker(self):
        # Apply 1 cut "epl" at 3 PM for the favorite. By apply 2 it is
        # recording, so it comes back as held with the SHORT end.
        epl = game("epl", at(13, 30))
        fav = game("fav", at(15, 30), favorite=True)
        held = {"epl": (at(13), at(15))}
        plan = rp.plan_recordings([epl, fav], slots=1, held=held, preference=rp.PREF_FAVORITES)
        assert plan.planned["epl"][1] == at(15)
        assert "fav" in plan.planned

    def test_a_running_recording_is_still_extended_when_there_is_room(self):
        held = {"a": (at(12, 30), at(16))}
        plan = rp.plan_recordings([game("a", at(13), dur=timedelta(hours=3))], slots=2, held=held)
        assert plan.planned["a"][1] == at(13) + timedelta(hours=3) + POST

    def test_a_dropped_taker_passes_the_handoff_to_whoever_took_the_slot(self):
        # "b" is cut for "c"; "c" is then dropped whole (under 15 min) by the
        # recorded team "d". The guide must say "d" takes b's slot.
        b = game("b", at(12))
        c = game("c", at(13), favorite=True)
        d = game("d", at(13, 10), recorded=True)
        plan = rp.plan_recordings([b, c, d], slots=1, preference=rp.PREF_FAVORITES)
        assert "c" in plan.no_slot
        assert plan.handoffs["b"][0] == "d"


class TestMinimalCuts:
    def test_a_yield_made_pointless_by_an_interruption_is_undone(self):
        # Codex case: A is in post-roll, B is live, the user records 2-5 PM on
        # a 2-slot budget, and higher-rated C needs 12:30-5 PM. Cutting B alone
        # fits C; A must keep its post-roll.
        a = rp.Candidate("a", at(10), at(9, 30), at(14), sched_end=at(12), recorded=False, rating=3)
        b = rp.Candidate("b", at(11), at(10, 30), at(18), sched_end=at(16), recorded=False, rating=4)
        c = rp.Candidate("c", at(13), at(12, 30), at(17), sched_end=at(15), recorded=False, rating=9)
        plan = rp.plan_recordings([a, b, c], slots=2, busy=[(at(14), at(17))], preference=rp.PREF_RATING)
        assert plan.planned["a"][1] == at(14)
        assert plan.handoffs == {"b": ("c", at(12, 30))}

    def test_a_holder_cut_for_a_dropped_taker_records_until_the_real_taker(self):
        b = game("b", at(12))
        c = game("c", at(13), favorite=True)
        d = game("d", at(13, 10), recorded=True)
        plan = rp.plan_recordings([b, c, d], slots=1, preference=rp.PREF_FAVORITES)
        assert plan.handoffs["b"] == ("d", at(12, 40))
        assert plan.planned["b"][1] == at(12, 40)


class TestPostRollYield:
    def test_a_game_in_post_roll_gives_its_slot_to_the_next_kickoff(self):
        # 1 PM game ends 3 PM; its post-roll runs to 5 PM. A 4 PM kickoff
        # (Live block from 3:30) takes the slot in every mode.
        for pref in rp.PREFERENCES:
            plan = rp.plan_recordings([game("one", at(13)), game("four", at(16))], slots=1, preference=pref)
            assert plan.planned["one"][1] == at(15, 30), pref
            assert set(plan.planned) == {"one", "four"}
            assert plan.handoffs["one"] == ("four", at(15, 30))

    def test_a_game_still_on_the_clock_does_not_yield(self):
        plan = rp.plan_recordings([game("one", at(13)), game("two", at(14))], slots=1)
        assert plan.no_slot == ["two"]

    def test_a_running_recording_in_post_roll_is_not_cut(self):
        # Dispatcharr ignores a shorter end_time on a recording that has
        # started, so planning a yield for it would promise a handoff that
        # never happens (the live timer that can stop it is phase C).
        held = {"one": (at(12, 30), at(17))}
        plan = rp.plan_recordings([game("one", at(13)), game("four", at(16))], slots=1, held=held)
        assert plan.no_slot == ["four"] and plan.handoffs == {}

    def test_only_as_many_post_rolls_as_needed_are_cut(self):
        plan = rp.plan_recordings(
            [game("a", at(12)), game("b", at(12, 30)), game("c", at(15, 30))], slots=2)
        assert plan.handoffs == {"a": ("c", at(15, 0))}
        assert plan.planned["b"][1] == at(12, 30) + timedelta(hours=2) + POST


class TestKeysAndText:
    def test_priority_key_orders_tiers(self):
        rec = game("r", at(13), recorded=True)
        fav = game("f", at(13), favorite=True)
        plain = game("p", at(13))
        ordered = sorted([plain, fav, rec], key=lambda c: rp.priority_key(c, rp.PREF_FAVORITES))
        assert [c.marker for c in ordered] == ["r", "f", "p"]

    def test_unknown_preference_falls_back_to_earliest(self):
        plan = rp.plan_recordings([game("a", at(13)), game("b", at(14), favorite=True)],
                                  slots=1, preference="bogus")
        assert plan.no_slot == ["b"]

    def test_handoff_description(self):
        assert rp.description_line(1, 1, until="3:00 PM", taker="NC State vs Louisville") == \
            "Recording until 3:00 PM, then NC State vs Louisville takes the slot."
        assert rp.description_line(2, 3) == "Recording (slot 2 of 3)."


class TestEligibility:
    def test_ranked_games_need_a_stream_and_a_listed_league(self, plugin):
        leagues = plugin._record_leagues({"record_leagues": "CFB, epl"})
        assert leagues == ["cfb", "epl"]
        ok = {"sport_prefix": "EPL", "channel_id": 5}
        assert plugin._ranked_eligible(ok, leagues)
        assert not plugin._ranked_eligible({"sport_prefix": "EPL"}, leagues)          # no stream yet
        assert not plugin._ranked_eligible({"sport_prefix": "NFL", "stream_ids": [1]}, leagues)
        assert plugin._ranked_eligible({"sport_prefix": "NFL", "stream_ids": [1]}, [])  # blank = all

    def test_apply_rematches_recorded_teams_from_the_current_setting(self, plugin):
        games = [{"home": "NC State", "away": "Duke", "recorded_matched": []},
                 {"home": "Arsenal", "away": "Chelsea", "recorded_matched": ["Arsenal"]}]
        plugin._rematch_recorded_teams(games, {"recorded_teams": "NC State"})
        assert games[0]["recorded_matched"] == ["NC State"]
        assert games[1]["recorded_matched"] == []          # removed since the last refresh
        plugin._rematch_recorded_teams(games, {"recorded_teams": ""})
        assert all(g["recorded_matched"] == [] for g in games)


class TestManifest:
    def test_phase_b_settings(self, plugin):
        with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as f:
            by = {x["id"]: x for x in json.load(f)["fields"]}
        assert by[plugin.RECORD_RANKED_SETTING]["default"] is False
        assert by[plugin.RECORD_LEAGUES_SETTING]["type"] == "string"
        pref = by[plugin.RECORDING_PREFERENCE_SETTING]
        assert pref["default"] == rp.PREF_EARLIEST
        assert [o["value"] for o in pref["options"]] == list(rp.PREFERENCES)


class TestWiring:
    def _body(self, src, name):
        i = src.index(f"def {name}(")
        return src[i:src.index("\ndef ", i + 1)]

    def test_rematch_runs_before_the_plan(self, src):
        body = self._body(src, "_action_apply")
        assert body.index("_rematch_recorded_teams(games, settings)") < body.index("_autorecord_prepare(games, settings)")

    def test_prepare_passes_the_preference_and_ranked_eligibility(self, src):
        body = self._body(src, "_autorecord_prepare")
        assert "preference=" in body and "RECORDING_PREFERENCE_SETTING" in body
        assert "_ranked_eligible(g, leagues)" in body
        assert "sched_end=live_end" in body

    def test_guide_line_names_the_handoff(self, src):
        body = self._body(src, "_action_apply")
        assert "autorec.plan.handoffs.get(marker)" in body
