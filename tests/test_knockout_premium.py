"""The international-tournament knockout premium covers the EURO (#250).

A knockout tie has no standings for the Monte Carlo to simulate, so its
importance is 0 and it scores its tournament-stage band alone (~0.9/10), under
the 5.0 `placeholder_min_score`: an unmatched game gets no channel at all. The
World Cup got a round-scaled premium for that; the EURO did not, so a EURO
2028 quarter-final or final could vanish from the guide. Football-Data's EC
competition uses LAST_16, QUARTER_FINALS, SEMI_FINALS and FINAL (measured
2026-10-12 on EURO 2024: 8 / 4 / 2 / 1 matches; there is no round of 32 and
no third-place match).

The premium is one stage -> value map shared by every competition granted it
(`scoring._KNOCKOUT_IMPORTANCE`, `_KNOCKOUT_PREMIUM_COMPETITIONS`). Values are
literals here, never read back from the constants under test.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups.scoring import (
    GameSignals, Weights, knockout_importance, score_game,
)

UTC = timezone.utc


# ---------------------------------------------------------------------------
# End to end: a refresh scores a EURO knockout game with the premium
# ---------------------------------------------------------------------------

def _knockout_source(code, stage, home="Spain", away="England"):
    from dispatcharr_ranked_matchups.sources.base import GameRow, SportSource
    kickoff = (datetime.now(UTC) + timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)

    class _Knockout(SportSource):
        sport_prefix = code
        sport_label = f"{code} knockout"

        def fetch_upcoming(self, days_ahead=7):
            return [GameRow(sport_prefix=code, sport_label=self.sport_label, home=home, away=away,
                            rank_home=None, rank_away=None, start_time=kickoff,
                            extra={"fd_competition_code": code, "stage": stage, "game_id": "k1"})]

    return _Knockout()


@pytest.mark.parametrize("stage,points", [
    ("LAST_16", 3.4), ("QUARTER_FINALS", 4.0), ("SEMI_FINALS", 4.6), ("FINAL", 6.0)])
def test_a_euro_knockout_game_gets_the_premium(refresh_env, stage, points):
    """WITNESS: a EURO knockout game scored 0.0 importance (the premium was
    World Cup only, #250). It now carries the World Cup's value for its round,
    and says why."""
    _write_prior, run, read = refresh_env
    run([_knockout_source("EC", stage)])
    row = read()["games"][0]
    assert row["importance_points"] == points
    assert row["importance_notes"] == ["EURO knockout: win or go home"]


def test_a_world_cup_knockout_game_is_unchanged(refresh_env):
    """Guard: the World Cup keeps its values and its note."""
    _write_prior, run, read = refresh_env
    run([_knockout_source("WC", "LAST_32", home="France", away="Sweden")])
    row = read()["games"][0]
    assert row["importance_points"] == 2.8
    assert row["importance_notes"] == ["World Cup knockout: win or go home"]


@pytest.mark.parametrize("code", ["CL", "PL", "EFLCUP"])
def test_a_club_or_domestic_cup_knockout_gets_no_premium(refresh_env, code):
    """Guard: club and domestic cups emit the same stage labels and stay
    excluded, which is what the original World-Cup-only scoping protected."""
    _write_prior, run, read = refresh_env
    run([_knockout_source(code, "QUARTER_FINALS", home="Arsenal", away="Chelsea")])
    assert read()["games"][0]["importance_points"] == 0.0


# ---------------------------------------------------------------------------
# The gate and the calibration
# ---------------------------------------------------------------------------

def test_the_gate_reads_the_competition_and_the_stage():
    """Guard: case-insensitive stage, None for a group game, an unknown stage,
    or no competition."""
    assert knockout_importance("EC", "quarter_finals") == 4.0
    assert knockout_importance("EC", "GROUP_STAGE") is None
    assert knockout_importance("EC", "REPLAY") is None
    assert knockout_importance("EC", None) is None
    assert knockout_importance(None, "FINAL") is None


def test_every_euro_round_clears_the_placeholder_bar_and_escalates():
    """Guard: at the default weights each EURO round clears the 5.0
    placeholder bar, and deeper rounds score higher (displayed ~6.5 / 7.5 /
    8.3 / 9.2, the World Cup's calibration)."""
    shown = [round(score_game(GameSignals(team_a="A", team_b="B", tournament_stage=s,
                                          importance_points=knockout_importance("EC", s)),
                              Weights()).final, 1)
             for s in ("LAST_16", "QUARTER_FINALS", "SEMI_FINALS", "FINAL")]
    assert shown == [6.5, 7.5, 8.3, 9.2]


# ---------------------------------------------------------------------------
# Class guard: every wired tournament bracket gets the premium
# ---------------------------------------------------------------------------

# Competitions whose bracket chain is wired but which deliberately get NO
# knockout premium, each with its reason. Empty today.
_PREMIUM_EXEMPT: dict = {}


def test_every_chained_tournament_gets_the_premium():
    """CLASS guard: a competition whose group -> knockout chain is wired
    (`soccer._CHAIN_BRACKETS`) is an international tournament whose knockout
    games score 0 Monte Carlo importance, the #250 defect. Each must be
    granted the premium, for every knockout round its bracket has, or be
    named in `_PREMIUM_EXEMPT` with a reason. Fails on the instrument first:
    the bracket table must hold the two tournaments known to be wired."""
    from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
    assert {"WC", "EC"} <= set(_CHAIN_BRACKETS)
    missing = []
    for code, bracket in _CHAIN_BRACKETS.items():
        if code in _PREMIUM_EXEMPT:
            continue
        stages = [bracket.entry_stage] + [stage for stage, _pairings in bracket.downstream]
        missing += [f"{code} {stage}" for stage in stages if knockout_importance(code, stage) is None]
    assert missing == []
