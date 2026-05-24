"""Tests for the sport-agnostic Monte Carlo importance simulator.

The simulator (`simulation.monte_carlo_importance`) is tested against an
in-memory FakeSource that implements the 7-method interface
deterministically. SoccerSource's implementation is tested separately in
test_sources.py — this file isolates the simulator from any source-specific
fetching, so failures here are unambiguously bugs in the algorithm, not in
HTTP plumbing or strength estimation.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pytest

from dispatcharr_ranked_matchups import simulation
from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult, SportSource


# ---------- kendall_tau_c ----------

class TestKendallTauC:
    def test_perfect_direct_association(self):
        # W → outcome happens, L → outcome doesn't. Maximum positive tau-c.
        table = [[1000, 0], [0, 0], [0, 1000]]
        assert simulation.kendall_tau_c(table) == pytest.approx(1.0, abs=0.001)

    def test_perfect_inverse_association(self):
        # W → outcome doesn't, L → does. Maximum negative tau-c.
        table = [[0, 1000], [0, 0], [1000, 0]]
        assert simulation.kendall_tau_c(table) == pytest.approx(-1.0, abs=0.001)

    def test_independence(self):
        # All rows look the same. tau-c = 0.
        table = [[100, 100], [100, 100], [100, 100]]
        assert simulation.kendall_tau_c(table) == pytest.approx(0.0)

    def test_empty_table_returns_zero(self):
        assert simulation.kendall_tau_c([[0, 0], [0, 0], [0, 0]]) == 0.0

    def test_single_row_returns_zero(self):
        # Degenerate: not enough rows for ordinal association.
        assert simulation.kendall_tau_c([[100, 200]]) == 0.0

    def test_single_column_returns_zero(self):
        # Degenerate: not enough columns.
        assert simulation.kendall_tau_c([[100], [200], [300]]) == 0.0

    def test_jagged_table_raises(self):
        with pytest.raises(ValueError):
            simulation.kendall_tau_c([[1, 2], [3]])

    def test_moderate_association(self):
        # W slightly favors yes, L slightly favors no, D neutral.
        # tau-c should be positive but well below 1.0.
        table = [[700, 300], [500, 500], [300, 700]]
        tau = simulation.kendall_tau_c(table)
        assert 0.0 < tau < 1.0
        # Sanity check: should mirror to negative on flip.
        table_flipped = [[300, 700], [500, 500], [700, 300]]
        assert simulation.kendall_tau_c(table_flipped) == pytest.approx(-tau)


# ---------- FakeSource for the simulator ----------

@dataclass
class FakeSource(SportSource):
    """In-memory deterministic source for unit-testing the simulator.

    The fake "league" has 4 teams playing a 6-match round-robin season,
    plus a fixed strength configuration. The strength + result functions
    are pure-Python and rng-driven so tests can hold seed and observe
    convergence in the contingency table.
    """
    all_matches: List[GameRow] = field(default_factory=list)
    strengths_map: Dict[str, Dict[str, float]] = field(default_factory=dict)
    teams: List[str] = field(default_factory=list)
    _supports_importance: bool = True
    _outcome_labels: List[str] = field(default_factory=lambda: ["champion", "wooden_spoon"])

    @property
    def supports_importance(self) -> bool:  # type: ignore[override]
        return self._supports_importance

    @property
    def sport_prefix(self) -> str:
        return "FAKE"

    @property
    def sport_label(self) -> str:
        return "Fake League"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        return list(self.all_matches)

    @property
    def outcome_labels(self) -> List[str]:
        return list(self._outcome_labels)

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        return self.strengths_map

    def initial_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {"_applied": frozenset()}
        for t in self.teams:
            state[t] = {"played": 0, "points": 0, "gf": 0, "ga": 0}
        return state

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        applied = state.get("_applied", frozenset())
        return [m for m in self.all_matches if m.extra.get("fd_id") not in applied]

    def sample_result(self, state, match, strengths, rng):
        h = strengths.get(match.home, {"lam_home": 1.0, "lam_away": 1.0})
        a = strengths.get(match.away, {"lam_home": 1.0, "lam_away": 1.0})
        lam_h = (h["lam_home"] + a.get("lam_conceded_away", 1.0)) / 2.0
        lam_a = (a["lam_away"] + h.get("lam_conceded_home", 1.0)) / 2.0
        return MatchResult(home_goals=_poisson(lam_h, rng), away_goals=_poisson(lam_a, rng))

    def apply_result(self, state, match, result):
        new_state = dict(state)
        new_state[match.home] = dict(state[match.home])
        new_state[match.away] = dict(state[match.away])
        h = new_state[match.home]
        a = new_state[match.away]
        h["played"] += 1
        a["played"] += 1
        h["gf"] += result.home_goals
        h["ga"] += result.away_goals
        a["gf"] += result.away_goals
        a["ga"] += result.home_goals
        if result.home_goals > result.away_goals:
            h["points"] += 3
        elif result.home_goals < result.away_goals:
            a["points"] += 3
        else:
            h["points"] += 1
            a["points"] += 1
        fd_id = match.extra.get("fd_id")
        if fd_id is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {fd_id}
        return new_state

    def terminal_outcomes(self, state):
        teams = [(name, row) for name, row in state.items() if name != "_applied"]
        teams.sort(key=lambda kv: (-kv[1]["points"], -(kv[1]["gf"] - kv[1]["ga"]), -kv[1]["gf"]))
        out: Dict[str, List[str]] = {n: [] for n, _ in teams}
        if teams:
            out[teams[0][0]].append("champion")
            out[teams[-1][0]].append("wooden_spoon")
        return out


def _poisson(lam: float, rng: random.Random) -> int:
    """Same as SoccerSource._poisson — duplicated here to keep the test file
    independent of soccer.py's module-private helper."""
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= rng.random()
    return k - 1


def _make_round_robin() -> Tuple[FakeSource, GameRow]:
    """Build a 4-team, 6-match fake season.

    Strengths are seeded so:
      - Alpha is a powerhouse (high attack home AND away)
      - Delta is dreadful (low attack, high conceded)
      - Beta / Gamma are middle-of-pack

    Match #3 (Alpha vs Delta) is the target match used by most tests.
    For an Alpha-vs-Delta game, Alpha winning strongly increases the
    chance Alpha finishes champion AND that Delta finishes wooden spoon
    — strong importance signal.
    """
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    teams = ["Alpha", "Beta", "Gamma", "Delta"]
    matches = [
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Alpha", away="Beta",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 1}),
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Gamma", away="Delta",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 2}),
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Alpha", away="Delta",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 3}),
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Beta", away="Gamma",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 4}),
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Delta", away="Beta",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 5}),
        GameRow(sport_prefix="FAKE", sport_label="Fake", home="Gamma", away="Alpha",
                rank_home=None, rank_away=None, start_time=base,
                extra={"fd_id": 6}),
    ]
    strengths = {
        "Alpha": {"lam_home": 3.0, "lam_away": 2.5, "lam_conceded_home": 0.5, "lam_conceded_away": 0.8},
        "Beta":  {"lam_home": 1.5, "lam_away": 1.2, "lam_conceded_home": 1.3, "lam_conceded_away": 1.5},
        "Gamma": {"lam_home": 1.3, "lam_away": 1.0, "lam_conceded_home": 1.4, "lam_conceded_away": 1.6},
        "Delta": {"lam_home": 0.5, "lam_away": 0.3, "lam_conceded_home": 2.8, "lam_conceded_away": 3.0},
    }
    source = FakeSource(
        all_matches=matches,
        strengths_map=strengths,
        teams=teams,
    )
    target = matches[2]  # Alpha vs Delta
    return source, target


# ---------- monte_carlo_importance ----------

class TestMonteCarloImportance:
    def test_returns_value_in_unit_interval(self):
        source, target = _make_round_robin()
        rng = random.Random(42)
        imp = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=200, rng=rng,
        )
        assert 0.0 <= imp <= 1.0

    def test_unsupported_source_raises(self):
        source, target = _make_round_robin()
        source._supports_importance = False
        with pytest.raises(ValueError, match="does not support importance"):
            simulation.monte_carlo_importance(
                source, target, "Alpha", "champion", n_sims=10,
            )

    def test_outcome_not_in_labels_yields_zero(self):
        # If the source never produces this outcome label, the contingency
        # table degenerates (col 1 fills entirely) → tau-c = 0.
        source, target = _make_round_robin()
        rng = random.Random(0)
        imp = simulation.monte_carlo_importance(
            source, target, "Alpha", "nonexistent_outcome",
            n_sims=50, rng=rng,
        )
        assert imp == 0.0

    def test_deterministic_with_seed(self):
        source, target = _make_round_robin()
        a = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=200, rng=random.Random(7),
        )
        b = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=200, rng=random.Random(7),
        )
        assert a == b

    def test_alpha_champion_has_meaningful_importance(self):
        # Alpha is the powerhouse; Alpha vs Delta should swing the title
        # outcome (an Alpha loss here lets Beta / Gamma chase).
        source, target = _make_round_robin()
        imp = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=400, rng=random.Random(13),
        )
        # Not asserting a tight bound (depends on strength config), but
        # importance for a powerhouse playing the weakest team should be
        # at least *some* signal — far from zero.
        assert imp > 0.05

    def test_uninvolved_team_outcome_yields_low_importance(self):
        # Beta isn't playing in the target match. The target match's
        # result has near-zero direct impact on Beta's wooden-spoon
        # chances (it can affect indirectly through table swings, but the
        # signal should be muted relative to the directly-involved teams).
        source, target = _make_round_robin()
        beta_imp = simulation.monte_carlo_importance(
            source, target, "Beta", "wooden_spoon",
            n_sims=400, rng=random.Random(13),
        )
        alpha_imp = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=400, rng=random.Random(13),
        )
        # Indirect signal exists but should be smaller than the direct
        # involvement signal.
        assert beta_imp < alpha_imp


# ---------- monte_carlo_importance_batch ----------

class TestMonteCarloImportanceBatch:
    def test_batch_matches_individual_for_single_query(self):
        # Single-query batch should produce the same value as the
        # individual call when seeded identically.
        source, target = _make_round_robin()
        single = simulation.monte_carlo_importance(
            source, target, "Alpha", "champion",
            n_sims=300, rng=random.Random(99),
        )
        batch = simulation.monte_carlo_importance_batch(
            source, target,
            queries=[("Alpha", "champion")],
            n_sims=300, rng=random.Random(99),
        )
        assert batch[("Alpha", "champion")] == single

    def test_batch_returns_one_entry_per_query(self):
        source, target = _make_round_robin()
        queries = [
            ("Alpha", "champion"),
            ("Alpha", "wooden_spoon"),
            ("Delta", "wooden_spoon"),
            ("Delta", "champion"),
        ]
        out = simulation.monte_carlo_importance_batch(
            source, target, queries=queries,
            n_sims=200, rng=random.Random(0),
        )
        assert set(out.keys()) == set(queries)
        for v in out.values():
            assert 0.0 <= v <= 1.0

    def test_batch_unsupported_source_raises(self):
        source, target = _make_round_robin()
        source._supports_importance = False
        with pytest.raises(ValueError, match="does not support importance"):
            simulation.monte_carlo_importance_batch(
                source, target, queries=[("Alpha", "champion")], n_sims=10,
            )

    def test_empty_queries_returns_empty_dict(self):
        source, target = _make_round_robin()
        out = simulation.monte_carlo_importance_batch(
            source, target, queries=[], n_sims=50, rng=random.Random(0),
        )
        assert out == {}


# ---------- _same_match ----------

class TestSameMatch:
    def test_identical_reference(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        m = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={"fd_id": 7})
        assert simulation._same_match(m, m) is True

    def test_same_fd_id_different_reference(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={"fd_id": 7})
        b = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={"fd_id": 7})
        assert a is not b
        assert simulation._same_match(a, b) is True

    def test_fallback_to_tuple_when_no_fd_id(self):
        # No fd_id in extra → compare (home, away, start_time).
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={})
        b = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={})
        assert simulation._same_match(a, b) is True

    def test_different_teams_not_same(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = GameRow(sport_prefix="X", sport_label="X", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=base, extra={"fd_id": 1})
        b = GameRow(sport_prefix="X", sport_label="X", home="A", away="C",
                    rank_home=None, rank_away=None, start_time=base, extra={"fd_id": 2})
        assert simulation._same_match(a, b) is False
