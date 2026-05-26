"""Sport-agnostic Monte Carlo match importance per Lahvička (2012).

For each (target_match, team, outcome) tuple, this module estimates the
*importance* of the match — the strength of association between the match's
W/D/L result and whether `team` reaches `outcome` at season end.

The algorithm, mirrored from the SOTA section of TUNING_REPORT.md:

  1. Estimate per-team strengths via `source.estimate_strengths()`
     (rolling-window goal averages for soccer, points averages for NCAAF, etc).
  2. Sim N seasons. Each sim:
     a. Sample the target match's result.
     b. Sample every remaining match's result.
     c. Roll up the final table / bracket / record per team.
     d. Record (target_result row, outcome row) in a 3x2 contingency table.
  3. Compute |Kendall tau-c| on the table. Returns in [0, 1].

Why |tau_c|: a "relegation" outcome where winning REDUCES the probability is
just as important as a "title" outcome where winning INCREASES it. Importance
is magnitude of association, not its sign.

This module knows nothing about specific sports. The `SportSource` ABC
defines the 7 methods the simulator needs (`estimate_strengths`,
`initial_state`, `remaining_matches`, `sample_result`, `apply_result`,
`terminal_outcomes`, `outcome_labels`); each source implements them.
Sources that don't implement them set `supports_importance = False` and the
caller skips this module entirely.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from .sources.base import GameRow, MatchResult, SportSource


# Number of Monte Carlo iterations per importance call. Per the SOTA section:
# 1000 sims × 10 matches per matchday × 2 competitions = 20k full-season
# simulations per refresh, ~50ms per sim with pure-Python Poisson sampling.
# Comfortably within the scheduler's 6-hour cadence.
#
# Lahvička (2012) used 10k sims for academic-grade convergence; 1k is plenty
# for the kind of rank-ordering decisions a TV-guide curator needs. Tau-c
# standard error scales as 1/sqrt(N); going from 1k → 10k halves the SE but
# 10x the runtime, and the SE at 1k is already finer than the consequence
# weights are calibrated.
DEFAULT_N_SIMS = 1000


# Indices into the 3x2 contingency table. Rows are ordered W < D < L from the
# target team's perspective; columns are outcome-happens=0, outcome-not=1.
# The fixed ordering matters: tau-c assumes ordinal categories, and "winning
# is better than drawing is better than losing" is the natural ranking.
_RESULT_ROW = {"W": 0, "D": 1, "L": 2}


def kendall_tau_c(table: Sequence[Sequence[int]]) -> float:
    """Kendall-Stuart tau-c for an ordinal contingency table.

    Returns the SIGNED tau-c. Caller takes abs() for importance magnitude.

    Formula (Stuart 1953): `tau_c = 2 * m * (P - Q) / (n^2 * (m - 1))` where
    `m = min(rows, cols)`, `n = sum of all cells`, `P = concordant pairs`,
    `Q = discordant pairs`.

    A concordant pair across two distinct cells (i, j) and (i', j') has both
    i < i' AND j < j' (or both >). A discordant pair has one direction
    flipped. Pairs of observations in the same cell (ties on either margin)
    don't contribute.

    Returns 0.0 if the table has fewer than 2 non-trivial rows/cols or zero
    total observations (degenerate cases — caller already had no signal).
    """
    rows = len(table)
    if rows < 2:
        return 0.0
    cols = len(table[0])
    if cols < 2:
        return 0.0
    if any(len(r) != cols for r in table):
        raise ValueError("kendall_tau_c: jagged table")

    n = sum(sum(r) for r in table)
    if n == 0:
        return 0.0

    p = 0  # concordant
    q = 0  # discordant
    for i in range(rows):
        for j in range(cols):
            cell = table[i][j]
            if cell == 0:
                continue
            for ii in range(i + 1, rows):
                for jj in range(j + 1, cols):
                    p += cell * table[ii][jj]
                for jj in range(0, j):
                    q += cell * table[ii][jj]

    m = min(rows, cols)
    denom = n * n * (m - 1) / m
    if denom == 0:
        return 0.0
    return 2.0 * (p - q) / denom


def _classify_target_result(result: "MatchResult", target_team: str, home: str, away: str) -> str:
    """W/D/L from `target_team`'s perspective for the target match."""
    if result.home_goals > result.away_goals:
        winner = home
    elif result.home_goals < result.away_goals:
        winner = away
    else:
        return "D"
    return "W" if winner == target_team else "L"


def monte_carlo_importance(
    source: "SportSource",
    target_match: "GameRow",
    target_team: str,
    target_outcome: str,
    n_sims: int = DEFAULT_N_SIMS,
    rng: Optional[random.Random] = None,
) -> float:
    """Importance of `target_match` for `target_team` reaching `target_outcome`.

    Returns the absolute value of Kendall tau-c in [0, 1]. 0 means the match
    result is already independent of the outcome (team mathematically locked
    in or eliminated); higher means more association.

    Caller is responsible for matching `target_team` and `target_outcome` to
    `source.outcome_labels` — passing an outcome the source doesn't track
    returns 0 (the team will never be in that bucket; tau-c degenerates).
    """
    if not getattr(source, "supports_importance", False):
        raise ValueError(
            f"{type(source).__name__} does not support importance simulation; "
            "check supports_importance before calling."
        )
    rng = rng or random.Random()
    strengths = source.estimate_strengths()
    base_state = source.initial_state()

    table = [[0, 0], [0, 0], [0, 0]]
    home = target_match.home
    away = target_match.away

    for _ in range(n_sims):
        # 1) Sample the target match first; record W/D/L from target_team.
        target_result = source.sample_result(base_state, target_match, strengths, rng)
        row = _RESULT_ROW[_classify_target_result(target_result, target_team, home, away)]

        # 2) Apply the target result, then drain remaining matches until none
        #    are eligible. Re-asking `remaining_matches` per pass (instead of
        #    snapshotting once) is identical behavior for league sources (the
        #    snapshot shrinks by one per apply and is empty after one pass)
        #    AND essential for knockout sources, where bracket downstream
        #    matches (QF → SF → Final) only become eligible as their feeder
        #    ties resolve.
        state = source.apply_result(base_state, target_match, target_result)
        while True:
            rem = [m for m in source.remaining_matches(state)
                   if not _same_match(m, target_match)]
            if not rem:
                break
            for m in rem:
                r = source.sample_result(state, m, strengths, rng)
                state = source.apply_result(state, m, r)

        # 3) Bucket the team's final outcomes; record whether target_outcome fired.
        outcomes = source.terminal_outcomes(state)
        col = 0 if target_outcome in outcomes.get(target_team, []) else 1
        table[row][col] += 1

    return abs(kendall_tau_c(table))


def _same_match(a: "GameRow", b: "GameRow") -> bool:
    """Match-identity check for the simulator's "skip target on second pass"
    deduplication. Compares the (home, away, start_time, fd_id) tuple — using
    `is` would only catch reference identity, which breaks once
    `remaining_matches` materializes fresh GameRow instances every call.
    """
    if a is b:
        return True
    a_extra = a.extra if isinstance(a.extra, dict) else {}
    b_extra = b.extra if isinstance(b.extra, dict) else {}
    a_id = a_extra.get("fd_id") if a_extra else None
    b_id = b_extra.get("fd_id") if b_extra else None
    if a_id is not None and b_id is not None:
        return a_id == b_id
    return (
        a.home == b.home
        and a.away == b.away
        and a.start_time == b.start_time
    )


def monte_carlo_importance_batch(
    source: "SportSource",
    target_match: "GameRow",
    queries: Sequence[Tuple[str, str]],
    n_sims: int = DEFAULT_N_SIMS,
    rng: Optional[random.Random] = None,
) -> Dict[Tuple[str, str], float]:
    """Compute importance for multiple (team, outcome) pairs against the same
    target match, sharing one set of N season simulations.

    A naive loop over `monte_carlo_importance(team, outcome)` re-runs N sims
    per query. Batching reuses the simulated seasons — for K queries this
    drops the cost from K*N to N seasons regardless of K. Critical for
    in-loop scoring where score_game needs importance per (home/away) team
    per outcome band (typically K=8 for EPL: 2 teams × 4 outcome bands).

    Returns `{(team, outcome): |tau_c|}` keyed by the input queries.
    """
    if not getattr(source, "supports_importance", False):
        raise ValueError(
            f"{type(source).__name__} does not support importance simulation; "
            "check supports_importance before calling."
        )
    rng = rng or random.Random()
    strengths = source.estimate_strengths()
    base_state = source.initial_state()

    home = target_match.home
    away = target_match.away

    # One 3x2 table per (team, outcome) query.
    tables: Dict[Tuple[str, str], List[List[int]]] = {
        q: [[0, 0], [0, 0], [0, 0]] for q in queries
    }

    for _ in range(n_sims):
        target_result = source.sample_result(base_state, target_match, strengths, rng)
        state = source.apply_result(base_state, target_match, target_result)
        # Same drain-until-empty loop as monte_carlo_importance — see there
        # for the reasoning. Identical behavior for league sources;
        # essential for knockout sources where downstream bracket matches
        # only become eligible after their feeder ties resolve.
        while True:
            rem = [m for m in source.remaining_matches(state)
                   if not _same_match(m, target_match)]
            if not rem:
                break
            for m in rem:
                r = source.sample_result(state, m, strengths, rng)
                state = source.apply_result(state, m, r)
        outcomes = source.terminal_outcomes(state)

        for team, outcome in queries:
            row = _RESULT_ROW[_classify_target_result(target_result, team, home, away)]
            col = 0 if outcome in outcomes.get(team, []) else 1
            tables[(team, outcome)][row][col] += 1

    return {q: abs(kendall_tau_c(t)) for q, t in tables.items()}


def monte_carlo_importance_batch_chain(
    primary: "SportSource",
    target_match: "GameRow",
    queries: Sequence[Tuple[str, str]],
    downstream: "SportSource",
    seed_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    n_sims: int = DEFAULT_N_SIMS,
    rng: Optional[random.Random] = None,
) -> Dict[Tuple[str, str], float]:
    """Importance batch with a cross-source advancement chain.

    Same shape as `monte_carlo_importance_batch` but runs a SECOND
    simulation phase per sim: after the primary source drains, the
    `seed_fn(primary_state)` callback builds the downstream's initial
    state (typically a knockout bracket whose participants the primary's
    standings determined), the downstream's remaining_matches are
    drained, and the per-team outcomes of both sources are merged
    before the tau-c contingency table is updated.

    Motivation: international soccer tournaments (#53). The group source
    ranks teams within their groups; the knockout source models the
    post-group bracket. A group winner faces a structurally different
    LAST_32 opponent than a runner-up, so group-game leverage on R16+
    bands requires cross-source advancement, not just within-source
    outcomes. The single-source `monte_carlo_importance_batch` cannot
    express this and would return 0 leverage on every downstream band.

    Contract:
      - Primary and downstream MUST emit disjoint outcome vocabularies.
        Otherwise the merge double-counts (a team labeled "advance" by
        both sources would have two entries in the merged list, but
        tau-c bucketing is membership-based so the duplicate is a no-op
        for the query column; it's still a smell). Today the
        GroupStageSoccerSource emits `advance` / `eliminated` and the
        KnockoutSoccerSource emits `round_of_16` / `quarterfinal` /
        `semifinal` / `final` / `winner` — disjoint by design.
      - `seed_fn` returns a state dict consumable by
        `downstream.remaining_matches` / `sample_result` / `apply_result`
        / `terminal_outcomes`. The downstream's own `initial_state` is
        NOT called by the chain — the seed is the entry point.
      - Strength estimation is shared (one call to
        `primary.estimate_strengths()` covers both phases). Today the
        SoccerSource subclasses both inherit identical
        `estimate_strengths` from the same fixture pool; if a future
        chain consumer needs source-specific strengths, the contract
        will need to be revisited.

    Returns `{(team, outcome): |tau_c|}` keyed by the input queries.
    """
    if not getattr(primary, "supports_importance", False):
        raise ValueError(
            f"{type(primary).__name__} does not support importance simulation; "
            "check supports_importance before calling."
        )
    if not getattr(downstream, "supports_importance", False):
        raise ValueError(
            f"{type(downstream).__name__} does not support importance simulation; "
            "check supports_importance before calling."
        )
    rng = rng or random.Random()
    strengths = primary.estimate_strengths()
    primary_base = primary.initial_state()

    home = target_match.home
    away = target_match.away

    tables: Dict[Tuple[str, str], List[List[int]]] = {
        q: [[0, 0], [0, 0], [0, 0]] for q in queries
    }

    for _ in range(n_sims):
        # Phase 1: drain primary from the target match. Same dedupe-and-
        # drain-until-empty loop as monte_carlo_importance_batch.
        target_result = primary.sample_result(primary_base, target_match, strengths, rng)
        primary_state = primary.apply_result(primary_base, target_match, target_result)
        while True:
            rem = [m for m in primary.remaining_matches(primary_state)
                   if not _same_match(m, target_match)]
            if not rem:
                break
            for m in rem:
                r = primary.sample_result(primary_state, m, strengths, rng)
                primary_state = primary.apply_result(primary_state, m, r)

        # Phase 2: seed and drain downstream. No target-match dedupe here:
        # the target match lives in primary's fixture pool, not the
        # downstream's bracket, so collision is impossible by design.
        downstream_state = seed_fn(primary_state)
        while True:
            rem = downstream.remaining_matches(downstream_state)
            if not rem:
                break
            for m in rem:
                r = downstream.sample_result(downstream_state, m, strengths, rng)
                downstream_state = downstream.apply_result(downstream_state, m, r)

        # Phase 3: per-team outcome union and contingency-table update.
        primary_outcomes = primary.terminal_outcomes(primary_state)
        downstream_outcomes = downstream.terminal_outcomes(downstream_state)
        all_teams = set(primary_outcomes) | set(downstream_outcomes)
        merged: Dict[str, List[str]] = {
            t: primary_outcomes.get(t, []) + downstream_outcomes.get(t, [])
            for t in all_teams
        }

        for team, outcome in queries:
            row = _RESULT_ROW[_classify_target_result(target_result, team, home, away)]
            col = 0 if outcome in merged.get(team, []) else 1
            tables[(team, outcome)][row][col] += 1

    return {q: abs(kendall_tau_c(t)) for q, t in tables.items()}
