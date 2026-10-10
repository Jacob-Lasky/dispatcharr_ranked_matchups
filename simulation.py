"""Sport-agnostic Monte Carlo match importance per Lahvička (2012).

For each (target_match, team, outcome) tuple, this module estimates the
*importance* of the match: the strength of association between the match's
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


# Number of Monte Carlo iterations per importance call when the caller does not
# say. Production passes the `importance_seasons` setting explicitly (see
# plugin._action_refresh), so this only applies to direct callers and tests.
#
# Lahvička (2012) used 10k sims for academic-grade convergence; 1k is plenty
# for the kind of rank-ordering decisions a TV-guide curator needs. Tau-c
# standard error scales as 1/sqrt(N).
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
    total observations (degenerate cases: caller already had no signal).
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
    `source.outcome_labels`: passing an outcome the source doesn't track
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


# Identity keys `_same_match` will honour, in priority order. `fd_id` is
# football-data.org's match id (SoccerSource); `game_id` is the key every
# PointsBasedSportSource and BracketSource stamps on the rows it returns
# from `remaining_matches`.
#
# DO NOT drop `game_id` and DO NOT reorder this to put the date tuple
# first. Without an id match, identity falls through to
# (home, away, start_time), and any source that emits a fixture whose
# upstream feed has not published a date -- `points_based.remaining_matches`
# substitutes a 2099-01-01 sentinel for a missing `start_time` -- fails to
# recognise the target match. The target then gets sampled a second time as
# a "remaining" match, and the contingency table is built from a season the
# recorded W/D/L row does not describe. Nothing raises; the importance
# number is simply wrong. See #65.
#
# NOT the same list as plugin._MARKER_ID_KEYS, deliberately. This one compares
# a scored row against the simulator's own fixture pool, whose rows carry only
# fd_id / game_id (and game_id is synthetic on some pool rows, which is fine
# here because both sides come from the same source). The marker is a CHANNEL
# identity across refreshes and must exclude bare game_id for exactly that
# reason. Merging the two would break one or the other (#217).
_MATCH_ID_KEYS = ("fd_id", "game_id")


def _same_match(a: "GameRow", b: "GameRow") -> bool:
    """Match-identity check for the simulator's "skip target on second pass"
    deduplication. Prefers a shared id from `_MATCH_ID_KEYS`, falling back
    to the (home, away, start_time) tuple only when the two rows share no
    id key: using `is` would only catch reference identity, which breaks
    once `remaining_matches` materializes fresh GameRow instances every
    call.

    An id is consulted only when BOTH rows carry the SAME key, so ids from
    different namespaces (an fd_id that happens to equal some other
    source's game_id) can never collide.
    """
    if a is b:
        return True
    a_extra = a.extra if isinstance(a.extra, dict) else {}
    b_extra = b.extra if isinstance(b.extra, dict) else {}
    for key in _MATCH_ID_KEYS:
        a_id = a_extra.get(key)
        b_id = b_extra.get(key)
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
    per query. Batching reuses the simulated seasons: for K queries this
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
        # Same drain-until-empty loop as monte_carlo_importance: see there
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
        `semifinal` / `final` / `winner`: disjoint by design.
      - `seed_fn` returns a state dict consumable by
        `downstream.remaining_matches` / `sample_result` / `apply_result`
        / `terminal_outcomes`. The downstream's own `initial_state` is
        NOT called by the chain: the seed is the entry point.
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


def monte_carlo_importance_shared(
    source: "SportSource",
    targets: Sequence["GameRow"],
    queries: Sequence[Sequence[Tuple[str, str]]],
    n_sims: int = DEFAULT_N_SIMS,
    rng: Optional[random.Random] = None,
) -> List[Optional[Dict[Tuple[str, str], float]]]:
    """Importance for MANY target matches of one source, from ONE set of N
    simulated seasons (#230).

    `monte_carlo_importance_batch` re-simulates the whole remaining season
    for every target match, so a refresh costs targets x N seasons. With a
    full slate (94 NHL games, 227 NCAA soccer games) that exceeded the
    25-minute pipeline ceiling on every run. Here each simulated season is
    played once, every target's result is read off it at its natural place
    in the schedule, and each (target, team, outcome) query gets its own
    contingency table from the same N seasons: cost is N seasons per source,
    whatever the number of targets. Measured on a real 499-game slate:
    2212 CPU-s at 500 sims per game vs 125 CPU-s at 2000 shared seasons,
    with the shared estimate CLOSER to a 2000-per-game reference (rank
    correlation 0.99 vs 0.97).

    ONLY VALID FOR SOURCES WHOSE `sample_result` IGNORES `state`
    (`shared_season_importance = True`). The per-game simulator samples the
    target FIRST, from the initial state; this one samples it in schedule
    order, from a mid-season state. Those draw from the same distribution
    only when the result model does not read the state. A knockout source
    that reads leg 1 when sampling leg 2 does read it, and stays on the
    per-game path. DO NOT call this for a source that has not opted in.

    Returns one entry per target: `{(team, outcome): |tau_c|}`, or None when
    that target was not played in every simulated season (absent from the
    source's remaining pool, or reachable only on some bracket paths). The
    caller must fall back to the per-game path for a None; it is NOT a zero.
    """
    if not getattr(source, "shared_season_importance", False):
        raise ValueError(
            f"{type(source).__name__} has not opted into shared-season importance "
            "(its sample_result may read state); use monte_carlo_importance_batch."
        )
    rng = rng or random.Random()
    strengths = source.estimate_strengths()
    base_state = source.initial_state()

    tables: List[Dict[Tuple[str, str], List[List[int]]]] = [
        {q: [[0, 0], [0, 0], [0, 0]] for q in qs} for qs in queries
    ]
    appearances = [0] * len(targets)

    # Which target (if any) a pool row IS. remaining_matches builds fresh
    # GameRows every call, so identity is decided by _same_match, exactly as
    # the per-game simulator decides it. Candidates come from every id the
    # row carries AND from its (home, away) pair: an id match must not be
    # missed because the scored row spells a team differently from the season
    # feed. The memo is keyed by ALL of the row's ids plus teams and kickoff,
    # never by one id: _same_match compares the first key BOTH rows carry, so
    # two pool rows sharing one id can still be different targets.
    by_id: Dict[Tuple[str, Any], List[int]] = {}
    by_pair: Dict[Tuple[str, str], List[int]] = {}
    for i, t in enumerate(targets):
        t_extra = t.extra if isinstance(t.extra, dict) else {}
        for k in _MATCH_ID_KEYS:
            if t_extra.get(k) is not None:
                by_id.setdefault((k, t_extra[k]), []).append(i)
        by_pair.setdefault((t.home, t.away), []).append(i)
    resolved: Dict[Any, Optional[int]] = {}

    def target_index(m: "GameRow") -> Optional[int]:
        extra = m.extra if isinstance(m.extra, dict) else {}
        ids = tuple((k, extra.get(k)) for k in _MATCH_ID_KEYS)
        memo_key = (ids, m.home, m.away, m.start_time)
        if memo_key not in resolved:
            candidates: List[int] = []
            for k, v in ids:
                if v is not None:
                    candidates += by_id.get((k, v), [])
            candidates += by_pair.get((m.home, m.away), [])
            resolved[memo_key] = next(
                (i for i in dict.fromkeys(candidates) if _same_match(m, targets[i])), None,
            )
        return resolved[memo_key]

    for _ in range(n_sims):
        state = base_state
        results: Dict[int, "MatchResult"] = {}
        # Drain until empty, as the per-game simulator does: one pass for a
        # league, and correct for any source whose later fixtures appear only
        # once earlier ones are applied.
        while True:
            rem = source.remaining_matches(state)
            if not rem:
                break
            for m in rem:
                ti = target_index(m)
                if ti is not None and ti in results:
                    # A repeated copy of a target already played this season.
                    # The per-game simulator excludes every copy of its target
                    # after sampling it once; sampling the copy here would give
                    # one team an extra game the old path never played.
                    continue
                r = source.sample_result(state, m, strengths, rng)
                if ti is not None:
                    results[ti] = r
                state = source.apply_result(state, m, r)
        outcomes = source.terminal_outcomes(state)
        for ti, r in results.items():
            appearances[ti] += 1
            target = targets[ti]
            for team, outcome in queries[ti]:
                row = _RESULT_ROW[_classify_target_result(r, team, target.home, target.away)]
                col = 0 if outcome in outcomes.get(team, []) else 1
                tables[ti][(team, outcome)][row][col] += 1

    # Only a target played in EVERY simulated season has an unconditional
    # table. One that appears in some seasons only (a bracket fixture that
    # exists only if a feeder tie went one way) would be measured conditional
    # on reaching it, while the per-game simulator forces it into every
    # season: return None so the caller falls back to that path.
    return [
        {q: abs(kendall_tau_c(t)) for q, t in tables[i].items()}
        if n_sims > 0 and appearances[i] == n_sims else None
        for i in range(len(targets))
    ]
