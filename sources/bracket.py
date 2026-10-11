"""Generic bracket Monte Carlo machinery for knockout / playoff sports.

Four concrete shapes share the bracket state machine:

  - Two-leg aggregate (UEFA cup soccer): tied teams play home + away;
    aggregate goals + away-goals/ET/penalties decide. See AggregateLegSource.
  - Best-of-N series (NHL / NBA / MLB / NCAA Baseball-Softball Super
    Regional + Finals): tied teams play up to N games; first to ceil(N/2)
    wins advances. See BestOfNSeriesSource.
  - N-team double-elimination (NCAA Baseball / Softball Regional sites
    and the 8-team MCWS/WCWS bracket modeled as two 4-team sub-brackets):
    4 teams enter the tie, each eliminated at 2 losses, last team
    standing wins. See DoubleEliminationSource.
  - Single-elimination (planned: NCAA M/W tournament): one game per tie.

The shared bits live in `BracketSportSource`:
  - Bracket structural inference (`feeds_from` keyed by FD-published team
    membership, so SCHEDULED upstream slots still resolve).
  - Per-team `round_reached` tracking with the `KNOCKOUT_ROUND_DEPTH`
    lookup from `scoring`.
  - `terminal_outcomes` cascade: a team that reached depth D gets every
    band whose cutoff depth <= D.
  - The `initial_state` / `apply_result` / `remaining_matches` driver
    that calls into subclass hooks for the tie-specific game emission
    and result-application logic.

Sport-specific bits (subclass hooks):
  - `_fetch_bracket_games()`: source data normalized to per-GAME records
    (one entry per leg / per series game / per single-elim game).
  - `_new_tie_record(tie_meta)`: empty tie state container.
  - `_record_game_into_tie(tie, home, away, hs, as_, game_index)`: how a
    game updates the tie state and possibly resolves it.
  - `_is_decisive_game(tie, leg_index, legs_in_tie)`: whether this game
    completes the tie (drives ET / OT logic in `sample_result`).
  - `_emit_remaining_games_for_tie(tie, tie_meta, applied, participants)`:
    next-game(s) eligible to play given current tie state.

Bracket data shape (returned by `_fetch_bracket_games`):
  [
    {
      "game_id":    any unique key (int / str),
      "stage":      str matching one of self.KO_STAGES,
      "matchday":   int,   # leg index for soccer, game index for series, 1 for single-elim
      "home":       str,
      "away":       str,
      "home_goals": int | None,   # None when SCHEDULED
      "away_goals": int | None,
      "status":     "FINISHED" | "SCHEDULED" | str,
      "start_time": datetime | None,
      "extra":      dict,         # carries source-specific game metadata
    },
    ...
  ]
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .base import GameRow, MatchResult, SportSource
from .._util import poisson_sample

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.bracket")


_SENTINEL_START = datetime(2099, 1, 1, tzinfo=timezone.utc)


def _game_index(extra: Dict[str, Any]) -> Any:
    """The game's position in its tie: the bracket's own index when
    place_upcoming stamped one (a fetched row's `matchday` is the source's,
    not a leg number; see place_upcoming), else `matchday` as before."""
    idx = extra.get("bracket_game_index")
    return idx if isinstance(idx, int) else extra.get("matchday", 1)


class ForcedWinnerPoissonSampler:
    """`sample_result` for a playoff game that cannot end level: Poisson
    points (goals, runs) for each side from the two teams' strengths, and a
    tied regulation result resolved by a coin-flipped +1 (overtime, extra
    innings, a shootout or penalties). Strength asymmetry already biased the
    means, so the tiebreak adds none. The ONE copy of this model for every
    playoff source (#249): MLB, NHL, NBA, NFL, WNBA, NCAAW, NCAA baseball,
    softball and soccer cups, MLS Cup. The regular-season sampler in
    points_based.py is the same model on purpose.

    DO NOT swap in a sport's regular-season sampler (MLS, NCAA soccer and
    NHL regular season allow draws or award OT points): a bracket cascade
    needs every game to have a winner.

    The class needs `_strength_for(strengths, team)` returning
    `{"pf_per_game", "pa_per_game"}`. Put it BEFORE the bracket base in the
    bases, or SportSource's NotImplementedError sample_result wins the MRO.
    """

    # Lowest scoring rate a side is sampled at. Soccer sides set 0.05: a
    # 0.1 floor would lift two blanking defences' expected goals noticeably.
    LAMBDA_FLOOR: float = 0.1

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: Any,
    ) -> MatchResult:
        del state  # per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(self.LAMBDA_FLOOR, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(self.LAMBDA_FLOOR, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home = poisson_sample(lam_home, rng)
        away = poisson_sample(lam_away, rng)
        if home == away:
            if rng.random() < 0.5:
                home += 1
            else:
                away += 1
        return MatchResult(home_goals=home, away_goals=away)


class BracketSportSource(SportSource):
    """Shared bracket state machine. Subclasses fill in the tie-shape hooks.

    State model:
      {
        "_applied":       frozenset of game IDs whose results are baked in,
        "_tie_results":   {tie_key: tie_dict},   # subclass-defined dict shape
        "_bracket":       {stage: [tie_meta, ...]},   # static, built once
        "_round_reached": {team: max KNOCKOUT_ROUND_DEPTH advanced INTO},
      }

    `tie_key = (stage, frozenset({team_a, team_b}))`. Order-independent so
    leg 1 / game 1 / etc. of the same tie key into the same dict regardless
    of which side is home in which game.

    Subclasses MUST set `KO_STAGES` to the stage ordering used by this
    sport (e.g., ("PLAYOFFS","LAST_16",...) for soccer cups, ("R1","R2",
    "CONF_FINAL","CUP_FINAL") for NHL playoffs).
    """

    supports_importance = True

    KO_STAGES: Tuple[str, ...] = ()

    # initial_state can be expensive (full-season fetch + bracket inference).
    # Cached on the instance; the simulator's `compute_match_importance` calls
    # initial_state once per importance query and reuses the result across
    # the per-game leverage sweep. Subclasses that already cache via
    # SoccerSource heritage share the same attr; if a subclass doesn't
    # initialize it in __init__, the class-level None default applies.
    _initial_state_cache: Optional[Dict[str, Any]] = None

    # ---------- bracket-fetch failure accounting (#245) ----------
    #
    # A bracket assembled from a fetch in which ANY request failed is not the
    # bracket. A missing day of an ESPN day sweep is missing games, so a
    # series reads 2-1 when it is 3-1 (importance computed against the wrong
    # series); every day failing is an empty postseason. DO NOT cache either
    # in `_bracket_games_cache`: the scoring-time placement (#243) and
    # initial_state() read the bracket again after allow_bracket_scoring_retry,
    # and a cached failure is what they would get instead of the retry.
    #
    # "Never cache a failure" is not "retry forever" (#228, the soccer
    # season fetch): the failure COUNT is remembered, the result is not.
    # BRACKET_FETCH_MAX_ATTEMPTS per refresh (sources are built fresh per
    # refresh, so this is per-instance state), scoring is guaranteed one of
    # them, and a source that gave up is reported by bracket_fetch_failure()
    # so the missing stakes are loud in the refresh summary and Show current
    # state. Once given up, _fetch_bracket_games returns [] with no network:
    # a partial bracket would file results into wrong ties, which is worse
    # than no bracket importance (place_upcoming's rule).
    BRACKET_FETCH_MAX_ATTEMPTS: int = 2
    # Class defaults; the methods below assign per instance. The day memo is
    # created per instance (a mutable class default would be shared).
    _bracket_fetch_attempts: int = 0
    _bracket_fetch_error: Optional[str] = None
    _bracket_sweep_memo: Optional[Dict[date, Dict[str, Any]]] = None

    def _bracket_fetch_given_up(self) -> bool:
        return self._bracket_fetch_attempts >= self.BRACKET_FETCH_MAX_ATTEMPTS

    def _bracket_fetch_failed(self, error: str) -> List[Dict[str, Any]]:
        """Record one failed bracket fetch and return the [] the caller hands
        back UNCACHED. Never assign the result of a failed fetch to
        `_bracket_games_cache`."""
        self._bracket_fetch_attempts += 1
        self._bracket_fetch_error = error
        logger.warning(
            "[%s] bracket fetch failed (attempt %d of %d this refresh): %s",
            self.sport_label, self._bracket_fetch_attempts,
            self.BRACKET_FETCH_MAX_ATTEMPTS, error,
        )
        return []

    def _bracket_fetch_retry_pending(self) -> bool:
        """The last bracket fetch failed and another attempt is allowed. A
        state built from it must not be cached either: initial_state() is the
        bracket importance reads, and caching it would pin the failure past
        the retry exactly as caching the games did."""
        return self._bracket_fetch_error is not None and not self._bracket_fetch_given_up()

    def _cache_bracket_games(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Cache a COMPLETE bracket fetch, and forget earlier failed attempts:
        a fetch that recovered is not reported as having given up."""
        self._bracket_games_cache = games
        self._bracket_fetch_attempts = 0
        self._bracket_fetch_error = None
        return games

    def allow_bracket_scoring_retry(self) -> None:
        """Guarantee scoring one more bracket fetch however many failed
        before it; plugin._action_refresh calls this as scoring begins, the
        bracket counterpart of soccer.allow_scoring_retry. A state that
        initial_state() cached from the failed fetch (it caches once the
        budget is spent) is dropped too, or scoring would read it instead of
        making the attempt this grants."""
        if self._bracket_fetch_error is None:
            return
        self._bracket_fetch_attempts = min(
            self._bracket_fetch_attempts, self.BRACKET_FETCH_MAX_ATTEMPTS - 1,
        )
        self._initial_state_cache = None

    def bracket_fetch_failure(self) -> Optional[str]:
        """The last error, when this refresh gave up on the bracket fetch;
        None otherwise (never failed, recovered, or still retryable). The
        source's games then carry no bracket stakes, so the refresh reports
        it like a failed source rather than letting a zero pass as real."""
        return self._bracket_fetch_error if self._bracket_fetch_given_up() else None

    def _sweep_bracket_days(
        self,
        start: date,
        end: date,
        fetch_day: Callable[[date], Any],
    ) -> Optional[List[Dict[str, Any]]]:
        """The payload of every day of [start, end], in date order (MLS Cup
        numbers series games by it); or None when a day's request failed
        (recorded through _bracket_fetch_failed, so the caller just returns
        []).

        `fetch_day(day)` returns the parsed payload, or anything that is not
        a dict (the sources' `_http_get` gives None) for a failed request. A
        dict, even an empty one, is an answer: a day with no games.

        Stops at the FIRST failed day: the sweep is discarded either way, and
        against an ESPN outage every further day is another request timeout.
        Days that did answer are kept on the instance until the sweep
        completes, so the retry asks only from the failed day on, not the
        whole window again. Injected `fetch_day` keeps each source's own
        patchable `_http_get`."""
        if self._bracket_fetch_given_up():
            return None
        if self._bracket_sweep_memo is None:
            self._bracket_sweep_memo = {}
        memo = self._bracket_sweep_memo
        total = (end - start).days + 1
        days: List[date] = []
        day = start
        while day <= end:
            if day not in memo:
                data = fetch_day(day)
                if not isinstance(data, dict):
                    self._bracket_fetch_failed(
                        f"scoreboard for {day.isoformat()} did not answer, "
                        f"day {len(days) + 1} of {total}"
                    )
                    return None
                memo[day] = data
            days.append(day)
            day += timedelta(days=1)
        self._bracket_sweep_memo = None  # complete: the caller caches the result
        return [memo[d] for d in days]

    # ---------- subclass hooks ----------

    @abstractmethod
    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Return per-game bracket records. Subclass adapts source data."""

    @abstractmethod
    def _new_tie_record(self, tie_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Empty tie-state container. Will be filled by _record_game_into_tie."""

    @abstractmethod
    def _record_game_into_tie(
        self,
        tie: Dict[str, Any],
        home_team: str,
        away_team: str,
        home_score: int,
        away_score: int,
        game_index: int,
    ) -> None:
        """Mutate `tie` in-place to record this game. If the tie is now
        complete, set `tie["winner"]` and `tie["loser"]`."""

    @abstractmethod
    def _is_decisive_game(
        self,
        tie: Dict[str, Any],
        game_index: int,
        games_in_tie: int,
    ) -> bool:
        """Return True iff this is the last game that could decide the tie
        (drives ET / OT / shootout logic in subclass sample_result).

        For two-leg soccer: leg 2 of a 2-leg tie, OR the only leg of a
        1-leg tie (FINAL). For series sports: every game (any of them
        could be the clincher), since OT is per-game not per-series."""

    @abstractmethod
    def _emit_remaining_games_for_tie(
        self,
        tie: Dict[str, Any],
        tie_meta: Dict[str, Any],
        applied: frozenset,
        team_a: str,
        team_b: str,
    ) -> List[GameRow]:
        """Emit GameRows for games in this tie still to play, with
        home/away resolved against the simulator's participant decision
        (which may differ from the source-published home/away for
        downstream ties when an upstream counterfactual changed the
        winner). Returns [] when the tie is complete."""

    def _sim_extra(self, tie_meta: Dict[str, Any], game: Dict[str, Any]) -> Dict[str, Any]:
        """The `extra` the simulator needs on a row for this bracket game. The
        ONE builder for it: _emit_remaining_games_for_tie uses it for the rows
        the bracket replays, and place_upcoming for the fetched row the
        simulator starts from, so the two cannot disagree. Each tie shape
        adds what its sample_result / apply_result read."""
        extra: Dict[str, Any] = {
            "game_id": game["game_id"],
            "stage": tie_meta["stage"],
            "matchday": game.get("matchday") or 1,
        }
        extra.update(game.get("extra", {}) or {})
        return extra

    # ---------- public importance interface ----------

    @property
    def outcome_labels(self) -> List[str]:
        from ..scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS.get(self._league_context_code())
        if ctx is None:
            return []
        return [label for _, label, _ in ctx.thresholds]

    @abstractmethod
    def _league_context_code(self) -> str:
        """LEAGUE_CONTEXTS key for this bracket (e.g., 'CL', 'NHL_PO')."""

    # True for sources whose every row is a playoff game (series, double
    # elimination). False here because a soccer knockout source also lists its
    # league phase. Read by place_upcoming.
    ALL_ROWS_POSTSEASON: bool = False

    def place_upcoming(self, games: List[GameRow], *, for_scoring: bool = False) -> int:
        """Give each upcoming row its bracket identity, from this source's own
        bracket records (#243). Mutates the rows; returns how many could not be
        placed.

        The importance simulator's TARGET is the row fetch_upcoming built, not
        the one the bracket emits, and `apply_result` keys on three fields of
        it. Each was missing or wrong on real rows, and each failed SILENTLY:

        - `stage`: the tie is found only when it is a string. The NFL/NBA/NHL/
          MLB/WNBA/NCAAW playoff rows had none, so the sampled result was
          dropped; live MLB LCS games scored ~0.15 importance instead of ~3.
          `plugin._is_postseason_game` reads it too.
        - the game index: read from `matchday`, which on a soccer row is
          Football-Data's, not a leg number. FD gives a UCL final 0 or None,
          every 2026 World Cup knockout None, EURO 2024 knockouts 4-7, and
          anything but 1 is filed as leg 2, so a single-leg tie never
          completes: 0.0 importance. The bracket's index is stamped as
          `bracket_game_index`, which apply_result prefers, rather than
          overwriting `matchday`, which the EPG text renders.

        - the rest of what `_sim_extra` gives the bracket's own rows, above
          all `is_decisive_leg`: without it sample_result plays a final, a
          second leg or a series game with no extra time or penalties, so it
          can end level.

        Everything comes from `_sim_extra` on the bracket's tie, the same
        builder the replayed rows use, and overrides what the row had: once
        matched, the bracket is authoritative for the simulator's keys. It is
        matched on the row's own id (`game_id`, else `fd_id`), compared
        exactly (simulation compares exactly too, so a "123" row is not 123),
        and only when the team pair agrees and the id names one bracket game:
        a wrong placement files the result into the wrong tie, which is worse
        than none.

        Rows of an ALL_ROWS_POSTSEASON source are flagged `postseason` first,
        so a failed bracket fetch cannot make them look like regular season.
        """
        if self.ALL_ROWS_POSTSEASON:
            for g in games:
                g.extra["postseason"] = True
        # Index every game the bracket knows, with its tie, by id. At scoring
        # this is initial_state()'s bracket, the one importance will use, so
        # the two cannot disagree about whether the bracket exists. Before
        # scoring it is a throwaway build: initial_state() caches, and a
        # season fetch failing that early would pin an empty bracket for the
        # whole run and defeat the scoring-time retry
        # (soccer.allow_scoring_retry).
        bracket = (
            self.initial_state().get("_bracket", {}) if for_scoring
            else self._build_bracket(self._fetch_bracket_games())
        )
        by_id: Dict[Any, Optional[Tuple[Dict[str, Any], Dict[str, Any]]]] = {}
        for ties in bracket.values():
            for tie_meta in ties:
                for game in tie_meta.get("games") or []:
                    gid = game.get("game_id")
                    if gid is None:
                        continue
                    # The same id twice is ambiguous: place neither.
                    by_id[gid] = None if gid in by_id else (tie_meta, game)
        unplaced = 0
        for g in games:
            extra = g.extra
            gid = extra.get("game_id")
            if gid is None:
                gid = extra.get("fd_id")
            hit = by_id.get(gid) if gid is not None else None
            if hit is None or (
                frozenset((hit[1].get("home"), hit[1].get("away")))
                != frozenset((g.home, g.away))
            ):
                if not isinstance(extra.get("stage"), str):
                    unplaced += 1
                continue
            tie_meta, game = hit
            # The keys the tie shape itself sets (built without the record's
            # pass-through extra) are what the simulator reads, and the
            # bracket is authoritative for them on a game matched by id and
            # teams: a fetched stage can be None or stale, and a fetched
            # grouping_key can name the wrong sub-bracket (the upcoming sweep
            # classifies against its own window, not opening day). The
            # record's pass-through metadata only fills gaps.
            own = self._sim_extra(tie_meta, {**game, "extra": {}})
            extra["bracket_game_index"] = own.pop("matchday")
            extra.update(own)
            for key, value in (game.get("extra") or {}).items():
                extra.setdefault(key, value)
            # The fetched matchday is left alone: the EPG text renders it.
            extra.setdefault("matchday", extra["bracket_game_index"])
        return unplaced

    def initial_state(self) -> Dict[str, Any]:
        if self._initial_state_cache is not None:
            return self._initial_state_cache
        games = self._fetch_bracket_games()
        bracket = self._build_bracket(games)
        applied: List[Any] = []
        tie_results: Dict[Any, Dict[str, Any]] = {}
        round_reached: Dict[str, int] = {}

        for stage in self.KO_STAGES:
            for tie_meta in bracket.get(stage, []):
                tk = (stage, frozenset(tie_meta["teams"]))
                tie_results[tk] = self._new_tie_record(tie_meta)
                for game in tie_meta["games"]:
                    if (
                        game.get("status") == "FINISHED"
                        and game.get("home_goals") is not None
                        and game.get("away_goals") is not None
                    ):
                        self._record_game_into_tie(
                            tie_results[tk],
                            game["home"], game["away"],
                            int(game["home_goals"]), int(game["away_goals"]),
                            game.get("matchday", 1),
                        )
                        applied.append(game["game_id"])
                winner = tie_results[tk].get("winner")
                loser = tie_results[tk].get("loser")
                if winner is not None and loser is not None:
                    self._advance_round_reached(round_reached, winner, loser, stage)

        state = {
            "_applied": frozenset(applied),
            "_tie_results": tie_results,
            "_bracket": bracket,
            "_round_reached": round_reached,
        }
        if not self._bracket_fetch_retry_pending():
            self._initial_state_cache = state
        return state

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        applied = state.get("_applied", frozenset())
        tie_results = state.get("_tie_results", {})
        bracket = state.get("_bracket", {})
        out: List[GameRow] = []
        for stage in self.KO_STAGES:
            for tie_meta in bracket.get(stage, []):
                participants = self._resolve_participants(tie_meta, bracket, tie_results)
                if participants is None:
                    continue
                team_a, team_b = participants
                tk = (stage, frozenset({team_a, team_b}))
                # Cross-check: when downstream resolved participants differ
                # from the source-published draw, the simulated tie's key
                # uses the simulated teams. tie_results may not yet have an
                # entry for the simulated tie_key: _emit_remaining_games_for_tie
                # is responsible for handling that gracefully (treat as fresh).
                tie = tie_results.get(tk) or self._new_tie_record(tie_meta)
                games = self._emit_remaining_games_for_tie(
                    tie, tie_meta, applied, team_a, team_b,
                )
                out.extend(games)
        return out

    def apply_result(self, state, match, result):
        new_state = dict(state)
        new_tie_results = dict(state.get("_tie_results", {}))
        extra = match.extra if isinstance(match.extra, dict) else {}
        stage = extra.get("stage")
        game_id = extra.get("game_id")
        game_index = _game_index(extra)

        # tie_key uses the simulator's actual participants (match.home / .away),
        # which may differ from the source-published draw when an upstream
        # counterfactual flipped a feeder.
        if isinstance(stage, str):
            tk = (stage, frozenset({match.home, match.away}))
            prior_tie = new_tie_results.get(tk)
            # A tie with no record yet (a downstream tie whose participants
            # the simulation just decided) is built from the game's own
            # `legs_in_tie`, which `_emit_remaining_games_for_tie` copied from
            # the bracket. DO NOT drop it: without it AggregateLegSource
            # assumes two legs, a single-leg WC / EURO knockout tie never
            # completes, and the simulated bracket stops one round after the
            # entry round (#78).
            new_tie = self._copy_tie_record(prior_tie) if prior_tie else self._new_tie_record({
                "stage": stage, "teams": frozenset({match.home, match.away}),
                "legs_in_tie": extra.get("legs_in_tie"),
            })
            prior_winner = (prior_tie or {}).get("winner")
            self._record_game_into_tie(
                new_tie, match.home, match.away,
                result.home_goals, result.away_goals, game_index,
            )
            new_tie_results[tk] = new_tie
            new_state["_tie_results"] = new_tie_results

            new_winner = new_tie.get("winner")
            new_loser = new_tie.get("loser")
            if new_winner is not None and prior_winner is None and new_loser is not None:
                new_round = dict(state.get("_round_reached", {}))
                self._advance_round_reached(new_round, new_winner, new_loser, stage)
                new_state["_round_reached"] = new_round

        if game_id is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {game_id}
        return new_state

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        from ..scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS.get(self._league_context_code())
        round_reached = state.get("_round_reached", {})
        if ctx is None or not round_reached:
            return {}
        outcomes: Dict[str, List[str]] = {team: [] for team in round_reached}
        for cutoff, label, _ in ctx.thresholds:
            cutoff_depth = KNOCKOUT_ROUND_DEPTH.get(cutoff, -1)
            if cutoff_depth < 0:
                continue
            for team, depth in round_reached.items():
                if depth >= cutoff_depth:
                    outcomes[team].append(label)
        return outcomes

    # ---------- shared bracket helpers ----------

    def _copy_tie_record(self, tie: Dict[str, Any]) -> Dict[str, Any]:
        """Default deep-ish copy: shallow at top level, deep enough for the
        common subclass shapes. Subclasses may override for nested dicts.
        Caller must guard against None (apply_result already does)."""
        out: Dict[str, Any] = {}
        for k, v in tie.items():
            if isinstance(v, dict):
                out[k] = dict(v)
            elif isinstance(v, list):
                out[k] = list(v)
            else:
                out[k] = v
        return out

    def _build_bracket(self, games: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group games by (stage, team-pair) into tie_meta records and wire
        feeds_from via participant-set membership across stages.

        feeds_from is keyed by source-published team name so each side of
        a tie resolves independently. This is essential for mixed-entry
        ties (e.g., UCL R16 where one side comes from PLAYOFFS and the
        other directly from the league phase).
        """
        by_stage: Dict[str, Dict[frozenset, List[Dict[str, Any]]]] = {
            s: {} for s in self.KO_STAGES
        }
        for g in games:
            stage = g.get("stage")
            if stage not in by_stage:
                continue
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            pair = frozenset({home, away})
            by_stage[stage].setdefault(pair, []).append(g)

        bracket: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.KO_STAGES}
        for stage in self.KO_STAGES:
            for pair, stage_games in by_stage[stage].items():
                stage_games.sort(key=lambda g: g.get("matchday") or 1)
                bracket[stage].append({
                    "stage": stage,
                    "teams": pair,
                    "games": stage_games,
                    "feeds_from": {},
                    "is_entry_tie": True,
                })

        # feeds_from wiring (structural: participant-set membership).
        participants_by_stage: Dict[str, Dict[str, int]] = {
            stage: {team: idx for idx, tie in enumerate(bracket[stage]) for team in tie["teams"]}
            for stage in self.KO_STAGES
        }
        for i, stage in enumerate(self.KO_STAGES):
            for tie in bracket[stage]:
                team_feeds: Dict[str, Tuple[str, int]] = {}
                for team in tie["teams"]:
                    for j in range(i - 1, -1, -1):
                        prev_stage = self.KO_STAGES[j]
                        prev_idx = participants_by_stage[prev_stage].get(team)
                        if prev_idx is not None:
                            team_feeds[team] = (prev_stage, prev_idx)
                            break
                tie["feeds_from"] = team_feeds
                tie["is_entry_tie"] = len(team_feeds) < len(tie["teams"])
        return bracket

    def _advance_round_reached(
        self,
        round_reached: Dict[str, int],
        winner: str,
        loser: str,
        stage: str,
    ) -> None:
        """Loser caps at this stage's depth; winner advances to the next
        deeper stage (FINAL → WINNER synthetic depth). Subclasses can
        override `_winner_advance_label(stage)` to map a FINAL-equivalent
        stage to a sport-specific WINNER label."""
        from ..scoring import KNOCKOUT_ROUND_DEPTH
        stage_depth = KNOCKOUT_ROUND_DEPTH.get(stage, -1)
        if stage_depth < 0:
            return
        round_reached[loser] = max(round_reached.get(loser, -1), stage_depth)
        winner_stage = self._winner_advance_label(stage)
        if winner_stage and winner_stage != stage:
            winner_depth = KNOCKOUT_ROUND_DEPTH.get(winner_stage, stage_depth + 1)
        else:
            winner_depth = stage_depth + 1
        round_reached[winner] = max(round_reached.get(winner, -1), winner_depth)

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        """Stage label the winner of a tie at `stage` advances to. Returns
        None for non-terminal stages (winner_depth = stage_depth + 1).
        Subclasses override for the FINAL → WINNER synthetic depth.

        Default: maps the last stage in KO_STAGES to "WINNER" if it's the
        ultimate; subclasses with NHL-style "CUP_WINNER" override this."""
        if not self.KO_STAGES:
            return None
        if stage == self.KO_STAGES[-1]:
            return "WINNER"
        return None

    def _resolve_participants(
        self,
        tie_meta: Dict[str, Any],
        bracket: Dict[str, List[Dict[str, Any]]],
        tie_results: Dict[Any, Dict[str, Any]],
    ) -> Optional[Tuple[str, str]]:
        """Resolve a tie's actual participants from simulator state.

        Each side resolves independently against feeds_from. Returns None
        when any required upstream has not yet settled."""
        games = tie_meta.get("games") or []
        if not games:
            return None
        first = games[0]
        source_home, source_away = first["home"], first["away"]
        feeds_from: Dict[str, Tuple[str, int]] = tie_meta.get("feeds_from") or {}

        def resolve_side(source_team: str) -> Optional[str]:
            feed_ref = feeds_from.get(source_team)
            if feed_ref is None:
                return source_team
            return self._winner_of(feed_ref, bracket, tie_results)

        home = resolve_side(source_home)
        away = resolve_side(source_away)
        if home is None or away is None:
            return None
        return home, away

    def _winner_of(
        self,
        feed_ref: Tuple[str, int],
        bracket: Dict[str, List[Dict[str, Any]]],
        tie_results: Dict[Any, Dict[str, Any]],
    ) -> Optional[str]:
        """Resolve the winner of the feeder tie at `feed_ref`. Looks up
        tie_results using the tie's RESOLVED participants (recursively
        via _resolve_participants), not its source-published team names.

        Two cases where the published names diverge from the simulated
        ones:
          1. UCL-style counterfactual: an upstream sim flipped a feeder,
             putting a different team into this tie than FD.org's draw
             published.
          2. WC chain seed (#53): the entry tie's participants come from
             the group-stage chain rather than FD.org; downstream-round
             tie_metas carry placeholder names ("L32_W1" etc.) that have
             to be resolved at lookup time, not at synthesis time.

        Both cases were silently broken when this method consulted
        `tie_meta["teams"]` directly: the lookup key was the published
        set, the stored key was the simulated set, lookup returned None,
        and the chain stalled.
        """
        stage, tie_idx = feed_ref
        ties = bracket.get(stage, [])
        if tie_idx >= len(ties):
            return None
        tie_meta = ties[tie_idx]
        participants = self._resolve_participants(tie_meta, bracket, tie_results)
        if participants is None:
            return None
        tk = (stage, frozenset(participants))
        return tie_results.get(tk, {}).get("winner")


# =====================================================================
# AggregateLegSource: two-leg knockout (UEFA cup soccer)
# =====================================================================

class AggregateLegSource(BracketSportSource):
    """Bracket with at most two legs per tie. Tie resolves when both legs
    are recorded; winner is the team with higher aggregate goal sum (with
    ET / penalty +1 boost already baked into the scoring side's
    `sample_result`).

    Tie record:
      {
        "stage":  str,
        "teams":  frozenset({home, away}),
        "leg1":   {home, away, home_goals, away_goals} | None,
        "leg2":   {home, away, home_goals, away_goals} | None,
        "winner": str | None,
        "loser":  str | None,
      }

    Single-leg finals (UCL FINAL) treat leg1 alone as complete.
    """

    def _new_tie_record(self, tie_meta: Dict[str, Any]) -> Dict[str, Any]:
        # Capture the number of games published for this tie so completeness
        # can be data-driven instead of guessing from stage name. UEFA cup
        # ties run 2 legs except the FINAL (1 leg). International tournaments
        # (WC, EURO) run 1 leg per round including non-finals. Single source
        # of truth: count the games FD.org publishes for the tie. DO NOT
        # branch on `stage == KO_STAGES[-1]`: that misclassifies single-leg
        # non-finals (WC LAST_16, EURO QF, etc.) as incomplete forever and
        # the round_reached cascade never fires. A tie created mid-simulation
        # has no published games, so `apply_result` passes the game's own
        # `legs_in_tie` instead.
        games_in_tie = len(tie_meta.get("games") or []) or tie_meta.get("legs_in_tie")
        return {
            "stage": tie_meta.get("stage"),
            "teams": tie_meta.get("teams"),
            "legs_in_tie": games_in_tie if games_in_tie else 2,
            "leg1": None,
            "leg2": None,
            "winner": None,
            "loser": None,
        }

    def _record_game_into_tie(
        self, tie, home_team, away_team,
        home_score, away_score, game_index,
    ) -> None:
        leg = {
            "home": home_team,
            "away": away_team,
            "home_goals": home_score,
            "away_goals": away_score,
        }
        if game_index == 1:
            tie["leg1"] = leg
        else:
            tie["leg2"] = leg

        legs = [tie["leg1"], tie["leg2"]]
        legs_present = [L for L in legs if L is not None]
        legs_in_tie = tie.get("legs_in_tie", 2)
        complete = (
            (legs_in_tie == 1 and tie["leg1"] is not None)
            or (legs_in_tie >= 2 and tie["leg1"] is not None and tie["leg2"] is not None)
        )
        if not complete:
            return

        teams = list(tie["teams"])
        if len(teams) != 2:
            return
        a, b = teams
        agg = {a: 0, b: 0}
        for L in legs_present:
            agg[L["home"]] += L["home_goals"]
            agg[L["away"]] += L["away_goals"]
        if agg[a] == agg[b]:
            # Decisive leg's sample_result is responsible for breaking ties
            # (ET / pen-winner +1). If we still landed on a draw here it's
            # a recording bug; pick decisive-leg home as a deterministic
            # fallback. Cannot happen with real FD.org data because the
            # +1 boost is applied at parse time.
            decisive = legs_present[-1]
            tie["winner"] = decisive["home"]
            tie["loser"] = decisive["away"]
            return
        tie["winner"] = a if agg[a] > agg[b] else b
        tie["loser"] = b if agg[a] > agg[b] else a

    def _is_decisive_game(self, tie, game_index, games_in_tie) -> bool:
        del tie  # decisive-leg classification depends only on game index vs tie length
        return game_index == games_in_tie

    def _sim_extra(self, tie_meta: Dict[str, Any], game: Dict[str, Any]) -> Dict[str, Any]:
        legs_in_tie = len(tie_meta.get("games") or [])
        matchday = game.get("matchday") or 1
        extra: Dict[str, Any] = {
            "game_id": game["game_id"],
            "stage": tie_meta["stage"],
            "matchday": matchday,
            "leg_index": matchday,
            "legs_in_tie": legs_in_tie,
            # Drives extra time and penalties in sample_result; without it a
            # final or a second leg is sampled as a game that can end level.
            "is_decisive_leg": matchday == legs_in_tie,
        }
        extra.update(game.get("extra", {}) or {})
        return extra

    def _emit_remaining_games_for_tie(
        self, tie, tie_meta, applied, team_a, team_b,
    ) -> List[GameRow]:
        del tie  # AggregateLegSource emits based on tie_meta.games; tie state not needed
        out: List[GameRow] = []
        for g in tie_meta.get("games") or []:
            if g["game_id"] in applied:
                continue
            matchday = g.get("matchday") or 1
            # Leg 1: source's leg-1 home is home; leg 2: swap.
            if matchday == 1:
                home, away = team_a, team_b
            else:
                home, away = team_b, team_a
            start = g.get("start_time") or _SENTINEL_START
            extra = self._sim_extra(tie_meta, g)
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra=extra,
            ))
        return out


# =====================================================================
# BestOfNSeriesSource: best-of-N playoff series (NHL / NBA / MLB)
# =====================================================================

class BestOfNSeriesSource(BracketSportSource):
    """Bracket with a best-of-N series per tie. Tie resolves when one team
    reaches ceil(N/2) wins.

    Tie record:
      {
        "stage":          str,
        "teams":          frozenset({a, b}),
        "series_wins":    {team_a: int, team_b: int},
        "games_recorded": frozenset of game_index values applied,
        "winner":         str | None,
        "loser":          str | None,
      }

    Subclasses set `SERIES_LENGTH` (e.g., 7 for NHL/NBA, varies for MLB).
    Game emission walks games in matchday order: a series at 2-1 emits
    the game-4 record (if applied=false), then game-5 / game-6 / game-7
    conditional on the series staying alive: but only games actually
    listed in the tie_meta's games[] are emitted. Subclasses generate
    those records (with home/away pattern) in `_fetch_bracket_games`.
    """

    ALL_ROWS_POSTSEASON = True

    SERIES_LENGTH: int = 7   # subclass overrides as needed (uniform-length sports)

    def _series_length_for_stage(self, stage: str) -> int:
        """Per-stage series length. Default returns the class-level
        SERIES_LENGTH so uniform-format sports (NHL, NBA) keep their
        existing behavior. MLB overrides because Wild Card = best-of-3,
        Division Series = best-of-5, LCS / World Series = best-of-7.

        `stage` MUST be one of the entries in self.KO_STAGES; behavior is
        undefined for unknown stages (returns the uniform default, which
        matches the SCHEDULED pre-bracket case gracefully).
        """
        del stage  # default returns uniform; subclass uses the arg
        return self.SERIES_LENGTH

    def _clinching_wins_for_stage(self, stage: str) -> int:
        """ceil(N/2) for the per-stage series length. Wins needed to
        win the tie."""
        n = self._series_length_for_stage(stage)
        return (n // 2) + 1

    def _new_tie_record(self, tie_meta: Dict[str, Any]) -> Dict[str, Any]:
        teams = list(tie_meta.get("teams") or [])
        return {
            "stage": tie_meta.get("stage"),
            "teams": tie_meta.get("teams"),
            "series_wins": {t: 0 for t in teams},
            "games_recorded": frozenset(),
            "winner": None,
            "loser": None,
        }

    def _copy_tie_record(self, tie: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "stage": tie.get("stage"),
            "teams": tie.get("teams"),
            "series_wins": dict(tie.get("series_wins") or {}),
            "games_recorded": tie.get("games_recorded") or frozenset(),
            "winner": tie.get("winner"),
            "loser": tie.get("loser"),
        }

    def _record_game_into_tie(
        self, tie, home_team, away_team,
        home_score, away_score, game_index,
    ) -> None:
        if tie.get("winner") is not None:
            return  # series already resolved
        wins = dict(tie.get("series_wins") or {})
        for t in (home_team, away_team):
            wins.setdefault(t, 0)
        if home_score > away_score:
            wins[home_team] += 1
        elif away_score > home_score:
            wins[away_team] += 1
        # Ties don't exist in hockey/basketball; defensive no-op if it slips in.
        tie["series_wins"] = wins
        tie["games_recorded"] = (tie.get("games_recorded") or frozenset()) | {game_index}

        target = self._clinching_wins_for_stage(tie.get("stage") or "")
        if wins.get(home_team, 0) >= target:
            tie["winner"] = home_team
            tie["loser"] = away_team
        elif wins.get(away_team, 0) >= target:
            tie["winner"] = away_team
            tie["loser"] = home_team

    def _is_decisive_game(self, tie, game_index, games_in_tie) -> bool:
        # Every series game can be a clincher (OT triggers per game, not
        # per series). For series sports this is "is this game a regular
        # match that could go to OT": always True; the actual OT decision
        # lives in the subclass `sample_result`.
        del tie, game_index, games_in_tie
        return True

    def _sim_extra(self, tie_meta: Dict[str, Any], game: Dict[str, Any]) -> Dict[str, Any]:
        matchday = game.get("matchday") or 1
        extra: Dict[str, Any] = {
            "game_id": game["game_id"],
            "stage": tie_meta["stage"],
            "matchday": matchday,
            "game_index": matchday,
            "games_in_series": self._series_length_for_stage(tie_meta.get("stage") or ""),
            "is_decisive_leg": True,  # every series game can go to OT
        }
        extra.update(game.get("extra", {}) or {})
        return extra

    def _emit_remaining_games_for_tie(
        self, tie, tie_meta, applied, team_a, team_b,
    ) -> List[GameRow]:
        if tie.get("winner") is not None:
            return []
        out: List[GameRow] = []
        games = tie_meta.get("games") or []
        wins_a = (tie.get("series_wins") or {}).get(team_a, 0)
        wins_b = (tie.get("series_wins") or {}).get(team_b, 0)
        stage = tie_meta.get("stage") or ""
        target = self._clinching_wins_for_stage(stage)
        # Walk source-published games in order; only emit ones not yet
        # applied AND that the series is still alive long enough to reach.
        # We model this by emitting games[i] only when wins_a < target AND
        # wins_b < target up to game i (lazily; the drain loop in the
        # simulator handles intermediate updates).
        running_a, running_b = wins_a, wins_b
        for g in games:
            if running_a >= target or running_b >= target:
                break
            if g["game_id"] in applied:
                # Already applied; don't emit but factor into running tally
                # if the game extra carries a known winner. Strict caller
                # never applies a game out of order, so this branch is
                # effectively defensive.
                continue
            matchday = g.get("matchday") or 1
            # Map source-published home/away to the simulator's actual
            # participants. For entry-level ties the source's home IS
            # team_a (or team_b) already; for downstream ties (winner of
            # upstream series) we need to resolve which simulator team
            # gets the home slot via _source_team_for.
            src_home = g.get("home")
            if src_home in tie_meta.get("teams") or frozenset():
                home, away = (team_a, team_b) if src_home == self._source_team_for(team_a, tie_meta) else (team_b, team_a)
            else:
                # Speculative games: walk the series home pattern from
                # tie_meta if present, fallback to alternating.
                pattern = tie_meta.get("home_pattern") or []
                top_seed_home = (
                    pattern[matchday - 1] if matchday - 1 < len(pattern) else (matchday % 2 == 1)
                )
                if top_seed_home:
                    home, away = team_a, team_b
                else:
                    home, away = team_b, team_a
            start = g.get("start_time") or _SENTINEL_START
            extra = self._sim_extra(tie_meta, g)
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra=extra,
            ))
        return out

    def _source_team_for(self, sim_team: str, tie_meta: Dict[str, Any]) -> str:
        """For top/bottom-seed identification when emitting downstream
        games whose simulator participants may differ from source-
        published. Default: identity (subclass overrides if it tracks
        seed roles in tie_meta). `tie_meta` is provided for the override
        contract; the default doesn't need it."""
        del tie_meta
        return sim_team


# =====================================================================
# DoubleEliminationSource: N-team double-elimination bracket
# =====================================================================

class DoubleEliminationSource(BracketSportSource):
    """N-team double-elimination tie (NCAA Baseball / Softball Regional
    sites, MCWS / WCWS 8-team bracket modeled as two 4-team sub-brackets).
    Each team is eliminated at 2 losses; the tie resolves when all but
    one team have crossed that threshold (the last team standing is the
    sub-bracket winner).

    Tie record:
      {
        "stage":                  str,
        "teams":                  frozenset of all n participants,
        "grouping_key":           str,                 # site label or sub-bracket id
        "losses_by_team":         {team: int},
        "games_recorded":         frozenset of game_index values applied,
        "elimination_loss_count": int,                 # default 2
        "winner":                 str | None,
        "eliminated_teams":       list[str],           # populated as teams hit threshold
      }

    Key deviations from the existing 2-team BracketSportSource shape:

      - Tie identity is `(stage, frozenset(all_n_participants))`, not
        `(stage, frozenset({home, away}))`. The simulator drives a SET
        of teams through one shared loss-tracking state, rather than a
        pair through a shared series-wins state.
      - `_build_bracket` groups games by `_tie_grouping_key(game)` (a
        subclass-provided string from the source headline: e.g.,
        "Auburn Regional" or "MCWS_sub1"), not by team-pair set.
      - `apply_result` looks up the tie via the game's `grouping_key`
        (carried in the GameRow `extra`), with a defensive fallback to
        membership-superset search.
      - `_advance_round_reached` is called once per eliminated team, not
        once per tie. The base method's `(winner, loser)` signature is
        idempotent under multiple calls with the same winner: each
        loser still caps at `stage_depth`, the winner is still set to
        `winner_depth` via `max()`.
      - Source-driven only: `_emit_remaining_games_for_tie` emits the
        source-published games (in chronological order) that haven't
        been applied yet. It does NOT synthesize speculative future
        games from the double-elim game tree: that's a #43 follow-up
        once live source data is wired in.

    Subclass contract: implement `_fetch_bracket_games` (inherited) AND
    `_tie_grouping_key(game)` (new). Set `KO_STAGES` and
    `_league_context_code` as for any BracketSportSource.
    """

    ALL_ROWS_POSTSEASON = True

    ELIMINATION_LOSS_COUNT: int = 2

    # ---------- subclass hooks ----------

    @abstractmethod
    def _tie_grouping_key(self, game: Dict[str, Any]) -> Optional[str]:
        """Return a string identifying which double-elim tie this game
        belongs to. Games with the same (stage, grouping_key) are grouped
        into one tie_meta and share one loss-tracking state. Return None
        to exclude the game from bracket construction (e.g., the source
        emitted a game whose sub-bracket couldn't yet be inferred)."""

    # ---------- tie record shape ----------

    def _new_tie_record(self, tie_meta: Dict[str, Any]) -> Dict[str, Any]:
        teams = list(tie_meta.get("teams") or [])
        return {
            "stage": tie_meta.get("stage"),
            "teams": tie_meta.get("teams"),
            "grouping_key": tie_meta.get("grouping_key"),
            "losses_by_team": {t: 0 for t in teams},
            "games_recorded": frozenset(),
            "elimination_loss_count": self.ELIMINATION_LOSS_COUNT,
            "winner": None,
            "eliminated_teams": [],
        }

    def _copy_tie_record(self, tie: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "stage": tie.get("stage"),
            "teams": tie.get("teams"),
            "grouping_key": tie.get("grouping_key"),
            "losses_by_team": dict(tie.get("losses_by_team") or {}),
            "games_recorded": tie.get("games_recorded") or frozenset(),
            "elimination_loss_count": tie.get(
                "elimination_loss_count", self.ELIMINATION_LOSS_COUNT
            ),
            "winner": tie.get("winner"),
            "eliminated_teams": list(tie.get("eliminated_teams") or []),
        }

    def _record_game_into_tie(
        self, tie, home_team, away_team,
        home_score, away_score, game_index,
    ) -> None:
        if tie.get("winner") is not None:
            return  # tie already resolved; defensive no-op for double-apply
        losses = dict(tie.get("losses_by_team") or {})
        for t in (home_team, away_team):
            losses.setdefault(t, 0)
        if home_score > away_score:
            new_loser_team = away_team
        elif away_score > home_score:
            new_loser_team = home_team
        else:
            return  # baseball / softball don't tie; defensive no-op
        losses[new_loser_team] += 1
        tie["losses_by_team"] = losses
        tie["games_recorded"] = (tie.get("games_recorded") or frozenset()) | {game_index}

        threshold = tie.get("elimination_loss_count", self.ELIMINATION_LOSS_COUNT)
        eliminated = list(tie.get("eliminated_teams") or [])
        if losses[new_loser_team] >= threshold and new_loser_team not in eliminated:
            eliminated.append(new_loser_team)
        tie["eliminated_teams"] = eliminated

        teams = list(tie.get("teams") or [])
        survivors = [t for t in teams if t not in eliminated]
        if teams and len(survivors) == 1:
            tie["winner"] = survivors[0]

    def _is_decisive_game(self, tie, game_index, games_in_tie) -> bool:
        # Every game in a double-elim CAN be a tie-ending game (the one
        # that puts the last 1-loss team at 2 losses), so always True.
        # The base class contract uses this for ET / extra-innings hooks
        # in subclass sample_result: baseball / softball use no shootout
        # so it doesn't matter, but stay consistent with BestOfNSeries.
        del tie, game_index, games_in_tie
        return True

    # ---------- emission / state machinery ----------

    def _build_bracket(self, games: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group games by (stage, _tie_grouping_key(game)) into tie_meta
        records carrying the full participant set. feeds_from wiring is
        intentionally empty for double-elim: each sub-bracket tie is an
        entry tie at its stage (this class doesn't connect Regional →
        Super Regional structurally: that handoff lives in the
        regular-season-strength sharing already on the playoff sources).
        """
        by_stage: Dict[str, Dict[str, Dict[str, Any]]] = {
            s: {} for s in self.KO_STAGES
        }
        for g in games:
            stage = g.get("stage")
            if stage not in by_stage:
                continue
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            key = self._tie_grouping_key(g)
            if key is None:
                continue
            slot = by_stage[stage].setdefault(key, {"teams": set(), "games": []})
            slot["teams"].add(home)
            slot["teams"].add(away)
            slot["games"].append(g)

        bracket: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.KO_STAGES}
        for stage in self.KO_STAGES:
            for grouping_key, slot in by_stage[stage].items():
                slot["games"].sort(
                    key=lambda gg: gg.get("start_time") or _SENTINEL_START
                )
                bracket[stage].append({
                    "stage": stage,
                    "teams": frozenset(slot["teams"]),
                    "games": slot["games"],
                    "grouping_key": grouping_key,
                    "feeds_from": {},
                    "is_entry_tie": True,
                })
        return bracket

    def initial_state(self) -> Dict[str, Any]:
        if self._initial_state_cache is not None:
            return self._initial_state_cache
        from ..scoring import KNOCKOUT_ROUND_DEPTH
        games = self._fetch_bracket_games()
        bracket = self._build_bracket(games)
        applied: List[Any] = []
        tie_results: Dict[Any, Dict[str, Any]] = {}
        round_reached: Dict[str, int] = {}

        for stage in self.KO_STAGES:
            for tie_meta in bracket.get(stage, []):
                tk = (stage, frozenset(tie_meta["teams"]))
                tie_results[tk] = self._new_tie_record(tie_meta)
                # Chronological replay: games in tie_meta are already sorted
                # by start_time in _build_bracket; apply each FINISHED one in
                # order so loss counts accumulate correctly.
                for game in tie_meta["games"]:
                    if (
                        game.get("status") == "FINISHED"
                        and game.get("home_goals") is not None
                        and game.get("away_goals") is not None
                    ):
                        self._record_game_into_tie(
                            tie_results[tk],
                            game["home"], game["away"],
                            int(game["home_goals"]), int(game["away_goals"]),
                            game.get("matchday", 1),
                        )
                        applied.append(game["game_id"])
                self._propagate_round_reached(
                    round_reached, stage, tie_results[tk], KNOCKOUT_ROUND_DEPTH,
                )

        state = {
            "_applied": frozenset(applied),
            "_tie_results": tie_results,
            "_bracket": bracket,
            "_round_reached": round_reached,
        }
        if not self._bracket_fetch_retry_pending():
            self._initial_state_cache = state
        return state

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        applied = state.get("_applied", frozenset())
        tie_results = state.get("_tie_results", {})
        bracket = state.get("_bracket", {})
        out: List[GameRow] = []
        for stage in self.KO_STAGES:
            for tie_meta in bracket.get(stage, []):
                tk = (stage, frozenset(tie_meta["teams"]))
                tie = tie_results.get(tk) or self._new_tie_record(tie_meta)
                # Double-elim has no fixed home/away pair per tie; pass
                # empty strings so _emit_remaining_games_for_tie skips the
                # pair-resolution path. Each game record carries its own
                # source-published home/away.
                out.extend(self._emit_remaining_games_for_tie(
                    tie, tie_meta, applied, "", "",
                ))
        return out

    def _sim_extra(self, tie_meta: Dict[str, Any], game: Dict[str, Any]) -> Dict[str, Any]:
        extra: Dict[str, Any] = {
            "game_id": game["game_id"],
            "stage": tie_meta["stage"],
            "matchday": game.get("matchday") or 1,
            "grouping_key": tie_meta.get("grouping_key"),
            "is_decisive_leg": True,  # any game can be a tie-ending game
        }
        extra.update(game.get("extra", {}) or {})
        return extra

    def _emit_remaining_games_for_tie(
        self, tie, tie_meta, applied, team_a, team_b,
    ) -> List[GameRow]:
        del team_a, team_b  # double-elim has no fixed pair; games are independent
        out: List[GameRow] = []
        if tie.get("winner") is not None:
            return out
        for g in tie_meta.get("games") or []:
            if g["game_id"] in applied:
                continue
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            start = g.get("start_time") or _SENTINEL_START
            extra = self._sim_extra(tie_meta, g)
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra=extra,
            ))
        return out

    def apply_result(self, state, match, result):
        from ..scoring import KNOCKOUT_ROUND_DEPTH
        new_state = dict(state)
        new_tie_results = dict(state.get("_tie_results", {}))
        extra = match.extra if isinstance(match.extra, dict) else {}
        stage_raw = extra.get("stage")
        game_id = extra.get("game_id")
        game_index = _game_index(extra)
        grouping_key = extra.get("grouping_key")
        bracket = state.get("_bracket", {})

        stage: Optional[str] = stage_raw if isinstance(stage_raw, str) else None
        target_meta = self._find_tie_meta(
            stage, grouping_key, match.home, match.away, bracket,
        ) if stage is not None else None

        if target_meta is not None and stage is not None:
            tk = (stage, frozenset(target_meta["teams"]))
            prior_tie = new_tie_results.get(tk)
            new_tie = (
                self._copy_tie_record(prior_tie) if prior_tie
                else self._new_tie_record(target_meta)
            )
            self._record_game_into_tie(
                new_tie, match.home, match.away,
                result.home_goals, result.away_goals, game_index,
            )
            new_tie_results[tk] = new_tie
            new_state["_tie_results"] = new_tie_results

            # Round-depth cascade. Idempotent under repeated calls
            # because `_propagate_round_reached` uses max() everywhere,
            # so we can drop the `prior_winner is None` / `newly_eliminated`
            # guards the base class uses for the 2-team tie shape.
            new_round = dict(state.get("_round_reached", {}))
            self._propagate_round_reached(
                new_round, stage, new_tie, KNOCKOUT_ROUND_DEPTH,
            )
            new_state["_round_reached"] = new_round

        if game_id is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {game_id}
        return new_state

    # ---------- helpers ----------

    def _find_tie_meta(
        self,
        stage: Optional[str],
        grouping_key: Optional[str],
        home: str,
        away: str,
        bracket: Dict[str, List[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        """Look up the tie_meta this game belongs to. Prefer grouping_key
        (carried in extras from emission) for an unambiguous match;
        fallback to team-set superset scan for defensive coverage when
        the simulator constructs a counterfactual GameRow without
        grouping_key (e.g., in tests, or a future synthesized game)."""
        if not isinstance(stage, str):
            return None
        candidates = bracket.get(stage, [])
        if grouping_key:
            for tm in candidates:
                if tm.get("grouping_key") == grouping_key:
                    return tm
        # Fallback: find a tie whose team set contains BOTH participants.
        # Strict superset (not just containment of one team) so games
        # that span sub-brackets (shouldn't happen with valid source
        # data) are ignored rather than misattributed.
        wanted = {home, away}
        for tm in candidates:
            teams = tm.get("teams") or frozenset()
            if wanted.issubset(teams):
                return tm
        return None

    def _propagate_round_reached(
        self,
        round_reached: Dict[str, int],
        stage: str,
        tie: Dict[str, Any],
        knockout_depth: Dict[str, int],
    ) -> None:
        """Replay `_advance_round_reached` for a fully-loaded tie at
        `initial_state` time. Mirrors the per-game logic in
        `apply_result`: resolved ties advance the winner + cap every
        loser; mid-tournament partial states cap eliminated teams at
        this stage's depth without crowning a winner."""
        winner = tie.get("winner")
        eliminated = tie.get("eliminated_teams") or []
        if winner is not None:
            for loser in eliminated:
                self._advance_round_reached(round_reached, winner, loser, stage)
            return
        depth = knockout_depth.get(stage, -1)
        if depth < 0:
            return
        for loser in eliminated:
            round_reached[loser] = max(round_reached.get(loser, -1), depth)
