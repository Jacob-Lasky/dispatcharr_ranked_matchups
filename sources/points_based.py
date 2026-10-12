"""Shared Monte Carlo importance machinery for points-based, round-robin-ish
sports (NCAAF, NCAAM, future NHL/NBA/MLB regular seasons).

The model: each team has a per-game points-scored and points-allowed average
derived from FINISHED games. `sample_result` draws Poisson(lam_home),
Poisson(lam_away) where lam_home blends home-team's offense + away-team's
defense, and lam_away mirrors. OT/overtime/extra-innings are abstracted:
draws are post-resolved by giving +1 to a random side so `_classify_target_
result` produces W/L rather than D (NCAA games never end in regulation ties
that affect win-count outcomes; the simulator's tau-c just needs a binary
W/L).

Terminal outcomes are win-count bands (e.g., "11+ wins" for elite CFB
seasons, "bowl_eligible" at 6+, "25+ wins" for NCAAM elite). The
LEAGUE_CONTEXTS entry's thresholds list is `(min_wins, label, weight)`
tuples: `cutoff` is interpreted as a win-count threshold rather than a
position cutoff (the SoccerSource interpretation).

The base assumes `_fetch_full_season_games()` returns a flat list of
match dicts with `{id, home, away, home_points, away_points, status,
start_time}`. Subclasses adapt the source-specific API (CFBD's
season-wide /games for NCAAF; CBBD's date-range paginated /games for
NCAAM). The shape is intentionally close to FD.org's so a future
refactor can unify all sources around one canonical match dict.
"""

from __future__ import annotations

import logging
import random
from abc import abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import _season_store
from .base import (
    FeedUnavailable, GameRow, MatchResult, SeasonUnavailable, SportSource, recording_lost_days,
)
from .._util import poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.points_based")


class PointsBasedSportSource(SportSource):
    """Round-robin-ish point-scoring sport adapter (NCAA football/basketball,
    NHL/NBA/MLB regular season). Subclasses provide the season fetch and
    a LEAGUE_CONTEXTS code identifying the win-count outcome bands.

    State model:
      {
        "_applied":      frozenset of game_ids already applied,
        "_teams":        {team_name: {wins, losses, pf, pa, games_played}},
      }

    Strengths cache:
      {team_name: {"pf_per_game": float, "pa_per_game": float}}

    `pf_per_game` (points-for per game) doubles as the team's offensive
    rate; `pa_per_game` as the defensive rate. Lam for a matchup blends
    these in the standard Lahvička way:

      lam_home = (home.pf_per_game + away.pa_per_game) / 2
      lam_away = (away.pf_per_game + home.pa_per_game) / 2

    Poisson under-estimates variance for high-scoring sports (NCAAF
    games have stddev ~12 but Poisson(30) has stddev ~5.5). The win/loss
    distribution from Poisson is close enough for our tau-c importance
    use case; the bias slightly over-confidences the signal vs reality,
    which the cross-sport consequence-weight calibration compensates for.
    """

    supports_importance = True
    # Result model is pure Poisson on fixed strengths; sample_result never
    # reads state, so the season can be shared across games (#230).
    shared_season_importance = True

    # Subclasses MUST set these. Keep type-checkable defaults so the ABC
    # surfaces a clear NotImplementedError at instantiation if missed.
    league_context_code: str = ""
    # Why this refresh's season list could not be fetched whole, or None. A
    # class default, not only set in __init__, because several subclasses
    # own their caches without calling super().__init__(). See
    # `_fetch_games_cached`.
    _season_failure: Optional[str] = None
    # When a failed read was replaced by the last complete one, that read's
    # time (`_season_store`); None otherwise. Same class-default reason.
    _season_fallback_at: Optional[datetime] = None
    _DEFAULT_POINTS_FOR: float = 25.0
    _DEFAULT_POINTS_AGAINST: float = 25.0

    # Subclass override for `format="points_count"` sports (NHL) where the
    # outcome threshold is total standings points (regulation wins = 2,
    # OT/SO losses = 1, regulation losses = 0). Default "wins" matches
    # NCAAF / NCAAM / NBA / MLB where the threshold is win count and
    # there's no OT-loss point. terminal_outcomes reads team[_count_field]
    # when bucketing by threshold cutoff; apply_result must populate the
    # named field per game so the count keeps pace.
    _count_field: str = "wins"

    def __init__(self) -> None:
        # Caches populated lazily on first importance call.
        self._all_games_cache: Optional[List[Dict[str, Any]]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._initial_state_cache: Optional[Dict[str, Any]] = None

    # ---------- subclass hook ----------

    @abstractmethod
    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Return the full list of regular-season games (FINISHED + SCHEDULED)
        as plain dicts. Each match must have:

          {
            "id":           int,    # unique game ID (CFBD/CBBD numeric ID)
            "home":         str,    # team name
            "away":         str,
            "home_points":  int | None,   # None when SCHEDULED
            "away_points":  int | None,
            "status":       "FINISHED" | "SCHEDULED" | ...,
            "start_time":   datetime | None,
          }

        Caller (this class) caches the result per source instance.

        MUST NOT return a partial season quietly (#289). A request that
        failed, or answered something that is not its answer, raises
        `base.SeasonUnavailable` (or the `RankingsUnavailable` of a poll the
        population is drawn from); a day an ESPN walk lost is reported by
        the walk itself (`base.report_lost_days`). `_fetch_games_cached`
        reads both. An answer with no games (an offseason) is `[]`, quiet.
        """
        raise NotImplementedError

    def _fetch_games_cached(self) -> List[Dict[str, Any]]:
        """The season list, fetched ONCE per source instance (one refresh).

        A season that could not be fetched whole is never simulated with holes
        in it (#289). Before, a failed request or a lost walk day dropped its
        games from the list and the simulator replayed the rest as the whole
        season: standings missing results, a calendar missing fixtures, wrong
        stakes for every game in the league, and nothing in the summary.
        Instead, `season_fetch_failure()` says why, and:

          - the list is the source's last COMPLETE read, if one is no older
            than `_season_store.MAX_AGE` (`season_fallback()` gives its time;
            the refresh reports "<label> season (using the last complete
            read from <time>)"), and the league is scored as usual;
          - otherwise the list is `[]`, and the refresh never scores a game
            against it: `require_season`, called right after
            `fetch_upcoming`, raises, and the label is held as an outage like
            a failed rank feed (#283).

        Either way the league's channels stay put: dropping every game to no
        stakes for a refresh could push them out of the top N to be reaped
        and returned, the churn #261/#264 and #283 prevent elsewhere.

        A complete read is remembered (`_season_store.remember`) and replaces
        the last one; a failed or partial read never is.

        Lost days are read in a record of their OWN, because this runs at
        scoring time (outside any) and at build time for the playoff
        strength seeds, where the days are the season's, not an upcoming
        fetch's.

        The failure is kept for the refresh and NOT retried. The ESPN walk
        already re-asks a blip of failed days (`_espn._retry_failed_days`),
        and a second whole-season walk per league would multiply an outage
        across a dozen leagues against the 25-minute pipeline ceiling: one
        walk can spend its 120 s budget, where Football-Data's bounded
        retry (#228) is one request. The next refresh fetches afresh.
        """
        if self._all_games_cache is not None:
            return self._all_games_cache
        if self._season_failure is not None:
            return []
        # The key BEFORE the read: NCAAF and NCAAM pick their season from the
        # clock, and a key taken after a read that crossed their season
        # boundary would file last season's games under the next season.
        key = self.season_store_key()
        failure: Optional[str] = None
        try:
            with recording_lost_days() as lost:
                out = self._fetch_full_season_games()
        except FeedUnavailable as e:
            failure = str(e) or type(e).__name__
        else:
            if lost:
                days = sorted(lost)
                failure = (f"no answer for {len(days)} day(s) of the season "
                           f"({days[0].isoformat()}..{days[-1].isoformat()})")
        if failure is not None:
            self._season_failure = failure
            kept = _season_store.recall(key) if key else None
            if kept is None:
                logger.warning("[points_based] %s season not simulated: %s",
                               self.sport_label, failure)
                return []
            logger.warning(
                "[points_based] %s season read failed (%s); simulating the last "
                "complete read, from %s", self.sport_label, failure, kept.read_at.isoformat())
            self._season_fallback_at = kept.read_at
            self._all_games_cache = kept.games
            return self._all_games_cache
        self._all_games_cache = out or []
        if key:
            _season_store.remember(key, self._all_games_cache)
        return self._all_games_cache

    def season_store_key(self) -> Optional[str]:
        """The key this source's complete season reads are kept under
        (`_season_store`): class, label and season, so a new season never
        reads the last one's games and two sources never share one. None
        when the source names no season (`_season_store_id`), which keeps
        nothing."""
        season = self._season_store_id()
        if season is None:
            return None
        return f"{type(self).__name__}|{self.sport_label}|{season}"

    def _season_store_id(self) -> Optional[str]:
        """The season this source simulates, for `season_store_key`. The
        family names it one of three ways (NHL and MLB `season`, the NCAA
        baseball, softball and soccer and MLS sources `season_year`, NBA,
        NFL, WNBA and NCAAW `season_end_year`); a source whose population
        also depends on settings (NCAAF divisions, NCAAM's poll) overrides
        this to say so."""
        for attr in ("season", "season_year", "season_end_year"):
            value = getattr(self, attr, None)
            if value is not None:
                return str(value)
        return None

    def season_fallback(self) -> Optional[datetime]:
        """When a failed read was replaced by the last complete one, that
        read's time, else None (see `_fetch_games_cached`)."""
        return self._season_fallback_at

    def season_fetch_failure(self) -> Optional[str]:
        """Why this refresh's season list could not be fetched whole, or None
        (see `_fetch_games_cached`)."""
        return self._season_failure

    def require_season(self) -> None:
        """Read the season now (cached for scoring) and RAISE
        `SeasonUnavailable` when it could not be fetched whole AND no recent
        complete read stands in for it. See `plugin._require_seasons` for why
        the refresh asks."""
        self._fetch_games_cached()
        if self._season_failure is not None and self._season_fallback_at is None:
            raise SeasonUnavailable(f"season not fetched whole: {self._season_failure}")

    # ---------- importance interface ----------

    @property
    def outcome_labels(self) -> List[str]:
        from ..scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS.get(self.league_context_code)
        if ctx is None:
            return []
        return [label for _, label, _ in ctx.thresholds]

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        if self._strengths_cache is not None:
            return self._strengths_cache
        games = self._fetch_games_cached()
        sums: Dict[str, Dict[str, float]] = {}
        for g in games:
            if g.get("status") != "FINISHED":
                continue
            hp = g.get("home_points")
            ap = g.get("away_points")
            if hp is None or ap is None:
                continue
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            for team in (home, away):
                if team not in sums:
                    sums[team] = {"n": 0.0, "pf_total": 0.0, "pa_total": 0.0}
            sums[home]["n"] += 1
            sums[home]["pf_total"] += float(hp)
            sums[home]["pa_total"] += float(ap)
            sums[away]["n"] += 1
            sums[away]["pf_total"] += float(ap)
            sums[away]["pa_total"] += float(hp)
        out: Dict[str, Dict[str, float]] = {}
        for team, s in sums.items():
            if s["n"] <= 0:
                continue
            out[team] = {
                "pf_per_game": s["pf_total"] / s["n"],
                "pa_per_game": s["pa_total"] / s["n"],
            }
        self._strengths_cache = out
        return out

    def _strength_for(self, strengths: Dict[str, Dict[str, float]], team: str) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": self._DEFAULT_POINTS_FOR,
            "pa_per_game": self._DEFAULT_POINTS_AGAINST,
        }

    def initial_state(self) -> Dict[str, Any]:
        if self._initial_state_cache is not None:
            return self._initial_state_cache
        games = self._fetch_games_cached()
        state: Dict[str, Any] = {"_applied": frozenset(), "_teams": {}}
        applied: List[Any] = []
        teams: Dict[str, Dict[str, int]] = {}
        for g in games:
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            for team in (home, away):
                if team not in teams:
                    teams[team] = {"wins": 0, "losses": 0,
                                   "pf": 0, "pa": 0, "games_played": 0}
            if g.get("status") == "FINISHED":
                hp = g.get("home_points")
                ap = g.get("away_points")
                if hp is None or ap is None:
                    continue
                self._record_result_into_state(
                    teams, home, away, int(hp), int(ap),
                    result_extra=g.get("extra"),
                )
                gid = g.get("id")
                if gid is not None:
                    applied.append(gid)
        state["_teams"] = teams
        state["_applied"] = frozenset(applied)
        self._initial_state_cache = state
        return state

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        applied = state.get("_applied", frozenset())
        games = self._fetch_games_cached()
        out: List[GameRow] = []
        for g in games:
            gid = g.get("id")
            if gid is None or gid in applied:
                continue
            home = g.get("home")
            away = g.get("away")
            if not home or not away:
                continue
            start = g.get("start_time")
            if start is None:
                # GameRow.start_time is non-Optional per dataclass declaration.
                # Use a sentinel far-future timestamp; the importance simulator
                # doesn't read start_time, but downstream code might if a
                # caller hand-rolls a remaining-matches sweep for display.
                start = datetime(2099, 1, 1, tzinfo=timezone.utc)
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=None,  # importance doesn't need ranks
                rank_away=None,
                start_time=start,
                extra={"game_id": gid},
            ))
        return out

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Sample Poisson points per side, force a non-tie outcome (NCAA
        regulation ties go to OT in reality; we coin-flip the +1 boost so
        the W/L classification is honest)."""
        del state  # interface-required, not used at this level
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_pts = _poisson(lam_home, rng)
        away_pts = _poisson(lam_away, rng)
        if home_pts == away_pts:
            # OT: coin flip to break the tie. Strength asymmetry already
            # influenced the Poisson means; we don't add additional bias.
            if rng.random() < 0.5:
                home_pts += 1
            else:
                away_pts += 1
        return MatchResult(home_goals=home_pts, away_goals=away_pts)

    def apply_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        result: MatchResult,
    ) -> Dict[str, Any]:
        new_state = dict(state)
        new_teams = dict(state.get("_teams", {}))
        # Copy the two team rows we're about to mutate.
        for team in (match.home, match.away):
            if team in new_teams:
                new_teams[team] = dict(new_teams[team])
            else:
                # Defensive: simulator could call apply_result on a team
                # not seen in initial_state (e.g., new-season team that
                # only appears in scheduled games). Seed a zero row.
                new_teams[team] = {"wins": 0, "losses": 0,
                                   "pf": 0, "pa": 0, "games_played": 0}
        result_extra = result.extra if isinstance(result.extra, dict) else None
        self._record_result_into_state(
            new_teams, match.home, match.away,
            result.home_goals, result.away_goals,
            result_extra=result_extra,
        )
        new_state["_teams"] = new_teams
        extra = match.extra if isinstance(match.extra, dict) else {}
        gid = extra.get("game_id")
        if gid is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {gid}
        return new_state

    def _record_result_into_state(
        self,
        teams: Dict[str, Dict[str, int]],
        home: str, away: str,
        home_pts: int, away_pts: int,
        result_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Mutate `teams` in-place to record one game's result. Used by both
        initial_state (where we own the mutable state) and apply_result
        (which copies first to preserve immutability). NCAA games shouldn't
        end in regulation ties post-OT; if they do here, treat as a no-op
        for the win/loss columns but still record points.

        `result_extra` is the sport-specific metadata bag (for NHL: the
        `last_period_type` from sample_result, used to compute standings
        points). Default ignores it; NHL subclasses override the method
        to bake in the OT-loss-point logic. DO NOT use a @staticmethod
        decorator: subclasses need `self` for overriding.
        """
        del result_extra  # base ignores; NHL subclass reads
        h = teams[home]
        a = teams[away]
        h["pf"] += home_pts
        h["pa"] += away_pts
        a["pf"] += away_pts
        a["pa"] += home_pts
        h["games_played"] += 1
        a["games_played"] += 1
        if home_pts > away_pts:
            h["wins"] += 1
            a["losses"] += 1
        elif home_pts < away_pts:
            a["wins"] += 1
            h["losses"] += 1
        # Tie: no W/L update (shouldn't happen: sample_result enforces a
        # winner via coin-flip, and FD.org doesn't publish tied finals for
        # NCAA football/basketball).

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        """Bucket each team by the count-field threshold for this sport.

        For format="win_count" sports (CFB, CBB, NBA, MLB): _count_field
        defaults to "wins". 11 wins satisfies "11+ wins" AND "10+ wins"
        AND "8+ wins" AND "bowl_eligible": the caller sums leverage ×
        consequence over the band set per the cross-sport calibration.

        For format="points_count" sports (NHL): _count_field is
        "standings_points" and apply_result populates it per game from
        the OT-aware result-classification (regulation_win = +2, OT_loss
        = +1, regulation_loss = +0).
        """
        from ..scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS.get(self.league_context_code)
        if ctx is None:
            return {}
        teams = state.get("_teams", {})
        eligible = self.outcome_eligible_teams()
        if eligible is not None:
            teams = {t: row for t, row in teams.items() if t in eligible}
        out: Dict[str, List[str]] = {team: [] for team in teams}
        for min_count, label, _ in ctx.thresholds:
            for team, row in teams.items():
                if row.get(self._count_field, 0) >= min_count:
                    out[team].append(label)
        return out

    def outcome_eligible_teams(self) -> Optional[frozenset]:
        """Teams that may be assigned this league's terminal outcome bands,
        or None (the default) meaning every team in the state is eligible.

        GAME INCLUSION AND OUTCOME ELIGIBILITY ARE DIFFERENT QUESTIONS, and
        conflating them is a real modelling error rather than a tidiness
        one. A source may need a game in the population purely so it counts
        toward one participant's record while the OTHER participant does
        not belong in this league's outcome space at all.

        Worked example (NCAAF, measured 2026-08-29): the FBS-only season
        population must keep FBS-vs-FCS games, because those wins count
        toward the FBS team's bowl eligibility. That pulls the FCS
        opponents into the state as a side effect: 100 of 238 teams end up
        with 4 or fewer games and most with exactly 1, so they are scored
        against a win-count band ("6+ wins = bowl eligible") they cannot
        reach on a one-game schedule. Their contingency tables degenerate
        and their leverage silently reads 0, which is the right answer for
        the wrong reason and would flip to a wrong answer the moment the
        thresholds moved.

        Default None keeps every other points-based source (MLB, NBA, NHL,
        WNBA, NCAA baseball/softball/basketball) byte-identical.
        """
        return None


