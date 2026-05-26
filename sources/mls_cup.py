"""MLS Cup playoff bracket: issue #30 part B.

Mixed-format postseason that neither AggregateLegSource nor a uniform
best-of-N source handles cleanly:

  - Wild Card round (`MLS_WC`): single-leg play-in, 8th vs 9th seed
    in each conference (2 total games)
  - Round One (`MLS_R1`): best-of-3 series, top 7 seeds + the 2 WC
    winners → 8 teams per conference in 4 series each (8 series total)
  - Conference Semifinals (`MLS_CSF`): single-leg elimination
  - Conference Finals (`MLS_CF`): single-leg at the higher seed
  - MLS Cup (`MLS_CUP`): single-leg cross-conference final at the
    higher seed
  - Champion (`MLS_CUP_WINNER`): synthetic depth, the cup winner

Per-stage series-length routing via `_series_length_for_stage`:
inherited from PR #51 (Phase F's BestOfNSeriesSource extension that
NCAA Baseball uses for the mixed Super Regional / MCWS Final bracket).
The same hook handles MLS's mix of best-of-3 R1 and single-leg
everything-else cleanly.

ESPN slug → stage mapping reflects the per-conference tagging on
ESPN's MLS scoreboard:

| ESPN season.slug                              | Stage label  |
|-----------------------------------------------|--------------|
| {eastern,western}-conference-playoffs---wild-card     | MLS_WC      |
| {eastern,western}-conference-playoffs---round-one     | MLS_R1      |
| {eastern,western}-conference-playoffs---semifinals    | MLS_CSF     |
| {eastern,western}-conference-playoffs---final         | MLS_CF      |
| mls-cup                                                | MLS_CUP     |

Strength sharing: `set_regular_season_strengths` lets the plugin seed
playoff goal-scoring rates from `MlsEastSource` / `MlsWestSource`
(issue #30 part A). Without the seed, sample_result falls back to the
1.4/1.4 league-average prior.

Best-of-3 matchday inference: ESPN doesn't tag MLS R1 events with a
game number on the event object: the `competition.notes[0].headline`
carries series state ("LA leads series 1-0") but not the specific
game index. Matchday is inferred from chronological ordering within
each (stage, frozenset(participants)) tuple in `_fetch_bracket_games`,
matching the order ESPN publishes them. BestOfNSeriesSource's state
machine then groups them into the right tie via the matchday counter
and resolves the series-clinch threshold (2 wins for best-of-3) via
`_series_length_for_stage("MLS_R1") == 3`.
"""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from .._util import parse_iso_utc, poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mls_cup")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"

# Tournament window: MLS playoffs run late October (Wild Card / R1)
# through early December (MLS Cup Final). Walking Oct 1 - Dec 15 covers
# both ends with margin.
PLAYOFF_START_MONTH = 10
PLAYOFF_END_MONTH = 12
PLAYOFF_END_DAY = 15

# Default per-team goal scoring/conceding rate prior. Matches the
# MlsStandingsSourceBase prior (modern MLS ~1.4 goals/team/game) so
# the regular-season → playoff strength seed lines up consistently.
_DEFAULT_GOALS_FOR = 1.4
_DEFAULT_GOALS_AGAINST = 1.4

# Per-stage best-of-N. The R1 best-of-3 is the only deviation from
# single-leg everything-else. SERIES_LENGTH=1 is the class fallback so
# any unmodeled stage clinches in 1 game by default (defense in depth
# against ESPN adding a new slug we haven't routed yet).
_MLS_SERIES_LENGTHS: Dict[str, int] = {
    "MLS_WC":  1,
    "MLS_R1":  3,
    "MLS_CSF": 1,
    "MLS_CF":  1,
    "MLS_CUP": 1,
}

# ESPN `season.slug` -> MLS bracket stage. Both conferences share the
# slug-suffix pattern (eastern-conference-playoffs---X vs western-
# conference-playoffs---X); the cross-conference MLS Cup Final uses
# its own `mls-cup` slug with no conference prefix. DO NOT consolidate
# the East/West entries; explicit keys keep the parser table
# self-documenting and a slug rename will fail loudly here rather than
# silently misroute one conference's bracket.
SLUG_TO_STAGE: Dict[str, str] = {
    "eastern-conference-playoffs---wild-card":  "MLS_WC",
    "western-conference-playoffs---wild-card":  "MLS_WC",
    "eastern-conference-playoffs---round-one":  "MLS_R1",
    "western-conference-playoffs---round-one":  "MLS_R1",
    "eastern-conference-playoffs---semifinals": "MLS_CSF",
    "western-conference-playoffs---semifinals": "MLS_CSF",
    "eastern-conference-playoffs---final":      "MLS_CF",
    "western-conference-playoffs---final":      "MLS_CF",
    "mls-cup":                                   "MLS_CUP",
}


def _http_get(url: str, timeout: float = 15.0) -> Optional[Any]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[mls_cup] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[mls_cup] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN soccer returns `team.displayName` ("Atlanta United FC",
    "LA Galaxy"). Same canonicalizer as MlsStandingsSourceBase /
    MlsSource so the cross-source team-name join (regular-season
    strength seed → playoff teams) holds without name drift."""
    name = (team_obj.get("displayName") or "").strip()
    if name:
        return name
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _default_season_year() -> int:
    """MLS season is named by the calendar year. Pre-October reads as
    the prior season's playoffs (which wrap in early December to January
    onward is offseason). Post-October reads as the current calendar
    year's playoffs in progress.
    """
    now = datetime.now(timezone.utc)
    return now.year if now.month >= PLAYOFF_START_MONTH else now.year - 1


class MlsCupSource(BestOfNSeriesSource):
    """MLS Cup playoff bracket as a BestOfNSeriesSource with per-stage
    series lengths. R1 best-of-3, all other stages single-leg
    (SERIES_LENGTH=1). Bracket cascade flows:

      MLS_WC (entry) → MLS_R1 → MLS_CSF → MLS_CF → MLS_CUP → MLS_CUP_WINNER

    Wild Card is the entry stage in modern MLS playoffs (since the 2024
    14-9 format expanded the field). Teams with a R1 bye (top 7 seeds
    of each conference) skip the WC tier: their `round_reached` enters
    at MLS_R1 depth.
    """

    KO_STAGES = ("MLS_WC", "MLS_R1", "MLS_CSF", "MLS_CF", "MLS_CUP")
    SERIES_LENGTH = 1  # fallback when a stage slips out of _MLS_SERIES_LENGTHS
    supports_importance = True

    def __init__(self, season_year: Optional[int] = None) -> None:
        self.season_year = season_year or _default_season_year()
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[
            Dict[str, Dict[str, float]]
        ] = None

    @property
    def sport_prefix(self) -> str:
        return "MLS"

    @property
    def sport_label(self) -> str:
        return "MLS Cup Playoffs"

    def _league_context_code(self) -> str:
        return "MLS_PO"

    def _series_length_for_stage(self, stage: str) -> int:
        # R1 = best-of-3, everything else = single-leg.
        return _MLS_SERIES_LENGTHS.get(stage, self.SERIES_LENGTH)

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # MLS Cup winner → MLS_CUP_WINNER synthetic depth. All other
        # stages flow into the next KO_STAGES entry via the base class
        # `_round_reached` cascade.
        if stage == "MLS_CUP":
            return "MLS_CUP_WINNER"
        return None

    # ---------- strength sharing ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate, seeded from the regular-
        season MlsEastSource + MlsWestSource via
        `set_regular_season_strengths` (the plugin merges both
        conferences' strengths into one dict before seeding). Without
        the seed, falls back to the league-average prior."""
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]],
    ) -> None:
        """Plugin hook to seed per-team scoring rates from the
        regular-season sources. Pass the merged East + West strength
        dict; the playoff sampler doesn't care which conference a
        team came from since MLS Cup is a cross-conference final."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_GOALS_FOR,
            "pa_per_game": _DEFAULT_GOALS_AGAINST,
        }

    # ---------- sample_result (force a winner: bracket games) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Bracket games CANNOT end in draws: MLS postseason goes to
        OT then PKs if needed. Same shape as NcaaSoccerCupSource's
        sample_result and distinct from the regular-season
        MlsStandingsSourceBase sample_result which allows draws (1
        standings point each). DO NOT borrow the regular-season
        sampler here; bracket cascades require deterministic winners.
        """
        del state  # interface-required, per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.05, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_goals = _poisson(lam_home, rng)
        away_goals = _poisson(lam_away, rng)
        if home_goals == away_goals:
            # PK shootouts are roughly coin-flips at the MLS level:
            # no additional bias beyond the Poisson means.
            if rng.random() < 0.5:
                home_goals += 1
            else:
                away_goals += 1
        return MatchResult(home_goals=home_goals, away_goals=away_goals)

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Emit upcoming playoff bracket games. Per-day sweep with the
        same slug filter that `_fetch_bracket_games` uses: only events
        whose `season.slug` is in `SLUG_TO_STAGE` surface here, so
        regular-season games (filtered out earlier in `MlsEastSource` /
        `MlsWestSource`) don't reappear via this source.
        """
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(
                f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}"
            )
            if not isinstance(data, dict):
                continue
            for event in data.get("events") or []:
                rec = self._extract_bracket_record(event, matchday=1)
                if rec is None:
                    continue
                eid = rec.get("game_id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                start = rec.get("start_time")
                if start is None:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=rec["home"],
                    away=rec["away"],
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "fd_competition_code": self._league_context_code(),
                        "stage": rec.get("stage"),
                    },
                ))
        return out

    # ---------- _fetch_bracket_games (importance side) ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the playoff window (Oct 1 - Dec 15 of season_year) and
        emit one canonical record per bracket game. Matchday inference
        for best-of-3 series: walk dates ascending and count games per
        (stage, frozenset(participants)) tuple: first chronological
        appearance is game 1, second is game 2, third is game 3.
        Single-leg stages always emit matchday=1.

        Single-game-elim stages (MLS_WC, MLS_CSF, MLS_CF, MLS_CUP) also
        need PK dedup. ESPN sometimes publishes two events when a game
        goes to PKs (one with the regulation tie, one with the PK
        result): see `dedupe_pk_shootout_pairs` in
        `sources/_soccer_bracket_helpers.py` (the canonical impl shared
        with ncaa_soccer_cup). The dedup criterion is sport-agnostic
        for single-game-elim brackets.

        Best-of-3 stages are NOT deduped on (stage, participants):
        the same teams legitimately play 2-3 games in a series, so
        collapsing them would lose the matchday cascade. PK dedup is
        applied per-matchday inside the best-of-3 grouping.
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        # First pass: collect all candidate records in chronological
        # order. Walk Oct 1 - Dec 15 of season_year.
        candidates: List[Dict[str, Any]] = []
        seen_ids: set = set()
        start = datetime(
            self.season_year, PLAYOFF_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        end = datetime(
            self.season_year, PLAYOFF_END_MONTH, PLAYOFF_END_DAY,
            tzinfo=timezone.utc,
        ).date()
        day = start
        while day <= end:
            data = _http_get(
                f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}"
            )
            if isinstance(data, dict):
                for event in data.get("events") or []:
                    # Pass matchday=None: the inference loop below
                    # assigns it per (stage, participants) tuple.
                    rec = self._extract_bracket_record(event, matchday=None)
                    if rec is None or rec["game_id"] is None:
                        continue
                    if rec["game_id"] in seen_ids:
                        continue
                    seen_ids.add(rec["game_id"])
                    candidates.append(rec)
            day += timedelta(days=1)

        # Second pass: matchday inference for best-of-3 series + PK
        # dedup for single-leg stages.
        out = _assign_matchdays_and_dedupe(candidates, _MLS_SERIES_LENGTHS)
        self._bracket_games_cache = out
        return out

    @staticmethod
    def _extract_bracket_record(
        event: Dict[str, Any],
        matchday: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        """Convert one ESPN MLS scoreboard event into the canonical
        per-game record. Returns None if the event's `season.slug`
        isn't a playoff slug (filters regular-season games).

        `matchday` is passed in: for fetch_upcoming we pass 1 (UI emit
        doesn't need the series ordering); for _fetch_bracket_games
        we pass None and infer matchday from chronological order in
        `_assign_matchdays_and_dedupe`.
        """
        season = event.get("season") or {}
        slug = (season.get("slug") or "").strip().lower()
        stage = SLUG_TO_STAGE.get(slug)
        if stage is None:
            return None

        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            return None
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            return None
        home_team = _team_canonical_name(home.get("team") or {})
        away_team = _team_canonical_name(away.get("team") or {})
        if not home_team or not away_team:
            return None

        status_type = (comp.get("status") or {}).get("type") or {}
        completed = bool(status_type.get("completed"))
        state = (status_type.get("state") or "").lower()
        # ESPN publishes phantom placeholder games for best-of-3
        # series that clinch in 2 games: the unnecessary game-3
        # has state="post" + completed=False + score=0-0. Skip
        # these entirely; emitting them (even as SCHEDULED) would
        # inject a never-going-to-happen game into the simulator's
        # remaining_matches list and confuse the series-clinch
        # cascade. Observed live on the 2024 R1 Colorado @ LA
        # Galaxy series: event 722587 has state="post" but
        # completed=False because the series ended at game 2. DO
        # NOT relax this gate without re-verifying against a
        # season where ESPN's phantom-game shape has changed.
        if state == "post" and not completed:
            return None
        is_final = completed
        status = "FINISHED" if is_final else "SCHEDULED"

        try:
            hg = int(home.get("score")) if is_final else None
        except (TypeError, ValueError):
            hg = None
        try:
            ag = int(away.get("score")) if is_final else None
        except (TypeError, ValueError):
            ag = None

        if is_final and (hg is None or ag is None):
            status = "SCHEDULED"
            hg = None
            ag = None

        return {
            "game_id": event.get("id"),
            "stage": stage,
            "matchday": matchday,  # may be None: inference assigns later
            "home": home_team,
            "away": away_team,
            "home_goals": hg,
            "away_goals": ag,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"season_slug": slug},
        }


# Bracket dedup helpers are shared with ncaa_soccer_cup -- canonical
# impls live in _soccer_bracket_helpers. Re-export under the legacy
# names so any tests / call sites grepping for
# `_dedupe_pk_shootout_pairs` in this module still find it.
from ._soccer_bracket_helpers import (
    dedupe_pk_shootout_pairs as _dedupe_pk_shootout_pairs,
    pick_decisive_event as _pick_decisive_event,
)


def _assign_matchdays_and_dedupe(
    candidates: List[Dict[str, Any]],
    series_lengths: Dict[str, int],
) -> List[Dict[str, Any]]:
    """Two-stage post-processor for the chronologically-ordered
    candidate game list:

      1. For best-of-N stages (`series_lengths[stage] > 1`): assign
         matchday by counting per (stage, frozenset(participants))
         occurrence in chronological order. First match in a series
         = matchday 1, second = 2, etc.

      2. For single-leg stages (`series_lengths[stage] == 1`): collapse
         PK-shootout pairs via the same `_dedupe_pk_shootout_pairs`
         logic from ncaa_soccer_cup. Imported here rather than
         duplicated; the criterion ("two finished events at the same
         single-leg stage with the same teams must be the regulation
         tie + PK result, so keep the non-tie one") is sport-agnostic.

    Returns the merged list with matchday assigned for all records.
    Best-of-N records carry their inferred 1/2/3 matchday; single-leg
    records carry matchday=1.
    """
    # Sort candidates by start_time (ascending) so matchday inference
    # follows real chronological order, not insertion-order of the
    # day-by-day fetch loop. None start_times sort last (defensive
    # against malformed events).
    def _sort_key(rec: Dict[str, Any]) -> datetime:
        st = rec.get("start_time")
        if isinstance(st, datetime):
            return st
        return datetime.max.replace(tzinfo=timezone.utc)

    candidates_sorted = sorted(candidates, key=_sort_key)

    # Bucket by stage so single-leg vs best-of-N can be handled separately.
    single_leg: List[Dict[str, Any]] = []
    best_of_n: List[Dict[str, Any]] = []
    for rec in candidates_sorted:
        stage = rec["stage"]
        length = series_lengths.get(stage, 1)
        if length > 1:
            best_of_n.append(rec)
        else:
            single_leg.append(rec)

    # Best-of-N: assign matchday by counting per (stage, participants).
    counters: Dict[Tuple[str, FrozenSet[str]], int] = defaultdict(int)
    for rec in best_of_n:
        key = (rec["stage"], frozenset((rec["home"], rec["away"])))
        counters[key] += 1
        rec["matchday"] = counters[key]

    # Single-leg: enforce matchday=1 (the second pass below dedupes
    # any PK-shootout pair).
    for rec in single_leg:
        rec["matchday"] = 1
    deduped_single = _dedupe_pk_shootout_pairs(single_leg)

    return deduped_single + best_of_n
