"""NCAA Baseball source — ESPN's unofficial `site.api.espn.com` API.

No API key required. ESPN's API is undocumented but stable enough for
a homelab TV-guide curator. If it ever 404s, `fetch_upcoming` and
`_fetch_full_season_games` return [] and the affected sport silently
drops out of the guide for that refresh cycle — graceful-degrade is
already the contract.

Two source classes ship under the `enable_ncaa_baseball` toggle:
  - `NcaaBaseballRegularSource(PointsBasedSportSource)`: regular-season
    win-count importance. Tournament-bubble through national-seed bands
    (see LEAGUE_CONTEXTS["BSB"]).
  - `NcaaBaseballPlayoffSource(BestOfNSeriesSource)`: postseason. Phase 1
    ships the cleanly-labeled best-of-3 stages — Super Regional and the
    MCWS Championship Final. Regional (4-team double-elim per site) and
    the 8-team MCWS bracket in Omaha are Phase 2 (#43): ESPN headlines
    on those stages carry no game-number or bracket-position metadata,
    so chronological inference is needed and that's a separate design.

API endpoints used:
  - /apis/site/v2/sports/baseball/college-baseball/scoreboard
    - Daily scoreboard. `dates=YYYYMMDD` returns all games for that day
      (the `dates=YYYYMMDD-YYYYMMDD` range form silently caps at 25
      events). Used by both fetch_upcoming and _fetch_full_season_games.
      Postseason events carry `season.type=3` and `competition.notes[]
      .headline` like "NCAA Baseball Championship - Auburn Super Regional
      - Game 2".
  - /apis/site/v2/sports/baseball/college-baseball/rankings
    - D1Baseball.com Top 25 (the canonical D1 poll). Used to populate
      rank_home / rank_away on GameRow records so the rank-pair signal
      fires for marquee matchups.

Team name canonicalization: ESPN returns `team.location` ("UCLA") and
`team.name` ("Bruins"). We use `team.location` because that's what
typically appears in EPG titles ("UCLA at Texas") rather than the
mascot ("Bruins at Longhorns"). The school name is the stable join
key for matching.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from .points_based import PointsBasedSportSource
from .._util import (
    extract_game_number_after_marker,
    parse_iso_utc,
    poisson_sample as _poisson,
)

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_baseball")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball"

# D1 baseball season runs Feb through June (CWS finals typically third
# week of June). We sweep month by month so a single huge range doesn't
# risk pagination cutoffs from ESPN's API.
SEASON_START_MONTH = 2   # February
SEASON_END_MONTH = 6     # June (CWS finals)

# Default per-team scoring rate priors for teams the simulator hasn't
# seen yet (transfers, early-season cold starts). D1 baseball averages
# ~6 runs/team/game over the full season.
_DEFAULT_RUNS_FOR = 6.0
_DEFAULT_RUNS_AGAINST = 6.0


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """ESPN unofficial API wrapper. Returns the parsed JSON or None on
    any error (4xx / 5xx / connection failure / JSON decode). Logs at
    WARNING so silent degradation is observable in the dispatcharr log
    when the API misbehaves.
    """
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_baseball] %s → %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_baseball] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN returns both `team.location` (school: 'UCLA') and `team.name`
    (mascot: 'Bruins'). EPG provider titles use the school name. Fall
    back to nickname / abbreviation if location is missing on some
    edge-case competitor entries (occasionally happens for non-D1
    opponents in early-season scrimmages).
    """
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _is_postseason_event(event: Dict[str, Any]) -> bool:
    """ESPN tags NCAA Baseball / Softball postseason events with
    `season.type` in the 3-6 range — each stage gets its own type
    value (verified against live 2026 data):
      - 2: Regular season (NOT postseason).
      - 3: Regional round (4-team double-elim per site).
      - 4: Super Regional round (best-of-3, 8 sites).
      - 5: 8-team Men's / Women's College World Series bracket in
           Omaha / OKC (double-elim).
      - 6: Championship Finals (best-of-3 in Omaha / OKC).

    The regular-season source filters these out; the playoff source
    filters them in. Headline parsing further narrows to the stages we
    actually model (Phase 1: SUPER_REGIONAL + FINALS only — the others
    are headline-metadata-poor and require chronological inference for
    Phase 2).

    DO NOT use the `notes[].headline` "Championship" substring as a
    postseason discriminator — D1 conference tournaments in May use
    that same word and are NOT postseason. season.type is the
    authoritative tag from ESPN.

    DO NOT narrow to `type == 3` only — that was the original (wrong)
    assumption from the issue body and would mute Super Regional and
    Finals coverage entirely (those are types 4 and 6).
    """
    season_type = (event.get("season") or {}).get("type")
    return isinstance(season_type, int) and season_type > 2


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical PointsBased
    game record. Returns None if the event isn't a two-team competition
    we can score (cancellations, postponements with no competitors, etc.).
    """
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
    if completed or state == "post":
        status = "FINISHED"
    elif state == "in":
        # In-progress games are unstable scores. Treat as SCHEDULED so
        # the simulator doesn't seed wins/losses from mid-game state.
        status = "SCHEDULED"
    else:
        status = "SCHEDULED"

    try:
        hp = int(home.get("score")) if status == "FINISHED" else None
    except (TypeError, ValueError):
        hp = None
    try:
        ap = int(away.get("score")) if status == "FINISHED" else None
    except (TypeError, ValueError):
        ap = None

    # If "FINISHED" but scores are missing, demote to SCHEDULED — the
    # importance simulator must not seed a 0-0 result.
    if status == "FINISHED" and (hp is None or ap is None):
        status = "SCHEDULED"
        hp = None
        ap = None

    return {
        "id": event.get("id"),
        "home": home_team,
        "away": away_team,
        "home_points": hp,
        "away_points": ap,
        "status": status,
        "start_time": parse_iso_utc(event.get("date")),
        "extra": {},
    }


class NcaaBaseballRegularSource(PointsBasedSportSource):
    """NCAA Division I baseball regular-season importance.

    Win-count threshold bands tuned against historical NCAA Tournament
    selection criteria. The 64-team field invites roughly the top D1
    teams by a blend of RPI/wins/strength-of-schedule; 35+ wins is the
    rough at-large cutoff line, and 45+ wins put a team in national-seed
    contention.

    Postseason games (season.type=3 in ESPN's tags) are filtered out
    of both fetch_upcoming and _fetch_full_season_games — NcaaBaseball
    PlayoffSource owns those. Otherwise the regular-season win count
    would inflate by postseason wins, breaking the threshold bands.
    """

    league_context_code = "BSB"
    _DEFAULT_POINTS_FOR = _DEFAULT_RUNS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_RUNS_AGAINST

    def __init__(self, season_year: Optional[int] = None) -> None:
        super().__init__()
        # NCAA baseball seasons are named by their calendar year (the
        # 2026 season starts Feb 2026, ends June 2026). Default to the
        # current calendar year; pre-February treat as last season.
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year
            if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )

    @property
    def sport_prefix(self) -> str:
        return "NCAABSB"

    @property
    def sport_label(self) -> str:
        return "NCAA Baseball"

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Hit the scoreboard endpoint per-day covering today through
        `days_ahead` days. Populate rank_home/rank_away from the
        D1Baseball.com Top 25 so the rank-pair signal fires for marquee
        matchups. Per-day rather than range because ESPN's range
        endpoint silently caps at 25 events (see _fetch_full_season_games
        comment).
        """
        rankings = self._fetch_rankings()
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if not data:
                continue
            for event in data.get("events") or []:
                if _is_postseason_event(event):
                    # Owned by NcaaBaseballPlayoffSource. Skipping here
                    # prevents duplicate GameRow records under one
                    # espn_event_id.
                    continue
                rec = _extract_game_record(event)
                if rec is None:
                    continue
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                home = rec["home"]
                away = rec["away"]
                start = rec.get("start_time")
                if start is None:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=rankings.get(home),
                    rank_away=rankings.get(away),
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk the season day-by-day via the single-date scoreboard
        endpoint. Dedupe by event id. Returns the canonical shape
        PointsBasedSportSource expects.

        DO NOT use the `dates=YYYYMMDD-YYYYMMDD` range syntax — empirically
        ESPN's scoreboard endpoint silently caps range responses at 25
        events regardless of the `limit` parameter. Single-day queries
        (`dates=YYYYMMDD`) return ALL games for that day (~70-100 during
        peak season). Per-day iteration is ~110 calls Feb-now, ~10s total
        for a refresh.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1, tzinfo=timezone.utc).date()
        # Stop at min(current date + 7 lookahead, season end). Pre-season
        # we still walk into the season's first week to catch opening day.
        now = datetime.now(timezone.utc).date()
        # End of season is the last day of SEASON_END_MONTH.
        season_end_first = datetime(self.season_year, SEASON_END_MONTH, 1, tzinfo=timezone.utc).date()
        season_end = (season_end_first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        end = min(now + timedelta(days=7), season_end)
        if end < season_start:
            # Off-season / pre-season — no games yet.
            return []
        day = season_start
        while day <= end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    if _is_postseason_event(event):
                        # Postseason win/loss counts must NOT flow into
                        # the regular-season Monte Carlo — would corrupt
                        # the win-count threshold bands. See class
                        # docstring.
                        continue
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = rec
            day += timedelta(days=1)
        return list(seen.values())

    # ---------- Rankings (D1Baseball.com poll) ----------

    def _fetch_rankings(self) -> Dict[str, int]:
        """Return {canonical_team_name: ranking_number} for the D1Baseball
        Top 25. ESPN exposes one poll; if it's missing or empty we
        gracefully return {} and the rank-pair signal sits out the
        refresh cycle.
        """
        data = _http_get(f"{ESPN_BASE}/rankings")
        if not data:
            return {}
        ranks_by_team: Dict[str, int] = {}
        polls = data.get("rankings") or []
        if not polls:
            return ranks_by_team
        # Prefer the D1Baseball poll; if multiple polls appear later
        # we'd want to make this selectable, but for now there's just one.
        poll = polls[0]
        for r in poll.get("ranks") or []:
            team_obj = r.get("team") or {}
            name = _team_canonical_name(team_obj)
            try:
                rank = int(r.get("current") or 0)
            except (TypeError, ValueError):
                continue
            if name and rank > 0:
                ranks_by_team[name] = rank
        return ranks_by_team


# =====================================================================
# NcaaBaseballPlayoffSource — Phase 1 best-of-3 stages
# =====================================================================


# Headline → (stage, game_index) parser. ESPN's 2025-2026 headlines for
# the best-of-3 stages have always included a "Game N" suffix. The marker
# strings below MUST stay synchronized with ESPN's exact headline text;
# if ESPN ever drops the " - Game " separator (or pluralizes "Final"
# on baseball the way they do for "Finals" on softball) the parser
# returns (None, None) and the events fall through to the favorite +
# rank-pair signals only — same graceful-degrade contract as the
# rest of the source.
_SUPER_REGIONAL_MARKER = "Super Regional - Game "
_FINALS_MARKER = "Championship Final - Game "


def _parse_baseball_playoff_headline(headline: str) -> Tuple[Optional[str], Optional[int]]:
    """Map an ESPN postseason headline to (stage, game_index) for the
    Phase 1 best-of-3 stages. Returns (None, None) for Regional games
    (no game number, no bracket position — Phase 2) and the MCWS bracket
    games ("Men's College World Series - Double Elimination Round" /
    "Elimination Game", also Phase 2).

    Patterns observed in 2025-2026 ESPN data:
      - "NCAA Baseball Championship - Auburn Super Regional - Game 1"
      - "NCAA Baseball Championship - Auburn Super Regional - Game 3 (if necessary)"
      - "Men's College World Series Championship Final - Game 1"
    """
    if not headline:
        return None, None
    if _SUPER_REGIONAL_MARKER in headline:
        return "BSB_SR", extract_game_number_after_marker(headline, _SUPER_REGIONAL_MARKER)
    if _FINALS_MARKER in headline:
        return "MCWS_F", extract_game_number_after_marker(headline, _FINALS_MARKER)
    return None, None


class NcaaBaseballPlayoffSource(BestOfNSeriesSource):
    """NCAA Baseball postseason — Phase 1: Super Regional + MCWS Finals.

    Both stages are best-of-3 (`SERIES_LENGTH = 3`) with ESPN headlines
    that carry the game number, so the BestOfNSeriesSource infrastructure
    fits cleanly. The intermediate Regional (4-team double-elim per
    site) and 8-team MCWS bracket in Omaha lack game-number metadata in
    ESPN's data and require chronological inference — Phase 2 (#43).

    The forward-compatible depth structure (BSB_REG=0 → BSB_SR=1 → MCWS=2
    → MCWS_F=3 → MCWS_W=4) means Phase 2 can extend KO_STAGES without
    touching threshold labels in LEAGUE_CONTEXTS["MCWS_PO"]. The
    `omaha_bound` band (depth 2) fires for Super Regional WINNERS in
    Phase 1 because `_winner_advance_label("BSB_SR")` returns None →
    default `stage_depth + 1` rule → winner gets depth 2 (= MCWS depth).

    Strength sharing: pre-postseason, the plugin pulls regular-season
    strength estimates from NcaaBaseballRegularSource and seeds them
    via `set_regular_season_strengths`. Without this hook, postseason
    sampling falls back to the 6-runs-per-team default — workable but
    less informative than per-team Poisson rates from the 55-game
    regular season.
    """

    KO_STAGES = ("BSB_SR", "MCWS_F")
    SERIES_LENGTH = 3
    supports_importance = True

    def __init__(self, season_year: Optional[int] = None) -> None:
        # BestOfNSeriesSource's parent (BracketSportSource) inherits from
        # SportSource which has no __init__ args. Match the MlbPlayoffSource
        # pattern: own all caches in __init__ rather than relying on
        # class-level None defaults (which leak across instances during
        # tests).
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year
            if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAABSB"

    @property
    def sport_label(self) -> str:
        return "NCAA Baseball Postseason"

    def _league_context_code(self) -> str:
        return "MCWS_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # MCWS Final winner → MCWS_W (depth 4). Super Regional winner
        # advances via the default `stage_depth + 1` rule → depth 2,
        # which is the placeholder MCWS depth (8-team bracket; Phase 2).
        if stage == "MCWS_F":
            return "MCWS_W"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day postseason games. Same per-day scoreboard
        sweep as the regular-season source but with `_is_postseason_event`
        flipped (filter IN postseason). Headlines are then parsed via
        `_parse_baseball_playoff_headline`; only stages Phase 1 models
        (Super Regional + MCWS Finals) survive — Regional and 8-team
        MCWS bracket games are silently dropped here AND from the
        regular-season source's sibling fetch. They are entirely absent
        from the curated guide until Phase 2 ships.

        DO NOT lift the headline filter without also implementing
        Phase 2 chronological inference: doing so would emit Regional
        games with `fd_competition_code="MCWS_PO"` and the importance
        simulator would see them with no upstream feeders, producing
        an under-informed leverage signal that's worse than no signal.
        """
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if not data:
                continue
            for event in data.get("events") or []:
                if not _is_postseason_event(event):
                    continue
                row = self._event_to_game_row(event)
                if row is None:
                    continue
                eid = row.extra.get("espn_event_id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                out.append(row)
        return out

    def _event_to_game_row(self, event: Dict[str, Any]) -> Optional[GameRow]:
        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break
        stage, game_num = _parse_baseball_playoff_headline(headline)
        if stage is None or game_num is None:
            return None
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            return None
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            return None
        home_team = _team_canonical_name(home.get("team") or {})
        away_team = _team_canonical_name(away.get("team") or {})
        # TBD-vs-TBD games appear in ESPN's data before participants are
        # determined (e.g., Finals before the MCWS bracket resolves).
        # Skip them — the bracket source can't track a placeholder team
        # and the EPG client doesn't surface a "TBD" matchup usefully.
        if not home_team or not away_team:
            return None
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None
        start = parse_iso_utc(event.get("date"))
        if start is None:
            return None
        return GameRow(
            sport_prefix=self.sport_prefix,
            sport_label=self.sport_label,
            home=home_team,
            away=away_team,
            rank_home=None,
            rank_away=None,
            start_time=start,
            extra={
                "espn_event_id": event.get("id"),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "matchday": game_num,
                "headline": headline,
            },
        )

    # ---------- strengths (reused from regular season) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
        """Hook for plugin.py to seed per-team scoring/conceding rates
        from the regular-season source. Same shape as MlbPlayoffSource."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_RUNS_FOR,
            "pa_per_game": _DEFAULT_RUNS_AGAINST,
        }

    # ---------- sample_result (per-game Poisson with extra-innings) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        del state  # per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_runs = _poisson(lam_home, rng)
        away_runs = _poisson(lam_away, rng)
        # NCAA postseason has no draws — coin-flip the +1 to break the tie.
        if home_runs == away_runs:
            if rng.random() < 0.5:
                home_runs += 1
            else:
                away_runs += 1
        return MatchResult(home_goals=home_runs, away_goals=away_runs)

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Sweep the postseason date window day-by-day, filter to
        season.type=3 events with a Phase 1 stage headline, and emit
        the bracket per-game record shape.

        Window: May 25 (selection day; Regional play starts Friday of
        Memorial Day weekend) through July 1 (worst-case Finals slip).
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        season_start = datetime(self.season_year, 5, 25, tzinfo=timezone.utc).date()
        season_end = datetime(self.season_year, 7, 1, tzinfo=timezone.utc).date()
        day = season_start
        while day <= season_end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    if not _is_postseason_event(event):
                        continue
                    rec = self._event_to_bracket_record(event)
                    if rec is not None:
                        out.append(rec)
            day += timedelta(days=1)

        self._bracket_games_cache = out
        return out

    def _event_to_bracket_record(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert one ESPN postseason event into the bracket per-game
        record shape (`game_id`, `stage`, `matchday`, `home`, `away`,
        `home_goals`, `away_goals`, `status`, `start_time`, `extra`).
        Returns None for stages we don't model in Phase 1.
        """
        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break
        stage, game_num = _parse_baseball_playoff_headline(headline)
        if stage is None or game_num is None:
            return None
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
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None

        status_type = (comp.get("status") or {}).get("type") or {}
        completed = bool(status_type.get("completed"))
        state = (status_type.get("state") or "").lower()
        if completed or state == "post":
            status = "FINISHED"
        else:
            status = "SCHEDULED"
        try:
            hr = int(home.get("score")) if status == "FINISHED" else None
        except (TypeError, ValueError):
            hr = None
        try:
            ar = int(away.get("score")) if status == "FINISHED" else None
        except (TypeError, ValueError):
            ar = None
        if status == "FINISHED" and (hr is None or ar is None):
            status = "SCHEDULED"
            hr = None
            ar = None

        return {
            "game_id": event.get("id"),
            "stage": stage,
            "matchday": game_num,
            "home": home_team,
            "away": away_team,
            "home_goals": hr,
            "away_goals": ar,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"headline": headline},
        }
