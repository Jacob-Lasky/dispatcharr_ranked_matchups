"""NBA source: ESPN's unofficial `site.api.espn.com` API. No key required.

stats.nba.com would be the official source for NBA data, but it's behind
a WAF that blocks most homelab egress (including pocket-dev's). ESPN's
unofficial scoreboard endpoint is the practical fallback: same shape as
NCAA Baseball / NCAA Soccer in this codebase, and stable enough for a
TV-guide curator. If ESPN ever changes its endpoint, `fetch_upcoming`
and the importance interfaces return [] and NBA silently drops out of
the guide for that refresh: graceful-degrade is already the contract.

Two source classes:
  - `NbaRegularSource(PointsBasedSportSource)`: 82-game regular season,
    raw win count as the threshold field (LEAGUE_CONTEXTS["NBA"] is
    format="win_count"). No OT bonus point: NBA OT wins count as
    normal wins. Thresholds at 40 (play-in bubble) / 50 (comfortable
    in) / 55 (top-3 seed pace) / 65 (historic).
  - `NbaPlayoffSource(BestOfNSeriesSource)`: best-of-7 each round, 4
    rounds (R1 / CSF / CF / FINALS). Same structural shape as NHL
    Stanley Cup Playoffs, modulo terminology.

API quirks captured here:
  - `season.type`: 2 = regular season, 3 = postseason. Use this as the
    discriminator between regular-season and playoff fetches; do NOT
    rely on `gameType` or date ranges (postseason can stretch from
    mid-April through mid-June).
  - ESPN encodes the postseason stage in the competition's notes
    `headline` field, e.g. "East 1st Round - Game 3", "West
    Semifinals - Game 2", "East Finals - Game 1", "NBA Finals - Game 6".
    No structured field exposes the round; the headline regex is the
    only way to map games to bracket stages.
  - Per-day iteration is required for the full season schedule. ESPN
    silently caps `dates=YYYYMMDD-YYYYMMDD` range responses at 25
    events; single-date queries return ALL games for that day. Same
    trap as `ncaa_baseball.py`.
  - Team names use `team.displayName` (e.g., "Oklahoma City Thunder")
    because that's what NBA EPG provider titles typically use, and
    they're unambiguous as the canonical join key.

The plugin opts into NBA via the `enable_nba` boolean in `plugin.json`.
Off by default.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow
from .bracket import BestOfNSeriesSource
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.nba")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# NBA regular-season scoring rate cluster: ~115 ppg per team in modern NBA.
# Used as the prior for teams the simulator hasn't seen yet (early-season
# imports before opening week).
_DEFAULT_POINTS_FOR = 115.0
_DEFAULT_POINTS_AGAINST = 115.0

# NBA season starts mid-to-late October, ends in mid-April (regular).
# Postseason runs mid-April through mid-June (Finals Game 7 latest).
# We iterate Oct 1 through max(today + 7d, Jun 30 of season-end year).
# ESPN's `season.year` field uses the ENDING calendar year (e.g., "2025"
# means the 2024-25 season).
SEASON_START_MONTH = 10  # October
SEASON_END_MONTH = 6     # June (Finals)


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """ESPN unofficial API wrapper. Returns parsed JSON or None on any
    error. Logs at WARNING so silent degradation is observable in the
    dispatcharr log."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[nba] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[nba] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN gives `team.displayName` ("Oklahoma City Thunder") and
    `team.name` ("Thunder"); EPG providers use the full City+Mascot
    form. Fall back to abbreviation only as a last resort."""
    name = (team_obj.get("displayName") or "").strip()
    if name:
        return name
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _default_season_end_year() -> int:
    """Current NBA season's ending calendar year (matching ESPN's
    `season.year`). The 2024-25 season → 2025. Pre-October treats the
    season as the current calendar year (still in the 2024-25 example
    until October 2025 when 2025-26 starts)."""
    now = datetime.now(timezone.utc)
    return now.year + 1 if now.month >= SEASON_START_MONTH else now.year


# Stage-headline parser. ESPN encodes the postseason round in
# `competition.notes[0].headline` as one of:
#   "East 1st Round - Game N"   / "West 1st Round - Game N"     -> R1
#   "East Semifinals - Game N"  / "West Semifinals - Game N"    -> CSF
#   "East Finals - Game N"      / "West Finals - Game N"        -> CF
#   "NBA Finals - Game N"                                       -> FINALS
# The regex captures the round descriptor + game number so the bracket
# source can route a game to the right tie + matchday.
_HEADLINE_RE = re.compile(
    r"^(?:(?P<conf>East|West)\s+(?P<round>1st\s+Round|Semifinals|Finals)|"
    r"NBA\s+(?P<nba_finals>Finals))\s*-\s*Game\s+(?P<game>\d+)",
    re.IGNORECASE,
)

# Maps the matched headline tokens to KO_STAGES labels.
_ROUND_TO_STAGE: Dict[str, str] = {
    "1st round": "R1",
    "semifinals": "CSF",
    "finals":     "CF",   # East/West Finals (conference finals)
}


def _parse_stage_from_headline(headline: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return {"stage": str, "matchday": int} on match, or None.

    The "Finals" token is ambiguous on its own: "East/West Finals" is
    the Conference Finals (stage=CF), while "NBA Finals" is the
    championship round (stage=FINALS). The regex disambiguates via the
    leading East/West vs NBA token; this helper just dispatches.
    """
    if not headline:
        return None
    m = _HEADLINE_RE.search(headline)
    if not m:
        return None
    try:
        matchday = int(m.group("game"))
    except (TypeError, ValueError):
        return None
    if m.group("nba_finals"):
        return {"stage": "FINALS", "matchday": matchday}
    round_key = (m.group("round") or "").lower().strip()
    # "1st round" comes through as "1st round" (\s+ matched).
    round_key = re.sub(r"\s+", " ", round_key)
    stage = _ROUND_TO_STAGE.get(round_key)
    if stage is None:
        return None
    return {"stage": stage, "matchday": matchday}


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical
    PointsBased game record. Returns None for unscoreable events
    (cancellations, postponements with no competitors) AND for
    non-bracket exhibition games like the All-Star Tournament
    (which ESPN labels season.type==2 alongside real regular-season
    games: see competition.type.abbreviation == "ALLSTAR").

    Returned `competition.type.abbreviation` values:
      - "STD"     -> regular-season game
      - "FINAL"   -> playoff game (ESPN's confusing label: applies
                     to every playoff round, not just the Finals)
      - "ALLSTAR" -> All-Star Tournament (rejected here)
    """
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
    # Skip All-Star Tournament games. Starting in 2024-25 ESPN labels
    # these with season.type=2 (regular-season-like) and exposes them
    # via the same scoreboard endpoint, which pollutes the regular-
    # season team list with "Team Chuck", "Team Shaq", etc. Filter
    # on competition.type.abbreviation which correctly tags them.
    comp_type_abbrev = ((comp.get("type") or {}).get("abbreviation") or "").upper()
    if comp_type_abbrev == "ALLSTAR":
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

    status_type = (comp.get("status") or {}).get("type") or {}
    completed = bool(status_type.get("completed"))
    state = (status_type.get("state") or "").lower()
    if completed or state == "post":
        status = "FINISHED"
    elif state == "in":
        # In-progress games: don't seed mid-game scores into the
        # importance simulator. Treat as SCHEDULED.
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

    if status == "FINISHED" and (hp is None or ap is None):
        # Status says FINISHED but a score is missing: demote so the
        # simulator doesn't seed a phantom 0-0 result.
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
        "season_type": ((event.get("season") or {}).get("type")),  # 2=regular, 3=postseason
        "notes": comp.get("notes") or [],
    }


# =====================================================================
# NbaRegularSource
# =====================================================================


class NbaRegularSource(PointsBasedSportSource):
    """NBA regular-season importance via PointsBasedSportSource.

    Uses raw `wins` (LEAGUE_CONTEXTS["NBA"] is format="win_count"). NBA
    has no OT-loss point: an overtime loss is still just a loss. Goal-
    sampling Poisson on points-per-game; tied regulation gets a coin-flip
    +1 boost (modeling OT as a normal W/L).
    """

    league_context_code = "NBA"
    _count_field = "wins"
    _DEFAULT_POINTS_FOR = _DEFAULT_POINTS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_POINTS_AGAINST

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        super().__init__()
        self.season_end_year = season_end_year or _default_season_end_year()

    @property
    def sport_prefix(self) -> str:
        return "NBA"

    @property
    def sport_label(self) -> str:
        return "NBA"

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull the next-N-day schedule via per-day scoreboard queries
        (the date-range syntax silently caps at 25 events). Restricted
        to regular-season games here; the playoff source has its own
        fetch_upcoming that filters on season.type == 3.
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
                rec = _extract_game_record(event)
                if rec is None:
                    continue
                # Regular season only (season.type == 2). The playoff
                # source handles type == 3.
                if rec.get("season_type") != 2:
                    continue
                eid = rec.get("id")
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
                        "nba_game_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk every day from season start (Oct 1 of season_end_year-1)
        through min(today + 7d, end of regular season). Filter on
        season.type == 2 so postseason games don't leak into the
        regular-season importance calculation.

        Per-day rather than date-range because ESPN's range query
        silently caps at 25 events.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        # NBA season: starts mid-late October of (end_year - 1),
        # regular season ends mid-April of end_year. We start Oct 1
        # to catch preseason games (which fall under season.type=1
        # and will be filtered out by the type==2 gate below).
        season_start = datetime(
            self.season_end_year - 1, SEASON_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        # End of regular season: April 30 of the ending year (some
        # play-in games happen in mid-April; this comfortably covers).
        regular_end = datetime(
            self.season_end_year, 4, 30, tzinfo=timezone.utc,
        ).date()
        now = datetime.now(timezone.utc).date()
        end = min(now + timedelta(days=7), regular_end)
        if end < season_start:
            return []
        day = season_start
        while day <= end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec.get("season_type") != 2:
                        # Skip preseason (type=1) and postseason (type=3).
                        continue
                    if rec["id"] in seen:
                        continue
                    # The base PointsBasedSportSource doesn't read the
                    # season_type or notes fields: strip them so the
                    # downstream contract stays tight.
                    seen[rec["id"]] = {
                        k: v for k, v in rec.items()
                        if k not in ("season_type", "notes")
                    }
            day += timedelta(days=1)
        return list(seen.values())


# =====================================================================
# NbaPlayoffSource
# =====================================================================


class NbaPlayoffSource(BestOfNSeriesSource):
    """NBA playoffs as a best-of-7 BestOfNSeriesSource. Same shape as
    NhlPlayoffSource: 4 rounds, uniform SERIES_LENGTH=7, conference-
    bracket structure where R1 has 8 series total (4 East + 4 West),
    CSF has 4 series, CF has 2 series, and FINALS has 1 cross-
    conference series.

    Stage routing comes from parsing the `competition.notes[0].headline`
    field on each ESPN scoreboard event: there's no structured stage
    field on the API.
    """

    KO_STAGES = ("R1", "CSF", "CF", "FINALS")
    SERIES_LENGTH = 7
    supports_importance = True

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        self.season_end_year = season_end_year or _default_season_end_year()
        # Caches for the importance interface: same pattern as NhlPlayoffSource.
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "NBA"

    @property
    def sport_label(self) -> str:
        return "NBA Playoffs"

    def _league_context_code(self) -> str:
        return "NBA_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # FINALS winner → FINALS_WINNER synthetic depth.
        if stage == "FINALS":
            return "FINALS_WINNER"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day postseason schedule via per-day scoreboard
        queries. Filter on season.type == 3 so regular-season games
        don't leak into the playoff source's output."""
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if not data:
                continue
            for event in data.get("events") or []:
                rec = _extract_game_record(event)
                if rec is None or rec.get("season_type") != 3:
                    continue
                eid = rec.get("id")
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
                        "nba_game_id": eid,
                        "fd_competition_code": self._league_context_code(),
                    },
                ))
        return out

    # ---------- strengths (reused from regular season) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate. Playoff samples are sparse
        (a postseason team plays at most ~28 games); preload from a
        regular-season NbaRegularSource via `set_regular_season_strengths`
        when available."""
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
        """Hook for plugin to share regular-season strength estimates
        with the playoff source. Same shape as NhlPlayoffSource's."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_POINTS_FOR,
            "pa_per_game": _DEFAULT_POINTS_AGAINST,
        }

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the entire postseason schedule and normalize each game
        to the bracket per-game record shape. Stage comes from parsing
        the ESPN headline ("East 1st Round - Game 3", etc.): that's
        the only place ESPN exposes round information.

        Iteration window: April 1 (early play-in) through July 1 (latest
        possible Game-7 NBA Finals tail) of the season-end calendar year.
        That's a small enough window (~90 days) that the per-day cost
        is bounded: playoff days have at most 4 games (R1) and often
        just 1 (Finals), so the dedupe map stays small.
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        seen_ids: set = set()
        # Playoff window: April-June of season_end_year (mid-April through
        # mid-June typically; we widen to April 1 - July 1 to cover all
        # historical and possible-future shifts).
        start = datetime(self.season_end_year, 4, 1, tzinfo=timezone.utc).date()
        end = datetime(self.season_end_year, 7, 1, tzinfo=timezone.utc).date()
        day = start
        while day <= end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec.get("season_type") != 3:
                        continue
                    if rec["id"] in seen_ids:
                        continue
                    # Decode stage from notes[0].headline.
                    headline = None
                    notes = rec.get("notes") or []
                    if notes:
                        headline = (notes[0] or {}).get("headline")
                    parsed = _parse_stage_from_headline(headline)
                    if parsed is None:
                        # Play-in tournament games have headlines like
                        # "Play-In Tournament" with no game number. They
                        # aren't part of the 16-team bracket, so skip.
                        continue
                    seen_ids.add(rec["id"])
                    out.append({
                        "game_id": rec["id"],
                        "stage": parsed["stage"],
                        "matchday": parsed["matchday"],
                        "home": rec["home"],
                        "away": rec["away"],
                        "home_goals": rec["home_points"],
                        "away_goals": rec["away_points"],
                        "status": rec["status"],
                        "start_time": rec["start_time"],
                        "extra": {"headline": headline},
                    })
            day += timedelta(days=1)

        self._bracket_games_cache = out
        return out
