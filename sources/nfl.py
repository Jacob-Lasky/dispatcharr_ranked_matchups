"""NFL source: ESPN's unofficial site.api.espn.com. No API key required.

Two source classes:
  - `NflRegularSource(PointsBasedSportSource)`: 17-game season since
    2021 (was 16 before). Raw wins as the threshold field; bands at
    7 / 9 / 11 / 13 wins.
  - `NflPlayoffSource(BestOfNSeriesSource)`: single-game elimination
    bracket, SERIES_LENGTH=1 per stage. Stages: WC -> DIV -> CONF ->
    SB. Same architectural shape as the NCAA March Madness bracket.

API patterns:
  - /scoreboard?dates=YYYYMMDD for per-day games.
  - Tournament stage in competition.notes[0].headline:
      "AFC Wild Card Playoffs" / "NFC Wild Card Playoffs" -> WC
      "AFC Divisional Playoffs" / "NFC Divisional Playoffs" -> DIV
      "AFC Championship" / "NFC Championship" -> CONF
      "Super Bowl LIX" (Roman numerals vary) -> SB
  - No "Game N" suffix on NFL headlines because each round is a
    single game. Headline parser maps headline to stage only;
    matchday is always 1.
  - All-Star / Pro Bowl filter: `competition.type.abbreviation
    == "ALLSTAR"` (same NBA/WNBA trap).

Team names use ESPN's `team.displayName` (e.g., "Philadelphia
Eagles", "Kansas City Chiefs"), which is what EPG providers use.
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

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.nfl")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"

# Modern NFL averages ~22 points per team per game. Used as the prior
# for teams the simulator hasn't seen yet (early-season imports).
_DEFAULT_POINTS_FOR = 22.0
_DEFAULT_POINTS_AGAINST = 22.0

# NFL regular season runs early-September through early-January; playoffs
# January through early-February. ESPN tags `season.year` by the FINAL
# calendar year of the season (e.g., "2025" for the 2024 season: the
# Super Bowl was in February 2025).
SEASON_START_MONTH = 9   # September
PLAYOFF_END_MONTH = 2    # February (Super Bowl)


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[nfl] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[nfl] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    name = (team_obj.get("displayName") or "").strip()
    if name:
        return name
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _default_season_end_year() -> int:
    """ESPN's `season.year` is the calendar year the Super Bowl is
    played in. The 2024 season -> 2025. Pre-September treats the
    current calendar year's season as already concluding."""
    now = datetime.now(timezone.utc)
    return now.year + 1 if now.month >= SEASON_START_MONTH else now.year


# Headline parser. NFL exposes the playoff stage in `notes[0].headline`.
# Patterns observed against 2024 postseason (ESPN's live API):
#   "AFC Wild Card Playoffs" / "NFC Wild Card Playoffs"     -> WC
#   "AFC Divisional Playoffs" / "NFC Divisional Playoffs"   -> DIV
#   "AFC Championship" / "NFC Championship"                 -> CONF
#   "Super Bowl LIX" (the Roman numeral varies year-to-year) -> SB
_HEADLINE_RE = re.compile(
    r"^(?:"
    r"(?P<conf>AFC|NFC)\s+(?P<round>Wild\s+Card\s+Playoffs|Divisional\s+Playoffs|Championship)"
    r"|Super\s+Bowl(?:\s+[IVXLCDM]+)?"
    r")",
    re.IGNORECASE,
)

_ROUND_TO_STAGE: Dict[str, str] = {
    "wild card playoffs":  "WC",
    "divisional playoffs": "DIV",
    "championship":        "CONF",
}


def _parse_stage_from_headline(headline: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return {"stage": str, "matchday": 1}, or None."""
    if not headline:
        return None
    m = _HEADLINE_RE.search(headline)
    if not m:
        return None
    # Super Bowl: regex matches the "Super Bowl" branch with no `conf`
    # / `round` group filled in.
    if not m.group("round"):
        return {"stage": "SB", "matchday": 1}
    round_key = re.sub(r"\s+", " ", m.group("round").lower().strip())
    stage = _ROUND_TO_STAGE.get(round_key)
    if stage is None:
        return None
    return {"stage": stage, "matchday": 1}


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical record.
    Filters out the Pro Bowl (tagged competition.type.abbreviation
    == "ALLSTAR": same NBA/WNBA trap)."""
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
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
        "season_type": ((event.get("season") or {}).get("type")),
        "notes": comp.get("notes") or [],
    }


# =====================================================================
# NflRegularSource
# =====================================================================


class NflRegularSource(PointsBasedSportSource):
    """NFL regular-season importance via PointsBasedSportSource.

    Uses raw `wins` (LEAGUE_CONTEXTS["NFL"] is format="win_count"). NFL
    has 17 games per team since 2021: the 7th playoff seed cutoff has
    settled around 9-10 wins. Bands tuned to the modern 17-game era;
    pre-2021 seasons would have lower cutoffs but the curator only
    surfaces current games anyway.
    """

    league_context_code = "NFL"
    _count_field = "wins"
    _DEFAULT_POINTS_FOR = _DEFAULT_POINTS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_POINTS_AGAINST

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        super().__init__()
        self.season_end_year = season_end_year or _default_season_end_year()

    @property
    def sport_prefix(self) -> str:
        return "NFL"

    @property
    def sport_label(self) -> str:
        return "NFL"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep for the next N days (regular season
        only, season.type==2)."""
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
                if rec is None or rec.get("season_type") != 2:
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
                        "nfl_game_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk every day of the regular season (early-September through
        early-January of the next calendar year). Filter to
        season.type == 2 so playoffs don't leak into the win-count
        importance signal: the playoff source covers that separately.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        # NFL season starts early-September of (end_year - 1).
        season_start = datetime(
            self.season_end_year - 1, SEASON_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        # Regular season ends first week of January. Pad to Jan 15.
        regular_end = datetime(self.season_end_year, 1, 15, tzinfo=timezone.utc).date()
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
                        # Skip preseason (type=1), Pro Bowl, postseason.
                        continue
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = {
                        k: v for k, v in rec.items()
                        if k not in ("season_type", "notes")
                    }
            day += timedelta(days=1)
        return list(seen.values())


# =====================================================================
# NflPlayoffSource
# =====================================================================


class NflPlayoffSource(BestOfNSeriesSource):
    """NFL playoffs as single-game-elim BestOfNSeriesSource (same shape
    as March Madness, just with 4 rounds instead of 6).

    Stages: WC -> DIV -> CONF -> SB. Each "series" is one game;
    clinching wins == 1. Cross-conference until the Super Bowl:
    AFC and NFC ladders feed independently until the SB pairs the
    two conference winners.
    """

    KO_STAGES = ("WC", "DIV", "CONF", "SB")
    SERIES_LENGTH = 1
    supports_importance = True

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        self.season_end_year = season_end_year or _default_season_end_year()
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "NFL"

    @property
    def sport_label(self) -> str:
        return "NFL Playoffs"

    def _league_context_code(self) -> str:
        return "NFL_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        if stage == "SB":
            return "SB_WINNER"
        return None

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
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
                        "nfl_game_id": eid,
                        "fd_competition_code": self._league_context_code(),
                    },
                ))
        return out

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
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

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the postseason window: January 1 through mid-February
        of season_end_year (Super Bowl is typically the first Sunday
        of February but moves with the schedule)."""
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        seen_ids: set = set()
        start = datetime(self.season_end_year, 1, 1, tzinfo=timezone.utc).date()
        end = datetime(self.season_end_year, 2, 28, tzinfo=timezone.utc).date()
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
                    headline = None
                    notes = rec.get("notes") or []
                    if notes:
                        headline = (notes[0] or {}).get("headline")
                    parsed = _parse_stage_from_headline(headline)
                    if parsed is None:
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
