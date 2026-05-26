"""WNBA source: ESPN's unofficial `site.api.espn.com` API. No key required.

Two source classes mirroring the NBA pattern, with WNBA-specific
adjustments:

  - `WnbaRegularSource(PointsBasedSportSource)`: 40-game regular season,
    raw wins as the threshold field. LEAGUE_CONTEXTS["WNBA"] thresholds
    at 20 (playoff bubble: 8 of 13 teams make playoffs) / 25
    (comfortable) / 30 (top-seed pace) / 35 (elite).
  - `WnbaPlayoffSource(BestOfNSeriesSource)`: variable series lengths
    via `_series_length_for_stage`: R1 best-of-3, Semifinals best-of-5,
    Finals best-of-5 (≤2024) or best-of-7 (≥2025 when the WNBA expanded
    the championship series). The stage labels are R1 / SF / FINALS;
    cross-conference seeding (no East/West split in the playoffs since
    2022's reseed format).

API quirks captured here:
  - `season.type`: 2 = regular season, 3 = postseason. Same as NBA.
  - All-Star Tournament games are tagged `competition.type.abbreviation
    == "ALLSTAR"` and filtered out. Same trap as NBA.
  - Headline format differs from NBA: WNBA has no East/West discriminator.
    Patterns: "First Round - Game N" (R1), "WNBA Semifinals - Game N"
    (SF), "WNBA Finals - Game N" (FINALS).
  - Team-name canonicalization: `team.displayName` ("New York Liberty",
    "Minnesota Lynx").

The plugin opts into WNBA via `enable_wnba` in `plugin.json`. Off by
default.
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

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.wnba")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"

# WNBA averages ~82 points per team per game in the modern (post-2020)
# era. Used as the prior for teams the simulator hasn't seen yet
# (expansion teams, early-season imports).
_DEFAULT_POINTS_FOR = 82.0
_DEFAULT_POINTS_AGAINST = 82.0

# WNBA season runs mid-May through mid-October (regular), with playoffs
# in late-September through mid-October (mid-October through November
# for the new 2025+ Finals format). Window: May 1 - Nov 30.
SEASON_START_MONTH = 5   # May
SEASON_END_MONTH = 11    # November (covers expanded 2025+ playoff window)

# 2025+ WNBA Finals are best-of-7; 2024 and earlier were best-of-5.
# Series lengths per stage:
_WNBA_SERIES_LENGTHS_MODERN: Dict[str, int] = {
    "R1": 3,        # best-of-3 since 2022
    "SF": 5,        # best-of-5 since 2022
    "FINALS": 7,    # best-of-7 from 2025
}
_WNBA_SERIES_LENGTHS_LEGACY: Dict[str, int] = {
    "R1": 3,
    "SF": 5,
    "FINALS": 5,    # best-of-5 in 2024 and earlier
}


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """ESPN unofficial API wrapper. Returns parsed JSON or None on
    any error. Logs at WARNING."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[wnba] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[wnba] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN's WNBA displayName is the full City+Mascot form, which is
    what EPG providers use."""
    name = (team_obj.get("displayName") or "").strip()
    if name:
        return name
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _default_season_year() -> int:
    """WNBA seasons are named by the calendar year. Use current year
    in-season; pre-May treat as last season (postseason / off-season)."""
    now = datetime.now(timezone.utc)
    return now.year if now.month >= SEASON_START_MONTH else now.year - 1


# Headline parser. WNBA exposes playoff stage in `competition.notes[0].
# headline`. Unlike NBA, there's no East/West prefix: the league
# uses cross-conference reseeding since 2022.
_HEADLINE_RE = re.compile(
    r"^(?:(?P<round>First\s+Round)|WNBA\s+(?P<wnba>Semifinals|Finals))\s*-\s*Game\s+(?P<game>\d+)",
    re.IGNORECASE,
)

_ROUND_TO_STAGE: Dict[str, str] = {
    "first round": "R1",
    "semifinals":  "SF",
    "finals":      "FINALS",
}


def _parse_stage_from_headline(headline: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return {"stage": str, "matchday": int} on match, or None."""
    if not headline:
        return None
    m = _HEADLINE_RE.search(headline)
    if not m:
        return None
    try:
        matchday = int(m.group("game"))
    except (TypeError, ValueError):
        return None
    if m.group("wnba"):
        round_key = m.group("wnba").lower().strip()
    else:
        round_key = (m.group("round") or "").lower().strip()
    round_key = re.sub(r"\s+", " ", round_key)
    stage = _ROUND_TO_STAGE.get(round_key)
    if stage is None:
        return None
    return {"stage": stage, "matchday": matchday}


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical record.
    Returns None for unscoreable events (cancellations, postponements
    with no competitors) AND for non-bracket exhibition games like
    the WNBA All-Star Game (tagged `competition.type.abbreviation
    == "ALLSTAR"`).
    """
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
# WnbaRegularSource
# =====================================================================


class WnbaRegularSource(PointsBasedSportSource):
    """WNBA regular-season importance via PointsBasedSportSource.

    Uses raw `wins` count (no OT bonus). 40-game season; 8 teams make
    playoffs out of 12-13. Thresholds reflect that cut-line shape: 20
    wins is roughly the 8th seed line, 30 wins is top-seed pace.
    """

    league_context_code = "WNBA"
    _count_field = "wins"
    _DEFAULT_POINTS_FOR = _DEFAULT_POINTS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_POINTS_AGAINST

    def __init__(self, season_year: Optional[int] = None) -> None:
        super().__init__()
        self.season_year = season_year or _default_season_year()

    @property
    def sport_prefix(self) -> str:
        return "WNBA"

    @property
    def sport_label(self) -> str:
        return "WNBA"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep for the next N days. Regular-season
        games only (filter via season.type==2)."""
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
                        "wnba_game_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk every day of the regular season window. WNBA's regular
        season is mid-May through mid-September; this covers May
        through end of September with daily iteration. ESPN's range
        endpoint silently caps at 25 events; daily queries return
        everything for that day."""
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1,
                                tzinfo=timezone.utc).date()
        # Regular season ends ~September 19; pad to Sept 30.
        regular_end = datetime(self.season_year, 9, 30, tzinfo=timezone.utc).date()
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
# WnbaPlayoffSource
# =====================================================================


class WnbaPlayoffSource(BestOfNSeriesSource):
    """WNBA playoffs with per-stage variable series lengths.

    Series lengths (since 2022 reseeding format):
      - R1 (First Round):    best-of-3
      - SF (Semifinals):     best-of-5
      - FINALS:              best-of-5 in 2024 and earlier, best-of-7
                             from 2025. Series length picked from the
                             instance's season_year.
    """

    KO_STAGES = ("R1", "SF", "FINALS")
    SERIES_LENGTH = 7   # uniform fallback; per-stage map below overrides
    supports_importance = True

    def __init__(self, season_year: Optional[int] = None) -> None:
        self.season_year = season_year or _default_season_year()
        # 2025+ uses best-of-7 Finals; 2024 and earlier are best-of-5.
        self._stage_lengths = (
            _WNBA_SERIES_LENGTHS_MODERN
            if self.season_year >= 2025
            else _WNBA_SERIES_LENGTHS_LEGACY
        )
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "WNBA"

    @property
    def sport_label(self) -> str:
        return "WNBA Playoffs"

    def _league_context_code(self) -> str:
        return "WNBA_PO"

    def _series_length_for_stage(self, stage: str) -> int:
        return self._stage_lengths.get(stage, self.SERIES_LENGTH)

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        if stage == "FINALS":
            return "WNBA_WINNER"
        return None

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Next-N-day playoff schedule (season.type==3 filter)."""
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
                        "wnba_game_id": eid,
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
        """Pull the postseason schedule via per-day scoreboard and parse
        the ESPN headline for stage routing. Window: Sept 1 - Oct 31 of
        season_year (covers all historical WNBA playoff windows including
        the expanded 2025+ Finals which can stretch into late October).
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        seen_ids: set = set()
        start = datetime(self.season_year, 9, 1, tzinfo=timezone.utc).date()
        # WNBA Finals can stretch to Nov 1 in the new best-of-7 format.
        end = datetime(self.season_year, 11, 15, tzinfo=timezone.utc).date()
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
