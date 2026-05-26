"""NCAA Women's Basketball source: ESPN's unofficial site.api.espn.com.
No API key required.

Two source classes:
  - `NcaawBasketballRegularSource(PointsBasedSportSource)`: regular
    season win-count importance. AP Top-25 ranks attached to upcoming
    games via the rankings endpoint.
  - `NcaawBasketballPlayoffSource(BestOfNSeriesSource)`: March Madness
    bracket as single-game elim (SERIES_LENGTH=1 per stage, clinches
    at ceil(1/2)=1 win). Stages: R64 -> R32 -> S16 -> E8 -> F4 -> NCG.
    First Four play-in games are NOT modeled (they're structurally
    before R64 and only feed 4 of the 64 R64 slots).

API patterns:
  - /scoreboard?dates=YYYYMMDD for per-day games (range syntax caps at
    25 events).
  - /rankings exposes AP Top 25 + Coaches Poll. Prefer AP.
  - Tournament stage in competition.notes[0].headline, e.g.
      "NCAA Women's Championship - Regional 2 in Birmingham - 1st Round"
      "NCAA Women's Championship - Regional 2 in Birmingham - Sweet 16"
      "NCAA Women's Championship - Final Four"
      "NCAA Women's Championship - National Championship"

Team names use ESPN's `team.location` ("UConn", "South Carolina")
which matches EPG provider titles better than the full displayName
("UConn Huskies").
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

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaaw_basketball")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball"

# Modern D1 women's basketball averages ~72 points per team per game.
_DEFAULT_POINTS_FOR = 72.0
_DEFAULT_POINTS_AGAINST = 72.0

# Season runs early-November through early-April (regular ends ~mid-March,
# tournament early April). Window for per-day iteration.
SEASON_START_MONTH = 11  # November
SEASON_END_MONTH = 4     # April (tournament)


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaaw_basketball] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaaw_basketball] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Use `team.location` (school name) which is what NCAA EPG entries
    use (e.g., 'UConn at South Carolina', not 'UConn Huskies at South
    Carolina Gamecocks')."""
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _default_season_end_year() -> int:
    """Season runs Nov YYYY through April YYYY+1. The "season year" is
    the calendar year the tournament ends in (matching ESPN's convention
    for tournaments: the 2024-25 season ends with the March 2025 NCG)."""
    now = datetime.now(timezone.utc)
    # Pre-November: still in previous season's offseason.
    return now.year + 1 if now.month >= SEASON_START_MONTH else now.year


# Tournament-stage regex. Matches the round suffix on ESPN's headline.
# The "Regional N in {City}" prefix only appears for R64..E8; F4 and NCG
# have no regional prefix. Both formats handled here.
_HEADLINE_RE = re.compile(
    r"NCAA\s+Women'?s?\s+Championship\s*-\s*"
    r"(?:Regional\s+\d+\s+in\s+[^-]+\s*-\s*)?"
    r"(?P<round>1st\s+Round|2nd\s+Round|Sweet\s*16|Elite\s*8|Final\s+Four|National\s+Championship)",
    re.IGNORECASE,
)

_ROUND_TO_STAGE: Dict[str, str] = {
    "1st round":             "R64",
    "2nd round":             "R32",
    "sweet 16":              "S16",
    "elite 8":               "E8",
    "final four":            "F4",
    "national championship": "NCG",
}


def _parse_stage_from_headline(headline: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return {"stage": str, "matchday": 1}, or None.

    Single-elimination, so matchday is always 1 (each "tie" is one
    game; BestOfNSeriesSource resolves at ceil(1/2)=1 win)."""
    if not headline:
        return None
    m = _HEADLINE_RE.search(headline)
    if not m:
        return None
    round_key = re.sub(r"\s+", " ", m.group("round").lower().strip())
    # Normalize "sweet16" / "elite8" variants (sometimes ESPN drops the space).
    round_key = round_key.replace("sweet16", "sweet 16").replace("elite8", "elite 8")
    stage = _ROUND_TO_STAGE.get(round_key)
    if stage is None:
        return None
    return {"stage": stage, "matchday": 1}


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
    # No All-Star filter needed for NCAA: there's no All-Star Game in
    # college basketball. competition.type.abbreviation is always STD
    # for regular season, and the tournament uses headline-based
    # stage routing.
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
# NcaawBasketballRegularSource
# =====================================================================


class NcaawBasketballRegularSource(PointsBasedSportSource):
    """D1 Women's Basketball regular season via ESPN.

    Win-count importance bands tuned against historical NCAA Tournament
    selection criteria (64-team field). 20-win lock for at-large is a
    soft baseline; 25+ tends to anchor a top-4 seed line.
    """

    league_context_code = "NCAAW_BBALL"
    _DEFAULT_POINTS_FOR = _DEFAULT_POINTS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_POINTS_AGAINST

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        super().__init__()
        self.season_end_year = season_end_year or _default_season_end_year()

    @property
    def sport_prefix(self) -> str:
        return "NCAAW"

    @property
    def sport_label(self) -> str:
        return "NCAA Women's Basketball"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep + AP Top 25 attached as ranks."""
        ranks = self._fetch_rankings()
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
                # Both regular-season AND tournament games surface here.
                # Filtering by season.type isn't strictly necessary: we
                # surface postseason games in fetch_upcoming so the user's
                # guide picks them up; the playoff source covers the
                # importance-cascade side separately.
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                start = rec.get("start_time")
                if start is None:
                    continue
                rank_home = ranks.get(rec["home"])
                rank_away = ranks.get(rec["away"])
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=rec["home"],
                    away=rec["away"],
                    rank_home=rank_home,
                    rank_away=rank_away,
                    start_time=start,
                    extra={
                        "ncaaw_game_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    def _fetch_rankings(self) -> Dict[str, int]:
        """Return {team_location: rank} from ESPN's AP poll. Empty
        dict if missing: rank-pair signal sits out the cycle."""
        data = _http_get(f"{ESPN_BASE}/rankings")
        if not data:
            return {}
        ranks_by_team: Dict[str, int] = {}
        polls = data.get("rankings") or []
        if not polls:
            return ranks_by_team
        # Prefer AP Top 25; fall back to first poll otherwise.
        poll = next(
            (p for p in polls if "ap" in (p.get("shortName") or "").lower()),
            polls[0],
        )
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

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk every day from season start through min(today + 7d,
        regular season end). Filter to regular-season games only
        (season.type == 2) so the tournament doesn't pollute the
        win-count importance signal: the playoff source has its own
        bracket-based importance."""
        seen: Dict[Any, Dict[str, Any]] = {}
        # Season starts early-November of (end_year - 1).
        season_start = datetime(
            self.season_end_year - 1, SEASON_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        # Regular season ends mid-March of end_year (around the start of
        # the tournament). Pad to March 31.
        regular_end = datetime(self.season_end_year, 3, 31, tzinfo=timezone.utc).date()
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
# NcaawBasketballPlayoffSource (March Madness)
# =====================================================================


class NcaawBasketballPlayoffSource(BestOfNSeriesSource):
    """NCAA Women's March Madness as a single-game-elim
    BestOfNSeriesSource with SERIES_LENGTH=1 per stage.

    Each "series" is one game; first-to-1-win clinches. The bracket
    state machine handles round_reached cascade as long as every
    stage's series_length is 1 (so clinching_wins == 1).
    """

    KO_STAGES = ("R64", "R32", "S16", "E8", "F4", "NCG")
    SERIES_LENGTH = 1   # uniform single-game elim
    supports_importance = True

    def __init__(self, season_end_year: Optional[int] = None) -> None:
        self.season_end_year = season_end_year or _default_season_end_year()
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAAW"

    @property
    def sport_label(self) -> str:
        return "NCAA Women's Tournament"

    def _league_context_code(self) -> str:
        return "NCAAW_BBALL_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        if stage == "NCG":
            return "NCG_WINNER"
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
                        "ncaaw_game_id": eid,
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
        """Pull the March Madness window (mid-March through early-April
        of season_end_year). Stage routing via headline regex."""
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        seen_ids: set = set()
        # Tournament window: March 15 - April 15 of season_end_year.
        start = datetime(self.season_end_year, 3, 15, tzinfo=timezone.utc).date()
        end = datetime(self.season_end_year, 4, 15, tzinfo=timezone.utc).date()
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
