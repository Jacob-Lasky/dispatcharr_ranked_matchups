"""NCAA Softball source — ESPN's unofficial site.api.espn.com.
No API key required.

V1 (Phase N) covers the regular season only. The Women's College
World Series bracket is double-elimination (4 super-regional brackets
of 8 teams each, then the 8-team WCWS bracket with double-elim
through the semis, then a best-of-3 Championship Series). Neither
the BestOfNSeriesSource nor AggregateLegSource tie shapes cleanly
models double-elim — filed as a Phase N follow-up.

API path: ESPN groups college softball under the `baseball` sport
namespace (NOT `softball`). The scoreboard endpoint:
  /apis/site/v2/sports/baseball/college-softball/scoreboard?dates=YYYYMMDD

Rankings poll: ESPN exposes the ESPN.com/USA Softball Collegiate
Top 25 (the canonical D1 softball poll). Used for the rank-pair
signal on upcoming matchups.

Per-day iteration is required — ESPN's range syntax silently caps
at 25 events. D1 softball can produce 60+ games on a Saturday
during peak season; per-day single-date queries return all of
them.

Team-name canonicalization uses ESPN's `team.location` (school name:
"Oklahoma", "Texas") rather than the mascot ("Sooners", "Longhorns")
because EPG entries use the school name.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_softball")

# ESPN groups college softball under the baseball sport namespace.
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-softball"

# D1 softball season runs February through early-June (WCWS Finals
# typically first week of June). Per-day window covers regular season.
SEASON_START_MONTH = 2   # February
SEASON_END_MONTH = 6     # June (WCWS Finals)

# D1 softball averages ~5 runs/team/game across the full season.
# Used as the prior for teams the simulator hasn't seen yet.
_DEFAULT_RUNS_FOR = 5.0
_DEFAULT_RUNS_AGAINST = 5.0


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """ESPN unofficial wrapper. Returns parsed JSON or None on any
    failure. Logs at WARNING so silent degradation is observable."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_softball] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_softball] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN gives `team.location` (school: 'Oklahoma') and
    `team.name` (mascot: 'Sooners'). EPG provider titles use the
    school name."""
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical
    PointsBased game record. Returns None for unscoreable events.
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
        "extra": {},
    }


class NcaaSoftballSource(PointsBasedSportSource):
    """D1 NCAA Softball regular-season importance.

    Win-count threshold bands tuned against historical NCAA Tournament
    selection criteria (64-team field — same field size as baseball).
    The selection committee weights RPI + strength-of-schedule + wins,
    but win-count alone is a strong proxy for tournament status.
    """

    league_context_code = "SBL"
    _DEFAULT_POINTS_FOR = _DEFAULT_RUNS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_RUNS_AGAINST

    def __init__(self, season_year: Optional[int] = None) -> None:
        super().__init__()
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year
            if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )

    @property
    def sport_prefix(self) -> str:
        return "NCAASBL"

    @property
    def sport_label(self) -> str:
        return "NCAA Softball"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep. Attaches AP-style softball rank
        from the rankings endpoint when available."""
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
                rec = _extract_game_record(event)
                if rec is None:
                    continue
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                start = rec.get("start_time")
                if start is None:
                    continue
                rank_home = rankings.get(rec["home"])
                rank_away = rankings.get(rec["away"])
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=rec["home"],
                    away=rec["away"],
                    rank_home=rank_home,
                    rank_away=rank_away,
                    start_time=start,
                    extra={
                        "ncaasbl_game_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    def _fetch_rankings(self) -> Dict[str, int]:
        """Return {school: rank} from ESPN's softball poll. Empty
        dict if missing."""
        data = _http_get(f"{ESPN_BASE}/rankings")
        if not data:
            return {}
        ranks_by_team: Dict[str, int] = {}
        polls = data.get("rankings") or []
        if not polls:
            return ranks_by_team
        # Softball has one canonical poll: ESPN.com/USA Softball Collegiate
        # Top 25. Take the first poll entry; if ESPN ever adds more we'd
        # pick the official one explicitly.
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

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Per-day iteration from season start (Feb 1) through
        min(today + 7d, end of season). Same trap as ncaa_baseball.py:
        ESPN range syntax caps at 25 events, daily queries don't.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1,
                                tzinfo=timezone.utc).date()
        # End of season = end of June (covers WCWS Finals).
        season_end_first = datetime(self.season_year, SEASON_END_MONTH, 1,
                                    tzinfo=timezone.utc).date()
        season_end = (season_end_first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        now = datetime.now(timezone.utc).date()
        end = min(now + timedelta(days=7), season_end)
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
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = rec
            day += timedelta(days=1)
        return list(seen.values())
