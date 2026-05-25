"""NCAA Baseball source — ESPN's unofficial `site.api.espn.com` API.

No API key required. ESPN's API is undocumented but stable enough for
a homelab TV-guide curator. If it ever 404s, `fetch_upcoming` and
`_fetch_full_season_games` return [] and the affected sport silently
drops out of the guide for that refresh cycle — graceful-degrade is
already the contract.

API endpoints used:
  - /apis/site/v2/sports/baseball/college-baseball/scoreboard
    - Daily / date-range scoreboard. `dates=YYYYMMDD` or
      `dates=YYYYMMDD-YYYYMMDD` for ranges. Returns competitions[]
      with competitors[] (2 teams), score, status. Used by both
      fetch_upcoming and _fetch_full_season_games.
  - /apis/site/v2/sports/baseball/college-baseball/rankings
    - D1Baseball.com Top 25 (the canonical D1 poll). Used to populate
      rank_home / rank_away on GameRow records so the rank-pair signal
      fires for marquee matchups.

Team name canonicalization: ESPN returns `team.location` ("UCLA") and
`team.name` ("Bruins"). We use `team.location` because that's what
typically appears in EPG titles ("UCLA at Texas") rather than the
mascot ("Bruins at Longhorns"). The school name is the stable join
key for matching.

CWS bracket is NOT modeled in V1 — the double-elimination structure
(regional / super-regional / CWS bracket / CWS Finals) is a new tie
shape neither AggregateLegSource nor BestOfNSeriesSource handles.
Importance during the postseason falls back to favorite + rank-pair
signals; structural CWS-progression leverage is filed as follow-up.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc

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


class NcaaBaseballSource(PointsBasedSportSource):
    """NCAA Division I baseball regular-season importance.

    Win-count threshold bands tuned against historical NCAA Tournament
    selection criteria. The 64-team field invites roughly the top D1
    teams by a blend of RPI/wins/strength-of-schedule; 35+ wins is the
    rough at-large cutoff line, and 45+ wins put a team in national-seed
    contention.
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
