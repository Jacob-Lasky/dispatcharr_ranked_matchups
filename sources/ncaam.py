"""NCAA Men's Basketball source via CollegeBasketballData.com.

Same author and same Bearer-token auth as CollegeFootballData, but the
endpoints' wire shape differs in non-trivial ways (verified against the
live API on 2026-04-27):

  - /rankings is a FLAT list of (team, ranking, pollType, week) tuples,
    not nested under polls[].ranks[]. Field names: `team` not `school`,
    `ranking` not `rank`, `pollType` not `poll`. Filter by season.
  - /games is paginated at 3000 results without offset support. Use
    startDateRange / endDateRange query params instead: they accept any
    YYYY-MM-DD range and return only games in that window. Games have no
    `week` field. Excitement is `excitement` not `excitementIndex`.
  - /lines accepts the same date-range params and is keyed by `gameId`.

These shape differences make sharing code with NcaafSource brittle: each
source implements SportSource directly.

Free tier: same as CFBD (1k req/day, free key from collegebasketballdata.com).
Season runs roughly November → early April (regular season ends, then
March Madness). Calls in May-October return [] cleanly.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, report_lost_day_range
from .points_based import PointsBasedSportSource
from ._espn import upcoming_window
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaam")

CBB_BASE = "https://api.collegebasketballdata.com"


def _completed(game: Dict[str, Any]) -> bool:
    """Whether a CBBD /games row is a finished game. CBBD encodes completion
    via the homeWinner/awayWinner booleans being non-null AND the *Points
    fields being numeric; `status` doesn't always reflect completion, so the
    joint check. A game in progress has points and no winner yet."""
    return (game.get("homePoints") is not None and game.get("awayPoints") is not None
            and (game.get("homeWinner") is not None
                 or game.get("awayWinner") is not None))


class NcaamSource(PointsBasedSportSource):
    league_context_code = "CBB"

    # The AP Top 25 is drawn from Division I (#198): 365 programmes for the
    # 2025-26 season, measured 2026-10-11 from CBBD `/teams?season=2026`.
    # See SportSource.rank_field_size.
    rank_field_size = 365

    # NCAA men's basketball averages ~75 points/game per team. Cold-start
    # fallback for teams without any FINISHED games yet (early November).
    _DEFAULT_POINTS_FOR = 75.0
    _DEFAULT_POINTS_AGAINST = 75.0

    @property
    def sport_prefix(self) -> str:
        return "CBB"

    @property
    def sport_label(self) -> str:
        return "NCAA Men's Basketball"

    def __init__(self, api_key: str, poll_name: str = "AP Top 25"):
        super().__init__()
        self.api_key = api_key
        self.poll_name = poll_name
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            logger.warning("[ncaam] no CBB-Data API key configured; returning []")
            return []

        season_year = self._current_season_year()
        # Raises when the request FAILS: that is an outage the refresh holds
        # the previous games for (#240), where None, no poll published yet,
        # is a real preseason and stays quiet. DO NOT catch it back into
        # None: it was logged as "offseason" and the slate zeroed silently.
        rank_by_team = self._fetch_rankings(season_year, raise_on_failure=True)
        if rank_by_team is None:
            logger.info("[ncaam] no %s poll published for %d yet (preseason / "
                        "offseason); skipping", self.poll_name, season_year)
            return []

        # The window starts at the US-Eastern lookback day, not today-UTC
        # (#256): from 00:00Z an evening-Eastern tip-off sits on yesterday's
        # UTC date, and `startDateRange=<today-UTC>` cannot return it while it
        # is being played. `admits` then drops the finished and the stale.
        window = upcoming_window(days_ahead)
        games = self._fetch_games(window.first_day, window.last_day)
        if games is None:
            # The one request for every day of the window failed: those days
            # are lost, not quiet (#261, see base.report_lost_day_range).
            report_lost_day_range(window.first_day, window.last_day)
            return []
        games = [g for g in games
                 if window.admits(parse_iso_utc(g.get("startDate")), _completed(g))]
        if not games:
            logger.info("[ncaam] no upcoming games in next %d days", days_ahead)
            return []

        spread_by_id = self._fetch_spreads(window.first_day, window.last_day)

        rows: List[GameRow] = []
        for g in games:
            home = g.get("homeTeam")
            away = g.get("awayTeam")
            if not home or not away:
                continue
            start = parse_iso_utc(g.get("startDate"))
            cbb_id = g.get("id")
            spread = spread_by_id.get(cbb_id) if cbb_id is not None else None
            rows.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=rank_by_team.get(home),
                rank_away=rank_by_team.get(away),
                start_time=start,
                venue=g.get("venue"),
                spread=spread,
                extra={
                    "cbb_id": g.get("id"),
                    "season": g.get("season"),
                    "neutral": g.get("neutralSite", False),
                    "conference_game": g.get("conferenceGame", False),
                    "excitement_index": g.get("excitement"),
                    "tournament": g.get("tournament"),
                    # Importance lookup key for LEAGUE_CONTEXTS.
                    "fd_competition_code": self.league_context_code,
                },
            ))
        return rows

    @staticmethod
    def _current_season_year() -> int:
        # CBB's ?season= is the END year of the season (?season=2025 means
        # 2024-25). Pivot at November (when the season opens). In May-Oct we
        # return last season's end-year: /games for that season is in the
        # past so fetch_upcoming filters everything out and returns [].
        now = datetime.now(timezone.utc)
        return now.year + 1 if now.month >= 11 else now.year

    def _fetch_rankings(
        self, season: int, raise_on_failure: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Return {team_name → rank} for the latest week of the configured
        poll. None if no poll has been published yet (preseason, before
        mid-October's preseason AP poll), and None too for a FAILED request
        unless `raise_on_failure`, which `fetch_upcoming` sets so a failure is
        not read as a preseason (see there)."""
        try:
            r = requests.get(
                f"{CBB_BASE}/rankings",
                headers=self._headers,
                params={"season": season},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list) or not all(
                    isinstance(e, dict) and "pollType" in e for e in data):
                # /rankings is a flat JSON list of poll rows, [] before the
                # first poll; anything else (an error object, null, rows
                # without a pollType) is a failure, not a preseason.
                raise ValueError(f"/rankings answered {str(data)[:80]!r}, not a list of poll rows")
        except Exception as e:
            logger.error("[ncaam] /rankings failed: %s", e)
            if raise_on_failure:
                raise
            return None
        if not data:
            return None
        # Flat list: filter by pollType, then take the latest week.
        relevant = [r for r in data if r.get("pollType") == self.poll_name]
        if not relevant:
            return None
        latest_week = max((r.get("week") or 0) for r in relevant)
        latest = [r for r in relevant if (r.get("week") or 0) == latest_week]
        out: Dict[str, int] = {}
        for entry in latest:
            team = entry.get("team")
            rank = entry.get("ranking")
            if team and rank is not None:
                out[team] = int(rank)
        return out or None

    def _fetch_games(self, start: date, end: date) -> Optional[List[Dict]]:
        """Use startDateRange/endDateRange: /games is hard-capped at 3000
        without offset support, so a season-wide call would silently miss
        late-season games.

        None when the request FAILED, distinct from [] (it answered with no
        games): the caller reports the window's days as lost on None.
        """
        try:
            r = requests.get(
                f"{CBB_BASE}/games",
                headers=self._headers,
                params={
                    "startDateRange": start.strftime("%Y-%m-%d"),
                    "endDateRange": end.strftime("%Y-%m-%d"),
                    "seasonType": "regular",
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                # /games answers with a JSON list, empty included; anything
                # else (an error object, null) is not an answer.
                raise ValueError(f"/games answered {type(data).__name__}, not a list")
            return data
        except Exception as e:
            logger.error("[ncaam] /games failed: %s", e)
            return None

    def _fetch_spreads(self, start: date, end: date) -> Dict[int, float]:
        """Same date-range as /games. Returns {gameId: abs(spread)}."""
        out: Dict[int, float] = {}
        try:
            r = requests.get(
                f"{CBB_BASE}/lines",
                headers=self._headers,
                params={
                    "startDateRange": start.strftime("%Y-%m-%d"),
                    "endDateRange": end.strftime("%Y-%m-%d"),
                    "seasonType": "regular",
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning("[ncaam] /lines failed: %s", e)
            return {}
        for entry in data:
            gid = entry.get("gameId")
            if gid is None:
                continue
            line_list = entry.get("lines") or []
            # CBB returns per-bookmaker lines without a "consensus" entry:
            # take the first non-null spread we see (mirrors what the user
            # would see at the top of any sportsbook).
            for line in line_list:
                spread = line.get("spread")
                if spread is None:
                    continue
                try:
                    out[gid] = abs(float(spread))
                    break
                except (TypeError, ValueError):
                    continue
        return out

    # ---------- Monte Carlo importance ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Return the current regular-season game list filtered to games
        involving an AP-ranked team. The full D1 schedule is ~7500 games
       : too many for the Monte Carlo simulator to iterate every refresh
        at acceptable latency. Filtering to AP-relevant games drops it to
        ~700-1000 (each of ~25 AP teams plays ~30 games, with overlap),
        which keeps per-refresh importance cost in the seconds, not
        minutes.

        Known limitation: a non-AP favorite team's games against non-AP
        opponents aren't in the cache, so the simulator misses those
        wins when computing the favorite's win-count outcomes. The
        plugin's `favorites_in_league` mechanism still queries those
        teams but the leverage is biased low.
        """
        if not self.api_key:
            return []
        season = self._current_season_year()
        ranked = self._fetch_rankings(season)
        if not ranked:
            # No AP poll yet (preseason). Importance signal returns 0
            # via terminal_outcomes' empty dict: graceful no-op.
            return []
        ap_teams = set(ranked.keys())

        # Season window: Nov 1 of (season-1) through Apr 30 of season.
        # Three date-range chunks to stay under CBBD's 3000-row cap per call.
        chunks: List[tuple] = [
            (datetime(season - 1, 11, 1, tzinfo=timezone.utc),
             datetime(season - 1, 12, 31, tzinfo=timezone.utc)),
            (datetime(season, 1, 1, tzinfo=timezone.utc),
             datetime(season, 2, 15, tzinfo=timezone.utc)),
            (datetime(season, 2, 16, tzinfo=timezone.utc),
             datetime(season, 4, 30, tzinfo=timezone.utc)),
        ]
        all_games: List[Dict[str, Any]] = []
        for start, end in chunks:
            try:
                r = requests.get(
                    f"{CBB_BASE}/games",
                    headers=self._headers,
                    params={
                        "startDateRange": start.strftime("%Y-%m-%d"),
                        "endDateRange": end.strftime("%Y-%m-%d"),
                        "seasonType": "regular",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                all_games.extend(r.json() or [])
            except Exception as e:
                logger.warning("[ncaam] full-season chunk %s..%s failed: %s",
                               start.date(), end.date(), e)
                continue

        out: List[Dict[str, Any]] = []
        for g in all_games:
            home = g.get("homeTeam")
            away = g.get("awayTeam")
            if not home or not away:
                continue
            if home not in ap_teams and away not in ap_teams:
                continue
            hp = g.get("homePoints")
            ap_pts = g.get("awayPoints")
            completed = _completed(g)
            out.append({
                "id": g.get("id"),
                "home": home,
                "away": away,
                "home_points": hp if completed else None,
                "away_points": ap_pts if completed else None,
                "status": "FINISHED" if completed else "SCHEDULED",
                "start_time": parse_iso_utc(g.get("startDate")),
            })
        return out
