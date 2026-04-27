"""NCAA Football source — uses CollegeFootballData.com API.

Free tier rate limits are generous (1k req/day). We make 3 calls per refresh:
  1) /rankings — current AP Top-25
  2) /games   — schedule for the upcoming week
  3) /lines   — betting lines for those games (Phase 3 spread signal)

During offseason (Feb-Aug) /games returns no upcoming results; fetch_upcoming
returns []. Caller logs and skips.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

from .base import GameRow, SportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaaf")

CFBD_BASE = "https://api.collegefootballdata.com"


class NcaafSource(SportSource):
    sport_prefix = "CFB"
    sport_label = "NCAA Football"

    def __init__(self, api_key: str, poll_name: str = "AP Top 25"):
        self.api_key = api_key
        self.poll_name = poll_name
        self._headers = {"Authorization": f"Bearer {api_key}"}

    # ---------- public ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            logger.warning("[ncaaf] no CFBD API key configured; returning []")
            return []

        season_year = self._current_season_year()
        rankings = self._fetch_rankings(season_year)
        if rankings is None:
            # Offseason — no current poll available. Bail without erroring.
            logger.info("[ncaaf] no current rankings available (offseason?); skipping")
            return []

        games = self._fetch_games(season_year, days_ahead)
        if not games:
            logger.info("[ncaaf] no upcoming games in the next %d days", days_ahead)
            return []

        rank_by_team = {r["school"]: r["rank"] for r in rankings}
        spread_by_id = self._fetch_spreads(season_year, games)

        rows: List[GameRow] = []
        for g in games:
            # CFBD uses camelCase: homeTeam, awayTeam, startDate, neutralSite, etc.
            home = g.get("homeTeam") or g.get("home_team")
            away = g.get("awayTeam") or g.get("away_team")
            if not home or not away:
                continue
            start = parse_iso_utc(g.get("startDate"))
            if start is None:
                continue
            rows.append(
                GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=rank_by_team.get(home),
                    rank_away=rank_by_team.get(away),
                    start_time=start,
                    venue=g.get("venue"),
                    spread=spread_by_id.get(g.get("id")),
                    extra={
                        "cfbd_id": g.get("id"),
                        "week": g.get("week"),
                        "season": g.get("season"),
                        "neutral": g.get("neutralSite", False),
                        "conference_game": g.get("conferenceGame", False),
                        "excitement_index": g.get("excitementIndex"),  # Phase 3 backfill signal
                    },
                )
            )
        return rows

    # ---------- private ----------

    @staticmethod
    def _current_season_year() -> int:
        # NCAAF season runs Aug-Jan, so before August use the prior year.
        now = datetime.now(timezone.utc)
        return now.year if now.month >= 8 else now.year - 1

    def _fetch_rankings(self, year: int) -> Optional[List[Dict]]:
        """Returns the latest available AP Top-25 ranks for the season.
        None if no poll data exists yet (e.g., preseason before week 1)."""
        try:
            r = requests.get(
                f"{CFBD_BASE}/rankings",
                headers=self._headers,
                params={"year": year},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error("[ncaaf] /rankings failed: %s", e)
            return None
        if not data:
            return None
        # data is a list of weekly poll snapshots; we want the latest
        latest = data[-1]
        polls = latest.get("polls", [])
        for p in polls:
            if p.get("poll") == self.poll_name:
                return p.get("ranks", [])
        return None

    def _fetch_games(self, year: int, days_ahead: int) -> List[Dict]:
        """Get games scheduled in the upcoming days_ahead window."""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)
        # Fetch the current week and next week, then filter.
        # CFBD lets us query by week; figuring out "current week" is annoying,
        # so we just pull the whole regular season's games and filter.
        try:
            r = requests.get(
                f"{CFBD_BASE}/games",
                headers=self._headers,
                params={"year": year, "seasonType": "regular"},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error("[ncaaf] /games failed: %s", e)
            return []

        upcoming: List[Dict] = []
        for g in data:
            start = parse_iso_utc(g.get("startDate"))
            if start is None:
                continue
            if now <= start <= cutoff:
                upcoming.append(g)
        return upcoming

    def _fetch_spreads(self, year: int, games: List[Dict]) -> Dict[int, float]:
        """Return {game_id: absolute spread}. Empty if /lines fails."""
        if not games:
            return {}
        weeks = {g.get("week") for g in games if g.get("week") is not None}
        out: Dict[int, float] = {}
        for week in weeks:
            try:
                r = requests.get(
                    f"{CFBD_BASE}/lines",
                    headers=self._headers,
                    params={"year": year, "week": week, "seasonType": "regular"},
                    timeout=15,
                )
                r.raise_for_status()
                lines = r.json()
            except Exception as e:
                logger.warning("[ncaaf] /lines week %s failed: %s", week, e)
                continue
            for entry in lines:
                gid = entry.get("id")
                line_list = entry.get("lines", [])
                # Take the consensus line if available, else first.
                consensus = next((l for l in line_list if l.get("provider") == "consensus"), None)
                line = consensus or (line_list[0] if line_list else None)
                if line is None:
                    continue
                spread = line.get("spread")
                if spread is None:
                    continue
                try:
                    out[gid] = abs(float(spread))
                except (TypeError, ValueError):
                    continue
        return out
