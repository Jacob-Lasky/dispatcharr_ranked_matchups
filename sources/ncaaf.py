"""NCAA Football source via CollegeFootballData.com.

Free tier: 1k req/day. We make 3 calls per refresh:
  1) /rankings — current AP Top-25 (nested polls→ranks shape)
  2) /games   — schedule for the upcoming window (param: ?year=)
  3) /lines   — betting lines per week

Offseason (Feb-Aug) /games returns no upcoming results; fetch_upcoming
returns []. CFBD identifies a season by its START year (?year=2024 means
the 2024-25 NCAAF season).
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

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            logger.warning("[ncaaf] no CFBD API key configured; returning []")
            return []

        season_year = self._current_season_year()
        rank_by_team = self._fetch_rankings(season_year)
        if rank_by_team is None:
            logger.info("[ncaaf] no current rankings (offseason?); skipping")
            return []

        games = self._fetch_games(season_year, days_ahead)
        if not games:
            logger.info("[ncaaf] no upcoming games in next %d days", days_ahead)
            return []

        spread_by_id = self._fetch_spreads(season_year, games)

        rows: List[GameRow] = []
        for g in games:
            home = g.get("homeTeam") or g.get("home_team")
            away = g.get("awayTeam") or g.get("away_team")
            if not home or not away:
                continue
            start = parse_iso_utc(g.get("startDate"))
            if start is None:
                continue
            rows.append(GameRow(
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
                    "excitement_index": g.get("excitementIndex"),
                },
            ))
        return rows

    @staticmethod
    def _current_season_year() -> int:
        # NCAAF season runs Aug-Jan; CFBD's ?year= is the START year.
        # Before Aug we're in the tail end of the prior year's season.
        now = datetime.now(timezone.utc)
        return now.year if now.month >= 8 else now.year - 1

    def _fetch_rankings(self, year: int) -> Optional[Dict[str, int]]:
        """Return {team_name → rank} for the latest snapshot, or None if no
        poll has been published yet (preseason)."""
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
        latest = data[-1]
        for p in latest.get("polls", []):
            if p.get("poll") == self.poll_name:
                return {r["school"]: r["rank"] for r in p.get("ranks", [])}
        return None

    def _fetch_games(self, year: int, days_ahead: int) -> List[Dict]:
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)
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
