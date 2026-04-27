"""Shared base for the CollegeFootballData / CollegeBasketballData APIs.

Both APIs (run by the same author) share:
  - Bearer-token auth
  - Endpoints: /rankings, /games, /lines
  - JSON shape (camelCase: homeTeam, awayTeam, startDate, neutralSite,
    excitementIndex, conferenceGame)
  - Free tier (1k req/day)

Subclasses set five class-level attributes — see NcaafSource and NcaamSource
for examples — and the SportSource interface is satisfied automatically.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

from .base import GameRow, SportSource
from .._util import parse_iso_utc


class CollegeDataSource(SportSource):
    """CFBD-pattern adapter. Subclass and set the five constants below."""

    # Subclasses MUST set these as class attributes.
    api_base: str = ""             # e.g. "https://api.collegefootballdata.com"
    sport_prefix: str = ""         # e.g. "CFB" — channel-name prefix
    sport_label: str = ""          # e.g. "NCAA Football"
    poll_name: str = "AP Top 25"   # which poll to read from /rankings
    season_pivot_month: int = 8    # months below this use prior calendar year

    def __init__(self, api_key: str, poll_name: Optional[str] = None):
        if not self.api_base:
            raise NotImplementedError(
                f"{type(self).__name__} must set api_base / sport_prefix / sport_label"
            )
        self.api_key = api_key
        if poll_name:
            self.poll_name = poll_name
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._logger = logging.getLogger(
            f"plugins.dispatcharr_ranked_matchups.{self.sport_prefix.lower()}"
        )

    # ---------- public ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            self._logger.warning("[%s] no API key configured; returning []",
                                 self.sport_prefix.lower())
            return []

        season_year = self._current_season_year()
        rankings = self._fetch_rankings(season_year)
        if rankings is None:
            self._logger.info("[%s] no current rankings (offseason?); skipping",
                              self.sport_prefix.lower())
            return []

        games = self._fetch_games(season_year, days_ahead)
        if not games:
            self._logger.info("[%s] no upcoming games in next %d days",
                              self.sport_prefix.lower(), days_ahead)
            return []

        rank_by_team = {r["school"]: r["rank"] for r in rankings}
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

    # ---------- private ----------

    def _current_season_year(self) -> int:
        """A college season runs across two calendar years. Before
        season_pivot_month we're inside the back half of the previous year's
        season. NCAAF: pivot=8 (Aug). NCAAM: pivot=11 (Nov)."""
        now = datetime.now(timezone.utc)
        return now.year if now.month >= self.season_pivot_month else now.year - 1

    def _fetch_rankings(self, year: int) -> Optional[List[Dict]]:
        """Latest available poll snapshot for the season. None if no poll has
        been published yet (preseason)."""
        try:
            r = requests.get(
                f"{self.api_base}/rankings",
                headers=self._headers,
                params={"year": year},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            self._logger.error("[%s] /rankings failed: %s", self.sport_prefix.lower(), e)
            return None
        if not data:
            return None
        latest = data[-1]
        for p in latest.get("polls", []):
            if p.get("poll") == self.poll_name:
                return p.get("ranks", [])
        return None

    def _fetch_games(self, year: int, days_ahead: int) -> List[Dict]:
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)
        try:
            r = requests.get(
                f"{self.api_base}/games",
                headers=self._headers,
                params={"year": year, "seasonType": "regular"},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            self._logger.error("[%s] /games failed: %s", self.sport_prefix.lower(), e)
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
                    f"{self.api_base}/lines",
                    headers=self._headers,
                    params={"year": year, "week": week, "seasonType": "regular"},
                    timeout=15,
                )
                r.raise_for_status()
                lines = r.json()
            except Exception as e:
                self._logger.warning("[%s] /lines week %s failed: %s",
                                     self.sport_prefix.lower(), week, e)
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
