"""European football (soccer) source — Football-Data.org for fixtures + standings,
The Odds API for spreads.

Free tier coverage on Football-Data.org includes:
  - PL  (Premier League)
  - ELC (English Football League Championship — i.e., the second tier)
  - CL  (UEFA Champions League)
  + Bundesliga, La Liga, Serie A, Ligue 1, etc. — easy to add later.

Soccer rank: there's no AP poll, so we use **league position** as the rank.
Wrexham 6th, Hull 7th → "6v7" matchup, both fighting for the playoff spot.

For UCL (knockout / group stage), there's no single league position — instead
we'd use group standings. For v1 we leave UCL ranks unset (None) and let the
favorites + odds signals carry it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

from .base import GameRow, SportSource
from .._util import parse_iso_utc
from ..scoring import TEAM_SUFFIX_TOKENS

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.soccer")

FD_BASE = "https://api.football-data.org/v4"
ODDS_BASE = "https://api.the-odds-api.com/v4"

# When the current season's median playedGames falls below this number,
# replace the current standings with the previous season's final table as
# a "position prior." Without the seed, MD1-3 produces tied scores (no
# rank_pair signal, no stakes signal) and the algorithm has nothing but
# favorites + spread to work with. See TUNING_REPORT.md finding #2.
# DO NOT raise this above ~5 — by MD5 enough matches have played that
# the current-season position is a stronger signal than the prior year's.
# Public (no underscore) because the sim harness imports it to mirror
# this exact cutoff in its standings_as_of replay; a divergence would
# desync the sim's MD1-3 output from production.
SEED_PLAYED_THRESHOLD = 5


def _previous_season_start_year(now: Optional[datetime] = None) -> int:
    """Return the FD.org season-start year for the season BEFORE the
    current one. FD.org's `season` parameter is the start-year:
    `season=2024` is the 2024-25 season.

    Soccer seasons start in August. Aug-Dec → current season started
    this calendar year; Jan-Jul → current season started last year.
    Previous-season is whichever current-minus-one resolves to.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    current_start = now.year if now.month >= 8 else now.year - 1
    return current_start - 1


@dataclass
class SoccerCompetitionConfig:
    fd_code: str        # Football-Data.org competition code (e.g., "PL", "ELC")
    sport_prefix: str   # Channel-name prefix (e.g., "EPL", "EFL")
    sport_label: str    # Human label
    odds_sport_key: Optional[str] = None  # The Odds API sport key
    use_position_as_rank: bool = True     # League position → rank slot
    rank_cap: int = 25  # Treat positions > rank_cap as unranked (None) so they
                        # don't dominate the rank-pair signal
    total_matchdays: int = 0  # Season length for "Matchday X of Y" display.
                              # 0 = N/A (knockouts).


# Built-in catalog. Extend as the user opts into more leagues.
COMPETITIONS: Dict[str, SoccerCompetitionConfig] = {
    "epl": SoccerCompetitionConfig(
        fd_code="PL",
        sport_prefix="EPL",
        sport_label="English Premier League",
        odds_sport_key="soccer_epl",
        rank_cap=20,
        total_matchdays=38,
    ),
    "championship": SoccerCompetitionConfig(
        fd_code="ELC",
        sport_prefix="EFL",
        sport_label="EFL Championship",
        odds_sport_key="soccer_efl_champ",
        rank_cap=24,
        total_matchdays=46,
    ),
    "ucl": SoccerCompetitionConfig(
        fd_code="CL",
        sport_prefix="UCL",
        sport_label="UEFA Champions League",
        odds_sport_key="soccer_uefa_champs_league",
        use_position_as_rank=False,  # group/knockout standings don't map cleanly
        # total_matchdays=0 — knockout, not a fixed-length season
    ),
}


class SoccerSource(SportSource):
    """Adapter for one Football-Data.org competition."""

    def __init__(self, config_key: str, fd_api_key: str, odds_api_key: str = ""):
        if config_key not in COMPETITIONS:
            raise ValueError(f"unknown soccer config: {config_key}")
        self.config = COMPETITIONS[config_key]
        self.fd_api_key = fd_api_key
        self.odds_api_key = odds_api_key

    @property
    def sport_prefix(self) -> str:
        return self.config.sport_prefix

    @property
    def sport_label(self) -> str:
        return self.config.sport_label

    # ---------- public ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.fd_api_key:
            logger.warning("[soccer:%s] no Football-Data.org key", self.config.fd_code)
            return []

        position_by_team, table_full = (
            self._fetch_standings_with_seed() if self.config.use_position_as_rank else ({}, [])
        )
        fixtures = self._fetch_fixtures(days_ahead)
        if not fixtures:
            logger.info("[soccer:%s] no upcoming fixtures in next %d days",
                        self.config.fd_code, days_ahead)
            return []

        # Build team-name → spread lookup (best-effort)
        spread_by_pair = self._fetch_spreads()

        # Compute season_progress from standings: avg of (playedGames / total).
        season_progress = self._estimate_season_progress(table_full)

        rows: List[GameRow] = []
        for f in fixtures:
            home = (f.get("homeTeam") or {}).get("name")
            away = (f.get("awayTeam") or {}).get("name")
            if not home or not away:
                continue
            start = parse_iso_utc(f.get("utcDate"))
            if start is None:
                continue
            rh = position_by_team.get(home)
            ra = position_by_team.get(away)
            cap = self.config.rank_cap
            rh_capped = rh if (rh is not None and rh <= cap) else None
            ra_capped = ra if (ra is not None and ra <= cap) else None
            spread = self._lookup_spread(spread_by_pair, home, away)
            rows.append(GameRow(
                sport_prefix=self.config.sport_prefix,
                sport_label=self.config.sport_label,
                home=home, away=away,
                rank_home=rh_capped, rank_away=ra_capped,
                start_time=start,
                venue=(f.get("venue") if isinstance(f.get("venue"), str) else None),
                spread=spread,
                extra={
                    "fd_id": f.get("id"),
                    "matchday": f.get("matchday"),
                    "matchdays_total": self.config.total_matchdays or None,
                    "stage": f.get("stage"),
                    "status": f.get("status"),
                    "raw_position_home": rh,
                    "raw_position_away": ra,
                    "fd_competition_code": self.config.fd_code,
                    "season_progress": season_progress,
                    # Standings is position-based (not poll-based) — the WHY
                    # renderer uses this to skip "both top-N" framing that's
                    # meaningless when every team in the league is already
                    # "ranked" (e.g. all 20 EPL teams are top-25).
                    "rank_source": "standings",
                    # Standings used for impact-on-favorites computation
                    # downstream. Points + played are used to build natural-
                    # language narrative ("City sits #2, 1 spot and 3 pts
                    # ahead of Man United").
                    "standings_table": [
                        {
                            "name": e["name"],
                            "position": e["position"],
                            "points": e.get("points"),
                            "played": e.get("playedGames"),
                        }
                        for e in table_full
                    ],
                },
            ))
        return rows

    @staticmethod
    def _estimate_season_progress(table: List[Dict]) -> float:
        """Approximate fraction of season completed.

        Uses average of (playedGames / total_matchdays). For knockout comps with
        no league table (UCL etc.), falls back to 0 (means: don't apply
        late-season multiplier).
        """
        if not table:
            return 0.0
        played: List[int] = [
            int(t["playedGames"]) for t in table if t.get("playedGames") is not None
        ]
        if not played:
            return 0.0
        # Round-robin home/away → total_md = (n - 1) * 2 for n teams in the table.
        n = len(table)
        total_md = max(1, (n - 1) * 2)
        avg_played = sum(played) / len(played)
        return min(1.0, avg_played / total_md)

    # ---------- standings ----------

    def _fetch_standings(self, season: Optional[int] = None) -> Tuple[Dict[str, int], List[Dict]]:
        """Returns (team_name → position, full_table_with_extra_fields).

        `season` is the start-year (e.g. 2024 = 2024-25 season). None →
        FD.org's default (current season). Pass an explicit year to fetch
        a historical final table (B.2 cold-start seed).
        """
        try:
            r = requests.get(
                f"{FD_BASE}/competitions/{self.config.fd_code}/standings",
                headers={"X-Auth-Token": self.fd_api_key},
                params={"season": season} if season is not None else None,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error("[soccer:%s] standings fetch failed (season=%s): %s",
                         self.config.fd_code, season, e)
            return {}, []
        standings = data.get("standings") or []
        if not standings:
            return {}, []
        table = standings[0].get("table", [])
        out: Dict[str, int] = {}
        rich: List[Dict] = []
        for row in table:
            team = (row.get("team") or {}).get("name")
            pos = row.get("position")
            if team and pos:
                out[team] = pos
                rich.append({
                    "name": team,
                    "position": pos,
                    "points": row.get("points"),
                    "playedGames": row.get("playedGames"),
                })
        return out, rich

    def _fetch_standings_with_seed(self) -> Tuple[Dict[str, int], List[Dict]]:
        """Current season's table if it has matchdays played; otherwise
        replace it entirely with the previous season's final standings as
        a "position prior." Produces sane MD1-3 ranks instead of the
        all-3.99-tie cold start. See TUNING_REPORT.md finding #2.

        Trade-off: teams promoted INTO this league won't appear in the
        seed (they were in a different competition last year) and stay
        unranked through the seed window. That's correct — they have no
        positional signal anyway. The 17 carry-over teams get realistic
        ranks; the 3 promoted teams fall back to the existing
        one-ranked-one-unranked rank-pair path.
        """
        current_by_team, current_table = self._fetch_standings()
        plays = [e.get("playedGames") or 0 for e in current_table]
        median_played = sorted(plays)[len(plays) // 2] if plays else 0
        if median_played >= SEED_PLAYED_THRESHOLD:
            return current_by_team, current_table
        prev_year = _previous_season_start_year()
        seed_by_team, seed_table = self._fetch_standings(season=prev_year)
        if not seed_table:
            return current_by_team, current_table
        # Reset playedGames to 0 — the seed represents a fresh season's
        # prior, not last year's residual. compute_team_stakes uses
        # matches_remaining = total_md - playedGames; we want the full
        # new season ahead, so no race is mathematically locked at MD0.
        # Keep last year's points so the impact-narrative still renders
        # "1 spot and 3 points away" with realistic numbers — gating
        # math is dominated by matches_remaining*3 at this stage anyway.
        fresh_table = [
            {
                "name": r["name"],
                "position": r["position"],
                "points": r.get("points"),
                "playedGames": 0,
            }
            for r in seed_table
        ]
        logger.info(
            "[soccer:%s] using previous-season seed (median_played=%d, season=%s)",
            self.config.fd_code, median_played, prev_year,
        )
        return seed_by_team, fresh_table

    # ---------- fixtures ----------

    def _fetch_fixtures(self, days_ahead: int) -> List[Dict]:
        now = datetime.now(timezone.utc)
        start = now.strftime("%Y-%m-%d")
        end = (now + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        try:
            r = requests.get(
                f"{FD_BASE}/competitions/{self.config.fd_code}/matches",
                headers={"X-Auth-Token": self.fd_api_key},
                params={"dateFrom": start, "dateTo": end},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error("[soccer:%s] fixtures fetch failed: %s",
                         self.config.fd_code, e)
            return []
        return data.get("matches", [])

    # ---------- odds ----------

    def _fetch_spreads(self) -> Dict[Tuple[str, str], float]:
        """Return {(home, away) lower-cased: abs(spread)}.

        The Odds API returns soccer odds with "spreads" market = Asian handicap.
        We pick the consensus or first available bookmaker, take the home team's
        point handicap, and use its absolute value as the closeness signal.
        """
        if not self.odds_api_key or not self.config.odds_sport_key:
            return {}
        try:
            r = requests.get(
                f"{ODDS_BASE}/sports/{self.config.odds_sport_key}/odds/",
                params={
                    "regions": "uk,us,eu",
                    "markets": "spreads",
                    "apiKey": self.odds_api_key,
                    "oddsFormat": "decimal",
                },
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning("[soccer:%s] odds fetch failed: %s",
                           self.config.fd_code, e)
            return {}

        out: Dict[Tuple[str, str], float] = {}
        for ev in data:
            home = (ev.get("home_team") or "").strip().lower()
            away = (ev.get("away_team") or "").strip().lower()
            if not home or not away:
                continue
            books = ev.get("bookmakers") or []
            if not books:
                continue
            spread_val: Optional[float] = None
            for bk in books:
                for mk in bk.get("markets", []):
                    if mk.get("key") != "spreads":
                        continue
                    for outcome in mk.get("outcomes", []):
                        # Outcome name is one of the team names; we want the
                        # home team's point handicap.
                        if outcome.get("name", "").strip().lower() == home:
                            try:
                                spread_val = abs(float(outcome.get("point", 0.0)))
                                break
                            except (TypeError, ValueError):
                                continue
                    if spread_val is not None:
                        break
                if spread_val is not None:
                    break
            if spread_val is not None:
                out[(home, away)] = spread_val
        return out

    @staticmethod
    def _lookup_spread(spread_map: Dict[Tuple[str, str], float],
                       home: str, away: str) -> Optional[float]:
        """Match Football-Data.org team names to The Odds API team names with
        a fuzzy fallback. Football-Data uses 'Wrexham AFC', Odds API uses
        'Wrexham' — strip common suffixes and try lowercase substring."""
        h_lc = home.lower()
        a_lc = away.lower()
        if (h_lc, a_lc) in spread_map:
            return spread_map[(h_lc, a_lc)]
        # Strip the structural club-tag suffixes (FC / AFC / CF / SC) so
        # Football-Data ("Wrexham AFC") and Odds API ("Wrexham") align. Tokens
        # are the shared TEAM_SUFFIX_TOKENS so this stays in sync with the
        # favorite-matcher's notion of "same team after suffix strip".
        def normalize(n: str) -> str:
            n = n.lower().strip()
            for s in TEAM_SUFFIX_TOKENS:
                tag = " " + s
                if n.endswith(tag):
                    n = n[: -len(tag)]
                    break
            return n.strip()
        h_n = normalize(home)
        a_n = normalize(away)
        for (hk, ak), v in spread_map.items():
            if (h_n in hk or hk in h_n) and (a_n in ak or ak in a_n):
                return v
        return None
