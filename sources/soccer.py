"""European football (soccer) source — Football-Data.org for fixtures + standings,
The Odds API for spreads.

Free tier coverage on Football-Data.org includes:
  - PL  (Premier League)
  - ELC (English Football League Championship — i.e., the second tier)
  - CL  (UEFA Champions League)
  + Bundesliga, La Liga, Serie A, Ligue 1, etc. — easy to add later.

Soccer rank: there's no AP poll, so we use **league position** as the rank.
Wrexham 6th, Hull 7th → "6v7" matchup, both fighting for the playoff spot.

UCL (and other knockout competitions) ship as `KnockoutSoccerSource`, a
subclass that swaps the standings-shaped importance state machine for a
bracket-shaped one. The base SoccerSource class assumes league shape
(points table, integer position thresholds); the knockout subclass tracks
tie aggregates and propagates winners through bracket feeds. Pick the
right class by `LEAGUE_CONTEXTS[fd_code].format`.
"""

from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult, SportSource
from .._util import parse_iso_utc
from ..scoring import KNOCKOUT_ROUND_DEPTH, LEAGUE_CONTEXTS, TEAM_SUFFIX_TOKENS

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


def _h2h_to_closeness(
    outcomes: List[Dict], home_lc: str, away_lc: str
) -> Optional[float]:
    """Convert a bookmaker's h2h outcomes (3-way: home/draw/away decimal
    odds) into a coinflip-ness measure in [0, 1]. Returns None if the
    outcomes don't yield clean numbers.

    Math:
      raw_implied_i = 1 / decimal_odds_i  (per outcome)
      vig           = sum(raw_implied) - 1  (overround)
      p_i           = raw_implied_i / sum(raw_implied)  (devigged)
      closeness     = 2 * min(p_home, p_away)

    The draw probability is intentionally not part of the closeness
    formula — "either team could win" is what we care about, and the
    draw outcome is its own state. A 33/34/33 split → closeness 0.66,
    not 1.0; that's correct (a draw-heavy market isn't a coinflip).
    """
    p_home_raw: Optional[float] = None
    p_away_raw: Optional[float] = None
    total_raw = 0.0
    for o in outcomes:
        try:
            price = float(o.get("price", 0))
        except (TypeError, ValueError):
            continue
        if price <= 1.0:
            continue
        implied = 1.0 / price
        total_raw += implied
        name = (o.get("name") or "").strip().lower()
        if name == home_lc:
            p_home_raw = implied
        elif name == away_lc:
            p_away_raw = implied
    if p_home_raw is None or p_away_raw is None or total_raw <= 0:
        return None
    p_home = p_home_raw / total_raw
    p_away = p_away_raw / total_raw
    return max(0.0, min(1.0, 2.0 * min(p_home, p_away)))


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

    # Phase C: SoccerSource implements the Monte Carlo importance interface.
    # Requires a full-season fixtures fetch (done lazily on first call).
    # UCL etc. set use_position_as_rank=False; they have no league table, so
    # the importance simulator can't run on them. The lazy cache populates
    # only for the configs where LEAGUE_CONTEXTS has thresholds defined.
    supports_importance = True

    # Pre-season fallback per-team strength (rolling-window estimate has no
    # samples yet). 1.4 goals/match home and 1.1 away approximates the EPL
    # historical average and is a reasonable prior for an unsampled team.
    _DEFAULT_STRENGTH_HOME_SCORED = 1.4
    _DEFAULT_STRENGTH_HOME_CONCEDED = 1.1
    _DEFAULT_STRENGTH_AWAY_SCORED = 1.1
    _DEFAULT_STRENGTH_AWAY_CONCEDED = 1.4

    def __init__(self, config_key: str, fd_api_key: str, odds_api_key: str = ""):
        if config_key not in COMPETITIONS:
            raise ValueError(f"unknown soccer config: {config_key}")
        self.config = COMPETITIONS[config_key]
        self.fd_api_key = fd_api_key
        self.odds_api_key = odds_api_key
        # Importance-mode cache (lazy). Populated by _ensure_importance_cache
        # on first call to any of the Phase C methods. None means uninitialized;
        # an empty list / dict means we tried and got nothing back.
        self._all_matches_cache: Optional[List[Dict]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._initial_state_cache: Optional[Dict[str, Any]] = None

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

        # Build team-name → closeness lookup (best-effort). B.3
        # replaced the old Asian-handicap "spreads" market with the
        # devigged h2h moneyline market; downstream we populate
        # GameRow.closeness (not GameRow.spread).
        closeness_by_pair = self._fetch_closeness()

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
            closeness = self._lookup_odds(closeness_by_pair, home, away)
            rows.append(GameRow(
                sport_prefix=self.config.sport_prefix,
                sport_label=self.config.sport_label,
                home=home, away=away,
                rank_home=rh_capped, rank_away=ra_capped,
                start_time=start,
                venue=(f.get("venue") if isinstance(f.get("venue"), str) else None),
                # B.3: soccer populates `closeness` (probability-based);
                # `spread` stays None so the GameRow contract
                # "exactly one of spread/closeness" holds.
                spread=None,
                closeness=closeness,
                extra={
                    "fd_id": f.get("id"),
                    "matchday": f.get("matchday"),
                    "matchdays_total": self.config.total_matchdays or None,
                    "stage": f.get("stage"),
                    "status": f.get("status"),
                    "raw_position_home": rh,
                    "raw_position_away": ra,
                    "fd_competition_code": self.config.fd_code,
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
        # prior, not last year's residual. Downstream consumers
        # (build_impact_narratives, the matcher's rank cap) read these
        # numbers; the importance simulator builds its own standings from
        # the fixture list and doesn't depend on this field. Keep last
        # year's points so the narrative still renders "1 spot and 3
        # points away" with realistic numbers.
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

    def _fetch_closeness(self) -> Dict[Tuple[str, str], float]:
        """Return {(home, away) lower-cased: closeness_in_[0,1]}.

        B.3 / TUNING_REPORT finding #6: replaces the spreads-based
        signal with a principled coinflip-ness measure derived from the
        bookmaker's h2h (moneyline) market. The pipeline:

          decimal_odds → 1/odds (raw implied prob) → devig by dividing
          by total_implied → 2 * min(P_home, P_away) → closeness in [0, 1].

        For soccer's 3-way market (home / draw / away), we use only the
        home and away win probabilities; the draw outcome doesn't change
        the "either team could win" intuition. A perfect coinflip
        (45/10/45) → closeness 0.90. A blowout (80/15/5) → closeness 0.10.

        Pre-B.3 used the "spreads" market (Asian handicap). That signal
        had ~3 raw points of contribution flat across the season; nearly
        zero differentiation. Moneyline-derived closeness restores it.
        """
        if not self.odds_api_key or not self.config.odds_sport_key:
            return {}
        try:
            r = requests.get(
                f"{ODDS_BASE}/sports/{self.config.odds_sport_key}/odds/",
                params={
                    "regions": "uk,us,eu",
                    "markets": "h2h",
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
            closeness_val: Optional[float] = None
            for bk in books:
                for mk in bk.get("markets", []):
                    if mk.get("key") != "h2h":
                        continue
                    closeness_val = _h2h_to_closeness(mk.get("outcomes") or [], home, away)
                    if closeness_val is not None:
                        break
                if closeness_val is not None:
                    break
            if closeness_val is not None:
                out[(home, away)] = closeness_val
        return out

    @staticmethod
    def _lookup_odds(odds_map: Dict[Tuple[str, str], float],
                     home: str, away: str) -> Optional[float]:
        """Match Football-Data.org team names to The Odds API team names with
        a fuzzy fallback. Football-Data uses 'Wrexham AFC', Odds API uses
        'Wrexham' — strip common suffixes and try lowercase substring.
        Generic over whatever value the odds_map carries — pre-B.3 was
        spread floats, post-B.3 is closeness floats."""
        h_lc = home.lower()
        a_lc = away.lower()
        if (h_lc, a_lc) in odds_map:
            return odds_map[(h_lc, a_lc)]
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
        for (hk, ak), v in odds_map.items():
            if (h_n in hk or hk in h_n) and (a_n in ak or ak in a_n):
                return v
        return None

    # ---------- Phase C: Monte Carlo importance ----------

    def _fetch_all_season_matches(self) -> List[Dict]:
        """All matches for the current competition season — finished AND
        scheduled. One FD.org call per refresh. Cached on the instance so
        repeat calls within the same refresh don't re-bill.
        """
        if self._all_matches_cache is not None:
            return self._all_matches_cache
        if not self.fd_api_key:
            self._all_matches_cache = []
            return []
        try:
            r = requests.get(
                f"{FD_BASE}/competitions/{self.config.fd_code}/matches",
                headers={"X-Auth-Token": self.fd_api_key},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning("[soccer:%s] all-matches fetch failed: %s",
                           self.config.fd_code, e)
            self._all_matches_cache = []
            return []
        cache: List[Dict] = data.get("matches", []) or []
        self._all_matches_cache = cache
        return cache

    @property
    def outcome_labels(self) -> List[str]:
        ctx = LEAGUE_CONTEXTS.get(self.config.fd_code)
        if ctx is None:
            return []
        return [label for _, label, _ in ctx.thresholds]

    @staticmethod
    def _is_bottom_outcome(label: str) -> bool:
        """Direction inference for an outcome band. Bottom-side outcomes
        (relegation, demotion) fire on positions worse than the cutoff;
        top-side outcomes (title, UCL, Europa, promotion, playoff) fire on
        positions at or above the cutoff. Detected by label substring.
        """
        l = label.lower()
        return "relegat" in l or "demot" in l or "drop" in l

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team home/away goal averages from finished matches. Lahvička
        uses a 19-match rolling window; we use all FINISHED matches in the
        current season for simplicity (the window matters more for handling
        mid-season form swings; for our per-refresh sim a season-wide avg is
        close enough).

        Returns `{team: {sh, ch, sa, ca}}` where:
          sh = avg goals scored at home
          ch = avg goals conceded at home
          sa = avg goals scored away
          ca = avg goals conceded away

        Teams with zero finished matches (newly promoted, pre-season) get
        league-average defaults via `_DEFAULT_STRENGTH_*`. The pre-season
        seed already handles position-based ranks (Phase B.2); strengths are
        the goals-side fallback.
        """
        if self._strengths_cache is not None:
            return self._strengths_cache
        matches = self._fetch_all_season_matches()
        # Per-team running sums. Use "h" / "a" prefixed totals so a team can
        # have asymmetric home/away form (a common real pattern).
        sums: Dict[str, Dict[str, float]] = {}
        for m in matches:
            if m.get("status") != "FINISHED":
                continue
            ft = (m.get("score") or {}).get("fullTime") or {}
            hg = ft.get("home")
            ag = ft.get("away")
            if hg is None or ag is None:
                continue
            home_name = (m.get("homeTeam") or {}).get("name")
            away_name = (m.get("awayTeam") or {}).get("name")
            if not home_name or not away_name:
                continue
            for name in (home_name, away_name):
                if name not in sums:
                    sums[name] = {
                        "n_home": 0.0, "sh_total": 0.0, "ch_total": 0.0,
                        "n_away": 0.0, "sa_total": 0.0, "ca_total": 0.0,
                    }
            sums[home_name]["n_home"] += 1
            sums[home_name]["sh_total"] += float(hg)
            sums[home_name]["ch_total"] += float(ag)
            sums[away_name]["n_away"] += 1
            sums[away_name]["sa_total"] += float(ag)
            sums[away_name]["ca_total"] += float(hg)
        out: Dict[str, Dict[str, float]] = {}
        for team, s in sums.items():
            # Per-side fallback: a team can have home games but no away
            # games yet (or vice versa) early in the season. Each side
            # falls back to its own default independently.
            if s["n_home"] > 0:
                sh = s["sh_total"] / s["n_home"]
                ch = s["ch_total"] / s["n_home"]
            else:
                sh = self._DEFAULT_STRENGTH_HOME_SCORED
                ch = self._DEFAULT_STRENGTH_HOME_CONCEDED
            if s["n_away"] > 0:
                sa = s["sa_total"] / s["n_away"]
                ca = s["ca_total"] / s["n_away"]
            else:
                sa = self._DEFAULT_STRENGTH_AWAY_SCORED
                ca = self._DEFAULT_STRENGTH_AWAY_CONCEDED
            out[team] = {"sh": sh, "ch": ch, "sa": sa, "ca": ca}
        self._strengths_cache = out
        return out

    def _strength_for(self, strengths: Dict[str, Dict[str, float]], team: str) -> Dict[str, float]:
        """Lookup with fallback. A team in the fixture list but not in
        `strengths` (newly promoted, no finished matches) gets defaults.
        """
        if team in strengths:
            return strengths[team]
        return {
            "sh": self._DEFAULT_STRENGTH_HOME_SCORED,
            "ch": self._DEFAULT_STRENGTH_HOME_CONCEDED,
            "sa": self._DEFAULT_STRENGTH_AWAY_SCORED,
            "ca": self._DEFAULT_STRENGTH_AWAY_CONCEDED,
        }

    def initial_state(self) -> Dict[str, Any]:
        """Standings snapshot as of the moment importance is computed. Built
        from finished matches in the season (NOT from the FD.org standings
        endpoint — that endpoint can include in-progress results that
        de-sync from the fixture list, and the simulator needs the two to
        agree).

        State shape:
          {
            "_applied": frozenset of fd_ids already reflected in points/gf/ga,
            <team_name>: {"played": int, "points": int, "gf": int, "ga": int},
            ...
          }

        Teams that appear in the fixture list but haven't played yet still
        get a zero-row so the simulator can apply their results without a
        KeyError.
        """
        if self._initial_state_cache is not None:
            return self._initial_state_cache
        matches = self._fetch_all_season_matches()
        state: Dict[str, Any] = {"_applied": frozenset()}
        applied: List[Any] = []
        teams_seen: set = set()
        # Seed every team that appears anywhere in the season fixtures with
        # a zero row, so apply_result doesn't need to defensively create
        # rows for newly-encountered teams.
        for m in matches:
            for side in ("homeTeam", "awayTeam"):
                name = (m.get(side) or {}).get("name")
                if name and name not in teams_seen:
                    teams_seen.add(name)
                    state[name] = {"played": 0, "points": 0, "gf": 0, "ga": 0}
        # Apply FINISHED matches to populate the standings snapshot.
        for m in matches:
            if m.get("status") != "FINISHED":
                continue
            ft = (m.get("score") or {}).get("fullTime") or {}
            hg = ft.get("home")
            ag = ft.get("away")
            if hg is None or ag is None:
                continue
            home = (m.get("homeTeam") or {}).get("name")
            away = (m.get("awayTeam") or {}).get("name")
            fd_id = m.get("id")
            if not home or not away or fd_id is None:
                continue
            applied.append(fd_id)
            self._mutate_apply(state, home, away, int(hg), int(ag))
        state["_applied"] = frozenset(applied)
        self._initial_state_cache = state
        return state

    @staticmethod
    def _mutate_apply(state: Dict[str, Any], home: str, away: str, hg: int, ag: int) -> None:
        """Apply one finished result to a state dict in place. Used by
        `initial_state` (where mutation is fine — we own the new state) and
        `apply_result` (which copies first to preserve immutability)."""
        h = state[home]
        a = state[away]
        h["played"] += 1
        a["played"] += 1
        h["gf"] += hg
        h["ga"] += ag
        a["gf"] += ag
        a["ga"] += hg
        if hg > ag:
            h["points"] += 3
        elif hg < ag:
            a["points"] += 3
        else:
            h["points"] += 1
            a["points"] += 1

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        """All matches not yet applied. The simulator uses this to know
        which matches still need sampling after applying the target match.
        """
        applied = state.get("_applied", frozenset())
        matches = self._fetch_all_season_matches()
        out: List[GameRow] = []
        for m in matches:
            fd_id = m.get("id")
            if fd_id is None or fd_id in applied:
                continue
            home = (m.get("homeTeam") or {}).get("name")
            away = (m.get("awayTeam") or {}).get("name")
            start = parse_iso_utc(m.get("utcDate"))
            if not home or not away or start is None:
                continue
            out.append(GameRow(
                sport_prefix=self.config.sport_prefix,
                sport_label=self.config.sport_label,
                home=home,
                away=away,
                rank_home=None,  # importance doesn't need ranks
                rank_away=None,
                start_time=start,
                extra={"fd_id": fd_id, "matchday": m.get("matchday")},
            ))
        return out

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Sample one (home_goals, away_goals) via Poisson with rolling-
        average rates per Lahvička. The lambda for home goals is the
        average of home team's home-attack rate and away team's away-defense
        rate; mirror for away goals.

        `state` is part of the SportSource ABC signature but unused by the
        league-shape Poisson model — team strengths fully determine the
        rates. KnockoutSoccerSource overrides this and DOES read state
        (to look up leg 1's result when sampling leg 2). Reference the
        param explicitly to silence "unused parameter" hints.
        """
        del state  # interface-required, not used by league shape
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["sh"] + a["ca"]) / 2.0)
        lam_away = max(0.05, (a["sa"] + h["ch"]) / 2.0)
        return MatchResult(
            home_goals=_poisson(lam_home, rng),
            away_goals=_poisson(lam_away, rng),
        )

    def apply_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        result: MatchResult,
    ) -> Dict[str, Any]:
        """Return a NEW state with `match`'s `result` applied. Pure — the
        simulator depends on this NOT mutating `state` so one initial_state
        can seed many sampled seasons.
        """
        # Shallow-copy the state dict plus the two team rows we'll mutate.
        # Other teams are shared by reference; cheap and correct because
        # we never write back through them in this update.
        new_state = dict(state)
        new_state[match.home] = dict(state[match.home])
        new_state[match.away] = dict(state[match.away])
        self._mutate_apply(new_state, match.home, match.away,
                           result.home_goals, result.away_goals)
        fd_id = match.extra.get("fd_id") if isinstance(match.extra, dict) else None
        if fd_id is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {fd_id}
        return new_state

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        """{team: [outcome_labels]} at the final standings encoded in
        `state`. Sorts by (points desc, goal_diff desc, gf desc) — the
        standard soccer tiebreak. Assigns labels based on
        `LEAGUE_CONTEXTS[fd_code].thresholds`.

        A team can match multiple top-side labels (champion also qualifies
        for UCL and Europa). Bottom-side labels fire on positions worse
        than the cutoff. The caller (scoring.compute_match_importance)
        decides aggregation per the cross-sport sum rule.
        """
        ctx = LEAGUE_CONTEXTS.get(self.config.fd_code)
        if ctx is None:
            return {team: [] for team in state if team != "_applied"}
        # DO NOT use this method for knockout-format contexts (UCL, etc.).
        # The cutoff in knockout thresholds is a stage string ("LAST_16"),
        # not an int position — the `pos > cutoff` comparison below would
        # TypeError. The factory in plugin.py routes knockout configs to
        # KnockoutSoccerSource; falling here means a misroute. Return empty
        # so importance silently produces 0 (consistent with "no league
        # context" branch above) rather than crashing the refresh.
        if ctx.format != "league":
            logger.warning(
                "[soccer:%s] SoccerSource.terminal_outcomes called on a non-league "
                "context (format=%s); returning empty. This is a wiring bug — the "
                "factory should route this competition to KnockoutSoccerSource.",
                self.config.fd_code, ctx.format,
            )
            return {team: [] for team in state if team != "_applied"}
        teams = [(name, row) for name, row in state.items() if name != "_applied"]
        teams.sort(
            key=lambda kv: (
                -kv[1]["points"],
                -(kv[1]["gf"] - kv[1]["ga"]),
                -kv[1]["gf"],
            )
        )
        positions = {name: i + 1 for i, (name, _) in enumerate(teams)}
        outcomes: Dict[str, List[str]] = {name: [] for name, _ in teams}
        for cutoff, label, _weight in ctx.thresholds:
            bottom = self._is_bottom_outcome(label)
            for name, pos in positions.items():
                if bottom:
                    if pos > cutoff:
                        outcomes[name].append(label)
                else:
                    if pos <= cutoff:
                        outcomes[name].append(label)
        return outcomes


class KnockoutSoccerSource(SoccerSource):
    """Knockout-bracket adapter for cup competitions (UCL, UEL, etc.).

    Inherits fixtures, standings (best-effort, often empty for cups),
    odds-derived closeness, and per-team strength estimation from
    SoccerSource. Overrides the importance state machine for bracket
    shape: state tracks tie aggregates, propagates winners through
    bracket feeds, and resolves ET/penalty shootouts in `sample_result`.

    State model:
      {
        "_applied":       frozenset of fd_ids whose results are in state,
        "_tie_results":   {tie_key: tie_dict},
        "_bracket":       {stage: [tie_meta, ...]},   # static, built once
        "_round_reached": {team: max KNOCKOUT_ROUND_DEPTH advanced INTO},
      }

    `tie_key` = (stage, frozenset({team_a, team_b})). The frozenset is
    order-independent so leg 1 and leg 2 of the same tie key into the
    same dict entry regardless of which side is home on which leg.

    `_bracket` is built once at `initial_state` time from the FD.org
    fixture list. For each tie in stage S > LAST_16, the participants'
    prior-stage tie winners are recorded as `feeds_from`. The simulator
    uses `feeds_from` to substitute simulated winners in place of
    FD.org's published downstream participants when an upstream
    counterfactual changes the bracket path.

    For 2-leg ties, `sample_result` for leg 1 returns regulation goals
    only. For the decisive leg (leg 2 of a 2-leg tie, or the single
    FINAL), `sample_result` returns goals including ET and a +1 pen-
    shootout-winner boost so the W/L classification is captured in
    `MatchResult.home_goals/away_goals` — never D for decisive legs.
    """

    # FD.org knockout stage enum, in increasing depth order.
    KO_STAGES = ("PLAYOFFS", "LAST_16", "QUARTER_FINALS", "SEMI_FINALS", "FINAL")

    def initial_state(self) -> Dict[str, Any]:
        """Build the bracket structure and per-team round_reached snapshot
        from the FD.org match list. Cached on the instance so repeat
        importance queries within one refresh share the same baseline.
        """
        if self._initial_state_cache is not None:
            return self._initial_state_cache
        matches = self._fetch_all_season_matches()
        bracket = self._build_bracket(matches)
        applied: List[Any] = []
        tie_results: Dict[Any, Dict[str, Any]] = {}
        round_reached: Dict[str, int] = {}

        # Pre-fill tie_results from FINISHED legs in the FD.org data. A tie
        # whose all legs are FINISHED has its aggregate winner computed
        # here and stored — the simulator starts from that ground truth.
        for stage in self.KO_STAGES:
            for tie in bracket.get(stage, []):
                tk = (stage, frozenset(tie["teams"]))
                tie_results[tk] = self._new_tie_record(tie)
                for leg in tie["legs"]:
                    if leg.get("status") == "FINISHED" and leg.get("home_goals") is not None:
                        # Record this leg's regulation+ET+pen-determined outcome
                        # straight from FD.org so simulation starts from truth.
                        self._record_leg_into_tie(
                            tie_results[tk],
                            leg["home"], leg["away"],
                            leg["home_goals"], leg["away_goals"],
                            leg.get("matchday", 1),
                        )
                        applied.append(leg["fd_id"])
                # If the tie is fully decided, propagate to round_reached.
                if tie_results[tk]["aggregate_winner"] is not None:
                    self._advance_round_reached(
                        round_reached,
                        tie_results[tk]["aggregate_winner"],
                        tie_results[tk]["aggregate_loser"],
                        stage,
                    )

        state: Dict[str, Any] = {
            "_applied": frozenset(applied),
            "_tie_results": tie_results,
            "_bracket": bracket,
            "_round_reached": round_reached,
        }
        self._initial_state_cache = state
        return state

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        """Knockout ties whose participants are CURRENTLY resolvable and
        whose legs aren't yet applied. A tie is resolvable when:
          - It has no `feeds_from` (entry-level: LAST_16 ties or seeded
            top-8 teams entering R16), OR
          - All its `feeds_from` upstream ties have resolved aggregate winners.

        Within a tie, legs are emitted in matchday order. The first
        unapplied leg's home/away comes from the resolved participants;
        the second leg swaps home/away.
        """
        applied = state.get("_applied", frozenset())
        tie_results = state.get("_tie_results", {})
        bracket = state.get("_bracket", {})
        out: List[GameRow] = []
        for stage in self.KO_STAGES:
            ties = bracket.get(stage, [])
            for tie in ties:
                participants = self._resolve_participants(tie, bracket, tie_results)
                if participants is None:
                    continue  # upstream not yet settled
                team_a, team_b = participants
                # Iterate this tie's legs in matchday order.
                for leg in tie["legs"]:
                    if leg["fd_id"] in applied:
                        continue
                    # In the FD.org-published data, leg 1 has one team home
                    # and leg 2 swaps. We mirror that swap using the resolved
                    # participants from upstream (which may differ from FD's
                    # published home/away if the simulator changed the path).
                    if leg["matchday"] == 1:
                        home, away = team_a, team_b
                    else:
                        home, away = team_b, team_a
                    legs_in_tie = len(tie["legs"])
                    out.append(GameRow(
                        sport_prefix=self.config.sport_prefix,
                        sport_label=self.config.sport_label,
                        home=home,
                        away=away,
                        rank_home=None,
                        rank_away=None,
                        start_time=leg["start_time"],
                        extra={
                            "fd_id": leg["fd_id"],
                            "stage": stage,
                            "matchday": leg["matchday"],
                            "tie_key": (stage, frozenset({team_a, team_b})),
                            "leg_index": leg["matchday"],
                            "legs_in_tie": legs_in_tie,
                            # is_decisive_leg: leg 2 of a 2-leg tie, or the
                            # only leg in a 1-leg tie. sample_result fires
                            # ET/penalty resolution on decisive legs only.
                            "is_decisive_leg": leg["matchday"] == legs_in_tie,
                        },
                    ))
        return out

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Sample one leg via Poisson. For decisive legs (leg 2 of a 2-leg
        tie or the single FINAL) with aggregate tied after regulation,
        sample ET (30 min at 1/3 the regulation rate). If still tied, run
        a penalty shootout (5 shots each + sudden death, 70% conversion).
        Returns MatchResult with `home_goals/away_goals` post-ET and a +1
        pen-winner boost on shootouts so W/L is encoded for tau-c.
        """
        # Regulation goals via inherited Poisson formula.
        reg = super().sample_result(state, match, strengths, rng)
        extra = match.extra if isinstance(match.extra, dict) else {}
        if not extra.get("is_decisive_leg", False):
            return reg

        legs_in_tie = extra.get("legs_in_tie", 1)
        tie_key = extra.get("tie_key")
        home_team, away_team = match.home, match.away

        # Compute aggregate. For 1-leg ties (FINAL), aggregate IS this leg.
        if legs_in_tie == 1:
            agg_home = reg.home_goals
            agg_away = reg.away_goals
        else:
            # 2-leg: pull leg 1 from state. Leg 1's home was the team that's
            # AWAY in this (leg 2) match; so leg1.home_goals counts toward
            # the team now away, and leg1.away_goals toward the team now home.
            tie = state.get("_tie_results", {}).get(tie_key, {})
            leg1 = tie.get("leg1")
            if leg1 is None:
                # Defensive: leg 1 should have been applied before leg 2.
                # If it's missing, treat this leg as standalone (consistent
                # with the FINAL path).
                agg_home = reg.home_goals
                agg_away = reg.away_goals
            else:
                # leg1 stores: {home, home_goals, away, away_goals}
                if leg1["home"] == home_team:
                    # Unusual: same team home in both legs (shouldn't happen
                    # in a proper bracket draw). Sum naively as a safety net.
                    agg_home = reg.home_goals + leg1["home_goals"]
                    agg_away = reg.away_goals + leg1["away_goals"]
                else:
                    # Normal swap: home_team was away in leg 1.
                    agg_home = reg.home_goals + leg1["away_goals"]
                    agg_away = reg.away_goals + leg1["home_goals"]

        home_goals = reg.home_goals
        away_goals = reg.away_goals
        result_extra: Dict[str, Any] = {"regulation_goals": (reg.home_goals, reg.away_goals)}

        if agg_home != agg_away:
            return MatchResult(home_goals=home_goals, away_goals=away_goals, extra=result_extra)

        # Tied after regulation: sample ET (1/3 regulation rate, 30 min vs 90 min).
        h = self._strength_for(strengths, home_team)
        a = self._strength_for(strengths, away_team)
        lam_h_et = max(0.02, (h["sh"] + a["ca"]) / 2.0 / 3.0)
        lam_a_et = max(0.02, (a["sa"] + h["ch"]) / 2.0 / 3.0)
        et_h = _poisson(lam_h_et, rng)
        et_a = _poisson(lam_a_et, rng)
        home_goals += et_h
        away_goals += et_a
        agg_home += et_h
        agg_away += et_a
        result_extra["et_goals"] = (et_h, et_a)

        if agg_home != agg_away:
            return MatchResult(home_goals=home_goals, away_goals=away_goals, extra=result_extra)

        # Still tied: penalty shootout. Encode the winner as a +1 goal so
        # _classify_target_result sees a non-draw — keeps the simulator's
        # ordinal W/D/L classification honest for tau-c. Symmetric 70%
        # conversion per shot; pen shootouts are dominated by variance, so
        # skill bumps don't shift the outcome distribution materially.
        pen_winner = self._sample_penalty_shootout(rng)
        if pen_winner == "HOME":
            home_goals += 1
        else:
            away_goals += 1
        result_extra["pen_winner"] = pen_winner
        return MatchResult(home_goals=home_goals, away_goals=away_goals, extra=result_extra)

    def apply_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        result: MatchResult,
    ) -> Dict[str, Any]:
        """Return a NEW state with this leg's result recorded. If the leg
        completes a tie (i.e., this is the decisive leg), compute the
        aggregate winner and advance the winning team's round_reached.
        """
        new_state = dict(state)
        # Copy the tie_results dict and the specific tie we're about to mutate.
        new_tie_results = dict(state.get("_tie_results", {}))
        extra = match.extra if isinstance(match.extra, dict) else {}
        tie_key = extra.get("tie_key")
        leg_index = extra.get("matchday", 1)
        if tie_key is None or tie_key not in new_tie_results:
            # Defensive: match without a tie context. Shouldn't happen via
            # remaining_matches but possible if a caller hand-rolls a match.
            new_state["_tie_results"] = new_tie_results
            fd_id = extra.get("fd_id")
            if fd_id is not None:
                new_state["_applied"] = state.get("_applied", frozenset()) | {fd_id}
            return new_state

        prior_tie = new_tie_results[tie_key]
        new_tie = {
            "stage": prior_tie["stage"],
            "teams": prior_tie["teams"],
            "leg1": dict(prior_tie["leg1"]) if prior_tie.get("leg1") else None,
            "leg2": dict(prior_tie["leg2"]) if prior_tie.get("leg2") else None,
            "aggregate_winner": prior_tie["aggregate_winner"],
            "aggregate_loser": prior_tie["aggregate_loser"],
        }
        self._record_leg_into_tie(
            new_tie,
            match.home, match.away,
            result.home_goals, result.away_goals,
            leg_index,
        )
        new_tie_results[tie_key] = new_tie
        new_state["_tie_results"] = new_tie_results

        # Update _applied.
        fd_id = extra.get("fd_id")
        if fd_id is not None:
            new_state["_applied"] = state.get("_applied", frozenset()) | {fd_id}

        # If the tie just decided, propagate to _round_reached.
        if new_tie["aggregate_winner"] is not None and prior_tie["aggregate_winner"] is None:
            new_round = dict(state.get("_round_reached", {}))
            self._advance_round_reached(
                new_round,
                new_tie["aggregate_winner"],
                new_tie["aggregate_loser"],
                new_tie["stage"],
            )
            new_state["_round_reached"] = new_round
        return new_state

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        """{team: [outcome_labels]} based on each team's deepest round
        reached. A team that won the SF gets ["round_of_16", "quarterfinal",
        "semifinal", "final"] (made the final); the eventual champion
        additionally gets "winner".

        The label cascade is automatic: each band whose `cutoff` (a stage
        identifier) maps to a depth <= the team's reached depth fires.
        """
        ctx = LEAGUE_CONTEXTS.get(self.config.fd_code)
        round_reached = state.get("_round_reached", {})
        if ctx is None or not round_reached:
            return {}
        # Build the team set from _round_reached. Teams that never appeared
        # in any tracked tie don't show up; that's correct (they didn't reach
        # any of the cup's outcome bands).
        outcomes: Dict[str, List[str]] = {team: [] for team in round_reached}
        for cutoff, label, _ in ctx.thresholds:
            cutoff_depth = KNOCKOUT_ROUND_DEPTH.get(cutoff, -1)
            if cutoff_depth < 0:
                continue
            for team, depth in round_reached.items():
                if depth >= cutoff_depth:
                    outcomes[team].append(label)
        return outcomes

    # ---------- helpers ----------

    @staticmethod
    def _new_tie_record(tie_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Empty tie-result record. Filled in by _record_leg_into_tie."""
        return {
            "stage": tie_meta["stage"],
            "teams": tie_meta["teams"],
            "leg1": None,
            "leg2": None,
            "aggregate_winner": None,
            "aggregate_loser": None,
        }

    def _record_leg_into_tie(
        self,
        tie: Dict[str, Any],
        home_team: str,
        away_team: str,
        home_goals: int,
        away_goals: int,
        leg_index: int,
    ) -> None:
        """Record a leg's result into the tie dict in-place. Computes
        aggregate_winner if all legs are now present.

        Aggregate rule: sum goals across legs from each team's perspective.
        For decisive legs that include ET + pen-winner boost (added in
        sample_result), the goal counts encode the tie's winning side —
        the aggregate computation comes out right.
        """
        leg_record = {
            "home": home_team,
            "away": away_team,
            "home_goals": home_goals,
            "away_goals": away_goals,
        }
        if leg_index == 1:
            tie["leg1"] = leg_record
        else:
            tie["leg2"] = leg_record

        # Compute aggregate.
        legs_present = [tie["leg1"], tie["leg2"]]
        legs_present = [leg for leg in legs_present if leg is not None]
        # Tie has 1 leg (FINAL) if leg2 is None and stage is FINAL, OR
        # 2 legs otherwise. We can't reliably tell from inside the tie
        # alone; rely on the caller (initial_state / apply_result) to
        # only call _record_leg_into_tie when the tie's legs are filled.
        # If only leg1 is set and the stage IS FINAL, treat it as complete.
        is_final_stage = (tie["stage"] == "FINAL")
        is_complete = (
            (is_final_stage and tie["leg1"] is not None)
            or (tie["leg1"] is not None and tie["leg2"] is not None)
        )
        if not is_complete:
            return

        # Sum goals from each team's perspective.
        teams = list(tie["teams"])
        if len(teams) != 2:
            return
        team_a, team_b = teams
        agg = {team_a: 0, team_b: 0}
        for leg in legs_present:
            agg[leg["home"]] += leg["home_goals"]
            agg[leg["away"]] += leg["away_goals"]
        if agg[team_a] == agg[team_b]:
            # Should not happen for decisive legs (sample_result resolves
            # ties via ET+pens), but guard anyway. Defensive default: pick
            # the home team of the final leg as the winner (arbitrary but
            # deterministic for replay). Real FD.org data would never end
            # in a regulation tie at this point — knockout matches that
            # remained drawn would have ET/pen scores in FD.org's data.
            decisive_leg = legs_present[-1]
            tie["aggregate_winner"] = decisive_leg["home"]
            tie["aggregate_loser"] = decisive_leg["away"]
            return
        if agg[team_a] > agg[team_b]:
            tie["aggregate_winner"] = team_a
            tie["aggregate_loser"] = team_b
        else:
            tie["aggregate_winner"] = team_b
            tie["aggregate_loser"] = team_a

    @staticmethod
    def _advance_round_reached(
        round_reached: Dict[str, int],
        winner: str,
        loser: str,
        stage: str,
    ) -> None:
        """When a tie at `stage` resolves: winner advances to stage+1,
        loser is locked at stage. Records the deeper of the existing
        round_reached value or the new one for each team.
        """
        stage_depth = KNOCKOUT_ROUND_DEPTH.get(stage, -1)
        if stage_depth < 0:
            return
        # Loser reached THIS stage (the stage they lost in).
        round_reached[loser] = max(round_reached.get(loser, -1), stage_depth)
        # Winner advances to the next stage's depth (or to WINNER if this
        # was the FINAL).
        if stage == "FINAL":
            winner_depth = KNOCKOUT_ROUND_DEPTH.get("WINNER", stage_depth + 1)
        else:
            winner_depth = stage_depth + 1
        round_reached[winner] = max(round_reached.get(winner, -1), winner_depth)

    def _resolve_participants(
        self,
        tie: Dict[str, Any],
        bracket: Dict[str, List[Dict[str, Any]]],
        tie_results: Dict[Any, Dict[str, Any]],
    ) -> Optional[Tuple[str, str]]:
        """Return (team_a, team_b) for this tie based on simulator state,
        or None if the tie isn't yet eligible to play.

        Eligibility rules:
          - Entry-level ties (no fully-tracked upstream): use FD-published
            participants, substituting any side that DID come from a tracked
            upstream tie if the simulator produced a different winner.
          - Downstream ties (all participants come from tracked upstream):
            require all feeds resolved; return None otherwise.

        `feeds_from` is keyed by FD-published team name, so each leg-1
        home/away slot resolves independently. This is essential for
        UCL R16 where one side comes from PLAYOFFS (tracked) and the
        other enters directly from the league phase (untracked).
        """
        legs = tie.get("legs") or []
        if not legs:
            return None
        leg1 = legs[0]
        fd_home, fd_away = leg1["home"], leg1["away"]
        feeds_from: Dict[str, Tuple[str, int]] = tie.get("feeds_from") or {}
        is_entry_tie: bool = tie.get("is_entry_tie", True)

        def resolve_side(fd_team: str) -> Optional[str]:
            """Resolve a single side. If the FD-published team has a feed,
            return the simulator's current winner of that upstream tie. If
            no feed, return the FD-published team (direct entry). Returns
            None if the feed exists but the upstream isn't yet settled."""
            feed_ref = feeds_from.get(fd_team)
            if feed_ref is None:
                return fd_team
            return self._winner_of(feed_ref, bracket, tie_results)

        home = resolve_side(fd_home)
        away = resolve_side(fd_away)

        if home is None or away is None:
            # An upstream this tie depends on hasn't settled yet.
            # Entry-level ties with mixed entry can fall back to FD-published
            # for the unresolved side ONLY when the corresponding feed itself
            # doesn't exist (we already handled that in resolve_side). If we
            # got None it means a feed DOES exist but hasn't resolved — block.
            if is_entry_tie:
                # For pure entry-level (no feeds at all), home/away from
                # resolve_side will never be None because feeds_from is
                # empty. If we land here on an entry tie, it's mixed-entry
                # with a pending upstream — still block until upstream
                # settles, so the simulator doesn't sample with wrong teams.
                return None
            return None
        return home, away

    @staticmethod
    def _winner_of(
        feed_ref: Tuple[str, int],
        bracket: Dict[str, List[Dict[str, Any]]],
        tie_results: Dict[Any, Dict[str, Any]],
    ) -> Optional[str]:
        """Look up the aggregate_winner of an upstream tie referenced by
        (stage, tie_idx). Returns None if unsettled or out-of-range.
        """
        stage, tie_idx = feed_ref
        ties = bracket.get(stage, [])
        if tie_idx >= len(ties):
            return None
        tie_meta = ties[tie_idx]
        tk = (stage, frozenset(tie_meta["teams"]))
        return tie_results.get(tk, {}).get("aggregate_winner")

    def _build_bracket(self, matches: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse FD.org's match list into per-stage tie metadata. For each
        stage > LAST_16, attach `feeds_from` pointers to the upstream
        ties whose winners participate in this tie.

        Stage-and-team-pair uniquely identifies a tie. Within a tie, legs
        are ordered by matchday (1 then 2). For the FINAL there's only one
        leg.

        Stages we don't model (LEAGUE_STAGE in the new UCL format) are
        skipped — they're round-robin within the league phase and have no
        bracket structure to track.
        """
        # Group matches by stage and pair.
        by_stage: Dict[str, Dict[frozenset, List[Dict[str, Any]]]] = {
            s: {} for s in self.KO_STAGES
        }
        for m in matches:
            stage = m.get("stage")
            if stage not in by_stage:
                continue
            home = (m.get("homeTeam") or {}).get("name")
            away = (m.get("awayTeam") or {}).get("name")
            if not home or not away:
                continue
            pair = frozenset({home, away})
            score = m.get("score") or {}
            full_time = score.get("fullTime") or {}
            # Use the duration field if present (REGULAR / EXTRA_TIME / PENALTY_SHOOTOUT)
            # to know whether fullTime.home includes ET or not. FD.org stores
            # fullTime as the score that decided the leg (so if a penalty shootout
            # decided it, fullTime is the post-ET score and the shootout outcome
            # lives in score.penalties). For our state ingestion we record
            # fullTime + (pen-winner +1) so the aggregate maths come out right.
            home_goals = full_time.get("home")
            away_goals = full_time.get("away")
            duration = score.get("duration")
            penalties = score.get("penalties") or {}
            if duration == "PENALTY_SHOOTOUT" and home_goals is not None and away_goals is not None:
                # Apply the +1 boost to the pen winner so aggregate sum
                # reflects the tie's actual winner.
                pen_home = penalties.get("home")
                pen_away = penalties.get("away")
                if pen_home is not None and pen_away is not None:
                    if pen_home > pen_away:
                        home_goals = (home_goals or 0) + 1
                    elif pen_away > pen_home:
                        away_goals = (away_goals or 0) + 1
            leg = {
                "fd_id": m.get("id"),
                "matchday": m.get("matchday") or 1,
                "home": home,
                "away": away,
                "home_goals": home_goals,
                "away_goals": away_goals,
                "status": m.get("status"),
                "start_time": parse_iso_utc(m.get("utcDate")),
            }
            by_stage[stage].setdefault(pair, []).append(leg)

        # Sort each tie's legs by matchday and build the tie_meta list.
        bracket: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.KO_STAGES}
        for stage in self.KO_STAGES:
            stage_ties = by_stage[stage]
            for pair, legs in stage_ties.items():
                legs.sort(key=lambda L: L["matchday"])
                bracket[stage].append({
                    "stage": stage,
                    "teams": pair,
                    "legs": legs,
                    "feeds_from": {},      # {team_name: (prev_stage, tie_idx)}
                    "is_entry_tie": True,  # overridden below if any participant
                                           # came from a tracked upstream tie
                })

        # Wire feeds_from for downstream stages. For each tie at stage S,
        # each participant team T is linked to the most recent earlier-
        # stage tie where T is a participant (NOT necessarily a winner —
        # this is a structural inference based on the bracket draw, so it
        # works when upstream results are SCHEDULED as well as FINISHED).
        # Keyed by team name so each side resolves independently — handles
        # UCL R16 where one side enters from PLAYOFFS (tracked) and the
        # other from top-8 direct league-phase entry (untracked, no feed).
        #
        # is_entry_tie is True iff at least one participant has no upstream
        # feed (entry stage or mixed-entry). Downstream-only ties are
        # blocked until all feeds resolve via aggregate_winner.
        participants_by_stage: Dict[str, Dict[str, int]] = {
            stage: {team: idx for idx, tie in enumerate(bracket[stage]) for team in tie["teams"]}
            for stage in self.KO_STAGES
        }
        for i, stage in enumerate(self.KO_STAGES):
            for tie in bracket[stage]:
                team_feeds: Dict[str, Tuple[str, int]] = {}
                for team in tie["teams"]:
                    # Search earlier tracked stages from nearest to farthest.
                    for j in range(i - 1, -1, -1):
                        prev_stage = self.KO_STAGES[j]
                        prev_idx = participants_by_stage[prev_stage].get(team)
                        if prev_idx is not None:
                            team_feeds[team] = (prev_stage, prev_idx)
                            break
                tie["feeds_from"] = team_feeds
                tie["is_entry_tie"] = len(team_feeds) < len(tie["teams"])
        return bracket

    @staticmethod
    def _compute_aggregate_from_fd_legs(legs: List[Dict[str, Any]]) -> Optional[str]:
        """Given the FD.org-published legs of a tie, return the aggregate
        winner team name. Returns None if any leg is unfinished or the
        aggregate is somehow tied (shouldn't happen with real FD data
        because shootouts give a +1 boost in `_build_bracket`).
        """
        if not legs:
            return None
        if any(L.get("home_goals") is None or L.get("away_goals") is None for L in legs):
            return None
        teams: set = set()
        for L in legs:
            teams.add(L["home"])
            teams.add(L["away"])
        if len(teams) != 2:
            return None
        team_a, team_b = list(teams)
        agg = {team_a: 0, team_b: 0}
        for L in legs:
            agg[L["home"]] += L["home_goals"]
            agg[L["away"]] += L["away_goals"]
        if agg[team_a] == agg[team_b]:
            return None
        return team_a if agg[team_a] > agg[team_b] else team_b

    @staticmethod
    def _sample_penalty_shootout(rng: random.Random) -> str:
        """Return 'HOME' or 'AWAY'. Symmetric 70% conversion per kick over
        5 rounds plus sudden death. Penalty shootouts are dominated by
        variance — modelling per-team conversion skill barely shifts the
        outcome distribution at the calibration precision we use.
        """
        h = sum(1 for _ in range(5) if rng.random() < 0.70)
        a = sum(1 for _ in range(5) if rng.random() < 0.70)
        # Sudden death: each team takes one more shot per round until they differ.
        # Cap at 30 rounds defensively — a 60-round sudden death has probability
        # ~3e-15 at p=0.7 and we don't need to model that tail.
        for _ in range(30):
            if h != a:
                break
            h += int(rng.random() < 0.70)
            a += int(rng.random() < 0.70)
        return "HOME" if h > a else "AWAY"


def _poisson(lam: float, rng: random.Random) -> int:
    """Draw one Poisson sample via Knuth's algorithm. Pure-Python (no numpy
    dependency, the plugin runs in Dispatcharr's lean container).

    For lam in [0, 10] this is fast enough — soccer goal counts hover at
    ~1.4 mean, deepest tail rarely exceeds 7-8 goals/game. For larger lam
    a normal approximation would be needed; not relevant here.
    """
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= rng.random()
    return k - 1
