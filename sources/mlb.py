"""MLB source — official `statsapi.mlb.com` for both regular-season schedule
and the postseason bracket. No API key required.

Two source classes:
  - `MlbRegularSource(PointsBasedSportSource)`: regular season. Uses raw win
    count (no OT/SO bonus like NHL — MLB extra-innings winners get a normal
    +1 W); LEAGUE_CONTEXTS["MLB"] has format="win_count" with thresholds at
    85 (wildcard bubble) / 90 (division lead) / 95 (elite) / 100 (historic).
  - `MlbPlayoffSource(BestOfNSeriesSource)`: full postseason. Mixed series
    lengths per round (Wild Card best-of-3, Division Series best-of-5,
    LCS / World Series best-of-7) — model leans on the BestOfNSeriesSource
    per-stage series-length hook (`_series_length_for_stage`).

API quirks captured here:
  - `/api/v1/schedule?sportId=1&season=YYYY&gameType=R` returns the entire
    regular season in a single response (≈2430 games). No per-day or
    per-team iteration needed — unlike ESPN unofficial endpoints where
    multi-day ranges silently cap at 25 events.
  - `/api/v1/schedule/postseason?season=YYYY` returns every postseason
    game with `seriesDescription` ("AL Wild Card Series", "NL Division
    Series", "World Series", etc.) and `seriesGameNumber` (1..N) per game.
    Future-round games are not emitted until participants are decided;
    e.g., during LCS week the World Series ties don't exist in the
    response. That tracks issue #17's NHL CUP_FINAL story — filed as a
    follow-up for MLB World Series leverage during LCS.
  - `gameType` values: R=regular, F=Wild Card, D=Division Series,
    L=League Championship Series, W=World Series. (Plus S=spring,
    P=preseason, A=allstar, I=intersquad — all filtered out.)
  - `status.abstractGameState` is "Final" for FINISHED, "Preview" or
    "Live" otherwise. We treat anything not Final as SCHEDULED for
    importance purposes.
  - Team names come from `teams.home.team.name` (e.g., "Cleveland
    Guardians"). The `team.abbreviation` field is empty in the schedule
    endpoint — use full name as the canonical key.

The plugin opts into MLB via the `enable_mlb` boolean in `plugin.json`.
Off by default; baseball coverage is new in Phase F.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc, poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mlb")

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"

# Regular-season per-team runs averages cluster around 4.5 / game in the
# modern (post-2015) MLB. Used as the prior for teams the simulator
# hasn't seen yet (e.g., spring-training import before opening day).
_DEFAULT_RUNS_FOR = 4.5
_DEFAULT_RUNS_AGAINST = 4.5


# seriesDescription → KO_STAGES label. Both leagues feed into the same
# stage entry (AL and NL Wild Card both go to "WC") — the bracket
# inference in BracketSportSource groups by (stage, team-pair), so AL/NL
# bracket halves stay independent because they involve different teams.
_SERIES_DESC_TO_STAGE: Dict[str, str] = {
    "AL Wild Card Series": "WC",
    "NL Wild Card Series": "WC",
    "AL Division Series": "LDS",
    "NL Division Series": "LDS",
    "AL Championship Series": "LCS",
    "NL Championship Series": "LCS",
    "World Series": "WS",
}

# Per-stage best-of-N: WC=3, LDS=5, LCS=7, WS=7.
_MLB_SERIES_LENGTHS: Dict[str, int] = {
    "WC": 3,
    "LDS": 5,
    "LCS": 7,
    "WS": 7,
}


def _http_get(url: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    """Wrapper around requests.get with logging on non-2xx. Returns
    the parsed JSON dict or None. Timeout is 20s (season schedule is
    ~2MB JSON; default 15s sometimes flakes from pocket-dev's egress)."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[mlb] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[mlb] %s failed: %s", url, exc)
        return None


def _default_season() -> int:
    """Current MLB season as a 4-digit year. Regular season runs
    late-March through October; postseason wraps in early November.
    November 16th onward reads as the next year's season because the
    schedule endpoint populates the new year's spring training before
    the current postseason wraps."""
    now = datetime.now(timezone.utc)
    if now.month >= 11 and now.day >= 16:
        return now.year + 1
    return now.year


# =====================================================================
# MlbRegularSource
# =====================================================================


class MlbRegularSource(PointsBasedSportSource):
    """MLB regular-season importance via PointsBasedSportSource.

    Uses raw `wins` as the threshold field (LEAGUE_CONTEXTS["MLB"] is
    format="win_count"). Unlike NHL, MLB has no OT-loss consolation
    point — a 10-inning loss is still just a loss. Extra innings start
    with a runner on second base since 2020, but for our importance
    sample_result the tie-breaker is a simple coin-flip — the resulting
    +1 boost gets attributed as a normal regulation win/loss.

    Goal-sampling: Poisson(λ) per side with the standard
    home_pf + away_pa blend (PointsBasedSportSource handles this in
    the base sample_result).
    """

    league_context_code = "MLB"
    _count_field = "wins"
    _DEFAULT_POINTS_FOR = _DEFAULT_RUNS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_RUNS_AGAINST

    def __init__(self, season: Optional[int] = None) -> None:
        super().__init__()
        self.season = season or _default_season()

    @property
    def sport_prefix(self) -> str:
        return "MLB"

    @property
    def sport_label(self) -> str:
        return "MLB"

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull the next-N-day regular-season schedule via the single
        `/schedule` endpoint with a startDate/endDate filter. MLB's
        schedule endpoint reliably honors date ranges (no silent cap
        like ESPN's). Closeness signal is left None — there is no Odds
        API integration for MLB in V1; structural importance carries.
        """
        today = datetime.now(timezone.utc).date()
        end = today.fromordinal(today.toordinal() + days_ahead)
        url = (
            f"{MLB_API_BASE}/schedule?sportId=1&gameType=R"
            f"&startDate={today.isoformat()}&endDate={end.isoformat()}"
        )
        data = _http_get(url)
        out: List[GameRow] = []
        if not data:
            return out
        for date_entry in data.get("dates", []) or []:
            for g in date_entry.get("games", []) or []:
                gid = g.get("gamePk")
                if gid is None:
                    continue
                teams = g.get("teams") or {}
                home = ((teams.get("home") or {}).get("team") or {}).get("name")
                away = ((teams.get("away") or {}).get("team") or {}).get("name")
                start = parse_iso_utc(g.get("gameDate"))
                if not home or not away or start is None:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "mlb_game_id": gid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Fetch the entire regular-season schedule in one shot. MLB's
        schedule endpoint returns the full season (~2430 games) reliably
        as a single response — no per-team iteration needed.
        """
        url = (
            f"{MLB_API_BASE}/schedule?sportId=1&gameType=R&season={self.season}"
        )
        data = _http_get(url)
        out: List[Dict[str, Any]] = []
        if not data:
            return out
        for date_entry in data.get("dates", []) or []:
            for g in date_entry.get("games", []) or []:
                gid = g.get("gamePk")
                if gid is None:
                    continue
                teams = g.get("teams") or {}
                home_obj = teams.get("home") or {}
                away_obj = teams.get("away") or {}
                home = ((home_obj.get("team") or {})).get("name")
                away = ((away_obj.get("team") or {})).get("name")
                if not home or not away:
                    continue
                state = (g.get("status") or {}).get("abstractGameState")
                hp = home_obj.get("score")
                ap = away_obj.get("score")
                if state == "Final" and hp is not None and ap is not None:
                    home_points: Optional[int] = int(hp)
                    away_points: Optional[int] = int(ap)
                    status = "FINISHED"
                else:
                    home_points = None
                    away_points = None
                    status = "SCHEDULED"
                out.append({
                    "id": gid,
                    "home": home,
                    "away": away,
                    "home_points": home_points,
                    "away_points": away_points,
                    "status": status,
                    "start_time": parse_iso_utc(g.get("gameDate")),
                })
        return out


# =====================================================================
# MlbPlayoffSource
# =====================================================================


class MlbPlayoffSource(BestOfNSeriesSource):
    """MLB postseason as a per-stage variable-length BestOfNSeriesSource.

    Series lengths per stage:
      - WC (Wild Card Series):  best-of-3
      - LDS (Division Series):  best-of-5
      - LCS (Championship Series): best-of-7
      - WS (World Series):      best-of-7

    Per-game sampling: Poisson runs per side; tied regulation gets a
    coin-flip +1 (extra innings, treated as a normal W/L for the
    importance signal). Unlike NHL, there's no shootout — but the
    distinction doesn't matter because MLB doesn't have a consolation
    point: `wins`-based count_field for the regular season, but
    BestOfNSeriesSource doesn't read either count field at all (series
    advancement is purely series_wins).
    """

    KO_STAGES = ("WC", "LDS", "LCS", "WS")
    # SERIES_LENGTH on the class is the fallback for stages outside the
    # per-stage map; in practice every MLB postseason stage is in the
    # map, so this value never actually applies. Set to 7 for parity
    # with the longest series so a hypothetical out-of-map stage
    # behaves like LCS rather than truncating early.
    SERIES_LENGTH = 7
    supports_importance = True

    def __init__(self, season: Optional[int] = None) -> None:
        self.season = season or _default_season()
        # Caches for the importance interface; same pattern as NhlPlayoffSource.
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "MLB"

    @property
    def sport_label(self) -> str:
        return "MLB Postseason"

    def _league_context_code(self) -> str:
        return "MLB_PO"

    def _series_length_for_stage(self, stage: str) -> int:
        return _MLB_SERIES_LENGTHS.get(stage, self.SERIES_LENGTH)

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # World Series winner → WS_WINNER synthetic depth.
        if stage == "WS":
            return "WS_WINNER"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day postseason schedule via the postseason endpoint
        filtered to the relevant date range. The endpoint emits past
        and future games, so we filter client-side by start_time.
        """
        out: List[GameRow] = []
        data = _http_get(f"{MLB_API_BASE}/schedule/postseason?season={self.season}")
        if not data:
            return out
        now = datetime.now(timezone.utc)
        horizon = now.fromordinal(now.toordinal() + days_ahead)
        for date_entry in data.get("dates", []) or []:
            for g in date_entry.get("games", []) or []:
                gid = g.get("gamePk")
                if gid is None:
                    continue
                start = parse_iso_utc(g.get("gameDate"))
                if start is None or start < now or start > horizon:
                    continue
                teams = g.get("teams") or {}
                home = ((teams.get("home") or {}).get("team") or {}).get("name")
                away = ((teams.get("away") or {}).get("team") or {}).get("name")
                if not home or not away:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "mlb_game_id": gid,
                        "fd_competition_code": self._league_context_code(),
                    },
                ))
        return out

    # ---------- strengths (reused from regular season) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate. Playoff samples are sparse
        (a postseason team plays at most ~19 games); preload from a
        regular-season MlbRegularSource via `set_regular_season_strengths`
        when available."""
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
        """Hook for plugin to share regular-season strength estimates
        with the playoff source. Same shape as NhlPlayoffSource's."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_RUNS_FOR,
            "pa_per_game": _DEFAULT_RUNS_AGAINST,
        }

    # ---------- sample_result (per-game Poisson with extra-innings) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        del state  # per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_runs = _poisson(lam_home, rng)
        away_runs = _poisson(lam_away, rng)
        # Extra innings: tied regulation always resolves (MLB postseason
        # has no draws); coin-flip the winner of the extra-innings boost.
        if home_runs == away_runs:
            if rng.random() < 0.5:
                home_runs += 1
            else:
                away_runs += 1
        return MatchResult(home_goals=home_runs, away_goals=away_runs)

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the entire postseason schedule and normalize to the
        bracket per-game record shape. statsapi.mlb.com returns games
        with `seriesDescription` already identifying the stage, so no
        secondary endpoint lookup is needed (unlike NHL which requires
        both the bracket and per-series schedule endpoints).
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        data = _http_get(f"{MLB_API_BASE}/schedule/postseason?season={self.season}")
        if not data:
            self._bracket_games_cache = []
            return []

        out: List[Dict[str, Any]] = []
        for date_entry in data.get("dates", []) or []:
            for g in date_entry.get("games", []) or []:
                gid = g.get("gamePk")
                if gid is None:
                    continue
                desc = g.get("seriesDescription") or ""
                stage = _SERIES_DESC_TO_STAGE.get(desc)
                if stage is None:
                    continue
                teams = g.get("teams") or {}
                home_obj = teams.get("home") or {}
                away_obj = teams.get("away") or {}
                home = ((home_obj.get("team") or {})).get("name")
                away = ((away_obj.get("team") or {})).get("name")
                if not home or not away:
                    continue
                state = (g.get("status") or {}).get("abstractGameState")
                hr = home_obj.get("score")
                ar = away_obj.get("score")
                if state == "Final" and hr is not None and ar is not None:
                    home_runs: Optional[int] = int(hr)
                    away_runs: Optional[int] = int(ar)
                    status = "FINISHED"
                else:
                    home_runs = None
                    away_runs = None
                    status = "SCHEDULED"
                out.append({
                    "game_id": gid,
                    "stage": stage,
                    "matchday": g.get("seriesGameNumber") or 1,
                    "home": home,
                    "away": away,
                    "home_goals": home_runs,
                    "away_goals": away_runs,
                    "status": status,
                    "start_time": parse_iso_utc(g.get("gameDate")),
                    "extra": {
                        "series_description": desc,
                        "games_in_series_published": g.get("gamesInSeries"),
                    },
                })

        self._bracket_games_cache = out
        return out
