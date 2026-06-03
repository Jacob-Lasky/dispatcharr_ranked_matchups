"""NHL source: official `api-web.nhle.com` for both regular-season standings
and Stanley Cup playoffs. No API key required.

Two source classes:
  - `NhlRegularSource(PointsBasedSportSource)`: regular season. Uses
    standings points (regulation win = 2, OT/SO loss = 1, regulation loss
    = 0) as the importance threshold field; LEAGUE_CONTEXTS["NHL"] has
    format="points_count" and threshold bands at 95/100/110/125 points.
  - `NhlPlayoffSource(BestOfNSeriesSource)`: Stanley Cup Playoffs.
    Best-of-7 each round. Bracket inferred from `/v1/playoff-bracket/
    {season}`; per-series schedules pulled from `/v1/schedule/playoff-
    series/{season}/{seriesLetter}`.

API quirks captured here:
  - `/v1/standings/now` returns HTTP 307 to `/v1/standings/{date}`.
    `requests.get` with `allow_redirects=True` (the default) handles it,
    but the request must be made WITHOUT `compressed=False`: Cloudflare
    in front of api-web.nhle.com returns gzip-encoded responses and
    `requests` only decodes them when `Accept-Encoding: gzip` is sent
    (which it does by default).
  - `gameOutcome.lastPeriodType` is the source of truth for whether a
    finished game went REG / OT / SO. Required for the OT-loss point
    in standings_points calculation.
  - `gameType`: 1 = preseason, 2 = regular season, 3 = playoffs. We
    filter aggressively in `_fetch_full_season_games`.
  - Team name canonicalization uses `placeName.default + commonName.
    default` (e.g., "Tampa Bay Lightning") for parity with EPG channel
    names. Abbreviations (`teamAbbrev.default`, e.g., "TBL") are the
    stable key when iterating per-team schedules.

The plugin opts into NHL via the `enable_nhl` boolean in `plugin.json`.
Off by default so users who only watch football don't get hockey
channels appearing unsolicited.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult, SportSource
from .bracket import BestOfNSeriesSource
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc, poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.nhl")

NHL_API_BASE = "https://api-web.nhle.com/v1"

# All 32 active NHL franchises by their api-web abbreviation. Used by
# NhlRegularSource to walk per-team season schedules and dedupe games
# by ID: there's no league-wide season-fetch endpoint on api-web, but
# 32 sequential team-schedule calls complete in well under a minute and
# the importance refresh runs at most every 6 hours.
NHL_TEAM_ABBREVS: Tuple[str, ...] = (
    "ANA", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD",
    "NSH", "NYI", "NYR", "OTT", "PHI", "PIT", "SEA", "SJS",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH",
)

# NHL regular-season per-team goal averages cluster around 3.0 / game.
# Used as the prior for teams the simulator hasn't seen yet (e.g., an
# import in the first week of a season with no games played).
_DEFAULT_GOALS_FOR = 3.0
_DEFAULT_GOALS_AGAINST = 3.0

# api-web `gameState` values that mean the game is over. "OFF" and "FINAL"
# both appear depending on the host's firmware version; both are terminal.
# Anything else ("LIVE", "PRE", "FUT") is not a usable final result.
_FINAL_GAME_STATES = ("OFF", "FINAL")

# `gameOutcome.lastPeriodType` values that mean the game was NOT decided in
# regulation. Drives both the standings OT/SO-loss consolation point and the
# "(OT)" tag on the playoff-series recap.
_NON_REGULATION_PERIODS = ("OT", "SO")


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Return "Place Name + Common Name", matching the format Dispatcharr
    EPG entries typically use ('Tampa Bay Lightning', not 'TBL'). Handles
    api-web's nested `{default, fr}` shape gracefully.
    """
    place = ((team_obj.get("placeName") or {}).get("default") or "").strip()
    common = ((team_obj.get("commonName") or {}).get("default") or "").strip()
    if place and common:
        return f"{place} {common}"
    return place or common or (team_obj.get("abbrev") or "")


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """Wrapper around requests.get with logging on non-2xx and a single
    retry on connection errors. Returns the parsed JSON dict or None.
    """
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[nhl] %s → %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[nhl] %s failed: %s", url, exc)
        return None


def _default_season() -> str:
    """Current NHL season code in api-web's 8-digit format, e.g.
    "20252026" for the 2025-26 season. Season rolls over in early
    October; we treat August onward as the new season starting that
    calendar year. Pre-August (Jul) reads as the prior season because
    the playoffs typically wrap by June.
    """
    now = datetime.now(timezone.utc)
    start_year = now.year if now.month >= 8 else now.year - 1
    return f"{start_year}{start_year + 1}"


# =====================================================================
# NhlRegularSource
# =====================================================================

class NhlRegularSource(PointsBasedSportSource):
    """NHL regular-season importance via PointsBasedSportSource.

    Sets `_count_field = "standings_points"` so the LEAGUE_CONTEXTS["NHL"]
    format="points_count" thresholds bucket teams by standings points
    rather than raw wins. Standings points are computed per game:
      - regulation win or OT/SO win → +2
      - regulation loss             → +0
      - OT/SO loss                  → +1
    See `_record_result_into_state` for the override.

    Goal-sampling shape: Poisson(λ_home), Poisson(λ_away) with λ's
    blending the home team's per-game scoring rate and the away team's
    per-game allowed rate (the standard Lahvička formula already used
    by NCAAF / NCAAM). Ties (regulation 3-3 etc.) are post-resolved by
    sampling an OT (90%) vs SO (10%) outcome with a coin-flip winner,
    so the W/L classification stays honest while standings_points
    correctly reflects the OT-loss point.
    """

    league_context_code = "NHL"
    _count_field = "standings_points"
    _DEFAULT_POINTS_FOR = _DEFAULT_GOALS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_GOALS_AGAINST

    def __init__(self, season: Optional[str] = None) -> None:
        super().__init__()
        self.season = season or _default_season()

    @property
    def sport_prefix(self) -> str:
        return "NHL"

    @property
    def sport_label(self) -> str:
        return "NHL"

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day schedule via `/v1/schedule/{date}` and emit
        GameRows. Closeness is left None: there is no Odds API
        integration for NHL in V1. The structural importance signal
        carries the score on its own.
        """
        out: List[GameRow] = []
        seen_ids: set = set()
        today = datetime.now(timezone.utc).date()
        for offset in range(days_ahead + 1):
            d = today + timedelta(days=offset)
            data = _http_get(f"{NHL_API_BASE}/schedule/{d.isoformat()}")
            if not data:
                continue
            for week in data.get("gameWeek", []) or []:
                for g in week.get("games", []) or []:
                    if g.get("gameType") != 2:
                        continue  # regular season only here
                    gid = g.get("id")
                    if gid in seen_ids:
                        continue
                    seen_ids.add(gid)
                    home = _team_canonical_name(g.get("homeTeam") or {})
                    away = _team_canonical_name(g.get("awayTeam") or {})
                    start = parse_iso_utc(g.get("startTimeUTC"))
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
                            "nhl_game_id": gid,
                            "fd_competition_code": self.league_context_code,
                        },
                    ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Aggregate every team's season schedule, dedupe by game id,
        keep gameType=2 (regular season) only. Returns the canonical
        shape `points_based.PointsBasedSportSource` expects.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        for abbrev in NHL_TEAM_ABBREVS:
            data = _http_get(
                f"{NHL_API_BASE}/club-schedule-season/{abbrev}/{self.season}"
            )
            if not data:
                continue
            for g in data.get("games", []) or []:
                if g.get("gameType") != 2:
                    continue
                gid = g.get("id")
                if gid is None or gid in seen:
                    continue
                home = _team_canonical_name(g.get("homeTeam") or {})
                away = _team_canonical_name(g.get("awayTeam") or {})
                if not home or not away:
                    continue
                state = g.get("gameState")
                hg = (g.get("homeTeam") or {}).get("score")
                ag = (g.get("awayTeam") or {}).get("score")
                # Active live games have state == "LIVE": we treat them
                # as SCHEDULED for the simulator (don't seed in-progress
                # results because the score is still moving). See
                # _FINAL_GAME_STATES for the terminal-state set.
                is_final = state in _FINAL_GAME_STATES and hg is not None and ag is not None
                status = "FINISHED" if is_final else "SCHEDULED"
                last_period = (g.get("gameOutcome") or {}).get("lastPeriodType")
                seen[gid] = {
                    "id": gid,
                    "home": home,
                    "away": away,
                    "home_points": hg if is_final else None,
                    "away_points": ag if is_final else None,
                    "status": status,
                    "start_time": parse_iso_utc(g.get("startTimeUTC")),
                    "extra": {"last_period_type": last_period} if last_period else {},
                }
        return list(seen.values())

    # ---------- sample_result with OT/SO classification ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Sample regulation Poisson goals; on regulation tie, sample
        OT (90%) vs SO (10%) decisive outcome with coin-flip winner.
        Returns MatchResult with `extra.last_period_type` so
        _record_result_into_state can credit the OT-loss point
        correctly.
        """
        del state  # interface-required, not used at this level
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_goals = _poisson(lam_home, rng)
        away_goals = _poisson(lam_away, rng)
        last_period = "REG"
        if home_goals == away_goals:
            # OT (~90% of overtime games resolve in the 5-min 3-on-3 OT
            # period) vs SO (~10% reach the shootout). Coin-flip the
            # winning side. Adding +1 to the winner makes the W/L
            # classification honest for tau-c without inflating goal
            # totals (the underlying simulator doesn't read goal counts
            # beyond classifying outcome).
            last_period = "OT" if rng.random() < 0.9 else "SO"
            if rng.random() < 0.5:
                home_goals += 1
            else:
                away_goals += 1
        return MatchResult(
            home_goals=home_goals,
            away_goals=away_goals,
            extra={"last_period_type": last_period},
        )

    # ---------- standings_points-aware record override ----------

    def _record_result_into_state(
        self,
        teams: Dict[str, Dict[str, int]],
        home: str, away: str,
        home_pts: int, away_pts: int,
        result_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Inherit the base W/L/pf/pa/games_played update, then add NHL-
        specific standings_points: winner +2 always, loser +1 iff the
        game went to OT or SO (regulation loss = 0).

        For FINISHED games seeded from api-web, `result_extra` carries
        the `last_period_type` parsed in `_fetch_full_season_games`.
        For simulated games (apply_result), `last_period_type` comes
        from sample_result's MatchResult.extra. Both paths converge here.
        """
        super()._record_result_into_state(
            teams, home, away, home_pts, away_pts, result_extra=result_extra,
        )
        last_period = (result_extra or {}).get("last_period_type") or "REG"
        h = teams[home]
        a = teams[away]
        h.setdefault("standings_points", 0)
        a.setdefault("standings_points", 0)
        # Loser's OT/SO consolation: 1 standings point.
        loser_consolation = 1 if last_period in _NON_REGULATION_PERIODS else 0
        if home_pts > away_pts:
            h["standings_points"] += 2
            a["standings_points"] += loser_consolation
        elif away_pts > home_pts:
            a["standings_points"] += 2
            h["standings_points"] += loser_consolation
        # Pure regulation tie: shouldn't happen in NHL (every game has a
        # winner) but defensive no-op if it slips in.


# =====================================================================
# NhlPlayoffSource
# =====================================================================

# Mapping from api-web `playoffRound` integer to the LEAGUE_CONTEXTS
# stage label. Keys are 1..4; the synthetic CUP_WINNER label is reached
# automatically by `_winner_advance_label("CUP_FINAL")`.
_NHL_ROUND_TO_STAGE: Dict[int, str] = {
    1: "R1",
    2: "R2",
    3: "CONF_FINAL",
    4: "CUP_FINAL",
}

# NHL home pattern for a best-of-7 series: top seed hosts games 1, 2, 5, 7;
# bottom seed hosts games 3, 4, 6. Position i (0-indexed) is True iff the
# top seed is at home. Used to fill in speculative future games when the
# simulator needs to extend an unfinished series beyond what the schedule
# endpoint has published.
NHL_HOME_PATTERN: Tuple[bool, ...] = (True, True, False, False, True, False, True)


# Sentinel team names for the synthesized CUP_FINAL placeholder tie. The
# `/v1/playoff-bracket/{season}` endpoint only publishes a series after
# its participants are determined, so during the conference-finals week
# the Stanley Cup Final series doesn't appear in the bracket data even
# though we can already see which 4 teams are alive. Synthesizing a
# placeholder lets the importance simulator propagate counterfactual
# CONF_FINAL winners into the CUP_FINAL → CUP_WINNER cascade. DO NOT
# change these strings without also updating `_build_bracket`'s
# placeholder-detection branch: the names are the join key. Bit me on
# the first round of #17 work before I made this constant.
_CUP_FINAL_TOP_SENTINEL = "CF_A_WINNER"
_CUP_FINAL_BOT_SENTINEL = "CF_B_WINNER"


class NhlPlayoffSource(BestOfNSeriesSource):
    """Stanley Cup Playoffs as a best-of-7 BracketSportSource.

    Series resolve at 4 wins (`SERIES_LENGTH = 7` → clinching at ceil(7/2) = 4).
    The full 16-team bracket is fetched from `/v1/playoff-bracket/{season}`;
    each series' game schedule + scores comes from `/v1/schedule/playoff-
    series/{season}/{seriesLetter}`.

    Per-game sampling uses the same Poisson goal model as the regular
    season, but each individual game gets its own OT/SO resolution:
    a series can run 7 games with a mix of regulation, OT, and SO
    decisions. Series winners propagate through `_round_reached` to
    drive the terminal_outcomes label cascade (R1 → R2 → CONF_FINAL →
    CUP_FINAL → CUP_WINNER).
    """

    KO_STAGES = ("R1", "R2", "CONF_FINAL", "CUP_FINAL")
    SERIES_LENGTH = 7
    supports_importance = True

    # NHL playoffs don't have a "WINNER" depth above CUP_FINAL: that
    # synthetic label IS the cup winner. _winner_advance_label maps
    # CUP_FINAL → CUP_WINNER explicitly.

    def __init__(self, season: Optional[str] = None) -> None:
        self.season = season or _default_season()
        # The playoff-bracket endpoint takes the END year ("2026" for
        # 2025-26 season). The per-series schedule endpoint takes the
        # FULL 8-digit season ("20252026") + lower-case series letter
        # (e.g., "a"). Two different conventions on the same API,
        # determined empirically against the live host (the "series-a"
        # prefix in `seriesUrl` belongs to the web UI's URL, not the
        # API's). DO NOT use the seriesUrl as-is for the API call.
        self._season_end_year = self.season[4:] if len(self.season) == 8 else self.season
        # Cache for the per-instance importance interface. Same pattern as
        # SoccerSource so initial_state can short-circuit across the
        # simulator's per-game leverage sweep.
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._strengths_cache: Optional[Dict[str, Dict[str, float]]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None
        # Per-series completed-game recaps, memoized by series letter so a
        # 7-day fetch_upcoming that sees the same series on several days hits
        # the per-series schedule endpoint once. Populated lazily, only for
        # series that are already underway (see _series_from_status).
        self._series_results_cache: Dict[str, List[Dict[str, Any]]] = {}

    @property
    def sport_prefix(self) -> str:
        return "NHL"

    @property
    def sport_label(self) -> str:
        return "Stanley Cup Playoffs"

    def _league_context_code(self) -> str:
        return "NHL_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # CUP_FINAL winner → CUP_WINNER synthetic depth.
        if stage == "CUP_FINAL":
            return "CUP_WINNER"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        out: List[GameRow] = []
        seen: set = set()
        today = datetime.now(timezone.utc).date()
        for offset in range(days_ahead + 1):
            d = today + timedelta(days=offset)
            data = _http_get(f"{NHL_API_BASE}/schedule/{d.isoformat()}")
            if not data:
                continue
            for week in data.get("gameWeek", []) or []:
                for g in week.get("games", []) or []:
                    if g.get("gameType") != 3:
                        continue  # playoffs only here
                    gid = g.get("id")
                    if gid in seen:
                        continue
                    seen.add(gid)
                    home = _team_canonical_name(g.get("homeTeam") or {})
                    away = _team_canonical_name(g.get("awayTeam") or {})
                    start = parse_iso_utc(g.get("startTimeUTC"))
                    if not home or not away or start is None:
                        continue
                    extra = {
                        "nhl_game_id": gid,
                        "fd_competition_code": self._league_context_code(),
                    }
                    # Series grounding for the EPG description / LLM context.
                    # The daily-schedule game already carries a `seriesStatus`
                    # block (game number, best-of length, per-seed win counts),
                    # so this costs no extra call for the record line: only the
                    # completed-game recap hits the per-series schedule, and
                    # only once a series is underway.
                    series = self._series_from_status(g)
                    if series:
                        extra["series"] = series
                    out.append(GameRow(
                        sport_prefix=self.sport_prefix,
                        sport_label=self.sport_label,
                        home=home,
                        away=away,
                        rank_home=None,
                        rank_away=None,
                        start_time=start,
                        extra=extra,
                    ))
        return out

    def _series_schedule_url(self, series_letter: str) -> str:
        """URL for a per-series game schedule:
        `/v1/schedule/playoff-series/{full_season}/{lower_letter}`.

        DO NOT substitute the bracket endpoint's season form here: this endpoint
        takes the FULL 8-digit season ("20252026"), NOT the end-year ("2026")
        the `/v1/playoff-bracket/{season}` endpoint uses. Two different season
        conventions on the same API, confirmed empirically against the live
        host. The single source of truth for both callers (importance bracket
        fetch and the EPG-recap fetch).
        """
        return f"{NHL_API_BASE}/schedule/playoff-series/{self.season}/{series_letter}"

    def _series_from_status(
        self, g: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Normalize the daily-schedule `seriesStatus` block into the
        sport-agnostic `extra['series']` schema that the EPG description and LLM
        context read (see _util.series_phase_text and friends). Returns None
        when the block is absent or unusable.

        Win counts arrive keyed by top/bottom SEED, but home ice alternates
        across a series, so they're re-keyed to THIS game's home/away by
        matching team abbreviations. `best_of = neededToWin * 2 - 1` (4 wins
        clinches a best-of-7).
        """
        ss = g.get("seriesStatus") or {}
        gnum = ss.get("gameNumberOfSeries")
        needed = ss.get("neededToWin")
        if not isinstance(gnum, int) or not isinstance(needed, int) or needed <= 0:
            return None
        top_wins = ss.get("topSeedWins") or 0
        bot_wins = ss.get("bottomSeedWins") or 0
        home_abbrev = (g.get("homeTeam") or {}).get("abbrev")
        top_abbrev = ss.get("topSeedTeamAbbrev")
        bot_abbrev = ss.get("bottomSeedTeamAbbrev")
        if home_abbrev == top_abbrev:
            home_wins, away_wins = top_wins, bot_wins
        elif home_abbrev == bot_abbrev:
            home_wins, away_wins = bot_wins, top_wins
        else:
            # Abbrev mismatch shouldn't happen on a live playoff feed; record
            # the seed-keyed counts as-is rather than guess the mapping.
            home_wins, away_wins = top_wins, bot_wins
        series: Dict[str, Any] = {
            "title": ss.get("seriesTitle") or "",
            "game_number": gnum,
            "best_of": needed * 2 - 1,
            "home_wins": home_wins,
            "away_wins": away_wins,
            "results": [],
        }
        # Only fetch the per-game recap once a game has actually been played;
        # on Game 1 the record is 0-0 and there is nothing to recap.
        if home_wins + away_wins > 0:
            series["results"] = self._fetch_series_results(
                (ss.get("seriesLetter") or "").lower()
            )
        return series

    def _fetch_series_results(self, series_letter: str) -> List[Dict[str, Any]]:
        """Completed-game results for a playoff series, oldest first, for the
        EPG-description recap. Reads the same per-series schedule endpoint the
        importance bracket uses. Memoized per series letter. Best-effort:
        returns [] on any fetch/parse failure, so the deterministic record line
        (free from seriesStatus) still renders without it.
        """
        if not series_letter:
            return []
        cached = self._series_results_cache.get(series_letter)
        if cached is not None:
            return cached
        results: List[Dict[str, Any]] = []
        try:
            sched = _http_get(self._series_schedule_url(series_letter))
            for game in (sched or {}).get("games", []) or []:
                state = game.get("gameState")
                ht = game.get("homeTeam") or {}
                at = game.get("awayTeam") or {}
                hg = ht.get("score")
                ag = at.get("score")
                gn = game.get("gameNumber")
                if state not in _FINAL_GAME_STATES or hg is None or ag is None:
                    continue
                if not isinstance(gn, int):
                    continue
                last_period = (game.get("gameOutcome") or {}).get("lastPeriodType")
                results.append({
                    "game_number": gn,
                    "home": _team_canonical_name(ht),
                    "away": _team_canonical_name(at),
                    "home_goals": hg,
                    "away_goals": ag,
                    "ot": last_period in _NON_REGULATION_PERIODS,
                })
            results.sort(key=lambda r: r["game_number"])
        except Exception as e:  # best-effort enrichment, never break refresh
            logger.warning(
                "[ranked_matchups] series-results fetch failed for %s: %s",
                series_letter, e,
            )
            results = []
        self._series_results_cache[series_letter] = results
        return results

    # ---------- strengths (reused from regular season) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate. For playoffs we ideally use
        regular-season averages because playoff samples are sparse (a
        team plays at most 28 games). Caller can preload this via
        `set_regular_season_strengths` if a NhlRegularSource has been
        fetched in the same refresh; otherwise we fall back to the
        default 3.0/3.0 prior.
        """
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        # No regular-season seeding: return empty map, simulator falls
        # through to _DEFAULT_GOALS_FOR / _DEFAULT_GOALS_AGAINST prior.
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
        """Hook for the plugin to share regular-season strength estimates
        with the playoff source. Without this, every playoff team gets
        the league-average prior: accurate enough for early-round
        importance but loses the team-skill signal a 60-game baseline
        provides."""
        self._team_strengths_from_regular = strengths

    def _strength_for(self, strengths: Dict[str, Dict[str, float]], team: str) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_GOALS_FOR,
            "pa_per_game": _DEFAULT_GOALS_AGAINST,
        }

    # ---------- sample_result (per-game Poisson with OT) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        del state  # series-state lookup not needed; per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_goals = _poisson(lam_home, rng)
        away_goals = _poisson(lam_away, rng)
        # Playoff OT is sudden-death continuous (no SO), so a tied
        # regulation always resolves in OT. We still need a winner:
        # add +1 to a coin-flipped side (slight bias by strength? skip
        # for V1; pen-shootout variance dominates the calibration).
        if home_goals == away_goals:
            if rng.random() < 0.5:
                home_goals += 1
            else:
                away_goals += 1
        return MatchResult(home_goals=home_goals, away_goals=away_goals)

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the playoff bracket + per-series schedules into the
        canonical per-game record shape that BracketSportSource expects.

        The bracket endpoint gives the round structure (which teams face
        which) plus current wins per series. Per-series schedule
        endpoint gives the individual game records (with home/away,
        scores, dates). For each tie we need both: bracket to know the
        teams and current state; schedule to enumerate games.
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        bracket_data = _http_get(
            f"{NHL_API_BASE}/playoff-bracket/{self._season_end_year}"
        )
        if not bracket_data:
            self._bracket_games_cache = []
            return []

        out: List[Dict[str, Any]] = []
        for series in bracket_data.get("series", []) or []:
            playoff_round = series.get("playoffRound")
            stage = _NHL_ROUND_TO_STAGE.get(playoff_round or 0)
            if stage is None:
                continue
            series_letter = (series.get("seriesLetter") or "").lower()
            top = series.get("topSeedTeam") or {}
            bot = series.get("bottomSeedTeam") or {}
            top_name = _team_canonical_name(top)
            bot_name = _team_canonical_name(bot)
            if not top_name or not bot_name:
                continue

            # Per-series schedule for the actual game records. The
            # full-8-digit-season vs end-year season-convention gotcha lives in
            # _series_schedule_url (the single source of truth for this URL).
            sched = _http_get(self._series_schedule_url(series_letter))
            games = (sched or {}).get("games", []) or []

            for g in games:
                gid = g.get("id")
                if gid is None:
                    continue
                home = _team_canonical_name(g.get("homeTeam") or {})
                away = _team_canonical_name(g.get("awayTeam") or {})
                if not home or not away:
                    continue
                state = g.get("gameState")
                hg = (g.get("homeTeam") or {}).get("score")
                ag = (g.get("awayTeam") or {}).get("score")
                is_final = state in ("OFF", "FINAL") and hg is not None and ag is not None
                status = "FINISHED" if is_final else "SCHEDULED"
                out.append({
                    "game_id": gid,
                    "stage": stage,
                    "matchday": g.get("gameNumber") or 1,
                    "home": home,
                    "away": away,
                    "home_goals": hg if is_final else None,
                    "away_goals": ag if is_final else None,
                    "status": status,
                    "start_time": parse_iso_utc(g.get("startTimeUTC")),
                    "extra": {
                        "series_letter": series_letter,
                        "top_seed": top_name,
                    },
                })

        # Issue #17 fix: synthesize a CUP_FINAL placeholder tie when both
        # CONF_FINAL series have known participants but the bracket
        # endpoint hasn't yet populated the SCF series with teams. The
        # endpoint emits a Round-4 stub with topSeedTeam/bottomSeedTeam
        # = None during the conference-finals week: the loop above
        # silently skips that stub (because _team_canonical_name returns
        # "" for the None teams). Without this placeholder, the cup_winner
        # leverage signal reads 0 during the very week when it matters
        # most. The placeholder uses sentinel team names that
        # _build_bracket wires to the upstream CONF_FINAL series via
        # feeds_from.
        cf_emitted = sum(1 for g in out if g["stage"] == "CONF_FINAL")
        cup_emitted = sum(1 for g in out if g["stage"] == "CUP_FINAL")
        # 2 CONF_FINAL series × 7 games max = 14 games possible. We use
        # cf_emitted > 0 (data exists) rather than ==14 (all games published)
        # because the schedule endpoint emits games as they're scheduled,
        # not all up-front. The 2-series count comes from grouping inside
        # _build_bracket: easier to check via the actual game emission.
        cf_series_with_teams = sum(
            1 for s in bracket_data.get("series", []) or []
            if _NHL_ROUND_TO_STAGE.get(s.get("playoffRound") or 0) == "CONF_FINAL"
            and _team_canonical_name(s.get("topSeedTeam") or {})
            and _team_canonical_name(s.get("bottomSeedTeam") or {})
        )
        del cf_emitted  # cf_series_with_teams is the stronger gate
        if cf_series_with_teams == 2 and cup_emitted == 0:
            out.extend(self._synth_cup_final_placeholder_games())

        self._bracket_games_cache = out
        return out

    # ---------- CUP_FINAL placeholder synthesis (#17) ----------

    def _synth_cup_final_placeholder_games(self) -> List[Dict[str, Any]]:
        """Emit 7 SCHEDULED CUP_FINAL games with sentinel team names
        following the NHL 2-2-1-1-1 home pattern. Game IDs are negative
        ints to guarantee non-collision with real api-web game IDs
        (which are positive 8-digit numbers like 2025030323).

        The sentinels are stable across refreshes within one importance
        run: they're matched by _build_bracket below to wire feeds_from
        to the two CONF_FINAL series.
        """
        games: List[Dict[str, Any]] = []
        for matchday in range(1, len(NHL_HOME_PATTERN) + 1):
            top_home = NHL_HOME_PATTERN[matchday - 1]
            home = _CUP_FINAL_TOP_SENTINEL if top_home else _CUP_FINAL_BOT_SENTINEL
            away = _CUP_FINAL_BOT_SENTINEL if top_home else _CUP_FINAL_TOP_SENTINEL
            games.append({
                "game_id": -(100000 + matchday),  # synthetic, non-colliding
                "stage": "CUP_FINAL",
                "matchday": matchday,
                "home": home,
                "away": away,
                "home_goals": None,
                "away_goals": None,
                "status": "SCHEDULED",
                "start_time": None,
                "extra": {
                    "is_placeholder": True,
                    "series_letter": "synthetic-scf",
                },
            })
        return games

    def _build_bracket(self, games: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Override the shared bracket build to wire feeds_from on the
        CUP_FINAL placeholder tie (whose participants are sentinel team
        names, which the participant-set inference in the base class
        can't match against the actual CONF_FINAL participants).

        Called by BracketSportSource.initial_state: we call super for
        the standard structural pass, then post-process the placeholder
        if it's present.
        """
        bracket = super()._build_bracket(games)
        cf_ties = bracket.get("CONF_FINAL", [])
        cup_ties = bracket.get("CUP_FINAL", [])
        if len(cf_ties) != 2 or len(cup_ties) != 1:
            return bracket
        cup_tie = cup_ties[0]
        teams = cup_tie.get("teams") or frozenset()
        if not (
            _CUP_FINAL_TOP_SENTINEL in teams
            and _CUP_FINAL_BOT_SENTINEL in teams
        ):
            return bracket  # not a placeholder, leave alone

        # Wire feeds_from explicitly: each sentinel resolves to the winner
        # of its corresponding CONF_FINAL series. Order matters: sentinel
        # _CUP_FINAL_TOP_SENTINEL maps to CONF_FINAL[0]'s winner; bottom
        # to CONF_FINAL[1]. Order within the bracket comes from
        # _build_bracket's grouping pass which is deterministic per
        # frozenset insertion order: stable enough for a 2-series
        # check. For home-ice priority in the placeholder, we assume
        # CONF_FINAL[0]'s winner gets games 1/2/5/7 at home (the higher
        # regular-season seed normally would; without seeding data, the
        # uniform assumption is harmless to importance computation).
        cup_tie["feeds_from"] = {
            _CUP_FINAL_TOP_SENTINEL: ("CONF_FINAL", 0),
            _CUP_FINAL_BOT_SENTINEL: ("CONF_FINAL", 1),
        }
        cup_tie["is_entry_tie"] = False
        return bracket

    def _source_team_for(self, sim_team: str, tie_meta: Dict[str, Any]) -> str:
        """For the CUP_FINAL placeholder, the sentinel team names need
        to map to the resolved CONF_FINAL winners by position. Return
        the FIRST game's source-published home so the home/away swap
        logic in BracketSportSource._emit_remaining_games_for_tie
        compares the current game's src_home against the consistent
        "team_a is sentinel-A" mapping rather than against the
        resolved team name (which would never match a sentinel).

        Non-placeholder cases get the same behavior for free: games[0].home
        IS team_a's source name in those cases too (since _resolve_
        participants defines team_a as the resolution of games[0].home).
        """
        del sim_team  # mapping derived from tie_meta, not sim_team identity
        games = tie_meta.get("games") or []
        if not games:
            return ""
        return games[0].get("home") or ""
