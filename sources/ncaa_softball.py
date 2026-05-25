"""NCAA Softball source — ESPN's unofficial site.api.espn.com.
No API key required.

Two source classes ship under the `enable_ncaa_softball` toggle:
  - `NcaaSoftballRegularSource(PointsBasedSportSource)`: regular-season
    win-count importance (LEAGUE_CONTEXTS["SBL"]).
  - `NcaaSoftballPlayoffSource(BestOfNSeriesSource)`: postseason.
    Currently models the best-of-3 stages — Super Regional and WCWS
    Championship Finals — both with clean ESPN game-number metadata.
    Regional (4-team double-elim per site) and the 8-team WCWS bracket
    in OKC are tracked in #43: ESPN headlines on those stages are
    "Women's College World Series - Double Elimination Round" with no
    game-number or bracket-position metadata, so chronological
    inference is needed.

API path: ESPN groups college softball under the `baseball` sport
namespace (NOT `softball`). The scoreboard endpoint:
  /apis/site/v2/sports/baseball/college-softball/scoreboard?dates=YYYYMMDD

Postseason events carry `season.type=3` and headlines like
"NCAA Softball Championship - Lincoln Super Regional - Game 2" or
"Women's College World Series Championship Finals - Game 1". Note:
softball uses "Finals" (plural) where baseball uses "Final" (singular).

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
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from .points_based import PointsBasedSportSource
from .._util import (
    extract_game_number_after_marker,
    parse_iso_utc,
    poisson_sample as _poisson,
)

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


def _is_postseason_event(event: Dict[str, Any]) -> bool:
    """ESPN postseason discriminator. See
    ncaa_baseball._is_postseason_event for the full stage → type
    mapping (3=Regional, 4=Super Regional, 5=WCWS bracket, 6=Finals).
    """
    season_type = (event.get("season") or {}).get("type")
    return isinstance(season_type, int) and season_type > 2


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


class NcaaSoftballRegularSource(PointsBasedSportSource):
    """D1 NCAA Softball regular-season importance.

    Win-count threshold bands tuned against historical NCAA Tournament
    selection criteria (64-team field — same field size as baseball).
    The selection committee weights RPI + strength-of-schedule + wins,
    but win-count alone is a strong proxy for tournament status.

    Postseason games (season.type=3) are filtered out of both
    fetch_upcoming and _fetch_full_season_games — NcaaSoftballPlayoff
    Source owns those. Otherwise the regular-season win count would
    inflate by postseason wins, breaking the threshold bands.
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
                if _is_postseason_event(event):
                    # Owned by NcaaSoftballPlayoffSource.
                    continue
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
                    if _is_postseason_event(event):
                        # Postseason win/loss counts must NOT flow into
                        # the regular-season Monte Carlo. See class
                        # docstring.
                        continue
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = rec
            day += timedelta(days=1)
        return list(seen.values())


# =====================================================================
# NcaaSoftballPlayoffSource — best-of-3 stages (Super Regional + WCWS Finals)
# =====================================================================


# ESPN's softball headlines use "Finals" (plural) where baseball uses
# "Final" (singular). Both sports share the "Super Regional - Game N"
# convention.
_SUPER_REGIONAL_MARKER = "Super Regional - Game "
_FINALS_MARKER = "Championship Finals - Game "


def _parse_softball_playoff_headline(headline: str) -> Tuple[Optional[str], Optional[int]]:
    """Map an ESPN softball postseason headline to (stage, game_index).
    Returns (None, None) for unmodeled stages (Regional, 8-team WCWS
    bracket — both lack headline game metadata in ESPN's data; see #43).

    Patterns observed in 2025-2026 ESPN data:
      - "NCAA Softball Championship - Lincoln Super Regional - Game 1"
      - "NCAA Softball Championship - Lincoln Super Regional - Game 3 (if necessary)"
      - "Women's College World Series Championship Finals - Game 1"
    """
    if not headline:
        return None, None
    if _SUPER_REGIONAL_MARKER in headline:
        return "SB_SR", extract_game_number_after_marker(headline, _SUPER_REGIONAL_MARKER)
    if _FINALS_MARKER in headline:
        return "WCWS_F", extract_game_number_after_marker(headline, _FINALS_MARKER)
    return None, None


class NcaaSoftballPlayoffSource(BestOfNSeriesSource):
    """NCAA Softball postseason: Super Regional + WCWS Finals.

    Both stages are best-of-3 (`SERIES_LENGTH = 3`). Regional double-elim
    and the 8-team WCWS bracket in OKC carry no headline game-number
    metadata in ESPN's data and require chronological inference —
    tracked in #43.

    The depth structure (SB_REG=0 → SB_SR=1 → WCWS=2 → WCWS_F=3 →
    WCWS_W=4) is set up so #43 can extend KO_STAGES without touching
    threshold labels. The `okc_bound` band (depth 2) fires for Super
    Regional WINNERS via the default `stage_depth + 1` advance rule.

    Strength sharing: pre-postseason, the plugin pulls regular-season
    strengths from NcaaSoftballRegularSource and seeds them via
    `set_regular_season_strengths`. Fallback default is 5.0 runs per
    side (softball league average).
    """

    KO_STAGES = ("SB_SR", "WCWS_F")
    SERIES_LENGTH = 3
    supports_importance = True

    def __init__(self, season_year: Optional[int] = None) -> None:
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year
            if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )
        self._initial_state_cache: Optional[Dict[str, Any]] = None
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAASBL"

    @property
    def sport_label(self) -> str:
        return "NCAA Softball Postseason"

    def _league_context_code(self) -> str:
        return "WCWS_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        if stage == "WCWS_F":
            return "WCWS_W"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if not data:
                continue
            for event in data.get("events") or []:
                if not _is_postseason_event(event):
                    continue
                row = self._event_to_game_row(event)
                if row is None:
                    continue
                eid = row.extra.get("ncaasbl_game_id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                out.append(row)
        return out

    def _event_to_game_row(self, event: Dict[str, Any]) -> Optional[GameRow]:
        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break
        stage, game_num = _parse_softball_playoff_headline(headline)
        if stage is None or game_num is None:
            return None
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
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None
        start = parse_iso_utc(event.get("date"))
        if start is None:
            return None
        return GameRow(
            sport_prefix=self.sport_prefix,
            sport_label=self.sport_label,
            home=home_team,
            away=away_team,
            rank_home=None,
            rank_away=None,
            start_time=start,
            extra={
                "ncaasbl_game_id": event.get("id"),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "matchday": game_num,
                "headline": headline,
            },
        )

    # ---------- strengths ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]]
    ) -> None:
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

    # ---------- sample_result ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        del state
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.1, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.1, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_runs = _poisson(lam_home, rng)
        away_runs = _poisson(lam_away, rng)
        if home_runs == away_runs:
            if rng.random() < 0.5:
                home_runs += 1
            else:
                away_runs += 1
        return MatchResult(home_goals=home_runs, away_goals=away_runs)

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Sweep the WCWS date window day-by-day, filter to postseason
        events whose headline matches a modeled stage, emit the bracket
        per-game record shape.

        Window: May 15 (Regional play opens the postseason; we start a
        few days early to catch any timezone wraparound) through
        June 15 (Championship Finals worst-case Game 3 slip).
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        season_start = datetime(self.season_year, 5, 15, tzinfo=timezone.utc).date()
        season_end = datetime(self.season_year, 6, 15, tzinfo=timezone.utc).date()
        day = season_start
        while day <= season_end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    if not _is_postseason_event(event):
                        continue
                    rec = self._event_to_bracket_record(event)
                    if rec is not None:
                        out.append(rec)
            day += timedelta(days=1)

        self._bracket_games_cache = out
        return out

    def _event_to_bracket_record(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break
        stage, game_num = _parse_softball_playoff_headline(headline)
        if stage is None or game_num is None:
            return None
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
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None

        status_type = (comp.get("status") or {}).get("type") or {}
        completed = bool(status_type.get("completed"))
        state = (status_type.get("state") or "").lower()
        if completed or state == "post":
            status = "FINISHED"
        else:
            status = "SCHEDULED"
        try:
            hr = int(home.get("score")) if status == "FINISHED" else None
        except (TypeError, ValueError):
            hr = None
        try:
            ar = int(away.get("score")) if status == "FINISHED" else None
        except (TypeError, ValueError):
            ar = None
        if status == "FINISHED" and (hr is None or ar is None):
            status = "SCHEDULED"
            hr = None
            ar = None

        return {
            "game_id": event.get("id"),
            "stage": stage,
            "matchday": game_num,
            "home": home_team,
            "away": away_team,
            "home_goals": hr,
            "away_goals": ar,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"headline": headline},
        }
