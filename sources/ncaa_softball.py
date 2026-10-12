"""NCAA Softball source: ESPN's unofficial site.api.espn.com.
No API key required.

Two source classes ship under the `enable_ncaa_softball` league (the
"Softball" half of the NCAA Baseball & Softball setting, #208):
  - `NcaaSoftballRegularSource(PointsBasedSportSource)`: regular-season
    win-count importance (LEAGUE_CONTEXTS["SBL"]).
  - `NcaaSoftballPlayoffSource(BestOfNSeriesSource)`: postseason.
    Currently models the best-of-3 stages: Super Regional and WCWS
    Championship Finals: both with clean ESPN game-number metadata.
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

Per-day iteration with an explicit page size is required: a peak
Saturday carries ~150 games, past ESPN's default 100-event page, and
the multi-day `dates=` forms either 400 or silently drop events. See
`_espn.SCOREBOARD_EVENT_LIMIT`.

Team-name canonicalization uses ESPN's `team.location` (school name:
"Oklahoma", "Texas") rather than the mascot ("Sooners", "Longhorns")
because EPG entries use the school name.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, attach_poll_ranks
from .bracket import BestOfNSeriesSource, DoubleEliminationSource, ForcedWinnerPoissonSampler
from .points_based import PointsBasedSportSource
from .._util import (
    extract_game_number_after_marker,
    parse_iso_utc,
    http_get_json,
)
from ._espn import (
    dedupe_events,
    event_identity,
    fetch_espn_poll,
    fetch_scoreboard_day,
    fetch_scoreboard_days,
    scoreboard_event_over,
    scoreboard_events,
    sweep_upcoming_events,
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
    """Patch point for this module's HTTP; the body is `_util.http_get_json`."""
    return http_get_json(requests, url, log=logger, tag="ncaa_softball", timeout=timeout)


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

    status = "FINISHED" if scoreboard_event_over(event) else "SCHEDULED"

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
    selection criteria (64-team field: same field size as baseball).
    The selection committee weights RPI + strength-of-schedule + wins,
    but win-count alone is a strong proxy for tournament status.

    Postseason games (season.type=3) are filtered out of both
    fetch_upcoming and _fetch_full_season_games: NcaaSoftballPlayoff
    Source owns those. Otherwise the regular-season win count would
    inflate by postseason wins, breaking the threshold bands.
    """

    league_context_code = "SBL"
    # The softball Top 25 is drawn from Division I (#198): 309 teams in the
    # 2025 season (Wikipedia, "2025 NCAA Division I softball season",
    # checked 2026-10-11). See SportSource.rank_field_size.
    rank_field_size = 309
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
        """Upcoming regular-season games through
        `_espn.sweep_upcoming_events` (window, lookback day, FINISHED and
        stale drops, dedupe; #256). Attaches the softball poll's ranks after
        the sweep, only when it found a game (`base.attach_poll_ranks`); a
        failed poll raises (#283)."""
        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            if _is_postseason_event(event):
                # Owned by NcaaSoftballPlayoffSource.
                return None
            rec = _extract_game_record(event)
            if rec is None:
                return None
            start = rec.get("start_time")
            if start is None:
                return None
            eid = rec.get("id")
            return GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=rec["home"],
                away=rec["away"],
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra={
                    "ncaasbl_game_id": eid,
                    # Identity key the simulator compares on; see GameRow.extra in base.py. #181
                    "game_id": event_identity(event),
                    "fd_competition_code": self.league_context_code,
                },
            )
        return attach_poll_ranks(
            sweep_upcoming_events(_http_get, f"{ESPN_BASE}/scoreboard", days_ahead, to_row),
            self._fetch_rankings,
        )

    def _fetch_rankings(self) -> Dict[str, int]:
        """{school: rank} from the ESPN.com/USA Softball Collegiate Top 25,
        the one poll ESPN exposes. Raises `RankingsUnavailable` when it did
        not answer (#283; see `_espn.fetch_espn_poll`)."""
        return fetch_espn_poll(_http_get, f"{ESPN_BASE}/rankings", _team_canonical_name)

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Per-day iteration from season start (Feb 1) through the end
        of the season (#229), via `_espn.fetch_scoreboard_day`.
        """
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1,
                                tzinfo=timezone.utc).date()
        # End of season = end of June (covers WCWS Finals).
        season_end_first = datetime(self.season_year, SEASON_END_MONTH, 1,
                                    tzinfo=timezone.utc).date()
        season_end = (season_end_first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        # Walk to the END of the season, never to now + lookahead (#229). This
        # list is the simulator's whole season: the fixtures still to play are
        # what decide who finishes where. Capping it at next week made the
        # season end next week, so no team could change band and importance
        # read 0 (every NFL game, measured 2026-10-10). Same bug #65 fixed for
        # MLS in mls_standings.py; tests/test_full_season_window.py guards all.
        end = season_end

        def keep(event: Dict[str, Any], key: Any) -> Optional[Dict[str, Any]]:
            if _is_postseason_event(event):
                # Postseason win/loss counts must NOT flow into
                # the regular-season Monte Carlo. See class
                # docstring.
                return None
            rec = _extract_game_record(event)
            if rec is not None:
                rec["id"] = key  # the event's identity, never None: see dedupe_events
            return rec

        return dedupe_events(scoreboard_events(
            data for _day, data in fetch_scoreboard_days(
                _http_get, f"{ESPN_BASE}/scoreboard", season_start, end,
            )
        ), keep)


# =====================================================================
# NcaaSoftballPlayoffSource: best-of-3 stages (Super Regional + WCWS Finals)
# =====================================================================


# ESPN's softball headlines use "Finals" (plural) where baseball uses
# "Final" (singular). Both sports share the "Super Regional - Game N"
# convention.
_SUPER_REGIONAL_MARKER = "Super Regional - Game "
_FINALS_MARKER = "Championship Finals - Game "


def _parse_softball_playoff_headline(headline: str) -> Tuple[Optional[str], Optional[int]]:
    """Map an ESPN softball postseason headline to (stage, game_index).
    Returns (None, None) for unmodeled stages (Regional, 8-team WCWS
    bracket: both lack headline game metadata in ESPN's data; see #43).

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


class _SoftballPlayoffStrengthsMixin(ForcedWinnerPoissonSampler):
    """Shared per-team Poisson strength tracking + sample_result for the
    two NCAA Softball playoff source classes (Super Regional + WCWS Finals
    via BestOfNSeriesSource; Regional + 8-team WCWS bracket via
    DoubleEliminationSource). Both pull regular-season strengths from
    NcaaSoftballRegularSource via `set_regular_season_strengths` and use
    the same Poisson sampling with extra-innings coin-flip tiebreak.
    """

    _team_strengths_from_regular: Optional[Dict[str, Dict[str, float]]] = None

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]],
    ) -> None:
        """Hook for plugin.py to seed per-team scoring/conceding rates
        from the regular-season source."""
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


class NcaaSoftballPlayoffSource(_SoftballPlayoffStrengthsMixin, BestOfNSeriesSource):
    """NCAA Softball postseason: Super Regional + WCWS Finals.

    Both stages are best-of-3 (`SERIES_LENGTH = 3`). Regional double-elim
    and the 8-team WCWS bracket in OKC are owned by NcaaSoftballPlayoff
    BracketSource (sibling source) which extends DoubleEliminationSource
    and uses chronological inference + headline site labels for grouping.

    The depth structure (SB_REG=0 → SB_SR=1 → WCWS=2 → WCWS_F=3 →
    WCWS_W=4) is shared across the two playoff sources; this class
    handles SB_SR + WCWS_F, the sibling handles SB_REG + WCWS.

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
        """Upcoming postseason games through `_espn.sweep_upcoming_events`
        (window, lookback day, FINISHED and stale drops, dedupe; #256), with
        `_is_postseason_event` filtering IN postseason."""
        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            if not _is_postseason_event(event):
                return None
            return self._event_to_game_row(event)
        return sweep_upcoming_events(_http_get, f"{ESPN_BASE}/scoreboard", days_ahead, to_row)

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
                "game_id": event_identity(event),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "matchday": game_num,
                "headline": headline,
            },
        )

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
        payloads = self._sweep_bracket_days(
            season_start, season_end,
            lambda d: fetch_scoreboard_day(_http_get, f"{ESPN_BASE}/scoreboard", d),
        )
        if payloads is None:
            # A day failed: NOT cached (#245; see BracketSportSource).
            return []
        for data in payloads:
            for event in data.get("events") or []:
                if not _is_postseason_event(event):
                    continue
                rec = self._event_to_bracket_record(event)
                if rec is not None:
                    out.append(rec)

        return self._cache_bracket_games(out)

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

        status = "FINISHED" if scoreboard_event_over(event) else "SCHEDULED"
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
            "game_id": event_identity(event),  # never None: see _espn.dedupe_events
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


# =====================================================================
# NcaaSoftballPlayoffBracketSource: Regional + 8-team WCWS double-elim
# =====================================================================


# Site name is everything between "NCAA Softball Championship - " and
# " Regional" (e.g., "Lincoln Regional").
_REGIONAL_SITE_REGEX = re.compile(
    r"NCAA Softball Championship - (?P<site>[^-]+? Regional)\b"
)

# Headline substrings that classify a postseason game as a stage of the
# Regional double-elim. Parallels the baseball patterns:
#   - "NCAA Softball Championship - Lincoln Regional - Game 1"
#   - "NCAA Softball Championship - Lincoln Regional - Elimination Game"
#   - "NCAA Softball Championship - Lincoln Regional - {team} advances to Super Regional"
_REGIONAL_MARKERS = (
    "Regional - Game ",
    "Regional - Elimination Game",
    "Regional - ",
)

# 8-team WCWS bracket headline substrings. Note "Women's College World
# Series" vs baseball's "Men's College World Series".
_WCWS_BRACKET_MARKERS = (
    "Women's College World Series - Double Elimination Round",
    "Women's College World Series - Elimination Game",
    " advances to Championship Series",
)


def _parse_softball_bracket_headline(headline: str) -> Tuple[Optional[str], Optional[str]]:
    """Map an ESPN softball postseason headline to (stage, partial_grouping_key).

    For Regional games, returns ("SB_REG", "<site> Regional"). For 8-team
    WCWS bracket games, returns ("WCWS", None): the sub-bracket
    assignment is resolved chronologically by `_classify_wcws_sub_brackets`.

    Returns (None, None) for best-of-3 SB_SR / WCWS_F headlines (owned by
    the sibling NcaaSoftballPlayoffSource) and non-classifiable events.
    """
    if not headline:
        return None, None
    if _SUPER_REGIONAL_MARKER in headline or _FINALS_MARKER in headline:
        return None, None
    m = _REGIONAL_SITE_REGEX.search(headline)
    if m and any(marker in headline for marker in _REGIONAL_MARKERS):
        return "SB_REG", m.group("site")
    if any(marker in headline for marker in _WCWS_BRACKET_MARKERS):
        return "WCWS", None
    return None, None


try:
    from zoneinfo import ZoneInfo
    _WCWS_VENUE_TZ = ZoneInfo("America/Chicago")  # OKC (Devon Park)
except ImportError:
    _WCWS_VENUE_TZ = timezone.utc


def _classify_wcws_sub_brackets(
    wcws_games: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Day-partition heuristic for the 8-team WCWS bracket in OKC. See
    the baseball sibling docstring for verification details + the
    UTC-vs-venue-local rationale (WCWS evening games in CT cross UTC
    midnight, so partition MUST run in America/Chicago local time)."""
    games_sorted = sorted(
        wcws_games,
        key=lambda g: g.get("start_time") or datetime(2099, 1, 1, tzinfo=timezone.utc),
    )
    sub_bracket: Dict[str, str] = {}
    days_seen: List[Any] = []
    for g in games_sorted:
        ts = g.get("start_time")
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        d = ts.astimezone(_WCWS_VENUE_TZ).date()
        if d not in days_seen:
            days_seen.append(d)
        home = g.get("home")
        away = g.get("away")
        if not home or not away:
            continue
        if d == days_seen[0]:
            sub_bracket.setdefault(home, "WCWS_sub1")
            sub_bracket.setdefault(away, "WCWS_sub1")
        elif len(days_seen) >= 2 and d == days_seen[1]:
            sub_bracket.setdefault(home, "WCWS_sub2")
            sub_bracket.setdefault(away, "WCWS_sub2")
        else:
            home_sb = sub_bracket.get(home)
            away_sb = sub_bracket.get(away)
            if home_sb and not away_sb:
                sub_bracket[away] = home_sb
            elif away_sb and not home_sb:
                sub_bracket[home] = away_sb
    return sub_bracket


def _wcws_meta_from_raw_events(
    raw_events: List[Tuple[Dict[str, Any], str, Optional[str], Any]],
) -> List[Dict[str, Any]]:
    """(home, away, start_time) projection for WCWS events, for
    _fetch_bracket_games only. See the baseball sibling helper for why
    fetch_upcoming does not classify."""
    out: List[Dict[str, Any]] = []
    for event, stage, _partial_key, _identity in raw_events:
        if stage != "WCWS":
            continue
        comps = event.get("competitions") or []
        if not comps:
            continue
        competitors = comps[0].get("competitors") or []
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            continue
        out.append({
            "home": _team_canonical_name(home.get("team") or {}),
            "away": _team_canonical_name(away.get("team") or {}),
            "start_time": parse_iso_utc(event.get("date")),
        })
    return out


class NcaaSoftballPlayoffBracketSource(_SoftballPlayoffStrengthsMixin, DoubleEliminationSource):
    """NCAA Softball postseason BRACKET stages: 4-team Regional double-
    elim (16 sites) and the 8-team WCWS bracket (modeled as two 4-team
    sub-brackets). Sibling to NcaaSoftballPlayoffSource which handles
    the cleanly-labeled best-of-3 stages (Super Regional + WCWS Finals).

    Tie grouping_key:
      - SB_REG: site name from headline (e.g., "Lincoln Regional").
      - WCWS: "WCWS_sub1" / "WCWS_sub2" assigned by the day-partition
        heuristic in `_classify_wcws_sub_brackets`.
    """

    KO_STAGES = ("SB_REG", "WCWS")
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
        # WCWS sub-bracket winner → advances to WCWS_F (depth 3, handled by
        # sibling source). SB_REG winner falls through to default
        # `stage_depth + 1` → depth 1 = SB_SR depth.
        if stage == "WCWS":
            return "WCWS_F"
        return None

    def _tie_grouping_key(self, game: Dict[str, Any]) -> Optional[str]:
        return (game.get("extra") or {}).get("grouping_key")

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Upcoming postseason games whose headline matches a bracket stage
        (Regional or 8-team WCWS bracket), through
        `_espn.sweep_upcoming_events` (window, lookback day, FINISHED and
        stale drops, dedupe; #256)."""
        def staged(event: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], str, Optional[str]]]:
            if not _is_postseason_event(event):
                return None
            comps = event.get("competitions") or []
            if not comps:
                return None
            headline = ""
            for note in (comps[0].get("notes") or []):
                if note.get("type") == "event":
                    headline = note.get("headline") or ""
                    break
            stage, partial_key = _parse_softball_bracket_headline(headline)
            if stage is None:
                return None
            return event, stage, partial_key

        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            tagged = staged(event)
            return self._event_to_game_row(*tagged) if tagged is not None else None

        return sweep_upcoming_events(_http_get, f"{ESPN_BASE}/scoreboard", days_ahead, to_row)

    def _event_to_game_row(
        self,
        event: Dict[str, Any],
        stage: str,
        partial_key: Optional[str],
    ) -> Optional[GameRow]:
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
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None
        start = parse_iso_utc(event.get("date"))
        if start is None:
            return None

        # A Regional's site is in its own headline. A WCWS game's sub-bracket is
        # NOT knowable from the upcoming window: it is read off opening-day
        # pairings, which by Day 3 are outside the window (and FINISHED, so
        # swept out of it, before that). DO NOT guess it here: the old
        # window-relative guess labelled whatever came first in the window
        # sub-bracket 1, and dropped a game whose teams it had not seen (#256
        # review). Leave it None. place_upcoming stamps the bracket's own key
        # on every row it places, and a row it cannot place resolves its tie
        # by team set in `_find_tie_meta`, which a WRONG key would defeat.
        grouping_key = partial_key if stage == "SB_REG" else None

        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break

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
                "game_id": event_identity(event),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "grouping_key": grouping_key,
                "headline": headline,
            },
        )

    # ---------- bracket fetch (full-season for Monte Carlo) ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        season_start = datetime(self.season_year, 5, 15, tzinfo=timezone.utc).date()
        season_end = datetime(self.season_year, 6, 15, tzinfo=timezone.utc).date()
        payloads = self._sweep_bracket_days(
            season_start, season_end,
            lambda d: fetch_scoreboard_day(_http_get, f"{ESPN_BASE}/scoreboard", d),
        )
        if payloads is None:
            # A day failed: NOT cached (#245; see BracketSportSource).
            return []

        def keep(
            event: Dict[str, Any], key: Any,
        ) -> Optional[Tuple[Dict[str, Any], str, Optional[str], Any]]:
            if not _is_postseason_event(event):
                return None
            comps = event.get("competitions") or []
            if not comps:
                return None
            headline = ""
            for note in (comps[0].get("notes") or []):
                if note.get("type") == "event":
                    headline = note.get("headline") or ""
                    break
            stage, partial_key = _parse_softball_bracket_headline(headline)
            if stage is None:
                return None
            return (event, stage, partial_key, key)

        raw = dedupe_events(scoreboard_events(payloads), keep)

        wcws_sub_by_team = _classify_wcws_sub_brackets(
            _wcws_meta_from_raw_events(raw)
        )

        out: List[Dict[str, Any]] = []
        for event, stage, partial_key, key in raw:
            rec = self._event_to_bracket_record(event, stage, partial_key, wcws_sub_by_team, key)
            if rec is not None:
                out.append(rec)
        return self._cache_bracket_games(out)

    def _event_to_bracket_record(
        self,
        event: Dict[str, Any],
        stage: str,
        partial_key: Optional[str],
        wcws_sub_by_team: Dict[str, str],
        key: Any,
    ) -> Optional[Dict[str, Any]]:
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
        if home_team.upper() == "TBD" or away_team.upper() == "TBD":
            return None

        if stage == "SB_REG":
            grouping_key = partial_key
        else:  # WCWS
            grouping_key = wcws_sub_by_team.get(home_team) or wcws_sub_by_team.get(away_team)
        if grouping_key is None:
            return None

        status = "FINISHED" if scoreboard_event_over(event) else "SCHEDULED"
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
            "game_id": key,  # the event's identity, never None: see _espn.dedupe_events
            "stage": stage,
            "matchday": 1,
            "home": home_team,
            "away": away_team,
            "home_goals": hr,
            "away_goals": ar,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"grouping_key": grouping_key},
        }
