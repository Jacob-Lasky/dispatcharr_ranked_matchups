"""NCAA Baseball source: ESPN's unofficial `site.api.espn.com` API.

No API key required. ESPN's API is undocumented but stable enough for
a homelab TV-guide curator. If it ever 404s, `fetch_upcoming` and
`_fetch_full_season_games` return [] and the affected sport silently
drops out of the guide for that refresh cycle: graceful-degrade is
already the contract.

Two source classes ship under the `enable_ncaa_baseball` toggle:
  - `NcaaBaseballRegularSource(PointsBasedSportSource)`: regular-season
    win-count importance. Tournament-bubble through national-seed bands
    (see LEAGUE_CONTEXTS["BSB"]).
  - `NcaaBaseballPlayoffSource(BestOfNSeriesSource)`: postseason.
    Currently models the best-of-3 stages: Super Regional and MCWS
    Championship Final: both of which carry clean ESPN game-number
    metadata. Regional (4-team double-elim per site) and the 8-team
    MCWS bracket in Omaha are tracked in #43: ESPN headlines on those
    stages carry no game-number or bracket-position metadata, requiring
    chronological inference.

API endpoints used:
  - /apis/site/v2/sports/baseball/college-baseball/scoreboard
    - Daily scoreboard. `dates=YYYYMMDD` returns all games for that day
      (the `dates=YYYYMMDD-YYYYMMDD` range form silently caps at 25
      events). Used by both fetch_upcoming and _fetch_full_season_games.
      Postseason events carry `season.type=3` and `competition.notes[]
      .headline` like "NCAA Baseball Championship - Auburn Super Regional
      - Game 2".
  - /apis/site/v2/sports/baseball/college-baseball/rankings
    - D1Baseball.com Top 25 (the canonical D1 poll). Used to populate
      rank_home / rank_away on GameRow records so the rank-pair signal
      fires for marquee matchups.

Team name canonicalization: ESPN returns `team.location` ("UCLA") and
`team.name` ("Bruins"). We use `team.location` because that's what
typically appears in EPG titles ("UCLA at Texas") rather than the
mascot ("Bruins at Longhorns"). The school name is the stable join
key for matching.
"""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource, DoubleEliminationSource
from .points_based import PointsBasedSportSource
from .._util import (
    extract_game_number_after_marker,
    parse_iso_utc,
    poisson_sample as _poisson,
)
from ._espn import extract_espn_scoreboard_event

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_baseball")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball"

# D1 baseball season runs Feb through June (CWS finals typically third
# week of June). We sweep month by month so a single huge range doesn't
# risk pagination cutoffs from ESPN's API.
SEASON_START_MONTH = 2   # February
SEASON_END_MONTH = 6     # June (CWS finals)

# Default per-team scoring rate priors for teams the simulator hasn't
# seen yet (transfers, early-season cold starts). D1 baseball averages
# ~6 runs/team/game over the full season.
_DEFAULT_RUNS_FOR = 6.0
_DEFAULT_RUNS_AGAINST = 6.0


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """ESPN unofficial API wrapper. Returns the parsed JSON or None on
    any error (4xx / 5xx / connection failure / JSON decode). Logs at
    WARNING so silent degradation is observable in the dispatcharr log
    when the API misbehaves.
    """
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_baseball] %s → %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_baseball] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN returns both `team.location` (school: 'UCLA') and `team.name`
    (mascot: 'Bruins'). EPG provider titles use the school name. Fall
    back to nickname / abbreviation if location is missing on some
    edge-case competitor entries (occasionally happens for non-D1
    opponents in early-season scrimmages).
    """
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _is_postseason_event(event: Dict[str, Any]) -> bool:
    """ESPN tags NCAA Baseball / Softball postseason events with
    `season.type` in the 3-6 range: each stage gets its own type
    value (verified against live 2026 data):
      - 2: Regular season (NOT postseason).
      - 3: Regional round (4-team double-elim per site).
      - 4: Super Regional round (best-of-3, 8 sites).
      - 5: 8-team Men's / Women's College World Series bracket in
           Omaha / OKC (double-elim).
      - 6: Championship Finals (best-of-3 in Omaha / OKC).

    The regular-season source filters these out; the playoff source
    filters them in. Headline parsing further narrows to the stages
    actually modeled (SUPER_REGIONAL + FINALS); the Regional and 8-team
    MCWS bracket stages are headline-metadata-poor and tracked in #43.

    DO NOT use the `notes[].headline` "Championship" substring as a
    postseason discriminator: D1 conference tournaments in May use
    that same word and are NOT postseason. season.type is the
    authoritative tag from ESPN.

    DO NOT narrow to `type == 3` only: that was the original (wrong)
    assumption from the issue body and would mute Super Regional and
    Finals coverage entirely (those are types 4 and 6).
    """
    season_type = (event.get("season") or {}).get("type")
    return isinstance(season_type, int) and season_type > 2


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical PointsBased
    game record. Returns None if the event isn't a two-team competition
    we can score (cancellations, postponements with no competitors, etc.).
    The shared parser handles status / score / FINISHED-without-scores
    demotion; ncaa_baseball's previous explicit `state == "in"` branch
    is collapsed because the shared parser's else-branch produces the
    same SCHEDULED outcome for in-progress games."""
    return extract_espn_scoreboard_event(event, team_namer=_team_canonical_name)


class NcaaBaseballRegularSource(PointsBasedSportSource):
    """NCAA Division I baseball regular-season importance.

    Win-count threshold bands tuned against historical NCAA Tournament
    selection criteria. The 64-team field invites roughly the top D1
    teams by a blend of RPI/wins/strength-of-schedule; 35+ wins is the
    rough at-large cutoff line, and 45+ wins put a team in national-seed
    contention.

    Postseason games (season.type=3 in ESPN's tags) are filtered out
    of both fetch_upcoming and _fetch_full_season_games: NcaaBaseball
    PlayoffSource owns those. Otherwise the regular-season win count
    would inflate by postseason wins, breaking the threshold bands.
    """

    league_context_code = "BSB"
    _DEFAULT_POINTS_FOR = _DEFAULT_RUNS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_RUNS_AGAINST

    def __init__(self, season_year: Optional[int] = None) -> None:
        super().__init__()
        # NCAA baseball seasons are named by their calendar year (the
        # 2026 season starts Feb 2026, ends June 2026). Default to the
        # current calendar year; pre-February treat as last season.
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year
            if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )

    @property
    def sport_prefix(self) -> str:
        return "NCAABSB"

    @property
    def sport_label(self) -> str:
        return "NCAA Baseball"

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Hit the scoreboard endpoint per-day covering today through
        `days_ahead` days. Populate rank_home/rank_away from the
        D1Baseball.com Top 25 so the rank-pair signal fires for marquee
        matchups. Per-day rather than range because ESPN's range
        endpoint silently caps at 25 events (see _fetch_full_season_games
        comment).
        """
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
                    # Owned by NcaaBaseballPlayoffSource. Skipping here
                    # prevents duplicate GameRow records under one
                    # espn_event_id.
                    continue
                rec = _extract_game_record(event)
                if rec is None:
                    continue
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                home = rec["home"]
                away = rec["away"]
                start = rec.get("start_time")
                if start is None:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=rankings.get(home),
                    rank_away=rankings.get(away),
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "fd_competition_code": self.league_context_code,
                    },
                ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Walk the season day-by-day via the single-date scoreboard
        endpoint. Dedupe by event id. Returns the canonical shape
        PointsBasedSportSource expects.

        DO NOT use the `dates=YYYYMMDD-YYYYMMDD` range syntax: empirically
        ESPN's scoreboard endpoint silently caps range responses at 25
        events regardless of the `limit` parameter. Single-day queries
        (`dates=YYYYMMDD`) return ALL games for that day (~70-100 during
        peak season). Per-day iteration is ~110 calls Feb-now, ~10s total
        for a refresh.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1, tzinfo=timezone.utc).date()
        # Stop at min(current date + 7 lookahead, season end). Pre-season
        # we still walk into the season's first week to catch opening day.
        now = datetime.now(timezone.utc).date()
        # End of season is the last day of SEASON_END_MONTH.
        season_end_first = datetime(self.season_year, SEASON_END_MONTH, 1, tzinfo=timezone.utc).date()
        season_end = (season_end_first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        end = min(now + timedelta(days=7), season_end)
        if end < season_start:
            # Off-season / pre-season: no games yet.
            return []
        day = season_start
        while day <= end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    if _is_postseason_event(event):
                        # Postseason win/loss counts must NOT flow into
                        # the regular-season Monte Carlo: would corrupt
                        # the win-count threshold bands. See class
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

    # ---------- Rankings (D1Baseball.com poll) ----------

    def _fetch_rankings(self) -> Dict[str, int]:
        """Return {canonical_team_name: ranking_number} for the D1Baseball
        Top 25. ESPN exposes one poll; if it's missing or empty we
        gracefully return {} and the rank-pair signal sits out the
        refresh cycle.
        """
        data = _http_get(f"{ESPN_BASE}/rankings")
        if not data:
            return {}
        ranks_by_team: Dict[str, int] = {}
        polls = data.get("rankings") or []
        if not polls:
            return ranks_by_team
        # Prefer the D1Baseball poll; if multiple polls appear later
        # we'd want to make this selectable, but for now there's just one.
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


# =====================================================================
# NcaaBaseballPlayoffSource: best-of-3 stages (Super Regional + MCWS Final)
# =====================================================================


# Headline → (stage, game_index) parser. ESPN's 2025-2026 headlines for
# the best-of-3 stages have always included a "Game N" suffix. The marker
# strings below MUST stay synchronized with ESPN's exact headline text;
# if ESPN ever drops the " - Game " separator (or pluralizes "Final"
# on baseball the way they do for "Finals" on softball) the parser
# returns (None, None) and the events fall through to the favorite +
# rank-pair signals only: same graceful-degrade contract as the
# rest of the source.
_SUPER_REGIONAL_MARKER = "Super Regional - Game "
_FINALS_MARKER = "Championship Final - Game "


def _parse_baseball_playoff_headline(headline: str) -> Tuple[Optional[str], Optional[int]]:
    """Map an ESPN postseason headline to (stage, game_index). Returns
    (None, None) for Regional games (no game number, no bracket position)
    and MCWS 8-team bracket games ("Men's College World Series - Double
    Elimination Round" / "Elimination Game"): those stages are tracked
    in #43.

    Patterns observed in 2025-2026 ESPN data:
      - "NCAA Baseball Championship - Auburn Super Regional - Game 1"
      - "NCAA Baseball Championship - Auburn Super Regional - Game 3 (if necessary)"
      - "Men's College World Series Championship Final - Game 1"
    """
    if not headline:
        return None, None
    if _SUPER_REGIONAL_MARKER in headline:
        return "BSB_SR", extract_game_number_after_marker(headline, _SUPER_REGIONAL_MARKER)
    if _FINALS_MARKER in headline:
        return "MCWS_F", extract_game_number_after_marker(headline, _FINALS_MARKER)
    return None, None


class _BaseballPlayoffStrengthsMixin:
    """Shared per-team Poisson strength tracking + sample_result for the
    two NCAA Baseball playoff source classes (Super Regional + MCWS Finals
    via BestOfNSeriesSource; Regional + 8-team MCWS bracket via
    DoubleEliminationSource). Both pull regular-season strengths from
    NcaaBaseballRegularSource via `set_regular_season_strengths` and use
    the same Poisson sampling with extra-innings coin-flip tiebreak.
    """

    # Defined here so type-checkers know the attr exists; concrete
    # subclasses initialize it in __init__ to avoid the class-level
    # singleton pitfall.
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
        # NCAA postseason has no draws: coin-flip the +1 to break the tie.
        if home_runs == away_runs:
            if rng.random() < 0.5:
                home_runs += 1
            else:
                away_runs += 1
        return MatchResult(home_goals=home_runs, away_goals=away_runs)


class NcaaBaseballPlayoffSource(_BaseballPlayoffStrengthsMixin, BestOfNSeriesSource):
    """NCAA Baseball postseason: Super Regional + MCWS Finals.

    Both stages are best-of-3 (`SERIES_LENGTH = 3`) with ESPN headlines
    that carry the game number, so the BestOfNSeriesSource infrastructure
    fits cleanly. The Regional (4-team double-elim per site) and 8-team
    MCWS bracket stages are owned by NcaaBaseballPlayoffBracketSource
    (sibling source), which extends DoubleEliminationSource and uses
    chronological inference + headline site labels for grouping.

    The depth structure (BSB_REG=0 → BSB_SR=1 → MCWS=2 → MCWS_F=3 →
    MCWS_W=4) is shared across the two playoff sources; this class
    handles BSB_SR + MCWS_F, the sibling handles BSB_REG + MCWS.

    Strength sharing: pre-postseason, the plugin pulls regular-season
    strength estimates from NcaaBaseballRegularSource and seeds them
    via `set_regular_season_strengths`. Without this hook, postseason
    sampling falls back to the 6-runs-per-team default: workable but
    less informative than per-team Poisson rates from the 55-game
    regular season.
    """

    KO_STAGES = ("BSB_SR", "MCWS_F")
    SERIES_LENGTH = 3
    supports_importance = True

    def __init__(self, season_year: Optional[int] = None) -> None:
        # BestOfNSeriesSource's parent (BracketSportSource) inherits from
        # SportSource which has no __init__ args. Match the MlbPlayoffSource
        # pattern: own all caches in __init__ rather than relying on
        # class-level None defaults (which leak across instances during
        # tests).
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
        return "NCAABSB"

    @property
    def sport_label(self) -> str:
        return "NCAA Baseball Postseason"

    def _league_context_code(self) -> str:
        return "MCWS_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # MCWS Final winner → MCWS_W (depth 4). Super Regional winner
        # advances via the default `stage_depth + 1` rule → depth 2,
        # which is the MCWS bracket depth (modeled in #43).
        if stage == "MCWS_F":
            return "MCWS_W"
        return None

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day postseason games. Same per-day scoreboard
        sweep as the regular-season source but with `_is_postseason_event`
        flipped (filter IN postseason). Headlines are then parsed via
        `_parse_baseball_playoff_headline`; only the modeled stages
        (Super Regional + MCWS Finals) survive: Regional and 8-team
        MCWS bracket games are silently dropped here AND from the
        regular-season source's sibling fetch. Those stages are tracked
        in #43.

        DO NOT lift the headline filter without also implementing the
        #43 chronological inference: doing so would emit Regional games
        with `fd_competition_code="MCWS_PO"` and the importance simulator
        would see them with no upstream feeders, producing an under-
        informed leverage signal that's worse than no signal.
        """
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
                eid = row.extra.get("espn_event_id")
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
        stage, game_num = _parse_baseball_playoff_headline(headline)
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
        # TBD-vs-TBD games appear in ESPN's data before participants are
        # determined (e.g., Finals before the MCWS bracket resolves).
        # Skip them: the bracket source can't track a placeholder team
        # and the EPG client doesn't surface a "TBD" matchup usefully.
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
                "espn_event_id": event.get("id"),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "matchday": game_num,
                "headline": headline,
            },
        )

    # ---------- bracket fetch ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Sweep the postseason date window day-by-day, filter to
        postseason events whose headline matches a modeled stage, and
        emit the bracket per-game record shape.

        Window: May 25 (selection day; Regional play starts Friday of
        Memorial Day weekend) through July 1 (worst-case Finals slip).
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        out: List[Dict[str, Any]] = []
        season_start = datetime(self.season_year, 5, 25, tzinfo=timezone.utc).date()
        season_end = datetime(self.season_year, 7, 1, tzinfo=timezone.utc).date()
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
        """Convert one ESPN postseason event into the bracket per-game
        record shape (`game_id`, `stage`, `matchday`, `home`, `away`,
        `home_goals`, `away_goals`, `status`, `start_time`, `extra`).
        Returns None for stages whose headlines don't match a modeled
        stage marker (Regional + 8-team MCWS bracket; see #43).
        """
        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        headline = ""
        for note in (comp.get("notes") or []):
            if note.get("type") == "event":
                headline = note.get("headline") or ""
                break
        stage, game_num = _parse_baseball_playoff_headline(headline)
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


# =====================================================================
# NcaaBaseballPlayoffBracketSource: Regional + 8-team MCWS double-elim
# =====================================================================


# Site name is everything between "NCAA Baseball Championship - " and
# " Regional": e.g., "Auburn Regional" from
# "NCAA Baseball Championship - Auburn Regional - Game 1".
_REGIONAL_SITE_REGEX = re.compile(
    r"NCAA Baseball Championship - (?P<site>[^-]+? Regional)\b"
)

# Headline substrings that classify a postseason game as a stage of the
# Regional double-elim. ESPN tags the same event multiple ways across
# its event lifecycle:
#   - "NCAA Baseball Championship - Auburn Regional - Game 1"
#   - "NCAA Baseball Championship - Auburn Regional - Elimination Game"
#   - "NCAA Baseball Championship - Auburn Regional - {team} advances to Super Regional"
_REGIONAL_MARKERS = (
    "Regional - Game ",
    "Regional - Elimination Game",
    "Regional - ",  # catch-all for "advances to" + future ESPN variants
)

# 8-team MCWS bracket headline substrings (the cleanly-labeled best-of-3
# Championship Final is _FINALS_MARKER above; this set covers the
# sub-bracket double-elim that precedes it).
_MCWS_BRACKET_MARKERS = (
    "Men's College World Series - Double Elimination Round",
    "Men's College World Series - Elimination Game",
    " advances to Championship Series",
)


def _parse_baseball_bracket_headline(headline: str) -> Tuple[Optional[str], Optional[str]]:
    """Map an ESPN postseason headline to (stage, partial_grouping_key).

    For Regional games, returns ("BSB_REG", "<site> Regional"): the
    grouping_key is the site label parsed from the headline.

    For 8-team MCWS bracket games, returns ("MCWS", None): the
    sub-bracket assignment can't be determined from one headline alone;
    `_classify_mcws_sub_brackets` does the chronological grouping later
    in `_fetch_bracket_games` once all MCWS events are collected.

    Returns (None, None) for headlines that belong to the sibling
    NcaaBaseballPlayoffSource (Super Regional + Final) or are
    non-postseason / unclassifiable.

    DO NOT collapse the Regional / MCWS branches into a single regex
   : Regional headlines carry the site name in a structured position
    that we can extract; MCWS bracket headlines don't have any
    sub-bracket hint, so the partition is purely chronological.
    """
    if not headline:
        return None, None
    # Best-of-3 stages are owned by the sibling playoff source: explicitly
    # skip them here so the bracket source doesn't try to claim them.
    if _SUPER_REGIONAL_MARKER in headline or _FINALS_MARKER in headline:
        return None, None
    m = _REGIONAL_SITE_REGEX.search(headline)
    if m and any(marker in headline for marker in _REGIONAL_MARKERS):
        return "BSB_REG", m.group("site")
    if any(marker in headline for marker in _MCWS_BRACKET_MARKERS):
        return "MCWS", None
    return None, None


try:
    from zoneinfo import ZoneInfo
    _MCWS_VENUE_TZ = ZoneInfo("America/Chicago")  # Omaha (Charles Schwab Field)
except ImportError:
    _MCWS_VENUE_TZ = timezone.utc  # fallback; shouldn't fire on Py 3.9+


def _classify_mcws_sub_brackets(
    mcws_games: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Assign each MCWS team to one of two sub-brackets based on the
    opening-day pairings heuristic verified against 2025 MCWS data:

      - Day 1's 2 games define sub-bracket 1 (the 4 teams that played
        on the earliest MCWS local-time date in Omaha).
      - Day 2's 2 games define sub-bracket 2 (the 4 teams that played
        on the second-earliest local-time date).
      - Day 3+ games are partitioned by which sub-bracket each team
        was assigned to on its opening day. Teams that somehow appear
        without an opening-day assignment (rained-out openers, etc.)
        inherit their opponent's sub-bracket if known, otherwise are
        dropped.

    Returns a dict {team_name: "MCWS_sub1" | "MCWS_sub2"}.

    DO NOT partition by UTC date: MCWS evening games in Omaha
    (CT) routinely cross UTC midnight (a 7:30 PM CT game is 12:30 AM
    UTC the next day), which would split a single MCWS broadcast day
    across two UTC dates and put opening-day pairings into different
    sub-brackets. Converting to America/Chicago first pins the partition
    to the broadcast calendar regardless of DST or UTC roll-over.

    The 2025 MCWS schedule had two games on Day 1 (Jun 13: Coastal
    Carolina vs Arizona, Oregon State vs Louisville) and two on Day 2
    (Jun 14: UCLA vs Murray State, Arkansas vs LSU). The partition
    holds 100% on Day 3+ (Oregon State vs Coastal Carolina is sub-
    bracket 1, LSU vs UCLA is sub-bracket 2).
    """
    games_sorted = sorted(
        mcws_games,
        key=lambda g: g.get("start_time") or datetime(2099, 1, 1, tzinfo=timezone.utc),
    )
    sub_bracket: Dict[str, str] = {}
    days_seen: List[Any] = []   # ordered list of unique venue-local dates
    for g in games_sorted:
        ts = g.get("start_time")
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        d = ts.astimezone(_MCWS_VENUE_TZ).date()
        if d not in days_seen:
            days_seen.append(d)
        home = g.get("home")
        away = g.get("away")
        if not home or not away:
            continue
        if d == days_seen[0]:
            sub_bracket.setdefault(home, "MCWS_sub1")
            sub_bracket.setdefault(away, "MCWS_sub1")
        elif len(days_seen) >= 2 and d == days_seen[1]:
            sub_bracket.setdefault(home, "MCWS_sub2")
            sub_bracket.setdefault(away, "MCWS_sub2")
        else:
            # Day 3+: rely on prior assignments. If one team is
            # classified and the other isn't, inherit from the
            # classified one (defensive coverage for opening-day no-shows).
            home_sb = sub_bracket.get(home)
            away_sb = sub_bracket.get(away)
            if home_sb and not away_sb:
                sub_bracket[away] = home_sb
            elif away_sb and not home_sb:
                sub_bracket[home] = away_sb
    return sub_bracket


def _mcws_meta_from_raw_events(
    raw_events: List[Tuple[Dict[str, Any], str, Optional[str]]],
) -> List[Dict[str, Any]]:
    """Pluck the (home, away, start_time) projection from raw ESPN
    events tagged MCWS, suitable for feeding `_classify_mcws_sub_brackets`.
    Shared between fetch_upcoming and _fetch_bracket_games so the
    chronological-grouping projection lives in one place."""
    out: List[Dict[str, Any]] = []
    for event, stage, _key in raw_events:
        if stage != "MCWS":
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


class NcaaBaseballPlayoffBracketSource(_BaseballPlayoffStrengthsMixin, DoubleEliminationSource):
    """NCAA Baseball postseason BRACKET stages: 4-team Regional double-
    elim (16 sites) and the 8-team MCWS bracket (modeled as two 4-team
    sub-brackets). Sibling to NcaaBaseballPlayoffSource which handles
    the cleanly-labeled best-of-3 stages.

    Tie grouping_key:
      - BSB_REG: site name from headline (e.g., "Auburn Regional").
      - MCWS: "MCWS_sub1" / "MCWS_sub2" assigned by the day-partition
        heuristic in `_classify_mcws_sub_brackets`.

    The bracket source emits records for BSB_REG and MCWS only: the
    best-of-3 BSB_SR and MCWS_F headlines are explicitly skipped so the
    sibling source owns them without duplication.

    Strength sharing: same hook as the sibling: the plugin seeds
    per-team strengths via `set_regular_season_strengths` from the
    regular-season source. Without seeding, the 6-runs-per-team
    Poisson default applies.
    """

    KO_STAGES = ("BSB_REG", "MCWS")
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
        return "NCAABSB"

    @property
    def sport_label(self) -> str:
        return "NCAA Baseball Postseason"

    def _league_context_code(self) -> str:
        return "MCWS_PO"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # MCWS sub-bracket winner advances to MCWS_F (depth 3), handled by
        # the sibling source. BSB_REG winner falls through to default
        # `stage_depth + 1` → depth 1 = BSB_SR depth.
        if stage == "MCWS":
            return "MCWS_F"
        return None

    def _tie_grouping_key(self, game: Dict[str, Any]) -> Optional[str]:
        return (game.get("extra") or {}).get("grouping_key")

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull next-N-day postseason games whose headline matches a
        bracket stage (Regional or 8-team MCWS bracket). Sibling
        NcaaBaseballPlayoffSource handles the best-of-3 Super Regional
        + Final headlines.
        """
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        # First sweep: collect ALL upcoming bracket events so MCWS
        # sub-bracket grouping can use the full chronological context.
        raw_events: List[Tuple[Dict[str, Any], str, Optional[str]]] = []
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if not data:
                continue
            for event in data.get("events") or []:
                if not _is_postseason_event(event):
                    continue
                eid = event.get("id")
                if eid in seen_ids:
                    continue
                comps = event.get("competitions") or []
                if not comps:
                    continue
                headline = ""
                for note in (comps[0].get("notes") or []):
                    if note.get("type") == "event":
                        headline = note.get("headline") or ""
                        break
                stage, partial_key = _parse_baseball_bracket_headline(headline)
                if stage is None:
                    continue
                seen_ids.add(eid)
                raw_events.append((event, stage, partial_key))

        mcws_sub_by_team = _classify_mcws_sub_brackets(
            _mcws_meta_from_raw_events(raw_events)
        )
        for event, stage, partial_key in raw_events:
            row = self._event_to_game_row(event, stage, partial_key, mcws_sub_by_team)
            if row is not None:
                out.append(row)
        return out

    def _event_to_game_row(
        self,
        event: Dict[str, Any],
        stage: str,
        partial_key: Optional[str],
        mcws_sub_by_team: Dict[str, str],
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

        if stage == "BSB_REG":
            grouping_key = partial_key
        else:  # MCWS
            grouping_key = mcws_sub_by_team.get(home_team) or mcws_sub_by_team.get(away_team)
        if grouping_key is None:
            return None

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
                "espn_event_id": event.get("id"),
                "fd_competition_code": self._league_context_code(),
                "stage": stage,
                "grouping_key": grouping_key,
                "headline": headline,
            },
        )

    # ---------- bracket fetch (full-season for Monte Carlo) ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Sweep the postseason date window day-by-day. Emit bracket
        per-game records with the `grouping_key` attached so the
        DoubleEliminationSource base can group games into tie_metas."""
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        raw: List[Tuple[Dict[str, Any], str, Optional[str]]] = []
        season_start = datetime(self.season_year, 5, 25, tzinfo=timezone.utc).date()
        season_end = datetime(self.season_year, 7, 1, tzinfo=timezone.utc).date()
        day = season_start
        seen_ids: set = set()
        while day <= season_end:
            data = _http_get(f"{ESPN_BASE}/scoreboard?dates={day.strftime('%Y%m%d')}")
            if data:
                for event in data.get("events") or []:
                    if not _is_postseason_event(event):
                        continue
                    eid = event.get("id")
                    if eid in seen_ids:
                        continue
                    comps = event.get("competitions") or []
                    if not comps:
                        continue
                    headline = ""
                    for note in (comps[0].get("notes") or []):
                        if note.get("type") == "event":
                            headline = note.get("headline") or ""
                            break
                    stage, partial_key = _parse_baseball_bracket_headline(headline)
                    if stage is None:
                        continue
                    seen_ids.add(eid)
                    raw.append((event, stage, partial_key))
            day += timedelta(days=1)

        mcws_sub_by_team = _classify_mcws_sub_brackets(
            _mcws_meta_from_raw_events(raw)
        )

        out: List[Dict[str, Any]] = []
        for event, stage, partial_key in raw:
            rec = self._event_to_bracket_record(event, stage, partial_key, mcws_sub_by_team)
            if rec is not None:
                out.append(rec)
        self._bracket_games_cache = out
        return out

    def _event_to_bracket_record(
        self,
        event: Dict[str, Any],
        stage: str,
        partial_key: Optional[str],
        mcws_sub_by_team: Dict[str, str],
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

        if stage == "BSB_REG":
            grouping_key = partial_key
        else:  # MCWS
            grouping_key = mcws_sub_by_team.get(home_team) or mcws_sub_by_team.get(away_team)
        if grouping_key is None:
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
            "matchday": 1,    # double-elim has no fixed matchday; default 1
            "home": home_team,
            "away": away_team,
            "home_goals": hr,
            "away_goals": ar,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"grouping_key": grouping_key},
        }
