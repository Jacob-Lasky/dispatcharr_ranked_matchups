"""NCAA Soccer source: ESPN's unofficial API.

One source class, parametrized on `gender` ("m" or "w"). The same
structure / endpoints / threshold semantics apply to both: only the
URL slug (`usa.ncaa.m.1` vs `usa.ncaa.w.1`) and the LEAGUE_CONTEXTS
code (`NCAA_MSOC` vs `NCAA_WSOC`) differ. Both seasons run roughly
August - early December, with the College Cup (NCAA Tournament) in
late November / December.

Key difference from NCAA Baseball: SOCCER HAS DRAWS. The standings-
points scheme is 3 / 1 / 0 (win / draw / loss). `_count_field` is
"standings_points" so the LEAGUE_CONTEXTS bands threshold against
that field instead of raw wins. A team going 13-3-8 (Saint Louis
men's, 2025) has only 13 wins but 47 standings points: without the
draws credit the win-count thresholds would misclassify them as
bubble when they're a national seed contender.

College Cup postseason bracket is NOT modeled: same shape as the
WC / EURO knockout (single-leg elimination), so the existing
KnockoutSoccerSource machinery can absorb it when wired up.
fetch_upcoming still surfaces postseason games via rank + favorite
signals.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from ._espn import (
    extract_espn_scoreboard_event,
    fetch_days,
    fetch_season_type_windows,
    sweep_upcoming_scoreboard,
)

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_soccer")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# NCAA soccer regular season: August through early December (College
# Cup wraps mid-December). Walk Aug 1 through Dec 31 day-by-day to
# avoid ESPN's silent 25-event cap on date-range scoreboard queries
# (see ncaa_baseball.py for the same trap).
SEASON_START_MONTH = 8   # August

# FALLBACK last day (US Eastern scoreboard date) of each gender's regular
# season, conference tournaments included, as (month, day), keyed by the
# first season it applies to. Used ONLY when ESPN's season calendar is
# unreachable: the real boundary is the end of the regular-season type on
# that calendar (`_regular_season_cutoff`, #235), which ESPN publishes per
# season and which tracks the NCAA's own moves. Each value sits in the gap
# between the last conference final and the first round: men's Nov 16 /
# Nov 20 in 2025 and Nov 19 in 2026; women's Nov 9 / Nov 14 in 2025, then
# the NCAA moved the women's bracket a week later from 2026 (the Women's
# College Cup joined the men's on the second weekend of December; first
# round Nov 20, 2026), where the 2025 value would cut the conference finals
# off the season.
_REGULAR_SEASON_END = {
    "m": ((2025, (11, 18)),),
    "w": ((2025, (11, 11)), (2026, (11, 17))),
}


def _fallback_regular_season_end(gender: str, season_year: int) -> Tuple[int, int]:
    """`(month, day)` from `_REGULAR_SEASON_END` for `season_year`: the
    latest entry whose first season is not after it (the earliest entry
    for seasons before the table starts)."""
    entries = _REGULAR_SEASON_END[gender]
    chosen = entries[0][1]
    for first_season, month_day in entries:
        if first_season <= season_year:
            chosen = month_day
    return chosen

# ESPN buckets its scoreboard by US Eastern date. The regular season always
# ends in mid-November, after DST, so Eastern is UTC-5 at that boundary.
_EASTERN_STANDARD_OFFSET = timedelta(hours=5)

# Default per-team scoring rate priors. NCAA D1 soccer averages
# ~1.5 goals/team/game in regulation across both M's and W's.
_DEFAULT_GOALS_FOR = 1.5
_DEFAULT_GOALS_AGAINST = 1.5


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_soccer] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_soccer] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Prefer `team.location` (school) over `team.name` (mascot). Matches
    EPG provider titles like "Washington at Stanford" vs "Huskies at
    Cardinal"."""
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical PointsBased
    game record. Soccer-specific: a tied score in a regulation game IS
    a draw (not a "force a winner via overtime" outcome). The shared
    parser preserves the tie semantics; this wrapper just supplies the
    soccer team-name canonicalizer."""
    return extract_espn_scoreboard_event(event, team_namer=_team_canonical_name)


def _event_season_year(event: Dict[str, Any]) -> Dict[str, Any]:
    """`extras_fn` for the upcoming sweep: the event's own `season.year`."""
    try:
        year: Optional[int] = int((event.get("season") or {}).get("year"))
    except (TypeError, ValueError):
        year = None
    return {"season_year": year}


class NcaaSoccerSource(PointsBasedSportSource):
    """NCAA Division I soccer importance, parametrized on gender ("m"
    or "w"). Tracks standings points (3 W / 1 D / 0 L) instead of raw
    wins because draws are common in college soccer and a draws-heavy
    team's win count understates their tournament position.
    """

    _count_field = "standings_points"
    _DEFAULT_POINTS_FOR = _DEFAULT_GOALS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_GOALS_AGAINST

    def __init__(self, gender: str = "m", season_year: Optional[int] = None) -> None:
        super().__init__()
        g = (gender or "").lower().strip()
        if g not in ("m", "w"):
            raise ValueError(f"gender must be 'm' or 'w', got {gender!r}")
        self.gender = g
        # Set league_context_code as an instance attribute (overriding the
        # base PointsBasedSportSource class-attribute default) so it's
        # gender-dependent without needing a property descriptor (which
        # pyright flags as an incompatible-variable-override against the
        # base's class-level `str` declaration).
        self.league_context_code = "NCAA_MSOC" if g == "m" else "NCAA_WSOC"
        # NCAA soccer seasons are named by their calendar year (the 2025
        # season runs Aug-Dec 2025). Default to the current year; pre-August
        # treat as the prior season's postseason (Tournament wraps in mid-Dec).
        now = datetime.now(timezone.utc)
        self.season_year = (
            season_year if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )
        # Memoised `_regular_season_cutoff` for this instance (one refresh).
        self._cutoff: Optional[datetime] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAAMSOC" if self.gender == "m" else "NCAAWSOC"

    @property
    def sport_label(self) -> str:
        return "NCAA Men's Soccer" if self.gender == "m" else "NCAA Women's Soccer"

    @property
    def _espn_slug(self) -> str:
        return f"usa.ncaa.{self.gender}.1"

    # ---------- regular-season boundary ----------

    def _regular_season_cutoff(self) -> datetime:
        """The first UTC instant AFTER the regular season (exclusive).

        From ESPN's season calendar when it answers: the end of the
        season's first season type, the regular season, whose windows
        were checked to sit in the gap before the first round for both
        genders in 2024, 2025 and 2026. The College Cup's games carry
        the same season slug, type and notes as an October game (#229,
        #235), so this boundary is the ONLY thing that keeps them out of
        this source: out of the simulated season, and out of
        `fetch_upcoming`, where a regular-season row for a tournament
        game shares the College Cup row's sport prefix and teams and
        wins `_dedup_series_games` (same kickoff, earlier source),
        dropping the College Cup row and its stage.

        Falls back to `_REGULAR_SEASON_END` (the end of that Eastern
        date) when the calendar is unreachable or incomplete. Memoised per instance;
        the next refresh builds a new source and asks again.
        """
        if self._cutoff is not None:
            return self._cutoff
        windows = fetch_season_type_windows(self._espn_slug, self.season_year, _http_get)
        if windows:
            # ESPN's end bound is inclusive at minute precision.
            self._cutoff = windows[0].end + timedelta(minutes=1)
        else:
            month, day_of_month = _fallback_regular_season_end(self.gender, self.season_year)
            logger.warning(
                "[ncaa_soccer] no %d season calendar for %s; ending the "
                "regular season at the %02d-%02d fallback",
                self.season_year, self._espn_slug, month, day_of_month,
            )
            self._cutoff = (
                datetime(self.season_year, month, day_of_month, tzinfo=timezone.utc)
                + timedelta(days=1) + _EASTERN_STANDARD_OFFSET
            )
        return self._cutoff

    # ---------- fetch_upcoming ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Upcoming regular-season games, through the shared ESPN upcoming
        sweep (Eastern-bucket lookback, FINISHED drop, stale floor, id
        dedupe: see `_espn.sweep_upcoming_scoreboard`), cut at the end of
        the regular season (`_regular_season_cutoff`)."""
        rankings = self._fetch_rankings()
        cutoff = self._regular_season_cutoff()
        recs = sweep_upcoming_scoreboard(
            slug=self._espn_slug,
            days_ahead=days_ahead,
            team_namer=_team_canonical_name,
            http_get=_http_get,
            extras_fn=_event_season_year,
            base=ESPN_BASE,
        )
        out: List[GameRow] = []
        for rec in recs:
            start = rec["start_time"]
            year = rec["season_year"]
            if start >= cutoff and (year if year is not None else start.year) == self.season_year:
                # A College Cup game: NcaaSoccerCupSource emits it with its
                # round. See _regular_season_cutoff. Only THIS season's: in
                # late July the source still points at last season, and the
                # new season's August openers are past last season's cutoff.
                continue
            home = rec["home"]
            away = rec["away"]
            eid = rec.get("id")
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
                    # Identity key the simulator compares on; see GameRow.extra in base.py. #181
                    "game_id": eid,
                    "fd_competition_code": self.league_context_code,
                },
            ))
        return out

    # ---------- _fetch_full_season_games ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Per-day sweep across the season. Roughly Aug-Dec = ~150
        days × ~100ms = ~15s. Acceptable for a 6-hour refresh budget.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1, tzinfo=timezone.utc).date()
        # The REGULAR season's end, not December's: ESPN's NCAA soccer
        # scoreboard gives tournament games the same season slug, type and
        # note as regular-season games (checked against 2025: the College Cup
        # final on Dec 15 reads exactly like an October game), so they cannot
        # be filtered by field. Walking into the tournament would add its
        # results to the regular-season table the bands are scored on (#229).
        cutoff = self._regular_season_cutoff()
        # The last Eastern scoreboard date that can hold a game before it.
        season_end = (cutoff - _EASTERN_STANDARD_OFFSET - timedelta(minutes=1)).date()
        # Walk to the END of the season, never to now + lookahead (#229). This
        # list is the simulator's whole season: the fixtures still to play are
        # what decide who finishes where. Capping it at next week made the
        # season end next week, so no team could change band and importance
        # read 0 (every NFL game, measured 2026-10-10). Same bug #65 fixed for
        # MLS in mls_standings.py; tests/test_full_season_window.py guards all.
        end = season_end

        def day_board(day):
            return _http_get(f"{ESPN_BASE}/{self._espn_slug}/scoreboard?dates={day.strftime('%Y%m%d')}")

        for _day, data in fetch_days(season_start, end, day_board):
            if data:
                for event in data.get("events") or []:
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec.get("start_time") is not None and rec["start_time"] >= cutoff:
                        continue
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = rec
        return list(seen.values())

    # ---------- Rankings ----------

    def _fetch_rankings(self) -> Dict[str, int]:
        data = _http_get(f"{ESPN_BASE}/{self._espn_slug}/rankings")
        if not data:
            return {}
        ranks: Dict[str, int] = {}
        polls = data.get("rankings") or []
        if not polls:
            return ranks
        poll = polls[0]  # United Soccer Coaches Top 25 (only poll exposed)
        for r in poll.get("ranks") or []:
            team_obj = r.get("team") or {}
            name = _team_canonical_name(team_obj)
            try:
                rank = int(r.get("current") or 0)
            except (TypeError, ValueError):
                continue
            if name and rank > 0:
                ranks[name] = rank
        return ranks

    # ---------- sample_result (allows ties: draws are legitimate) ----------

    def sample_result(self, state, match, strengths, rng):
        """Sample Poisson goals per side. Soccer regulation outcomes
        include draws: DO NOT coin-flip a tied score into a win.
        The base PointsBasedSportSource.sample_result forces a non-tie
        outcome for NCAAF / NCAAM / NHL where regulation ties resolve
        into overtime; for college soccer the regulation tie IS the
        outcome (1 standings point for each side via _record_result_
        into_state).

        NCAA regular-season soccer DOES play one 10-minute overtime if
        regulation ends tied, but it's GOLDEN GOAL: meaning a regulation
        tie that survives OT is still a draw in the standings. We model
        that by just sampling regulation; the small chance of OT-broken
        ties is folded into the Poisson noise.
        """
        del state  # interface-required
        from .._util import poisson_sample
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.05, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        from .base import MatchResult
        return MatchResult(
            home_goals=poisson_sample(lam_home, rng),
            away_goals=poisson_sample(lam_away, rng),
        )

    # ---------- standings-points-aware record (3 / 1 / 0) ----------

    def _record_result_into_state(
        self,
        teams: Dict[str, Dict[str, int]],
        home: str, away: str,
        home_pts: int, away_pts: int,
        result_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Soccer scheme: 3 standings points for a win, 1 each for a
        draw, 0 for a loss. The base PointsBasedSportSource records
        wins / losses / pf / pa / games_played; we extend with the
        standings_points credit and a 'draws' counter (used by the
        threshold cascade and the cache.json display).
        """
        del result_extra  # not used: soccer ties are part of regulation
        h = teams[home]
        a = teams[away]
        h.setdefault("draws", 0)
        a.setdefault("draws", 0)
        h.setdefault("standings_points", 0)
        a.setdefault("standings_points", 0)
        h["pf"] += home_pts
        h["pa"] += away_pts
        a["pf"] += away_pts
        a["pa"] += home_pts
        h["games_played"] += 1
        a["games_played"] += 1
        if home_pts > away_pts:
            h["wins"] += 1
            a["losses"] += 1
            h["standings_points"] += 3
        elif away_pts > home_pts:
            a["wins"] += 1
            h["losses"] += 1
            a["standings_points"] += 3
        else:
            # Draw: 1 standings point each, no W/L update.
            h["draws"] += 1
            a["draws"] += 1
            h["standings_points"] += 1
            a["standings_points"] += 1
