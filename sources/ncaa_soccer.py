"""NCAA Soccer source — ESPN's unofficial API.

One source class, parametrized on `gender` ("m" or "w"). The same
structure / endpoints / threshold semantics apply to both — only the
URL slug (`usa.ncaa.m.1` vs `usa.ncaa.w.1`) and the LEAGUE_CONTEXTS
code (`NCAA_MSOC` vs `NCAA_WSOC`) differ. Both seasons run roughly
August - early December, with the College Cup (NCAA Tournament) in
late November / December.

Key difference from NCAA Baseball: SOCCER HAS DRAWS. The standings-
points scheme is 3 / 1 / 0 (win / draw / loss). `_count_field` is
"standings_points" so the LEAGUE_CONTEXTS bands threshold against
that field instead of raw wins. A team going 13-3-8 (Saint Louis
men's, 2025) has only 13 wins but 47 standings points — without the
draws credit the win-count thresholds would misclassify them as
bubble when they're a national seed contender.

College Cup postseason bracket is NOT modeled — same shape as the
WC / EURO knockout (single-leg elimination), so the existing
KnockoutSoccerSource machinery can absorb it when wired up.
fetch_upcoming still surfaces postseason games via rank + favorite
signals.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_soccer")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# NCAA soccer regular season: August through early December (College
# Cup wraps mid-December). Walk Aug 1 through Dec 31 day-by-day to
# avoid ESPN's silent 25-event cap on date-range scoreboard queries
# (see ncaa_baseball.py for the same trap).
SEASON_START_MONTH = 8   # August
SEASON_END_MONTH = 12    # December

# Default per-team scoring rate priors. NCAA D1 soccer averages
# ~1.5 goals/team/game in regulation across both M's and W's.
_DEFAULT_GOALS_FOR = 1.5
_DEFAULT_GOALS_AGAINST = 1.5


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_soccer] %s → %d", url, r.status_code)
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
    a draw (not a "force a winner via overtime" outcome). Status
    classification mirrors ncaa_baseball but allows hp == ap in FINISHED
    games.
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

    @property
    def sport_prefix(self) -> str:
        return "NCAAMSOC" if self.gender == "m" else "NCAAWSOC"

    @property
    def sport_label(self) -> str:
        return "NCAA Men's Soccer" if self.gender == "m" else "NCAA Women's Soccer"

    @property
    def _espn_slug(self) -> str:
        return f"usa.ncaa.{self.gender}.1"

    # ---------- fetch_upcoming ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep (ESPN's date-range syntax silently
        caps at 25 events — see ncaa_baseball.py)."""
        rankings = self._fetch_rankings()
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard?dates={day.strftime('%Y%m%d')}"
            )
            if not data:
                continue
            for event in data.get("events") or []:
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

    # ---------- _fetch_full_season_games ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Per-day sweep across the season. Roughly Aug-Dec = ~150
        days × ~100ms = ~15s. Acceptable for a 6-hour refresh budget.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(self.season_year, SEASON_START_MONTH, 1, tzinfo=timezone.utc).date()
        season_end_first = datetime(self.season_year, SEASON_END_MONTH, 1, tzinfo=timezone.utc).date()
        season_end = (season_end_first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
        now = datetime.now(timezone.utc).date()
        end = min(now + timedelta(days=7), season_end)
        if end < season_start:
            return []
        day = season_start
        while day <= end:
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard?dates={day.strftime('%Y%m%d')}"
            )
            if data:
                for event in data.get("events") or []:
                    rec = _extract_game_record(event)
                    if rec is None or rec["id"] is None:
                        continue
                    if rec["id"] in seen:
                        continue
                    seen[rec["id"]] = rec
            day += timedelta(days=1)
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

    # ---------- sample_result (allows ties — draws are legitimate) ----------

    def sample_result(self, state, match, strengths, rng):
        """Sample Poisson goals per side. Soccer regulation outcomes
        include draws — DO NOT coin-flip a tied score into a win.
        The base PointsBasedSportSource.sample_result forces a non-tie
        outcome for NCAAF / NCAAM / NHL where regulation ties resolve
        into overtime; for college soccer the regulation tie IS the
        outcome (1 standings point for each side via _record_result_
        into_state).

        NCAA regular-season soccer DOES play one 10-minute overtime if
        regulation ends tied, but it's GOLDEN GOAL — meaning a regulation
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
        del result_extra  # not used — soccer ties are part of regulation
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
            # Draw — 1 standings point each, no W/L update.
            h["draws"] += 1
            a["draws"] += 1
            h["standings_points"] += 1
            a["standings_points"] += 1
