"""NCAA College Cup source: single-game-elim bracket via ESPN.

Issue #24: NCAA Men's + Women's D1 Soccer Tournament. Both genders use
the same structural shape (single-leg elimination across multiple
rounds); only ESPN's URL slug, the LEAGUE_CONTEXTS code, and the
gender-specific round labels differ. One source class parametrized on
gender, mirroring NcaaSoccerSource (regular season): same pattern.

Bracket shape (6 rounds, both genders):

| ESPN round slug                 | Stage label | KNOCKOUT_ROUND_DEPTH |
|---------------------------------|-------------|----------------------|
| first-round                     | R64         | 0 (entry)            |
| second-round                    | R32         | 1                    |
| third-round                     | S16         | 2 (Sweet 16)         |
| quarterfinals                   | E8          | 3 (Elite 8)          |
| college-cup---semifinal (M's)   | F4          | 4 (College Cup SF)   |
| semifinals (W's)                | F4          | 4                    |
| college-cup---championship (M's)| NCG         | 5 (Final)            |
| college-cup (W's)               | NCG         | 5                    |

Gender-specific slug aliases for the final two rounds (M's uses "college-
cup---semifinal" / "college-cup---championship"; W's uses "semifinals" /
"college-cup"). Both map to the same canonical R64/R32/S16/E8/F4/NCG
stage labels, which match `KNOCKOUT_ROUND_DEPTH` entries already in use
by NCAA Women's Basketball March Madness (also a 6-round single-game
elim bracket: the depth dict is shared and the cascade is identical).

Round routing (#235): the round slug is NOT reliably on the event. ESPN
tagged every 2024 tournament event with it (`event.season.slug`), but
in 2025 it tagged 5 of 47 men's events and 3 of 63 women's; the rest
carry the plain season slug, exactly like an October game. The rounds
still exist in ESPN's season calendar as season types with UTC
windows, so `_stage_for_event` reads, in order: the event's own round
slug, the event's own round season-type id, then the round window its
kickoff falls in (`_espn.fetch_season_type_windows`). DO NOT go back
to slug-only routing: on 2025 data it finds 5 men's third-round games
and nothing else, and the cascade reads that as the whole bracket.

Field size note: Men's NCAA Soccer Tournament uses a 48-team field with
16 byes (top 16 seeds play their first match in the second round), so
"first-round" has only 16 games (32 of 48 teams), then "second-round"
has 16 games (32 teams: 16 byes + 16 R1 winners). Treating the entry
round as R64 is correct conceptually: the cascade only cares about
depth ordering, and a team that played and lost R1 has reached the
"R64" entry tier even though the tier isn't full. Women's bracket is
a full 64-team field with no byes.

Strength sharing: `set_regular_season_strengths` lets the plugin seed
playoff sampling with per-team Poisson rates from the regular-season
NcaaSoccerSource: the College Cup samples then reflect actual scoring
skill from the 20-game regular season instead of the 1.5/1.5 league-
average prior. Without the seed the source falls back to the prior.

Postseason is filed under `LEAGUE_CONTEXTS["NCAA_MSOC_CUP"]` /
`["NCAA_WSOC_CUP"]` (sibling to the regular-season `NCAA_MSOC` /
`NCAA_WSOC` contexts already shipped in Phase O). Weights ramp into
the College Cup Final because postseason is when these channels get
real TV pickup, and the National Championship Game has the highest
viewership of any D1 soccer game by an order of magnitude.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from ._espn import (
    SeasonTypeWindow,
    extract_espn_scoreboard_event,
    fetch_season_type_windows,
    season_type_at,
    sweep_upcoming_scoreboard,
)
# ONE canonicalizer for both NCAA soccer sources, imported rather than
# copied: the regular-season strength seed is keyed by the regular source's
# team names and looked up by this source's, so the two MUST agree.
from .ncaa_soccer import _team_canonical_name
from .._util import parse_iso_utc, poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_soccer_cup")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# Tournament window: M's runs late Nov through mid-Dec, W's runs mid-Nov
# through early-Dec. Walking Nov 1 through Dec 31 of season_year covers
# both with margin on either side.
TOURNAMENT_START_MONTH = 11
TOURNAMENT_END_MONTH = 12

# Per-team scoring rate priors. NCAA D1 soccer averages ~1.5 goals/team/
# game across both M's and W's. Same prior as NcaaSoccerSource so the
# regular-season → playoff strength seed lines up consistently when
# `set_regular_season_strengths` is wired.
_DEFAULT_GOALS_FOR = 1.5
_DEFAULT_GOALS_AGAINST = 1.5

# Map ESPN's round slug to canonical bracket stage labels. The same slugs
# name the rounds whether they arrive on the event (`season.slug`) or on
# the season calendar's round types (`SeasonTypeWindow.slug`); see
# `NcaaSoccerCupSource._stage_for_event`.
# Stage labels are reused from NCAAW Basketball (R64/R32/S16/E8/F4/NCG)
# because the depth ordering in KNOCKOUT_ROUND_DEPTH already supports
# them and the cascade behaves identically for single-game elim brackets.
# Gender-specific keys for the last two rounds: both M's and W's map
# to the same canonical labels via slug aliases.
SLUG_TO_STAGE: Dict[str, str] = {
    "first-round":                  "R64",
    "second-round":                 "R32",
    "third-round":                  "S16",
    "quarterfinals":                "E8",
    # College Cup Semifinals: gender split:
    "college-cup---semifinal":      "F4",  # M's slug
    "semifinals":                   "F4",  # W's slug
    # National Championship Game: gender split:
    "college-cup---championship":   "NCG", # M's slug
    "college-cup":                  "NCG", # W's slug (the final, not the full event)
}


class CalendarUnavailable(RuntimeError):
    """ESPN's season calendar could not be read while tournament games may
    be on the board. See `NcaaSoccerCupSource.fetch_upcoming`."""


def _http_get(url: str, timeout: float = 15.0) -> Optional[Any]:
    """ESPN endpoints return dicts; the wrapper type is Optional[Any]
    rather than Optional[Dict] to keep the contract honest about the
    None failure path. Caller checks isinstance(data, dict)."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_soccer_cup] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_soccer_cup] %s failed: %s", url, exc)
        return None


# Bracket dedup helper shared with mls_cup -- the canonical impl lives in
# _soccer_bracket_helpers. Imported under the legacy name, which tests
# import from this module.
from ._soccer_bracket_helpers import (
    dedupe_pk_shootout_pairs as _dedupe_pk_shootout_pairs,
)


class NcaaSoccerCupSource(BestOfNSeriesSource):
    """NCAA D1 soccer College Cup bracket, parametrized on gender.

    Six single-game elim rounds (R64 → R32 → S16 → E8 → F4 → NCG).
    SERIES_LENGTH = 1 so each "series" clinches at ceil(1/2) = 1 win:
    the BestOfNSeriesSource state machine handles round_reached and
    terminal_outcomes for free at this length.

    Field-size asymmetry: M's has 48 teams (16 byes at entry), W's has
    64 teams (no byes). The bracket source doesn't care: both genders
    surface the same 6 stage labels via slug aliases in `SLUG_TO_STAGE`.
    """

    KO_STAGES = ("R64", "R32", "S16", "E8", "F4", "NCG")
    SERIES_LENGTH = 1
    supports_importance = True

    def __init__(
        self,
        gender: str = "m",
        season_year: Optional[int] = None,
    ) -> None:
        g = (gender or "").lower().strip()
        if g not in ("m", "w"):
            raise ValueError(f"gender must be 'm' or 'w', got {gender!r}")
        self.gender = g
        now = datetime.now(timezone.utc)
        # NCAA soccer seasons are named by their calendar year (2025
        # season runs Aug-Dec 2025, with the tournament wrapping mid-
        # December 2025). Default to current calendar year; pre-August
        # treats as the prior season (postseason wraps by mid-Dec, so
        # January reads as offseason → use prior year for any lingering
        # postseason data fetch).
        self.season_year = (
            season_year if season_year is not None
            else (now.year if now.month >= 8 else now.year - 1)
        )
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        # Round windows per season year, memoised for this instance (one
        # refresh). A failed fetch is memoised too, as None, so a refresh
        # makes one calendar request per year rather than one per event;
        # the next refresh builds a new source and asks again.
        self._season_windows_by_year: Dict[int, Optional[List[SeasonTypeWindow]]] = {}
        self._team_strengths_from_regular: Optional[
            Dict[str, Dict[str, float]]
        ] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAAMSOC" if self.gender == "m" else "NCAAWSOC"

    @property
    def sport_label(self) -> str:
        return (
            "NCAA Men's College Cup"
            if self.gender == "m"
            else "NCAA Women's College Cup"
        )

    @property
    def _espn_slug(self) -> str:
        return f"usa.ncaa.{self.gender}.1"

    def _league_context_code(self) -> str:
        return "NCAA_MSOC_CUP" if self.gender == "m" else "NCAA_WSOC_CUP"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # Champion is the synthetic depth above NCG (final game). Mirror
        # the NCAAW Basketball NCG → NCG_WINNER mapping: same depth
        # entry in KNOCKOUT_ROUND_DEPTH (already at depth 6).
        if stage == "NCG":
            return "NCG_WINNER"
        return None

    # ---------- strength sharing (regular season → playoff) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate. Seeded from the regular-
        season NcaaSoccerSource via `set_regular_season_strengths`;
        without the seed, falls back to the league-average prior."""
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]],
    ) -> None:
        """Hook for the plugin to share regular-season strength
        estimates with the College Cup source. Without this, every
        bracket team gets the league-average 1.5/1.5 prior: accurate
        enough for early-round importance but loses the team-skill
        signal that a 20-game regular season provides."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_GOALS_FOR,
            "pa_per_game": _DEFAULT_GOALS_AGAINST,
        }

    # ---------- sample_result (force a winner: bracket games) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Bracket games CANNOT end in a draw: NCAA postseason soccer
        goes to OT then PKs if needed. Sample Poisson goals; on a tied
        regulation result, coin-flip the +1 boost so the bracket
        cascade gets a clean winner. Distinct from NcaaSoccerSource
        regular-season sample_result which allows draws (1 standings
        point each): bracket scenarios have no draw outcome at all.
        """
        del state  # interface-required, per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.05, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_goals = _poisson(lam_home, rng)
        away_goals = _poisson(lam_away, rng)
        if home_goals == away_goals:
            # OT / PKs in real life; coin-flip the +1 here. Strength
            # asymmetry already biased the Poisson means: no further
            # bias added (PK shootouts are roughly coin-flips empirically
            # at the NCAA level).
            if rng.random() < 0.5:
                home_goals += 1
            else:
                away_goals += 1
        return MatchResult(home_goals=home_goals, away_goals=away_goals)

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Emit upcoming tournament games (any stage). Per-day sweep:
        ESPN's date-range syntax silently caps at 25 events, same trap
        as ncaa_baseball.py / ncaa_soccer.py. Only events
        `_stage_for_event` places in a tournament round surface here;
        regular-season games are filtered out.

        RAISES `CalendarUnavailable` when the board holds an event in the
        tournament months and ESPN's season calendar is unreachable.
        Returning only the few events ESPN tags (1 of 25 on the 2025
        women's Nov 14 board) would read as a real, shorter slate: the
        refresh would not hold the channels already published and apply
        could reap them. A raise is what the refresh treats as this
        source's outage (it holds the previous rows, #240). Even a tagged
        event is not published then, because the importance bracket needs
        the calendar too. Before November nothing can be a tournament game,
        so a missing calendar then costs nothing and is not an error.
        """
        # The shared upcoming sweep owns the Eastern-bucket lookback, the
        # FINISHED drop, the stale-SCHEDULED floor and the id dedupe (see
        # _espn.sweep_upcoming_scoreboard). The bracket SWEEP below must not
        # use it (it keeps finished games); this EPG path wants exactly it.
        recs = sweep_upcoming_scoreboard(
            slug=self._espn_slug,
            days_ahead=days_ahead,
            team_namer=_team_canonical_name,
            http_get=_http_get,
            extras_fn=lambda event: {
                "_routed": self._stage_for_event(event),
                "_season_year": self._event_season_year(event),
            },
            base=ESPN_BASE,
        )
        out: List[GameRow] = []
        for rec in recs:
            year = rec["_season_year"]
            # Checked for EVERY tournament-month event, tagged ones too: the
            # importance bracket (`_fetch_bracket_games`, which reads THIS
            # source's season) is empty without the calendar, so a tagged
            # row published now would be scored against no bracket at all.
            # The event's own season is checked as well because routing
            # reads that one.
            if rec["start_time"].month >= TOURNAMENT_START_MONTH:
                for needed in sorted({year, self.season_year}):
                    if self._season_windows(needed) is None:
                        raise CalendarUnavailable(
                            f"ESPN {needed} season calendar for {self._espn_slug} "
                            f"is unavailable, so tournament rounds cannot be placed"
                        )
            routed = rec.get("_routed")
            if routed is None:
                continue
            eid = rec.get("id")
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=rec["home"],
                away=rec["away"],
                rank_home=None,
                rank_away=None,
                start_time=rec["start_time"],
                extra={
                    "espn_event_id": eid,
                    # Identity key the simulator compares on; see GameRow.extra in base.py. #181
                    "game_id": eid,
                    "fd_competition_code": self._league_context_code(),
                    "stage": routed[0],
                },
            ))
        return out

    # ---------- _fetch_bracket_games (importance side) ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the November-December tournament window and emit one
        canonical record per bracket game. Filters out non-tournament
        events via `_stage_for_event`: a regular-season game has no
        round slug, no round type and a kickoff outside every round
        window, so it falls out.

        Without ESPN's round calendar this returns [] and does NOT cache
        it. Slug-only routing would keep just the few events ESPN still
        tags (3 of 63 in the 2025 women's bracket) and the cascade would
        read every other team as never having advanced, which is a WRONG
        importance signal rather than a missing one (#235).

        ESPN sometimes records two events for a single bracket game
        that goes to OT / PKs: one with the regulation 0-0 / X-X tie
        and a second with the PK shootout final score (observed live
        on the 2024 M's Marshall @ SMU QF: events 723937 with 0-0
        and 724472 with 3-2). Both are tagged status="post" and
        share the same slug, so we dedupe by (stage, frozenset
        participants) keeping the event with a non-tie outcome (or
        the latest by start_time when both are non-tie). Single-game
        elimination brackets have at most one game per (stage,
        participants); a second event at the same stage between the
        same two teams is ESPN data noise, not a real second leg.
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache
        if self._season_windows(self.season_year) is None:
            return []

        candidates: List[Dict[str, Any]] = []
        seen_ids: set = set()
        # Tournament window: Nov 1 - Dec 31 of season_year.
        start = datetime(
            self.season_year, TOURNAMENT_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        # End of December: build Jan 1 of next year, subtract one day.
        end = (
            datetime(self.season_year + 1, 1, 1, tzinfo=timezone.utc).date()
            - timedelta(days=1)
        )
        day = start
        while day <= end:
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard?"
                f"dates={day.strftime('%Y%m%d')}"
            )
            if isinstance(data, dict):
                for event in data.get("events") or []:
                    rec = self._extract_bracket_record(event)
                    if rec is None or rec["game_id"] is None:
                        continue
                    if rec["game_id"] in seen_ids:
                        continue
                    seen_ids.add(rec["game_id"])
                    candidates.append(rec)
            day += timedelta(days=1)

        self._bracket_games_cache = _dedupe_pk_shootout_pairs(candidates)
        return self._bracket_games_cache

    # ---------- round routing ----------

    def _season_windows(self, year: int) -> Optional[List[SeasonTypeWindow]]:
        """ESPN's season calendar for `year`, memoised per instance (see
        `_season_windows_by_year`). Logs once per year when it is missing."""
        if year not in self._season_windows_by_year:
            windows = fetch_season_type_windows(self._espn_slug, year, _http_get)
            if windows is None:
                logger.warning(
                    "[ncaa_soccer_cup] no %d season calendar for %s; %s",
                    year, self._espn_slug,
                    "the importance bracket is empty and a tournament-month "
                    "board raises CalendarUnavailable"
                    if year == self.season_year
                    else "that season's untagged events cannot be placed in a round",
                )
            self._season_windows_by_year[year] = windows
        return self._season_windows_by_year[year]

    def _event_season_year(self, event: Dict[str, Any]) -> int:
        """The event's own `season.year`, else this source's season."""
        try:
            return int((event.get("season") or {}).get("year") or self.season_year)
        except (TypeError, ValueError):
            return self.season_year

    def _stage_for_event(
        self, event: Dict[str, Any],
    ) -> Optional[tuple[str, str, str]]:
        """`(stage, round_slug, how)` for a tournament event, else None.

        Three signals, strongest first, because ESPN fills them
        inconsistently (#235; see the module docstring):

          1. `event.season.slug` is a round slug (every 2024 event).
          2. `event.season.type` is a round's season-type id.
          3. The kickoff falls inside a round's window on ESPN's season
             calendar (every untagged 2025 event).

        Signal 3 is only consulted for an event of the calendar's own
        season year, so a stray event from another season is never read
        against the wrong year's windows.
        """
        season = event.get("season") or {}
        slug = (season.get("slug") or "").strip().lower()
        if slug in SLUG_TO_STAGE:
            return SLUG_TO_STAGE[slug], slug, "slug"
        windows = self._season_windows(self._event_season_year(event))
        if not windows:
            return None
        try:
            type_id: Optional[int] = int(season.get("type"))
        except (TypeError, ValueError):
            type_id = None
        if type_id is not None:
            for w in windows:
                if w.type_id == type_id and w.slug in SLUG_TO_STAGE:
                    return SLUG_TO_STAGE[w.slug], w.slug, "season_type"
        w = season_type_at(windows, parse_iso_utc(event.get("date")))
        if w is not None and w.slug in SLUG_TO_STAGE:
            return SLUG_TO_STAGE[w.slug], w.slug, "round_window"
        return None

    def _extract_bracket_record(
        self, event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Convert one ESPN scoreboard event into the canonical
        BracketSportSource per-game record shape, OR None if the event
        isn't a tournament bracket game.

        Stage routing is `_stage_for_event`; read it before assuming the
        event names its own round (since 2025 it mostly does not).
        """
        routed = self._stage_for_event(event)
        if routed is None:
            return None
        stage, round_slug, routed_by = routed

        # The shared parser owns competitor, status and score handling,
        # including the FINISHED-without-score demotion (see _espn).
        rec = extract_espn_scoreboard_event(event, team_namer=_team_canonical_name)
        if rec is None:
            return None

        return {
            "game_id": rec["id"],
            "stage": stage,
            # SERIES_LENGTH=1 means each game is its own one-game series.
            # matchday is always 1; BracketSportSource clinches at first win.
            "matchday": 1,
            "home": rec["home"],
            "away": rec["away"],
            "home_goals": rec["home_points"],
            "away_goals": rec["away_points"],
            "status": rec["status"],
            "start_time": rec["start_time"],
            # season_slug is what the EVENT said; round_slug is the round
            # it was placed in and stage_routed_by which signal placed it,
            # so a misrouted game can be traced from the cache.
            "extra": {
                "season_slug": (event.get("season") or {}).get("slug"),
                "round_slug": round_slug,
                "stage_routed_by": routed_by,
            },
        }
