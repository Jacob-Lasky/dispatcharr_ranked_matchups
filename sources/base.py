"""Common interface for all sport data sources.

A SportSource fetches the upcoming games for one sport/league and the current
ranks (if any) and returns GameRow records. The plugin then scores each row
and matches it to a Dispatcharr channel via EPG.

Sources optionally implement a Monte Carlo importance interface (Lahvička
2012): 7 methods plus a `supports_importance` flag. Sources that flip the
flag MUST override all 7; the simulator (simulation.py) inspects the flag
before calling and falls through gracefully when it's false.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, TypedDict


# ---------- partial-fetch accounting (#261) ----------
#
# A per-day sweep (ESPN's scoreboard, NHL's schedule) that loses a day still
# returns the other days' games, so its result looks like a clean, shorter
# slate. The refresh's on-air hold (#240) reads "this source returned nothing"
# as an outage and keeps the previous games; a source that lost ONE day is
# not empty, so without this record the games of that day were dropped and
# their channels deleted. A sweep reports every day it asked for and did not
# get (failed after its retry, or never requested because the walk was
# abandoned), and the refresh holds previously published games dated on one.
# A RANGED request (one call for several days: statsapi startDate/endDate,
# Football-Data dateFrom/dateTo chunks, CollegeBasketballData
# startDateRange/endDateRange, the boxing `days=` schedule) loses every day it
# covered at once, and reports them with `report_lost_day_range`. Exactly the
# days that request covered, never the whole window: a chunk that answered
# vouches for its days. An answer with no games is a quiet day, not a lost one.
#
# A ContextVar, not a return value: fifteen fetch_upcoming implementations
# walk days through the two shared walks, and threading a second result
# through each is fifteen chances to drop it. The refresh opens the record
# around ONE source's fetch_upcoming at a time (sources are fetched one after
# another, and the walks report from the calling thread, never from their
# pool threads), so a day lands on the source that lost it. Outside a record
# (full-season walks during scoring, tests) reporting is a no-op.
_LOST_DAYS: ContextVar[Optional[Set[date]]] = ContextVar("ranked_matchups_lost_days", default=None)


def report_lost_days(days: Iterable[date]) -> None:
    """Record calendar days this fetch asked for and got no answer for."""
    sink = _LOST_DAYS.get()
    if sink is not None:
        sink.update(days)


def report_lost_day_range(first: date, last: date) -> None:
    """Record every day from `first` through `last` (inclusive) as lost: the
    days one ranged request covered when it got no answer."""
    report_lost_days(first + timedelta(days=i) for i in range((last - first).days + 1))


@contextmanager
def recording_lost_days() -> Iterator[Set[date]]:
    """Collect the days `report_lost_days` is told about inside the block."""
    sink: Set[date] = set()
    token = _LOST_DAYS.set(sink)
    try:
        yield sink
    finally:
        _LOST_DAYS.reset(token)


# ---------- rankings polls (#283) ----------
#
# A college source ranks its games from a poll it fetches separately. A FAILED
# poll request used to read as "no poll": every game went out unranked, its
# rank-pair signal fell away and ranked matchups dropped down or off the slate,
# with nothing in the refresh summary. A failed poll now RAISES
# `RankingsUnavailable` out of `fetch_upcoming`, so the refresh counts the
# source as an outage (#240) and keeps the games it published last time,
# scored against the last poll it had, instead of republishing them flattened.
# An answered poll with no rankings in it (preseason) is not a failure.


class RankingsUnavailable(RuntimeError):
    """A source's rankings poll did not answer: raised out of
    `fetch_upcoming` so the refresh holds the source's previous games."""


def attach_poll_ranks(
    rows: List["GameRow"], fetch_ranks: Callable[[], Dict[str, int]],
) -> List["GameRow"]:
    """`rows` with `rank_home` / `rank_away` read from `fetch_ranks()`, which
    returns {team: rank} and RAISES when the poll did not answer.

    The poll is asked for only when there is a game to rank. That is not just
    a saved request: ESPN's college baseball and softball `/rankings` answer
    HTTP 404 all offseason ("Unable to get sport data", measured 2026-10-11,
    when neither scoreboard lists a game from the end of June to February), so
    asking with nothing to rank would report an outage on every offseason
    refresh. DO NOT hoist the fetch above the sweep for that reason.
    """
    if not rows:
        return rows
    ranks = fetch_ranks()
    for row in rows:
        row.rank_home = ranks.get(row.home)
        row.rank_away = ranks.get(row.away)
    return rows


class SoccerTeamRow(TypedDict):
    """Per-team standings row used by SoccerSource / GroupStageSoccerSource.

    DO NOT add fields here without checking every producer and consumer in
    sources/soccer.py -- this row is constructed in two `initial_state`
    methods and mutated in `_mutate_apply`; a missing field is a runtime
    KeyError on the first match the simulator applies. A renamed field is
    worse: silent miscounting in the standings sort that drives
    terminal_outcomes' advance/eliminated labels."""
    played: int
    points: int
    gf: int   # goals for
    ga: int   # goals against


class PointsBasedTeamRow(TypedDict):
    """Per-team row used by PointsBasedSportSource (NCAAF / NCAAM / NFL /
    NHL / MLB / NBA / MLS conference standings / NCAA baseball / etc.).

    NHL subclasses extend with a `standings_points` field via
    `_record_result_into_state` override; the TypedDict is total=False so
    a typed consumer can read it without pyright complaining that it might
    be missing on non-NHL rows."""
    wins: int
    losses: int
    pf: int            # points-for (aggregated across games)
    pa: int            # points-against
    games_played: int


class PointsBasedTeamRowWithStandings(PointsBasedTeamRow, total=False):
    """NHL extends the base row with `standings_points` (regulation +
    OT + shootout per the NHL 2-point regulation / 1-point OT-loss
    rule). Other points-based sports don't need this field. Optional
    via total=False so consumers can read row.get('standings_points')
    safely."""
    standings_points: int


@dataclass
class GameRow:
    """One upcoming game from one sport. Sport-agnostic."""
    sport_prefix: str           # "CFB", "CBB", "EPL", "EFL", etc.: used in channel name
    sport_label: str            # Full label, e.g., "NCAA Football"
    home: str                   # team name
    away: str
    rank_home: Optional[int]    # current ranking (None if unranked)
    rank_away: Optional[int]
    start_time: datetime        # when the game starts (UTC)
    venue: Optional[str] = None
    spread: Optional[float] = None      # absolute pre-game spread
    # Close-game signal: bookmaker-implied coinflip-ness in [0, 1].
    # 1.0 = pick'em (both teams equally likely to win); 0.0 = blowout.
    # Soccer populates this from the h2h moneyline market (devigged
    # probabilities, then 2 * min(p_home, p_away)). NCAAF / NCAAM still
    # populate `spread` instead: score_game normalizes either path into
    # the same [0, 1] effective closeness, but ONE of these two fields
    # is None on every GameRow. DO NOT set both: keeps the contract
    # "closeness wins if present, fall back to spread otherwise" honest.
    closeness: Optional[float] = None
    is_rivalry: bool = False             # known rivalry
    # Source-specific metadata. One key in here is NOT source-specific:
    #
    #   `game_id` is the identity key the importance simulator compares on.
    #   `PointsBasedSportSource.apply_result` records `extra["game_id"]` into
    #   `_applied` and `remaining_matches` filters on it, `BracketSportSource`
    #   does the same, and `simulation._same_match` consults an id only when
    #   BOTH rows carry the SAME key (so `fd_id` can never be matched against
    #   another namespace's id). A row that omits `game_id` therefore shares no
    #   key with the fixture pool, and identity silently degrades to the
    #   (home, away, start_time) fallback.
    #
    # That fallback is not equivalent. `points_based.remaining_matches`
    # substitutes a 2099-01-01 sentinel for a fixture with no published
    # start_time, and a RESCHEDULED game (routine in MLB and NFL) simply has a
    # different kickoff in the two separate fetches. Either way the target
    # fails to match itself, is sampled a second time as a "remaining" match,
    # and the contingency table describes a season the recorded W/D/L row does
    # not. Nothing raises: the importance number is just wrong. MLS shipped
    # this live (#65); #181 is the generalised fix.
    #
    # So ANY source that simulates must stamp `game_id`, alongside whatever
    # sport-specific id it also wants to expose. This is deliberately NOT
    # auto-mirrored from a `<sport>_game_id` key in __post_init__: the mirror is
    # only correct where that id really is per-GAME identity, and only the
    # source's author knows whether theirs is (an event id covering several
    # games would map wrongly, and silently). Enforced by
    # tests/test_match_identity_contract.py, which also records the two sources
    # legitimately exempt because they never reach the simulator.
    extra: dict = field(default_factory=dict)  # see note above re: `game_id`


@dataclass(frozen=True)
class MatchResult:
    """One sampled match outcome. Used by the Monte Carlo importance simulator.

    Integer goals/points; ties (draws) are encoded as home_goals == away_goals
    (legal in soccer, vanishingly rare in NCAAF: sources that ban draws
    should never sample one). Sub-game state (penalty shootouts in cup
    knockouts, OT in CFB) is encoded in `extra` per source; the simulator
    core only reads the goal counts.
    """
    home_goals: int
    away_goals: int
    extra: Dict[str, Any] = field(default_factory=dict)


class SportSource(ABC):
    """Adapter contract."""

    # Monte Carlo importance opt-in. Sources that implement the 7 importance
    # methods below flip this to True; the simulator (simulation.py) checks
    # the flag before calling the methods and contributes zero importance
    # points for this source's games when it's false.
    supports_importance: bool = False

    # How many teams carry a rank in the pool `rank_home` / `rank_away` are
    # drawn from. Poll sources (AP Top 25, United Soccer Coaches Top 25,
    # D1Baseball Top 25) are 25 deep, which is the default. A LEAGUE source
    # should leave this alone and emit `extra["standings_table"]` instead:
    # the table's length is the pool and it is already per-competition, so
    # the Premier League's 20 and the Championship's 24 stay distinct without
    # a second place to keep in sync.
    #
    # DO NOT treat this as cosmetic. scoring._rank_strength divides by it, so
    # a wrong value silently rescales every game this source emits.
    #
    # None means "not declared", and scoring applies RANK_POOL_DEFAULT. DO NOT
    # write the poll depth here as a literal: scoring.RANK_POOL_DEFAULT is the
    # single source of truth for it, and a second copy in this file would be
    # free to drift with nothing to report the divergence.
    rank_pool_size: Optional[int] = None

    # How many teams a POLL source's ranks are drawn from (#198): the
    # sport's whole eligible field, e.g. the 138 FBS programmes the AP
    # football poll picks its 25 from. scoring._rank_strength reads a poll
    # rank as a position in this field, so AP #25 is ~0.82 (25th of 138)
    # instead of 0.04 (last of a 25-team list).
    #
    # Declare it on poll sources only. A LEAGUE source must leave it None:
    # its standings table is exhaustive, so the table already IS the field,
    # and plugin.py does not forward a field for an exhaustive pool at all.
    #
    # None means "not declared" and keeps the pre-#198 behaviour (the
    # published pool stands in for the field). The value drifts by a
    # programme or two a year as schools reclassify; that is deliberate
    # tolerance, not a bug: a field of 136 vs 138 moves AP #25 from 0.822 to
    # 0.825. Getting the ORDER of magnitude right (25 of ~140, not 25 of 25)
    # is what matters, so DO NOT fetch it per refresh from an API.
    rank_field_size: Optional[int] = None

    @property
    @abstractmethod
    def sport_prefix(self) -> str:
        """Short prefix for channel names (e.g., 'CFB')."""

    @property
    @abstractmethod
    def sport_label(self) -> str:
        """Human-readable label for logs/UI (e.g., 'NCAA Football')."""

    @abstractmethod
    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Return upcoming games in the next `days_ahead` days. Empty list during
        offseason. Caller does not need to filter by date."""

    # Opt-in to shared-season importance (#230): every scored game of this
    # source is read off ONE set of simulated seasons instead of re-simulating
    # the season per game. Valid ONLY when sample_result ignores `state`, since
    # the shared path samples each target mid-season rather than first. A
    # subclass that reads state in sample_result (a knockout reading leg 1 to
    # sample leg 2) MUST leave this False. tests/test_shared_importance.py
    # enforces that statically for every opted-in class.
    shared_season_importance: bool = False

    # ---------- Monte Carlo importance interface ----------
    # All 7 default to NotImplementedError. Sources that flip
    # `supports_importance = True` MUST override all of them; the simulator
    # calls each one per sim iteration.

    @property
    def outcome_labels(self) -> List[str]:
        """The complete set of possible outcome labels for this competition,
        e.g. `['title', 'UCL', 'Europa', 'relegation']` for EPL.

        These come from the competition's threshold table (`LEAGUE_CONTEXTS`
        for soccer). The simulator uses them to know which outcomes to track;
        the caller (scoring) iterates them to weight by consequence.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def estimate_strengths(self) -> Dict[str, Any]:
        """Per-team baseline used to sample match results. Shape is
        source-defined (only `sample_result` reads it). Soccer returns a dict
        keyed by team name with home/away goal-scored and goal-conceded
        averages; NCAAF returns points averages."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def initial_state(self) -> Dict[str, Any]:
        """Current competition state at the moment importance is being
        computed. Soccer: a dict keyed by team name with played / points /
        gf / ga so the simulator can apply more matches and arrive at a
        final standings table."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def remaining_matches(self, state: Dict[str, Any]) -> List[GameRow]:
        """Matches not yet played given `state`. For league competitions this
        is the full season fixture list filtered to status != FINISHED. For
        knockouts the bracket determines the next round's pairings, which
        themselves depend on prior rounds' results in `state`."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Any],
        rng: random.Random,
    ) -> MatchResult:
        """Sample one outcome for one match. Sport-specific. Pure (rng-driven,
        doesn't mutate `state` or `strengths`)."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def apply_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        result: MatchResult,
    ) -> Dict[str, Any]:
        """Return a new state with `match`'s `result` applied. Pure: the
        simulator depends on apply_result NOT mutating the input so that one
        `initial_state()` can seed many sampled seasons."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        """{team_name: [outcome_labels]} once all matches in `state` are
        played. Labels are a subset of `self.outcome_labels`. A team can
        appear in multiple bands (e.g. winning the title also qualifies for
        UCL); the caller decides how to aggregate."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")
