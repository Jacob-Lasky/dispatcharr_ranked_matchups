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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, TypedDict


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
