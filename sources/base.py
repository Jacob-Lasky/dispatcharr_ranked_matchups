"""Common interface for all sport data sources.

A SportSource fetches the upcoming games for one sport/league and the current
ranks (if any) and returns GameRow records. The plugin then scores each row
and matches it to a Dispatcharr channel via EPG.

Phase C adds an OPTIONAL Monte Carlo importance interface — 7 methods plus a
`supports_importance` flag. Sources that implement them get importance-aware
scoring (Lahvička 2012); sources that don't keep the legacy stakes/impact
path. The simulator inspects the flag before calling. See simulation.py.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class GameRow:
    """One upcoming game from one sport. Sport-agnostic."""
    sport_prefix: str           # "CFB", "CBB", "EPL", "EFL", etc. — used in channel name
    sport_label: str            # Full label, e.g., "NCAA Football"
    home: str                   # team name
    away: str
    rank_home: Optional[int]    # current ranking (None if unranked)
    rank_away: Optional[int]
    start_time: datetime        # when the game starts (UTC)
    venue: Optional[str] = None
    spread: Optional[float] = None      # absolute pre-game spread (Phase 3+)
    # B.3 close-game signal: bookmaker-implied coinflip-ness in [0, 1].
    # 1.0 = pick'em (both teams equally likely to win); 0.0 = blowout.
    # Soccer populates this from the h2h moneyline market (devigged
    # probabilities, then 2 * min(p_home, p_away)). NCAAF / NCAAM still
    # populate `spread` instead — score_game normalizes either path into
    # the same [0, 1] effective closeness, but ONE of these two fields
    # is None on every GameRow. DO NOT set both — keeps the contract
    # "closeness wins if present, fall back to spread otherwise" honest.
    closeness: Optional[float] = None
    is_rivalry: bool = False             # known rivalry (Phase 3+)
    extra: dict = field(default_factory=dict)  # source-specific metadata


@dataclass(frozen=True)
class MatchResult:
    """One sampled match outcome. Used by the Monte Carlo importance simulator.

    For soccer / NCAAF / NCAAM we always have integer goals/points; ties
    (draws) are encoded as home_goals == away_goals (legal in soccer,
    vanishingly rare in NCAAF — sources that ban draws should never sample
    one). Sub-game state (penalty shootouts in cup knockouts, OT in CFB) is
    encoded in `extra` per source; the simulator core only reads the goal
    counts.
    """
    home_goals: int
    away_goals: int
    extra: Dict[str, Any] = field(default_factory=dict)


class SportSource(ABC):
    """Adapter contract."""

    # ---------- Phase C: Monte Carlo importance opt-in ----------
    # Sources that implement the 7 importance methods below set this to True.
    # The simulator (simulation.py) checks this flag before calling the
    # methods; falsy → caller skips importance for this source's games and
    # falls back to legacy stakes/impact signals.
    #
    # Class-level default is False so existing sources (NCAAFSource,
    # NCAAMSource, etc.) opt out without needing a code change.
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

    # ---------- Phase C: Monte Carlo importance interface ----------
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
        """Return a new state with `match`'s `result` applied. Pure — the
        simulator depends on apply_result NOT mutating the input so that one
        `initial_state()` can seed many sampled seasons."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")

    def terminal_outcomes(self, state: Dict[str, Any]) -> Dict[str, List[str]]:
        """{team_name: [outcome_labels]} once all matches in `state` are
        played. Labels are a subset of `self.outcome_labels`. A team can
        appear in multiple bands (e.g. winning the title also qualifies for
        UCL); the caller decides how to aggregate."""
        raise NotImplementedError(f"{type(self).__name__} does not support importance simulation")
