"""Common interface for all sport data sources.

A SportSource fetches the upcoming games for one sport/league and the current
ranks (if any) and returns GameRow records. The plugin then scores each row
and matches it to a Dispatcharr channel via EPG.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


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
    is_rivalry: bool = False             # known rivalry (Phase 3+)
    extra: dict = field(default_factory=dict)  # source-specific metadata


class SportSource(ABC):
    """Adapter contract."""

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
