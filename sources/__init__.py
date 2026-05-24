"""Per-sport data sources. Each source produces a list of GameRow."""
from .base import GameRow, SportSource
from .ncaaf import NcaafSource
from .ncaam import NcaamSource
from .soccer import (
    COMPETITIONS as SOCCER_COMPETITIONS,
    KnockoutSoccerSource,
    SoccerSource,
)

__all__ = [
    "GameRow", "SportSource",
    "NcaafSource", "NcaamSource",
    "SoccerSource", "KnockoutSoccerSource", "SOCCER_COMPETITIONS",
]
