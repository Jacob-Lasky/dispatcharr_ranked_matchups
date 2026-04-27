"""Per-sport data sources. Each source produces a list of GameRow."""
from .base import GameRow, SportSource
from .ncaaf import NcaafSource
from .ncaam import NcaamSource
from .soccer import SoccerSource, COMPETITIONS as SOCCER_COMPETITIONS

__all__ = [
    "GameRow", "SportSource",
    "NcaafSource", "NcaamSource",
    "SoccerSource", "SOCCER_COMPETITIONS",
]
