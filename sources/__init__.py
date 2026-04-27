"""Per-sport data sources. Each source produces a list of GameRow."""
from .base import GameRow, SportSource
from .ncaaf import NcaafSource
from .soccer import SoccerSource, COMPETITIONS as SOCCER_COMPETITIONS

__all__ = ["GameRow", "SportSource", "NcaafSource", "SoccerSource", "SOCCER_COMPETITIONS"]
