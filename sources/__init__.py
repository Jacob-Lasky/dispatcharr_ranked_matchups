"""Per-sport data sources. Each source produces a list of GameRow."""
from .base import GameRow, SportSource
from .mlb import MlbPlayoffSource, MlbRegularSource
from .mls import MlsSource
from .nba import NbaPlayoffSource, NbaRegularSource
from .ncaa_baseball import NcaaBaseballSource
from .ncaa_soccer import NcaaSoccerSource
from .ncaaf import NcaafSource
from .ncaam import NcaamSource
from .nhl import NhlPlayoffSource, NhlRegularSource
from .soccer import (
    COMPETITIONS as SOCCER_COMPETITIONS,
    KnockoutSoccerSource,
    SoccerSource,
)

__all__ = [
    "GameRow", "SportSource",
    "MlbRegularSource", "MlbPlayoffSource",
    "MlsSource",
    "NbaRegularSource", "NbaPlayoffSource",
    "NcaaBaseballSource", "NcaaSoccerSource",
    "NcaafSource", "NcaamSource",
    "NhlRegularSource", "NhlPlayoffSource",
    "SoccerSource", "KnockoutSoccerSource", "SOCCER_COMPETITIONS",
]
