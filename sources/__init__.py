"""Per-sport data sources. Each source produces a list of GameRow."""
from .base import GameRow, SportSource
from .mlb import MlbPlayoffSource, MlbRegularSource
from .mls import MlsSource
from .mls_standings import MlsEastSource, MlsWestSource
from .nwsl import NwslSource
from .liga_mx import LigaMxSource
from .field_event import (
    F1Source, NascarSource, GolfSource, UfcSource,
    AtpSource, WtaSource, FieldEventSource,
)
from .nba import NbaPlayoffSource, NbaRegularSource
from .wnba import WnbaPlayoffSource, WnbaRegularSource
from .ncaaw_basketball import (
    NcaawBasketballPlayoffSource, NcaawBasketballRegularSource,
)
from .nfl import NflPlayoffSource, NflRegularSource
from .ncaa_baseball import (
    NcaaBaseballPlayoffBracketSource,
    NcaaBaseballPlayoffSource,
    NcaaBaseballRegularSource,
)
from .ncaa_softball import (
    NcaaSoftballPlayoffBracketSource,
    NcaaSoftballPlayoffSource,
    NcaaSoftballRegularSource,
)
from .ncaa_soccer import NcaaSoccerSource
from .ncaaf import NcaafSource
from .ncaam import NcaamSource
from .nhl import NhlPlayoffSource, NhlRegularSource
from .soccer import (
    COMPETITIONS as SOCCER_COMPETITIONS,
    GroupStageSoccerSource,
    KnockoutSoccerSource,
    SoccerSource,
)

__all__ = [
    "GameRow", "SportSource",
    "MlbRegularSource", "MlbPlayoffSource",
    "MlsSource", "MlsEastSource", "MlsWestSource",
    "NwslSource", "LigaMxSource",
    "FieldEventSource", "F1Source", "NascarSource", "GolfSource", "UfcSource",
    "AtpSource", "WtaSource",
    "NbaRegularSource", "NbaPlayoffSource",
    "WnbaRegularSource", "WnbaPlayoffSource",
    "NcaawBasketballRegularSource", "NcaawBasketballPlayoffSource",
    "NcaaBaseballRegularSource", "NcaaBaseballPlayoffSource", "NcaaBaseballPlayoffBracketSource",
    "NcaaSoftballRegularSource", "NcaaSoftballPlayoffSource", "NcaaSoftballPlayoffBracketSource",
    "NcaaSoccerSource",
    "NcaafSource", "NcaamSource",
    "NhlRegularSource", "NhlPlayoffSource",
    "NflRegularSource", "NflPlayoffSource",
    "SoccerSource", "KnockoutSoccerSource", "GroupStageSoccerSource", "SOCCER_COMPETITIONS",
]
