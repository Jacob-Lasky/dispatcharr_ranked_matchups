"""NWSL source: ESPN unofficial + Odds API closeness.

Same minimal pattern as MlsSource: schedule from ESPN + closeness
from The Odds API. No standings-based importance and no playoff
bracket: NWSL playoff format mirrors MLS Cup (mixed best-of-3 and
single-leg rounds), tracked under #30.

NWSL Odds API key: `soccer_usa_nwsl`. ESPN slug: `soccer/usa.nwsl`.
"""

from __future__ import annotations

from .mls import MlsSource


class NwslSource(MlsSource):
    """NWSL: National Women's Soccer League. Same schedule+closeness
    adapter as MlsSource with NWSL-specific endpoint + Odds API key
    overrides."""

    _ESPN_SLUG = "soccer/usa.nwsl"
    _ODDS_SPORT_KEY = "soccer_usa_nwsl"
    _FD_CODE = "NWSL"
    _SPORT_PREFIX = "NWSL"
    _SPORT_LABEL = "NWSL"
