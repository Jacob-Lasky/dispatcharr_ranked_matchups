"""NWSL source — ESPN unofficial + Odds API closeness.

Same V1 minimal pattern as MlsSource (Phase J): schedule from ESPN +
closeness from The Odds API. No standings-based importance and no
playoff bracket — both are follow-ups. NWSL playoff format mirrors
MLS Cup (mixed best-of-3 and single-leg rounds) and would benefit
from the same bracket work.

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
