"""Liga MX source — ESPN unofficial + Odds API closeness.

Same V1 minimal pattern as MlsSource (Phase J): schedule + closeness
only, no standings-based importance or Liguilla playoff bracket.

Liga MX runs two seasons per calendar year (Apertura ≈ Jul-Dec,
Clausura ≈ Jan-May), each followed by a 12-team Liguilla playoff.
Season slug values surface as `extra.season_slug`:
  - "torneo-apertura"  — fall season
  - "torneo-clausura"  — spring season
  - "liguilla"         — playoff round (single-leg + two-leg ties)

Liga MX Odds API key: `soccer_mexico_ligamx`. ESPN slug: `soccer/mex.1`.
Relevant for US viewers via Univision / Telemundo EPG entries.
"""

from __future__ import annotations

from .mls import MlsSource


class LigaMxSource(MlsSource):
    """Liga MX: Mexican top-flight soccer. Same schedule+closeness
    adapter as MlsSource with Liga MX-specific endpoint + Odds API
    key overrides."""

    _ESPN_SLUG = "soccer/mex.1"
    _ODDS_SPORT_KEY = "soccer_mexico_ligamx"
    _FD_CODE = "LigaMX"
    _SPORT_PREFIX = "LigaMX"
    _SPORT_LABEL = "Liga MX"
