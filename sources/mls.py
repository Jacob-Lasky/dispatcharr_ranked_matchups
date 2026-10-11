"""MLS source: ESPN's unofficial `site.api.espn.com` for the schedule
+ The Odds API for closeness. MLS games surface with `favorite +
closeness` scoring only; standings-based importance is filed as #30.

Scope:

  - `fetch_upcoming` returns the next-N-day MLS schedule (regular
    season AND playoffs, undifferentiated). Game records carry
    `closeness` populated from the bookmaker h2h moneyline market via
    The Odds API.
  - **No** `supports_importance` flag. MLS Cup playoff bracket is
    structurally awkward (best-of-3 first round, single-leg subsequent
    rounds: neither BestOfNSeriesSource nor AggregateLegSource fits
    cleanly); conference-standings importance needs its own bands.
    Both are tracked in #30.
  - The Odds API sport key is `soccer_usa_mls`. The Odds API team
    names ("LA Galaxy") often drop the FC/SC tag ESPN includes
    ("LA Galaxy" → "LA Galaxy", but "New York Red Bulls" vs ESPN's
    "Red Bull New York" can differ). The fuzzy team-name matcher is
    lifted directly from the soccer.py SoccerSource pattern.

Plugin opts in via `enable_mls` in `plugin.json`. Off by default.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from ._espn import sweep_upcoming_events
from ._odds import fetch_odds_events
from .base import GameRow, SportSource
from .._util import TEAM_SUFFIX_TOKENS, http_get_json, parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mls")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"
ODDS_SPORT_KEY = "soccer_usa_mls"

# Reuse the canonical TEAM_SUFFIX_TOKENS from _util.py: ("afc","fc","cf","sc").
# DO NOT add "united" here: it's a substantive body word for several MLS
# franchises (Atlanta United, D.C. United, Minnesota United, New York
# Red Bulls' historical "Metrostars United" naming), and stripping it
# collapses different teams together. Same constraint as the soccer.py
# matcher; both code paths reuse the same source of truth.


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """Patch point for this module's HTTP; the body is `_util.http_get_json`."""
    return http_get_json(requests, url, log=logger, tag="mls", timeout=timeout)


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """ESPN soccer returns `team.displayName` ("Atlanta United FC",
    "LA Galaxy"). Use that as the canonical join key: it matches
    most EPG provider titles. Fall back to nickname / abbreviation
    only if displayName is missing."""
    name = (team_obj.get("displayName") or "").strip()
    if name:
        return name
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


def _normalize_for_fuzzy(name: str) -> str:
    """Lowercase + strip common club-tag suffixes so ESPN's
    "Atlanta United FC" matches Odds API's "Atlanta United"."""
    n = name.lower().strip()
    for s in TEAM_SUFFIX_TOKENS:
        tag = " " + s
        if n.endswith(tag):
            n = n[: -len(tag)]
            break
    return n.strip()


def _h2h_to_closeness(
    outcomes: List[Dict[str, Any]], home_lc: str, away_lc: str,
) -> Optional[float]:
    """Devig moneyline outcomes -> closeness in [0, 1].

      1 / decimal_odds -> raw implied probability
      divide by total_implied -> normalized P_home, P_away
      closeness = 2 * min(P_home, P_away)

    A 3-way market (home / draw / away) excludes the draw: closeness
    measures the "either of the two teams could win" intuition, which
    isn't affected by draw probability. Pre-game blowouts (80/15/5)
    -> closeness 0.10; pickem 45/10/45 -> closeness 0.90.

    Mirrors the soccer.py `_h2h_to_closeness` shape so the calibration
    line between MLS and EPL stays consistent.
    """
    implied: Dict[str, float] = {}
    for o in outcomes:
        name = (o.get("name") or "").strip().lower()
        try:
            price = float(o.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        # Identify the home and away outcome rows. Odds API uses team
        # names for the outcome label, e.g. "LA Galaxy" / "Draw" /
        # "Inter Miami CF". Use the lower-cased substring match against
        # the home/away keys we were passed.
        nrm = _normalize_for_fuzzy(name)
        if nrm == _normalize_for_fuzzy(home_lc) or _normalize_for_fuzzy(home_lc) in nrm:
            implied["home"] = 1.0 / price
        elif nrm == _normalize_for_fuzzy(away_lc) or _normalize_for_fuzzy(away_lc) in nrm:
            implied["away"] = 1.0 / price
        # Skip "draw": closeness uses only home + away.
    if "home" not in implied or "away" not in implied:
        return None
    # Devig over home + away only (the draw probability would lower
    # both numbers symmetrically; we want the relative shape, not the
    # absolute level).
    total = implied["home"] + implied["away"]
    if total <= 0:
        return None
    p_home = implied["home"] / total
    p_away = implied["away"] / total
    return round(2.0 * min(p_home, p_away), 4)


class MlsSource(SportSource):
    """MLS schedule + closeness signal. Standings-based importance and
    MLS Cup playoff bracket modeling are tracked in #30.

    Class attrs `_ESPN_SLUG`, `_ODDS_SPORT_KEY`, `_FD_CODE`,
    `_SPORT_PREFIX`, `_SPORT_LABEL` parameterize the league. NwslSource
    and LigaMxSource subclass this and override these five attrs: the
    rest of the fetch/closeness machinery is shared.
    """

    # NOT supports_importance: the importance simulator skips MLS.
    # Importance comes from `favorite` + `closeness` only.

    # Per-league configuration (subclasses override):
    _ESPN_SLUG: str = "soccer/usa.1"
    _ODDS_SPORT_KEY: str = "soccer_usa_mls"
    _FD_CODE: str = "MLS"
    _SPORT_PREFIX: str = "MLS"
    _SPORT_LABEL: str = "MLS"

    def __init__(self, odds_api_key: str = "") -> None:
        self.odds_api_key = odds_api_key

    @property
    def sport_prefix(self) -> str:
        return self._SPORT_PREFIX

    @property
    def sport_label(self) -> str:
        return self._SPORT_LABEL

    def _espn_base(self) -> str:
        return f"https://site.api.espn.com/apis/site/v2/sports/{self._ESPN_SLUG}"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Upcoming games through `_espn.sweep_upcoming_events`, which owns
        the per-day window (including the US-Eastern lookback day, #256), the
        FINISHED and stale drops and the dedupe. Populate `closeness` from a
        single Odds API call that returns all upcoming matches for this league;
        ESPN gives the schedule, Odds API gives the moneyline market.

        One request per day through `_espn.fetch_scoreboard_day`; see
        `_espn.SCOREBOARD_EVENT_LIMIT` for why no multi-day form replaces
        the walk (#182).
        """
        closeness_by_pair = self._fetch_closeness()

        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            comps = event.get("competitions") or []
            if not comps:
                return None
            competitors = comps[0].get("competitors") or []
            if len(competitors) != 2:
                return None
            home_obj = next(
                (c for c in competitors if c.get("homeAway") == "home"), None
            )
            away_obj = next(
                (c for c in competitors if c.get("homeAway") == "away"), None
            )
            if home_obj is None or away_obj is None:
                return None
            home = _team_canonical_name(home_obj.get("team") or {})
            away = _team_canonical_name(away_obj.get("team") or {})
            if not home or not away:
                return None
            start = parse_iso_utc(event.get("date"))
            if start is None:
                return None
            # ESPN exposes the season/playoff stage in
            # `event.season.slug`. Carry it through as
            # `extra.season_slug` for downstream consumers (the
            # importance simulator filed under #30 will route on
            # it). MLS values: "regular-season", "mls-cup",
            # "mls-cup-playoffs". NWSL: "regular-season",
            # "nwsl-playoffs", etc. Liga MX: "torneo-apertura",
            # "torneo-clausura", "liguilla".
            season_slug = ((event.get("season") or {}).get("slug") or "")
            return GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=None,
                rank_away=None,
                start_time=start,
                closeness=self._lookup_closeness(closeness_by_pair, home, away),
                extra={
                    "espn_event_id": event.get("id"),
                    "season_slug": season_slug,
                    "fd_competition_code": self._FD_CODE,
                },
            )
        return sweep_upcoming_events(
            _http_get, f"{self._espn_base()}/scoreboard", days_ahead, to_row,
        )

    # ---------- closeness ----------

    def _fetch_closeness(self) -> Dict[Tuple[str, str], float]:
        """Return {(home_lc, away_lc): closeness} for upcoming matches
        in this league. Empty dict if the key is missing or the call
        fails.
        """
        if not self.odds_api_key:
            return {}
        # One region, cached a day: see sources/_odds.py for the credit budget.
        data = fetch_odds_events(
            self._ODDS_SPORT_KEY, self.odds_api_key, "us", log_tag=f"odds:{self._ODDS_SPORT_KEY}",
        )
        if not isinstance(data, list):
            return {}
        out: Dict[Tuple[str, str], float] = {}
        for ev in data:
            home = (ev.get("home_team") or "").strip().lower()
            away = (ev.get("away_team") or "").strip().lower()
            if not home or not away:
                continue
            books = ev.get("bookmakers") or []
            closeness_val: Optional[float] = None
            for bk in books:
                for mk in bk.get("markets", []):
                    if mk.get("key") != "h2h":
                        continue
                    closeness_val = _h2h_to_closeness(
                        mk.get("outcomes") or [], home, away,
                    )
                    if closeness_val is not None:
                        break
                if closeness_val is not None:
                    break
            if closeness_val is not None:
                out[(home, away)] = closeness_val
        return out

    @staticmethod
    def _lookup_closeness(
        closeness_map: Dict[Tuple[str, str], float],
        home: str,
        away: str,
    ) -> Optional[float]:
        """Match ESPN team names to Odds API team names with a fuzzy
        fallback. ESPN uses "Atlanta United FC", Odds API uses
        "Atlanta United" (no suffix); the normalize step makes those
        align. Same shape as soccer.py's `_lookup_odds` but specific
        to MLS-style names."""
        h_lc = home.lower()
        a_lc = away.lower()
        if (h_lc, a_lc) in closeness_map:
            return closeness_map[(h_lc, a_lc)]
        h_n = _normalize_for_fuzzy(home)
        a_n = _normalize_for_fuzzy(away)
        for (hk, ak), v in closeness_map.items():
            hk_n = _normalize_for_fuzzy(hk)
            ak_n = _normalize_for_fuzzy(ak)
            if (h_n == hk_n or h_n in hk_n or hk_n in h_n) and (
                a_n == ak_n or a_n in ak_n or ak_n in a_n
            ):
                return v
        return None
