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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, SportSource
from .._util import TEAM_SUFFIX_TOKENS, parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mls")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"
ODDS_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT_KEY = "soccer_usa_mls"

# Reuse the canonical TEAM_SUFFIX_TOKENS from _util.py: ("afc","fc","cf","sc").
# DO NOT add "united" here: it's a substantive body word for several MLS
# franchises (Atlanta United, D.C. United, Minnesota United, New York
# Red Bulls' historical "Metrostars United" naming), and stripping it
# collapses different teams together. Same constraint as the soccer.py
# matcher; both code paths reuse the same source of truth.


def _http_get(url: str, timeout: float = 15.0, **params: Any) -> Optional[Any]:
    """HTTP GET wrapper that logs on non-2xx and returns parsed JSON or
    None on any failure. Used for both ESPN (returns dict) and Odds API
    (returns list of dicts) calls: return type is union, caller does
    its own shape check."""
    try:
        r = requests.get(url, timeout=timeout, params=params or None)
        if r.status_code >= 400:
            logger.warning("[mls] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[mls] %s failed: %s", url, exc)
        return None


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
        """Per-day scoreboard sweep over today..today+days_ahead.
        Populate `closeness` from a single Odds API call that returns
        all upcoming matches for this league; ESPN gives the schedule,
        Odds API gives the moneyline market.

        ESPN's scoreboard range syntax (`dates=YYYYMMDD-YYYYMMDD`)
        silently caps at 25 events. MLS has fewer games per day than
        NBA, so daily iteration is overkill (a single range call
        would usually return everything), but per-day matches the
        pattern from ncaa_baseball.py / nba.py and stays safe across
        playoff-week clusters.
        """
        closeness_by_pair = self._fetch_closeness()

        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        espn_base = self._espn_base()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(f"{espn_base}/scoreboard", dates=day.strftime("%Y%m%d"))
            if not isinstance(data, dict):
                continue
            for event in data.get("events") or []:
                gid = event.get("id")
                if gid in seen_ids:
                    continue
                comps = event.get("competitions") or []
                if not comps:
                    continue
                comp = comps[0]
                competitors = comp.get("competitors") or []
                if len(competitors) != 2:
                    continue
                home_obj = next(
                    (c for c in competitors if c.get("homeAway") == "home"), None
                )
                away_obj = next(
                    (c for c in competitors if c.get("homeAway") == "away"), None
                )
                if home_obj is None or away_obj is None:
                    continue
                home = _team_canonical_name(home_obj.get("team") or {})
                away = _team_canonical_name(away_obj.get("team") or {})
                if not home or not away:
                    continue
                start = parse_iso_utc(event.get("date"))
                if start is None:
                    continue
                seen_ids.add(gid)
                closeness = self._lookup_closeness(closeness_by_pair, home, away)
                # ESPN exposes the season/playoff stage in
                # `event.season.slug`. Carry it through as
                # `extra.season_slug` for downstream consumers (the
                # importance simulator filed under #30 will route on
                # it). MLS values: "regular-season", "mls-cup",
                # "mls-cup-playoffs". NWSL: "regular-season",
                # "nwsl-playoffs", etc. Liga MX: "torneo-apertura",
                # "torneo-clausura", "liguilla".
                season_slug = ((event.get("season") or {}).get("slug") or "")
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    closeness=closeness,
                    extra={
                        "espn_event_id": gid,
                        "season_slug": season_slug,
                        "fd_competition_code": self._FD_CODE,
                    },
                ))
        return out

    # ---------- closeness ----------

    def _fetch_closeness(self) -> Dict[Tuple[str, str], float]:
        """Return {(home_lc, away_lc): closeness} for upcoming matches
        in this league. Empty dict if the key is missing or the call
        fails.
        """
        if not self.odds_api_key:
            return {}
        data = _http_get(
            f"{ODDS_BASE}/sports/{self._ODDS_SPORT_KEY}/odds/",
            regions="us,uk,eu",
            markets="h2h",
            apiKey=self.odds_api_key,
            oddsFormat="decimal",
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
