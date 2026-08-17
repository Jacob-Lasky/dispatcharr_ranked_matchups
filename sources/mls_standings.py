"""MLS conference-standings importance source: closes the gap left by
the Phase J V1 MlsSource (which surfaced schedule + closeness only).

Issue #30 (part A): MLS playoff seeding is per-conference (top 9 from
Eastern, top 9 from Western), not aggregate league points. Modeling
importance with a single league-wide threshold misclassifies bubble
teams when one conference is point-rich and the other point-poor.

Two source classes, one per conference, both extend the same
MlsStandingsSourceBase mixin which carries the 3 W / 1 D / 0 L
standings-points logic and the ESPN scoreboard fetch. Pattern mirrors
NCAA Soccer (which uses the same 3/1/0 scheme) more than NHL (which
uses a single league-wide source). Cross-conference games:
  - `_fetch_full_season_games` filters to intra-conference games only
    so the simulator's `_teams` dict never picks up out-of-conference
    rows (which would get the wrong threshold bands applied). Strength
    estimates lose ~9 cross-conf games per team out of ~34, an
    acceptable accuracy cost vs the alternative of polluting the
    outcome cascade.
  - `fetch_upcoming` emits games where the HOME team is in this
    conference. Cross-conf away games surface via the home team's
    source: they still get importance computed (against the home
    team's conference bands), they just don't double-emit.

Note: this module does NOT touch the existing MlsSource (which remains
the closeness-only base class for NwslSource / LigaMxSource). The
plugin swaps `enable_mls` to register East + West here rather than
the closeness-only MlsSource. NWSL / Liga MX keep the V1 shape until
their own importance follow-ups land.

Closeness via The Odds API is preserved (the V1 MlsSource computed
it and we don't regress that signal): each conference source pulls
the league-wide odds feed and attaches devigged h2h closeness to the
games it emits. Helpers `_h2h_to_closeness` and `ODDS_BASE` are
imported from mls.py to keep the calibration line consistent: the
two sources MUST produce the same closeness for any given matchup as
the legacy MlsSource would have.

ESPN endpoints used:
  - `/apis/v2/sports/soccer/usa.1/standings`: conference rosters +
    current standings points (3 W / 1 D / 0 L). Fetched once per
    refresh; the team→conference map is cached on the source instance.
  - `/apis/site/v2/sports/soccer/usa.1/scoreboard?dates=<range>&limit=`:
    the Feb-Nov regular season, whole window in one call. See
    SCOREBOARD_EVENT_LIMIT for why `limit` is not optional.

Remaining-fixtures coverage (#65, resolved 2026-08-15). This module used
to carry a "known limitation" claiming ESPN publishes only ~1-2 weeks of
future MLS fixtures, and that live mid-season importance therefore read
~0 through no fault of the code. That diagnosis was WRONG, and the two
halves of it failed differently:

  - ESPN publishes the full remaining season on the SCOREBOARD endpoint.
    Measured live mid-season 2026-08-15 against `soccer/usa.1`: 241
    future events through the Nov 8 regular-season finale, 195 of them
    intra-conference (95 East / 100 West), 11-15 remaining per team.
    What IS thin is `/teams/{id}/schedule`, which returned 0 future
    events for the same teams on the same day. The original probe read
    the thin endpoint and generalized to the wrong one.
  - `_fetch_full_season_games` clamped its own sweep to `now + 7 days`.
    THAT is what starved the simulator. Importance read ~0 because the
    code asked for one week of the season it needed all of.

So the fix was to stop truncating, not to synthesize fixtures. DO NOT
re-clamp the importance sweep to a near-term window, and DO NOT add a
round-robin pairing synthesizer on the theory that the fixtures are
missing: they are not, and synthesized pairings would duplicate real
ones that the pool already holds.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from .mls import (
    ODDS_BASE as _ODDS_BASE,
    _h2h_to_closeness,
    _team_canonical_name,
)
from .._util import parse_iso_utc, redact_secrets
from ._espn import extract_espn_scoreboard_event

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mls_standings")

ESPN_SCOREBOARD_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1"
ESPN_STANDINGS_URL = "https://site.api.espn.com/apis/v2/sports/soccer/usa.1/standings"

# MLS regular season runs late February through late October in the
# modern era. The season window spans Feb 1 through Nov 30 to be safe
# either side of the bookend weekends.
SEASON_START_MONTH = 2
SEASON_END_MONTH = 11

# ESPN's scoreboard accepts `dates=YYYYMMDD-YYYYMMDD`, but it PAGINATES
# and truncates silently: without an explicit `limit` a range returns
# only its first 100 events and says nothing about the rest. Passing
# `limit` lifts the page size and the whole window arrives in one call.
#
# DO NOT drop this param and DO NOT go back to a per-day sweep to work
# around a short result. Measured live 2026-08-15 against
# `soccer/usa.1`, full 2026 season window (Feb 1 - Nov 30):
#   dates=<range>                -> 100 events (silently truncated)
#   dates=<range>&limit=1000     -> 511 events
#   303 individual per-day calls -> 511 events
# The range-with-limit and per-day event-ID sets were IDENTICAL, with
# identical kickoff timestamps on every shared id. One call replaces
# 303. `_fetch_scoreboard_range` warns if a response ever comes back at
# the limit, which is the only symptom truncation would show.
SCOREBOARD_EVENT_LIMIT = 1000

# Per-team goal averages cluster around 1.4 goals/game in modern MLS
# (slightly higher than NCAA soccer's 1.5/game prior because pro-level
# finishing is sharper). Used as the league-average prior for any team
# the simulator hasn't seen finished results for.
_DEFAULT_GOALS_FOR = 1.4
_DEFAULT_GOALS_AGAINST = 1.4


def _http_get(url: str, timeout: float = 15.0, **params: Any) -> Optional[Any]:
    """HTTP GET wrapper. Return type is Optional[Any] because the
    callers hit two shapes: ESPN endpoints return dicts (standings,
    scoreboard) while The Odds API returns a JSON list. Caller checks
    via isinstance before iterating.
    """
    try:
        r = requests.get(url, timeout=timeout, params=params or None)
        if r.status_code >= 400:
            logger.warning("[mls_standings] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        # redact_secrets on BOTH: odds calls pass apiKey as a query param, and a
        # requests ConnectionError renders the full URL (params included) in its
        # message even though `url` here is the bare base. See #156.
        logger.warning("[mls_standings] %s failed: %s", redact_secrets(url), redact_secrets(exc))
        return None


def _fetch_scoreboard_range(start: date, end: date) -> List[Dict[str, Any]]:
    """Every ESPN MLS scoreboard event with a kickoff in [start, end].

    One HTTP call for the whole window (see SCOREBOARD_EVENT_LIMIT for
    why the `limit` param is mandatory). Returns [] on any failure, which
    the callers treat as "no games" rather than an error: a refresh that
    can't reach ESPN emits nothing instead of a wrong signal.
    """
    if end < start:
        return []
    window = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    data = _http_get(
        f"{ESPN_SCOREBOARD_BASE}/scoreboard",
        timeout=30.0,
        dates=window,
        limit=SCOREBOARD_EVENT_LIMIT,
    )
    if not isinstance(data, dict):
        return []
    events = data.get("events") or []
    if len(events) >= SCOREBOARD_EVENT_LIMIT:
        # Only symptom ESPN gives for a truncated page. A full MLS season
        # is ~511 events, so hitting the limit means either the window grew
        # or ESPN changed its paging: say so rather than silently modeling
        # a partial season.
        logger.warning(
            "[mls_standings] scoreboard dates=%s returned %d events, at the "
            "requested limit of %d: the result may be truncated",
            window, len(events), SCOREBOARD_EVENT_LIMIT,
        )
    return events


# `_team_canonical_name` is imported from mls.py: both modules need
# the same ESPN displayName join key across /standings and /scoreboard
# endpoints. DO NOT define a parallel canonicalizer here; divergence
# between the two would break the cross-endpoint team lookup.


# ESPN's season.slug value for MLS regular season. Playoff games have
# slugs like "eastern-conference-playoffs---round-one" or "mls-cup";
# the regular-season filter excludes them so postseason matchups don't
# pollute the bands the threshold cascade is calibrated against.
REGULAR_SEASON_SLUG = "regular-season"

# ESPN exposes MLS conference rosters under /standings → children[].abbreviation
# as either "East" or "West". DO NOT use `children[].name` ("Eastern
# Conference" / "Western Conference"): match the short token to keep
# `MlsEastSource._conference` / `MlsWestSource._conference` concise.
CONFERENCE_ABBREVIATIONS = ("East", "West")


def _fetch_conference_map() -> Dict[str, str]:
    """Pull the current MLS conference assignments from ESPN's /standings
    endpoint. Returns {team_displayName: "East" | "West"}. Empty dict
    on any failure: the source falls back to emitting zero games for
    that conference (no spurious importance signal).

    The conference roster changes year over year (expansion teams). The
    map is rebuilt on every refresh rather than cached, so an expansion
    team's first game in the new season gets the right conference
    routing without redeploy.
    """
    data = _http_get(ESPN_STANDINGS_URL)
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for child in data.get("children") or []:
        conf_abbrev = (child.get("abbreviation") or "").strip()
        if conf_abbrev not in CONFERENCE_ABBREVIATIONS:
            continue
        entries = (child.get("standings") or {}).get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            name = _team_canonical_name(team)
            if name:
                out[name] = conf_abbrev
    return out


def _extract_game_record(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert one ESPN MLS scoreboard event into the canonical
    PointsBasedSportSource game record. Adds `season_slug` (top-level
    AND inside `extra`) so callers can route bracket vs regular-season
    rows. Soccer-specific: a tied score in a finished regulation game
    IS a draw (1 point each) -- the shared parser preserves this
    (NCAAF / NCAAM coin-flip the tie upstream, not here).
    """
    def _mls_extras(ev: Dict[str, Any]) -> Dict[str, Any]:
        season_slug = ((ev.get("season") or {}).get("slug") or "").lower()
        return {"season_slug": season_slug, "extra": {"season_slug": season_slug}}

    return extract_espn_scoreboard_event(
        event, team_namer=_team_canonical_name, extras_fn=_mls_extras,
    )


class MlsStandingsSourceBase(PointsBasedSportSource):
    """Shared logic for MlsEastSource / MlsWestSource. Subclasses set
    `_conference` ("East" | "West") and the matching
    `league_context_code` ("MLS_EAST" | "MLS_WEST"). Everything else:
    the standings fetch, the scoreboard range sweep, the 3 / 1 / 0
    `_record_result_into_state` override: is shared here.
    """

    _count_field = "standings_points"
    _DEFAULT_POINTS_FOR = _DEFAULT_GOALS_FOR
    _DEFAULT_POINTS_AGAINST = _DEFAULT_GOALS_AGAINST

    # Subclasses MUST set these.
    _conference: str = ""

    def __init__(
        self,
        odds_api_key: str = "",
        season_year: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.odds_api_key = odds_api_key
        now = datetime.now(timezone.utc)
        # MLS season is named by calendar year (the 2025 season runs Feb-Nov
        # 2025). Default to current calendar year; pre-February in early
        # winter reads as the prior season (MLS Cup wraps in early Dec, so
        # January is firmly between seasons: default to prior year so
        # offseason refreshes still pick up the last-finished season's data).
        self.season_year = (
            season_year if season_year is not None
            else (now.year if now.month >= SEASON_START_MONTH else now.year - 1)
        )
        self._conference_map_cache: Optional[Dict[str, str]] = None
        self._closeness_cache: Optional[Dict[Tuple[str, str], float]] = None

    @property
    def sport_prefix(self) -> str:
        return "MLS"

    @property
    def sport_label(self) -> str:
        # The same channel label for both conferences keeps the user's
        # Top Matchups group cohesive. The conference identity lives in
        # the league_context_code, not the display label.
        return "MLS"

    # ---------- conference map (shared across instances per-refresh) ----------

    def _conference_map(self) -> Dict[str, str]:
        if self._conference_map_cache is not None:
            return self._conference_map_cache
        self._conference_map_cache = _fetch_conference_map()
        return self._conference_map_cache

    def _own_conf_teams(self) -> Set[str]:
        cmap = self._conference_map()
        return {name for name, conf in cmap.items() if conf == self._conference}

    # ---------- closeness (carried over from V1 MlsSource) ----------

    def _fetch_closeness(self) -> Dict[Tuple[str, str], float]:
        """Pull h2h odds for upcoming MLS matches from The Odds API and
        compute devigged closeness in [0, 1]. Returns {(home_lc, away_lc):
        closeness}. Empty dict on missing key or any API failure.

        Reuses the MlsSource pattern verbatim (helper functions imported
        from mls.py) so the calibration line between MlsEastSource and
        MlsWestSource matches the legacy MlsSource closeness output.
        """
        if not self.odds_api_key:
            return {}
        if self._closeness_cache is not None:
            return self._closeness_cache
        data = _http_get(
            f"{_ODDS_BASE}/sports/soccer_usa_mls/odds/",
            regions="us,uk,eu",
            markets="h2h",
            apiKey=self.odds_api_key,
            oddsFormat="decimal",
        )
        out: Dict[Tuple[str, str], float] = {}
        if isinstance(data, list):
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
        self._closeness_cache = out
        return out

    @staticmethod
    def _lookup_closeness(
        closeness_map: Dict[Tuple[str, str], float],
        home: str, away: str,
    ) -> Optional[float]:
        """Match canonical team names to Odds API team names. Exact
        lower-case match first, then a substring fallback for cases
        where ESPN includes a suffix the Odds API drops ("Atlanta
        United FC" vs "Atlanta United"). Same shape as MlsSource's
        legacy helper.
        """
        h_lc = home.lower()
        a_lc = away.lower()
        if (h_lc, a_lc) in closeness_map:
            return closeness_map[(h_lc, a_lc)]
        for (hk, ak), v in closeness_map.items():
            if (h_lc == hk or h_lc in hk or hk in h_lc) and (
                a_lc == ak or a_lc in ak or ak in a_lc
            ):
                return v
        return None

    # ---------- fetch_upcoming ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Emit upcoming games where the HOME team is in this conference.
        Cross-conf away games surface via the home team's source so the
        UI emit doesn't double-count.
        """
        own_conf = self._own_conf_teams()
        if not own_conf:
            return []
        closeness_map = self._fetch_closeness()
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: Set[Any] = set()
        for event in _fetch_scoreboard_range(
            today, today + timedelta(days=days_ahead),
        ):
            rec = _extract_game_record(event)
            if rec is None:
                continue
            eid = rec.get("id")
            if eid in seen_ids:
                continue
            home = rec["home"]
            if home not in own_conf:
                continue
            # Skip non-regular-season games for V1. MLS Cup playoff
            # bracket is filed under issue #30 part B; until it lands,
            # postseason MLS games should not be emitted here: they
            # would get importance computed against regular-season
            # bands, inflating their score.
            if rec.get("season_slug") != REGULAR_SEASON_SLUG:
                continue
            seen_ids.add(eid)
            start = rec.get("start_time")
            if start is None:
                continue
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=rec["away"],
                rank_home=None,
                rank_away=None,
                start_time=start,
                closeness=self._lookup_closeness(
                    closeness_map, home, rec["away"],
                ),
                extra={
                    "espn_event_id": eid,
                    # Same ESPN event id under the key the importance
                    # simulator uses for match identity. DO NOT drop
                    # this in favour of `espn_event_id` alone:
                    # `PointsBasedSportSource.apply_result` records
                    # `extra["game_id"]` into `_applied` and
                    # `remaining_matches` filters on it, so a target
                    # row without `game_id` is invisible to the
                    # target-match dedup and gets simulated TWICE per
                    # season the moment the fixture pool holds a
                    # date-less row (#65). `espn_event_id` stays for
                    # the cache.json consumers already reading it.
                    "game_id": eid,
                    "fd_competition_code": self.league_context_code,
                    "season_slug": rec.get("season_slug"),
                },
            ))
        return out

    # ---------- _fetch_full_season_games (importance side) ----------

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """The WHOLE season, Feb 1 - Nov 30 of `season_year`: finished
        games AND every published future fixture.

        Filters to INTRA-CONFERENCE regular-season games only: the
        simulator's `_teams` dict then never picks up out-of-conference
        teams (which would get the wrong threshold bands applied in
        terminal_outcomes).

        Cross-conf games are lost from strength estimation here (~9 per
        team per season vs ~34 total games per team). The accuracy hit
        on Poisson lambda estimates is small; the alternative of
        pollution of terminal_outcomes is structurally wrong.

        DO NOT clamp the window to the near future the way `fetch_upcoming`
        does. This is the importance path, not the emit path: the
        simulator projects each season to its END, so it needs every
        remaining fixture, not the ones about to kick off. An earlier
        version stopped at `now + 7 days` and was the direct cause of
        MLS importance reading ~0 all season (#65) -- with one week of
        fixtures left to play, no Monte Carlo run can separate a bubble
        team's final standing from any other, so tau-c collapsed to 0 on
        every band. Measured live 2026-08-15 mid-season: the 7-day clamp
        yielded 15 remaining Eastern-Conference fixtures against the 95
        ESPN had actually published.
        """
        own_conf = self._own_conf_teams()
        if not own_conf:
            return []
        seen: Dict[Any, Dict[str, Any]] = {}
        season_start = datetime(
            self.season_year, SEASON_START_MONTH, 1, tzinfo=timezone.utc
        ).date()
        # End-of-November: build December 1, subtract one day.
        season_end = (datetime(
            self.season_year, SEASON_END_MONTH + 1, 1, tzinfo=timezone.utc
        ).date()) - timedelta(days=1)
        for event in _fetch_scoreboard_range(season_start, season_end):
            rec = _extract_game_record(event)
            if rec is None or rec["id"] is None:
                continue
            if rec["id"] in seen:
                continue
            # Intra-conference regular season only.
            if rec.get("season_slug") != REGULAR_SEASON_SLUG:
                continue
            if rec["home"] not in own_conf:
                continue
            if rec["away"] not in own_conf:
                continue
            seen[rec["id"]] = rec
        return list(seen.values())

    # ---------- sample_result (allows draws: soccer rules) ----------

    def sample_result(self, state, match, strengths, rng):
        """Sample Poisson goals per side. Soccer regulation results
        include draws: DO NOT coin-flip a tied score into a win the
        way the base PointsBasedSportSource does for NCAAF / NCAAM /
        NHL. MLS regular-season games CAN end in regulation draws
        (MLS dropped the shootout tiebreaker in 2000). A draw banks 1
        standings point for each side.
        """
        del state  # interface-required, not used at this level
        from .._util import poisson_sample
        from .base import MatchResult
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.05, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        return MatchResult(
            home_goals=poisson_sample(lam_home, rng),
            away_goals=poisson_sample(lam_away, rng),
        )

    # ---------- 3 / 1 / 0 standings-points record ----------

    def _record_result_into_state(
        self,
        teams: Dict[str, Dict[str, int]],
        home: str, away: str,
        home_pts: int, away_pts: int,
        result_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """MLS standings scheme mirrors college soccer's 3 / 1 / 0:
        win = 3 standings points, draw = 1 each, loss = 0. Subclass
        of `ncaa_soccer._record_result_into_state` structurally:
        same pattern, no shared inheritance because the rest of the
        ESPN endpoint shape differs.
        """
        del result_extra  # not used: MLS regular-season ties bank 1 pt each
        h = teams[home]
        a = teams[away]
        h.setdefault("draws", 0)
        a.setdefault("draws", 0)
        h.setdefault("standings_points", 0)
        a.setdefault("standings_points", 0)
        h["pf"] += home_pts
        h["pa"] += away_pts
        a["pf"] += away_pts
        a["pa"] += home_pts
        h["games_played"] += 1
        a["games_played"] += 1
        if home_pts > away_pts:
            h["wins"] += 1
            a["losses"] += 1
            h["standings_points"] += 3
        elif away_pts > home_pts:
            a["wins"] += 1
            h["losses"] += 1
            a["standings_points"] += 3
        else:
            # Draw: 1 standings point each, no W/L update.
            h["draws"] += 1
            a["draws"] += 1
            h["standings_points"] += 1
            a["standings_points"] += 1


class MlsEastSource(MlsStandingsSourceBase):
    """Eastern Conference standings importance. Threshold bands from
    LEAGUE_CONTEXTS['MLS_EAST']: 30 / 45 / 55 / 70 standings points
    correspond to playoff bubble / playoff secured / top-4 home field /
    Supporters' Shield contender. Conference identity lives in
    `_conference = "East"`; the league_context_code routes the threshold
    cascade.
    """
    _conference = "East"
    league_context_code = "MLS_EAST"


class MlsWestSource(MlsStandingsSourceBase):
    """Western Conference standings importance. Same threshold bands as
    East (the conference cutoff lines have been within a couple of
    points of each other for the modern 14-9 playoff format)."""
    _conference = "West"
    league_context_code = "MLS_WEST"
