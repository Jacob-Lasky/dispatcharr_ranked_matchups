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
games it emits. `_h2h_to_closeness` is imported from mls.py, and the
request goes through sources/_odds.py, to keep the calibration line consistent: the
two sources MUST produce the same closeness for any given matchup as
the legacy MlsSource would have.

ESPN endpoints used:
  - `/apis/v2/sports/soccer/usa.1/standings`: conference rosters +
    current standings points (3 W / 1 D / 0 L). Fetched once per
    refresh; the team→conference map is cached on the source instance.
  - `/apis/site/v2/sports/soccer/usa.1/scoreboard?dates=YYYYMMDD&limit=`:
    one request per day of the Feb-Nov regular season, fetched
    concurrently. See `_fetch_scoreboard_range` for why not one ranged
    call.

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

from ._odds import fetch_odds_events
from .base import GameRow
from .points_based import PointsBasedSportSource
from .mls import (
    _h2h_to_closeness,
    _team_canonical_name,
)
from .._util import http_get_json
from ._espn import extract_espn_scoreboard_event, fetch_scoreboard_days, sweep_upcoming_events

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.mls_standings")

ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard"
ESPN_STANDINGS_URL = "https://site.api.espn.com/apis/v2/sports/soccer/usa.1/standings"

# MLS regular season runs late February through late October in the
# modern era. The season window spans Feb 1 through Nov 30 to be safe
# either side of the bookend weekends.
SEASON_START_MONTH = 2
SEASON_END_MONTH = 11

# Per-team goal averages cluster around 1.4 goals/game in modern MLS
# (slightly higher than NCAA soccer's 1.5/game prior because pro-level
# finishing is sharper). Used as the league-average prior for any team
# the simulator hasn't seen finished results for.
_DEFAULT_GOALS_FOR = 1.4
_DEFAULT_GOALS_AGAINST = 1.4


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """Patch point for this module's HTTP; the body is `_util.http_get_json`."""
    return http_get_json(requests, url, log=logger, tag="mls_standings", timeout=timeout)


def _fetch_scoreboard_range(start: date, end: date) -> List[Dict[str, Any]]:
    """Every ESPN MLS scoreboard event in the day buckets [start, end],
    in date order, ONE REQUEST PER DAY (fetched concurrently).

    DO NOT collapse this back into one `dates=<start>-<end>&limit=` call.
    That form returned the whole 2026 season (511 events) on 2026-08-15
    (#65), then began answering HTTP 400 "Failed to get events endpoint."
    for every team sport; the live plugin log shows this function getting
    400s since at least 2026-10-07 (the oldest retained log), which emptied MLS East/West (no upcoming rows and
    an empty simulated season) with nothing but a WARNING to show for it.
    Measured and explained at `_espn.SCOREBOARD_EVENT_LIMIT` (#182).

    A day that fails is retried once when it is a blip (see
    `_espn.SCOREBOARD_WALK_CHUNK_DAYS`); one still failing contributes
    nothing and the rest still arrive. The walk reports the lost day
    (`_espn.fetch_scoreboard_days`, #261), which is what lets the refresh
    hold the games it had published for that day instead of reading the
    shorter list as a quiet one.
    """
    if end < start:
        return []
    events: List[Dict[str, Any]] = []
    for _day, data in fetch_scoreboard_days(_http_get, ESPN_SCOREBOARD_URL, start, end):
        if isinstance(data, dict):
            events.extend(data.get("events") or [])
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


def _fetch_conference_map() -> Optional[Dict[str, str]]:
    """Pull the current MLS conference assignments from ESPN's /standings
    endpoint. Returns {team_displayName: "East" | "West"}, never a partial
    map: None when the request FAILED or listed clubs for only one
    conference (or none). The importance path then simulates nothing for
    that conference (no spurious signal), and `fetch_upcoming` raises (see
    there), because a quiet [] would read the conference's days as empty.

    The conference roster changes year over year (expansion teams). The
    map is rebuilt on every refresh rather than cached, so an expansion
    team's first game in the new season gets the right conference
    routing without redeploy.
    """
    data = _http_get(ESPN_STANDINGS_URL)
    if not isinstance(data, dict) or not isinstance(data.get("children"), list):
        # Every answer carries the conference tables as `children` (East and
        # West, 15 clubs each, measured 2026-10-11); a body without them (an
        # error object) is a failure.
        return None
    out: Dict[str, str] = {}
    for child in data["children"]:
        conf_abbrev = (child.get("abbreviation") or "").strip()
        if conf_abbrev not in CONFERENCE_ABBREVIATIONS:
            continue
        entries = (child.get("standings") or {}).get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            name = _team_canonical_name(team)
            if name:
                out[name] = conf_abbrev
    if set(out.values()) != set(CONFERENCE_ABBREVIATIONS):
        # A conference with no clubs is not an MLS season; it is a partial or
        # reshaped answer (a renamed abbreviation drops a whole conference).
        # DO NOT return the half map: the missing conference's source would
        # read its days as quiet while the other kept the shared "MLS" label
        # non-empty, and the refresh would drop its published games (#261).
        logger.warning(
            "[mls_standings] /standings listed clubs for %s only; treating "
            "it as a failed request", sorted(set(out.values())) or "no conference",
        )
        return None
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
        self._conference_map_fetched = False
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

    # ---------- conference map (cached per instance, so once per refresh) ----------

    def _conference_map(self) -> Optional[Dict[str, str]]:
        """The conference map, or None when its request failed. Fetched ONCE
        per source, a failure included. DO NOT retry a failure here: the
        strength estimate reads the season through this map when the source
        is BUILT, before `fetch_upcoming`, and caches what it got
        (`points_based`), so a retry that recovered would publish games
        scored against the empty season the failure left behind. Failing
        both, `fetch_upcoming` raises and the refresh holds the games it had."""
        if not self._conference_map_fetched:
            self._conference_map_cache = _fetch_conference_map()
            self._conference_map_fetched = True
        return self._conference_map_cache

    def _own_conf_teams(self) -> Optional[Set[str]]:
        """This conference's clubs, or None when the standings request failed."""
        cmap = self._conference_map()
        if cmap is None:
            return None
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
        # One region, cached a day: see sources/_odds.py for the credit budget.
        data = fetch_odds_events(
            "soccer_usa_mls", self.odds_api_key, "us", log_tag="mls_standings",
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
        UI emit doesn't double-count. Swept through
        `_espn.sweep_upcoming_events`, which owns the window (including the
        US-Eastern lookback day, #256), the FINISHED and stale drops and the
        dedupe.
        """
        own_conf = self._own_conf_teams()
        if own_conf is None:
            # Raise, DO NOT return []: both conference sources share the label
            # "MLS", so when only one conference's standings request failed
            # the label still had games, a quiet [] read that conference's
            # days as having none, and the refresh dropped the games it had
            # published for them. A raise is the outage the refresh holds the
            # previous games for (#240, #261).
            raise RuntimeError(
                "ESPN MLS standings unavailable, so games cannot be routed "
                f"to the {self._conference} conference"
            )
        if not own_conf:
            return []
        closeness_map = self._fetch_closeness()

        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            rec = _extract_game_record(event)
            if rec is None:
                return None
            home = rec["home"]
            if home not in own_conf:
                return None
            # Skip non-regular-season games for V1. MLS Cup playoff
            # bracket is filed under issue #30 part B; until it lands,
            # postseason MLS games should not be emitted here: they
            # would get importance computed against regular-season
            # bands, inflating their score.
            if rec.get("season_slug") != REGULAR_SEASON_SLUG:
                return None
            start = rec.get("start_time")
            if start is None:
                return None
            eid = rec.get("id")
            return GameRow(
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
            )
        return sweep_upcoming_events(_http_get, ESPN_SCOREBOARD_URL, days_ahead, to_row)

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
