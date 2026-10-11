"""Field-event source: racing and golf.

Racing and golf don't fit the team-based GameRow model because they're
field events: one race or tournament with 20+ competitors and a
finishing order, not a head-to-head outcome between two teams.

Design:

  - Emit one GameRow per scheduled event, with `home = event_name`
    (e.g., "Heineken Chinese GP", "The Masters") and `away = "Field"`
    as a sentinel.
  - Tag with `tournament_stage = "EVENT"` (regular tour stop),
    `"TOUR_PLAYOFF"` (a tour's own playoff events, between the two:
    golf's FedEx Cup playoffs, #188) or `"MAJOR"` (golf majors /
    marquee racing events the source flags as MAJOR). The scoring
    layer's `tournament` signal handles the rest; every label maps to
    a non-zero stage_score so field events always surface in the guide
    when toggled on.
  - No importance simulation, no closeness, no rank: the rarity of
    these events (~1/week) means just-show-it-up is the right product.
    The matcher fuzzy-matches the event name against EPG titles.

Subclasses parameterize the league via class attrs (ESPN_SLUG,
SPORT_PREFIX, SPORT_LABEL, MAJOR_REGEX). The base does all the work.

Fetched one scoreboard day at a time like every other ESPN source; see
`FieldEventSource.fetch_upcoming` for why the range form is not used.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Pattern

import requests

from .base import GameRow, SportSource
from ._espn import sweep_upcoming_events
from .._util import FIELD_AWAY_SENTINEL, field_event_extra, http_get_json, parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.field_event")


class FieldEventSource(SportSource):
    """ABC for field-event leagues (racing, golf). Subclasses set
    five class attrs:

      - `ESPN_SLUG`: path under `/apis/site/v2/sports/` (e.g.,
        "racing/f1", "racing/nascar-premier", "golf/pga").
      - `SPORT_PREFIX`: short channel-name prefix.
      - `SPORT_LABEL`: human-readable label.
      - `FD_COMPETITION_CODE`: arbitrary string carried through extra
        for downstream routing (e.g., score-breakdown attribution).
      - `MAJOR_REGEX`: optional compiled regex; when an event name
        matches, the event is tagged tournament_stage="MAJOR"
        (otherwise "EVENT"). Default None means everything is "EVENT".
      - `PLAYOFF_REGEX`: optional compiled regex for the tour's playoff
        events, tagged "TOUR_PLAYOFF". MAJOR wins when both match.
    """

    # supports_importance left at the SportSource default (False).
    # Field events surface via the tournament signal alone.

    ESPN_SLUG: str = ""
    SPORT_PREFIX: str = ""
    SPORT_LABEL: str = ""
    FD_COMPETITION_CODE: str = ""
    MAJOR_REGEX: Optional[Pattern[str]] = None
    PLAYOFF_REGEX: Optional[Pattern[str]] = None

    @property
    def sport_prefix(self) -> str:
        return self.SPORT_PREFIX

    @property
    def sport_label(self) -> str:
        return self.SPORT_LABEL

    def _espn_base(self) -> str:
        return f"https://site.api.espn.com/apis/site/v2/sports/{self.ESPN_SLUG}"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull events in the day buckets of `_espn.upcoming_window`, one
        scoreboard request per day (`_espn.fetch_scoreboard_day`), through
        `_espn.sweep_upcoming_events(field_events=True)`.

        The window starts one day BEFORE today-UTC (#256): ESPN files a
        UFC main card or a NASCAR night race under its US Eastern date, so
        from 00:00Z the event in progress sits in yesterday's bucket. Only
        that lookback bucket is filtered (over and stale events dropped,
        by event-level status); the forward buckets keep
        every event as they always have. `_espn._field_event_current` has
        the measurements behind both halves.

        DO NOT go back to one `dates=<today>-<end>` range call. It is the
        form ESPN now rejects with HTTP 400 for every team sport, and on
        these feeds, where it still answers, it EXCLUDES the window's last
        day: measured 2026-10-11, `racing/nascar-premier` 20261011-20261018
        returned the Charlotte race but not Phoenix on Oct 18, which the
        per-day walk finds (#182; see `_espn.SCOREBOARD_EVENT_LIMIT`). A
        multi-day event (a golf or tennis tournament) appears in every day's
        bucket it spans, so the sweep's dedupe keeps its first kept copy.
        """
        def to_row(event: Dict[str, Any]) -> Optional[GameRow]:
            name = (event.get("name") or "").strip()
            short_name = (event.get("shortName") or "").strip()
            display_name = name or short_name
            if not display_name:
                return None
            start = parse_iso_utc(event.get("date"))
            if start is None:
                return None
            # Detect majors so the score gets bumped to the MAJOR tier
            # (e.g., golf's Masters / PGA / US Open / British Open), then the
            # tour's playoff events (golf's FedEx Cup playoffs).
            stage = "EVENT"
            if self.MAJOR_REGEX is not None and self.MAJOR_REGEX.search(display_name):
                stage = "MAJOR"
            elif self.PLAYOFF_REGEX is not None and self.PLAYOFF_REGEX.search(display_name):
                stage = "TOUR_PLAYOFF"
            return GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=display_name,
                away=FIELD_AWAY_SENTINEL,
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra=field_event_extra(
                    self.FD_COMPETITION_CODE,
                    stage,
                    short_name,
                    espn_event_id=event.get("id"),
                ),
            )
        return sweep_upcoming_events(
            _http_get, f"{self._espn_base()}/scoreboard", days_ahead, to_row,
            field_events=True,
        )


def _http_get(url: str, timeout: float = 15.0) -> Optional[Any]:
    """Patch point for this module's HTTP; the body is `_util.http_get_json`."""
    return http_get_json(requests, url, log=logger, tag="field_event", timeout=timeout)


# =====================================================================
# Concrete subclasses
# =====================================================================


class F1Source(FieldEventSource):
    """Formula 1: ~24 Grands Prix per year. Each weekend covers
    practice + qualifying + race; ESPN's scoreboard usually returns
    one event per GP date with the race itself."""

    ESPN_SLUG = "racing/f1"
    SPORT_PREFIX = "F1"
    SPORT_LABEL = "Formula 1"
    FD_COMPETITION_CODE = "F1"
    # F1 has no "major" tier in the same sense as golf; the Monaco
    # and Italian GPs are historically marquee but the points-per-race
    # is identical across the calendar. No MAJOR_REGEX.


class NascarSource(FieldEventSource):
    """NASCAR Cup Series: ~36 races per year, including the Daytona
    500 (season opener) and the Coca-Cola 600. Both are 'crown jewels'
    but currently every race is EVENT-tier (no MAJOR_REGEX set)."""

    ESPN_SLUG = "racing/nascar-premier"
    SPORT_PREFIX = "NASCAR"
    SPORT_LABEL = "NASCAR Cup Series"
    FD_COMPETITION_CODE = "NASCAR"


# Golf majors. The Masters / PGA Championship / US Open / British
# Open (The Open Championship) are the four majors. Each typically
# runs Thursday-Sunday and shows up on ESPN as a single multi-day
# tournament event whose name contains the major name. Regex is
# liberal in word ordering to cover ESPN's stylings ("The Masters",
# "Masters Tournament", "U.S. Open Championship", "The Open
# Championship", "PGA Championship"). "PGA Championship" must not be the
# tail of a longer name: the DP World Tour's "BMW PGA Championship" and the
# "Senior PGA Championship" are not majors.
_GOLF_MAJOR_RE: Pattern[str] = re.compile(
    r"\b(masters|(?<!bmw\s)(?<!senior\s)pga\s+championship|u\.?s\.?\s+open|"
    r"the\s+open(?:\s+championship)?|british\s+open)\b",
    re.IGNORECASE,
)


# The FedEx Cup playoffs (#188): three events in August that decide the
# season-long title, so they rank above a regular week and below the majors.
# Names as ESPN publishes them (checked 2025 and 2026): "FedEx St. Jude
# Championship", "BMW Championship", "TOUR Championship". Each alternative is
# narrow on purpose:
#   - "st. jude CHAMPIONSHIP", not "st. jude": the pre-2022 "FedEx St. Jude
#     Classic" was an ordinary tour stop.
#   - "bmw championship" does not match the DP World Tour's "BMW PGA
#     Championship" (and that one is not on this feed).
#   - "tour championship" is anchored to the start of the name, so it cannot
#     match some "<sponsor> Tour Championship" on another tour, and "PGA TOUR
#     Q-School" does not contain it.
_GOLF_PLAYOFF_RE: Pattern[str] = re.compile(
    r"\bst\.?\s+jude\s+championship\b|\bbmw\s+championship\b|"
    r"^\s*(?:the\s+)?tour\s+championship\b",
    re.IGNORECASE,
)


class GolfSource(FieldEventSource):
    """PGA Tour: ~45 tournaments per year. Four majors get the
    MAJOR tier, the three FedEx Cup playoff events the TOUR_PLAYOFF
    tier, everything else EVENT. The MAJOR_REGEX intentionally catches
    both "The Open" and "British Open" framings; ESPN has
    inconsistently used both."""

    ESPN_SLUG = "golf/pga"
    SPORT_PREFIX = "PGA"
    SPORT_LABEL = "PGA Tour"
    FD_COMPETITION_CODE = "PGA"
    MAJOR_REGEX = _GOLF_MAJOR_RE
    PLAYOFF_REGEX = _GOLF_PLAYOFF_RE


# UFC PPV detection. ESPN names PPV events "UFC <number>: <headline>"
# (e.g., "UFC 309: Jones vs. Miocic"). Fight Nights and ESPN-broadcast
# events use "UFC Fight Night: …" or "UFC on ESPN: …" without a
# number. The MAJOR tier is reserved for numbered PPVs because those
# are the marquee cards (typically a title fight as the main event).
_UFC_PPV_RE: Pattern[str] = re.compile(r"^\s*UFC\s+\d+\b", re.IGNORECASE)


class UfcSource(FieldEventSource):
    """UFC: one EPG entry per fight night card. The card itself is
    the broadcast unit; ESPN's `event.name` carries the headliner
    framing ("UFC 309: Jones vs. Miocic", "UFC Fight Night:
    Sandhagen vs. Figueiredo").

    Architecturally the same shape as racing / golf: emit one row
    per scheduled card with the card title as `home`. PPVs (numbered
    UFC events) get MAJOR tier; Fight Nights get EVENT tier.
    """

    ESPN_SLUG = "mma/ufc"
    SPORT_PREFIX = "UFC"
    SPORT_LABEL = "UFC"
    FD_COMPETITION_CODE = "UFC"
    MAJOR_REGEX = _UFC_PPV_RE


# Tennis Grand Slams + marquee year-end events. ESPN's tennis
# scoreboard returns whole tournaments (each entry spans 1-2 weeks),
# not individual matches: so tennis fits the FieldEventSource model
# the same way racing and golf do. The four Slams plus the year-end
# Finals get the MAJOR tier; regular tour events stay EVENT.
_TENNIS_MAJOR_RE: Pattern[str] = re.compile(
    r"\b(?:wimbledon|australian\s+open|french\s+open|roland\s+garros|"
    r"u\.?s\.?\s+open|atp\s+finals|wta\s+finals)\b",
    re.IGNORECASE,
)


class AtpSource(FieldEventSource):
    """ATP Tour: men's professional tennis. One entry per active
    tournament. Grand Slams (Wimbledon / Australian Open / French
    Open / US Open) and ATP Finals get MAJOR; regular tour stops
    stay EVENT."""

    ESPN_SLUG = "tennis/atp"
    SPORT_PREFIX = "ATP"
    SPORT_LABEL = "ATP Tour"
    FD_COMPETITION_CODE = "ATP"
    MAJOR_REGEX = _TENNIS_MAJOR_RE


class WtaSource(FieldEventSource):
    """WTA Tour: women's professional tennis. Same shape and same
    Grand Slam roster as ATP (the Slams are shared events). MAJOR
    detection is identical."""

    ESPN_SLUG = "tennis/wta"
    SPORT_PREFIX = "WTA"
    SPORT_LABEL = "WTA Tour"
    FD_COMPETITION_CODE = "WTA"
    MAJOR_REGEX = _TENNIS_MAJOR_RE
