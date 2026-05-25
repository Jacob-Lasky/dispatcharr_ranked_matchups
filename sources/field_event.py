"""Field-event source — racing and golf.

Racing and golf don't fit the team-based GameRow model because they're
field events: one race or tournament with 20+ competitors and a
finishing order, not a head-to-head outcome between two teams.

V1 design (Phase R)

  - Emit one GameRow per scheduled event, with `home = event_name`
    (e.g., "Heineken Chinese GP", "The Masters") and `away = "Field"`
    as a sentinel.
  - Tag with `tournament_stage = "EVENT"` (regular tour stop) or
    `"MAJOR"` (golf majors / future marquee racing events). The
    scoring layer's `tournament` signal handles the rest; both labels
    map to non-zero stage_score so field events always surface in the
    guide when toggled on.
  - No importance simulation, no closeness, no rank — the rarity of
    these events (~1/week) means just-show-it-up is the right product.
    The matcher fuzzy-matches the event name against EPG titles.

Subclasses parameterize the league via class attrs (ESPN_SLUG,
SPORT_PREFIX, SPORT_LABEL, MAJOR_REGEX). The base does all the work.

ESPN doesn't enforce a 25-event range cap on these endpoints because
volumes are low (F1: 24 GPs/year, NASCAR: 36 races/year, PGA: ~45
tour events/year). Range queries are safe.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Pattern

import requests

from .base import GameRow, SportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.field_event")


_FIELD_AWAY_SENTINEL = "Field"


class FieldEventSource(SportSource):
    """ABC for field-event leagues (racing, golf). Subclasses set
    five class attrs:

      - `ESPN_SLUG`: path under `/apis/site/v2/sports/` (e.g.,
        "racing/f1", "racing/nascar-premier", "golf/pga").
      - `SPORT_PREFIX`: short channel-name prefix.
      - `SPORT_LABEL`: human-readable label.
      - `FD_COMPETITION_CODE`: arbitrary string carried through extra
        for future routing.
      - `MAJOR_REGEX`: optional compiled regex; when an event name
        matches, the event is tagged tournament_stage="MAJOR"
        (otherwise "EVENT"). Default None means everything is "EVENT".
    """

    # supports_importance left at the SportSource default (False).
    # Field events surface via the tournament signal alone in V1.

    ESPN_SLUG: str = ""
    SPORT_PREFIX: str = ""
    SPORT_LABEL: str = ""
    FD_COMPETITION_CODE: str = ""
    MAJOR_REGEX: Optional[Pattern[str]] = None

    @property
    def sport_prefix(self) -> str:
        return self.SPORT_PREFIX

    @property
    def sport_label(self) -> str:
        return self.SPORT_LABEL

    def _espn_base(self) -> str:
        return f"https://site.api.espn.com/apis/site/v2/sports/{self.ESPN_SLUG}"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull events scheduled in the next N days via the ESPN
        scoreboard range endpoint. Field-event sports have low enough
        volume that range queries (which silently cap at 25 for team
        sports) work fine — ESPN returns the whole window unfiltered.
        """
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=days_ahead)
        url = (
            f"{self._espn_base()}/scoreboard"
            f"?dates={today.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
        )
        data = _http_get(url)
        if not isinstance(data, dict):
            return []
        out: List[GameRow] = []
        seen_ids: set = set()
        for event in data.get("events") or []:
            eid = event.get("id")
            if eid in seen_ids:
                continue
            name = (event.get("name") or "").strip()
            short_name = (event.get("shortName") or "").strip()
            display_name = name or short_name
            if not display_name:
                continue
            start = parse_iso_utc(event.get("date"))
            if start is None:
                continue
            seen_ids.add(eid)
            # Detect majors so the score gets bumped to the MAJOR tier
            # (e.g., golf's Masters / PGA / US Open / British Open).
            stage = "EVENT"
            if self.MAJOR_REGEX is not None and self.MAJOR_REGEX.search(display_name):
                stage = "MAJOR"
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=display_name,
                away=_FIELD_AWAY_SENTINEL,
                rank_home=None,
                rank_away=None,
                start_time=start,
                extra={
                    "espn_event_id": eid,
                    "fd_competition_code": self.FD_COMPETITION_CODE,
                    "is_field_event": True,
                    "stage": stage,
                    "short_name": short_name,
                },
            ))
        return out


def _http_get(url: str, timeout: float = 15.0) -> Optional[Any]:
    """ESPN unofficial API wrapper. Returns parsed JSON or None on
    any error. Shared across all field-event subclasses."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[field_event] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[field_event] %s failed: %s", url, exc)
        return None


# =====================================================================
# Concrete subclasses
# =====================================================================


class F1Source(FieldEventSource):
    """Formula 1 — ~24 Grands Prix per year. Each weekend covers
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
    """NASCAR Cup Series — ~36 races per year, including the Daytona
    500 (season opener) and the Coca-Cola 600. Both are 'crown jewels'
    but for V1 every race is EVENT-tier."""

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
# Championship", "PGA Championship").
_GOLF_MAJOR_RE: Pattern[str] = re.compile(
    r"\b(masters|pga\s+championship|u\.?s\.?\s+open|"
    r"the\s+open(?:\s+championship)?|british\s+open)\b",
    re.IGNORECASE,
)


class GolfSource(FieldEventSource):
    """PGA Tour — ~45 tournaments per year. Four majors get the
    MAJOR tier; everything else is EVENT-tier. The MAJOR_REGEX
    intentionally catches both "The Open" and "British Open"
    framings; ESPN has inconsistently used both."""

    ESPN_SLUG = "golf/pga"
    SPORT_PREFIX = "PGA"
    SPORT_LABEL = "PGA Tour"
    FD_COMPETITION_CODE = "PGA"
    MAJOR_REGEX = _GOLF_MAJOR_RE


# UFC PPV detection. ESPN names PPV events "UFC <number>: <headline>"
# (e.g., "UFC 309: Jones vs. Miocic"). Fight Nights and ESPN-broadcast
# events use "UFC Fight Night: …" or "UFC on ESPN: …" without a
# number. The MAJOR tier is reserved for numbered PPVs because those
# are the marquee cards (typically a title fight as the main event).
_UFC_PPV_RE: Pattern[str] = re.compile(r"^\s*UFC\s+\d+\b", re.IGNORECASE)


class UfcSource(FieldEventSource):
    """UFC — one EPG entry per fight night card. The card itself is
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
# not individual matches — so tennis fits the FieldEventSource model
# the same way racing and golf do. The four Slams plus the year-end
# Finals get the MAJOR tier; regular tour events stay EVENT.
_TENNIS_MAJOR_RE: Pattern[str] = re.compile(
    r"\b(?:wimbledon|australian\s+open|french\s+open|roland\s+garros|"
    r"u\.?s\.?\s+open|atp\s+finals|wta\s+finals)\b",
    re.IGNORECASE,
)


class AtpSource(FieldEventSource):
    """ATP Tour — men's professional tennis. One entry per active
    tournament. Grand Slams (Wimbledon / Australian Open / French
    Open / US Open) and ATP Finals get MAJOR; regular tour stops
    stay EVENT."""

    ESPN_SLUG = "tennis/atp"
    SPORT_PREFIX = "ATP"
    SPORT_LABEL = "ATP Tour"
    FD_COMPETITION_CODE = "ATP"
    MAJOR_REGEX = _TENNIS_MAJOR_RE


class WtaSource(FieldEventSource):
    """WTA Tour — women's professional tennis. Same shape and same
    Grand Slam roster as ATP (the Slams are shared events). MAJOR
    detection is identical."""

    ESPN_SLUG = "tennis/wta"
    SPORT_PREFIX = "WTA"
    SPORT_LABEL = "WTA Tour"
    FD_COMPETITION_CODE = "WTA"
    MAJOR_REGEX = _TENNIS_MAJOR_RE
