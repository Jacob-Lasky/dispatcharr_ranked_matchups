"""Soccer friendlies from ESPN's unofficial API: two flavours, one sweep.

An EXHIBITION is the shared idea here. Whether it's a national-team FIFA-window
warm-up or a club pre-season tour match, the game has no league table, no
standings, and no elimination stakes. Both sources therefore DELIBERATELY leave
`supports_importance` at the base-class default of False. A friendly surfaces in
Top Matchups purely on the favorite / rivalry / narrative signals in score_game,
which is honest: a USA warm-up is worth watching because it's USA, not because
it swings a standings position. DO NOT flip supports_importance on in either
subclass and fabricate a league context: there is no table to simulate, and
compute_match_importance would have nothing to threshold against.

Two concrete sources, differing ONLY in which ESPN competition they sweep:

  InternationalFriendliesSource  senior national teams, parametrized on gender
                                 men's   -> "fifa.friendly"
                                 women's -> "fifa.friendly.w"

  ClubFriendliesSource           club sides, pre-season tours and mid-season
                                 exhibitions -> "club.friendly"

Why each exists:

  International: the FIFA World Cup source (sources/soccer.py, config key
  "world_cup") reads ONLY tournament fixtures from Football-Data.org. It does
  not, and should not, contain warm-up friendlies. Before this source there was
  no path for a pre-tournament national-team friendly to appear in the guide at
  all. See the "USA vs Senegal didn't show up" investigation.

  Club: the same gap, club half, found the same way. A live Wrexham v Leeds
  United pre-season friendly never reached the guide despite Wrexham being the
  user's top favorite, because `fifa.friendly` carries national teams only and
  Football-Data.org's competition codes (PL, ELC, CL, ...) carry no friendlies
  of any kind. The blind spot spans the whole European pre-season window
  (roughly July to mid-August), which is precisely when every league source in
  this plugin returns zero games. See #153.

Favorites gate (favorites_only): because a friendly's ONLY claim to a guide slot
is the favorite signal, a source can be told to emit only games involving a
configured favorite. A FIFA window produces dozens of fixtures between teams a
given user doesn't follow (Kenya vs Lesotho, Cambodia vs Hong Kong); a pre-season
Saturday produces just as many between clubs they don't follow (Bromley vs
Crystal Palace, SV Wehen Wiesbaden vs Bayern). Surfacing all of them buries the
one game the user cares about. With the gate on, a friendly between two
non-favorite teams is dropped EVEN IF it would otherwise pick up a rivalry or
narrative signal: an exhibition between teams you don't follow isn't a Top
Matchup. The gate is opt-out (the plugin defaults it on) so users who genuinely
want every friendly can disable it.

Offseason (no friendlies scheduled in the lookahead window) returns []. No API
key required (ESPN's site API is free, same as the NFL/NHL/NCAA-soccer sources).
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, SportSource
from ._espn import extract_espn_scoreboard_event

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.friendlies")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# How many extra days BEHIND today-UTC the scoreboard sweep reaches. See the
# long comment in _EspnFriendliesBase.fetch_upcoming: ESPN buckets by US
# Eastern date, so yesterday's bucket still holds tonight's fixtures. Used to
# derive BOTH the sweep's start day and its length; DO NOT hardcode either
# independently or the window silently shifts when one is edited.
_LOOKBACK_DAYS = 1

# Upper bound on how long ago a kept fixture may have kicked off. The lookback
# above deliberately reaches into a past bucket to catch in-progress games, but
# ESPN sometimes leaves a finished match tagged SCHEDULED (its status lag is
# how the Wrexham v Leeds kickoff still read SCHEDULED two hours in). Without a
# floor those stale rows would sail past the FINISHED filter and land in the
# guide as "upcoming" games that ended hours ago, sorting to the top because
# channels are numbered by kickoff time. Six hours clears any football match
# (90 minutes plus halftime, stoppage, and extra time) with wide margin while
# still discarding yesterday's leftovers.
_MAX_AGE_AFTER_KICKOFF = timedelta(hours=6)


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[friendlies] %s → %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[friendlies] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Soccer namer, shared by BOTH friendlies sources.

    ESPN populates `location` with the full entity name on its soccer
    endpoints: the country for international fixtures ("United States",
    "Senegal") and the full club name for club fixtures ("Leeds United",
    "Tottenham Hotspur"), which is exactly the form the favorites list and EPG
    provider titles use. This was sampled across 45 distinct clubs on
    club.friendly, where location == name == displayName on every single one,
    which is why the club source does NOT need its own namer. Same
    location -> name -> abbreviation preference documented for soccer callers
    in _espn.py.

    DO NOT "improve" this to prefer shortDisplayName. ESPN's short forms are
    lossy in exactly the way that breaks matching: "Leeds United" becomes
    "Leeds", "Tottenham Hotspur" becomes "Spurs", "Internazionale" becomes
    "Inter Milan". Both the favorites gate and the EPG title match run against
    this string.
    """
    for key in ("location", "displayName", "name", "abbreviation"):
        val = (team_obj.get(key) or "").strip()
        if val:
            return val
    return ""


class _EspnFriendliesBase(SportSource):
    """Shared per-day ESPN scoreboard sweep for exhibition soccer.

    Subclasses supply `_espn_slug`, `sport_prefix`, and `sport_label`; every
    other behavior (the sweep, the FINISHED drop, the favorites gate, the
    GameRow shape) lives here so the two flavours cannot drift apart. Abstract
    on all three, so this class is not instantiable on its own.
    """

    def __init__(
        self,
        favorites: Optional[List[str]] = None,
        favorites_only: bool = False,
    ) -> None:
        # favorites_only gates fetch_upcoming to games involving a configured
        # favorite. Matching reuses scoring.match_favorites so the word-boundary
        # rules are IDENTICAL to how the favorite SCORING signal is computed;
        # DO NOT reimplement substring matching here and risk the gate and the
        # score disagreeing on what "involves a favorite" means. self.favorites
        # holds the user's own Favorites list, spelled as ESPN spells it
        # ("United States", not "USA"; "Leeds United", not "Leeds").
        self.favorites = list(favorites or [])
        self.favorites_only = bool(favorites_only)

    @property
    @abstractmethod
    def _espn_slug(self) -> str:
        """ESPN soccer competition slug to sweep."""

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep. ESPN's date-RANGE syntax silently caps at
        25 events, so we walk one day at a time (same trap documented in
        ncaa_baseball.py / ncaa_soccer.py).

        The window runs from `_LOOKBACK_DAYS` before today-UTC through
        `days_ahead` after it; see the comment on the loop for why it has to
        reach backwards at all.

        Three filters, in order:
          - FINISHED games are dropped: a friendly that already kicked off and
            ended is not an upcoming Top Matchup. A live (in-progress) game
            classifies as SCHEDULED in the shared parser and is kept, so a game
            "playing right now" still surfaces.
          - Games that kicked off more than `_MAX_AGE_AFTER_KICKOFF` ago are
            dropped even when still tagged SCHEDULED, which catches ESPN's
            status lag on yesterday's results.
          - When favorites_only is set, games not involving a favorite are
            dropped (see the module docstring on the favorites gate)."""
        # Lazy import to keep the source module's top-level import graph free of
        # scoring (matches the lazy-import idiom used across sources/*.py).
        from ..scoring import match_favorites

        if self.favorites_only and not self.favorites:
            # Gate is on but the user configured no favorites, so every game
            # would be dropped. Surface this once per fetch rather than fail
            # silently with an empty guide section.
            logger.warning(
                "[friendlies] favorites_only is on but no favorites are "
                "configured: all %s fixtures will be suppressed",
                self.sport_label,
            )

        # Start ONE day BEFORE today-UTC. ESPN buckets its scoreboard by US
        # EASTERN calendar date, not UTC, so the `dates=YYYYMMDD` bucket for
        # day D holds events with UTC timestamps running through D+1 04:59Z
        # (23:59 EST). Anchoring the sweep at today-UTC therefore goes blind
        # to every fixture in the 00:00Z-05:00Z window, which is prime time in
        # the Americas and the Pacific. Verified live 2026-07-26T01:25Z: the
        # `dates=20260725` bucket still held "Tottenham Hotspur at Auckland FC"
        # at 2026-07-26T03:00Z (a favorite, 95 minutes from kickoff) and
        # "Trinidad and Tobago at Louisville City FC" already in progress,
        # while the `dates=20260726` bucket held neither.
        #
        # DO NOT "optimize" this back to starting at today: the extra bucket is
        # one HTTP request, the FINISHED filter below drops anything that has
        # already ended, and `seen_ids` dedupes the overlap, so the day costs
        # nothing. One day back fully covers the skew (max US Eastern offset is
        # UTC-5). Reaching backwards does admit stale rows that ESPN has not
        # yet retagged FINISHED, which is what _MAX_AGE_AFTER_KICKOFF handles.
        now = datetime.now(timezone.utc)
        start_day = now.date() - timedelta(days=_LOOKBACK_DAYS)
        oldest_kickoff = now - _MAX_AGE_AFTER_KICKOFF
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + _LOOKBACK_DAYS + 1):
            day = start_day + timedelta(days=offset)
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard"
                f"?dates={day.strftime('%Y%m%d')}"
            )
            if not data:
                continue
            for event in data.get("events") or []:
                rec = extract_espn_scoreboard_event(
                    event, team_namer=_team_canonical_name,
                )
                if rec is None:
                    continue
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                if rec.get("status") == "FINISHED":
                    continue
                start = rec.get("start_time")
                if start is None:
                    continue
                if start < oldest_kickoff:
                    # Kicked off long enough ago that it cannot still be
                    # running: ESPN just hasn't retagged it FINISHED. Keeping
                    # it would put a finished match in the guide as an upcoming
                    # game. See _MAX_AGE_AFTER_KICKOFF.
                    continue
                home = rec["home"]
                away = rec["away"]
                if self.favorites_only and not match_favorites(
                    home, away, self.favorites
                ):
                    # Exhibition between teams the user doesn't follow: no
                    # standings/rank to earn a slot and no favorite to rescue
                    # it. Drop rather than let it pad the guide.
                    continue
                # No rank (friendlies have no poll), no spread, no closeness,
                # no fd_competition_code: the scoring loop sees no league
                # context and contributes zero importance, which is correct for
                # an exhibition. Favorite / rivalry / narrative carry the signal.
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "espn_league_slug": self._espn_slug,
                    },
                ))
        return out


class InternationalFriendliesSource(_EspnFriendliesBase):
    """Senior national-team friendlies (FIFA international windows and
    pre-tournament warm-ups, e.g. a USMNT vs Senegal tune-up the week before the
    World Cup). Parametrized on gender ("m"/"w"), mirroring NcaaSoccerSource."""

    def __init__(
        self,
        gender: str = "m",
        favorites: Optional[List[str]] = None,
        favorites_only: bool = False,
    ) -> None:
        g = (gender or "").lower().strip()
        if g not in ("m", "w"):
            raise ValueError(f"gender must be 'm' or 'w', got {gender!r}")
        self.gender = g
        super().__init__(favorites=favorites, favorites_only=favorites_only)

    @property
    def sport_prefix(self) -> str:
        return "FRIENDLY" if self.gender == "m" else "FRIENDLYW"

    @property
    def sport_label(self) -> str:
        return (
            "International Friendly" if self.gender == "m"
            else "Women's International Friendly"
        )

    @property
    def _espn_slug(self) -> str:
        return "fifa.friendly" if self.gender == "m" else "fifa.friendly.w"


class ClubFriendliesSource(_EspnFriendliesBase):
    """Club friendlies: pre-season tours and mid-season exhibitions.

    NOT parametrized on gender, unlike the international source: ESPN publishes
    no `club.friendly.w` competition, so a gender kwarg would be a knob that
    silently does nothing. Add one only if ESPN ships the endpoint.

    ESPN files the occasional touring national team under this competition too
    (e.g. Trinidad and Tobago at Louisville City FC). That needs no special
    handling: the row is still an exhibition scored on favorite / rivalry /
    narrative, which is exactly how the international source would treat it.
    """

    @property
    def sport_prefix(self) -> str:
        # Distinct from the international source's "FRIENDLY". rivalries.json
        # and logos.SPORTSDB_TOURNAMENT_LEAGUE_IDS are keyed on sport_prefix,
        # so reusing "FRIENDLY" would cross-wire both maps between national
        # teams and clubs. The absence of a "CLUBFRIENDLY" key in
        # rivalries.json is DELIBERATE and load-bearing: a pre-season kickabout
        # between two clubs is not a derby, so the rivalry bonus must not fire
        # on it even when the same two clubs are genuine league rivals.
        return "CLUBFRIENDLY"

    @property
    def sport_label(self) -> str:
        return "Club Friendly"

    @property
    def _espn_slug(self) -> str:
        return "club.friendly"
