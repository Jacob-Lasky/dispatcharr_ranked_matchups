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
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, SportSource
from ._espn import sweep_upcoming_scoreboard

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.friendlies")

# The per-day sweep, its US-Eastern-bucket lookback, and the stale-SCHEDULED
# age floor all live in `_espn.sweep_upcoming_scoreboard` now: `english_cup.py`
# needs the IDENTICAL window, and both date constants encode the reasoning for
# a live bug each, so duplicating them here is how that reasoning drifts. See
# #190. The favorites gate below stays local; it is the only part of this
# source's filtering that is not shared.


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[friendlies] %s -> %d", url, r.status_code)
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
        """Per-day ESPN sweep (shared with english_cup via
        `_espn.sweep_upcoming_scoreboard`, which owns the window, the FINISHED
        drop, the stale-SCHEDULED age floor, and the id dedupe), then the one
        filter unique to friendlies:

          - When favorites_only is set, games not involving a favorite are
            dropped. See the module docstring on the favorites gate."""
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

        out: List[GameRow] = []
        for rec in sweep_upcoming_scoreboard(
            slug=self._espn_slug,
            days_ahead=days_ahead,
            team_namer=_team_canonical_name,
            http_get=_http_get,
        ):
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
                start_time=rec["start_time"],
                extra={
                    "espn_event_id": rec.get("id"),
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
