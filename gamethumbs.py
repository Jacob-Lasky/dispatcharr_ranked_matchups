"""game-thumbs matchup-composite resolver.

Second tier of the per-game logo chain (issue #174), sitting between
TheSportsDB's pre-rendered event thumb (logos.py) and the league badge.

The two providers fail in different ways, which is the whole point of having
both. SportsDB only has a thumb if it pre-rendered one for that specific
INDEXED event, so its miss rate tracks how well SportsDB covers a league:
measured 50% on the live instance (9 of 18 fixtures), with every MLS, Brazilian
Serie A and NCAA soccer game missing. game-thumbs composites two ESPN crests on
request, so it hits for any league/team pair it knows whether or not the fixture
is indexed anywhere.

Resolution is deterministic: (league_slug, away, home) fully determines the URL,
so there is no search step and no event-matching heuristic to get wrong. A
composite is downloaded once to /data/logos/ and reused, exactly like a SportsDB
thumb, so the local file is the real cache and a game-thumbs outage cannot break
already-applied channels.

DO NOT point Channel.logo at a game-thumbs URL directly. The same constraint
that logos.py documents applies here: Dispatcharr serves /data paths off disk
and proxies remote URLs through an in-memory cache that dies with the container.

Upstream: https://github.com/sethwv/game-thumbs (MIT). Public instance
https://game-thumbs.tickarr.com; self-hostable as ghcr.io/sethwv/game-thumbs,
which is the way to escape the rate limit documented below.
"""
from __future__ import annotations

import logging
import time
import urllib.error
import urllib.parse
from typing import Optional, Tuple

from ._util import is_field_event
from .logos import download_to_file

logger = logging.getLogger(__name__)

_USER_AGENT = "dispatcharr_ranked_matchups"

DEFAULT_BASE_URL = "https://game-thumbs.tickarr.com"

# /logo?style=1 renders a colour-split banner carrying both crests and the
# league mark, at a FIXED 995x346 across every league. The fixed part is the
# reason for the choice: style=2 varies from 1.76:1 to 2.84:1 depending on the
# crest shapes, which would give the curated list a ragged mix of logo shapes.
# /thumb is fixed too but 4:3, which letterboxes badly beside the 16:9 SportsDB
# thumbs it shares a list with.
_ENDPOINT = "logo"
_STYLE = "1"

# game-thumbs renders PNG; SportsDB thumbs are JPG. The extension has to follow
# the real format because Dispatcharr serves these files off disk and the
# browser sniffs by path.
FILE_EXT = "png"

# The public instance rate-limits to 30 requests/minute (RateLimit-Limit: 30,
# RateLimit-Policy: 30;w=60, measured 2026-08-15) and a self-hosted one defaults
# to the same via RATE_LIMIT_PER_MINUTE. Pace under it rather than discovering
# the ceiling with a burst: an apply that resolves a fresh slate can otherwise
# 429 most of its games and drop them all to the league badge. Steady state is
# zero requests, because a downloaded composite is never re-fetched.
_MIN_REQUEST_INTERVAL_S = 60.0 / 30 + 0.1
_last_request_at: float = 0.0

# One retry, honouring Retry-After, for the case where something else already
# consumed the window. Bounded because the caller has a good fallback (the
# league badge) and apply latency matters more than squeezing out one logo.
_MAX_RETRY_AFTER_S = 10.0

_DOWNLOAD_TIMEOUT_S = 15.0

# Plugin sport_prefix -> game-thumbs league slug.
#
# EVERY entry below was confirmed live against a running game-thumbs instance by
# rendering a real fixture for that league and getting back a 200 image/png; the
# slugs are ESPN-style and are NOT guessable from the prefix (men's college
# basketball is "mens-college-basketball", not "ncaa/basketball/mens", which
# 404s). If you add a league, confirm it the same way instead of extrapolating.
#
# Field-event sports (F1, NASCAR, PGA, UFC, ATP, WTA, BOX) are deliberately
# absent: they carry away=FIELD_AWAY_SENTINEL, so there is no head-to-head pair
# to composite and is_field_event() short-circuits before any lookup.
GAMETHUMBS_LEAGUE_SLUGS: dict[str, str] = {
    # US majors
    "NFL": "nfl",
    "NBA": "nba",
    "WNBA": "wnba",
    "MLB": "mlb",
    "NHL": "nhl",
    "MLS": "mls",
    "NWSL": "usa.nwsl",
    "LigaMX": "mex.1",
    # NCAA
    "CFB": "ncaa/football",
    "CBB": "mens-college-basketball",
    "NCAAW": "womens-college-basketball",
    "NCAAMSOC": "ncaa/soccer",
    "NCAAWSOC": "ncaa/womens-soccer",
    "NCAABSB": "ncaa/baseball",
    "NCAASBL": "ncaa/softball",
    # European club soccer
    "EPL": "eng.1",
    "EFL": "eng.2",
    "UCL": "uefa.champions",
    "BL1": "ger.1",
    "LaLiga": "esp.1",
    "SerieA": "ita.1",
    "Ligue1": "fra.1",
    "Eredivisie": "ned.1",
    "PrimeiraLiga": "por.1",
    "BSA": "bra.1",
    # International
    "WC": "fifa.world",
    "EURO": "uefa.euro",
    "FRIENDLY": "fifa.friendly",
    "FRIENDLYW": "fifa.friendly.w",
    "CLUBFRIENDLY": "club.friendly",
}

# HTTP codes that mean "this league/team pair does not exist here". game-thumbs
# answers an unknown team with 400 and a body enumerating the league's real
# teams, an unknown league with 400, and an unknown route shape with 444. All
# three are stable facts about the request, so they are safe to negative-cache.
_DEFINITIVE_MISS_CODES = frozenset({400, 404, 444})


def league_slug_for(sport_prefix: Optional[str]) -> Optional[str]:
    """Return the game-thumbs league slug for a sport prefix, or None.

    None means "no lookup possible", and the caller falls through to the league
    badge. An unmapped prefix costs no HTTP at all.
    """
    if not sport_prefix:
        return None
    return GAMETHUMBS_LEAGUE_SLUGS.get(sport_prefix)


def build_url(base_url: str, league_slug: str, away: str, home: str) -> str:
    """Compose the matchup URL. Away first, matching the /:league/:t1/:t2 route.

    Team names are passed through verbatim (quoted): game-thumbs resolves full
    names, cities, abbreviations and partial matches server-side, so
    "Inter Miami CF" and "SE Palmeiras" both resolve without pre-slugging.
    The league slug is NOT quoted: several are multi-segment paths
    ("ncaa/football") whose slashes are real path separators.
    """
    return "{base}/{slug}/{away}/{home}/{endpoint}?style={style}".format(
        base=base_url.rstrip("/"),
        slug=league_slug,
        away=urllib.parse.quote(away.strip(), safe=""),
        home=urllib.parse.quote(home.strip(), safe=""),
        endpoint=_ENDPOINT,
        style=_STYLE,
    )


def _pace() -> None:
    """Block until at least _MIN_REQUEST_INTERVAL_S has passed since the last
    request. Cooperative under gevent (time.sleep is monkey-patched), so this
    yields the hub rather than freezing the worker."""
    global _last_request_at
    wait = _MIN_REQUEST_INTERVAL_S - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def fetch_matchup_composite(
    base_url: str,
    sport_prefix: Optional[str],
    away: str,
    home: str,
    dest_path: str,
) -> Tuple[Optional[str], bool]:
    """Download a game's matchup composite to dest_path.

    Returns (url_or_None, is_definitive_miss). The second element is what the
    caller MUST branch on before writing a cache entry:

      (url,  False) -> downloaded to dest_path. Cache the URL.
      (None, True)  -> definitive miss (field event, unmapped league, blank base
                       URL, or the server said this league/team does not exist).
                       Safe to negative-cache: re-probing cannot change it.
      (None, False) -> TRANSIENT failure (429 after one retry, timeout,
                       connection error, 5xx, or a write that failed).
                       DO NOT cache this. A rate-limited apply that negative-
                       cached its 429s would blank every one of those games'
                       logos for the whole negative-cache TTL, turning a
                       one-minute throttle into a day of league badges.

    No HTTP is issued for a field event, an unmapped league, or a blank base URL.
    """
    if is_field_event(away) or not away or not home:
        return None, True
    if not (base_url or "").strip():
        return None, True
    league_slug = league_slug_for(sport_prefix)
    if not league_slug:
        return None, True

    url = build_url(base_url, league_slug, away, home)
    for attempt in (1, 2):
        _pace()
        status, err = download_to_file(
            url, dest_path, timeout=_DOWNLOAD_TIMEOUT_S, user_agent=_USER_AGENT,
        )
        if status == 200:
            return url, False
        if status == 429 and attempt == 1:
            retry_after = _parse_retry_after(err)
            if retry_after is not None:
                logger.debug("game-thumbs rate limited; retrying in %.1fs", retry_after)
                time.sleep(retry_after)
                continue
            return None, False
        if status in _DEFINITIVE_MISS_CODES:
            logger.debug(
                "game-thumbs has no composite for %s %s @ %s (HTTP %s)",
                sport_prefix, away, home, status,
            )
            return None, True
        return None, False
    return None, False


def _parse_retry_after(err: Optional[BaseException]) -> Optional[float]:
    """Seconds to wait per the Retry-After header, or None when it is absent,
    unparseable, or longer than we are willing to hold up an apply for."""
    headers = getattr(err, "headers", None)
    if headers is None:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        secs = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if secs < 0 or secs > _MAX_RETRY_AFTER_S:
        return None
    return secs
