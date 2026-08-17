"""TheSportsDB matchup-thumbnail resolver and local-cache writer.

TheSportsDB pre-renders 960x540 matchup graphics (both team crests, league
wordmark, country/region backdrop) for every event in its database. This
module looks up a curated game's matchup thumb, downloads it once, and
points a Dispatcharr Logo row at the local file. Used by plugin.py's
apply pipeline to give each virtual channel a per-game logo instead of
inheriting the source channel's logo.

DO NOT swap to a remote-URL-only path. Dispatcharr's logo cache reads
/data/<path> directly off disk when the Logo URL starts with /data, and
proxies remote URLs through an in-memory HTTP cache that doesn't survive
a container restart. Local storage is both faster and resilient to
SportsDB outages or CDN URL rotations.

API docs: https://www.thesportsdb.com/api.php
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from ._util import is_field_event, redact_secrets

logger = logging.getLogger(__name__)

# TheSportsDB carries the API key as a PATH SEGMENT (/api/v1/json/<KEY>/...),
# not a header or query param, so any log line containing a composed SportsDB
# URL contains the user's key. `sportsdb_api_key` is an input_type=password
# field: masking the form input does nothing for the logs, and Dispatcharr logs
# go to container stdout.
#
# DO NOT pass a composed SportsDB URL to the logger. Route it through
# _util.redact_secrets() instead, which is shared with the Odds-API query-param
# leak of the same class so there is ONE redactor to keep correct. See #156.
_SEARCH_URL = "https://www.thesportsdb.com/api/v1/json/{key}/searchevents.php?e={q}"

_HTTP_TIMEOUT_S = 10.0
_DOWNLOAD_TIMEOUT_S = 15.0
_USER_AGENT = "dispatcharr_ranked_matchups"

# SportsDB returns a date string; allow ±2 days to absorb timezone wobble between
# the kickoff UTC we have and the league-local date SportsDB indexes.
_DATE_TOLERANCE_DAYS = 2

# Re-probe positive cache entries every 14 days (SportsDB rarely changes thumb
# URLs but does occasionally rebuild). Re-probe negatives every 1 day because
# events get indexed late and a miss now can be a hit tomorrow.
_POSITIVE_TTL = timedelta(days=14)
_NEGATIVE_TTL = timedelta(days=1)

# Files we own under /data/logos/. The filename prefix is the cleanup signature.
LOGO_DIR = "/data/logos"
LOGO_FILENAME_PREFIX = "ranked_matchups_"

# Trailing club-qualifier tokens that SportsDB drops from its event names
# (Manchester City vs Aston Villa, not Manchester City FC vs Aston Villa FC).
# Matters for European soccer where Football-Data.org returns the long form.
_TRAILING_CLUB_QUALIFIERS = ("FC", "AFC", "CF", "SC", "FK", "AC")
_TRAILING_QUALIFIER_RE = re.compile(
    r"\s+(" + "|".join(_TRAILING_CLUB_QUALIFIERS) + r")$", re.IGNORECASE
)

# Plugin sport_prefix -> substring that must appear in SportsDB's strLeague or
# strSport for the result to be accepted. Disambiguates same-name cross-sport
# collisions (e.g. "Alabama vs Auburn" is both a football and basketball
# fixture). Missing entries mean no disambiguation: date filter alone.
_SPORT_HINT: dict[str, str] = {
    "CFB": "NCAA",
    "CBB": "NCAA",
    "NFL": "NFL",
    "NHL": "NHL",
    "MLB": "MLB",
    "NBA": "NBA",
    "WNBA": "WNBA",
    "MLS": "Major League Soccer",
    "NWSL": "NWSL",
    "LigaMX": "Mexican",
    "EPL": "English Premier League",
    "EFL": "English League Championship",
    "UCL": "Champions League",
    "BL1": "Bundesliga",
    "LaLiga": "Spanish La Liga",
    "SerieA": "Italian Serie A",
    "Ligue1": "French Ligue 1",
    "WC": "FIFA World Cup",
    "EURO": "UEFA European Championship",
    "Eredivisie": "Eredivisie",
    "PrimeiraLiga": "Portuguese",
    "BSA": "Brazilian",
    # Field events (#104). These never resolve a matchup thumb at all -- their
    # `away` is the "Field" sentinel, so the "<home> vs Field" search finds
    # nothing -- but a badge-mapped prefix must carry a hint (enforced by
    # tests/test_logos.py), and each string below is a verified substring of
    # the strLeague that the mapped id returns.
    "F1": "Formula 1",
    "NASCAR": "NASCAR",
    "PGA": "PGA",
    "UFC": "UFC",
    "ATP": "ATP",
    "WTA": "WTA",
    "BOX": "Boxing",
}

# Plugin sport_prefix -> TheSportsDB league id, used to fetch a league/sport
# BADGE as the per-game logo fallback when no team-vs-team matchup thumbnail
# exists (issue #102). A provider channel's own logo ("ESPN") is the least
# useful image for a curated matchup, so we prefer the league badge first.
#
# DO NOT add an id here that you have not personally resolved through
# lookupleague.php and seen return BOTH the expected strLeague AND a non-empty
# strBadge. An unverified id is worse than a missing one: a missing prefix
# falls through to the source-channel logo, while a wrong id silently paints a
# wrong-sport badge on every game of that sport. The free key ("3") is rate
# limited and answers a burst with HTTP 429 or an EMPTY body, both of which
# look exactly like "this id does not exist" -- pace lookups seconds apart and
# re-run a known-good control (4328 = English Premier League) in the same batch
# before believing any negative.
#
# DO NOT trust a bulk id sweep's ABSENCES, no matter how paced. One measured
# sweep of 4380-4900 reported only three Fighting leagues in that range; paced
# per-id lookups with a passing control found at least sixteen, including the
# 4445 mapped below. It missed roughly 80% and said nothing was wrong. To
# DISCOVER an id, do not scan: ids are not clustered by sport (4607, NCAA
# basketball, sits inside a block of unrelated fight promotions). Use
# `search_all_leagues.php?c=<country>&s=<sport>` -- it works on the free key,
# unlike `searchleagues.php`, which 404s on every query. Its one limit is a
# hard 5-row cap ordered alphabetically by league name, with `page` and
# `offset` silently ignored, so a league sorting past the fifth name is simply
# unreachable on this tier. That cap, not the rate limit, is why the NCAA
# sub-sports are still unmapped.
#
# Cup prefixes (UCL/WC/EURO) map to the competition itself, so their badge IS
# the tournament badge. Prefixes deliberately left unmapped (the NCAA
# sub-sports and the friendlies, whose SportsDB league could not be confirmed)
# fall through to the source-channel logo rather than risk a wrong-sport badge.
SPORTSDB_LEAGUE_IDS: dict[str, int] = {
    "CFB": 4479,            # NCAA Division 1 (American Football)
    "CBB": 4607,            # NCAA Division I Basketball Mens
    "NFL": 4391, "NHL": 4380, "MLB": 4424, "NBA": 4387, "WNBA": 4516,
    "MLS": 4346, "NWSL": 4521, "LigaMX": 4350,
    "EPL": 4328, "EFL": 4329, "UCL": 4480, "BL1": 4331,
    "LaLiga": 4335, "SerieA": 4332, "Ligue1": 4334,
    "WC": 4429, "EURO": 4502,
    "Eredivisie": 4337, "PrimeiraLiga": 4344, "BSA": 4351,
    # Field events (#104). These sports have no head-to-head opponent pair, so
    # they can NEVER earn a matchup composite -- the league badge is the best
    # logo they will ever get, which is why they were mapped first.
    "F1": 4370,             # Formula 1
    "NASCAR": 4393,         # NASCAR Cup Series
    "PGA": 4425,            # PGA Tour
    "UFC": 4443,            # UFC
    "ATP": 4464,            # ATP World Tour
    "WTA": 4517,            # WTA Tour
    # DO NOT swap this for a specific promotion (Top Rank, Matchroom, Bellator,
    # ...). 4445 is SportsDB's GENERIC, promoter-neutral "Boxing" league, and
    # sources/boxing.py is a cross-promoter feed: any single promotion's badge
    # would be wrong on every card the other promoters put on.
    "BOX": 4445,            # Boxing (generic)
}

# Optional tournament-specific override, consulted FIRST when a game carries a
# `tournament_stage`, so a league source's postseason can show the competition
# badge rather than the regular-season league badge (the "tournament -> sport
# -> channel" order chosen for #102). Empty by default: cup SOURCES already map
# their tournament badge directly via SPORTSDB_LEAGUE_IDS, so this only matters
# for a league source that also runs a distinct postseason with its own
# SportsDB league id. Add entries as those ids are confirmed.
SPORTSDB_TOURNAMENT_LEAGUE_IDS: dict[str, int] = {}

_LEAGUE_URL = "https://www.thesportsdb.com/api/v1/json/{key}/lookupleague.php?id={id}"

# League badges are shared across all of a league's games (a small, finite set),
# so they use a distinct filename prefix that the per-game marker sweep skips.
BADGE_FILENAME_PREFIX = LOGO_FILENAME_PREFIX + "badge_"


def _strip_trailing_qualifier(name: str) -> str:
    """Strip trailing FC/AFC/CF/SC/FK/AC qualifiers iteratively.

    "Manchester City FC" -> "Manchester City"
    "Manchester City"    -> "Manchester City"  (no-op)
    """
    n = (name or "").strip()
    while True:
        nxt = _TRAILING_QUALIFIER_RE.sub("", n).strip()
        if nxt == n:
            return n
        n = nxt


def _build_search_query(home: str, away: str) -> str:
    """SportsDB searchevents accepts 'Home vs Away' (case-insensitive)."""
    return f"{_strip_trailing_qualifier(home)} vs {_strip_trailing_qualifier(away)}"


def marker_to_filename(marker: str, ext: str = "jpg") -> str:
    """Map a game marker (e.g. 'ranked_matchups:EPL:fd_535345') to a stable,
    filesystem-safe filename inside LOGO_DIR.

    SHA1 keeps the path short and avoids worrying about marker characters that
    aren't filesystem-safe (the marker contains ':' which works on Linux but
    is hostile on Windows-mounted /data shares).

    `ext` exists because the two providers emit different formats: SportsDB
    thumbs are JPG, game-thumbs composites are PNG, and Dispatcharr serves these
    off disk so the extension has to match the real bytes. Any extension added
    here MUST also be added to _SWEEPABLE_EXTS below, or the sweep will treat a
    live game's file as stale and delete the logo out from under it on the very
    next apply.
    """
    digest = hashlib.sha1(marker.encode("utf-8")).hexdigest()[:16]
    return f"{LOGO_FILENAME_PREFIX}{digest}.{ext.lstrip('.')}"


# Every extension marker_to_filename can produce. The sweep builds its live-file
# set from the cross-product of live markers and these, so a marker that is live
# keeps its file whichever provider wrote it.
_SWEEPABLE_EXTS = ("jpg", "png")


def badge_filename(league_id: int) -> str:
    """Stable filename for a cached league badge. Uses BADGE_FILENAME_PREFIX so
    the per-game marker sweep leaves it alone (badges are shared, not per-game)."""
    return f"{BADGE_FILENAME_PREFIX}{int(league_id)}.png"


def league_id_for(
    sport_prefix: Optional[str], tournament_stage: Optional[str] = None,
) -> Optional[int]:
    """Pick the SportsDB league id for a game's fallback badge.

    Tournament override first (when the game is in a tournament AND a distinct
    competition id is mapped), then the sport's league id, else None (the caller
    falls back to the source-channel logo). Implements the #102 order
    tournament -> sport -> channel.
    """
    if not sport_prefix:
        return None
    if tournament_stage and sport_prefix in SPORTSDB_TOURNAMENT_LEAGUE_IDS:
        return SPORTSDB_TOURNAMENT_LEAGUE_IDS[sport_prefix]
    return SPORTSDB_LEAGUE_IDS.get(sport_prefix)


def resolve_league_badge_url(league_id: int, api_key: str = "3") -> Optional[str]:
    """Return a league's badge image URL via SportsDB lookupleague.php, or None.

    `api_key`: "3" is SportsDB's free test tier (same as resolve_thumb_url)."""
    url = _LEAGUE_URL.format(
        key=urllib.parse.quote(api_key or "3"), id=int(league_id),
    )
    payload = _http_get_json(url)
    if not payload:
        return None
    leagues = payload.get("leagues") or []
    if not leagues:
        return None
    return leagues[0].get("strBadge") or None


def _date_in_tolerance(event_date_str: str, expected_dt: datetime) -> bool:
    """SportsDB's dateEvent is YYYY-MM-DD in the league-local timezone.

    Expected_dt is plugin-supplied UTC. ±2 days catches Australian /
    European fixtures whose local date is a day off from UTC midnight.
    """
    try:
        ev_date = date.fromisoformat(event_date_str)
    except (TypeError, ValueError):
        return False
    return abs((ev_date - expected_dt.date()).days) <= _DATE_TOLERANCE_DAYS


def _hint_matches(event: dict, sport_prefix: Optional[str]) -> bool:
    """Verify the SportsDB event matches the expected sport, if we have a hint.

    Falls open (returns True) when the prefix isn't in the hint map: better
    to accept a slightly-wrong match than to miss every game from an unmapped
    sport.
    """
    if not sport_prefix:
        return True
    hint = _SPORT_HINT.get(sport_prefix)
    if not hint:
        return True
    haystack = (
        (event.get("strLeague") or "") + " " + (event.get("strSport") or "")
    ).lower()
    return hint.lower() in haystack


def _http_get_json(url: str) -> Optional[dict]:
    """Single-shot JSON GET. Returns None on any failure (network, parse, HTTP)."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_S) as resp:
            if resp.status != 200:
                return None
            return json.load(resp)
    except Exception as e:
        # _redact_key, NOT url: the key is a path segment. See #156.
        logger.debug("sportsdb GET %s failed: %s", redact_secrets(url), redact_secrets(e))
        return None


def resolve_thumb_url(
    home: str,
    away: str,
    expected_dt: datetime,
    sport_prefix: Optional[str],
    api_key: str = "3",
) -> Optional[str]:
    """Search SportsDB for a matchup event and return its strThumb URL.

    Returns None when:
      - the game is a field event (is_field_event: no h2h matchup)
      - the search returns no events
      - no event matches both the date tolerance AND the sport hint
      - the matched event has no strThumb

    `api_key`: "3" is SportsDB's free test tier. Patreon keys
    (https://www.thesportsdb.com/api.php) unlock higher rate limits.
    """
    if not home or not away:
        return None
    if is_field_event(away):
        return None

    q = _build_search_query(home, away)
    url = _SEARCH_URL.format(
        key=urllib.parse.quote(api_key or "3"),
        q=urllib.parse.quote(q),
    )
    payload = _http_get_json(url)
    if not payload:
        return None

    events = payload.get("event") or []
    for ev in events:
        if not _date_in_tolerance(ev.get("dateEvent", ""), expected_dt):
            continue
        if not _hint_matches(ev, sport_prefix):
            continue
        thumb = ev.get("strThumb")
        if thumb:
            return thumb
    return None


# Magic-byte prefixes for the formats these providers actually serve: SportsDB
# returns JPEG, game-thumbs PNG, and league badges are PNG. GIF and WebP are
# included because a badge URL occasionally redirects to one.
_IMAGE_MAGIC = (
    b"\xff\xd8\xff",      # JPEG
    b"\x89PNG\r\n\x1a\n",  # PNG
    b"GIF87a",
    b"GIF89a",
)


def _looks_like_image(head: bytes) -> bool:
    """Whether `head` (the first bytes of a response body) starts with a known
    image signature. WebP is RIFF....WEBP, so it needs the offset check."""
    if not head:
        return False
    if head.startswith(_IMAGE_MAGIC):
        return True
    return head[:4] == b"RIFF" and head[8:12] == b"WEBP"


def download_to_file(
    url: str,
    dest_path: str,
    timeout: float = _DOWNLOAD_TIMEOUT_S,
    user_agent: str = _USER_AGENT,
) -> tuple[Optional[int], Optional[BaseException]]:
    """Download `url` to dest_path atomically. Returns (status_code, error).

    status_code is the HTTP status when the server answered at all (including
    error codes), or None when the request never got that far (DNS, connect,
    timeout, or a failed write). The error object is returned alongside so a
    caller can read response headers off an HTTPError, which is how
    gamethumbs.py honours Retry-After on a 429.

    DO NOT collapse this back into a bool. gamethumbs.py has to tell a 400
    ("this team does not exist", cache the negative forever) apart from a 429
    ("try again in a minute", caching it would blank a whole slate of logos for
    a day). Both are falsey; only the status distinguishes them.

    Atomic via tmp-file + os.replace so a partial download never leaves a
    half-written image that Dispatcharr's logo cache would serve as broken.
    """
    if not url:
        return None, None
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    tmp_path = f"{dest_path}.tmp.{os.getpid()}"
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return resp.status, None
            first = b""
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    if not first:
                        first = chunk[:16]
                    f.write(chunk)
        if not _looks_like_image(first):
            # A 200 carrying non-image bytes (captive portal, proxy error page,
            # HTML 200 from a misconfigured reverse proxy) must NOT be written:
            # callers treat the file's existence as the cache, so a corrupt file
            # would be served as a broken logo forever and never re-fetched.
            # Reported as a transient failure, so the next apply retries.
            logger.warning(
                "download %s returned 200 but not an image (%r); discarding",
                redact_secrets(url), first[:8],
            )
            _unlink_quietly(tmp_path)
            return None, None
        os.replace(tmp_path, dest_path)
        return 200, None
    except urllib.error.HTTPError as e:
        _unlink_quietly(tmp_path)
        return e.code, e
    except Exception as e:
        logger.warning("download %s failed: %s", redact_secrets(url), redact_secrets(e))
        _unlink_quietly(tmp_path)
        return None, e


def _unlink_quietly(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def download_thumb(thumb_url: str, dest_path: str) -> bool:
    """Download a thumb URL to dest_path atomically. Returns True on success.

    Thin wrapper over download_to_file for the SportsDB callers, which only
    ever need "did it land".
    """
    status, _ = download_to_file(thumb_url, dest_path)
    return status == 200


class ThumbCache:
    """Persistent JSON cache of marker -> (timestamp_iso, thumb_url_or_null).

    Negative entries (URL is None) are cached too so we don't re-probe every
    apply, but with a shorter TTL than positives because SportsDB indexes
    fixtures gradually: a miss this morning can be a hit by evening.
    """

    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self._data: dict[str, list] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.cache_path):
            return
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                self._data = obj
        except Exception as e:
            logger.warning("sportsdb cache load failed (%s); starting fresh", e)
            self._data = {}

    def save(self) -> None:
        tmp = f"{self.cache_path}.tmp.{os.getpid()}"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f)
            os.replace(tmp, self.cache_path)
        except Exception as e:
            logger.warning("sportsdb cache save failed: %s", e)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def get(self, marker: str) -> tuple[bool, Optional[str]]:
        """Returns (fresh, url_or_None). fresh=False means caller should refetch."""
        entry = self._data.get(marker)
        if not entry or len(entry) < 2:
            return False, None
        try:
            ts = datetime.fromisoformat(entry[0])
        except (TypeError, ValueError):
            return False, None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        url = entry[1]
        age = datetime.now(timezone.utc) - ts
        ttl = _NEGATIVE_TTL if url is None else _POSITIVE_TTL
        if age > ttl:
            return False, url
        return True, url

    def put(self, marker: str, url: Optional[str]) -> None:
        self._data[marker] = [datetime.now(timezone.utc).isoformat(), url]

    def prune(self, live_markers: set[str]) -> int:
        """Drop entries whose marker isn't in live_markers. Returns count dropped."""
        stale = [k for k in self._data if k not in live_markers]
        for k in stale:
            del self._data[k]
        return len(stale)


def sweep_stale_logo_files(live_markers: set[str], logo_dir: str = LOGO_DIR) -> int:
    """Delete /data/logos/ranked_matchups_*.jpg files whose marker isn't live.

    Called at the end of each apply pass. Returns number of files removed.
    Stale-detection is by filename hash: a file is live iff its name equals
    marker_to_filename(m, ext) for some m in live_markers and some ext this
    module can write. Both extensions are checked because a live game's logo may
    have come from either provider, and checking only .jpg would sweep every
    game-thumbs .png on the first apply after it was written.
    """
    if not os.path.isdir(logo_dir):
        return 0
    live_filenames = {
        marker_to_filename(m, ext)
        for m in live_markers
        for ext in _SWEEPABLE_EXTS
    }
    removed = 0
    try:
        entries = os.listdir(logo_dir)
    except OSError:
        return 0
    for name in entries:
        if not name.startswith(LOGO_FILENAME_PREFIX):
            continue
        # League badges (BADGE_FILENAME_PREFIX) are shared across games, not
        # keyed by a live marker; never sweep them here.
        if name.startswith(BADGE_FILENAME_PREFIX):
            continue
        if name in live_filenames:
            continue
        path = os.path.join(logo_dir, name)
        try:
            os.unlink(path)
            removed += 1
        except OSError as e:
            logger.debug("sportsdb sweep: failed to unlink %s: %s", path, e)
    return removed
