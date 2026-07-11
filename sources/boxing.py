"""Boxing source: professional boxing fight cards via the Boxing Data API.

Boxing is the same PRODUCT shape as UFC (see field_event.py's UfcSource):
one broadcast unit per fight card, no two-team head-to-head, surface-if-
toggled with no per-bout importance ranking. So it emits the SAME GameRow
contract every field event uses:

  - `home` = the card title ("Gassiev vs. Kadiru: IBA Pro 19")
  - `away` = FIELD_AWAY_SENTINEL ("Field"), which flips is_field_event and
    routes matching to the name-only, single-sided path (#127).
  - `extra["is_field_event"] = True`, `extra["stage"]` in {"MAJOR","EVENT"}
    (consumed by scoring's tournament_stage band exactly like golf/UFC),
    `extra["fd_competition_code"] = "BOXING"`.

DO NOT subclass FieldEventSource. That base hardcodes the ESPN site API
(ESPN_SLUG + scoreboard range query), and ESPN exposes NO boxing feed
(verified 2026-07-10: site + core APIs both 404 for boxing, while mma/ufc
works). Boxing's upstream is a DIFFERENT API (RapidAPI Boxing Data API),
so this is a standalone SportSource that reuses the field-event OUTPUT
contract, not the ESPN FETCH plumbing. The output contract itself is
single-sourced in _util.field_event_extra (shared with field_event.py), so
downstream scoring/matching treats every field event identically.

Free-tier constraints (verified live 2026-07-10 against the Boxing Data API):
  - The subscription caps the queryable date range: `days=7` returns data,
    `days>=14` returns {"error": {"code": "DateOutOfRange"}}. So we clamp
    the lookahead to _FREE_TIER_MAX_DAYS. The plugin's lookahead_days
    defaults to 7 anyway; a user who raises it just gets boxing capped.
  - Some events carry a real time; others are date-only and come back as
    T00:00:00 (no time). Combined with the feed's naive (no-offset) datetimes,
    the effective start_time can sit up to a day off the actual broadcast, so
    boxing gets a WIDENED EPG match window in plugin.py (name specificity,
    not the clock, is the real discriminator for a rare, name-unique event).
  - Cancelled cards keep "(Cancelled)" in the title; we drop them.

The feed has no title-fight / championship flag, so like F1/NASCAR every
card is EVENT tier by default; a conservative regex promotes the obvious
championship cards to MAJOR.
"""

from __future__ import annotations

import logging
import re
from typing import Any, List, Optional, Pattern

import requests

from .base import GameRow, SportSource
from .._util import FIELD_AWAY_SENTINEL, field_event_extra, parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.boxing")

# RapidAPI gateway for the Boxing Data API (boxing-data.com). The key is a
# RapidAPI key; the host header selects this API on the shared gateway.
_RAPIDAPI_HOST = "boxing-data-api.p.rapidapi.com"
_SCHEDULE_URL = f"https://{_RAPIDAPI_HOST}/v2/events/schedule"

# Free-tier date-range cap. days>=14 returns DateOutOfRange (verified live
# 2026-07-10). Raise this only after confirming a paid tier lifts the cap.
_FREE_TIER_MAX_DAYS = 7

# Conservative MAJOR detection. The feed exposes no championship flag, so we
# read the card title: an explicit belt (WBC/WBA/IBF/WBO/IBO/The Ring) or an
# "undisputed / world title / championship / vacant title" framing marks a
# marquee card. Everything else stays EVENT, mirroring F1/NASCAR where every
# entry is EVENT-tier. Kept deliberately tight to avoid false MAJORs: most
# card titles are just "A vs. B: Promo Name" and correctly stay EVENT.
_BOXING_MAJOR_RE: Pattern[str] = re.compile(
    r"\b(?:undisputed|world\s+title|world\s+championship|"
    r"unified|vacant\b.*\btitle|w\.?b\.?c\.?|w\.?b\.?a\.?|"
    r"i\.?b\.?f\.?|w\.?b\.?o\.?|i\.?b\.?o\.?|the\s+ring)\b",
    re.IGNORECASE,
)

# Cancelled cards carry this marker in the title (verified live 2026-07-10:
# "Olascuaga vs. Dominguez (Cancelled): History in the Making").
_CANCELLED_RE: Pattern[str] = re.compile(r"\(\s*cancell?ed\s*\)", re.IGNORECASE)

# "A vs. B" head detector. The feed's titles are "Fighter vs. Fighter: Card
# Name"; the fighters are the headliner.
_VS_RE: Pattern[str] = re.compile(r"\bvs?\.?\b", re.IGNORECASE)


def _match_name(title: str) -> str:
    """The name the EPG matcher keys off. DO NOT return the full card title:
    the matcher derives keyword variants (including a bare last-word fallback)
    from this string, and a card title's promo suffix is generic. "... IBA
    Pro 19" reduces to the keyword "19", "... Boxing 26" to "26", which
    substring-match channel names and numbers wholesale (offline replay:
    one such card matched 4388 channels, 2026-07-10). The Boxing Data API
    formats every card as "Fighter vs. Fighter: Card Name", so when the pre-
    colon head names the fighters we match on THAT alone (distinctive
    surnames); only a title with no "vs." head falls back to the full string.
    The full title is still used for MAJOR-tier detection and kept in extra.

    Layered with, NOT replaced by, matcher._is_weak_last_word: that shared
    backstop drops a bare-number/1-2-char last word so ANY field event (UFC
    included) is protected even without source-side cleaning. Keep BOTH: this
    also yields a cleaner display/home and a fighter-surname keyword the
    backstop alone cannot recover from a generic promo suffix. DO NOT delete
    one as redundant."""
    head = title.split(":", 1)[0].strip()
    # Strip a trailing "(Cancelled)"-style parenthetical from the head so a
    # non-cancelled parenthetical (e.g. "(II)") doesn't leak into keywords.
    head = re.sub(r"\s*\([^)]*\)\s*$", "", head).strip()
    if head and _VS_RE.search(head):
        return head
    return title


class BoxingSource(SportSource):
    """Professional boxing cards from the Boxing Data API (RapidAPI).

    Requires an API key (RapidAPI key). Emits one GameRow per scheduled,
    non-cancelled card using the shared field-event contract.
    """

    # supports_importance stays False (SportSource default): a boxing card is
    # a field event, surfaced by the tournament_stage band alone, exactly like
    # golf / UFC / racing. No Monte Carlo importance.

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @property
    def sport_prefix(self) -> str:
        return "BOX"

    @property
    def sport_label(self) -> str:
        return "Boxing"

    def _headers(self) -> dict:
        return {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": _RAPIDAPI_HOST,
        }

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Pull upcoming boxing cards in the next `days_ahead` days (clamped to
        the free-tier cap). Empty list on any error or when nothing is
        scheduled; the caller does not filter by date."""
        if not self.api_key:
            logger.warning("[boxing] no Boxing Data API key; skipping")
            return []
        days = max(1, min(days_ahead, _FREE_TIER_MAX_DAYS))
        params = {"days": days, "date_sort": "ASC", "page_size": 25}
        data = self._http_get(_SCHEDULE_URL, params=params)
        if not isinstance(data, dict):
            return []
        err = data.get("error")
        if err:
            # DateOutOfRange (or any subscription error) comes back in-band with
            # HTTP 200 and an empty data list. Log and yield nothing rather than
            # raising: a source that can't fetch should be a no-op, not a crash.
            logger.warning("[boxing] API error: %s", err)
            return []
        events = data.get("data") or []
        out: List[GameRow] = []
        for ev in events:
            row = self._to_row(ev)
            if row is not None:
                out.append(row)
        logger.info("[boxing] %d card(s) in next %dd", len(out), days)
        return out

    def _to_row(self, ev: Any) -> Optional[GameRow]:
        if not isinstance(ev, dict):
            return None
        title = (ev.get("title") or "").strip()
        if not title:
            return None
        # Drop cancelled cards: surfacing a cancelled fight as a live channel
        # is worse than omitting it.
        if _CANCELLED_RE.search(title):
            return None
        start = parse_iso_utc(ev.get("date"))
        if start is None:
            return None
        # MAJOR detection reads the FULL title: belt / "undisputed" framing
        # usually lives in the promo suffix ("... Undisputed Super Middleweight").
        stage = "MAJOR" if _BOXING_MAJOR_RE.search(title) else "EVENT"
        # Matching keys off the fighters, not the promo suffix (see _match_name).
        home = _match_name(title)
        # US broadcasters, when present, are a useful debugging hint for which
        # channel should carry the card; matching itself is name-driven.
        broadcasters: List[str] = []
        for b in ev.get("broadcast") or []:
            if isinstance(b, dict) and (b.get("country") or "").upper() in ("US", "USA"):
                broadcasters = [str(x) for x in (b.get("broadcasters") or [])]
                break
        return GameRow(
            sport_prefix=self.sport_prefix,
            sport_label=self.sport_label,
            home=home,
            away=FIELD_AWAY_SENTINEL,
            rank_home=None,
            rank_away=None,
            start_time=start,
            # Shared field-event contract via field_event_extra (single source
            # of truth in _util); boxing-specific metadata rides in **extra and
            # is informational only (not used for matching).
            extra=field_event_extra(
                "BOXING",
                stage,
                title,
                full_title=title,
                boxing_event_id=ev.get("id"),
                venue=ev.get("venue"),
                location=ev.get("location"),
                us_broadcasters=broadcasters,
                poster_image_url=ev.get("poster_image_url"),
            ),
        )

    def _http_get(self, url: str, params: dict, timeout: float = 15.0) -> Optional[Any]:
        try:
            r = requests.get(url, params=params, headers=self._headers(), timeout=timeout)
            if r.status_code >= 400:
                logger.warning("[boxing] %s -> %d", url, r.status_code)
                return None
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("[boxing] %s failed: %s", url, exc)
            return None
