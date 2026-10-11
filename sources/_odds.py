"""The Odds API: the one place this plugin calls it (#200).

The free plan is 500 credits a month, and a request costs one credit per
region per market. Before this module, three sources (soccer.py, mls.py,
mls_standings.py) each called it on every refresh with three regions, so one
refresh cost about 30 credits and the month's allowance was gone in days.
After that every call answered 401 with `OUT_OF_USAGE_CREDITS`, which the
sources logged as "401 Unauthorized", and two issues (#200, #211) went
looking for a bad key that was never bad.

So, per (sport key, region):

- ONE region per request (1 credit). Closeness reads a single bookmaker's
  h2h line, and the first bookmaker with a usable line wins, so more regions
  only bought more bookmakers nobody reads.
- Cached on disk for ODDS_CACHE_TTL. A pre-match moneyline barely moves in a
  day, and refreshes (five scheduled, plus every M3U rematch) would otherwise
  each spend a credit per league. About a dozen leagues once a day fits the
  plan with room to spare.
- When the credits run out, stop asking until EXHAUSTED_BACKOFF has passed,
  log that plainly once, and serve the last cached line where there is one.

DO NOT add a region or a market to a call without redoing the budget above:
each one multiplies the cost of every request.

The parsers (`_h2h_to_closeness`) stay with their sources on purpose: soccer.py
devigs over all three outcomes, mls.py over home and away only, and unifying
them would move every score on one side. This module returns raw events.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .._util import redact_secrets

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
ODDS_CACHE_TTL = timedelta(hours=24)
# The plan's reset date is not in any response, so a used-up month is retried
# daily rather than guessed at.
EXHAUSTED_BACKOFF = timedelta(hours=24)
# Beside cache.json, in the plugin folder. Sources run in the pipeline
# subprocess, so the file is shared across refreshes and workers.
CACHE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "odds_cache.json",
)

_OUT_OF_CREDITS = "OUT_OF_USAGE_CREDITS"
# Events that kicked off longer ago than this are dropped from what is
# returned, so a stale fallback cannot lend a past fixture's line to the next
# meeting of the same two teams (callers match on the team pair alone).
_MAX_AGE_AFTER_KICKOFF = timedelta(hours=6)


def _key_id(api_key: str) -> str:
    """A non-reversible tag for the key, so the backoff belongs to the key that
    ran out: a user who pastes a new key is not kept waiting a day. Never the
    key itself: this file is plain JSON in the plugin folder."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]


def _current(events: Any, now: datetime) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(events, list):
        return None
    oldest = now - _MAX_AGE_AFTER_KICKOFF
    out = []
    for ev in events:
        raw = str(ev.get("commence_time") or "") if isinstance(ev, dict) else ""
        start = _parse_time(raw.replace("Z", "+00:00")) if raw else None
        # Keep an event with no parseable time: dropping it would turn a
        # format change upstream into a silent loss of every line.
        if start is None or start >= oldest:
            out.append(ev)
    return out


def _load() -> Dict[str, Any]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save(data: Dict[str, Any]) -> None:
    # Per-writer temp name, then an atomic replace: same contract as the
    # plugin's _write_cache.
    tmp = f"{CACHE_PATH}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, CACHE_PATH)
    except OSError as e:
        logger.warning("[odds] could not write %s: %s", CACHE_PATH, e)
        try:
            os.remove(tmp)
        except OSError:
            pass


def _parse_time(raw: Any) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fetch_odds_events(
    sport_key: str, api_key: str, region: str, *, log_tag: str,
    now: Optional[datetime] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Upcoming events with h2h odds for one sport, or None when there is no
    usable data (no key, never fetched and the fetch failed).

    Fresh cache entries are returned without a request. A failed or refused
    request returns the last cached events for this sport, however old: a
    stale pre-match line is a better closeness estimate than none."""
    if not api_key or not sport_key:
        return None
    now = now or datetime.now(timezone.utc)
    entry_key = f"{sport_key}|{region}"
    data = _load()
    if not isinstance(data.get("entries"), dict):
        # A hand-edited or foreign file: start the entries over rather than
        # failing every league's fetch on it.
        data["entries"] = {}
    entries = data["entries"]
    entry = entries.get(entry_key) if isinstance(entries.get(entry_key), dict) else None
    cached = _current(entry.get("events"), now) if entry else None

    fetched_at = _parse_time(entry.get("fetched_at")) if entry else None
    if fetched_at is not None and now - fetched_at < ODDS_CACHE_TTL and isinstance(cached, list):
        return cached

    exhausted_until = _parse_time(data.get("exhausted_until"))
    # Bounded by EXHAUSTED_BACKOFF so a corrupt far-future value (or a clock
    # that went backwards) cannot suppress requests indefinitely.
    if (exhausted_until is not None and now < exhausted_until <= now + EXHAUSTED_BACKOFF
            and data.get("exhausted_key") == _key_id(api_key)):
        return cached

    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{sport_key}/odds/",
            params={"regions": region, "markets": "h2h", "oddsFormat": "decimal",
                    "apiKey": api_key},
            timeout=15,
        )
    except requests.RequestException as e:
        # redact_secrets: requests renders the URL, query (apiKey) included,
        # into its exception text. See #156.
        logger.warning("[%s] odds fetch failed: %s", log_tag, redact_secrets(e))
        return cached

    remaining = r.headers.get("x-requests-remaining")
    if r.status_code == 401:
        try:
            code = (r.json() or {}).get("error_code")
        except ValueError:
            code = None
        if code == _OUT_OF_CREDITS:
            data["exhausted_until"] = (now + EXHAUSTED_BACKOFF).isoformat()
            data["exhausted_key"] = _key_id(api_key)
            _save(data)
            logger.warning(
                "[odds] The Odds API monthly credits are used up; no odds requests "
                "until %s. Closeness uses the last cached lines meanwhile. The key "
                "itself is fine.", data["exhausted_until"],
            )
        else:
            # redact_secrets: the body is the API's text, and nothing stops it
            # echoing the request back.
            logger.warning("[%s] The Odds API rejected the key (401, %s)", log_tag,
                           redact_secrets(code))
        return cached
    if r.status_code >= 400:
        logger.warning("[%s] odds fetch failed: HTTP %d", log_tag, r.status_code)
        return cached
    try:
        events = r.json()
    except ValueError:
        logger.warning("[%s] odds response was not JSON", log_tag)
        return cached
    if not isinstance(events, list):
        return cached

    entries[entry_key] = {"fetched_at": now.isoformat(), "events": events}
    data.pop("exhausted_until", None)
    data.pop("exhausted_key", None)
    if remaining is not None:
        data["credits_remaining"] = remaining
    _save(data)
    # "with bookmakers" is the coverage check: one region per league means a
    # region that does not price a competition yields events with no lines,
    # and this count is where that would show.
    priced = sum(1 for ev in events if isinstance(ev, dict) and ev.get("bookmakers"))
    logger.info("[%s] odds fetched (%d events, %d with bookmakers; %s credits left "
                "this month)", log_tag, len(events), priced,
                remaining if remaining is not None else "?")
    return _current(events, now)
