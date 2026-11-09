"""CollegeFootballData and CollegeBasketballData: the one place this plugin calls them (#337).

The free tier is 1,000 calls a MONTH, in ONE pool both APIs draw from through the one key.
CFBD's /info said so on 2026-11-09: `"tierName":"Free","monthlyLimit":1000,"sharedPool":true,
"products":["cfb","cbb"],"resetAt":"2026-12-01T00:00:00.000Z"`. The sources used to say
"1k req/day". A refresh asks about ten questions (NCAAF: /rankings, the season's /games, /lines
per week in the window; NCAAM: /rankings, the window's /games and /lines, three season /games
chunks), and refreshes run 9 to 12 times a day (four scheduled plus M3U rematches; 28 a day in
late October). So the month's pool was gone within days and every /rankings call answered 429
`Monthly call quota exceeded.` until the reset: on 2026-11-09 the November pool had been empty
since November 1.

So, per question (API, path and parameters):

- The answer is kept on disk for FRESH_FOR and served from there without a request. At most
  two requests a day per question is about 20 a day, ~620 in a 31-day month, under the pool
  with room for the questions that change daily (the NCAAM window's dates).
- On the exact `Monthly call quota exceeded.` 429, no request is made with that key until the
  pool resets (the 1st of the next month, 00:00 UTC), logged plainly once. A new key is not
  held by the old one's wait.
- A failed or refused request raises `CollegeDataUnavailable`, and the caller's existing failure
  path runs unchanged: the season store stands in for a season read (`SeasonUnavailable`), a lost
  window is held, a missing line drops a spread. Only /rankings, which has no fallback of its own,
  is served from a kept answer meanwhile (`stale_for`): last week's poll ranks new games, where
  failing would hold the label with no new game added (#283, #240). DO NOT give a question a
  `stale_for` where the caller already falls back: two stand-ins for one read is two answers to
  "how old may this be", and the season store's is measured.
- Only an answer the caller's `valid` check accepts is kept, so an error object served with a
  200 is never replayed for twelve hours.

DO NOT shorten FRESH_FOR, add a question per refresh, or drop the quota wait without redoing
the budget above: each one multiplies the month's calls.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

import requests

from .._http import get_within_deadline
from .._util import key_id, parse_iso_utc, write_json_atomic

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.college_data")

# Beside cache.json, in the plugin folder (as `_season_store.STORE_DIR`). Sources run in the
# pipeline subprocess, one refresh at a time (the run lock), so there is one writer.
CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "college_cache",
)

FRESH_FOR = timedelta(hours=12)
# How old a kept poll may be and still stand in for a failed /rankings. A poll is published
# weekly, so last week's is the current one until the next arrives: a little over a week.
POLL_STANDS_IN = timedelta(days=8)
# Files older than the longest stand-in can never be served: pruned.
MAX_STALE = POLL_STANDS_IN
QUOTA_MESSAGE = "Monthly call quota exceeded."
_VERSION = 1


class CollegeDataUnavailable(RuntimeError):
    """No answer: the request failed or was refused, or the month's pool is used up, and no
    kept answer is recent enough to stand in. The message never carries the key."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _question(base: str, path: str, params: Dict[str, Any]) -> str:
    return json.dumps([base, path, sorted((str(k), str(v)) for k, v in params.items())])


def _path(question: str) -> str:
    return os.path.join(CACHE_DIR, hashlib.sha256(question.encode("utf-8")).hexdigest()[:24] + ".json.gz")


def _quota_path() -> str:
    return os.path.join(CACHE_DIR, "quota.json")


def _read(path: str, compressed: bool) -> Optional[Dict[str, Any]]:
    try:
        with (gzip.open(path, "rt", encoding="utf-8") if compressed else open(path, encoding="utf-8")) as f:
            data = json.load(f)
    except (OSError, ValueError, EOFError):
        return None
    return data if isinstance(data, dict) else None


def _write(path: str, data: Dict[str, Any], compressed: bool) -> None:
    """Never raises: an answer that cannot be kept is logged and the refresh goes on."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        write_json_atomic(path, data, compress=compressed, separators=(",", ":"))
    except (OSError, TypeError, ValueError) as e:
        logger.warning("[college_data] could not write %s: %s", path, e)


def _kept(question: str, now: datetime) -> Optional[Dict[str, Any]]:
    """The kept answer for `question` with its age, or None. Not from the future either: a
    clock that went backwards must not make an old answer look fresh."""
    data = _read(_path(question), compressed=True)
    if not (data and data.get("v") == _VERSION and data.get("question") == question and "body" in data):
        return None
    at = parse_iso_utc(data.get("fetched_at")) if isinstance(data.get("fetched_at"), str) else None
    if at is None or at > now + timedelta(minutes=5):
        return None
    return {"age": now - at, "body": data["body"]}


def _next_reset(now: datetime) -> datetime:
    """The 1st of the next month, 00:00 UTC: where CFBD's /info put `resetAt`."""
    return (now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)


def _waiting_until(api_key: str, now: datetime) -> Optional[datetime]:
    """When this key's used-up month resets, while that is still ahead. Bounded by one month
    so a corrupt far-future value (or a clock that went backwards) cannot stop requests
    for longer than a real wait."""
    data = _read(_quota_path(), compressed=False) or {}
    until = parse_iso_utc(data.get("exhausted_until")) if isinstance(data.get("exhausted_until"), str) else None
    if (until is not None and now < until <= now + timedelta(days=32)
            and data.get("exhausted_key") == key_id(api_key)):
        return until
    return None


def _prune() -> None:
    """Delete answers last written more than MAX_STALE ago: no caller can be served one."""
    cutoff = time.time() - MAX_STALE.total_seconds()
    try:
        names = os.listdir(CACHE_DIR)
    except OSError:
        return
    for name in names:
        if name.endswith(".json.gz"):
            path = os.path.join(CACHE_DIR, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except OSError:
                pass


def _quota_refusal(r: Any) -> bool:
    if r.status_code != 429:
        return False
    try:
        body = r.json()
    except ValueError:
        return False
    return isinstance(body, dict) and body.get("message") == QUOTA_MESSAGE


def get_json(
    base: str, path: str, params: Dict[str, Any], api_key: str, *, timeout: float,
    log_tag: str, valid: Callable[[Any], bool], expect: str,
    stale_for: timedelta = timedelta(0), now: Optional[datetime] = None,
) -> Any:
    """The answer to one question: kept while younger than FRESH_FOR, else asked.

    A failed or refused request, or a used-up month, serves the kept answer if it is younger
    than `stale_for` (default none, at most MAX_STALE) and logs that it did; otherwise RAISES
    `CollegeDataUnavailable`. An answer `valid` rejects is a failure and is not kept; `expect`
    names the shape it wanted, for that message."""
    now = now or _now()
    stale_for = min(stale_for, MAX_STALE)
    question = _question(base, path, params)
    kept = _kept(question, now)
    if kept is not None and kept["age"] < FRESH_FOR:
        return kept["body"]

    def stand_in(why: str) -> Any:
        if kept is not None and kept["age"] < stale_for:
            logger.warning("[%s] %s %s; using the answer kept %.1f h ago", log_tag, path, why,
                           kept["age"].total_seconds() / 3600)
            return kept["body"]
        raise CollegeDataUnavailable(f"{path} {why}")

    until = _waiting_until(api_key, now)
    if until is not None:
        return stand_in(f"not asked: the month's free-tier calls are used up until {until.isoformat()}")

    try:
        r = get_within_deadline(requests, f"{base}{path}", headers={"Authorization": f"Bearer {api_key}"},
                                params=params, timeout=timeout)
    except requests.RequestException as e:
        # The URL carries no key (the key is a header), so the exception text is safe to log.
        return stand_in(f"request failed ({e})")
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if not _quota_refusal(r):
            return stand_in(f"answered HTTP {r.status_code}")
        until = _next_reset(now)
        quota = _read(_quota_path(), compressed=False) or {}
        quota.update(exhausted_until=until.isoformat(), exhausted_key=key_id(api_key), refused_at=now.isoformat())
        _write(_quota_path(), quota, compressed=False)
        logger.warning(
            "[college_data] CollegeFootballData's free tier is 1,000 calls a month shared with "
            "CollegeBasketballData, and this month's are used up: no college data requests until "
            "%s. Kept answers stand in meanwhile; the key itself is fine.", until.isoformat(),
        )
        return stand_in(f"refused: the month's free-tier calls are used up until {until.isoformat()}")
    try:
        body = r.json()
    except ValueError:
        return stand_in("answered something that is not JSON")
    if not valid(body):
        return stand_in(f"answered {str(body)[:80]!r}, not {expect}")

    _write(_path(question), {"v": _VERSION, "question": question, "fetched_at": now.isoformat(),
                             "body": body}, compressed=True)
    # An answer proves the month is not used up, whatever an older wait said.
    remaining = r.headers.get("X-CallLimit-Remaining")
    quota = _read(_quota_path(), compressed=False) or {}
    quota.pop("exhausted_until", None)
    quota.pop("exhausted_key", None)
    quota.update(calls_remaining=remaining, checked_at=now.isoformat())
    _write(_quota_path(), quota, compressed=False)
    _prune()
    logger.info("[%s] %s fetched (%s calls left this month)", log_tag, path,
                remaining if remaining is not None else "?")
    return body
