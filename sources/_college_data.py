"""CollegeFootballData and CollegeBasketballData: the one place this plugin calls them (#337).

The free tier is 1,000 calls a MONTH, in ONE pool both APIs draw from through the one key.
CFBD's /info said so on 2026-11-09: `"tierName":"Free","monthlyLimit":1000,"sharedPool":true,
"products":["cfb","cbb"],"resetAt":"2026-12-01T00:00:00.000Z"`. The sources used to say
"1k req/day", asked about ten questions a refresh (NCAAF's /lines once per week in the
window), and refreshes run 9 to 12 times a day (four scheduled plus M3U rematches; 28 a day in
late October). So the month's pool was gone within days and every /rankings call answered 429
`Monthly call quota exceeded.` until the reset: on 2026-11-09 the November pool had been empty
since November 1.

The budget now. Nine questions, whatever the lookahead: NCAAF /rankings, the season's /games
and the season's /lines (ONE question, not one per week: a 90-day lookahead once asked eleven);
NCAAM /rankings, the window's /games and /lines, and three season /games chunks. Each answer is
kept for FRESH_FOR, so a question is asked at most twice a day, three times on the day the NCAAM
window moves: about 20 calls a day, ~620 in a 31-day month, under the pool with headroom.

So, per question (API, path and parameters):

- The answer is kept on disk for FRESH_FOR and served from there without a request.
- On the exact `Monthly call quota exceeded.` 429, no request is made with that key until the
  pool resets (the 1st of the next month, 00:00 UTC), logged plainly once. Each key has its own
  wait: a new key is not held by the old one's, and the old one's is not cleared by the new
  one's answers.
- A failed or refused request raises `CollegeDataUnavailable`, and the caller's existing failure
  path runs unchanged: the season store stands in for a season read (`SeasonUnavailable`), a lost
  window is held, a missing line drops a spread. Only /rankings, which has no fallback of its own,
  is served from a kept answer meanwhile (`stale_for`): last week's poll ranks new games, where
  failing would hold the label with no new game added (#283, #240). DO NOT give a question a
  `stale_for` where the caller already falls back: two stand-ins for one read is two answers to
  "how old may this be", and the season store's is measured.
- An answer is kept only after the caller's `check` (its real parser) accepts it, so a
  malformed answer served with a 200 never replaces a good one for twelve hours.
- `get_answer` returns when the answer was fetched, so a caller that keeps a derived copy (the
  season store) dates it by the data, not by the refresh that read it from here.

DO NOT shorten FRESH_FOR, add a question per refresh (or per week, day or team), or drop the
quota wait without redoing the budget above: each one multiplies the month's calls.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, NamedTuple, Optional

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
# A wait longer than this is a corrupt value or a clock that went backwards, never a real month.
_LONGEST_WAIT = timedelta(days=32)
QUOTA_MESSAGE = "Monthly call quota exceeded."
_VERSION = 1


class CollegeDataUnavailable(RuntimeError):
    """No answer: the request failed or was refused, or the month's pool is used up, and no
    kept answer is recent enough to stand in. The message never carries the key."""


class Answer(NamedTuple):
    body: Any
    fetched_at: datetime


def require_list(body: Any) -> None:
    """The `check` for an answer that must be a JSON list (`[]` included)."""
    if not isinstance(body, list):
        raise ValueError("not a list")


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


def _kept(question: str, now: datetime) -> Optional[Answer]:
    """The kept answer for `question`, or None. Not from the future either: a clock that went
    backwards must not make an old answer look fresh."""
    data = _read(_path(question), compressed=True)
    if not (data and data.get("v") == _VERSION and data.get("question") == question and "body" in data):
        return None
    at = parse_iso_utc(data.get("fetched_at")) if isinstance(data.get("fetched_at"), str) else None
    if at is None or at > now + timedelta(minutes=5):
        return None
    return Answer(data["body"], at)


def _next_reset(now: datetime) -> datetime:
    """The 1st of the next month, 00:00 UTC: where CFBD's /info put `resetAt`."""
    return (now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)


def _waits(now: datetime) -> Dict[str, str]:
    """{key tag: wait-until} still ahead and plausible, from quota.json."""
    data = _read(_quota_path(), compressed=False) or {}
    waits = data.get("waits") if isinstance(data.get("waits"), dict) else {}
    out = {}
    for kid, until in waits.items():
        at = parse_iso_utc(until) if isinstance(until, str) else None
        if at is not None and now < at <= now + _LONGEST_WAIT:
            out[kid] = until
    return out


def _record_quota(now: datetime, kid: str, *, wait_until: Optional[datetime] = None,
                  remaining: Optional[str] = None) -> None:
    """Set (`wait_until`) or clear (an answer) this key's wait, keeping every other key's."""
    data = _read(_quota_path(), compressed=False) or {}
    waits = _waits(now)
    if wait_until is not None:
        waits[kid] = wait_until.isoformat()
        data["refused_at"] = now.isoformat()
    else:
        waits.pop(kid, None)
        data.update(calls_remaining=remaining, checked_at=now.isoformat())
    data["waits"] = waits
    _write(_quota_path(), data, compressed=False)


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


def get_answer(
    base: str, path: str, params: Dict[str, Any], api_key: str, *, timeout: float,
    log_tag: str, check: Callable[[Any], Any], stale_for: timedelta = timedelta(0),
    now: Optional[datetime] = None,
) -> Answer:
    """The answer to one question and when it was fetched: kept while younger than FRESH_FOR,
    else asked.

    `check` is the caller's parser: an answer it raises on is a failure and is not kept. A
    failed or refused request, or a used-up month, serves the kept answer if it is younger than
    `stale_for` (default none, at most MAX_STALE) and logs that it did; otherwise RAISES
    `CollegeDataUnavailable`."""
    now = now or _now()
    stale_for = min(stale_for, MAX_STALE)
    question = _question(base, path, params)
    kept = _kept(question, now)
    if kept is not None and now - kept.fetched_at < FRESH_FOR:
        return kept

    def stand_in(why: str) -> Answer:
        if kept is not None and now - kept.fetched_at < stale_for:
            logger.warning("[%s] %s %s; using the answer kept %.1f h ago", log_tag, path, why,
                           (now - kept.fetched_at).total_seconds() / 3600)
            return kept
        raise CollegeDataUnavailable(f"{path} {why}")

    kid = key_id(api_key)
    until = _waits(now).get(kid)
    if until is not None:
        return stand_in(f"not asked: the month's free-tier calls are used up until {until}")

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
        reset = _next_reset(now)
        _record_quota(now, kid, wait_until=reset)
        logger.warning(
            "[college_data] CollegeFootballData's free tier is 1,000 calls a month shared with "
            "CollegeBasketballData, and this month's are used up: no college data requests with "
            "this key until %s. Kept answers stand in meanwhile; the key itself is fine.",
            reset.isoformat(),
        )
        return stand_in(f"refused: the month's free-tier calls are used up until {reset.isoformat()}")
    try:
        body = r.json()
    except ValueError:
        return stand_in("answered something that is not JSON")
    try:
        check(body)
    except Exception as e:  # the caller's parser: any raise is an unusable answer
        return stand_in(f"answered {str(body)[:80]!r}: {e}")

    _write(_path(question), {"v": _VERSION, "question": question, "fetched_at": now.isoformat(),
                             "body": body}, compressed=True)
    remaining = r.headers.get("X-CallLimit-Remaining")
    _record_quota(now, kid, remaining=remaining)   # an answer: this key's month is not used up
    _prune()
    logger.info("[%s] %s fetched (%s calls left this month)", log_tag, path,
                remaining if remaining is not None else "?")
    return Answer(body, now)


def get_json(*args: Any, **kwargs: Any) -> Any:
    """`get_answer(...)`'s body, for a caller that keeps no copy of its own."""
    return get_answer(*args, **kwargs).body
