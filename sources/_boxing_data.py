"""The Boxing Data API (RapidAPI): the one place this plugin calls it (#361).

The free tier is 100 calls a MONTH, a hard limit (a user report, #361; RapidAPI's free plans
are monthly hard limits, its `x-rate-limit-rapid-free-plans-hard-limit-*` headers). Before
this module `sources/boxing.py` asked on every refresh, up to `_MAX_PAGES` (4) requests each,
and refreshes run four scheduled times a day plus every post-M3U rematch (up to one each
15 min while a curated game is live), so about 50 calls a day: the month was gone in two.

The budget now. A COMPLETE schedule (every page read, nothing failed) is kept on disk for
FRESH_FOR (12 h) and served from there without a request. Complete reads of the same lookahead,
once saved, are at least 12 h apart, so at most 2 a day, 63 in a 31-day month of one-page weeks (a week has had 3 cards when measured,
2026-07-10; a page holds 25). A failed or partial read is not kept, so the next refresh asks again; that is
why every outbound attempt, successful or not, is reserved on disk BEFORE it is sent against a
MONTHLY_ATTEMPTS (80) ceiling per UTC month and hashed key, so a run of failures (one every
15 min spends 80 in about 20 hours) or busy multi-page weeks stops at 80 and leaves 20 of the 100 for the user.
Boxing cards are announced weeks ahead, so a schedule a few hours old loses nothing.

So, per key:

- On a quota refusal no request is made with that key until the reset (`_quota_wait`): HTTP
  429 whose `message` names a quota, or any answer whose remaining-requests header reads 0.
  The reset is that used-up counter's own header (`x-ratelimit-requests-reset`, or the
  free plan's `x-rate-limit-rapid-free-plans-hard-limit-reset`), seconds until the quota
  resets per https://docs.rapidapi.com/docs/response-headers, else the 1st of the next
  month, 00:00 UTC. NOT live-measured: no Boxing Data key is configured on Tower. RapidAPI's
  documented quota answer is 429 `{"message": "You have exceeded the MONTHLY quota for
  Requests on your current plan, ..."}`; a burst limit is 429 `{"message": "Too many
  requests"}`, which is an ordinary failure here, never a month-long wait.
  `tests/test_boxing_budget.py` pins both readings as the documented assumption they are.
- Each key has its own wait and its own count, tagged by `_util.key_id`: a new key is not held
  by the old one's, and no key is ever written to disk.
- A failed or refused request raises `BoxingDataUnavailable` (`BoxingQuotaUnavailable` for a
  quota hold), and `boxing.py` serves the kept schedule while it is younger than MAX_STALE,
  reporting the days it does not cover as lost (#261), else reports the whole window lost.

DO NOT shorten FRESH_FOR, raise MONTHLY_ATTEMPTS, or call this API from anywhere else without
redoing the arithmetic above: each multiplies calls against a 100-call month.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple, Optional

import requests

from .._http import get_within_deadline
from .. import storage
from .._util import redact_header_values, key_id, parse_iso_utc, redact_secrets, write_json_atomic

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.boxing")

# RapidAPI gateway for the Boxing Data API (boxing-data.com). The key is a RapidAPI key; the
# host header selects this API on the shared gateway.
RAPIDAPI_HOST = "boxing-data-api.p.rapidapi.com"
SCHEDULE_URL = f"https://{RAPIDAPI_HOST}/v2/events/schedule"

# Beside cache.json (storage root). Sources run in the pipeline subprocess, one refresh at a
# time (the run lock), so there is one writer.
CACHE_DIR = storage.path("boxing_cache")

FRESH_FOR = timedelta(hours=12)
MONTHLY_ATTEMPTS = 80
# A kept schedule covers `days` (at most 7) from the day it was read; older than that it
# covers nothing still ahead, so it is never served.
MAX_STALE = timedelta(days=7)
# A wait longer than this is a corrupt value or a clock that went backwards, never a real month.
_LONGEST_WAIT = timedelta(days=32)
_VERSION = 1
# (remaining, reset) per RapidAPI counter: the plan's, and the free plan's monthly hard limit.
_COUNTERS = (
    ("x-ratelimit-requests-remaining", "x-ratelimit-requests-reset"),
    ("x-rate-limit-rapid-free-plans-hard-limit-remaining", "x-rate-limit-rapid-free-plans-hard-limit-reset"),
)
_REMAINING_HEADERS = tuple(remaining for remaining, _reset in _COUNTERS)


class BoxingDataUnavailable(RuntimeError):
    """No answer: the request failed or was refused, or the month's budget is used up. The
    message never carries the key."""

    quota = False


class BoxingQuotaUnavailable(BoxingDataUnavailable):
    """The key's quota (RapidAPI's or this module's ceiling) is used up until a reset."""

    quota = True


class Schedule(NamedTuple):
    events: List[Any]
    fetched_at: datetime
    first_day: date
    days: int

    @property
    def last_day(self) -> date:
        return self.first_day + timedelta(days=self.days)


def now() -> datetime:
    return datetime.now(timezone.utc)


def _schedule_path() -> str:
    return os.path.join(CACHE_DIR, "schedule.json")


def _quota_path() -> str:
    return os.path.join(CACHE_DIR, "quota.json")


def _read(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _write(path: str, data: Dict[str, Any]) -> bool:
    """Whether the state was saved; never raises."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        write_json_atomic(path, data, separators=(",", ":"))
        return True
    except (OSError, TypeError, ValueError) as e:
        logger.warning("[boxing] could not write %s: %s", path, e)
        return False


def kept_schedule(days: int, at: datetime) -> Optional[Schedule]:
    """The kept complete schedule for a `days`-day request, or None. Not from the future: a
    clock that went backwards must not make an old schedule look fresh."""
    data = _read(_schedule_path())
    if data.get("v") != _VERSION or data.get("days") != days or not isinstance(data.get("events"), list):
        return None
    fetched = parse_iso_utc(data.get("fetched_at")) if isinstance(data.get("fetched_at"), str) else None
    try:
        first = date.fromisoformat(data.get("first_day"))
    except (TypeError, ValueError):
        return None
    if fetched is None or fetched > at + timedelta(minutes=5):
        return None
    return Schedule(data["events"], fetched, first, days)


def keep_schedule(schedule: Schedule) -> None:
    _write(_schedule_path(), {
        "v": _VERSION, "days": schedule.days, "first_day": schedule.first_day.isoformat(),
        "fetched_at": schedule.fetched_at.isoformat(), "events": schedule.events,
    })


def _next_month(at: datetime) -> datetime:
    return (at.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)


def _waits(at: datetime) -> Dict[str, str]:
    """{key tag: wait-until} still ahead and plausible."""
    waits = _read(_quota_path()).get("waits")
    out = {}
    for kid, until in (waits if isinstance(waits, dict) else {}).items():
        when = parse_iso_utc(until) if isinstance(until, str) else None
        if when is not None and at < when <= at + _LONGEST_WAIT:
            out[kid] = until
    return out


def _reserve_attempt(at: datetime, kid: str) -> None:
    """Count this attempt BEFORE it is sent, so a timeout or crash still spends it. A wait
    still ahead, a used-up month, or a counter that cannot be saved refuses the request."""
    until = _waits(at).get(kid)
    if until is not None:
        raise BoxingQuotaUnavailable(f"not asked: the plan's quota is used up until {until}")
    data = _read(_quota_path())
    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    month = at.strftime("%Y-%m")
    current = usage.get(kid) if isinstance(usage.get(kid), dict) else {}
    attempts = current.get("attempts", 0) if current.get("month") == month else 0
    if not isinstance(attempts, int):
        attempts = 0
    if attempts >= MONTHLY_ATTEMPTS:
        raise BoxingQuotaUnavailable(
            f"not asked: the local {MONTHLY_ATTEMPTS}-attempt monthly budget is used up until "
            f"{_next_month(at).isoformat()} (free tier: 100 calls a month)")
    usage[kid] = {"month": month, "attempts": attempts + 1}
    data["usage"] = usage
    if not _write(_quota_path(), data):
        raise BoxingDataUnavailable("not asked: the monthly attempt counter could not be saved")


def _header_int(headers: Dict[str, str], names: tuple) -> Optional[int]:
    for name in names:
        try:
            return int(str(headers[name]).strip())
        except (KeyError, ValueError):
            continue
    return None


def _quota_wait(status: int, body: Any, headers: Dict[str, str], at: datetime) -> Optional[datetime]:
    """When this key may ask again, if the answer says its quota is used up; else None.

    The rule: the latest deadline among RapidAPI's counters that read 0 remaining (the
    plan's `x-ratelimit-requests-*`, the free plan's monthly `hard-limit-*`), each counter's
    own reset header, or the 1st of the next month when that header is missing or implausible.
    A 429 whose message names a quota with no counter at 0 waits for the next month. Any other
    429 (RapidAPI's per-second "Too many requests") is an ordinary failure: waiting a month
    for a burst limit would cost the whole sport."""
    deadlines = []
    for remaining, reset in _COUNTERS:
        left = _header_int(headers, (remaining,))
        if left is not None and left <= 0:
            seconds = _header_int(headers, (reset,))
            plausible = seconds is not None and 0 < seconds <= _LONGEST_WAIT.total_seconds()
            deadlines.append(at + timedelta(seconds=seconds) if plausible else _next_month(at))
    if deadlines:
        return max(deadlines)
    message = body.get("message") if isinstance(body, dict) else None
    if status == 429 and isinstance(message, str) and "quota" in message.lower():
        return _next_month(at)
    return None


def _record_wait(at: datetime, kid: str, until: Optional[datetime], remaining: Optional[int]) -> None:
    """Set (`until`) or clear (an answer with quota left) this key's wait, keeping every other
    key's, and remember the last remaining-requests figure for diagnosis."""
    data = _read(_quota_path())
    waits = _waits(at)
    if until is not None:
        waits[kid] = until.isoformat()
        data["refused_at"] = at.isoformat()
    else:
        waits.pop(kid, None)
    data["waits"] = waits
    data.update(calls_remaining=remaining, checked_at=at.isoformat())
    _write(_quota_path(), data)


def get_page(params: Dict[str, Any], api_key: str, *, at: datetime, timeout: float = 15.0) -> Any:
    """One schedule request's JSON body. Raises `BoxingDataUnavailable` (or
    `BoxingQuotaUnavailable`) when it was not sent, failed, or was refused."""
    kid = key_id(api_key)
    _reserve_attempt(at, kid)
    headers = {"x-rapidapi-key": api_key, "x-rapidapi-host": RAPIDAPI_HOST}
    try:
        # DO NOT follow redirects: a redirect is a second send on this one reserved attempt,
        # and `requests` forwards a custom header (the key) to whatever host it points at.
        r = get_within_deadline(requests, SCHEDULE_URL, params=params, headers=headers,
                                timeout=timeout, allow_redirects=False)
    except requests.RequestException as e:
        # `requests` quotes a malformed header value (the key) verbatim in InvalidHeader.
        raise BoxingDataUnavailable(
            f"request failed ({redact_header_values(redact_secrets(e), headers)})") from None
    answer = {str(k).lower(): v for k, v in (getattr(r, "headers", None) or {}).items()}
    try:
        body = r.json()
    except ValueError:
        body = None
    until = _quota_wait(r.status_code, body, answer, at)
    remaining = _header_int(answer, _REMAINING_HEADERS)
    if until is not None or r.status_code < 300:
        _record_wait(at, kid, until, remaining)
    if r.status_code >= 300:
        if until is not None:
            raise BoxingQuotaUnavailable(f"refused: the plan's quota is used up until {until.isoformat()}")
        message = body.get("message") if isinstance(body, dict) else None
        # Upstream text is masked for the key too: an error message may quote it.
        shown = redact_header_values(redact_secrets(str(message)), headers)[:120] if message else ""
        raise BoxingDataUnavailable(f"answered HTTP {r.status_code}" + (f": {shown}" if shown else ""))
    if body is None:
        raise BoxingDataUnavailable("answered something that is not JSON")
    logger.info("[boxing] schedule page fetched (%s calls left on the plan)",
                remaining if remaining is not None else "?")
    return body
