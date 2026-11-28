"""The schedule APIs' answers, kept on disk for as long as their data cannot change (#363).

Before this module every refresh re-read everything. Measured 2026-11-28 on Tower, one
refresh sent 1,331 requests (ESPN 1,285: MLS walked its whole season four times, NFL,
college baseball and NCAA soccer once each; NHL 36; football-data.org 16), and Tower runs
about twelve refreshes a day (five scheduled plus M3U rematches, Dispatcharr's task
history), so about 16,000 requests a day for data that had not changed since the last one.

The rule here is that a refresh asks only for what could have changed since it was last
asked. `fetch_json` keeps each successful answer (gzipped JSON, one file per request key)
and serves it with no request while `fresh` says it is current; `schedule_fresh` is the
policy for the schedule endpoints, by what the request covers:

- A DAY'S scoreboard or schedule (ESPN `dates=`, NHL `schedule/{date}`, a dated MLB or
  football-data.org range), judged against today's US Eastern date (ESPN, NHL and statsapi
  bucket by it; `sources/_espn.py` `upcoming_window`):
  - fetched two or more days after its last day: FINAL, kept for good. Results do not
    change, and the season walks the importance simulator replays are mostly finished days.
  - its days touch yesterday..tomorrow: 4 h (`NEAR_FOR`). Kickoff times and statuses can
    still move; four hours keeps a refresh at most four hours behind on today's slate, and
    `plugin._rows_to_hold` and the reaper already own what happens once a game kicks off.
  - within the next two weeks: 24 h (`MID_FOR`). Fixture dates move days ahead, not hours.
  - further out: 7 days (`FAR_FOR`). Those days only feed the season simulation.
  - a past day not yet fetched as final: asked again, once, then final.
- No date (standings, polls, whole-season lists, calendars), or a range longer than two
  weeks that does not touch yesterday..tomorrow: the caller's `max_age`. A range that
  touches them (an upcoming window, whatever its lookahead) is near.

`max_age` and the day policy are LIFETIMES, not retry intervals: a failed request is never
kept (`fetch` returns None or raises, `check` raises), so the caller's own failure path
(lost days, season store, held games) runs exactly as before and the next refresh asks
again. DO NOT keep a failure here, and DO NOT add a stand-in for a failed request: each
caller already has one, and two answers to "how old may this be" disagree
(`sources/_college_data.py` states the same rule).

DO NOT shorten a lifetime without redoing the day budget `tests/test_api_budget.py` pins
(a day of refreshes through NFL): each one multiplies calls by the number of refreshes. DO NOT key a request on anything that
carries a credential: keys are hashed for the file name, but the key text is stored in
the file. ESPN, NHL and statsapi take no key; football-data.org takes its key as a header.

Files are pruned when untouched for `PRUNE_AFTER` (a served file has its mtime refreshed
at most daily), so a finished day of a season still being simulated stays, and a past
season goes. Writers: the pipeline child (one run at a time, the run lock), whose sweep
threads write distinct files through the one atomic writer.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional, Tuple
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from . import storage
from ._util import parse_iso_utc, write_json_atomic

CACHE_DIR = storage.path("api_cache")
NEAR_FOR = timedelta(hours=4)
MID_FOR = timedelta(hours=24)
FAR_FOR = timedelta(days=7)
MID_DAYS = 14
PRUNE_AFTER = timedelta(days=21)
_TOUCH_AFTER = 24 * 3600
_EASTERN = ZoneInfo("America/New_York")
_VERSION = 1
_pruned_at = 0.0

Fresh = Callable[[datetime, datetime], bool]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _path(key: str) -> str:
    return os.path.join(CACHE_DIR, hashlib.sha256(key.encode("utf-8")).hexdigest()[:32] + ".json.gz")


def eastern_date(at: datetime) -> date:
    return at.astimezone(_EASTERN).date()


def _parse_day(raw: str) -> Optional[date]:
    raw = raw.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:10] if "-" in raw else raw[:8], fmt).date()
        except ValueError:
            continue
    return None


def request_days(url: str, params: Optional[dict] = None) -> Optional[Tuple[date, date]]:
    """The first and last date a schedule request covers, or None for an undated one.

    ESPN `dates=YYYYMMDD` (one day), NHL `/schedule/YYYY-MM-DD` (api-web answers that day's
    week: seven days), MLB `startDate`/`endDate`, football-data.org `dateFrom`/`dateTo`
    (`dateTo` is exclusive there, so the last day is the one before it)."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    query = {k: v[0] for k, v in parse_qs(parts.query).items() if v}
    for k, v in (params or {}).items():
        query[str(k)] = str(v)
    if "dates" in query and re.fullmatch(r"\d{8}", query["dates"]):
        day = _parse_day(query["dates"])
        return (day, day) if day else None
    m = re.search(r"/schedule/(\d{4}-\d{2}-\d{2})$", parts.path)
    if m and "nhle.com" in (parts.hostname or ""):
        day = _parse_day(m.group(1))
        return (day, day + timedelta(days=6)) if day else None
    if "startDate" in query and "endDate" in query:
        first, last = _parse_day(query["startDate"]), _parse_day(query["endDate"])
        return (first, last) if first and last else None
    if "dateFrom" in query and "dateTo" in query:
        first, end = _parse_day(query["dateFrom"]), _parse_day(query["dateTo"])
        return (first, max(first, end - timedelta(days=1))) if first and end else None
    return None


def schedule_fresh(days: Optional[Tuple[date, date]], max_age: timedelta) -> Fresh:
    """`fresh(fetched_at, now)` for a request covering `days` (None: undated, `max_age`)."""
    def fresh(fetched_at: datetime, now: datetime) -> bool:
        age = now - fetched_at
        if age < timedelta(0):
            return False  # from the future: a clock went backwards, ask again
        if days is None:
            return age < max_age
        first, last = days
        if eastern_date(fetched_at) >= last + timedelta(days=2):
            return True
        today = eastern_date(now)
        if last < today - timedelta(days=1):
            return False  # a past day not yet read as final
        if first <= today + timedelta(days=1):
            # Touches yesterday..tomorrow, however long the range: an upcoming
            # window with a long lookahead still carries today's slate.
            return age < NEAR_FOR
        if (last - first).days > MID_DAYS:
            return age < max_age   # a future whole-season range: the caller's lifetime
        if first <= today + timedelta(days=MID_DAYS):
            return age < MID_FOR
        return age < FAR_FOR
    return fresh


def _read(key: str) -> Optional[Tuple[Any, datetime]]:
    path = _path(key)
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, EOFError):
        return None
    if not (isinstance(data, dict) and data.get("v") == _VERSION and data.get("key") == key
            and "body" in data and isinstance(data.get("fetched_at"), str)):
        return None
    at = parse_iso_utc(data["fetched_at"])
    if at is None:
        return None
    try:
        if time.time() - os.path.getmtime(path) > _TOUCH_AFTER:
            os.utime(path)
    except OSError:
        pass
    return data["body"], at


def _prune() -> None:
    """At most once an hour per process: remove files untouched for PRUNE_AFTER."""
    global _pruned_at
    if time.time() - _pruned_at < 3600:
        return
    _pruned_at = time.time()
    cutoff = time.time() - PRUNE_AFTER.total_seconds()
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


def kept(key: str, fresh: Fresh, now: Optional[datetime] = None) -> Optional[Any]:
    """The kept answer for `key` when `fresh` says it is current, else None."""
    hit = _read(key)
    if hit is None:
        return None
    body, at = hit
    return body if fresh(at, now or _now()) else None


def keep(key: str, body: Any, now: Optional[datetime] = None) -> None:
    """Keep a CHECKED answer. Never raises: an answer that cannot be kept is still used."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        write_json_atomic(_path(key), {"v": _VERSION, "key": key,
                                       "fetched_at": (now or _now()).isoformat(), "body": body},
                          compress=True, separators=(",", ":"))
    except (OSError, TypeError, ValueError):
        return
    _prune()


# (URL fragment, the list an answer from it always carries). A 200 without it is an error
# body (ESPN answers `{"code": 404}`, statsapi and api-web their own), which every caller
# reads as a failure (#282) and which must not be kept and replayed for hours.
_ANSWER_LISTS = (
    ("/scoreboard", "events"),
    ("/rankings", "rankings"),
    ("site.api.espn.com/apis/v2/", "children"),  # ESPN standings
    ("club-schedule-season/", "games"),
    ("/schedule/playoff-series/", "games"),
    ("api-web.nhle.com/v1/schedule/", "gameWeek"),
    ("statsapi.mlb.com/api/v1/schedule", "dates"),
)
_ERROR_KEYS = {"code", "error", "errorCode", "message", "status"}


def looks_like_answer(url: str, body: Any) -> bool:
    """Whether a 200's body is the endpoint's answer and not an error object."""
    for fragment, list_key in _ANSWER_LISTS:
        if fragment in url:
            return isinstance(body, dict) and isinstance(body.get(list_key), list)
    if isinstance(body, list):
        return True
    return isinstance(body, dict) and bool(body) and not set(body) <= _ERROR_KEYS


def lifetime(url: str) -> timedelta:
    """How long an UNDATED answer from `url` stays current (a dated one follows the day
    policy in the docstring). Weekly polls a day; standings and whole-season lists half a
    day (they change once a matchday's results are in); calendars a week; a playoff
    bracket or series four hours (it changes with each game of a live series)."""
    if "/rankings" in url:
        return timedelta(hours=24)
    if "sports.core.api.espn.com" in url:
        return timedelta(days=7)
    if "playoff" in url or "postseason" in url:
        return NEAR_FOR
    return timedelta(hours=12)


_CREDENTIAL = re.compile(r"key|token|secret|auth|pass", re.IGNORECASE)


def request_key(url: str, params: Optional[dict] = None) -> Optional[str]:
    """The cache key for a GET of `url` with `params`, or None when the URL or a
    parameter name looks like it carries a credential (never kept, see the docstring)."""
    if _CREDENTIAL.search(urlsplit(url).query) or any(_CREDENTIAL.search(str(k)) for k in params or {}):
        return None
    return json.dumps([url, sorted((str(k), str(v)) for k, v in (params or {}).items())])


def fetch_json(key: str, fresh: Fresh, fetch: Callable[[], Any],
               check: Optional[Callable[[Any], Any]] = None,
               keep_if: Optional[Callable[[Any], bool]] = None) -> Any:
    """The answer for `key`: kept while `fresh`, else `fetch()`.

    A fetched answer is kept only when it is not None, `check` (the caller's parser, which
    raises on an unusable answer) accepts it, and `keep_if` (a shape test for callers whose
    failure is a returned value, not a raise) says yes. `fetch` or `check` raising
    propagates unchanged and keeps nothing; an answer `keep_if` refuses is returned to the
    caller, whose own parser then treats it as the failure it is, and not kept."""
    now = _now()
    body = kept(key, fresh, now)
    if body is not None:
        return body
    body = fetch()
    if body is None:
        return None
    if check is not None:
        check(body)
    if keep_if is None or keep_if(body):
        keep(key, body, now)
    return body
