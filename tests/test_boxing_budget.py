"""The Boxing Data API monthly budget (#361).

A user reported the free tier (100 calls) gone in about two days: the schedule
was fetched on every refresh, up to four pages each, and post-M3U rematches
run a full refresh up to every 15 minutes while a game is live.

The quota answers below are RapidAPI's documented ones
(https://docs.rapidapi.com/docs/response-headers): a monthly quota refusal is
HTTP 429 with a message naming the quota, a burst limit is 429 "Too many
requests", and `x-ratelimit-requests-reset` is seconds until the quota resets.
NOT live-captured: no Boxing Data key is configured on Tower, so these pin a
documented assumption, not a measurement.
"""

import importlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from tests.conftest import stub_requests_get

UTC = timezone.utc
NOW = datetime(2026, 11, 26, 4, 0, tzinfo=UTC)
KEY = "rapid-k3y-that-must-never-be-written"
QUOTA_MESSAGE = ("You have exceeded the MONTHLY quota for Requests on your current plan, BASIC. "
                 "Upgrade your plan at https://rapidapi.com/bengroves1993/api/boxing-data-api")
SOURCES = Path(__file__).resolve().parent.parent / "sources"
ROOT = SOURCES.parent


def _card(i, day="2026-11-28T20:00:00"):
    return {"id": f"c{i}", "title": f"Fighter{i} vs. Rival{i}: Card", "date": day}


def _page(cards, page=1, total_pages=1, total_items=None):
    return {"pagination": {"page": page, "items": len(cards), "total_pages": total_pages,
                           "total_items": len(cards) if total_items is None else total_items},
            "error": None, "data": cards}


class FakeResp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = _page([_card(1)]) if body is None else body
        self.headers = {"X-RateLimit-Requests-Remaining": "90"} if headers is None else headers

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


@pytest.fixture
def mods():
    boxing = importlib.import_module("dispatcharr_ranked_matchups.sources.boxing")
    data = importlib.import_module("dispatcharr_ranked_matchups.sources._boxing_data")
    return boxing, data


@pytest.fixture
def clock(mods, monkeypatch):
    """Pins both clocks the source reads: the budget's and `upcoming_window`'s."""
    from dispatcharr_ranked_matchups.sources import _espn
    _boxing, data = mods
    state = {"now": NOW}

    class _Pinned(datetime):
        @classmethod
        def now(cls, tz=None):
            return state["now"] if tz is not None else state["now"].replace(tzinfo=None)

    monkeypatch.setattr(_espn, "datetime", _Pinned)
    monkeypatch.setattr(data, "now", lambda: state["now"])
    return state


@pytest.fixture
def http(monkeypatch):
    calls, queue = [], []

    def get(url, params=None, headers=None, timeout=None, allow_redirects=True):
        assert allow_redirects is False       # a redirect would be a second, unreserved send
        calls.append((url, dict(params or {}), dict(headers or {})))
        answer = queue.pop(0) if queue else FakeResp()
        if isinstance(answer, Exception):
            raise answer
        return answer

    stub_requests_get(monkeypatch, get)
    return calls, queue


def _refresh(mods, key=KEY, days_ahead=7):
    """One refresh's boxing fetch: (rows, lost days)."""
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    boxing, _data = mods
    with recording_lost_days() as lost:
        rows = boxing.BoxingSource(api_key=key).fetch_upcoming(days_ahead=days_ahead)
    return rows, lost


def _all_days(first, last):
    return {first + timedelta(days=i) for i in range((last - first).days + 1)}


WEEK = _all_days(date(2026, 11, 26), date(2026, 12, 3))


def test_a_schedule_is_kept_for_twelve_hours(mods, clock, http):
    """WITNESS: every refresh asked again. Four scheduled refreshes plus a
    rematch every 15 min on a game day spent ~50 of the 100 calls a day."""
    calls, _ = http
    rows, lost = _refresh(mods)
    assert [r.home for r in rows] == ["Fighter1 vs. Rival1"] and lost == set()
    for minutes in range(15, 12 * 60, 15):          # a rematch every 15 minutes
        clock["now"] = NOW + timedelta(minutes=minutes)
        rows, lost = _refresh(mods)
        assert len(rows) == 1 and lost == set()
    assert len(calls) == 1
    clock["now"] = NOW + timedelta(hours=12)
    _refresh(mods)
    assert len(calls) == 2


def test_a_month_of_refreshes_fits_the_free_tier(mods, clock, http):
    """The budget's arithmetic, run: 31 days of the default four slots plus a
    rematch every 15 minutes all day is at most two requests a day."""
    calls, _ = http
    for step in range(31 * 24 * 4):
        clock["now"] = NOW + timedelta(minutes=15 * step)
        _refresh(mods)
    assert len(calls) <= 63, len(calls)


def test_attempts_are_capped_below_the_plan_even_when_every_request_fails(mods, clock, http):
    """A failed request is not kept, so without the counter every refresh of a
    broken day would ask again. Failures count too, and stop at the ceiling."""
    calls, queue = http
    _boxing, data = mods
    for step in range(200):
        clock["now"] = NOW.replace(day=1) + timedelta(minutes=15 * step)
        queue.append(FakeResp(503, {"message": "upstream down"}))
        _refresh(mods)
    assert len(calls) == data.MONTHLY_ATTEMPTS == 80
    clock["now"] = datetime(2026, 12, 1, 0, 5, tzinfo=UTC)     # a new UTC month
    _refresh(mods)
    assert len(calls) == 81


def test_the_counter_is_written_before_the_request(mods, clock, http, monkeypatch):
    """A request that never returns (a crash, a killed child) still spent a call."""
    _boxing, data = mods
    seen = []

    def get(url, params=None, headers=None, timeout=None, allow_redirects=True):
        seen.append(json.loads(Path(data.CACHE_DIR, "quota.json").read_text()))
        return FakeResp()

    stub_requests_get(monkeypatch, get)
    _refresh(mods)
    usage = seen[0]["usage"]
    assert list(usage.values()) == [{"month": "2026-11", "attempts": 1}]


def test_a_counter_that_cannot_be_saved_refuses_the_request(mods, clock, http, monkeypatch):
    calls, _ = http
    _boxing, data = mods
    monkeypatch.setattr(data, "_write", lambda *a, **k: False)
    _rows, lost = _refresh(mods)
    assert calls == [] and lost == WEEK


def test_the_monthly_quota_answer_stops_requests_until_the_reset(mods, clock, http, caplog):
    calls, queue = http
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE},
                          {"X-RateLimit-Requests-Remaining": "0",
                           "X-RateLimit-Requests-Reset": str(5 * 86400)}))
    with caplog.at_level(logging.WARNING):
        _rows, lost = _refresh(mods)
    assert lost == WEEK and len(calls) == 1
    assert "quota is used up until 2026-12-01T04:00:00+00:00" in caplog.text
    clock["now"] = NOW + timedelta(days=4, hours=23)
    _refresh(mods)
    assert len(calls) == 1
    clock["now"] = NOW + timedelta(days=5)
    _refresh(mods)
    assert len(calls) == 2


def test_a_quota_answer_without_a_reset_header_waits_for_the_next_month(mods, clock, http):
    calls, queue = http
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE}, {}))
    _refresh(mods)
    clock["now"] = datetime(2026, 11, 30, 23, 59, tzinfo=UTC)
    _refresh(mods)
    assert len(calls) == 1
    clock["now"] = datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    _refresh(mods)
    assert len(calls) == 2


def test_control_a_burst_limit_429_is_a_failure_not_a_month_long_wait(mods, clock, http):
    """The control for the quota reading: RapidAPI's per-second limit is also a
    429. Waiting a month on it would cost the whole sport."""
    calls, queue = http
    queue.append(FakeResp(429, {"message": "Too many requests"}, {"X-RateLimit-Requests-Remaining": "60"}))
    _rows, lost = _refresh(mods)
    assert lost == WEEK
    clock["now"] = NOW + timedelta(minutes=15)
    _refresh(mods)
    assert len(calls) == 2


def test_the_last_call_of_the_plan_holds_the_next_refresh(mods, clock, http):
    """An answer whose remaining count reads 0 used the plan's last call: the
    next request would be refused, so it is not sent."""
    calls, queue = http
    queue.append(FakeResp(200, _page([_card(1)]),
                          {"x-rate-limit-rapid-free-plans-hard-limit-remaining": "0",
                           "x-rate-limit-rapid-free-plans-hard-limit-reset": str(3 * 86400)}))
    rows, lost = _refresh(mods)
    assert len(rows) == 1 and lost == set()
    clock["now"] = NOW + timedelta(hours=25)
    rows, lost = _refresh(mods)
    assert len(calls) == 1 and len(rows) == 1
    # Held, so the kept schedule stands in and only the uncovered day is lost.
    assert lost == {date(2026, 12, 4)}


def test_the_wait_is_the_reset_of_the_counter_that_ran_out(mods, clock, http):
    """RapidAPI sends separate counters: the plan's (with its own short reset)
    and the free plan's monthly hard limit. The used-up one's reset decides."""
    calls, queue = http
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE}, {
        "x-ratelimit-requests-remaining": "25", "x-ratelimit-requests-reset": "30",
        "x-rate-limit-rapid-free-plans-hard-limit-remaining": "0",
        "x-rate-limit-rapid-free-plans-hard-limit-reset": str(3 * 86400)}))
    _refresh(mods)
    clock["now"] = NOW + timedelta(days=2, hours=23)
    _refresh(mods)
    assert len(calls) == 1
    clock["now"] = NOW + timedelta(days=3)
    _refresh(mods)
    assert len(calls) == 2


def test_a_used_up_counter_without_a_reset_waits_for_the_next_month(mods, clock, http):
    """Both counters at 0, only the plan's 30 s reset given: the monthly one
    still holds until its own (unknown, so next-month) reset."""
    calls, queue = http
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE}, {
        "x-ratelimit-requests-remaining": "0", "x-ratelimit-requests-reset": "30",
        "x-rate-limit-rapid-free-plans-hard-limit-remaining": "0"}))
    _refresh(mods)
    clock["now"] = datetime(2026, 11, 30, 23, 0, tzinfo=UTC)
    _refresh(mods)
    assert len(calls) == 1


def test_a_counter_with_calls_left_never_sets_the_wait(mods, clock, http):
    """A message-only quota 429 is held to the next month, not to the reset of
    a counter that still has calls left."""
    calls, queue = http
    clock["now"] = datetime(2026, 11, 29, 4, 0, tzinfo=UTC)
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE}, {
        "x-rate-limit-rapid-free-plans-hard-limit-remaining": "40",
        "x-rate-limit-rapid-free-plans-hard-limit-reset": str(20 * 86400)}))
    _refresh(mods)
    clock["now"] = datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    _refresh(mods)
    assert len(calls) == 2


def test_the_ceiling_stops_a_schedule_between_pages(mods, clock, http):
    """Page 1 takes the month's last attempt and promises a page 2: page 2 is
    not sent, and the half-read schedule is not kept as the whole one."""
    calls, queue = http
    _boxing, data = mods
    os.makedirs(data.CACHE_DIR, exist_ok=True)
    from dispatcharr_ranked_matchups._util import key_id
    Path(data.CACHE_DIR, "quota.json").write_text(json.dumps(
        {"usage": {key_id(KEY): {"month": "2026-11", "attempts": data.MONTHLY_ATTEMPTS - 1}}}))
    queue.append(FakeResp(200, _page([_card(i) for i in range(25)], 1, 2, 27)))
    rows, lost = _refresh(mods)
    assert len(calls) == 1 and len(rows) == 25 and lost == WEEK
    assert not Path(data.CACHE_DIR, "schedule.json").exists()


def test_a_redirect_is_a_failure_and_is_not_followed(mods, clock, http):
    calls, queue = http
    _boxing, data = mods
    # A valid schedule in the body: only the status can reject it.
    queue.append(FakeResp(307, _page([_card(1)]), {"Location": "https://elsewhere.example/"}))
    rows, lost = _refresh(mods)
    assert len(calls) == 1 and rows == [] and lost == WEEK
    assert not Path(data.CACHE_DIR, "schedule.json").exists()


def test_an_error_message_quoting_the_key_is_masked(mods, clock, http, caplog):
    _calls, queue = http
    queue.append(FakeResp(401, {"message": f"Invalid API key {KEY}"}, {}))
    with caplog.at_level(logging.WARNING):
        _refresh(mods)
    assert "HTTP 401" in caplog.text and KEY not in caplog.text


def test_the_wait_belongs_to_the_key_that_ran_out(mods, clock, http):
    calls, queue = http
    queue.append(FakeResp(429, {"message": QUOTA_MESSAGE}, {}))
    _refresh(mods)
    _rows, lost = _refresh(mods, key="a-new-key-pasted-after")
    assert len(calls) == 2 and lost == set()


def test_a_far_future_wait_is_ignored(mods, clock, http):
    calls, _ = http
    _boxing, data = mods
    os.makedirs(data.CACHE_DIR, exist_ok=True)
    from dispatcharr_ranked_matchups._util import key_id
    Path(data.CACHE_DIR, "quota.json").write_text(json.dumps(
        {"waits": {key_id(KEY): "2027-06-01T00:00:00+00:00"}}))
    _refresh(mods)
    assert len(calls) == 1


def test_a_failed_refresh_serves_the_kept_schedule_and_reports_only_uncovered_days(mods, clock, http):
    """#261: a stand-in that covers the window is not an outage. The day past
    its end has no answer at all, so that one day is lost."""
    calls, queue = http
    _refresh(mods)
    clock["now"] = NOW + timedelta(days=1, hours=1)
    queue.append(FakeResp(503, {"message": "upstream down"}))
    rows, lost = _refresh(mods)
    assert len(calls) == 2 and [r.home for r in rows] == ["Fighter1 vs. Rival1"]
    assert lost == {date(2026, 12, 4)}


def test_a_kept_schedule_never_republishes_a_past_card(mods, clock, http):
    calls, queue = http
    clock["now"] = datetime(2026, 11, 26, 20, 0, tzinfo=UTC)
    queue.append(FakeResp(200, _page([_card(1, "2026-11-26T21:00:00"), _card(2)], total_items=2)))
    assert len(_refresh(mods)[0]) == 2
    clock["now"] = datetime(2026, 11, 26, 23, 0, tzinfo=UTC)
    assert len(_refresh(mods)[0]) == 2                     # still the 26th: both listed
    clock["now"] = datetime(2026, 11, 27, 3, 0, tzinfo=UTC)  # kept, the 26th is over
    assert [r.home for r in _refresh(mods)[0]] == ["Fighter2 vs. Rival2"]
    assert len(calls) == 1


def test_a_kept_schedule_is_not_served_past_its_window(mods, clock, http):
    calls, queue = http
    _refresh(mods)
    clock["now"] = NOW + timedelta(days=7)
    queue.append(FakeResp(503, {}))
    rows, lost = _refresh(mods)
    assert rows == [] and lost == _all_days(date(2026, 12, 3), date(2026, 12, 10))


def test_a_partial_read_is_never_kept(mods, clock, http):
    """Page 2 failing must not leave page 1 kept as the whole schedule for 12 h."""
    calls, queue = http
    queue.extend([FakeResp(200, _page([_card(i) for i in range(25)], 1, 2, 27)),
                  FakeResp(503, {})])
    rows, lost = _refresh(mods)
    assert len(rows) == 25 and lost == WEEK
    clock["now"] = NOW + timedelta(minutes=15)
    _refresh(mods)
    assert len(calls) > 2


def test_a_different_lookahead_is_a_different_question(mods, clock, http):
    calls, _ = http
    _refresh(mods, days_ahead=7)
    _refresh(mods, days_ahead=3)
    assert len(calls) == 2


def test_the_key_goes_in_a_header_and_never_into_a_file(mods, clock, http):
    calls, _ = http
    _boxing, data = mods
    _refresh(mods)
    assert calls[0][2]["x-rapidapi-key"] == KEY
    assert calls[0][2]["x-rapidapi-host"] == data.RAPIDAPI_HOST
    for name in os.listdir(data.CACHE_DIR):
        assert KEY not in Path(data.CACHE_DIR, name).read_text()


def test_a_malformed_header_key_never_reaches_the_log(mods, clock, http, caplog):
    """`requests` echoes a bad header value verbatim in InvalidHeader."""
    import requests
    _calls, queue = http
    queue.append(requests.RequestException(
        "header value: 'SEKRITKEY99' for host boxing-data-api.p.rapidapi.com"))
    with caplog.at_level(logging.WARNING):
        _refresh(mods, key="SEKRITKEY99")
    assert "SEKRITKEY99" not in caplog.text
    assert "boxing-data-api.p.rapidapi.com" in caplog.text


def test_a_wrong_shaped_state_file_starts_over(mods, clock, http):
    calls, _ = http
    _boxing, data = mods
    os.makedirs(data.CACHE_DIR, exist_ok=True)
    Path(data.CACHE_DIR, "quota.json").write_text('["not", "an", "object"]')
    Path(data.CACHE_DIR, "schedule.json").write_text('{"v": 1, "days": 7, "events": "x"}')
    rows, _lost = _refresh(mods)
    assert len(calls) == 1 and len(rows) == 1


def test_no_other_module_calls_the_boxing_api():
    """Everything goes through `_boxing_data`, or the budget is bypassed."""
    offenders = []
    for path in sorted(ROOT.rglob("*.py")):
        if "tests" in path.parts or path.name == "_boxing_data.py":
            continue
        if re.search(r"boxing-data-api|rapidapi\.com", path.read_text(encoding="utf-8")):
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []
    # Control: the scan finds the module that does call it.
    assert "boxing-data-api" in (SOURCES / "_boxing_data.py").read_text(encoding="utf-8")
