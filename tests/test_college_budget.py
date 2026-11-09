"""The CollegeFootballData / CollegeBasketballData monthly budget (#337).

On 2026-11-09 every NCAAF and NCAAM /rankings call had answered 429 `Monthly
call quota exceeded.` since November 1. CFBD's /info: free tier, 1,000 calls a
MONTH in one pool for both APIs, all used. The sources assumed 1k a day and
asked about ten questions on each of 9 to 28 refreshes a day.
"""

import importlib
import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from tests.conftest import stub_requests_get

UTC = timezone.utc
NOW = datetime(2026, 11, 9, 22, 0, tzinfo=UTC)
KEY = "cfbd-k3y-that-must-never-be-written"
BASE = "https://api.collegefootballdata.com"
POLL = [{"week": 11, "polls": [{"poll": "AP Top 25", "ranks": [{"school": "Georgia", "rank": 1}]}]}]


@pytest.fixture
def college():
    return importlib.import_module("dispatcharr_ranked_matchups.sources._college_data")


class FakeResp:
    def __init__(self, status=200, body=None, remaining="900"):
        self.status_code = status
        self._body = POLL if body is None else body
        self.headers = {"X-CallLimit-Remaining": remaining} if remaining is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code), response=self)

    def json(self):
        return self._body


QUOTA = FakeResp(429, {"message": "Monthly call quota exceeded."}, remaining="0")


@pytest.fixture
def http(college, monkeypatch):
    """Records every request; `queue` holds the answers to hand out."""
    calls, queue = [], []

    def get(url, params=None, headers=None, timeout=None):
        calls.append((url, dict(params or {}), dict(headers or {})))
        answer = queue.pop(0) if queue else FakeResp()
        if isinstance(answer, Exception):
            raise answer
        return answer

    stub_requests_get(monkeypatch, get)
    return calls, queue


def _ask(college, now=NOW, key=KEY, path="/rankings", params=None, stale_for=None):
    kw = {} if stale_for is None else {"stale_for": stale_for}
    return college.get_json(BASE, path, params or {"year": 2026}, key, timeout=15, log_tag="t",
                            valid=lambda d: isinstance(d, list), expect="a list", now=now, **kw)


def test_an_answer_is_kept_for_twelve_hours(college, http):
    calls, _ = http
    assert _ask(college) == POLL
    assert _ask(college, now=NOW + timedelta(hours=11, minutes=59)) == POLL
    assert len(calls) == 1
    _ask(college, now=NOW + timedelta(hours=12))
    assert len(calls) == 2


def test_the_key_goes_in_a_header_and_never_into_a_file(college, http):
    calls, queue = http
    _ask(college)
    queue.append(QUOTA)
    with pytest.raises(college.CollegeDataUnavailable):
        _ask(college, path="/games")
    (_url, params, headers), _ = calls
    assert headers == {"Authorization": f"Bearer {KEY}"} and KEY not in json.dumps(params)
    for name in os.listdir(college.CACHE_DIR):
        raw = open(os.path.join(college.CACHE_DIR, name), "rb").read()
        assert KEY.encode() not in raw, name


def test_the_used_up_month_stops_every_question_until_the_reset(college, http, caplog):
    calls, queue = http
    queue.append(QUOTA)
    with pytest.raises(college.CollegeDataUnavailable, match="used up until 2026-12-01T00:00:00"):
        _ask(college)
    assert "1,000 calls a month" in caplog.text
    for later in (NOW + timedelta(hours=1), NOW + timedelta(days=10)):
        with pytest.raises(college.CollegeDataUnavailable, match="not asked"):
            _ask(college, now=later, path="/games")
    assert len(calls) == 1
    _ask(college, now=datetime(2026, 12, 1, 0, 0, 1, tzinfo=UTC))   # the new month asks again
    assert len(calls) == 2


def test_the_wait_belongs_to_the_key_that_ran_out(college, http):
    calls, queue = http
    queue.append(QUOTA)
    with pytest.raises(college.CollegeDataUnavailable):
        _ask(college)
    assert _ask(college, key="a-new-key", now=NOW + timedelta(minutes=1)) == POLL
    assert len(calls) == 2


def test_control_another_429_is_a_failure_not_a_month_long_wait(college, http):
    calls, queue = http
    queue.append(FakeResp(429, {"message": "Too many requests, slow down"}))
    with pytest.raises(college.CollegeDataUnavailable, match="HTTP 429"):
        _ask(college)
    _ask(college, now=NOW + timedelta(minutes=1))
    assert len(calls) == 2


def test_a_far_future_wait_is_ignored(college, http):
    calls, _ = http
    os.makedirs(college.CACHE_DIR, exist_ok=True)
    key_id = importlib.import_module("dispatcharr_ranked_matchups._util").key_id
    json.dump({"exhausted_until": "2099-01-01T00:00:00+00:00", "exhausted_key": key_id(KEY)},
              open(os.path.join(college.CACHE_DIR, "quota.json"), "w"))
    _ask(college)
    assert len(calls) == 1


def test_an_answer_clears_the_wait_and_records_the_calls_left(college, http):
    _calls, queue = http
    queue.append(QUOTA)
    with pytest.raises(college.CollegeDataUnavailable):
        _ask(college)
    _ask(college, key="a-new-key", now=NOW + timedelta(minutes=1))
    quota = json.load(open(os.path.join(college.CACHE_DIR, "quota.json")))
    assert "exhausted_until" not in quota and quota["calls_remaining"] == "900"


def test_a_kept_poll_stands_in_for_a_week_and_no_longer(college, http):
    """The poll is published weekly: last week's ranks new games, where failing
    would hold the label and add none (#240)."""
    import requests
    calls, queue = http
    _ask(college, stale_for=college.POLL_STANDS_IN)
    queue.append(requests.ConnectionError("refused"))
    assert _ask(college, now=NOW + timedelta(days=7), stale_for=college.POLL_STANDS_IN) == POLL
    queue.append(QUOTA)
    assert _ask(college, now=NOW + timedelta(days=7, hours=1), stale_for=college.POLL_STANDS_IN) == POLL
    with pytest.raises(college.CollegeDataUnavailable):
        _ask(college, now=NOW + timedelta(days=8, hours=1), stale_for=college.POLL_STANDS_IN)


def test_control_without_a_stand_in_a_failure_raises_however_recent_the_answer(college, http):
    """Season reads, windows and lines have fallbacks of their own (the season
    store, the lost-day hold): the cache never answers for them after a failure."""
    import requests
    calls, queue = http
    _ask(college, path="/games")
    queue.append(requests.ConnectionError("refused"))
    with pytest.raises(college.CollegeDataUnavailable, match="request failed"):
        _ask(college, path="/games", now=NOW + timedelta(hours=13))


def test_a_wrong_shape_is_a_failure_and_is_not_kept(college, http):
    calls, queue = http
    queue.append(FakeResp(200, {"message": "Unauthorized"}))
    with pytest.raises(college.CollegeDataUnavailable, match="not a list"):
        _ask(college)
    assert _ask(college, now=NOW + timedelta(minutes=1)) == POLL   # asked again, not replayed
    assert len(calls) == 2


@pytest.mark.parametrize("now,reset", [
    (datetime(2026, 11, 9, 22, 0, tzinfo=UTC), datetime(2026, 12, 1, tzinfo=UTC)),
    (datetime(2026, 12, 31, 23, 59, tzinfo=UTC), datetime(2027, 1, 1, tzinfo=UTC)),
    (datetime(2027, 1, 31, 12, 0, tzinfo=UTC), datetime(2027, 2, 1, tzinfo=UTC)),
])
def test_the_reset_is_the_first_of_the_next_month(college, now, reset):
    assert college._next_reset(now) == reset


def test_a_refresh_day_fits_the_month(college, http, monkeypatch):
    """The budget the module docstring promises, measured through the real
    sources: twelve refreshes in a day ask each question at most twice."""
    calls, _ = http
    ncaaf = importlib.import_module("dispatcharr_ranked_matchups.sources.ncaaf")
    clock = {"now": NOW}
    monkeypatch.setattr(college, "_now", lambda: clock["now"])
    monkeypatch.setattr(ncaaf.NcaafSource, "_current_season_year", staticmethod(lambda: 2026))
    for hour in range(0, 24, 2):
        clock["now"] = NOW + timedelta(hours=hour)
        ncaaf.NcaafSource(api_key=KEY)._fetch_rankings(2026)
        ncaaf.NcaafSource(api_key=KEY)._fetch_raw_season(2026)
    per_question = {}
    for url, params, _h in calls:
        per_question[(url, tuple(sorted(params.items())))] = per_question.get(
            (url, tuple(sorted(params.items()))), 0) + 1
    assert len(per_question) == 2 and max(per_question.values()) == 2


def test_no_other_module_calls_the_college_apis_directly():
    """Every CFBD/CBBD request goes through `_college_data`, or a new call site
    spends the month's pool outside the budget."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    offenders = []
    for dirpath, dirnames, files in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in (".git", "tests", "__pycache__", "tools")]
        for name in files:
            if name.endswith(".py") and name != "_college_data.py":
                text = open(os.path.join(dirpath, name), encoding="utf-8").read()
                if ("collegefootballdata.com" in text or "collegebasketballdata.com" in text) and (
                        "get_within_deadline" in text or "import requests" in text):
                    offenders.append(name)
    assert offenders == []
