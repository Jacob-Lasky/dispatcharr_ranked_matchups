"""The Odds API credit budget (#200).

On 2026-10-10 every odds call answered 401 `OUT_OF_USAGE_CREDITS`: 499 of the
free plan's 500 monthly credits were spent, because each refresh asked every
league for three regions (3 credits a call). The log said "401 Unauthorized"
and two issues blamed the key, which was valid all along.
"""

import importlib
import logging
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc
NOW = datetime(2026, 10, 11, 3, 0, tzinfo=UTC)
EVENTS = [{"home_team": "Liverpool", "away_team": "Man City", "bookmakers": []}]
KEY = "k3y-that-must-never-be-logged"


@pytest.fixture
def odds():
    return importlib.import_module("dispatcharr_ranked_matchups.sources._odds")


class FakeResp:
    def __init__(self, status=200, body=None, remaining="400"):
        self.status_code = status
        self._body = EVENTS if body is None else body
        self.headers = {"x-requests-remaining": remaining}

    def json(self):
        return self._body


@pytest.fixture
def http(odds, monkeypatch):
    """Records every request; `queue` holds the responses to hand out."""
    calls, queue = [], []

    def get(url, params=None, timeout=None):
        calls.append((url, dict(params or {})))
        return queue.pop(0) if queue else FakeResp()

    monkeypatch.setattr(odds.requests, "get", get)
    return calls, queue


def _fetch(odds, now=NOW, sport="soccer_epl"):
    return odds.fetch_odds_events(sport, KEY, "eu", log_tag="t", now=now)


def test_one_region_per_request(odds, http):
    calls, _ = http
    assert _fetch(odds) == EVENTS
    (_url, params), = calls
    assert params["regions"] == "eu"  # one region = one credit
    assert params["markets"] == "h2h"


def test_second_refresh_within_a_day_spends_nothing(odds, http):
    calls, _ = http
    _fetch(odds)
    assert _fetch(odds, now=NOW + timedelta(hours=23)) == EVENTS
    assert len(calls) == 1


def test_refetches_after_a_day(odds, http):
    calls, _ = http
    _fetch(odds)
    _fetch(odds, now=NOW + timedelta(hours=25))
    assert len(calls) == 2


def test_each_sport_is_cached_separately(odds, http):
    calls, _ = http
    _fetch(odds, sport="soccer_epl")
    _fetch(odds, sport="soccer_spain_la_liga")
    assert len(calls) == 2


def test_out_of_credits_stops_requests_and_says_so(odds, http, caplog):
    calls, queue = http
    _fetch(odds)  # cache a line
    queue.append(FakeResp(401, {"error_code": "OUT_OF_USAGE_CREDITS"}, remaining="0"))
    later = NOW + timedelta(hours=25)
    with caplog.at_level(logging.WARNING):
        # Stale line served rather than nothing.
        assert _fetch(odds, now=later) == EVENTS
    assert "credits are used up" in caplog.text
    assert "The key itself is fine" in caplog.text
    # Every league after that, for the rest of the backoff, asks nothing.
    _fetch(odds, now=later + timedelta(minutes=5), sport="soccer_italy_serie_a")
    _fetch(odds, now=later + timedelta(hours=23), sport="soccer_epl")
    assert len(calls) == 2
    # And tries again once the backoff has passed.
    _fetch(odds, now=later + timedelta(hours=25))
    assert len(calls) == 3


def test_invalid_key_is_reported_as_a_key_problem(odds, http, caplog):
    _calls, queue = http
    queue.append(FakeResp(401, {"error_code": "INVALID_KEY"}))
    with caplog.at_level(logging.WARNING):
        assert _fetch(odds) is None
    assert "rejected the key" in caplog.text
    assert "credits" not in caplog.text


def test_network_error_serves_the_cached_line_and_redacts_the_key(odds, http, monkeypatch, caplog):
    _fetch(odds)

    def boom(url, params=None, timeout=None):
        raise odds.requests.ConnectionError(f"failed {url}?apiKey={KEY}")

    monkeypatch.setattr(odds.requests, "get", boom)
    with caplog.at_level(logging.WARNING):
        assert _fetch(odds, now=NOW + timedelta(hours=30)) == EVENTS
    assert KEY not in caplog.text


def test_no_key_makes_no_request(odds, http):
    calls, _ = http
    assert odds.fetch_odds_events("soccer_epl", "", "eu", log_tag="t", now=NOW) is None
    assert calls == []


def test_callers_go_through_the_shared_fetcher():
    """The budget only holds if nothing calls the Odds API around it."""
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    offenders = []
    for dirpath, _dirs, files in os.walk(root):
        if "tests" in dirpath.split(os.sep) or "__pycache__" in dirpath:
            continue
        for f in files:
            if not f.endswith(".py") or f == "_odds.py":
                continue
            path = os.path.join(dirpath, f)
            if "the-odds-api.com" in open(path, encoding="utf-8").read():
                offenders.append(os.path.relpath(path, root))
    assert offenders == []


@pytest.mark.parametrize("module, build", [
    ("soccer", lambda m: m.SoccerSource("epl", fd_api_key="fake", odds_api_key="k")),
    ("mls", lambda m: m.MlsSource(odds_api_key="k")),
    ("mls_standings", lambda m: m.MlsEastSource(odds_api_key="k")),
])
def test_every_caller_asks_for_exactly_one_region(module, build, monkeypatch):
    """A comma in `regions` multiplies the cost of every request; this is the
    regression that spent the 2026-10 allowance."""
    mod = importlib.import_module(f"dispatcharr_ranked_matchups.sources.{module}")
    seen = []

    def fake(sport_key, api_key, region, *, log_tag, now=None):
        seen.append(region)
        return []

    monkeypatch.setattr(mod, "fetch_odds_events", fake)
    build(mod)._fetch_closeness()
    assert len(seen) == 1 and "," not in seen[0] and seen[0]
