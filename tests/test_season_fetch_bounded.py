"""A failed full-season fetch is retried a bounded number of times per
refresh, and reported, instead of once per simulated season (#228).

The importance simulator reads the season list inside its per-season loop.
The fetcher deliberately does not cache a failure as an empty season (that
would silently zero importance), so before this fix a single transient
football-data.org error was re-fetched, with backoff sleeps, on every
simulated season: measured offline at 28 s per season.
"""
import inspect

import pytest

from dispatcharr_ranked_matchups.sources import soccer


@pytest.fixture(autouse=True)
def _fresh_caches():
    soccer._clear_fd_caches()
    yield
    soccer._clear_fd_caches()


def _failing(calls):
    def fake(url, key, **kw):
        calls.append(url)
        raise ConnectionError("SSLEOFError: EOF occurred in violation of protocol")
    return fake


class TestBoundedRetry:
    def test_repeated_reads_stop_hitting_the_network(self, monkeypatch):
        calls = []
        monkeypatch.setattr(soccer, "_fd_get", _failing(calls))
        for _ in range(50):  # one read per simulated season
            assert soccer._fetch_season_matches_for_code("key", "ELC") == []
        assert len(calls) == 2  # literal on purpose: the bound, not 50

    def test_given_up_competition_is_reported(self, monkeypatch):
        monkeypatch.setattr(soccer, "_fd_get", _failing([]))
        soccer._fetch_season_matches_for_code("key", "ELC")
        assert soccer.season_fetch_failures() == {}  # one failure: still retryable
        soccer._fetch_season_matches_for_code("key", "ELC")
        assert set(soccer.season_fetch_failures()) == {"ELC"}
        assert "SSLEOFError" in soccer.season_fetch_failures()["ELC"]

    def test_a_retry_that_succeeds_is_cached_and_not_reported(self, monkeypatch):
        calls = []

        class Resp:
            def json(self):
                return {"matches": [{"id": 1}]}

        def flaky(url, key, **kw):
            calls.append(url)
            if len(calls) == 1:
                raise ConnectionError("429")
            return Resp()
        monkeypatch.setattr(soccer, "_fd_get", flaky)
        assert soccer._fetch_season_matches_for_code("key", "ELC") == []
        assert soccer._fetch_season_matches_for_code("key", "ELC") == [{"id": 1}]
        assert soccer._fetch_season_matches_for_code("key", "ELC") == [{"id": 1}]
        assert len(calls) == 2
        assert soccer.season_fetch_failures() == {}

    def test_failures_reset_each_refresh(self, monkeypatch):
        monkeypatch.setattr(soccer, "_fd_get", _failing([]))
        soccer._fetch_season_matches_for_code("key", "ELC")
        soccer._fetch_season_matches_for_code("key", "ELC")
        soccer._clear_fd_caches()
        assert soccer.season_fetch_failures() == {}

    def test_other_competitions_are_independent(self, monkeypatch):
        calls = []
        monkeypatch.setattr(soccer, "_fd_get", _failing(calls))
        for _ in range(5):
            soccer._fetch_season_matches_for_code("key", "ELC")
            soccer._fetch_season_matches_for_code("key", "BL1")
        assert len(calls) == 4


class TestRefreshReportsIt:
    def test_failures_join_the_summary_before_the_cache_write(self, plugin):
        src = inspect.getsource(plugin._action_refresh)
        report = src.index("_soccer_season_fetch_failures()")
        write = src.index("_write_cache(cache)")
        assert report < write, "reported after the cache write, Show current state would never see it"

    def test_reported_entry_reads_as_a_failed_source(self, plugin):
        entry = plugin._source_error_entry("ELC season fixtures (stakes not simulated)", "429")
        (problem,) = plugin._slate_problems([entry], [])
        assert "1 source(s) failed to fetch (ELC season fixtures (stakes not simulated))" in problem


class TestScoringGetsItsRetry:
    def test_early_reads_cannot_spend_the_scoring_retry(self, monkeypatch):
        # World Cup group + knockout sources share an fd_code; both early
        # head-to-head reads can fail. Scoring must still get one attempt.
        calls = []
        monkeypatch.setattr(soccer, "_fd_get", _failing(calls))
        soccer._fetch_season_matches_for_code("key", "WC")   # knockout source's early read
        soccer._fetch_season_matches_for_code("key", "WC")   # group source's early read
        assert len(calls) == 2
        soccer.allow_scoring_retry()

        class Resp:
            def json(self):
                return {"matches": [{"id": 7}]}
        monkeypatch.setattr(soccer, "_fd_get", lambda url, key, **kw: (calls.append(url), Resp())[1])
        assert soccer._fetch_season_matches_for_code("key", "WC") == [{"id": 7}]
        assert len(calls) == 3
        assert soccer.season_fetch_failures() == {}

    def test_the_scoring_retry_is_exactly_one(self, monkeypatch):
        calls = []
        monkeypatch.setattr(soccer, "_fd_get", _failing(calls))
        for _ in range(3):
            soccer._fetch_season_matches_for_code("key", "WC")
        soccer.allow_scoring_retry()
        for _ in range(40):
            soccer._fetch_season_matches_for_code("key", "WC")
        assert len(calls) == 3   # two early, ONE at scoring, none per season
        assert set(soccer.season_fetch_failures()) == {"WC"}

    def test_refresh_grants_it_before_the_scoring_loop(self, plugin):
        src = inspect.getsource(plugin._action_refresh)
        assert src.index("_soccer_allow_scoring_retry()") < src.index("for done_n, (g, src) in enumerate(")
