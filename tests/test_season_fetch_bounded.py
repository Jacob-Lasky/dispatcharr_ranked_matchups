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
        entry = plugin._source_error_entry("ELC season data (importance scored 0)", "429")
        (problem,) = plugin._slate_problems([entry], [])
        assert "1 source(s) failed to fetch (ELC season data (importance scored 0))" in problem
