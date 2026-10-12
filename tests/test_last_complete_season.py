"""A failed season read simulates the last COMPLETE read, if recent (#289
follow-up).

#289 stopped simulating a season with holes in it, but a failed read then
scored every game of the league with no standings stakes for that refresh,
so its channels could fall out of the top N and be reaped, then come back:
churn. Each complete read is now kept (`sources/_season_store`, gzipped JSON
per season beside cache.json), and a failed read no older than
`_season_store.MAX_AGE` (36 h) behind it simulates that read instead,
reported as "<label> season (using the last complete read from <time>)".
With no usable read it is still not simulated, as in #289.

Times are pinned through `_season_store._now`, and expected values are
literals.
"""
from __future__ import annotations

import gzip
import importlib
import json
import os
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.conftest import stub_requests_get
from tests.test_season_walk_failures import _EVERY_SEASON, _answer, _event, _finished

UTC = timezone.utc
PKG = "dispatcharr_ranked_matchups.sources"
T0 = datetime(2026, 10, 12, 4, 0, tzinfo=UTC)


@pytest.fixture
def store(monkeypatch):
    mod = importlib.import_module(f"{PKG}._season_store")
    clock = {"now": T0}
    monkeypatch.setattr(mod, "_now", lambda: clock["now"])
    mod.clock = clock
    return mod


def _nba(monkeypatch, *, dead_day=None, games_on=("20261105",)):
    """An NBA 2026-27 season with one finished game on each day in
    `games_on`, and `dead_day` failing every time it is asked."""
    nba = importlib.import_module(f"{PKG}.nba")

    def http_get(url, *a, **k):
        day = url.split("dates=")[1][:8]
        if day == dead_day:
            return None
        if day in games_on:
            d = date(int(day[:4]), int(day[4:6]), int(day[6:]))
            return {"events": [_finished(f"g{day}", "Boston", "Denver", d)]}
        return {"events": []}

    monkeypatch.setattr(nba, "_http_get", http_get)
    return nba.NbaRegularSource(season_end_year=2027)


def _ids(games):
    return sorted(g["id"] for g in games)


def test_a_failed_read_simulates_the_last_complete_one(monkeypatch, store):
    """WITNESS: the 04:00 refresh read the season whole; at 10:00 the walk
    lost 2026-12-25. #289 left the season unsimulated ([]), every NBA game
    on no standings stakes. Now the 04:00 read stands in, exactly as read."""
    first = _nba(monkeypatch)
    read = first._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=6)
    later = _nba(monkeypatch, dead_day="20261225")
    games = later._fetch_games_cached()
    assert games == read and _ids(games) == ["g20261105"]
    assert games[0]["start_time"] == datetime(2026, 11, 5, 23, 0, tzinfo=UTC)
    assert later.season_fetch_failure() == (
        "no answer for 1 day(s) of the season (2026-12-25..2026-12-25)")
    assert later.season_fallback() == T0


@pytest.mark.parametrize("hours,used", [(35, True), (37, False)])
def test_a_read_older_than_36_hours_is_not_used(monkeypatch, store, hours, used):
    """WITNESS (37 h) and guard (35 h): past MAX_AGE the last read is too
    old to stand in, and the season is not simulated, as in #289."""
    _nba(monkeypatch)._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=hours)
    later = _nba(monkeypatch, dead_day="20261225")
    assert (later._fetch_games_cached() != []) is used
    assert (later.season_fallback() is not None) is used


def test_a_partial_read_never_replaces_a_complete_one(monkeypatch, store):
    """WITNESS: 05:00's read lost a day and held a game 04:00's did not; it
    must not become the read a later failure falls back to."""
    _nba(monkeypatch)._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=1)
    _nba(monkeypatch, dead_day="20261225", games_on=("20261105", "20261106"))._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=2)
    later = _nba(monkeypatch, dead_day="20261225")
    assert _ids(later._fetch_games_cached()) == ["g20261105"]
    assert later.season_fallback() == T0


def test_a_complete_read_replaces_the_last_one(monkeypatch, store):
    """WITNESS: a newer complete read is the one a failure falls back to."""
    _nba(monkeypatch)._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=6)
    _nba(monkeypatch, games_on=("20261105", "20261106"))._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=7)
    later = _nba(monkeypatch, dead_day="20261225")
    assert _ids(later._fetch_games_cached()) == ["g20261105", "g20261106"]
    assert later.season_fallback() == T0 + timedelta(hours=6)


def test_another_season_does_not_stand_in(monkeypatch, store):
    """Guard: the key names the season, so a failed 2027-28 read never
    simulates 2026-27's games."""
    _nba(monkeypatch)._fetch_games_cached()
    nba = importlib.import_module(f"{PKG}.nba")
    monkeypatch.setattr(nba, "_http_get", lambda url, *a, **k: None)
    nxt = nba.NbaRegularSource(season_end_year=2028)
    assert nxt._fetch_games_cached() == []
    assert nxt.season_fallback() is None


def test_a_read_that_crosses_a_season_boundary_is_kept_under_its_own_season(monkeypatch, store):
    """WITNESS (Codex, round 1): NCAAF and NCAAM pick their season from the
    clock, so a read running across their boundary (Aug 1 / Nov 1 UTC) read
    one season while a key taken AFTER the read named the next one, and a
    later failure in that next season recalled the wrong games. The key is
    taken before the read. Modelled on NBA by moving its season mid-read."""
    src = _nba(monkeypatch)
    old_key = src.season_store_key()
    real = src._fetch_full_season_games

    def crossing():
        games = real()
        src.season_end_year = 2028   # the clock moved on while reading
        return games

    monkeypatch.setattr(src, "_fetch_full_season_games", crossing)
    src._fetch_games_cached()
    assert store.recall(old_key) is not None
    assert store.recall(src.season_store_key()) is None


def test_no_prior_read_is_not_simulated(monkeypatch, store):
    """Guard: the #289 behaviour when there is nothing to fall back to."""
    src = _nba(monkeypatch, dead_day="20261225")
    assert src._fetch_games_cached() == []
    assert src.season_fallback() is None and src.season_fetch_failure() is not None


def test_ncaaf_eligibility_survives_the_fallback(monkeypatch, store):
    """WITNESS: NCAAF's win-count bands are restricted to the selected
    divisions' teams (`outcome_eligible_teams`). That read CFBD's raw season,
    which a failed read does not have; it now reads the population, so the
    restriction holds when the last complete read stands in."""
    import requests
    ncaaf = importlib.import_module(f"{PKG}.ncaaf")
    rows = [{"id": 1, "homeTeam": "Georgia", "awayTeam": "Samford", "homeClassification": "fbs",
             "awayClassification": "fcs", "homePoints": 45, "awayPoints": 3, "completed": True,
             "startDate": "2026-09-06T23:00:00.000Z"}]
    answers = {"ok": True}

    def get(url, **k):
        if answers["ok"]:
            from tests.test_hold_failed_days import _Resp
            return _Resp(rows)
        raise requests.ConnectionError("refused")

    stub_requests_get(monkeypatch, get)
    monkeypatch.setattr(ncaaf.NcaafSource, "_current_season_year", staticmethod(lambda: 2026))
    first = ncaaf.NcaafSource(api_key="k", divisions="fbs")
    first._fetch_games_cached()   # the complete read, kept
    assert first.outcome_eligible_teams() == frozenset({"Georgia"})
    answers["ok"] = False
    store.clock["now"] = T0 + timedelta(hours=6)
    later = ncaaf.NcaafSource(api_key="k", divisions="fbs")
    assert [g["home"] for g in later._fetch_games_cached()] == ["Georgia"]   # the kept read
    assert later.season_fallback() is not None
    assert later.outcome_eligible_teams() == frozenset({"Georgia"})


# ---------------------------------------------------------------------------
# The store itself: fidelity, bounds, trust
# ---------------------------------------------------------------------------

def test_a_season_round_trips_exactly(store):
    """Guard: what is recalled is what was read, datetimes and nested extras
    included (NHL keeps `last_period_type`, NCAA baseball a dict)."""
    games = [{"id": 7, "home": "A", "away": "B", "home_points": 3, "away_points": 2,
              "status": "FINISHED", "start_time": datetime(2026, 10, 1, 23, 0, tzinfo=UTC),
              "extra": {"last_period_type": "OT", "nested": [1, {"x": None}]}},
             {"id": 8, "home": "C", "away": "D", "home_points": None, "away_points": None,
              "status": "SCHEDULED", "start_time": None, "extra": {}}]
    store.remember("K|L|2027", games)
    assert store.recall("K|L|2027").games == games


@pytest.mark.parametrize("damage", ["garbage", "other-key", "other-version", "future"])
def test_an_untrustworthy_file_is_ignored(store, damage):
    """Guard: a file that is unreadable, for another key, from another
    version, or dated in the future (a clock that went backwards) is never
    recalled."""
    store.remember("K|L|2027", [])
    path = store._path("K|L|2027")
    if damage == "garbage":
        with open(path, "wb") as f:
            f.write(b"not gzip")
    else:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        if damage == "other-key":
            data["key"] = "K|L|2026"
        elif damage == "other-version":
            data["v"] = 99
        else:
            data["read_at"] = (T0 + timedelta(hours=2)).isoformat()
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f)
    assert store.recall("K|L|2027") is None


def test_the_store_is_bounded(store):
    """Guard: one file per key, rewritten in place, and a file older than
    MAX_AGE deleted when another is written."""
    store.remember("K|L|2027", [])
    store.remember("K|L|2027", [])
    old = store._path("K|OLD|2026")
    store.remember("K|OLD|2026", [])
    stale = (datetime.now(UTC) - timedelta(hours=37)).timestamp()
    os.utime(old, (stale, stale))
    store.remember("K|L|2027", [])
    assert sorted(os.listdir(store.STORE_DIR)) == [os.path.basename(store._path("K|L|2027"))]


def test_a_write_failure_does_not_fail_the_read(monkeypatch, store):
    """Guard: a season that cannot be stored is logged; the read stands."""
    monkeypatch.setattr(store, "STORE_DIR", "/proc/no-such-dir/season_cache")
    src = _nba(monkeypatch)
    assert _ids(src._fetch_games_cached()) == ["g20261105"]


# ---------------------------------------------------------------------------
# The refresh reports it
# ---------------------------------------------------------------------------

def test_refresh_reports_the_read_it_simulated(monkeypatch, refresh_env, store):
    """WITNESS: the summary says the stakes came from the last complete
    read, and when, as an informational entry rather than an error."""
    _nba(monkeypatch)._fetch_games_cached()
    store.clock["now"] = T0 + timedelta(hours=6)
    nba = importlib.import_module(f"{PKG}.nba")
    tomorrow = (datetime.now(UTC) + timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
    game = _event("t1", "Boston", "Denver", tomorrow, "pre")
    monkeypatch.setattr(nba, "_http_get", lambda url, *a, **k: (
        None if "dates=20261225" in url
        else {"events": [game]} if f"dates={tomorrow.strftime('%Y%m%d')}" in url
        else {"events": []}))
    _write_prior, run, read = refresh_env
    run([nba.NbaRegularSource(season_end_year=2027)], weight_importance=1)
    summary = read()["summary"]
    assert [s for s in summary if "season (" in s] == [
        "NBA season (using the last complete read from 2026-10-12 04:00Z)"]


# ---------------------------------------------------------------------------
# Class guard: every built points-based source keeps and recalls its season
# ---------------------------------------------------------------------------

def _build(plugin, monkeypatch, get):
    from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource
    from tests.test_mapped_prefixes import _all_on_settings
    stub_requests_get(monkeypatch, get)
    return [s for s in plugin._build_sources(_all_on_settings())
            if isinstance(s, PointsBasedSportSource)]


def test_every_points_based_source_falls_back_to_its_last_complete_read(plugin, monkeypatch, store):
    """CLASS guard. Every points-based source `_build_sources` builds reads
    its season whole (each answered with its own distinct game, carrying a
    datetime and an extra, through its instance's season fetch), then a
    refresh 6 h later builds them again with every request failing. Each
    must simulate exactly the read it kept: a distinct key naming its season,
    remembered on success, recalled on failure. A new source that forgets to
    name its season, a store keyed so two sources collide, or a recall that
    returns another source's games fails here."""
    import requests
    first = _build(plugin, monkeypatch, lambda url, **k: _answer(url, k.get("params") or {}))
    assert {type(s).__name__ for s in first} == _EVERY_SEASON
    keys = [s.season_store_key() for s in first]
    assert None not in keys and len(set(keys)) == len(keys)
    expected = {}
    for s in first:
        game = {"id": s.season_store_key(), "home": "A", "away": "B", "home_points": 1,
                "away_points": 0, "status": "FINISHED",
                "start_time": datetime(2026, 10, 1, 23, 0, tzinfo=UTC), "extra": {"k": s.sport_label}}
        s._all_games_cache = None
        s._season_failure = None
        monkeypatch.setattr(s, "_fetch_full_season_games", lambda game=game: [game])
        assert s._fetch_games_cached() == [game]
        expected[s.season_store_key()] = [game]

    def refuse(url, **_k):
        raise requests.ConnectionError("every request fails in this test")

    store.clock["now"] = T0 + timedelta(hours=6)
    later = _build(plugin, monkeypatch, refuse)
    wrong = {}
    for s in later:
        got = s._fetch_games_cached()
        if s.season_fetch_failure() is None or s.season_fallback() != T0 or got != expected[s.season_store_key()]:
            wrong[s.season_store_key()] = got
    assert wrong == {}


def test_a_label_split_between_a_kept_read_and_none_is_one_error_entry(plugin):
    """Guard (Codex, round 1): MLS East fell back, West had no kept read;
    the shared label is one entry, an error, naming both."""
    class _Src:
        sport_label = "MLS"

        def __init__(self, failure, read_at):
            self._f, self._r = failure, read_at

        def season_fetch_failure(self):
            return self._f

        def season_fallback(self):
            return self._r

    east, west = _Src("lost a day", T0), _Src("map unavailable", None)
    assert plugin._season_failure_entries([east, west], [east, west]) == [
        "MLS season (stakes not simulated): error (map unavailable; the rest used "
        "the last complete read from 2026-10-12 04:00Z)"
    ]
    assert plugin._season_failure_entries([east], [east]) == [
        "MLS season (using the last complete read from 2026-10-12 04:00Z)"]
