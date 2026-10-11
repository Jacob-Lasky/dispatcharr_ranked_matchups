"""A per-day sweep that lost a day must say so, and the refresh must hold the
games it had published for that day (#261).

The failure: ESPN's per-day `fetch_upcoming` sweeps (and NHL's schedule walk)
skip a day whose request failed after its one blip retry. The sweep still
returns the other days, so its label is not empty, and #240's hold, which
treats only an EMPTY label as an outage, did not keep the lost day's games.
The refresh dropped them and apply deleted their channels, the same failure
#240 fixed for a source that failed entirely.

Assertions use literal dates, labels and summary text rather than the
plugin's constants, so a constant that drifted fails here instead of moving
with the test.
"""
from __future__ import annotations

import ast
import importlib
import re
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from tests.conftest import stub_requests_get

UTC = timezone.utc
SOURCES = Path(__file__).resolve().parent.parent / "sources"
PKG = "dispatcharr_ranked_matchups.sources"


def _walk(dead, start=date(2026, 10, 10), end=date(2026, 10, 17)):
    """Walk `start..end` with the day offsets in `dead` failing every time,
    inside a lost-day record. Returns (pages, recorded days)."""
    from dispatcharr_ranked_matchups.sources import _espn
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days

    def http_get(url):
        day = datetime.strptime(url.split("dates=")[1][:8], "%Y%m%d").date()
        if (day - start).days in dead:
            return None
        return {"events": []}

    with recording_lost_days() as lost:
        pages = _espn.fetch_scoreboard_days(http_get, "https://x.test/scoreboard", start, end)
    return pages, lost


# ---------------------------------------------------------------------------
# The shared ESPN walk reports what it lost
# ---------------------------------------------------------------------------

class TestWalkReportsLostDays:
    def test_a_day_still_failing_after_its_retry_is_reported(self):
        """WITNESS: one day of an 8-day upcoming sweep fails twice (its blip
        retry too). The walk returns 8 pages, one of them None; before #261
        nothing recorded that the None was a failure, not a quiet day."""
        pages, lost = _walk(dead={3})
        assert len(pages) == 8 and pages[3] == (date(2026, 10, 13), None)
        assert lost == {date(2026, 10, 13)}

    def test_a_quiet_day_is_not_lost(self):
        """Guard: a day that answered with no events is a real empty day."""
        _pages, lost = _walk(dead=set())
        assert lost == set()

    def test_days_an_abandoned_walk_never_requested_are_lost(self, monkeypatch):
        """The hang rule stops a 40-day walk after its first chunk (16 days,
        all failed, "slow" by a zero threshold). The 24 days never requested
        are as missing as the 16 that failed."""
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_SLOW_CHUNK_SECONDS", 0.0)
        pages, lost = _walk(dead=set(range(40)), end=date(2026, 10, 10) + timedelta(days=39))
        assert len(pages) == 16
        assert len(lost) == 40
        assert min(lost) == date(2026, 10, 10) and max(lost) == date(2026, 11, 18)

    def test_outside_a_record_reporting_is_a_no_op(self):
        """Full-season walks run during scoring, outside any record."""
        from dispatcharr_ranked_matchups.sources import _espn
        pages = _espn.fetch_scoreboard_days(
            lambda url: None, "https://x.test/scoreboard", date(2026, 10, 10), date(2026, 10, 10),
        )
        assert pages == [(date(2026, 10, 10), None)]

    def test_records_do_not_leak_between_sources(self):
        """The refresh opens one record per source; a day lost by the first
        must not land on the second."""
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days, report_lost_days
        with recording_lost_days() as first:
            report_lost_days([date(2026, 10, 13)])
        with recording_lost_days() as second:
            pass
        assert first == {date(2026, 10, 13)} and second == set()


def test_a_source_sweep_reports_through_its_own_fetch_upcoming(monkeypatch):
    """End to end on one real source: NBA's fetch_upcoming walks through the
    shared walk, and a day that keeps failing comes out of the record."""
    nba = importlib.import_module(f"{PKG}.nba")
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    today = datetime.now(UTC).date()
    lost_day = today + timedelta(days=2)
    seen = []

    def http_get(url, *a, **k):
        seen.append(url)
        if f"dates={lost_day.strftime('%Y%m%d')}" in url:
            return None
        return {"events": []}

    monkeypatch.setattr(nba, "_http_get", http_get)
    with recording_lost_days() as lost:
        nba.NbaRegularSource(season_end_year=2027).fetch_upcoming(days_ahead=7)
    assert any("scoreboard" in u for u in seen)
    assert lost == {lost_day}


def test_nhl_schedule_walk_reports_a_day_answered_with_an_error_object(monkeypatch):
    """WITNESS (Codex round 5): a 200 without a `gameWeek` list (an error
    object) read as a quiet day. Every real answer carries the list, a quiet
    July day's included (measured 2026-10-11)."""
    nhl = importlib.import_module(f"{PKG}.nhl")
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    _pin_window(monkeypatch, NOON)

    def http_get(url, *a, **k):
        if url.endswith("/schedule/2026-10-11"):
            return {"message": "rate limited"}
        return {"gameWeek": []}

    monkeypatch.setattr(nhl, "_http_get", http_get)
    with recording_lost_days() as lost:
        nhl.NhlRegularSource(season="20262027").fetch_upcoming(days_ahead=2)
    assert lost == {date(2026, 10, 11)}


@pytest.mark.parametrize("cls_name", ["NhlRegularSource", "NhlPlayoffSource"])
def test_nhl_schedule_walk_reports_a_failed_day(monkeypatch, cls_name):
    """WITNESS for the one upcoming walk that does not go through the shared
    ESPN walk: NHL asks `/schedule/{date}` per day and skipped a failure."""
    nhl = importlib.import_module(f"{PKG}.nhl")
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    today = datetime.now(UTC).date()
    lost_day = today + timedelta(days=1)

    def http_get(url, *a, **k):
        if url.endswith(f"/schedule/{lost_day.isoformat()}"):
            return None
        return {"gameWeek": []}

    monkeypatch.setattr(nhl, "_http_get", http_get)
    with recording_lost_days() as lost:
        getattr(nhl, cls_name)(season="20262027").fetch_upcoming(days_ahead=3)
    assert lost == {lost_day}


_HTTP_CALLS = ("_http_get", "http_get", "get_json", "http_get_json", "_fd_get")
# Either reports: per day, or the days of one ranged request.
_REPORTERS = {"report_lost_days", "report_lost_day_range"}


def _call_name(call):
    f = call.func
    return f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else ""


def _upcoming_day_loops(tree):
    """[(function name, loop)] for every loop that makes an HTTP request in a
    `fetch_upcoming`, or in any function of the same module that one reaches
    by name (`helper(...)` or `self.helper(...)`), transitively. The helpers
    matter: #256 moved NHL's per-day walk out of both `fetch_upcoming` bodies
    into `_upcoming_schedule_games`, and a guard reading only `fetch_upcoming`
    then judged nothing there."""
    defs = {}
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef):
            defs.setdefault(fn.name, []).append(fn)
    todo = list(defs.get("fetch_upcoming", []))
    reached = []
    while todo:
        fn = todo.pop()
        if fn in reached:
            continue
        reached.append(fn)
        for c in ast.walk(fn):
            if isinstance(c, ast.Call) and _call_name(c) in defs:
                todo.extend(defs[_call_name(c)])
    return [
        (fn.name, loop) for fn in reached for loop in ast.walk(fn)
        if isinstance(loop, (ast.For, ast.While))
        and any(isinstance(c, ast.Call) and _call_name(c) in _HTTP_CALLS for c in ast.walk(loop))
    ]


def test_every_upcoming_day_loop_reports_its_lost_days():
    """Guard for the next source: a `fetch_upcoming` that walks days with its
    OWN loop of HTTP requests (rather than through `_espn.fetch_scoreboard_days`,
    which reports for it), directly or in a helper it calls, must call
    `report_lost_days`, or a failed day reads as a quiet one again. AST over
    every module in sources/."""
    offenders = []
    for path in sorted(SOURCES.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for name, loop in _upcoming_day_loops(tree):
            called = {_call_name(c) for c in ast.walk(loop) if isinstance(c, ast.Call)}
            if not called & _REPORTERS:
                offenders.append(f"{path.name}:{name}:{loop.lineno}")
    assert offenders == []


def test_the_guard_sees_the_nhl_loop():
    """The AST guard above must actually find a loop to judge, or it passes
    by finding nothing (a zero from a search is a claim about the search).
    NHL's one per-day walk, shared by both of its sources since #256."""
    tree = ast.parse((SOURCES / "nhl.py").read_text(encoding="utf-8"))
    assert [name for name, _loop in _upcoming_day_loops(tree)] == ["_upcoming_schedule_games"]


def test_the_guard_sees_the_football_data_chunk_loop():
    """Same check for the ranged walk: Football-Data's 10-day chunks, reached
    from `SoccerSource.fetch_upcoming` through `_fetch_fixtures`."""
    tree = ast.parse((SOURCES / "soccer.py").read_text(encoding="utf-8"))
    assert [name for name, _loop in _upcoming_day_loops(tree)] == ["_fetch_tier_fixtures"]


# ---------------------------------------------------------------------------
# _rows_to_hold: a game on a lost day is held, one on an answered day is not
# ---------------------------------------------------------------------------

def _row(start, cfbd_id, label="NBA", prefix="NBA"):
    return {
        "sport_prefix": prefix,
        "sport_label": label,
        "home": f"Home{cfbd_id}",
        "away": f"Away{cfbd_id}",
        "start_time_utc": start.isoformat(),
        "score": 6.0,
        "channel_id": 5,
        "channel_ids": [5],
        "stream_ids": [],
        "extra": {"espn_event_id": str(cfbd_id), "game_id": str(cfbd_id)},
    }


class TestRowsToHoldOnLostDays:
    NOW = datetime(2026, 10, 11, 16, 0, tzinfo=UTC)

    def _hold(self, plugin, rows, lost):
        return plugin._rows_to_hold(rows, self.NOW, 30, {}, set(), lost)

    def test_game_on_a_lost_day_is_held(self, plugin):
        """WITNESS: 2026-10-13 23:30Z (19:30 ET on the 13th) on a lost 13th."""
        row = _row(datetime(2026, 10, 13, 23, 30, tzinfo=UTC), 1)
        held = self._hold(plugin, [row], {"NBA": {date(2026, 10, 13)}})
        assert list(held) == [plugin._build_marker_key(row)]

    def test_game_on_a_day_that_answered_is_not_held(self, plugin):
        """Guard: the partial source still answered for the 14th, and no
        longer lists this game, so it really is gone."""
        row = _row(datetime(2026, 10, 14, 23, 30, tzinfo=UTC), 2)
        assert self._hold(plugin, [row], {"NBA": {date(2026, 10, 13)}}) == {}

    def test_late_eastern_kickoff_sits_in_the_previous_days_bucket(self, plugin):
        """ESPN buckets by US Eastern date: 01:00Z on the 14th is 21:00 ET on
        the 13th, so a lost 13th covers it."""
        row = _row(datetime(2026, 10, 14, 1, 0, tzinfo=UTC), 3)
        held = self._hold(plugin, [row], {"NBA": {date(2026, 10, 13)}})
        assert list(held) == [plugin._build_marker_key(row)]

    def test_a_lost_day_holds_only_its_own_labels_games(self, plugin):
        row = _row(datetime(2026, 10, 13, 23, 30, tzinfo=UTC), 4, label="NFL", prefix="NFL")
        assert self._hold(plugin, [row], {"NBA": {date(2026, 10, 13)}}) == {}

    def test_no_lost_days_is_the_old_rule(self, plugin):
        row = _row(datetime(2026, 10, 13, 23, 30, tzinfo=UTC), 5)
        assert plugin._rows_to_hold([row], self.NOW, 30, {}, set()) == {}


def test_a_partial_source_is_an_outage_for_vouching(plugin):
    """An empty result built on an unvouched cache cannot be vouched while a
    source lost days: those days' games may be what is missing."""
    assert plugin._slate_known({}, {"NBA"}, 0) is False
    assert plugin._slate_known({}, set(), 0) is True


# ---------------------------------------------------------------------------
# End to end through _action_refresh
# ---------------------------------------------------------------------------

class _PartialSource:
    """A source whose sweep lost `lost` days and returned `games`."""

    sport_label = "NBA"
    sport_prefix = "NBA"

    def __init__(self, games, lost):
        self._games, self._lost = list(games), list(lost)

    def fetch_upcoming(self, days_ahead=7):
        from dispatcharr_ranked_matchups.sources.base import report_lost_days
        report_lost_days(self._lost)
        return list(self._games)


def _game(start, eid):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(sport_prefix="NBA", sport_label="NBA", home=f"Home{eid}", away=f"Away{eid}",
                   rank_home=None, rank_away=None, start_time=start,
                   extra={"espn_event_id": str(eid), "game_id": str(eid)})


def test_refresh_keeps_a_published_game_on_its_sources_lost_day(plugin, refresh_env):
    """WITNESS, the #261 repro: the previous slate airs a game two days out;
    this refresh the source answers for every day but that one. The game is
    kept (re-matched) and the summary says the source was partial."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    kept_start = (now + timedelta(days=2)).replace(hour=23, minute=30, second=0, microsecond=0)
    lost_day = (kept_start - timedelta(hours=5)).date()
    write_prior([_row(kept_start, 900)])
    other = _game(now + timedelta(days=3), 901)

    result = run([_PartialSource([other], [lost_day])])

    assert result["status"] == "ok", result
    cache = read()
    assert sorted(g["home"] for g in cache["games"]) == ["Home900", "Home901"]
    held = next(g for g in cache["games"] if g["home"] == "Home900")
    assert held["held_on_air"] is True and held["channel_id"] == 777
    assert f"NBA: partial (1 games, no answer for {lost_day.isoformat()})" in cache["summary"]
    assert ("1 source(s) lost days of their schedule (NBA); games published for "
            "those days stay on air") in result["message"]


def test_refresh_drops_a_game_its_partial_source_answered_without(plugin, refresh_env):
    """Guard: a lost day does not freeze the source. A game on a day that
    answered and no longer lists it goes, as it did before #261."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    gone_start = (now + timedelta(days=2)).replace(hour=23, minute=30, second=0, microsecond=0)
    write_prior([_row(gone_start, 900)])
    other = _game(now + timedelta(days=3), 901)
    unrelated_lost = (now + timedelta(days=5)).date()

    run([_PartialSource([other], [unrelated_lost])])

    assert [g["home"] for g in read()["games"]] == ["Home901"]


def test_an_empty_result_from_a_partial_source_is_not_vouched(plugin, refresh_env, tmp_path):
    """The previous cache is gone, and the only source lost a day: an empty
    result (here Favorites-only dropped what it did list) may be missing that
    day's games, so it must not be vouched, or apply would publish it and
    delete their channels (#241's rule, with the lost day as the outage)."""
    _write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    (tmp_path / "cache.json").unlink(missing_ok=True)

    run([_PartialSource([_game(now + timedelta(days=3), 901)], [(now + timedelta(days=2)).date()])],
        favorites="Nobody", favorites_only="strict")

    cache = read()
    assert cache["games"] == [] and cache["empty_slate_known"] is False


def test_a_clean_source_is_not_reported_partial(plugin, refresh_env):
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(now + timedelta(days=4), 900)])
    run([_PartialSource([_game(now + timedelta(days=3), 901)], [])])
    assert read()["summary"][0] == "NBA: 1 games"


def test_lost_days_reach_the_refresh_from_threads_of_the_walk(plugin):
    """The walk fetches days on a thread pool; the report must still land in
    the record opened on the calling thread (it is made after the pool has
    joined). A regression that reported from inside `fetch_day` would lose
    every day, since pool threads do not inherit the record."""
    from dispatcharr_ranked_matchups.sources import _espn
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    callers = set()

    def http_get(url):
        callers.add(threading.get_ident())
        return None if url.endswith("dates=20261012&limit=1000") else {"events": []}

    with recording_lost_days() as lost:
        _espn.fetch_scoreboard_days(http_get, "https://x.test/scoreboard",
                                    date(2026, 10, 10), date(2026, 10, 17))
    assert threading.get_ident() not in callers  # the requests really were pooled
    assert lost == {date(2026, 10, 12)}


# ---------------------------------------------------------------------------
# Range requests: one call for several days loses all of them at once
# ---------------------------------------------------------------------------
#
# The sources below do not walk day by day. They ask once (or in a few
# chunks) for a DATE RANGE, and each swallowed a failed request as an empty or
# shorter answer. The clocks are pinned and every expected day is a literal.

# 2026-10-11T16:00Z: noon Eastern on Sunday the 11th.
NOON = datetime(2026, 10, 11, 16, 0, tzinfo=UTC)
# 2026-10-11T02:00Z: 22:00 Eastern on Saturday the 10th, the hour the UTC date
# has already moved on while the evening's games are still being played.
EVENING = datetime(2026, 10, 11, 2, 0, tzinfo=UTC)


def _pinned(now):
    """A `datetime` subclass whose now() is `now`, to patch into a module."""
    class _Pinned(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is not None else now.replace(tzinfo=None)
    return _Pinned


def _pin_window(monkeypatch, now):
    """Pin `_espn.upcoming_window`'s clock, which MLB, CBB and boxing read."""
    from dispatcharr_ranked_matchups.sources import _espn
    monkeypatch.setattr(_espn, "datetime", _pinned(now))


def _days(first, last):
    return {first + timedelta(days=i) for i in range((last - first).days + 1)}


class _Resp:
    """A `requests.Response` stand-in for the sources that call requests
    directly (CBB, Football-Data), and for the guard's undated answers."""

    def __init__(self, body, status=200):
        self._body, self.status_code = body, status
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self):
        return self._body


class TestMlbRangeReportsLostDays:
    def test_a_failed_schedule_request_reports_every_day_it_asked_for(self, monkeypatch):
        """WITNESS: the regular-season `fetch_upcoming` asks statsapi for
        startDate=2026-10-10&endDate=2026-10-13 in ONE request. It failed,
        `_http_get` gave None, and the source returned [] with nothing
        recorded, so a refresh read four lost days as four quiet ones."""
        mlb = importlib.import_module(f"{PKG}.mlb")
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        seen = []

        def http_get(url, *a, **k):
            seen.append(url)
            return None

        monkeypatch.setattr(mlb, "_http_get", http_get)
        with recording_lost_days() as lost:
            rows = mlb.MlbRegularSource(season=2026).fetch_upcoming(days_ahead=2)
        assert rows == []
        assert "startDate=2026-10-10&endDate=2026-10-13" in seen[0]
        assert lost == {date(2026, 10, 10), date(2026, 10, 11),
                        date(2026, 10, 12), date(2026, 10, 13)}

    @pytest.mark.parametrize("body", [[], {}, {"message": "quota exceeded"}])
    def test_an_answer_that_is_not_a_schedule_is_a_failure(self, monkeypatch, body):
        """WITNESS (Codex rounds 4-5): a 200 whose body is not a schedule
        (no `dates` list) read as an empty one, or, for a non-object, raised
        AttributeError and took the whole label out, instead of reporting
        the days the request covered."""
        mlb = importlib.import_module(f"{PKG}.mlb")
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: body)
        with recording_lost_days() as lost:
            assert mlb.MlbRegularSource(season=2026).fetch_upcoming(days_ahead=2) == []
        assert lost == {date(2026, 10, 10), date(2026, 10, 11),
                        date(2026, 10, 12), date(2026, 10, 13)}

    def test_an_answer_with_no_games_is_quiet(self, monkeypatch):
        """Guard: statsapi answering with no dates is the off-season, not an
        outage. Its shape, measured 2026-10-11 over 2026-12-20..23: the
        `dates` list is there, empty."""
        mlb = importlib.import_module(f"{PKG}.mlb")
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: {
            "copyright": "", "totalItems": 0, "totalGames": 0, "dates": []})
        with recording_lost_days() as lost:
            assert mlb.MlbRegularSource(season=2026).fetch_upcoming(days_ahead=2) == []
        assert lost == set()


class TestFootballDataChunksReportLostDays:
    """Football-Data's /v4/matches takes dateFrom/dateTo and caps a request at
    10 days, so a 14-day lookahead from 2026-10-11 is two chunks:
    2026-10-11..2026-10-21 and 2026-10-22..2026-10-25. The result is cached
    and shared by every FD competition in the refresh."""

    def _stub(self, monkeypatch, fail_chunk_from, bad_body=None):
        soccer = importlib.import_module(f"{PKG}.soccer")
        soccer._clear_fd_caches()
        monkeypatch.setattr(soccer, "datetime", _pinned(NOON))
        ranged = []

        def get(url, headers=None, params=None, timeout=None):
            params = params or {}
            if url.endswith("/v4/matches"):
                ranged.append((params["dateFrom"], params["dateTo"]))
                if params["dateFrom"] in fail_chunk_from:
                    if bad_body is not None:
                        return _Resp(bad_body)
                    raise soccer.requests.ConnectionError("chunk refused")
                # One fixture per competition on the chunk's first day, so a
                # row says which chunk answered.
                kickoff = f"{params['dateFrom']}T14:00:00Z"
                return _Resp({"matches": [
                    {"id": 1, "competition": {"code": "PL"}, "utcDate": kickoff,
                     "homeTeam": {"name": "Arsenal"}, "awayTeam": {"name": "Chelsea"},
                     "status": "TIMED", "stage": "REGULAR_SEASON", "matchday": 8},
                    {"id": 2, "competition": {"code": "ELC"}, "utcDate": kickoff,
                     "homeTeam": {"name": "Leeds"}, "awayTeam": {"name": "Hull"},
                     "status": "TIMED", "stage": "REGULAR_SEASON", "matchday": 11},
                ]})
            return _Resp({"standings": [], "matches": []})

        stub_requests_get(monkeypatch, get)
        return soccer, ranged

    def _fetch(self, soccer, config_key):
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        with recording_lost_days() as lost:
            rows = soccer.SoccerSource(config_key, "test-key").fetch_upcoming(days_ahead=14)
        return rows, lost

    def test_a_failed_second_chunk_reports_its_own_days(self, monkeypatch):
        """WITNESS: the first chunk answered, the second failed after its
        retries. `_fetch_tier_fixtures` kept the first chunk's games and said
        nothing, so 2026-10-22..25 read as four quiet days for EVERY FD
        competition, and each still had games, so none read as an outage."""
        soccer, ranged = self._stub(monkeypatch, fail_chunk_from={"2026-10-22"})
        rows, lost = self._fetch(soccer, "epl")
        assert ranged[0] == ("2026-10-11", "2026-10-21")
        assert ranged[-1] == ("2026-10-22", "2026-10-25")
        assert [(r.home, r.start_time.date()) for r in rows] == [("Arsenal", date(2026, 10, 11))]
        assert lost == {date(2026, 10, 22), date(2026, 10, 23),
                        date(2026, 10, 24), date(2026, 10, 25)}

    def test_a_competition_reading_the_cached_result_reports_them_too(self, monkeypatch):
        """WITNESS: the second competition makes no request of its own (the
        tier result is cached), and lost the same four days."""
        soccer, ranged = self._stub(monkeypatch, fail_chunk_from={"2026-10-22"})
        self._fetch(soccer, "epl")
        requests_so_far = len(ranged)
        rows, lost = self._fetch(soccer, "championship")
        assert len(ranged) == requests_so_far  # served from the cache
        assert [(r.home, r.start_time.date()) for r in rows] == [("Leeds", date(2026, 10, 11))]
        assert lost == {date(2026, 10, 22), date(2026, 10, 23),
                        date(2026, 10, 24), date(2026, 10, 25)}

    def test_a_failed_first_chunk_does_not_stop_the_walk(self, monkeypatch):
        """WITNESS: the walk stopped at the first failed chunk, so the second
        was never asked for and its days were lost without any request
        covering them. It now asks for every chunk and reports only the one
        that failed: the second answered for 2026-10-22..25."""
        soccer, ranged = self._stub(monkeypatch, fail_chunk_from={"2026-10-11"})
        rows, lost = self._fetch(soccer, "epl")
        assert ranged[-1] == ("2026-10-22", "2026-10-25")
        assert [(r.home, r.start_time.date()) for r in rows] == [("Arsenal", date(2026, 10, 22))]
        assert lost == _days(date(2026, 10, 11), date(2026, 10, 21))

    def test_an_answer_without_a_matches_list_is_a_failure(self, monkeypatch):
        """WITNESS: a 200 whose body carries no `matches` list (an error
        object) was read as a chunk with no fixtures. Every real answer has
        the list, `[]` included."""
        soccer, _ranged = self._stub(monkeypatch, fail_chunk_from={"2026-10-22"},
                                     bad_body={"message": "quota", "errorCode": 429})
        _rows, lost = self._fetch(soccer, "epl")
        assert lost == {date(2026, 10, 22), date(2026, 10, 23),
                        date(2026, 10, 24), date(2026, 10, 25)}

    def test_chunks_that_answer_are_quiet(self, monkeypatch):
        soccer, _ranged = self._stub(monkeypatch, fail_chunk_from=set())
        _rows, lost = self._fetch(soccer, "epl")
        assert lost == set()


class TestCbbRangeReportsLostDays:
    """NcaamSource asks CollegeBasketballData for /games over
    startDateRange..endDateRange in one request, after /rankings."""

    POLL = [{"pollType": "AP Top 25", "week": 1, "team": "Duke", "ranking": 1}]

    def _stub(self, monkeypatch, *, rankings, games):
        ncaam = importlib.import_module(f"{PKG}.ncaam")
        ranged = []

        def get(url, headers=None, params=None, timeout=None):
            params = params or {}
            if url.endswith("/rankings"):
                return rankings() if callable(rankings) else _Resp(rankings)
            if url.endswith("/games"):
                ranged.append((params["startDateRange"], params["endDateRange"]))
                return games() if callable(games) else _Resp(games)
            return _Resp([])  # /lines

        stub_requests_get(monkeypatch, get)
        return ncaam, ranged

    def _refused(self):
        import requests
        raise requests.ConnectionError("refused")

    def test_a_failed_games_request_reports_every_day_it_asked_for(self, monkeypatch):
        """WITNESS: /games failed, `_fetch_games` returned [], and
        `fetch_upcoming` logged "no upcoming games" and returned [] with
        nothing recorded. Under the 22:00-Eastern clock the request covers the
        lookback day 2026-10-10 through 2026-10-13."""
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, EVENING)
        ncaam, ranged = self._stub(monkeypatch, rankings=self.POLL, games=self._refused)
        with recording_lost_days() as lost:
            rows = ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2)
        assert rows == []
        assert ranged == [("2026-10-10", "2026-10-13")]
        assert lost == {date(2026, 10, 10), date(2026, 10, 11),
                        date(2026, 10, 12), date(2026, 10, 13)}

    def test_the_window_starts_at_the_eastern_lookback_day(self, monkeypatch):
        """WITNESS for #256's class: at 02:00Z the UTC date is the 11th, and a
        19:00 Eastern tip-off on the 10th (23:00Z) is in progress. The source
        asked from today-UTC, 2026-10-11, which cannot return it. It is
        published now; a game that finished that evening is not."""
        _pin_window(monkeypatch, EVENING)
        live = {"id": 1, "homeTeam": "Duke", "awayTeam": "UNC",
                "startDate": "2026-10-10T23:00:00.000Z",
                "homePoints": 40, "awayPoints": 38, "homeWinner": None, "awayWinner": None}
        over = {"id": 2, "homeTeam": "Kansas", "awayTeam": "Baylor",
                "startDate": "2026-10-10T22:00:00.000Z",
                "homePoints": 70, "awayPoints": 60, "homeWinner": True, "awayWinner": False}
        ncaam, ranged = self._stub(monkeypatch, rankings=self.POLL, games=[live, over])
        rows = ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2)
        assert ranged == [("2026-10-10", "2026-10-13")]
        assert [r.home for r in rows] == ["Duke"]

    def test_a_failed_rankings_request_is_an_outage_not_an_offseason(self, monkeypatch):
        """WITNESS: /rankings failed, `_fetch_rankings` returned None, and
        `fetch_upcoming` logged "no current rankings (preseason / offseason?)"
        and returned [] without asking for a single game. Raising is how the
        refresh is told the source is out (it holds the previous games,
        #240), as NcaaSoccerCupSource does for its calendar."""
        _pin_window(monkeypatch, NOON)
        ncaam, ranged = self._stub(monkeypatch, rankings=self._refused, games=[])
        with pytest.raises(Exception, match="refused"):
            ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2)
        assert ranged == []

    @pytest.mark.parametrize("body", [{}, None, {"message": "quota exceeded"}])
    def test_a_games_answer_that_is_not_a_list_is_a_failure(self, monkeypatch, body):
        """WITNESS: `r.json() or []` read a 200 carrying null or an error
        object as "no games". /games answers with a list, `[]` included."""
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        ncaam, _ranged = self._stub(monkeypatch, rankings=self.POLL, games=lambda: _Resp(body))
        with recording_lost_days() as lost:
            assert ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2) == []
        assert lost == {date(2026, 10, 10), date(2026, 10, 11),
                        date(2026, 10, 12), date(2026, 10, 13)}

    @pytest.mark.parametrize("body", [{}, None, {"message": "quota exceeded"}, [{}]])
    def test_a_rankings_answer_that_is_not_a_list_is_an_outage(self, monkeypatch, body):
        """WITNESS: a 200 carrying null, `{}` or rows without a pollType from
        /rankings read as "no poll yet". The poll is a flat list of poll
        rows, `[]` before the first one."""
        _pin_window(monkeypatch, NOON)
        ncaam, ranged = self._stub(monkeypatch, rankings=lambda: _Resp(body), games=[])
        with pytest.raises(ValueError, match="not a list"):
            ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2)
        assert ranged == []

    def test_no_poll_yet_is_a_quiet_offseason(self, monkeypatch):
        """Guard: /rankings answered with no poll; that is a real offseason."""
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        ncaam, ranged = self._stub(monkeypatch, rankings=[], games=[])
        with recording_lost_days() as lost:
            assert ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2) == []
        assert lost == set() and ranged == []

    def test_no_games_in_the_window_is_quiet(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        ncaam, _ranged = self._stub(monkeypatch, rankings=self.POLL, games=[])
        with recording_lost_days() as lost:
            assert ncaam.NcaamSource(api_key="k").fetch_upcoming(days_ahead=2) == []
        assert lost == set()

    def test_the_season_walk_still_reads_a_failed_poll_as_no_signal(self, monkeypatch):
        """Guard: the importance-side season fetch keeps its no-op on a
        failed /rankings (an empty population, zero importance), because it
        runs at scoring time, outside the fetch the refresh holds games for."""
        ncaam, _ranged = self._stub(monkeypatch, rankings=self._refused, games=[])
        assert ncaam.NcaamSource(api_key="k")._fetch_full_season_games() == []


@pytest.mark.parametrize("standings", [None, {"code": 500, "message": "unavailable"}])
def test_mls_conference_sources_raise_when_the_standings_are_unreachable(monkeypatch, standings):
    """WITNESS: MlsEastSource / MlsWestSource route games by a conference map
    read from ESPN /standings. A failed request gave an empty map and the
    source returned [] WITHOUT walking a day. The two share the label "MLS",
    so when only one conference's request failed the label still had games
    and that conference's slate was read as quiet. Raising makes the label an
    outage the refresh holds (#240)."""
    mls_standings = importlib.import_module(f"{PKG}.mls_standings")
    walked = []

    def http_get(url, *a, **k):
        if "standings" in url:
            return standings
        walked.append(url)
        return {"events": []}

    monkeypatch.setattr(mls_standings, "_http_get", http_get)
    with pytest.raises(RuntimeError, match="standings"):
        mls_standings.MlsEastSource().fetch_upcoming(days_ahead=2)
    assert walked == []


def _standings(*conferences):
    """An ESPN MLS /standings body listing one club per named conference."""
    return {"children": [
        {"abbreviation": conf, "standings": {"entries": [{"team": {
            "displayName": f"{conf} FC", "location": conf, "name": f"{conf} FC",
            "abbreviation": conf[:3].upper()}}]}}
        for conf in conferences
    ]}


def test_a_failed_standings_request_stays_failed_for_the_source(monkeypatch):
    """WITNESS (Codex round 3): the source's season, read through the
    conference map when it is built, came back empty on a failed request and
    was cached; a retried map that recovered then let `fetch_upcoming` publish
    games scored against that empty season. One request per source, so the
    upcoming fetch sees the same failure and raises."""
    mls_standings = importlib.import_module(f"{PKG}.mls_standings")
    answers = [None, _standings("East", "West")]
    monkeypatch.setattr(
        mls_standings, "_http_get",
        lambda url, *a, **k: answers.pop(0) if url.endswith("/standings") else {"events": []})
    src = mls_standings.MlsEastSource()
    assert src._fetch_full_season_games() == []
    with pytest.raises(RuntimeError, match="standings"):
        src.fetch_upcoming(days_ahead=2)
    assert len(answers) == 1   # never asked again


def test_an_answered_standings_request_is_read_once(monkeypatch):
    """Guard: the map is cached per source once it answers."""
    mls_standings = importlib.import_module(f"{PKG}.mls_standings")
    answers = [_standings("East", "West"), None]
    monkeypatch.setattr(mls_standings, "_http_get", lambda url, *a, **k: answers.pop(0))
    src = mls_standings.MlsEastSource()
    assert src._own_conf_teams() == {"East FC"}
    assert src._own_conf_teams() == {"East FC"}
    assert answers == [None]


@pytest.mark.parametrize("body", [_standings("East"), _standings(), {"children": []},
                                  _standings("Eastern Conference", "Western Conference")])
def test_a_standings_answer_missing_a_conference_is_a_failure(monkeypatch, body):
    """WITNESS: a /standings answer listing clubs for one conference (or
    none, or under renamed abbreviations) gave a partial map. The missing
    conference's source returned [] quietly while the other kept the shared
    "MLS" label non-empty, so its published games were dropped."""
    mls_standings = importlib.import_module(f"{PKG}.mls_standings")
    monkeypatch.setattr(mls_standings, "_http_get", lambda url, *a, **k: body)
    assert mls_standings._fetch_conference_map() is None
    with pytest.raises(RuntimeError, match="standings"):
        mls_standings.MlsWestSource().fetch_upcoming(days_ahead=2)


class TestBoxingReportsLostDays:
    def _run(self, monkeypatch, body, days_ahead=3):
        boxing = importlib.import_module(f"{PKG}.boxing")
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        _pin_window(monkeypatch, NOON)
        asked = []

        def http_get(self, url, params, timeout=15.0):
            asked.append(dict(params))
            return body

        monkeypatch.setattr(boxing.BoxingSource, "_http_get", http_get)
        with recording_lost_days() as lost:
            rows = boxing.BoxingSource(api_key="k").fetch_upcoming(days_ahead=days_ahead)
        return rows, lost, asked

    def test_a_failed_schedule_request_reports_the_days_it_asked_for(self, monkeypatch):
        """WITNESS: the schedule is one `days=3` request from today. It
        failed, and the source returned [] with nothing recorded."""
        rows, lost, asked = self._run(monkeypatch, None)
        assert rows == [] and asked[0]["days"] == 3
        assert lost == {date(2026, 10, 11), date(2026, 10, 12),
                        date(2026, 10, 13), date(2026, 10, 14)}

    @pytest.mark.parametrize("body", [{"error": "DateOutOfRange", "data": []},
                                      {"error": None, "data": None}, {}])
    def test_an_in_band_error_is_a_failure_too(self, monkeypatch, body):
        """The API reports a subscription or range error with HTTP 200 and an
        empty list: no answer for those days either. Nor is a body with no
        `data` list (Codex round 4)."""
        _rows, lost, _asked = self._run(monkeypatch, body)
        assert lost == {date(2026, 10, 11), date(2026, 10, 12),
                        date(2026, 10, 13), date(2026, 10, 14)}

    def test_a_request_failing_across_utc_midnight_reports_the_days_it_covered(self, monkeypatch):
        """WITNESS: sent at 23:59:59Z on the 11th, failed after midnight. The
        days were read off the clock AFTER the call, so the report started on
        the 12th: the 11th went unreported and the 15th, which no request
        covered, was reported."""
        boxing = importlib.import_module(f"{PKG}.boxing")
        from dispatcharr_ranked_matchups.sources import _espn
        from dispatcharr_ranked_matchups.sources.base import recording_lost_days
        clock = {"now": datetime(2026, 10, 11, 23, 59, 59, tzinfo=UTC)}

        class _Moving(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock["now"]

        def http_get(self, url, params, timeout=15.0):
            clock["now"] = datetime(2026, 10, 12, 0, 0, 1, tzinfo=UTC)
            return None

        monkeypatch.setattr(_espn, "datetime", _Moving)
        monkeypatch.setattr(boxing.BoxingSource, "_http_get", http_get)
        with recording_lost_days() as lost:
            boxing.BoxingSource(api_key="k").fetch_upcoming(days_ahead=3)
        assert lost == {date(2026, 10, 11), date(2026, 10, 12),
                        date(2026, 10, 13), date(2026, 10, 14)}

    def test_no_cards_scheduled_is_quiet(self, monkeypatch):
        rows, lost, _asked = self._run(monkeypatch, {"data": []})
        assert rows == [] and lost == set()


# ---------------------------------------------------------------------------
# Behavioural class guard: every built source, every dated request failing
# ---------------------------------------------------------------------------

# What a request asked for, read off the URL AND the `params=` kwarg (CBB,
# Football-Data and boxing pass their dates as params, not in the URL). A
# ranged request expands to every day it covers.
_DAY_IN_URL = (
    re.compile(r"[?&]dates=(\d{4})(\d{2})(\d{2})"),            # ESPN scoreboard
    re.compile(r"/schedule/(\d{4})-(\d{2})-(\d{2})(?:$|\?)"),  # NHL api-web
)
_RANGE_PARAMS = (
    ("startDate", "endDate"),             # MLB statsapi
    ("dateFrom", "dateTo"),               # Football-Data /v4/matches
    ("startDateRange", "endDateRange"),   # CollegeBasketballData
)


def _asked_days(url, params):
    """The calendar days one request asked for, or an empty set for an
    undated one."""
    from urllib.parse import parse_qsl, urlsplit
    days = set()
    for rx in _DAY_IN_URL:
        m = rx.search(url)
        if m:
            days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    query = dict(parse_qsl(urlsplit(url).query))
    query.update({k: str(v) for k, v in (params or {}).items()})
    for lo, hi in _RANGE_PARAMS:
        if lo in query and hi in query:
            days |= _days(date.fromisoformat(query[lo][:10]), date.fromisoformat(query[hi][:10]))
    if url.endswith("/events/schedule") and "days" in query:   # boxing, from today-UTC
        today = datetime.now(UTC).date()
        days |= _days(today, today + timedelta(days=int(query["days"])))
    return days


# A body every dated endpoint reads as "answered, no games": ESPN `events`,
# NHL `gameWeek`, statsapi `dates`, Football-Data `matches`, boxing `data`.
# CollegeBasketballData answers with a bare list.
_EMPTY_DATED = {"events": [], "gameWeek": [], "dates": [], "matches": [], "data": []}


def _drive_every_source(plugin, monkeypatch, answer, fails=lambda days: True):
    """Run `fetch_upcoming(days_ahead=12)` on every source `_build_sources`
    builds with everything on. `answer(url)` is what an UNDATED request gets
    (a response, or raise). A DATED request fails when `fails(its days)`,
    and otherwise answers with no games. Returns (failed days by class,
    unreported by class, over-reported by class): unreported is a day of a
    failed request the source did not report, over-reported a day it
    reported that no failed request of its own covered (one that answered,
    or one never asked for)."""

    import requests
    from dispatcharr_ranked_matchups.sources import soccer
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    from tests.test_mapped_prefixes import _all_on_settings

    failed: dict = {}
    # Requests made while the sources are BUILT (strength seeding, MLS's
    # conference map, which it then keeps) get the same answers, filed under
    # this name rather than any source's.
    current = {"name": "(building sources)"}

    def get(url, *a, **k):
        days = _asked_days(url, k.get("params"))
        if not days:
            return answer(url)
        if fails(days):
            failed.setdefault(current["name"], set()).update(days)
            raise requests.ConnectionError("this dated request fails in this test")
        return _Resp([] if "collegebasketballdata" in url else _EMPTY_DATED)

    stub_requests_get(monkeypatch, get)
    sources = plugin._build_sources(_all_on_settings())
    unreported, overreported = {}, {}
    for src in sources:
        name = type(src).__name__
        current["name"] = name
        # Football-Data's tier result is cached and shared by every FD source,
        # so a source served from it would ask for nothing here while still
        # reporting the days. Cold per source, so each asks for itself; the
        # cached path has its own witness above.
        soccer._clear_fd_caches()
        try:
            with recording_lost_days() as lost:
                # 12 days, so Football-Data's 10-day cap splits the window
                # into two chunks (today..today+10, then the last two days).
                src.fetch_upcoming(days_ahead=12)
        except Exception:
            continue  # an outage the refresh sees for itself
        missing = failed.get(name, set()) - lost
        extra = lost - failed.get(name, set())
        if missing:
            unreported[name] = sorted(d.isoformat() for d in missing)
        if extra:
            overreported[name] = sorted(d.isoformat() for d in extra)
    return failed, unreported, overreported


def _undated_answers(url):
    """Undated requests answer: CBB's poll with a ranked team, ESPN's MLS
    standings with both conferences, and an empty object otherwise."""
    if "collegebasketballdata" in url and url.endswith("/rankings"):
        return _Resp(TestCbbRangeReportsLostDays.POLL)
    if url.endswith("/usa.1/standings"):
        return _Resp(_standings("East", "West"))
    return _Resp({})


def test_every_built_source_reports_the_days_it_could_not_fetch(plugin, monkeypatch):
    """The AST guard above reads code shapes, and a shape it does not know (a
    per-day helper with no loop of its own, a direct `requests.get`, ONE
    request for a date range) slips past it (1.34.0 batch review). This half
    does not care how a source walks: every source `_build_sources` builds
    with everything on runs `fetch_upcoming` with EVERY request failing, and
    each dated day it asked for must come back as lost, or the source must
    raise (a whole-source outage, which the refresh already counts).
    Returning quietly with a day unreported is the #261 defect. A day
    reported that no request of the source covered is a defect too: it holds
    games on a day the source vouched for."""
    import requests

    def refuse(url):
        raise requests.ConnectionError("every request fails in this test")

    failed, unreported, overreported = _drive_every_source(plugin, monkeypatch, refuse)
    # The instrument reached the per-day walks (NHL's included) AND the ranged
    # requests (statsapi, Football-Data, boxing).
    assert {"NbaRegularSource", "NhlRegularSource", "NhlPlayoffSource",
            "UfcSource", "EflCupSource", "MlbRegularSource", "SoccerSource",
            "KnockoutSoccerSource", "BoxingSource"} <= set(failed)
    assert unreported == {}
    assert overreported == {}


def test_every_built_source_reports_the_days_its_dated_requests_lost(plugin, monkeypatch):
    """The same guard with only the DATED requests failing. A source gated on
    an undated request first (CBB's /rankings, MLS's /standings) never
    reaches its dated one when everything fails, so the first variant cannot
    judge it: here the undated requests answer and the dated ones fail."""
    failed, unreported, overreported = _drive_every_source(
        plugin, monkeypatch, _undated_answers)
    assert {"NcaamSource", "MlsEastSource", "MlsWestSource", "MlbRegularSource",
            "SoccerSource", "NbaRegularSource", "BoxingSource"} <= set(failed)
    assert unreported == {}
    assert overreported == {}


def test_no_source_reports_a_day_a_request_answered_for(plugin, monkeypatch):
    """The same guard with ONE day failing: any dated request covering
    tomorrow (UTC) fails, every other one answers with no games. A per-day
    walk loses tomorrow alone; a ranged request covering it loses its whole
    range; and Football-Data's second chunk, which does not cover it, answers.
    A source reporting a day only an ANSWERED request covered fails here as
    over-reported: that holds games on a day the source vouched for."""
    tomorrow = datetime.now(UTC).date() + timedelta(days=1)
    failed, unreported, overreported = _drive_every_source(
        plugin, monkeypatch, _undated_answers, fails=lambda days: tomorrow in days)
    assert {"NbaRegularSource", "NhlRegularSource", "NcaamSource", "MlsEastSource",
            "MlbRegularSource", "SoccerSource", "BoxingSource"} <= set(failed)
    assert failed["NbaRegularSource"] == {tomorrow}
    assert max(failed["SoccerSource"]) == tomorrow + timedelta(days=9)  # chunk 1 only
    assert unreported == {}
    assert overreported == {}
