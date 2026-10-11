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
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

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


def test_every_upcoming_day_loop_reports_its_lost_days():
    """Guard for the next source: a `fetch_upcoming` that walks days with its
    OWN loop of HTTP requests (rather than through `_espn.fetch_scoreboard_days`,
    which reports for it) must call `report_lost_days`, or a failed day reads
    as a quiet one again. AST over every module in sources/."""
    offenders = []
    for path in sorted(SOURCES.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.FunctionDef) and fn.name == "fetch_upcoming"):
                continue
            for loop in ast.walk(fn):
                if not isinstance(loop, (ast.For, ast.While)):
                    continue
                called = set()
                for c in ast.walk(loop):
                    if isinstance(c, ast.Call):
                        f = c.func
                        called.add(f.id if isinstance(f, ast.Name)
                                   else f.attr if isinstance(f, ast.Attribute) else "")
                http = {n for n in called if n in ("_http_get", "http_get", "get_json", "http_get_json")}
                if http and "report_lost_days" not in called:
                    offenders.append(f"{path.name}:{loop.lineno}")
    assert offenders == []


def test_the_guard_sees_the_nhl_loop():
    """The AST guard above must actually find a loop to judge, or it passes
    by finding nothing (a zero from a search is a claim about the search)."""
    tree = ast.parse((SOURCES / "nhl.py").read_text(encoding="utf-8"))
    loops = [
        loop for fn in ast.walk(tree)
        if isinstance(fn, ast.FunctionDef) and fn.name == "fetch_upcoming"
        for loop in ast.walk(fn) if isinstance(loop, ast.For)
        and any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                and c.func.id == "_http_get" for c in ast.walk(loop))
    ]
    assert len(loops) == 2


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
