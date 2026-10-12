"""A points-based season that could not be fetched whole is reported, not
simulated with holes in it (#289).

The importance simulator replays each points-based league's season list
(`PointsBasedSportSource._fetch_full_season_games`, cached by
`_fetch_games_cached`). That list is read at SCORING time, and at build time
for the playoff strength seeds, outside any lost-day record, so a failure in
it was silent:

- an ESPN season walk (NBA, NFL, WNBA, NCAAW, NCAA baseball, softball and
  soccer, MLS) reported its lost days to no record, and its other days were
  replayed as the whole season;
- NHL skipped a club whose schedule did not answer, MLB and NCAAF read a
  failed season request as an empty season, NCAAM skipped a failed third of
  its season, and MLS read a failed conference map as an empty season.

Every game in the league then scored stakes from standings missing results
and a calendar missing fixtures, and the refresh summary said nothing. Now
the season is not simulated (its games score no standings stakes) and the
refresh reports "<label> season (stakes not simulated)", as it does for a
Football-Data season (#228) and a playoff bracket (#245).

Three layers: per-source WITNESSES with literal days, GUARDS that an
answered season (an offseason's empty one included) stays quiet and that a
failure is not re-fetched, and the CLASS guard over every built
points-based source.
"""
from __future__ import annotations

import importlib
import zlib
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.conftest import stub_requests_get
from tests.test_hold_failed_days import _Resp, _standings
from tests.test_upcoming_window import _event

UTC = timezone.utc
PKG = "dispatcharr_ranked_matchups.sources"


def _finished(eid, home, away, day):
    return _event(eid, home, away, datetime(day.year, day.month, day.day, 23, 0, tzinfo=UTC), "post")


# ---------------------------------------------------------------------------
# ESPN season walks: a lost day is a failed season
# ---------------------------------------------------------------------------

def _nba_board(monkeypatch, dead_day):
    """NBA's scoreboard for the 2026-27 regular season (walked 2026-10-01 to
    2027-04-30): one finished game on 2026-11-05, nothing else, and
    `dead_day` (YYYYMMDD) failing every time it is asked."""
    nba = importlib.import_module(f"{PKG}.nba")
    asked = []

    def http_get(url, *a, **k):
        day = url.split("dates=")[1][:8]
        asked.append(day)
        if day == dead_day:
            return None
        if day == "20261105":
            return {"events": [_finished("g1", "Boston", "Denver", date(2026, 11, 5))]}
        return {"events": []}

    monkeypatch.setattr(nba, "_http_get", http_get)
    return nba.NbaRegularSource(season_end_year=2027), asked


def test_a_lost_walk_day_is_a_failed_season_not_a_shorter_one(monkeypatch):
    """WITNESS: 2026-12-25 failed twice (its blip retry too). The walk
    returned the other 211 days and the simulator replayed them as the whole
    season, Christmas Day's games missing, with nothing reported. Now the
    season is not simulated and says which day it lost."""
    src, _asked = _nba_board(monkeypatch, dead_day="20261225")
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() == (
        "no answer for 1 day(s) of the season (2026-12-25..2026-12-25)")


def test_an_answered_season_is_simulated_and_quiet(monkeypatch):
    """Guard: every day answered, one finished game in the season."""
    src, _asked = _nba_board(monkeypatch, dead_day=None)
    assert [g["id"] for g in src._fetch_games_cached()] == ["g1"]
    assert src.season_fetch_failure() is None


def test_a_failed_season_is_not_walked_again_this_refresh(monkeypatch):
    """Guard: the strengths, the initial state and every remaining-matches
    call read the season; a failed one is not re-walked for each (one
    walk can spend 120 s, and a dozen leagues would pass the pipeline's
    25-minute ceiling). The next refresh builds a new source and asks
    afresh."""
    src, asked = _nba_board(monkeypatch, dead_day="20261225")
    src._fetch_games_cached()
    walked = len(asked)
    src.estimate_strengths()
    src.initial_state()
    src._fetch_games_cached()
    assert len(asked) == walked


def test_the_season_days_do_not_land_in_an_upcoming_record(monkeypatch):
    """Guard: a season read inside a refresh's upcoming record (MLS reads its
    season when it is built) keeps its days to itself. A season day is not
    an upcoming day; reported there it would hold games on days the
    upcoming fetch answered for."""
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    src, _asked = _nba_board(monkeypatch, dead_day="20261225")
    with recording_lost_days() as upcoming:
        src._fetch_games_cached()
    assert upcoming == set()
    assert src.season_fetch_failure() is not None


# ---------------------------------------------------------------------------
# The non-ESPN season fetches
# ---------------------------------------------------------------------------

def test_nhl_a_club_schedule_that_did_not_answer_fails_the_season(monkeypatch):
    """WITNESS: Boston's `club-schedule-season` failed and was skipped, so
    every Bruins game, results and fixtures, was missing from the season."""
    nhl = importlib.import_module(f"{PKG}.nhl")
    monkeypatch.setattr(nhl, "_http_get", lambda url, *a, **k: (
        None if "/BOS/" in url else {"games": []}))
    src = nhl.NhlRegularSource(season="20262027")
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() == "club-schedule-season/BOS did not answer"


@pytest.mark.parametrize("body", [{"message": "rate limited"}, {"games": None}])
def test_nhl_a_club_schedule_without_a_games_list_fails_the_season(monkeypatch, body):
    """WITNESS: a 200 error object was truthy, had no `games`, and the club
    was read as having no games. All 32 clubs answer with the list (measured
    2026-10-11)."""
    nhl = importlib.import_module(f"{PKG}.nhl")
    monkeypatch.setattr(nhl, "_http_get", lambda url, *a, **k: (
        body if "/TOR/" in url else {"games": []}))
    src = nhl.NhlRegularSource(season="20262027")
    src._fetch_games_cached()
    assert src.season_fetch_failure() == "club-schedule-season/TOR did not answer"


@pytest.mark.parametrize("body", [None, {}, {"message": "error"}])
def test_mlb_a_season_request_that_did_not_answer_fails_the_season(monkeypatch, body):
    """WITNESS: MLB's one season request failed (or answered something that
    is not a schedule) and the season read as empty."""
    mlb = importlib.import_module(f"{PKG}.mlb")
    monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: body)
    src = mlb.MlbRegularSource(season=2026)
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() == "regular-season schedule did not answer"


def test_mlb_an_empty_season_is_quiet(monkeypatch):
    """Guard: statsapi answers a season with no games as `"dates": []`
    (measured 2026-10-11, season=2031)."""
    mlb = importlib.import_module(f"{PKG}.mlb")
    monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: {"totalGames": 0, "dates": []})
    src = mlb.MlbRegularSource(season=2031)
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() is None


@pytest.mark.parametrize("answer", ["refused", {"message": "quota"}, None])
def test_ncaaf_a_failed_season_request_is_a_failure_not_an_empty_season(monkeypatch, answer):
    """WITNESS: CFBD `/games` failed (or answered an error object, or null),
    and `_fetch_raw_season` returned [], which `fetch_upcoming` read as no
    games and the simulator as an empty season. It raises now: the label is
    an outage the refresh holds (#240), and the season a failure it reports."""
    import requests

    from dispatcharr_ranked_matchups.sources.base import SeasonUnavailable
    ncaaf = importlib.import_module(f"{PKG}.ncaaf")

    def get(url, **_k):
        if url.endswith("/rankings"):
            return _Resp([{"week": 7, "polls": [{"poll": "AP Top 25", "ranks": [
                {"school": "Georgia", "rank": 1}]}]}])
        if answer == "refused":
            raise requests.ConnectionError("refused")
        return _Resp(answer)

    stub_requests_get(monkeypatch, get)
    src = ncaaf.NcaafSource(api_key="k")
    assert src._fetch_games_cached() == []
    assert (src.season_fetch_failure() or "").startswith("/games failed")
    with pytest.raises(SeasonUnavailable):
        ncaaf.NcaafSource(api_key="k").fetch_upcoming(days_ahead=7)


def test_ncaam_a_failed_season_chunk_fails_the_season(monkeypatch):
    """WITNESS: the second of NCAAM's three `/games` season chunks
    (2026-01-01..2026-02-15) failed and was skipped: a third of the season,
    the heart of conference play, gone from the simulation."""
    import requests
    ncaam = importlib.import_module(f"{PKG}.ncaam")
    poll = [{"pollType": "AP Top 25", "week": 1, "team": "Duke", "ranking": 1}]

    def get(url, **k):
        if url.endswith("/rankings"):
            return _Resp(poll)
        if (k.get("params") or {}).get("startDateRange") == "2026-01-01":
            raise requests.ConnectionError("refused")
        return _Resp([])

    stub_requests_get(monkeypatch, get)
    src = ncaam.NcaamSource(api_key="k")
    monkeypatch.setattr(src, "_current_season_year", lambda: 2026)
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure().startswith(
        "full-season chunk 2026-01-01..2026-02-15 failed")


def test_mls_a_failed_conference_map_fails_the_season(monkeypatch):
    """WITNESS: the map failed and the conference's season read as empty."""
    mls_standings = importlib.import_module(f"{PKG}.mls_standings")
    monkeypatch.setattr(mls_standings, "_http_get", lambda url, *a, **k: (
        None if url.endswith("/standings") else {"events": []}))
    src = mls_standings.MlsWestSource()
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() == "standings conference map unavailable"


# ---------------------------------------------------------------------------
# End to end: the refresh reports it
# ---------------------------------------------------------------------------

def test_refresh_reports_a_season_that_was_not_simulated(monkeypatch, refresh_env, plugin):
    """An NBA game tomorrow, and the season walk losing a day: the game is
    published, with no standings stakes, and the summary says why."""
    nba = importlib.import_module(f"{PKG}.nba")
    tomorrow = datetime.now(UTC) + timedelta(days=1)
    game = _event("t1", "Boston", "Denver", tomorrow.replace(hour=23, minute=0), "pre")

    def http_get(url, *a, **k):
        day = url.split("dates=")[1][:8]
        if day == "20261225":
            return None
        if day == tomorrow.strftime("%Y%m%d"):
            return {"events": [game]}
        return {"events": []}

    monkeypatch.setattr(nba, "_http_get", http_get)
    src = nba.NbaRegularSource(season_end_year=2027)
    _write_prior, run, read = refresh_env
    run([src], weight_importance=1)   # refresh_env turns importance off by default
    cache = read()
    assert [(g["home"], g["away"]) for g in cache["games"]] == [("Boston", "Denver")]
    assert cache["games"][0]["importance_points"] == 0
    assert [s for s in cache["summary"] if "season (stakes not simulated)" in s] == [
        "NBA season (stakes not simulated): error "
        "(no answer for 1 day(s) of the season (2026-12-25..2026-12-25))"
    ]
    problems = plugin._slate_problems(cache["summary"], cache["games"])
    assert any("NBA season" in p for p in problems)


def test_refresh_does_not_report_a_failed_season_with_nothing_on_the_slate(monkeypatch, refresh_env):
    """Guard: a league with no game in this refresh lost nothing to the
    failure (a season read only for a playoff seed, an offseason)."""
    nba = importlib.import_module(f"{PKG}.nba")
    monkeypatch.setattr(nba, "_http_get", lambda url, *a, **k: (
        None if "dates=20261225" in url else {"events": []}))
    src = nba.NbaRegularSource(season_end_year=2027)
    src._fetch_games_cached()
    assert src.season_fetch_failure() is not None
    _write_prior, run, read = refresh_env
    run([src], weight_importance=1)
    assert not any("season (stakes not simulated)" in s for s in read().get("summary", []))


# ---------------------------------------------------------------------------
# Class guard: every built points-based source, every season request
# ---------------------------------------------------------------------------

def _answer(url, params):
    """An answered request with no games, in each API's measured shape. The
    CBBD poll answers with one ranked team (NCAAM simulates only the
    population its poll names) and ESPN MLS standings with both conferences
    (East and West cut their seasons by the map)."""
    if "collegebasketballdata" in url:
        if url.endswith("/rankings"):
            return _Resp([{"pollType": "AP Top 25", "week": 1, "team": "Duke", "ranking": 1}])
        return _Resp([])
    if "collegefootballdata" in url:
        if url.endswith("/rankings"):
            return _Resp([])
        return _Resp([])
    if url.endswith("/usa.1/standings"):
        return _Resp(_standings("East", "West"))
    if "/club-schedule-season/" in url:
        return _Resp({"games": []})
    if "statsapi.mlb.com" in url:
        return _Resp({"dates": []})
    return _Resp({"events": [], "dates": [], "gameWeek": [], "matches": [], "data": []})


def _drive_every_season(plugin, monkeypatch, fails):
    """Build every source with everything on (every request answering), then
    read each points-based source's season with `fails(url, params)` deciding
    which requests fail, every time they are asked. Returns ({class: requests
    made}, {class: failed requests}, {class: season_fetch_failure()})."""
    import requests

    from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource
    from tests.test_mapped_prefixes import _all_on_settings

    mode = {"reading": None}
    made, failed = {}, {}

    def get(url, **k):
        params = k.get("params") or {}
        name = mode["reading"]
        if name is not None:
            made[name] = made.get(name, 0) + 1
            if fails(url, params):
                failed[name] = failed.get(name, 0) + 1
                raise requests.ConnectionError("this season request fails in this test")
        return _answer(url, params)

    stub_requests_get(monkeypatch, get)
    sources = [s for s in plugin._build_sources(_all_on_settings())
               if isinstance(s, PointsBasedSportSource)]
    failures = {}
    for src in sources:
        name = f"{type(src).__name__}:{src.sport_label}"
        # Built above with every request answering, which already read some
        # seasons (the playoff strength seeds). Forget that read so this one
        # is the one judged. The two attributes are the mechanism under test.
        src._all_games_cache = None
        src._season_failure = None
        mode["reading"] = name
        try:
            src._fetch_games_cached()
        finally:
            mode["reading"] = None
        failures[name] = src.season_fetch_failure()
    return made, failed, failures


_EVERY_SEASON = {
    "NbaRegularSource", "NflRegularSource", "WnbaRegularSource",
    "NcaawBasketballRegularSource", "NcaaBaseballRegularSource",
    "NcaaSoftballRegularSource", "NcaaSoccerSource", "MlsEastSource",
    "MlsWestSource", "NhlRegularSource", "MlbRegularSource", "NcaafSource",
    "NcaamSource",
}


def _classes(names):
    return {n.split(":", 1)[0] for n in names}


@pytest.mark.parametrize("pattern", ["every", "sampled"])
def test_every_season_that_lost_a_request_reports_it(plugin, monkeypatch, pattern):
    """CLASS guard. Every points-based source `_build_sources` builds reads
    its season with requests failing: all of them, or a deterministic sample
    (one in five URLs, by CRC32, so a long walk loses a scattering of days
    and a per-club fetch a few clubs). A source that saw a failed request
    must report its season failed; one that saw none must not. The ESPN
    walk's blip retry re-asks a failed day, and the sample fails it again,
    so a sampled day stays lost."""
    if pattern == "every":
        fails = lambda url, params: True  # noqa: E731
    else:
        fails = lambda url, params: zlib.crc32(  # noqa: E731
            (url + repr(sorted(params.items()))).encode()) % 5 == 0
    made, failed, failures = _drive_every_season(plugin, monkeypatch, fails)
    # The instrument reached every points-based class, and each asked for
    # something while its season was read.
    assert _classes(made) == _EVERY_SEASON, _classes(made) ^ _EVERY_SEASON
    if pattern == "every":
        assert _classes(failed) == _EVERY_SEASON
    else:
        # The sample must actually hit the multi-request walks.
        assert {"NbaRegularSource", "NhlRegularSource", "NcaaBaseballRegularSource"} <= _classes(failed)
    unreported = sorted(n for n in failed if failures[n] is None)
    overreported = sorted(n for n, f in failures.items() if f is not None and n not in failed)
    assert unreported == []
    assert overreported == []


def test_every_answered_season_is_quiet(plugin, monkeypatch):
    """Guard: the same drive with every request answering (each API's empty
    shape). No season reports a failure."""
    made, _failed, failures = _drive_every_season(plugin, monkeypatch, lambda url, params: False)
    assert _classes(made) == _EVERY_SEASON
    assert {n: f for n, f in failures.items() if f is not None} == {}


def test_a_season_read_at_build_time_reports_too(plugin, monkeypatch):
    """CLASS guard, the production order: the sources are BUILT with every
    request failing, so the playoff strength seeds read their regular seasons
    then (and fail), and every other season fails on its first read after.
    Every points-based source must report its season, whichever read it was."""
    import requests

    from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource
    from tests.test_mapped_prefixes import _all_on_settings

    def refuse(url, **_k):
        raise requests.ConnectionError("every request fails in this test")

    stub_requests_get(monkeypatch, refuse)
    sources = [s for s in plugin._build_sources(_all_on_settings())
               if isinstance(s, PointsBasedSportSource)]
    assert {type(s).__name__ for s in sources} == _EVERY_SEASON
    for src in sources:
        src._fetch_games_cached()
    assert sorted(f"{type(s).__name__}:{s.sport_label}" for s in sources
                  if s.season_fetch_failure() is None) == []


# ---------------------------------------------------------------------------
# Reporting: a playoff slate whose strengths came from a failed season
# ---------------------------------------------------------------------------

class _Season:
    def __init__(self, label, failure):
        self.sport_label, self._failure = label, failure

    def season_fetch_failure(self):
        return self._failure


class _Playoff:
    sport_label = "NFL Playoffs"

    def __init__(self, *seeds):
        if seeds:
            self.strength_sources = seeds


def test_a_scored_playoff_reports_the_season_its_strengths_came_from(plugin):
    """WITNESS (Codex, #289 round 1): only playoff games on the slate, and the
    regular season they took their team strengths from failed at build time.
    The playoff model ran on its prior and nothing said so, because the
    regular source had no game in the slate."""
    reg = _Season("NFL", "no answer for 1 day(s) of the season (2026-12-25..2026-12-25)")
    po = _Playoff(reg)
    assert plugin._season_failure_entries([reg, po], [po]) == [
        "NFL season (stakes not simulated): error "
        "(no answer for 1 day(s) of the season (2026-12-25..2026-12-25))"
    ]


def test_an_unrelated_or_unscored_season_is_not_reported(plugin):
    """Guard: a failed season with no game in the slate, and none in a
    playoff seeded from it, lost nothing to report."""
    reg = _Season("NFL", "boom")
    assert plugin._season_failure_entries([reg, _Playoff()], [_Playoff()]) == []
    assert plugin._season_failure_entries([reg], []) == []


def test_two_conferences_sharing_a_label_report_one_entry(plugin):
    """Guard: MLS East and West share "MLS"; both failing, and the MLS Cup
    seeded from both, is one entry listing each distinct reason once."""
    east, west = _Season("MLS", "map"), _Season("MLS", "map")
    cup = _Playoff(east, west)
    assert plugin._season_failure_entries([east, west, cup], [cup]) == [
        "MLS season (stakes not simulated): error (map)"
    ]


def test_every_seeded_playoff_records_its_strength_sources(plugin, monkeypatch):
    """CLASS guard for the wiring: every source `_build_sources` builds that
    takes regular-season strengths names, through `strength_sources`, the
    built points-based sources they came from (`plugin._seeded_from`). A new
    playoff pair that skips it would hide its regular season's failure on a
    playoff-only slate."""
    from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource
    from tests.test_mapped_prefixes import _all_on_settings
    stub_requests_get(monkeypatch, lambda url, **k: _answer(url, k.get("params") or {}))
    sources = plugin._build_sources(_all_on_settings())
    built = {id(s) for s in sources}
    seeded = [s for s in sources if hasattr(s, "set_regular_season_strengths")]
    assert len(seeded) >= 11   # the instrument found the playoff family
    missing = sorted(type(s).__name__ for s in seeded if not getattr(s, "strength_sources", ()))
    assert missing == []
    for s in seeded:
        assert all(isinstance(r, PointsBasedSportSource) and id(r) in built
                   for r in s.strength_sources), type(s).__name__


# ---------------------------------------------------------------------------
# MLB: a date entry without its games list
# ---------------------------------------------------------------------------

_DAMAGED_SCHEDULE = {"dates": [{"date": "2026-04-01", "games": None}]}


def test_mlb_a_date_without_its_games_fails_the_season(monkeypatch):
    """WITNESS (Codex, #289 round 1): statsapi lists a date only for the
    games on it, and a date entry with `games: null` was read as a day off."""
    mlb = importlib.import_module(f"{PKG}.mlb")
    monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: _DAMAGED_SCHEDULE)
    src = mlb.MlbRegularSource(season=2026)
    assert src._fetch_games_cached() == []
    assert src.season_fetch_failure() == "regular-season schedule did not answer"


def test_mlb_upcoming_a_date_without_its_games_loses_the_window(monkeypatch):
    """WITNESS: the same damage in the upcoming request lost that day's
    games quietly; it now reports the request's days, like a failed one."""
    from dispatcharr_ranked_matchups.sources.base import recording_lost_days
    from tests.test_hold_failed_days import NOON, _pin_window
    mlb = importlib.import_module(f"{PKG}.mlb")
    _pin_window(monkeypatch, NOON)
    monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: _DAMAGED_SCHEDULE)
    with recording_lost_days() as lost:
        assert mlb.MlbRegularSource(season=2026).fetch_upcoming(days_ahead=2) == []
    assert lost == {date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12), date(2026, 10, 13)}


@pytest.mark.parametrize("body", [{"message": "rate limited"}, _DAMAGED_SCHEDULE])
def test_mlb_postseason_a_damaged_answer_is_not_an_empty_bracket(monkeypatch, body):
    """WITNESS: a truthy 200 error object (or a damaged date) parsed as a
    postseason with no games and was CACHED, so scoring's retry (#243)
    never asked again and nothing was reported."""
    mlb = importlib.import_module(f"{PKG}.mlb")
    monkeypatch.setattr(mlb, "_http_get", lambda url, *a, **k: body)
    src = mlb.MlbPlayoffSource(season=2026)
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None
