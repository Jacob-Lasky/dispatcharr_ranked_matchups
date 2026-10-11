"""A bracket fetch with a failed request is not cached, is retried a bounded
number of times, and is reported when it gives up (#245).

The ESPN day-sweep bracket sources (NBA, NFL, WNBA, NCAAW, NCAA baseball and
softball, NCAA soccer cups, MLS Cup) walk a fixed calendar window one
scoreboard request per day. A failed day was just missing games, and whatever
the sweep assembled was cached for the whole refresh: a partial bracket put
the wrong series score into initial_state, and an all-days-failed sweep cached
[], so the scoring-time re-placement added in #243 could not recover.

Rows are built the way production builds them: `fetch_upcoming` on the same
stubbed scoreboard, then the real refresh.
"""
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest

UTC = timezone.utc

from dispatcharr_ranked_matchups.sources import (
    mls_cup, mlb, nba, ncaa_baseball, ncaa_soccer_cup, ncaa_softball, ncaaw_basketball,
    nfl, nhl, wnba,
)


def _day_of(url):
    """The YYYYMMDD a scoreboard URL asks for."""
    return parse_qs(urlparse(url).query)["dates"][0]


def _nba_event(eid, home, away, start, headline, home_pts=None, away_pts=None):
    final = home_pts is not None
    return {
        "id": eid,
        "date": start.strftime("%Y-%m-%dT%H:%MZ"),
        "season": {"type": 3},
        "competitions": [{
            "type": {"abbreviation": "FINAL"},
            "notes": [{"headline": headline}],
            "status": {"type": {"completed": final, "state": "post" if final else "pre"}},
            "competitors": [
                {"homeAway": "home", "team": {"displayName": home}, "score": str(home_pts)},
                {"homeAway": "away", "team": {"displayName": away}, "score": str(away_pts)},
            ],
        }],
    }


class Scoreboard:
    """A stub `_http_get`: one ESPN scoreboard payload per YYYYMMDD, an empty
    day otherwise, and None (a failed request) for the days in `failing`
    until each has failed `fail_times` times."""

    def __init__(self, by_day=None, failing=(), fail_times=1):
        self.by_day = by_day or {}
        self.failures_left = {d: fail_times for d in failing}
        self.calls = []

    def __call__(self, url, *a, **kw):
        day = _day_of(url)
        self.calls.append(day)
        if self.failures_left.get(day, 0) > 0:
            self.failures_left[day] -= 1
            return None
        return {"events": list(self.by_day.get(day, []))}


# 2026 Western Conference Finals: Game 1 played on 2026-05-19, Game 2 on the
# 20th. The 20th failing is the whole of the issue: the series reads 1-0
# when it is 1-1.
WCF_G1 = _nba_event("401", "Oklahoma City Thunder", "Denver Nuggets",
                    datetime(2026, 5, 19, 0, 30, tzinfo=UTC), "West Finals - Game 1", 110, 101)
WCF_G2 = _nba_event("402", "Oklahoma City Thunder", "Denver Nuggets",
                    datetime(2026, 5, 21, 0, 30, tzinfo=UTC), "West Finals - Game 2", 99, 104)


@pytest.fixture
def nba_sweep(monkeypatch):
    board = Scoreboard({"20260519": [WCF_G1], "20260520": [WCF_G2]}, failing=["20260520"])
    monkeypatch.setattr(nba, "_http_get", board)
    return board, nba.NbaPlayoffSource(season_end_year=2026)


# ---------------------------------------------------------------------------
# Witness: the issue's own
# ---------------------------------------------------------------------------

def test_a_failed_day_is_not_cached_and_the_next_call_re_requests_it(nba_sweep):
    board, src = nba_sweep
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None
    assert board.calls.count("20260520") == 1

    games = src._fetch_bracket_games()
    assert board.calls.count("20260520") == 2
    assert sorted(g["game_id"] for g in games) == ["401", "402"]
    assert src._bracket_games_cache == games


def test_the_series_score_comes_from_the_complete_sweep(nba_sweep):
    """The consequence the issue names: a missing day was a wrong series score
    in initial_state. Here the scoring-time read follows a fetch-time
    failure, and the Western Conference Finals stand 1-1, not 1-0."""
    _board, src = nba_sweep
    src._fetch_bracket_games()          # fetch pass: the 20th fails
    state = src.initial_state()         # scoring pass
    tie = state["_tie_results"][("CF", frozenset({"Oklahoma City Thunder", "Denver Nuggets"}))]
    assert tie["series_wins"] == {"Oklahoma City Thunder": 1, "Denver Nuggets": 1}


# ---------------------------------------------------------------------------
# Every day-sweep source, not just NBA
# ---------------------------------------------------------------------------

def _sweep_cases():
    # (module, factory, a day inside the source's sweep window)
    return [
        (nba, lambda: nba.NbaPlayoffSource(season_end_year=2026), "20260501"),
        (nfl, lambda: nfl.NflPlayoffSource(season_end_year=2026), "20260118"),
        (wnba, lambda: wnba.WnbaPlayoffSource(season_year=2025), "20250920"),
        (ncaaw_basketball, lambda: ncaaw_basketball.NcaawBasketballPlayoffSource(season_end_year=2026),
         "20260322"),
        (ncaa_baseball, lambda: ncaa_baseball.NcaaBaseballPlayoffSource(season_year=2026), "20260606"),
        (ncaa_baseball, lambda: ncaa_baseball.NcaaBaseballPlayoffBracketSource(season_year=2026),
         "20260614"),
        (ncaa_softball, lambda: ncaa_softball.NcaaSoftballPlayoffSource(season_year=2026), "20260530"),
        (ncaa_softball, lambda: ncaa_softball.NcaaSoftballPlayoffBracketSource(season_year=2026),
         "20260601"),
        (ncaa_soccer_cup, lambda: ncaa_soccer_cup.NcaaSoccerCupSource("m", season_year=2025), "20251207"),
        (ncaa_soccer_cup, lambda: ncaa_soccer_cup.NcaaSoccerCupSource("w", season_year=2025), "20251205"),
        (mls_cup, lambda: mls_cup.MlsCupSource(season_year=2025), "20251101"),
    ]


@pytest.mark.parametrize("module,factory,bad_day", _sweep_cases(),
                         ids=lambda v: getattr(v, "__name__", None) if not isinstance(v, str) else v)
def test_every_sweep_leaves_a_failed_day_uncached(monkeypatch, module, factory, bad_day):
    """An empty postseason is cached as []; a failed day must not be, or the
    scoring retry reads the cached failure instead of asking again."""
    board = Scoreboard(failing=[bad_day])
    monkeypatch.setattr(module, "_http_get", board)
    src = factory()
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None
    assert src._fetch_bracket_games() == []
    assert board.calls.count(bad_day) == 2
    assert src._bracket_games_cache == []   # the retry answered: empty is real now


# ---------------------------------------------------------------------------
# Bounded, like the soccer season fetch (#228)
# ---------------------------------------------------------------------------

def test_the_retry_asks_only_from_the_failed_day(nba_sweep):
    board, src = nba_sweep
    src._fetch_bracket_games()
    src._fetch_bracket_games()
    assert board.calls.count("20260519") == 1
    assert board.calls.count("20260401") == 1


def test_a_sweep_stops_at_the_first_failed_day(nba_sweep):
    """The sweep is discarded either way; against an outage every further day
    would be another 15 s request timeout."""
    board, src = nba_sweep
    src._fetch_bracket_games()
    assert board.calls[-1] == "20260520"
    assert "20260521" not in board.calls


def test_a_day_that_keeps_failing_stops_hitting_the_network(monkeypatch):
    board = Scoreboard(failing=["20260520"], fail_times=99)
    monkeypatch.setattr(nba, "_http_get", board)
    src = nba.NbaPlayoffSource(season_end_year=2026)
    for _ in range(20):  # e.g. one read per importance query
        assert src._fetch_bracket_games() == []
    assert board.calls.count("20260520") == 2  # literal on purpose: the bound


def test_scoring_is_guaranteed_one_more_attempt(monkeypatch):
    board = Scoreboard(failing=["20260520"], fail_times=2)
    monkeypatch.setattr(nba, "_http_get", board)
    src = nba.NbaPlayoffSource(season_end_year=2026)
    src._fetch_bracket_games()
    src._fetch_bracket_games()
    assert src.bracket_fetch_failure() is not None
    src.allow_bracket_scoring_retry()
    assert src.bracket_fetch_failure() is None
    assert src._fetch_bracket_games() == []   # answered: an empty postseason
    assert board.calls.count("20260520") == 3
    assert src._bracket_games_cache == []


def test_a_given_up_sweep_is_reported_with_the_day(monkeypatch):
    monkeypatch.setattr(nba, "_http_get", Scoreboard(failing=["20260520"], fail_times=99))
    src = nba.NbaPlayoffSource(season_end_year=2026)
    src._fetch_bracket_games()
    assert src.bracket_fetch_failure() is None  # one failure: still retryable
    src._fetch_bracket_games()
    failure = src.bracket_fetch_failure()
    assert isinstance(failure, str) and "2026-05-20" in failure


def test_a_sweep_that_recovers_is_not_reported(monkeypatch):
    board = Scoreboard(failing=["20260520"], fail_times=2)
    monkeypatch.setattr(nba, "_http_get", board)
    src = nba.NbaPlayoffSource(season_end_year=2026)
    src._fetch_bracket_games()
    src._fetch_bracket_games()
    src.allow_bracket_scoring_retry()
    src._fetch_bracket_games()
    assert src.bracket_fetch_failure() is None


def test_an_empty_day_is_an_answer_not_a_failure(monkeypatch):
    """`{}` (a day with nothing on it) was skipped before and still is."""
    monkeypatch.setattr(nba, "_http_get", lambda *a, **kw: {})
    src = nba.NbaPlayoffSource(season_end_year=2026)
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache == []
    assert src.bracket_fetch_failure() is None


# ---------------------------------------------------------------------------
# MLB and NHL: one-request fetches get the same accounting
# ---------------------------------------------------------------------------

def test_mlb_gives_up_after_two_failed_requests(monkeypatch):
    calls = []
    monkeypatch.setattr(mlb, "_http_get", lambda *a, **kw: calls.append(1))
    src = mlb.MlbPlayoffSource(season=2026)
    for _ in range(5):
        assert src._fetch_bracket_games() == []
    assert len(calls) == 2
    assert src.bracket_fetch_failure() == "postseason schedule did not answer"


def _nhl_series(letter, top, bottom, top_wins, bottom_wins):
    return {"playoffRound": 1, "seriesLetter": letter.upper(),
            "topSeedTeam": {"placeName": {"default": top}, "commonName": {"default": "FC"}},
            "bottomSeedTeam": {"placeName": {"default": bottom}, "commonName": {"default": "FC"}},
            "topSeedWins": top_wins, "bottomSeedWins": bottom_wins}


def _nhl_stub(series, schedules):
    calls = []

    def get(url, *a, **kw):
        calls.append(url)
        if "/playoff-bracket/" in url:
            return {"series": series}
        return schedules.get(url.rsplit("/", 1)[-1])
    return get, calls


def test_nhl_a_started_series_whose_schedule_failed_is_not_cached(monkeypatch):
    """The bracket says Boston have won twice; without the schedule those wins
    are missing from the series, so the bracket is not the bracket."""
    get, _calls = _nhl_stub([_nhl_series("a", "Boston", "Buffalo", 2, 1)], {})
    monkeypatch.setattr(nhl, "_http_get", get)
    src = nhl.NhlPlayoffSource(season="20252026")
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None
    src._fetch_bracket_games()
    failure = src.bracket_fetch_failure()
    assert isinstance(failure, str) and "series a" in failure


def test_nhl_a_series_not_yet_started_may_have_no_schedule(monkeypatch):
    """A series nobody has played yet can 404 (the endpoint answers 404 for a
    series it does not know, live 2026-10-11): that is the old tolerance,
    kept, so a freshly drawn round does not void the whole bracket."""
    get, _calls = _nhl_stub([_nhl_series("i", "Boston", "Buffalo", 0, 0)], {})
    monkeypatch.setattr(nhl, "_http_get", get)
    src = nhl.NhlPlayoffSource(season="20252026")
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None       # first attempt: retried
    src.allow_bracket_scoring_retry()
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache == []         # last attempt: accepted
    assert src.bracket_fetch_failure() is None


def test_nhl_a_series_not_yet_started_gets_its_retry(monkeypatch):
    """_http_get cannot tell a 503 from an unpublished 404, so a 0-0 series
    whose schedule failed is retried before it is accepted as empty."""
    game = {"id": 2025030111, "gameNumber": 1, "gameState": "FUT",
            "startTimeUTC": "2026-04-20T23:00:00Z",
            "homeTeam": {"placeName": {"default": "Boston"}, "commonName": {"default": "FC"}},
            "awayTeam": {"placeName": {"default": "Buffalo"}, "commonName": {"default": "FC"}}}
    answers = [None, {"games": [game]}]
    get, _calls = _nhl_stub([_nhl_series("a", "Boston", "Buffalo", 0, 0)], {})

    def flaky(url, *a, **kw):
        if "/playoff-series/" in url:
            return answers.pop(0)
        return get(url, *a, **kw)
    monkeypatch.setattr(nhl, "_http_get", flaky)
    src = nhl.NhlPlayoffSource(season="20252026")
    src._fetch_bracket_games()
    assert [g["game_id"] for g in src._fetch_bracket_games()] == [2025030111]


def _nhl_game(gid, home, away):
    return {"id": gid, "gameNumber": 1, "gameState": "FUT", "startTimeUTC": "2026-04-20T23:00:00Z",
            "homeTeam": {"placeName": {"default": home}, "commonName": {"default": "FC"}},
            "awayTeam": {"placeName": {"default": away}, "commonName": {"default": "FC"}}}


def _nhl_scripted(series, answers_by_letter):
    """Bracket always answers; each series schedule pops its next scripted
    answer (None = a failed request), repeating the last one."""
    def get(url, *a, **kw):
        if "/playoff-bracket/" in url:
            return {"series": series}
        script = answers_by_letter[url.rsplit("/", 1)[-1]]
        return script.pop(0) if len(script) > 1 else script[0]
    return get


def test_nhl_a_first_failure_on_the_second_fetch_is_not_accepted_as_unpublished(monkeypatch):
    """Series a fails on the first fetch, b first fails on the second: b never
    had a retry, so it is not waved through as unpublished. The bracket gives
    up, loudly, instead of silently dropping b's games."""
    monkeypatch.setattr(nhl, "_http_get", _nhl_scripted(
        [_nhl_series("a", "Boston", "Buffalo", 0, 0), _nhl_series("b", "Tampa", "Montreal", 0, 0)],
        {"a": [None, {"games": [_nhl_game(1, "Boston", "Buffalo")]}],
         "b": [{"games": [_nhl_game(2, "Tampa", "Montreal")]}, None]},
    ))
    src = nhl.NhlPlayoffSource(season="20252026")
    assert src._fetch_bracket_games() == []
    assert src._fetch_bracket_games() == []
    assert src._bracket_games_cache is None
    failure = src.bracket_fetch_failure()
    assert isinstance(failure, str) and "series b" in failure


def test_nhl_an_unpublished_series_does_not_void_a_recovered_bracket(monkeypatch):
    """A transient failure on series a and an unpublished series i, both on
    the first fetch: the second fetch keeps a's games and accepts i as empty."""
    monkeypatch.setattr(nhl, "_http_get", _nhl_scripted(
        [_nhl_series("a", "Boston", "Buffalo", 0, 0), _nhl_series("i", "Tampa", "Montreal", 0, 0)],
        {"a": [None, {"games": [_nhl_game(1, "Boston", "Buffalo")]}], "i": [None]},
    ))
    src = nhl.NhlPlayoffSource(season="20252026")
    src._fetch_bracket_games()
    assert [g["game_id"] for g in src._fetch_bracket_games()] == [1]
    assert src.bracket_fetch_failure() is None


def test_nhl_a_series_that_answered_in_between_is_owed_a_fresh_retry(monkeypatch):
    """a fails, answers, then fails again: the last failure is a new one, not
    the 'unpublished' repeat, so a's known game is not silently dropped."""
    a_games = {"games": [_nhl_game(1, "Boston", "Buffalo")]}
    b_games = {"games": [_nhl_game(2, "Tampa", "Montreal")]}
    monkeypatch.setattr(nhl, "_http_get", _nhl_scripted(
        [_nhl_series("a", "Boston", "Buffalo", 0, 0), _nhl_series("b", "Tampa", "Montreal", 0, 0)],
        {"a": [None, a_games, None], "b": [b_games, None, b_games]},
    ))
    src = nhl.NhlPlayoffSource(season="20252026")
    src._fetch_bracket_games()          # a fails
    src._fetch_bracket_games()          # a answers, b fails
    src.allow_bracket_scoring_retry()
    assert src._fetch_bracket_games() == []   # a fails afresh: not waved through
    assert src._bracket_games_cache is None


def test_scoring_retry_is_not_defeated_by_a_state_cached_after_giving_up(monkeypatch):
    """Two failed initial_state() reads spend the budget, and the second
    caches its empty state. The scoring retry must drop that state and ask
    again, or the outage is both unrecovered and unreported."""
    answers = [None, None]
    payload = {"dates": [{"games": [{
        "gamePk": 7, "gameDate": "2026-10-20T23:00:00Z", "seriesDescription": "World Series",
        "seriesGameNumber": 1, "gamesInSeries": 7, "status": {"abstractGameState": "Preview"},
        "teams": {"home": {"team": {"name": "Toronto Blue Jays"}},
                  "away": {"team": {"name": "Los Angeles Dodgers"}}},
    }]}]}
    calls = []

    def get(*a, **kw):
        calls.append(1)
        return answers.pop(0) if answers else payload
    monkeypatch.setattr(mlb, "_http_get", get)
    src = mlb.MlbPlayoffSource(season=2026)
    src.initial_state()
    src.initial_state()
    src.allow_bracket_scoring_retry()
    assert src.initial_state()["_bracket"].get("WS")
    assert len(calls) == 3
    assert src.bracket_fetch_failure() is None


# ---------------------------------------------------------------------------
# End to end through refresh: retried at scoring, reported when given up
# ---------------------------------------------------------------------------

@pytest.fixture
def nba_live(monkeypatch):
    """NBA Finals Game 1 tomorrow, listed by the upcoming sweep and by the
    bracket sweep; the bracket's 2026-05-20 day fails `fail_times` times."""
    def make(fail_times):
        tomorrow = datetime.now(UTC) + timedelta(days=1)
        finals = _nba_event("9001", "Oklahoma City Thunder", "New York Knicks",
                            tomorrow, "NBA Finals - Game 1")
        board = Scoreboard({tomorrow.strftime("%Y%m%d"): [finals], "20260604": [finals]},
                           failing=["20260520"], fail_times=fail_times)
        monkeypatch.setattr(nba, "_http_get", board)
        return board, nba.NbaPlayoffSource(season_end_year=2026)
    return make


def test_refresh_places_the_rows_when_scorings_retry_succeeds(nba_live, refresh_env):
    _board, src = nba_live(fail_times=1)
    _write_prior, run, read = refresh_env
    result = run([src])
    assert result["status"] == "ok", result
    cache = read()
    assert [g["extra"].get("stage") for g in cache["games"]] == ["FINALS"]
    assert not any("bracket (stakes not simulated)" in s for s in cache["summary"])


def test_refresh_hands_scoring_a_retry_after_the_budget_is_spent(nba_live, refresh_env):
    """Both attempts gone before scoring (an earlier read, then the fetch-time
    placement): refresh still grants scoring its own, and it succeeds."""
    board, src = nba_live(fail_times=2)
    src._fetch_bracket_games()  # an earlier read spends the first attempt
    _write_prior, run, read = refresh_env
    run([src])
    cache = read()
    assert board.calls.count("20260520") == 3
    assert [g["extra"].get("stage") for g in cache["games"]] == ["FINALS"]
    assert not any("bracket (stakes not simulated)" in s for s in cache["summary"])


def test_an_early_initial_state_read_does_not_pin_the_failure(nba_sweep):
    """initial_state() caches what it builds; built from a failed sweep while a
    retry is still allowed, it must not, or importance reads the failure."""
    _board, src = nba_sweep
    early = src.initial_state()
    assert not early["_tie_results"].get(("CF", frozenset({"Oklahoma City Thunder", "Denver Nuggets"})))
    tie = src.initial_state()["_tie_results"][("CF", frozenset({"Oklahoma City Thunder", "Denver Nuggets"}))]
    assert tie["series_wins"] == {"Oklahoma City Thunder": 1, "Denver Nuggets": 1}


def _mls_event(eid, home, away, start, slug):
    return {
        "id": eid,
        "date": start.strftime("%Y-%m-%dT%H:%MZ"),
        "season": {"slug": slug},
        "competitions": [{
            "status": {"type": {"completed": False, "state": "pre"}},
            "competitors": [
                {"homeAway": "home", "team": {"displayName": home}},
                {"homeAway": "away", "team": {"displayName": away}},
            ],
        }],
    }


def test_a_game_on_the_failed_day_gets_its_importance_after_the_retry(monkeypatch):
    """The importance half, measured: MLS Cup is on the day that failed at
    fetch. Cached, the bracket had no final and the game scored no stakes;
    retried at scoring it decides the Cup. (MLS Cup, not NBA: the NBA, NFL,
    WNBA and NCAAW playoff sources have no sample_result.)"""
    import random
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, compute_match_importance
    start = datetime.now(UTC) + timedelta(days=1)
    final = _mls_event("7001", "LA Galaxy", "Columbus Crew", start, "mls-cup")
    board = Scoreboard({start.strftime("%Y%m%d"): [final], "20251206": [final]}, failing=["20251206"])
    monkeypatch.setattr(mls_cup, "_http_get", board)
    src = mls_cup.MlsCupSource(season_year=2025)
    rows = src.fetch_upcoming(days_ahead=2)
    src.place_upcoming(rows)                          # fetch: the final's day failed
    assert "bracket_game_index" not in rows[0].extra
    src.allow_bracket_scoring_retry()
    assert src.place_upcoming(rows, for_scoring=True) == 0
    assert rows[0].extra.get("bracket_game_index") == 1
    points, _notes, _bands = compute_match_importance(
        src, rows[0], LEAGUE_CONTEXTS["MLS_PO"], n_sims=400, rng=random.Random(5),
    )
    assert points > 0.5


def test_two_sources_sharing_a_label_report_one_failure(nba_live, refresh_env):
    """The NCAA Baseball Postseason pair share a label: one outage is one
    failed entry, not two."""
    _board, first = nba_live(fail_times=99)
    second = nba.NbaPlayoffSource(season_end_year=2026)
    _write_prior, run, read = refresh_env
    run([first, second])
    entries = [s for s in read()["summary"] if "bracket (stakes not simulated)" in s]
    assert entries == [
        "NBA Playoffs bracket (stakes not simulated): error "
        "(scoreboard for 2026-05-20 did not answer, day 50 of 92)"
    ]


def test_refresh_reports_a_bracket_that_gave_up(nba_live, refresh_env, plugin):
    board, src = nba_live(fail_times=99)
    _write_prior, run, read = refresh_env
    run([src])
    cache = read()
    assert board.calls.count("20260520") == 2  # fetch, then scoring's retry
    entry = [s for s in cache["summary"] if "bracket (stakes not simulated)" in s]
    assert entry == [
        "NBA Playoffs bracket (stakes not simulated): error "
        "(scoreboard for 2026-05-20 did not answer, day 50 of 92)"
    ]
    problems = plugin._slate_problems(cache["summary"], cache["games"])
    assert any("NBA Playoffs bracket" in p for p in problems)


def test_only_the_base_class_writes_the_bracket_cache():
    """`_cache_bracket_games` is the one writer, because it is also what
    clears the failure count. A source assigning the cache directly is how a
    partial sweep got cached in the first place."""
    import os
    import re
    root = os.path.join(os.path.dirname(__file__), "..", "sources")
    writers = []
    for name in sorted(os.listdir(root)):
        if not name.endswith(".py") or name == "bracket.py":
            continue
        with open(os.path.join(root, name)) as fh:
            for n, line in enumerate(fh, 1):
                if re.search(r"_bracket_games_cache\s*=(?!=)", line) and not re.search(
                        r"=\s*None\s*$", line):
                    writers.append(f"{name}:{n}")
    assert writers == []
