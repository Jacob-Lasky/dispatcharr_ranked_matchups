"""The outside-API call budget (#363): the ledger that measures it, the kept schedule
answers that bound it, and the per-source budgets around them.

Measured 2026-11-28 on Tower before this change: one refresh sent 1,331 requests and
about twelve refreshes ran a day. The day test below drives a real ESPN source through
a day of refreshes and pins what a day costs now.
"""
import ast
import importlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.conftest import REPO_ROOT, stub_requests_get, stub_urlopen

UTC = timezone.utc
NOW = datetime(2026, 11, 28, 15, 0, tzinfo=UTC)   # 10:00 US Eastern
PKG = "dispatcharr_ranked_matchups"


@pytest.fixture
def cache():
    return importlib.import_module(f"{PKG}._api_cache")


@pytest.fixture
def ledger():
    return importlib.import_module(f"{PKG}._api_ledger")


class _Resp:
    def __init__(self, body, status=200):
        self._body = body
        self.status_code = status
        self.headers = {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code), response=self)


# ---------- the ledger ----------

@pytest.mark.parametrize("url, expected", [
    ("https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard?dates=20261128&limit=1000",
     "site.api.espn.com soccer/usa.N/scoreboard"),
    # TheSportsDB carries its key in the PATH; Kuma its push token; Odds API in the query.
    ("https://www.thesportsdb.com/api/v1/json/a1b2c3d4e5f6/searchevents.php?e=x",
     "www.thesportsdb.com json/*/searchevents.php"),
    ("http://172.17.0.1:3001/api/push/4Sayz9QKKZabcdef?status=up&msg=x", "172.17.0.1 api/push/*"),
    ("https://api.the-odds-api.com/v4/sports/soccer_epl/odds/?apiKey=SECRETKEY123",
     "api.the-odds-api.com sports/soccer_epl/odds"),
    ("https://api-web.nhle.com/v1/club-schedule-season/TOR/20252026",
     "api-web.nhle.com club-schedule-season/TOR/N"),
    # Letters-only tokens: mixed case, or by their position after push/json.
    ("http://kuma:3001/api/push/AbCdEfGhIjKl", "kuma api/push/*"),
    ("http://kuma:3001/api/push/abcdefghij", "kuma api/push/*"),
    ("https://www.thesportsdb.com/api/v1/json/abcdefgh/lookupleague.php",
     "www.thesportsdb.com json/*/lookupleague.php"),
])
def test_an_endpoint_never_carries_a_credential(ledger, url, expected):
    assert ledger.endpoint(url) == expected


def test_both_senders_count_every_request_by_trigger(ledger, monkeypatch):
    """`_http.get_within_deadline` and `urlopen_read` are the only senders; each
    request is counted once, under the trigger the run kind names."""
    from dispatcharr_ranked_matchups import _http
    import requests
    stub_requests_get(monkeypatch, lambda url, **k: _Resp({"events": []}))
    stub_urlopen(monkeypatch, lambda request, timeout=None: _FakeUrlopen())
    monkeypatch.setenv(ledger.TRIGGER_ENV, "scheduled_auto_pipeline")
    _http.get_within_deadline(requests, "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
                              timeout=5)
    _http.get_within_deadline(requests, "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
                              timeout=5)
    monkeypatch.delenv(ledger.TRIGGER_ENV)
    _http.urlopen_read("https://api.anthropic.com/v1/messages", timeout=5)
    today = ledger.day_counts(datetime.now(UTC).strftime("%Y-%m-%d"))
    assert today == {
        "scheduled_auto_pipeline": {"site.api.espn.com football/nfl/scoreboard": 2},
        "service": {"api.anthropic.com vN/messages": 1},
    }
    lines = ledger.summary_lines()
    assert lines and lines[0].startswith("Outside API calls today so far (UTC): 3 (site.api.espn.com 2")


class _FakeUrlopen:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, *a):
        return b"{}"


def test_a_malformed_ledger_never_fails_a_request(ledger, monkeypatch):
    """A valid JSON ledger of the wrong shape (a list under today) is repaired, and
    any error in counting is swallowed: counting must never stop the request."""
    from dispatcharr_ranked_matchups import _http
    import requests
    os.makedirs(os.path.dirname(ledger.LEDGER_PATH), exist_ok=True)
    with open(ledger.LEDGER_PATH, "w", encoding="utf-8") as f:
        json.dump({"v": 1, "days": {datetime.now(UTC).strftime("%Y-%m-%d"): []}}, f)
    stub_requests_get(monkeypatch, lambda url, **k: _Resp({"events": []}))
    assert _http.get_within_deadline(requests, "https://example.com/a", timeout=5).status_code == 200
    assert ledger.day_counts(datetime.now(UTC).strftime("%Y-%m-%d")) == {"service": {"example.com a": 1}}


def test_the_ledger_keeps_two_weeks(ledger):
    for days_ago in (15, 13, 0):
        ledger.record("https://example.com/a", now=NOW - timedelta(days=days_ago))
    with open(ledger.LEDGER_PATH, encoding="utf-8") as f:
        days = json.load(f)["days"]
    assert sorted(days) == ["2026-11-15", "2026-11-28"]


# ---------- the kept-answer policy ----------

def _fresh(cache, url, fetched, now, params=None, lifetime=timedelta(hours=12)):
    return cache.schedule_fresh(cache.request_days(url, params), lifetime)(fetched, now)


ESPN = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={}&limit=1000"


@pytest.mark.parametrize("day, age, fresh", [
    ("20261128", timedelta(hours=3, minutes=59), True),    # today: 4 h
    ("20261128", timedelta(hours=4), False),
    ("20261127", timedelta(hours=3), True),                # yesterday is near too
    ("20261129", timedelta(hours=5), False),               # tomorrow is near too
    ("20261205", timedelta(hours=23), True),               # within two weeks: a day
    ("20261205", timedelta(hours=25), False),
    ("20261231", timedelta(days=6), True),                 # further out: a week
    ("20261231", timedelta(days=8), False),
])
def test_a_day_is_kept_by_how_far_it_is_from_today(cache, day, age, fresh):
    assert _fresh(cache, ESPN.format(day), NOW - age, NOW) is fresh


def test_a_finished_day_read_after_it_ended_is_kept_for_good(cache):
    read_after = datetime(2026, 11, 22, 15, 0, tzinfo=UTC)
    assert _fresh(cache, ESPN.format("20261120"), read_after, NOW)
    # Read while it was still today: asked once more, and that read is final.
    read_during = datetime(2026, 11, 20, 18, 0, tzinfo=UTC)
    assert not _fresh(cache, ESPN.format("20261120"), read_during, NOW)


def test_a_future_whole_season_range_takes_the_callers_lifetime(cache):
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2027-03-01&endDate=2027-10-31"
    assert _fresh(cache, url, NOW - timedelta(hours=11), NOW)
    assert not _fresh(cache, url, NOW - timedelta(hours=13), NOW)


def test_a_long_upcoming_window_that_touches_today_is_near(cache):
    """WITNESS (round-1 review): a 14-day MLB lookahead plus the lookback day is a
    16-day range; it was read as a whole season and kept 12 h, so a same-day
    postponement stayed published 8 h past the 4-hour near lifetime."""
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2026-11-27&endDate=2026-12-12"
    assert not _fresh(cache, url, NOW - timedelta(hours=5), NOW)


def test_request_days_reads_every_dated_shape(cache):
    assert cache.request_days(ESPN.format("20261128")) == (date(2026, 11, 28), date(2026, 11, 28))
    assert cache.request_days("https://api-web.nhle.com/v1/schedule/2026-11-28") == (
        date(2026, 11, 28), date(2026, 12, 4))
    assert cache.request_days("https://api.football-data.org/v4/matches",
                              {"dateFrom": "2026-11-27", "dateTo": "2026-12-07"}) == (
        date(2026, 11, 27), date(2026, 12, 6))
    assert cache.request_days("https://site.api.espn.com/apis/v2/sports/soccer/usa.1/standings") is None


def test_an_answer_from_the_future_is_asked_again(cache):
    assert not _fresh(cache, "https://x/standings", NOW + timedelta(hours=1), NOW)


@pytest.mark.parametrize("url, body, ok", [
    (ESPN.format("20261128"), {"events": []}, True),
    (ESPN.format("20261128"), {"code": 404}, False),
    ("https://statsapi.mlb.com/api/v1/schedule?sportId=1", {"dates": []}, True),
    ("https://statsapi.mlb.com/api/v1/schedule?sportId=1", {"message": "nope"}, False),
    ("https://sports.core.api.espn.com/v2/sports/soccer/leagues/x/seasons/2026", {"types": {}}, True),
    ("https://sports.core.api.espn.com/v2/sports/soccer/leagues/x/seasons/2026", {"error": "x"}, False),
])
def test_an_error_body_is_never_kept(cache, url, body, ok):
    assert cache.looks_like_answer(url, body) is ok


def test_a_credential_url_is_never_keyed(cache):
    assert cache.request_key("https://x/odds/?apiKey=abc") is None
    assert cache.request_key("https://x/a", {"token": "t"}) is None
    assert cache.request_key(ESPN.format("20261128")) is not None


# ---------- http_get_json(keep=True) ----------

def test_a_kept_answer_is_served_without_a_request_and_a_failure_is_not_kept(cache, monkeypatch):
    from dispatcharr_ranked_matchups import _util
    import requests
    calls = []
    answers = [None, {"events": [1]}]

    def get(url, **k):
        calls.append(url)
        body = answers.pop(0)
        return _Resp({}, status=503) if body is None else _Resp(body)

    stub_requests_get(monkeypatch, get)
    url = ESPN.format("20261128")
    log = logging.getLogger("t")
    assert _util.http_get_json(requests, url, log=log, tag="t", keep=True) is None   # failed
    assert _util.http_get_json(requests, url, log=log, tag="t", keep=True) == {"events": [1]}
    assert _util.http_get_json(requests, url, log=log, tag="t", keep=True) == {"events": [1]}
    assert len(calls) == 2


def test_an_error_body_is_returned_but_not_kept(monkeypatch):
    from dispatcharr_ranked_matchups import _util
    import requests
    calls = []
    stub_requests_get(monkeypatch, lambda url, **k: (calls.append(url), _Resp({"code": 404}))[1])
    url = ESPN.format("20261128")
    for _ in range(2):
        assert _util.http_get_json(requests, url, log=logging.getLogger("t"), tag="t", keep=True) == {"code": 404}
    assert len(calls) == 2


def _keep_callers():
    """{file: [function]} for every `http_get_json(..., keep=True)` call in the plugin."""
    found = {}
    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in {"tests", "tools", ".git", "__pycache__"}]
        for fn in files:
            if not fn.endswith(".py"):
                continue
            path = os.path.join(root, fn)
            tree = ast.parse(open(path, encoding="utf-8").read())
            for func in ast.walk(tree):
                if not isinstance(func, ast.FunctionDef):
                    continue
                for call in ast.walk(func):
                    if (isinstance(call, ast.Call) and getattr(call.func, "id", None) == "http_get_json"
                            and any(k.arg == "keep" for k in call.keywords)):
                        found.setdefault(os.path.relpath(path, REPO_ROOT), []).append(func.name)
    return found


def test_only_the_schedule_sources_keep_answers():
    """The ESPN final check needs the LIVE scoreboard (a recording stops within a
    minute of the whistle), and the outage slate the live provider: neither may
    ever read a kept answer. Only the schedule sources' `_http_get` wrappers do."""
    found = _keep_callers()
    assert found, "the scan found no caller at all"   # a zero is a claim about the scan
    assert "recording_finals.py" not in found and "slate.py" not in found
    assert all(path.startswith("sources" + os.sep) and names == ["_http_get"]
               for path, names in found.items()), found
    assert len(found) == 16, sorted(found)


# ---------- a day of refreshes through a real source ----------

def test_a_day_of_refreshes_through_nfl_costs_a_fraction_of_one_refresh(cache, monkeypatch):
    """Twelve refreshes across a day (Tower's measured rate), each doing what a refresh
    does for NFL: the upcoming sweep and the whole-season walk. Before #363 every
    refresh re-sent every request; now a day costs the first read plus the near days
    every 4 h and the next two weeks once."""
    nfl = importlib.import_module(f"{PKG}.sources.nfl")
    espn = importlib.import_module(f"{PKG}.sources._espn")
    calls = []
    stub_requests_get(monkeypatch, lambda url, **k: (calls.append(url), _Resp({"events": []}))[1])
    clock = {"now": NOW}
    monkeypatch.setattr(cache, "_now", lambda: clock["now"])

    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"] if tz is not None else clock["now"].replace(tzinfo=None)

    monkeypatch.setattr(espn, "datetime", _Clock)
    per_refresh = []
    for step in range(12):
        clock["now"] = NOW + timedelta(hours=2 * step)
        before = len(calls)
        src = nfl.NflRegularSource()
        src.fetch_upcoming(days_ahead=7)
        src._fetch_games_cached()
        per_refresh.append(len(calls) - before)
    print("NFL requests per refresh over a day:", per_refresh)
    first, rest = per_refresh[0], per_refresh[1:]
    assert first > 100                       # the whole season, once
    assert per_refresh[1] == 0               # two hours later: nothing changed
    # The near days (yesterday..tomorrow) at most every 4 h, the rest of the next
    # two weeks once, finished days never again: a few dozen, not 11 x first.
    assert sum(rest) <= 40, per_refresh


# ---------- Football-Data ----------

def test_football_data_answers_are_kept_and_failures_are_not(monkeypatch):
    soccer = importlib.import_module(f"{PKG}.sources.soccer")
    calls = []
    answers = [_Resp({"errorCode": 429}, status=503), _Resp({"matches": [{"id": 1}]})]
    stub_requests_get(monkeypatch, lambda url, **k: (calls.append(url), answers.pop(0))[1])
    monkeypatch.setattr(soccer, "_FD_MAX_ATTEMPTS", 1)
    url = f"{soccer.FD_BASE}/competitions/PL/matches"
    with pytest.raises(Exception):
        soccer._fd_get_json(url, "k", keep_for=soccer._FD_CURRENT_KEEP, check=soccer._require_list("matches"))
    for _ in range(3):
        assert soccer._fd_get_json(url, "k", keep_for=soccer._FD_CURRENT_KEEP,
                                   check=soccer._require_list("matches")) == {"matches": [{"id": 1}]}
    assert len(calls) == 2


def test_a_football_data_answer_the_check_refuses_is_not_kept(monkeypatch):
    soccer = importlib.import_module(f"{PKG}.sources.soccer")
    calls = []
    stub_requests_get(monkeypatch, lambda url, **k: (calls.append(url), _Resp({"message": "x"}))[1])
    url = f"{soccer.FD_BASE}/competitions/PL/standings"
    for _ in range(2):
        with pytest.raises(ValueError):
            soccer._fd_get_json(url, "k", keep_for=soccer._FD_CURRENT_KEEP, check=soccer._require_list("standings"))
    assert len(calls) == 2


# ---------- the matcher's Claude answers ----------

def test_the_matcher_asks_claude_once_per_game_and_candidate_set(monkeypatch):
    from dispatcharr_ranked_matchups import matcher
    from tests.test_matcher import _Game, _cand
    asked = []

    def post(api_key, model, system, user, timeout=60):
        asked.append(json.loads(user.split("\n\n", 1)[1]))
        return {"0": 100}

    monkeypatch.setattr(matcher, "_post_claude", post)
    cands = [_cand(100, "ESPN", "Penn State at Ohio State"), _cand(101, "FOX", "Penn State vs Ohio State")]
    games = [(_Game("Penn State", "Ohio State"), None, None)]
    for _ in range(3):
        result = matcher.match_games_to_channels(games, lambda g: cands, api_key="x", model="m")
        assert result[0].method == "llm" and result[0].channel_id == 100
    assert len(asked) == 1
    # A changed candidate list is a different question.
    cands.append(_cand(102, "ABC", "Penn State v Ohio State"))
    matcher.match_games_to_channels(games, lambda g: cands, api_key="x", model="m")
    assert len(asked) == 2


def test_an_unusable_matcher_answer_is_not_kept(monkeypatch):
    """A pick outside the candidate list, or a game the answer omitted, is asked again;
    only an explicit null or a candidate id is kept."""
    from dispatcharr_ranked_matchups import matcher
    from tests.test_matcher import _Game, _cand
    answers = [{"0": 999}, {}, {"0": None}, {"0": 101}]
    monkeypatch.setattr(matcher, "_post_claude", lambda *a, **k: answers.pop(0))
    cands = [_cand(100, "ESPN", "Penn State at Ohio State"), _cand(101, "FOX", "Penn State vs Ohio State")]
    games = [(_Game("Penn State", "Ohio State"), None, None)]
    for _ in range(4):
        matcher.match_games_to_channels(games, lambda g: cands, api_key="x", model="m")
    assert answers == [{"0": 101}]   # the explicit null was kept, so the fourth run asked nothing


def test_a_failed_matcher_call_is_not_kept(monkeypatch):
    from dispatcharr_ranked_matchups import matcher
    from tests.test_matcher import _Game, _cand
    answers = [None, {"0": 101}]
    monkeypatch.setattr(matcher, "_post_claude", lambda *a, **k: answers.pop(0))
    cands = [_cand(100, "ESPN", "Penn State at Ohio State"), _cand(101, "FOX", "Penn State vs Ohio State")]
    games = [(_Game("Penn State", "Ohio State"), None, None)]
    assert matcher.match_games_to_channels(games, lambda g: cands, api_key="x", model="m")[0].channel_id is None
    assert matcher.match_games_to_channels(games, lambda g: cands, api_key="x", model="m")[0].channel_id == 101
    assert answers == []


# ---------- LLM descriptions ----------

def test_an_importance_band_flip_does_not_re_ask_for_a_description():
    from dispatcharr_ranked_matchups import llm_descriptions as llm
    from tests.test_llm_descriptions import _sample_game
    g = _sample_game()
    flipped = {**g, "importance_thresholds_hit": ["relegation", "europa"]}
    key = lambda game, tagline: llm.description_cache_key("k", llm.build_llm_context(game, tagline), game, "m")
    assert "Outcome bands in play" in llm.build_llm_context(g, "relegation six-pointer")
    assert key(g, "relegation six-pointer") == key(flipped, "survival fight")
    # A real change of stakes (the table moved) still asks again.
    moved = json.loads(json.dumps(g))
    moved["extra"]["standings_table"][4]["points"] = 52
    moved["extra"]["standings_table_current"] = moved["extra"]["standings_table"]
    g2 = {**g, "extra": {**g["extra"], "standings_table_current": g["extra"]["standings_table"]}}
    assert key(g2, "x") != key(moved, "x")
    # The last band dropping out is still the same question (round-2 review)...
    no_bands = {**g, "importance_thresholds_hit": []}
    assert key(g, "relegation six-pointer") == key(no_bands, "toss-up")
    # ...while the band-free frame's real inputs are keyed: a top-10 rank pair.
    ranked = {**no_bands, "extra": {**g["extra"], "rank_source": "poll"}, "rank_home": 3, "rank_away": 8}
    assert key(no_bands, "x") != key(ranked, "x")


# ---------- odds ----------

def test_odds_stop_at_the_monthly_attempt_ceiling(monkeypatch, caplog):
    odds = importlib.import_module(f"{PKG}.sources._odds")
    calls = []
    stub_requests_get(monkeypatch, lambda url, **k: (calls.append(url), _Resp([]))[1])
    monkeypatch.setattr(odds, "MONTHLY_ATTEMPTS", 3)
    for i in range(5):
        odds.fetch_odds_events(f"sport_{i}", "key", "eu", log_tag="t", now=NOW)
    assert len(calls) == 3
    assert "monthly budget is used up" in caplog.text
    odds.fetch_odds_events("sport_9", "key", "eu", log_tag="t", now=NOW + timedelta(days=5))
    assert len(calls) == 4   # December is a new month


# ---------- quota holds and health ----------

def test_a_quota_hold_is_degraded_not_a_failure(plugin):
    summary = [plugin._source_quota_entry("NCAA Football", "not asked: used up until 2026-12-01")
               + "; no previous games to hold", "NHL: 4 games"]
    problems = plugin._slate_problems(summary, [{"channel_id": 1}])
    assert problems == ["DEGRADED: 1 source(s) held on their API's free-tier quota (NCAA Football); "
                        "no new games from them until it resets (2026-12-01)"]


def test_the_hold_counts_only_games_it_actually_kept(plugin):
    now = NOW
    held = {
        "a": {"sport_label": "NCAA Football", "sport_prefix": "NCAAF",
              "start_time_utc": (now + timedelta(hours=2)).isoformat()},
        "b": {"sport_label": "NCAA Football", "sport_prefix": "NCAAF",
              "start_time_utc": (now - timedelta(days=2)).isoformat()},   # long over
        "c": {"sport_label": "NHL", "sport_prefix": "NHL", "start_time_utc": now.isoformat()},
    }
    assert plugin._quota_hold_tail("NCAA Football", held, now) == "; 1 previous game(s) held this refresh"
    assert plugin._quota_hold_tail("NCAA Men's Basketball", held, now) == "; no previous games to hold"


def test_a_down_heartbeat_names_its_monitor_at_warning(monkeypatch, caplog):
    from dispatcharr_ranked_matchups import health
    stub_requests_get(monkeypatch, lambda url, **k: _Resp({"ok": True}))
    with caplog.at_level(logging.INFO):
        health.push("http://kuma/api/push/x", ["DEGRADED: 1 source(s) held"], "ok")
        health.push("http://kuma/api/push/y", [], "M3U and EPG refreshes healthy", monitor="refresh")
    down, up = [r for r in caplog.records if "health" in r.name]
    assert down.levelno == logging.WARNING and "pipeline monitor DOWN: DEGRADED" in down.getMessage()
    assert up.levelno == logging.INFO and "refresh monitor UP" in up.getMessage()
