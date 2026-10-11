"""A failed rankings request is an outage, not "no poll" (#283).

College sources rank their games from a poll fetched separately (ESPN
`/rankings` for NCAA baseball, softball, women's basketball and soccer;
CollegeFootballData and CollegeBasketballData `/rankings` for NCAAF and NCAAM),
and the Football-Data leagues rank theirs by the league table (`/standings`).
A failed or malformed request used to read as "nothing ranked": every game
went out unranked, ranked matchups scored lower and dropped down or off the
slate, and the refresh summary said nothing. Now the source RAISES
`RankingsUnavailable` out of `fetch_upcoming`, which the refresh counts as an
outage and answers by holding that source's previous games (#240).

Three layers:

- per-source WITNESSES, red on the pre-fix code as failed assertions (the
  fetch returned rows instead of raising);
- GUARDS that an answered empty poll stays quiet, and that the ESPN poll is
  not asked for at all when there is no game to rank (college baseball and
  softball answer 404 all offseason, measured 2026-10-11);
- the CLASS guard: every source `_build_sources` builds with everything on,
  run with every rank feed failing, must raise if it asked for one.
"""
from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

from dispatcharr_ranked_matchups.sources.base import RankingsUnavailable
from tests.conftest import stub_requests_get
from tests.test_hold_failed_days import NOON, _Resp, _pin_window, _standings
from tests.test_upcoming_window import _event

UTC = timezone.utc
PKG = "dispatcharr_ranked_matchups.sources"

# The pinned clock's tomorrow (NOON is 2026-10-11T16:00Z).
TOMORROW = datetime(2026, 10, 12, 19, 0, tzinfo=UTC)
TOMORROW_BUCKET = "20261012"

# ESPN /rankings page, trimmed to what the parser reads: two polls, the AP
# one second, so a source preferring AP is seen to choose it.
_POLL_PAGE = {"rankings": [
    {"name": "Coaches Poll", "shortName": "Coaches Poll", "ranks": [
        {"current": 3, "team": {"location": "Alpha"}},
    ]},
    {"name": "AP Top 25", "shortName": "AP Poll", "ranks": [
        {"current": 1, "team": {"location": "Alpha"}},
        {"current": 7, "team": {"location": "Beta"}},
    ]},
]}

# (module, class, constructor kwargs, scoreboard URL fragment, rank of Alpha)
_ESPN_POLL_SOURCES = [
    ("ncaa_baseball", "NcaaBaseballRegularSource", {}, "college-baseball", 3),
    ("ncaa_softball", "NcaaSoftballRegularSource", {}, "college-softball", 3),
    ("ncaaw_basketball", "NcaawBasketballRegularSource", {}, "womens-college-basketball", 1),
    ("ncaa_soccer", "NcaaSoccerSource", {"gender": "m"}, "usa.ncaa.m.1", 3),
    ("ncaa_soccer", "NcaaSoccerSource", {"gender": "w"}, "usa.ncaa.w.1", 3),
]
_IDS = [f"{cls}{kw}" for _m, cls, kw, _u, _r in _ESPN_POLL_SOURCES]


def _espn_source(monkeypatch, module, cls, kwargs, poll, games=True):
    """The source with `_http_get` serving one game tomorrow (when `games`)
    and `poll` for /rankings: a body, or None for a failed request. Returns
    (source, the rankings requests made)."""
    mod = importlib.import_module(f"{PKG}.{module}")
    _pin_window(monkeypatch, NOON)
    asked = []

    def http_get(url, *a, **k):
        if url.endswith("/rankings"):
            asked.append(url)
            return poll
        if "/seasons/" in url:
            return None   # no season calendar: NCAA soccer's fixed cutoff
        if games and f"dates={TOMORROW_BUCKET}" in url:
            return {"events": [_event("g1", "Alpha", "Beta", TOMORROW, "pre")]}
        return {"events": []}

    monkeypatch.setattr(mod, "_http_get", http_get)
    src = getattr(mod, cls)(**kwargs)
    if cls == "NcaaSoccerSource":
        # The College Cup cutoff moves with the real calendar; past it a
        # game tomorrow belongs to NcaaSoccerCupSource. Not under test here.
        monkeypatch.setattr(src, "_regular_season_cutoff",
                            lambda: datetime(2100, 1, 1, tzinfo=UTC))
    return src, asked


@pytest.mark.parametrize("module,cls,kwargs,_frag,_rank", _ESPN_POLL_SOURCES, ids=_IDS)
@pytest.mark.parametrize("poll", [None, {}, {"code": 404, "message": "x"},
                                  {"rankings": [{"name": "AP Top 25"}]},
                                  {"rankings": [None]},
                                  {"rankings": [{"name": "AP Top 25", "ranks": [{"team": {}}]}]}],
                         ids=["failed", "empty-object", "error-body", "poll-without-ranks",
                              "poll-not-an-object", "no-readable-rank"])
def test_a_failed_poll_raises(monkeypatch, module, cls, kwargs, _frag, _rank, poll):
    """WITNESS: the poll request failed (or answered something that is not a
    poll), `_fetch_rankings` returned {}, and the game went out unranked."""
    src, asked = _espn_source(monkeypatch, module, cls, kwargs, poll)
    with pytest.raises(RankingsUnavailable):
        src.fetch_upcoming(days_ahead=2)
    assert len(asked) == 1


@pytest.mark.parametrize("module,cls,kwargs,_frag,rank", _ESPN_POLL_SOURCES, ids=_IDS)
def test_an_answered_poll_ranks_the_game(monkeypatch, module, cls, kwargs, _frag, rank):
    """Guard: the poll still ranks the game (the AP poll where a source
    prefers it, the first listed otherwise), and an unranked team stays None."""
    src, _asked = _espn_source(monkeypatch, module, cls, kwargs, _POLL_PAGE)
    rows = src.fetch_upcoming(days_ahead=2)
    assert [(r.home, r.rank_home, r.away) for r in rows] == [("Alpha", rank, "Beta")]
    assert rows[0].rank_away == (7 if rank == 1 else None)


@pytest.mark.parametrize("module,cls,kwargs,_frag,_rank", _ESPN_POLL_SOURCES, ids=_IDS)
def test_an_answered_poll_with_no_polls_is_quiet(monkeypatch, module, cls, kwargs, _frag, _rank):
    """Guard: an answered page listing no poll is "nothing ranked yet"."""
    src, _asked = _espn_source(monkeypatch, module, cls, kwargs, {"rankings": []})
    rows = src.fetch_upcoming(days_ahead=2)
    assert [(r.home, r.rank_home, r.rank_away) for r in rows] == [("Alpha", None, None)]


@pytest.mark.parametrize("module,cls,kwargs,_frag,_rank", _ESPN_POLL_SOURCES, ids=_IDS)
def test_no_game_means_no_poll_request(monkeypatch, module, cls, kwargs, _frag, _rank):
    """Guard: ESPN's college baseball and softball /rankings answer 404 all
    offseason (measured 2026-10-11), when no game is listed either. Asked
    with nothing to rank, that 404 would report an outage every refresh."""
    src, asked = _espn_source(monkeypatch, module, cls, kwargs, None, games=False)
    assert src.fetch_upcoming(days_ahead=2) == []
    assert asked == []


# ---------------------------------------------------------------------------
# NCAAF (CollegeFootballData)
# ---------------------------------------------------------------------------

def _ncaaf(monkeypatch, rankings):
    ncaaf = importlib.import_module(f"{PKG}.ncaaf")
    games_asked = []

    def get(url, headers=None, params=None, timeout=None):
        if url.endswith("/rankings"):
            return rankings() if callable(rankings) else _Resp(rankings)
        games_asked.append(url)
        return _Resp([])

    stub_requests_get(monkeypatch, get)
    return ncaaf.NcaafSource(api_key="k"), games_asked


def _refused():
    import requests
    raise requests.ConnectionError("refused")


@pytest.mark.parametrize("rankings", [_refused, {}, {"message": "quota"}, [{"week": 7}],
                                      [{"week": 7, "polls": [{"poll": "AP Top 25"}]}],
                                      [{"week": 7, "polls": [{"poll": "AP Top 25",
                                                              "ranks": [{"school": "Georgia"}]}]}]],
                         ids=["failed", "empty-object", "error-object", "snapshot-without-polls",
                              "poll-without-ranks", "no-readable-rank"])
def test_ncaaf_a_failed_poll_raises(monkeypatch, rankings):
    """WITNESS: CFBD /rankings failed, `_fetch_rankings` returned None and
    NCAAF returned [] like a preseason. The empty label was held (#240), but
    the summary read "NCAA Football: 0 games", not an outage, and a malformed
    answer raised AttributeError deep in the parse instead."""
    src, games_asked = _ncaaf(monkeypatch, rankings)
    with pytest.raises(RankingsUnavailable):
        src.fetch_upcoming(days_ahead=2)
    assert games_asked == []


def test_ncaaf_no_snapshot_yet_is_a_quiet_preseason(monkeypatch):
    """Guard: `[]` is CFBD before the first poll of the season."""
    src, _games = _ncaaf(monkeypatch, [])
    assert src.fetch_upcoming(days_ahead=2) == []


# ---------------------------------------------------------------------------
# Football-Data leagues rank by the table
# ---------------------------------------------------------------------------

def _fd(monkeypatch, standings, fixtures=True):
    """An FD league source with one Premier League fixture tomorrow (when
    `fixtures`) and `standings` for /standings (a body, or a callable that
    raises). Returns (source, standings requests made)."""
    soccer = importlib.import_module(f"{PKG}.soccer")
    soccer._clear_fd_caches()
    monkeypatch.setattr(soccer, "_fd_sleep", lambda s: None)
    _pin_window(monkeypatch, NOON)
    asked = []

    def get(url, headers=None, params=None, timeout=None):
        if url.endswith("/standings"):
            asked.append(params)
            return standings() if callable(standings) else _Resp(standings)
        if url.endswith("/v4/matches") and fixtures:
            return _Resp({"matches": [{
                "id": 1, "competition": {"code": "PL"}, "utcDate": "2026-10-12T19:00:00Z",
                "status": "TIMED", "stage": "REGULAR_SEASON", "matchday": 8,
                "homeTeam": {"name": "Arsenal FC"}, "awayTeam": {"name": "Chelsea FC"},
            }]})
        return _Resp({"matches": []})

    stub_requests_get(monkeypatch, get)
    return soccer.SoccerSource("epl", fd_api_key="k", odds_api_key=""), asked


def _table(played):
    return {"standings": [{"type": "TOTAL", "table": [
        {"position": 1, "team": {"name": "Arsenal FC"}, "points": 19, "playedGames": played},
        {"position": 2, "team": {"name": "Chelsea FC"}, "points": 17, "playedGames": played},
    ]}]}


@pytest.mark.parametrize("standings", [_refused, {}, {"errorCode": 403, "message": "restricted"},
                                       {"standings": [{"type": "TOTAL"}]}],
                         ids=["failed", "empty-object", "error-body", "standings-without-a-table"])
def test_fd_a_failed_table_raises(monkeypatch, standings):
    """WITNESS: /standings failed (or answered FD's error body, measured
    2026-10-11 as `{"errorCode": 403, "message": ...}`) and every Premier
    League game went out unranked."""
    src, _asked = _fd(monkeypatch, standings)
    with pytest.raises(RankingsUnavailable):
        src.fetch_upcoming(days_ahead=2)


def test_fd_a_failed_seed_table_raises(monkeypatch):
    """WITNESS: early in the season the ranks come from LAST season's final
    table (the seed). Its request failed and the source fell back to this
    season's table, whose matchday-2 positions the seed exists to replace."""
    answers = [lambda: _Resp(_table(2)), _refused]
    src, asked = _fd(monkeypatch, lambda: answers.pop(0)())
    with pytest.raises(RankingsUnavailable):
        src.fetch_upcoming(days_ahead=2)
    # This season's table, then last season's (asked again by _fd_get's own
    # retries before it gives up).
    assert asked[0] is None and len(asked) >= 2
    assert all((p or {}).get("season") is not None for p in asked[1:])


def test_fd_an_answered_table_ranks_the_game(monkeypatch):
    src, _asked = _fd(monkeypatch, _table(9))
    rows = src.fetch_upcoming(days_ahead=2)
    assert [(r.home, r.rank_home, r.away, r.rank_away) for r in rows] == [
        ("Arsenal FC", 1, "Chelsea FC", 2)]


def test_fd_no_fixture_means_no_table_request(monkeypatch):
    """Guard: the table is read only once there is a fixture to rank, so an
    international break costs no request against FD's 10/min quota."""
    src, asked = _fd(monkeypatch, _refused, fixtures=False)
    assert src.fetch_upcoming(days_ahead=2) == []
    assert asked == []


# ---------------------------------------------------------------------------
# Class guard: every built source that asks for a rank feed surfaces a failure
# ---------------------------------------------------------------------------

def _is_rank_feed(url):
    """A poll (`/rankings`, any provider) or a Football-Data league table."""
    path = url.split("?", 1)[0]
    return path.endswith("/rankings") or (
        "football-data.org" in path and path.endswith("/standings"))


def test_every_built_source_that_reads_a_rank_feed_raises_when_it_fails(plugin, monkeypatch):
    """Every source `_build_sources` builds with everything on runs
    `fetch_upcoming` with one game tomorrow on every feed that lists games
    and EVERY rank feed failing. A source that asked for a rank feed and
    returned anyway published its games unranked: the #283 defect, in a
    source this file does not name."""
    import requests

    from dispatcharr_ranked_matchups.sources import soccer
    from dispatcharr_ranked_matchups.sources.ncaa_soccer import NcaaSoccerSource
    from tests.test_mapped_prefixes import _all_on_settings

    monkeypatch.setattr(soccer, "_fd_sleep", lambda s: None)
    monkeypatch.setattr(NcaaSoccerSource, "_regular_season_cutoff",
                        lambda self: datetime(2100, 1, 1, tzinfo=UTC))
    _pin_window(monkeypatch, NOON)
    fd_codes = sorted({c.fd_code for c in soccer.COMPETITIONS.values()})
    asked = set()
    current = {"name": "(building sources)"}

    def get(url, *a, **k):
        if _is_rank_feed(url):
            asked.add(current["name"])
            raise requests.ConnectionError("every rank feed fails in this test")
        if f"dates={TOMORROW_BUCKET}" in url:
            return _Resp({"events": [_event("g1", "Alpha", "Beta", TOMORROW, "pre")]})
        if "dates=" in url:
            return _Resp({"events": []})
        if url.endswith("/usa.1/standings"):
            return _Resp(_standings("East", "West"))
        if "collegebasketballdata" in url and url.endswith("/games"):
            return _Resp([{"id": 1, "homeTeam": "Alpha", "awayTeam": "Beta",
                           "startDate": "2026-10-12T19:00:00.000Z"}])
        if url.endswith("/v4/matches"):
            return _Resp({"matches": [{
                "id": i, "competition": {"code": code}, "utcDate": "2026-10-12T19:00:00Z",
                "status": "TIMED", "stage": "REGULAR_SEASON", "matchday": 8,
                "homeTeam": {"name": f"{code} Home"}, "awayTeam": {"name": f"{code} Away"},
            } for i, code in enumerate(fd_codes)]})
        return _Resp([] if ("collegebasketballdata" in url or "collegefootballdata" in url)
                     else {"matches": [], "events": [], "dates": [], "gameWeek": [], "data": []})

    stub_requests_get(monkeypatch, get)
    sources = plugin._build_sources(_all_on_settings())
    quiet = {}
    for src in sources:
        name = type(src).__name__
        current["name"] = name
        soccer._clear_fd_caches()
        try:
            src.fetch_upcoming(days_ahead=2)
        except RankingsUnavailable:
            continue
        except Exception as e:  # noqa: BLE001 - recorded, judged below
            if name in asked:
                quiet[name] = f"raised {type(e).__name__}, not RankingsUnavailable: {e}"
            continue
        if name in asked:
            quiet[name] = "returned games without its ranks"
    # The instrument reached every kind of rank feed.
    assert {"NcaaBaseballRegularSource", "NcaaSoftballRegularSource",
            "NcaawBasketballRegularSource", "NcaaSoccerSource", "NcaafSource",
            "NcaamSource", "SoccerSource"} <= asked, asked
    assert quiet == {}
