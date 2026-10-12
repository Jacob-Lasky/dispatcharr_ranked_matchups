"""#182: every ESPN scoreboard request is ONE DAY with an explicit page size.

Measured live 2026-10-11 (full numbers at `_espn.SCOREBOARD_EVENT_LIMIT`):

  - a bare `?dates=YYYYMMDD` pages silently at 100 events (225 games of the
    2026 women's NCAA soccer season never reached the simulator);
  - `limit` above 1000 silently answers with a 25-event page;
  - `dates=<start>-<end>` answers HTTP 400 for team sports, and for racing
    it drops the window's last day;
  - `dates=YYYYMM` silently returned 742 of 1033 NCAAW games.

`EspnEmulator` below reproduces the first and third behaviours so the
witnesses fail as assertions against the old code, not only as URL checks.
"""
from __future__ import annotations

import ast
import importlib
import logging
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

SOURCES = Path(__file__).resolve().parent.parent / "sources"
PKG = "dispatcharr_ranked_matchups.sources"
DAY_URL = re.compile(r"/scoreboard\?dates=\d{8}&limit=1000$")


class EspnEmulator:
    """Serves a scoreboard the way ESPN did on 2026-10-11.

    `by_day` maps "YYYYMMDD" to that bucket's events. A one-day request
    returns at most `limit` events (default page 100, and an over-1000 limit
    falls back to 25). A `dates=A-B` range request returns None, which is what
    a source's `_http_get` hands back for ESPN's HTTP 400 on that form. A
    core-API season calendar (`/seasons/YYYY`, which the College Cup needs
    before it sweeps, #235) gets one phase covering the year. Any other URL
    (rankings, standings) gets an empty dict.
    """

    def __init__(self, by_day=None):
        self.by_day = by_day or {}
        self.urls = []

    def __call__(self, url, *_a, **params):
        self.urls.append(url)
        if "/seasons/" in url:
            year = url.rstrip("/").rsplit("/", 1)[1]
            return {"types": {"count": 1, "items": [{
                "slug": "regular", "type": 1,
                "startDate": f"{year}-01-01T00:00Z", "endDate": f"{year}-12-31T23:59Z",
            }]}}
        if "/scoreboard" not in url:
            return {}
        q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
        q.update({k: str(v) for k, v in params.items() if k in ("dates", "limit")})
        dates = q.get("dates", "")
        if "-" in dates:
            return None
        limit = int(q.get("limit", 100))
        if limit > 1000:
            limit = 25
        return {"events": list(self.by_day.get(dates, []))[:limit]}


def _event(eid, home, away, when, *, completed=False, slug="regular-season", season_type=2):
    return {
        "id": str(eid),
        "date": when,
        "name": f"{away} at {home}",
        "shortName": f"{away} @ {home}",
        "season": {"slug": slug, "type": season_type},
        "competitions": [{
            "status": {"type": {"completed": completed, "state": "post" if completed else "pre"}},
            "competitors": [
                {"homeAway": "home", "team": {"displayName": home, "location": home},
                 "score": "2" if completed else None},
                {"homeAway": "away", "team": {"displayName": away, "location": away},
                 "score": "1" if completed else None},
            ],
        }],
    }


# ---------------------------------------------------------------------------
# The URL contract
# ---------------------------------------------------------------------------

class TestDayUrl:
    def test_shape_is_one_day_with_the_page_limit(self):
        from dispatcharr_ranked_matchups.sources._espn import scoreboard_day_url
        assert scoreboard_day_url("https://x.test/a/scoreboard", date(2026, 10, 25)) == (
            "https://x.test/a/scoreboard?dates=20261025&limit=1000"
        )

    def test_limit_is_exactly_the_largest_espn_honours(self):
        """1000, literally: 1001 or more silently returns a 25-event page."""
        from dispatcharr_ranked_matchups.sources import _espn
        assert _espn.SCOREBOARD_EVENT_LIMIT == 1000

    def test_full_page_is_reported(self, monkeypatch, caplog):
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_EVENT_LIMIT", 3)
        full = {"events": [{"id": 1}, {"id": 2}, {"id": 3}]}
        with caplog.at_level(logging.WARNING, logger=_espn.logger.name):
            out = _espn.fetch_scoreboard_day(lambda url: full, "https://x.test/scoreboard",
                                             date(2026, 10, 25))
        assert out is full
        assert any("may be truncated" in r.getMessage() for r in caplog.records)

    def test_short_page_is_quiet(self, caplog):
        from dispatcharr_ranked_matchups.sources import _espn
        with caplog.at_level(logging.WARNING, logger=_espn.logger.name):
            _espn.fetch_scoreboard_day(lambda url: {"events": [{"id": 1}]},
                                       "https://x.test/scoreboard", date(2026, 10, 25))
            _espn.fetch_scoreboard_day(lambda url: None,
                                       "https://x.test/scoreboard", date(2026, 10, 25))
        assert caplog.records == []


def _docstring_nodes(tree):
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                out.add(id(body[0].value))
    return out


def test_no_source_builds_a_scoreboard_query_itself():
    """Every `scoreboard?dates=` URL comes from `_espn.scoreboard_day_url`.
    AST, not text: docstrings explaining the forms must not trip it, and an
    f-string split across lines must."""
    offenders = []
    for path in sorted(SOURCES.glob("*.py")):
        if path.name == "_espn.py":
            continue
        tree = ast.parse(path.read_text(), filename=path.name)
        docs = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.JoinedStr):
                text = "".join(v.value for v in node.values
                               if isinstance(v, ast.Constant) and isinstance(v.value, str))
            elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
                  and id(node) not in docs):
                text = node.value
            else:
                continue
            if "dates=" in text or "scoreboard?" in text:
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == [], (
        "build ESPN scoreboard URLs with _espn.fetch_scoreboard_day / "
        f"scoreboard_day_url, not by hand: {offenders}"
    )


# ---------------------------------------------------------------------------
# Every ESPN sweep, run against the emulator: only one-day, limited URLs
# ---------------------------------------------------------------------------

def _sweeps():
    """(id, module, factory, method). Every code path that walks an ESPN
    scoreboard, so a new path that bypasses the helper shows up here."""
    S = []

    def add(mod, cls, *methods, **kw):
        for m in methods:
            S.append((f"{mod}.{cls}.{m}", mod, cls, kw, m))

    add("nba", "NbaRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("nba", "NbaPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("wnba", "WnbaRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("wnba", "WnbaPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("nfl", "NflRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("nfl", "NflPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaaw_basketball", "NcaawBasketballRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("ncaaw_basketball", "NcaawBasketballPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaa_baseball", "NcaaBaseballRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("ncaa_baseball", "NcaaBaseballPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaa_baseball", "NcaaBaseballPlayoffBracketSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaa_softball", "NcaaSoftballRegularSource", "fetch_upcoming", "_fetch_full_season_games")
    add("ncaa_softball", "NcaaSoftballPlayoffSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaa_softball", "NcaaSoftballPlayoffBracketSource", "fetch_upcoming", "_fetch_bracket_games")
    add("ncaa_soccer", "NcaaSoccerSource", "fetch_upcoming", "_fetch_full_season_games")
    add("ncaa_soccer_cup", "NcaaSoccerCupSource", "fetch_upcoming", "_fetch_bracket_games")
    add("mls_cup", "MlsCupSource", "fetch_upcoming", "_fetch_bracket_games")
    add("mls", "MlsSource", "fetch_upcoming")
    add("nwsl", "NwslSource", "fetch_upcoming", http_module="mls")
    add("liga_mx", "LigaMxSource", "fetch_upcoming", http_module="mls")
    add("mls_standings", "MlsEastSource", "fetch_upcoming", "_fetch_full_season_games")
    add("friendlies", "InternationalFriendliesSource", "fetch_upcoming")
    add("friendlies", "ClubFriendliesSource", "fetch_upcoming")
    add("english_cup", "EflCupSource", "fetch_upcoming")
    add("english_cup", "FaCupSource", "fetch_upcoming")
    for cls in ("F1Source", "NascarSource", "GolfSource", "UfcSource", "AtpSource", "WtaSource"):
        add("field_event", cls, "fetch_upcoming")
    return S


def _run_sweep(monkeypatch, mod_name, cls_name, kw, method, emulator):
    mod = importlib.import_module(f"{PKG}.{mod_name}")
    http_mod = importlib.import_module(f"{PKG}.{kw.get('http_module', mod_name)}")
    monkeypatch.setattr(http_mod, "_http_get", emulator)
    if mod_name == "mls_standings":
        monkeypatch.setattr(mod, "_fetch_conference_map", lambda: {"A": "East", "B": "East"})
    src = getattr(mod, cls_name)()
    if hasattr(src, "_fetch_closeness"):
        monkeypatch.setattr(src, "_fetch_closeness", lambda: {})
    if hasattr(src, "_fetch_rankings"):
        monkeypatch.setattr(src, "_fetch_rankings", lambda *a, **k: {})
    return getattr(src, method)()


@pytest.mark.parametrize("sweep", _sweeps(), ids=lambda s: s[0])
def test_every_espn_sweep_asks_for_single_limited_days(sweep, monkeypatch):
    _id, mod_name, cls_name, kw, method = sweep
    emu = EspnEmulator()
    _run_sweep(monkeypatch, mod_name, cls_name, kw, method, emu)
    boards = [u for u in emu.urls if "/scoreboard" in u]
    assert boards, f"{_id} made no scoreboard request"
    bad = [u for u in boards if not DAY_URL.search(u)]
    assert bad == [], f"{_id} built a scoreboard URL without the per-day limit: {bad[:3]}"


class SlowEmulator(EspnEmulator):
    """EspnEmulator that holds each request briefly and records how many were
    in flight at once."""

    def __init__(self, by_day=None, delay=0.01):
        super().__init__(by_day)
        self.delay = delay
        self.inflight = 0
        self.max_inflight = 0
        self._lock = threading.Lock()

    def __call__(self, url, *a, **k):
        with self._lock:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            time.sleep(self.delay)
            return super().__call__(url, *a, **k)
        finally:
            with self._lock:
                self.inflight -= 1


@pytest.mark.parametrize("sweep", _sweeps(), ids=lambda s: s[0])
def test_every_espn_sweep_fetches_its_days_concurrently(sweep, monkeypatch):
    """A hung ESPN endpoint costs a 15 s timeout per request. Walked one day
    after another, a 7-day sweep across ~25 sources overran the pipeline's
    25-minute ceiling; walked concurrently it costs one timeout per batch."""
    _id, mod_name, cls_name, kw, method = sweep
    emu = SlowEmulator()
    _run_sweep(monkeypatch, mod_name, cls_name, kw, method, emu)
    assert emu.max_inflight > 1, f"{_id} walked its scoreboard days one at a time"


class TestWalkGivesUpOnAHangingEndpoint:
    """A hung ESPN costs a 15 s timeout per request even concurrently; a
    303-day MLS walk is ~38 batches, and several such walks in a row overran
    the pipeline's 1500 s ceiling (#182 review). A chunk of 16 days that ALL
    fail AND take long stops the walk; fast failures do not. "Slow" is set by
    patching the threshold, so these tests do not sleep. Literals (16, 32,
    303) on purpose."""

    @staticmethod
    def _walk(monkeypatch, ok_days, slow, start=date(2025, 2, 1), end=date(2025, 11, 30)):
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_SLOW_CHUNK_SECONDS", 0.0 if slow else 1e9)
        asked = []
        lock = threading.Lock()

        def http_get(url):
            day = datetime.strptime(url.split("dates=")[1][:8], "%Y%m%d").date()
            with lock:
                asked.append(day)
            return {"events": []} if (day - start).days in ok_days else None
        pages = _espn.fetch_scoreboard_days(http_get, "https://x.test/scoreboard", start, end)
        return asked, pages

    def test_hanging_endpoint_stops_after_one_chunk(self, monkeypatch, caplog):
        with caplog.at_level(logging.WARNING):
            asked, pages = self._walk(monkeypatch, ok_days=set(), slow=True)
        assert len(asked) == 16 and len(pages) == 16
        assert max(asked) == date(2025, 2, 16)
        assert any("not requesting the remaining days" in r.getMessage()
                   for r in caplog.records)

    def test_endpoint_that_answers_then_hangs_is_still_caught(self, monkeypatch):
        asked, pages = self._walk(monkeypatch, ok_days={0}, slow=True)
        assert len(asked) == 32 and len(pages) == 32

    def test_fast_failures_walk_the_whole_window(self, monkeypatch):
        """A 503 or a refused connection costs nothing to keep asking, and the
        endpoint may recover, so the season is not truncated for it."""
        asked, pages = self._walk(monkeypatch, ok_days={200}, slow=False)
        assert len(asked) == 303 and len(pages) == 303
        assert [d for d, p in pages if p is not None] == [date(2025, 2, 1) + timedelta(days=200)]

    def test_a_window_no_longer_than_one_chunk_is_never_cut(self, monkeypatch):
        asked, pages = self._walk(monkeypatch, ok_days=set(), slow=True,
                                  end=date(2025, 2, 16))
        assert len(asked) == 16 and len(pages) == 16

    def test_partial_hang_is_stopped_by_the_walk_budget(self, monkeypatch, caplog):
        """One good day per chunk keeps every chunk off the all-None rule;
        the walk budget still ends it. Budget patched to zero: the walk
        stops after its first chunk."""
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_WALK_BUDGET_SECONDS", 0.0)
        with caplog.at_level(logging.WARNING):
            asked, pages = self._walk(monkeypatch, ok_days=set(range(0, 303, 16)), slow=True)
        assert len(asked) == 16 and len(pages) == 16
        assert any("budget" in r.getMessage() for r in caplog.records)

    def test_a_healthy_walk_is_well_inside_the_budget(self, monkeypatch):
        """120 s, literally: a guard that the budget was not tightened to a
        value a healthy season walk (~5 s measured) could reach."""
        from dispatcharr_ranked_matchups.sources import _espn
        assert _espn.SCOREBOARD_WALK_BUDGET_SECONDS == 120.0
        asked, pages = self._walk(monkeypatch, ok_days=set(range(303)), slow=False)
        assert len(asked) == 303

    def test_last_chunk_never_counts_as_a_reason_to_stop(self, monkeypatch, caplog):
        with caplog.at_level(logging.WARNING):
            self._walk(monkeypatch, ok_days=set(range(16)), slow=True,
                       end=date(2025, 3, 1))
        assert not any("not requesting" in r.getMessage() for r in caplog.records)


class TestWalkRetriesABlip:
    """A walk that ran to its end re-requests a FEW failed days once. Measured
    2026-10-11: one read timeout on 20261006 in a live 303-day MLS walk, and a
    lost day silently drops its finished games from the standings the
    simulator starts from. Literals (303, 8, 16) on purpose."""

    @staticmethod
    def _walk(flaky, dead=(), start=date(2025, 2, 1), end=date(2025, 11, 30)):
        """`flaky` day offsets fail on their FIRST request only; `dead` ones
        fail every time. Returns (requests per offset, pages)."""
        from dispatcharr_ranked_matchups.sources import _espn
        asked = {}
        lock = threading.Lock()

        def http_get(url):
            day = datetime.strptime(url.split("dates=")[1][:8], "%Y%m%d").date()
            off = (day - start).days
            with lock:
                asked[off] = asked.get(off, 0) + 1
                n = asked[off]
            if off in dead or (off in flaky and n == 1):
                return None
            return {"events": [{"id": str(off)}]}
        pages = _espn.fetch_scoreboard_days(http_get, "https://x.test/scoreboard", start, end)
        return asked, pages

    def test_one_timed_out_day_of_a_season_is_recovered(self):
        """WITNESS: without the retry the day stays None."""
        asked, pages = self._walk(flaky={247})
        assert len(pages) == 303
        assert pages[247] == (date(2025, 10, 6), {"events": [{"id": "247"}]})
        assert asked[247] == 2 and sum(asked.values()) == 304

    def test_one_failed_day_of_an_upcoming_week_is_recovered(self):
        """WITNESS on the emit side: an 8-day sweep, one blip."""
        asked, pages = self._walk(flaky={3}, end=date(2025, 2, 8))
        assert [p is not None for _d, p in pages] == [True] * 8
        assert sum(asked.values()) == 9

    def test_retry_is_once(self):
        asked, pages = self._walk(flaky=set(), dead={100})
        assert asked[100] == 2 and pages[100][1] is None

    def test_many_failures_are_an_outage_not_a_blip(self):
        """19 of 303 is past one in 16: no retry, so a degraded endpoint is
        not asked twice for most of a season."""
        dead = set(range(0, 303, 16))
        assert len(dead) == 19
        asked, _pages = self._walk(flaky=set(), dead=dead)
        assert sum(asked.values()) == 303

    def test_a_hung_final_stretch_is_not_retried(self):
        """15 failed days of 303 is inside the one-in-16 share but past one
        batch (8): retrying them would cost two more timeouts after a final
        chunk the hang rule never checks."""
        asked, _pages = self._walk(flaky=set(), dead=set(range(288, 303)))
        assert sum(asked.values()) == 303

    def test_a_hung_last_chunk_is_not_retried(self, monkeypatch):
        """136 days, the last 8 (the final chunk) all failing slowly: an
        outage that began mid-walk, inside every count cap, so only the
        last-chunk hang check keeps it from costing another timeout."""
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_SLOW_CHUNK_SECONDS", 0.0)
        asked, _pages = self._walk(flaky=set(), dead=set(range(128, 136)),
                                   end=date(2025, 2, 1) + timedelta(days=135))
        assert sum(asked.values()) == 136

    def test_a_fast_failing_last_chunk_is_retried(self):
        """Same 8 days failing FAST: a 503 costs nothing to ask again."""
        asked, _pages = self._walk(flaky=set(), dead=set(range(128, 136)),
                                   end=date(2025, 2, 1) + timedelta(days=135))
        assert sum(asked.values()) == 144

    def test_up_to_one_batch_of_blips_is_retried(self):
        flaky = set(range(0, 303, 40))
        assert len(flaky) == 8
        asked, pages = self._walk(flaky=flaky)
        assert all(p is not None for _d, p in pages) and sum(asked.values()) == 311

    def test_two_failures_in_a_week_are_not_retried(self):
        asked, _pages = self._walk(flaky=set(), dead={1, 5}, end=date(2025, 2, 8))
        assert sum(asked.values()) == 8

    def test_every_day_failed_is_not_retried(self):
        asked, _pages = self._walk(flaky=set(), dead={0}, end=date(2025, 2, 1))
        assert sum(asked.values()) == 1

    def test_an_abandoned_walk_is_not_retried(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_WALK_BUDGET_SECONDS", 0.0)
        asked, pages = self._walk(flaky={3})
        assert len(pages) == 16 and sum(asked.values()) == 16

    def test_a_walk_abandoned_for_a_hang_is_not_retried(self, monkeypatch):
        """The hang rule stops the walk at its 16th chunk (days 240-255, all
        failed, "slow" by a zero threshold). 16 failed of 256 is inside the
        one-in-16 share, so only the 8-day batch cap keeps those 16 hung days
        from being asked again."""
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_SLOW_CHUNK_SECONDS", 0.0)
        asked, pages = self._walk(flaky=set(), dead=set(range(240, 256)))
        assert len(pages) == 256 and sum(asked.values()) == 256

    def test_no_retry_once_the_budget_is_spent(self, monkeypatch):
        """A one-chunk walk is never abandoned, so this isolates the retry's
        own budget check."""
        from dispatcharr_ranked_matchups.sources import _espn
        monkeypatch.setattr(_espn, "SCOREBOARD_WALK_BUDGET_SECONDS", 0.0)
        asked, pages = self._walk(flaky={3}, end=date(2025, 2, 8))
        assert sum(asked.values()) == 8 and pages[3][1] is None


def test_concurrent_walk_returns_days_in_date_order():
    """Earlier days answer LAST here, so completion order is the reverse of
    date order; the walk must still hand back date order, which is what the
    callers' first-occurrence dedupe relies on."""
    from dispatcharr_ranked_matchups.sources._espn import fetch_scoreboard_days

    def http_get(url):
        dates = url.split("dates=")[1][:8]
        time.sleep((31 - int(dates[-2:])) * 0.002)
        return {"events": [{"id": dates}]}
    pages = fetch_scoreboard_days(http_get, "https://x.test/scoreboard",
                                  date(2026, 10, 1), date(2026, 10, 20))
    assert [d.isoformat() for d, _ in pages] == [
        f"2026-10-{n:02d}" for n in range(1, 21)]
    assert [p["events"][0]["id"] for _, p in pages] == [
        f"202610{n:02d}" for n in range(1, 21)]


# ---------------------------------------------------------------------------
# Behavioural witnesses: the old code returned the wrong ROWS
# ---------------------------------------------------------------------------

def test_busy_day_past_espns_default_page_reaches_the_season_walk(monkeypatch):
    """The live case: 101 women's NCAA soccer games on 2026-10-25. Without
    `limit`, ESPN serves 100 and game 101 never reaches the simulator."""
    events = [_event(9000 + i, f"Home {i}", f"Away {i}", "2026-10-25T23:00Z")
              for i in range(101)]
    emu = EspnEmulator({"20261025": events})
    from dispatcharr_ranked_matchups.sources import ncaa_soccer
    monkeypatch.setattr(ncaa_soccer, "_http_get", emu)
    games = ncaa_soccer.NcaaSoccerSource(gender="w", season_year=2026)._fetch_full_season_games()
    assert len(games) == 101


def test_busy_day_reaches_the_upcoming_sweep(monkeypatch):
    from dispatcharr_ranked_matchups.sources import ncaa_soccer
    today = datetime.now(timezone.utc)
    when = (today + timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
    events = [_event(9000 + i, f"Home {i}", f"Away {i}", when.strftime("%Y-%m-%dT%H:%MZ"))
              for i in range(101)]
    emu = EspnEmulator({when.strftime("%Y%m%d"): events})
    monkeypatch.setattr(ncaa_soccer, "_http_get", emu)
    src = ncaa_soccer.NcaaSoccerSource(gender="w")
    monkeypatch.setattr(src, "_fetch_rankings", lambda *a, **k: {})
    assert len(src.fetch_upcoming(days_ahead=7)) == 101


def test_field_event_on_the_last_day_of_the_window_is_found(monkeypatch):
    """The racing range form dropped the window's last day (Phoenix,
    2026-10-18, missing from 20261011-20261018). The per-day walk asks for
    that day like every other."""
    from dispatcharr_ranked_matchups.sources import field_event
    today = datetime.now(timezone.utc).date()
    last = today + timedelta(days=7)
    race = {"id": "R7", "name": "NASCAR Cup Series at Phoenix",
            "shortName": "Phoenix", "date": f"{last.isoformat()}T19:00Z"}
    emu = EspnEmulator({last.strftime("%Y%m%d"): [race]})
    monkeypatch.setattr(field_event, "_http_get", emu)
    rows = field_event.NascarSource().fetch_upcoming(days_ahead=7)
    assert [r.home for r in rows] == ["NASCAR Cup Series at Phoenix"]


def test_multi_day_field_event_is_emitted_once(monkeypatch):
    """A tournament sits in every day bucket it spans; one row, not seven."""
    from dispatcharr_ranked_matchups.sources import field_event
    today = datetime.now(timezone.utc).date()
    open_ = {"id": "T1", "name": "Shanghai Masters", "shortName": "Shanghai",
             "date": f"{today.isoformat()}T03:00Z"}
    emu = EspnEmulator({(today + timedelta(days=i)).strftime("%Y%m%d"): [open_]
                        for i in range(7)})
    monkeypatch.setattr(field_event, "_http_get", emu)
    rows = field_event.AtpSource().fetch_upcoming(days_ahead=7)
    assert [r.home for r in rows] == ["Shanghai Masters"]


def test_mls_upcoming_survives_espn_rejecting_the_range_form(monkeypatch):
    """`mls_standings.fetch_upcoming` used the range form too, so the live
    400s emptied MLS East/West's emitted games as well as their season."""
    from dispatcharr_ranked_matchups.sources import mls_standings
    when = (datetime.now(timezone.utc) + timedelta(days=2)).replace(
        hour=23, minute=30, second=0, microsecond=0)
    emu = EspnEmulator({when.strftime("%Y%m%d"): [
        _event(77, "A", "B", when.strftime("%Y-%m-%dT%H:%MZ"))]})
    monkeypatch.setattr(mls_standings, "_http_get", emu)
    monkeypatch.setattr(mls_standings, "_fetch_conference_map",
                        lambda: {"A": "East", "B": "East"})
    src = mls_standings.MlsEastSource()
    monkeypatch.setattr(src, "_fetch_closeness", lambda: {})
    rows = src.fetch_upcoming(days_ahead=7)
    assert [(r.home, r.away) for r in rows] == [("A", "B")]


def test_mls_season_keeps_date_order_and_first_occurrence(monkeypatch):
    """The per-day walk is concurrent; results must still come back in date
    order so the caller's first-occurrence dedupe keeps the EARLIER copy."""
    from dispatcharr_ranked_matchups.sources import mls_standings
    early = _event(5, "A", "B", "2025-04-01T19:00Z", completed=True)
    late = _event(5, "A", "B", "2025-04-02T19:00Z")
    other = _event(6, "B", "A", "2025-03-01T19:00Z", completed=True)
    emu = EspnEmulator({"20250301": [other], "20250401": [early], "20250402": [late]})
    monkeypatch.setattr(mls_standings, "_http_get", emu)
    events = mls_standings._fetch_scoreboard_range(date(2025, 2, 1), date(2025, 11, 30))
    assert [(e["id"], e["date"]) for e in events] == [
        ("6", "2025-03-01T19:00Z"), ("5", "2025-04-01T19:00Z"), ("5", "2025-04-02T19:00Z"),
    ]
    monkeypatch.setattr(mls_standings, "_fetch_conference_map",
                        lambda: {"A": "East", "B": "East"})
    games = mls_standings.MlsEastSource(season_year=2025)._fetch_full_season_games()
    by_id = {g["id"]: g for g in games}
    assert by_id["5"]["status"] == "FINISHED"


def test_claude_md_describes_the_per_day_scoreboard_helpers():
    """The repo's agent guide is how the next source author finds the rule.
    PR #266's docs-bot commit restored main's `_espn.py` row, which names
    neither helper and calls `fetch_days` "the" season-walk helper, so a
    reader would hand-build `?dates=` URLs again. Text-only on purpose."""
    root = SOURCES.parent
    row = [line for line in (root / "CLAUDE.md").read_text().splitlines()
           if line.startswith("| `sources/_espn.py`")]
    assert len(row) == 1, "CLAUDE.md lost its sources/_espn.py row"
    for pattern in (r"\bfetch_scoreboard_day\b", r"\bfetch_scoreboard_days\b", r"limit=1000"):
        assert re.search(pattern, row[0]), (
            f"CLAUDE.md's _espn.py row no longer mentions {pattern}")
    assert re.search(r"\bfetch_scoreboard_day\b", (root / "CONTRIBUTING.md").read_text())
