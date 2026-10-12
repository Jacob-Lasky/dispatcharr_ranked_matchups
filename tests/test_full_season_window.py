"""The importance simulator's season list must run to the season's END (#229).

Seven sources capped their full-season walk at `now + 7 days`, so the
simulated season ended next week: NFL importance read exactly 0 for every
game (measured 2026-10-10, 80 of 272 games seen). MLS had the same bug and
was fixed alone in #65; this is the CLASS guard, so the next source copied
from an old one cannot reintroduce it.
"""
import ast
import glob
import os
import textwrap
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

SOURCES_DIR = os.path.join(os.path.dirname(__file__), "..", "sources")


def _season_fetchers():
    """(file, function node) for every _fetch_full_season_games in sources/."""
    out = []
    for path in sorted(glob.glob(os.path.join(SOURCES_DIR, "*.py"))):
        tree = ast.parse(open(path).read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_fetch_full_season_games":
                out.append((os.path.basename(path), node))
    return out


def _clock_calls(fn):
    return [
        n.func.attr for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("now", "today", "utcnow")
    ]


class TestNoSeasonFetchReadsTheClock:
    def test_the_instrument_finds_the_fetchers(self):
        # Fails on the instrument first: a broken discovery would pass vacuously.
        names = {f for f, _ in _season_fetchers()}
        assert {"nfl.py", "nba.py", "ncaa_soccer.py", "mls_standings.py", "nhl.py"} <= names

    def test_the_instrument_can_fire(self):
        bad = ast.parse(textwrap.dedent('''
            def _fetch_full_season_games(self):
                now = datetime.now(timezone.utc).date()
                end = min(now + timedelta(days=7), regular_end)
        ''')).body[0]
        assert _clock_calls(bad) == ["now"]

    def test_no_full_season_fetch_is_bounded_by_today(self):
        offenders = {f: _clock_calls(fn) for f, fn in _season_fetchers() if _clock_calls(fn)}
        assert offenders == {}, (
            "a full-season fetch reads the clock; the simulator needs the season to its END"
        )


class TestNflWalksToTheRegularSeasonEnd:
    def test_walk_reaches_mid_january(self):
        from dispatcharr_ranked_matchups.sources import nfl
        src = nfl.NflRegularSource()
        days = []

        def fake_get(url):
            days.append(url.rsplit("dates=", 1)[1][:8])
            return {"events": []}
        with patch.object(nfl, "_http_get", side_effect=fake_get):
            src._fetch_full_season_games()
        assert days, "walk made no requests"
        # max(), not days[-1]: the walk is concurrent, so call order is not date order.
        assert max(days) == f"{src.season_end_year}0115"  # literal: the documented Jan 15 pad


# (module, class, how to pin its season so the walk itself is the only
# variable, expected last day as MMDD)
_WALKERS = [
    ("nfl", "NflRegularSource", {"season_end_year": 2027}, "0115"),
    ("nba", "NbaRegularSource", {"season_end_year": 2027}, "0430"),
    ("wnba", "WnbaRegularSource", {"season_year": 2026}, "0930"),
    ("ncaaw_basketball", "NcaawBasketballRegularSource", {"season_end_year": 2027}, "0331"),
    # The NCAA soccer pair runs on the FALLBACK boundary here: the stub below
    # answers ESPN's season calendar with nothing. The calendar-derived
    # boundary is pinned in TestNcaaSoccerRegularSeasonCutoff (test_sources.py).
    ("ncaa_soccer", "NcaaSoccerSource", {"season_year": 2026, "gender": "m"}, "1118"),
    ("ncaa_soccer", "NcaaSoccerSource", {"season_year": 2026, "gender": "w"}, "1117"),
]


class _FakeDatetime:
    """A datetime stand-in whose now() is pinned; everything else delegates."""
    def __init__(self, pinned):
        self._pinned = pinned

    def now(self, tz=None):
        return self._pinned

    def __call__(self, *a, **k):
        return datetime(*a, **k)

    def __getattr__(self, name):
        return getattr(datetime, name)


def _walk_end(module_name, cls_name, attrs, pinned_now):
    import importlib
    mod = importlib.import_module(f"dispatcharr_ranked_matchups.sources.{module_name}")
    days = []

    def fake_get(url, *a, **k):
        # Only scoreboard requests are walk days; a source may also ask for
        # its season calendar (ncaa_soccer, #235), which carries no date.
        if "dates=" in url:
            days.append(url.rsplit("dates=", 1)[1][:8])
        return {"events": []}
    cls = getattr(mod, cls_name)
    src = cls(gender=attrs["gender"]) if "gender" in attrs else cls()
    for k, v in attrs.items():
        if k != "gender":
            setattr(src, k, v)
    with patch.object(mod, "_http_get", side_effect=fake_get), \
         patch.object(mod, "datetime", _FakeDatetime(pinned_now)):
        src._fetch_full_season_games()
    return max(days) if days else None


class TestWalkEndDoesNotDependOnTheClock:
    """Behavioural half of the guard: the AST check above cannot see a clock
    read hidden behind time.time() or a helper. Whatever the clock says, the
    walk must end on the season's last day."""

    @pytest.mark.parametrize("module_name,cls_name,attrs,mmdd", _WALKERS)
    def test_same_end_under_two_clocks(self, module_name, cls_name, attrs, mmdd):
        early = _walk_end(module_name, cls_name, attrs, datetime(2026, 9, 2, tzinfo=timezone.utc))
        late = _walk_end(module_name, cls_name, attrs, datetime(2027, 3, 1, tzinfo=timezone.utc))
        assert early == late
        assert early is not None and early.endswith(mmdd)


class TestFetchDays:
    def test_returns_every_day_in_date_order(self):
        from dispatcharr_ranked_matchups.sources import _espn
        import random
        import time as _time

        def slow(d):
            _time.sleep(random.random() / 200)  # finish out of order on purpose
            return d.isoformat()
        out = _espn.fetch_days(date(2026, 1, 30), date(2026, 2, 3), slow)
        assert [d.isoformat() for d, _ in out] == ["2026-01-30", "2026-01-31", "2026-02-01", "2026-02-02", "2026-02-03"]
        assert all(v == d.isoformat() for d, v in out)

    def test_empty_range(self):
        from dispatcharr_ranked_matchups.sources import _espn
        assert _espn.fetch_days(date(2026, 2, 3), date(2026, 2, 1), lambda d: 1) == []

    def test_runs_concurrently(self):
        # 16 days of 0.1 s each: sequential would take 1.6 s.
        from dispatcharr_ranked_matchups.sources import _espn
        import time as _time
        t = _time.monotonic()
        _espn.fetch_days(date(2026, 1, 1), date(2026, 1, 16), lambda d: _time.sleep(0.1))
        assert _time.monotonic() - t < 0.8

    def test_espn_module_still_never_imports_requests(self):
        src = open(os.path.join(SOURCES_DIR, "_espn.py")).read()
        assert "import requests" not in src
