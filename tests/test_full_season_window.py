"""The importance simulator's season list must run to the season's END (#229).

Seven sources capped their full-season walk at `now + 7 days`, so the
simulated season ended next week: NFL importance read exactly 0 for every
game (measured 2026-10-10, 80 of 272 games seen). MLS had the same bug and
was fixed alone in #65; this is the CLASS guard, so the next source copied
from an old one cannot reintroduce it.
"""
import ast
import glob
import inspect
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
            days.append(url.rsplit("dates=", 1)[1])
            return {"events": []}
        with patch.object(nfl, "_http_get", side_effect=fake_get):
            src._fetch_full_season_games()
        assert days, "walk made no requests"
        assert days[-1] == f"{src.season_end_year}0115"  # literal: the documented Jan 15 pad
