"""Every ESPN walk dedupes on the event's identity, never on its raw id (#279).

`_espn.sweep_upcoming_events` keyed on the event's identity since #256, but the
bracket sweeps and full-season walks still keyed a seen-set on
`event.get("id")`. A missing id is None for every id-less event, so the NCAA
baseball and softball bracket sweeps kept the first id-less game and dropped
every later one as its duplicate, and the other walks skipped a None id
outright, losing every id-less game. ESPN sends ids today, so this was latent.

The walks now go through `_espn.dedupe_events`, and every ESPN row or record
that reaches the simulator carries `_espn.event_identity` as its `game_id` /
`id`: the simulator marks a game played by that id, so a None id there is the
same collapse one step later, and the upcoming row a refresh scores must carry
the same identity as the record it is matched against.

Witnesses below drive the real sweeps on a stubbed scoreboard. The class guard
is an AST scan of sources/ for a seen-set keyed on an `id` / `game_id`, with its
non-ESPN exemptions named and checked for staleness.
"""
import ast
import glob
import os
import json
import textwrap
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest

from dispatcharr_ranked_matchups import simulation
from dispatcharr_ranked_matchups.sources import (
    mls_cup, nba, ncaa_baseball, ncaa_soccer_cup, ncaa_softball,
)
from dispatcharr_ranked_matchups.sources.base import MatchResult

UTC = timezone.utc
SOURCES_DIR = os.path.join(os.path.dirname(__file__), "..", "sources")
PLUGIN_PY = os.path.join(os.path.dirname(__file__), "..", "plugin.py")


class Scoreboard:
    """A stub `_http_get`: one ESPN scoreboard payload per YYYYMMDD, an empty
    day otherwise."""

    def __init__(self, by_day):
        self.by_day = by_day

    def __call__(self, url, *a, **kw):
        if "/seasons/" in url:
            # ESPN's season calendar, which the College Cup needs before it
            # sweeps at all (#235): one phase covering the year is enough.
            year = url.rstrip("/").rsplit("/", 1)[1]
            return {"types": {"count": 1, "items": [{
                "slug": "regular", "type": 1,
                "startDate": f"{year}-01-01T00:00Z", "endDate": f"{year}-12-31T23:59Z",
            }]}}
        day = parse_qs(urlparse(url).query)["dates"][0]
        return {"events": list(self.by_day.get(day, []))}


def _event(home, away, start, *, season_type, headline=None, score=None, eid=None):
    """One ESPN scoreboard event; id-less unless `eid` is given. `score` is
    (home, away) for a FINISHED game."""
    final = score is not None
    comp = {
        "type": {"abbreviation": "FINAL" if season_type >= 3 else "STD"},
        "status": {"type": {"completed": final, "state": "post" if final else "pre"}},
        "competitors": [
            {"homeAway": "home", "team": {"displayName": home, "location": home},
             "score": str(score[0]) if final else None},
            {"homeAway": "away", "team": {"displayName": away, "location": away},
             "score": str(score[1]) if final else None},
        ],
    }
    if headline is not None:
        comp["notes"] = [{"type": "event", "headline": headline}]
    ev = {
        "date": start.strftime("%Y-%m-%dT%H:%MZ"),
        "season": {"type": season_type},
        "competitions": [comp],
    }
    if eid is not None:
        ev["id"] = eid
    return ev


def _pairs(games, home="home", away="away"):
    return sorted((g[home], g[away]) for g in games)


# ---------------------------------------------------------------------------
# Witnesses: the real sweeps, two id-less games and one listed on two days
# ---------------------------------------------------------------------------

# NBA East 1st Round. Celtics-Heat Game 1 is listed under BOTH Apr 19 and 20
# (the overlapping-bucket case the dedupe exists for); Knicks-Pistons is a
# different id-less game.
BOS_MIA = _event("Boston Celtics", "Miami Heat", datetime(2026, 4, 19, 23, 0, tzinfo=UTC),
                 season_type=3, headline="East 1st Round - Game 1")
NYK_DET = _event("New York Knicks", "Detroit Pistons", datetime(2026, 4, 20, 23, 0, tzinfo=UTC),
                 season_type=3, headline="East 1st Round - Game 1")


def test_nba_bracket_sweep_keeps_each_id_less_game_once(monkeypatch):
    monkeypatch.setattr(nba, "_http_get", Scoreboard({
        "20260419": [BOS_MIA], "20260420": [BOS_MIA, NYK_DET],
    }))
    games = nba.NbaPlayoffSource(season_end_year=2026)._fetch_bracket_games()
    assert _pairs(games) == [("Boston Celtics", "Miami Heat"), ("New York Knicks", "Detroit Pistons")]
    ids = [g["game_id"] for g in games]
    assert None not in ids and len(set(ids)) == 2


def _regional(home, away, day, game_no):
    return _event(home, away, datetime(2026, 5, 30 + day, 17, 0, tzinfo=UTC), season_type=3,
                  headline=f"NCAA Baseball Championship - Auburn Regional - Game {game_no}")


def test_baseball_bracket_sweep_keeps_each_id_less_game_once(monkeypatch):
    """The issue's own site: this sweep had no None check, so the second
    id-less game collapsed into the first."""
    g1 = _regional("Auburn", "Stetson", 0, 1)
    g2 = _regional("NC State", "Central Connecticut", 0, 2)
    monkeypatch.setattr(ncaa_baseball, "_http_get", Scoreboard({
        "20260530": [g1, g2], "20260531": [g1],
    }))
    games = ncaa_baseball.NcaaBaseballPlayoffBracketSource(season_year=2026)._fetch_bracket_games()
    assert _pairs(games) == [("Auburn", "Stetson"), ("NC State", "Central Connecticut")]
    assert None not in {g["game_id"] for g in games}


def test_softball_bracket_sweep_keeps_each_id_less_game_once(monkeypatch):
    def regional(home, away, game_no):
        return _event(home, away, datetime(2026, 5, 16, 17, 0, tzinfo=UTC), season_type=3,
                      headline=f"NCAA Softball Championship - Lincoln Regional - Game {game_no}")
    g1 = regional("Nebraska", "Omaha", 1)
    g2 = regional("Florida State", "Kentucky", 2)
    monkeypatch.setattr(ncaa_softball, "_http_get", Scoreboard({
        "20260516": [g1, g2], "20260517": [g1],
    }))
    games = ncaa_softball.NcaaSoftballPlayoffBracketSource(season_year=2026)._fetch_bracket_games()
    assert _pairs(games) == [("Florida State", "Kentucky"), ("Nebraska", "Omaha")]
    assert None not in {g["game_id"] for g in games}


def _season_game(home, away, day, score=None):
    return _event(home, away, datetime(2026, 1, day, 0, 30, tzinfo=UTC),
                  season_type=2, score=score)


def test_nba_full_season_walk_keeps_each_id_less_game_once(monkeypatch):
    a = _season_game("Boston Celtics", "Miami Heat", 10)
    b = _season_game("New York Knicks", "Detroit Pistons", 10)
    monkeypatch.setattr(nba, "_http_get", Scoreboard({"20260109": [a, b], "20260110": [a]}))
    games = nba.NbaRegularSource(season_end_year=2026)._fetch_full_season_games()
    assert _pairs(games) == [("Boston Celtics", "Miami Heat"), ("New York Knicks", "Detroit Pistons")]
    assert None not in {g["id"] for g in games}


def test_an_id_is_still_the_key_when_espn_sends_one(monkeypatch):
    """The identity IS the id for an id'd event, so nothing an id'd source
    produced changes, and two copies of one id'd game are still one game."""
    a = _season_game("Boston Celtics", "Miami Heat", 10)
    a["id"] = "401"
    moved = dict(a, date="2026-01-11T00:30Z")   # same game, kickoff edited between days
    monkeypatch.setattr(nba, "_http_get", Scoreboard({"20260109": [a], "20260110": [moved]}))
    games = nba.NbaRegularSource(season_end_year=2026)._fetch_full_season_games()
    assert [g["id"] for g in games] == ["401"]
    assert games[0]["start_time"] == datetime(2026, 1, 10, 0, 30, tzinfo=UTC)   # first copy wins


# ---------------------------------------------------------------------------
# Witnesses: the simulator, where an id-less game's id decides whether it is
# ever played
# ---------------------------------------------------------------------------

def _drain(src):
    """The simulator's drain loop (simulation.monte_carlo_importance), with a
    fixed home win: every remaining game played once, bounded so a game that
    can never be marked played fails instead of hanging."""
    state = src.initial_state()
    played = []
    for _ in range(50):
        rem = src.remaining_matches(state)
        if not rem:
            return played
        for m in rem:
            played.append((m.home, m.away))
            state = src.apply_result(state, m, MatchResult(home_goals=101, away_goals=99))
    raise AssertionError(f"drain did not terminate: {played[:6]}")


def test_id_less_season_games_are_counted_and_simulated(monkeypatch):
    """A FINISHED id-less game reaches the table, and each unplayed one is
    simulated exactly once."""
    done = _season_game("Boston Celtics", "Miami Heat", 5, score=(110, 100))
    left_1 = _season_game("Miami Heat", "Boston Celtics", 20)
    left_2 = _season_game("New York Knicks", "Detroit Pistons", 20)
    monkeypatch.setattr(nba, "_http_get", Scoreboard({
        "20260104": [done], "20260119": [left_1, left_2],
    }))
    src = nba.NbaRegularSource(season_end_year=2026)
    teams = src.initial_state()["_teams"]
    assert (teams.get("Boston Celtics", {}).get("wins"), teams.get("Miami Heat", {}).get("losses")) == (1, 1)
    assert sorted(_drain(src)) == [("Miami Heat", "Boston Celtics"), ("New York Knicks", "Detroit Pistons")]


def test_a_finished_id_less_bracket_game_does_not_hide_the_next_one(monkeypatch):
    """With a None game id, the FINISHED Game 1 put None into `_applied`, and
    the unplayed id-less Game 2 then read as already played."""
    g1 = _event("Boston Celtics", "Miami Heat", datetime(2026, 4, 19, 23, 0, tzinfo=UTC),
                season_type=3, headline="East 1st Round - Game 1", score=(110, 100))
    g2 = _event("Boston Celtics", "Miami Heat", datetime(2026, 4, 22, 23, 0, tzinfo=UTC),
                season_type=3, headline="East 1st Round - Game 2")
    monkeypatch.setattr(nba, "_http_get", Scoreboard({"20260419": [g1], "20260422": [g2]}))
    src = nba.NbaPlayoffSource(season_end_year=2026)
    tie = src.initial_state()["_tie_results"].get(
        ("R1", frozenset({"Boston Celtics", "Miami Heat"}))) or {}
    assert tie.get("series_wins") == {"Boston Celtics": 1, "Miami Heat": 0}
    rem = src.remaining_matches(src.initial_state())
    assert len(rem) >= 1 and {rem[0].home, rem[0].away} == {"Boston Celtics", "Miami Heat"}
    assert rem[0].extra["game_id"] is not None


class EveryDay:
    """A stub `_http_get` that lists the same events on every day it is asked
    for, so the upcoming sweep and a season or bracket walk see one game
    through separate requests, as a refresh does."""

    def __init__(self, events):
        self.events = events

    def __call__(self, url, *a, **kw):
        return {"events": [dict(e) for e in self.events]}


def _soon(**kw):
    """An id-less game a day from now, inside the real upcoming window."""
    start = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(days=1)
    return _event("Boston Celtics", "Miami Heat", start, **kw)


def test_an_id_less_upcoming_row_is_its_own_season_fixture(monkeypatch):
    """The row a refresh scores and the fixture pool's copy of the same game
    must be one match to the simulator (`_same_match`), or the target is
    sampled a second time as a remaining game."""
    monkeypatch.setattr(nba, "_http_get", EveryDay([_soon(season_type=2)]))
    src = nba.NbaRegularSource(season_end_year=2026)
    rows = src.fetch_upcoming(days_ahead=7)
    pool = src.remaining_matches(src.initial_state())
    assert len(rows) == 1 and len(pool) == 1
    assert rows[0].extra["game_id"] is not None
    assert rows[0].extra["game_id"] == pool[0].extra["game_id"]
    assert simulation._same_match(rows[0], pool[0])


def test_an_id_less_playoff_row_is_placed_in_its_tie(monkeypatch):
    """`place_upcoming` matches a row to its bracket game by `game_id`, and
    only a placed row gets the stage `apply_result` needs to file the sampled
    result into the tie (Codex review of #279)."""
    monkeypatch.setattr(nba, "_http_get", EveryDay(
        [_soon(season_type=3, headline="East 1st Round - Game 1")]))
    src = nba.NbaPlayoffSource(season_end_year=2026)
    rows = src.fetch_upcoming(days_ahead=7)
    assert len(rows) == 1
    assert src.place_upcoming(rows) == 0
    assert rows[0].extra.get("stage") == "R1"


def test_the_natural_identity_survives_json():
    """A row's game_id is written to cache.json and the guide's
    custom_properties, so it must come back as the same hashable value."""
    from dispatcharr_ranked_matchups.sources._espn import event_identity
    gid = event_identity(_soon(season_type=2))
    assert isinstance(gid, str) and json.loads(json.dumps(gid)) == gid


def _mls_cup_event(home, away, score, slug="eastern-conference-playoffs---semifinals"):
    ev = _event(home, away, datetime(2025, 11, 22, 20, 0, tzinfo=UTC), season_type=3, score=score)
    ev["season"] = {"type": 3, "slug": slug}
    return ev


def test_an_id_less_pk_pair_reaches_the_shootout_selector(monkeypatch):
    """ESPN can publish a knockout game that went to penalties as two FINISHED
    records, the regulation 0-0 and the shootout 3-2, and the cup sweeps keep
    the decisive one (`dedupe_pk_shootout_pairs`). Without ids the two share
    teams and kickoff, so keyed on `event_identity` alone the 0-0 claimed the
    game first and the tie had no winner (Codex review of #279)."""
    regulation = _mls_cup_event("Inter Miami CF", "Orlando City SC", (0, 0))
    shootout = _mls_cup_event("Inter Miami CF", "Orlando City SC", (3, 2))
    monkeypatch.setattr(mls_cup, "_http_get", Scoreboard({
        "20251122": [regulation, shootout], "20251123": [regulation],
    }))
    games = mls_cup.MlsCupSource(season_year=2025)._fetch_bracket_games()
    assert [(g["home_goals"], g["away_goals"]) for g in games] == [(3, 2)]


def test_an_id_less_college_cup_pk_pair_reaches_the_shootout_selector(monkeypatch):
    def ev(score):
        e = _event("Marshall", "SMU", datetime(2025, 12, 6, 0, 0, tzinfo=UTC), season_type=3, score=score)
        e["season"] = {"type": 3, "slug": "quarterfinals", "year": 2025}
        return e
    monkeypatch.setattr(ncaa_soccer_cup, "_http_get", Scoreboard({"20251205": [ev((0, 0)), ev((3, 2))]}))
    games = ncaa_soccer_cup.NcaaSoccerCupSource("m", season_year=2025)._fetch_bracket_games()
    assert [(g["home_goals"], g["away_goals"]) for g in games] == [(3, 2)]


def test_a_best_of_n_lagging_copy_is_still_one_game(monkeypatch):
    """A best-of-3 game listed twice, stale then final, must stay ONE game of
    the series: the PK-pair key applies to single-leg stages only."""
    slug = "eastern-conference-playoffs---round-one"
    stale = _mls_cup_event("Inter Miami CF", "Orlando City SC", None, slug)
    final = _mls_cup_event("Inter Miami CF", "Orlando City SC", (2, 1), slug)
    monkeypatch.setattr(mls_cup, "_http_get", Scoreboard({"20251122": [stale], "20251123": [final]}))
    games = mls_cup.MlsCupSource(season_year=2025)._fetch_bracket_games()
    assert len(games) == 1


class TestEventIdentity:
    def _identity(self, event):
        from dispatcharr_ranked_matchups.sources._espn import event_identity
        return event_identity(event)

    def test_teams_named_only_by_location_are_still_told_apart(self):
        """The NCAA sources name a team by `location`; with nothing else on
        the team, two different games at one kickoff were one key."""
        def ev(home, away):
            e = _season_game(home, away, 1)
            for c in e["competitions"][0]["competitors"]:
                c["team"] = {"location": c["team"]["location"]}
            return e
        assert self._identity(ev("Auburn", "Stetson")) != self._identity(ev("NC State", "Duke"))

    def test_one_kickoff_written_two_ways_is_one_key(self):
        a = _season_game("A", "B", 1)
        b = dict(a, date=a["date"][:-1] + ":00Z")   # ...T00:30Z vs ...T00:30:00Z
        assert a["date"] != b["date"] and self._identity(a) == self._identity(b)

    def test_an_id_is_its_own_identity(self):
        a = _season_game("A", "B", 1)
        a["id"] = "401"
        assert self._identity(a) == "401"


# ---------------------------------------------------------------------------
# The shared dedupe itself
# ---------------------------------------------------------------------------

class TestDedupeEvents:
    def _dedupe(self, events, keep=lambda e, k: e):
        from dispatcharr_ranked_matchups.sources._espn import dedupe_events
        return dedupe_events(events, keep)

    def test_distinct_id_less_events_both_survive_and_a_repeat_does_not(self):
        a = _season_game("A", "B", 1)
        b = _season_game("C", "D", 1)
        assert self._dedupe([a, b, dict(a)]) == [a, b]

    def test_a_skipped_copy_claims_nothing(self):
        """`sweep_upcoming_events` relies on this: a stale copy that `keep`
        drops must not suppress the good copy after it."""
        stale = _season_game("A", "B", 1)
        good = dict(stale, fresh=True)
        assert self._dedupe([stale, good], lambda e, k: e if e.get("fresh") else None) == [good]

    def test_keep_gets_the_identity_and_it_is_the_id_when_there_is_one(self):
        a = _season_game("A", "B", 1, score=None)
        a["id"] = "7"
        keys = []
        self._dedupe([a, _season_game("C", "D", 1), "not an event"], lambda e, k: keys.append(k))
        assert keys[0] == "7" and keys[1] is not None and len(keys) == 2


# ---------------------------------------------------------------------------
# Class guard: no seen-set keyed on an id in sources/ outside _espn.py
# ---------------------------------------------------------------------------

_ID_KEYS = ("id", "game_id")


def _is_id_expr(node, id_names):
    """`x.get("id")`, `x["id"]` (or `game_id`), or a local bound to one."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get" and node.args
            and isinstance(node.args[0], ast.Constant) and node.args[0].value in _ID_KEYS):
        return True
    if (isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant)
            and node.slice.value in _ID_KEYS):
        return True
    return isinstance(node, ast.Name) and node.id in id_names


def _id_names(fn):
    """Locals bound to an id expression, directly or through another such
    local (`gid = e.get("id"); alias = gid`), to a fixed point."""
    names = set()
    while True:
        grown = names | {
            t.id for n in ast.walk(fn) if isinstance(n, (ast.Assign, ast.AnnAssign))
            and n.value is not None and _is_id_expr(n.value, names)
            for t in (n.targets if isinstance(n, ast.Assign) else [n.target])
            if isinstance(t, ast.Name)
        }
        if grown == names:
            return names
        names = grown


def _mentions_id(node, id_names):
    return any(_is_id_expr(n, id_names) for n in ast.walk(node))


def _id_keyed_dedupes(source):
    """(function, line) for each membership test, `.add` / `.update` /
    `.setdefault` / `.fromkeys`, item assignment, or set / dict comprehension
    keyed on an id expression."""
    hits = []
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        id_names = _id_names(fn)
        for n in ast.walk(fn):
            keyed = (
                (isinstance(n, ast.Compare) and _is_id_expr(n.left, id_names)
                 and any(isinstance(op, (ast.In, ast.NotIn)) for op in n.ops))
                or (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                    and n.func.attr in ("add", "setdefault") and n.args
                    and _is_id_expr(n.args[0], id_names))
                or (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                    and n.func.attr in ("update", "fromkeys") and n.args
                    and _mentions_id(n.args[0], id_names))
                or (isinstance(n, ast.Assign) and any(
                    isinstance(t, ast.Subscript) and _is_id_expr(t.slice, id_names)
                    for t in n.targets))
                or (isinstance(n, ast.DictComp) and _is_id_expr(n.key, id_names))
                or (isinstance(n, ast.SetComp) and _is_id_expr(n.elt, id_names))
            )
            if keyed:
                hits.append((fn.name, n.lineno))
    return hits


# Sites keyed on an id that are NOT an ESPN walk, each checked below to still
# exist so the list cannot go stale. Adding to it needs the same argument:
# the id cannot be absent, or a None id is skipped AND the site is not a walk.
_EXEMPT = {
    # NHL api-web: `id` is the game's primary key, and both skip None.
    ("nhl.py", "_upcoming_schedule_games"),
    ("nhl.py", "_fetch_full_season_games"),
    # Football-Data match ids (primary key): simulator bookkeeping, and the
    # tier fixture sweep, which also keeps an id-less match rather than
    # keying it on None.
    ("soccer.py", "remaining_matches"),
    ("soccer.py", "_fetch_tier_fixtures"),
    # CFBD game id, a spreads lookup table rather than a dedupe.
    ("ncaaf.py", "_fetch_spreads"),
    # The simulator's applied-set bookkeeping over records a walk already
    # built: the ESPN walks stamp the event identity as the id
    # (`_espn.dedupe_events`), so these never see an id-less ESPN game as None.
    ("points_based.py", "remaining_matches"),
    ("bracket.py", "place_upcoming"),
    ("bracket.py", "_emit_remaining_games_for_tie"),
}

# The pre-#279 shapes, verbatim from the base revision: the baseball bracket
# sweep (no None check), the NBA full-season walk (None skipped) and the MLS
# Cup sweep (keyed on the record's game_id).
_OLD_SITES = {
    "ncaa_baseball._fetch_bracket_games": '''
        def _fetch_bracket_games(self):
            seen_ids: set = set()
            for data in payloads:
                for event in data.get("events") or []:
                    eid = event.get("id")
                    if eid in seen_ids:
                        continue
                    seen_ids.add(eid)
                    raw.append((event, stage, partial_key))
    ''',
    "nba._fetch_full_season_games": '''
        def _fetch_full_season_games(self):
            for event in data.get("events") or []:
                rec = _extract_game_record(event)
                if rec is None or rec["id"] is None:
                    continue
                if rec["id"] in seen:
                    continue
                seen[rec["id"]] = rec
    ''',
    "mls_cup._fetch_bracket_games": '''
        def _fetch_bracket_games(self):
            for event in data.get("events") or []:
                rec = self._extract_bracket_record(event, matchday=None)
                if rec["game_id"] in seen_ids:
                    continue
                seen_ids.add(rec["game_id"])
    ''',
    # Shapes no site used, which a guard written against the sites above
    # would still miss: a comprehension, and an alias fed to update().
    "set comprehension": '''
        def walk(events):
            seen = {e["id"] for e in events}
    ''',
    "aliased update": '''
        def walk(events):
            for e in events:
                gid = e.get("id")
                alias = gid
                seen.update([alias])
    ''',
}


def _scanned_files():
    paths = sorted(glob.glob(os.path.join(SOURCES_DIR, "*.py"))) + [PLUGIN_PY]
    return [p for p in paths if os.path.basename(p) != "_espn.py"]


def _all_hits():
    out = {}
    for path in _scanned_files():
        with open(path) as f:
            for fn, line in _id_keyed_dedupes(f.read()):
                out.setdefault((os.path.basename(path), fn), []).append(line)
    return out


class TestNoIdKeyedDedupeOutsideEspn:
    @pytest.mark.parametrize("name", sorted(_OLD_SITES))
    def test_the_instrument_flags_the_old_shape(self, name):
        assert _id_keyed_dedupes(textwrap.dedent(_OLD_SITES[name])), name

    def test_the_instrument_passes_the_fixed_shape(self):
        fixed = textwrap.dedent('''
            def _fetch_bracket_games(self):
                def keep(event, key):
                    rec = _extract_game_record(event)
                    if rec is not None:
                        rec["id"] = key
                    return rec
                return dedupe_events(scoreboard_events(payloads), keep)
        ''')
        assert _id_keyed_dedupes(fixed) == []

    def test_the_instrument_sees_the_sources(self):
        names = {os.path.basename(p) for p in _scanned_files()}
        assert {"nba.py", "ncaa_baseball.py", "mls_cup.py", "plugin.py"} <= names
        assert "_espn.py" not in names

    def test_no_espn_walk_keys_a_seen_set_on_an_id(self):
        offenders = {k: v for k, v in _all_hits().items() if k not in _EXEMPT}
        assert offenders == {}, (
            "dedupe ESPN events through _espn.dedupe_events, never on their raw id (#279)"
        )

    def test_every_exemption_is_still_a_real_site(self):
        hits = _all_hits()
        assert sorted(k for k in _EXEMPT if k not in hits) == []


# ---------------------------------------------------------------------------
# Class guards: what reaches the simulator carries the identity, not the raw id
# ---------------------------------------------------------------------------

def _keeps_ignoring_their_key(source):
    """(enclosing function, keep) for each `dedupe_events(events, keep)` whose
    `keep` (a nested def or a lambda) never reads its key, plus every call it
    examined, so a guard over nothing cannot pass. Every such walk feeds the
    simulator, whose `_applied` / drain loop track a game by the id the walk
    stamps (see `_espn.dedupe_events`)."""
    ignoring, examined = [], []
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        nested = {n.name: n for n in fn.body if isinstance(n, ast.FunctionDef)}
        for call in ast.walk(fn):
            if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                    and call.func.id == "dedupe_events" and len(call.args) >= 2):
                continue
            keep = call.args[1]
            if isinstance(keep, ast.Name) and keep.id in nested:
                keep = nested[keep.id]
            if not isinstance(keep, (ast.FunctionDef, ast.Lambda)):
                continue
            key = keep.args.args[1].arg
            examined.append(fn.name)
            body = keep.body if isinstance(keep, ast.Lambda) else keep
            if not any(isinstance(n, ast.Name) and n.id == key for n in ast.walk(body)):
                ignoring.append(fn.name)
    return ignoring, examined


def _raw_game_ids(source):
    """(function, line) for each dict literal whose `game_id` is an id
    expression: a row or record stamped with the raw id, None for an id-less
    event. ESPN code stamps `event_identity(event)` (or the walk's key)."""
    hits = []
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        id_names = _id_names(fn)
        for n in ast.walk(fn):
            if isinstance(n, ast.Dict) and any(
                isinstance(k, ast.Constant) and k.value == "game_id" and _is_id_expr(v, id_names)
                for k, v in zip(n.keys, n.values)
            ):
                hits.append((fn.name, n.lineno))
    return hits


# Walks whose keep ignores its key because the record builder it calls
# stamps `event_identity(event)` itself, which `_raw_game_ids` holds it to.
_BUILDER_STAMPS = {
    ("mls_cup.py", "_fetch_bracket_games"),
    ("ncaa_soccer_cup.py", "_fetch_bracket_games"),
}

# Raw-id game_id stamps outside ESPN code, each with an id that cannot be
# absent, or copied from a record whose id its walk already set.
_RAW_GAME_ID_EXEMPT = {
    ("nhl.py", "fetch_upcoming"),           # NHL api-web game id (primary key)
    ("nhl.py", "_fetch_bracket_games"),
    ("soccer.py", "_fetch_bracket_games"),  # Football-Data match id
    ("points_based.py", "remaining_matches"),  # copies the walk's record id
    ("bracket.py", "_sim_extra"),           # copies the bracket record's game_id
}


class TestEverythingTheSimulatorSeesCarriesTheIdentity:
    def test_the_instruments_can_fire(self):
        bad = textwrap.dedent('''
            def _fetch_bracket_games(self):
                def keep(event, _key):
                    rec = _extract_game_record(event)
                    return {"game_id": rec["id"]}
                return dedupe_events(scoreboard_events(payloads), keep)
        ''')
        assert _keeps_ignoring_their_key(bad) == (["_fetch_bracket_games"], ["_fetch_bracket_games"])
        assert ("keep", 5) in _raw_game_ids(bad)
        row = textwrap.dedent('''
            def fetch_upcoming(self, days_ahead=7):
                def to_row(event):
                    eid = event.get("id")
                    return GameRow(extra={"nba_game_id": eid, "game_id": eid})
        ''')
        assert "to_row" in {fn for fn, _ in _raw_game_ids(row)}

    def _per_file(self, scan):
        out = {}
        for path in _scanned_files():
            with open(path) as f:
                for fn, line in scan(f.read()):
                    out.setdefault((os.path.basename(path), fn), []).append(line)
        return out

    def test_every_walk_stamps_its_key(self):
        ignoring, examined = set(), []
        for path in _scanned_files():
            with open(path) as f:
                bad, seen = _keeps_ignoring_their_key(f.read())
            ignoring |= {(os.path.basename(path), fn) for fn in bad}
            examined += seen
        # The 16 walks #279 converted: fails on the instrument before the rule.
        assert len(examined) >= 16, examined
        assert ignoring - _BUILDER_STAMPS == set(), (
            "stamp `key` as the record's id / game_id: see _espn.dedupe_events")
        assert _BUILDER_STAMPS - ignoring == set(), "stale _BUILDER_STAMPS entry"

    def test_no_espn_row_or_record_stamps_a_raw_game_id(self):
        hits = self._per_file(_raw_game_ids)
        assert {k: v for k, v in hits.items() if k not in _RAW_GAME_ID_EXEMPT} == {}, (
            "stamp event_identity(event) as game_id: a raw id is None for an id-less event (#279)")
        assert sorted(k for k in _RAW_GAME_ID_EXEMPT if k not in hits) == []
