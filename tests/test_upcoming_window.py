"""Every upcoming sweep starts one day BEFORE today-UTC (#256).

ESPN buckets `scoreboard?dates=YYYYMMDD` by US EASTERN date, and so do the two
non-ESPN schedules that take a date (api-web.nhle.com `schedule/{date}`,
statsapi.mlb.com `startDate=`). From 00:00Z to 05:00Z, 8pm to 1am Eastern,
tonight's late games therefore sit in YESTERDAY's bucket. Only
`_espn.sweep_upcoming_scoreboard` reached back for them; twelve other sources
computed their own today-UTC and dropped every evening game in progress (and,
for MLB, every late West Coast game before first pitch too).

Measured live 2026-10-11 (see the probes in the PR): the College Cup semifinal
NC State v Saint Louis (2025-12-13T01:30Z) is under `dates=20251212`; NHL
`schedule/2026-10-10` lists ANA at CGY (2026-10-11T02:00Z) under 2026-10-10;
MLB lists Diamondbacks at Padres (2026-09-27T00:40Z) under 2026-09-26.

Three layers:

- `TestTheWindow` / `TestTheSharedSweep`: the one definition, `_espn.upcoming_window`
  and `_espn.sweep_upcoming_events`.
- `TestEveningGameIsFound`: the WITNESS, one per affected source. The clock is
  pinned at 2026-10-11T02:00Z (10pm Eastern on the 10th) and the stub serves a
  game in progress ONLY under `dates=20261010`, as ESPN does. Every one of these
  failed as an assertion (no row) before #256.
- `TestNoSourceAnchorsAtTodayUtc`: the CLASS guard, an AST check over
  `sources/` plus a behavioural check over every source `_build_sources`
  builds, so a new sweep cannot start at today-UTC again.

Kickoffs and expected dates are LITERALS against the pinned clock, so a
constant that drifted fails here rather than moving with the test.
"""
import ast
import glob
import os
import re
import socket
import textwrap
from datetime import date, datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups.sources import _espn

SOURCES_DIR = os.path.join(os.path.dirname(__file__), "..", "sources")

# 2026-10-11T02:00Z is 22:00 EDT on Saturday 2026-10-10: prime time.
PINNED = datetime(2026, 10, 11, 2, 0, tzinfo=timezone.utc)
LOOKBACK_BUCKET = "20261010"
TODAY_BUCKET = "20261011"


def _pin_clock(monkeypatch, now=PINNED):
    """Pin `_espn`'s clock, the only clock an upcoming sweep may read."""
    class _Pinned(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is not None else now.replace(tzinfo=None)

    monkeypatch.setattr(_espn, "datetime", _Pinned)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def _team(name):
    return {"displayName": name, "location": name, "name": name,
            "abbreviation": name[:3].upper()}


def _event(eid, home, away, kickoff, state, *, season_type=2,
           slug="regular-season", headline=None, name=None):
    """One ESPN scoreboard event in the shape every ESPN source parses:
    status on the event AND on competitions[0], `season.type` and `slug`,
    and an event note when `headline` is given."""
    completed = state == "post"
    status = {"type": {"state": state, "completed": completed}}
    comp = {
        "status": status,
        "competitors": [
            {"homeAway": "home", "team": _team(home), "score": "3" if completed else None},
            {"homeAway": "away", "team": _team(away), "score": "1" if completed else None},
        ],
    }
    if headline:
        comp["notes"] = [{"type": "event", "headline": headline}]
    return {
        "id": eid,
        "date": _iso(kickoff),
        "name": name or f"{away} at {home}",
        "shortName": f"{away[:3]} @ {home[:3]}",
        "season": {"year": 2026, "type": season_type, "slug": slug},
        "status": status,
        "competitions": [comp],
    }


def _evening(**shape):
    """Yesterday's US-Eastern bucket at 22:00 EDT: one game in progress (the
    one the sweep must find), one that finished this evening, and one ESPN
    still calls SCHEDULED nine hours after kickoff (status lag)."""
    return {
        LOOKBACK_BUCKET: [
            _event("live-1", "Home Live", "Away Live",
                   datetime(2026, 10, 11, 0, 30, tzinfo=timezone.utc), "in", **shape),
            _event("done-1", "Home Done", "Away Done",
                   datetime(2026, 10, 10, 22, 0, tzinfo=timezone.utc), "post", **shape),
            _event("stale-1", "Home Stale", "Away Stale",
                   datetime(2026, 10, 10, 17, 0, tzinfo=timezone.utc), "pre", **shape),
        ],
    }


_LEAGUE_PATH = {
    "nba": "basketball/nba", "nfl": "football/nfl", "wnba": "basketball/wnba",
    "ncaaw_basketball": "basketball/womens-college-basketball",
    "ncaa_baseball": "baseball/college-baseball", "ncaa_softball": "baseball/college-softball",
    "mls": "soccer/usa.1", "mls_cup": "soccer/usa.1", "mls_standings": "soccer/usa.1",
}


def _serve_buckets(monkeypatch, module, buckets, requested=None, urls=None):
    """Stub `module._http_get`: a scoreboard request gets the events filed
    under its `dates=` bucket; anything else (rankings, standings) gets an
    empty dict, which every source reads as "no data"."""
    def fake(url, *a, **k):
        m = re.search(r"dates=(\d{8})", url)
        if not m:
            return {}
        if urls is not None:
            urls.append(url)
        if requested is not None:
            requested.append(m.group(1))
        return {"events": list(buckets.get(m.group(1), []))}
    monkeypatch.setattr(module, "_http_get", fake)


def _homes(rows):
    return sorted(r.home for r in rows)


# ---------------------------------------------------------------- the window

class TestTheWindow:
    def test_starts_one_day_before_today_utc_and_runs_days_ahead_past_it(self, monkeypatch):
        _pin_clock(monkeypatch)
        w = _espn.upcoming_window(7)
        assert w.today == date(2026, 10, 11)
        assert w.first_day == date(2026, 10, 10)
        assert w.last_day == date(2026, 10, 18)
        assert w.oldest_kickoff == datetime(2026, 10, 10, 20, 0, tzinfo=timezone.utc)

    def test_admits_only_games_still_upcoming_or_live(self, monkeypatch):
        _pin_clock(monkeypatch)
        w = _espn.upcoming_window(0)
        assert w.admits(datetime(2026, 10, 11, 0, 30, tzinfo=timezone.utc), over=False)
        assert w.admits(datetime(2026, 10, 10, 20, 0, tzinfo=timezone.utc), over=False)  # the floor itself
        assert not w.admits(datetime(2026, 10, 10, 19, 59, tzinfo=timezone.utc), over=False)
        assert not w.admits(datetime(2026, 10, 11, 0, 30, tzinfo=timezone.utc), over=True)
        assert not w.admits(None, over=False)

    def test_a_missing_competition_status_falls_back_to_the_events(self):
        ev = {"status": {"type": {"state": "post", "completed": True}},
              "competitions": [{"competitors": []}]}
        assert _espn.scoreboard_event_over(ev)

    def test_a_postponed_game_is_over(self):
        # ESPN's postponed/cancelled shape: `post` without `completed`.
        ev = {"competitions": [{"status": {"type": {"state": "post", "completed": False}}}]}
        assert _espn.scoreboard_event_over(ev)
        assert not _espn.scoreboard_event_over(
            {"competitions": [{"status": {"type": {"state": "in", "completed": False}}}]})


class TestTheSharedSweep:
    URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

    def _sweep(self, monkeypatch, buckets, convert=lambda ev: ev["id"], **kw):
        _pin_clock(monkeypatch)
        requested = []

        def http_get(url):
            day = re.search(r"dates=(\d{8})", url).group(1)
            requested.append(day)
            return {"events": list(buckets.get(day, []))}
        out = _espn.sweep_upcoming_events(http_get, self.URL, 1, convert, **kw)
        return out, sorted(requested)

    def test_requests_the_lookback_bucket_through_days_ahead(self, monkeypatch):
        _out, requested = self._sweep(monkeypatch, {})
        assert requested == ["20261010", "20261011", "20261012"]

    def test_keeps_the_live_game_and_drops_finished_and_stale(self, monkeypatch):
        out, _ = self._sweep(monkeypatch, _evening())
        assert out == ["live-1"]

    def test_drops_a_finished_game_in_a_forward_bucket_too(self, monkeypatch):
        finished_today = _event("done-2", "H", "A",
                                datetime(2026, 10, 11, 1, 0, tzinfo=timezone.utc), "post")
        out, _ = self._sweep(monkeypatch, {TODAY_BUCKET: [finished_today]})
        assert out == []

    def test_a_stale_copy_in_the_lookback_does_not_hide_the_good_copy(self, monkeypatch):
        # ESPN serves the two buckets from separate cache entries, so the
        # lookback copy of one event can still read FINISHED-lagged while the
        # forward copy is right. Admission runs before the dedupe.
        kickoff = datetime(2026, 10, 11, 1, 0, tzinfo=timezone.utc)
        out, _ = self._sweep(monkeypatch, {
            LOOKBACK_BUCKET: [_event("same", "H", "A", kickoff, "post")],
            TODAY_BUCKET: [_event("same", "H", "A", kickoff, "in")],
        })
        assert out == ["same"]

    def test_a_skipped_event_does_not_claim_its_id(self, monkeypatch):
        kickoff = datetime(2026, 10, 11, 1, 0, tzinfo=timezone.utc)
        first = _event("same", "H", "A", kickoff, "in", season_type=99)
        second = _event("same", "H", "A", kickoff, "in", season_type=2)
        out, _ = self._sweep(
            monkeypatch,
            {LOOKBACK_BUCKET: [first], TODAY_BUCKET: [second]},
            convert=lambda ev: ev["season"]["type"] if ev["season"]["type"] == 2 else None,
        )
        assert out == [2]

    def test_overlapping_buckets_emit_an_event_once(self, monkeypatch):
        live = _event("live-1", "H", "A", datetime(2026, 10, 11, 0, 30, tzinfo=timezone.utc), "in")
        out, _ = self._sweep(monkeypatch, {LOOKBACK_BUCKET: [live], TODAY_BUCKET: [live]})
        assert out == ["live-1"]

    def test_an_id_less_game_renamed_between_buckets_is_one_row(self, monkeypatch):
        kickoff = datetime(2026, 10, 11, 3, 0, tzinfo=timezone.utc)
        a = _event(None, "H1", "A1", kickoff, "pre")
        b = dict(_event(None, "H1", "A1", kickoff, "pre"), name="A1 @ H1 (rescheduled)")
        out, _ = self._sweep(monkeypatch, {LOOKBACK_BUCKET: [a], TODAY_BUCKET: [b]},
                             convert=lambda ev: ev["name"])
        assert out == ["A1 at H1"]

    def test_id_less_events_are_not_merged(self, monkeypatch):
        kickoff = datetime(2026, 10, 11, 3, 0, tzinfo=timezone.utc)
        a = _event(None, "H1", "A1", kickoff, "pre")
        b = _event(None, "H2", "A2", kickoff, "pre")
        out, _ = self._sweep(monkeypatch, {TODAY_BUCKET: [a, b]},
                             convert=lambda ev: ev["name"])
        assert out == ["A1 at H1", "A2 at H2"]

    def test_field_events_filter_only_the_lookback_buckets(self, monkeypatch):
        # A tournament's status describes the current day's play: the forward
        # bucket's `post` Shanghai Masters (measured 2026-10-11) runs to the 19th.
        tennis = _event("315-2026", "Rolex Shanghai Masters", "Field",
                        datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc), "post")
        finished_card = _event("ufc-done", "UFC Fight Night", "Field",
                               datetime(2026, 10, 10, 18, 0, tzinfo=timezone.utc), "post")
        out, _ = self._sweep(monkeypatch, {
            LOOKBACK_BUCKET: [finished_card, tennis],
            TODAY_BUCKET: [tennis],
        }, field_events=True)
        assert out == ["315-2026"]


# ----------------------------------------------------------------- witnesses

def _src(module_name, cls_name, **kw):
    import importlib
    mod = importlib.import_module(f"dispatcharr_ranked_matchups.sources.{module_name}")
    return mod, getattr(mod, cls_name)(**kw)


# (module, class, constructor kwargs, event shape for the source's own filter)
# The league path is a LITERAL, not the module's ESPN_BASE: the witness must
# fail if a source sweeps the right days of the wrong league.
_ESPN_TEAM_SWEEPS = [
    ("nba", "NbaRegularSource", {}, {"season_type": 2}),
    ("nba", "NbaPlayoffSource", {}, {"season_type": 3}),
    ("nfl", "NflRegularSource", {}, {"season_type": 2}),
    ("nfl", "NflPlayoffSource", {}, {"season_type": 3}),
    ("wnba", "WnbaRegularSource", {}, {"season_type": 2}),
    ("wnba", "WnbaPlayoffSource", {}, {"season_type": 3}),
    ("ncaaw_basketball", "NcaawBasketballRegularSource", {}, {"season_type": 2}),
    ("ncaaw_basketball", "NcaawBasketballPlayoffSource", {}, {"season_type": 3}),
    ("ncaa_baseball", "NcaaBaseballRegularSource", {}, {"season_type": 2}),
    ("ncaa_baseball", "NcaaBaseballPlayoffSource", {}, {
        "season_type": 4,
        "headline": "NCAA Baseball Championship - Auburn Super Regional - Game 1"}),
    ("ncaa_baseball", "NcaaBaseballPlayoffBracketSource", {}, {
        "season_type": 3,
        "headline": "NCAA Baseball Championship - Auburn Regional - Game 1"}),
    ("ncaa_softball", "NcaaSoftballRegularSource", {}, {"season_type": 2}),
    ("ncaa_softball", "NcaaSoftballPlayoffSource", {}, {
        "season_type": 4,
        "headline": "NCAA Softball Championship - Norman Super Regional - Game 1"}),
    ("ncaa_softball", "NcaaSoftballPlayoffBracketSource", {}, {
        "season_type": 3,
        "headline": "NCAA Softball Championship - Norman Regional - Game 1"}),
    ("mls", "MlsSource", {"odds_api_key": ""}, {}),
    ("mls_cup", "MlsCupSource", {}, {"slug": "eastern-conference-playoffs---round-one"}),
]


class TestEveningGameIsFound:
    @pytest.mark.parametrize(
        "module_name,cls_name,kwargs,shape", _ESPN_TEAM_SWEEPS,
        ids=[f"{m}.{c}" for m, c, _k, _s in _ESPN_TEAM_SWEEPS],
    )
    def test_espn_sweep_finds_the_game_in_progress_at_10pm_eastern(
            self, monkeypatch, module_name, cls_name, kwargs, shape):
        mod, src = _src(module_name, cls_name, **kwargs)
        _pin_clock(monkeypatch)
        urls = []
        _serve_buckets(monkeypatch, mod, _evening(**shape), urls=urls)
        rows = src.fetch_upcoming(days_ahead=1)
        assert _homes(rows) == ["Home Live"]
        league = _LEAGUE_PATH[module_name]
        assert urls and all(f"/apis/site/v2/sports/{league}/scoreboard?" in u for u in urls)

    def test_mls_conference_sweep_finds_it(self, monkeypatch):
        mod, src = _src("mls_standings", "MlsEastSource")
        _pin_clock(monkeypatch)
        urls = []
        _serve_buckets(monkeypatch, mod, _evening(), urls=urls)
        monkeypatch.setattr(src, "_own_conf_teams",
                            lambda: {"Home Live", "Home Done", "Home Stale"})
        monkeypatch.setattr(src, "_fetch_closeness", lambda: {})
        assert _homes(src.fetch_upcoming(days_ahead=1)) == ["Home Live"]
        assert urls and all("/apis/site/v2/sports/soccer/usa.1/scoreboard?" in u for u in urls)

    def test_ufc_main_card_is_found_after_midnight_utc(self, monkeypatch):
        # 03:30Z (23:30 EDT): the card listed at 22:00Z is filed under the
        # 10th, live, 5.5 h after its listed start.
        mod, src = _src("field_event", "UfcSource")
        _pin_clock(monkeypatch, datetime(2026, 10, 11, 3, 30, tzinfo=timezone.utc))
        _serve_buckets(monkeypatch, mod, {LOOKBACK_BUCKET: [
            _event("ufc-live", "UFC 330: Main vs Event", "Field",
                   datetime(2026, 10, 10, 22, 0, tzinfo=timezone.utc), "in",
                   name="UFC 330: Main vs Event"),
            _event("ufc-done", "UFC Fight Night: Done", "Field",
                   datetime(2026, 10, 10, 18, 0, tzinfo=timezone.utc), "post",
                   name="UFC Fight Night: Done"),
            _event("ufc-stale", "UFC Fight Night: Stale", "Field",
                   datetime(2026, 10, 10, 17, 0, tzinfo=timezone.utc), "pre",
                   name="UFC Fight Night: Stale"),
        ]})
        assert _homes(src.fetch_upcoming(days_ahead=1)) == ["UFC 330: Main vs Event"]

    def test_a_card_past_the_age_floor_is_not_readmitted_even_live(self, monkeypatch):
        # 04:30Z: 6.5 h after the listed start. plugin._game_end_utc dated the
        # card's end at start + 4 h, so it is already a Past slot; an applied
        # card stays on air through _rows_to_hold, a new one is not added.
        mod, src = _src("field_event", "UfcSource")
        _pin_clock(monkeypatch, datetime(2026, 10, 11, 4, 30, tzinfo=timezone.utc))
        _serve_buckets(monkeypatch, mod, {LOOKBACK_BUCKET: [
            _event("ufc-live", "UFC 330: Main vs Event", "Field",
                   datetime(2026, 10, 10, 22, 0, tzinfo=timezone.utc), "in",
                   name="UFC 330: Main vs Event"),
        ]})
        assert src.fetch_upcoming(days_ahead=1) == []

    @pytest.mark.parametrize("module_name,cls_name,headline,sub1,sub2", [
        ("ncaa_baseball", "NcaaBaseballPlayoffBracketSource",
         "Men's College World Series - Double Elimination Round", "MCWS_sub1", "MCWS_sub2"),
        ("ncaa_softball", "NcaaSoftballPlayoffBracketSource",
         "Women's College World Series - Double Elimination Round", "WCWS_sub1", "WCWS_sub2"),
    ])
    @pytest.mark.parametrize("now", [
        datetime(2026, 6, 5, 23, 30, tzinfo=timezone.utc),  # opening night, one opener over
        datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),   # Day 3: opening day out of the window
    ], ids=["opening-night", "day-3"])
    def test_world_series_games_survive_and_get_the_brackets_sub_bracket(
            self, monkeypatch, module_name, cls_name, headline, sub1, sub2, now):
        """The sub-brackets are read off opening-day pairings, which the
        upcoming window cannot see once they are FINISHED or a day old. The
        sweep must neither drop a game for that nor guess: rows carry no key
        until place_upcoming stamps the bracket's (built from the whole
        calendar). Before the codex review fixes, the opener's teams were
        unknown on opening night and Day 1 was outside the window on Day 3,
        and their games were dropped. Literal keys: Day 1 pairs are
        sub-bracket 1, Day 2 pairs sub-bracket 2."""
        mod, src = _src(module_name, cls_name, season_year=2026)
        _pin_clock(monkeypatch, now)
        shape = {"season_type": 5, "headline": headline}
        june = lambda d, h: datetime(2026, 6, d, h, 0, tzinfo=timezone.utc)  # noqa: E731
        # Inside BOTH bracket calendars (softball's ends June 15).
        day2 = "post" if now.day == 7 else "pre"
        day1_late = "in" if now.day == 5 else "post"
        _serve_buckets(monkeypatch, mod, {
            "20260605": [
                _event("op-1", "Arizona", "Coastal Carolina", june(5, 18), "post", **shape),
                _event("op-2", "Louisville", "Oregon State", june(5, 23), day1_late, **shape),
            ],
            "20260606": [
                _event("d2-1", "Murray State", "UCLA", june(6, 18), day2, **shape),
                _event("d2-2", "LSU", "Arkansas", june(6, 23), day2, **shape),
            ],
            "20260607": [
                _event("d3-1", "Arizona", "Coastal Carolina", june(7, 18), "pre", **shape),
            ],
            "20260608": [
                _event("d4-1", "Louisville", "Oregon State", june(8, 18), "pre", **shape),
            ],
        })
        rows = src.fetch_upcoming(days_ahead=3)
        assert all(r.extra["grouping_key"] is None for r in rows)
        assert src.place_upcoming(rows) == 0
        keys = {(r.home, r.start_time.day): r.extra["grouping_key"] for r in rows}
        expected = {("Arizona", 7): sub1, ("Louisville", 8): sub1}
        if now.day == 5:
            expected.update({("Louisville", 5): sub1, ("Murray State", 6): sub2, ("LSU", 6): sub2})
        assert keys == expected

    def test_nhl_late_game_is_found(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import nhl
        _pin_clock(monkeypatch)
        requested = []

        def game(gid, home, start, state, game_type):
            return {"id": gid, "gameType": game_type, "gameState": state,
                    "startTimeUTC": start,
                    "homeTeam": {"placeName": {"default": home}, "commonName": {"default": "H"}},
                    "awayTeam": {"placeName": {"default": "Away"}, "commonName": {"default": "A"}}}

        def fake(url, *a, **k):
            day = url.rsplit("/", 1)[1]
            requested.append(day)
            first = date.fromisoformat(day)
            weeks = []
            for i in range(7):
                d = first + timedelta(days=i)
                games = []
                if d == date(2026, 10, 10):
                    for gt in (2, 3):
                        games += [
                            game(10 * gt + 1, f"Live{gt}", "2026-10-11T00:30:00Z", "LIVE", gt),
                            game(10 * gt + 2, f"Done{gt}", "2026-10-10T23:00:00Z", "OFF", gt),
                            game(10 * gt + 3, f"Stale{gt}", "2026-10-10T17:00:00Z", "FUT", gt),
                        ]
                if d == date(2026, 10, 20):  # past days_ahead=7: outside the window
                    games.append(game(99, "TooFar", "2026-10-20T23:00:00Z", "FUT", 2))
                weeks.append({"date": d.isoformat(), "games": games})
            return {"gameWeek": weeks}
        monkeypatch.setattr(nhl, "_http_get", fake)
        assert _homes(nhl.NhlRegularSource().fetch_upcoming(days_ahead=7)) == ["Live2 H"]
        assert _homes(nhl.NhlPlayoffSource().fetch_upcoming(days_ahead=7)) == ["Live3 H"]
        # One request per returned WEEK, from the lookback day: 2026-10-10 and
        # 2026-10-17 cover 10-10..10-18 for each source, where the old loop
        # asked eight times each.
        assert requested == ["2026-10-10", "2026-10-17"] * 2

    def test_mlb_late_west_coast_game_is_found_before_and_during_play(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import mlb
        _pin_clock(monkeypatch)
        urls = []

        def game(pk, home, start, state):
            return {"gamePk": pk, "gameDate": start, "status": {"abstractGameState": state},
                    "teams": {"home": {"team": {"name": home}}, "away": {"team": {"name": "Away"}}}}

        def fake(url, *a, **k):
            urls.append(url)
            start = re.search(r"startDate=(\d{4}-\d{2}-\d{2})", url).group(1)
            if start > "2026-10-10":  # statsapi files these under 2026-10-10
                return {"dates": []}
            return {"dates": [{"date": "2026-10-10", "games": [
                game(1, "Padres", "2026-10-11T02:40:00Z", "Preview"),   # first pitch in 40 min
                game(2, "Mariners", "2026-10-11T01:40:00Z", "Live"),
                game(3, "Dodgers", "2026-10-10T23:00:00Z", "Final"),
                game(4, "Postponed", "2026-10-11T00:10:00Z", "Final"),  # how statsapi reads a PPD
                game(5, "Stale", "2026-10-10T17:00:00Z", "Preview"),
            ]}, {"date": "2026-10-12", "games": [
                game(2, "Mariners", "2026-10-11T01:40:00Z", "Live"),     # a makeup listing
            ]}]}
        monkeypatch.setattr(mlb, "_http_get", fake)
        rows = mlb.MlbRegularSource().fetch_upcoming(days_ahead=7)
        assert _homes(rows) == ["Mariners", "Padres"]
        assert "startDate=2026-10-10&endDate=2026-10-18" in urls[0]

    def test_nhl_walk_does_not_skip_days_a_short_week_left_out(self, monkeypatch):
        """A gameWeek that comes back short or gapped (here: only 10-17 for a
        10-10 request) must not move the walk past 10-11..10-16 (codex)."""
        from dispatcharr_ranked_matchups.sources import nhl
        _pin_clock(monkeypatch)
        requested = []

        def fake(url, *a, **k):
            day = url.rsplit("/", 1)[1]
            requested.append(day)
            if day == "2026-10-10":
                return {"gameWeek": [{"date": "2026-10-17", "games": []}]}
            return {"gameWeek": [{"date": day, "games": [{
                "id": int(day.replace("-", "")), "gameType": 2, "gameState": "FUT",
                "startTimeUTC": f"{day}T23:00:00Z",
                "homeTeam": {"placeName": {"default": "H" + day[-2:]}, "commonName": {"default": "X"}},
                "awayTeam": {"placeName": {"default": "A"}, "commonName": {"default": "Y"}}}]}]}
        monkeypatch.setattr(nhl, "_http_get", fake)
        rows = nhl.NhlRegularSource().fetch_upcoming(days_ahead=2)
        assert requested == ["2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13"]
        assert _homes(rows) == ["H11 X", "H12 X", "H13 X"]

    def test_nhl_takes_a_day_from_its_own_answer_not_a_gapped_earlier_one(self, monkeypatch):
        """`schedule/2026-10-10` answers 10-10 and, past a gap, 10-12 with game
        42 at 20:00Z; `schedule/2026-10-11` corrects it to 21:00Z. The
        corrected kickoff is the one published (codex review)."""
        from dispatcharr_ranked_matchups.sources import nhl
        _pin_clock(monkeypatch)

        def game(start):
            return {"id": 42, "gameType": 2, "gameState": "FUT", "startTimeUTC": start,
                    "homeTeam": {"placeName": {"default": "Home"}, "commonName": {"default": "H"}},
                    "awayTeam": {"placeName": {"default": "Away"}, "commonName": {"default": "A"}}}

        answers = {
            "2026-10-10": [{"date": "2026-10-10", "games": []},
                           {"date": "2026-10-12", "games": [game("2026-10-12T20:00:00Z")]}],
            "2026-10-11": [{"date": "2026-10-11", "games": []},
                           {"date": "2026-10-12", "games": [game("2026-10-12T21:00:00Z")]},
                           {"date": "2026-10-13", "games": []}],
        }
        monkeypatch.setattr(nhl, "_http_get",
                            lambda url, *a, **k: {"gameWeek": answers.get(url.rsplit("/", 1)[1], [])})
        rows = nhl.NhlRegularSource().fetch_upcoming(days_ahead=2)
        assert [r.start_time.isoformat() for r in rows] == ["2026-10-12T21:00:00+00:00"]

    def test_nhl_merges_a_date_listed_twice(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import nhl
        _pin_clock(monkeypatch)
        live = {"id": 42, "gameType": 2, "gameState": "LIVE", "startTimeUTC": "2026-10-11T00:30:00Z",
                "homeTeam": {"placeName": {"default": "Home"}, "commonName": {"default": "H"}},
                "awayTeam": {"placeName": {"default": "Away"}, "commonName": {"default": "A"}}}
        monkeypatch.setattr(nhl, "_http_get", lambda url, *a, **k: {"gameWeek": [
            {"date": "2026-10-10", "games": []},
            {"date": "2026-10-10", "games": [live]},
            {"date": "2026-10-11", "games": []},
        ]} if url.endswith("/2026-10-10") else {"gameWeek": []})
        assert _homes(nhl.NhlRegularSource().fetch_upcoming(days_ahead=0)) == ["Home H"]

    def test_an_unplaced_world_series_row_is_reported(self, monkeypatch):
        """A College World Series row leaves fetch with no sub-bracket; if the
        bracket cannot place it, it scores no bracket importance, and
        place_upcoming must say so rather than count it placed (codex)."""
        from dispatcharr_ranked_matchups.sources.base import GameRow
        mod, src = _src("ncaa_baseball", "NcaaBaseballPlayoffBracketSource", season_year=2026)
        monkeypatch.setattr(src, "_fetch_bracket_games", lambda: [])
        row = GameRow(sport_prefix=src.sport_prefix, sport_label=src.sport_label,
                      home="Arizona", away="Coastal Carolina", rank_home=None, rank_away=None,
                      start_time=datetime(2026, 6, 7, 18, 0, tzinfo=timezone.utc),
                      extra={"game_id": "d3-1", "stage": "MCWS", "grouping_key": None})
        assert src.place_upcoming([row]) == 1

    def _bracket_row(self, src, home, away, stage, key, gid):
        from dispatcharr_ranked_matchups.sources.base import GameRow
        return GameRow(sport_prefix=src.sport_prefix, sport_label=src.sport_label,
                       home=home, away=away, rank_home=None, rank_away=None,
                       start_time=datetime(2026, 6, 7, 18, 0, tzinfo=timezone.utc),
                       extra={"game_id": gid, "stage": stage, "grouping_key": key})

    def test_an_unmatched_world_series_row_whose_teams_are_in_a_tie_is_not_reported(self, monkeypatch):
        """Not matched by id, but apply_result's team-set lookup still files
        its result into the tie holding both teams: scorable, so not counted
        (codex review round 4)."""
        mod, src = _src("ncaa_baseball", "NcaaBaseballPlayoffBracketSource", season_year=2026)
        day1 = datetime(2026, 6, 5, 18, 0, tzinfo=timezone.utc)

        def rec(gid, home, away):
            return {"game_id": gid, "stage": "MCWS", "matchday": 1, "home": home, "away": away,
                    "home_goals": None, "away_goals": None, "status": "SCHEDULED",
                    "start_time": day1, "extra": {"grouping_key": "MCWS_sub1"}}
        monkeypatch.setattr(src, "_fetch_bracket_games", lambda: [
            rec("op-1", "Arizona", "Coastal Carolina"), rec("op-2", "Louisville", "Oregon State")])
        row = self._bracket_row(src, "Arizona", "Coastal Carolina", "MCWS", None, "not-in-bracket")
        assert src.place_upcoming([row]) == 0

    def test_a_regional_row_with_no_tie_to_reach_is_reported(self, monkeypatch):
        mod, src = _src("ncaa_baseball", "NcaaBaseballPlayoffBracketSource", season_year=2026)
        monkeypatch.setattr(src, "_fetch_bracket_games", lambda: [])
        row = self._bracket_row(src, "Auburn", "Navy", "BSB_REG", "Auburn Regional", "r-1")
        assert src.place_upcoming([row]) == 1

    @pytest.mark.parametrize("old_day,old_start,new_day,new_start", [
        ("2026-10-10", "2026-10-11T03:00:00Z", "2026-10-12", "2026-10-12T23:00:00Z"),  # moved later
        ("2026-10-12", "2026-10-12T23:00:00Z", "2026-10-11", "2026-10-11T17:00:00Z"),  # moved earlier
    ], ids=["moved-later", "moved-earlier"])
    @pytest.mark.parametrize("old_first", [True, False], ids=["old-listed-first", "old-listed-last"])
    def test_mlb_publishes_the_slot_a_moved_game_moved_to(
            self, monkeypatch, old_day, old_start, new_day, new_start, old_first):
        """statsapi lists a moved game under both dates with one gamePk; the
        abandoned listing carries `rescheduleDate` (measured on all 28
        postponements of 2026). Whatever the order and the direction, and
        while both copies still read Preview, the new slot is published
        (codex review)."""
        from dispatcharr_ranked_matchups.sources import mlb
        _pin_clock(monkeypatch)

        def listing(day, start, **marks):
            return {"date": day, "games": [{
                "gamePk": 7, "gameDate": start, "status": {"abstractGameState": "Preview"},
                "teams": {"home": {"team": {"name": "Padres"}}, "away": {"team": {"name": "Giants"}}},
                **marks}]}
        old = listing(old_day, old_start, rescheduleDate=new_start, rescheduleGameDate=new_day)
        new = listing(new_day, new_start, rescheduledFrom=old_start, rescheduledFromDate=old_day)
        monkeypatch.setattr(mlb, "_http_get", lambda *a, **k: {"dates": [old, new] if old_first else [new, old]})
        rows = mlb.MlbRegularSource().fetch_upcoming(days_ahead=7)
        assert [r.start_time.isoformat() for r in rows] == [new_start.replace("Z", "+00:00")]

# ---------------------------------------------------------------- class guard

def _is_clock_call(node):
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("now", "utcnow"))


def _today_anchors(tree):
    """Calls in `tree` that turn the clock into a date: `<now()>.date()`,
    `<utcnow()>.date()`, `x.date()` where `x = <now()>` in the same tree,
    and `date.today()` / `datetime.today()`."""
    clock_names = {
        t.id for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and _is_clock_call(node.value)
        for t in node.targets if isinstance(t, ast.Name)
    }
    hits = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        attr, recv = node.func.attr, node.func.value
        if attr == "date" and (_is_clock_call(recv)
                               or (isinstance(recv, ast.Name) and recv.id in clock_names)):
            hits.append(node.lineno)
        elif attr == "today" and isinstance(recv, ast.Name) and recv.id in ("date", "datetime"):
            hits.append(node.lineno)
    return hits


def _source_functions():
    for path in sorted(glob.glob(os.path.join(SOURCES_DIR, "*.py"))):
        tree = ast.parse(open(path, encoding="utf-8").read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                yield os.path.basename(path), node


def _direct_day_walks(fn):
    return [
        n.func.id if isinstance(n.func, ast.Name) else n.func.attr
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and (getattr(n.func, "id", None) or getattr(n.func, "attr", None))
        in ("fetch_scoreboard_days", "fetch_scoreboard_day")
    ]


class TestNoSourceAnchorsAtTodayUtc:
    def test_the_instrument_fires(self):
        bad = ast.parse(textwrap.dedent('''
            def fetch_upcoming(self, days_ahead=7):
                today = datetime.now(timezone.utc).date()
                also = date.today()
                now = datetime.now(timezone.utc)
                two_step = now.date()
        '''))
        assert len(_today_anchors(bad)) == 3

    def test_the_instrument_finds_the_one_allowed_anchor(self):
        allowed = [(f, fn.name) for f, fn in _source_functions() if _today_anchors(fn)]
        # ast.walk visits nested defs twice (once inside their parent), so
        # compare as a set.
        assert set(allowed) == {("_espn.py", "upcoming_window")}

    def test_no_source_turns_the_clock_into_a_date(self):
        offenders = {
            (f, fn.name): _today_anchors(fn) for f, fn in _source_functions()
            if _today_anchors(fn) and not (f == "_espn.py" and fn.name == "upcoming_window")
        }
        assert offenders == {}, (
            "compute today-UTC through _espn.upcoming_window; a sweep anchored at "
            "today-UTC misses every evening-Eastern game from 00:00Z (#256)"
        )

    def test_no_fetch_upcoming_walks_scoreboard_days_itself(self):
        offenders = {
            f: _direct_day_walks(fn) for f, fn in _source_functions()
            if fn.name == "fetch_upcoming" and _direct_day_walks(fn)
        }
        assert offenders == {}, (
            "an ESPN fetch_upcoming walks its days through _espn.sweep_upcoming_events, "
            "which owns the lookback window and the FINISHED/stale drops"
        )


_DATED = (
    re.compile(r"[?&]dates=(\d{4})(\d{2})(\d{2})"),        # ESPN scoreboard
    re.compile(r"/schedule/(\d{4})-(\d{2})-(\d{2})(?:$|\?)"),  # NHL api-web
    re.compile(r"[?&]startDate=(\d{4})-(\d{2})-(\d{2})"),  # MLB statsapi
)


@pytest.fixture
def every_source_under_a_pinned_clock(plugin, monkeypatch):
    """Every source `_build_sources` builds with everything switched on, each
    `fetch_upcoming` run once against a stub that records the dated requests.
    Returns {class name: EARLIEST date requested}. Earliest, not first:
    the days are fetched concurrently, and the sweep's dedupe runs in date
    order whatever order the requests went out in."""
    import requests
    from tests.test_mapped_prefixes import _all_on_settings

    def _offline(*_a, **_kw):
        raise socket.gaierror("network disabled in tests/test_upcoming_window.py")

    monkeypatch.setattr(socket, "getaddrinfo", _offline)
    sources = plugin._build_sources(_all_on_settings())
    _pin_clock(monkeypatch)

    current = {}

    class _Resp:
        status_code = 200
        headers = {}
        text = "{}"

        def json(self):
            return {}

        def raise_for_status(self):
            return None

    def fake_get(url, *a, **k):
        for rx in _DATED:
            m = rx.search(url)
            if m:
                day = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                name = current["name"]
                current["earliest"][name] = min(day, current["earliest"].get(name, day))
        return _Resp()

    monkeypatch.setattr(requests, "get", fake_get)
    current["earliest"] = {}
    for src in sources:
        current["name"] = type(src).__name__
        src.fetch_upcoming(days_ahead=2)
    return current["earliest"]


class TestEverySourceStartsAtTheLookbackDay:
    """Behavioural half: the AST check cannot see a date computed some other
    way (a helper, `time.time()`, an arithmetic on a datetime). Under a clock
    pinned at 2026-10-11T02:00Z, the earliest day any source requests must be
    2026-10-10: not later (the #256 defect), and not earlier either (a window
    that drifted back would admit the day before as well)."""

    def test_the_instrument_reaches_the_dated_sweeps(self, every_source_under_a_pinned_clock):
        # mls_standings is absent on purpose: with no standings it returns
        # before sweeping (its sweep is pinned by the witness above).
        expected = {
            "NbaRegularSource", "NbaPlayoffSource", "NflRegularSource", "NflPlayoffSource",
            "WnbaRegularSource", "WnbaPlayoffSource", "NhlRegularSource", "NhlPlayoffSource",
            "MlbRegularSource", "NcaaBaseballRegularSource", "NcaaSoftballRegularSource",
            "NcaawBasketballRegularSource", "MlsCupSource", "UfcSource", "GolfSource",
            "InternationalFriendliesSource", "EflCupSource",
        }
        missing = expected - set(every_source_under_a_pinned_clock)
        assert missing == set()

    def test_every_dated_sweep_starts_one_day_before_today_utc(self, every_source_under_a_pinned_clock):
        late = {name: earliest.isoformat() for name, earliest in every_source_under_a_pinned_clock.items()
                if earliest != date(2026, 10, 10)}
        assert late == {}
