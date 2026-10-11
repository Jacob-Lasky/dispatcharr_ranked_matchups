"""College Cup round routing against ESPN's real 2024 and 2025 data (#235).

ESPN stopped putting the round on NCAA soccer tournament events: in 2024 every
College Cup event carried `season.slug == "first-round"` (etc.), in 2025 only
5 of the 47 men's events (all third round) and 3 of the 63 women's did. The
rounds survive as season types on ESPN's season calendar, each with a UTC
window. These tests
drive the PRODUCTION fetch paths (`_fetch_bracket_games`, `fetch_upcoming`)
over the real scoreboards and calendars, trimmed into
`fixtures/espn_ncaa_soccer_cup.json`, with every expectation written as a
literal counted from that data.
"""
import copy
import json
import os
from collections import Counter
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from dispatcharr_ranked_matchups.sources import _espn
from dispatcharr_ranked_matchups.sources import ncaa_soccer as reg_mod
from dispatcharr_ranked_matchups.sources import ncaa_soccer_cup as cup_mod

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "espn_ncaa_soccer_cup.json")

with open(FIXTURE) as _f:
    DATA = json.load(_f)


def _event(row):
    """Rebuild the ESPN scoreboard event shape from one trimmed fixture row."""
    (eid, date, year, stype, slug, home, away, hs, as_, state, completed) = row
    return {
        "id": eid,
        "date": date,
        "season": {"year": year, "type": stype, "slug": slug},
        "competitions": [{
            "status": {"type": {"state": state, "completed": completed}},
            "competitors": [
                {"homeAway": "home", "team": {"location": home}, "score": hs},
                {"homeAway": "away", "team": {"location": away}, "score": as_},
            ],
            "notes": [],
        }],
    }


class FakeEspn:
    """Serves the fixture for exactly the two URL shapes the sources request,
    and FAILS the test on any other URL, so nothing reaches the network."""

    def __init__(self, key, *, calendar=True, strip_round_tags=False,
                 not_yet_played=False, live_ids=()):
        self.key = key
        self.calendar = calendar
        self.strip = strip_round_tags
        # The fixture is history: every game is FINISHED. The upcoming paths
        # drop finished games, so serve them as ESPN did before kickoff
        # (state "pre", no score), or mid-game for `live_ids`.
        self.not_yet_played = not_yet_played
        self.live_ids = set(live_ids)
        self.urls = []
        d = DATA[key]
        self.regular = d["season_types"][0]

    def __call__(self, url, *a, **k):
        self.urls.append(url)
        g, year = self.key[0], self.key[1:]
        if url == f"{_espn.ESPN_CORE_SOCCER_BASE}/usa.ncaa.{g}.1/seasons/{year}":
            if not self.calendar:
                return None
            return {"types": {"items": copy.deepcopy(DATA[self.key]["season_types"])}}
        prefix = f"{cup_mod.ESPN_BASE}/usa.ncaa.{g}.1/scoreboard?dates="
        assert url.startswith(prefix), f"unexpected URL {url}"
        day = url[len(prefix):]
        rows = DATA[self.key]["scoreboard"].get(day, [])
        events = [_event(r) for r in rows]
        if self.not_yet_played or self.live_ids:
            for e in events:
                if self.not_yet_played or e["id"] in self.live_ids:
                    live = e["id"] in self.live_ids
                    e["competitions"][0]["status"]["type"] = {
                        "state": "in" if live else "pre", "completed": False}
                    for c in e["competitions"][0]["competitors"]:
                        c["score"] = "0" if live else None
        if self.strip:
            # 2024 with its round tags removed: what 2025 looks like.
            for e in events:
                e["season"]["slug"] = self.regular["slug"]
                e["season"]["type"] = self.regular["type"]
        return {"events": events}


def _cup(key, **fake_kw):
    fake = FakeEspn(key, **fake_kw)
    src = cup_mod.NcaaSoccerCupSource(gender=key[0], season_year=int(key[1:]))
    return src, fake


def _bracket(key, **fake_kw):
    src, fake = _cup(key, **fake_kw)
    with patch.object(cup_mod, "_http_get", side_effect=fake):
        games = src._fetch_bracket_games()
    return games, src, fake


class _FakeDatetime:
    def __init__(self, pinned):
        self._pinned = pinned

    def now(self, tz=None):
        return self._pinned

    def __call__(self, *a, **k):
        return datetime(*a, **k)

    def __getattr__(self, name):
        return getattr(datetime, name)


# ---------------------------------------------------------------------------
# The 2025 brackets ESPN no longer tags (the bug)
# ---------------------------------------------------------------------------

class TestTwentyTwentyFiveBracket:
    def test_mens_bracket_is_found_whole(self):
        games, _, _ = _bracket("m2025")
        assert Counter(g["stage"] for g in games) == {
            "R64": 16, "R32": 16, "S16": 8, "E8": 4, "F4": 2, "NCG": 1,
        }

    def test_womens_bracket_is_found_whole(self):
        games, _, _ = _bracket("w2025")
        assert Counter(g["stage"] for g in games) == {
            "R64": 32, "R32": 16, "S16": 8, "E8": 4, "F4": 2, "NCG": 1,
        }

    def test_mens_rounds_land_on_the_right_games(self):
        # Literal games from ESPN's 2025 men's scoreboard (issue #235 quotes
        # the last three): one per round.
        games, _, _ = _bracket("m2025")
        by_id = {g["game_id"]: g for g in games}
        assert {"761332", "761411", "761410"} <= set(by_id)
        assert (by_id["761332"]["stage"], by_id["761332"]["home"], by_id["761332"]["away"]) == (
            "R64", "Duke", "Fairleigh Dickinson")
        assert by_id["761411"]["stage"] == "R32"   # Saint Louis at Indiana, Nov 23
        assert by_id["761410"]["stage"] == "R32"   # Marshall at NC State, Nov 23
        finals = [g for g in games if g["stage"] == "NCG"]
        assert [(g["home"], g["away"], g["status"]) for g in finals] == [
            ("NC State", "Washington", "FINISHED")]
        semis = sorted((g["home"], g["away"]) for g in games if g["stage"] == "F4")
        assert ("NC State", "Saint Louis") in semis

    def test_conference_tournament_games_stay_out(self):
        # Nov 16 2025 conference finals carry the same slug and type as every
        # tournament game; only the calendar tells them apart.
        games, _, _ = _bracket("m2025")
        ids = {g["game_id"] for g in games}
        assert "761062" not in ids   # Dayton at Saint Louis, A-10 final
        assert "761060" not in ids   # UCLA at Michigan, Big Ten final

    def test_routing_signal_is_recorded(self):
        games, _, _ = _bracket("w2025")
        how = Counter(g["extra"]["stage_routed_by"] for g in games)
        assert how == {"slug": 3, "round_window": 60}

    def test_one_calendar_request_per_refresh(self):
        _, _, fake = _bracket("m2025")
        cal = [u for u in fake.urls if "/seasons/" in u]
        assert cal == ["https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.ncaa.m.1/seasons/2025"]


# ---------------------------------------------------------------------------
# 2024: the calendar agrees with every round tag ESPN did publish
# ---------------------------------------------------------------------------

class TestCalendarAgreesWithTwentyTwentyFourTags:
    @pytest.mark.parametrize("key,expected", [
        # 48 men's events: the Marshall at SMU quarterfinal is listed twice
        # (723937 regulation 0-0, 724472 the shootout) and the dedupe keeps one.
        ("m2024", {"R64": 16, "R32": 16, "S16": 8, "E8": 4, "F4": 2, "NCG": 1}),
        ("w2024", {"R64": 32, "R32": 16, "S16": 8, "E8": 4, "F4": 2, "NCG": 1}),
    ])
    def test_untagged_routing_reproduces_every_tagged_stage(self, key, expected):
        tagged, _, _ = _bracket(key)
        untagged, _, _ = _bracket(key, strip_round_tags=True)
        assert Counter(g["stage"] for g in tagged) == expected
        assert {g["game_id"]: g["stage"] for g in untagged} == {
            g["game_id"]: g["stage"] for g in tagged}
        assert {g["extra"]["stage_routed_by"] for g in tagged} == {"slug"}
        assert {g["extra"]["stage_routed_by"] for g in untagged} == {"round_window"}

    def test_pk_duplicate_dedupes_under_window_routing_too(self):
        untagged, _, _ = _bracket("m2024", strip_round_tags=True)
        ids = {g["game_id"] for g in untagged}
        assert "724472" in ids and "723937" not in ids


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------

class TestCalendarUnavailable:
    def test_bracket_is_empty_and_not_cached(self):
        src, fake = _cup("w2025", calendar=False)
        with patch.object(cup_mod, "_http_get", side_effect=fake):
            assert src._fetch_bracket_games() == []
        assert src._bracket_games_cache is None
        # The next refresh builds a fresh source and gets the whole bracket.
        games, _, _ = _bracket("w2025")
        assert len(games) == 63

    def test_bracket_does_not_sweep_without_a_calendar(self):
        # A partial (slug-only) bracket would read 60 of 63 games as never
        # played: a wrong cascade, not a missing one. So no sweep at all.
        _, _, fake = _bracket("w2025", calendar=False)
        assert [u for u in fake.urls if "scoreboard" in u] == []

    def test_fetch_upcoming_degrades_to_tagged_events(self):
        src, fake = _cup("w2025", calendar=False, not_yet_played=True)
        pinned = datetime(2025, 11, 14, 12, 0, tzinfo=timezone.utc)
        rows = _upcoming(src, cup_mod, fake, pinned, 0)
        # The Nov 14 board holds the first first-round games; ESPN tagged one.
        assert [r.extra["stage"] for r in rows] == ["R64"]
        assert len([u for u in fake.urls if "/seasons/" in u]) == 1


# ---------------------------------------------------------------------------
# The rows production actually publishes
# ---------------------------------------------------------------------------

def _upcoming(source, mod, fake, pinned, days_ahead):
    # The clock is read by the shared upcoming sweep in _espn.
    with patch.object(mod, "_http_get", side_effect=fake), \
         patch.object(_espn, "datetime", _FakeDatetime(pinned)):
        return source.fetch_upcoming(days_ahead=days_ahead)


class TestUpcomingRows:
    def test_cup_emits_quarterfinals_and_semifinals_with_stages(self):
        src, fake = _cup("m2025", not_yet_played=True)
        rows = _upcoming(src, cup_mod, fake, datetime(2025, 12, 5, 15, 0, tzinfo=timezone.utc), 7)
        assert Counter(r.extra["stage"] for r in rows) == {"E8": 4, "F4": 2}
        assert {r.extra["fd_competition_code"] for r in rows} == {"NCAA_MSOC_CUP"}
        assert {r.sport_prefix for r in rows} == {"NCAAMSOC"}

    def test_evening_kickoff_in_progress_is_found_after_utc_midnight(self):
        """The semifinal NC State v Saint Louis kicked off 2025-12-13T01:30Z,
        8:30pm Eastern on Dec 12, so ESPN files it under dates=20251212. At
        02:00Z, mid-game, today-UTC is already Dec 13: a sweep starting
        there never asks for Dec 12 and the live semifinal drops out of the
        guide. The shared sweep's Eastern lookback asks for it."""
        src, fake = _cup("m2025", live_ids={"762455"})
        rows = _upcoming(src, cup_mod, fake, datetime(2025, 12, 13, 2, 0, tzinfo=timezone.utc), 3)
        assert [(r.extra["espn_event_id"], r.extra["stage"]) for r in rows] == [("762455", "F4")]

    def test_finished_games_are_not_upcoming(self):
        """Dec 6 2025, 21:30Z: two quarterfinals kicked off at 18:00Z and are
        over (the fixture's final scores), Georgetown v NC State (20:00Z) is
        still playing. A game that has ended is not an upcoming matchup."""
        src, fake = _cup("m2025", live_ids={"762207"})
        rows = _upcoming(src, cup_mod, fake, datetime(2025, 12, 6, 21, 30, tzinfo=timezone.utc), 0)
        assert [r.extra["espn_event_id"] for r in rows] == ["762207"]

    def test_regular_season_source_stops_at_the_tournament(self):
        reg = reg_mod.NcaaSoccerSource(gender="m", season_year=2025)
        reg._fetch_rankings = lambda: {}
        fake = FakeEspn("m2025", not_yet_played=True)
        rows = _upcoming(reg, reg_mod, fake, datetime(2025, 11, 14, 15, 0, tzinfo=timezone.utc), 7)
        # The Nov 14-21 2025 boards hold 42 games: 26 conference-tournament
        # games, then the 16 first-round games from Nov 20, which belong to
        # the College Cup source.
        assert len(rows) == 26
        assert max(r.start_time for r in rows) < datetime(2025, 11, 18, 5, 0, tzinfo=timezone.utc)

    def test_college_cup_row_survives_the_series_dedup(self, plugin):
        """End to end over the plugin's own collapse: regular source first,
        cup source second, exactly as _build_sources orders them. Before
        #235 the regular source also listed every tournament game, won the
        tie (same kickoff, earlier index) and the College Cup row, with its
        stage and College Cup importance, was the one dropped."""
        pinned = datetime(2025, 12, 5, 15, 0, tzinfo=timezone.utc)
        reg = reg_mod.NcaaSoccerSource(gender="m", season_year=2025)
        reg._fetch_rankings = lambda: {}
        cup, _ = _cup("m2025")
        reg_rows = _upcoming(reg, reg_mod, FakeEspn("m2025", not_yet_played=True), pinned, 7)
        cup_rows = _upcoming(cup, cup_mod, FakeEspn("m2025", not_yet_played=True), pinned, 7)
        games = reg_rows + cup_rows
        sources = [reg] * len(reg_rows) + [cup] * len(cup_rows)
        kept, kept_sources, _ = plugin._dedup_series_games(games, sources)
        assert len(kept) == 6
        assert all(s is cup for s in kept_sources)
        assert sorted(g.extra["stage"] for g in kept) == ["E8", "E8", "E8", "E8", "F4", "F4"]


# ---------------------------------------------------------------------------
# The regular season's boundary
# ---------------------------------------------------------------------------

class TestRegularSeasonCutoff:
    @pytest.mark.parametrize("key,cutoff", [
        ("m2025", datetime(2025, 11, 18, 5, 0, tzinfo=timezone.utc)),
        ("w2025", datetime(2025, 11, 11, 5, 0, tzinfo=timezone.utc)),
        ("m2024", datetime(2024, 11, 18, 5, 0, tzinfo=timezone.utc)),
        ("w2024", datetime(2024, 11, 12, 5, 0, tzinfo=timezone.utc)),
    ])
    def test_cutoff_comes_from_the_calendar(self, key, cutoff):
        src = reg_mod.NcaaSoccerSource(gender=key[0], season_year=int(key[1:]))
        with patch.object(reg_mod, "_http_get", side_effect=FakeEspn(key)):
            assert src._regular_season_cutoff() == cutoff

    def test_fallback_cutoff_is_the_end_of_the_eastern_day(self):
        src = reg_mod.NcaaSoccerSource(gender="w", season_year=2026)
        with patch.object(reg_mod, "_http_get", return_value=None):
            # Nov 17 Eastern ends at Nov 18 05:00Z.
            assert src._regular_season_cutoff() == datetime(2026, 11, 18, 5, 0, tzinfo=timezone.utc)

    def test_season_walk_ends_before_the_first_round(self):
        src = reg_mod.NcaaSoccerSource(gender="w", season_year=2025)
        fake = FakeEspn("w2025")
        days = []

        def tracking(url, *a, **k):
            if "dates=" in url:
                days.append(url.rsplit("dates=", 1)[1])
                # Days before Nov 1 are outside the fixture: an empty board.
                if url.rsplit("dates=", 1)[1] < "20251101":
                    return {"events": []}
            return fake(url)
        with patch.object(reg_mod, "_http_get", side_effect=tracking):
            games = src._fetch_full_season_games()
        assert max(days) == "20251110"
        # The women's first round started Nov 14; none of it is in the season.
        assert max(g["start_time"] for g in games) < datetime(2025, 11, 11, 5, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# The shared calendar helpers
# ---------------------------------------------------------------------------

class TestSeasonTypeWindows:
    def test_parses_sorts_and_skips_unusable_items(self):
        items = [
            {"slug": "second-round", "type": 2, "startDate": "2025-11-22T05:00Z", "endDate": "2025-11-25T04:59Z"},
            {"slug": "first-round", "type": "1", "startDate": "2025-11-18T05:00Z", "endDate": "2025-11-22T04:59Z"},
            {"slug": "", "type": 3, "startDate": "2025-11-25T05:00Z", "endDate": "2025-11-26T04:59Z"},
            {"slug": "backwards", "type": 4, "startDate": "2025-12-01T05:00Z", "endDate": "2025-11-30T04:59Z"},
            {"slug": "no-type", "startDate": "2025-12-02T05:00Z", "endDate": "2025-12-03T04:59Z"},
            "junk",
        ]
        ws = _espn.fetch_season_type_windows("usa.ncaa.m.1", 2025, lambda u: {"types": {"items": items}})
        assert [(w.slug, w.type_id) for w in ws] == [
            ("first-round", 1), ("second-round", 2), ("no-type", None)]

    @pytest.mark.parametrize("payload", [None, {}, {"types": {"items": []}}, "oops"])
    def test_nothing_usable_is_none(self, payload):
        assert _espn.fetch_season_type_windows("x", 2025, lambda u: payload) is None

    def test_boundary_minute_belongs_to_the_earlier_round(self):
        ws = _espn.fetch_season_type_windows(
            "usa.ncaa.m.1", 2025,
            lambda u: {"types": {"items": DATA["m2025"]["season_types"]}})
        at = lambda *a: _espn.season_type_at(ws, datetime(*a, tzinfo=timezone.utc))
        assert at(2025, 11, 18, 4, 59, 30).slug == "2025-ncaa-division-i-men"
        assert at(2025, 11, 18, 5, 0).slug == "first-round"
        assert at(2026, 1, 1, 5, 0) is None
        assert _espn.season_type_at(ws, None) is None


class TestRoundTypeId:
    def test_round_type_id_routes_without_slug_or_window(self):
        """Signal 2: an event carrying a round's season-type id is placed in
        that round even if its kickoff sits in another window (a game moved
        after ESPN set the calendar)."""
        src, fake = _cup("m2025")
        ev = _event(["x1", "2025-12-06T19:00Z", 2025, 13335, "2025-ncaa-division-i-men",
                     "A", "B", None, None, "pre", False])
        with patch.object(cup_mod, "_http_get", side_effect=fake):
            rec = src._extract_bracket_record(ev)
        assert rec is not None
        assert (rec["stage"], rec["extra"]["stage_routed_by"]) == ("F4", "season_type")

    def test_other_season_events_use_their_own_calendar(self):
        """An event stamped 2024 is read against the 2024 calendar, never the
        source's own year's windows."""
        src = cup_mod.NcaaSoccerCupSource(gender="m", season_year=2025)
        f24, f25 = FakeEspn("m2024"), FakeEspn("m2025")

        def both(url, *a, **k):
            return (f24 if url.endswith("/2024") else f25)(url)
        ev = _event(["x2", "2024-12-16T00:00Z", 2024, 12594, "2024-ncaa-division-i-men",
                     "A", "B", None, None, "pre", False])
        with patch.object(cup_mod, "_http_get", side_effect=both):
            rec = src._extract_bracket_record(ev)
        assert rec is not None
        assert rec["stage"] == "NCG"
        assert rec["extra"]["round_slug"] == "college-cup---championship"
