"""Sanity tests for the sport adapters. Network isn't called — we just check
class-level constants and the shape-handling logic so a typo or refactor
doesn't ship silently."""

from datetime import datetime, timezone

from dispatcharr_ranked_matchups.sources import (
    NcaafSource,
    NcaamSource,
    SoccerSource,
    SOCCER_COMPETITIONS,
)
from dispatcharr_ranked_matchups.sources.base import SportSource


class TestNcaafConstants:
    def test_implements_interface(self):
        assert issubclass(NcaafSource, SportSource)

    def test_constants(self):
        assert NcaafSource.sport_prefix == "CFB"
        assert NcaafSource.sport_label == "NCAA Football"

    def test_no_key_returns_empty(self):
        assert NcaafSource(api_key="").fetch_upcoming() == []

    def test_default_poll(self):
        assert NcaafSource(api_key="x").poll_name == "AP Top 25"

    def test_custom_poll(self):
        assert NcaafSource(api_key="x", poll_name="Coaches Poll").poll_name == "Coaches Poll"


class TestNcaafSeasonYear:
    """CFBD ?year= is the season's START year (NCAAF runs Aug-Jan).
    Pivot at month 8."""

    def test_september_uses_current_year(self):
        # Current = Sep 2026 → NCAAF 2026 season started → year=2026
        from dispatcharr_ranked_matchups.sources import ncaaf as ncaaf_mod

        class FakeDt:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 9, 5, tzinfo=timezone.utc)

        original = ncaaf_mod.datetime
        ncaaf_mod.datetime = FakeDt
        try:
            assert NcaafSource._current_season_year() == 2026
        finally:
            ncaaf_mod.datetime = original

    def test_january_uses_prior_year(self):
        from dispatcharr_ranked_matchups.sources import ncaaf as ncaaf_mod

        class FakeDt:
            @staticmethod
            def now(tz=None):
                return datetime(2027, 1, 5, tzinfo=timezone.utc)

        original = ncaaf_mod.datetime
        ncaaf_mod.datetime = FakeDt
        try:
            # Jan 2027 → still in the 2026-27 season → year=2026
            assert NcaafSource._current_season_year() == 2026
        finally:
            ncaaf_mod.datetime = original


class TestNcaamConstants:
    def test_implements_interface(self):
        assert issubclass(NcaamSource, SportSource)

    def test_constants(self):
        assert NcaamSource.sport_prefix == "CBB"
        assert NcaamSource.sport_label == "NCAA Men's Basketball"

    def test_no_key_returns_empty(self):
        assert NcaamSource(api_key="").fetch_upcoming() == []


class TestNcaamSeasonYear:
    """CBB ?season= is the season's END year (NCAAM runs Nov-Apr).
    Pivot at month 11. season=2025 means 2024-25 season."""

    def test_january_uses_current_year(self):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeDt:
            @staticmethod
            def now(tz=None):
                return datetime(2027, 1, 5, tzinfo=timezone.utc)

        original = ncaam_mod.datetime
        ncaam_mod.datetime = FakeDt
        try:
            # Jan 2027 in the 2026-27 season → end-year = 2027
            assert NcaamSource._current_season_year() == 2027
        finally:
            ncaam_mod.datetime = original

    def test_november_uses_next_year(self):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeDt:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 11, 5, tzinfo=timezone.utc)

        original = ncaam_mod.datetime
        ncaam_mod.datetime = FakeDt
        try:
            # Nov 5 2026 = 2026-27 season starting → end-year = 2027
            assert NcaamSource._current_season_year() == 2027
        finally:
            ncaam_mod.datetime = original

    def test_offseason_uses_current_year(self):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeDt:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 7, 5, tzinfo=timezone.utc)

        original = ncaam_mod.datetime
        ncaam_mod.datetime = FakeDt
        try:
            # Jul 2026 (offseason) → returns 2026, gets last season's data,
            # date filter drops everything since all games are in the past.
            assert NcaamSource._current_season_year() == 2026
        finally:
            ncaam_mod.datetime = original


class TestNcaamRankingsShape:
    """CBB /rankings is a flat list. Make sure we parse the latest week's
    AP poll out correctly without falling back to early-season weeks."""

    def test_latest_week_wins(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeResp:
            status_code = 200
            def __init__(self, payload):
                self._p = payload
            def raise_for_status(self):
                pass
            def json(self):
                return self._p

        # Mix of week=1 and week=15 entries — we want only week 15.
        payload = [
            {"week": 1,  "pollType": "AP Top 25", "team": "Old #1", "ranking": 1},
            {"week": 1,  "pollType": "AP Top 25", "team": "Old #2", "ranking": 2},
            {"week": 15, "pollType": "AP Top 25", "team": "Latest #1", "ranking": 1},
            {"week": 15, "pollType": "AP Top 25", "team": "Latest #2", "ranking": 2},
            # Different poll type — should be ignored
            {"week": 15, "pollType": "Coaches Poll", "team": "Other", "ranking": 1},
        ]
        monkeypatch.setattr(ncaam_mod.requests, "get", lambda *a, **kw: FakeResp(payload))

        src = NcaamSource(api_key="x")
        ranks = src._fetch_rankings(2026)
        assert ranks == {"Latest #1": 1, "Latest #2": 2}

    def test_empty_response_returns_none(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeResp:
            def raise_for_status(self): pass
            def json(self): return []

        monkeypatch.setattr(ncaam_mod.requests, "get", lambda *a, **kw: FakeResp())
        assert NcaamSource(api_key="x")._fetch_rankings(2026) is None

    def test_unknown_poll_type_returns_none(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeResp:
            def raise_for_status(self): pass
            def json(self):
                return [{"week": 1, "pollType": "Coaches Poll", "team": "X", "ranking": 1}]

        monkeypatch.setattr(ncaam_mod.requests, "get", lambda *a, **kw: FakeResp())
        # We default to AP Top 25 — Coaches Poll doesn't match.
        assert NcaamSource(api_key="x")._fetch_rankings(2026) is None


class TestNcaamGamesShape:
    """CBB /games has no `week` field, uses startDateRange/endDateRange."""

    def test_uses_date_range_params(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        captured = {}

        class FakeResp:
            def raise_for_status(self): pass
            def json(self): return []

        def fake_get(url, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResp()

        monkeypatch.setattr(ncaam_mod.requests, "get", fake_get)

        src = NcaamSource(api_key="x")
        start = datetime(2025, 2, 1, tzinfo=timezone.utc)
        end = datetime(2025, 2, 8, tzinfo=timezone.utc)
        src._fetch_games(start, end)

        # /games endpoint is hard-capped at 3000 without offset; date-range
        # is the only correct way to query a specific window.
        assert captured["url"].endswith("/games")
        assert captured["params"]["startDateRange"] == "2025-02-01"
        assert captured["params"]["endDateRange"] == "2025-02-08"
        # Critically: we are NOT using ?season= or ?year= which would pull
        # the first 3000 games of the season and silently miss late ones.
        assert "season" not in captured["params"]
        assert "year" not in captured["params"]


class TestNcaamSpreadsShape:
    """CBB /lines is keyed by gameId, not id. Use date-range, not week."""

    def test_keyed_by_game_id(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        payload = [
            {"gameId": 100, "lines": [{"provider": "ESPN BET", "spread": 3.5}]},
            {"gameId": 101, "lines": [{"provider": "FanDuel", "spread": -7.0}]},
            {"gameId": 102, "lines": []},  # no lines available
            {"gameId": 103, "lines": [{"provider": "X", "spread": None},
                                       {"provider": "Y", "spread": 2.0}]},
        ]

        class FakeResp:
            def raise_for_status(self): pass
            def json(self): return payload

        monkeypatch.setattr(ncaam_mod.requests, "get", lambda *a, **kw: FakeResp())

        src = NcaamSource(api_key="x")
        start = datetime(2025, 2, 1, tzinfo=timezone.utc)
        end = datetime(2025, 2, 8, tzinfo=timezone.utc)
        spreads = src._fetch_spreads(start, end)
        # Always absolute values
        assert spreads == {100: 3.5, 101: 7.0, 103: 2.0}


class TestSoccerCompetitions:
    def test_known_keys(self):
        for k in ("epl", "championship", "ucl"):
            assert k in SOCCER_COMPETITIONS

    def test_epl_config(self):
        cfg = SOCCER_COMPETITIONS["epl"]
        assert cfg.fd_code == "PL"
        assert cfg.sport_prefix == "EPL"
        assert cfg.use_position_as_rank is True
        assert cfg.rank_cap == 20
        # 20 teams × 2 (home + away) - 2 (each side plays itself once) = 38
        assert cfg.total_matchdays == 38

    def test_championship_total_matchdays(self):
        # 24 teams → 46 matchdays. Drives the "Matchday X of 46" line in
        # descriptions; off-by-one would silently mislead users.
        assert SOCCER_COMPETITIONS["championship"].total_matchdays == 46

    def test_ucl_no_total_matchdays(self):
        # Knockout/group format has no fixed-length season; total stays 0
        # so the matchday line is suppressed for UCL fixtures.
        assert SOCCER_COMPETITIONS["ucl"].total_matchdays == 0

    def test_ucl_does_not_use_position_as_rank(self):
        assert SOCCER_COMPETITIONS["ucl"].use_position_as_rank is False

    def test_unknown_competition_rejected(self):
        try:
            SoccerSource("bundesliga", fd_api_key="x")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")


class TestPreviousSeasonStartYear:
    """B.2 / TUNING_REPORT finding #2: the seed-year derivation must pin
    to the right calendar pivot. Soccer seasons run Aug-May. Pre-August →
    current season started last year → previous-season is two years ago.
    August onward → current season started this year → previous is last
    year. Off-by-one would query the wrong FD.org season."""

    def test_august_returns_previous_year(self):
        from dispatcharr_ranked_matchups.sources import soccer as soccer_mod
        # Aug 2026 → 2026-27 season is current → previous is 2025-26 (start=2025).
        assert soccer_mod._previous_season_start_year(
            datetime(2026, 8, 15, tzinfo=timezone.utc)
        ) == 2025

    def test_january_returns_two_years_ago(self):
        from dispatcharr_ranked_matchups.sources import soccer as soccer_mod
        # Jan 2026 → still in 2025-26 season → previous is 2024-25 (start=2024).
        assert soccer_mod._previous_season_start_year(
            datetime(2026, 1, 15, tzinfo=timezone.utc)
        ) == 2024

    def test_may_end_of_season_returns_two_years_ago(self):
        from dispatcharr_ranked_matchups.sources import soccer as soccer_mod
        # May 2026 → 2025-26 season ending → previous is still 2024-25.
        assert soccer_mod._previous_season_start_year(
            datetime(2026, 5, 24, tzinfo=timezone.utc)
        ) == 2024

    def test_july_returns_two_years_ago(self):
        from dispatcharr_ranked_matchups.sources import soccer as soccer_mod
        # July (preseason) → current season hasn't started yet → previous
        # is the one that just ENDED (2024-25), not the upcoming one.
        assert soccer_mod._previous_season_start_year(
            datetime(2026, 7, 31, tzinfo=timezone.utc)
        ) == 2024


class TestH2HToCloseness:
    """B.3: bookmaker h2h odds → devigged probabilities → coinflip-ness.
    The math is small; the tests pin it so a future refactor (e.g.,
    different sportsbook market format) doesn't silently change the
    close-game signal magnitude across the user's whole season."""

    def _call(self, *args, **kwargs):
        from dispatcharr_ranked_matchups.sources import soccer as soccer_mod
        return soccer_mod._h2h_to_closeness(*args, **kwargs)

    def test_perfect_coinflip(self):
        # 3-way: home 2.0, draw 4.0, away 2.0 (vig-free).
        # raw_implied: 0.5, 0.25, 0.5 → total 1.25.
        # devigged: 0.4, 0.2, 0.4 → 2 * min(0.4, 0.4) = 0.8.
        outcomes = [
            {"name": "home_team", "price": 2.0},
            {"name": "draw",      "price": 4.0},
            {"name": "away_team", "price": 2.0},
        ]
        c = self._call(outcomes, "home_team", "away_team")
        assert c is not None
        assert abs(c - 0.8) < 0.01

    def test_blowout_low_closeness(self):
        # 1.20 / 6.0 / 12.0: heavy home favorite.
        # raw: 0.833, 0.167, 0.083 → total 1.083.
        # devigged: 0.769, 0.154, 0.077 → 2 * min = 0.154.
        outcomes = [
            {"name": "home_team", "price": 1.20},
            {"name": "draw",      "price": 6.0},
            {"name": "away_team", "price": 12.0},
        ]
        c = self._call(outcomes, "home_team", "away_team")
        assert c is not None
        assert c < 0.20

    def test_two_way_market_no_draw(self):
        # NCAAF / NCAAM h2h is 2-way (no draw). Same math.
        # 1.91 / 1.91 → raw 0.524, 0.524 → total 1.047.
        # devigged 0.5, 0.5 → 2 * min = 1.0 (true coinflip).
        outcomes = [
            {"name": "ohio_state", "price": 1.91},
            {"name": "penn_state", "price": 1.91},
        ]
        c = self._call(outcomes, "ohio_state", "penn_state")
        assert c is not None
        assert abs(c - 1.0) < 0.01

    def test_missing_home_returns_none(self):
        # If the bookmaker doesn't list the home team's outcome, we
        # can't compute closeness — return None instead of guessing.
        outcomes = [
            {"name": "draw",      "price": 4.0},
            {"name": "away_team", "price": 2.0},
        ]
        c = self._call(outcomes, "home_team", "away_team")
        assert c is None

    def test_invalid_price_skipped(self):
        # A 1.0 or non-numeric price is malformed; skipping it leaves
        # one outcome valid → home_team's data missing → None.
        outcomes = [
            {"name": "home_team", "price": 1.0},
            {"name": "away_team", "price": 2.0},
        ]
        c = self._call(outcomes, "home_team", "away_team")
        assert c is None

    def test_clamps_to_unit_interval(self):
        # Defensive: a degenerate input shouldn't push closeness outside [0, 1].
        outcomes = [
            {"name": "home_team", "price": 1.01},
            {"name": "away_team", "price": 100.0},
        ]
        c = self._call(outcomes, "home_team", "away_team")
        assert c is not None
        assert 0.0 <= c <= 1.0


class TestSoccerSeedFromPreviousSeason:
    """B.2: when current standings show too few games played, the seed
    function swaps to the previous season's final table so MD1-3 isn't
    the all-tied cold-start mess."""

    def _make_source(self):
        return SoccerSource("epl", fd_api_key="fake")

    def test_seed_skipped_when_median_played_at_threshold(self):
        # Median played >= threshold → keep current. Single FD.org call.
        src = self._make_source()
        current_table = [
            {"name": f"Team {i}", "position": i, "points": 50 - i, "playedGames": 5}
            for i in range(1, 21)
        ]
        calls = []
        def fake_fetch(season=None):
            calls.append(season)
            return ({r["name"]: r["position"] for r in current_table}, current_table)
        src._fetch_standings = fake_fetch
        by_team, table = src._fetch_standings_with_seed()
        assert table == current_table
        assert calls == [None]  # only the current-season call; no seed fetch

    def test_seed_used_when_median_played_below_threshold(self):
        # Median played < threshold → fetch previous and use it.
        src = self._make_source()
        current_table = [
            {"name": f"Team {i}", "position": i, "points": 0, "playedGames": 0}
            for i in range(1, 21)
        ]
        seed_table = [
            {"name": f"Veteran {i}", "position": i, "points": 90 - i * 4, "playedGames": 38}
            for i in range(1, 21)
        ]
        calls = []
        def fake_fetch(season=None):
            calls.append(season)
            if season is None:
                return ({r["name"]: r["position"] for r in current_table}, current_table)
            return ({r["name"]: r["position"] for r in seed_table}, seed_table)
        src._fetch_standings = fake_fetch
        by_team, table = src._fetch_standings_with_seed()
        assert len(calls) == 2
        assert calls[0] is None
        assert calls[1] is not None  # previous-season year passed
        # Returned table came from the seed, not the current sparse table.
        assert all(r["name"].startswith("Veteran") for r in table)
        # playedGames reset to 0 — the seed represents a fresh-season prior,
        # not last year's residual (matches_remaining must be the full new season).
        assert all(r["playedGames"] == 0 for r in table)
        # Positions preserved from the seed table.
        assert table[0]["position"] == 1
        # Points carried through so impact-narrative can render "X pts ahead".
        assert table[0]["points"] == seed_table[0]["points"]

    def test_seed_falls_through_when_previous_season_empty(self):
        # If the previous-season fetch returns nothing (API failure or
        # this is a brand-new competition with no history), keep the
        # current sparse table rather than producing empty rows.
        src = self._make_source()
        current_table = [
            {"name": "Team 1", "position": 1, "points": 0, "playedGames": 0},
        ]
        def fake_fetch(season=None):
            if season is None:
                return ({"Team 1": 1}, current_table)
            return ({}, [])
        src._fetch_standings = fake_fetch
        by_team, table = src._fetch_standings_with_seed()
        assert table == current_table

    def test_seed_skipped_when_current_table_empty(self):
        # Empty current table (median computes to 0) — by the rules above,
        # 0 < threshold → seed should fire. Verify it does.
        src = self._make_source()
        seed_table = [
            {"name": "Veteran 1", "position": 1, "points": 90, "playedGames": 38},
        ]
        def fake_fetch(season=None):
            if season is None:
                return ({}, [])
            return ({"Veteran 1": 1}, seed_table)
        src._fetch_standings = fake_fetch
        by_team, table = src._fetch_standings_with_seed()
        # Note: empty current → median is 0 (the `or [0]` fallback inside),
        # so the seed fires and we get the previous-season table.
        assert any(r["name"] == "Veteran 1" for r in table)
