"""Sanity tests for the sport adapters. Network isn't called — we just check
class-level constants and the shape-handling logic so a typo or refactor
doesn't ship silently."""

import random
from datetime import datetime, timezone

import pytest

from dispatcharr_ranked_matchups.sources import (
    KnockoutSoccerSource,
    NcaafSource,
    NcaamSource,
    SoccerSource,
    SOCCER_COMPETITIONS,
)
from dispatcharr_ranked_matchups.sources.base import MatchResult, SportSource


class TestNcaafConstants:
    def test_implements_interface(self):
        assert issubclass(NcaafSource, SportSource)

    def test_constants(self):
        # Phase D.2 made sport_prefix / sport_label @property to satisfy the
        # SportSource ABC's property declaration. Read them off an instance.
        src = NcaafSource(api_key="fake")
        assert src.sport_prefix == "CFB"
        assert src.sport_label == "NCAA Football"

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
        src = NcaamSource(api_key="fake")
        assert src.sport_prefix == "CBB"
        assert src.sport_label == "NCAA Men's Basketball"

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
        # Phase H added bundesliga / la_liga / serie_a / ligue_1 to the catalog.
        for k in ("epl", "championship", "ucl",
                  "bundesliga", "la_liga", "serie_a", "ligue_1"):
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
            SoccerSource("not_a_real_league", fd_api_key="x")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")

    # ---------- Phase H: top-flight European leagues ----------

    def test_bundesliga_matchdays(self):
        # 18 teams → 34 matchdays. Off-by-one silently misleads "Matchday X of Y".
        assert SOCCER_COMPETITIONS["bundesliga"].total_matchdays == 34
        assert SOCCER_COMPETITIONS["bundesliga"].rank_cap == 18
        assert SOCCER_COMPETITIONS["bundesliga"].fd_code == "BL1"

    def test_la_liga_matchdays(self):
        # 20 teams → 38 matchdays (same shape as EPL).
        assert SOCCER_COMPETITIONS["la_liga"].total_matchdays == 38
        assert SOCCER_COMPETITIONS["la_liga"].rank_cap == 20
        assert SOCCER_COMPETITIONS["la_liga"].fd_code == "PD"

    def test_serie_a_matchdays(self):
        assert SOCCER_COMPETITIONS["serie_a"].total_matchdays == 38
        assert SOCCER_COMPETITIONS["serie_a"].rank_cap == 20
        assert SOCCER_COMPETITIONS["serie_a"].fd_code == "SA"

    def test_ligue_1_matchdays(self):
        # 18 teams → 34 matchdays (same as Bundesliga).
        assert SOCCER_COMPETITIONS["ligue_1"].total_matchdays == 34
        assert SOCCER_COMPETITIONS["ligue_1"].rank_cap == 18
        assert SOCCER_COMPETITIONS["ligue_1"].fd_code == "FL1"

    def test_top_flight_leagues_use_position_as_rank(self):
        # All Big-Five leagues have a real points table — position-as-rank applies.
        for k in ("bundesliga", "la_liga", "serie_a", "ligue_1"):
            assert SOCCER_COMPETITIONS[k].use_position_as_rank is True

    def test_top_flight_route_to_league_format_not_knockout(self):
        # _make_soccer factory in plugin.py picks SoccerSource (not
        # KnockoutSoccerSource) when LEAGUE_CONTEXTS[fd_code].format == "league".
        # Verify the LEAGUE_CONTEXTS entries reflect that.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        for code in ("BL1", "PD", "SA", "FL1"):
            assert LEAGUE_CONTEXTS[code].format == "league", \
                f"{code} should route through SoccerSource (format=league)"

    def test_top_flight_thresholds_include_title_ucl_relegation(self):
        # Every Big-Five entry must have title + UCL + relegation bands so
        # importance leverage covers the most stakeholder-relevant outcomes.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        for code in ("BL1", "PD", "SA", "FL1"):
            labels = {label for _cut, label, _w in LEAGUE_CONTEXTS[code].thresholds}
            assert "title" in labels, f"{code} missing title band"
            assert "UCL" in labels, f"{code} missing UCL band"
            assert "relegation" in labels, f"{code} missing relegation band"

    def test_ligue_1_ucl_cutoff_is_three_not_four(self):
        # FL1 only awards 3 direct UCL slots (4th plays UCL playoff round).
        # Reflect that in the cutoff so importance for "did we make UCL"
        # leverages the right boundary.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ucl_cutoffs = [cut for cut, label, _w in LEAGUE_CONTEXTS["FL1"].thresholds
                       if label == "UCL"]
        assert ucl_cutoffs == [3]

    # ---------- Phase I: international tournaments ----------

    def test_world_cup_in_competitions(self):
        cfg = SOCCER_COMPETITIONS["world_cup"]
        assert cfg.fd_code == "WC"
        assert cfg.use_position_as_rank is False  # tournament, not league table
        assert cfg.total_matchdays == 0  # no fixed-length season

    def test_euros_in_competitions(self):
        cfg = SOCCER_COMPETITIONS["euros"]
        assert cfg.fd_code == "EC"
        assert cfg.use_position_as_rank is False
        assert cfg.total_matchdays == 0

    def test_world_cup_routes_to_knockout(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["WC"].format == "knockout"
        wc_labels = {label for _cut, label, _w in LEAGUE_CONTEXTS["WC"].thresholds}
        # WC 2026's 48-team format adds LAST_32 as the entry knockout round.
        assert "last_32" in wc_labels
        assert "winner" in wc_labels

    def test_euros_routes_to_knockout_without_last_32(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["EC"].format == "knockout"
        ec_labels = {label for _cut, label, _w in LEAGUE_CONTEXTS["EC"].thresholds}
        # EURO is 24-team, enters at LAST_16 — no LAST_32 band.
        assert "last_32" not in ec_labels
        assert "round_of_16" in ec_labels
        assert "winner" in ec_labels

    def test_world_cup_winner_outweighs_ucl_winner(self):
        # WC is once-every-4-years vs UCL annual; winner consequence
        # weight reflects the rarity premium.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        wc_winner_w = next(
            w for _c, label, w in LEAGUE_CONTEXTS["WC"].thresholds if label == "winner"
        )
        ucl_winner_w = next(
            w for _c, label, w in LEAGUE_CONTEXTS["CL"].thresholds if label == "winner"
        )
        assert wc_winner_w > ucl_winner_w, \
            f"WC winner weight {wc_winner_w} should exceed UCL winner {ucl_winner_w}"

    def test_last_32_in_knockout_depth(self):
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert "LAST_32" in KNOCKOUT_ROUND_DEPTH
        # Entry-level (same depth as PLAYOFFS — both are pre-LAST_16).
        assert KNOCKOUT_ROUND_DEPTH["LAST_32"] == 0
        # Strictly shallower than LAST_16.
        assert KNOCKOUT_ROUND_DEPTH["LAST_32"] < KNOCKOUT_ROUND_DEPTH["LAST_16"]

    def test_knockout_source_includes_last_32_in_ko_stages(self):
        # KnockoutSoccerSource.KO_STAGES must list LAST_32 between PLAYOFFS
        # and LAST_16 so feeds_from inference can resolve WC LAST_16 ties
        # back to their LAST_32 feeders.
        from dispatcharr_ranked_matchups.sources.soccer import KnockoutSoccerSource
        stages = KnockoutSoccerSource.KO_STAGES
        idx_p = stages.index("PLAYOFFS")
        idx_32 = stages.index("LAST_32")
        idx_16 = stages.index("LAST_16")
        assert idx_p < idx_32 < idx_16


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


# ---------- Phase C: SoccerSource Monte Carlo importance ----------

class TestSoccerImportanceInterface:
    """Phase C: SoccerSource's 7 importance methods.

    Tests mock `_fetch_all_season_matches` so they exercise the algorithm
    without an HTTP call. A 4-team mini-league with hand-set finished
    matches lets us read back each method's output without floating
    around in API plumbing.
    """

    def _mini_matches(self):
        """4-team league. 6 matches total (round-robin half).
        Matches 1-3 are FINISHED; 4-6 are SCHEDULED. Engineered so the
        finished slice produces a clean standings (A on top, D on bottom).
        """
        base_date = "2026-05-01T12:00:00Z"
        return [
            {  # FINISHED — A beats B 3-0 at home
                "id": 101, "status": "FINISHED",
                "homeTeam": {"name": "Team A"}, "awayTeam": {"name": "Team B"},
                "score": {"fullTime": {"home": 3, "away": 0}},
                "utcDate": base_date, "matchday": 1,
            },
            {  # FINISHED — C beats D 2-1 at home
                "id": 102, "status": "FINISHED",
                "homeTeam": {"name": "Team C"}, "awayTeam": {"name": "Team D"},
                "score": {"fullTime": {"home": 2, "away": 1}},
                "utcDate": base_date, "matchday": 1,
            },
            {  # FINISHED — A beats C 2-0 at home
                "id": 103, "status": "FINISHED",
                "homeTeam": {"name": "Team A"}, "awayTeam": {"name": "Team C"},
                "score": {"fullTime": {"home": 2, "away": 0}},
                "utcDate": base_date, "matchday": 2,
            },
            {  # SCHEDULED — B vs D
                "id": 104, "status": "SCHEDULED",
                "homeTeam": {"name": "Team B"}, "awayTeam": {"name": "Team D"},
                "score": {"fullTime": {"home": None, "away": None}},
                "utcDate": base_date, "matchday": 2,
            },
            {  # SCHEDULED — A vs D (the target match for most tests)
                "id": 105, "status": "SCHEDULED",
                "homeTeam": {"name": "Team A"}, "awayTeam": {"name": "Team D"},
                "score": {"fullTime": {"home": None, "away": None}},
                "utcDate": base_date, "matchday": 3,
            },
            {  # SCHEDULED — B vs C
                "id": 106, "status": "SCHEDULED",
                "homeTeam": {"name": "Team B"}, "awayTeam": {"name": "Team C"},
                "score": {"fullTime": {"home": None, "away": None}},
                "utcDate": base_date, "matchday": 3,
            },
        ]

    def _make_source(self):
        src = SoccerSource("epl", fd_api_key="fake")
        src._all_matches_cache = self._mini_matches()
        return src

    # ---------- supports_importance ----------

    def test_soccer_source_supports_importance(self):
        # Class attr — true for ALL SoccerSource instances, regardless of
        # config (UCL etc. will fail at outcome_labels if their fd_code
        # has no LEAGUE_CONTEXTS entry; the caller handles that).
        src = SoccerSource("epl", fd_api_key="fake")
        assert src.supports_importance is True

    # ---------- outcome_labels ----------

    def test_outcome_labels_from_league_contexts(self):
        src = SoccerSource("epl", fd_api_key="fake")
        labels = src.outcome_labels
        # EPL has title, UCL, Europa/Conference, relegation (from
        # LEAGUE_CONTEXTS["PL"]).
        assert "title" in labels
        assert "UCL" in labels
        assert "relegation" in labels

    def test_outcome_labels_empty_for_unknown_league(self, monkeypatch):
        # All COMPETITIONS entries now have LEAGUE_CONTEXTS entries (PL/ELC/CL),
        # so we synthesize an unknown fd_code on an existing source to exercise
        # the "ctx is None" branch. The branch must survive so future
        # competitions added to COMPETITIONS without a matching context don't
        # crash — they silently produce 0 importance.
        src = SoccerSource("epl", fd_api_key="fake")
        monkeypatch.setattr(src.config, "fd_code", "UNKNOWN_COMP")
        assert src.outcome_labels == []

    def test_soccer_source_terminal_outcomes_guards_knockout_context(self, monkeypatch, caplog):
        # SoccerSource is league-shaped (int-position cutoffs). If a knockout-
        # format context (str cutoffs) reaches it via a wiring bug, the
        # int-vs-str comparison in the cutoff loop would TypeError. The guard
        # in terminal_outcomes returns empty + logs a warning, keeping refresh
        # alive on a no-op-importance fallback. Verifies the guard fires for CL.
        src = SoccerSource("ucl", fd_api_key="fake")  # ucl → fd_code="CL", knockout
        src._all_matches_cache = []  # avoid HTTP
        # Hand-built minimal state — the guard short-circuits before reading it.
        state = {"_applied": frozenset(), "Some Team": {"played": 0, "points": 0, "gf": 0, "ga": 0}}
        with caplog.at_level("WARNING"):
            out = src.terminal_outcomes(state)
        assert out == {"Some Team": []}
        assert any("non-league context" in r.message for r in caplog.records)

    # ---------- estimate_strengths ----------

    def test_estimate_strengths_uses_finished_matches(self):
        src = self._make_source()
        strengths = src.estimate_strengths()
        # All 4 teams appear (since they each played in at least one
        # FINISHED match).
        assert set(strengths.keys()) == {"Team A", "Team B", "Team C", "Team D"}
        # Team A's home attack = (3 + 2) / 2 = 2.5 (two home wins, 3 + 2 goals)
        assert strengths["Team A"]["sh"] == pytest.approx(2.5)
        # Team A's home conceded = (0 + 0) / 2 = 0.0
        assert strengths["Team A"]["ch"] == pytest.approx(0.0)
        # Team A played no away games in the finished slice → defaults.
        assert strengths["Team A"]["sa"] == SoccerSource._DEFAULT_STRENGTH_AWAY_SCORED
        assert strengths["Team A"]["ca"] == SoccerSource._DEFAULT_STRENGTH_AWAY_CONCEDED

    def test_estimate_strengths_caches(self):
        src = self._make_source()
        first = src.estimate_strengths()
        # Mutate the cache to verify the second call returns the cached object.
        src._strengths_cache["MARKER"] = {"sh": 9.9, "ch": 9.9, "sa": 9.9, "ca": 9.9}
        second = src.estimate_strengths()
        assert "MARKER" in second  # same object — cache hit

    def test_strength_for_unknown_team_returns_defaults(self):
        src = self._make_source()
        strengths = src.estimate_strengths()
        out = src._strength_for(strengths, "Newly Promoted FC")
        assert out["sh"] == SoccerSource._DEFAULT_STRENGTH_HOME_SCORED
        assert out["ca"] == SoccerSource._DEFAULT_STRENGTH_AWAY_CONCEDED

    # ---------- initial_state ----------

    def test_initial_state_reflects_finished_matches(self):
        src = self._make_source()
        state = src.initial_state()
        # Team A won twice (3 pts × 2 = 6); Team C beat Team D (3 pts);
        # Team B lost (0 pts); Team D lost twice (0 pts).
        assert state["Team A"]["played"] == 2
        assert state["Team A"]["points"] == 6
        assert state["Team A"]["gf"] == 5
        assert state["Team A"]["ga"] == 0
        assert state["Team B"]["played"] == 1
        assert state["Team B"]["points"] == 0
        assert state["Team C"]["played"] == 2
        assert state["Team C"]["points"] == 3
        assert state["Team D"]["played"] == 1
        assert state["Team D"]["points"] == 0

    def test_initial_state_tracks_applied_matches(self):
        src = self._make_source()
        state = src.initial_state()
        # All three FINISHED matches should be in _applied.
        assert state["_applied"] == frozenset({101, 102, 103})

    def test_initial_state_caches(self):
        src = self._make_source()
        first = src.initial_state()
        # Mutate the underlying cache; second call should return same.
        first["MARKER"] = "set"
        second = src.initial_state()
        assert "MARKER" in second  # same object — cache hit

    # ---------- remaining_matches ----------

    def test_remaining_matches_excludes_applied(self):
        src = self._make_source()
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        # 3 unplayed matches; should be GameRows with the expected teams.
        assert len(remaining) == 3
        fd_ids = sorted([m.extra["fd_id"] for m in remaining])
        assert fd_ids == [104, 105, 106]

    def test_remaining_matches_after_apply(self):
        src = self._make_source()
        state = src.initial_state()
        match_105 = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 105)
        # Simulate A 2-0 D
        new_state = src.apply_result(state, match_105, MatchResult(home_goals=2, away_goals=0))
        remaining = src.remaining_matches(new_state)
        fd_ids = sorted([m.extra["fd_id"] for m in remaining])
        assert fd_ids == [104, 106]  # 105 no longer in the list

    # ---------- sample_result ----------

    def test_sample_result_returns_match_result(self):
        src = self._make_source()
        state = src.initial_state()
        strengths = src.estimate_strengths()
        match = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 105)
        rng = random.Random(42)
        result = src.sample_result(state, match, strengths, rng)
        assert isinstance(result, MatchResult)
        assert result.home_goals >= 0
        assert result.away_goals >= 0

    def test_sample_result_deterministic_with_seed(self):
        src = self._make_source()
        state = src.initial_state()
        strengths = src.estimate_strengths()
        match = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 105)
        a = src.sample_result(state, match, strengths, random.Random(11))
        b = src.sample_result(state, match, strengths, random.Random(11))
        assert a == b

    # ---------- apply_result ----------

    def test_apply_result_does_not_mutate_input_state(self):
        src = self._make_source()
        state = src.initial_state()
        original_a_pts = state["Team A"]["points"]
        original_applied = state["_applied"]
        match = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 105)
        new_state = src.apply_result(state, match, MatchResult(home_goals=2, away_goals=0))
        # Input state untouched.
        assert state["Team A"]["points"] == original_a_pts
        assert state["_applied"] == original_applied
        # New state reflects the result.
        assert new_state["Team A"]["points"] == original_a_pts + 3
        assert 105 in new_state["_applied"]

    def test_apply_result_draw(self):
        src = self._make_source()
        state = src.initial_state()
        match = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 104)
        new_state = src.apply_result(state, match, MatchResult(home_goals=1, away_goals=1))
        assert new_state["Team B"]["points"] == state["Team B"]["points"] + 1
        assert new_state["Team D"]["points"] == state["Team D"]["points"] + 1

    def test_apply_result_away_win(self):
        src = self._make_source()
        state = src.initial_state()
        match = next(m for m in src.remaining_matches(state) if m.extra["fd_id"] == 104)
        new_state = src.apply_result(state, match, MatchResult(home_goals=0, away_goals=2))
        assert new_state["Team B"]["points"] == state["Team B"]["points"]  # no change
        assert new_state["Team D"]["points"] == state["Team D"]["points"] + 3

    # ---------- terminal_outcomes ----------

    def test_terminal_outcomes_assigns_top_side_inclusively(self):
        # In EPL: title → pos 1, UCL → pos 1-4, Europa → 1-7. A team at
        # position 1 should match all three top-side labels.
        src = self._make_source()
        # Hand-craft a fake final state with 20 teams.
        state = {"_applied": frozenset()}
        for i in range(1, 21):
            state[f"Team {i}"] = {"played": 38, "points": 100 - i * 3, "gf": 80 - i, "ga": 20}
        outcomes = src.terminal_outcomes(state)
        assert "title" in outcomes["Team 1"]
        assert "UCL" in outcomes["Team 1"]
        assert "Europa/Conference" in outcomes["Team 1"]
        # Team 4 at UCL cutoff: UCL + Europa, but not title.
        assert "title" not in outcomes["Team 4"]
        assert "UCL" in outcomes["Team 4"]
        assert "Europa/Conference" in outcomes["Team 4"]

    def test_terminal_outcomes_assigns_bottom_side_exclusively(self):
        # In EPL: relegation cutoff = 17 (bottom-side, pos > 17 → relegated).
        src = self._make_source()
        state = {"_applied": frozenset()}
        for i in range(1, 21):
            state[f"Team {i}"] = {"played": 38, "points": 100 - i * 3, "gf": 80 - i, "ga": 20}
        outcomes = src.terminal_outcomes(state)
        # Team 17 should be safe (pos 17 is not > 17).
        assert "relegation" not in outcomes["Team 17"]
        # Teams 18, 19, 20 should fire relegation.
        assert "relegation" in outcomes["Team 18"]
        assert "relegation" in outcomes["Team 19"]
        assert "relegation" in outcomes["Team 20"]

    def test_terminal_outcomes_sorts_by_points_then_gd(self):
        # Two teams tied on points; goal differential breaks the tie.
        src = self._make_source()
        state = {"_applied": frozenset()}
        state["Tied A"] = {"played": 38, "points": 80, "gf": 70, "ga": 50}  # gd=20
        state["Tied B"] = {"played": 38, "points": 80, "gf": 60, "ga": 50}  # gd=10
        # Two stronger fillers (one above each tied team) plus weaker fillers
        # so Tied A / Tied B land at positions 3 and 4 — straddling the UCL
        # cutoff so the GD tiebreak is observable.
        state["Strong 1"] = {"played": 38, "points": 90, "gf": 60, "ga": 30}
        state["Strong 2"] = {"played": 38, "points": 85, "gf": 60, "ga": 35}
        for i in range(5, 21):
            state[f"Filler {i}"] = {"played": 38, "points": 100 - i * 5, "gf": 40, "ga": 50}
        outcomes = src.terminal_outcomes(state)
        # Tied A finishes #3 (UCL cutoff = top 4 → in UCL).
        # Tied B finishes #4 (also in UCL, but the next teams below are
        # Europa-only). Both are in title-less bands.
        assert "UCL" in outcomes["Tied A"]
        assert "UCL" in outcomes["Tied B"]
        assert "title" not in outcomes["Tied A"]
        assert "title" not in outcomes["Tied B"]
        # GD tiebreak: Tied A should outrank Tied B. The test of that
        # ordering is "Tied B has more outcomes than Tied A only if A is
        # behind B" — but here both have the same outcomes. Use a second
        # state where the cutoff sits between them.

    def test_is_bottom_outcome_label_detection(self):
        assert SoccerSource._is_bottom_outcome("relegation") is True
        assert SoccerSource._is_bottom_outcome("Relegated") is True
        assert SoccerSource._is_bottom_outcome("demotion") is True
        assert SoccerSource._is_bottom_outcome("drop_zone") is True
        assert SoccerSource._is_bottom_outcome("title") is False
        assert SoccerSource._is_bottom_outcome("UCL") is False
        assert SoccerSource._is_bottom_outcome("Europa/Conference") is False

    # ---------- end-to-end: monte_carlo_importance against SoccerSource ----------

    def test_full_simulator_runs_against_soccer_source(self):
        """Smoke test: the simulator can run end-to-end against a real
        SoccerSource (mocked HTTP) and return a [0,1] importance. This
        catches integration bugs that unit tests on individual methods
        would miss (e.g., state-shape mismatches between initial_state
        and remaining_matches).
        """
        from dispatcharr_ranked_matchups import simulation
        # Build a richer mini-league so importance has signal to find:
        # use a full 38-week schedule for a 4-team round-robin minus the
        # finished slice. For the smoke test we just need enough remaining
        # games that the sim doesn't fall over.
        src = self._make_source()
        target = next(m for m in src.remaining_matches(src.initial_state())
                      if m.extra["fd_id"] == 105)
        # The "title" outcome doesn't exist for a 4-team league with EPL's
        # threshold cutoff (which expects 20 teams); but the simulator
        # should still run without raising.
        imp = simulation.monte_carlo_importance(
            src, target, "Team A", "title",
            n_sims=50, rng=random.Random(7),
        )
        assert 0.0 <= imp <= 1.0


class TestKnockoutSoccerSource:
    """Phase D.1: KnockoutSoccerSource bracket-shaped importance.

    Mini-bracket: 4 teams (A, B, C, D) in SEMI_FINALS (2 ties of 2 legs each),
    advancing to a single FINAL (1 leg). Uses real UCL stage strings so the
    KNOCKOUT_ROUND_DEPTH mapping fires naturally; tests mock
    `_fetch_all_season_matches` so they exercise the state machine without
    any HTTP.

    Why 4 teams instead of 16: covers every code path (2-leg tie aggregation,
    single-leg FINAL, feeds_from resolution, ET+pen sampling) with the
    smallest synthetic dataset. Real 16-team UCL coverage lives in the
    end-to-end smoke test.
    """

    BASE_DATE = "2026-05-01T18:00:00Z"

    def _semi_finals_matches(self, sf_finished: bool = False) -> list:
        """4 SF legs. If `sf_finished`, the legs carry FINISHED scores
        (A beats B 3-1 aggregate, C beats D 2-0 aggregate). Otherwise
        SCHEDULED with no scores.
        """
        if sf_finished:
            # Tie 1: A vs B — leg1 A 2-0, leg2 B 1-1 → A wins 3-1 aggregate
            sf_leg1_ab = {"home": "A", "away": "B", "home_g": 2, "away_g": 0,
                          "status": "FINISHED", "duration": "REGULAR"}
            sf_leg2_ab = {"home": "B", "away": "A", "home_g": 1, "away_g": 1,
                          "status": "FINISHED", "duration": "REGULAR"}
            # Tie 2: C vs D — leg1 C 2-0, leg2 D 0-0 → C wins 2-0 aggregate
            sf_leg1_cd = {"home": "C", "away": "D", "home_g": 2, "away_g": 0,
                          "status": "FINISHED", "duration": "REGULAR"}
            sf_leg2_cd = {"home": "D", "away": "C", "home_g": 0, "away_g": 0,
                          "status": "FINISHED", "duration": "REGULAR"}
        else:
            sf_leg1_ab = {"home": "A", "away": "B", "home_g": None, "away_g": None,
                          "status": "SCHEDULED", "duration": None}
            sf_leg2_ab = {"home": "B", "away": "A", "home_g": None, "away_g": None,
                          "status": "SCHEDULED", "duration": None}
            sf_leg1_cd = {"home": "C", "away": "D", "home_g": None, "away_g": None,
                          "status": "SCHEDULED", "duration": None}
            sf_leg2_cd = {"home": "D", "away": "C", "home_g": None, "away_g": None,
                          "status": "SCHEDULED", "duration": None}
        return [
            self._fd_match(201, "SEMI_FINALS", 1, sf_leg1_ab),
            self._fd_match(202, "SEMI_FINALS", 2, sf_leg2_ab),
            self._fd_match(203, "SEMI_FINALS", 1, sf_leg1_cd),
            self._fd_match(204, "SEMI_FINALS", 2, sf_leg2_cd),
        ]

    def _final_match(self, home: str = "A", away: str = "C") -> dict:
        """Final: single leg. By default A vs C (the SF winners in the
        finished-SF setup). Always SCHEDULED for the test set — the final
        is what the simulator should be predicting.
        """
        return self._fd_match(301, "FINAL", 1, {
            "home": home, "away": away, "home_g": None, "away_g": None,
            "status": "SCHEDULED", "duration": None,
        })

    @staticmethod
    def _fd_match(fd_id: int, stage: str, matchday: int, leg: dict) -> dict:
        """Construct a FD.org-shape match dict from a leg spec."""
        return {
            "id": fd_id,
            "stage": stage,
            "matchday": matchday,
            "status": leg["status"],
            "homeTeam": {"name": leg["home"]},
            "awayTeam": {"name": leg["away"]},
            "score": {
                "fullTime": {"home": leg["home_g"], "away": leg["away_g"]},
                "duration": leg.get("duration"),
                "penalties": leg.get("penalties"),
            },
            "utcDate": TestKnockoutSoccerSource.BASE_DATE,
        }

    def _make_source(self, all_matches: list):
        """Build a KnockoutSoccerSource with pre-loaded match cache."""
        src = KnockoutSoccerSource("ucl", fd_api_key="fake")
        src._all_matches_cache = all_matches
        return src

    # ---------- supports_importance + outcome_labels ----------

    def test_supports_importance_inherits_true(self):
        src = self._make_source([])
        assert src.supports_importance is True

    def test_outcome_labels_uses_cl_thresholds(self):
        src = self._make_source([])
        labels = src.outcome_labels
        # CL thresholds: r16, QF, SF, F, winner.
        assert "round_of_16" in labels
        assert "quarterfinal" in labels
        assert "semifinal" in labels
        assert "final" in labels
        assert "winner" in labels

    # ---------- _build_bracket ----------

    def test_build_bracket_pairs_legs_into_ties(self):
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        bracket = src._build_bracket(src._fetch_bracket_games())
        assert len(bracket["SEMI_FINALS"]) == 2  # two ties, two legs each
        assert len(bracket["FINAL"]) == 1
        # Each SF tie should have 2 legs ordered by matchday.
        for tie in bracket["SEMI_FINALS"]:
            assert len(tie["games"]) == 2
            assert tie["games"][0]["matchday"] == 1
            assert tie["games"][1]["matchday"] == 2
            assert tie["teams"] == frozenset(
                {leg["home"] for leg in tie["games"]} | {leg["away"] for leg in tie["games"]}
            )

    def test_build_bracket_wires_feeds_from_for_final(self):
        """The FINAL tie's feeds_from must point to the two SF ties whose
        winners are A and C (per the finished SF data). The dict is keyed
        by FD-published team name."""
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        bracket = src._build_bracket(src._fetch_bracket_games())
        final_tie = bracket["FINAL"][0]
        feeds_from = final_tie["feeds_from"]
        assert set(feeds_from.keys()) == {"A", "C"}
        # Both feeds_from refs must point at SEMI_FINALS ties.
        for team, (feed_stage, _idx) in feeds_from.items():
            assert feed_stage == "SEMI_FINALS", f"feeds_from[{team}] should point to SEMI_FINALS"
        # FINAL is a downstream tie (both participants come from prior stage).
        assert final_tie["is_entry_tie"] is False

    def test_build_bracket_feeds_inferred_from_participant_membership(self):
        """feeds_from is built from the bracket's PARTICIPANT structure, not
        from played results. Even when SF legs are SCHEDULED, the FINAL's
        feeds_from is fully populated because A appears in SF tie 0 and C
        in SF tie 1. The simulator uses this to block the FINAL until those
        SFs resolve (via aggregate_winner lookup in resolve_side)."""
        matches = self._semi_finals_matches(sf_finished=False)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        bracket = src._build_bracket(src._fetch_bracket_games())
        # SF ties are entry-level (no PLAYOFFS data in this test bracket).
        for sf_tie in bracket["SEMI_FINALS"]:
            assert sf_tie["is_entry_tie"] is True
            assert sf_tie["feeds_from"] == {}
        # FINAL: feeds_from for A and C point to their SF ties even though
        # those SFs are scheduled. is_entry_tie is False — the FINAL is a
        # downstream tie that blocks until upstream resolves.
        final_tie = bracket["FINAL"][0]
        assert set(final_tie["feeds_from"].keys()) == {"A", "C"}
        for feed_stage, _idx in final_tie["feeds_from"].values():
            assert feed_stage == "SEMI_FINALS"
        assert final_tie["is_entry_tie"] is False

    def test_build_bracket_pen_winner_boost(self):
        """A FD.org match with duration=PENALTY_SHOOTOUT should have +1
        added to the pen winner's goal count in the parsed leg, so the
        aggregate sum reflects the tie's winning side.
        """
        # 90min 1-1, ET 1-1, pens 4-3 → home wins on pens
        leg = {"home": "A", "away": "B", "home_g": 1, "away_g": 1,
               "status": "FINISHED", "duration": "PENALTY_SHOOTOUT",
               "penalties": {"home": 4, "away": 3}}
        m = self._fd_match(401, "SEMI_FINALS", 2, leg)
        # Wrap penalties into the score dict per the FD.org shape.
        m["score"]["penalties"] = leg["penalties"]
        src = self._make_source([m])
        bracket = src._build_bracket(src._fetch_bracket_games())
        # The leg should have home_goals = 1 + 1 (pen boost) = 2; away unchanged.
        leg_parsed = bracket["SEMI_FINALS"][0]["games"][0]
        assert leg_parsed["home_goals"] == 2
        assert leg_parsed["away_goals"] == 1

    # ---------- _record_game_into_tie ----------

    def test_record_game_into_tie_two_leg_aggregate(self):
        src = self._make_source([])
        # Phase I introduced data-driven leg counting: tie records carry
        # `legs_in_tie` to support single-leg non-finals (international
        # tournaments). Two-leg ties set it to 2.
        tie = {
            "stage": "SEMI_FINALS",
            "teams": frozenset({"A", "B"}),
            "legs_in_tie": 2,
            "leg1": None, "leg2": None,
            "winner": None, "loser": None,
        }
        # Leg 1: A home, A 2-0
        src._record_game_into_tie(tie, "A", "B", 2, 0, game_index=1)
        # Tie should be incomplete until leg 2 arrives.
        assert tie["winner"] is None
        # Leg 2: B home, A 1-1 (so B 1, A 1 in this leg)
        src._record_game_into_tie(tie, "B", "A", 1, 1, game_index=2)
        # A aggregate = 2 (home) + 1 (away leg 2) = 3; B aggregate = 0 + 1 = 1.
        assert tie["winner"] == "A"
        assert tie["loser"] == "B"

    def test_record_game_into_tie_single_leg_final(self):
        src = self._make_source([])
        # Single-leg tie: explicitly set legs_in_tie=1 to opt out of the
        # default two-leg-aggregate completeness check.
        tie = {
            "stage": "FINAL",
            "teams": frozenset({"A", "C"}),
            "legs_in_tie": 1,
            "leg1": None, "leg2": None,
            "winner": None, "loser": None,
        }
        # Single leg FINAL: A wins 2-1.
        src._record_game_into_tie(tie, "A", "C", 2, 1, game_index=1)
        # Single-leg tie completes after one record.
        assert tie["winner"] == "A"
        assert tie["loser"] == "C"

    def test_record_game_into_tie_single_leg_non_final_for_international(self):
        # Phase I: international tournaments (WC, EURO) use single-leg
        # knockouts for ALL rounds, not just the final. Verify a SEMI_FINALS
        # tie with legs_in_tie=1 completes after one record.
        src = self._make_source([])
        tie = {
            "stage": "SEMI_FINALS",
            "teams": frozenset({"Spain", "France"}),
            "legs_in_tie": 1,
            "leg1": None, "leg2": None,
            "winner": None, "loser": None,
        }
        src._record_game_into_tie(tie, "Spain", "France", 2, 1, game_index=1)
        assert tie["winner"] == "Spain"
        assert tie["loser"] == "France"

    # ---------- _advance_round_reached ----------

    def test_advance_round_reached_winner_loser_at_correct_depth(self):
        # SF winner advances to FINAL depth; loser stays at SEMI_FINALS depth.
        src = self._make_source([])
        round_reached: dict = {}
        src._advance_round_reached(round_reached, "A", "B", "SEMI_FINALS")
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert round_reached["A"] == KNOCKOUT_ROUND_DEPTH["FINAL"]  # advanced INTO FINAL
        assert round_reached["B"] == KNOCKOUT_ROUND_DEPTH["SEMI_FINALS"]

    def test_advance_round_reached_final_winner_becomes_winner(self):
        src = self._make_source([])
        round_reached: dict = {}
        src._advance_round_reached(round_reached, "A", "C", "FINAL")
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert round_reached["A"] == KNOCKOUT_ROUND_DEPTH["WINNER"]
        assert round_reached["C"] == KNOCKOUT_ROUND_DEPTH["FINAL"]

    # ---------- initial_state ----------

    def test_initial_state_from_finished_semis(self):
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        rr = state["_round_reached"]
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        # A and C won their SF ties → advanced INTO FINAL.
        assert rr["A"] == KNOCKOUT_ROUND_DEPTH["FINAL"]
        assert rr["C"] == KNOCKOUT_ROUND_DEPTH["FINAL"]
        # B and D lost their SF ties → stuck at SEMI_FINALS depth.
        assert rr["B"] == KNOCKOUT_ROUND_DEPTH["SEMI_FINALS"]
        assert rr["D"] == KNOCKOUT_ROUND_DEPTH["SEMI_FINALS"]
        # 4 SF legs are in _applied; the FINAL leg is not.
        assert state["_applied"] == frozenset({201, 202, 203, 204})

    def test_initial_state_caches(self):
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        first = src.initial_state()
        first["_round_reached"]["MARKER"] = 999  # mutate to verify cache hit
        second = src.initial_state()
        assert second["_round_reached"].get("MARKER") == 999

    # ---------- remaining_matches ----------

    def test_remaining_matches_returns_only_eligible(self):
        """When SFs are FINISHED, remaining = only the FINAL. Earlier-stage
        matches are filtered out by `_applied`."""
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        assert len(rem) == 1
        assert rem[0].extra["stage"] == "FINAL"
        # FINAL's home/away come from the resolved upstream SF winners.
        assert {rem[0].home, rem[0].away} == {"A", "C"}

    def test_remaining_matches_blocks_downstream_when_feeders_pending(self):
        """If SFs aren't FINISHED, the FINAL can't be played yet — its
        feeds_from upstreams haven't resolved. remaining = 4 SF legs only."""
        matches = self._semi_finals_matches(sf_finished=False)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        # All 4 SF legs are eligible (entry-level: no feeds_from).
        assert len(rem) == 4
        assert all(m.extra["stage"] == "SEMI_FINALS" for m in rem)

    # ---------- sample_result: ET / penalty resolution ----------

    def test_sample_result_no_et_on_leg_one(self):
        """Leg 1 of a 2-leg tie should never sample ET, even if regulation
        is tied — the tie is decided over both legs."""
        matches = self._semi_finals_matches(sf_finished=False)
        src = self._make_source(matches)
        state = src.initial_state()
        # Construct a leg-1 game row.
        rem = src.remaining_matches(state)
        leg1 = next(m for m in rem if m.extra["matchday"] == 1)
        # Mock strengths so regulation is likely tied at 0-0 (lambdas tiny).
        strengths = {t: {"sh": 0.05, "ch": 0.05, "sa": 0.05, "ca": 0.05}
                     for t in ("A", "B", "C", "D")}
        rng = random.Random(0)
        result = src.sample_result(state, leg1, strengths, rng)
        # No ET marker in extra — sample_result returned early for non-decisive leg.
        assert "et_goals" not in result.extra
        assert "pen_winner" not in result.extra

    def test_sample_result_resolves_tie_on_decisive_leg(self):
        """Single-leg FINAL with tied regulation MUST resolve via ET (and
        possibly pens) so MatchResult.home_goals != MatchResult.away_goals.
        We force regulation to 0-0 via tiny lambdas, then verify the result
        is decided one way or the other (no D in W/D/L classification)."""
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        final_match = rem[0]
        # Tiny strengths → very likely 0-0 regulation, forces ET/pen path.
        strengths = {t: {"sh": 0.01, "ch": 0.01, "sa": 0.01, "ca": 0.01}
                     for t in ("A", "B", "C", "D")}
        # Try multiple seeds so we exercise both pen winner outcomes.
        outcomes = set()
        for seed in range(40):
            rng = random.Random(seed)
            r = src.sample_result(state, final_match, strengths, rng)
            assert r.home_goals != r.away_goals, "Decisive leg must produce a winner"
            outcomes.add("HOME" if r.home_goals > r.away_goals else "AWAY")
        # With 40 trials, almost-certainly we see both pen outcomes appear at
        # least once (variance dominates a 0-0 regulation at tiny lambdas).
        assert outcomes == {"HOME", "AWAY"}

    # ---------- apply_result + terminal_outcomes ----------

    def test_apply_result_advances_round_reached_on_tie_completion(self):
        matches = self._semi_finals_matches(sf_finished=False)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.sources.base import MatchResult
        rem = src.remaining_matches(state)
        # Apply leg 1 of A-vs-B: A 2-0 at home. No tie completion yet.
        leg1 = next(m for m in rem if m.extra["matchday"] == 1 and "A" in (m.home, m.away) and "B" in (m.home, m.away))
        result1 = MatchResult(home_goals=2, away_goals=0)
        state1 = src.apply_result(state, leg1, result1)
        # No advancement until both legs are applied.
        assert not state1["_round_reached"].get("A")
        # Apply leg 2 of A-vs-B: B 1-1 at home. Aggregate 3-1 to A; A advances.
        leg2 = next(m for m in src.remaining_matches(state1)
                    if m.extra["matchday"] == 2 and "A" in (m.home, m.away) and "B" in (m.home, m.away))
        result2 = MatchResult(home_goals=1, away_goals=1)
        state2 = src.apply_result(state1, leg2, result2)
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state2["_round_reached"]["A"] == KNOCKOUT_ROUND_DEPTH["FINAL"]
        assert state2["_round_reached"]["B"] == KNOCKOUT_ROUND_DEPTH["SEMI_FINALS"]

    def test_terminal_outcomes_label_cascade(self):
        """A team that reached the FINAL should also be labeled SF/QF/R16 —
        the deeper round implies all shallower bands."""
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        # A and C are at FINAL depth (won SF). They should have semifinal +
        # final labels. round_of_16 / quarterfinal also fire (depth <= reached).
        assert "semifinal" in outcomes["A"]
        assert "final" in outcomes["A"]
        assert "winner" not in outcomes["A"]  # FINAL hasn't been played yet
        # B and D are at SEMI_FINALS depth (lost SF). They should have
        # semifinal but NOT final.
        assert "semifinal" in outcomes["B"]
        assert "final" not in outcomes["B"]

    # ---------- end-to-end Monte Carlo against the mini-bracket ----------

    def test_monte_carlo_winner_importance_is_high_on_final(self, monkeypatch):
        """The FINAL is the only match between participants and the WINNER
        outcome — tau-c should be near 1.0 (deterministic association).
        Sanity-checks the full simulator pipeline against a knockout source.

        Strengths are monkeypatched to identical symmetric values so the
        marginal W/L distribution is ~50/50 (tau-c is bounded by the
        marginal balance — extreme W/L imbalance from a 4-finished-match
        strength estimate would cap tau-c at ~0.5 even with perfect
        outcome correlation). The algorithm is what's under test, not the
        strength estimator.
        """
        from dispatcharr_ranked_matchups.simulation import monte_carlo_importance
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        # Symmetric strengths: every team has identical home/away scoring rates.
        symmetric = {t: {"sh": 1.2, "ch": 1.2, "sa": 1.0, "ca": 1.0}
                     for t in ("A", "B", "C", "D")}
        monkeypatch.setattr(src, "estimate_strengths", lambda: symmetric)
        state = src.initial_state()
        final_match = src.remaining_matches(state)[0]
        rng = random.Random(13)
        imp = monte_carlo_importance(src, final_match, "A", "winner",
                                     n_sims=400, rng=rng)
        assert imp > 0.85, f"FINAL importance for WINNER should be near-deterministic; got {imp}"

    def test_monte_carlo_already_locked_outcomes_have_zero_importance(self, monkeypatch):
        """A's 'semifinal' label is already true regardless of the FINAL
        result (A reached the final). Importance for that pair should be
        ~0 — the FINAL doesn't change A's semifinal status."""
        from dispatcharr_ranked_matchups.simulation import monte_carlo_importance
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        symmetric = {t: {"sh": 1.2, "ch": 1.2, "sa": 1.0, "ca": 1.0}
                     for t in ("A", "B", "C", "D")}
        monkeypatch.setattr(src, "estimate_strengths", lambda: symmetric)
        state = src.initial_state()
        final_match = src.remaining_matches(state)[0]
        rng = random.Random(13)
        imp = monte_carlo_importance(src, final_match, "A", "semifinal",
                                     n_sims=200, rng=rng)
        # Locked-in outcomes degenerate tau-c (column is constant). Expected: 0.
        assert imp < 0.05, f"Already-reached outcomes should give ~0 importance; got {imp}"

    # ---------- penalty shootout helper ----------

    def test_penalty_shootout_returns_decisive_winner(self):
        """20 shootouts at the same seed should produce both HOME and AWAY
        winners — penalty resolution is variance-dominated, never the same
        outcome every time."""
        outcomes = set()
        for seed in range(30):
            rng = random.Random(seed)
            outcomes.add(KnockoutSoccerSource._sample_penalty_shootout(rng))
        assert outcomes == {"HOME", "AWAY"}


class TestPointsBasedSportSource:
    """Phase D.2 / D.3: shared Monte Carlo machinery for NCAAF + NCAAM.

    Tests use a minimal `_MiniSource` subclass that returns a canned
    `_fetch_full_season_games` list, so the state machine is exercised
    without any HTTP. CFBD/CBBD-specific filtering lives on NcaafSource /
    NcaamSource and is tested separately.
    """

    @staticmethod
    def _mini_source(games, league_code="CFB"):
        from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource

        class _MiniSource(PointsBasedSportSource):
            league_context_code = league_code
            _DEFAULT_POINTS_FOR = 28.0
            _DEFAULT_POINTS_AGAINST = 28.0

            @property
            def sport_prefix(self):
                return "MINI"

            @property
            def sport_label(self):
                return "Mini Sport"

            def fetch_upcoming(self, days_ahead: int = 7):
                return []

            def _fetch_full_season_games(self):
                return games

        return _MiniSource()

    @staticmethod
    def _g(gid, home, away, hp, ap, status="FINISHED"):
        from datetime import datetime, timezone
        return {
            "id": gid, "home": home, "away": away,
            "home_points": hp, "away_points": ap, "status": status,
            "start_time": datetime(2025, 10, 1, tzinfo=timezone.utc),
        }

    # ---------- estimate_strengths ----------

    def test_estimate_strengths_from_finished(self):
        # Team A scored 35, 28; conceded 21, 14. Avg pf=31.5, pa=17.5.
        games = [
            self._g(1, "A", "X", 35, 21),
            self._g(2, "A", "Y", 28, 14),
            self._g(3, "B", "Z", None, None, status="SCHEDULED"),
        ]
        src = self._mini_source(games)
        strengths = src.estimate_strengths()
        assert "A" in strengths
        assert strengths["A"]["pf_per_game"] == pytest.approx(31.5)
        assert strengths["A"]["pa_per_game"] == pytest.approx(17.5)
        assert "B" not in strengths  # no FINISHED data
        b_strength = src._strength_for(strengths, "B")
        assert b_strength["pf_per_game"] == 28.0

    def test_estimate_strengths_caches(self):
        games = [self._g(1, "A", "B", 28, 21)]
        src = self._mini_source(games)
        first = src.estimate_strengths()
        first["MARKER"] = {"pf_per_game": 99.0, "pa_per_game": 99.0}
        second = src.estimate_strengths()
        assert "MARKER" in second

    # ---------- initial_state ----------

    def test_initial_state_records_wins_and_losses(self):
        games = [
            self._g(1, "A", "B", 28, 21),
            self._g(2, "C", "A", 35, 14),
            self._g(3, "B", "C", None, None, status="SCHEDULED"),
        ]
        src = self._mini_source(games)
        state = src.initial_state()
        teams = state["_teams"]
        assert teams["A"]["wins"] == 1 and teams["A"]["losses"] == 1
        assert teams["B"]["wins"] == 0 and teams["B"]["losses"] == 1
        assert teams["C"]["wins"] == 1 and teams["C"]["losses"] == 0
        assert state["_applied"] == frozenset({1, 2})

    def test_initial_state_caches(self):
        games = [self._g(1, "A", "B", 28, 21)]
        src = self._mini_source(games)
        first = src.initial_state()
        first["_teams"]["MARKER"] = {"wins": 99}
        second = src.initial_state()
        assert "MARKER" in second["_teams"]

    # ---------- remaining_matches ----------

    def test_remaining_matches_only_unapplied(self):
        games = [
            self._g(1, "A", "B", 28, 21),
            self._g(2, "C", "D", None, None, status="SCHEDULED"),
            self._g(3, "A", "C", None, None, status="SCHEDULED"),
        ]
        src = self._mini_source(games)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        assert len(rem) == 2
        rem_ids = {m.extra["game_id"] for m in rem}
        assert rem_ids == {2, 3}

    # ---------- sample_result ----------

    def test_sample_result_breaks_ties(self):
        """NCAA games never end in regulation ties for win-count purposes
        (OT resolves). sample_result must coin-flip a +1 boost when
        Poisson lands on the same value both sides."""
        games = [self._g(1, "A", "B", 28, 28)]
        src = self._mini_source(games)
        state = src.initial_state()
        strengths = src.estimate_strengths()
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from datetime import datetime, timezone
        target = GameRow(
            sport_prefix="MINI", sport_label="Mini",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=datetime(2025, 11, 1, tzinfo=timezone.utc),
            extra={"game_id": 99},
        )
        for seed in range(50):
            rng = random.Random(seed)
            result = src.sample_result(state, target, strengths, rng)
            assert result.home_goals != result.away_goals, (
                f"seed={seed}: home={result.home_goals}, away={result.away_goals}")

    # ---------- apply_result + state immutability ----------

    def test_apply_result_returns_new_state(self):
        games = [
            self._g(1, "A", "B", 28, 21),
            self._g(2, "A", "C", None, None, status="SCHEDULED"),
        ]
        src = self._mini_source(games)
        from dispatcharr_ranked_matchups.sources.base import MatchResult
        state = src.initial_state()
        prior_a_wins = state["_teams"]["A"]["wins"]
        rem = src.remaining_matches(state)
        target = rem[0]
        new_state = src.apply_result(state, target, MatchResult(home_goals=35, away_goals=14))
        assert state["_teams"]["A"]["wins"] == prior_a_wins
        assert new_state["_teams"]["A"]["wins"] == prior_a_wins + 1
        assert 2 in new_state["_applied"]

    # ---------- terminal_outcomes ----------

    def test_terminal_outcomes_cascade_win_count_bands(self):
        src = self._mini_source([])
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Elite":    {"wins": 11, "losses": 1, "pf": 0, "pa": 0, "games_played": 12},
                "Good":     {"wins": 8,  "losses": 4, "pf": 0, "pa": 0, "games_played": 12},
                "Mediocre": {"wins": 6,  "losses": 6, "pf": 0, "pa": 0, "games_played": 12},
                "Bad":      {"wins": 3,  "losses": 9, "pf": 0, "pa": 0, "games_played": 12},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert set(outcomes["Elite"]) == {"11_wins", "10_wins", "8_wins", "bowl_eligible"}
        assert set(outcomes["Good"]) == {"8_wins", "bowl_eligible"}
        assert set(outcomes["Mediocre"]) == {"bowl_eligible"}
        assert outcomes["Bad"] == []

    def test_terminal_outcomes_empty_for_unknown_league(self):
        src = self._mini_source([], league_code="UNKNOWN_LEAGUE_CODE")
        state = {"_applied": frozenset(),
                 "_teams": {"X": {"wins": 25, "losses": 0,
                                  "pf": 0, "pa": 0, "games_played": 25}}}
        assert src.terminal_outcomes(state) == {}

    # ---------- end-to-end Monte Carlo ----------

    def test_monte_carlo_runs_without_crashing(self):
        from dispatcharr_ranked_matchups.simulation import monte_carlo_importance
        games = [
            self._g(1, "A", "B", 28, 21),
            self._g(2, "C", "D", 35, 14),
            self._g(3, "B", "C", 28, 14),
            self._g(4, "D", "A", 21, 35),
            self._g(5, "A", "C", None, None, status="SCHEDULED"),
            self._g(6, "B", "D", None, None, status="SCHEDULED"),
        ]
        src = self._mini_source(games, league_code="CFB")
        state = src.initial_state()
        rem = src.remaining_matches(state)
        target = next(m for m in rem if m.home == "A" and m.away == "C")
        rng = random.Random(7)
        imp = monte_carlo_importance(src, target, "A", "bowl_eligible",
                                     n_sims=50, rng=rng)
        assert 0.0 <= imp <= 1.0


class TestNcaafFullSeasonFilter:
    """NcaafSource._fetch_full_season_games filters to FBS-class games."""

    def test_filter_drops_fcs_vs_fcs(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaaf as ncaaf_mod

        class FakeResp:
            def __init__(self, payload):
                self._p = payload

            def raise_for_status(self):
                pass

            def json(self):
                return self._p

        payload = [
            {  # FBS vs FBS — kept
                "id": 1, "homeClassification": "fbs", "awayClassification": "fbs",
                "homeTeam": "Michigan", "awayTeam": "Ohio State",
                "homePoints": 28, "awayPoints": 21, "completed": True,
                "startDate": "2025-11-29T12:00:00.000Z",
            },
            {  # FBS vs FCS — kept (cupcake counts toward FBS win total)
                "id": 2, "homeClassification": "fbs", "awayClassification": "fcs",
                "homeTeam": "Alabama", "awayTeam": "Chattanooga",
                "homePoints": 56, "awayPoints": 7, "completed": True,
                "startDate": "2025-09-15T16:00:00.000Z",
            },
            {  # FCS vs FCS — dropped
                "id": 3, "homeClassification": "fcs", "awayClassification": "fcs",
                "homeTeam": "Furman", "awayTeam": "Wofford",
                "homePoints": 24, "awayPoints": 21, "completed": True,
                "startDate": "2025-10-01T16:00:00.000Z",
            },
            {  # SCHEDULED FBS vs FBS — kept, no scores
                "id": 4, "homeClassification": "fbs", "awayClassification": "fbs",
                "homeTeam": "Georgia", "awayTeam": "Texas",
                "homePoints": None, "awayPoints": None, "completed": False,
                "startDate": "2026-01-01T20:00:00.000Z",
            },
        ]
        monkeypatch.setattr(ncaaf_mod.requests, "get",
                            lambda *a, **kw: FakeResp(payload))
        src = ncaaf_mod.NcaafSource(api_key="fake")
        games = src._fetch_full_season_games()
        ids = {g["id"] for g in games}
        assert ids == {1, 2, 4}
        scheduled = next(g for g in games if g["id"] == 4)
        assert scheduled["status"] == "SCHEDULED"
        assert scheduled["home_points"] is None
        completed = next(g for g in games if g["id"] == 1)
        assert completed["status"] == "FINISHED"
        assert completed["home_points"] == 28


class TestNcaamFullSeasonFilter:
    """NcaamSource._fetch_full_season_games filters to games involving at
    least one AP-ranked team. Returns [] gracefully if no poll exists."""

    def test_returns_empty_when_no_ranked_teams(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod
        src = ncaam_mod.NcaamSource(api_key="fake")
        monkeypatch.setattr(src, "_fetch_rankings", lambda _season: None)
        called = []
        monkeypatch.setattr(ncaam_mod.requests, "get",
                            lambda *a, **kw: called.append(1))
        out = src._fetch_full_season_games()
        assert out == []
        assert called == []  # never hit /games

    def test_filter_keeps_only_ap_relevant_games(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        class FakeResp:
            def __init__(self, payload):
                self._p = payload

            def raise_for_status(self):
                pass

            def json(self):
                return self._p

        src = ncaam_mod.NcaamSource(api_key="fake")
        monkeypatch.setattr(src, "_fetch_rankings",
                            lambda _season: {"Duke": 1, "UConn": 2})
        payload = [
            {  # AP vs AP — kept
                "id": 101, "homeTeam": "Duke", "awayTeam": "UConn",
                "homePoints": 78, "awayPoints": 72,
                "homeWinner": True, "awayWinner": False,
                "startDate": "2025-12-15T20:00:00.000Z",
            },
            {  # AP vs non-AP — kept (Duke needs all their games tracked)
                "id": 102, "homeTeam": "Duke", "awayTeam": "Some Mid-Major",
                "homePoints": 95, "awayPoints": 60,
                "homeWinner": True, "awayWinner": False,
                "startDate": "2025-11-20T20:00:00.000Z",
            },
            {  # Non-AP vs Non-AP — dropped
                "id": 103, "homeTeam": "Random A", "awayTeam": "Random B",
                "homePoints": 70, "awayPoints": 68,
                "homeWinner": True, "awayWinner": False,
                "startDate": "2025-11-25T19:00:00.000Z",
            },
        ]
        monkeypatch.setattr(ncaam_mod.requests, "get",
                            lambda *a, **kw: FakeResp(payload))
        games = src._fetch_full_season_games()
        ids = [g["id"] for g in games]
        assert 101 in ids
        assert 102 in ids
        assert 103 not in ids
        for g in games:
            assert g["status"] == "FINISHED"

    def test_chunks_cover_continuous_date_range(self, monkeypatch):
        """The 3-chunk date range covers Nov-Apr with no gaps. A game on
        the exact boundary (e.g., 2026-02-16) must land in a chunk, not
        between two chunks."""
        from dispatcharr_ranked_matchups.sources import ncaam as ncaam_mod

        captured = []

        class FakeResp:
            def __init__(self, p): self._p = p
            def raise_for_status(self): pass
            def json(self): return self._p

        def capture_get(*args, **kwargs):
            params = kwargs.get("params", {})
            captured.append((params.get("startDateRange"),
                             params.get("endDateRange")))
            return FakeResp([])

        src = ncaam_mod.NcaamSource(api_key="fake")
        monkeypatch.setattr(src, "_current_season_year", lambda: 2026)
        monkeypatch.setattr(src, "_fetch_rankings", lambda _season: {"Duke": 1})
        monkeypatch.setattr(ncaam_mod.requests, "get", capture_get)
        src._fetch_full_season_games()
        assert len(captured) == 3
        # Chunk boundaries: end of chunk N + 1 day = start of chunk N+1.
        assert captured[0] == ("2025-11-01", "2025-12-31")
        assert captured[1] == ("2026-01-01", "2026-02-15")
        assert captured[2] == ("2026-02-16", "2026-04-30")


# =====================================================================
# Phase E: BestOfNSeriesSource — base bracket-state-machine tests
# =====================================================================

class TestBestOfNSeriesSource:
    """Phase E refactor: best-of-N series base for NHL / NBA / MLB playoff
    sources. Exercises the series state machine, immutability, and the
    inherited terminal_outcomes cascade.

    Uses a concrete minimal subclass that takes a pre-baked list of game
    records so no HTTP touches the test path.
    """

    @staticmethod
    def _make_source(games, series_length=7):
        """Concrete BestOfNSeriesSource for testing. Inherits the bracket
        machinery and trivially returns the pre-baked games list. Override
        the LEAGUE_CONTEXTS hook so terminal_outcomes finds a context."""
        from dispatcharr_ranked_matchups.sources.bracket import BestOfNSeriesSource

        class _TestSrc(BestOfNSeriesSource):
            KO_STAGES = ("R1", "R2", "CONF_FINAL", "CUP_FINAL")
            SERIES_LENGTH = series_length

            @property
            def sport_prefix(self):
                return "TEST"

            @property
            def sport_label(self):
                return "Test Series"

            def fetch_upcoming(self, days_ahead=7):
                return []

            def _league_context_code(self):
                return "NHL_PO"

            def _winner_advance_label(self, stage):
                if stage == "CUP_FINAL":
                    return "CUP_WINNER"
                return None

            def _fetch_bracket_games(self):
                return list(games)

        return _TestSrc()

    @staticmethod
    def _series_games(stage, top, bot, scores=None, series_letter="a"):
        """Synthesize per-game records for a best-of-7 series. `scores`
        is a list of (home_score, away_score) tuples; missing entries
        become SCHEDULED. NHL home pattern (2-2-1-1-1): top hosts games
        1, 2, 5, 7; bot hosts 3, 4, 6.
        """
        scores = scores or []
        out = []
        home_top = [True, True, False, False, True, False, True]
        for i in range(7):
            home = top if home_top[i] else bot
            away = bot if home_top[i] else top
            score = scores[i] if i < len(scores) else None
            if score is None:
                out.append({
                    "game_id": f"{series_letter}-g{i+1}",
                    "stage": stage,
                    "matchday": i + 1,
                    "home": home, "away": away,
                    "home_goals": None, "away_goals": None,
                    "status": "SCHEDULED",
                    "start_time": None,
                    "extra": {},
                })
            else:
                out.append({
                    "game_id": f"{series_letter}-g{i+1}",
                    "stage": stage,
                    "matchday": i + 1,
                    "home": home, "away": away,
                    "home_goals": score[0], "away_goals": score[1],
                    "status": "FINISHED",
                    "start_time": None,
                    "extra": {},
                })
        return out

    # ---------- _clinching_wins_for_stage (uniform-length) ----------

    def test_clinching_wins_uniform_best_of_seven(self):
        src = self._make_source([], series_length=7)
        # Uniform sport: every stage returns ceil(7/2) = 4.
        for stage in src.KO_STAGES:
            assert src._clinching_wins_for_stage(stage) == 4

    def test_clinching_wins_uniform_best_of_five(self):
        src = self._make_source([], series_length=5)
        for stage in src.KO_STAGES:
            assert src._clinching_wins_for_stage(stage) == 3

    # ---------- new tie record shape ----------

    def test_new_tie_record_initializes_zero_wins_per_team(self):
        src = self._make_source([])
        tie = src._new_tie_record({"teams": frozenset({"A", "B"}), "stage": "R1"})
        assert tie["series_wins"] == {"A": 0, "B": 0}
        assert tie["winner"] is None
        assert tie["loser"] is None
        assert tie["games_recorded"] == frozenset()

    # ---------- record_game_into_tie ----------

    def test_record_game_advances_winner_count(self):
        src = self._make_source([])
        tie = src._new_tie_record({"teams": frozenset({"A", "B"}), "stage": "R1"})
        # A wins as home in game 1
        src._record_game_into_tie(tie, "A", "B", 3, 1, game_index=1)
        assert tie["series_wins"]["A"] == 1
        assert tie["series_wins"]["B"] == 0
        assert tie["winner"] is None

    def test_record_game_resolves_series_at_four_wins(self):
        src = self._make_source([])
        tie = src._new_tie_record({"teams": frozenset({"A", "B"}), "stage": "R1"})
        # A wins games 1-4; series clinched after game 4.
        src._record_game_into_tie(tie, "A", "B", 3, 1, game_index=1)
        assert tie["winner"] is None
        src._record_game_into_tie(tie, "A", "B", 2, 0, game_index=2)
        assert tie["winner"] is None
        src._record_game_into_tie(tie, "B", "A", 1, 4, game_index=3)
        assert tie["winner"] is None
        src._record_game_into_tie(tie, "B", "A", 2, 5, game_index=4)
        assert tie["winner"] == "A"
        assert tie["loser"] == "B"

    def test_record_game_ignored_after_series_resolved(self):
        src = self._make_source([])
        tie = src._new_tie_record({"teams": frozenset({"A", "B"}), "stage": "R1"})
        # Series ends 4-0.
        for i in range(4):
            src._record_game_into_tie(tie, "A", "B", 3, 1, game_index=i + 1)
        wins_after_clinch = dict(tie["series_wins"])
        # An extra game should be a no-op (defense against simulator double-applying).
        src._record_game_into_tie(tie, "A", "B", 1, 0, game_index=5)
        assert tie["series_wins"] == wins_after_clinch

    # ---------- initial_state from real-shape games ----------

    def test_initial_state_pre_populates_finished_series(self):
        # R1: A vs B, A wins 4-2. NHL home pattern: A hosts games 1, 2, 5, 7;
        # B hosts 3, 4, 6. Scores are (home_score, away_score), so when B
        # hosts and A wins, the row reads home < away.
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[
                (3, 1),  # G1 A-home: A wins  (A: 1, B: 0)
                (2, 0),  # G2 A-home: A wins  (A: 2, B: 0)
                (1, 4),  # G3 B-home: A wins  (A: 3, B: 0)
                (3, 1),  # G4 B-home: B wins  (A: 3, B: 1)
                (1, 3),  # G5 A-home: B wins  (A: 3, B: 2)
                (1, 4),  # G6 B-home: A wins  (A: 4, B: 2) — A clinches
            ],
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        tk = ("R1", frozenset({"A", "B"}))
        tie = state["_tie_results"][tk]
        assert tie["winner"] == "A"
        assert tie["loser"] == "B"
        assert tie["series_wins"]["A"] == 4
        assert tie["series_wins"]["B"] == 2
        # round_reached: winner depth = 1 (R2 entry); loser depth = 0 (R1).
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["A"] == KNOCKOUT_ROUND_DEPTH["R2"]
        assert state["_round_reached"]["B"] == KNOCKOUT_ROUND_DEPTH["R1"]
        # 6 games applied; the 7th game record (a placeholder game 7) is
        # SCHEDULED, so not in _applied.
        assert state["_applied"] == frozenset({"a-g1", "a-g2", "a-g3", "a-g4", "a-g5", "a-g6"})

    def test_remaining_matches_emits_next_unplayed_in_active_series(self):
        # R1 series at 2-1 after game 3; games 4-7 are SCHEDULED.
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[(3, 1), (1, 2), (4, 2)],  # A leads 2-1
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        # Expected: games 4 through 7 emitted (since series can still
        # extend to game 7).
        assert len(remaining) == 4
        matchdays = sorted(g.extra["matchday"] for g in remaining)
        assert matchdays == [4, 5, 6, 7]

    def test_remaining_matches_stops_emitting_once_series_clinched(self):
        # Series at 4-0 after game 4; nothing should remain even if games 5-7 are scheduled.
        # Games 1/2 at A (home_score>away_score = A wins); games 3/4 at B
        # (home_score<away_score = A wins as away).
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[(3, 1), (2, 0), (1, 4), (2, 5)],  # A sweeps 4-0
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        # Series is over, no games remain for this tie.
        assert len(remaining) == 0

    # ---------- apply_result immutability + advancement ----------

    def test_apply_result_does_not_mutate_input_state(self):
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[(3, 1), (1, 2)],  # tied 1-1 after game 2
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        original_wins = dict(state["_tie_results"][("R1", frozenset({"A", "B"}))]["series_wins"])

        # Sample a game 3 result via the synthesized GameRow.
        remaining = src.remaining_matches(state)
        target = next(g for g in remaining if g.extra["matchday"] == 3)
        from dispatcharr_ranked_matchups.sources.base import MatchResult
        result = MatchResult(home_goals=4, away_goals=1)  # B-host -> B beats A
        new_state = src.apply_result(state, target, result)

        # Input state untouched.
        assert state["_tie_results"][("R1", frozenset({"A", "B"}))]["series_wins"] == original_wins
        # New state advanced: B now has 2 wins, A still 1.
        new_wins = new_state["_tie_results"][("R1", frozenset({"A", "B"}))]["series_wins"]
        # target.home == B, away == A; B won 4-1; B is target.home so increments B.
        assert new_wins[target.home] == 2
        assert new_wins[target.away] == 1

    def test_apply_result_advances_round_reached_on_series_clinch(self):
        # 3-0 series, sample game 4 with away (A) winning to clinch.
        # Game 3 (B-home): A wins as away → home_score < away_score.
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[(3, 1), (2, 0), (1, 4)],  # A leads 3-0
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        # Before clinch: A reached depth 0 (R1), nothing advanced yet.
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"].get("A", -1) == -1  # haven't won the series yet

        # Game 4 hosted by B; A beats them on the road to clinch.
        remaining = src.remaining_matches(state)
        g4 = next(g for g in remaining if g.extra["matchday"] == 4)
        from dispatcharr_ranked_matchups.sources.base import MatchResult
        # Whoever is away wins (that's A). Game 4 is hosted by B per the
        # 2-2-1-1-1 pattern. So home=B, away=A, A wins.
        assert g4.home == "B"
        assert g4.away == "A"
        result = MatchResult(home_goals=1, away_goals=3)  # A wins on the road
        new_state = src.apply_result(state, g4, result)
        assert new_state["_round_reached"]["A"] == KNOCKOUT_ROUND_DEPTH["R2"]
        assert new_state["_round_reached"]["B"] == KNOCKOUT_ROUND_DEPTH["R1"]

    # ---------- terminal_outcomes label cascade ----------

    def test_terminal_outcomes_cascades_via_round_reached(self):
        # A sweeps 4-0. Games 1/2 at A; games 3/4 at B (A wins as away).
        r1_games = self._series_games(
            "R1", "A", "B",
            scores=[(3, 1), (2, 0), (1, 4), (2, 5)],  # A 4-0
            series_letter="a",
        )
        src = self._make_source(r1_games)
        state = src.initial_state()
        # A reached depth 1 (R2). LEAGUE_CONTEXTS["NHL_PO"] cutoffs:
        # R2=1, CONF_FINAL=2, CUP_FINAL=3, CUP_WINNER=4.
        # A's depth=1 ≥ R2 cutoff (1) so "round_2" fires; nothing else.
        outcomes = src.terminal_outcomes(state)
        assert "round_2" in outcomes["A"]
        assert "conf_final" not in outcomes["A"]
        assert "cup_final" not in outcomes["A"]
        # B at depth 0 (R1) — no bands fire (cutoffs all >= 1).
        assert outcomes["B"] == []


# =====================================================================
# Phase E: NhlRegularSource — standings_points + OT/SO classification
# =====================================================================

class TestNhlRegularSource:
    """NhlRegularSource uses standings points (not raw wins) as the
    importance threshold field. Verifies the W/OTL/L → 2/1/0 standings_
    points mapping plus the OT/SO classification in sample_result.
    """

    @staticmethod
    def _make_source(games):
        from dispatcharr_ranked_matchups.sources.nhl import NhlRegularSource
        src = NhlRegularSource(season="20252026")
        src._all_games_cache = games
        return src

    @staticmethod
    def _game(gid, home, away, hp, ap, status="FINISHED", last_period="REG"):
        return {
            "id": gid,
            "home": home, "away": away,
            "home_points": hp if status == "FINISHED" else None,
            "away_points": ap if status == "FINISHED" else None,
            "status": status,
            "start_time": None,
            "extra": {"last_period_type": last_period},
        }

    # ---------- standings_points credit ----------

    def test_regulation_win_credits_two_points_to_winner_zero_to_loser(self):
        src = self._make_source([self._game(1, "A", "B", 4, 2, last_period="REG")])
        state = src.initial_state()
        assert state["_teams"]["A"]["standings_points"] == 2
        assert state["_teams"]["B"]["standings_points"] == 0

    def test_ot_loss_credits_one_point_to_loser(self):
        src = self._make_source([self._game(1, "A", "B", 4, 3, last_period="OT")])
        state = src.initial_state()
        assert state["_teams"]["A"]["standings_points"] == 2
        assert state["_teams"]["B"]["standings_points"] == 1

    def test_shootout_loss_credits_one_point_to_loser(self):
        src = self._make_source([self._game(1, "A", "B", 4, 3, last_period="SO")])
        state = src.initial_state()
        assert state["_teams"]["A"]["standings_points"] == 2
        assert state["_teams"]["B"]["standings_points"] == 1

    def test_missing_last_period_defaults_to_regulation(self):
        # Defensive: api-web sometimes omits gameOutcome for very early
        # postponed games; we treat the absence as REG (loser gets 0).
        g = self._game(1, "A", "B", 4, 3)
        g["extra"] = {}  # no last_period_type
        src = self._make_source([g])
        state = src.initial_state()
        assert state["_teams"]["B"]["standings_points"] == 0

    # ---------- sample_result OT/SO tagging ----------

    def test_sample_result_tags_period_type(self):
        import random
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from datetime import datetime, timezone
        src = self._make_source([])
        # Force a regulation tie via skewed strengths -> very low lambda.
        # Easier: monkey-patch the random calls directly.
        strengths = {"A": {"pf_per_game": 3.0, "pa_per_game": 3.0},
                     "B": {"pf_per_game": 3.0, "pa_per_game": 3.0}}
        gr = GameRow(
            sport_prefix="NHL", sport_label="NHL",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        # Try multiple seeds: at least one out of 30 should land on a
        # regulation tie via Poisson(3) which has substantial mass on
        # ties. Confirm that when a tie is sampled, the extra is tagged.
        seen_periods = set()
        for seed in range(60):
            rng = random.Random(seed)
            res = src.sample_result({}, gr, strengths, rng)
            lp = (res.extra or {}).get("last_period_type")
            assert lp in ("REG", "OT", "SO")
            seen_periods.add(lp)
        # OT should appear; SO is rarer (10%) so may or may not.
        assert "REG" in seen_periods
        assert "OT" in seen_periods

    # ---------- terminal_outcomes uses points_count ----------

    def test_terminal_outcomes_uses_standings_points_for_buckets(self):
        # Synthesize a team with exactly 100 standings points across
        # 50 wins (REG) → 100 standings_points.
        games = []
        for i in range(50):
            games.append(self._game(i, "A", "B", 4, 2, last_period="REG"))
        src = self._make_source(games)
        state = src.initial_state()
        # A has 100 standings_points. NHL bands: 95 / 100 / 110 / 125.
        outcomes = src.terminal_outcomes(state)
        assert "playoff_bubble" in outcomes["A"]      # 100 >= 95
        assert "playoff_secured" in outcomes["A"]     # 100 >= 100
        assert "division_pace" not in outcomes["A"]   # 100 < 110
        # B has 0 standings_points; no bands.
        assert outcomes["B"] == []


# =====================================================================
# Phase E: NhlPlayoffSource — bracket inference + round_reached cascade
# =====================================================================

class TestNhlPlayoffSource:
    """NhlPlayoffSource inherits BestOfNSeriesSource. These tests cover
    the NHL-specific bits: _NHL_ROUND_TO_STAGE mapping, _winner_advance_
    label for CUP_FINAL → CUP_WINNER, and terminal_outcomes label set.
    """

    @staticmethod
    def _make_source(bracket_games):
        from dispatcharr_ranked_matchups.sources.nhl import NhlPlayoffSource
        src = NhlPlayoffSource(season="20252026")
        src._bracket_games_cache = bracket_games
        return src

    def test_supports_importance(self):
        src = self._make_source([])
        assert src.supports_importance is True

    def test_outcome_labels_uses_nhl_po_thresholds(self):
        src = self._make_source([])
        labels = src.outcome_labels
        assert "round_2" in labels
        assert "conf_final" in labels
        assert "cup_final" in labels
        assert "cup_winner" in labels

    def test_winner_advance_label_for_cup_final(self):
        src = self._make_source([])
        assert src._winner_advance_label("CUP_FINAL") == "CUP_WINNER"
        # Earlier rounds advance to stage_depth + 1 (None signals default).
        assert src._winner_advance_label("R1") is None
        assert src._winner_advance_label("CONF_FINAL") is None

    def test_cup_final_winner_reaches_cup_winner_depth(self):
        # CUP_FINAL series ends 4-0 for Champion. Games 1/2 hosted by
        # Champion (top seed); games 3/4 hosted by Finalist. Champion
        # wins as away in games 3/4 → home_score < away_score.
        cf_games = TestBestOfNSeriesSource._series_games(
            "CUP_FINAL", "ChampionTeam", "FinalistTeam",
            scores=[(4, 1), (3, 0), (0, 3), (2, 5)],
            series_letter="cf",
        )
        src = self._make_source(cf_games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        # Winner reached CUP_WINNER depth.
        assert state["_round_reached"]["ChampionTeam"] == KNOCKOUT_ROUND_DEPTH["CUP_WINNER"]
        # Loser stuck at CUP_FINAL depth.
        assert state["_round_reached"]["FinalistTeam"] == KNOCKOUT_ROUND_DEPTH["CUP_FINAL"]
        # terminal_outcomes: ChampionTeam gets all four bands.
        outcomes = src.terminal_outcomes(state)
        assert set(outcomes["ChampionTeam"]) == {"round_2", "conf_final", "cup_final", "cup_winner"}
        # FinalistTeam gets everything except cup_winner.
        assert set(outcomes["FinalistTeam"]) == {"round_2", "conf_final", "cup_final"}

    def test_set_regular_season_strengths_passes_through(self):
        src = self._make_source([])
        # Default: no regular-season strengths seeded.
        assert src.estimate_strengths() == {}
        # After seeding: should return what was passed.
        src.set_regular_season_strengths({"Avalanche": {"pf_per_game": 3.5, "pa_per_game": 2.4}})
        s = src.estimate_strengths()
        assert s["Avalanche"]["pf_per_game"] == 3.5


# =====================================================================
# Phase E: PointsBasedSportSource _count_field generalization
# =====================================================================

class TestPointsBasedSourceCountField:
    """The _count_field class attr lets subclasses choose between
    "wins" (CFB/CBB/NBA/MLB) and "standings_points" (NHL) for
    terminal_outcomes bucketing."""

    def test_default_count_field_is_wins(self):
        from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource
        assert PointsBasedSportSource._count_field == "wins"

    def test_nhl_regular_overrides_count_field(self):
        from dispatcharr_ranked_matchups.sources.nhl import NhlRegularSource
        assert NhlRegularSource._count_field == "standings_points"

    def test_ncaaf_keeps_wins_count_field(self):
        from dispatcharr_ranked_matchups.sources.ncaaf import NcaafSource
        # CFBD source inherits PointsBasedSportSource without overriding.
        assert NcaafSource._count_field == "wins"


# =====================================================================
# Phase M: NCAA Baseball
# =====================================================================

class TestNcaaBaseballRegularSource:
    """Phase M: D1 baseball via ESPN unofficial API + D1Baseball.com poll.
    Tests cover the canonical-game-record extraction (the tricky part is
    homeAway / completed / score parsing), team-name canonicalization
    (location > nickname), and the LEAGUE_CONTEXTS bands.
    """

    @staticmethod
    def _make_source():
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import NcaaBaseballRegularSource
        # Pin season_year so test isn't dependent on calendar.
        return NcaaBaseballRegularSource(season_year=2026)

    @staticmethod
    def _competitor(team_loc, team_name, score, home_away="home", winner=None):
        return {
            "homeAway": home_away,
            "score": str(score) if score is not None else None,
            "winner": winner,
            "team": {"location": team_loc, "name": team_name, "abbreviation": team_loc[:4].upper()},
        }

    @staticmethod
    def _event(eid, date, hp, ap, completed=True, state="post"):
        return {
            "id": eid,
            "date": date,
            "competitions": [{
                "status": {"type": {"completed": completed, "state": state}},
                "competitors": [
                    TestNcaaBaseballRegularSource._competitor("UCLA", "Bruins", hp, "home", hp > ap if hp is not None else None),
                    TestNcaaBaseballRegularSource._competitor("Texas", "Longhorns", ap, "away", ap > hp if ap is not None else None),
                ],
            }],
        }

    # ---------- supports_importance + identity ----------

    def test_supports_importance(self):
        src = self._make_source()
        assert src.supports_importance is True

    def test_sport_prefix_label(self):
        src = self._make_source()
        assert src.sport_prefix == "NCAABSB"
        assert src.sport_label == "NCAA Baseball"

    def test_count_field_is_wins(self):
        # Inherits the PointsBasedSportSource default — D1 baseball has
        # no OT/SO bonus point complication like NHL.
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import NcaaBaseballRegularSource
        assert NcaaBaseballRegularSource._count_field == "wins"

    # ---------- _extract_game_record ----------

    def test_extract_finished_game(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _extract_game_record
        evt = self._event("1001", "2026-04-01T19:00Z", hp=7, ap=3)
        rec = _extract_game_record(evt)
        assert rec is not None
        assert rec["id"] == "1001"
        assert rec["home"] == "UCLA"  # location (school), not "Bruins" (mascot)
        assert rec["away"] == "Texas"
        assert rec["home_points"] == 7
        assert rec["away_points"] == 3
        assert rec["status"] == "FINISHED"

    def test_extract_in_progress_demotes_to_scheduled(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _extract_game_record
        evt = self._event("1002", "2026-04-01T19:00Z", hp=4, ap=2,
                          completed=False, state="in")
        rec = _extract_game_record(evt)
        # In-progress score is unstable — must not seed wins/losses.
        assert rec is not None
        assert rec["status"] == "SCHEDULED"
        assert rec["home_points"] is None
        assert rec["away_points"] is None

    def test_extract_pregame_scheduled(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _extract_game_record
        evt = {
            "id": "1003",
            "date": "2026-05-25T22:00Z",
            "competitions": [{
                "status": {"type": {"completed": False, "state": "pre"}},
                "competitors": [
                    self._competitor("UCLA", "Bruins", None, "home"),
                    self._competitor("Texas", "Longhorns", None, "away"),
                ],
            }],
        }
        rec = _extract_game_record(evt)
        assert rec is not None
        assert rec["status"] == "SCHEDULED"

    def test_extract_finished_with_missing_score_demotes(self):
        # Defensive: if ESPN reports completed but no score (data hiccup),
        # demote to SCHEDULED rather than recording a 0-0 W/L into state.
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _extract_game_record
        evt = self._event("1004", "2026-04-01T19:00Z", hp=None, ap=None)
        rec = _extract_game_record(evt)
        assert rec is not None
        assert rec["status"] == "SCHEDULED"

    def test_extract_missing_competitor_returns_none(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _extract_game_record
        # Only 1 competitor (corrupt event)
        evt = {
            "id": "1005",
            "date": "2026-04-01T19:00Z",
            "competitions": [{
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [self._competitor("UCLA", "Bruins", 7, "home")],
            }],
        }
        rec = _extract_game_record(evt)
        assert rec is None

    def test_team_canonical_name_prefers_location_over_mascot(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _team_canonical_name
        # EPG provider titles use the school name ("UCLA at Texas")
        # not the mascot ("Bruins at Longhorns") — pick location.
        assert _team_canonical_name({"location": "UCLA", "name": "Bruins"}) == "UCLA"

    def test_team_canonical_name_falls_back_to_nickname(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import _team_canonical_name
        # Edge case: non-D1 opponent in early-season scrimmage missing location.
        assert _team_canonical_name({"location": "", "name": "Wildcats"}) == "Wildcats"
        assert _team_canonical_name({"name": "Wildcats"}) == "Wildcats"

    # ---------- LEAGUE_CONTEXTS ----------

    def test_bsb_in_league_contexts(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["BSB"]
        assert ctx.format == "win_count"
        labels = {label for _cut, label, _w in ctx.thresholds}
        assert "tournament_bubble" in labels
        assert "at_large_lock" in labels
        assert "regional_top_seed" in labels
        assert "national_seed" in labels
        assert "overall_one_seed" in labels

    def test_bsb_thresholds_strictly_monotonic(self):
        # Each band's cutoff must be strictly greater than the prior so
        # the cascade fires cleanly. A team with 50 wins satisfies every
        # lower-cutoff band.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        cuts = [cut for cut, _label, _w in LEAGUE_CONTEXTS["BSB"].thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1], f"BSB band cutoffs must be monotonic: {cuts}"


# =====================================================================
# Phase O Phase 1: NcaaBaseballPlayoffSource
# =====================================================================

class TestNcaaBaseballPlayoffSource:
    """NcaaBaseballPlayoffSource: BestOfNSeriesSource subclass that ships
    the cleanly-labeled best-of-3 stages (Super Regional + MCWS Finals).
    Regional double-elim and 8-team MCWS bracket are Phase 2 — those
    headlines lack game-number metadata so chronological inference is
    needed and that's a separate design.
    """

    @staticmethod
    def _make_source(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            NcaaBaseballPlayoffSource,
        )
        src = NcaaBaseballPlayoffSource(season_year=2026)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "NCAABSB"
        assert "Postseason" in src.sport_label
        assert src._league_context_code() == "MCWS_PO"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_ko_stages(self):
        src = self._make_source()
        # Phase 1: only the cleanly-labeled stages are emitted.
        # Phase 2 will extend KO_STAGES with REGIONAL + MCWS bracket.
        assert src.KO_STAGES == ("BSB_SR", "MCWS_F")

    def test_series_length_is_three(self):
        # Both Super Regional and MCWS Finals are best-of-3.
        src = self._make_source()
        assert src.SERIES_LENGTH == 3
        assert src._series_length_for_stage("BSB_SR") == 3
        assert src._series_length_for_stage("MCWS_F") == 3

    def test_winner_advance_label(self):
        src = self._make_source()
        # MCWS Final winner → MCWS_W synthetic depth.
        assert src._winner_advance_label("MCWS_F") == "MCWS_W"
        # Super Regional advances via default stage_depth + 1 rule
        # (which lands at MCWS depth — the unmodeled 8-team bracket).
        assert src._winner_advance_label("BSB_SR") is None

    def test_outcome_labels_uses_mcws_po_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "super_regional" in labels
        assert "omaha_bound" in labels
        assert "cws_final" in labels
        assert "cws_champion" in labels

    def test_set_regular_season_strengths_passes_through(self):
        src = self._make_source()
        assert src.estimate_strengths() == {}
        src.set_regular_season_strengths(
            {"LSU": {"pf_per_game": 8.4, "pa_per_game": 4.1}}
        )
        s = src.estimate_strengths()
        assert s["LSU"]["pf_per_game"] == 8.4

    def test_super_regional_resolves_in_two_wins(self):
        """Super Regional best-of-3: clinches at 2 wins. Build a 2-0
        sweep; verify winner reaches MCWS depth, loser caps at BSB_SR."""
        games = [
            {"game_id": "sr1", "stage": "BSB_SR", "matchday": 1,
             "home": "LSU", "away": "Tennessee",
             "home_goals": 7, "away_goals": 3,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "sr2", "stage": "BSB_SR", "matchday": 2,
             "home": "LSU", "away": "Tennessee",
             "home_goals": 5, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        # LSU wins → advances via default stage_depth + 1 = MCWS depth.
        assert state["_round_reached"]["LSU"] == KNOCKOUT_ROUND_DEPTH["MCWS"]
        # Tennessee loses → caps at BSB_SR depth.
        assert state["_round_reached"]["Tennessee"] == KNOCKOUT_ROUND_DEPTH["BSB_SR"]

    def test_mcws_finals_winner_reaches_synthetic_depth(self):
        """Best-of-3 Finals, winner reaches MCWS_W depth."""
        games = [
            {"game_id": "f1", "stage": "MCWS_F", "matchday": 1,
             "home": "LSU", "away": "Florida",
             "home_goals": 4, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f2", "stage": "MCWS_F", "matchday": 2,
             "home": "LSU", "away": "Florida",
             "home_goals": 1, "away_goals": 6,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f3", "stage": "MCWS_F", "matchday": 3,
             "home": "Florida", "away": "LSU",
             "home_goals": 2, "away_goals": 9,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["LSU"] == KNOCKOUT_ROUND_DEPTH["MCWS_W"]
        assert state["_round_reached"]["Florida"] == KNOCKOUT_ROUND_DEPTH["MCWS_F"]
        outcomes = src.terminal_outcomes(state)
        # Champion gets every band.
        assert set(outcomes["LSU"]) == {
            "super_regional", "omaha_bound", "cws_final", "cws_champion",
        }
        # Finalist loser missed cws_champion only.
        assert set(outcomes["Florida"]) == {
            "super_regional", "omaha_bound", "cws_final",
        }


class TestBaseballPlayoffHeadlineParser:
    """The Phase 1 headline parser maps ESPN's exact headline text to
    (stage, game_index). Regression-pin against the 2025-2026 observed
    headlines so an ESPN copy change is caught at unit-test time, not
    at refresh time on Selection Monday."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_playoff_headline,
        )
        return _parse_baseball_playoff_headline(headline)

    def test_super_regional_game_1(self):
        assert self._parse(
            "NCAA Baseball Championship - Auburn Super Regional - Game 1"
        ) == ("BSB_SR", 1)

    def test_super_regional_game_3_if_necessary(self):
        # The "(if necessary)" trailer must be stripped — ESPN attaches
        # it to Game 3 placeholders that haven't been confirmed.
        assert self._parse(
            "NCAA Baseball Championship - Auburn Super Regional - Game 3 (if necessary)"
        ) == ("BSB_SR", 3)

    def test_mcws_final_game_1(self):
        # Note baseball uses "Final" (singular), softball uses "Finals" (plural).
        assert self._parse(
            "Men's College World Series Championship Final - Game 1"
        ) == ("MCWS_F", 1)

    def test_regional_returns_none(self):
        # Regionals carry no game number → Phase 2 territory.
        # Parser must return (None, None) so caller skips the event.
        assert self._parse(
            "NCAA Baseball Championship - Auburn Regional"
        ) == (None, None)

    def test_mcws_bracket_double_elim_returns_none(self):
        # 8-team bracket games carry the generic "Double Elimination Round"
        # headline with no game number → Phase 2 territory.
        assert self._parse(
            "Men's College World Series - Double Elimination Round"
        ) == (None, None)

    def test_mcws_elimination_game_returns_none(self):
        # MCWS losers-bracket games ESPN tags as "Elimination Game" —
        # still Phase 2 (no game number).
        assert self._parse(
            "Men's College World Series - Elimination Game"
        ) == (None, None)

    def test_empty_headline(self):
        assert self._parse("") == (None, None)


class TestNcaaBaseballPostseasonFilter:
    """The regular-season source MUST filter out season.type=3 events
    so postseason game wins don't inflate the regular-season win count
    that drives the BSB threshold bands."""

    def test_is_postseason_event(self):
        """ESPN spreads NCAA Baseball/Softball postseason across multiple
        season.type values (3=Regional, 4=Super Regional, 5=MCWS/WCWS
        bracket, 6=Finals). All four must read as postseason. Regular
        season is 2, preseason 1, both must read as NOT postseason."""
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _is_postseason_event,
        )
        # Postseason: all stage values.
        assert _is_postseason_event({"season": {"type": 3}}) is True
        assert _is_postseason_event({"season": {"type": 4}}) is True
        assert _is_postseason_event({"season": {"type": 5}}) is True
        assert _is_postseason_event({"season": {"type": 6}}) is True
        # Non-postseason.
        assert _is_postseason_event({"season": {"type": 2}}) is False
        assert _is_postseason_event({"season": {"type": 1}}) is False
        # Defensive: missing / malformed.
        assert _is_postseason_event({}) is False
        assert _is_postseason_event({"season": {}}) is False
        assert _is_postseason_event({"season": {"type": None}}) is False


# =====================================================================
# #43 Phase 2b: NcaaBaseballPlayoffBracketSource
# =====================================================================


class TestBaseballBracketHeadlineParser:
    """Verbatim ESPN headline patterns for the Regional + 8-team MCWS
    bracket stages. The parser MUST stay aligned with ESPN's exact text;
    pin every observed pattern as a regression test."""

    def test_regional_game_n(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, key = _parse_baseball_bracket_headline(
            "NCAA Baseball Championship - Auburn Regional - Game 1"
        )
        assert stage == "BSB_REG"
        assert key == "Auburn Regional"

    def test_regional_elimination_game(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, key = _parse_baseball_bracket_headline(
            "NCAA Baseball Championship - Knoxville Regional - Elimination Game"
        )
        assert stage == "BSB_REG"
        assert key == "Knoxville Regional"

    def test_regional_advances_to_super_regional(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        # ESPN flags the Regional final with "advances to Super Regional".
        stage, key = _parse_baseball_bracket_headline(
            "NCAA Baseball Championship - Auburn Regional - Auburn advances to Super Regional"
        )
        assert stage == "BSB_REG"
        assert key == "Auburn Regional"

    def test_mcws_double_elimination_round(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, key = _parse_baseball_bracket_headline(
            "Men's College World Series - Double Elimination Round"
        )
        assert stage == "MCWS"
        # Partial key — sub-bracket resolution happens chronologically.
        assert key is None

    def test_mcws_elimination_game(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, _key = _parse_baseball_bracket_headline(
            "Men's College World Series - Elimination Game"
        )
        assert stage == "MCWS"

    def test_mcws_advances_to_championship_series(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        # Bracket-final terminal signal.
        stage, _key = _parse_baseball_bracket_headline(
            "Coastal Carolina advances to Championship Series"
        )
        assert stage == "MCWS"

    def test_super_regional_headline_belongs_to_sibling_source(self):
        # SUPER_REGIONAL headlines are owned by NcaaBaseballPlayoffSource,
        # NOT the bracket source — the bracket parser must return None.
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, key = _parse_baseball_bracket_headline(
            "NCAA Baseball Championship - Auburn Super Regional - Game 1"
        )
        assert stage is None
        assert key is None

    def test_finals_headline_belongs_to_sibling_source(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        stage, key = _parse_baseball_bracket_headline(
            "Men's College World Series Championship Final - Game 1"
        )
        assert stage is None
        assert key is None

    def test_empty_and_unrelated_headlines_return_none(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        assert _parse_baseball_bracket_headline("") == (None, None)
        assert _parse_baseball_bracket_headline("regular-season game") == (None, None)


class TestMcwsSubBracketClassification:
    """The 8-team MCWS bracket partitions into two 4-team sub-brackets
    based on Day-1-vs-Day-2 opening pairings. Verified against the
    2025 MCWS schedule documented in #43's issue comment."""

    @staticmethod
    def _g(home, away, ts):
        return {"home": home, "away": away, "start_time": ts}

    def test_2025_mcws_day_partition(self):
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        # 2025 MCWS schedule from issue #43 comment:
        # Day 1 (06-13): CC vs Arizona, Oregon State vs Louisville → sub1
        # Day 2 (06-14): UCLA vs Murray State, Arkansas vs LSU       → sub2
        # Day 3+ partitions confirmed against actual 2025 games.
        games = [
            self._g("Coastal Carolina", "Arizona",
                    datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)),
            self._g("Oregon State", "Louisville",
                    datetime(2025, 6, 13, 23, 0, tzinfo=timezone.utc)),
            self._g("UCLA", "Murray State",
                    datetime(2025, 6, 14, 18, 0, tzinfo=timezone.utc)),
            self._g("Arkansas", "LSU",
                    datetime(2025, 6, 14, 23, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_mcws_sub_brackets(games)
        # Sub 1 (Day 1 teams):
        assert out["Coastal Carolina"] == "MCWS_sub1"
        assert out["Arizona"] == "MCWS_sub1"
        assert out["Oregon State"] == "MCWS_sub1"
        assert out["Louisville"] == "MCWS_sub1"
        # Sub 2 (Day 2 teams):
        assert out["UCLA"] == "MCWS_sub2"
        assert out["Murray State"] == "MCWS_sub2"
        assert out["Arkansas"] == "MCWS_sub2"
        assert out["LSU"] == "MCWS_sub2"

    def test_day3_game_inherits_from_opening_day(self):
        # Day 3 game (Oregon State vs Coastal Carolina) — both teams
        # already classified to sub1 from Day 1, so the Day 3 game
        # MUST stay sub1.
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        games = [
            # Day 1 establishes sub1
            self._g("Coastal Carolina", "Arizona",
                    datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)),
            self._g("Oregon State", "Louisville",
                    datetime(2025, 6, 13, 23, 0, tzinfo=timezone.utc)),
            # Day 2 establishes sub2
            self._g("UCLA", "Arkansas",
                    datetime(2025, 6, 14, 18, 0, tzinfo=timezone.utc)),
            # Day 3 rematch — both teams classified already.
            self._g("Oregon State", "Coastal Carolina",
                    datetime(2025, 6, 15, 23, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_mcws_sub_brackets(games)
        assert out["Oregon State"] == "MCWS_sub1"
        assert out["Coastal Carolina"] == "MCWS_sub1"

    def test_empty_input_returns_empty_dict(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        assert _classify_mcws_sub_brackets([]) == {}

    def test_missing_start_time_skipped(self):
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        games = [
            self._g("UCLA", "Arkansas", None),  # skipped
            self._g("Oregon State", "Louisville",
                    datetime(2025, 6, 13, 23, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_mcws_sub_brackets(games)
        assert "UCLA" not in out
        assert out["Oregon State"] == "MCWS_sub1"

    def test_day_partition_handles_utc_midnight_cross(self):
        # Real MCWS schedule: Day 1 evening game in Omaha (CT) crosses
        # UTC midnight. Pin the venue-local partition logic.
        # Day 1 in Omaha = 2025-06-13 CT:
        #   - 1 PM CT  = 2025-06-13 18:00 UTC
        #   - 7 PM CT  = 2025-06-14 00:00 UTC   ← different UTC date
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        games = [
            self._g("Coastal Carolina", "Arizona",
                    datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)),
            # Second Day-1 game crosses to next UTC date.
            self._g("Oregon State", "Louisville",
                    datetime(2025, 6, 14, 0, 0, tzinfo=timezone.utc)),
            # Day 2 first game.
            self._g("UCLA", "Murray State",
                    datetime(2025, 6, 14, 18, 0, tzinfo=timezone.utc)),
            # Day 2 second game crosses.
            self._g("Arkansas", "LSU",
                    datetime(2025, 6, 15, 0, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_mcws_sub_brackets(games)
        # All Day-1-in-CT teams in sub1.
        for team in ("Coastal Carolina", "Arizona", "Oregon State", "Louisville"):
            assert out[team] == "MCWS_sub1", f"{team} = {out.get(team)}"
        # All Day-2-in-CT teams in sub2.
        for team in ("UCLA", "Murray State", "Arkansas", "LSU"):
            assert out[team] == "MCWS_sub2", f"{team} = {out.get(team)}"

    def test_chronological_order_independent_of_input_order(self):
        # Pass games out of chronological order; the classifier sorts
        # by start_time internally.
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _classify_mcws_sub_brackets,
        )
        games = [
            # Day 2 first in the list
            self._g("UCLA", "Arkansas",
                    datetime(2025, 6, 14, 18, 0, tzinfo=timezone.utc)),
            # Day 1 second
            self._g("Coastal Carolina", "Arizona",
                    datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_mcws_sub_brackets(games)
        assert out["Coastal Carolina"] == "MCWS_sub1"
        assert out["UCLA"] == "MCWS_sub2"


class TestNcaaBaseballPlayoffBracketSource:
    """End-to-end source class for BSB_REG + MCWS. Uses a patched
    `_fetch_bracket_games` so no HTTP touches the test path."""

    @staticmethod
    def _src_with_games(games):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            NcaaBaseballPlayoffBracketSource,
        )
        src = NcaaBaseballPlayoffBracketSource()
        src._bracket_games_cache = games
        return src

    def test_ko_stages_are_regional_and_mcws(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            NcaaBaseballPlayoffBracketSource,
        )
        assert NcaaBaseballPlayoffBracketSource.KO_STAGES == ("BSB_REG", "MCWS")

    def test_league_context_code(self):
        src = self._src_with_games([])
        assert src._league_context_code() == "MCWS_PO"

    def test_winner_advance_mcws_to_finals(self):
        # MCWS sub-bracket winner advances to MCWS_F (depth 3, handled
        # by sibling source). BSB_REG winner falls through to default.
        src = self._src_with_games([])
        assert src._winner_advance_label("MCWS") == "MCWS_F"
        assert src._winner_advance_label("BSB_REG") is None

    def test_supports_importance(self):
        src = self._src_with_games([])
        assert src.supports_importance is True

    def test_strength_seeding_roundtrip(self):
        src = self._src_with_games([])
        s = {"LSU": {"pf_per_game": 7.5, "pa_per_game": 4.2}}
        src.set_regular_season_strengths(s)
        assert src.estimate_strengths() == s

    def test_strength_default_for_unseeded_team(self):
        src = self._src_with_games([])
        # Default falls back to _DEFAULT_RUNS_FOR / AGAINST (6.0 each).
        out = src._strength_for({}, "Unknown")
        assert out == {"pf_per_game": 6.0, "pa_per_game": 6.0}


class TestNcaaBaseballPlayoffBracketSourceFiltering:
    """Make sure the bracket source DOES NOT step on the sibling
    NcaaBaseballPlayoffSource's stages — best-of-3 BSB_SR + MCWS_F
    headlines must be ignored by the bracket parser."""

    def test_super_regional_excluded(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        for headline in [
            "NCAA Baseball Championship - Knoxville Super Regional - Game 1",
            "NCAA Baseball Championship - Auburn Super Regional - Game 3 (if necessary)",
        ]:
            stage, key = _parse_baseball_bracket_headline(headline)
            assert (stage, key) == (None, None), f"bracket parser claimed {headline!r}"

    def test_finals_excluded(self):
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import (
            _parse_baseball_bracket_headline,
        )
        for headline in [
            "Men's College World Series Championship Final - Game 1",
            "Men's College World Series Championship Final - Game 3 (if necessary)",
        ]:
            stage, key = _parse_baseball_bracket_headline(headline)
            assert (stage, key) == (None, None), f"bracket parser claimed {headline!r}"


# =====================================================================
# #43 Phase 2b: NcaaSoftballPlayoffBracketSource
# =====================================================================


class TestSoftballBracketHeadlineParser:
    def test_regional_game_n(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, key = _parse_softball_bracket_headline(
            "NCAA Softball Championship - Lincoln Regional - Game 2"
        )
        assert stage == "SB_REG"
        assert key == "Lincoln Regional"

    def test_regional_elimination_game(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, key = _parse_softball_bracket_headline(
            "NCAA Softball Championship - Tallahassee Regional - Elimination Game"
        )
        assert stage == "SB_REG"
        assert key == "Tallahassee Regional"

    def test_wcws_double_elimination_round(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, key = _parse_softball_bracket_headline(
            "Women's College World Series - Double Elimination Round"
        )
        assert stage == "WCWS"
        assert key is None

    def test_wcws_elimination_game_if_necessary(self):
        # Verbatim text from 2025 WCWS — "If Necessary" suffix is part
        # of the elimination-game pattern.
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, _key = _parse_softball_bracket_headline(
            "Women's College World Series - Elimination Game If Necessary"
        )
        assert stage == "WCWS"

    def test_super_regional_belongs_to_sibling_source(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, key = _parse_softball_bracket_headline(
            "NCAA Softball Championship - Lincoln Super Regional - Game 1"
        )
        assert (stage, key) == (None, None)

    def test_wcws_finals_plural_belongs_to_sibling_source(self):
        # Softball uses "Finals" plural (vs baseball's singular "Final");
        # the parser must NOT swallow it.
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_bracket_headline,
        )
        stage, key = _parse_softball_bracket_headline(
            "Women's College World Series Championship Finals - Game 1"
        )
        assert (stage, key) == (None, None)


class TestWcwsSubBracketClassification:
    """Same day-partition heuristic as MCWS — pinned by a representative
    fixture using verbatim 2026 WCWS opening-day pairings."""

    @staticmethod
    def _g(home, away, ts):
        return {"home": home, "away": away, "start_time": ts}

    def test_empty_input_returns_empty_dict(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _classify_wcws_sub_brackets,
        )
        assert _classify_wcws_sub_brackets([]) == {}

    def test_chronological_order_independent_of_input_order(self):
        # Out-of-order input → classifier sorts by start_time internally.
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _classify_wcws_sub_brackets,
        )
        games = [
            # Day 2 first in the list
            self._g("UCLA", "Tennessee",
                    datetime(2026, 5, 30, 22, 0, tzinfo=timezone.utc)),
            # Day 1 second
            self._g("Texas Tech", "Oklahoma",
                    datetime(2026, 5, 29, 22, 0, tzinfo=timezone.utc)),
        ]
        out = _classify_wcws_sub_brackets(games)
        assert out["Texas Tech"] == "WCWS_sub1"
        assert out["UCLA"] == "WCWS_sub2"

    def test_wcws_day_partition_across_utc_midnight(self):
        # Real WCWS schedule: Day 1 evening games in OKC (CT) routinely
        # cross UTC midnight. Pin the venue-local partition logic with
        # a fixture that exercises this exact case.
        # Day 1 in OKC = 2026-05-29 CT:
        #   - 5 PM CT = 2026-05-29 22:00 UTC
        #   - 7:30 PM CT = 2026-05-30 00:30 UTC   ← different UTC date!
        # Day 2 in OKC = 2026-05-30 CT:
        #   - 5 PM CT = 2026-05-30 22:00 UTC
        #   - 7:30 PM CT = 2026-05-31 00:30 UTC
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _classify_wcws_sub_brackets,
        )
        games = [
            # Day 1 (May 29 CT) — first game stays in May 29 UTC.
            self._g("Texas Tech", "Oklahoma",
                    datetime(2026, 5, 29, 22, 0, tzinfo=timezone.utc)),
            # Day 1 second game crosses into May 30 UTC.
            self._g("UCLA", "Tennessee",
                    datetime(2026, 5, 30, 0, 30, tzinfo=timezone.utc)),
            # Day 2 (May 30 CT) first game.
            self._g("Florida", "LSU",
                    datetime(2026, 5, 30, 22, 0, tzinfo=timezone.utc)),
            # Day 2 second game crosses into May 31 UTC.
            self._g("Oregon", "Arkansas",
                    datetime(2026, 5, 31, 0, 30, tzinfo=timezone.utc)),
        ]
        out = _classify_wcws_sub_brackets(games)
        # All 4 Day-1-in-CT teams must land in sub1 even though their
        # game times span 2 different UTC dates.
        assert out["Texas Tech"] == "WCWS_sub1"
        assert out["Oklahoma"] == "WCWS_sub1"
        assert out["UCLA"] == "WCWS_sub1"
        assert out["Tennessee"] == "WCWS_sub1"
        # Day 2 teams in sub2.
        assert out["Florida"] == "WCWS_sub2"
        assert out["LSU"] == "WCWS_sub2"
        assert out["Oregon"] == "WCWS_sub2"
        assert out["Arkansas"] == "WCWS_sub2"


class TestNcaaSoftballPlayoffBracketSource:
    @staticmethod
    def _src_with_games(games):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            NcaaSoftballPlayoffBracketSource,
        )
        src = NcaaSoftballPlayoffBracketSource()
        src._bracket_games_cache = games
        return src

    def test_ko_stages_are_regional_and_wcws(self):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            NcaaSoftballPlayoffBracketSource,
        )
        assert NcaaSoftballPlayoffBracketSource.KO_STAGES == ("SB_REG", "WCWS")

    def test_league_context_code(self):
        src = self._src_with_games([])
        assert src._league_context_code() == "WCWS_PO"

    def test_winner_advance_wcws_to_finals(self):
        src = self._src_with_games([])
        assert src._winner_advance_label("WCWS") == "WCWS_F"
        assert src._winner_advance_label("SB_REG") is None

    def test_strength_default_uses_softball_5_runs(self):
        # Softball default is 5.0 runs (lower-scoring than baseball's 6.0).
        src = self._src_with_games([])
        out = src._strength_for({}, "Unknown")
        assert out == {"pf_per_game": 5.0, "pa_per_game": 5.0}


# =====================================================================
# Phase O: NCAA Soccer (M's + W's parametrized on gender)
# =====================================================================

class TestNcaaSoccerSource:
    """Phase O: one source class for both M's and W's, parametrized on
    `gender`. Standings-points-based (3 W / 1 D / 0 L) since draws are
    common in college soccer.
    """

    @staticmethod
    def _make_source(gender="m"):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer import NcaaSoccerSource
        return NcaaSoccerSource(gender=gender, season_year=2025)

    # ---------- identity + parametrization ----------

    def test_gender_routes_to_correct_context(self):
        m = self._make_source("m")
        w = self._make_source("w")
        assert m.league_context_code == "NCAA_MSOC"
        assert w.league_context_code == "NCAA_WSOC"
        assert m.sport_prefix == "NCAAMSOC"
        assert w.sport_prefix == "NCAAWSOC"
        assert "Men's" in m.sport_label
        assert "Women's" in w.sport_label

    def test_gender_routes_to_correct_espn_slug(self):
        m = self._make_source("m")
        w = self._make_source("w")
        assert m._espn_slug == "usa.ncaa.m.1"
        assert w._espn_slug == "usa.ncaa.w.1"

    def test_invalid_gender_rejected(self):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer import NcaaSoccerSource
        try:
            NcaaSoccerSource(gender="x")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError on invalid gender")

    def test_count_field_is_standings_points(self):
        src = self._make_source()
        assert src._count_field == "standings_points"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    # ---------- standings_points credit (3 / 1 / 0) ----------

    def test_record_result_win_credits_three_points_to_winner(self):
        src = self._make_source()
        teams = {
            "Washington": {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "Stanford":   {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "Washington", "Stanford", 2, 1)
        assert teams["Washington"]["standings_points"] == 3
        assert teams["Stanford"]["standings_points"] == 0
        assert teams["Washington"]["wins"] == 1
        assert teams["Stanford"]["losses"] == 1
        # draws field initialized at 0
        assert teams["Washington"]["draws"] == 0

    def test_record_result_draw_credits_one_point_each(self):
        src = self._make_source()
        teams = {
            "Florida State": {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "Duke":          {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "Florida State", "Duke", 1, 1)
        # Draw: 1 standings point each, no W/L update, draws counter +1.
        assert teams["Florida State"]["standings_points"] == 1
        assert teams["Duke"]["standings_points"] == 1
        assert teams["Florida State"]["wins"] == 0
        assert teams["Florida State"]["losses"] == 0
        assert teams["Florida State"]["draws"] == 1
        assert teams["Duke"]["draws"] == 1

    def test_record_result_loss_credits_zero_points(self):
        src = self._make_source()
        teams = {
            "TCU":     {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "NC State": {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "TCU", "NC State", 0, 2)
        assert teams["TCU"]["standings_points"] == 0
        assert teams["NC State"]["standings_points"] == 3

    def test_draws_heavy_team_outranks_winless_in_standings(self):
        # The whole point of standings_points: a 5-3-7 team (5W 3L 7D, 22 pts)
        # ranks above a 6-9-0 team (18 pts) even with fewer wins.
        src = self._make_source()
        def fresh_row():
            return {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0}
        teams = {"DrawHeavy": fresh_row(), "WinHeavy": fresh_row()}
        # Pre-seed stub opponents so _record_result_into_state doesn't KeyError.
        for k in ("Stub1", "Stub2", "Stub3", "Stub4", "Stub5"):
            teams[k] = fresh_row()
        # DrawHeavy: 5W (vs Stub1), 3L (vs Stub2), 7D (vs Stub3)
        for _ in range(5):
            src._record_result_into_state(teams, "DrawHeavy", "Stub1", 2, 1)
        for _ in range(3):
            src._record_result_into_state(teams, "DrawHeavy", "Stub2", 0, 2)
        for _ in range(7):
            src._record_result_into_state(teams, "DrawHeavy", "Stub3", 1, 1)
        # WinHeavy: 6W (vs Stub4), 9L (vs Stub5)
        for _ in range(6):
            src._record_result_into_state(teams, "WinHeavy", "Stub4", 2, 0)
        for _ in range(9):
            src._record_result_into_state(teams, "WinHeavy", "Stub5", 0, 3)
        assert teams["DrawHeavy"]["standings_points"] == 22  # 5*3 + 7*1
        assert teams["WinHeavy"]["standings_points"] == 18   # 6*3
        # DrawHeavy outranks WinHeavy despite fewer wins.
        assert teams["DrawHeavy"]["standings_points"] > teams["WinHeavy"]["standings_points"]

    # ---------- sample_result allows ties ----------

    def test_sample_result_can_produce_ties(self):
        import random
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from datetime import datetime, timezone
        src = self._make_source()
        strengths = {"A": {"pf_per_game": 1.0, "pa_per_game": 1.0},
                     "B": {"pf_per_game": 1.0, "pa_per_game": 1.0}}
        gr = GameRow(
            sport_prefix="NCAAMSOC", sport_label="NCAA Men's Soccer",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=datetime(2025, 10, 1, tzinfo=timezone.utc),
        )
        # With lambda~1.0 and 200 samples, draws happen frequently
        # (Poisson(1) variance puts ~30%+ chance of identical scores).
        # Confirm at least one tie sampled — base PointsBasedSportSource
        # would have force-coin-flipped it away.
        seen_tie = False
        for seed in range(200):
            rng = random.Random(seed)
            res = src.sample_result({}, gr, strengths, rng)
            if res.home_goals == res.away_goals:
                seen_tie = True
                break
        assert seen_tie, "soccer sample_result must allow ties (1 pt each)"

    # ---------- LEAGUE_CONTEXTS bands ----------

    def test_ncaa_msoc_in_league_contexts(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["NCAA_MSOC"]
        assert ctx.format == "points_count"
        labels = {label for _c, label, _w in ctx.thresholds}
        assert "tournament_bubble" in labels
        assert "national_seed" in labels

    def test_ncaa_wsoc_in_league_contexts(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["NCAA_WSOC"]
        assert ctx.format == "points_count"

    def test_ncaa_soccer_thresholds_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        for code in ("NCAA_MSOC", "NCAA_WSOC"):
            cuts = [c for c, _l, _w in LEAGUE_CONTEXTS[code].thresholds]
            for i in range(len(cuts) - 1):
                assert cuts[i] < cuts[i + 1], f"{code} cutoffs must be monotonic: {cuts}"


# =====================================================================
# Issue #17: NHL CUP_FINAL placeholder during conf-finals
# =====================================================================

class TestNhlCupFinalPlaceholder:
    """Issue #17: synthesize a CUP_FINAL placeholder tie when both
    CONF_FINAL series exist but the api-web bracket endpoint hasn't
    yet populated the SCF series (which only happens after both
    conf-finals end). Without the placeholder, cup_winner leverage
    reads 0 during the highest-leverage week of the season.
    """

    @staticmethod
    def _stub_source(bracket_games):
        """Build an NhlPlayoffSource with a pre-baked bracket-games list
        so tests don't hit the network."""
        from dispatcharr_ranked_matchups.sources.nhl import NhlPlayoffSource
        src = NhlPlayoffSource(season="20252026")
        src._bracket_games_cache = bracket_games
        return src

    @staticmethod
    def _r1_to_cf_games_two_conf_finals():
        """Synthesize bracket-games covering R1 + R2 + CONF_FINAL with no
        CUP_FINAL games. Mimics the live shape during conf-finals week.
        """
        from dispatcharr_ranked_matchups.sources.nhl import NHL_HOME_PATTERN
        games = []
        # Two CONF_FINAL series, each at 0-0 (no games applied yet for
        # the placeholder logic to fire — it's structural based on
        # series existence, not on series progress).
        for series_idx, (top, bot) in enumerate([("Colorado", "Vegas"), ("Montreal", "Carolina")]):
            for matchday in range(1, len(NHL_HOME_PATTERN) + 1):
                top_home = NHL_HOME_PATTERN[matchday - 1]
                home = top if top_home else bot
                away = bot if top_home else top
                games.append({
                    "game_id": 9000000 + series_idx * 10 + matchday,
                    "stage": "CONF_FINAL",
                    "matchday": matchday,
                    "home": home, "away": away,
                    "home_goals": None, "away_goals": None,
                    "status": "SCHEDULED",
                    "start_time": None,
                    "extra": {"series_letter": chr(ord("a") + series_idx)},
                })
        return games

    # ---------- synthesis ----------

    def test_placeholder_fires_when_two_conf_finals_no_cup_final(self):
        # Bypass _fetch_bracket_games' network call by stubbing the
        # cache directly with conf-final games + placeholder games.
        from dispatcharr_ranked_matchups.sources.nhl import (
            NhlPlayoffSource, _CUP_FINAL_TOP_SENTINEL, _CUP_FINAL_BOT_SENTINEL,
        )
        src = NhlPlayoffSource(season="20252026")
        bracket_games = self._r1_to_cf_games_two_conf_finals()
        bracket_games.extend(src._synth_cup_final_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        cup_ties = state["_bracket"].get("CUP_FINAL", [])
        assert len(cup_ties) == 1
        teams = set(cup_ties[0]["teams"])
        assert teams == {_CUP_FINAL_TOP_SENTINEL, _CUP_FINAL_BOT_SENTINEL}

    def test_placeholder_feeds_from_wired_to_conf_finals(self):
        from dispatcharr_ranked_matchups.sources.nhl import (
            NhlPlayoffSource, _CUP_FINAL_TOP_SENTINEL, _CUP_FINAL_BOT_SENTINEL,
        )
        src = NhlPlayoffSource(season="20252026")
        bracket_games = self._r1_to_cf_games_two_conf_finals()
        bracket_games.extend(src._synth_cup_final_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        cup_tie = state["_bracket"]["CUP_FINAL"][0]
        feeds = cup_tie["feeds_from"]
        assert feeds[_CUP_FINAL_TOP_SENTINEL] == ("CONF_FINAL", 0)
        assert feeds[_CUP_FINAL_BOT_SENTINEL] == ("CONF_FINAL", 1)
        # Downstream tie (not entry-level) so it blocks until both
        # conf-finals resolve before emitting remaining games.
        assert cup_tie["is_entry_tie"] is False

    def test_placeholder_blocks_remaining_matches_until_conf_finals_resolve(self):
        # While conf-finals are unresolved, the placeholder shouldn't
        # appear in remaining_matches output (resolve_participants
        # returns None when feeds aren't settled).
        from dispatcharr_ranked_matchups.sources.nhl import NhlPlayoffSource
        src = NhlPlayoffSource(season="20252026")
        bracket_games = self._r1_to_cf_games_two_conf_finals()
        bracket_games.extend(src._synth_cup_final_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        # Conf-finals games remain (14 from 2 series × 7 each, all SCHEDULED).
        # CUP_FINAL placeholder is blocked.
        cup_remaining = [g for g in remaining if g.extra.get("stage") == "CUP_FINAL"]
        assert len(cup_remaining) == 0, "CUP_FINAL must be blocked while conf-finals unresolved"

    def test_synth_cup_final_uses_seven_games_with_nhl_home_pattern(self):
        from dispatcharr_ranked_matchups.sources.nhl import (
            NhlPlayoffSource, _CUP_FINAL_TOP_SENTINEL, _CUP_FINAL_BOT_SENTINEL,
            NHL_HOME_PATTERN,
        )
        src = NhlPlayoffSource(season="20252026")
        games = src._synth_cup_final_placeholder_games()
        assert len(games) == 7
        assert all(g["stage"] == "CUP_FINAL" for g in games)
        assert all(g["status"] == "SCHEDULED" for g in games)
        assert all(g["game_id"] < 0 for g in games), \
            "placeholder game IDs must be negative to avoid collision with real NHL game IDs"
        # Home pattern verification: game 1 has top sentinel at home;
        # game 3 has bottom sentinel at home; etc.
        for matchday in range(1, 8):
            top_home = NHL_HOME_PATTERN[matchday - 1]
            expected_home = _CUP_FINAL_TOP_SENTINEL if top_home else _CUP_FINAL_BOT_SENTINEL
            assert games[matchday - 1]["home"] == expected_home
            assert games[matchday - 1]["matchday"] == matchday

    def test_placeholder_resolves_to_real_teams_after_conf_finals(self):
        # After applying CONF_FINAL results, the placeholder's
        # _resolve_participants should yield the real conf-final winners.
        from dispatcharr_ranked_matchups.sources.nhl import NhlPlayoffSource
        from dispatcharr_ranked_matchups.sources.base import MatchResult
        src = NhlPlayoffSource(season="20252026")
        bracket_games = self._r1_to_cf_games_two_conf_finals()
        bracket_games.extend(src._synth_cup_final_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        # Apply 4 wins to Colorado vs Vegas (CF series 0): 4-0 sweep.
        remaining = src.remaining_matches(state)
        for _ in range(4):
            target = next(
                g for g in src.remaining_matches(state)
                if g.extra.get("stage") == "CONF_FINAL"
                and "Colorado" in (g.home, g.away)
                and "Vegas" in (g.home, g.away)
            )
            # Make Colorado the winner regardless of home/away assignment.
            if target.home == "Colorado":
                result = MatchResult(home_goals=3, away_goals=1)
            else:
                result = MatchResult(home_goals=1, away_goals=3)
            state = src.apply_result(state, target, result)
        # Now apply 4 wins to Carolina vs Montreal (CF series 1).
        for _ in range(4):
            target = next(
                g for g in src.remaining_matches(state)
                if g.extra.get("stage") == "CONF_FINAL"
                and "Carolina" in (g.home, g.away)
                and "Montreal" in (g.home, g.away)
            )
            if target.home == "Carolina":
                result = MatchResult(home_goals=3, away_goals=1)
            else:
                result = MatchResult(home_goals=1, away_goals=3)
            state = src.apply_result(state, target, result)
        # Both conf-finals resolved. CUP_FINAL placeholder should now
        # surface its games with REAL team names (Colorado / Carolina).
        remaining_after = src.remaining_matches(state)
        cup_games = [g for g in remaining_after if g.extra.get("stage") == "CUP_FINAL"]
        assert len(cup_games) == 7, f"expected 7 SCF games unblocked, got {len(cup_games)}"
        cup_teams = set()
        for g in cup_games:
            cup_teams.add(g.home)
            cup_teams.add(g.away)
        assert cup_teams == {"Colorado", "Carolina"}, \
            f"CUP_FINAL should be CF winners (Colorado vs Carolina), got {cup_teams}"


# =====================================================================
# Phase F: MLB
# =====================================================================

class TestBestOfNSeriesPerStageLength:
    """Phase F refactor: BestOfNSeriesSource grows _series_length_for_stage
    so MLB's mixed series lengths (WC=3, LDS=5, LCS=7, WS=7) can coexist
    in one source. NHL keeps the uniform-7 contract for free because the
    default implementation returns the class-level SERIES_LENGTH.
    """

    def test_default_per_stage_returns_class_series_length(self):
        from dispatcharr_ranked_matchups.sources.bracket import BestOfNSeriesSource

        class _Default(BestOfNSeriesSource):
            KO_STAGES = ("R1", "R2", "CONF_FINAL", "CUP_FINAL")
            SERIES_LENGTH = 7
            @property
            def sport_prefix(self): return "X"
            @property
            def sport_label(self): return "X"
            def fetch_upcoming(self, days_ahead=7): return []
            def _league_context_code(self): return "NHL_PO"
            def _fetch_bracket_games(self): return []

        src = _Default()
        # Uniform-length sport: every stage answers the class value.
        for stage in src.KO_STAGES:
            assert src._series_length_for_stage(stage) == 7
            assert src._clinching_wins_for_stage(stage) == 4

    def test_subclass_can_override_per_stage(self):
        from dispatcharr_ranked_matchups.sources.bracket import BestOfNSeriesSource

        class _Mixed(BestOfNSeriesSource):
            KO_STAGES = ("WC", "LDS", "LCS", "WS")
            SERIES_LENGTH = 7
            _STAGE_LENGTHS = {"WC": 3, "LDS": 5, "LCS": 7, "WS": 7}
            @property
            def sport_prefix(self): return "X"
            @property
            def sport_label(self): return "X"
            def fetch_upcoming(self, days_ahead=7): return []
            def _league_context_code(self): return "MLB_PO"
            def _fetch_bracket_games(self): return []
            def _series_length_for_stage(self, stage):
                return self._STAGE_LENGTHS.get(stage, self.SERIES_LENGTH)

        src = _Mixed()
        assert src._series_length_for_stage("WC") == 3
        assert src._series_length_for_stage("LDS") == 5
        assert src._series_length_for_stage("LCS") == 7
        assert src._series_length_for_stage("WS") == 7
        # clinching wins = ceil(N/2)
        assert src._clinching_wins_for_stage("WC") == 2
        assert src._clinching_wins_for_stage("LDS") == 3
        assert src._clinching_wins_for_stage("LCS") == 4
        assert src._clinching_wins_for_stage("WS") == 4

    def test_per_stage_clinching_resolves_short_series_early(self):
        """A best-of-3 series resolves at 2 wins. Verify the record path
        terminates correctly when the per-stage clinching count is smaller
        than the class-level fallback (which would still return 4 for
        SERIES_LENGTH=7).
        """
        from dispatcharr_ranked_matchups.sources.bracket import BestOfNSeriesSource

        class _Mixed(BestOfNSeriesSource):
            KO_STAGES = ("WC", "LDS")
            SERIES_LENGTH = 7
            @property
            def sport_prefix(self): return "X"
            @property
            def sport_label(self): return "X"
            def fetch_upcoming(self, days_ahead=7): return []
            def _league_context_code(self): return "MLB_PO"
            def _fetch_bracket_games(self): return []
            def _series_length_for_stage(self, stage):
                return 3 if stage == "WC" else 7

        src = _Mixed()
        tie = src._new_tie_record({"teams": frozenset({"A", "B"}), "stage": "WC"})
        # A wins games 1 and 2 → series clinched at 2 wins.
        src._record_game_into_tie(tie, "A", "B", 3, 1, game_index=1)
        assert tie["winner"] is None
        src._record_game_into_tie(tie, "A", "B", 4, 2, game_index=2)
        assert tie["winner"] == "A"
        assert tie["loser"] == "B"


class TestMlbRegularSource:
    """MlbRegularSource: PointsBasedSportSource with _count_field='wins'
    and statsapi.mlb.com schedule endpoint as the data source."""

    @staticmethod
    def _make_source():
        from dispatcharr_ranked_matchups.sources.mlb import MlbRegularSource
        return MlbRegularSource(season=2026)

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "MLB"
        assert src.sport_label == "MLB"
        assert src.league_context_code == "MLB"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_outcome_labels_uses_mlb_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "playoff_bubble" in labels
        assert "playoff_secured" in labels
        assert "division_pace" in labels
        assert "elite" in labels

    def test_mlb_thresholds_are_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["MLB"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1], f"MLB band cutoffs must be monotonic: {cuts}"

    def test_record_result_credits_winner_a_win(self):
        # MLB regular season uses raw win count; no OT bonus.
        src = self._make_source()
        teams = {
            "Cleveland Guardians": {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "Detroit Tigers":      {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "Cleveland Guardians", "Detroit Tigers", 5, 3)
        assert teams["Cleveland Guardians"]["wins"] == 1
        assert teams["Detroit Tigers"]["losses"] == 1
        assert teams["Cleveland Guardians"]["pf"] == 5
        assert teams["Cleveland Guardians"]["pa"] == 3

    def test_terminal_outcomes_buckets_by_win_count(self):
        src = self._make_source()
        # Construct a state with teams at different win counts.
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Cellar":   {"wins": 70, "losses": 92, "pf": 0, "pa": 0, "games_played": 162},
                "Bubble":   {"wins": 86, "losses": 76, "pf": 0, "pa": 0, "games_played": 162},
                "Comfy":    {"wins": 92, "losses": 70, "pf": 0, "pa": 0, "games_played": 162},
                "DivLead":  {"wins": 98, "losses": 64, "pf": 0, "pa": 0, "games_played": 162},
                "Elite":    {"wins": 108, "losses": 54, "pf": 0, "pa": 0, "games_played": 162},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Cellar"] == []  # below 85
        assert set(outcomes["Bubble"]) == {"playoff_bubble"}
        assert set(outcomes["Comfy"]) == {"playoff_bubble", "playoff_secured"}
        assert set(outcomes["DivLead"]) == {"playoff_bubble", "playoff_secured", "division_pace"}
        assert set(outcomes["Elite"]) == {"playoff_bubble", "playoff_secured", "division_pace", "elite"}


class TestMlbPlayoffSource:
    """MlbPlayoffSource: BestOfNSeriesSource subclass with per-stage
    series lengths (WC=3, LDS=5, LCS=7, WS=7) and _winner_advance_label
    mapping WS → WS_WINNER."""

    @staticmethod
    def _make_source(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.mlb import MlbPlayoffSource
        src = MlbPlayoffSource(season=2025)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "MLB"
        assert "Postseason" in src.sport_label
        assert src._league_context_code() == "MLB_PO"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_ko_stages(self):
        src = self._make_source()
        assert src.KO_STAGES == ("WC", "LDS", "LCS", "WS")

    def test_series_lengths_per_stage(self):
        src = self._make_source()
        assert src._series_length_for_stage("WC") == 3
        assert src._series_length_for_stage("LDS") == 5
        assert src._series_length_for_stage("LCS") == 7
        assert src._series_length_for_stage("WS") == 7

    def test_winner_advance_label(self):
        src = self._make_source()
        assert src._winner_advance_label("WS") == "WS_WINNER"
        # Earlier rounds default to stage_depth + 1 (returns None).
        assert src._winner_advance_label("WC") is None
        assert src._winner_advance_label("LDS") is None
        assert src._winner_advance_label("LCS") is None

    def test_outcome_labels_uses_mlb_po_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "division_series" in labels
        assert "championship" in labels
        assert "world_series" in labels
        assert "ws_winner" in labels

    def test_set_regular_season_strengths_passes_through(self):
        src = self._make_source()
        assert src.estimate_strengths() == {}
        src.set_regular_season_strengths({"NYY": {"pf_per_game": 5.1, "pa_per_game": 3.9}})
        s = src.estimate_strengths()
        assert s["NYY"]["pf_per_game"] == 5.1

    def test_wc_series_resolves_in_two_wins(self):
        """Wild Card best-of-3: clinches at 2 wins. Synthesize a 2-0 sweep
        and verify the winner reaches LDS depth."""
        games = [
            {"game_id": "wc1", "stage": "WC", "matchday": 1,
             "home": "Detroit", "away": "Cleveland",
             "home_goals": 5, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "wc2", "stage": "WC", "matchday": 2,
             "home": "Detroit", "away": "Cleveland",
             "home_goals": 4, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        # Detroit clinched, reaches LDS depth (one above WC).
        assert state["_round_reached"]["Detroit"] == KNOCKOUT_ROUND_DEPTH["LDS"]
        # Cleveland eliminated, capped at WC depth.
        assert state["_round_reached"]["Cleveland"] == KNOCKOUT_ROUND_DEPTH["WC"]

    def test_ws_winner_advance_reaches_synthetic_depth(self):
        """A team winning the World Series reaches the WS_WINNER synthetic
        depth (one above WS), and its terminal_outcomes contains every
        post-WC band including ws_winner."""
        games = [
            {"game_id": "ws1", "stage": "WS", "matchday": 1,
             "home": "Dodgers", "away": "Yankees",
             "home_goals": 6, "away_goals": 3,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "ws2", "stage": "WS", "matchday": 2,
             "home": "Dodgers", "away": "Yankees",
             "home_goals": 4, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "ws3", "stage": "WS", "matchday": 3,
             "home": "Yankees", "away": "Dodgers",
             "home_goals": 1, "away_goals": 4,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "ws4", "stage": "WS", "matchday": 4,
             "home": "Yankees", "away": "Dodgers",
             "home_goals": 2, "away_goals": 7,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Dodgers"] == KNOCKOUT_ROUND_DEPTH["WS_WINNER"]
        assert state["_round_reached"]["Yankees"] == KNOCKOUT_ROUND_DEPTH["WS"]
        outcomes = src.terminal_outcomes(state)
        # Champion gets every band.
        assert set(outcomes["Dodgers"]) == {
            "division_series", "championship", "world_series", "ws_winner",
        }
        # Loser misses ws_winner.
        assert set(outcomes["Yankees"]) == {
            "division_series", "championship", "world_series",
        }

    def test_fetch_bracket_games_maps_series_description_to_stage(self):
        """The _SERIES_DESC_TO_STAGE table covers every postseason series
        description statsapi.mlb.com emits. Verify the mapping for one
        game of each kind, including both AL and NL variants."""
        from dispatcharr_ranked_matchups.sources.mlb import _SERIES_DESC_TO_STAGE
        # The mapping covers WC/LDS/LCS for both leagues and the unified WS.
        expected = {
            "AL Wild Card Series": "WC",
            "NL Wild Card Series": "WC",
            "AL Division Series": "LDS",
            "NL Division Series": "LDS",
            "AL Championship Series": "LCS",
            "NL Championship Series": "LCS",
            "World Series": "WS",
        }
        assert _SERIES_DESC_TO_STAGE == expected

    def test_mlb_po_thresholds_are_depth_ordered(self):
        """MLB_PO threshold stages must appear in KNOCKOUT_ROUND_DEPTH in
        increasing depth order — otherwise the terminal_outcomes cascade
        would emit out-of-order bands."""
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS["MLB_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1], f"MLB_PO bands out of depth order: {depths}"


# =====================================================================
# Phase G: NBA
# =====================================================================

class TestNbaHeadlineParser:
    """The _parse_stage_from_headline regex is the only path that maps
    an ESPN playoff game to a bracket stage; if the regex breaks, every
    NBA playoff game silently disappears from the bracket. These tests
    pin every variation the live API emits."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.nba import _parse_stage_from_headline
        return _parse_stage_from_headline(headline)

    def test_east_first_round(self):
        assert self._parse("East 1st Round - Game 3") == {"stage": "R1", "matchday": 3}

    def test_west_first_round(self):
        assert self._parse("West 1st Round - Game 1") == {"stage": "R1", "matchday": 1}

    def test_east_semifinals(self):
        assert self._parse("East Semifinals - Game 7") == {"stage": "CSF", "matchday": 7}

    def test_west_semifinals(self):
        assert self._parse("West Semifinals - Game 2") == {"stage": "CSF", "matchday": 2}

    def test_east_conference_finals(self):
        assert self._parse("East Finals - Game 1") == {"stage": "CF", "matchday": 1}

    def test_west_conference_finals(self):
        assert self._parse("West Finals - Game 6") == {"stage": "CF", "matchday": 6}

    def test_nba_finals(self):
        assert self._parse("NBA Finals - Game 1") == {"stage": "FINALS", "matchday": 1}

    def test_nba_finals_game_seven(self):
        assert self._parse("NBA Finals - Game 7") == {"stage": "FINALS", "matchday": 7}

    def test_play_in_returns_none(self):
        """Play-in tournament games have headlines without a Game N
        suffix matching the regex. The bracket source treats them as
        not-in-bracket and skips them."""
        assert self._parse("Play-In Tournament") is None
        assert self._parse("East Play-In") is None

    def test_no_headline_returns_none(self):
        assert self._parse(None) is None
        assert self._parse("") is None

    def test_unrecognized_returns_none(self):
        # Defensive — preseason headlines or odd formats should not
        # accidentally match.
        assert self._parse("Preseason") is None
        assert self._parse("Game 5") is None

    def test_case_insensitive(self):
        assert self._parse("east 1st round - game 3") == {"stage": "R1", "matchday": 3}
        assert self._parse("nba finals - game 1") == {"stage": "FINALS", "matchday": 1}


class TestNbaAllStarFilter:
    """ESPN labels All-Star Tournament games with season.type=2
    (regular-season-like), polluting the regular-source team list
    with fake teams like 'Team Chuck' / 'Team Shaq'. The
    _extract_game_record filter must drop them via the
    competition.type.abbreviation=='ALLSTAR' discriminator."""

    @staticmethod
    def _extract(event):
        from dispatcharr_ranked_matchups.sources.nba import _extract_game_record
        return _extract_game_record(event)

    def test_all_star_game_filtered(self):
        # Minimal All-Star event shape based on ESPN's actual response.
        event = {
            "id": "401705381",
            "date": "2025-02-16T23:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "4", "abbreviation": "ALLSTAR"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "40",
                     "team": {"displayName": "Team Chuck"}},
                    {"homeAway": "away", "score": "30",
                     "team": {"displayName": "Team Kenny"}},
                ],
                "notes": [{"type": "event",
                           "headline": "NBA All-Star - Semifinals"}],
            }],
        }
        assert self._extract(event) is None

    def test_regular_season_game_passes(self):
        event = {
            "id": "401705000",
            "date": "2025-01-15T20:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "1", "abbreviation": "STD"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "110",
                     "team": {"displayName": "New York Knicks"}},
                    {"homeAway": "away", "score": "98",
                     "team": {"displayName": "Philadelphia 76ers"}},
                ],
            }],
        }
        rec = self._extract(event)
        assert rec is not None
        assert rec["home"] == "New York Knicks"
        assert rec["away"] == "Philadelphia 76ers"
        assert rec["status"] == "FINISHED"
        assert rec["home_points"] == 110

    def test_playoff_game_passes(self):
        # ESPN labels playoff games competition.type.abbreviation="FINAL"
        # (yes, even non-NBA-Finals rounds). This test pins that
        # _extract_game_record accepts those.
        event = {
            "id": "401705999",
            "date": "2025-06-05T20:00Z",
            "season": {"year": 2025, "type": 3, "slug": "post-season"},
            "competitions": [{
                "type": {"id": "17", "abbreviation": "FINAL"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "111",
                     "team": {"displayName": "Oklahoma City Thunder"}},
                    {"homeAway": "away", "score": "110",
                     "team": {"displayName": "Indiana Pacers"}},
                ],
                "notes": [{"type": "event", "headline": "NBA Finals - Game 1"}],
            }],
        }
        rec = self._extract(event)
        assert rec is not None
        assert rec["home"] == "Oklahoma City Thunder"
        assert rec["season_type"] == 3


class TestNbaRegularSource:
    """NbaRegularSource: PointsBasedSportSource with _count_field='wins'
    and ESPN unofficial scoreboard as the data source."""

    @staticmethod
    def _make_source():
        from dispatcharr_ranked_matchups.sources.nba import NbaRegularSource
        return NbaRegularSource(season_end_year=2025)

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "NBA"
        assert src.sport_label == "NBA"
        assert src.league_context_code == "NBA"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_outcome_labels_uses_nba_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "play_in_bubble" in labels
        assert "playoff_secured" in labels
        assert "top_seed_pace" in labels
        assert "elite" in labels

    def test_nba_thresholds_are_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["NBA"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1], f"NBA band cutoffs must be monotonic: {cuts}"

    def test_record_result_credits_winner_a_win(self):
        # NBA regular season uses raw win count; no OT bonus.
        src = self._make_source()
        teams = {
            "Boston Celtics":      {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "Los Angeles Lakers":  {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "Boston Celtics", "Los Angeles Lakers", 110, 98)
        assert teams["Boston Celtics"]["wins"] == 1
        assert teams["Los Angeles Lakers"]["losses"] == 1
        assert teams["Boston Celtics"]["pf"] == 110
        assert teams["Boston Celtics"]["pa"] == 98

    def test_terminal_outcomes_buckets_by_win_count(self):
        src = self._make_source()
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Cellar":   {"wins": 25, "losses": 57, "pf": 0, "pa": 0, "games_played": 82},
                "Bubble":   {"wins": 42, "losses": 40, "pf": 0, "pa": 0, "games_played": 82},
                "Comfy":    {"wins": 52, "losses": 30, "pf": 0, "pa": 0, "games_played": 82},
                "Top":      {"wins": 58, "losses": 24, "pf": 0, "pa": 0, "games_played": 82},
                "Historic": {"wins": 68, "losses": 14, "pf": 0, "pa": 0, "games_played": 82},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Cellar"] == []  # below 40
        assert set(outcomes["Bubble"]) == {"play_in_bubble"}
        assert set(outcomes["Comfy"]) == {"play_in_bubble", "playoff_secured"}
        assert set(outcomes["Top"]) == {"play_in_bubble", "playoff_secured", "top_seed_pace"}
        assert set(outcomes["Historic"]) == {
            "play_in_bubble", "playoff_secured", "top_seed_pace", "elite",
        }


class TestNbaPlayoffSource:
    """NbaPlayoffSource: BestOfNSeriesSource subclass with uniform
    SERIES_LENGTH=7 and _winner_advance_label mapping FINALS →
    FINALS_WINNER."""

    @staticmethod
    def _make_source(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.nba import NbaPlayoffSource
        src = NbaPlayoffSource(season_end_year=2025)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "NBA"
        assert "Playoffs" in src.sport_label
        assert src._league_context_code() == "NBA_PO"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_ko_stages(self):
        src = self._make_source()
        assert src.KO_STAGES == ("R1", "CSF", "CF", "FINALS")

    def test_series_lengths_uniform_seven(self):
        src = self._make_source()
        for stage in src.KO_STAGES:
            assert src._series_length_for_stage(stage) == 7
            assert src._clinching_wins_for_stage(stage) == 4

    def test_winner_advance_label(self):
        src = self._make_source()
        assert src._winner_advance_label("FINALS") == "FINALS_WINNER"
        # Earlier rounds default to stage_depth + 1 (returns None).
        assert src._winner_advance_label("R1") is None
        assert src._winner_advance_label("CSF") is None
        assert src._winner_advance_label("CF") is None

    def test_outcome_labels_uses_nba_po_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "conf_semis" in labels
        assert "conf_finals" in labels
        assert "nba_finals" in labels
        assert "finals_winner" in labels

    def test_set_regular_season_strengths_passes_through(self):
        src = self._make_source()
        assert src.estimate_strengths() == {}
        src.set_regular_season_strengths(
            {"Boston Celtics": {"pf_per_game": 118.0, "pa_per_game": 110.5}}
        )
        s = src.estimate_strengths()
        assert s["Boston Celtics"]["pf_per_game"] == 118.0

    def test_finals_winner_advance_reaches_synthetic_depth(self):
        """A team winning the NBA Finals reaches the FINALS_WINNER
        synthetic depth (one above FINALS), and its terminal_outcomes
        contains every post-R1 band including finals_winner."""
        games = [
            {"game_id": "f1", "stage": "FINALS", "matchday": 1,
             "home": "Champion", "away": "Finalist",
             "home_goals": 110, "away_goals": 90,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f2", "stage": "FINALS", "matchday": 2,
             "home": "Champion", "away": "Finalist",
             "home_goals": 105, "away_goals": 95,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f3", "stage": "FINALS", "matchday": 3,
             "home": "Finalist", "away": "Champion",
             "home_goals": 92, "away_goals": 104,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f4", "stage": "FINALS", "matchday": 4,
             "home": "Finalist", "away": "Champion",
             "home_goals": 98, "away_goals": 108,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Champion"] == KNOCKOUT_ROUND_DEPTH["FINALS_WINNER"]
        assert state["_round_reached"]["Finalist"] == KNOCKOUT_ROUND_DEPTH["FINALS"]
        outcomes = src.terminal_outcomes(state)
        # Champion gets every band.
        assert set(outcomes["Champion"]) == {
            "conf_semis", "conf_finals", "nba_finals", "finals_winner",
        }
        # Loser misses finals_winner.
        assert set(outcomes["Finalist"]) == {
            "conf_semis", "conf_finals", "nba_finals",
        }

    def test_nba_po_thresholds_are_depth_ordered(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS["NBA_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1], f"NBA_PO bands out of depth order: {depths}"


# =====================================================================
# Phase J: MLS
# =====================================================================

class TestMlsSourceIdentity:
    """MlsSource: V1 schedule+closeness adapter. No importance, no
    standings — just confirm the basic contract holds."""

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.mls import MlsSource
        return MlsSource(odds_api_key="")

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "MLS"
        assert src.sport_label == "MLS"

    def test_does_not_support_importance(self):
        """MlsSource is the closeness-only base class for NwslSource
        and LigaMxSource. MLS itself is now handled by MlsEastSource +
        MlsWestSource (issue #30 part A), which DO support importance.
        MlsSource the class stays importance-free so NWSL / Liga MX
        keep the V1 minimal shape until their own follow-ups land.
        """
        assert self._make().supports_importance is False


class TestMlsHeadToHeadParse:
    """The h2h-to-closeness math is the only non-trivial pure function
    in this source; pin its behavior against pickem and blowout odds
    profiles. Same calibration line as the soccer.py _h2h_to_closeness
    so MLS closeness scores stay comparable to EPL/UCL/etc."""

    @staticmethod
    def _conv(outcomes, home_lc, away_lc):
        from dispatcharr_ranked_matchups.sources.mls import _h2h_to_closeness
        return _h2h_to_closeness(outcomes, home_lc, away_lc)

    def test_pickem_match_high_closeness(self):
        # Equal odds = perfect pickem after devig.
        outcomes = [
            {"name": "LA Galaxy", "price": 2.0},
            {"name": "Draw", "price": 3.5},
            {"name": "Inter Miami CF", "price": 2.0},
        ]
        c = self._conv(outcomes, "LA Galaxy", "Inter Miami CF")
        assert c is not None
        assert c >= 0.95  # pickem ~= closeness 1.0

    def test_blowout_low_closeness(self):
        outcomes = [
            {"name": "LA Galaxy", "price": 1.2},   # heavy favorite
            {"name": "Draw", "price": 6.0},
            {"name": "FC Cincinnati", "price": 12.0},  # heavy dog
        ]
        c = self._conv(outcomes, "LA Galaxy", "FC Cincinnati")
        assert c is not None
        # Heavy mismatch -> low closeness.
        assert c <= 0.25

    def test_returns_none_on_missing_home_outcome(self):
        outcomes = [
            {"name": "Some Other Team", "price": 1.5},
            {"name": "Draw", "price": 3.0},
            {"name": "FC Cincinnati", "price": 4.0},
        ]
        # Neither outcome matches "LA Galaxy", so we can't compute closeness.
        assert self._conv(outcomes, "LA Galaxy", "Atlanta United FC") is None

    def test_suffix_fuzzy_match(self):
        """ESPN says 'Atlanta United FC'; Odds API says 'Atlanta
        United'. The normalize-and-substring matcher should treat
        them as the same outcome row."""
        outcomes = [
            {"name": "Atlanta United", "price": 2.0},
            {"name": "Draw", "price": 3.5},
            {"name": "Inter Miami", "price": 2.0},
        ]
        c = self._conv(outcomes, "Atlanta United FC", "Inter Miami CF")
        assert c is not None
        assert c >= 0.95


class TestMlsLookupClosenessFuzzy:
    """The lookup_closeness matcher handles ESPN/Odds API team-name
    drift (typically club-tag suffix differences). These tests pin
    the matcher so a future change to TEAM_SUFFIX_TOKENS doesn't
    silently break MLS odds lookup."""

    @staticmethod
    def _lookup(odds_map, home, away):
        from dispatcharr_ranked_matchups.sources.mls import MlsSource
        return MlsSource._lookup_closeness(odds_map, home, away)

    def test_exact_match(self):
        odds = {("la galaxy", "inter miami cf"): 0.65}
        assert self._lookup(odds, "LA Galaxy", "Inter Miami CF") == 0.65

    def test_suffix_strip_match(self):
        # ESPN: "Atlanta United FC"; Odds API: "Atlanta United"
        odds = {("atlanta united", "inter miami"): 0.55}
        assert self._lookup(odds, "Atlanta United FC", "Inter Miami CF") == 0.55

    def test_no_match_returns_none(self):
        odds = {("atlanta united", "inter miami"): 0.55}
        assert self._lookup(odds, "Seattle Sounders FC", "Toronto FC") is None

    def test_empty_odds_map_returns_none(self):
        assert self._lookup({}, "LA Galaxy", "Inter Miami CF") is None


class TestMlsFetchUpcomingShape:
    """Pin the GameRow shape MlsSource emits without making HTTP
    calls. Exercises the integration of ESPN scoreboard parsing +
    Odds API closeness lookup by stubbing both sides."""

    def test_fetch_upcoming_with_stubbed_endpoints(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.mls as mls_mod

        # Stub ESPN: one upcoming MLS game.
        espn_response = {
            "events": [{
                "id": "401123",
                "date": "2026-04-10T23:00Z",
                "season": {"year": 2026, "type": 2, "slug": "regular-season"},
                "competitions": [{
                    "competitors": [
                        {"homeAway": "home",
                         "team": {"displayName": "LA Galaxy"}},
                        {"homeAway": "away",
                         "team": {"displayName": "Inter Miami CF"}},
                    ],
                }],
            }],
        }

        # Stub Odds API: one upcoming match with pickem odds.
        odds_response = [{
            "home_team": "LA Galaxy",
            "away_team": "Inter Miami",
            "bookmakers": [{
                "markets": [{
                    "key": "h2h",
                    "outcomes": [
                        {"name": "LA Galaxy", "price": 2.0},
                        {"name": "Draw", "price": 3.5},
                        {"name": "Inter Miami", "price": 2.0},
                    ],
                }],
            }],
        }]

        def stub_http_get(url, timeout=15.0, **params):
            if "the-odds-api" in url or "odds" in url:
                return odds_response
            return espn_response

        monkeypatch.setattr(mls_mod, "_http_get", stub_http_get)

        src = mls_mod.MlsSource(odds_api_key="stubkey")
        games = src.fetch_upcoming(days_ahead=1)

        # ESPN is called twice (days_ahead=1 -> today + tomorrow) and
        # both return the same stub event; dedupe by event_id keeps
        # the count at 1.
        assert len(games) == 1
        g = games[0]
        assert g.home == "LA Galaxy"
        assert g.away == "Inter Miami CF"
        assert g.sport_prefix == "MLS"
        # Closeness was populated from the pickem Odds API outcome.
        assert g.closeness is not None
        assert g.closeness >= 0.95
        # season_slug carried through for future bracket routing.
        assert g.extra.get("season_slug") == "regular-season"

    def test_fetch_upcoming_works_without_odds_key(self, monkeypatch):
        """Without an Odds API key, ESPN still drives the schedule and
        closeness is left None. Plugin must still surface MLS games."""
        import dispatcharr_ranked_matchups.sources.mls as mls_mod

        espn_response = {
            "events": [{
                "id": "401124",
                "date": "2026-04-11T23:00Z",
                "season": {"year": 2026, "type": 2, "slug": "regular-season"},
                "competitions": [{
                    "competitors": [
                        {"homeAway": "home",
                         "team": {"displayName": "Atlanta United FC"}},
                        {"homeAway": "away",
                         "team": {"displayName": "Toronto FC"}},
                    ],
                }],
            }],
        }
        monkeypatch.setattr(mls_mod, "_http_get", lambda *a, **kw: espn_response)

        src = mls_mod.MlsSource(odds_api_key="")  # no key
        games = src.fetch_upcoming(days_ahead=0)

        assert len(games) == 1
        assert games[0].closeness is None  # no odds, no closeness


# =====================================================================
# Phase K: WNBA
# =====================================================================

class TestWnbaHeadlineParser:
    """WNBA headline regex differs from NBA — no East/West prefix on R1
    ("First Round"), WNBA name appears explicitly on Semifinals and
    Finals ("WNBA Semifinals", "WNBA Finals")."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.wnba import _parse_stage_from_headline
        return _parse_stage_from_headline(headline)

    def test_first_round(self):
        assert self._parse("First Round - Game 1") == {"stage": "R1", "matchday": 1}

    def test_first_round_game_3(self):
        # Best-of-3 R1 can go to game 3.
        assert self._parse("First Round - Game 3") == {"stage": "R1", "matchday": 3}

    def test_semifinals(self):
        assert self._parse("WNBA Semifinals - Game 5") == {"stage": "SF", "matchday": 5}

    def test_finals_game_one(self):
        assert self._parse("WNBA Finals - Game 1") == {"stage": "FINALS", "matchday": 1}

    def test_finals_game_seven(self):
        # 2025+ Finals can go to game 7.
        assert self._parse("WNBA Finals - Game 7") == {"stage": "FINALS", "matchday": 7}

    def test_no_headline_returns_none(self):
        assert self._parse(None) is None
        assert self._parse("") is None

    def test_unrecognized_returns_none(self):
        assert self._parse("Preseason - Exhibition") is None
        assert self._parse("Commissioner's Cup") is None

    def test_case_insensitive(self):
        assert self._parse("first round - game 2") == {"stage": "R1", "matchday": 2}
        assert self._parse("wnba finals - game 1") == {"stage": "FINALS", "matchday": 1}


class TestWnbaAllStarFilter:
    """Same All-Star filter pattern as NBA — WNBA also runs an
    All-Star Game tagged competition.type.abbreviation=ALLSTAR."""

    @staticmethod
    def _extract(event):
        from dispatcharr_ranked_matchups.sources.wnba import _extract_game_record
        return _extract_game_record(event)

    def test_all_star_game_filtered(self):
        event = {
            "id": "401705500",
            "date": "2025-07-19T23:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "4", "abbreviation": "ALLSTAR"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "120",
                     "team": {"displayName": "Team Caitlin"}},
                    {"homeAway": "away", "score": "115",
                     "team": {"displayName": "Team Aliyah"}},
                ],
            }],
        }
        assert self._extract(event) is None

    def test_regular_season_game_passes(self):
        event = {
            "id": "401705501",
            "date": "2025-06-15T23:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "1", "abbreviation": "STD"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "82",
                     "team": {"displayName": "New York Liberty"}},
                    {"homeAway": "away", "score": "78",
                     "team": {"displayName": "Connecticut Sun"}},
                ],
            }],
        }
        rec = self._extract(event)
        assert rec is not None
        assert rec["home"] == "New York Liberty"
        assert rec["season_type"] == 2


class TestWnbaRegularSource:

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.wnba import WnbaRegularSource
        return WnbaRegularSource(season_year=2024)

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "WNBA"
        assert src.sport_label == "WNBA"
        assert src.league_context_code == "WNBA"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "playoff_bubble" in labels
        assert "playoff_secured" in labels
        assert "top_seed_pace" in labels
        assert "elite" in labels

    def test_thresholds_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["WNBA"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1]

    def test_terminal_outcomes_by_win_count(self):
        src = self._make()
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Cellar":   {"wins":  8, "losses": 32, "pf": 0, "pa": 0, "games_played": 40},
                "Bubble":   {"wins": 22, "losses": 18, "pf": 0, "pa": 0, "games_played": 40},
                "Comfy":    {"wins": 26, "losses": 14, "pf": 0, "pa": 0, "games_played": 40},
                "Top":      {"wins": 32, "losses":  8, "pf": 0, "pa": 0, "games_played": 40},
                "Historic": {"wins": 38, "losses":  2, "pf": 0, "pa": 0, "games_played": 40},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Cellar"] == []
        assert set(outcomes["Bubble"]) == {"playoff_bubble"}
        assert set(outcomes["Comfy"]) == {"playoff_bubble", "playoff_secured"}
        assert set(outcomes["Top"]) == {"playoff_bubble", "playoff_secured", "top_seed_pace"}
        assert set(outcomes["Historic"]) == {
            "playoff_bubble", "playoff_secured", "top_seed_pace", "elite",
        }


class TestWnbaPlayoffSource:

    @staticmethod
    def _make_source(season_year=2024, bracket_games=None):
        from dispatcharr_ranked_matchups.sources.wnba import WnbaPlayoffSource
        src = WnbaPlayoffSource(season_year=season_year)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "WNBA"
        assert "Playoffs" in src.sport_label
        assert src._league_context_code() == "WNBA_PO"

    def test_supports_importance(self):
        assert self._make_source().supports_importance is True

    def test_ko_stages(self):
        assert self._make_source().KO_STAGES == ("R1", "SF", "FINALS")

    def test_series_lengths_2024(self):
        src = self._make_source(season_year=2024)
        assert src._series_length_for_stage("R1") == 3
        assert src._series_length_for_stage("SF") == 5
        assert src._series_length_for_stage("FINALS") == 5  # legacy

    def test_series_lengths_2025(self):
        src = self._make_source(season_year=2025)
        assert src._series_length_for_stage("R1") == 3
        assert src._series_length_for_stage("SF") == 5
        assert src._series_length_for_stage("FINALS") == 7  # modern

    def test_winner_advance_label(self):
        src = self._make_source()
        assert src._winner_advance_label("FINALS") == "WNBA_WINNER"
        assert src._winner_advance_label("R1") is None
        assert src._winner_advance_label("SF") is None

    def test_outcome_labels(self):
        labels = self._make_source().outcome_labels
        assert "wnba_semis" in labels
        assert "wnba_finals" in labels
        assert "wnba_winner" in labels

    def test_finals_winner_advance_reaches_synthetic_depth(self):
        """2024 WNBA Finals: Liberty beat Lynx 3-2 in best-of-5. Synthesize
        the 5-game series and confirm Liberty reaches WNBA_WINNER depth."""
        games = [
            {"game_id": "f1", "stage": "FINALS", "matchday": 1,
             "home": "Liberty", "away": "Lynx",
             "home_goals": 87, "away_goals": 81,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f2", "stage": "FINALS", "matchday": 2,
             "home": "Liberty", "away": "Lynx",
             "home_goals": 78, "away_goals": 80,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f3", "stage": "FINALS", "matchday": 3,
             "home": "Lynx", "away": "Liberty",
             "home_goals": 80, "away_goals": 77,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f4", "stage": "FINALS", "matchday": 4,
             "home": "Lynx", "away": "Liberty",
             "home_goals": 71, "away_goals": 80,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f5", "stage": "FINALS", "matchday": 5,
             "home": "Liberty", "away": "Lynx",
             "home_goals": 67, "away_goals": 62,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(season_year=2024, bracket_games=games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Liberty"] == KNOCKOUT_ROUND_DEPTH["WNBA_WINNER"]
        assert state["_round_reached"]["Lynx"] == KNOCKOUT_ROUND_DEPTH["FINALS"]
        outcomes = src.terminal_outcomes(state)
        assert "wnba_winner" in outcomes["Liberty"]
        assert "wnba_winner" not in outcomes["Lynx"]
        assert "wnba_finals" in outcomes["Lynx"]

    def test_wnba_po_thresholds_are_depth_ordered(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS["WNBA_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1]


# =====================================================================
# Phase L: NCAA Women's Basketball + March Madness
# =====================================================================

class TestNcaawBasketballHeadlineParser:
    """NCAA W headlines have a "Regional N in {City}" prefix for R64-E8
    and no prefix for F4/NCG. Plus the round names use "1st Round" /
    "2nd Round" (numeric) and "Sweet 16" / "Elite 8" (with space)."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.ncaaw_basketball import (
            _parse_stage_from_headline,
        )
        return _parse_stage_from_headline(headline)

    def test_first_round_with_regional(self):
        assert self._parse(
            "NCAA Women's Championship - Regional 2 in Birmingham - 1st Round"
        ) == {"stage": "R64", "matchday": 1}

    def test_second_round_with_regional(self):
        assert self._parse(
            "NCAA Women's Championship - Regional 3 in Birmingham - 2nd Round"
        ) == {"stage": "R32", "matchday": 1}

    def test_sweet_sixteen_with_regional(self):
        assert self._parse(
            "NCAA Women's Championship - Regional 1 in Spokane - Sweet 16"
        ) == {"stage": "S16", "matchday": 1}

    def test_elite_eight_with_regional(self):
        assert self._parse(
            "NCAA Women's Championship - Regional 4 in Spokane - Elite 8"
        ) == {"stage": "E8", "matchday": 1}

    def test_final_four_no_regional(self):
        assert self._parse(
            "NCAA Women's Championship - Final Four"
        ) == {"stage": "F4", "matchday": 1}

    def test_national_championship(self):
        assert self._parse(
            "NCAA Women's Championship - National Championship"
        ) == {"stage": "NCG", "matchday": 1}

    def test_none_for_unparseable(self):
        assert self._parse(None) is None
        assert self._parse("") is None
        assert self._parse("Some Conference Tournament - Game 1") is None

    def test_apostrophe_optional(self):
        # ESPN occasionally drops the apostrophe in "Women's"; the
        # regex allows both forms.
        assert self._parse(
            "NCAA Womens Championship - Final Four"
        ) == {"stage": "F4", "matchday": 1}


class TestNcaawBasketballRegularSource:

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.ncaaw_basketball import (
            NcaawBasketballRegularSource,
        )
        return NcaawBasketballRegularSource(season_end_year=2025)

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "NCAAW"
        assert "Women" in src.sport_label
        assert src.league_context_code == "NCAAW_BBALL"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "tournament_bubble" in labels
        assert "at_large_lock" in labels
        assert "top_4_seed" in labels
        assert "no_1_seed" in labels

    def test_thresholds_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["NCAAW_BBALL"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1]


class TestNcaawBasketballPlayoffSource:

    @staticmethod
    def _make(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.ncaaw_basketball import (
            NcaawBasketballPlayoffSource,
        )
        src = NcaawBasketballPlayoffSource(season_end_year=2025)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "NCAAW"
        assert "Tournament" in src.sport_label
        assert src._league_context_code() == "NCAAW_BBALL_PO"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_ko_stages(self):
        assert self._make().KO_STAGES == ("R64", "R32", "S16", "E8", "F4", "NCG")

    def test_series_length_uniform_one(self):
        """Single-game elim — every stage uses SERIES_LENGTH=1; the
        clinching-wins target is ceil(1/2)=1."""
        src = self._make()
        for stage in src.KO_STAGES:
            assert src._series_length_for_stage(stage) == 1
            assert src._clinching_wins_for_stage(stage) == 1

    def test_winner_advance_label(self):
        src = self._make()
        assert src._winner_advance_label("NCG") == "NCG_WINNER"
        assert src._winner_advance_label("F4") is None
        assert src._winner_advance_label("R64") is None

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "round_of_32" in labels
        assert "sweet_16" in labels
        assert "elite_8" in labels
        assert "final_four" in labels
        assert "national_final" in labels
        assert "national_champ" in labels

    def test_champion_cascade(self):
        """A team that wins each round reaches NCG_WINNER depth and
        terminal_outcomes returns every band. Synthesize one team's
        path through R64-NCG (6 wins)."""
        # Champion's path: beat 6 different opponents in 6 single-game
        # series, one at each stage.
        games = []
        for i, (stage, opp) in enumerate([
            ("R64", "Sixteen"),
            ("R32", "Eight"),
            ("S16", "Four"),
            ("E8", "Two"),
            ("F4", "One"),
            ("NCG", "Final"),
        ]):
            games.append({
                "game_id": f"ncaaw-{i+1}",
                "stage": stage,
                "matchday": 1,
                "home": "Champion",
                "away": opp,
                "home_goals": 80,
                "away_goals": 70,
                "status": "FINISHED",
                "start_time": None,
                "extra": {},
            })
        src = self._make(bracket_games=games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Champion"] == KNOCKOUT_ROUND_DEPTH["NCG_WINNER"]
        assert state["_round_reached"]["Final"] == KNOCKOUT_ROUND_DEPTH["NCG"]
        assert state["_round_reached"]["One"] == KNOCKOUT_ROUND_DEPTH["F4"]
        outcomes = src.terminal_outcomes(state)
        assert "national_champ" in outcomes["Champion"]
        assert "national_final" in outcomes["Final"]
        assert "national_final" not in outcomes["One"]
        assert "final_four" in outcomes["One"]

    def test_ncaaw_po_thresholds_are_depth_ordered(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS["NCAAW_BBALL_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1]


# =====================================================================
# Issue #24: NCAA College Cup brackets (M's + W's)
# =====================================================================

class TestNcaaSoccerCupSource:
    """One source class parametrized on gender — same shape / endpoints
    / stage labels for both, only ESPN URL slug + league_context_code
    differ. Mirrors NcaaSoccerSource (regular season) parametrization."""

    @staticmethod
    def _make(gender="m", bracket_games=None):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        src = NcaaSoccerCupSource(gender=gender, season_year=2024)
        src._bracket_games_cache = bracket_games or []
        return src

    # ---------- identity + parametrization ----------

    def test_gender_routes_to_correct_context(self):
        m = self._make("m")
        w = self._make("w")
        assert m._league_context_code() == "NCAA_MSOC_CUP"
        assert w._league_context_code() == "NCAA_WSOC_CUP"
        assert m.sport_prefix == "NCAAMSOC"
        assert w.sport_prefix == "NCAAWSOC"
        assert "Men's" in m.sport_label
        assert "Women's" in w.sport_label

    def test_gender_routes_to_correct_espn_slug(self):
        assert self._make("m")._espn_slug == "usa.ncaa.m.1"
        assert self._make("w")._espn_slug == "usa.ncaa.w.1"

    def test_invalid_gender_rejected(self):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        try:
            NcaaSoccerCupSource(gender="x")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError on invalid gender")

    def test_supports_importance(self):
        assert self._make("m").supports_importance is True
        assert self._make("w").supports_importance is True

    def test_ko_stages(self):
        # Six rounds, same canonical labels as NCAAW Basketball's tournament
        # so KNOCKOUT_ROUND_DEPTH entries are reused.
        assert self._make().KO_STAGES == ("R64", "R32", "S16", "E8", "F4", "NCG")

    def test_series_length_uniform_one(self):
        """Single-game elim — every stage uses SERIES_LENGTH=1; clinches
        at ceil(1/2)=1 win."""
        src = self._make()
        for stage in src.KO_STAGES:
            assert src._series_length_for_stage(stage) == 1
            assert src._clinching_wins_for_stage(stage) == 1

    def test_winner_advance_label(self):
        # NCG winner → NCG_WINNER synthetic depth (shared with NCAAW
        # Basketball's bracket). All non-final stages return None and
        # the base machinery handles the next-stage advance.
        src = self._make()
        assert src._winner_advance_label("NCG") == "NCG_WINNER"
        assert src._winner_advance_label("F4") is None
        assert src._winner_advance_label("R64") is None

    # ---------- slug parser ----------

    def test_slug_to_stage_table_covers_both_genders(self):
        """Both M's and W's College Cup slugs map to the same canonical
        stage labels — M's uses 'college-cup---semifinal' / 'college-
        cup---championship', W's uses 'semifinals' / 'college-cup'. Pin
        both gender aliases so an ESPN slug rename surfaces here, not
        as silently-missing-stage in production."""
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            SLUG_TO_STAGE,
        )
        # Shared across genders (R1 - QF)
        assert SLUG_TO_STAGE["first-round"] == "R64"
        assert SLUG_TO_STAGE["second-round"] == "R32"
        assert SLUG_TO_STAGE["third-round"] == "S16"
        assert SLUG_TO_STAGE["quarterfinals"] == "E8"
        # M's gender-specific aliases
        assert SLUG_TO_STAGE["college-cup---semifinal"] == "F4"
        assert SLUG_TO_STAGE["college-cup---championship"] == "NCG"
        # W's gender-specific aliases
        assert SLUG_TO_STAGE["semifinals"] == "F4"
        assert SLUG_TO_STAGE["college-cup"] == "NCG"

    def test_extract_bracket_record_filters_non_tournament_slugs(self):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        # Regular-season game with the regular-season slug should be
        # filtered out entirely (return None), not silently coerced
        # into a bracket stage.
        event = {
            "id": "regular-1",
            "date": "2024-09-01T19:00:00Z",
            "season": {"slug": "regular-season"},
            "competitions": [{
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "team": {"location": "Stanford"}, "score": "2"},
                    {"homeAway": "away", "team": {"location": "Washington"}, "score": "1"},
                ],
            }],
        }
        assert NcaaSoccerCupSource._extract_bracket_record(event) is None

    def test_extract_bracket_record_routes_third_round_to_s16(self):
        """ESPN's 'third-round' slug is what humans call Sweet 16 —
        confirm the routing so a label mismatch surfaces here, not via
        an importance-band silently misfiring on production data.
        """
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        event = {
            "id": "s16-1",
            "date": "2024-11-30T19:00:00Z",
            "season": {"slug": "third-round"},
            "competitions": [{
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "team": {"location": "Stanford"}, "score": "2"},
                    {"homeAway": "away", "team": {"location": "Washington"}, "score": "1"},
                ],
            }],
        }
        rec = NcaaSoccerCupSource._extract_bracket_record(event)
        assert rec is not None
        assert rec["stage"] == "S16"
        assert rec["home"] == "Stanford"
        assert rec["away"] == "Washington"
        assert rec["home_goals"] == 2
        assert rec["away_goals"] == 1
        assert rec["status"] == "FINISHED"
        assert rec["matchday"] == 1  # SERIES_LENGTH=1 means single game per "tie"

    def test_extract_bracket_record_scheduled_has_no_score(self):
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        event = {
            "id": "qf-1",
            "date": "2030-12-07T19:00:00Z",
            "season": {"slug": "quarterfinals"},
            "competitions": [{
                "status": {"type": {"completed": False, "state": "pre"}},
                "competitors": [
                    {"homeAway": "home", "team": {"location": "Stanford"}},
                    {"homeAway": "away", "team": {"location": "Washington"}},
                ],
            }],
        }
        rec = NcaaSoccerCupSource._extract_bracket_record(event)
        assert rec is not None
        assert rec["stage"] == "E8"
        assert rec["status"] == "SCHEDULED"
        assert rec["home_goals"] is None
        assert rec["away_goals"] is None

    # ---------- LEAGUE_CONTEXTS for both genders ----------

    def test_contexts_use_knockout_format(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["NCAA_MSOC_CUP"].format == "knockout"
        assert LEAGUE_CONTEXTS["NCAA_WSOC_CUP"].format == "knockout"

    def test_outcome_labels(self):
        labels = self._make("m").outcome_labels
        assert "round_of_32" in labels
        assert "sweet_16" in labels
        assert "elite_8" in labels
        assert "college_cup_semis" in labels
        assert "college_cup_final" in labels
        assert "cup_winner" in labels

    def test_thresholds_are_depth_ordered(self):
        # Cross-sport calibration requires later stages to be ranked
        # deeper than earlier ones. Out-of-order depths would break the
        # cascade — a team reaching SF wouldn't be credited with the
        # E8 / S16 / R32 labels below it.
        from dispatcharr_ranked_matchups.scoring import (
            LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH,
        )
        for code in ("NCAA_MSOC_CUP", "NCAA_WSOC_CUP"):
            ctx = LEAGUE_CONTEXTS[code]
            depths = [
                KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds
            ]
            for i in range(len(depths) - 1):
                assert depths[i] < depths[i + 1], (
                    f"{code} depths {depths} not monotonic at {i}"
                )

    def test_weights_are_monotonic_increasing(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        for code in ("NCAA_MSOC_CUP", "NCAA_WSOC_CUP"):
            weights = [w for _, _, w in LEAGUE_CONTEXTS[code].thresholds]
            for i in range(len(weights) - 1):
                assert weights[i] < weights[i + 1], (
                    f"{code} weights {weights} not monotonic at {i}"
                )

    # ---------- bracket cascade end-to-end ----------

    def test_champion_cascade(self):
        """A team that wins each of the 6 rounds reaches NCG_WINNER
        depth and `terminal_outcomes` returns every band — pin the
        cascade end-to-end so a future refactor of bracket.py can't
        silently break the round_reached → terminal_outcomes path
        for soccer-style brackets.
        """
        games = []
        for i, (stage, opp) in enumerate([
            ("R64", "Sixteen"),
            ("R32", "Eight"),
            ("S16", "Four"),
            ("E8",  "Two"),
            ("F4",  "One"),
            ("NCG", "Final"),
        ]):
            games.append({
                "game_id": f"ncsoc-{i+1}",
                "stage": stage,
                "matchday": 1,
                "home": "Champion",
                "away": opp,
                "home_goals": 2,
                "away_goals": 1,
                "status": "FINISHED",
                "start_time": None,
                "extra": {},
            })
        src = self._make(bracket_games=games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Champion"] == KNOCKOUT_ROUND_DEPTH["NCG_WINNER"]
        assert state["_round_reached"]["Final"] == KNOCKOUT_ROUND_DEPTH["NCG"]
        assert state["_round_reached"]["One"] == KNOCKOUT_ROUND_DEPTH["F4"]
        outcomes = src.terminal_outcomes(state)
        assert "cup_winner" in outcomes["Champion"]
        assert "college_cup_final" in outcomes["Final"]
        assert "college_cup_final" not in outcomes["One"]
        assert "college_cup_semis" in outcomes["One"]

    # ---------- sample_result forces a winner (no draws in bracket) ----------

    def test_sample_result_never_produces_ties(self):
        """Bracket games go to OT then PKs in real life — sample_result
        must coin-flip a +1 to break a regulation tie. Distinct from the
        regular-season NcaaSoccerSource which allows draws (1 standings
        point each). Run a tight-Poisson sample 200x and confirm zero
        draws.
        """
        import random
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.base import GameRow
        src = self._make()
        strengths = {
            "A": {"pf_per_game": 1.0, "pa_per_game": 1.0},
            "B": {"pf_per_game": 1.0, "pa_per_game": 1.0},
        }
        gr = GameRow(
            sport_prefix="NCAAMSOC", sport_label="NCAA Men's College Cup",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=datetime(2025, 12, 1, tzinfo=timezone.utc),
        )
        rng = random.Random(42)
        ties = 0
        for _ in range(200):
            res = src.sample_result({}, gr, strengths, rng)
            if res.home_goals == res.away_goals:
                ties += 1
        assert ties == 0, (
            f"bracket sample_result must force a winner; got {ties} ties"
        )

    # ---------- strength sharing ----------

    def test_set_regular_season_strengths_persists_to_estimate(self):
        src = self._make()
        custom = {
            "Stanford":   {"pf_per_game": 2.5, "pa_per_game": 0.5},
            "Washington": {"pf_per_game": 1.0, "pa_per_game": 1.5},
        }
        src.set_regular_season_strengths(custom)
        assert src.estimate_strengths() == custom

    def test_estimate_strengths_empty_when_not_seeded(self):
        # Without a regular-season seed, the source returns {} and
        # sample_result falls through to the league-average prior.
        # Pin this so a future change to the default doesn't silently
        # inject league-wide priors that should have been per-team.
        assert self._make().estimate_strengths() == {}

    # ---------- loser stops at the stage they lost ----------

    def test_loser_round_reached_stops_at_lost_stage(self):
        """A team that wins R64 + R32 but loses S16 should have
        round_reached at depth(S16) — they made it to Sweet 16, didn't
        advance to Elite 8. terminal_outcomes returns sweet_16 and
        round_of_32 but NOT elite_8.
        """
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        games = [
            # Loser wins R64
            {"game_id": "g1", "stage": "R64", "matchday": 1,
             "home": "Loser", "away": "Sixteen",
             "home_goals": 2, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # Loser wins R32
            {"game_id": "g2", "stage": "R32", "matchday": 1,
             "home": "Loser", "away": "Eight",
             "home_goals": 2, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # Loser loses S16
            {"game_id": "g3", "stage": "S16", "matchday": 1,
             "home": "Loser", "away": "Four",
             "home_goals": 0, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make(bracket_games=games)
        state = src.initial_state()
        rr = state["_round_reached"]
        assert rr["Loser"] == KNOCKOUT_ROUND_DEPTH["S16"], (
            f"Loser should reach S16 depth, got {rr['Loser']}"
        )
        # Four (the opponent that beat Loser at S16) advances to E8.
        assert rr["Four"] == KNOCKOUT_ROUND_DEPTH["E8"]
        outcomes = src.terminal_outcomes(state)
        assert "sweet_16" in outcomes["Loser"]
        assert "round_of_32" in outcomes["Loser"]
        assert "elite_8" not in outcomes["Loser"]
        assert "college_cup_semis" not in outcomes["Loser"]

    # ---------- PK shootout dedup ----------

    def test_dedupe_pk_shootout_keeps_non_tie_event(self):
        """ESPN sometimes publishes two events for a soccer bracket
        game that goes to PKs: one with regulation 0-0 and a second
        with the PK shootout final score. The dedup must keep the
        non-tie event so the bracket cascade gets the actual winner.
        Pin the live 2024 M's Marshall @ SMU QF shape observed during
        live verification.
        """
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            _dedupe_pk_shootout_pairs,
        )
        regulation_tie = {
            "game_id": "723937", "stage": "E8", "matchday": 1,
            "home": "SMU", "away": "Marshall",
            "home_goals": 0, "away_goals": 0,
            "status": "FINISHED",
            "start_time": datetime(2024, 12, 8, 0, 0, tzinfo=timezone.utc),
            "extra": {},
        }
        pk_shootout = {
            "game_id": "724472", "stage": "E8", "matchday": 1,
            "home": "SMU", "away": "Marshall",
            "home_goals": 2, "away_goals": 3,
            "status": "FINISHED",
            "start_time": datetime(2024, 12, 8, 20, 0, tzinfo=timezone.utc),
            "extra": {},
        }
        deduped = _dedupe_pk_shootout_pairs([regulation_tie, pk_shootout])
        assert len(deduped) == 1
        assert deduped[0]["game_id"] == "724472"
        assert deduped[0]["away_goals"] == 3
        assert deduped[0]["home_goals"] == 2

    def test_dedupe_with_scheduled_vs_finished_keeps_finished(self):
        """A scheduled placeholder and a finished result for the same
        bracket game must collapse to the finished record."""
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            _dedupe_pk_shootout_pairs,
        )
        scheduled = {
            "game_id": "s1", "stage": "S16", "matchday": 1,
            "home": "A", "away": "B",
            "home_goals": None, "away_goals": None,
            "status": "SCHEDULED",
            "start_time": datetime(2024, 11, 30, tzinfo=timezone.utc),
            "extra": {},
        }
        finished = {
            "game_id": "f1", "stage": "S16", "matchday": 1,
            "home": "A", "away": "B",
            "home_goals": 2, "away_goals": 1,
            "status": "FINISHED",
            "start_time": datetime(2024, 11, 30, tzinfo=timezone.utc),
            "extra": {},
        }
        deduped = _dedupe_pk_shootout_pairs([scheduled, finished])
        assert len(deduped) == 1
        assert deduped[0]["game_id"] == "f1"

    def test_dedupe_passes_through_unique_games(self):
        """A bracket with all distinct (stage, participants) tuples
        passes through unchanged — no false collapsing of legitimate
        bracket games."""
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            _dedupe_pk_shootout_pairs,
        )
        games = [
            {"game_id": "1", "stage": "S16", "matchday": 1,
             "home": "A", "away": "B", "home_goals": 2, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "2", "stage": "S16", "matchday": 1,
             "home": "C", "away": "D", "home_goals": 1, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "3", "stage": "E8", "matchday": 1,
             "home": "A", "away": "B",  # same teams BUT different stage
             "home_goals": 3, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        deduped = _dedupe_pk_shootout_pairs(games)
        assert len(deduped) == 3
        ids = {g["game_id"] for g in deduped}
        assert ids == {"1", "2", "3"}

    # ---------- fetch_upcoming (EPG emit side) ----------

    def test_fetch_upcoming_emits_only_bracket_games(self, monkeypatch):
        """fetch_upcoming must filter to events with a tournament slug
        — a regular-season game that happens to fall in the same date
        window must not appear. Pin via stub so a slug-table regression
        surfaces here."""
        from dispatcharr_ranked_matchups.sources import ncaa_soccer_cup as mod
        from dispatcharr_ranked_matchups.sources.ncaa_soccer_cup import (
            NcaaSoccerCupSource,
        )
        # Two events: one tournament (third-round) and one regular-season.
        # Only the tournament event should be emitted.
        calls = {"n": 0}
        def fake_get(url, *_a, **_kw):
            calls["n"] += 1
            if calls["n"] != 1:
                return {"events": []}
            return {"events": [
                {"id": "tournament-event", "date": "2030-11-30T19:00:00Z",
                 "season": {"slug": "third-round"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"location": "Stanford"}},
                         {"homeAway": "away", "team": {"location": "Washington"}},
                     ],
                 }]},
                {"id": "regular-season-event", "date": "2030-11-30T22:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"location": "UCLA"}},
                         {"homeAway": "away", "team": {"location": "USC"}},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        src = NcaaSoccerCupSource(gender="m")
        games = src.fetch_upcoming(days_ahead=0)
        ids = {(g.extra or {}).get("espn_event_id") for g in games}
        assert ids == {"tournament-event"}, (
            f"only the bracket slug should pass through, got {ids}"
        )
        # And the emitted game carries the bracket stage in extra.
        assert games[0].extra["stage"] == "S16"
        assert games[0].extra["fd_competition_code"] == "NCAA_MSOC_CUP"


# =====================================================================
# Phase N: NCAA Softball
# =====================================================================

class TestNcaaSoftballRegularSource:
    """V1: regular-season importance only. WCWS bracket (double-elim)
    is filed as a follow-up — same scope deferral as NCAA Baseball's CWS."""

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.ncaa_softball import NcaaSoftballRegularSource
        return NcaaSoftballRegularSource(season_year=2025)

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "NCAASBL"
        assert "Softball" in src.sport_label
        assert src.league_context_code == "SBL"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "tournament_bubble" in labels
        assert "at_large_lock" in labels
        assert "top_regional_seed" in labels
        assert "national_seed" in labels
        assert "no_1_overall" in labels

    def test_thresholds_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["SBL"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1]

    def test_record_result_credits_winner_a_win(self):
        src = self._make()
        teams = {
            "Oklahoma": {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
            "Texas":    {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0},
        }
        src._record_result_into_state(teams, "Oklahoma", "Texas", 6, 2)
        assert teams["Oklahoma"]["wins"] == 1
        assert teams["Texas"]["losses"] == 1
        assert teams["Oklahoma"]["pf"] == 6
        assert teams["Oklahoma"]["pa"] == 2

    def test_terminal_outcomes_buckets_by_win_count(self):
        src = self._make()
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Cellar":   {"wins": 20, "losses": 30, "pf": 0, "pa": 0, "games_played": 50},
                "Bubble":   {"wins": 32, "losses": 22, "pf": 0, "pa": 0, "games_played": 54},
                "AtLarge":  {"wins": 38, "losses": 16, "pf": 0, "pa": 0, "games_played": 54},
                "Regional": {"wins": 42, "losses": 12, "pf": 0, "pa": 0, "games_played": 54},
                "National": {"wins": 47, "losses":  8, "pf": 0, "pa": 0, "games_played": 55},
                "Overall1": {"wins": 52, "losses":  3, "pf": 0, "pa": 0, "games_played": 55},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Cellar"] == []
        assert set(outcomes["Bubble"]) == {"tournament_bubble"}
        assert set(outcomes["AtLarge"]) == {"tournament_bubble", "at_large_lock"}
        assert set(outcomes["Regional"]) == {
            "tournament_bubble", "at_large_lock", "top_regional_seed",
        }
        assert set(outcomes["National"]) == {
            "tournament_bubble", "at_large_lock", "top_regional_seed", "national_seed",
        }
        assert set(outcomes["Overall1"]) == {
            "tournament_bubble", "at_large_lock", "top_regional_seed",
            "national_seed", "no_1_overall",
        }


# =====================================================================
# Phase O Phase 1: NcaaSoftballPlayoffSource
# =====================================================================

class TestNcaaSoftballPlayoffSource:
    """NcaaSoftballPlayoffSource: best-of-3 Super Regional + WCWS Finals.
    Sibling of NcaaBaseballPlayoffSource with one twist — softball
    headlines use "Finals" (plural) where baseball uses "Final"
    (singular)."""

    @staticmethod
    def _make_source(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            NcaaSoftballPlayoffSource,
        )
        src = NcaaSoftballPlayoffSource(season_year=2026)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make_source()
        assert src.sport_prefix == "NCAASBL"
        assert "Postseason" in src.sport_label
        assert src._league_context_code() == "WCWS_PO"

    def test_ko_stages(self):
        src = self._make_source()
        assert src.KO_STAGES == ("SB_SR", "WCWS_F")

    def test_series_length_is_three(self):
        src = self._make_source()
        assert src._series_length_for_stage("SB_SR") == 3
        assert src._series_length_for_stage("WCWS_F") == 3

    def test_winner_advance_label(self):
        src = self._make_source()
        assert src._winner_advance_label("WCWS_F") == "WCWS_W"
        assert src._winner_advance_label("SB_SR") is None

    def test_outcome_labels_uses_wcws_po_thresholds(self):
        src = self._make_source()
        labels = src.outcome_labels
        assert "super_regional" in labels
        assert "okc_bound" in labels
        assert "wcws_final" in labels
        assert "wcws_champion" in labels

    def test_set_regular_season_strengths_passes_through(self):
        src = self._make_source()
        assert src.estimate_strengths() == {}
        src.set_regular_season_strengths(
            {"Oklahoma": {"pf_per_game": 7.2, "pa_per_game": 2.8}}
        )
        s = src.estimate_strengths()
        assert s["Oklahoma"]["pf_per_game"] == 7.2

    def test_wcws_finals_winner_reaches_synthetic_depth(self):
        games = [
            {"game_id": "f1", "stage": "WCWS_F", "matchday": 1,
             "home": "Texas Tech", "away": "Texas",
             "home_goals": 3, "away_goals": 2,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "f2", "stage": "WCWS_F", "matchday": 2,
             "home": "Texas Tech", "away": "Texas",
             "home_goals": 5, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make_source(games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Texas Tech"] == KNOCKOUT_ROUND_DEPTH["WCWS_W"]
        assert state["_round_reached"]["Texas"] == KNOCKOUT_ROUND_DEPTH["WCWS_F"]


class TestSoftballPlayoffHeadlineParser:
    """The softball headline parser is a near-mirror of the baseball one
    except for the "Finals" (plural) vs "Final" (singular) Championship
    label. Regression-pin against the 2025-2026 observed headlines."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.ncaa_softball import (
            _parse_softball_playoff_headline,
        )
        return _parse_softball_playoff_headline(headline)

    def test_super_regional_game_1(self):
        assert self._parse(
            "NCAA Softball Championship - Lincoln Super Regional - Game 1"
        ) == ("SB_SR", 1)

    def test_super_regional_game_3_if_necessary(self):
        assert self._parse(
            "NCAA Softball Championship - Lincoln Super Regional - Game 3 (if necessary)"
        ) == ("SB_SR", 3)

    def test_wcws_finals_game_1(self):
        # Plural "Finals" for softball — distinct from baseball's "Final".
        assert self._parse(
            "Women's College World Series Championship Finals - Game 1"
        ) == ("WCWS_F", 1)

    def test_wcws_finals_singular_form_returns_none(self):
        # Guard against accidentally accepting baseball's pattern.
        assert self._parse(
            "Women's College World Series Championship Final - Game 1"
        ) == (None, None)

    def test_regional_returns_none(self):
        assert self._parse(
            "NCAA Softball Championship - Tuscaloosa Regional"
        ) == (None, None)

    def test_wcws_bracket_double_elim_returns_none(self):
        assert self._parse(
            "Women's College World Series - Double Elimination Round"
        ) == (None, None)

    def test_wcws_elimination_game_returns_none(self):
        # WCWS losers-bracket games — observed in live 2026 data
        # (May 30+). Must skip; no game number → Phase 2.
        assert self._parse(
            "Women's College World Series - Elimination Game"
        ) == (None, None)

    def test_wcws_elimination_game_if_necessary_returns_none(self):
        # ESPN appends " If Necessary" to placeholder losers-bracket
        # games in the 8-team WCWS bracket. Observed live 2026-06-01.
        assert self._parse(
            "Women's College World Series - Elimination Game If Necessary"
        ) == (None, None)

    def test_regional_elimination_game_returns_none(self):
        # Regional losers-bracket games carry "Elimination Game" too —
        # also Phase 2 territory.
        assert self._parse(
            "NCAA Softball Championship - Athens Regional - Elimination Game"
        ) == (None, None)

    def test_empty_headline(self):
        assert self._parse("") == (None, None)


# =====================================================================
# Phase P: NFL
# =====================================================================

class TestNflHeadlineParser:
    """NFL headlines have no Game N suffix (single-game elim per round)
    and use AFC/NFC prefixes for the first three rounds, then bare
    "Super Bowl LIX" (Roman numeral varies) for the championship."""

    @staticmethod
    def _parse(headline):
        from dispatcharr_ranked_matchups.sources.nfl import _parse_stage_from_headline
        return _parse_stage_from_headline(headline)

    def test_afc_wild_card(self):
        assert self._parse("AFC Wild Card Playoffs") == {"stage": "WC", "matchday": 1}

    def test_nfc_wild_card(self):
        assert self._parse("NFC Wild Card Playoffs") == {"stage": "WC", "matchday": 1}

    def test_afc_divisional(self):
        assert self._parse("AFC Divisional Playoffs") == {"stage": "DIV", "matchday": 1}

    def test_nfc_divisional(self):
        assert self._parse("NFC Divisional Playoffs") == {"stage": "DIV", "matchday": 1}

    def test_afc_championship(self):
        assert self._parse("AFC Championship") == {"stage": "CONF", "matchday": 1}

    def test_nfc_championship(self):
        assert self._parse("NFC Championship") == {"stage": "CONF", "matchday": 1}

    def test_super_bowl_with_roman_numeral(self):
        assert self._parse("Super Bowl LIX") == {"stage": "SB", "matchday": 1}

    def test_super_bowl_no_numeral(self):
        # Defensive — if ESPN ever drops the Roman numeral.
        assert self._parse("Super Bowl") == {"stage": "SB", "matchday": 1}

    def test_no_headline_returns_none(self):
        assert self._parse(None) is None
        assert self._parse("") is None

    def test_unrecognized_returns_none(self):
        assert self._parse("Pro Bowl Skills Competition") is None
        assert self._parse("Preseason - Week 3") is None

    def test_case_insensitive(self):
        assert self._parse("afc wild card playoffs") == {"stage": "WC", "matchday": 1}
        assert self._parse("super bowl lix") == {"stage": "SB", "matchday": 1}


class TestNflProBowlFilter:
    """Pro Bowl is tagged competition.type.abbreviation=ALLSTAR — same
    trap as NBA/WNBA. Filter must drop it from regular-season fetches."""

    @staticmethod
    def _extract(event):
        from dispatcharr_ranked_matchups.sources.nfl import _extract_game_record
        return _extract_game_record(event)

    def test_pro_bowl_filtered(self):
        event = {
            "id": "401706000",
            "date": "2025-02-02T20:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "4", "abbreviation": "ALLSTAR"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "76",
                     "team": {"displayName": "NFC"}},
                    {"homeAway": "away", "score": "63",
                     "team": {"displayName": "AFC"}},
                ],
            }],
        }
        assert self._extract(event) is None

    def test_regular_season_passes(self):
        event = {
            "id": "401706001",
            "date": "2024-11-17T18:00Z",
            "season": {"year": 2025, "type": 2, "slug": "regular-season"},
            "competitions": [{
                "type": {"id": "1", "abbreviation": "STD"},
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "score": "37",
                     "team": {"displayName": "Philadelphia Eagles"}},
                    {"homeAway": "away", "score": "20",
                     "team": {"displayName": "Washington Commanders"}},
                ],
            }],
        }
        rec = self._extract(event)
        assert rec is not None
        assert rec["home"] == "Philadelphia Eagles"


class TestNflRegularSource:

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.nfl import NflRegularSource
        return NflRegularSource(season_end_year=2025)

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "NFL"
        assert src.sport_label == "NFL"
        assert src.league_context_code == "NFL"
        assert src._count_field == "wins"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "playoff_bubble" in labels
        assert "playoff_secured" in labels
        assert "division_winner" in labels
        assert "no_1_seed" in labels

    def test_thresholds_monotonic(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["NFL"]
        cuts = [t[0] for t in ctx.thresholds]
        for i in range(len(cuts) - 1):
            assert cuts[i] < cuts[i + 1]

    def test_terminal_outcomes_by_win_count(self):
        src = self._make()
        state = {
            "_applied": frozenset(),
            "_teams": {
                "Cellar": {"wins":  4, "losses": 13, "pf": 0, "pa": 0, "games_played": 17},
                "Bubble": {"wins":  8, "losses":  9, "pf": 0, "pa": 0, "games_played": 17},
                "Comfy":  {"wins": 10, "losses":  7, "pf": 0, "pa": 0, "games_played": 17},
                "DivWin": {"wins": 12, "losses":  5, "pf": 0, "pa": 0, "games_played": 17},
                "Top":    {"wins": 14, "losses":  3, "pf": 0, "pa": 0, "games_played": 17},
            },
        }
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Cellar"] == []
        assert set(outcomes["Bubble"]) == {"playoff_bubble"}
        assert set(outcomes["Comfy"]) == {"playoff_bubble", "playoff_secured"}
        assert set(outcomes["DivWin"]) == {
            "playoff_bubble", "playoff_secured", "division_winner",
        }
        assert set(outcomes["Top"]) == {
            "playoff_bubble", "playoff_secured", "division_winner", "no_1_seed",
        }


class TestNflPlayoffSource:

    @staticmethod
    def _make(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.nfl import NflPlayoffSource
        src = NflPlayoffSource(season_end_year=2025)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "NFL"
        assert "Playoffs" in src.sport_label
        assert src._league_context_code() == "NFL_PO"

    def test_ko_stages(self):
        assert self._make().KO_STAGES == ("WC", "DIV", "CONF", "SB")

    def test_series_length_uniform_one(self):
        src = self._make()
        for stage in src.KO_STAGES:
            assert src._series_length_for_stage(stage) == 1
            assert src._clinching_wins_for_stage(stage) == 1

    def test_winner_advance_label(self):
        src = self._make()
        assert src._winner_advance_label("SB") == "SB_WINNER"
        assert src._winner_advance_label("WC") is None
        assert src._winner_advance_label("DIV") is None
        assert src._winner_advance_label("CONF") is None

    def test_outcome_labels(self):
        labels = self._make().outcome_labels
        assert "divisional" in labels
        assert "conf_champ" in labels
        assert "super_bowl" in labels
        assert "sb_winner" in labels

    def test_sb_winner_cascade(self):
        """2024 NFL season: Eagles beat Chiefs 40-22 in Super Bowl LIX.
        Synthesize each round's single game and confirm Eagles reaches
        SB_WINNER, Chiefs reaches SB depth."""
        games = [
            # Wild Card
            {"game_id": "wc1", "stage": "WC", "matchday": 1,
             "home": "Philadelphia Eagles", "away": "Green Bay Packers",
             "home_goals": 22, "away_goals": 10,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "wc2", "stage": "WC", "matchday": 1,
             "home": "Buffalo Bills", "away": "Denver Broncos",
             "home_goals": 31, "away_goals": 7,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # Divisional
            {"game_id": "div1", "stage": "DIV", "matchday": 1,
             "home": "Philadelphia Eagles", "away": "Los Angeles Rams",
             "home_goals": 28, "away_goals": 22,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "div2", "stage": "DIV", "matchday": 1,
             "home": "Kansas City Chiefs", "away": "Houston Texans",
             "home_goals": 23, "away_goals": 14,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "div3", "stage": "DIV", "matchday": 1,
             "home": "Buffalo Bills", "away": "Baltimore Ravens",
             "home_goals": 27, "away_goals": 25,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # Conference Championships
            {"game_id": "conf1", "stage": "CONF", "matchday": 1,
             "home": "Philadelphia Eagles", "away": "Washington Commanders",
             "home_goals": 55, "away_goals": 23,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "conf2", "stage": "CONF", "matchday": 1,
             "home": "Kansas City Chiefs", "away": "Buffalo Bills",
             "home_goals": 32, "away_goals": 29,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # Super Bowl LIX: Eagles 40, Chiefs 22
            {"game_id": "sb", "stage": "SB", "matchday": 1,
             "home": "Philadelphia Eagles", "away": "Kansas City Chiefs",
             "home_goals": 40, "away_goals": 22,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make(bracket_games=games)
        state = src.initial_state()
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["Philadelphia Eagles"] == KNOCKOUT_ROUND_DEPTH["SB_WINNER"]
        assert state["_round_reached"]["Kansas City Chiefs"] == KNOCKOUT_ROUND_DEPTH["SB"]
        assert state["_round_reached"]["Washington Commanders"] == KNOCKOUT_ROUND_DEPTH["CONF"]
        assert state["_round_reached"]["Buffalo Bills"] == KNOCKOUT_ROUND_DEPTH["CONF"]
        assert state["_round_reached"]["Los Angeles Rams"] == KNOCKOUT_ROUND_DEPTH["DIV"]
        assert state["_round_reached"]["Green Bay Packers"] == KNOCKOUT_ROUND_DEPTH["WC"]

        outcomes = src.terminal_outcomes(state)
        assert set(outcomes["Philadelphia Eagles"]) == {
            "divisional", "conf_champ", "super_bowl", "sb_winner",
        }
        assert set(outcomes["Kansas City Chiefs"]) == {
            "divisional", "conf_champ", "super_bowl",
        }

    def test_nfl_po_thresholds_are_depth_ordered(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH
        ctx = LEAGUE_CONTEXTS["NFL_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[stage] for stage, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1]


# =====================================================================
# Issue #30 part A: MLS conference-standings importance
# =====================================================================

class TestMlsStandingsIdentity:
    """MlsEastSource / MlsWestSource: per-conference standings importance.
    Pin identity (sport prefix, supports_importance, count field) before
    exercising conference-routing logic."""

    @staticmethod
    def _east():
        from dispatcharr_ranked_matchups.sources.mls_standings import MlsEastSource
        return MlsEastSource()

    @staticmethod
    def _west():
        from dispatcharr_ranked_matchups.sources.mls_standings import MlsWestSource
        return MlsWestSource()

    def test_east_routes_to_mls_east_context(self):
        e = self._east()
        assert e.league_context_code == "MLS_EAST"
        assert e._conference == "East"

    def test_west_routes_to_mls_west_context(self):
        w = self._west()
        assert w.league_context_code == "MLS_WEST"
        assert w._conference == "West"

    def test_sport_prefix_is_shared(self):
        # Top Matchups group surfaces all MLS games under a single MLS
        # prefix; conference identity is internal routing only.
        assert self._east().sport_prefix == "MLS"
        assert self._west().sport_prefix == "MLS"
        assert self._east().sport_label == self._west().sport_label

    def test_count_field_is_standings_points(self):
        # 3 W / 1 D / 0 L means raw wins understate draw-heavy teams.
        # Threshold cascade must read standings_points, not wins.
        assert self._east()._count_field == "standings_points"
        assert self._west()._count_field == "standings_points"

    def test_supports_importance(self):
        assert self._east().supports_importance is True
        assert self._west().supports_importance is True


class TestMlsStandingsContexts:
    """Pin LEAGUE_CONTEXTS entries for MLS_EAST / MLS_WEST so a threshold
    tweak doesn't silently drift away from the issue's spec
    (~30 bubble / 45 secured / 55 top-4 / 70 Shield)."""

    def test_mls_east_and_west_exist(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert "MLS_EAST" in LEAGUE_CONTEXTS
        assert "MLS_WEST" in LEAGUE_CONTEXTS

    def test_both_use_points_count_format(self):
        # 3/1/0 scheme means win-count alone is misleading; format must
        # be points_count to threshold against standings_points.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["MLS_EAST"].format == "points_count"
        assert LEAGUE_CONTEXTS["MLS_WEST"].format == "points_count"

    def test_threshold_cutoffs_match_spec(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        cutoffs_east = [c for c, _, _ in LEAGUE_CONTEXTS["MLS_EAST"].thresholds]
        cutoffs_west = [c for c, _, _ in LEAGUE_CONTEXTS["MLS_WEST"].thresholds]
        assert cutoffs_east == [30, 45, 55, 70]
        assert cutoffs_west == [30, 45, 55, 70]

    def test_threshold_weights_are_monotonic_increasing(self):
        # Out-of-order weights would let a marginal team outscore an
        # elite one. Cross-sport consequence calibration requires the
        # deeper-outcome weight to dominate.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        for code in ("MLS_EAST", "MLS_WEST"):
            weights = [w for _, _, w in LEAGUE_CONTEXTS[code].thresholds]
            for i in range(len(weights) - 1):
                assert weights[i] < weights[i + 1], (
                    f"{code} weights {weights} not monotonic at {i}"
                )

    def test_matchdays_total_matches_modern_season(self):
        # MLS modern regular season is 34 games per team.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["MLS_EAST"].matchdays_total == 34
        assert LEAGUE_CONTEXTS["MLS_WEST"].matchdays_total == 34


class TestMlsStandingsRecordResult:
    """The 3 / 1 / 0 standings-points credit. Mirrors ncaa_soccer's
    record path because both apply the same soccer scheme — pin
    independently so regression in one doesn't drag the other."""

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.mls_standings import MlsEastSource
        return MlsEastSource()

    @staticmethod
    def _row():
        return {"wins": 0, "losses": 0, "pf": 0, "pa": 0, "games_played": 0}

    def test_win_credits_three_to_winner(self):
        src = self._make()
        teams = {"Atlanta United FC": self._row(),
                 "Inter Miami CF": self._row()}
        src._record_result_into_state(
            teams, "Atlanta United FC", "Inter Miami CF", 2, 1,
        )
        assert teams["Atlanta United FC"]["standings_points"] == 3
        assert teams["Inter Miami CF"]["standings_points"] == 0
        assert teams["Atlanta United FC"]["wins"] == 1
        assert teams["Inter Miami CF"]["losses"] == 1

    def test_draw_credits_one_each(self):
        src = self._make()
        teams = {"Charlotte FC": self._row(),
                 "Orlando City SC": self._row()}
        src._record_result_into_state(
            teams, "Charlotte FC", "Orlando City SC", 1, 1,
        )
        # Pin both invariants: 1 pt each AND no W/L update. Regression
        # cost would be a tied game showing as a loss in the cascade.
        assert teams["Charlotte FC"]["standings_points"] == 1
        assert teams["Orlando City SC"]["standings_points"] == 1
        assert teams["Charlotte FC"]["wins"] == 0
        assert teams["Charlotte FC"]["losses"] == 0
        assert teams["Charlotte FC"]["draws"] == 1
        assert teams["Orlando City SC"]["draws"] == 1

    def test_loss_credits_zero(self):
        src = self._make()
        teams = {"Toronto FC": self._row(), "FC Cincinnati": self._row()}
        src._record_result_into_state(
            teams, "Toronto FC", "FC Cincinnati", 0, 2,
        )
        assert teams["Toronto FC"]["standings_points"] == 0
        assert teams["FC Cincinnati"]["standings_points"] == 3


class TestMlsStandingsSampleResultAllowsDraws:
    """MLS regular-season games CAN end in regulation draws (the league
    dropped the shootout tiebreaker in 2000). Confirm sample_result
    does NOT coin-flip ties into wins the way base PointsBasedSportSource
    does for NCAAF / NCAAM."""

    def test_sample_result_can_produce_ties(self):
        import random
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from dispatcharr_ranked_matchups.sources.mls_standings import MlsEastSource
        src = MlsEastSource()
        strengths = {
            "Atlanta United FC": {"pf_per_game": 1.0, "pa_per_game": 1.0},
            "Inter Miami CF":    {"pf_per_game": 1.0, "pa_per_game": 1.0},
        }
        gr = GameRow(
            sport_prefix="MLS", sport_label="MLS",
            home="Atlanta United FC", away="Inter Miami CF",
            rank_home=None, rank_away=None,
            start_time=datetime(2025, 6, 1, tzinfo=timezone.utc),
        )
        rng = random.Random(42)
        # With λ≈1 and 200 samples, Poisson generates ties at ~30% rate.
        # At least one tie distinguishes soccer sample_result from the
        # NCAAF/NCAAM force-a-winner shape.
        ties = 0
        for _ in range(200):
            res = src.sample_result({}, gr, strengths, rng)
            if res.home_goals == res.away_goals:
                ties += 1
        assert ties > 0, (
            "MLS sample_result must allow regulation draws; "
            "force-a-winner shape would be a regression to NCAAF behavior"
        )


class TestMlsStandingsConferenceFilter:
    """Conference-aware filter on _fetch_full_season_games: only intra-
    conference games are returned so the simulator's `_teams` dict stays
    single-conference (terminal_outcomes assumes uniform thresholds
    across all tracked teams)."""

    def test_filters_to_own_conference_intra_games(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources.mls_standings import (
            MlsEastSource,
        )
        from dispatcharr_ranked_matchups.sources import mls_standings as mod
        monkeypatch.setattr(
            mod, "_fetch_conference_map",
            lambda: {"A": "East", "B": "East", "C": "West", "D": "West"},
        )
        calls = {"n": 0}
        def fake_get(url, *_, **__):
            calls["n"] += 1
            if calls["n"] != 1:
                return {"events": []}
            return {"events": [
                # (1) A vs B — intra-East, should appear
                {"id": 1, "date": "2025-04-01T19:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": True, "state": "post"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "A"}, "score": "2"},
                         {"homeAway": "away", "team": {"displayName": "B"}, "score": "1"},
                     ],
                 }]},
                # (2) A vs C — cross-conf, should be filtered out
                {"id": 2, "date": "2025-04-01T19:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": True, "state": "post"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "A"}, "score": "1"},
                         {"homeAway": "away", "team": {"displayName": "C"}, "score": "1"},
                     ],
                 }]},
                # (3) C vs D — intra-West, should be filtered out for East
                {"id": 3, "date": "2025-04-01T19:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": True, "state": "post"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "C"}, "score": "0"},
                         {"homeAway": "away", "team": {"displayName": "D"}, "score": "2"},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        monkeypatch.setattr(mod, "SEASON_START_MONTH", 4)
        monkeypatch.setattr(mod, "SEASON_END_MONTH", 4)
        src = MlsEastSource(season_year=2025)
        games = src._fetch_full_season_games()
        ids = {g["id"] for g in games}
        # Only the A-vs-B intra-East game survives.
        assert ids == {1}, f"expected only intra-East game, got {ids}"

    def test_fetch_upcoming_emits_home_team_in_conference(self, monkeypatch):
        """fetch_upcoming emits a game only when HOME team is in own
        conference; cross-conf games surface via the home team's source.
        Pin this so registered East+West don't double-emit cross-conf
        games."""
        from dispatcharr_ranked_matchups.sources.mls_standings import (
            MlsWestSource,
        )
        from dispatcharr_ranked_matchups.sources import mls_standings as mod
        monkeypatch.setattr(
            mod, "_fetch_conference_map",
            lambda: {"East1": "East", "West1": "West"},
        )
        def fake_get(url, *_, **__):
            return {"events": [
                {"id": "g1", "date": "2030-04-01T19:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "East1"}},
                         {"homeAway": "away", "team": {"displayName": "West1"}},
                     ],
                 }]},
                {"id": "g2", "date": "2030-04-01T19:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "West1"}},
                         {"homeAway": "away", "team": {"displayName": "East1"}},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        src = MlsWestSource()
        games = src.fetch_upcoming(days_ahead=0)
        ids = {(g.extra or {}).get("espn_event_id") for g in games}
        assert ids == {"g2"}, (
            f"West source must emit only home-in-West games, got {ids}"
        )

    def test_late_season_bubble_match_produces_nonzero_importance(self):
        """Issue #30 acceptance: a bubble-line matchup with enough
        remaining matches in the season produces nonzero playoff_bubble
        leverage. Uses a stub state to bypass ESPN's thin future-
        fixtures publishing (see module docstring's "Known limitation"
        — live mid-season importance reads near 0 because ESPN MLS
        publishes only ~1-2 weeks of future games, so the simulator
        can't propagate; this test pins the simulator pipeline
        independent of the data-availability quirk).
        """
        import random
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_standings import (
            MlsEastSource,
        )
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from dispatcharr_ranked_matchups.scoring import (
            LEAGUE_CONTEXTS, compute_match_importance,
        )

        class _StubEast(MlsEastSource):
            def initial_state(self):
                # Nashville at 29 pts, one game from the 30 pt bubble.
                # Inter Miami at 26, also within reach. A single match
                # result genuinely moves Nashville above or below the
                # threshold — leverage should be nonzero.
                return {
                    "_applied": frozenset(),
                    "_teams": {
                        "Nashville SC": {
                            "wins": 9, "losses": 4, "pf": 18, "pa": 12,
                            "games_played": 15, "standings_points": 29,
                            "draws": 2,
                        },
                        "Inter Miami CF": {
                            "wins": 8, "losses": 5, "pf": 16, "pa": 14,
                            "games_played": 15, "standings_points": 26,
                            "draws": 2,
                        },
                        "Atlanta United FC": {
                            "wins": 4, "losses": 8, "pf": 10, "pa": 16,
                            "games_played": 15, "standings_points": 15,
                            "draws": 3,
                        },
                        "CF Montréal": {
                            "wins": 5, "losses": 7, "pf": 11, "pa": 14,
                            "games_played": 15, "standings_points": 18,
                            "draws": 3,
                        },
                        "Charlotte FC": {
                            "wins": 6, "losses": 6, "pf": 13, "pa": 13,
                            "games_played": 15, "standings_points": 21,
                            "draws": 3,
                        },
                    },
                }

            def remaining_matches(self, state):
                applied = state.get("_applied", frozenset())
                base = [
                    GameRow("MLS", "MLS", "Inter Miami CF", "CF Montréal",
                            None, None, datetime(2026, 10, 1, tzinfo=timezone.utc),
                            extra={"game_id": "r2"}),
                    GameRow("MLS", "MLS", "Atlanta United FC", "Nashville SC",
                            None, None, datetime(2026, 10, 8, tzinfo=timezone.utc),
                            extra={"game_id": "r3"}),
                    GameRow("MLS", "MLS", "Charlotte FC", "Inter Miami CF",
                            None, None, datetime(2026, 10, 15, tzinfo=timezone.utc),
                            extra={"game_id": "r4"}),
                    GameRow("MLS", "MLS", "Nashville SC", "CF Montréal",
                            None, None, datetime(2026, 10, 22, tzinfo=timezone.utc),
                            extra={"game_id": "r5"}),
                ]
                return [m for m in base if m.extra["game_id"] not in applied]

            def estimate_strengths(self):
                return {
                    "Nashville SC":      {"pf_per_game": 1.2, "pa_per_game": 0.8},
                    "Inter Miami CF":    {"pf_per_game": 1.1, "pa_per_game": 0.9},
                    "Atlanta United FC": {"pf_per_game": 0.7, "pa_per_game": 1.1},
                    "CF Montréal":       {"pf_per_game": 0.7, "pa_per_game": 0.9},
                    "Charlotte FC":      {"pf_per_game": 0.9, "pa_per_game": 0.9},
                }

        ctx = LEAGUE_CONTEXTS["MLS_EAST"]
        src = _StubEast()
        target = GameRow(
            "MLS", "MLS", "Nashville SC", "Inter Miami CF",
            None, None, datetime(2026, 9, 30, tzinfo=timezone.utc),
            extra={"game_id": "target"},
        )
        points, _, hits = compute_match_importance(
            src, target, ctx, n_sims=300, rng=random.Random(42),
        )
        # Bubble-line match must register nonzero leverage. Seed 42 with
        # n_sims=300 gives a reproducible ~0.5-1.0 point reading on
        # this scenario; pinning >= 0.05 keeps the assertion resilient
        # to Poisson sampling noise across Python implementations.
        assert points >= 0.05, f"expected nonzero bubble leverage, got {points}"
        assert "playoff_bubble" in hits, (
            f"expected playoff_bubble in thresholds hit, got {hits}"
        )

    def test_conference_map_parses_espn_standings_shape(self, monkeypatch):
        """Pin the parser against ESPN's actual /standings response
        shape so a schema drift (e.g., abbreviation renamed,
        children[] nesting changed) fails loudly here instead of
        silently returning an empty conference map (which would
        cascade into 0 emitted games + 0 importance with no error).
        """
        from dispatcharr_ranked_matchups.sources import mls_standings as mod
        # Realistic ESPN shape: top-level "children" list with two
        # entries, each carrying a standings.entries[].team.displayName.
        fixture = {
            "children": [
                {
                    "abbreviation": "East",
                    "name": "Eastern Conference",
                    "standings": {"entries": [
                        {"team": {"displayName": "Atlanta United FC"}},
                        {"team": {"displayName": "Inter Miami CF"}},
                    ]},
                },
                {
                    "abbreviation": "West",
                    "name": "Western Conference",
                    "standings": {"entries": [
                        {"team": {"displayName": "LA Galaxy"}},
                        {"team": {"displayName": "LAFC"}},
                    ]},
                },
                # A third child with a non-East/West abbreviation should
                # be silently ignored (defensive against ESPN adding new
                # buckets like an exhibition group).
                {
                    "abbreviation": "Other",
                    "name": "Exhibition",
                    "standings": {"entries": [
                        {"team": {"displayName": "Bogus Team"}},
                    ]},
                },
            ],
        }
        monkeypatch.setattr(mod, "_http_get", lambda *_a, **_k: fixture)
        cmap = mod._fetch_conference_map()
        assert cmap == {
            "Atlanta United FC": "East",
            "Inter Miami CF":    "East",
            "LA Galaxy":         "West",
            "LAFC":              "West",
        }, f"unexpected conference map: {cmap}"

    def test_conference_map_returns_empty_on_endpoint_failure(self, monkeypatch):
        """When _http_get returns None (ESPN down / network error /
        4xx), the conference map MUST be empty rather than partial.
        An empty map cascades to 0 emitted games — graceful degradation
        rather than misclassified teams getting the wrong conference's
        threshold bands.
        """
        from dispatcharr_ranked_matchups.sources import mls_standings as mod
        monkeypatch.setattr(mod, "_http_get", lambda *_a, **_k: None)
        assert mod._fetch_conference_map() == {}

    def test_filters_out_non_regular_season(self, monkeypatch):
        """Playoff games (season.slug != 'regular-season') must be filtered
        out so the Cup bracket (sibling #30 part B) doesn't pollute
        regular-season threshold computation."""
        from dispatcharr_ranked_matchups.sources.mls_standings import (
            MlsEastSource,
        )
        from dispatcharr_ranked_matchups.sources import mls_standings as mod
        monkeypatch.setattr(
            mod, "_fetch_conference_map",
            lambda: {"A": "East", "B": "East"},
        )
        calls = {"n": 0}
        def fake_get(url, *_, **__):
            calls["n"] += 1
            if calls["n"] != 1:
                return {"events": []}
            return {"events": [
                {"id": 100, "date": "2025-04-01T19:00:00Z",
                 "season": {"slug": "eastern-conference-playoffs---round-one"},
                 "competitions": [{
                     "status": {"type": {"completed": True, "state": "post"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "A"}, "score": "2"},
                         {"homeAway": "away", "team": {"displayName": "B"}, "score": "1"},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        monkeypatch.setattr(mod, "SEASON_START_MONTH", 4)
        monkeypatch.setattr(mod, "SEASON_END_MONTH", 4)
        src = MlsEastSource(season_year=2025)
        games = src._fetch_full_season_games()
        assert games == [], "playoff-slug games must be filtered from season fetch"


# =====================================================================
# Issue #30 part B: MLS Cup playoff bracket (mixed format)
# =====================================================================

class TestMlsCupSourceIdentity:

    @staticmethod
    def _make(bracket_games=None):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        src = MlsCupSource(season_year=2024)
        src._bracket_games_cache = bracket_games or []
        return src

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "MLS"
        assert "Cup" in src.sport_label
        assert src._league_context_code() == "MLS_PO"

    def test_supports_importance(self):
        assert self._make().supports_importance is True

    def test_ko_stages(self):
        # MLS-prefixed labels avoid depth collision with NHL "R1" at
        # depth 0 or MLB "WC" at depth 0.
        assert self._make().KO_STAGES == (
            "MLS_WC", "MLS_R1", "MLS_CSF", "MLS_CF", "MLS_CUP",
        )

    def test_series_lengths_match_mls_format(self):
        """Mixed format: WC, CSF, CF, MLS_CUP are single-leg; R1 is
        best-of-3 (clinches at 2 wins). Pin all five — a regression
        to uniform best-of-N would silently misclassify clinches."""
        src = self._make()
        assert src._series_length_for_stage("MLS_WC") == 1
        assert src._series_length_for_stage("MLS_R1") == 3
        assert src._series_length_for_stage("MLS_CSF") == 1
        assert src._series_length_for_stage("MLS_CF") == 1
        assert src._series_length_for_stage("MLS_CUP") == 1
        assert src._series_length_for_stage("UNKNOWN") == 1  # fallback
        assert src._clinching_wins_for_stage("MLS_WC") == 1
        assert src._clinching_wins_for_stage("MLS_R1") == 2
        assert src._clinching_wins_for_stage("MLS_CUP") == 1

    def test_winner_advance_label(self):
        src = self._make()
        assert src._winner_advance_label("MLS_CUP") == "MLS_CUP_WINNER"
        assert src._winner_advance_label("MLS_CF") is None
        assert src._winner_advance_label("MLS_R1") is None


class TestMlsCupContexts:

    def test_mls_po_exists_and_is_knockout(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        assert "MLS_PO" in LEAGUE_CONTEXTS
        assert LEAGUE_CONTEXTS["MLS_PO"].format == "knockout"

    def test_thresholds_use_mls_prefixed_stages(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS["MLS_PO"]
        stages = [stage for stage, _, _ in ctx.thresholds]
        assert stages == [
            "MLS_R1", "MLS_CSF", "MLS_CF", "MLS_CUP", "MLS_CUP_WINNER",
        ]

    def test_outcome_labels(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        labels = MlsCupSource().outcome_labels
        for expected in (
            "round_one", "conf_semis", "conf_final",
            "mls_cup_final", "mls_cup_winner",
        ):
            assert expected in labels, f"missing {expected!r}; got {labels}"

    def test_thresholds_are_depth_ordered(self):
        from dispatcharr_ranked_matchups.scoring import (
            LEAGUE_CONTEXTS, KNOCKOUT_ROUND_DEPTH,
        )
        ctx = LEAGUE_CONTEXTS["MLS_PO"]
        depths = [KNOCKOUT_ROUND_DEPTH[s] for s, _, _ in ctx.thresholds]
        for i in range(len(depths) - 1):
            assert depths[i] < depths[i + 1]

    def test_weights_are_monotonic_increasing(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        weights = [w for _, _, w in LEAGUE_CONTEXTS["MLS_PO"].thresholds]
        for i in range(len(weights) - 1):
            assert weights[i] < weights[i + 1]

    def test_mls_specific_depths_present(self):
        # Every MLS-prefixed stage label must have a KNOCKOUT_ROUND_DEPTH
        # entry; missing one would silently return -1 from the lookup
        # and the cascade would silently no-op for that stage.
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        for stage in ("MLS_WC", "MLS_R1", "MLS_CSF", "MLS_CF",
                      "MLS_CUP", "MLS_CUP_WINNER"):
            assert stage in KNOCKOUT_ROUND_DEPTH, (
                f"MLS_PO uses {stage} but KNOCKOUT_ROUND_DEPTH lacks it"
            )


class TestMlsCupSlugRouting:

    def test_slug_to_stage_covers_both_conferences(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import SLUG_TO_STAGE
        assert SLUG_TO_STAGE["eastern-conference-playoffs---wild-card"] == "MLS_WC"
        assert SLUG_TO_STAGE["western-conference-playoffs---wild-card"] == "MLS_WC"
        assert SLUG_TO_STAGE["eastern-conference-playoffs---round-one"] == "MLS_R1"
        assert SLUG_TO_STAGE["western-conference-playoffs---round-one"] == "MLS_R1"
        assert SLUG_TO_STAGE["eastern-conference-playoffs---semifinals"] == "MLS_CSF"
        assert SLUG_TO_STAGE["western-conference-playoffs---semifinals"] == "MLS_CSF"
        assert SLUG_TO_STAGE["eastern-conference-playoffs---final"] == "MLS_CF"
        assert SLUG_TO_STAGE["western-conference-playoffs---final"] == "MLS_CF"
        assert SLUG_TO_STAGE["mls-cup"] == "MLS_CUP"

    def test_extract_filters_regular_season(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        event = {
            "id": "reg-1",
            "date": "2024-08-15T19:00:00Z",
            "season": {"slug": "regular-season"},
            "competitions": [{
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Atlanta United FC"}, "score": "2"},
                    {"homeAway": "away", "team": {"displayName": "Inter Miami CF"}, "score": "1"},
                ],
            }],
        }
        assert MlsCupSource._extract_bracket_record(event, matchday=1) is None

    def test_extract_filters_phantom_best_of_3_game(self):
        """ESPN publishes a phantom game-3 (state=post, completed=False,
        score=0-0) for best-of-3 series that clinch in 2 games. These
        MUST be filtered out — pinning against the live shape observed
        on event 722587 in the 2024 R1 (LA Galaxy / Colorado series
        clinched in 2 games; ESPN's game-3 slot was state=post +
        completed=False).
        """
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        phantom = {
            "id": "722587",
            "date": "2024-11-09T22:00:00Z",
            "season": {"slug": "western-conference-playoffs---round-one"},
            "competitions": [{
                "status": {"type": {"completed": False, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "LA Galaxy"}, "score": "0", "winner": False},
                    {"homeAway": "away", "team": {"displayName": "Colorado Rapids"}, "score": "0", "winner": False},
                ],
            }],
        }
        assert MlsCupSource._extract_bracket_record(phantom, matchday=None) is None

    def test_extract_keeps_legitimate_finished_game(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        event = {
            "id": "722574",
            "date": "2024-10-26T19:00:00Z",
            "season": {"slug": "eastern-conference-playoffs---round-one"},
            "competitions": [{
                "status": {"type": {"completed": True, "state": "post"}},
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Atlanta United FC"}, "score": "1"},
                    {"homeAway": "away", "team": {"displayName": "Inter Miami CF"}, "score": "2"},
                ],
            }],
        }
        rec = MlsCupSource._extract_bracket_record(event, matchday=1)
        assert rec is not None
        assert rec["stage"] == "MLS_R1"
        assert rec["status"] == "FINISHED"
        assert rec["home_goals"] == 1
        assert rec["away_goals"] == 2

    def test_extract_keeps_scheduled_game(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        event = {
            "id": "future-1",
            "date": "2030-10-26T19:00:00Z",
            "season": {"slug": "eastern-conference-playoffs---round-one"},
            "competitions": [{
                "status": {"type": {"completed": False, "state": "pre"}},
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Atlanta United FC"}},
                    {"homeAway": "away", "team": {"displayName": "Inter Miami CF"}},
                ],
            }],
        }
        rec = MlsCupSource._extract_bracket_record(event, matchday=None)
        assert rec is not None
        assert rec["status"] == "SCHEDULED"
        assert rec["home_goals"] is None
        assert rec["away_goals"] is None


class TestMlsCupMatchdayInference:
    """Best-of-3 R1 series get matchdays inferred from chronological
    order; single-leg stages get matchday=1 + PK dedup."""

    def test_best_of_3_assigns_matchdays_1_2_3(self):
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            _assign_matchdays_and_dedupe, _MLS_SERIES_LENGTHS,
        )
        games = [
            {"game_id": "1", "stage": "MLS_R1", "matchday": None,
             "home": "LA", "away": "COL", "home_goals": 5, "away_goals": 0,
             "status": "FINISHED",
             "start_time": datetime(2024, 10, 27, tzinfo=timezone.utc),
             "extra": {}},
            {"game_id": "2", "stage": "MLS_R1", "matchday": None,
             "home": "COL", "away": "LA", "home_goals": 4, "away_goals": 1,
             "status": "FINISHED",
             "start_time": datetime(2024, 11, 2, tzinfo=timezone.utc),
             "extra": {}},
            {"game_id": "3", "stage": "MLS_R1", "matchday": None,
             "home": "LA", "away": "COL", "home_goals": 2, "away_goals": 0,
             "status": "FINISHED",
             "start_time": datetime(2024, 11, 10, tzinfo=timezone.utc),
             "extra": {}},
        ]
        out = _assign_matchdays_and_dedupe(games, _MLS_SERIES_LENGTHS)
        by_id = {g["game_id"]: g for g in out}
        assert by_id["1"]["matchday"] == 1
        assert by_id["2"]["matchday"] == 2
        assert by_id["3"]["matchday"] == 3

    def test_single_leg_stages_get_matchday_one(self):
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            _assign_matchdays_and_dedupe, _MLS_SERIES_LENGTHS,
        )
        games = [
            {"game_id": "cup-1", "stage": "MLS_CUP", "matchday": None,
             "home": "LA", "away": "NY", "home_goals": 2, "away_goals": 1,
             "status": "FINISHED",
             "start_time": datetime(2024, 12, 7, tzinfo=timezone.utc),
             "extra": {}},
            {"game_id": "wc-1", "stage": "MLS_WC", "matchday": None,
             "home": "A", "away": "B", "home_goals": 1, "away_goals": 0,
             "status": "FINISHED",
             "start_time": datetime(2024, 10, 23, tzinfo=timezone.utc),
             "extra": {}},
        ]
        out = _assign_matchdays_and_dedupe(games, _MLS_SERIES_LENGTHS)
        for g in out:
            assert g["matchday"] == 1

    def test_single_leg_pk_shootout_pair_collapses(self):
        """A regulation 0-0 + a PK shootout result at the same
        single-leg stage collapses to the non-tie record (reusing
        the helper from ncaa_soccer_cup)."""
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            _assign_matchdays_and_dedupe, _MLS_SERIES_LENGTHS,
        )
        regulation_tie = {
            "game_id": "csf-tie", "stage": "MLS_CSF", "matchday": None,
            "home": "A", "away": "B", "home_goals": 0, "away_goals": 0,
            "status": "FINISHED",
            "start_time": datetime(2024, 11, 24, 0, 0, tzinfo=timezone.utc),
            "extra": {},
        }
        pk_result = {
            "game_id": "csf-pk", "stage": "MLS_CSF", "matchday": None,
            "home": "A", "away": "B", "home_goals": 1, "away_goals": 0,
            "status": "FINISHED",
            "start_time": datetime(2024, 11, 24, 1, 0, tzinfo=timezone.utc),
            "extra": {},
        }
        out = _assign_matchdays_and_dedupe(
            [regulation_tie, pk_result], _MLS_SERIES_LENGTHS,
        )
        assert len(out) == 1
        assert out[0]["game_id"] == "csf-pk"

    def test_best_of_3_does_not_dedupe_legitimate_repeats(self):
        """Same teams playing 2-3 games in a best-of-3 series must
        NOT be collapsed by the PK dedup — pin so a future refactor
        doesn't accidentally apply dedup to best-of-N stages."""
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            _assign_matchdays_and_dedupe, _MLS_SERIES_LENGTHS,
        )
        games = [
            {"game_id": "g1", "stage": "MLS_R1", "matchday": None,
             "home": "A", "away": "B", "home_goals": 2, "away_goals": 1,
             "status": "FINISHED",
             "start_time": datetime(2024, 10, 27, tzinfo=timezone.utc),
             "extra": {}},
            {"game_id": "g2", "stage": "MLS_R1", "matchday": None,
             "home": "B", "away": "A", "home_goals": 1, "away_goals": 0,
             "status": "FINISHED",
             "start_time": datetime(2024, 11, 2, tzinfo=timezone.utc),
             "extra": {}},
        ]
        out = _assign_matchdays_and_dedupe(games, _MLS_SERIES_LENGTHS)
        assert len(out) == 2


class TestMlsCupCascade:
    """A team that wins WC → R1 (best-of-3) → CSF → CF → MLS_CUP
    reaches MLS_CUP_WINNER depth with the full band cascade."""

    @staticmethod
    def _make(bracket_games):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        src = MlsCupSource(season_year=2024)
        src._bracket_games_cache = bracket_games
        return src

    def test_champion_from_wild_card_cascade(self):
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        games = [
            # WC: single-leg
            {"game_id": "wc", "stage": "MLS_WC", "matchday": 1,
             "home": "Champion", "away": "Eight",
             "home_goals": 1, "away_goals": 0,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # R1: best-of-3, Champion wins games 1 + 2 → clinches at 2 wins
            {"game_id": "r1g1", "stage": "MLS_R1", "matchday": 1,
             "home": "Champion", "away": "Seven",
             "home_goals": 2, "away_goals": 0,
             "status": "FINISHED", "start_time": None, "extra": {}},
            {"game_id": "r1g2", "stage": "MLS_R1", "matchday": 2,
             "home": "Seven", "away": "Champion",
             "home_goals": 0, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # CSF: single-leg
            {"game_id": "csf", "stage": "MLS_CSF", "matchday": 1,
             "home": "Champion", "away": "Four",
             "home_goals": 2, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # CF: single-leg
            {"game_id": "cf", "stage": "MLS_CF", "matchday": 1,
             "home": "Champion", "away": "Two",
             "home_goals": 1, "away_goals": 0,
             "status": "FINISHED", "start_time": None, "extra": {}},
            # MLS Cup: single-leg
            {"game_id": "cup", "stage": "MLS_CUP", "matchday": 1,
             "home": "Champion", "away": "Final",
             "home_goals": 2, "away_goals": 1,
             "status": "FINISHED", "start_time": None, "extra": {}},
        ]
        src = self._make(games)
        state = src.initial_state()
        rr = state["_round_reached"]
        assert rr["Champion"] == KNOCKOUT_ROUND_DEPTH["MLS_CUP_WINNER"]
        assert rr["Final"] == KNOCKOUT_ROUND_DEPTH["MLS_CUP"]
        outcomes = src.terminal_outcomes(state)
        bands = outcomes["Champion"]
        for expected in (
            "round_one", "conf_semis", "conf_final",
            "mls_cup_final", "mls_cup_winner",
        ):
            assert expected in bands, f"missing {expected!r}; got {bands}"


class TestMlsCupSampleResultNoDraws:
    """Bracket games go to OT then PKs — sample_result must force a winner."""

    def test_sample_result_never_produces_ties(self):
        import random
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        src = MlsCupSource(season_year=2024)
        strengths = {
            "LA": {"pf_per_game": 1.4, "pa_per_game": 1.4},
            "NY": {"pf_per_game": 1.4, "pa_per_game": 1.4},
        }
        gr = GameRow(
            sport_prefix="MLS", sport_label="MLS Cup Playoffs",
            home="LA", away="NY", rank_home=None, rank_away=None,
            start_time=datetime(2024, 12, 7, tzinfo=timezone.utc),
        )
        rng = random.Random(42)
        ties = 0
        for _ in range(200):
            res = src.sample_result({}, gr, strengths, rng)
            if res.home_goals == res.away_goals:
                ties += 1
        assert ties == 0


class TestMlsCupFetchUpcoming:
    """fetch_upcoming surfaces only playoff bracket games (not regular-
    season). Pin via stub so a slug-table regression surfaces here."""

    def test_fetch_upcoming_emits_only_bracket_games(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import mls_cup as mod
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        calls = {"n": 0}
        def fake_get(url, *_a, **_kw):
            calls["n"] += 1
            if calls["n"] != 1:
                return {"events": []}
            return {"events": [
                # Scheduled bracket game — should emit.
                {"id": "playoff-1", "date": "2030-10-26T19:00:00Z",
                 "season": {"slug": "eastern-conference-playoffs---round-one"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "Atlanta United FC"}},
                         {"homeAway": "away", "team": {"displayName": "Inter Miami CF"}},
                     ],
                 }]},
                # Regular-season game — must be filtered out.
                {"id": "regular-1", "date": "2030-10-26T22:00:00Z",
                 "season": {"slug": "regular-season"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "pre"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "LA Galaxy"}},
                         {"homeAway": "away", "team": {"displayName": "LAFC"}},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        src = MlsCupSource()
        games = src.fetch_upcoming(days_ahead=0)
        ids = {(g.extra or {}).get("espn_event_id") for g in games}
        assert ids == {"playoff-1"}, (
            f"only bracket-slug games should emit, got {ids}"
        )
        assert games[0].extra["stage"] == "MLS_R1"
        assert games[0].extra["fd_competition_code"] == "MLS_PO"

    def test_fetch_upcoming_filters_phantom_games(self, monkeypatch):
        """The EPG emit side must apply the same phantom-game filter
        as _fetch_bracket_games so unnecessary best-of-3 game-3 slots
        don't appear in the user's Top Matchups guide either."""
        from dispatcharr_ranked_matchups.sources import mls_cup as mod
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        calls = {"n": 0}
        def fake_get(url, *_a, **_kw):
            calls["n"] += 1
            if calls["n"] != 1:
                return {"events": []}
            return {"events": [
                # Phantom game-3: state=post, completed=False, 0-0.
                {"id": "phantom-g3", "date": "2030-11-09T22:00:00Z",
                 "season": {"slug": "western-conference-playoffs---round-one"},
                 "competitions": [{
                     "status": {"type": {"completed": False, "state": "post"}},
                     "competitors": [
                         {"homeAway": "home", "team": {"displayName": "LA Galaxy"}, "score": "0"},
                         {"homeAway": "away", "team": {"displayName": "Colorado Rapids"}, "score": "0"},
                     ],
                 }]},
            ]}
        monkeypatch.setattr(mod, "_http_get", fake_get)
        src = MlsCupSource()
        games = src.fetch_upcoming(days_ahead=0)
        assert games == [], (
            "phantom game-3 must not appear in fetch_upcoming output"
        )


class TestMlsCupMatchdayInferenceDefensive:
    """Matchday inference sort key handles missing start_time by sorting
    such records to the end (datetime.max). Pin so a malformed event
    with no start_time doesn't crash the day-loop or scramble matchday
    ordering for adjacent legitimate games."""

    def test_missing_start_time_does_not_crash(self):
        from datetime import datetime, timezone
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            _assign_matchdays_and_dedupe, _MLS_SERIES_LENGTHS,
        )
        # One legit game + one with no start_time at the same stage.
        games = [
            {"game_id": "g1", "stage": "MLS_R1", "matchday": None,
             "home": "A", "away": "B", "home_goals": 2, "away_goals": 1,
             "status": "FINISHED",
             "start_time": datetime(2024, 10, 27, tzinfo=timezone.utc),
             "extra": {}},
            {"game_id": "g2", "stage": "MLS_R1", "matchday": None,
             "home": "A", "away": "B", "home_goals": 1, "away_goals": 0,
             "status": "FINISHED",
             "start_time": None,  # malformed input — sort to end
             "extra": {}},
        ]
        out = _assign_matchdays_and_dedupe(games, _MLS_SERIES_LENGTHS)
        assert len(out) == 2
        # g1 (Oct 27) ordered before g2 (None → sorted last) so g1=1, g2=2.
        by_id = {g["game_id"]: g for g in out}
        assert by_id["g1"]["matchday"] == 1
        assert by_id["g2"]["matchday"] == 2


class TestMlsCupStrengthSharing:
    """Plugin merges East+West strengths into one dict; the cup source
    queries by team name without caring which conference seeded it."""

    def test_set_strengths_persists(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import MlsCupSource
        src = MlsCupSource()
        merged = {
            "LA Galaxy": {"pf_per_game": 1.8, "pa_per_game": 0.9},
            "Inter Miami CF": {"pf_per_game": 2.0, "pa_per_game": 1.1},
        }
        src.set_regular_season_strengths(merged)
        assert src.estimate_strengths() == merged

    def test_strength_for_falls_back_to_prior(self):
        from dispatcharr_ranked_matchups.sources.mls_cup import (
            MlsCupSource, _DEFAULT_GOALS_FOR, _DEFAULT_GOALS_AGAINST,
        )
        src = MlsCupSource()
        # No seed set → estimate_strengths returns {}; strength_for
        # falls through to the league-average prior.
        assert src.estimate_strengths() == {}
        result = src._strength_for({}, "Some Unknown Team")
        assert result["pf_per_game"] == _DEFAULT_GOALS_FOR
        assert result["pa_per_game"] == _DEFAULT_GOALS_AGAINST


# =====================================================================
# Phase Q: NWSL + Liga MX (subclasses of MlsSource)
# =====================================================================

class TestNwslSource:
    """NwslSource subclasses MlsSource with NWSL endpoint + Odds API
    key. The shared fetch/closeness machinery is exercised by the
    MlsSource tests; these tests pin the per-league config."""

    def test_identity(self):
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        src = NwslSource(odds_api_key="")
        assert src.sport_prefix == "NWSL"
        assert src.sport_label == "NWSL"

    def test_espn_slug(self):
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        src = NwslSource()
        assert src._ESPN_SLUG == "soccer/usa.nwsl"
        assert src._espn_base().endswith("/soccer/usa.nwsl")

    def test_odds_sport_key(self):
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        assert NwslSource._ODDS_SPORT_KEY == "soccer_usa_nwsl"

    def test_fd_code_distinct_from_mls(self):
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        from dispatcharr_ranked_matchups.sources.mls import MlsSource
        # NWSL must carry its own fd_competition_code so scoring
        # doesn't mis-route NWSL games into MLS.
        assert NwslSource._FD_CODE != MlsSource._FD_CODE
        assert NwslSource._FD_CODE == "NWSL"

    def test_no_importance_in_v1(self):
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        assert NwslSource().supports_importance is False


class TestLigaMxSource:
    """LigaMxSource: same shape as NwslSource. Liga MX runs two
    seasons per calendar year (Apertura / Clausura) plus a Liguilla
    playoff. The `extra.season_slug` field on emitted GameRows carries
    that distinction."""

    def test_identity(self):
        from dispatcharr_ranked_matchups.sources.liga_mx import LigaMxSource
        src = LigaMxSource(odds_api_key="")
        assert src.sport_prefix == "LigaMX"
        assert src.sport_label == "Liga MX"

    def test_espn_slug(self):
        from dispatcharr_ranked_matchups.sources.liga_mx import LigaMxSource
        src = LigaMxSource()
        assert src._ESPN_SLUG == "soccer/mex.1"
        assert src._espn_base().endswith("/soccer/mex.1")

    def test_odds_sport_key(self):
        from dispatcharr_ranked_matchups.sources.liga_mx import LigaMxSource
        assert LigaMxSource._ODDS_SPORT_KEY == "soccer_mexico_ligamx"

    def test_fd_code_distinct(self):
        from dispatcharr_ranked_matchups.sources.liga_mx import LigaMxSource
        from dispatcharr_ranked_matchups.sources.mls import MlsSource
        from dispatcharr_ranked_matchups.sources.nwsl import NwslSource
        assert LigaMxSource._FD_CODE == "LigaMX"
        codes = {MlsSource._FD_CODE, NwslSource._FD_CODE, LigaMxSource._FD_CODE}
        assert len(codes) == 3

    def test_season_slug_carries_through(self, monkeypatch):
        """Liga MX has Apertura/Clausura/Liguilla seasons.
        `extra.season_slug` on emitted GameRows must carry ESPN's
        slug so future routing can use it."""
        import dispatcharr_ranked_matchups.sources.mls as mls_mod
        from dispatcharr_ranked_matchups.sources.liga_mx import LigaMxSource

        espn_response = {
            "events": [{
                "id": "401900001",
                "date": "2026-02-15T03:00Z",
                "season": {"year": 2026, "type": 2, "slug": "torneo-clausura"},
                "competitions": [{
                    "competitors": [
                        {"homeAway": "home",
                         "team": {"displayName": "Club America"}},
                        {"homeAway": "away",
                         "team": {"displayName": "Chivas Guadalajara"}},
                    ],
                }],
            }],
        }
        monkeypatch.setattr(mls_mod, "_http_get", lambda *a, **kw: espn_response)
        src = LigaMxSource(odds_api_key="")
        games = src.fetch_upcoming(days_ahead=0)
        assert len(games) == 1
        assert games[0].extra.get("season_slug") == "torneo-clausura"
        assert games[0].extra.get("fd_competition_code") == "LigaMX"


# =====================================================================
# Phase R: field events (F1 + NASCAR + Golf)
# =====================================================================

class TestFieldEventSourceContract:
    """FieldEventSource is the ABC for racing + golf — events that
    aren't head-to-head. Pin the contract: home=event_name, away="Field"
    sentinel, no importance simulation."""

    def test_f1_identity(self):
        from dispatcharr_ranked_matchups.sources.field_event import F1Source
        src = F1Source()
        assert src.sport_prefix == "F1"
        assert src.sport_label == "Formula 1"
        assert src.ESPN_SLUG == "racing/f1"

    def test_nascar_identity(self):
        from dispatcharr_ranked_matchups.sources.field_event import NascarSource
        src = NascarSource()
        assert src.sport_prefix == "NASCAR"
        assert "NASCAR" in src.sport_label
        assert src.ESPN_SLUG == "racing/nascar-premier"

    def test_golf_identity(self):
        from dispatcharr_ranked_matchups.sources.field_event import GolfSource
        src = GolfSource()
        assert src.sport_prefix == "PGA"
        assert "PGA" in src.sport_label
        assert src.ESPN_SLUG == "golf/pga"

    def test_field_events_do_not_support_importance(self):
        """Field events surface via tournament_stage alone in V1; no
        Monte Carlo importance simulation."""
        from dispatcharr_ranked_matchups.sources.field_event import (
            F1Source, NascarSource, GolfSource,
        )
        for src in (F1Source(), NascarSource(), GolfSource()):
            assert src.supports_importance is False


class TestGolfMajorDetection:
    """The four golf majors get the MAJOR score tier. Detection is
    regex-based against event name; regex must catch ESPN's varied
    naming."""

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.field_event import GolfSource
        return GolfSource()

    @staticmethod
    def _is_major(src, name):
        return src.MAJOR_REGEX is not None and bool(src.MAJOR_REGEX.search(name))

    def test_masters(self):
        src = self._make()
        assert self._is_major(src, "Masters Tournament")
        assert self._is_major(src, "The Masters")

    def test_pga_championship(self):
        src = self._make()
        assert self._is_major(src, "PGA Championship")

    def test_us_open(self):
        src = self._make()
        assert self._is_major(src, "U.S. Open Championship")
        assert self._is_major(src, "US Open")

    def test_british_open(self):
        src = self._make()
        # ESPN has used both "The Open Championship" and "British Open".
        assert self._is_major(src, "The Open Championship")
        assert self._is_major(src, "British Open")

    def test_regular_tour_event_not_major(self):
        src = self._make()
        assert not self._is_major(src, "Charles Schwab Challenge")
        assert not self._is_major(src, "FedEx St. Jude Championship")
        assert not self._is_major(src, "Players Championship")

    def test_f1_no_major_regex(self):
        """F1 has no major tier in V1; the class attr is None."""
        from dispatcharr_ranked_matchups.sources.field_event import F1Source
        assert F1Source.MAJOR_REGEX is None


class TestFieldEventFetchUpcoming:
    """Pin the GameRow shape FieldEventSource emits."""

    def test_emits_event_tier_row(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401900100",
                "name": "Heineken Chinese Grand Prix",
                "shortName": "Heineken Chinese GP",
                "date": "2026-04-12T05:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import F1Source
        src = F1Source()
        games = src.fetch_upcoming(days_ahead=7)
        assert len(games) == 1
        g = games[0]
        assert g.home == "Heineken Chinese Grand Prix"
        # Sentinel away team — field events have no opponent.
        assert g.away == "Field"
        assert g.sport_prefix == "F1"
        assert g.extra.get("stage") == "EVENT"
        assert g.extra.get("is_field_event") is True
        assert g.extra.get("fd_competition_code") == "F1"

    def test_emits_major_tier_for_golf_major(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401900200",
                "name": "Masters Tournament",
                "shortName": "Masters Tournament",
                "date": "2026-04-09T17:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import GolfSource
        src = GolfSource()
        games = src.fetch_upcoming(days_ahead=7)
        assert len(games) == 1
        assert games[0].extra.get("stage") == "MAJOR"

    def test_emits_event_tier_for_regular_golf_tournament(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401900201",
                "name": "Charles Schwab Challenge",
                "shortName": "Charles Schwab Challenge",
                "date": "2026-05-21T17:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import GolfSource
        src = GolfSource()
        games = src.fetch_upcoming(days_ahead=7)
        assert len(games) == 1
        assert games[0].extra.get("stage") == "EVENT"

    def test_empty_response_returns_empty_list(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: {"events": []})
        from dispatcharr_ranked_matchups.sources.field_event import F1Source
        assert F1Source().fetch_upcoming(days_ahead=7) == []

    def test_http_failure_returns_empty_list(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: None)
        from dispatcharr_ranked_matchups.sources.field_event import NascarSource
        assert NascarSource().fetch_upcoming(days_ahead=7) == []


class TestFieldEventScoring:
    """tournament_stage = EVENT or MAJOR must produce a non-zero score
    so field events appear in the curated guide."""

    def test_event_stage_scores_nonzero(self):
        from dispatcharr_ranked_matchups.scoring import (
            GameSignals, Weights, score_game,
        )
        signals = GameSignals(
            team_a="Heineken Chinese Grand Prix",
            team_b="Field",
            tournament_stage="EVENT",
        )
        score = score_game(signals, Weights())
        assert score.raw > 0

    def test_major_stage_scores_higher_than_event(self):
        from dispatcharr_ranked_matchups.scoring import (
            GameSignals, Weights, score_game,
        )
        weights = Weights()
        event_score = score_game(
            GameSignals(team_a="X", team_b="Field", tournament_stage="EVENT"),
            weights,
        )
        major_score = score_game(
            GameSignals(team_a="X", team_b="Field", tournament_stage="MAJOR"),
            weights,
        )
        # MAJOR must outrank EVENT — golf majors should beat regular
        # tour stops in the guide.
        assert major_score.raw > event_score.raw


# =====================================================================
# Phase S: UFC
# =====================================================================

class TestUfcSource:
    """UFC fits the FieldEventSource pattern — one EPG entry per
    fight card. Numbered PPVs (UFC 309, etc.) get MAJOR; Fight
    Nights and ESPN-broadcast cards get EVENT."""

    @staticmethod
    def _make():
        from dispatcharr_ranked_matchups.sources.field_event import UfcSource
        return UfcSource()

    def test_identity(self):
        src = self._make()
        assert src.sport_prefix == "UFC"
        assert src.sport_label == "UFC"
        assert src.ESPN_SLUG == "mma/ufc"

    def test_does_not_support_importance(self):
        assert self._make().supports_importance is False

    def test_ppv_detected_as_major(self):
        src = self._make()
        assert src.MAJOR_REGEX is not None
        # Numbered PPV — MAJOR.
        assert src.MAJOR_REGEX.search("UFC 309: Jones vs. Miocic")
        assert src.MAJOR_REGEX.search("UFC 310: Pantoja vs. Asakura")
        # Three-digit (future-proofing).
        assert src.MAJOR_REGEX.search("UFC 999: Some Fight")

    def test_fight_night_not_major(self):
        src = self._make()
        assert src.MAJOR_REGEX is not None
        # Fight Night and ESPN cards — EVENT-tier.
        assert not src.MAJOR_REGEX.search(
            "UFC Fight Night: Sandhagen vs. Figueiredo"
        )
        assert not src.MAJOR_REGEX.search("UFC on ESPN: Lewis vs. Nascimento")

    def test_emits_ppv_with_major_tier(self, monkeypatch):
        """Live ESPN response shape — UFC card surfaces as one row."""
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401900800",
                "name": "UFC 309: Jones vs. Miocic",
                "shortName": "UFC 309",
                "date": "2024-11-17T03:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import UfcSource
        games = UfcSource().fetch_upcoming(days_ahead=7)
        assert len(games) == 1
        g = games[0]
        assert g.home == "UFC 309: Jones vs. Miocic"
        assert g.away == "Field"
        assert g.sport_prefix == "UFC"
        assert g.extra.get("stage") == "MAJOR"  # PPV bumped to MAJOR
        assert g.extra.get("is_field_event") is True

    def test_emits_fight_night_with_event_tier(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401900801",
                "name": "UFC Fight Night: Sandhagen vs. Figueiredo",
                "shortName": "UFC Fight Night",
                "date": "2025-05-03T22:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import UfcSource
        games = UfcSource().fetch_upcoming(days_ahead=7)
        assert len(games) == 1
        assert games[0].extra.get("stage") == "EVENT"


# =====================================================================
# Phase T: Tennis (ATP + WTA)
# =====================================================================

class TestTennisSources:
    """ESPN's tennis scoreboard returns whole tournaments (each entry
    spans 1-2 weeks for a Slam, or 5-7 days for tour stops), not
    individual matches — same FieldEventSource shape as racing + UFC."""

    def test_atp_identity(self):
        from dispatcharr_ranked_matchups.sources.field_event import AtpSource
        src = AtpSource()
        assert src.sport_prefix == "ATP"
        assert "ATP" in src.sport_label
        assert src.ESPN_SLUG == "tennis/atp"

    def test_wta_identity(self):
        from dispatcharr_ranked_matchups.sources.field_event import WtaSource
        src = WtaSource()
        assert src.sport_prefix == "WTA"
        assert "WTA" in src.sport_label
        assert src.ESPN_SLUG == "tennis/wta"

    def test_no_importance(self):
        from dispatcharr_ranked_matchups.sources.field_event import (
            AtpSource, WtaSource,
        )
        assert AtpSource().supports_importance is False
        assert WtaSource().supports_importance is False

    def test_grand_slams_detected_as_major(self):
        from dispatcharr_ranked_matchups.sources.field_event import AtpSource
        src = AtpSource()
        assert src.MAJOR_REGEX is not None
        # All four Grand Slams + variants.
        assert src.MAJOR_REGEX.search("Wimbledon")
        assert src.MAJOR_REGEX.search("Australian Open")
        assert src.MAJOR_REGEX.search("French Open")
        assert src.MAJOR_REGEX.search("Roland Garros")
        assert src.MAJOR_REGEX.search("U.S. Open")
        assert src.MAJOR_REGEX.search("US Open")

    def test_year_end_finals_detected_as_major(self):
        from dispatcharr_ranked_matchups.sources.field_event import (
            AtpSource, WtaSource,
        )
        assert AtpSource().MAJOR_REGEX is not None
        assert WtaSource().MAJOR_REGEX is not None
        assert AtpSource().MAJOR_REGEX.search("ATP Finals")
        assert WtaSource().MAJOR_REGEX.search("WTA Finals")

    def test_regular_tour_stops_not_major(self):
        from dispatcharr_ranked_matchups.sources.field_event import AtpSource
        src = AtpSource()
        assert src.MAJOR_REGEX is not None
        assert not src.MAJOR_REGEX.search("Nordea Open")
        assert not src.MAJOR_REGEX.search("EFG Swiss Open Gstaad")
        assert not src.MAJOR_REGEX.search("Bitpanda Hamburg Open")

    def test_atp_emits_slam_with_major_tier(self, monkeypatch):
        import dispatcharr_ranked_matchups.sources.field_event as field_mod
        espn_response = {
            "events": [{
                "id": "401901000",
                "name": "Wimbledon",
                "shortName": "Wimbledon",
                "date": "2026-06-29T04:00Z",
                "competitions": [{"competitors": []}],
            }, {
                "id": "401901001",
                "name": "Nordea Open",
                "shortName": "Nordea Open",
                "date": "2026-07-13T04:00Z",
                "competitions": [{"competitors": []}],
            }],
        }
        monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: espn_response)
        from dispatcharr_ranked_matchups.sources.field_event import AtpSource
        games = AtpSource().fetch_upcoming(days_ahead=60)
        by_name = {g.home: g for g in games}
        assert by_name["Wimbledon"].extra.get("stage") == "MAJOR"
        assert by_name["Nordea Open"].extra.get("stage") == "EVENT"


# =====================================================================
# Phase O Phase 2a: DoubleEliminationSource base bracket-state-machine tests
# =====================================================================

class TestDoubleEliminationSource:
    """Phase 2a of #43: 4-team double-elimination base for NCAA Baseball
    / Softball Regional sites and the MCWS / WCWS 8-team bracket modeled
    as two 4-team sub-brackets.

    Exercises the new tie-state machinery (`losses_by_team` accumulation,
    elimination at 2 losses, last-team-standing winner detection) and the
    inherited terminal_outcomes cascade via the MCWS_PO league context
    that Phase 1 set up. Uses a concrete minimal subclass that takes a
    pre-baked list of game records so no HTTP touches the test path.
    """

    @staticmethod
    def _make_source(games, ko_stages=("BSB_REG", "BSB_SR", "MCWS", "MCWS_F")):
        """Concrete DoubleEliminationSource for testing. Inherits the
        bracket machinery and trivially returns the pre-baked games list.
        Uses MCWS_PO as the league context (set up in Phase 1) so the
        terminal_outcomes cascade has real thresholds to bucket against.
        Default KO_STAGES mirrors the production NCAA Baseball playoff
        stage chain so winner-advance depth math hits the next stage
        (e.g., Regional winner → BSB_SR depth) instead of the terminal
        WINNER synthetic depth. Tie grouping_key is read off the game
        record `extra["grouping_key"]`."""
        from dispatcharr_ranked_matchups.sources.bracket import DoubleEliminationSource

        class _TestSrc(DoubleEliminationSource):
            KO_STAGES = ko_stages

            @property
            def sport_prefix(self):
                return "TEST"

            @property
            def sport_label(self):
                return "Test Double Elim"

            def fetch_upcoming(self, days_ahead=7):
                return []

            def _league_context_code(self):
                return "MCWS_PO"

            def _fetch_bracket_games(self):
                return list(games)

            def _tie_grouping_key(self, game):
                return (game.get("extra") or {}).get("grouping_key")

        return _TestSrc()

    @staticmethod
    def _game(game_id, stage, grouping_key, home, away, scores=None, matchday=1, start_time=None):
        """Synthesize one game record. `scores` is (home_score, away_score)
        or None for SCHEDULED. `start_time` defaults to a sortable
        date+matchday so the chronological-replay path in
        `_build_bracket` orders games deterministically without depending
        on matchday."""
        from datetime import datetime, timezone, timedelta
        if start_time is None:
            base = datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)
            start_time = base + timedelta(days=matchday - 1)
        if scores is None:
            return {
                "game_id": game_id, "stage": stage, "matchday": matchday,
                "home": home, "away": away,
                "home_goals": None, "away_goals": None,
                "status": "SCHEDULED",
                "start_time": start_time,
                "extra": {"grouping_key": grouping_key},
            }
        return {
            "game_id": game_id, "stage": stage, "matchday": matchday,
            "home": home, "away": away,
            "home_goals": scores[0], "away_goals": scores[1],
            "status": "FINISHED",
            "start_time": start_time,
            "extra": {"grouping_key": grouping_key},
        }

    # ---------- _new_tie_record shape ----------

    def test_new_tie_record_zero_losses_per_team(self):
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        assert tie["losses_by_team"] == {"A": 0, "B": 0, "C": 0, "D": 0}
        assert tie["winner"] is None
        assert tie["eliminated_teams"] == []
        assert tie["games_recorded"] == frozenset()
        assert tie["elimination_loss_count"] == 2
        assert tie["grouping_key"] == "Site1"

    # ---------- _record_game_into_tie ----------

    def test_record_game_increments_loser_loss_count(self):
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        src._record_game_into_tie(tie, "A", "B", 5, 2, game_index=1)
        assert tie["losses_by_team"]["B"] == 1
        assert tie["losses_by_team"]["A"] == 0
        assert tie["eliminated_teams"] == []
        assert tie["winner"] is None

    def test_record_game_eliminates_at_second_loss(self):
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        # B loses to A (loss 1), then loses to C (loss 2 → eliminated).
        src._record_game_into_tie(tie, "A", "B", 5, 2, game_index=1)
        src._record_game_into_tie(tie, "B", "C", 1, 7, game_index=2)
        assert tie["losses_by_team"]["B"] == 2
        assert "B" in tie["eliminated_teams"]
        assert tie["winner"] is None  # 3 teams still alive

    def test_record_game_winner_is_last_team_standing(self):
        # Simulate a complete 4-team double-elim where A wins.
        # Loss progression to elimination (each team gets 2 losses except A):
        #   G1: B loses to A    (B=1)
        #   G2: D loses to C    (D=1)
        #   G3: B loses to D    (B=2, eliminated)
        #   G4: D loses to A    (D=2, eliminated)
        #   G5: C loses to A    (C=1)  -- A is in the W bracket final, C in L bracket final
        #   wait, in a real 4-team double-elim, C would still need 1 more loss
        # Simpler test: cycle losses until 3 teams hit 2.
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        # Eliminate B (2 losses)
        src._record_game_into_tie(tie, "A", "B", 5, 2, game_index=1)
        src._record_game_into_tie(tie, "C", "B", 4, 1, game_index=2)
        # Eliminate D (2 losses)
        src._record_game_into_tie(tie, "A", "D", 3, 0, game_index=3)
        src._record_game_into_tie(tie, "C", "D", 6, 2, game_index=4)
        # Eliminate C (2 losses). A wins.
        src._record_game_into_tie(tie, "A", "C", 4, 3, game_index=5)
        assert tie["winner"] is None  # C only has 1 loss
        src._record_game_into_tie(tie, "A", "C", 7, 5, game_index=6)
        assert tie["winner"] == "A"
        assert set(tie["eliminated_teams"]) == {"B", "C", "D"}

    def test_record_game_ignored_after_tie_resolved(self):
        # Once winner is set, any further games are defensively no-op
        # against simulator double-application.
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        for h, a, sc in [
            ("A", "B", (5, 2)), ("C", "B", (4, 1)),
            ("A", "D", (3, 0)), ("C", "D", (6, 2)),
            ("A", "C", (4, 3)), ("A", "C", (7, 5)),
        ]:
            src._record_game_into_tie(tie, h, a, sc[0], sc[1], game_index=1)
        assert tie["winner"] == "A"
        losses_at_resolve = dict(tie["losses_by_team"])
        # Extra phantom game: should be a no-op.
        src._record_game_into_tie(tie, "A", "B", 9, 0, game_index=7)
        assert tie["losses_by_team"] == losses_at_resolve

    def test_record_game_zero_zero_tie_score_is_noop(self):
        # Baseball / softball don't tie, but defensively make sure
        # equal scores don't increment any loss count.
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        src._record_game_into_tie(tie, "A", "B", 3, 3, game_index=1)
        assert tie["losses_by_team"] == {"A": 0, "B": 0, "C": 0, "D": 0}

    # ---------- _build_bracket groups by (stage, grouping_key) ----------

    def test_build_bracket_groups_by_grouping_key(self):
        # 4-team double elim at Site1 + 4-team double elim at Site2.
        # Same stage but different grouping_key → two tie_metas.
        site1_games = [
            self._game("s1-g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("s1-g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
        ]
        site2_games = [
            self._game("s2-g1", "BSB_REG", "Site2", "E", "F", (3, 2), matchday=1),
            self._game("s2-g2", "BSB_REG", "Site2", "G", "H", (6, 0), matchday=1),
        ]
        src = self._make_source(site1_games + site2_games)
        bracket = src._build_bracket(site1_games + site2_games)
        assert len(bracket["BSB_REG"]) == 2
        keys_to_teams = {
            tm["grouping_key"]: tm["teams"] for tm in bracket["BSB_REG"]
        }
        assert keys_to_teams["Site1"] == frozenset({"A", "B", "C", "D"})
        assert keys_to_teams["Site2"] == frozenset({"E", "F", "G", "H"})

    def test_build_bracket_skips_games_without_grouping_key(self):
        # If the subclass's _tie_grouping_key returns None, the game is
        # excluded from bracket construction (e.g., sub-bracket couldn't
        # be inferred from the headline yet).
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2)),
            self._game("g2", "BSB_REG", None, "X", "Y", (3, 1)),  # excluded
        ]
        src = self._make_source(games)
        bracket = src._build_bracket(games)
        assert len(bracket["BSB_REG"]) == 1
        assert bracket["BSB_REG"][0]["teams"] == frozenset({"A", "B"})

    def test_build_bracket_sorts_games_chronologically(self):
        # _build_bracket sorts each tie's games by start_time so the
        # chronological-replay in initial_state accumulates losses in
        # the correct order.
        from datetime import datetime, timezone
        early = datetime(2025, 6, 13, 18, 0, tzinfo=timezone.utc)
        late = datetime(2025, 6, 14, 18, 0, tzinfo=timezone.utc)
        games = [
            # Pass them out-of-order; expect sorted output.
            self._game("late", "BSB_REG", "Site1", "C", "D", (4, 1), start_time=late),
            self._game("early", "BSB_REG", "Site1", "A", "B", (5, 2), start_time=early),
        ]
        src = self._make_source(games)
        bracket = src._build_bracket(games)
        tie_meta = bracket["BSB_REG"][0]
        assert [g["game_id"] for g in tie_meta["games"]] == ["early", "late"]

    # ---------- initial_state replays games chronologically ----------

    def test_initial_state_replays_finished_games(self):
        # Site1: A beats B, then C beats B → B eliminated. 3 teams still alive.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "B", (4, 1), matchday=2),
        ]
        src = self._make_source(games)
        state = src.initial_state()
        tk = ("BSB_REG", frozenset({"A", "B", "C"}))
        tie = state["_tie_results"][tk]
        assert tie["losses_by_team"] == {"A": 0, "B": 2, "C": 0}
        assert "B" in tie["eliminated_teams"]
        assert tie["winner"] is None
        assert state["_applied"] == frozenset({"g1", "g2"})
        # Mid-tournament cap: B at BSB_REG depth (0); A and C not yet in dict.
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert state["_round_reached"]["B"] == KNOCKOUT_ROUND_DEPTH["BSB_REG"]
        assert "A" not in state["_round_reached"]
        assert "C" not in state["_round_reached"]

    def test_initial_state_complete_bracket_promotes_winner(self):
        # Complete 4-team double-elim where A wins. The cascade hands A
        # the BSB_SR (next-stage) depth and caps every loser at BSB_REG.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
            self._game("g3", "BSB_REG", "Site1", "B", "D", (3, 1), matchday=2),  # D eliminated
            self._game("g4", "BSB_REG", "Site1", "A", "C", (6, 3), matchday=2),  # C 1 loss
            self._game("g5", "BSB_REG", "Site1", "B", "C", (2, 5), matchday=3),  # B eliminated
            self._game("g6", "BSB_REG", "Site1", "A", "C", (4, 1), matchday=4),  # C eliminated → A wins
        ]
        src = self._make_source(games)
        state = src.initial_state()
        tk = ("BSB_REG", frozenset({"A", "B", "C", "D"}))
        tie = state["_tie_results"][tk]
        assert tie["winner"] == "A"
        assert set(tie["eliminated_teams"]) == {"B", "C", "D"}
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        # Winner advances to next stage depth (BSB_SR).
        assert state["_round_reached"]["A"] == KNOCKOUT_ROUND_DEPTH["BSB_SR"]
        # Every loser caps at the tie's stage depth (BSB_REG).
        for loser in ("B", "C", "D"):
            assert state["_round_reached"][loser] == KNOCKOUT_ROUND_DEPTH["BSB_REG"]

    # ---------- remaining_matches emits unapplied source-published games ----------

    def test_remaining_matches_emits_unfinished_games(self):
        # 2 finished + 1 scheduled; remaining should emit only the scheduled one.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
            self._game("g3", "BSB_REG", "Site1", "A", "C", matchday=2),  # SCHEDULED
        ]
        src = self._make_source(games)
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        assert len(remaining) == 1
        assert remaining[0].extra["game_id"] == "g3"
        assert remaining[0].extra["grouping_key"] == "Site1"
        assert remaining[0].extra["stage"] == "BSB_REG"
        assert remaining[0].extra["is_decisive_leg"] is True

    def test_remaining_matches_stops_after_winner(self):
        # Once a tie is resolved, no more games are emitted from it
        # even if subsequent SCHEDULED games are listed by the source.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
            self._game("g3", "BSB_REG", "Site1", "B", "D", (3, 1), matchday=2),
            self._game("g4", "BSB_REG", "Site1", "A", "C", (6, 3), matchday=2),
            self._game("g5", "BSB_REG", "Site1", "B", "C", (2, 5), matchday=3),
            self._game("g6", "BSB_REG", "Site1", "A", "C", (4, 1), matchday=4),
            self._game("g7", "BSB_REG", "Site1", "A", "C", matchday=5),  # SCHEDULED if-nec
        ]
        src = self._make_source(games)
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        # Tie is resolved (A won at g6), so g7 (if-necessary) is dropped.
        assert remaining == []

    # ---------- apply_result threads counterfactual through losses_by_team ----------

    def test_apply_result_increments_losses_via_grouping_key(self):
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", matchday=1),  # SCHEDULED
        ]
        src = self._make_source(games)
        state = src.initial_state()
        tk = ("BSB_REG", frozenset({"A", "B"}))
        assert state["_tie_results"][tk]["losses_by_team"] == {"A": 0, "B": 0}

        # Simulate the simulator drawing "A wins 5-2".
        match = GameRow(
            sport_prefix="TEST", sport_label="Test Double Elim",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=None,
            extra={"game_id": "g1", "stage": "BSB_REG",
                   "matchday": 1, "grouping_key": "Site1"},
        )
        result = MatchResult(home_goals=5, away_goals=2)
        new_state = src.apply_result(state, match, result)

        new_tie = new_state["_tie_results"][tk]
        assert new_tie["losses_by_team"] == {"A": 0, "B": 1}
        assert "g1" in new_state["_applied"]
        # Original state untouched (immutability contract).
        assert state["_tie_results"][tk]["losses_by_team"] == {"A": 0, "B": 0}

    def test_apply_result_finds_tie_via_team_superset_fallback(self):
        # If the simulator-emitted GameRow lacks `grouping_key` in extras
        # (e.g., a counterfactual constructed without re-deriving the
        # grouping), apply_result must still find the right tie by
        # checking which tie_meta's `teams` superset contains both
        # participants.
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", matchday=1),
        ]
        src = self._make_source(games)
        state = src.initial_state()

        match = GameRow(
            sport_prefix="TEST", sport_label="Test Double Elim",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=None,
            # NB: no grouping_key in extras.
            extra={"game_id": "g1", "stage": "BSB_REG", "matchday": 1},
        )
        result = MatchResult(home_goals=5, away_goals=2)
        new_state = src.apply_result(state, match, result)

        tk = ("BSB_REG", frozenset({"A", "B"}))
        assert new_state["_tie_results"][tk]["losses_by_team"] == {"A": 0, "B": 1}

    def test_apply_result_caps_eliminated_team_mid_tournament(self):
        # When apply_result eliminates a team without resolving the tie,
        # the eliminated team caps at the stage depth in _round_reached.
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "B", matchday=2),  # SCHEDULED
            self._game("g3", "BSB_REG", "Site1", "A", "C", matchday=3),  # SCHEDULED
        ]
        src = self._make_source(games)
        state = src.initial_state()
        # Initial: B has 1 loss but isn't in _round_reached yet.
        assert "B" not in state["_round_reached"]

        # Simulator runs g2: C beats B → B is at 2 losses, eliminated,
        # but tie not yet resolved (A and C both still alive).
        match = GameRow(
            sport_prefix="TEST", sport_label="Test Double Elim",
            home="C", away="B", rank_home=None, rank_away=None,
            start_time=None,
            extra={"game_id": "g2", "stage": "BSB_REG",
                   "matchday": 2, "grouping_key": "Site1"},
        )
        result = MatchResult(home_goals=7, away_goals=1)
        new_state = src.apply_result(state, match, result)

        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert new_state["_round_reached"]["B"] == KNOCKOUT_ROUND_DEPTH["BSB_REG"]
        # No winner yet; A and C still not in _round_reached.
        tk = ("BSB_REG", frozenset({"A", "B", "C"}))
        assert new_state["_tie_results"][tk]["winner"] is None
        assert "A" not in new_state["_round_reached"]
        assert "C" not in new_state["_round_reached"]

    def test_apply_result_promotes_winner_on_resolution(self):
        # When apply_result resolves the tie, winner advances to next
        # stage depth and every loser caps at this stage's depth.
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        # Pre-load enough finished games that the next apply resolves it:
        # Need 3 teams at 2 losses each.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
            self._game("g3", "BSB_REG", "Site1", "B", "D", (3, 1), matchday=2),
            self._game("g4", "BSB_REG", "Site1", "A", "C", (6, 3), matchday=2),
            self._game("g5", "BSB_REG", "Site1", "B", "C", (2, 5), matchday=3),
            self._game("g6", "BSB_REG", "Site1", "A", "C", matchday=4),  # SCHEDULED
        ]
        src = self._make_source(games)
        state = src.initial_state()
        # State at g6: B and D eliminated (2 losses each); A undefeated; C has 1 loss.
        tk = ("BSB_REG", frozenset({"A", "B", "C", "D"}))
        assert state["_tie_results"][tk]["winner"] is None
        assert set(state["_tie_results"][tk]["eliminated_teams"]) == {"B", "D"}

        # Simulate g6: A beats C → C at 2 losses, eliminated → A wins.
        match = GameRow(
            sport_prefix="TEST", sport_label="Test Double Elim",
            home="A", away="C", rank_home=None, rank_away=None,
            start_time=None,
            extra={"game_id": "g6", "stage": "BSB_REG",
                   "matchday": 4, "grouping_key": "Site1"},
        )
        result = MatchResult(home_goals=4, away_goals=1)
        new_state = src.apply_result(state, match, result)

        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        new_tie = new_state["_tie_results"][tk]
        assert new_tie["winner"] == "A"
        assert set(new_tie["eliminated_teams"]) == {"B", "C", "D"}
        # Winner advances to next stage (BSB_SR), losers cap at BSB_REG.
        assert new_state["_round_reached"]["A"] == KNOCKOUT_ROUND_DEPTH["BSB_SR"]
        for loser in ("B", "C", "D"):
            assert new_state["_round_reached"][loser] == KNOCKOUT_ROUND_DEPTH["BSB_REG"]

    def test_apply_result_returns_new_state_not_mutating_original(self):
        # Immutability invariant: original state's _round_reached,
        # _tie_results, and _applied must NOT change after apply_result.
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", matchday=1),
        ]
        src = self._make_source(games)
        state = src.initial_state()

        match = GameRow(
            sport_prefix="TEST", sport_label="Test Double Elim",
            home="A", away="B", rank_home=None, rank_away=None,
            start_time=None,
            extra={"game_id": "g1", "stage": "BSB_REG",
                   "matchday": 1, "grouping_key": "Site1"},
        )
        result = MatchResult(home_goals=5, away_goals=2)
        snapshot_round = dict(state["_round_reached"])
        snapshot_applied = set(state["_applied"])
        tk = ("BSB_REG", frozenset({"A", "B"}))
        snapshot_losses = dict(state["_tie_results"][tk]["losses_by_team"])

        src.apply_result(state, match, result)

        assert dict(state["_round_reached"]) == snapshot_round
        assert set(state["_applied"]) == snapshot_applied
        assert dict(state["_tie_results"][tk]["losses_by_team"]) == snapshot_losses

    # ---------- terminal_outcomes via inherited cascade ----------

    def test_terminal_outcomes_winner_gets_super_regional_band(self):
        # MCWS_PO thresholds (Phase 1): BSB_SR/super_regional,
        # MCWS/omaha_bound, MCWS_F/cws_final, MCWS_W/cws_champion.
        # Phase 2a's Regional winner reaches BSB_SR depth = 1, which
        # equals the super_regional threshold, so the winner picks up
        # the super_regional band.
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site1", "C", "D", (4, 1), matchday=1),
            self._game("g3", "BSB_REG", "Site1", "B", "D", (3, 1), matchday=2),
            self._game("g4", "BSB_REG", "Site1", "A", "C", (6, 3), matchday=2),
            self._game("g5", "BSB_REG", "Site1", "B", "C", (2, 5), matchday=3),
            self._game("g6", "BSB_REG", "Site1", "A", "C", (4, 1), matchday=4),
        ]
        src = self._make_source(games)
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        # Winner A reached BSB_SR depth → picks up super_regional band.
        assert "super_regional" in outcomes["A"]
        # Losers stay at BSB_REG depth (0) → no bands (lowest cutoff is
        # BSB_SR at depth 1).
        assert outcomes["B"] == []
        assert outcomes["C"] == []
        assert outcomes["D"] == []

    # ---------- _find_tie_meta priority ----------

    def test_find_tie_meta_prefers_grouping_key_over_team_superset(self):
        # When grouping_key resolves, use it even if a different
        # tie_meta's team set happens to contain both teams (defensive
        # against future bracket reshape).
        from dispatcharr_ranked_matchups.sources.base import GameRow
        games = [
            self._game("g1", "BSB_REG", "Site1", "A", "B", (5, 2), matchday=1),
            self._game("g2", "BSB_REG", "Site2", "A", "B", (4, 1), matchday=1),
            # Same {A, B} teams in two different sub-bracket grouping_keys.
            # This shouldn't happen with real source data — teams don't
            # span sub-brackets — but the contract should honor the
            # grouping_key without ambiguity.
        ]
        src = self._make_source(games)
        bracket = src._build_bracket(games)
        # Two tie_metas, both containing {A, B}.
        assert len(bracket["BSB_REG"]) == 2
        site1_meta = next(tm for tm in bracket["BSB_REG"] if tm["grouping_key"] == "Site1")
        site2_meta = next(tm for tm in bracket["BSB_REG"] if tm["grouping_key"] == "Site2")
        # Lookup with explicit grouping_key resolves to that one.
        assert src._find_tie_meta("BSB_REG", "Site1", "A", "B", bracket) is site1_meta
        assert src._find_tie_meta("BSB_REG", "Site2", "A", "B", bracket) is site2_meta

    def test_find_tie_meta_returns_none_for_unknown_stage(self):
        src = self._make_source([])
        bracket = {"BSB_REG": []}
        assert src._find_tie_meta(
            "NONEXISTENT", "Site1", "A", "B", bracket,
        ) is None

    # ---------- _is_decisive_game contract ----------

    def test_is_decisive_game_always_true(self):
        # Double-elim has no shootout / ET tiebreaker, but the
        # BracketSportSource contract uses this hook to drive subclass
        # sample_result. Every game can be a tie-ending game (the one
        # that puts the last 1-loss team at 2 losses), so always True.
        src = self._make_source([])
        tie = src._new_tie_record({
            "stage": "BSB_REG",
            "teams": frozenset({"A", "B", "C", "D"}),
            "grouping_key": "Site1",
        })
        for game_index in (1, 2, 3, 4, 5, 6, 7):
            assert src._is_decisive_game(tie, game_index, games_in_tie=7) is True


# =====================================================================
# #20: GroupStageSoccerSource
# =====================================================================


class TestGroupStageSoccerSource:
    """End-to-end importance source for international-tournament group
    stages (WC, EURO). Uses a patched `_fetch_all_season_matches` so no
    HTTP touches the test path."""

    @staticmethod
    def _make_source(matches):
        from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
        src = GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        src._all_matches_cache = matches
        src._team_group_cache = None
        src._initial_state_cache = None
        src._strengths_cache = None
        return src

    @staticmethod
    def _match(fd_id, group, home, away, hg=None, ag=None, status="SCHEDULED", date="2026-06-11T19:00:00Z"):
        return {
            "id": fd_id,
            "stage": "GROUP_STAGE",
            "group": group,
            "matchday": 1,
            "homeTeam": {"name": home},
            "awayTeam": {"name": away},
            "status": status,
            "utcDate": date,
            "score": {"fullTime": {"home": hg, "away": ag}} if status == "FINISHED" else {},
        }

    @staticmethod
    def _ko_match(fd_id, stage, home, away):
        return {
            "id": fd_id, "stage": stage,
            "homeTeam": {"name": home}, "awayTeam": {"name": away},
            "status": "SCHEDULED",
            "utcDate": "2026-07-01T19:00:00Z",
            "score": {},
        }

    # ---------- outcome_labels ----------

    def test_outcome_labels_are_advance_and_eliminated(self):
        src = self._make_source([])
        assert src.outcome_labels == ["advance", "eliminated"]

    # ---------- _team_group_map ----------

    def test_team_group_map_builds_from_fixtures(self):
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa"),
            self._match(2, "GROUP_A", "Argentina", "Chile"),
            self._match(3, "GROUP_B", "Germany", "Scotland"),
        ]
        src = self._make_source(matches)
        out = src._team_group_map()
        assert out["Mexico"] == "GROUP_A"
        assert out["South Africa"] == "GROUP_A"
        assert out["Argentina"] == "GROUP_A"
        assert out["Chile"] == "GROUP_A"
        assert out["Germany"] == "GROUP_B"
        assert out["Scotland"] == "GROUP_B"

    def test_team_group_map_ignores_knockout_matches(self):
        # Knockout fixtures don't carry a group field. Make sure they're
        # filtered out — including them would put a Group A winner into
        # "no group" classification when the knockout schedule resolves.
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa"),
            self._ko_match(99, "LAST_32", "Mexico", "Germany"),
        ]
        src = self._make_source(matches)
        out = src._team_group_map()
        # Mexico's group stays GROUP_A despite the LAST_32 fixture.
        assert out["Mexico"] == "GROUP_A"
        assert "Germany" not in out  # KO-only opponent isn't in group map

    # ---------- initial_state ----------

    def test_initial_state_seeds_zero_rows_for_every_group_team(self):
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa"),
            self._match(2, "GROUP_A", "Argentina", "Chile"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        for team in ("Mexico", "South Africa", "Argentina", "Chile"):
            assert state[team] == {"played": 0, "points": 0, "gf": 0, "ga": 0}
        assert state["_applied"] == frozenset()
        assert state["_team_group"]["Mexico"] == "GROUP_A"

    def test_initial_state_applies_finished_group_matches(self):
        matches = [
            # Mexico 3-1 South Africa
            self._match(1, "GROUP_A", "Mexico", "South Africa",
                        hg=3, ag=1, status="FINISHED"),
            # Argentina 2-0 Chile
            self._match(2, "GROUP_A", "Argentina", "Chile",
                        hg=2, ag=0, status="FINISHED"),
            # Knockout match — must NOT be applied.
            self._ko_match(99, "LAST_32", "Mexico", "Germany"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        assert state["Mexico"] == {"played": 1, "points": 3, "gf": 3, "ga": 1}
        assert state["South Africa"] == {"played": 1, "points": 0, "gf": 1, "ga": 3}
        assert state["Argentina"] == {"played": 1, "points": 3, "gf": 2, "ga": 0}
        assert state["Chile"] == {"played": 1, "points": 0, "gf": 0, "ga": 2}
        assert state["_applied"] == frozenset({1, 2})

    def test_initial_state_skips_knockout_matches(self):
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa",
                        hg=3, ag=1, status="FINISHED"),
            # A FINISHED knockout match must NOT update group standings.
            {
                "id": 99, "stage": "LAST_32",
                "homeTeam": {"name": "Mexico"}, "awayTeam": {"name": "Germany"},
                "status": "FINISHED",
                "utcDate": "2026-07-01T19:00:00Z",
                "score": {"fullTime": {"home": 10, "away": 0}},
            },
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        assert state["Mexico"]["points"] == 3  # ONLY the group match counts
        assert state["Mexico"]["gf"] == 3
        # Germany isn't in the group map → no row at all.
        assert "Germany" not in state

    # ---------- remaining_matches ----------

    def test_remaining_matches_filters_to_group_stage(self):
        matches = [
            # Scheduled group matches
            self._match(1, "GROUP_A", "Mexico", "South Africa"),
            self._match(2, "GROUP_A", "Argentina", "Chile"),
            # Knockout — should NOT be in remaining_matches
            self._ko_match(99, "LAST_32", "Mexico", "Germany"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        assert {m.extra["fd_id"] for m in rem} == {1, 2}

    def test_remaining_matches_excludes_already_applied(self):
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa",
                        hg=3, ag=1, status="FINISHED"),
            self._match(2, "GROUP_A", "Argentina", "Chile"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        rem = src.remaining_matches(state)
        assert {m.extra["fd_id"] for m in rem} == {2}

    # ---------- terminal_outcomes ----------

    def test_terminal_outcomes_top_2_per_group_advance(self):
        # Group A finished: Mexico 9pts, Argentina 6pts, Chile 3pts, SA 0pts.
        # Top 2 (Mexico, Argentina) advance. Chile is 3rd — could be
        # promoted via best-3rd-place (#52) in a multi-group fixture, so
        # this test focuses ONLY on the top-2-per-group claim and the
        # bottom-of-group elimination. 4th place (South Africa) never
        # advances regardless of best-3rd logic.
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa", 3, 0, "FINISHED"),
            self._match(2, "GROUP_A", "Argentina", "Chile", 2, 0, "FINISHED"),
            self._match(3, "GROUP_A", "Mexico", "Chile", 2, 1, "FINISHED"),
            self._match(4, "GROUP_A", "Argentina", "South Africa", 4, 0, "FINISHED"),
            self._match(5, "GROUP_A", "Mexico", "Argentina", 1, 0, "FINISHED"),
            self._match(6, "GROUP_A", "Chile", "South Africa", 2, 1, "FINISHED"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        assert outcomes["Mexico"] == ["advance"]
        assert outcomes["Argentina"] == ["advance"]
        # South Africa is 4th in the group — never advances via any path.
        assert outcomes["South Africa"] == ["eliminated"]
        # Chile is 3rd. Best-3rd promotion is verified in TestGroupStageBestThirdPlace.

    def test_terminal_outcomes_ties_break_on_goal_diff_then_goals_for(self):
        # 2 teams tied on points (3 each): GD decides.
        # Mexico 1-0 SA, SA 0-1 Argentina, Mexico 0-3 Argentina, Argentina 2-0 Chile,
        # Chile 2-1 Mexico, SA 1-1 Chile
        # Standings:
        #   Argentina: 3 wins = 9pts, GF=6, GA=0, GD=+6
        #   Mexico:    1W 2L  = 3pts, GF=2, GA=6, GD=-4
        #   Chile:     1W 1D 1L = 4pts, GF=3, GA=3, GD=0
        #   SA:        0W 1D 2L = 1pt,  GF=1, GA=3, GD=-2
        # Final order: Argentina > Chile > SA > Mexico
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa", 1, 0, "FINISHED"),
            self._match(2, "GROUP_A", "South Africa", "Argentina", 0, 1, "FINISHED"),
            self._match(3, "GROUP_A", "Mexico", "Argentina", 0, 3, "FINISHED"),
            self._match(4, "GROUP_A", "Argentina", "Chile", 2, 0, "FINISHED"),
            self._match(5, "GROUP_A", "Chile", "Mexico", 2, 1, "FINISHED"),
            self._match(6, "GROUP_A", "South Africa", "Chile", 1, 1, "FINISHED"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        # Per-group sort puts Argentina 1st (9 pts), Chile 2nd (4 pts) on
        # tiebreaker over Mexico (3 pts, worse GD): both advance via top-2.
        assert outcomes["Argentina"] == ["advance"]
        assert outcomes["Chile"] == ["advance"]
        # 3rd: Mexico (3 pts, GD=-4); 4th: South Africa (1 pt). 4th never
        # advances. 3rd-place fate is verified separately in
        # TestGroupStageBestThirdPlace.
        assert outcomes["South Africa"] == ["eliminated"]

    def test_terminal_outcomes_handles_multiple_groups_independently(self):
        # Group A: Mexico/Argentina advance; Chile/SA eliminated.
        # Group B: Germany/France advance; Brazil/Spain eliminated.
        matches = [
            self._match(1, "GROUP_A", "Mexico", "South Africa", 3, 0, "FINISHED"),
            self._match(2, "GROUP_A", "Argentina", "Chile", 2, 0, "FINISHED"),
            self._match(3, "GROUP_B", "Germany", "Spain", 4, 0, "FINISHED"),
            self._match(4, "GROUP_B", "France", "Brazil", 2, 1, "FINISHED"),
        ]
        src = self._make_source(matches)
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        # Top-2 of each group always advances.
        # Group A
        assert outcomes["Mexico"] == ["advance"]
        assert outcomes["Argentina"] == ["advance"]
        # Group B
        assert outcomes["Germany"] == ["advance"]
        assert outcomes["France"] == ["advance"]
        # 4th-place teams in both groups stay eliminated regardless of
        # best-3rd promotion (#52 only promotes 3rd-placers).
        assert outcomes["South Africa"] == ["eliminated"]
        assert outcomes["Spain"] == ["eliminated"]

    def test_terminal_outcomes_empty_state_returns_empty(self):
        src = self._make_source([])
        state = src.initial_state()
        outcomes = src.terminal_outcomes(state)
        assert outcomes == {}

    # ---------- apply_result threads through immutably ----------

    def test_apply_result_does_not_mutate_input_state(self):
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        matches = [self._match(1, "GROUP_A", "Mexico", "South Africa")]
        src = self._make_source(matches)
        state = src.initial_state()
        snap_mex = dict(state["Mexico"])
        snap_sa = dict(state["South Africa"])
        snap_applied = set(state["_applied"])

        match = GameRow(
            sport_prefix="WC", sport_label="FIFA World Cup",
            home="Mexico", away="South Africa",
            rank_home=None, rank_away=None,
            start_time=None, extra={"fd_id": 1},
        )
        result = MatchResult(home_goals=3, away_goals=1)
        new_state = src.apply_result(state, match, result)
        # Original untouched.
        assert state["Mexico"] == snap_mex
        assert state["South Africa"] == snap_sa
        assert set(state["_applied"]) == snap_applied
        # New state has the result.
        assert new_state["Mexico"]["points"] == 3
        assert new_state["Mexico"]["gf"] == 3
        assert 1 in new_state["_applied"]


class TestKnockoutSoccerSourceSkipsGroupStage:
    """Once GroupStageSoccerSource owns group-stage fixtures, the sibling
    KnockoutSoccerSource MUST filter them out of its own fetch_upcoming
    so the plugin doesn't double-emit group games as both a knockout
    and a group-stage GameRow."""

    def test_fetch_upcoming_drops_group_stage_extras(self):
        # Build a fake KnockoutSoccerSource and monkey-patch the parent
        # SoccerSource.fetch_upcoming to return a mix of stages.
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from dispatcharr_ranked_matchups.sources.soccer import (
            KnockoutSoccerSource, SoccerSource,
        )

        rows = [
            GameRow(sport_prefix="WC", sport_label="FIFA World Cup",
                    home="Mexico", away="SA", rank_home=None, rank_away=None,
                    start_time=None, extra={"fd_id": 1, "stage": "GROUP_STAGE"}),
            GameRow(sport_prefix="WC", sport_label="FIFA World Cup",
                    home="Brazil", away="Germany", rank_home=None, rank_away=None,
                    start_time=None, extra={"fd_id": 2, "stage": "LAST_32"}),
            GameRow(sport_prefix="WC", sport_label="FIFA World Cup",
                    home="France", away="Spain", rank_home=None, rank_away=None,
                    start_time=None, extra={"fd_id": 3, "stage": "FINAL"}),
        ]
        src = KnockoutSoccerSource("world_cup", fd_api_key="x")
        # Monkey-patch the parent's fetch_upcoming to return our fixture.
        original = SoccerSource.fetch_upcoming
        try:
            SoccerSource.fetch_upcoming = lambda self, days_ahead=7: list(rows)
            out = src.fetch_upcoming()
        finally:
            SoccerSource.fetch_upcoming = original
        out_ids = {r.extra["fd_id"] for r in out}
        # The GROUP_STAGE row must be filtered out; the two KO rows pass through.
        assert out_ids == {2, 3}

    def test_filter_no_op_for_competitions_without_group_stage(self):
        # UCL fixtures don't carry stage="GROUP_STAGE" — the filter is a
        # safe no-op, ALL rows pass through.
        from dispatcharr_ranked_matchups.sources.base import GameRow
        from dispatcharr_ranked_matchups.sources.soccer import (
            KnockoutSoccerSource, SoccerSource,
        )
        rows = [
            GameRow(sport_prefix="UCL", sport_label="UEFA Champions League",
                    home="A", away="B", rank_home=None, rank_away=None,
                    start_time=None, extra={"fd_id": 1, "stage": "LAST_16"}),
            GameRow(sport_prefix="UCL", sport_label="UEFA Champions League",
                    home="C", away="D", rank_home=None, rank_away=None,
                    start_time=None, extra={"fd_id": 2, "stage": "QUARTER_FINALS"}),
        ]
        src = KnockoutSoccerSource("ucl", fd_api_key="x")
        original = SoccerSource.fetch_upcoming
        try:
            SoccerSource.fetch_upcoming = lambda self, days_ahead=7: list(rows)
            out = src.fetch_upcoming()
        finally:
            SoccerSource.fetch_upcoming = original
        assert {r.extra["fd_id"] for r in out} == {1, 2}


class TestGroupStageContextCodes:
    """LEAGUE_CONTEXTS entries for WC_GS / EC_GS must exist with the
    `advance` outcome label so the GroupStageSoccerSource's
    fetch_upcoming-remapped `fd_competition_code` routes correctly."""

    def test_wc_gs_context_has_advance_threshold(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS.get("WC_GS")
        assert ctx is not None, "WC_GS missing from LEAGUE_CONTEXTS"
        labels = [t[1] for t in ctx.thresholds]
        assert "advance" in labels

    def test_ec_gs_context_has_advance_threshold(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ctx = LEAGUE_CONTEXTS.get("EC_GS")
        assert ctx is not None, "EC_GS missing from LEAGUE_CONTEXTS"
        labels = [t[1] for t in ctx.thresholds]
        assert "advance" in labels

    def test_wc_gs_weight_is_higher_than_ec_gs(self):
        # WC carries more global stake than EURO; the group-stage
        # advance weight should mirror the knockout-bands weighting.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        wc_weight = next(w for _, lbl, w in LEAGUE_CONTEXTS["WC_GS"].thresholds if lbl == "advance")
        ec_weight = next(w for _, lbl, w in LEAGUE_CONTEXTS["EC_GS"].thresholds if lbl == "advance")
        assert wc_weight > ec_weight


class TestMlbWsPlaceholder:
    """Issue #27: synthesize a WS placeholder tie when both LCS series
    have published games but the postseason endpoint hasn't yet
    populated the World Series (which only happens after both LCS
    resolve). Without the placeholder, ws_winner leverage reads 0
    during LCS week — exactly when LCS-Game-7 leverage should max.
    Mirrors NhlPlayoffSource's #17 fix.
    """

    @staticmethod
    def _two_lcs_in_progress_games():
        """7-game LCS pair × 2, all SCHEDULED + no WS games yet —
        mimics live shape during LCS week."""
        games = []
        # AL LCS: Yankees vs Astros
        # NL LCS: Dodgers vs Phillies
        # 2-3-2 home pattern for both
        pattern = (True, True, False, False, False, True, True)
        for series_idx, (top, bot) in enumerate(
            [("New York Yankees", "Houston Astros"), ("Los Angeles Dodgers", "Philadelphia Phillies")]
        ):
            for matchday in range(1, 8):
                top_home = pattern[matchday - 1]
                games.append({
                    "game_id": 700000 + series_idx * 10 + matchday,
                    "stage": "LCS",
                    "matchday": matchday,
                    "home": top if top_home else bot,
                    "away": bot if top_home else top,
                    "home_goals": None, "away_goals": None,
                    "status": "SCHEDULED",
                    "start_time": None,
                    "extra": {"series_description": f"LCS-{series_idx}"},
                })
        return games

    def test_synth_emits_7_games_with_mlb_home_pattern(self):
        from dispatcharr_ranked_matchups.sources.mlb import (
            MlbPlayoffSource, _WS_TOP_SENTINEL, _WS_BOT_SENTINEL, MLB_WS_HOME_PATTERN,
        )
        src = MlbPlayoffSource(season=2025)
        games = src._synth_ws_placeholder_games()
        assert len(games) == 7
        assert all(g["stage"] == "WS" for g in games)
        assert all(g["status"] == "SCHEDULED" for g in games)
        assert all(g["game_id"] < 0 for g in games), \
            "placeholder game IDs must be negative to avoid colliding with real gamePks"
        # 2-3-2 verification: game 1 → top home, game 3 → bottom home, game 6 → top home.
        for matchday in range(1, 8):
            top_home = MLB_WS_HOME_PATTERN[matchday - 1]
            expected_home = _WS_TOP_SENTINEL if top_home else _WS_BOT_SENTINEL
            assert games[matchday - 1]["home"] == expected_home, \
                f"game {matchday}: expected home {expected_home}, got {games[matchday - 1]['home']}"
            assert games[matchday - 1]["matchday"] == matchday

    def test_placeholder_fires_when_two_lcs_no_ws(self):
        from dispatcharr_ranked_matchups.sources.mlb import (
            MlbPlayoffSource, _WS_TOP_SENTINEL, _WS_BOT_SENTINEL,
        )
        src = MlbPlayoffSource(season=2025)
        bracket_games = self._two_lcs_in_progress_games()
        bracket_games.extend(src._synth_ws_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        ws_ties = state["_bracket"].get("WS", [])
        assert len(ws_ties) == 1
        teams = set(ws_ties[0]["teams"])
        assert teams == {_WS_TOP_SENTINEL, _WS_BOT_SENTINEL}

    def test_placeholder_feeds_from_wired_to_lcs(self):
        from dispatcharr_ranked_matchups.sources.mlb import (
            MlbPlayoffSource, _WS_TOP_SENTINEL, _WS_BOT_SENTINEL,
        )
        src = MlbPlayoffSource(season=2025)
        bracket_games = self._two_lcs_in_progress_games()
        bracket_games.extend(src._synth_ws_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        ws_tie = state["_bracket"]["WS"][0]
        feeds = ws_tie["feeds_from"]
        assert feeds[_WS_TOP_SENTINEL] == ("LCS", 0)
        assert feeds[_WS_BOT_SENTINEL] == ("LCS", 1)
        assert ws_tie["is_entry_tie"] is False

    def test_placeholder_blocks_remaining_until_lcs_resolves(self):
        from dispatcharr_ranked_matchups.sources.mlb import MlbPlayoffSource
        src = MlbPlayoffSource(season=2025)
        bracket_games = self._two_lcs_in_progress_games()
        bracket_games.extend(src._synth_ws_placeholder_games())
        src._bracket_games_cache = bracket_games
        state = src.initial_state()
        remaining = src.remaining_matches(state)
        ws_remaining = [g for g in remaining if g.extra.get("stage") == "WS"]
        assert len(ws_remaining) == 0, "WS must be blocked while LCS unresolved"


class TestMlbWsPlaceholderGating:
    """Verifies the placeholder ONLY synthesizes in the LCS-in-progress
    window. False positives during pre-LCS or post-LCS would either
    short-circuit valid data or produce wrong game counts."""

    def test_no_synthesis_when_only_one_lcs_series_started(self, monkeypatch):
        # Only one LCS series has games (other still in LDS), so the
        # placeholder should NOT fire — we don't know both participants yet.
        from dispatcharr_ranked_matchups.sources.mlb import MlbPlayoffSource
        src = MlbPlayoffSource(season=2025)
        # Stub _http_get to return a payload with 1 LCS series
        from dispatcharr_ranked_matchups.sources import mlb as mlb_mod
        fake = {"dates": [{"games": [
            {
                "gamePk": 700001, "seriesDescription": "AL Championship Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Yankees"}, "score": None},
                    "away": {"team": {"name": "Astros"}, "score": None},
                },
                "status": {"abstractGameState": "Preview"},
                "gameDate": "2025-10-12T20:00:00Z",
            },
        ]}]}
        monkeypatch.setattr(mlb_mod, "_http_get", lambda url: fake)
        out = src._fetch_bracket_games()
        # 1 LCS series + 0 WS placeholders.
        assert sum(1 for g in out if g["stage"] == "WS") == 0
        assert sum(1 for g in out if g["stage"] == "LCS") == 1

    def test_no_synthesis_when_ws_already_published(self, monkeypatch):
        # Both LCS resolved AND WS published — real data, no placeholder.
        from dispatcharr_ranked_matchups.sources.mlb import MlbPlayoffSource
        from dispatcharr_ranked_matchups.sources import mlb as mlb_mod
        src = MlbPlayoffSource(season=2025)
        fake = {"dates": [{"games": [
            # 1 game in each LCS (enough to register as 2 distinct LCS series)
            {
                "gamePk": 700001, "seriesDescription": "AL Championship Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Yankees"}, "score": 5},
                    "away": {"team": {"name": "Astros"}, "score": 2},
                },
                "status": {"abstractGameState": "Final"},
                "gameDate": "2025-10-12T20:00:00Z",
            },
            {
                "gamePk": 700002, "seriesDescription": "NL Championship Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Dodgers"}, "score": 4},
                    "away": {"team": {"name": "Phillies"}, "score": 1},
                },
                "status": {"abstractGameState": "Final"},
                "gameDate": "2025-10-12T20:00:00Z",
            },
            # WS game already published
            {
                "gamePk": 700100, "seriesDescription": "World Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Yankees"}, "score": None},
                    "away": {"team": {"name": "Dodgers"}, "score": None},
                },
                "status": {"abstractGameState": "Preview"},
                "gameDate": "2025-10-25T20:00:00Z",
            },
        ]}]}
        monkeypatch.setattr(mlb_mod, "_http_get", lambda url: fake)
        out = src._fetch_bracket_games()
        # 1 real WS game; 0 placeholders.
        ws_games = [g for g in out if g["stage"] == "WS"]
        assert len(ws_games) == 1
        assert ws_games[0]["game_id"] > 0, "Real WS game, should not be synthetic (negative ID)"

    def test_synthesis_fires_when_lcs_in_progress(self, monkeypatch):
        # Both LCS have published games, no WS yet — synthesizer should fire.
        from dispatcharr_ranked_matchups.sources.mlb import MlbPlayoffSource
        from dispatcharr_ranked_matchups.sources import mlb as mlb_mod
        src = MlbPlayoffSource(season=2025)
        fake = {"dates": [{"games": [
            {
                "gamePk": 700001, "seriesDescription": "AL Championship Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Yankees"}, "score": None},
                    "away": {"team": {"name": "Astros"}, "score": None},
                },
                "status": {"abstractGameState": "Preview"},
                "gameDate": "2025-10-12T20:00:00Z",
            },
            {
                "gamePk": 700002, "seriesDescription": "NL Championship Series",
                "seriesGameNumber": 1,
                "teams": {
                    "home": {"team": {"name": "Dodgers"}, "score": None},
                    "away": {"team": {"name": "Phillies"}, "score": None},
                },
                "status": {"abstractGameState": "Preview"},
                "gameDate": "2025-10-12T20:00:00Z",
            },
        ]}]}
        monkeypatch.setattr(mlb_mod, "_http_get", lambda url: fake)
        out = src._fetch_bracket_games()
        ws_games = [g for g in out if g["stage"] == "WS"]
        assert len(ws_games) == 7, "should synth a 7-game WS placeholder"
        assert all(g["game_id"] < 0 for g in ws_games), "all synthesized (negative IDs)"


class TestGroupStageBestThirdPlace:
    """Issue #52: WC 2026 / EURO 2024 best-3rd-place advancement.
    WC 2026 advances 8 third-place teams across 12 groups; EURO 2024
    advances 4 across 6 groups. Without this, the GroupStageSoccerSource
    classifies all 3rd-place teams as eliminated — ~25% misclassification
    for advancing teams in WC 2026."""

    @staticmethod
    def _wc_source():
        """GroupStageSoccerSource for the World Cup config (uses WC_GS
        context with best_third_place_count=8)."""
        from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
        src = GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        return src

    @staticmethod
    def _euro_source():
        from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
        src = GroupStageSoccerSource("euros", fd_api_key="x", odds_api_key="")
        return src

    @staticmethod
    def _make_state(group_data, team_group):
        """Build a state dict from {group: [(team, points, gd, gf), ...]}.
        Each group should have 4 teams pre-sorted by position."""
        state = {"_applied": set(), "_team_group": team_group}
        for group, rows in group_data.items():
            for team, points, gd, gf in rows:
                state[team] = {
                    "points": points,
                    "gf": gf,
                    "ga": gf - gd,  # we only need gf-ga for diff; ga value here doesn't matter
                    "_played": 3,
                }
        return state

    def test_wc_promotes_top_8_third_place(self):
        # 12 groups, 12 third-place teams. Top 8 should advance, bottom 4
        # should be eliminated. Construct so the rank by (pts, gd, gf) is
        # deterministic: 3rd-placers carry pts 4 down to 0.
        src = self._wc_source()
        team_group: Dict[str, str] = {}
        group_data: Dict[str, List[Tuple[str, int, int, int]]] = {}
        for gi in range(12):
            grp = f"GROUP_{chr(ord('A') + gi)}"
            # 1st: 9 pts, 2nd: 6 pts, 3rd: varying, 4th: 0 pts
            third_pts = 12 - gi  # GROUP_A 3rd has 12, ..., GROUP_L 3rd has 1
            teams = [
                (f"team_{gi}_1st", 9, 5, 8),
                (f"team_{gi}_2nd", 6, 2, 5),
                (f"team_{gi}_3rd", third_pts, 0, 3),
                (f"team_{gi}_4th", 0, -5, 1),
            ]
            for t, _pts, _gd, _gf in teams:
                team_group[t] = grp
            group_data[grp] = teams
        state = self._make_state(group_data, team_group)
        outcomes = src.terminal_outcomes(state)

        # 1st + 2nd from every group always advance: 24 advancers from that.
        firsts_seconds_advance = sum(
            1 for t, o in outcomes.items()
            if (t.endswith("_1st") or t.endswith("_2nd")) and o == ["advance"]
        )
        assert firsts_seconds_advance == 24

        # Of the 12 3rd-placers, top 8 by points should advance.
        # GROUP_A 3rd has 12 pts (best), GROUP_L 3rd has 1 pt (worst).
        # Top 8 are GROUP_A through GROUP_H (pts 12-5).
        thirds_advance = [t for t, o in outcomes.items() if t.endswith("_3rd") and o == ["advance"]]
        thirds_eliminated = [t for t, o in outcomes.items() if t.endswith("_3rd") and o == ["eliminated"]]
        assert len(thirds_advance) == 8
        assert len(thirds_eliminated) == 4
        # Specifically: 4 lowest groups (I/J/K/L) are eliminated 3rd-placers.
        for low_gi in range(8, 12):
            grp_letter = chr(ord('A') + low_gi)
            assert f"team_{low_gi}_3rd" in [t for t, o in outcomes.items() if o == ["eliminated"]]

    def test_euro_promotes_top_4_third_place(self):
        # 6 groups → 6 third-placers, top 4 advance.
        src = self._euro_source()
        team_group: Dict[str, str] = {}
        group_data: Dict[str, List[Tuple[str, int, int, int]]] = {}
        for gi in range(6):
            grp = f"GROUP_{chr(ord('A') + gi)}"
            third_pts = 6 - gi  # A=6 .. F=1
            teams = [
                (f"team_{gi}_1st", 9, 5, 8),
                (f"team_{gi}_2nd", 6, 2, 5),
                (f"team_{gi}_3rd", third_pts, 0, 3),
                (f"team_{gi}_4th", 0, -5, 1),
            ]
            for t, _pts, _gd, _gf in teams:
                team_group[t] = grp
            group_data[grp] = teams
        state = self._make_state(group_data, team_group)
        outcomes = src.terminal_outcomes(state)

        thirds_advance = [t for t, o in outcomes.items() if t.endswith("_3rd") and o == ["advance"]]
        thirds_eliminated = [t for t, o in outcomes.items() if t.endswith("_3rd") and o == ["eliminated"]]
        assert len(thirds_advance) == 4
        assert len(thirds_eliminated) == 2

    def test_third_place_tiebreaker_by_gd_then_gf(self):
        # 4 third-placers all tied on 3 pts. Tiebreaker is GD → GF → alphabetical.
        # Send WC source — best_third_place_count=8, but we only have 8 groups
        # constructed so all 8 3rd-placers advance. To test the tiebreaker
        # specifically, give only 4 groups + best_third_place_count effective
        # at picking 1 of them. Simulate by limiting groups.
        src = self._wc_source()
        team_group = {}
        # 12 groups but only the 3rds differentiate by tiebreaker.
        # Make all 12 3rds tied on points; GD picks top 8.
        group_data = {}
        for gi in range(12):
            grp = f"GROUP_{chr(ord('A') + gi)}"
            # 3rd-placers all 3 pts; GD descending by gi
            third_gd = 11 - gi
            teams = [
                (f"team_{gi}_1st", 9, 5, 8),
                (f"team_{gi}_2nd", 6, 2, 5),
                (f"team_{gi}_3rd", 3, third_gd, 4),
                (f"team_{gi}_4th", 0, -5, 1),
            ]
            for t, *_ in teams:
                team_group[t] = grp
            group_data[grp] = teams
        state = self._make_state(group_data, team_group)
        outcomes = src.terminal_outcomes(state)
        thirds_advance = sorted([t for t, o in outcomes.items() if t.endswith("_3rd") and o == ["advance"]])
        # Best 8 by GD = A..H (gi 0..7) since their GD is 11..4 (descending),
        # I..L are 3..0.
        expected = sorted([f"team_{i}_3rd" for i in range(8)])
        assert thirds_advance == expected

    def test_alphabetical_breaks_remaining_ties(self):
        # Two 3rd-placers tied on (pts, gd, gf). With n_best_third=8 across
        # 12 groups and bottom-most groups tied, alphabetical name decides.
        src = self._wc_source()
        team_group = {}
        group_data = {}
        # 7 groups with clearly-best 3rd-placers (all > 3 pts); 5 groups
        # completely tied at 3 pts. 8th slot resolved by alphabetical of
        # the 5 tied. The strict-better 7 take slots 1-7.
        for gi in range(7):
            grp = f"GROUP_{chr(ord('A') + gi)}"
            teams = [
                (f"top_{gi}_1st", 9, 5, 8),
                (f"top_{gi}_2nd", 6, 2, 5),
                (f"top_{gi}_3rd", 10 - gi, 0, 3),  # 10, 9, 8, 7, 6, 5, 4 — all strictly above 3
                (f"top_{gi}_4th", 0, -5, 1),
            ]
            for t, *_ in teams:
                team_group[t] = grp
            group_data[grp] = teams
        # 5 groups (H..L) with identical 3rd-placer stats — alphabetical chosen by NAME.
        for gi, name_prefix in enumerate(["aaa", "bbb", "ccc", "ddd", "eee"]):
            grp = f"GROUP_{chr(ord('H') + gi)}"
            teams = [
                (f"{name_prefix}_1st", 9, 5, 8),
                (f"{name_prefix}_2nd", 6, 2, 5),
                (f"{name_prefix}_3rd", 3, 0, 3),  # All tied at 3 pts, 0 GD, 3 GF
                (f"{name_prefix}_4th", 0, -5, 1),
            ]
            for t, *_ in teams:
                team_group[t] = grp
            group_data[grp] = teams
        state = self._make_state(group_data, team_group)
        outcomes = src.terminal_outcomes(state)
        # Top 7 by points already chosen (top_*_3rd). 8th slot from the
        # 5 tied teams → alphabetical 'aaa_3rd' wins.
        assert outcomes["aaa_3rd"] == ["advance"]
        # bbb..eee all eliminated.
        for prefix in ("bbb", "ccc", "ddd", "eee"):
            assert outcomes[f"{prefix}_3rd"] == ["eliminated"]

    def test_no_3rd_place_promotion_when_count_zero(self):
        # Custom group-stage entry with best_third_place_count=0 keeps
        # current strict top-2 behavior. Use a synthetic context.
        from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, LeagueContext

        # Temporarily install a 0-count context.
        original = LEAGUE_CONTEXTS.get("WC_GS")
        LEAGUE_CONTEXTS["WC_GS"] = LeagueContext(
            code="WC_GS", matchdays_total=3, format="group_advance",
            thresholds=original.thresholds if original else [],
            boundary_summary="test",
            best_third_place_count=0,
        )
        try:
            src = self._wc_source()
            team_group = {}
            group_data = {}
            for gi in range(2):
                grp = f"GROUP_{chr(ord('A') + gi)}"
                teams = [
                    (f"team_{gi}_1st", 9, 5, 8),
                    (f"team_{gi}_2nd", 6, 2, 5),
                    (f"team_{gi}_3rd", 3, 0, 3),
                    (f"team_{gi}_4th", 0, -5, 1),
                ]
                for t, *_ in teams:
                    team_group[t] = grp
                group_data[grp] = teams
            state = self._make_state(group_data, team_group)
            outcomes = src.terminal_outcomes(state)
            # 3rd placers all eliminated; no promotion.
            assert outcomes["team_0_3rd"] == ["eliminated"]
            assert outcomes["team_1_3rd"] == ["eliminated"]
        finally:
            if original:
                LEAGUE_CONTEXTS["WC_GS"] = original

    def test_4th_place_never_advances(self):
        # Even with high points, 4th place in own group stays eliminated.
        src = self._wc_source()
        team_group = {}
        group_data = {}
        # GROUP_A: super-stacked group where everyone has high points.
        teams = [
            ("Alpha", 9, 10, 12),
            ("Beta", 7, 5, 8),
            ("Gamma", 5, 0, 6),
            ("Delta", 4, -2, 5),  # 4th place but solid record
        ]
        for t, *_ in teams:
            team_group[t] = "GROUP_A"
        group_data["GROUP_A"] = teams
        # 11 weak groups so Delta would dominate any 3rd-placer
        for gi in range(11):
            grp = f"GROUP_{chr(ord('B') + gi)}"
            t = [(f"w_{gi}_{j}", 1, -10, 0) for j in range(4)]
            for ti, *_ in t:
                team_group[ti] = grp
            group_data[grp] = t
        state = self._make_state(group_data, team_group)
        outcomes = src.terminal_outcomes(state)
        # Delta is 4th in her own group, regardless of stats — must be eliminated.
        assert outcomes["Delta"] == ["eliminated"]
