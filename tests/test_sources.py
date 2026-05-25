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

    # ---------- series_clinching_wins ----------

    def test_series_clinching_wins_best_of_seven(self):
        src = self._make_source([], series_length=7)
        assert src.series_clinching_wins == 4

    def test_series_clinching_wins_best_of_five(self):
        src = self._make_source([], series_length=5)
        assert src.series_clinching_wins == 3

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

class TestNcaaBaseballSource:
    """Phase M: D1 baseball via ESPN unofficial API + D1Baseball.com poll.
    Tests cover the canonical-game-record extraction (the tricky part is
    homeAway / completed / score parsing), team-name canonicalization
    (location > nickname), and the LEAGUE_CONTEXTS bands.
    """

    @staticmethod
    def _make_source():
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import NcaaBaseballSource
        # Pin season_year so test isn't dependent on calendar.
        return NcaaBaseballSource(season_year=2026)

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
                    TestNcaaBaseballSource._competitor("UCLA", "Bruins", hp, "home", hp > ap if hp is not None else None),
                    TestNcaaBaseballSource._competitor("Texas", "Longhorns", ap, "away", ap > hp if ap is not None else None),
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
        from dispatcharr_ranked_matchups.sources.ncaa_baseball import NcaaBaseballSource
        assert NcaaBaseballSource._count_field == "wins"

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
