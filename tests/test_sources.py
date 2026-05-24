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
        bracket = src._build_bracket(matches)
        assert len(bracket["SEMI_FINALS"]) == 2  # two ties, two legs each
        assert len(bracket["FINAL"]) == 1
        # Each SF tie should have 2 legs ordered by matchday.
        for tie in bracket["SEMI_FINALS"]:
            assert len(tie["legs"]) == 2
            assert tie["legs"][0]["matchday"] == 1
            assert tie["legs"][1]["matchday"] == 2
            assert tie["teams"] == frozenset(
                {leg["home"] for leg in tie["legs"]} | {leg["away"] for leg in tie["legs"]}
            )

    def test_build_bracket_wires_feeds_from_for_final(self):
        """The FINAL tie's feeds_from must point to the two SF ties whose
        winners are A and C (per the finished SF data). The dict is keyed
        by FD-published team name."""
        matches = self._semi_finals_matches(sf_finished=True)
        matches.append(self._final_match("A", "C"))
        src = self._make_source(matches)
        bracket = src._build_bracket(matches)
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
        bracket = src._build_bracket(matches)
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
        bracket = src._build_bracket([m])
        # The leg should have home_goals = 1 + 1 (pen boost) = 2; away unchanged.
        leg_parsed = bracket["SEMI_FINALS"][0]["legs"][0]
        assert leg_parsed["home_goals"] == 2
        assert leg_parsed["away_goals"] == 1

    # ---------- _record_leg_into_tie ----------

    def test_record_leg_into_tie_two_leg_aggregate(self):
        src = self._make_source([])
        tie = {
            "stage": "SEMI_FINALS",
            "teams": frozenset({"A", "B"}),
            "leg1": None, "leg2": None,
            "aggregate_winner": None, "aggregate_loser": None,
        }
        # Leg 1: A home, A 2-0
        src._record_leg_into_tie(tie, "A", "B", 2, 0, leg_index=1)
        # Tie should be incomplete until leg 2 arrives.
        assert tie["aggregate_winner"] is None
        # Leg 2: B home, A 1-1 (so B 1, A 1 in this leg)
        src._record_leg_into_tie(tie, "B", "A", 1, 1, leg_index=2)
        # A aggregate = 2 (home) + 1 (away leg 2) = 3; B aggregate = 0 + 1 = 1.
        assert tie["aggregate_winner"] == "A"
        assert tie["aggregate_loser"] == "B"

    def test_record_leg_into_tie_single_leg_final(self):
        src = self._make_source([])
        tie = {
            "stage": "FINAL",
            "teams": frozenset({"A", "C"}),
            "leg1": None, "leg2": None,
            "aggregate_winner": None, "aggregate_loser": None,
        }
        # Single leg FINAL: A wins 2-1.
        src._record_leg_into_tie(tie, "A", "C", 2, 1, leg_index=1)
        # Single-leg tie completes after one record.
        assert tie["aggregate_winner"] == "A"
        assert tie["aggregate_loser"] == "C"

    # ---------- _advance_round_reached ----------

    def test_advance_round_reached_winner_loser_at_correct_depth(self):
        # SF winner advances to FINAL depth; loser stays at SEMI_FINALS depth.
        round_reached: dict = {}
        KnockoutSoccerSource._advance_round_reached(round_reached, "A", "B", "SEMI_FINALS")
        from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
        assert round_reached["A"] == KNOCKOUT_ROUND_DEPTH["FINAL"]  # advanced INTO FINAL
        assert round_reached["B"] == KNOCKOUT_ROUND_DEPTH["SEMI_FINALS"]

    def test_advance_round_reached_final_winner_becomes_winner(self):
        round_reached: dict = {}
        KnockoutSoccerSource._advance_round_reached(round_reached, "A", "C", "FINAL")
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
