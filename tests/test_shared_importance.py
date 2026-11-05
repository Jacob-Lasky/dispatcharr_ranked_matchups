"""Shared-season importance (#230).

Importance used to re-simulate the whole remaining season for every scored
game; on a full slate that ran past the 25-minute pipeline ceiling on every
refresh. The shared path simulates each league's season once and reads every
game off it. These tests pin:
  - it estimates the SAME quantity as the per-game simulator (agreement on a
    league where the answer differs strongly between games);
  - it refuses sources that have not opted in, and reports a game missing
    from the pool as None (fall back), never as zero;
  - the opt-in is only ever set on sources whose result model ignores state
    (a CLASS guard over every source, not a list of today's classes);
  - _action_refresh's helper groups by league, skips active chains, and
    falls back on failure.
"""
from __future__ import annotations

import ast
import inspect
import random
import textwrap
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from dispatcharr_ranked_matchups import scoring, simulation
from dispatcharr_ranked_matchups.sources import base as sources_base
from dispatcharr_ranked_matchups.sources.base import GameRow
from dispatcharr_ranked_matchups.sources.points_based import PointsBasedSportSource


class _League(PointsBasedSportSource):
    """A 12-game win-count season (CFB bands: 6/8/10/11 wins) over 10 teams
    of very different strength, 4 games played. Strength spread makes
    importance differ a lot between fixtures, so rank agreement means
    something."""
    league_context_code = "CFB"

    def __init__(self, games: List[Dict[str, Any]]) -> None:
        super().__init__()
        self._games = games

    @property
    def sport_prefix(self) -> str:
        return "TST"

    @property
    def sport_label(self) -> str:
        return "Test League"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        return []

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        return self._games


def _season(seed: int = 3):
    rng = random.Random(seed)
    teams = [f"T{i}" for i in range(10)]
    power = {t: 10 + 4 * i for i, t in enumerate(teams)}  # T9 strongest
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    games, gid = [], 0
    for week in range(12):
        order = teams[:]
        rng.shuffle(order)
        for k in range(0, 10, 2):
            h, a = order[k], order[k + 1]
            played = week < 4
            hp = ap = None
            if played:
                hp, ap = rng.randint(0, power[h]), rng.randint(0, power[a])
                if hp == ap:
                    hp += 1
            games.append({"id": gid, "home": h, "away": a,
                          "status": "FINISHED" if played else "SCHEDULED",
                          "home_points": hp, "away_points": ap,
                          "start_time": t0 + timedelta(days=7 * week, hours=k)})
            gid += 1
    return games


def _targets(games, n=10):
    out = []
    for g in games:
        if g["status"] == "SCHEDULED":
            out.append(GameRow(sport_prefix="TST", sport_label="Test League", home=g["home"],
                               away=g["away"], rank_home=None, rank_away=None,
                               start_time=g["start_time"], extra={"game_id": g["id"]}))
        if len(out) == n:
            break
    return out


def _spearman(a, b):
    def ranks(x):
        order = sorted(range(len(x)), key=lambda i: x[i])
        r = [0.0] * len(x)
        for pos, i in enumerate(order):
            r[i] = pos
        return r
    ra, rb = ranks(a), ranks(b)
    n = len(a)
    return 1 - 6 * sum((x - y) ** 2 for x, y in zip(ra, rb)) / (n * (n * n - 1))


class TestSharedMatchesPerGame:
    def test_ranks_games_like_the_per_game_simulator(self):
        games = _season()
        src = _League(games)
        ctx = scoring.LEAGUE_CONTEXTS["CFB"]
        targets = _targets(games)
        per_game = [scoring.compute_match_importance(src, t, ctx, n_sims=1500, rng=random.Random(i))[0]
                    for i, t in enumerate(targets)]
        shared = [r[0] for r in scoring.compute_league_importance(src, targets, ctx, n_sims=1500,
                                                                  rng=random.Random(99))]
        assert max(per_game) > 3 * (min(per_game) + 0.05), "fixture must separate games, or agreement is vacuous"
        assert _spearman(per_game, shared) >= 0.85
        for p, s in zip(per_game, shared):
            assert s == pytest.approx(p, abs=0.6)

    def test_output_shape_matches_compute_match_importance(self):
        games = _season()
        src = _League(games)
        ctx = scoring.LEAGUE_CONTEXTS["CFB"]
        (raw, notes, labels), = scoring.compute_league_importance(src, _targets(games, 1), ctx, n_sims=200,
                                                                    rng=random.Random(1))
        assert isinstance(raw, float) and all(" leverage × " in n for n in notes)
        assert set(labels) <= {label for _, label, _ in ctx.thresholds}


class TestSharedContract:
    def test_refuses_a_source_that_has_not_opted_in(self):
        class _NoOpt(_League):
            shared_season_importance = False
        with pytest.raises(ValueError):
            simulation.monte_carlo_importance_shared(_NoOpt(_season()), [], [], n_sims=1)
        ctx = scoring.LEAGUE_CONTEXTS["CFB"]
        assert scoring.compute_league_importance(_NoOpt(_season()), _targets(_season(), 2), ctx) == [None, None]

    def test_game_missing_from_the_pool_is_none_not_zero(self):
        games = _season()
        src = _League(games)
        ghost = GameRow(sport_prefix="TST", sport_label="Test League", home="T0", away="T1",
                        rank_home=None, rank_away=None,
                        start_time=datetime(2030, 1, 1, tzinfo=timezone.utc), extra={"game_id": 99999})
        real = _targets(games, 1)[0]
        out = scoring.compute_league_importance(src, [ghost, real], scoring.LEAGUE_CONTEXTS["CFB"],
                                                n_sims=50, rng=random.Random(2))
        assert out[0] is None and out[1] is not None


def _all_source_classes():
    import dispatcharr_ranked_matchups.sources as pkg
    import importlib
    import pkgutil
    for info in pkgutil.iter_modules(pkg.__path__):
        importlib.import_module(f"{pkg.__name__}.{info.name}")
    seen, stack = set(), [sources_base.SportSource]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            if sub not in seen:
                seen.add(sub)
                stack.append(sub)
    return seen


def _reads_state(fn) -> bool:
    """True if the function body uses its `state` parameter for anything other
    than `del state`."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    func = tree.body[0]
    for node in ast.walk(func):
        if isinstance(node, ast.Name) and node.id == "state" and isinstance(node.ctx, ast.Load):
            return True
    return False


class TestOptInIsOnlyOnStateFreeModels:
    def test_every_opted_in_source_samples_without_reading_state(self):
        # CLASS guard: iterates every SportSource subclass that exists, so a new
        # source that opts in (or inherits the opt-in) while its sample_result
        # reads state fails here without anyone remembering to list it.
        classes = _all_source_classes()
        opted = [c for c in classes if getattr(c, "shared_season_importance", False)]
        assert len(opted) >= 10, f"source discovery found only {len(opted)} opted-in classes"
        offenders = [c.__name__ for c in opted if _reads_state(c.sample_result)]
        assert offenders == []

    def test_the_guard_can_fire(self):
        # The instrument must reject a state-reading model, or the guard above
        # passes vacuously.
        from dispatcharr_ranked_matchups.sources.soccer import KnockoutSoccerSource
        assert _reads_state(KnockoutSoccerSource.sample_result)
        assert KnockoutSoccerSource.shared_season_importance is False


class TestRefreshHelper:
    def _game(self, code, home, away):
        return GameRow(sport_prefix="TST", sport_label="T", home=home, away=away, rank_home=None,
                       rank_away=None, start_time=datetime(2026, 10, 1, tzinfo=timezone.utc),
                       extra={"fd_competition_code": code})

    def test_groups_by_league_and_calls_once_per_group(self, plugin, monkeypatch):
        calls = []

        def fake_league(src, matches, ctx, n_sims, favorites_by_match):
            calls.append((src, len(matches), n_sims))
            return [(1.0 + i, [], []) for i in range(len(matches))]
        monkeypatch.setattr(scoring, "compute_league_importance", fake_league)

        class S:
            supports_importance = True
            shared_season_importance = True
            sport_label = "S"
        a, b = S(), S()
        games = [self._game("CFB", "x", "y"), self._game("CFB", "p", "q"), self._game("NHL", "m", "n")]
        out = plugin._shared_league_importance(games, [a, a, b], [], 777)
        assert sorted(len for _, len, _ in calls) == [1, 2]
        assert all(n == 777 for _, _, n in calls)
        assert set(out) == {0, 1, 2}

    def test_active_chain_and_non_opted_sources_fall_back(self, plugin, monkeypatch):
        monkeypatch.setattr(scoring, "compute_league_importance",
                            lambda *a, **k: pytest.fail("must not be called"))

        class Chained:
            supports_importance = True
            shared_season_importance = True
            sport_label = "WC"
            def cross_source_chain(self):
                return ("downstream", lambda s: s)

        class Knockout:
            supports_importance = True
            shared_season_importance = False
            sport_label = "UCL"
        games = [self._game("CFB", "x", "y"), self._game("CFB", "p", "q")]
        assert plugin._shared_league_importance(games, [Chained(), Knockout()], [], 100) == {}

    def test_a_failing_league_is_skipped_not_fatal(self, plugin, monkeypatch, caplog):
        def boom(*a, **k):
            raise RuntimeError("season fetch failed")
        monkeypatch.setattr(scoring, "compute_league_importance", boom)

        class S:
            supports_importance = True
            shared_season_importance = True
            sport_label = "S"
        caplog.set_level("WARNING")
        assert plugin._shared_league_importance([self._game("CFB", "x", "y")], [S()], [], 10) == {}
        assert any("falling back to per-game" in r.getMessage() for r in caplog.records)

    def test_setting_defaults_and_ignores_the_old_per_game_key(self, plugin):
        # The old key counted seasons PER GAME and installs saved 500 under it;
        # reading it with the new per-league meaning would pin them low.
        assert plugin._importance_seasons({}) == 2000
        assert plugin._importance_seasons({"n_importance_sims": 500}) == 2000
        assert plugin._importance_seasons({"importance_seasons": 800}) == 800
        assert plugin._importance_seasons({"importance_seasons": "junk"}) == 2000
        assert plugin._importance_seasons({"importance_seasons": 0}) == 2000

    def test_manifest_default_matches_code(self, plugin):
        import json
        import os
        manifest = json.load(open(os.path.join(os.path.dirname(plugin.__file__), "plugin.json")))
        ids = {f["id"]: f for f in manifest["fields"]}
        assert "n_importance_sims" not in ids
        assert ids["importance_seasons"]["default"] == 2000


class _Resp:
    """A CBBD HTTP answer for the NcaamSource season walk."""
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class TestOutOfPopulationGamesAreNotSimulated:
    """W171: NcaamSource simulates only games involving an AP-ranked team,
    but emits the whole Division I slate. Every game outside that population
    fell to the per-game simulator: ~4.4s each, always 0.0000 (measured on
    the live CBBD API, 116 of 126 slate games). ~313 of them a refresh pushed
    every pipeline past its 25-minute ceiling from 2026-10-31, and since
    matching only runs inside the pipeline, a stale "no feed yet" verdict
    left a live game's channel without a stream for four days."""

    def _game(self, home, away, code="CBB"):
        return GameRow(sport_prefix="CBB", sport_label="NCAA Men's Basketball", home=home,
                       away=away, rank_home=None, rank_away=None,
                       start_time=datetime(2026, 11, 5, tzinfo=timezone.utc),
                       extra={"fd_competition_code": code})

    class _Subset:
        """A source that simulates a subset, like NcaamSource."""
        supports_importance = True
        shared_season_importance = True
        sport_label = "NCAA Men's Basketball"
        population = {"Michigan State", "Florida"}

        def importance_covers(self, game):
            return game.home in self.population or game.away in self.population

    def test_shared_path_simulates_only_covered_games(self, plugin, monkeypatch, caplog):
        seen = []

        def fake_league(src, matches, ctx, n_sims, favorites_by_match):
            seen.extend((m.home, m.away) for m in matches)
            return [(2.0, [], []) for _ in matches]
        monkeypatch.setattr(scoring, "compute_league_importance", fake_league)
        src = self._Subset()
        games = [self._game("Michigan State", "Toledo"), self._game("Dartmouth", "Framingham State"),
                 self._game("Butler", "Fairleigh Dickinson"), self._game("Florida", "Jacksonville")]
        caplog.set_level("INFO")
        out = plugin._shared_league_importance(games, [src] * 4, [], 50)
        assert seen == [("Michigan State", "Toledo"), ("Florida", "Jacksonville")]
        assert set(out) == {0, 3}
        assert any("2 games that cannot move a band scored 0 without simulating"
                   in r.getMessage() for r in caplog.records)

    def test_per_game_path_skips_uncovered_games(self, plugin):
        """The per-game fallback reads the same predicate with the game: an
        uncovered game never reaches compute_match_importance."""
        ctx = scoring.LEAGUE_CONTEXTS["CBB"]
        src = self._Subset()
        assert plugin._importance_applies(src, ctx, self._game("Florida", "Jacksonville"))
        assert not plugin._importance_applies(src, ctx, self._game("Dartmouth", "Framingham State"))
        refresh = inspect.getsource(plugin._action_refresh)
        assert "_importance_applies(src, league_ctx, g)" in refresh

    def test_a_source_without_the_predicate_covers_everything(self, plugin):
        class Plain:
            supports_importance = True
        assert plugin._importance_applies(Plain(), scoring.LEAGUE_CONTEXTS["CBB"],
                                          self._game("Dartmouth", "Framingham State"))

    def test_a_failing_predicate_simulates_rather_than_failing_the_refresh(self, plugin, caplog):
        class Broken(self._Subset):
            def importance_covers(self, game):
                raise RuntimeError("poll gone")
        caplog.set_level("WARNING")
        assert plugin._importance_applies(Broken(), scoring.LEAGUE_CONTEXTS["CBB"],
                                          self._game("Dartmouth", "Framingham State"))
        assert any("importance_covers failed" in r.getMessage() for r in caplog.records)

    def test_ncaam_covers_exactly_the_games_it_simulates(self, monkeypatch):
        """The real source, real team names from the 2026-11-04 measurement.
        Early in a season no outside team can reach a band, so
        `importance_covers` and the season walk's population filter must
        agree game for game, or the scorer skips a game the simulation could
        answer (or the slow fallback comes back)."""
        from dispatcharr_ranked_matchups.sources import ncaam
        poll = {"Michigan State": 12, "Florida": 3, "Vanderbilt": 20}
        src = ncaam.NcaamSource(api_key="k")
        monkeypatch.setattr(src, "_fetch_rankings", lambda season: poll)
        pairs = [("Michigan State", "Toledo"), ("Florida", "Jacksonville"),
                 ("Vanderbilt", "Wake Forest"), ("Dartmouth", "Framingham State"),
                 ("App State", "Christendom"), ("Butler", "Fairleigh Dickinson")]

        rows = [{"id": i, "homeTeam": h, "awayTeam": a, "startDate": "2026-11-05T00:00:00Z"}
                for i, (h, a) in enumerate(pairs)]
        answers = [rows, [], []]
        monkeypatch.setattr(ncaam, "get_within_deadline", lambda *a, **k: _Resp(answers.pop(0)))
        simulated = {(g["home"], g["away"]) for g in src._fetch_games_cached()}
        covered = {p for p in pairs if src.importance_covers(self._game(*p))}
        assert covered == simulated == set(pairs[:3])

    def test_skipping_changes_no_score(self, monkeypatch):
        """The skip is free only if the simulator would have said 0 anyway.
        Oracle: the real per-game simulator on the real source. An
        out-of-population team plays only its population games (here two),
        so it can never reach the 15-win band and its column is constant;
        an in-population game with a live race scores above zero, so the
        zero is not just a dead simulator."""
        from dispatcharr_ranked_matchups.sources import ncaam
        src = ncaam.NcaamSource(api_key="k")
        monkeypatch.setattr(src, "_fetch_rankings", lambda season: {"Duke": 1})
        rows, gid = [], 0
        for i in range(12):  # Duke 12-0 against a rotating field, all played
            gid += 1
            rows.append({"id": gid, "homeTeam": "Duke", "awayTeam": f"Opp{i}",
                         "homePoints": 80, "awayPoints": 60, "homeWinner": True,
                         "awayWinner": False, "startDate": "2025-12-01T00:00:00Z"})
        for i in range(6):  # six left: 15 wins is live
            gid += 1
            rows.append({"id": gid, "homeTeam": "Duke", "awayTeam": f"Late{i}",
                         "startDate": "2026-02-20T00:00:00Z"})
        rows.append({"id": gid + 1, "homeTeam": "Duke", "awayTeam": "Dartmouth",
                     "startDate": "2026-02-21T00:00:00Z"})
        rows.append({"id": gid + 2, "homeTeam": "Duke", "awayTeam": "Framingham State",
                     "startDate": "2026-02-22T00:00:00Z"})

        answers = [rows, [], []]  # the season walk's three date chunks
        monkeypatch.setattr(ncaam, "get_within_deadline",
                            lambda *a, **k: _Resp(answers.pop(0)))
        ctx = scoring.LEAGUE_CONTEXTS["CBB"]
        outside = self._game("Dartmouth", "Framingham State")
        inside = self._game("Duke", "Late0")
        assert not src.importance_covers(outside) and src.importance_covers(inside)
        pts_out, _, _ = scoring.compute_match_importance(src, outside, ctx, n_sims=200,
                                                         rng=random.Random(7))
        pts_in, _, _ = scoring.compute_match_importance(src, inside, ctx, n_sims=200,
                                                        rng=random.Random(7))
        assert pts_out == 0.0
        assert pts_in > 0.0

    def _outside_team_with(self, monkeypatch, finished, left):
        """A real NcaamSource whose season has unranked Gonzaga beating poll
        teams `finished` times, with `left` such games still to play."""
        from dispatcharr_ranked_matchups.sources import ncaam
        src = ncaam.NcaamSource(api_key="k")
        monkeypatch.setattr(src, "_fetch_rankings", lambda season: {"Duke": 1, "Kansas": 2})
        rows = []
        for i in range(finished):
            rows.append({"id": i + 1, "homeTeam": "Gonzaga", "awayTeam": ("Duke", "Kansas")[i % 2],
                         "homePoints": 80, "awayPoints": 60, "homeWinner": True,
                         "awayWinner": False, "startDate": "2025-12-01T00:00:00Z"})
        for i in range(left):
            rows.append({"id": 100 + i, "homeTeam": "Gonzaga", "awayTeam": "Duke",
                         "startDate": "2026-02-20T00:00:00Z"})
        answers = [rows, [], []]
        monkeypatch.setattr(ncaam, "get_within_deadline", lambda *a, **k: _Resp(answers.pop(0)))
        return src

    @pytest.mark.parametrize("finished,left", [(14, 0), (13, 1)])
    def test_an_outside_team_that_can_still_reach_a_band_is_simulated(
            self, monkeypatch, finished, left):
        """Second-opinion counterexample (2026-11-04): an unranked team with
        14 wins over poll teams, playing another unranked team, reaches the
        15-win band only by winning THIS game, so the game has real stakes
        (the simulator, the oracle here, scores it above zero). Population
        membership alone would have zeroed it."""
        src = self._outside_team_with(monkeypatch, finished, left)
        game = self._game("Gonzaga", "Dartmouth")
        assert src.importance_covers(game)
        pts, _, _ = scoring.compute_match_importance(src, game, scoring.LEAGUE_CONTEXTS["CBB"],
                                                     n_sims=200, rng=random.Random(7))
        assert pts > 0.0

    def test_an_outside_team_one_win_short_is_skipped_and_scores_zero(self, monkeypatch):
        """The other side of the boundary: 13 wins and nothing left, so even
        winning this game leaves it at 14. Skipped, and the simulator agrees."""
        src = self._outside_team_with(monkeypatch, 13, 0)
        game = self._game("Gonzaga", "Dartmouth")
        assert not src.importance_covers(game)
        pts, _, _ = scoring.compute_match_importance(src, game, scoring.LEAGUE_CONTEXTS["CBB"],
                                                     n_sims=200, rng=random.Random(7))
        assert pts == 0.0

    def test_ncaam_failed_season_read_simulates(self, monkeypatch):
        """A season read that raises answers True (the simulation then
        reports it) rather than zeroing the slate."""
        from dispatcharr_ranked_matchups.sources import ncaam
        src = ncaam.NcaamSource(api_key="k")
        monkeypatch.setattr(src, "_fetch_rankings", lambda season: {"Duke": 1})

        def boom():
            raise RuntimeError("season gone")
        monkeypatch.setattr(src, "_win_ceiling", boom)
        assert src.importance_covers(self._game("Dartmouth", "Framingham State"))

    def test_ncaam_failed_poll_is_not_read_as_out_of_population(self, monkeypatch):
        """A failed poll answers True, so the importance call raises and logs
        the outage (#283) instead of every game silently scoring zero."""
        from dispatcharr_ranked_matchups.sources import ncaam
        from dispatcharr_ranked_matchups.sources.base import RankingsUnavailable
        src = ncaam.NcaamSource(api_key="k")

        def refused(season):
            raise RankingsUnavailable("refused")
        monkeypatch.setattr(src, "_fetch_rankings", refused)
        assert src.importance_covers(self._game("Dartmouth", "Framingham State"))

    def test_ncaam_no_poll_yet_covers_nothing(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import ncaam
        src = ncaam.NcaamSource(api_key="k")
        monkeypatch.setattr(src, "_fetch_rankings", lambda season: None)
        assert not src.importance_covers(self._game("Florida", "Jacksonville"))


class TestCostScalesWithSeasonsNotGames:
    """The defect #230 fixes, witnessed directly: the old per-game simulator
    sampled targets x seasons x season-length results; the shared one samples
    seasons x season-length, whatever the number of targets."""

    def _count_samples(self, fn):
        games = _season()
        src = _League(games)
        calls = {"n": 0}
        real = src.sample_result

        def counting(*a, **k):
            calls["n"] += 1
            return real(*a, **k)
        src.sample_result = counting
        fn(src, games)
        remaining = sum(1 for g in games if g["status"] == "SCHEDULED")
        return calls["n"], remaining

    def test_shared_samples_each_season_once(self):
        ctx = scoring.LEAGUE_CONTEXTS["CFB"]
        n, remaining = self._count_samples(
            lambda src, games: scoring.compute_league_importance(
                src, _targets(games, 10), ctx, n_sims=20, rng=random.Random(1)))
        assert n == 20 * remaining  # literal relation: seasons x fixtures, no x targets

    def test_per_game_path_is_the_cost_that_was_removed(self):
        # The same 10 targets through the old path cost ~10x: this is the
        # comparison that makes the test above a witness rather than a tautology.
        ctx = scoring.LEAGUE_CONTEXTS["CFB"]
        n, remaining = self._count_samples(
            lambda src, games: [scoring.compute_match_importance(src, t, ctx, n_sims=20, rng=random.Random(1))
                                for t in _targets(games, 10)])
        assert n == 10 * 20 * remaining

    def test_fallback_path_keeps_the_historical_per_game_budget(self, plugin):
        import inspect
        assert plugin.PER_GAME_IMPORTANCE_SIMS == 500
        src = inspect.getsource(plugin._action_refresh)
        assert "n_sims=PER_GAME_IMPORTANCE_SIMS" in src


class TestIdentityAndAppearanceEdges:
    """Edges an outside review (gpt-6-sol) probed, each verified by building
    the case it described."""

    ctx = scoring.LEAGUE_CONTEXTS["CFB"]

    def _shared(self, src, targets, n=400, seed=5):
        return scoring.compute_league_importance(src, targets, self.ctx, n_sims=n, rng=random.Random(seed))

    def _per_game(self, src, t, n=400, seed=6):
        return scoring.compute_match_importance(src, t, self.ctx, n_sims=n, rng=random.Random(seed))[0]

    def test_two_rows_sharing_one_id_are_still_two_targets(self):
        # Pool rows {fd_id: 99, game_id: g1} and {fd_id: 99, game_id: g2};
        # targets carry only game_id. _same_match compares game_id (the only
        # key BOTH carry), so each pool row is its own target. A memo keyed by
        # the row's FIRST id (fd_id) collapsed both onto target 1.
        games = _season()
        targets = _targets(games, 2)
        ids = {t.extra["game_id"] for t in targets}

        class Shared(_League):
            def remaining_matches(self, state):
                rows = super().remaining_matches(state)
                for r in rows:
                    if r.extra.get("game_id") in ids:
                        r.extra["fd_id"] = 99
                return rows
        src = Shared(games)
        out = self._shared(src, targets)
        assert out[0] is not None and out[1] is not None
        for t, res in zip(targets, out):
            assert res[0] == pytest.approx(self._per_game(src, t), abs=0.8)

    def test_a_repeated_target_row_is_played_once_per_season(self):
        games = _season()
        (target,) = _targets(games, 1)
        dup = next(g for g in games if g["id"] == target.extra["game_id"])
        src = _League(games + [dict(dup)])  # the feed repeats one scheduled row
        calls = {"n": 0}
        real = src.sample_result

        def counting(state, m, *a, **k):
            if m.extra.get("game_id") == dup["id"]:
                calls["n"] += 1
            return real(state, m, *a, **k)
        src.sample_result = counting
        self._shared(src, [target], n=30)
        assert calls["n"] == 30  # once per season, not twice

    def test_a_target_reachable_only_on_some_paths_falls_back(self):
        # A "final" that exists only if T9 won its last scheduled game: the
        # shared table would be conditional on reaching it, so it must not
        # answer. (Per-game forces the target into every season.)
        games = _season()
        final = GameRow(sport_prefix="TST", sport_label="Test League", home="T9", away="T8",
                        rank_home=None, rank_away=None,
                        start_time=datetime(2027, 1, 1, tzinfo=timezone.utc), extra={"game_id": 10_000})

        class Bracket(_League):
            def remaining_matches(self, state):
                rows = super().remaining_matches(state)
                if rows or 10_000 in state.get("_applied", frozenset()):
                    return rows
                if state["_teams"]["T9"]["wins"] % 2 == 0:
                    return [final]
                return []
        assert self._shared(Bracket(games), [final], n=60) == [None]

    def test_an_id_match_is_found_even_when_a_name_differs(self):
        games = _season()
        (target,) = _targets(games, 1)
        renamed = GameRow(sport_prefix="TST", sport_label="Test League", home="The " + target.home,
                          away=target.away, rank_home=None, rank_away=None,
                          start_time=target.start_time, extra=dict(target.extra))
        (res,) = self._shared(_League(games), [renamed], n=50)
        assert res is not None
