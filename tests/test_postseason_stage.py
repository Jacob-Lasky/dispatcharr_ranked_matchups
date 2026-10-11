"""Playoff rows carry their bracket round (#243).

The NFL / NBA / NHL / MLB / WNBA / NCAA women's basketball playoff sources
built their upcoming rows without `extra["stage"]`. Two things
read that key and both failed silently:

- `BestOfNSeriesSource.apply_result` records the importance simulator's target
  game into its tie only when the stage is a string. Without it the sampled
  result was dropped and the same game replayed from the bracket, so a League
  Championship Series game measured about 0.15 importance instead of about 3
  (live MLB data, 2026-10-11).
- `plugin._is_postseason_game`, so favorites_only "postseason" mode dropped the
  very playoff games it promises to keep.

The fixture is an MLB postseason payload in statsapi's shape: four Division
Series sweeps, then the ALCS level at 1-1 and the NLCS under way. Like
statsapi, it lists all seven potential games of each LCS ("if necessary"),
because the simulator replays only published games.
"""

import random
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc


def _game(pk, desc, number, home, away, start, home_runs=None, away_runs=None):
    final = home_runs is not None
    return {
        "gamePk": pk,
        "gameDate": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seriesDescription": desc,
        "seriesGameNumber": number,
        "gamesInSeries": 7,
        "status": {"abstractGameState": "Final" if final else "Preview"},
        "teams": {
            "home": {"team": {"name": home}, "score": home_runs},
            "away": {"team": {"name": away}, "score": away_runs},
        },
    }


@pytest.fixture
def mlb(monkeypatch):
    """An MlbPlayoffSource whose statsapi calls return the fixture."""
    import dispatcharr_ranked_matchups.sources.mlb as mlb_mod
    now = datetime.now(UTC)
    games = []
    for pk, (desc, winner, loser) in enumerate([
        ("AL Division Series", "Cleveland Guardians", "New York Yankees"),
        ("AL Division Series", "Tampa Bay Rays", "Houston Astros"),
        ("NL Division Series", "Milwaukee Brewers", "Philadelphia Phillies"),
        ("NL Division Series", "Los Angeles Dodgers", "San Diego Padres"),
    ]):
        for n in (1, 2, 3):
            games.append(_game(100 + 10 * pk + n, desc, n, winner, loser,
                               now - timedelta(days=9 - n), 5, 1))
    al, nl = "AL Championship Series", "NL Championship Series"
    games += [
        _game(1, al, 1, "Tampa Bay Rays", "Cleveland Guardians", now - timedelta(days=3), 4, 2),
        _game(2, al, 2, "Tampa Bay Rays", "Cleveland Guardians", now - timedelta(days=2), 1, 3),
        *[_game(n, al, n, "Cleveland Guardians", "Tampa Bay Rays", now + timedelta(days=n - 2))
          for n in (3, 4, 5, 6, 7)],
        _game(11, nl, 1, "Milwaukee Brewers", "Los Angeles Dodgers", now - timedelta(days=2), 2, 5),
        *[_game(10 + n, nl, n, "Milwaukee Brewers", "Los Angeles Dodgers", now + timedelta(days=n - 1))
          for n in (2, 3, 4, 5, 6, 7)],
    ]
    payload = {"dates": [{"games": games}]}
    monkeypatch.setattr(mlb_mod, "_http_get", lambda *a, **kw: payload)
    return mlb_mod.MlbPlayoffSource(season=2026)


def _by_pk(rows):
    return {r.extra["mlb_game_id"]: r for r in rows}


def test_place_upcoming_stamps_round_and_game_number(mlb):
    rows = mlb.fetch_upcoming(days_ahead=7)
    assert mlb.place_upcoming(rows) == 0
    game3 = _by_pk(rows)[3]
    assert game3.extra["stage"] == "LCS"
    assert game3.extra["matchday"] == 3


def test_placed_playoff_game_has_bracket_importance(mlb):
    """The witness for the importance half: ALCS Game 3 at 1-1 decides who
    leads a series one win from the World Series."""
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, compute_match_importance
    rows = mlb.fetch_upcoming(days_ahead=7)
    mlb.place_upcoming(rows)
    points, _notes, _bands = compute_match_importance(
        mlb, _by_pk(rows)[3], LEAGUE_CONTEXTS["MLB_PO"], n_sims=600, rng=random.Random(7),
    )
    assert points > 1.0


def test_unplaced_row_is_counted_and_left_alone(mlb):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    stray = GameRow(sport_prefix="MLB", sport_label="MLB Postseason", home="A", away="B",
                    rank_home=None, rank_away=None, start_time=datetime.now(UTC),
                    extra={"mlb_game_id": 999, "game_id": 999})
    assert mlb.place_upcoming([stray]) == 1
    assert "stage" not in stray.extra


def test_a_stage_the_source_set_itself_is_kept(mlb):
    rows = mlb.fetch_upcoming(days_ahead=7)
    game3 = _by_pk(rows)[3]
    game3.extra["stage"] = "SOURCE_OWN"
    mlb.place_upcoming(rows)
    assert game3.extra["stage"] == "SOURCE_OWN"


def test_postseason_mode_keeps_a_playoff_game_without_a_favorite(mlb, refresh_env):
    """The witness for the favorites_only half, end to end through refresh."""
    _write_prior, run, read = refresh_env
    result = run([mlb], favorites="Arsenal", favorites_only="postseason")
    assert result["status"] == "ok", result
    kept = {(g["home"], g["away"]) for g in read()["games"]}
    assert ("Cleveland Guardians", "Tampa Bay Rays") in kept


def test_a_failed_bracket_fetch_keeps_the_games_as_postseason(mlb, refresh_env, monkeypatch, caplog):
    """The rows are flagged before the bracket is fetched, so postseason mode
    still keeps them when that fetch blows up."""
    def boom():
        raise RuntimeError("bracket fetch blew up")
    monkeypatch.setattr(mlb, "_fetch_bracket_games", boom)
    _write_prior, run, read = refresh_env
    run([mlb], favorites="Arsenal", favorites_only="postseason")
    # One channel per series, as for any refresh.
    assert {g["home"] for g in read()["games"]} == {"Cleveland Guardians", "Milwaukee Brewers"}
    assert "could not place games in the bracket" in caplog.text


# ---------------------------------------------------------------------------
# Guards: a wrong placement files a result into the wrong tie, worse than none
# ---------------------------------------------------------------------------

def _row(home, away, **extra):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(sport_prefix="MLB", sport_label="MLB Postseason", home=home, away=away,
                   rank_home=None, rank_away=None, start_time=datetime.now(UTC), extra=extra)


def test_an_id_of_another_type_is_not_placed(mlb):
    """Simulation compares ids exactly, so "3" is not game 3."""
    row = _row("Cleveland Guardians", "Tampa Bay Rays", game_id="3")
    assert mlb.place_upcoming([row]) == 1
    assert "stage" not in row.extra


def test_a_team_mismatch_is_not_placed(mlb):
    row = _row("Boston Red Sox", "Tampa Bay Rays", game_id=3)
    assert mlb.place_upcoming([row]) == 1
    assert "stage" not in row.extra


def test_an_id_on_two_bracket_games_is_not_placed(mlb, monkeypatch):
    """One id in two rounds, same two teams: which round the game is in cannot
    be known, so it is not guessed."""
    def game(stage):
        return {"game_id": 3, "stage": stage, "matchday": 1,
                "home": "Cleveland Guardians", "away": "Tampa Bay Rays"}
    bracket = {
        "LDS": [{"stage": "LDS", "games": [game("LDS")]}],
        "LCS": [{"stage": "LCS", "games": [game("LCS")]}],
    }
    monkeypatch.setattr(mlb, "initial_state", lambda: {"_bracket": bracket})
    row = _row("Cleveland Guardians", "Tampa Bay Rays", game_id=3)
    assert mlb.place_upcoming([row]) == 1
    assert "stage" not in row.extra


def test_league_phase_is_not_postseason(plugin):
    """UCL / UEL / UECL league phase rows carry LEAGUE_STAGE (live cache,
    2026-10-11); it is their group stage."""
    from types import SimpleNamespace
    assert not plugin._is_postseason_game(SimpleNamespace(extra={"stage": "LEAGUE_STAGE"}))


# ---------------------------------------------------------------------------
# Soccer knockouts: Football-Data's matchday is not a leg number
# ---------------------------------------------------------------------------

def _fd_match(fd_id, stage, matchday, home, away, hg=None, ag=None):
    return {
        "id": fd_id, "stage": stage, "matchday": matchday,
        "status": "FINISHED" if hg is not None else "SCHEDULED",
        "homeTeam": {"name": home}, "awayTeam": {"name": away},
        "score": {"fullTime": {"home": hg, "away": ag}, "duration": "REGULAR", "penalties": None},
        "utcDate": "2026-05-01T18:00:00Z",
    }


@pytest.mark.parametrize("fd_matchday", [None, 0, 7, 1])
def test_soccer_final_has_importance_whatever_fd_calls_its_matchday(fd_matchday):
    """Football-Data gives a final's matchday as 0 (UCL 2024), None (UCL 2025,
    every 2026 World Cup knockout) or the tournament day (EURO 2024 final: 7).
    Read as a leg number, anything but 1 filed the final as leg 2, the tie
    never completed, and the final scored 0.0. The row is built the way
    fetch_upcoming builds it: fd_id, FD's matchday, no game_id."""
    from dispatcharr_ranked_matchups.simulation import monte_carlo_importance
    from dispatcharr_ranked_matchups.sources.base import GameRow
    from dispatcharr_ranked_matchups.sources.soccer import KnockoutSoccerSource
    src = KnockoutSoccerSource("ucl", fd_api_key="fake")
    src._all_matches_cache = [
        _fd_match(201, "SEMI_FINALS", 1, "A", "B", 2, 0),
        _fd_match(202, "SEMI_FINALS", 2, "B", "A", 1, 1),
        _fd_match(203, "SEMI_FINALS", 1, "C", "D", 2, 0),
        _fd_match(204, "SEMI_FINALS", 2, "D", "C", 0, 0),
        _fd_match(301, "FINAL", fd_matchday, "A", "C"),
    ]
    # Symmetric strengths so the final is a coin flip (tau-c is capped by an
    # unbalanced W/L marginal); the bracket mechanics are under test.
    src.estimate_strengths = lambda: {t: {"sh": 1.2, "ch": 1.2, "sa": 1.0, "ca": 1.0} for t in "ABCD"}
    final = GameRow(sport_prefix="UCL", sport_label="UEFA Champions League", home="A", away="C",
                    rank_home=None, rank_away=None, start_time=datetime(2026, 5, 30, 19, tzinfo=UTC),
                    extra={"fd_id": 301, "matchday": fd_matchday, "stage": "FINAL",
                           "fd_competition_code": "CL"})
    assert src.place_upcoming([final]) == 0
    assert final.extra["matchday"] == fd_matchday  # the EPG text keeps FD's own
    imp = monte_carlo_importance(src, final, "A", "winner", n_sims=400, rng=random.Random(13))
    assert imp > 0.95
