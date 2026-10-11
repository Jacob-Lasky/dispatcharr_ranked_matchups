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


def test_a_failed_placement_keeps_the_games(mlb, refresh_env, monkeypatch, caplog):
    def boom(_rows):
        raise RuntimeError("bracket fetch blew up")
    monkeypatch.setattr(mlb, "place_upcoming", boom)
    _write_prior, run, read = refresh_env
    run([mlb])
    # One channel per series, as for any refresh.
    assert {g["home"] for g in read()["games"]} == {"Cleveland Guardians", "Milwaukee Brewers"}
    assert "could not place games in the bracket" in caplog.text
