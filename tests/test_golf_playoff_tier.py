"""Golf's FedEx Cup playoffs get their own tier (#188).

The three playoff events (FedEx St. Jude, BMW, TOUR Championship) decide the
season-long title and used to score as an ordinary tour stop. They now sit
between a regular week and a major.

Expected scores are literals on purpose (3.0, not the table's value): a test
written in terms of the constant cannot fail when the constant is wrong.
"""

import pytest

from dispatcharr_ranked_matchups.scoring import (
    GameSignals, Weights, score_game, tournament_stage_label,
)

# Names exactly as ESPN's golf/pga scoreboard published them in 2025 and 2026.
PLAYOFF_NAMES = [
    "FedEx St. Jude Championship",
    "BMW Championship",
    "TOUR Championship",
]


def _fetch_one(monkeypatch, name):
    import dispatcharr_ranked_matchups.sources.field_event as field_mod
    payload = {"events": [{
        "id": "401703520", "name": name, "shortName": name,
        "date": "2026-08-27T16:00Z", "competitions": [{"competitors": []}],
    }]}
    monkeypatch.setattr(field_mod, "_http_get", lambda *a, **kw: payload)
    (game,) = field_mod.GolfSource().fetch_upcoming(days_ahead=7)
    return game


@pytest.mark.parametrize("name", PLAYOFF_NAMES)
def test_playoff_events_get_the_playoff_tier(monkeypatch, name):
    assert _fetch_one(monkeypatch, name).extra["stage"] == "TOUR_PLAYOFF"


@pytest.mark.parametrize("name", [
    "Wyndham Championship",            # the week before the playoffs
    "THE PLAYERS Championship",
    "FedEx St. Jude Classic",          # pre-2022 name, an ordinary tour stop
    "PGA TOUR Q-School presented by Korn Ferry",
    "Korn Ferry Tour Championship",    # another tour's finale
])
def test_near_misses_stay_regular_events(monkeypatch, name):
    assert _fetch_one(monkeypatch, name).extra["stage"] == "EVENT"


@pytest.mark.parametrize("name", [
    "Masters Tournament", "PGA Championship", "U.S. Open", "The Open Championship",
])
def test_majors_are_still_majors(monkeypatch, name):
    assert _fetch_one(monkeypatch, name).extra["stage"] == "MAJOR"


@pytest.mark.parametrize("name", ["BMW PGA Championship", "Senior PGA Championship"])
def test_pga_championship_inside_a_longer_name_is_not_a_major(monkeypatch, name):
    assert _fetch_one(monkeypatch, name).extra["stage"] != "MAJOR"


def _stage_points(stage):
    weights = Weights()
    score = score_game(GameSignals(team_a="X", team_b="Field", tournament_stage=stage), weights)
    return score.breakdown["tournament_stage"] / weights.tournament


def test_playoff_tier_sits_between_event_and_major():
    assert _stage_points("EVENT") == pytest.approx(1.5, abs=0.01)
    assert _stage_points("TOUR_PLAYOFF") == pytest.approx(3.0, abs=0.01)
    assert _stage_points("MAJOR") == pytest.approx(4.5, abs=0.01)


def test_playoff_tier_is_labelled_playoff():
    """The label is what the {tournament} naming token and the EPG note show."""
    assert tournament_stage_label("TOUR_PLAYOFF") == "Playoff"


def test_playoff_events_count_as_postseason(plugin):
    """favorites_only 'postseason' keeps them, as it keeps the majors."""
    from types import SimpleNamespace
    assert plugin._is_postseason_game(SimpleNamespace(extra={"stage": "TOUR_PLAYOFF"}))
    assert not plugin._is_postseason_game(SimpleNamespace(extra={"stage": "EVENT"}))
