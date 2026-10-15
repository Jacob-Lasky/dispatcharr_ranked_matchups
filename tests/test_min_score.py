"""Minimum score (#313): an opt-in floor on the displayed 0-10 score.

Low-scored filler games are intended by default, so the setting is off (0)
unless the user sets it, and it never drops a game the cap itself is forbidden
to drop: favorites, recorded teams, a game held on air. These drive the real
_action_refresh through conftest's refresh_env (no network, no DB).

The slate: ranked games (both teams in the top 5) score well above 2, and
unranked games with every other signal off score exactly 0.0. Both facts are
asserted, not assumed, so a scoring change cannot make these tests vacuous.
"""

from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc
START = datetime.now(UTC) + timedelta(hours=3)


class _Source:
    def __init__(self, games):
        self.sport_label = "NCAA Football"
        self.sport_prefix = "CFB"
        self._games = list(games)

    def fetch_upcoming(self, days_ahead=7):
        return list(self._games)


def _game(home, away, gid, ranked):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(sport_prefix="CFB", sport_label="NCAA Football", home=home, away=away,
                   rank_home=1 if ranked else None, rank_away=2 if ranked else None,
                   start_time=START + timedelta(minutes=gid),
                   extra={"cfbd_id": gid, "game_id": gid})


def _slate():
    return [_game("Georgia", "Alabama", 1, True), _game("Texas", "Ohio State", 2, True),
            _game("Filler One", "Filler Two", 3, False), _game("Filler Three", "Filler Four", 4, False)]


def _homes(rows):
    return sorted(g["home"] for g in rows)


def test_off_by_default_keeps_the_zero_scored_filler(refresh_env):
    _write, run, read = refresh_env
    run([_Source(_slate())], curation_preset="manual")
    games = read()["games"]
    assert _homes(games) == ["Filler One", "Filler Three", "Georgia", "Texas"]
    scores = {g["home"]: g["score"] for g in games}
    # The premise of every other test here.
    assert scores["Filler One"] == 0.0 and scores["Filler Three"] == 0.0
    assert scores["Georgia"] > 2 and scores["Texas"] > 2


def test_drops_games_below_the_minimum_and_says_so(refresh_env):
    _write, run, read = refresh_env
    result = run([_Source(_slate())], curation_preset="manual", min_score=2)
    assert _homes(read()["games"]) == ["Georgia", "Texas"]
    assert "2 below the minimum score left out" in result["message"]


def test_a_game_exactly_at_the_minimum_is_kept(refresh_env):
    _write, run, read = refresh_env
    run([_Source(_slate())], curation_preset="manual")
    exact = min(g["score"] for g in read()["games"] if g["score"] > 0)
    run([_Source(_slate())], curation_preset="manual", min_score=exact)
    # Both ranked games score exactly `exact`; `>`, not `>=`, would drop them.
    assert _homes(read()["games"]) == ["Georgia", "Texas"]
    assert {g["score"] for g in read()["games"]} == {exact}


def test_favorites_and_recorded_teams_are_never_dropped(refresh_env):
    _write, run, read = refresh_env
    # 10 is above every possible score, so only the forced games survive.
    run([_Source(_slate())], curation_preset="manual", min_score=10,
        favorites="Filler One", recorded_teams="Filler Three")
    assert _homes(read()["games"]) == ["Filler One", "Filler Three"]


def test_the_bench_is_filtered_too(refresh_env):
    """A bench game is promoted straight onto the list when a slot frees, so a
    game below the minimum must not wait there."""
    _write, run, read = refresh_env
    run([_Source(_slate())], curation_preset="manual", min_score=2,
        max_games=1, bench_size=5, remove_finished_after_minutes=30)
    cache = read()
    # The cap still splits what passed: the earlier kickoff of two equal
    # scores is listed, the other waits on the bench; the filler is in neither.
    assert _homes(cache["games"]) == ["Georgia"]
    assert _homes(cache.get("bench") or []) == ["Texas"]


def test_applies_under_a_curation_preset(refresh_env):
    _write, run, read = refresh_env
    run([_Source(_slate())], curation_preset="high_coverage", min_score=2)
    assert all(g["score"] >= 2 for g in read()["games"])
    assert read()["games"]


def _prior_row(home, start, gid, score):
    return {"sport_prefix": "CFB", "sport_label": "NCAA Football", "home": home,
            "away": "Somebody", "start_time_utc": start.isoformat(), "score": score,
            "score_raw": score, "channel_id": 5, "channel_ids": [5], "stream_ids": [],
            "match_method": "regex_strict", "extra": {"cfbd_id": gid}}


def test_a_game_held_through_a_source_outage_still_meets_the_minimum(refresh_env):
    """Second-opinion finding: a not-yet-started game carried over because its
    source returned nothing skipped the floor. It is filtered now; a game
    already on air is kept whatever it scored, as is a favorite."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([
        _prior_row("Upcoming Low", now + timedelta(hours=20), 901, 0.5),
        _prior_row("Upcoming High", now + timedelta(hours=21), 902, 6.0),
        _prior_row("Upcoming Fav", now + timedelta(hours=22), 903, 0.5),
        _prior_row("Live Low", now - timedelta(minutes=30), 904, 0.5),
    ])
    result = run([_Source([])], curation_preset="manual", min_score=2,
                 favorites="Upcoming Fav")
    assert _homes(read()["games"]) == ["Live Low", "Upcoming Fav", "Upcoming High"]
    assert "1 below the minimum score left out" in result["message"]


@pytest.mark.parametrize("final, floor, meets", [
    (1.96, 2, True),    # shown as 2.0, so it clears a floor of 2
    (1.94, 2, False),   # shown as 1.9
    (2.0, 2, True), (None, 2, False), (0.0, 0, True), (None, 0, True),
])
def test_the_floor_compares_the_displayed_score(plugin, final, floor, meets):
    assert plugin._meets_min_score(final, floor) is meets


@pytest.mark.parametrize("raw, value", [
    (None, 0.0), ("", 0.0), ("abc", 0.0), ("nan", 0.0), ("inf", 0.0),
    (-1, 0.0), (0, 0.0), ("2.5", 2.5), (7, 7.0), (11, 10.0),
])
def test_min_score_is_parse_safe_and_clamped(plugin, raw, value):
    # Literals, not MAX_MIN_SCORE: the clamp is what is under test.
    assert plugin._resolve_min_score({"min_score": raw}) == value


def test_the_setting_is_declared_off_by_default(plugin):
    import json
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, os.pardir, "plugin.json"), encoding="utf-8") as f:
        field = next(x for x in json.load(f)["fields"] if x["id"] == "min_score")
    assert field["type"] == "number" and field["default"] == 0
