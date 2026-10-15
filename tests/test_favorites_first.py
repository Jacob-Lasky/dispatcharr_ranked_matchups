"""Favorites first (#310): favorite games take the lowest channel numbers.

The guide binds by the bare channel number (#117, #121), so the ordering key
must not change while a game is on the list. Favorite status qualifies; the
score does not. KICKOFF mode shifts every non-favorite's offset one fixed block
up (FAVORITES_FIRST_SHIFT); COMPACT mode hands out slots to favorites first.

The scenarios drive the real `_action_apply` against a real Django ORM
(tests/orm_harness/run_scenarios.py), because the claims are about what apply
writes, to the channels and to numbering_state.json. Floor 11 is the auto floor
above the user's own channel 10, as in test_apply_kickoff_record.py.
"""

import importlib.util
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load_kit():
    spec = importlib.util.spec_from_file_location(
        "_apply_harness_kit", os.path.join(HERE, "test_apply_empty_slate.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_kit = _load_kit()
_settings, _seed_source_channel, _row, _write_cache, _publish, _owned = (
    _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish, _kit._owned,
)

KICKOFF = datetime(2030, 1, 5, 20, 0, tzinfo=timezone.utc)
FLOOR = 11


def _fav(gid, start, channel_id):
    return _row(gid, start, channel_id, home="Georgia", away="Alabama")


def _other(gid, start, channel_id):
    return _row(gid, start, channel_id, home="Texas", away="Oklahoma")


def _marker(gid):
    return f"ranked_matchups:CFB:{gid}"


def _plain(env, gid, start):
    """The number the game gets with Favorites first off."""
    return env.plugin.stable_channel_number(FLOOR, start, _marker(gid), timezone.utc)


def _numbers(env):
    return {m: int(c.channel_number) for m, c in _owned(env).items()}


def _state(env):
    with open(env.plugin.NUMBERING_STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def _apply(env, games, **over):
    _write_cache(env, games)
    result = env.plugin._action_apply(_settings(**over))
    assert result["status"] == "ok", result
    return result


ON = {"favorites_first": True, "favorites": "Georgia"}


# ------------------------------------------------------------- kickoff ---

def scenario_a_favorite_numbers_below_an_earlier_game(env):
    """The favorite kicks off an hour LATER and still numbers first; it keeps
    the plain kickoff number and the other game moves up by the shift."""
    user_ch, _ = _seed_source_channel(env)
    later, earlier = KICKOFF + timedelta(hours=1), KICKOFF
    _publish(env, [_fav(1, later, user_ch.id), _other(2, earlier, user_ch.id)],
             settings=_settings(**ON))
    shift = env.plugin.FAVORITES_FIRST_SHIFT
    assert _numbers(env) == {
        _marker(1): _plain(env, 1, later),
        _marker(2): _plain(env, 2, earlier) + shift,
    }
    assert _numbers(env)[_marker(1)] < _numbers(env)[_marker(2)]
    # The record carries the shifted offset, so the next apply holds it (#278).
    assert _state(env)["numbered_for"][_marker(2)][0] == _plain(env, 2, earlier) - FLOOR + shift


def scenario_a_favorite_arriving_or_leaving_moves_nobody_else(env):
    """The shift depends on the setting and the game, never on whether the
    slate has a favorite in it: the one property that keeps numbers still."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_other(2, KICKOFF, user_ch.id), _other(3, KICKOFF + timedelta(hours=2), user_ch.id)],
             settings=_settings(**ON))
    before = _numbers(env)
    _apply(env, [_other(2, KICKOFF, user_ch.id), _other(3, KICKOFF + timedelta(hours=2), user_ch.id),
                 _fav(1, KICKOFF + timedelta(hours=1), user_ch.id)], **ON)
    after = _numbers(env)
    assert {m: after[m] for m in before} == before
    assert after[_marker(1)] == _plain(env, 1, KICKOFF + timedelta(hours=1))
    _apply(env, [_other(2, KICKOFF, user_ch.id), _other(3, KICKOFF + timedelta(hours=2), user_ch.id)], **ON)
    assert _numbers(env) == before


def scenario_turning_it_on_renumbers_only_the_other_games_once(env):
    user_ch, _ = _seed_source_channel(env)
    games = [_fav(1, KICKOFF + timedelta(hours=1), user_ch.id), _other(2, KICKOFF, user_ch.id)]
    _publish(env, games, settings=_settings(favorites="Georgia"))
    off = _numbers(env)
    assert off == {_marker(1): _plain(env, 1, KICKOFF + timedelta(hours=1)),
                   _marker(2): _plain(env, 2, KICKOFF)}
    _apply(env, games, **ON)
    on = _numbers(env)
    assert on == {_marker(1): off[_marker(1)],
                  _marker(2): off[_marker(2)] + env.plugin.FAVORITES_FIRST_SHIFT}
    _apply(env, games, **ON)
    assert _numbers(env) == on
    # And back off: the other game returns to its plain kickoff number.
    _apply(env, games, favorites="Georgia")
    assert _numbers(env) == off


def scenario_on_with_no_favorites_listed_shifts_everything(env):
    """Setting on, favorites blank: every game is behind the (empty) favorites.
    Listing a team later moves only that team's game, down to its plain number."""
    user_ch, _ = _seed_source_channel(env)
    games = [_fav(1, KICKOFF + timedelta(hours=1), user_ch.id), _other(2, KICKOFF, user_ch.id)]
    _publish(env, games, settings=_settings(favorites_first=True))
    shift = env.plugin.FAVORITES_FIRST_SHIFT
    assert _numbers(env) == {_marker(1): _plain(env, 1, KICKOFF + timedelta(hours=1)) + shift,
                             _marker(2): _plain(env, 2, KICKOFF) + shift}
    _apply(env, games, **ON)
    assert _numbers(env) == {_marker(1): _plain(env, 1, KICKOFF + timedelta(hours=1)),
                             _marker(2): _plain(env, 2, KICKOFF) + shift}


def scenario_a_lost_state_file_infers_the_floor_through_the_shift(env):
    """No remembered floor: _infer_kickoff_base must compute offsets the way
    placement does (shift included), or it picks a floor that moves every
    non-favorite. Nothing moves."""
    user_ch, _ = _seed_source_channel(env)
    games = [_fav(1, KICKOFF + timedelta(hours=1), user_ch.id), _other(2, KICKOFF, user_ch.id),
             _other(3, KICKOFF + timedelta(hours=3), user_ch.id)]
    _publish(env, games, settings=_settings(**ON))
    before = _numbers(env)
    os.remove(env.plugin.NUMBERING_STATE_PATH)
    _apply(env, games, **ON)
    assert _numbers(env) == before
    assert _state(env)["kickoff_base"] == FLOOR


def scenario_a_knockout_slot_filled_by_a_favorite_moves_once(env):
    """Same event id, the provider names the teams once the slot is decided:
    the game becomes a favorite and moves down to its plain kickoff number,
    once, as documented (like a reschedule), then stays."""
    user_ch, _ = _seed_source_channel(env)
    tbd = _row(4, KICKOFF, user_ch.id, home="TBD", away="Alabama")
    _publish(env, [tbd], settings=_settings(**ON))
    assert _numbers(env) == {_marker(4): _plain(env, 4, KICKOFF) + env.plugin.FAVORITES_FIRST_SHIFT}
    decided = _row(4, KICKOFF, user_ch.id, home="Georgia", away="Alabama")
    _apply(env, [decided], **ON)
    assert _numbers(env) == {_marker(4): _plain(env, 4, KICKOFF)}
    _apply(env, [decided], **ON)
    assert _numbers(env) == {_marker(4): _plain(env, 4, KICKOFF)}


# ------------------------------------------------------------- compact ---

def scenario_compact_fills_favorites_first(env):
    """A fresh band: the favorite, kicking off LAST, takes the first slot; the
    rest follow in kickoff order."""
    user_ch, _ = _seed_source_channel(env)
    games = [_other(2, KICKOFF, user_ch.id), _other(3, KICKOFF + timedelta(hours=1), user_ch.id),
             _fav(1, KICKOFF + timedelta(hours=2), user_ch.id)]
    _publish(env, games, settings=_settings(
        channel_numbering_mode="compact", virtual_channel_base=400, **ON))
    assert _numbers(env) == {_marker(1): 400, _marker(2): 401, _marker(3): 402}


def scenario_compact_never_moves_a_published_game_for_a_favorite(env):
    """A published game holds its slot; a favorite arriving later takes the
    next free slot like any newcomer (the help text says so)."""
    user_ch, _ = _seed_source_channel(env)
    compact = dict(channel_numbering_mode="compact", virtual_channel_base=400,
                   compact_band_size=5, **ON)
    _publish(env, [_other(2, KICKOFF, user_ch.id)], settings=_settings(**compact))
    assert _numbers(env) == {_marker(2): 400}
    _apply(env, [_other(2, KICKOFF, user_ch.id), _fav(1, KICKOFF + timedelta(hours=1), user_ch.id)],
           **compact)
    assert _numbers(env) == {_marker(2): 400, _marker(1): 401}


# ----------------------------------------------------- in-process checks ---

def test_the_shift_keeps_the_minute_block_arithmetic(plugin):
    # _place_kickoff_numbers finds a game's block as number - offset % SLOTS.
    assert plugin.FAVORITES_FIRST_SHIFT % plugin.CHANNEL_NUMBER_TIEBREAK_SLOTS == 0


def test_the_shift_is_wider_than_the_longest_allowed_slate(plugin):
    """A favorite kicking off at the far end of the longest lookahead still
    numbers below a non-favorite kicking off now (second-opinion finding: with
    no cap, lookahead_days=500 put a favorite 435 days out AFTER it). Plus a
    day of margin for finished games kept for their recordings."""
    span_days = plugin.MAX_LOOKAHEAD_DAYS + 1
    assert span_days * 1440 * plugin.CHANNEL_NUMBER_TIEBREAK_SLOTS < plugin.FAVORITES_FIRST_SHIFT
    now = KICKOFF
    far = now + timedelta(days=plugin.MAX_LOOKAHEAD_DAYS)
    fav = plugin._kickoff_offset(far, _marker(1), timezone.utc, frozenset({_marker(1)}))
    other = plugin._kickoff_offset(now, _marker(2), timezone.utc, frozenset({_marker(1)}))
    assert fav < other


@pytest.mark.parametrize("raw, days", [
    (7, 7), ("14", 14), ("", 7), (None, 7), ("nan", 7), ("abc", 7),
    (-3, 0), (0, 0), (400, 400), (500, 400), (10**9, 400),
])
def test_lookahead_is_parse_safe_and_capped(plugin, raw, days):
    # Literals, not MAX_LOOKAHEAD_DAYS: the cap is what is under test.
    assert plugin._resolve_lookahead_days({"lookahead_days": raw}) == days


def test_leaders_are_none_when_off_and_follow_the_current_setting(plugin):
    games = [_fav(1, KICKOFF, 1), _other(2, KICKOFF, 1)]
    assert plugin._numbering_leaders(games, {"favorites": "Georgia"}) is None
    assert plugin._numbering_leaders(games, {"favorites_first": True}) == frozenset()
    # The cache's favorites_matched is stale by design; the setting decides.
    games[1]["favorites_matched"] = ["Texas"]
    assert plugin._numbering_leaders(games, ON) == frozenset({_marker(1)})


def test_offset_shift_applies_only_behind_the_leaders(plugin):
    base = plugin.stable_channel_number(0, KICKOFF, _marker(2), timezone.utc)
    off = plugin._kickoff_offset
    assert off(KICKOFF, _marker(2), timezone.utc) == base
    assert off(KICKOFF, _marker(2), timezone.utc, frozenset({_marker(2)})) == base
    assert off(KICKOFF, _marker(2), timezone.utc, frozenset()) == base + plugin.FAVORITES_FIRST_SHIFT


def test_the_setting_is_declared_off_by_default(plugin):
    with open(os.path.join(HERE, os.pardir, "plugin.json"), encoding="utf-8") as f:
        field = next(x for x in json.load(f)["fields"] if x["id"] == "favorites_first")
    assert field["type"] == "boolean" and field["default"] is False


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_favorites_first(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
