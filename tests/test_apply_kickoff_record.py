"""A nudged kickoff channel keeps its number when a game below it leaves (#278).

The defect: two games in one kickoff minute share a tiebreak slot, so the second
is nudged one number up, past the end of that minute's 16-number block. #267
holds such a channel only while every number from its computed one up to its own
stays taken. When the game below it leaves the slate (same-minute games finish
at different times) that chain breaks, apply released the nudged channel back to
its computed number, and a live channel moved, often while its game was still on
air. Dispatcharr's outputs bind the guide by the bare number (#117).

The fix keeps a record in numbering_state.json of the kickoff offset and number
each published channel was numbered for, and holds a channel whose game still
has that offset. These scenarios drive the real `_action_apply` against a real
Django ORM (tests/orm_harness/run_scenarios.py), because the claims are about
what apply writes, to the channels and to the state file, and when.

The three Georgia/Alabama-style games kick off together on 2030-01-05 at 20:00
UTC, published from the auto floor 11 (the user's own channel is 10). Their
markers hash to these slots (pinned by test_the_fixture_slots_are_what_the_literals_assume):
CFB 415 -> 14 (33772825), CFB 411 and 431 -> 15 (33772826, the block's last
number), CFB 450 -> 15. So 431 is nudged to 33772827, past the block.
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
_now, _settings, _seed_source_channel, _row, _write_cache, _publish, _owned = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish, _kit._owned,
)

KICKOFF = datetime(2030, 1, 5, 20, 0, tzinfo=timezone.utc)
G415, G411, G431, G450 = (f"ranked_matchups:CFB:{i}" for i in (415, 411, 431, 450))


# --------------------------------------------------------------- helpers ---

def _game(gid, channel_id):
    return _row(gid, KICKOFF, channel_id, home=f"Home{gid}", away=f"Away{gid}")


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


def _three_published(env):
    """415, 411 and 431 on air from floor 11, 431 nudged past the block."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_game(g, user_ch.id) for g in (415, 411, 431)])
    assert _numbers(env) == {G415: 33772825, G411: 33772826, G431: 33772827}
    return user_ch


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_witness_a_nudged_channel_keeps_its_number_when_the_game_below_it_leaves(env):
    """The issue's shape: 411 (33772826, the block's last number) leaves the
    slate. Without the record 431's chain had a gap at 33772826, so it was
    released onto it and moved while its game was still on."""
    user_ch = _three_published(env)
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)])
    assert _numbers(env) == {G415: 33772825, G431: 33772827}
    # The record names what is published now: 411's entry is pruned with it.
    assert _state(env) == {
        "kickoff_base": 11,
        "numbered_for": {G415: [33772814, 33772825, 11], G431: [33772815, 33772827, 11]},
    }


def scenario_witness_a_newcomer_takes_the_freed_number_not_the_nudged_channel(env):
    """411 leaves as 450 (same minute, same slot) arrives. 450 takes the number
    411 freed; 431 stays. Without the record 431 was released onto 33772826
    and 450 put on 33772827, so two numbers changed hands at once."""
    user_ch = _three_published(env)
    _apply(env, [_game(g, user_ch.id) for g in (415, 431, 450)])
    assert _numbers(env) == {G415: 33772825, G431: 33772827, G450: 33772826}


def scenario_witness_a_channel_kept_for_a_recording_keeps_its_record(env):
    """431 leaves the slate while it is recording, so apply keeps its channel
    (#146) on its number. Its record entry must survive that apply: when 431
    returns and 411 has left, it keeps 33772827 instead of being released."""
    m = env.m
    user_ch = _three_published(env)
    ch431 = _owned(env)[G431]
    m.Recording.objects.create(
        channel=ch431, start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    _apply(env, [_game(415, user_ch.id), _game(411, user_ch.id)])
    assert _numbers(env) == {G415: 33772825, G411: 33772826, G431: 33772827}
    assert _state(env).get("numbered_for", {}).get(G431) == [33772815, 33772827, 11]
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)])
    assert _numbers(env) == {G415: 33772825, G431: 33772827}


def scenario_witness_a_kept_channel_keeps_its_record_across_floor_changes(env):
    """Second opinion (#278 round 1): while 431 is kept for its recording,
    the user sets Starting channel number to 12 and then back to 11. The kept
    channel never moves off 33772827, the number floor 11 gave it, so its
    entry (naming floor 11) must outlive the floor-12 apply: when 431 returns
    under floor 11 and 411 has left, it keeps 33772827."""
    m = env.m
    user_ch = _three_published(env)
    m.Recording.objects.create(
        channel=_owned(env)[G431], start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    for base in (12, 11):
        _apply(env, [_game(415, user_ch.id), _game(411, user_ch.id)], virtual_channel_base=base)
        assert _numbers(env) == {G415: 33772825, G411: 33772826, G431: 33772827}, base
        assert _state(env)["numbered_for"][G431] == [33772815, 33772827, 11], base
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)], virtual_channel_base=11)
    assert _numbers(env) == {G415: 33772825, G431: 33772827}


def scenario_witness_a_skipped_recording_channel_keeps_its_number(env):
    """#286: 431 is recording when its game moves to 20:02 and loses its feed,
    scoring too low for a placeholder, so apply skips it and keeps the channel
    for its recording (#146). Its old number 33772827 is no longer where its
    game numbers (20:02's block starts at 33772843), and CFB 400, new at 20:01,
    computes exactly 33772827. The numbers held back for kept channels only
    covered games that had LEFT the slate, so 400 was handed 33772827, the
    restore after parking found it taken, and the recording channel was left
    on a parking number for the rest of the recording."""
    m = env.m
    user_ch = _three_published(env)
    ch431 = _owned(env)[G431]
    m.Recording.objects.create(
        channel=ch431, start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    moved = _row(431, KICKOFF + timedelta(minutes=2), 999999, home="Home431", away="Away431")
    moved["score"] = 1.0  # below the placeholder threshold, no source channel
    result = _apply(env, [
        _game(415, user_ch.id), _game(411, user_ch.id), moved,
        _row(400, KICKOFF + timedelta(minutes=1), user_ch.id, home="Home400", away="Away400"),
    ])
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert got == {
        G415: 33772825, G411: 33772826, G431: 33772827, "ranked_matchups:CFB:400": 33772828,
    }, got


def scenario_witness_an_inferred_floor_is_judged_with_the_kept_numbers(env):
    """Second opinion on #286, round 1: with no state file the floor is
    inferred, and the inference must hold back the same numbers the placement
    does. CFB 129 (recording) moves from 20:00 to 20:01, loses its feed and is
    skipped, and CFB 128 is new at 20:02. Judged without 129's number the
    inference picked floor 16, and the placement then moved 150, which is
    still written, from 33772833 to 33772831. Judged with it, floor 17 keeps
    every number."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    m.Channel.objects.filter(pk=user_ch.pk).update(channel_number=16)
    spec = {129: 0, 150: 0, 103: 2, 126: 3, 115: 3}
    _publish(env, [
        _row(g, KICKOFF + timedelta(minutes=k), user_ch.id, home=f"Home{g}", away=f"Away{g}")
        for g, k in spec.items()
    ])
    before = _numbers(env)
    assert before == {
        "ranked_matchups:CFB:129": 33772832, "ranked_matchups:CFB:150": 33772833,
        "ranked_matchups:CFB:103": 33772849, "ranked_matchups:CFB:126": 33772866,
        "ranked_matchups:CFB:115": 33772867,
    }, before
    m.Recording.objects.create(
        channel=_owned(env)["ranked_matchups:CFB:129"], start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    os.remove(env.plugin.NUMBERING_STATE_PATH)
    moved = _row(129, KICKOFF + timedelta(minutes=1), 999999, home="Home129", away="Away129")
    moved["score"] = 1.0
    rows = [moved] + [
        _row(g, KICKOFF + timedelta(minutes=k), user_ch.id, home=f"Home{g}", away=f"Away{g}")
        for g, k in spec.items() if g != 129
    ] + [_row(128, KICKOFF + timedelta(minutes=2), user_ch.id, home="Home128", away="Away128")]
    result = _apply(env, rows)
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert {m_: got[m_] for m_ in before} == before, got
    assert _state(env)["kickoff_base"] == 17


def scenario_compact_mode_keeps_a_skipped_recording_channels_slot(env):
    """The #286 shape in COMPACT numbering, which takes no second placement:
    every published game keeps its own slot there, so the skipped recording
    channel's slot is never handed to the new game, which goes above the
    highest slot in use."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500}
    _publish(env, [_game(g, user_ch.id) for g in (415, 411, 431)], settings=_settings(**compact))
    assert _numbers(env) == {G411: 500, G415: 501, G431: 502}
    m.Recording.objects.create(
        channel=_owned(env)[G431], start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    moved = _row(431, KICKOFF + timedelta(minutes=2), 999999, home="Home431", away="Away431")
    moved["score"] = 1.0
    result = _apply(env, [
        _game(415, user_ch.id), _game(411, user_ch.id), moved,
        _row(400, KICKOFF + timedelta(minutes=1), user_ch.id, home="Home400", away="Away400"),
    ], **compact)
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert got == {G411: 500, G415: 501, G431: 502, "ranked_matchups:CFB:400": 503}, got


def scenario_a_dry_run_writes_no_record(env):
    """A dry run on a fresh install writes no state file, and one after a real
    apply leaves it byte for byte, even when the slate it rehearses would
    change the record."""
    user_ch, _ = _seed_source_channel(env)
    path = env.plugin.NUMBERING_STATE_PATH
    _apply(env, [_game(g, user_ch.id) for g in (415, 411, 431)], dry_run=True)
    assert not os.path.exists(path)
    _publish(env, [_game(g, user_ch.id) for g in (415, 411, 431)])
    with open(path, "rb") as f:
        before = f.read()
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)], dry_run=True)
    with open(path, "rb") as f:
        assert f.read() == before
    assert _numbers(env) == {G415: 33772825, G411: 33772826, G431: 33772827}


def scenario_a_new_floor_does_not_trust_the_record(env):
    """The user sets Starting channel number to 1. Every entry names floor 11,
    where 431's offset meant 33772827; under floor 1 every channel moves to the
    new floor's numbers. Trusted anyway, the record would have held all three
    on their floor-11 numbers, ten above where floor 1 puts them."""
    user_ch = _three_published(env)
    _apply(env, [_game(g, user_ch.id) for g in (415, 411, 431)], virtual_channel_base=1)
    assert _numbers(env) == {G415: 33772815, G411: 33772816, G431: 33772817}
    assert _state(env) == {
        "kickoff_base": 1,
        "numbered_for": {
            G411: [33772815, 33772816, 1], G415: [33772814, 33772815, 1],
            G431: [33772815, 33772817, 1],
        },
    }


def scenario_a_lost_state_file_falls_back_to_the_inference(env):
    """No file, so no floor and no record: the floor is inferred by running the
    placement with no record (#219), exactly as apply then places. Floor 11
    would release 431 onto 33772826 (the chain has a gap); floor 12 puts both
    survivors inside their block, so the inference picks it and nothing moves.
    The record is then rebuilt under floor 12 (offsets do not depend on it)."""
    user_ch = _three_published(env)
    os.remove(env.plugin.NUMBERING_STATE_PATH)
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)])
    assert _numbers(env) == {G415: 33772825, G431: 33772827}
    assert _state(env) == {
        "kickoff_base": 12,
        "numbered_for": {G415: [33772814, 33772825, 12], G431: [33772815, 33772827, 12]},
    }


def scenario_a_damaged_record_falls_back_to_the_chain_rule(env):
    """The floor survives but 431's entry does not parse (two values, no
    floor): 431 alone falls back to the chain rule and is released; the apply
    writes a good record."""
    user_ch = _three_published(env)
    with open(env.plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"kickoff_base": 11, "numbered_for": {
            G415: [33772814, 33772825, 11], G431: [33772815, 33772827],
        }}, f)
    _apply(env, [_game(415, user_ch.id), _game(431, user_ch.id)])
    assert _numbers(env) == {G415: 33772825, G431: 33772826}
    assert _state(env)["numbered_for"] == {
        G415: [33772814, 33772825, 11], G431: [33772815, 33772826, 11],
    }


# ----------------------------------------------------- in-process checks ---

def test_the_fixture_slots_are_what_the_literals_assume(plugin):
    for gid, number in ((415, 33772825), (411, 33772826), (431, 33772826), (450, 33772826)):
        marker = plugin._build_marker_key(_game(gid, 1))
        assert marker == f"ranked_matchups:CFB:{gid}"
        assert plugin.stable_channel_number(11, KICKOFF, marker, timezone.utc) == number, gid
    # #286: 400 at 20:01 lands on 20:01's first number, 431 at 20:02 on 20:02's last.
    for gid, start, number in ((400, KICKOFF + timedelta(minutes=1), 33772827),
                               (431, KICKOFF + timedelta(minutes=2), 33772858)):
        marker = f"ranked_matchups:CFB:{gid}"
        assert plugin.stable_channel_number(11, start, marker, timezone.utc) == number, gid


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_kickoff_record(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
