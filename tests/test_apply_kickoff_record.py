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


def scenario_witness_a_full_compact_band_gives_a_skipped_games_slot_to_a_new_game(env):
    """#288: a compact band of one number, 500, holds 411. 411 then loses its
    feed and scores below the placeholder threshold (no recording), and 431 is
    new and playable. Slots were allocated before apply knew 411 would be
    skipped, so 411 kept 500, 431 was dropped for want of a slot, and 411's
    channel was then reaped: the band sat empty until the next apply."""
    user_ch, _ = _seed_source_channel(env)
    compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
               "compact_band_size": 1}
    _publish(env, [_game(411, user_ch.id)], settings=_settings(**compact))
    assert _numbers(env) == {G411: 500}
    gone = _row(411, KICKOFF, 999999, home="Home411", away="Away411")
    gone["score"] = 1.0  # no source channel, below the placeholder threshold
    result = _apply(env, [gone, _game(431, user_ch.id)], **compact)
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert got == {G431: 500}, got


def scenario_a_skipped_game_that_is_not_kept_holds_no_number(env):
    """#288's class in KICKOFF mode: only the games apply writes are numbered.
    411 loses its feed (no recording, so its channel is reaped) as 450, on
    the same slot, arrives: 450 is placed on 33772826 as if 411 had left the
    slate. Numbered anyway, 411 kept 33772826 for a channel about to be
    deleted, and 450 was nudged past the block, past 431, to 33772828."""
    user_ch = _three_published(env)
    gone = _row(411, KICKOFF, 999999, home="Home411", away="Away411")
    gone["score"] = 1.0
    result = _apply(env, [_game(415, user_ch.id), gone, _game(431, user_ch.id),
                          _game(450, user_ch.id)])
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert got == {G415: 33772825, G431: 33772827, G450: 33772826}, got


def scenario_a_compact_game_left_without_a_slot_is_reaped_not_stranded(env):
    """The 2026-08-29 stranding, after #288 moved numbering behind the
    pre-pass. A band of 500-501 holds 411 and 431; the user shrinks it to one
    number. 411 keeps 500 and 431 has no slot. Its plan must be dropped before
    seen_markers is built: marked seen and then skipped by the write loop, its
    channel was neither renumbered nor reaped and stayed on a parking number."""
    user_ch, _ = _seed_source_channel(env)
    band = {"channel_numbering_mode": "compact", "virtual_channel_base": 500}
    _publish(env, [_game(g, user_ch.id) for g in (411, 431)],
             settings=_settings(compact_band_size=2, **band))
    assert _numbers(env) == {G411: 500, G431: 501}
    _apply(env, [_game(g, user_ch.id) for g in (411, 431)], compact_band_size=1, **band)
    got = _numbers(env)
    assert got == {G411: 500}, got


def scenario_an_inferred_floor_ignores_the_channels_of_skipped_games(env):
    """#288: the floor inference runs the placement apply runs, over the games
    apply writes. 411 and 400 sit on the numbers floor 12 would give them; both
    lose their feed (no recording, so both are reaped) and the state file is
    gone. Judged over the whole slate, floor 12 kept three channels against
    floor 11's two and was remembered, one above the floor 415 was numbered
    from; judged over 415 alone, floor 11 names it exactly."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    later = _row(400, KICKOFF + timedelta(minutes=1), user_ch.id, home="Home400", away="Away400")
    _publish(env, [_game(415, user_ch.id), _game(411, user_ch.id), later])
    owned = _owned(env)
    m.Channel.objects.filter(pk=owned[G411].pk).update(channel_number=33772827)
    m.Channel.objects.filter(pk=owned["ranked_matchups:CFB:400"].pk).update(channel_number=33772828)
    os.remove(env.plugin.NUMBERING_STATE_PATH)
    gone = [_row(411, KICKOFF, 999999, home="Home411", away="Away411"),
            _row(400, KICKOFF + timedelta(minutes=1), 999999, home="Home400", away="Away400")]
    for g in gone:
        g["score"] = 1.0
    result = _apply(env, [_game(415, user_ch.id)] + gone)
    assert "unmatched_skipped=2" in result["message"], result
    assert _numbers(env) == {G415: 33772825}
    assert _state(env)["kickoff_base"] == 11


def scenario_witness_a_recording_apply_cancels_does_not_hold_a_full_bands_slot(env):
    """Second opinion on #288: 411 on 500 (a band of one) has a recording the
    plugin scheduled for a Recorded team. The user clears Recorded teams, 411
    loses its feed, and 431 is new and playable. The recording is cancelled by
    this apply before the reap, so 411's channel is reaped; counted as keeping
    the channel, it held 500 and the full band refused 431 (an error, nothing
    published)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
               "compact_band_size": 1}
    soon = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, soon, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(recorded_teams="Home411", **compact))
    rec = m.Recording.objects.get(channel=owned[G411])
    assert rec.custom_properties.get("ranked_matchups_marker") == G411
    gone = _row(411, soon, 999999, home="Home411", away="Away411")
    gone["score"] = 1.0
    result = _apply(env, [gone, _row(431, soon, user_ch.id, home="Home431", away="Away431")],
                    recorded_teams="", **compact)
    assert "unmatched_skipped=1" in result["message"], result
    got = _numbers(env)
    assert got == {G431: 500}, got
    assert not m.Recording.objects.filter(pk=rec.pk).exists()


def scenario_witness_a_recording_starting_soon_is_cancelled_before_numbering(env):
    """Second opinion on #288, round 2: the same, with the unwanted recording
    due to start in ten minutes. An earlier revision left out of the held
    numbers only cancellations starting 30+ minutes ahead, so this one still
    held 500, the full band aborted before the cancellation ran, and the
    recording went ahead. Cancelling before numbering reads the real state."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
               "compact_band_size": 1}
    kickoff = _now() + timedelta(minutes=40)  # recording starts 30 minutes before
    owned = _publish(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(recorded_teams="Home411", **compact))
    rec = m.Recording.objects.get(channel=owned[G411])
    assert _now() < rec.start_time < _now() + timedelta(minutes=30)
    gone = _row(411, kickoff, 999999, home="Home411", away="Away411")
    gone["score"] = 1.0
    _apply(env, [gone, _row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
           recorded_teams="", **compact)
    got = _numbers(env)
    assert got == {G431: 500}, got
    assert not m.Recording.objects.filter(pk=rec.pk).exists()


def scenario_witness_an_aborted_apply_keeps_the_cancellation_bookkeeping(env):
    """Second opinion on #288, round 3: the cancellation now commits before
    numbering, so an apply that then aborts (a full compact band) must still
    drop the cancelled recording from the "created" state. Left there, the
    next plan saw the row missing and tombstoned the game as deleted by the
    user, never to be recorded again."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
               "compact_band_size": 1}
    soon = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, soon, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(recorded_teams="Home411", **compact))
    rec = m.Recording.objects.get(channel=owned[G411])
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert G411 in json.load(f)["created"]
    m.Channel.objects.create(name="Stranger", channel_number=500, channel_group=user_ch.channel_group)
    gone = _row(411, soon, 999999, home="Home411", away="Away411")
    gone["score"] = 1.0
    _write_cache(env, [gone, _row(431, soon, user_ch.id, home="Home431", away="Away431")])
    result = env.plugin._action_apply(_settings(recorded_teams="", **compact))
    assert result["status"] == "error", result
    assert not m.Recording.objects.filter(pk=rec.pk).exists()
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        state = json.load(f)
    assert G411 not in state["created"], state
    assert G411 not in state["tombstones"], state


def scenario_logos_are_fetched_only_for_games_apply_writes(env):
    """Second opinion on #288, round 4: the network half of the pre-pass
    (descriptions, logos) runs after numbering, for placed games only. A band
    of one keeps 411 and leaves 431 without a slot; only 411's logo is looked
    up. Run before numbering, it spent the network budget on 431 too, and on
    a full band the fetches were made for an apply that then aborted."""
    import importlib
    logos = importlib.import_module(env.plugin.__name__.rsplit(".", 1)[0] + ".logos")
    looked_up = []
    saved = (logos.resolve_thumb_url, logos.league_id_for)
    logos.resolve_thumb_url = lambda home, away, *a, **k: looked_up.append(home)
    logos.league_id_for = lambda *a, **k: None
    try:
        user_ch, _ = _seed_source_channel(env)
        compact = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
                   "compact_band_size": 1, "enable_matchup_logos": True,
                   "gamethumbs_base_url": ""}
        _publish(env, [_game(411, user_ch.id)], settings=_settings(**compact))
        looked_up.clear()
        os.remove(env.plugin.SPORTSDB_THUMB_CACHE_PATH)  # so 411 is looked up again
        _apply(env, [_game(411, user_ch.id), _game(431, user_ch.id)], **compact)
        assert _numbers(env) == {G411: 500}
        assert looked_up == ["Home411"], looked_up
    finally:
        logos.resolve_thumb_url, logos.league_id_for = saved


def scenario_witness_a_slotless_game_does_not_take_the_incumbents_recording(env):
    """#291: a compact band of one number holds 411, a Recorded team's game at
    20:00 with its recording scheduled, and the plan has one recording slot.
    431, an earlier Recorded game overlapping it, arrives. The plan was built
    over the whole slate before numbering, so it gave the slot to 431 and
    cancelled 411's recording; numbering then kept 411 on 500 and left 431
    without a slot, so 431 got no channel and no recording either: nothing was
    recorded. A game without a channel cannot be recorded, so the plan must be
    built over the games apply places."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    settings = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
                "compact_band_size": 1, "recorded_teams": "Home411, Home431",
                "recording_slots": 1}
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(**settings))
    rec = m.Recording.objects.get(channel=owned[G411])
    result = _apply(env, [
        _row(411, kickoff, user_ch.id, home="Home411", away="Away411"),
        _row(431, kickoff - timedelta(minutes=30), user_ch.id, home="Home431", away="Away431"),
    ], **settings)
    got = _numbers(env)
    assert got == {G411: 500}, got
    recs = list(m.Recording.objects.values_list("channel_id", flat=True))
    assert m.Recording.objects.filter(pk=rec.pk).exists(), (recs, result["message"])
    assert recs == [owned[G411].id], recs


def scenario_witness_a_skipped_ranked_game_does_not_take_the_recording_slot(env):
    """#291's class in KICKOFF mode: any game apply does not write has no
    channel to record on. With Record ranked games on and one slot, 431 (an
    earlier ranked game whose matched channel no longer exists, so it has no
    feed and scores too low for a placeholder: skipped) still counted as
    eligible, because eligibility reads the cached match. Planned over the
    whole slate it took the slot, and 411, written with a feed, was left
    with no recording and nothing recorded at all."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    settings = {"record_ranked_games": True, "recording_slots": 1}
    kickoff = _now() + timedelta(hours=6)
    skipped = _row(431, kickoff - timedelta(minutes=30), 999999, home="Home431", away="Away431")
    skipped["score"] = 1.0
    result = _apply(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411"), skipped],
                    **settings)
    assert "unmatched_skipped=1" in result["message"], result
    owned = _owned(env)
    assert set(owned) == {G411}
    recs = list(m.Recording.objects.values_list("channel_id", flat=True))
    assert recs == [owned[G411].id], (recs, result["message"])


def _ranked_recording_then_skipped(env):
    """431, a ranked game with a feed, gets a recording; then its feed goes
    and it scores too low for a placeholder, so apply skips it."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    settings = {"record_ranked_games": True, "recording_slots": 1}
    owned = _publish(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
                     settings=_settings(**settings))
    rec = m.Recording.objects.get(channel=owned[G431])
    skipped = _row(431, kickoff, 999999, home="Home431", away="Away431")
    skipped["score"] = 1.0
    result = _apply(env, [skipped], **settings)
    assert "unmatched_skipped=1" in result["message"], result
    return rec, user_ch, kickoff


def scenario_a_skipped_games_scheduled_recording_is_cancelled(env):
    """#291: a game apply does not write has no channel to record on, so the
    plan leaves it out; the cache still knows it, so its scheduled recording
    is cancelled as known and unplanned, and its channel reaped. It is planned
    again on the apply that gives it a channel. (Kept as "missing from the
    slate" instead, a review found it judged by a stored reason the cache had
    moved past, and outside the slot budget.)"""
    rec, _user_ch, _kickoff = _ranked_recording_then_skipped(env)
    assert not env.m.Recording.objects.filter(pk=rec.pk).exists()
    assert _owned(env) == {}


def scenario_a_recording_for_a_game_the_cache_corrected_is_cancelled(env):
    """#291 review: the cache corrects 431's teams (same event id, so the same
    marker) to ones no longer Recorded, and the game has no feed. Judged as
    missing from the slate, its recording was checked against the OLD teams
    stored on it and kept; the cache knows the game, so it is cancelled."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
                     settings=_settings(recorded_teams="Home431"))
    rec = m.Recording.objects.get(channel=owned[G431])
    corrected = _row(431, kickoff, 999999, home="Home999", away="Away999")
    corrected["score"] = 1.0
    result = _apply(env, [corrected], recorded_teams="Home431")
    assert "unmatched_skipped=1" in result["message"], result
    assert not m.Recording.objects.filter(pk=rec.pk).exists()


def scenario_witness_a_recording_kept_for_a_missing_game_occupies_its_slot(env):
    """#291 review (the #224 path): 411's game vanishes from the cache while
    its Recorded team is still listed, so its scheduled recording is kept as
    a possible source outage. The plan did not count it, and with one slot
    scheduled overlapping 431 beside it: two captures for one stream."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    settings = {"recorded_teams": "Home411, Home431", "recording_slots": 1}
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(**settings))
    rec = m.Recording.objects.get(channel=owned[G411])
    result = _apply(env, [_row(431, kickoff + timedelta(minutes=30), user_ch.id,
                               home="Home431", away="Away431")], **settings)
    assert m.Recording.objects.filter(pk=rec.pk).exists(), result["message"]
    assert m.Recording.objects.count() == 1, (
        list(m.Recording.objects.values_list("channel_id", flat=True)), result["message"])


def scenario_a_guide_kept_skipped_ranked_game_holds_no_recording_slot(env):
    """#291 review, decided: the cache cannot vouch for the slate (lost during
    an outage) and lists 431 with no feed, below the placeholder threshold.
    Apply keeps 431's unstamped channel by its guide (#264), but a ranked game
    with no stream attached is never eligible (_ranked_eligible), so its
    scheduled recording is cancelled like any skipped game's and planned again
    once a refresh gives it a stream. Judged by its stored reason instead (an
    earlier revision of this fix), it kept a recording through cache rows that
    had moved on, and through a stream the group policy had just excluded."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    settings = {"record_ranked_games": True, "recording_slots": 1}
    owned = _publish(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
                     settings=_settings(**settings))
    rec = m.Recording.objects.get(channel=owned[G431])
    _kit._strip_stamps(env)
    m.ChannelStream.objects.filter(channel=user_ch).delete()
    skipped = _row(431, kickoff, user_ch.id, home="Home431", away="Away431")
    skipped["score"] = 1.0
    _write_cache(env, [skipped], empty_slate_known=False)
    result = env.plugin._action_apply(_settings(**settings))
    assert result["status"] == "ok", result
    assert "unmatched_skipped=1" in result["message"], result
    assert set(_owned(env)) == {G431}, "the guide keeps 431's channel"
    assert not m.Recording.objects.filter(pk=rec.pk).exists(), result["message"]


def scenario_a_guide_kept_game_the_cache_corrected_loses_its_recording(env):
    """#291 review, round 5 (guard): the guide-kept case above with the cache
    (unvouched) correcting 431 to teams no longer Recorded. The game stays in
    the plan's slate, known and unplanned: its recording is cancelled rather
    than kept by the old teams stored on it."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
                     settings=_settings(recorded_teams="Home431"))
    rec = m.Recording.objects.get(channel=owned[G431])
    _kit._strip_stamps(env)
    m.ChannelStream.objects.filter(channel=user_ch).delete()
    corrected = _row(431, kickoff, user_ch.id, home="Home999", away="Away999")
    corrected["score"] = 1.0
    _write_cache(env, [corrected], empty_slate_known=False)
    result = env.plugin._action_apply(_settings(recorded_teams="Home431"))
    assert result["status"] == "ok", result
    assert "unmatched_skipped=1" in result["message"], result
    assert not m.Recording.objects.filter(pk=rec.pk).exists(), result["message"]


def scenario_witness_a_streamless_ranked_placeholder_does_not_take_the_slot(env):
    """#291 review: 431 kicks off first and scores high enough for a
    placeholder, but the channel its cached match names no longer exists, so
    its published channel carries no stream. Eligibility read the cached match,
    so 431 took the only slot and 411, playable, recorded nothing."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    stale = _row(431, kickoff - timedelta(minutes=30), 999999, home="Home431", away="Away431")
    result = _apply(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411"), stale],
                    record_ranked_games=True, recording_slots=1)
    owned = _owned(env)
    assert set(owned) == {G411, G431}, "431 is published as a placeholder"
    recs = list(m.Recording.objects.values_list("channel_id", flat=True))
    assert recs == [owned[G411].id], (recs, result["message"])


def scenario_witness_a_recording_the_user_edited_occupies_its_slot(env):
    """#291 review: the user extended 411's scheduled recording, so it is
    theirs (never moved or cancelled). With 411 gone from the cache and its
    team no longer Recorded, the plan did not count it, and with one slot
    scheduled an overlapping 431 beside it."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(recorded_teams="Home411", recording_slots=1))
    rec = m.Recording.objects.get(channel=owned[G411])
    m.Recording.objects.filter(pk=rec.pk).update(end_time=rec.end_time + timedelta(minutes=30))
    result = _apply(env, [_row(431, kickoff + timedelta(minutes=30), user_ch.id,
                               home="Home431", away="Away431")],
                    recorded_teams="Home431", recording_slots=1)
    assert m.Recording.objects.filter(pk=rec.pk).exists(), result["message"]
    assert m.Recording.objects.count() == 1, (
        list(m.Recording.objects.values_list("channel_id", flat=True)), result["message"])


def scenario_witness_a_deletion_seen_before_a_game_loses_its_slot_is_tombstoned(env):
    """#291 review: the user deleted 431's recording, then shrank the compact
    band so 431 has no slot. The first plan (over the decided games) saw the
    deletion; the second (over the placed ones) no longer considers 431, and
    the tombstone was dropped with the first plan, so a later apply that gave
    431 a slot again would record what the user deleted."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    band = {"channel_numbering_mode": "compact", "virtual_channel_base": 500,
            "recorded_teams": "Home411, Home431"}
    rows = [_row(411, kickoff, user_ch.id, home="Home411", away="Away411"),
            _row(431, kickoff + timedelta(hours=3), user_ch.id, home="Home431", away="Away431")]
    owned = _publish(env, rows, settings=_settings(compact_band_size=2, **band))
    assert _numbers(env) == {G411: 500, G431: 501}
    m.Recording.objects.filter(channel=owned[G431]).delete()  # the user, in the DVR tab
    _apply(env, rows, compact_band_size=1, **band)
    assert _numbers(env) == {G411: 500}
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert G431 in json.load(f)["tombstones"]


def scenario_witness_a_deletion_while_a_game_has_no_channel_is_tombstoned(env):
    """#291 review, final round: the user deleted 431's recording, and on the
    next apply 431 has no channel (no feed, too low for a placeholder), so it
    is no plan candidate. The deletion must still become a tombstone: left
    undetected, 431's "created" entry aged out while it was away and the game
    was recorded again when it came back."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    settings = {"record_ranked_games": True, "recording_slots": 1}
    owned = _publish(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
                     settings=_settings(**settings))
    m.Recording.objects.filter(channel=owned[G431]).delete()  # the user, in the DVR tab
    skipped = _row(431, kickoff, 999999, home="Home431", away="Away431")
    skipped["score"] = 1.0
    result = _apply(env, [skipped], **settings)
    assert "unmatched_skipped=1" in result["message"], result
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert G431 in json.load(f)["tombstones"]


def scenario_witness_a_deletion_while_a_game_is_on_the_bench_is_tombstoned(env):
    """#291 review: the same, with 431 moved to the bench (the list is full)
    instead of losing its channel. Bench games are never candidates either."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    settings = {"record_ranked_games": True, "recording_slots": 1}
    row431 = _row(431, kickoff, user_ch.id, home="Home431", away="Away431")
    owned = _publish(env, [row431], settings=_settings(**settings))
    m.Recording.objects.filter(channel=owned[G431]).delete()  # the user, in the DVR tab
    _write_cache(env, [_row(411, kickoff + timedelta(days=1), user_ch.id,
                            home="Home411", away="Away411")], bench=[row431])
    result = env.plugin._action_apply(_settings(**settings))
    assert result["status"] == "ok", result
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert G431 in json.load(f)["tombstones"]


def scenario_witness_a_benched_games_deletion_is_matched_against_current_teams(env):
    """#291 review: the bench row still carries the match its refresh made
    (none: 431 was recorded as a ranked game), and the user has since added
    431's team to Recorded teams and turned ranked recording off. The watch
    must use the CURRENT teams, as the games do, to see the deletion."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    row431 = _row(431, kickoff, user_ch.id, home="Home431", away="Away431")
    owned = _publish(env, [row431], settings=_settings(record_ranked_games=True, recording_slots=1))
    m.Recording.objects.filter(channel=owned[G431]).delete()  # the user, in the DVR tab
    benched = dict(row431, recorded_matched=[])
    _write_cache(env, [_row(411, kickoff + timedelta(days=1), user_ch.id,
                            home="Home411", away="Away411")], bench=[benched])
    result = env.plugin._action_apply(_settings(recorded_teams="Home431, Home411"))
    assert result["status"] == "ok", result
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        assert G431 in json.load(f)["tombstones"]


def scenario_a_skipped_games_recording_is_planned_again_with_its_channel(env):
    """The other half: its feed returns, it is written, and planned again."""
    m = env.m
    _rec, user_ch, kickoff = _ranked_recording_then_skipped(env)
    _apply(env, [_row(431, kickoff, user_ch.id, home="Home431", away="Away431")],
           record_ranked_games=True, recording_slots=1)
    owned = _owned(env)
    assert set(owned) == {G431}
    assert list(m.Recording.objects.values_list("channel_id", flat=True)) == [owned[G431].id]


def _allowed_source_channel(env):
    """A user channel whose one stream sits in group "Allowed" (the seeded
    ones are all in "Sports")."""
    m = env.m
    acct = m.M3UAccount.objects.create(name="acct-allowed", max_streams=4)
    grp, _ = m.ChannelGroup.objects.get_or_create(name="Allowed")
    stream = m.Stream.objects.create(name="Allowed HD", m3u_account=acct, channel_group=grp)
    ch = m.Channel.objects.create(name="Allowed", channel_number=20, channel_group=grp)
    m.ChannelStream.objects.create(channel=ch, stream=stream, order=0)
    return ch


def scenario_witness_a_kept_recording_whose_only_stream_is_excluded_holds_no_slot(env):
    """#297: 411's game vanishes from the cache (a source outage) while its
    team is still Recorded, so its scheduled recording is kept (#224) and its
    channel with it. The user has excluded the "Sports" group, so apply strips
    the kept channel's only stream (#206): the recording will capture nothing.
    It still held the only slot, and 431, playable from an allowed group, got
    no recording, on this apply and every one after."""
    m = env.m
    sports_ch, _ = _seed_source_channel(env)
    allowed_ch = _allowed_source_channel(env)
    settings = {"recorded_teams": "Home411, Home431", "recording_slots": 1}
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, kickoff, sports_ch.id, home="Home411", away="Away411")],
                     settings=_settings(**settings))
    rec = m.Recording.objects.get(channel=owned[G411])
    result = _apply(env, [_row(431, kickoff + timedelta(minutes=30), allowed_ch.id,
                               home="Home431", away="Away431")],
                    excluded_groups="Sports", **settings)
    owned = _owned(env)
    assert G431 in owned, result["message"]
    recs = list(m.Recording.objects.values_list("channel_id", flat=True))
    assert recs == [owned[G431].id], (recs, result["message"])
    assert not m.Recording.objects.filter(pk=rec.pk).exists()


def scenario_a_kept_recording_with_an_allowed_stream_left_is_kept(env):
    """#297's guard: 411's channel carries a stream from "Sports" AND one from
    "Allowed". Excluding "Sports" leaves it one it can record from, so the
    outage protection (#224) still keeps the recording, and it still holds
    the only slot."""
    m = env.m
    sports_ch, _ = _seed_source_channel(env)
    allowed_ch = _allowed_source_channel(env)
    settings = {"recorded_teams": "Home411, Home431", "recording_slots": 1}
    kickoff = _now() + timedelta(hours=6)
    both = _row(411, kickoff, sports_ch.id, home="Home411", away="Away411")
    both["channel_ids"] = [sports_ch.id, allowed_ch.id]
    owned = _publish(env, [both], settings=_settings(**settings))
    assert m.ChannelStream.objects.filter(channel=owned[G411]).count() == 2
    rec = m.Recording.objects.get(channel=owned[G411])
    _apply(env, [_row(431, kickoff + timedelta(minutes=30), allowed_ch.id,
                      home="Home431", away="Away431")],
           excluded_groups="Sports", **settings)
    assert m.Recording.objects.filter(pk=rec.pk).exists()
    assert m.Recording.objects.count() == 1
    assert m.ChannelStream.objects.filter(channel=owned[G411]).count() == 1


def scenario_a_kept_recording_on_a_channel_with_no_stream_is_cancelled(env):
    """The same rule without any exclusion: 411 was published as a placeholder
    (a Recorded team, no feed yet) and its game then vanished from the cache.
    Its channel has nothing to record from, so its recording is not kept as
    an outage survivor; it is planned again when the game returns."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    settings = {"recorded_teams": "Home411"}
    kickoff = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, kickoff, 999999, home="Home411", away="Away411")],
                     settings=_settings(**settings))
    assert m.ChannelStream.objects.filter(channel=owned[G411]).count() == 0
    rec = m.Recording.objects.get(channel=owned[G411])
    _apply(env, [_row(431, kickoff + timedelta(days=1), user_ch.id,
                      home="Home431", away="Away431")], **settings)
    assert not m.Recording.objects.filter(pk=rec.pk).exists()
    assert G411 not in _owned(env)
    # Back on the list with a feed: planned and recorded again.
    _apply(env, [_row(411, kickoff, user_ch.id, home="Home411", away="Away411")], **settings)
    owned = _owned(env)
    assert list(m.Recording.objects.values_list("channel_id", flat=True)) == [owned[G411].id]


def scenario_a_full_compact_band_still_aborts(env):
    """The abort moved behind the pre-pass (#288). A band of one whose number
    another channel publishes still refuses the new game with an error, and
    adds, renumbers and removes no channel."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    m.Channel.objects.create(name="Stranger", channel_number=500, channel_group=user_ch.channel_group)
    _write_cache(env, [_game(431, user_ch.id)])
    result = env.plugin._action_apply(_settings(
        channel_numbering_mode="compact", virtual_channel_base=500, compact_band_size=1,
    ))
    assert result["status"] == "error", result
    assert "no free channel numbers in 500-500" in result["message"], result
    assert _numbers(env) == {}


def scenario_witness_a_kept_channels_logo_file_is_not_swept(env):
    """Second opinion on #288: the logo sweep at the end of apply kept the
    files of the games apply wrote, and deleted the matchup logo of a channel
    kept for its recording (its game left the slate), so the live channel
    pointed at a deleted image for the rest of the recording. Logo lookups are
    stubbed to misses: nothing here may reach the network."""
    import importlib
    m = env.m
    logos = importlib.import_module(env.plugin.__name__.rsplit(".", 1)[0] + ".logos")
    # The harness runs every scenario in one process: put the lookups back.
    saved = (logos.resolve_thumb_url, logos.league_id_for)
    logos.resolve_thumb_url = lambda *a, **k: None
    logos.league_id_for = lambda *a, **k: None
    try:
        with_logos = {"enable_matchup_logos": True, "gamethumbs_base_url": ""}
        user_ch = _seed_source_channel(env)[0]
        _publish(env, [_game(g, user_ch.id) for g in (415, 431)], settings=_settings(**with_logos))
        m.Recording.objects.create(
            channel=_owned(env)[G431], start_time=_now() - timedelta(minutes=30),
            end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
        )
        os.makedirs(logos.LOGO_DIR, exist_ok=True)
        kept_logo = os.path.join(logos.LOGO_DIR, logos.marker_to_filename(G431))
        # Round 2: a channel re-keyed (#217) and then kept shows the file named
        # after its OLD marker; the kept 431 points at one here.
        old_name = os.path.join(logos.LOGO_DIR, logos.marker_to_filename("ranked_matchups:CFB:7302311"))
        gone_logo = os.path.join(logos.LOGO_DIR, logos.marker_to_filename("ranked_matchups:CFB:999"))
        for path in (kept_logo, old_name, gone_logo):
            with open(path, "wb") as f:
                f.write(b"jpg")
        logo = m.Logo.objects.create(name="old marker's logo", url=old_name)
        m.Channel.objects.filter(pk=_owned(env)[G431].pk).update(logo_id=logo.id)
        # Round 3: the user gave one of their own channels a matchup logo.
        users_pick = os.path.join(logos.LOGO_DIR, logos.marker_to_filename("ranked_matchups:CFB:12"))
        with open(users_pick, "wb") as f:
            f.write(b"jpg")
        pick = m.Logo.objects.create(name="user's pick", url=users_pick)
        # Not a source of any game here: a source's logo is copied onto the
        # matchup channel and would be kept through it instead.
        m.Channel.objects.create(name="Mine", channel_number=11, logo_id=pick.id,
                                 channel_group=user_ch.channel_group)
        _apply(env, [_game(415, user_ch.id)], **with_logos)
        assert set(_owned(env)) == {G415, G431}, "431 is kept for its recording"
        assert os.path.exists(kept_logo), "the kept channel's logo file was swept"
        assert os.path.exists(old_name), "the file the kept channel points at was swept"
        assert os.path.exists(users_pick), "the file the user's own channel points at was swept"
        assert not os.path.exists(gone_logo), "the sweep itself must still run"
    finally:
        logos.resolve_thumb_url, logos.league_id_for = saved


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
