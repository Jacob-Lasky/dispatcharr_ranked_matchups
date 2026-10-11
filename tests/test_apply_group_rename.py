"""Renaming the live group keeps the guide of a channel kept for a recording (#263).

The reported defect: a plugin channel is recording, its game has left the
cache, and the user renames the live group (`channel_profile_name`). Apply
migrated the channel into the new group, then deleted the old group's
EPGSource outright, which CASCADEd the channel's EPGData and ProgramData. The
stale reap kept the channel for its recording (#146), but a kept channel never
goes through the write loop, so nothing recreated its guide entry and it showed
no programme for the rest of the recording.

Driven through the real `_action_apply` against a real Django ORM (see
tests/orm_harness/run_scenarios.py), because the claim is about which rows
survive. Helpers and the subprocess driver come from test_apply_empty_slate.py,
loaded by path so the same code works under pytest and inside the harness.
Assertions use literal names ("Top Matchups", "!Top Matchups", the
"ranked_matchups:" prefix), not the plugin's constants.
"""

import importlib.util
import os
from datetime import timedelta

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
_now, _settings, _seed_source_channel, _row, _write_cache, _publish = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish,
)

OLD = "Top Matchups"
NEW = "!Top Matchups"
RECORDING = "ranked_matchups:CFB:401"
OTHER = "ranked_matchups:CFB:402"


# --------------------------------------------------------------- helpers ---

def _programmes(env, epg_id):
    return sorted(
        (p.title, p.start_time.isoformat(), p.end_time.isoformat())
        for p in env.m.ProgramData.objects.filter(epg_id=epg_id)
    )


def _recording_on_air(env):
    """Game 401 on air in OLD and recording; game 402 waiting. Returns
    (user channel, the recording channel, its guide programmes)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [
        _row(401, _now() - timedelta(minutes=30), user_ch.id),
        _row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma"),
    ])
    rec_ch = owned[RECORDING]
    assert rec_ch.epg_data.epg_source.name == OLD
    programmes = _programmes(env, rec_ch.epg_data_id)
    assert programmes, "setup: the recording channel has no guide to lose"
    m.Recording.objects.create(
        channel=rec_ch, start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    return user_ch, rec_ch, programmes


def _rename_with_401_off_the_slate(env, user_ch, **over):
    """401 leaves the cache (it is no longer listed), 402 stays, and the user
    renames the live group."""
    _write_cache(env, [
        _row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma"),
    ])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **over))
    assert result["status"] == "ok", result
    return result


def _assert_guide_kept(env, rec_ch, programmes):
    m = env.m
    rec_ch.refresh_from_db()
    assert rec_ch.channel_group.name == NEW
    assert rec_ch.epg_data_id is not None, "the recording channel lost its guide entry"
    assert rec_ch.epg_data.tvg_id == RECORDING
    assert rec_ch.epg_data.epg_source.name == NEW
    assert _programmes(env, rec_ch.epg_data_id) == programmes, "its programmes changed"
    assert m.Recording.objects.filter(channel=rec_ch).count() == 1


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_rename_keeps_the_guide_of_a_channel_kept_for_a_recording(env):
    """The #263 repro: the recording channel keeps its entry and programmes,
    and the old source, now empty, is gone."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)

    result = _rename_with_401_off_the_slate(env, user_ch)

    assert "1 channel(s) kept" in result["message"], result
    _assert_guide_kept(env, rec_ch, programmes)
    assert not m.EPGSource.objects.filter(name=OLD).exists()
    assert not m.ChannelGroup.objects.filter(name=OLD).exists()
    # The game still on the slate has its guide on the new source too.
    other = m.Channel.objects.get(tvg_id=OTHER)
    assert other.channel_group.name == NEW
    assert other.epg_data.epg_source.name == NEW
    assert m.ProgramData.objects.filter(epg=other.epg_data).exists()


def scenario_a_kept_channel_is_not_restored_onto_a_strangers_number(env):
    """Codex review, 1.34.0 batch: a channel kept for its recording is parked
    and then restored to its old number, which was checked only against the
    numbers this apply wrote. After a rename the target group can already hold
    someone else's channel on that RAW number (here one the user filed in the
    new group by hand). Nothing in the database refuses the duplicate, so both
    sat on one number, the guide bound arbitrarily and the UI refused to save
    either. The kept channel must stay parked instead."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    number = rec_ch.channel_number
    new_group = m.ChannelGroup.objects.create(name=NEW)
    stranger = m.Channel.objects.create(
        name="Hand-filed", channel_number=number, channel_group=new_group,
    )

    _rename_with_401_off_the_slate(env, user_ch)

    rec_ch.refresh_from_db()
    stranger.refresh_from_db()
    assert stranger.channel_number == number
    assert rec_ch.channel_group.name == NEW
    assert rec_ch.channel_number != number, "restored onto the stranger's number"
    raw = list(m.Channel.objects.filter(channel_group__name=NEW)
               .values_list("channel_number", flat=True))
    assert len(raw) == len(set(raw)), raw
    _assert_guide_kept(env, rec_ch, programmes)


def scenario_the_moved_entry_goes_with_its_channel_when_the_recording_ends(env):
    """No orphan guide once the recording ends: the next apply reaps the
    channel and its moved entry together, and nothing of 401 is left."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    _rename_with_401_off_the_slate(env, user_ch)
    _assert_guide_kept(env, rec_ch, programmes)  # the row survived the rename
    moved_id = rec_ch.epg_data_id
    m.Recording.objects.filter(channel=rec_ch).update(
        end_time=_now() - timedelta(minutes=1), custom_properties={"status": "completed"},
    )

    _rename_with_401_off_the_slate(env, user_ch)

    assert not m.Channel.objects.filter(pk=rec_ch.pk).exists()
    assert not m.EPGData.objects.filter(pk=moved_id).exists()
    assert not m.EPGData.objects.filter(tvg_id=RECORDING).exists()
    assert not m.ProgramData.objects.filter(tvg_id=RECORDING).exists()
    assert sorted(m.EPGData.objects.values_list("epg_source__name", "tvg_id")) == [(NEW, OTHER)]
    assert sorted(m.EPGSource.objects.values_list("name", flat=True)) == [NEW]
    # The recording itself was preserved on the archive channel (#146).
    rec = m.Recording.objects.get()
    assert rec.channel.tvg_id == "ranked_matchups:recordings_archive"


def scenario_rename_keeps_the_guide_when_the_old_source_is_shared(env):
    """The old source also holds a row we did not write. Our rows still move
    (they used to be deleted), and the source and the other row stay."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    old_src = m.EPGSource.objects.get(name=OLD)
    theirs = m.EPGData.objects.create(epg_source=old_src, tvg_id="someone.else.1", name="theirs")

    _rename_with_401_off_the_slate(env, user_ch)

    _assert_guide_kept(env, rec_ch, programmes)
    assert m.EPGSource.objects.filter(name=OLD).exists()
    assert list(m.EPGData.objects.filter(epg_source__name=OLD).values_list("id", flat=True)) == [theirs.id]


def scenario_rename_onto_a_source_with_a_stale_row_for_the_marker(env):
    """Renamed away and back: the new source still holds an unused row for the
    recording's marker. unique (tvg_id, epg_source) must not fail the apply,
    and the row the channel uses, with its programmes, is the one kept."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    new_src = m.EPGSource.objects.create(name=NEW, source_type="xmltv", is_active=False)
    stale = m.EPGData.objects.create(epg_source=new_src, tvg_id=RECORDING, name="stale")
    m.ProgramData.objects.create(
        epg=stale, start_time=_now(), end_time=_now() + timedelta(hours=1),
        title="stale programme", tvg_id=RECORDING,
    )

    name_before = rec_ch.epg_data.name
    # A user's override pins their own channel to the recording's guide entry.
    # Its FK is a hidden relation (related_name="+"), which is why it is here.
    override = m.ChannelOverride.objects.create(channel=user_ch, epg_data_id=rec_ch.epg_data_id)

    _rename_with_401_off_the_slate(env, user_ch)

    _assert_guide_kept(env, rec_ch, programmes)
    assert rec_ch.epg_data_id == stale.id, "the new source's row keeps its id"
    assert rec_ch.epg_data.name == name_before != "stale", "the entry kept the stale name"
    assert m.EPGData.objects.filter(tvg_id=RECORDING).count() == 1
    assert not m.ProgramData.objects.filter(title="stale programme").exists()
    override.refresh_from_db()
    assert override.epg_data_id == stale.id, "the user's override lost its guide entry"


def scenario_rename_onto_a_row_another_channel_uses(env):
    """The new source's row for the marker is in use (a user's own channel was
    pointed at it). That row stays, both channels end up on it carrying the
    recording's programmes, and the old row is dropped instead of tripping
    unique (tvg_id, epg_source)."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    new_src = m.EPGSource.objects.create(name=NEW, source_type="xmltv", is_active=False)
    in_use = m.EPGData.objects.create(epg_source=new_src, tvg_id=RECORDING, name="in use")
    m.Channel.objects.filter(pk=user_ch.pk).update(epg_data_id=in_use.id)
    old_row_id = rec_ch.epg_data_id

    _rename_with_401_off_the_slate(env, user_ch)

    rec_ch.refresh_from_db()
    user_ch.refresh_from_db()
    assert rec_ch.epg_data_id == in_use.id, "the recording channel lost its guide entry"
    assert user_ch.epg_data_id == in_use.id, "the user's own channel lost its guide entry"
    assert _programmes(env, in_use.id) == programmes, "the recording's programmes were lost"
    assert not m.EPGData.objects.filter(pk=old_row_id).exists()
    assert m.EPGData.objects.filter(tvg_id=RECORDING).count() == 1


def scenario_rename_of_a_game_still_on_the_slate(env):
    """A listed game's channel is rewritten by the write loop as before: one
    entry for it, on the new source, with its programmes."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma")])

    _rename_with_401_off_the_slate(env, user_ch)

    ch = m.Channel.objects.get(tvg_id=OTHER)
    assert ch.channel_group.name == NEW
    assert list(m.EPGData.objects.filter(tvg_id=OTHER).values_list("id", "epg_source__name")) == [
        (ch.epg_data_id, NEW),
    ]
    assert m.ProgramData.objects.filter(epg_id=ch.epg_data_id).exists()
    assert sorted(m.EPGSource.objects.values_list("name", flat=True)) == [NEW]


def _stale_second_source(env, created_first):
    """A second old source, named after an even older group, carrying the
    recording's marker with a stale programme. Created before or after the
    live old source, so both processing orders are exercised."""
    m = env.m
    user_ch, rec_ch, programmes = (None, None, None)
    if created_first:
        stale_src = m.EPGSource.objects.create(name="Top Games", source_type="xmltv", is_active=False)
        user_ch, rec_ch, programmes = _recording_on_air(env)
    else:
        user_ch, rec_ch, programmes = _recording_on_air(env)
        stale_src = m.EPGSource.objects.create(name="Top Games", source_type="xmltv", is_active=False)
    stale = m.EPGData.objects.create(epg_source=stale_src, tvg_id=RECORDING, name="stale")
    m.ProgramData.objects.create(
        epg=stale, start_time=_now(), end_time=_now() + timedelta(hours=1),
        title="stale programme", tvg_id=RECORDING,
    )
    assert stale_src.id < rec_ch.epg_data.epg_source_id if created_first else stale_src.id > rec_ch.epg_data.epg_source_id

    _rename_with_401_off_the_slate(env, user_ch)

    _assert_guide_kept(env, rec_ch, programmes)
    assert m.EPGData.objects.filter(tvg_id=RECORDING).count() == 1
    assert not m.ProgramData.objects.filter(title="stale programme").exists()
    assert sorted(m.EPGSource.objects.values_list("name", flat=True)) == [NEW]


def scenario_a_stale_second_source_processed_last_does_not_overwrite_the_guide(env):
    _stale_second_source(env, created_first=False)


def scenario_a_stale_second_source_processed_first_does_not_overwrite_the_guide(env):
    _stale_second_source(env, created_first=True)


# Three ways a source fetches its own guide. Ours does none of them.
FETCHING = {
    "url": dict(source_type="xmltv", url="http://192.168.86.10:9191/output/epg"),
    "file": dict(source_type="xmltv", file_path="/data/epgs/mirror.xml"),
    "schedules_direct": dict(source_type="schedules_direct"),
}


def _fetched_source_with_our_marker_is_not_ours(env, shape):
    """A source that fetches is fed from somewhere else (here, a user who added
    this server's own XMLTV output), even though its row carries our marker.
    The rename sweep must leave it, its row, its programmes and the user's
    channel on it alone. The old code deleted the whole source."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma")])
    manual = m.EPGSource.objects.create(name="Manual Guide", **FETCHING[shape])
    row = m.EPGData.objects.create(epg_source=manual, tvg_id=RECORDING, name="mirrored")
    m.ProgramData.objects.create(
        epg=row, start_time=_now(), end_time=_now() + timedelta(hours=1),
        title="mirrored programme", tvg_id=RECORDING,
    )
    m.Channel.objects.filter(pk=user_ch.pk).update(epg_data_id=row.id)

    _rename_with_401_off_the_slate(env, user_ch)

    assert list(m.EPGData.objects.filter(epg_source__name="Manual Guide").values_list("id", flat=True)) == [row.id]
    assert list(m.ProgramData.objects.filter(epg=row).values_list("title", flat=True)) == ["mirrored programme"]
    user_ch.refresh_from_db()
    assert user_ch.epg_data_id == row.id
    assert sorted(m.EPGSource.objects.values_list("name", flat=True)) == [NEW, "Manual Guide"]


def scenario_a_fetched_url_source_with_our_marker_is_not_ours(env):
    _fetched_source_with_our_marker_is_not_ours(env, "url")


def scenario_a_fetched_file_source_with_our_marker_is_not_ours(env):
    _fetched_source_with_our_marker_is_not_ours(env, "file")


def scenario_a_schedules_direct_source_with_our_marker_is_not_ours(env):
    _fetched_source_with_our_marker_is_not_ours(env, "schedules_direct")


def _renaming_onto_a_fetched_source_changes_nothing(env, shape, dry_run=False):
    """The new group name is also the name of the user's own fetched EPG
    source, mirroring our output, so it even holds a row for the recording's
    marker. Apply must refuse before writing, dry run or not: before, step 2
    adopted the source (switched it to an inactive xmltv source) and the rename
    wrote our rows into it."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    theirs = m.EPGSource.objects.create(name=NEW, is_active=True, **FETCHING[shape])
    source_type = theirs.source_type
    row = m.EPGData.objects.create(epg_source=theirs, tvg_id=RECORDING, name="mirrored")
    m.ProgramData.objects.create(
        epg=row, start_time=_now(), end_time=_now() + timedelta(hours=1),
        title="mirrored programme", tvg_id=RECORDING,
    )
    before = sorted(m.EPGData.objects.values_list("id", "epg_source__name", "tvg_id"))

    _write_cache(env, [
        _row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma"),
    ])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, dry_run=dry_run))

    assert result["status"] == "error", result
    assert "Nothing was changed" in result["message"], result
    theirs.refresh_from_db()
    assert theirs.is_active is True and theirs.source_type == source_type
    assert sorted(m.EPGData.objects.values_list("id", "epg_source__name", "tvg_id")) == before
    assert list(m.ProgramData.objects.filter(epg=row).values_list("title", flat=True)) == ["mirrored programme"]
    rec_ch.refresh_from_db()
    assert rec_ch.channel_group.name == OLD
    assert _programmes(env, rec_ch.epg_data_id) == programmes
    assert not m.ChannelGroup.objects.filter(name=NEW).exists()


def scenario_renaming_onto_a_url_source_changes_nothing(env):
    _renaming_onto_a_fetched_source_changes_nothing(env, "url")


def scenario_renaming_onto_a_file_source_changes_nothing(env):
    _renaming_onto_a_fetched_source_changes_nothing(env, "file")


def scenario_renaming_onto_a_schedules_direct_source_changes_nothing(env):
    _renaming_onto_a_fetched_source_changes_nothing(env, "schedules_direct")


def scenario_a_dry_run_onto_a_fetched_source_reports_the_clash(env):
    _renaming_onto_a_fetched_source_changes_nothing(env, "url", dry_run=True)


def scenario_move_reports_every_row_it_put_on_the_new_source(env):
    """The helper's count, called directly: two of our rows (one clashing with
    a row already on the destination) move, the other owner's row does not."""
    m = env.m
    old = m.EPGSource.objects.create(name=OLD, source_type="xmltv", is_active=False)
    new = m.EPGSource.objects.create(name=NEW, source_type="xmltv", is_active=False)
    m.EPGData.objects.create(epg_source=old, tvg_id=RECORDING, name="a")
    m.EPGData.objects.create(epg_source=old, tvg_id=OTHER, name="b")
    m.EPGData.objects.create(epg_source=old, tvg_id="someone.else.1", name="theirs")
    m.EPGData.objects.create(epg_source=new, tvg_id=OTHER, name="b, earlier")

    assert env.plugin._move_owned_epg_rows(old, new) == 2

    assert sorted(m.EPGData.objects.filter(epg_source=new).values_list("tvg_id", flat=True)) == [RECORDING, OTHER]
    assert list(m.EPGData.objects.filter(epg_source=old).values_list("tvg_id", flat=True)) == ["someone.else.1"]


def scenario_rename_with_an_unparseable_placeholder_threshold(env):
    """The rename cleanup commits before the main apply transaction, so a
    setting apply cannot parse used to fail the action with the database half
    changed (ValueError from float("")). It falls back to the default 5.0
    instead, and the rename completes."""
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)

    for bad in ("", None, "five", 10 ** 1000):
        result = _rename_with_401_off_the_slate(env, user_ch, placeholder_min_score=bad)
        assert "1 channel(s) kept" in result["message"], (bad, result)

    _assert_guide_kept(env, rec_ch, programmes)
    assert m.Channel.objects.get(tvg_id=OTHER).channel_group.name == NEW


def scenario_dry_run_rename_moves_nothing(env):
    m = env.m
    user_ch, rec_ch, programmes = _recording_on_air(env)
    # Both already exist (renamed away and back), else a dry run returns before
    # the rename cleanup, or has no source to move rows onto.
    m.ChannelGroup.objects.create(name=NEW)
    m.EPGSource.objects.create(name=NEW, source_type="xmltv", is_active=False)
    before = sorted(m.EPGData.objects.values_list("id", "epg_source__name", "tvg_id"))

    _rename_with_401_off_the_slate(env, user_ch, dry_run=True)

    assert sorted(m.EPGData.objects.values_list("id", "epg_source__name", "tvg_id")) == before
    assert sorted(m.EPGSource.objects.values_list("name", flat=True)) == [NEW, OLD]
    rec_ch.refresh_from_db()
    assert rec_ch.channel_group.name == OLD
    assert _programmes(env, rec_ch.epg_data_id) == programmes


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_group_rename(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
