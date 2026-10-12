"""Nothing commits before apply's last time check (#271 follow-up, rename residual).

`_apply_time_short` can stop apply at each of its checks, and a stopped apply
must leave nothing behind. Before this change the live-group rename (step 1:
create the new group, move our channels into it, delete the old one; step 2:
move our guide rows off the old EPG source, #263) committed BEFORE the second
check. A stopped apply then left our channel in the new group on its old
number, beside a channel the user had filed there on the same number: two
channels on 401 in one group, which the guide binds by the bare number and the
UI refuses to save.

Driven through the real `_action_apply` against a real Django ORM (see
tests/orm_harness/run_scenarios.py), with the fake child clock from
test_apply_time_reserve.py, so nothing sleeps.
"""

import ast
import hashlib
import importlib.util
import os
from datetime import timedelta

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_kit_{name}", os.path.join(HERE, name))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_kit = _load("test_apply_empty_slate.py")
_reserve = _load("test_apply_time_reserve.py")
_now, _settings, _seed_source_channel, _row, _write_cache, _publish, _owned = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish, _kit._owned,
)
_Child = _reserve._Child
PLUGIN_PY = os.path.join(os.path.dirname(HERE), "plugin.py")

OLD, NEW = "Top Matchups", "!Top Matchups"
G401 = "ranked_matchups:CFB:401"
COMPACT = {"channel_numbering_mode": "compact", "virtual_channel_base": 401,
           "compact_band_size": 2}


def _published_on_401(env):
    """Our 401 game on air in OLD at 401, and the user's own channel filed by
    hand at 401 in NEW, the group the next apply renames to."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    soon = _now() + timedelta(hours=6)
    game = _row(401, soon, user_ch.id)
    owned = _publish(env, [game], settings=_settings(**COMPACT))
    assert int(owned[G401].channel_number) == 401
    new_group = m.ChannelGroup.objects.create(name=NEW)
    stranger = m.Channel.objects.create(name="My channel", channel_number=401, channel_group=new_group)
    return game, owned[G401], stranger


def _stop_at_the_last_check(env, child):
    """The deciding half (before the last check) spends 100 of the 200s left."""
    prepare = env.plugin._autorecord_prepare

    def slow_prepare(*a, **k):
        child.clock.now += 100
        return prepare(*a, **k)

    child.patch(env.plugin, "_autorecord_prepare", slow_prepare)


def _numbers_in(env, group_name):
    return sorted(
        int(c.channel_number) for c in env.m.Channel.objects.filter(channel_group__name=group_name)
    )


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_witness_a_stopped_rename_leaves_no_two_channels_on_401(env):
    m = env.m
    game, ours, stranger = _published_on_401(env)
    _write_cache(env, [game])
    with _Child(env, seconds_left=200) as child:
        _stop_at_the_last_check(env, child)
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "error" and "not enough time" in result["message"], result
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the stopped apply moved our channel into {ours.channel_group.name!r}; "
        f"{NEW!r} now holds {_numbers_in(env, NEW)}"
    )
    assert _numbers_in(env, NEW) == [401]
    assert m.ChannelGroup.objects.filter(name=OLD).exists()

    # The next apply, with time, renames and renumbers around the user's 401.
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    stranger.refresh_from_db()
    assert (ours.channel_group.name, int(ours.channel_number)) == (NEW, 402)
    assert int(stranger.channel_number) == 401
    assert _numbers_in(env, NEW) == [401, 402]


def scenario_witness_a_refused_rename_leaves_no_two_channels_on_401(env):
    """Codex review: the full-band refusal (compact, every slot taken) returns
    after numbering, and the group setup used to have committed by then. A
    band of ONE slot, 401, held in the new group by the user's own channel:
    the rename is refused, and must leave our channel where it was."""
    m = env.m
    game, ours, stranger = _published_on_401(env)
    _write_cache(env, [game])
    one_slot = dict(COMPACT, compact_band_size=1)
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **one_slot))
    assert result["status"] == "error" and "no free channel numbers" in result["message"], result
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the refused apply moved our channel into {ours.channel_group.name!r}; "
        f"{NEW!r} now holds {_numbers_in(env, NEW)}"
    )
    assert _numbers_in(env, NEW) == [401]
    assert m.ChannelGroup.objects.filter(name=OLD).exists()


def scenario_witness_a_case_only_archive_rename_keeps_the_archive(env):
    """Codex review: 'Recordings group name' changed only in case. The archive
    group matched the protected name exactly, so it looked like a previous
    LIVE group: its archive channel was moved into the live group and, on
    the base branch, reaped there (deleted)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    archive_ch = m.Channel.objects.create(
        name="Matchups Recordings", channel_number=9999, channel_group=archive,
        tvg_id=env.plugin.ARCHIVE_TVG_ID,
    )
    # A finished recording lives there: the archive exists only while it
    # holds one, and an empty one is (rightly) cleaned up.
    m.Recording.objects.create(
        channel=archive_ch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(recordings_group_name="matchups recordings", **COMPACT))
    row = m.Channel.objects.filter(pk=archive_ch.pk).first()
    assert row is not None and row.channel_group_id == archive.pk, (
        "the archive channel was " + ("deleted" if row is None
                                      else f"moved into {row.channel_group.name!r}")
    )
    assert m.ChannelGroup.objects.filter(pk=archive.pk).exists()


def scenario_witness_a_source_made_to_fetch_during_the_prepass_is_not_adopted(env):
    """Codex review, round 12: the "group named after a fetching EPG source"
    refusal runs before the pre-pass, and the source is now created or
    upgraded only in the main transaction, minutes later. A user who points
    that source at a guide meanwhile had it switched inactive and filled
    with our rows. Re-checked under a row lock, apply refuses instead."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    _write_cache(env, [game])
    prepare = env.plugin._autorecord_prepare

    def user_edits_the_source(*a, **k):
        m.EPGSource.objects.filter(name=OLD).update(url="http://guide.example/xmltv.xml",
                                                    is_active=True)
        return prepare(*a, **k)

    env.plugin._autorecord_prepare = user_edits_the_source
    try:
        result = env.plugin._action_apply(_settings(**COMPACT))
    finally:
        env.plugin._autorecord_prepare = prepare
    assert result["status"] == "error" and "fetches its own guide" in result["message"], result
    assert "No channel or guide was changed." in result["message"], result
    src = m.EPGSource.objects.get(name=OLD)
    assert src.is_active is True, "the user's fetching source was switched inactive"


def scenario_witness_an_old_source_made_to_fetch_during_the_prepass_is_kept(env):
    """Codex review, round 13: the previous live group's EPG source is chosen
    for cleanup before the pre-pass and emptied and deleted in the main
    transaction. A user who pointed it at a guide meanwhile lost it. The
    setup re-reads the sources it acts on, row-locked, and a fetching one is
    not ours (_never_fetches_q)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    _write_cache(env, [game])
    old_src = m.EPGSource.objects.get(name=OLD)
    prepare = env.plugin._autorecord_prepare

    def user_edits_the_old_source(*a, **k):
        m.EPGSource.objects.filter(pk=old_src.pk).update(url="http://guide.example/xmltv.xml")
        return prepare(*a, **k)

    env.plugin._autorecord_prepare = user_edits_the_old_source
    try:
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    finally:
        env.plugin._autorecord_prepare = prepare
    assert result["status"] == "ok", result
    assert m.EPGSource.objects.filter(pk=old_src.pk).exists(), "the user's fetching source was deleted"


def _during_the_prepass(env, edit):
    """Run `edit` while apply's pre-pass runs (after its early reads)."""
    prepare = env.plugin._autorecord_prepare

    def edited(*a, **k):
        edit()
        return prepare(*a, **k)

    env.plugin._autorecord_prepare = edited
    return lambda: setattr(env.plugin, "_autorecord_prepare", prepare)


def scenario_witness_a_live_group_created_during_the_prepass_is_adopted(env):
    """Codex review, round 19: the live group, missing when apply looked
    before its pre-pass, was created in the main transaction; a user who
    created it meanwhile made that create hit the unique name, failing the
    transaction after the recording cancellations had committed. The setup
    re-reads it, row-locked, and adopts it."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    made = []
    restore = _during_the_prepass(env, lambda: made.append(m.ChannelGroup.objects.create(name=NEW)))
    try:
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    finally:
        restore()
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert made and ours.channel_group_id == made[0].pk
    assert m.ChannelGroup.objects.filter(name=NEW).count() == 1


def scenario_witness_an_old_source_renamed_to_the_live_name_keeps_its_guide(env):
    """Codex review, round 14: the previous live group's source, renamed by
    the user to the NEW live name during the pre-pass, was both the live
    source and an old one: its guide rows were "moved" onto themselves, each
    a clash with itself, and deleted. A channel kept for an active recording
    (the write loop does not visit it) lost its guide. (Here, on sqlite,
    whose foreign-key checks wait for the commit, the commit failed instead.)"""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() - timedelta(minutes=30), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    m.Recording.objects.create(channel=ours, start_time=_now() - timedelta(minutes=30),
                               end_time=_now() + timedelta(hours=2), custom_properties={})
    assert m.ProgramData.objects.filter(epg__tvg_id=G401).exists()
    _write_cache(env, [_row(402, _now() + timedelta(hours=6), user_ch.id)])
    restore = _during_the_prepass(env, lambda: m.EPGSource.objects.filter(name=OLD).update(name=NEW))
    try:
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    finally:
        restore()
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert ours.epg_data_id is not None and m.ProgramData.objects.filter(epg_id=ours.epg_data_id).exists(), (
        "the recording channel's guide was deleted"
    )


def scenario_witness_an_old_group_renamed_during_the_prepass_is_judged_by_its_new_name(env):
    """Codex review, round 14: the old live group, renamed by the user to
    'Kept' during the pre-pass, was deleted once our channel moved out: the
    lock re-read the row and threw it away, and the name test read the stale
    'Top Matchups', a name this plugin creates."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    old = ours.channel_group
    _write_cache(env, [game])
    restore = _during_the_prepass(env, lambda: m.ChannelGroup.objects.filter(pk=old.pk).update(name="Kept"))
    try:
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    finally:
        restore()
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert ours.channel_group.name == NEW
    assert m.ChannelGroup.objects.filter(pk=old.pk, name="Kept").exists(), "the user's 'Kept' was deleted"


def scenario_witness_a_source_named_as_the_recordings_group_is_left_alone(env):
    """Codex review, round 16: narrowing the by-name protection to the live
    name (right for GROUPS, whose archive is protected per channel) also
    unprotected a SOURCE named as the recordings group, which base's sweep
    excludes (#146). Swept, its rows were merged onto the live source,
    replacing the programmes of a row with the same marker there."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    _write_cache(env, [game])
    rec_src = m.EPGSource.objects.create(name="Matchups Recordings", source_type="xmltv",
                                         is_active=False)
    row = m.EPGData.objects.create(tvg_id=G401, name="kept", epg_source=rec_src)
    result = env.plugin._action_apply(_settings(**COMPACT))
    assert result["status"] == "ok", result
    assert m.EPGSource.objects.filter(pk=rec_src.pk).exists(), "the recordings-named source was deleted"
    assert m.EPGData.objects.filter(pk=row.pk, epg_source=rec_src).exists(), "its row was moved"


def scenario_a_rename_keeps_our_numbers(env):
    """Guard: numbering now runs BEFORE the group move commits, so it must
    count our channels in the group being renamed away from as ours. Counted
    as somebody else's, our own 401 reserves 401 against us and the rename
    renumbers the channel to 402."""
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert (ours.channel_group.name, int(ours.channel_number)) == (NEW, 401)


def scenario_a_case_only_rename_of_the_live_group_moves_our_channel(env):
    """Guard: the LIVE name is matched exactly, unlike the archive's. A
    case-only rename must still move our channel out of the old-case group
    (same Channel.id); matched ignoring case, the old group was protected,
    our channel stranded in it, and a second channel was created."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _write_cache(env, [game])
    # Not _publish: it counts channels in the literal "Top Matchups" group.
    first = env.plugin._action_apply(_settings(channel_profile_name="top matchups", **COMPACT))
    assert first["status"] == "ok", first
    ours = m.Channel.objects.get(tvg_id=G401)
    assert ours.channel_group.name == "top matchups"
    result = env.plugin._action_apply(_settings(channel_profile_name="Top Matchups", **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [c.pk for c in mine] == [ours.pk], [(c.pk, c.channel_group.name) for c in mine]
    assert mine[0].channel_group.name == "Top Matchups"


def scenario_witness_a_failed_rename_rolls_back(env):
    """Codex review, round 2: the setup committed on its own, so an error
    later in it (an EPGSource name the database rejects, here the #263 row
    move failing) left our channels moved and their guide on the old source.
    Inside the main transaction it rolls back with everything else."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    real = env.plugin._move_owned_epg_rows

    def rejected(*a, **k):
        raise RuntimeError("the database rejected the write")

    env.plugin._move_owned_epg_rows = rejected
    try:
        try:
            env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
        except RuntimeError:
            pass
        else:
            raise AssertionError("instrument: the injected failure never happened")
    finally:
        env.plugin._move_owned_epg_rows = real
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the failed apply left our channel moved into {ours.channel_group.name!r}"
    )
    assert not m.ChannelGroup.objects.filter(name=NEW).exists()
    assert m.EPGSource.objects.filter(name=OLD).exists()


def scenario_witness_an_archive_name_like_the_old_live_name_strands_nothing(env):
    """Codex review, round 2: the live group goes Top Matchups -> !Top Matchups
    while Recordings group name is set to 'top matchups'. Protecting archive
    NAMES ignoring case protected the old live group: our channel stayed in it
    and the game got a second channel. The archive is protected by the archive
    channel it holds, and the names exactly."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(
        channel_profile_name=NEW, recordings_group_name="top matchups", **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [(c.pk, c.channel_group.name, int(c.channel_number)) for c in mine] == [(ours.pk, NEW, 401)], (
        [(c.pk, c.channel_group.name, c.channel_number) for c in mine]
    )


def scenario_witness_a_case_only_archive_rename_reuses_the_archive(env):
    """Codex review, round 2: archive creation and cleanup looked the group up
    by exact name, so 'matchups recordings' made a second archive group and
    channel beside 'Matchups Recordings', splitting the recordings."""
    m = env.m
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    arch = m.Channel.objects.create(
        name="Matchups Recordings", channel_number=9999, channel_group=archive,
        tvg_id=env.plugin.ARCHIVE_TVG_ID,
    )
    got = env.plugin._ensure_archive_channel("matchups recordings")
    assert got.pk == arch.pk, "a second archive channel was created"
    assert list(m.ChannelGroup.objects.filter(name__iexact="matchups recordings")) == [archive]
    assert env.plugin._cleanup_empty_archive("matchups recordings") is True  # it holds no recording
    assert not m.Channel.objects.filter(pk=arch.pk).exists()


def scenario_witness_a_stray_archive_channel_does_not_strand_the_old_live_group(env):
    """Codex review, round 3: an old live group holding our game channel AND a
    stray archive channel (left there by the earlier case bug). Excluding
    whole GROUPS that hold the archive kept the game channel there and the
    rename made it a second channel. Per channel: the game channel moves to
    the live group, and the archive channel, holding a recording, never
    does; cleanup folds it into the recordings group (round 9)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    old = ours.channel_group
    stray = m.Channel.objects.create(
        name="Matchups Recordings", channel_number=9999, channel_group=old,
        tvg_id=env.plugin.ARCHIVE_TVG_ID,
    )
    m.Recording.objects.create(
        channel=stray, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [(c.pk, c.channel_group.name) for c in mine] == [(ours.pk, NEW)], (
        [(c.pk, c.channel_group.name) for c in mine]
    )
    stray.refresh_from_db()
    assert stray.channel_group.name == "Matchups Recordings", stray.channel_group.name
    assert m.Recording.objects.filter(channel=stray).count() == 1
    # Codex review, round 10: the rename left the old group for the archive,
    # and the archive's move then left it for good (nothing of ours in it
    # any more, so no later apply found it). A name we create, it goes.
    assert not m.ChannelGroup.objects.filter(pk=old.pk).exists(), "the old live group was stranded"


def scenario_witness_split_archives_use_the_one_the_setting_names(env):
    """Codex review, round 3: an install the old exact-name lookup already
    split has an archive in 'Matchups Recordings' (older) and in 'matchups
    recordings'. The setting names the second: new recordings go there, and
    cleanup empties BOTH variants, not only the first one found."""
    m = env.m
    older = m.ChannelGroup.objects.create(name="Matchups Recordings")
    named = m.ChannelGroup.objects.create(name="matchups recordings")
    # Each archive is named as the group it was made for, as
    # _ensure_archive_channel names it.
    older_arch = m.Channel.objects.create(name=older.name, channel_number=9998, channel_group=older,
                                          tvg_id=env.plugin.ARCHIVE_TVG_ID)
    named_arch = m.Channel.objects.create(name=named.name, channel_number=9999, channel_group=named,
                                          tvg_id=env.plugin.ARCHIVE_TVG_ID)
    assert env.plugin._ensure_archive_channel("matchups recordings").pk == named_arch.pk
    assert env.plugin._cleanup_empty_archive("matchups recordings") is True
    assert not m.Channel.objects.filter(pk__in=[older_arch.pk, named_arch.pk]).exists()
    assert not m.ChannelGroup.objects.filter(pk__in=[older.pk, named.pk]).exists()


def scenario_witness_a_renamed_away_group_with_m3u_settings_is_kept(env):
    """Codex review, round 7: "empty" counted channels and streams only. An M3U
    account's settings for the group (ChannelGroupM3UAccount, CASCADE) went
    with it, and a channel override naming it (SET_NULL) lost its group."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    old = ours.channel_group
    acct = m.M3UAccount.objects.create(name="provider")
    link = m.ChannelGroupM3UAccount.objects.create(channel_group=old, m3u_account=acct,
                                                    auto_channel_sync=True)
    override = m.ChannelOverride.objects.create(channel=user_ch, channel_group=old)
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert ours.channel_group.name == NEW
    assert m.ChannelGroupM3UAccount.objects.filter(pk=link.pk, channel_group=old).exists(), (
        "the M3U account's settings for the group were deleted with it"
    )
    override.refresh_from_db()
    assert override.channel_group_id == old.pk, "the override lost its group"


def scenario_every_reference_to_a_group_is_seen(env):
    """Instrument for _group_is_referenced: it must see the HIDDEN relation
    (ChannelOverride.channel_group is related_name="+") as well as the rest."""
    m = env.m
    from apps.channels.models import ChannelGroup
    names = {rel.field.model.__name__ for rel in env.plugin._reverse_foreign_keys(ChannelGroup)}
    assert {"Channel", "Stream", "ChannelOverride", "ChannelGroupM3UAccount"} <= names, names
    grp = m.ChannelGroup.objects.create(name="probe")
    assert env.plugin._group_is_referenced(grp) is False
    user_ch, _ = _seed_source_channel(env)
    m.ChannelOverride.objects.create(channel=user_ch, channel_group=grp)
    assert env.plugin._group_is_referenced(grp) is True


def scenario_witness_archive_cleanup_keeps_a_group_holding_streams(env):
    """Codex review, round 5: an empty archive group was deleted even while
    streams belonged to it, nulling their group."""
    m = env.m
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                             channel_group=archive, tvg_id=env.plugin.ARCHIVE_TVG_ID)
    stream = m.Stream.objects.create(name="Grouped stream", channel_group=archive)
    env.plugin._cleanup_empty_archive("Matchups Recordings")
    stream.refresh_from_db()
    assert stream.channel_group_id == archive.pk, "the stream lost its group"


def _archive_with_recording(env, group_name, recorded=True):
    m = env.m
    grp = m.ChannelGroup.objects.create(name=group_name)
    arch = m.Channel.objects.create(name=group_name, channel_number=9999, channel_group=grp,
                                    tvg_id=env.plugin.ARCHIVE_TVG_ID)
    rec = m.Recording.objects.create(
        channel=arch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    ) if recorded else None
    return grp, arch, rec


def scenario_witness_a_renamed_recordings_group_moves_its_one_archive(env):
    """Codex review, round 8: 'Recordings group name' Matchups Recordings ->
    Saved Games. The next rehome made a SECOND archive channel (same tvg_id)
    in Saved Games and left the old one, with its recordings, behind. The one
    archive moves, recordings and all, and its emptied old group goes."""
    m = env.m
    old, arch, rec = _archive_with_recording(env, "Matchups Recordings")
    got = env.plugin._ensure_archive_channel("Saved Games")
    assert got.pk == arch.pk, "a second archive channel was created"
    assert m.Channel.objects.filter(tvg_id=env.plugin.ARCHIVE_TVG_ID).count() == 1
    got.refresh_from_db()
    assert got.channel_group.name == "Saved Games"
    assert m.Recording.objects.get(pk=rec.pk).channel_id == arch.pk
    assert not m.ChannelGroup.objects.filter(pk=old.pk).exists()


def scenario_witness_a_renamed_away_archive_is_cleaned_up(env):
    """Codex review, round 8: after the rename, the old archive's last
    recording went, and cleanup (looking up the NEW name) never saw it: the
    archive channel and its group stayed for good."""
    m = env.m
    old, arch, _rec = _archive_with_recording(env, "Matchups Recordings", recorded=False)
    assert env.plugin._cleanup_empty_archive("Saved Games") is True
    assert not m.Channel.objects.filter(pk=arch.pk).exists()
    assert not m.ChannelGroup.objects.filter(pk=old.pk).exists()


def scenario_witness_an_archive_in_the_live_group_keeps_its_recordings(env):
    """Codex review, round 9: an archive channel sitting in the live group
    (left there by the earlier case-only rename bug) counted among our game
    channels, so apply found it stale, "re-homed" its recordings onto itself
    and reaped it: the CASCADE deleted them."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    arch = m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                                    channel_group=ours.channel_group,
                                    tvg_id=env.plugin.ARCHIVE_TVG_ID)
    rec = m.Recording.objects.create(
        channel=arch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(**COMPACT))
    assert result["status"] == "ok", result
    assert m.Recording.objects.filter(pk=rec.pk).exists(), "the recording was deleted"
    rec = m.Recording.objects.get(pk=rec.pk)
    assert rec.channel.channel_group.name == "Matchups Recordings", rec.channel.channel_group.name
    assert m.Channel.objects.filter(tvg_id=env.plugin.ARCHIVE_TVG_ID).count() == 1


def scenario_witness_an_archive_in_the_live_group_keeps_its_number(env):
    """Codex review, round 10: numbering counted every channel of ours in the
    live group as a game's, the archive included, so its 9999 was not
    reserved: a game moved onto 9999, and the archive kept 9999 when it was
    folded into the recordings group. Two channels published on 9999."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    arch = m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                                    channel_group=ours.channel_group,
                                    tvg_id=env.plugin.ARCHIVE_TVG_ID)
    m.Recording.objects.create(
        channel=arch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(**dict(COMPACT, virtual_channel_base=9999)))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    arch.refresh_from_db()
    assert int(ours.channel_number) != int(arch.channel_number), (
        f"our game and the archive both publish on {int(arch.channel_number)}"
    )


def _the_live_group_survives_the_archive_leaving(env, archive_name):
    """Reaping the last game empties the live group while an archive named
    `archive_name` sits in it; folding the archive out must not delete the
    group apply is publishing to."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    ours = _publish(env, [_row(401, _now() - timedelta(hours=10), user_ch.id)])[G401]
    live = ours.channel_group
    _archive = m.Channel.objects.create(name=archive_name, channel_number=9999,
                                        channel_group=live, tvg_id=env.plugin.ARCHIVE_TVG_ID)
    m.Recording.objects.create(
        channel=_archive, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    result = env.plugin._action_reap(_settings())
    assert result["status"] == "ok" and result["reaped"] == 1, result
    _archive.refresh_from_db()
    assert _archive.channel_group.name == "Matchups Recordings"
    assert m.ChannelGroup.objects.filter(pk=live.pk).exists(), "the live group was deleted"


def scenario_the_live_group_is_never_deleted_as_the_archives(env):
    """Guard: a name this plugin creates makes an archive's group ours
    (_archive_group_is_ours), but never the live group's own name."""
    _the_live_group_survives_the_archive_leaving(env, "Matchups Recordings")


def scenario_witness_a_live_group_named_as_the_archive_is_kept(env):
    """Codex review, round 11: an archive made when 'Recordings group name'
    was Top Matchups carries that name; with the live group now Top Matchups,
    _made_for_archive called the live group the archive's and deleted it."""
    _the_live_group_survives_the_archive_leaving(env, OLD)


def scenario_an_overridden_archive_in_the_live_group_keeps_its_raw_number(env):
    """Guard: the archive is not ours for numbering in either half of
    _reserved_channel_numbers. Published under an override (8888), its RAW
    9999 must still be reserved in its group (Channel.clean() compares raw
    numbers). The harness has no with_effective_values, so a stand-in
    publishes the archive at 8888 and everything else at its raw number."""
    import sys
    import types
    from django.db.models import Case, F, Value, When
    m = env.m
    managers = types.ModuleType("apps.channels.managers")
    managers.with_effective_values = lambda qs: qs.annotate(effective_channel_number=Case(
        When(tvg_id=env.plugin.ARCHIVE_TVG_ID, then=Value(8888.0)), default=F("channel_number")))
    sys.modules["apps.channels.managers"] = managers
    try:
        user_ch, _ = _seed_source_channel(env)
        game = _row(401, _now() + timedelta(hours=6), user_ch.id)
        ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
        arch = m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                                        channel_group=ours.channel_group,
                                        tvg_id=env.plugin.ARCHIVE_TVG_ID)
        _write_cache(env, [game])
        # Recordings named as the live group: archiving is off, the archive stays put.
        result = env.plugin._action_apply(_settings(
            recordings_group_name=OLD, **dict(COMPACT, virtual_channel_base=9999)))
        assert result["status"] == "ok", result
    finally:
        del sys.modules["apps.channels.managers"]
    ours.refresh_from_db()
    arch.refresh_from_db()
    assert ours.channel_group_id == arch.channel_group_id
    assert int(ours.channel_number) != int(arch.channel_number), (
        f"our game took the archive's raw {int(arch.channel_number)} in its group"
    )


def scenario_witness_a_users_group_the_archive_was_moved_into_is_kept(env):
    """Codex review, round 9: a group counted as ours merely for holding the
    archive channel. The user filed it in their own 'Kept' group: moving it
    out (a renamed setting) or cleaning it up deleted 'Kept'."""
    m = env.m
    theirs, arch, rec = _archive_with_recording(env, "Matchups Recordings")
    theirs.name = "Kept"
    theirs.save()
    env.plugin._ensure_archive_channel("Saved Games")
    arch.refresh_from_db()
    assert arch.channel_group.name == "Saved Games"
    assert m.ChannelGroup.objects.filter(pk=theirs.pk).exists(), "moving the archive out deleted 'Kept'"

    mine = m.ChannelGroup.objects.create(name="Kept too")
    m.Channel.objects.filter(pk=arch.pk).update(channel_group=mine)
    rec.delete()
    assert env.plugin._cleanup_empty_archive("Saved Games") is True
    assert not m.Channel.objects.filter(pk=arch.pk).exists()
    assert m.ChannelGroup.objects.filter(pk=mine.pk).exists(), "cleanup deleted 'Kept too'"


def scenario_witness_the_archive_goes_to_the_group_named_exactly_as_the_setting(env):
    """Codex review, round 17: with an (empty) group named exactly as the
    setting AND a case variant holding the old archive, the lookup chose the
    variant, so recordings kept going there, not where the setting says, and
    cleanup could delete the exactly named group. The exact group wins; the
    one archive moves into it."""
    m = env.m
    variant, arch, rec = _archive_with_recording(env, "matchups recordings")
    exact = m.ChannelGroup.objects.create(name="Matchups Recordings")
    got = env.plugin._ensure_archive_channel("Matchups Recordings")
    assert got.pk == arch.pk
    arch.refresh_from_db()
    assert arch.channel_group_id == exact.pk, arch.channel_group.name
    assert m.Recording.objects.get(pk=rec.pk).channel_id == arch.pk
    assert not m.ChannelGroup.objects.filter(pk=variant.pk).exists()


def scenario_an_archive_renamed_twice_leaves_no_group_behind(env):
    """Guard: a group is ours when the archive channel carries its name
    (_made_for_archive), so a move must rename the channel too. Renamed
    twice, the middle group would otherwise look like a user's and stay."""
    m = env.m
    first, arch, _rec = _archive_with_recording(env, "Matchups Recordings")
    env.plugin._ensure_archive_channel("Saved Games")
    middle = m.ChannelGroup.objects.get(name="Saved Games")
    env.plugin._ensure_archive_channel("Kept Games")
    arch.refresh_from_db()
    assert (arch.name, arch.channel_group.name) == ("Kept Games", "Kept Games")
    assert not m.ChannelGroup.objects.filter(pk__in=[first.pk, middle.pk]).exists()


def scenario_witness_split_archives_holding_recordings_converge(env):
    """Codex review, round 9: an install the old exact-name lookup split has an
    archive in 'Matchups Recordings' and in 'matchups recordings', each with a
    recording. Cleanup removed only EMPTY archives, so both stayed for good
    and new recordings went to one. They fold into the one the setting names."""
    m = env.m
    older, older_arch, older_rec = _archive_with_recording(env, "Matchups Recordings")
    named, named_arch, named_rec = _archive_with_recording(env, "matchups recordings")
    env.plugin._cleanup_empty_archive("matchups recordings")
    assert list(m.Channel.objects.filter(tvg_id=env.plugin.ARCHIVE_TVG_ID)
                .values_list("pk", flat=True)) == [named_arch.pk]
    assert {r.channel_id for r in m.Recording.objects.filter(pk__in=[older_rec.pk, named_rec.pk])} \
        == {named_arch.pk}
    assert not m.ChannelGroup.objects.filter(pk=older.pk).exists()
    assert m.ChannelGroup.objects.filter(pk=named.pk).exists()


def scenario_cleanup_leaves_a_users_empty_case_variant_group(env):
    """Guard: cleanup now visits case variants, but a group that never held
    our archive and is not named exactly as the setting is the user's."""
    m = env.m
    theirs = m.ChannelGroup.objects.create(name="MATCHUPS RECORDINGS")
    env.plugin._cleanup_empty_archive("Matchups Recordings")
    assert m.ChannelGroup.objects.filter(pk=theirs.pk).exists()


def scenario_witness_a_dry_run_previews_a_rename_as_updates(env):
    """Codex review, round 3: the dry run left the channels the rename moves
    out of what it counted, so it previewed them as CREATED where the real
    apply updates them in place (same Channel.id)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    m.ChannelGroup.objects.create(name=NEW)  # else a dry run returns before counting
    _write_cache(env, [game])
    dry = env.plugin._action_apply(_settings(channel_profile_name=NEW, dry_run=True, **COMPACT))
    real = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert "created=0, updated=1" in real["message"], real
    assert "created=0, updated=1" in dry["message"], dry


def scenario_witness_a_recordings_name_like_the_old_live_name_strands_nothing(env):
    """Codex review, round 4: Top Matchups -> !Top Matchups with 'Recordings
    group name' set to Top Matchups. Protected BY NAME as the archive, the old
    live group kept our game channel and the rename made a copy. The archive
    is protected per channel now, so only the live name is."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    stream = m.Stream.objects.create(name="Grouped stream", channel_group=ours.channel_group)
    result = env.plugin._action_apply(_settings(
        channel_profile_name=NEW, recordings_group_name=OLD, **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [(c.pk, c.channel_group.name) for c in mine] == [(ours.pk, NEW)], (
        [(c.pk, c.channel_group.name) for c in mine]
    )
    # Codex review, round 5: emptied, the old group (now the recordings
    # group's name) was deleted by the move or by archive cleanup, and the
    # streams in it lost their group. Neither deletes a group with streams.
    stream.refresh_from_db()
    assert stream.channel_group is not None and stream.channel_group.name == OLD, (
        "the stream lost its group"
    )


def scenario_witness_a_dry_run_into_a_new_group_says_it_moves(env):
    """Codex review, round 4: with the new group not created yet, the dry run
    returns early and said it would CLONE the games, where the real apply
    moves the existing channels in place."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    # An archive beside it holds nothing that moves: not a previous group.
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                             channel_group=archive, tvg_id=env.plugin.ARCHIVE_TVG_ID)
    _write_cache(env, [game])
    dry = env.plugin._action_apply(_settings(channel_profile_name=NEW, dry_run=True, **COMPACT))
    assert "move 1 existing channel(s)" in dry["message"], dry
    assert "from 1 previous group(s)" in dry["message"], dry
    assert "clone" not in dry["message"], dry


def scenario_witness_the_archive_is_not_made_in_a_users_case_variant_group(env):
    """Codex review, round 4: the archive lookup fell back to ANY case
    variant, so a user's own empty 'MATCHUPS RECORDINGS' became the archive,
    and cleanup later deleted it as ours."""
    m = env.m
    theirs = m.ChannelGroup.objects.create(name="MATCHUPS RECORDINGS")
    arch = env.plugin._ensure_archive_channel("Matchups Recordings")
    assert arch.channel_group.name == "Matchups Recordings", arch.channel_group.name
    assert not m.Channel.objects.filter(channel_group=theirs).exists()
    env.plugin._cleanup_empty_archive("Matchups Recordings")
    assert m.ChannelGroup.objects.filter(pk=theirs.pk).exists()


def scenario_witness_a_dry_run_makes_no_paid_description_call(env):
    """Codex review, round 4: with LLM descriptions on, a dry run called the
    provider on every cache miss, for prose it then neither wrote nor cached."""
    import importlib
    llm = importlib.import_module(env.plugin.__name__.rsplit(".", 1)[0] + ".llm_descriptions")
    calls = []
    real = llm._call_anthropic
    llm._call_anthropic = lambda *a, **k: calls.append(1) or "A preview."
    try:
        user_ch, _ = _seed_source_channel(env)
        game = _row(401, _now() + timedelta(hours=6), user_ch.id)
        _publish(env, [game], settings=_settings(**COMPACT))
        _write_cache(env, [game])
        env.plugin._action_apply(_settings(
            dry_run=True, llm_descriptions_enabled=True, anthropic_api_key="test-key", **COMPACT))
    finally:
        llm._call_anthropic = real
    assert calls == [], f"{len(calls)} paid description call(s) in a dry run"


def scenario_an_archive_beside_the_live_group_is_not_a_previous_group(env):
    """Guard: with the recordings name no longer protected by name, a normal
    apply must still leave the archive group alone and report no migration."""
    m = env.m
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    arch = m.Channel.objects.create(name="Matchups Recordings", channel_number=9999,
                                    channel_group=archive, tvg_id=env.plugin.ARCHIVE_TVG_ID)
    m.Recording.objects.create(
        channel=arch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(**COMPACT))
    assert result["status"] == "ok" and "Migrated from old target" not in result["message"], result
    arch.refresh_from_db()
    assert arch.channel_group_id == archive.pk


def scenario_witness_a_renamed_away_group_holding_streams_is_kept(env):
    """Codex review, round 5: emptied of our channels, the old group was
    deleted even while M3U streams still belonged to it, and SET_NULL left
    those streams with no group."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    old = ours.channel_group
    stream = m.Stream.objects.create(name="Grouped stream", channel_group=old)
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert ours.channel_group.name == NEW
    stream.refresh_from_db()
    assert stream.channel_group_id == old.pk, "the stream lost its group"


def _snapshot(env):
    """Every row of every model, every state file the plugin writes, and the
    logo directory: what a stopped apply must leave exactly as it found it."""
    from django.apps import apps as registry
    rows = {
        model.__name__: sorted(repr(sorted(r.items())) for r in model.objects.values())
        for model in registry.get_models()
    }
    # Every file under the scenario's temp dir: the redirected state files,
    # the logo dir, and any sidecar a new write might add. (A file written
    # into the repo checkout instead fails the harness itself.)
    files = {}
    for root, _dirs, names in os.walk(env.tmp):
        for name in names:
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                files[os.path.relpath(path, env.tmp)] = hashlib.sha256(f.read()).hexdigest()
    return rows, files


def _gate_count():
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
    apply = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_action_apply")
    return sum(1 for n in ast.walk(apply) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "_apply_time_short")


def scenario_stopping_at_any_check_leaves_nothing_behind(env):
    """Class guard, behavioural: for EVERY _apply_time_short call in
    _action_apply, stop apply there and compare the whole database, the state
    files and the logo directory with what they were. The apply renames to a
    group that does not exist yet, so every pre-check write path is armed:
    create the group, move our channel, delete the old group, create our
    EPGSource, move our guide rows off the old source and delete it (#263).
    A write moved above any check, however it is spelled, fails here."""
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    # Auto-record armed too, so its state and recordings are in play.
    armed = dict(COMPACT, recorded_teams="Georgia", record_ranked_games=True)
    _publish(env, [game], settings=_settings(**armed))
    assert env.m.Recording.objects.exists(), "instrument: auto-record did not run"
    _write_cache(env, [game])
    gates = _gate_count()
    assert gates >= 2, f"instrument: found {gates} time checks in _action_apply"
    real = env.plugin._apply_time_short
    for stop_at in range(1, gates + 1):
        before = _snapshot(env)
        calls = []

        def check(*a, **k):
            calls.append(1)
            if len(calls) == stop_at:
                return {"status": "error", "message": "stopped by the test"}
            return real(*a, **k)

        env.plugin._apply_time_short = check
        try:
            result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **armed))
        finally:
            env.plugin._apply_time_short = real
        assert result.get("message") == "stopped by the test", (
            f"instrument: apply never reached check #{stop_at}: {result}"
        )
        after = _snapshot(env)
        changed = sorted(k for k in set(before[0]) | set(after[0]) if before[0].get(k) != after[0].get(k))
        changed += sorted(k for k in set(before[1]) | set(after[1]) if before[1].get(k) != after[1].get(k))
        assert not changed, f"apply stopped at check #{stop_at} of {gates} still wrote: {changed}"
    # Instrument: unstopped, the same apply DOES write all of it.
    result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **armed))
    assert result["status"] == "ok", result
    m = env.m
    assert m.ChannelGroup.objects.filter(name="Renamed Matchups").exists()
    assert not m.ChannelGroup.objects.filter(name=OLD).exists()
    assert not m.EPGSource.objects.filter(name=OLD).exists()
    assert m.ProgramData.objects.filter(epg__epg_source__name="Renamed Matchups").exists()


def scenario_witness_a_name_the_database_rejects_is_refused_before_any_write(env):
    """Codex review, round 18: the group setup now runs in the main
    transaction, AFTER the recording cancellations commit. A live-group name
    the database rejects (longer than EPGSource.name allows) failed every
    apply there, after its cancellations, so a displaced recording was
    cancelled and its replacement, planned in the rolled-back transaction,
    never scheduled; no later apply could repair it. On base the setup failed
    first. Refused now before the first write. sqlite does not enforce
    lengths, so a pre_save receiver stands in for Postgres's refusal."""
    from django.db.models.signals import pre_save
    from django.db.utils import DataError
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    armed = dict(COMPACT, recorded_teams="Georgia", record_ranked_games=True)
    _publish(env, [game], settings=_settings(**armed))
    assert m.Recording.objects.exists(), "instrument: auto-record did not run"
    _write_cache(env, [game])
    limit = m.EPGSource._meta.get_field("name").max_length
    too_long = "M" * (limit + 1)

    def postgres_length_check(sender, instance, **kw):
        if len(instance.name or "") > limit:
            raise DataError("value too long for type character varying(%d)" % limit)

    pre_save.connect(postgres_length_check, sender=m.EPGSource)
    before = _snapshot(env)
    try:
        # Recording no team now: the plan cancels the scheduled recording.
        result = env.plugin._action_apply(_settings(
            channel_profile_name=too_long, **dict(armed, recorded_teams="", record_ranked_games=False)))
    except DataError:
        result = None
    finally:
        pre_save.disconnect(postgres_length_check, sender=m.EPGSource)
    after = _snapshot(env)
    changed = sorted(k for k in set(before[0]) | set(after[0]) if before[0].get(k) != after[0].get(k))
    changed += sorted(k for k in set(before[1]) | set(after[1]) if before[1].get(k) != after[1].get(k))
    assert not changed, f"an apply whose setup the database rejected still wrote: {changed}"
    assert result is not None and result["status"] == "error" and str(limit) in result["message"], result


def scenario_every_name_the_setup_writes_is_checked(env):
    """Guard for _setup_name_problem: both settings, every model each names,
    the NUL Postgres refuses in any text, and nothing at the limit itself."""
    m = env.m
    check = env.plugin._setup_name_problem
    src_limit = m.EPGSource._meta.get_field("name").max_length
    chan_limit = m.Channel._meta.get_field("name").max_length
    assert src_limit and chan_limit, "instrument: the harness models carry no limits"
    assert check("Top Matchups", "Matchups Recordings") is None
    assert check("M" * src_limit, "R" * chan_limit) is None
    assert "Target group name" in check("M" * (src_limit + 1), "Matchups Recordings")
    assert "Recordings group name" in check("Top Matchups", "R" * (chan_limit + 1))
    assert "NUL" in check("Top\x00Matchups", "Matchups Recordings")
    assert "NUL" in check("Top Matchups", "Matchups\x00Recordings")


# ----------------------------------------- class guard, by source (AST) ---

_ORM_WRITES = {"create", "get_or_create", "update_or_create", "bulk_create", "bulk_update",
               "save", "delete"}
_FILE_WRITES = {"write_json_atomic", "save_state", "write_text", "write_bytes"}
_ATOMIC = "transaction.atomic()"

# Committed writes apply makes AFTER its last time check but OUTSIDE its main
# transaction, each with why it cannot wait for that transaction. Anything new
# there must be added here, with its reason, or fail.
ALLOWED_BEFORE_MAIN_TRANSACTION = {
    "_autorecord_cancel_committed": (
        "Recording cancellations commit before numbering on purpose (#288: a "
        "recording counted as keeping its channel held a full compact band's "
        "slot; #291: the second plan must name the channels numbering placed). "
        "Their state is written ahead of the commit, and a kill after one is "
        "repaired by the next plan (#303)."
    ),
    "_resolve_matchup_logo_id": (
        "The network half's Logo rows (get_or_create) for images it downloads. "
        "The network half must stay outside the transaction (#136); the rows "
        "are idempotent, and a kill leaves one the next apply reuses."
    ),
}


def _is_atomic(node):
    return isinstance(node, ast.With) and any(
        ast.unparse(i.context_expr) == _ATOMIC for i in node.items)


def _write_sites(fn, writers, aliases=None):
    """(line, label) for every write `fn` makes itself, not inside nested
    defs (those count where they are CALLED, through `writers`). `writers`
    holds plain names (functions of the same module) and "module.func" names
    (other plugin modules), resolved through `aliases` ({local name: module})."""
    aliases = aliases or {}
    querysets = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Assign) and any(k in ast.unparse(n.value) for k in (".objects", ".filter(")):
            querysets |= {t.id for t in n.targets if isinstance(t, ast.Name)}
    out = []

    def visit(n):
        if n is not fn and isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return
        if _is_atomic(n):
            out.append((n.lineno, _ATOMIC))
        if isinstance(n, ast.Call):
            f = n.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            receiver = ast.unparse(f.value) if isinstance(f, ast.Attribute) else ""
            qualified = (f"{aliases[receiver]}.{f.attr}" if isinstance(f, ast.Attribute)
                         and receiver in aliases else aliases.get(name))
            if isinstance(f, ast.Attribute) and f.attr in _ORM_WRITES and receiver != "os.path":
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and f.attr == "update" and (
                    ".objects" in receiver or "filter(" in receiver or receiver in querysets):
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and (
                    (receiver == "os" and f.attr in ("replace", "remove", "unlink", "rename"))
                    or receiver == "shutil"):
                out.append((n.lineno, ast.unparse(f)))
            elif name == "open" and _opens_for_writing(n, positional=1 if not receiver else 0):
                out.append((n.lineno, ast.unparse(f)))
            elif name in _FILE_WRITES or (not receiver and name in writers) or qualified in writers:
                out.append((n.lineno, qualified or name))
        for c in ast.iter_child_nodes(n):
            visit(c)

    visit(fn)
    return out


def _opens_for_writing(call, positional):
    """open(path, "w") / Path(p).open("w"): a mode with w, a, x or +."""
    mode = None
    if len(call.args) > positional and isinstance(call.args[positional], ast.Constant):
        mode = call.args[positional].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            mode = kw.value.value
    return isinstance(mode, str) and any(c in mode for c in "wax+")


def _writers(functions, known=(), aliases=None):
    """Names among `functions` that write, directly or through each other."""
    found = set(known)
    changed = True
    while changed:
        changed = False
        for name, fn in functions.items():
            if name not in found and _write_sites(fn, found, aliases):
                found.add(name)
                changed = True
    return found


def _module_writers(repo=os.path.dirname(PLUGIN_PY)):
    """"module.func" for every module-level function of a plugin module that
    writes (a file, the database), resolved within each module."""
    found = set()
    paths = [os.path.join(repo, n) for n in os.listdir(repo) if n.endswith(".py")]
    sources = os.path.join(repo, "sources")
    paths += [os.path.join(sources, n) for n in os.listdir(sources) if n.endswith(".py")]
    for path in paths:
        mod = os.path.relpath(path, repo)[:-3].replace(os.sep, ".")
        tree = ast.parse(open(path, encoding="utf-8").read())
        fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        found |= {f"{mod}.{name}" for name in _writers(fns)}
    return found


def _aliases(tree):
    """{local name: module} for `from . import X as Y` and `from .X import f`
    anywhere in `tree` (apply imports its siblings locally)."""
    out = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.level == 1:
            for a in n.names:
                if n.module is None:
                    out[a.asname or a.name] = a.name
                else:
                    out[a.asname or a.name] = f"{n.module}.{a.name}"
    return out


def _apply_layout(source, module_writers=frozenset()):
    tree = ast.parse(source)
    aliases = _aliases(tree)
    # A `from .X import f` alias names the function itself.
    func_aliases = {k: v for k, v in aliases.items() if v in module_writers}
    module = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    apply = module["_action_apply"]
    nested = {n.name: n for n in ast.walk(apply) if isinstance(n, ast.FunctionDef) and n is not apply}
    known = set(module_writers) | set(func_aliases)
    mod_aliases = {k: v for k, v in aliases.items() if k not in func_aliases}
    writers = _writers(nested, _writers(module, known, mod_aliases), mod_aliases)
    checks = [n.lineno for n in ast.walk(apply) if isinstance(n, ast.Call)
              and getattr(n.func, "id", None) == "_apply_time_short"]
    main = [n.lineno for n in apply.body if _is_atomic(n)]
    return sorted(set(_write_sites(apply, writers, mod_aliases))), checks, main


@pytest.mark.parametrize("fn_name", ["_commit_group_setup", "_cleanup_empty_archive", "_move_archive_into"])
def test_a_group_is_locked_before_its_references_are_checked(fn_name):
    """Codex review, round 6: a stream assigned to a group between the
    "does it hold streams?" check and the delete still lost its group. The
    group row is locked first (Postgres serialises the stream's foreign-key
    check behind it); a sqlite harness cannot show the race, so this pins the
    order in the source."""
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == fn_name)
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)]
    locks = [n.lineno for n in calls if isinstance(n.func, ast.Attribute)
             and n.func.attr == "select_for_update" and "ChannelGroup" in ast.unparse(n.func)]
    checks = [n.lineno for n in calls if getattr(n.func, "id", None) == "_group_is_referenced"]
    assert locks and checks, (locks, checks)
    assert min(locks) < min(checks), f"{fn_name} checks references before locking the group"


@pytest.mark.parametrize("fn_name,lock_needle,check_needle", [
    ("_cleanup_empty_archive", "ARCHIVE_TVG_ID", "channel_id"),
    ("_action_apply", "stale_ids_all", "stale_ids_all"),
])
def test_a_channel_is_locked_before_its_recordings_are_checked(fn_name, lock_needle, check_needle):
    """Codex review, round 10: a recording made on a channel between the "has
    it any recordings?" read and its delete went with it (CASCADE). The rows
    are locked first; sqlite cannot show the race, so the order is pinned in
    the source."""
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == fn_name)
    calls = [(n.lineno, ast.unparse(n)) for n in ast.walk(fn) if isinstance(n, ast.Call)]
    locks = [ln for ln, src in calls
             if src.startswith("Channel.objects.select_for_update().filter(") and lock_needle in src]
    checks = [ln for ln, src in calls
              if src.startswith("Recording.objects.filter(") and check_needle in src]
    assert locks and checks, (locks, checks)
    assert min(locks) < min(checks), f"{fn_name} reads recordings before locking the channels"


def _locks_through_a_join(source):
    """Lines of every statement that row-locks (select_for_update) through a
    relation lookup (a `__` keyword other than pk__in / id__in, or a lookup
    on a field: `name__iexact` is not a join, `channels__tvg_id` is)."""
    allowed_suffixes = ("__in", "__iexact", "__isnull", "__startswith", "__exact")
    found = []
    for stmt in ast.walk(ast.parse(source)):
        if not isinstance(stmt, ast.stmt) or isinstance(stmt, (ast.FunctionDef, ast.ClassDef,
                                                               ast.If, ast.For, ast.With, ast.Try)):
            continue
        if "select_for_update(" not in ast.unparse(stmt):
            continue
        for call in (n for n in ast.walk(stmt) if isinstance(n, ast.Call)):
            for kw in call.keywords:
                name = kw.arg or ""
                base = next((name[: -len(x)] for x in allowed_suffixes if name.endswith(x)), name)
                if "__" in base:
                    found.append((stmt.lineno, name))
    return found


def test_no_row_lock_goes_through_a_join():
    """Postgres refuses FOR UPDATE on the nullable side of an outer join,
    which a lookup across a relation (OR'd, or nullable) produces; sqlite,
    which ignores select_for_update, cannot show it. Every lock here reads
    one table, by pk or by its own columns."""
    assert [n for _, n in _locks_through_a_join(
        "list(ChannelGroup.objects.select_for_update().filter(Q(channels__tvg_id=1) | Q(name=2)))\n"
        "x = Channel.objects.select_for_update().filter(channel_group__name='a').first()\n"
        "y = EPGSource.objects.select_for_update().filter(pk__in=[1], name__iexact='a').first()\n"
    )] == ["channels__tvg_id", "channel_group__name"], "instrument"
    source = open(PLUGIN_PY, encoding="utf-8").read()
    assert source.count("select_for_update(") >= 6, "instrument: the plugin's locks were not found"
    assert _locks_through_a_join(source) == []


class TestNothingCommitsBeforeTheLastCheck:
    def _layout(self):
        return _apply_layout(open(PLUGIN_PY, encoding="utf-8").read(), _module_writers())

    def test_instrument(self):
        sites, checks, main = self._layout()
        assert len(checks) >= 2 and len(main) == 1, (checks, main)
        assert any(label == _ATOMIC for _l, label in sites)
        # Writes in sibling modules are resolved, not only plugin.py's own.
        writers = _module_writers()
        assert {"logos.download_thumb", "recording.save_state", "_util.write_json_atomic"} <= writers

    def test_the_group_setup_runs_inside_the_main_transaction(self):
        """Atomic with the slate: a stopped, refused or failed apply never
        leaves a rename half done (Codex review, round 2)."""
        tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
        apply = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_action_apply")
        main = [n for n in apply.body if _is_atomic(n)]
        calls = [n for n in ast.walk(apply) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", None) == "_commit_group_setup"]
        assert len(main) == 1 and len(calls) == 1
        assert any(n is calls[0] for n in ast.walk(main[0])), "the setup left the main transaction"

    def test_a_write_through_an_imported_module_is_seen(self):
        snippet = ("def _action_apply(s):\n    from . import logos as matchup_logos\n"
                   "    matchup_logos.download_thumb('u', 'p')\n"
                   "    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n"
                   "    with transaction.atomic():\n        pass")
        sites, checks, _main = _apply_layout(snippet, {"logos.download_thumb"})
        assert [s for s in sites if s[0] < max(checks)], sites

    def test_no_write_precedes_the_last_check(self):
        sites, checks, _main = self._layout()
        early = [(line, label) for line, label in sites if line < max(checks)]
        assert not early, (
            f"committed writes before apply's last time check (line {max(checks)}): {early}. "
            "A stopped apply must leave nothing behind; move them after it."
        )

    def test_writes_outside_the_main_transaction_are_allowlisted(self):
        sites, checks, main = self._layout()
        between = {label for line, label in sites if max(checks) < line < main[0]}
        unexplained = between - set(ALLOWED_BEFORE_MAIN_TRANSACTION)
        assert not unexplained, (
            f"committed writes between the last time check and the main transaction "
            f"with no stated reason: {sorted(unexplained)}"
        )
        stale = set(ALLOWED_BEFORE_MAIN_TRANSACTION) - between
        assert not stale, f"allowlist entries apply no longer makes: {sorted(stale)}"

    @pytest.mark.parametrize("snippet", [
        "def _action_apply(s):\n    Channel.objects.create(name='x')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    qs = Channel.objects.filter(a=1)\n    qs.update(b=2)\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    with transaction.atomic():\n        pass\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def helper():\n    write_json_atomic('p', {})\ndef _action_apply(s):\n    helper()\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    def inner():\n        Logo.objects.get_or_create(url='u')\n    inner()\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    os.replace('a', 'b')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    Path('x').write_text('y')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    shutil.move('a', 'b')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    with open('new.json', 'w') as f:\n        f.write('x')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    Path('x').open(mode='a')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
    ])
    def test_the_scan_sees_an_early_write(self, snippet):
        sites, checks, _main = _apply_layout(snippet)
        assert [s for s in sites if s[0] < max(checks)], f"scan missed: {snippet!r}"

    def test_the_scan_ignores_dicts_sets_and_strings(self):
        snippet = ("def _action_apply(s):\n    d = {}\n    d.update(a=1)\n    ids = set()\n"
                   "    ids.update([1])\n    'a_b'.replace('_', '-')\n    open('r.json').read()\n"
                   "    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n"
                   "    with transaction.atomic():\n        pass")
        sites, checks, _main = _apply_layout(snippet)
        assert [s for s in sites if s[0] < max(checks)] == []


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_no_write_before_gate(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
