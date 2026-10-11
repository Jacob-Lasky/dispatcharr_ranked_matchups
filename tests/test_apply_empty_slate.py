"""Apply on an empty slate (#241), and the on-air hold after cache.json was
lost (#264), driven through the real ORM.

The reported defect: when the reaper takes the LAST applied game off the list
(nothing left to promote from the bench), it rewrites the cache with
`games == []` and calls `_action_apply`, which returned "Cache empty; run
refresh first." before reaching its stale-channel deletion. The finished game's
channel stayed on air, the reaper reported ok, and every later reap saw an
empty cache and did nothing.

These run in a subprocess against a real Django ORM over sqlite (see
tests/orm_harness/run_scenarios.py), because the claim under test is about what
apply does to rows: which channels, EPG entries, stream links and recordings
are left afterwards. Each `scenario_*` below is executed there with a fresh
database; the pytest functions at the bottom only launch it and report.

Assertions use literal names and values ("Top Matchups", "Matchups Recordings",
the "ranked_matchups:" tvg_id prefix) rather than the plugin's own constants,
so a constant that drifted would fail here rather than move with the test.
"""

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.path.join(HERE, "orm_harness", "run_scenarios.py")
NO_CACHE_MESSAGE = "Cache empty; run refresh first."
OUTAGE_MESSAGE_PART = "cannot rule out a source outage, so no channels were removed"


# --------------------------------------------------------------- helpers ---
# Used inside the harness subprocess only (they take the scenario `env`).

def _now():
    return datetime.now(timezone.utc)


def _settings(**over):
    s = {
        "dry_run": False,
        "remove_finished_after_minutes": 30,
        # The logo tiers make HTTP calls for every game; nothing here is about
        # logos, and a test must not reach the network.
        "enable_matchup_logos": False,
    }
    s.update(over)
    return s


def _seed_source_channel(env, name="ESPN"):
    """A user's own channel with one stream: what the matcher matches to."""
    m = env.m
    acct = m.M3UAccount.objects.create(name=f"acct-{name}", max_streams=4)
    grp, _ = m.ChannelGroup.objects.get_or_create(name="Sports")
    stream = m.Stream.objects.create(name=f"{name} HD", m3u_account=acct, channel_group=grp)
    ch = m.Channel.objects.create(name=name, channel_number=10, channel_group=grp)
    m.ChannelStream.objects.create(channel=ch, stream=stream, order=0)
    return ch, stream


def _row(cfbd_id, start, channel_id, home="Georgia", away="Alabama"):
    """One cache row with the keys refresh's payload writes (plugin.py step 5),
    matched to `channel_id` the way `_match_fields` records a whole-channel
    match."""
    return {
        "sport_prefix": "CFB",
        "sport_label": "NCAA Football",
        "home": home,
        "away": away,
        "rank_home": 3,
        "rank_away": 5,
        "rank_pool_size": 25,
        "rank_pool_exhaustive": False,
        "start_time_utc": start.isoformat(),
        "kickoff_local": start.strftime("%a %-I:%M %p"),
        "is_today": True,
        "venue": "Bryant-Denny Stadium",
        "spread": 2.5,
        "closeness": None,
        "score": 7.4,
        "score_raw": 2.1,
        "score_breakdown": {"rank": 1.2, "closeness": 0.9},
        "score_notes": [],
        "favorites_matched": [],
        "recorded_matched": [],
        "is_rivalry": False,
        "tournament_stage": None,
        "importance_points": 0.0,
        "importance_notes": [],
        "importance_thresholds_hit": [],
        "channel_id": channel_id,
        "channel_ids": [channel_id],
        "stream_ids": [],
        "channel_name_current": "ESPN",
        "program_title": f"{away} at {home}",
        "match_method": "regex_strict",
        "match_note": "",
        "extra": {"cfbd_id": cfbd_id},
    }


def _write_cache(env, games, refreshed_at=None, **extra):
    """A cache as a current writer leaves it, one that knew the slate it
    replaced (the reaper, or a refresh that read the previous cache)."""
    cache = {
        "refreshed_at": (refreshed_at or _now()).isoformat(),
        "matched_at": (refreshed_at or _now()).isoformat(),
        "summary": [],
        "games": games,
        "bench": [],
        "empty_slate_known": True,
    }
    cache.update(extra)
    with open(env.plugin.CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f)


def _read_cache(env):
    with open(env.plugin.CACHE_PATH, encoding="utf-8") as f:
        return json.load(f)


def _owned(env):
    """Our channels in the live group, by tvg_id."""
    return {
        c.tvg_id: c for c in env.m.Channel.objects.filter(
            tvg_id__startswith="ranked_matchups:", channel_group__name="Top Matchups",
        )
    }


def _publish(env, games, settings=None):
    """Put `games` on air through the real apply, and check it did."""
    _write_cache(env, games)
    result = env.plugin._action_apply(settings or _settings())
    assert result["status"] == "ok", result
    owned = _owned(env)
    assert len(owned) == len(games), (result, sorted(owned))
    return owned


def _strip_stamps(env):
    """Make every published channel look like one an earlier release wrote:
    no cache row stamped on its guide entries (#264)."""
    env.m.ProgramData.objects.update(custom_properties={})


def _end_guide(env, marker):
    """Time passing for one channel's guide: every entry it has is over."""
    past = _now() - timedelta(minutes=1)
    env.m.ProgramData.objects.filter(epg__tvg_id=marker).update(
        start_time=past - timedelta(hours=3), end_time=past,
    )


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_reaping_the_last_game_removes_its_channel(env):
    """The #241 repro, end to end: apply puts one finished game on air, the
    reaper takes it off, and its channel, guide entry and stream links must go."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    start = _now() - timedelta(hours=10)
    owned = _publish(env, [_row(401, start, user_ch.id)])
    (marker, vc), = owned.items()
    assert marker == "ranked_matchups:CFB:401"
    # Sanity: the harness ran apply's real write path, not an early return.
    assert list(m.ChannelStream.objects.filter(channel=vc).values_list("stream_id", flat=True)) == [stream.id]
    assert m.ProgramData.objects.filter(epg__tvg_id=marker).count() >= 1

    result = env.plugin._action_reap(_settings())

    assert result["status"] == "ok", result
    assert result["reaped"] == 1 and result["remaining"] == 0, result
    assert NO_CACHE_MESSAGE not in result["message"], result
    assert _owned(env) == {}, "the finished game's channel is still on air"
    assert not m.Channel.objects.filter(pk=vc.pk).exists()
    assert not m.ChannelStream.objects.filter(channel_id=vc.pk).exists()
    assert not m.EPGData.objects.filter(tvg_id=marker).exists()
    assert not m.ProgramData.objects.filter(tvg_id=marker).exists()
    cache = _read_cache(env)
    assert cache["games"] == [] and cache["reap_apply_pending"] is False
    # Source side untouched: the user's channel and its stream link.
    assert m.Channel.objects.filter(pk=user_ch.pk).exists()
    assert list(m.ChannelStream.objects.filter(channel=user_ch).values_list("stream_id", flat=True)) == [stream.id]


def scenario_reaping_a_cache_from_before_the_marker(env):
    """The first reap after the upgrade reads a cache the previous release
    wrote, with no `empty_slate_known` key. Its refresh applied it, so taking
    the last game off must still delete that game's channel."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(hours=10), user_ch.id)])
    cache = _read_cache(env)
    del cache["empty_slate_known"]
    with open(env.plugin.CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f)

    result = env.plugin._action_reap(_settings())

    assert result["status"] == "ok" and result["reaped"] == 1, result
    assert _owned(env) == {}


def scenario_reaper_does_not_vouch_for_an_unvouched_slate(env):
    """cache.json was lost and the next refresh ran during a CFB outage: it
    could not carry the live CFB game, listed one other game, and wrote that
    non-empty slate with `empty_slate_known: false`. Reaping the other game
    empties the slate, and publishing THAT would delete the CFB channel the
    slate never knew about."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=1), user_ch.id),
                   _row(402, _now() - timedelta(hours=10), user_ch.id, home="Texas", away="Oklahoma")])
    _write_cache(env, [_row(402, _now() - timedelta(hours=10), user_ch.id, home="Texas", away="Oklahoma")],
                 empty_slate_known=False)

    result = env.plugin._action_reap(_settings())

    assert result["reaped"] == 1 and OUTAGE_MESSAGE_PART in result["message"], result
    assert _read_cache(env)["empty_slate_known"] is False
    assert "ranked_matchups:CFB:401" in _owned(env), "a live channel was deleted"


def scenario_refresh_does_not_vouch_on_top_of_an_unvouched_slate(env):
    """Same lost-cache outage, one step further: the unvouched slate's only game
    finishes and the next refresh, CFB still failing, finds nothing. It read a
    previous cache, but that cache could not vouch for itself, so neither can
    this empty result, and the live CFB channel must stay."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=1), user_ch.id)])
    # Published before apply stamped its rows: the DB cannot name it either
    # (with a stamp the refresh holds it, see the #264 scenarios below).
    _strip_stamps(env)
    _write_cache(env, [_row(402, _now() - timedelta(hours=10), user_ch.id, home="Texas", away="Oklahoma")],
                 empty_slate_known=False)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525"))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    cache = _read_cache(env)
    assert cache["games"] == [] and cache["empty_slate_known"] is False, cache

    assert env.plugin._action_apply(_settings())["status"] == "ok"
    assert "ranked_matchups:CFB:401" in _owned(env), "a live channel was deleted"


def scenario_a_second_reap_after_the_last_game_is_a_no_op(env):
    """After the slate empties, later reaps must stay quiet and harmless."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(hours=10), user_ch.id)])
    assert env.plugin._action_reap(_settings())["status"] == "ok"

    again = env.plugin._action_reap(_settings())

    assert again == {"status": "ok", "message": "Cache empty; nothing to reap."}
    assert _owned(env) == {}


class _StubSource:
    """Just enough of a SportSource for `_action_refresh`'s fetch loop."""

    sport_prefix = "CFB"
    sport_label = "NCAA Football"

    def __init__(self, games=(), error=None):
        self._games, self._error = list(games), error

    def fetch_upcoming(self, days_ahead=7):
        if self._error is not None:
            raise self._error
        return list(self._games)


@contextmanager
def _sources(env, *sources):
    """Make the real `_action_refresh` fetch from `sources` instead of APIs."""
    real = env.plugin._build_sources
    env.plugin._build_sources = lambda settings: list(sources)
    try:
        yield
    finally:
        env.plugin._build_sources = real


def _game_row(home, away, start, cfbd_id=777):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(
        sport_prefix="CFB", sport_label="NCAA Football", home=home, away=away,
        rank_home=None, rank_away=None, start_time=start, extra={"cfbd_id": cfbd_id},
    )


def _off_season_source():
    """A source that answers with NO games, as a tournament out of season does
    on every refresh of a real install (Tower's cache, 2026-10-11, lists 15 of
    its 29 sources at "0 games": both World Cups, every playoff source, the
    cups). `_rows_to_hold` counts such a label as an outage, so on a real
    install "every source answered" is never true."""
    src = _StubSource()
    src.sport_prefix, src.sport_label = "WC", "FIFA World Cup"
    return src


def _finish_cached_games(env, cfbd_ids=None):
    """Time passing, as the reaper sees it: the cached games (all, or those
    with `cfbd_ids`) kicked off ten hours ago. Rewrites the file directly so
    every other key, the `empty_slate_known` stamp included, stays exactly as
    the plugin left it."""
    cache = _read_cache(env)
    for g in cache["games"]:
        if cfbd_ids is None or g["extra"]["cfbd_id"] in cfbd_ids:
            g["start_time_utc"] = (_now() - timedelta(hours=10)).isoformat()
    with open(env.plugin.CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f)


def scenario_refresh_that_finds_no_games_clears_the_guide(env):
    """The other producer of a genuine empty cache, driven through the real
    refresh: Favorites-only is on, the source lists only a non-favorite game,
    so refresh writes `games == []` via _empty_refresh_result, and the apply
    that auto_pipeline runs next must take the old slate's channels down (as it
    always did when one favorite game survived the filter)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    later = _now() + timedelta(days=2)
    settings = _settings(favorites="Georgia", favorites_only="strict")
    _publish(env, [_row(401, later, user_ch.id), _row(402, later, user_ch.id, home="Georgia", away="Auburn")])
    # An orphan guide entry on our source with no channel at all.
    src = m.EPGSource.objects.get(name="Top Matchups")
    m.EPGData.objects.create(epg_source=src, tvg_id="ranked_matchups:CFB:999", name="orphan")

    with _sources(env, _StubSource([_game_row("Texas", "Oklahoma", later)])):
        refreshed = env.plugin._action_refresh(settings)
    assert refreshed["status"] == "ok", refreshed
    cache = _read_cache(env)
    assert cache["games"] == [] and cache["refreshed_at"], cache

    result = env.plugin._action_apply(settings)

    assert result["status"] == "ok", result
    assert "stale_deleted=2" in result["message"], result
    assert result["message"].startswith("No games to air"), result
    assert _owned(env) == {}
    assert m.EPGData.objects.filter(epg_source=src).count() == 0
    assert m.ProgramData.objects.count() == 0
    # The group and the source stay: the next refresh with games reuses them.
    assert m.ChannelGroup.objects.filter(name="Top Matchups").exists()
    assert m.EPGSource.objects.filter(name="Top Matchups").exists()


def scenario_source_outage_does_not_empty_the_slate(env):
    """What makes letting an empty slate through safe: a refresh whose source
    FAILS holds the previous games (#240), so the cache it writes is not empty
    and apply keeps their channels. Without the hold, this outage would now
    clear the guide."""
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)])

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525"))):
        refreshed = env.plugin._action_refresh(_settings())
    assert refreshed["status"] == "ok", refreshed
    assert [g["extra"]["cfbd_id"] for g in _read_cache(env)["games"]] == [401]

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert set(_owned(env)) == set(owned)


def scenario_outage_after_the_cache_was_lost_removes_nothing(env):
    """The case neither record of the slate covers: cache.json is gone (the
    plugin folder was replaced on upgrade), the live channel was published by
    a release that did not stamp its row into the DB, and the first refresh
    hits an outage. With no previous games to carry it writes `games == []`,
    and publishing that would delete the live channel. It is marked instead,
    and apply leaves the guide alone."""
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525"))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert OUTAGE_MESSAGE_PART in result["message"], result
    assert set(_owned(env)) == set(owned)

    # The source recovers and lists another game. The live channel is still
    # unknown to the slate, so apply keeps it while its guide says it is on
    # air (#264), and once that guide has run out it goes as usual.
    other = _game_row("Texas", "Oklahoma", _now() + timedelta(days=1))
    with _sources(env, _StubSource([other])):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False  # one channel unaccounted
    result = env.plugin._action_apply(_settings())
    assert "Kept until their guide ends (not in the cache): 1" in result["message"], result
    assert "ranked_matchups:CFB:401" in _owned(env)

    _end_guide(env, "ranked_matchups:CFB:401")
    result = env.plugin._action_apply(_settings())
    assert result.get("slate_vouched") is True, result
    assert "ranked_matchups:CFB:401" not in _owned(env)
    assert _read_cache(env)["empty_slate_known"] is True


def scenario_lost_cache_then_a_healthy_empty_refresh_clears_the_guide(env):
    """The lost-cache guard must not stick on a HEALTHY empty result: every
    source answered, Favorites-only dropped what they listed, so nothing an
    outage could have dropped exists and the old channels go."""
    user_ch, _ = _seed_source_channel(env)
    settings = _settings(favorites="Georgia", favorites_only="strict")
    _publish(env, [_row(402, _now() + timedelta(days=1), user_ch.id, home="Georgia", away="Auburn")], settings)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource([_game_row("Texas", "Oklahoma", _now() + timedelta(days=1))])):
        assert env.plugin._action_refresh(settings)["status"] == "ok"
    cache = _read_cache(env)
    assert cache["games"] == [] and cache["empty_slate_known"] is True, cache

    assert env.plugin._action_apply(settings)["status"] == "ok"
    assert _owned(env) == {}


def scenario_reaping_the_last_game_after_the_cache_was_lost(env):
    """#241 on a real install after an upgrade. Dispatcharr's plugin import
    replaces the plugin folder, so cache.json is gone, and some source always
    answers with no games, so no refresh after that can say every source
    answered. Once apply has published a slate, the DB holds nothing that slate
    omits, and reaping its last game must take the channel down. Before apply
    vouched for what it published, the doubt from the lost cache passed from
    refresh to refresh forever and this channel stayed on air."""
    user_ch, _ = _seed_source_channel(env, name="Oklahoma at Texas")
    assert not os.path.exists(env.plugin.CACHE_PATH)
    game = _game_row("Texas", "Oklahoma", _now() + timedelta(hours=1))
    sources = (_StubSource([game]), _off_season_source())

    with _sources(env, *sources):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False  # the lost-cache doubt
    assert env.plugin._action_apply(_settings())["status"] == "ok"
    assert set(_owned(env)) == {"ranked_matchups:CFB:777"}
    # A later scheduled refresh, same sources, same game.
    with _sources(env, *sources):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert env.plugin._action_apply(_settings())["status"] == "ok"

    _finish_cached_games(env)
    result = env.plugin._action_reap(_settings())

    assert result["status"] == "ok" and result["reaped"] == 1, result
    assert OUTAGE_MESSAGE_PART not in result["message"], result
    assert _owned(env) == {}, "the finished game's channel is still on air"


def scenario_reaper_keeps_the_stamp_its_apply_wrote(env):
    """The reaper rewrites the cache after its apply returns. When that apply
    published an unvouched slate (a manual refresh after the cache was lost,
    no apply since), the reaper's write must keep apply's stamp, or the reap
    that later empties the slate is refused and the last channel stays."""
    _seed_source_channel(env, name="Oklahoma at Texas")
    _seed_source_channel(env, name="Alabama at Georgia")
    first = _game_row("Texas", "Oklahoma", _now() + timedelta(hours=1), cfbd_id=777)
    second = _game_row("Georgia", "Alabama", _now() + timedelta(days=1), cfbd_id=778)
    with _sources(env, _StubSource([first, second]), _off_season_source()):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False
    # The first game finishes before any apply ran.
    _finish_cached_games(env, cfbd_ids={777})

    reaped = env.plugin._action_reap(_settings())
    assert reaped["status"] == "ok" and reaped["remaining"] == 1, reaped
    assert set(_owned(env)) == {"ranked_matchups:CFB:778"}
    assert _read_cache(env)["empty_slate_known"] is True, "the reaper erased apply's stamp"

    _finish_cached_games(env)
    result = env.plugin._action_reap(_settings())
    assert result["status"] == "ok" and result["reaped"] == 1, result
    assert _owned(env) == {}, "the last game's channel is still on air"


def scenario_a_dry_run_apply_does_not_vouch(env):
    """A dry run deletes nothing, so the DB can still hold a channel the slate
    omits, and the slate must stay unvouched."""
    _seed_source_channel(env, name="Oklahoma at Texas")
    game = _game_row("Texas", "Oklahoma", _now() + timedelta(hours=1))
    with _sources(env, _StubSource([game]), _off_season_source()):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"

    # The group already exists, or a dry run returns before it would vouch.
    env.m.ChannelGroup.objects.create(name="Top Matchups")

    result = env.plugin._action_apply(_settings(dry_run=True))

    assert result["status"] == "ok" and "stale_deleted=" in result["message"], result
    assert "slate_vouched" not in result, result
    assert _read_cache(env)["empty_slate_known"] is False


def scenario_apply_does_not_vouch_while_it_keeps_an_unlisted_recording_channel(env):
    """cache.json was lost, a CFB game later today has a recording scheduled,
    and the next refresh no longer lists that game (and an off-season source
    answers with nothing). Apply keeps the CFB channel for its recording
    without the slate listing it. The slate does not cover what is on air, so
    it must stay unvouched, or a later empty slate would take the CFB channel
    down once the recording stops. (A game that had KICKED OFF would be held
    by the refresh itself, from the row apply stamped into the DB, #264.)"""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(hours=3), user_ch.id)])
    m.Recording.objects.create(
        channel=owned["ranked_matchups:CFB:401"], start_time=_now() + timedelta(hours=3),
        end_time=_now() + timedelta(hours=6), custom_properties={},
    )
    os.remove(env.plugin.CACHE_PATH)
    _seed_source_channel(env, name="Oklahoma at Texas")
    other = _game_row("Texas", "Oklahoma", _now() + timedelta(days=1))
    with _sources(env, _StubSource([other]), _off_season_source()):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok" and "1 channel(s) kept" in result["message"], result
    assert set(_owned(env)) == {"ranked_matchups:CFB:401", "ranked_matchups:CFB:777"}
    assert "slate_vouched" not in result, result
    assert _read_cache(env)["empty_slate_known"] is False


def scenario_apply_does_not_vouch_for_a_cache_rewritten_under_it(env):
    """The stamp is for the slate apply published. If cache.json changed while
    apply ran (the scheduler lock failed open and a refresh got in), the file
    now holds a slate apply never saw, and it must not be vouched for."""
    _seed_source_channel(env, name="Oklahoma at Texas")
    game = _game_row("Texas", "Oklahoma", _now() + timedelta(hours=1))
    with _sources(env, _StubSource([game]), _off_season_source()):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    real = env.plugin._autorecord_finish

    def refresh_lands_mid_apply(*a, **k):
        cache = _read_cache(env)
        cache["refreshed_at"] = (_now() + timedelta(minutes=1)).isoformat()
        with open(env.plugin.CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        return real(*a, **k)

    env.plugin._autorecord_finish = refresh_lands_mid_apply
    try:
        result = env.plugin._action_apply(_settings())
    finally:
        env.plugin._autorecord_finish = real

    assert result["status"] == "ok" and "slate_vouched" not in result, result
    assert _read_cache(env)["empty_slate_known"] is False


def scenario_empty_slate_keeps_a_channel_that_is_recording(env):
    """Zero games must not change the recording rules (#146): a channel with a
    recording in progress is kept at its own number, and a channel whose
    recording finished has the recording moved to the archive first."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    start = _now() - timedelta(hours=1)
    owned = _publish(env, [_row(401, start, user_ch.id), _row(402, start, user_ch.id, home="Texas", away="Oklahoma")])
    recording_ch = owned["ranked_matchups:CFB:401"]
    finished_ch = owned["ranked_matchups:CFB:402"]
    number_before = recording_ch.channel_number
    live_rec = m.Recording.objects.create(
        channel=recording_ch, start_time=start, end_time=_now() + timedelta(hours=2),
        custom_properties={"status": "recording"},
    )
    done_rec = m.Recording.objects.create(
        channel=finished_ch, start_time=start - timedelta(days=1),
        end_time=start - timedelta(hours=20), custom_properties={"status": "completed"},
    )
    _write_cache(env, [])

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert set(_owned(env)) == {"ranked_matchups:CFB:401"}
    recording_ch.refresh_from_db()
    assert recording_ch.channel_number == number_before, "kept channel stranded at its parking number"
    live_rec.refresh_from_db()
    done_rec.refresh_from_db()
    assert live_rec.channel_id == recording_ch.id
    assert done_rec.channel.tvg_id == "ranked_matchups:recordings_archive"
    assert done_rec.channel.channel_group.name == "Matchups Recordings"
    assert not m.Channel.objects.filter(pk=finished_ch.pk).exists()


def scenario_empty_slate_rewrites_the_recording_stops_file(env):
    """A live stop planned by an earlier apply must not survive a slate that no
    longer plans it. The reaper loop fires whatever recording_stops.json holds."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=1), user_ch.id)])
    with open(env.plugin.RECORDING_STOPS_PATH, "w", encoding="utf-8") as f:
        json.dump({"ranked_matchups:CFB:401": {"rec_id": 1, "at": _now().isoformat()}}, f)
    _write_cache(env, [])

    env.plugin._action_apply(_settings())

    with open(env.plugin.RECORDING_STOPS_PATH, encoding="utf-8") as f:
        assert json.load(f) == {}


def scenario_empty_slate_keeps_a_scheduled_recording_of_a_recorded_team(env):
    """Auto-record on zero games: a scheduled recording the plugin made for a
    Recorded team, whose game is simply missing from the cache, is the #224
    case (could be a source outage) and must be kept, not cancelled."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    kickoff = _now() + timedelta(hours=6)
    row = _row(401, kickoff, user_ch.id)
    settings = _settings(recorded_teams="Georgia")
    owned = _publish(env, [row], settings)
    rec = m.Recording.objects.get(channel=owned["ranked_matchups:CFB:401"])
    assert rec.custom_properties.get("ranked_matchups_marker") == "ranked_matchups:CFB:401"
    _write_cache(env, [])

    result = env.plugin._action_apply(settings)

    assert result["status"] == "ok", result
    assert m.Recording.objects.filter(pk=rec.pk).exists(), "scheduled recording was cancelled"
    # And its channel is kept for it rather than deleted out from under it.
    assert set(_owned(env)) == {"ranked_matchups:CFB:401"}


def scenario_empty_slate_in_compact_mode(env):
    """Compact numbering has its own all-slots-taken error return; an empty
    slate must not be mistaken for it."""
    user_ch, _ = _seed_source_channel(env)
    settings = _settings(channel_numbering_mode="compact", virtual_channel_base=400)
    _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)], settings)
    _write_cache(env, [])

    result = env.plugin._action_apply(settings)

    assert result["status"] == "ok", result
    assert _owned(env) == {}


def scenario_empty_slate_dry_run_deletes_nothing(env):
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)])
    _write_cache(env, [])

    result = env.plugin._action_apply(_settings(dry_run=True))

    assert result["status"] == "ok", result
    assert "stale_deleted=1" in result["message"], result
    assert set(_owned(env)) == set(owned)
    assert m.EPGData.objects.filter(tvg_id="ranked_matchups:CFB:401").exists()


# ------------------------------- #264: the hold after cache.json was lost ---

def _nfl_source(game_start):
    """A healthy second source, so the refresh after the outage is NOT empty:
    the case #241's refusal never covered, because a non-empty slate is
    published whatever its stamp says."""
    from dispatcharr_ranked_matchups.sources.base import GameRow
    src = _StubSource([GameRow(
        sport_prefix="NFL", sport_label="NFL", home="Chiefs", away="Bills",
        rank_home=None, rank_away=None, start_time=game_start,
        extra={"nfl_game_id": 9001, "game_id": 9001},
    )])
    src.sport_prefix, src.sport_label = "NFL", "NFL"
    return src


def scenario_apply_stamps_the_published_row_on_the_live_entry(env):
    """The DB record the lost-cache hold reads: the cache row each channel was
    published from, on its LIVE guide entry only."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)])

    progs = list(m.ProgramData.objects.filter(epg__tvg_id="ranked_matchups:CFB:401"))
    stamped = [p for p in progs if (p.custom_properties or {}).get("ranked_matchups_row")]
    assert len(progs) == 3 and len(stamped) == 1, [(p.title, p.custom_properties) for p in progs]
    assert stamped[0].title.endswith("ᴸᶦᵛᵉ"), stamped[0].title
    row = stamped[0].custom_properties["ranked_matchups_row"]
    assert row["extra"] == {"cfbd_id": 401} and row["home"] == "Georgia"


def scenario_a_lost_cache_does_not_drop_a_failed_sources_live_game(env):
    """WITNESS, the #264 repro: cache.json is gone, CFB fails, NFL answers.
    The refresh used to write a non-empty slate of the NFL game alone, and
    the apply after it deleted the live CFB channel mid-game."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    cache = _read_cache(env)
    held = [g for g in cache["games"] if g["extra"].get("cfbd_id") == 401]
    assert len(held) == 1 and held[0]["held_on_air"] is True, cache["games"]

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert "ranked_matchups:CFB:401" in _owned(env), "the live CFB channel was deleted"


def scenario_a_lost_cache_does_not_drop_a_live_game_its_healthy_source_stopped_listing(env):
    """WITNESS: no outage at all. ncaaf.py lists only games that have not
    kicked off, so with cache.json gone the live game is simply missing from
    a healthy fetch, and only the DB record can hold it."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    os.remove(env.plugin.CACHE_PATH)
    later = _game_row("Texas", "Oklahoma", _now() + timedelta(days=1), cfbd_id=777)

    with _sources(env, _StubSource([later])):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert env.plugin._action_apply(_settings())["status"] == "ok"

    assert "ranked_matchups:CFB:401" in _owned(env), "the live CFB channel was deleted"


def scenario_a_lost_cache_holds_a_failed_sources_upcoming_game(env):
    """WITNESS: the #240 outage case (CFBD down before kickoff) on the first
    refresh after cache.json was lost. Two hours out, so the guide's live
    window has not opened and apply's on-air fallback cannot be what keeps
    it: only the row read back from the DB does."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=2), user_ch.id)])
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert env.plugin._action_apply(_settings())["status"] == "ok"

    assert "ranked_matchups:CFB:401" in _owned(env)


def scenario_a_lost_cache_does_not_freeze_a_healthy_sources_upcoming_game(env):
    """Guard: the DB record is held by the same rule as the cache. A game that
    has not started, from a source that answered and no longer lists it, goes.
    The off-season source keeps the slate unvouched, so apply's fallback runs
    too, and must not keep a STAMPED channel the refresh judged."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=3), user_ch.id)])
    os.remove(env.plugin.CACHE_PATH)
    later = _game_row("Texas", "Oklahoma", _now() + timedelta(days=1), cfbd_id=777)

    with _sources(env, _StubSource([later]), _off_season_source()):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False
    result = env.plugin._action_apply(_settings())

    assert "Kept until their guide ends" not in result["message"], result
    assert "ranked_matchups:CFB:401" not in _owned(env)


def scenario_a_live_channel_published_before_the_stamp_is_kept_while_on_air(env):
    """WITNESS for apply's fallback: the first refresh after upgrading to the
    release that stamps rows still finds channels without one. cache.json is
    gone, CFB fails, NFL answers: the live CFB channel stays while its own
    guide says it is on air, the slate is not vouched meanwhile, and once its
    guide has run out it goes and the slate is vouched."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    # Nothing in either record names the CFB game: only the NFL one is listed.
    assert [(g["sport_prefix"], g["home"]) for g in _read_cache(env)["games"]] == [("NFL", "Chiefs")]

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert "Kept until their guide ends (not in the cache): 1" in result["message"], result
    assert "slate_vouched" not in result, result
    assert "ranked_matchups:CFB:401" in _owned(env), "the live CFB channel was deleted"

    _end_guide(env, "ranked_matchups:CFB:401")
    result = env.plugin._action_apply(_settings())
    assert result.get("slate_vouched") is True, result
    assert "ranked_matchups:CFB:401" not in _owned(env)


def scenario_an_unstamped_failed_sources_upcoming_channel_is_kept_until_its_guide_ends(env):
    """WITNESS: the first refresh after upgrading to the stamping release, CFB
    failing two hours before kickoff, NFL answering. The refresh has no row
    for the CFB game (no stamp), so it is the #240 outage case with nothing
    to hold it; apply keeps the channel by its own guide instead, and lets it
    go once that guide has run out."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(hours=2), user_ch.id)])
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    result = env.plugin._action_apply(_settings())

    assert "Kept until their guide ends (not in the cache): 1" in result["message"], result
    assert "ranked_matchups:CFB:401" in _owned(env)

    _end_guide(env, "ranked_matchups:CFB:401")
    result = env.plugin._action_apply(_settings())
    assert "ranked_matchups:CFB:401" not in _owned(env)
    assert result.get("slate_vouched") is True, result


def scenario_a_channel_on_someone_elses_guide_is_not_kept(env):
    """The fallback reads only the entries apply wrote for the channel. A
    plugin channel a user pointed at an ordinary EPG feed (always something
    current on it) must not read as a game on air, or it would be kept, and
    the slate left unvouched, for as long as the feed runs."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(hours=3), user_ch.id)])
    feed = m.EPGSource.objects.create(name="Provider", source_type="xmltv")
    espn = m.EPGData.objects.create(epg_source=feed, tvg_id="espn.us", name="ESPN")
    m.ProgramData.objects.create(epg=espn, tvg_id="espn.us", title="SportsCenter",
                                 start_time=_now() - timedelta(hours=1),
                                 end_time=_now() + timedelta(hours=1))
    m.Channel.objects.filter(pk=owned["ranked_matchups:CFB:401"].pk).update(epg_data_id=espn.id)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    result = env.plugin._action_apply(_settings())

    assert "Kept until their guide ends" not in result["message"], result
    assert "ranked_matchups:CFB:401" not in _owned(env)


def scenario_a_kept_channel_survives_a_group_rename(env):
    """Apply's rename cleanup deletes the old group's EPG source, guide entries
    and all. Read after it, an unstamped live channel's guide was gone and the
    channel was reaped mid-game; the keep is read before the cleanup, and the
    cleanup leaves the kept channel's guide for a later apply."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)
    renamed = _settings(channel_profile_name="Top Games")

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(renamed)["status"] == "ok"
    result = env.plugin._action_apply(renamed)

    assert result["status"] == "ok", result
    live = m.Channel.objects.filter(tvg_id="ranked_matchups:CFB:401")
    assert [c.channel_group.name for c in live] == ["Top Games"], result
    assert m.ProgramData.objects.filter(tvg_id="ranked_matchups:CFB:401").exists()


def scenario_a_healthy_empty_refresh_keeps_an_unstamped_live_channel(env):
    """WITNESS for the vouching side: cache.json gone, every source answered,
    Favorites-only dropped what they listed, so the result is empty and no
    source failed. Before #264 that vouched for the empty slate, and apply
    deleted the live channel the refresh never knew about (ncaaf.py does not
    list a game that has kicked off). A channel the hold could not see now
    keeps the result unvouched."""
    user_ch, _ = _seed_source_channel(env)
    settings = _settings(favorites="Georgia", favorites_only="strict")
    _publish(env, [_row(402, _now() - timedelta(minutes=30), user_ch.id, home="Georgia", away="Auburn")],
             settings)
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource([_game_row("Texas", "Oklahoma", _now() + timedelta(days=1))])):
        assert env.plugin._action_refresh(settings)["status"] == "ok"
    cache = _read_cache(env)
    assert cache["games"] == [] and cache["empty_slate_known"] is False, cache

    result = env.plugin._action_apply(settings)

    assert OUTAGE_MESSAGE_PART in result["message"], result
    assert "ranked_matchups:CFB:402" in _owned(env), "the live channel was deleted"


def scenario_a_stamp_that_names_another_game_is_not_read(env):
    """A stamp counts only for the channel it describes. One whose row builds
    a different marker (an EPG entry left behind by a re-keyed channel, a
    hand edit) is ignored, and its channel counts as one the hold cannot see."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)])
    live = m.ProgramData.objects.get(custom_properties__has_key="ranked_matchups_row")
    props = dict(live.custom_properties)
    props["ranked_matchups_row"] = {**props["ranked_matchups_row"], "extra": {"cfbd_id": 999}}
    m.ProgramData.objects.filter(pk=live.pk).update(custom_properties=props)

    rows, unaccounted = env.plugin._published_rows()

    assert rows == [] and unaccounted == 1


def scenario_a_live_game_whose_feed_vanished_keeps_its_channel(env):
    """WITNESS (codex review): the hold carries a live game with its previous
    match, but its stream is gone mid-game. Its score is under the placeholder
    threshold, so apply used to skip the row and reap the live channel as
    stale, the removal #240 exists to prevent. It stays, as a placeholder."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    settings = _settings(placeholder_min_score=9.0)
    row = _row(401, _now() - timedelta(minutes=30), user_ch.id)
    _write_cache(env, [{**row, "score": 9.5}])
    assert env.plugin._action_apply(settings)["status"] == "ok"
    assert "ranked_matchups:CFB:401" in _owned(env)
    m.Stream.objects.filter(pk=stream.pk).delete()
    _write_cache(env, [{**row, "score": 7.4, "held_on_air": True}])

    result = env.plugin._action_apply(settings)

    assert result["status"] == "ok", result
    assert "ranked_matchups:CFB:401" in _owned(env), result
    assert "unmatched_skipped=0" in result["message"], result


def scenario_a_recording_live_game_whose_feed_vanished_keeps_its_stream_links(env):
    """Guard (codex review): a live channel that is RECORDING keeps the #146
    path, kept for its recording with its stream links, rather than becoming
    a placeholder whose links are stripped mid-recording. The stream is
    removed from the source channel; the Stream row itself remains."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    settings = _settings(placeholder_min_score=9.0)
    row = _row(401, _now() - timedelta(minutes=30), user_ch.id)
    _write_cache(env, [{**row, "score": 9.5}])
    assert env.plugin._action_apply(settings)["status"] == "ok"
    ch = _owned(env)["ranked_matchups:CFB:401"]
    m.Recording.objects.create(channel=ch, start_time=_now() - timedelta(minutes=30),
                               end_time=_now() + timedelta(hours=3),
                               custom_properties={"status": "recording"})
    m.ChannelStream.objects.filter(channel=user_ch).delete()
    _write_cache(env, [{**row, "score": 7.4}])

    result = env.plugin._action_apply(settings)

    assert "ranked_matchups:CFB:401" in _owned(env), result
    assert list(m.ChannelStream.objects.filter(channel=ch).values_list("stream_id", flat=True)) == [stream.id]


def scenario_a_finished_unmatched_game_is_not_kept_as_on_air(env):
    """Guard (codex review): "on air" ends at the reap deadline. A source that
    still lists a finished game must not keep its stream-less channel."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    settings = _settings(placeholder_min_score=9.0)
    row = _row(401, _now() - timedelta(minutes=30), user_ch.id)
    _write_cache(env, [{**row, "score": 9.5}])
    assert env.plugin._action_apply(settings)["status"] == "ok"
    m.Stream.objects.filter(pk=stream.pk).delete()
    _write_cache(env, [{**row, "score": 7.4,
                        "start_time_utc": (_now() - timedelta(hours=10)).isoformat()}])

    result = env.plugin._action_apply(settings)

    assert "unmatched_skipped=1" in result["message"], result
    assert "ranked_matchups:CFB:401" not in _owned(env)


def scenario_an_unmatched_game_not_yet_on_air_is_still_skipped(env):
    """Guard: only a game that has kicked off keeps a stream-less channel
    below the threshold. One that has not started goes as before."""
    m = env.m
    user_ch, stream = _seed_source_channel(env)
    settings = _settings(placeholder_min_score=9.0)
    row = _row(401, _now() + timedelta(hours=3), user_ch.id)
    _write_cache(env, [{**row, "score": 9.5}])
    assert env.plugin._action_apply(settings)["status"] == "ok"
    m.Stream.objects.filter(pk=stream.pk).delete()
    _write_cache(env, [{**row, "score": 7.4}])

    result = env.plugin._action_apply(settings)

    assert "unmatched_skipped=1" in result["message"], result
    assert "ranked_matchups:CFB:401" not in _owned(env)


def scenario_a_lost_cache_reads_nothing_from_the_archive_channel(env):
    """The recordings archive is ours but is never a published game: it must
    neither be held nor count as a channel the hold could not see, or it
    would keep every outage-free empty result unvouched for good."""
    env.plugin._ensure_archive_channel("Matchups Recordings")

    rows, unaccounted = env.plugin._published_rows()

    assert rows == [] and unaccounted == 0


def scenario_a_dry_run_does_not_count_a_channel_kept_by_its_guide_as_deleted(env):
    """WITNESS (#170's rule on #264's keep): a rehearsal reports what a real
    run would do. After an upgrade lost cache.json, a real apply keeps an
    unstamped live channel by its guide; the dry run used to report it in
    `stale_deleted`, so the rehearsal said the live game would be removed."""
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    _strip_stamps(env)
    os.remove(env.plugin.CACHE_PATH)
    with _sources(env, _StubSource(error=RuntimeError("CFBD 525")),
                  _nfl_source(_now() + timedelta(days=1))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"

    result = env.plugin._action_apply(_settings(dry_run=True))

    assert result["status"] == "ok", result
    assert "stale_deleted=0" in result["message"], result
    assert "Kept until their guide ends (not in the cache): 1" in result["message"], result
    assert set(_owned(env)) == set(owned)


def scenario_a_dry_run_does_not_count_a_recording_channel_as_deleted(env):
    """WITNESS: the same for a channel the reap keeps for its active
    recording (#146). It left the slate, a real apply keeps it, and the dry
    run must not report it deleted."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    m.Recording.objects.create(
        channel=owned["ranked_matchups:CFB:401"], start_time=_now() - timedelta(minutes=30),
        end_time=_now() + timedelta(hours=3), custom_properties={"status": "recording"},
    )
    _write_cache(env, [_row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma")])

    result = env.plugin._action_apply(_settings(dry_run=True))

    assert result["status"] == "ok", result
    assert "stale_deleted=0" in result["message"], result
    assert "1 channel(s) kept (active recording)" in result["message"], result
    assert set(_owned(env)) == set(owned)


def scenario_a_dry_run_counts_a_channel_whose_recording_auto_record_cancels(env):
    """Guard (codex review of the dry-run count): auto-record cancels its own
    scheduled recording BEFORE the reap, so a real apply then reaps the
    channel. The dry run cancels nothing, and must still not count that
    recording as a reason to keep the channel."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(hours=6), user_ch.id)],
                     _settings(recorded_teams="Georgia"))
    rec = m.Recording.objects.get(channel=owned["ranked_matchups:CFB:401"])
    _write_cache(env, [_row(402, _now() + timedelta(days=1), user_ch.id, home="Texas", away="Oklahoma")])

    dry = env.plugin._action_apply(_settings(dry_run=True))
    assert "stale_deleted=1" in dry["message"], dry
    assert m.Recording.objects.filter(pk=rec.pk).exists()

    real = env.plugin._action_apply(_settings())
    assert "stale_deleted=1" in real["message"], real
    assert not m.Recording.objects.filter(pk=rec.pk).exists()
    assert "ranked_matchups:CFB:401" not in _owned(env)


def _assert_no_cache_touches_nothing(env, write_bad_cache, message=NO_CACHE_MESSAGE):
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() + timedelta(days=1), user_ch.id)])
    write_bad_cache(env.plugin.CACHE_PATH)
    try:
        result = env.plugin._action_apply(_settings())
    except Exception as e:  # the pre-fix behaviour for a wrong-shaped file
        raise AssertionError(f"apply crashed on an unusable cache: {e!r}") from e
    assert result["status"] == "ok" and message in result["message"], result
    assert set(_owned(env)) == set(owned)
    assert m.ProgramData.objects.filter(tvg_id="ranked_matchups:CFB:401").exists()


def scenario_missing_cache_touches_nothing(env):
    _assert_no_cache_touches_nothing(env, os.remove)


def _writes(data: bytes):
    """A `write_bad_cache` that puts exactly `data` in the cache file."""
    def write(path):
        with open(path, "wb") as f:
            f.write(data)
    return write


def scenario_unparseable_cache_touches_nothing(env):
    _assert_no_cache_touches_nothing(env, _writes(b'{"games": ['))


def scenario_non_utf8_cache_touches_nothing(env):
    _assert_no_cache_touches_nothing(
        env, _writes(b'{"games": [], "refreshed_at": "\xff\xfe"}'),
    )


def scenario_non_object_cache_touches_nothing(env):
    _assert_no_cache_touches_nothing(env, _writes(b"[]"))


def scenario_cache_without_refreshed_at_is_not_a_slate(env):
    _assert_no_cache_touches_nothing(env, _writes(b'{"games": []}'))


def scenario_cache_with_null_games_is_not_a_slate(env):
    """Stamped, but `games` is not a list. No writer produces this, so it is a
    damaged file, and reading it as "zero games" would reap the whole group."""
    _assert_no_cache_touches_nothing(
        env, _writes(b'{"games": null, "refreshed_at": "2026-10-11T12:00:00+00:00"}'),
    )


def scenario_cache_without_games_is_not_a_slate(env):
    _assert_no_cache_touches_nothing(
        env, _writes(b'{"refreshed_at": "2026-10-11T12:00:00+00:00", "summary": []}'),
    )


def scenario_empty_cache_from_before_the_marker_is_not_a_slate(env):
    """A pre-#241 empty cache carries a stamp but no `empty_slate_known`, and
    nothing says whether its refresh could see what was on air (an outage after
    the cache was lost writes exactly this). The old apply left the guide alone
    for it, and so must the first apply after the upgrade."""
    _assert_no_cache_touches_nothing(
        env,
        _writes(b'{"games": [], "refreshed_at": "2026-10-11T12:00:00+00:00", "summary": []}'),
        message=OUTAGE_MESSAGE_PART,
    )


def scenario_marker_must_be_true_not_truthy(env):
    _assert_no_cache_touches_nothing(
        env,
        _writes(b'{"games": [], "refreshed_at": "2026-10-11T12:00:00+00:00", '
                b'"empty_slate_known": "false"}'),
        message=OUTAGE_MESSAGE_PART,
    )


# --------------------------------------------------------- pytest driver ---

def collect_scenarios(namespace):
    """The `scenario_*` functions in a test module's globals, without the prefix."""
    return sorted(
        name[len("scenario_"):] for name, obj in list(namespace.items())
        if name.startswith("scenario_") and callable(obj)
    )


def run_harness(tmp_path_factory, test_file, scenarios):
    """Run `scenarios` from `test_file` in the ORM harness subprocess and return
    {name: None | traceback}. Shared with the other harness-driven test files
    (test_apply_group_rename.py), which load this module by path."""
    out = tmp_path_factory.mktemp("orm_harness") / "results.json"
    proc = subprocess.run(
        [sys.executable, RUNNER, str(out), os.path.abspath(test_file), *scenarios],
        capture_output=True, text=True, timeout=600,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0 and out.exists(), (
        f"harness did not run (rc={proc.returncode})\n{proc.stdout[-4000:]}\n{proc.stderr[-4000:]}"
    )
    return json.loads(out.read_text(encoding="utf-8"))


SCENARIOS = collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_empty_slate(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error


def test_read_cache_keeps_a_damaged_stamped_file_for_show_state(plugin, tmp_path, monkeypatch):
    """`games` not being a list makes apply refuse the file (above), but
    _read_cache must still return it: Show current state reads `summary` and
    `refreshed_at` from it to report the failed sources."""
    path = tmp_path / "cache.json"
    path.write_text(json.dumps({
        "games": None,
        "refreshed_at": "2026-10-11T12:00:00+00:00",
        "summary": ["NCAA Football: error (HTTP 525)"],
    }), encoding="utf-8")
    monkeypatch.setattr(plugin, "CACHE_PATH", str(path))
    cache = plugin._read_cache()
    assert cache["summary"] == ["NCAA Football: error (HTTP 525)"]
    assert cache["refreshed_at"] == "2026-10-11T12:00:00+00:00"


def _module_level_json_paths():
    """{(module, NAME): file name} for every public module-level `NAME_PATH = ...` in the
    plugin's own code whose value names a `.json` file: the state files it
    writes beside itself. Private `_..._PATH` names are the shipped data files
    it only reads (team_aliases.json, rivalries.json, ...)."""
    import ast
    repo = os.path.dirname(HERE)
    found = {}
    for root, dirs, files in os.walk(repo):
        dirs[:] = [d for d in dirs if d not in {"tests", "tools", ".git", "__pycache__"}]
        for fn in files:
            if not fn.endswith(".py"):
                continue
            path = os.path.join(root, fn)
            module = os.path.relpath(path, repo)[:-3].replace(os.sep, ".")
            with open(path, encoding="utf-8") as f:
                tree = ast.parse(f.read())
            for node in tree.body:
                if not isinstance(node, ast.Assign):
                    continue
                strings = [c.value for c in ast.walk(node.value)
                           if isinstance(c, ast.Constant) and isinstance(c.value, str)]
                names = [s for s in strings if s.endswith(".json")]
                for target in node.targets:
                    if (isinstance(target, ast.Name) and target.id.endswith("_PATH")
                            and not target.id.startswith("_") and names):
                        found[(module, target.id)] = names[-1]
    return found


def test_harness_redirects_every_state_file_the_plugin_writes():
    """A state file the harness does not redirect is read from and written to
    the checkout. numbering_state.json was missed: on a clean checkout every
    scenario failed with "harness wrote into the repo", and on any later run
    the file left by the first one fed its kickoff floor to every scenario."""
    import ast
    with open(RUNNER, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    redirected = next(
        ast.literal_eval(node.value) for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "_REDIRECTED_PATHS" for t in node.targets)
    )
    state_files = _module_level_json_paths()
    # The scan finds the one that was missed, by its literal file name.
    assert state_files[("plugin", "NUMBERING_STATE_PATH")] == "numbering_state.json"
    assert state_files[("plugin", "CACHE_PATH")] == "cache.json"
    assert sorted(set(state_files) - set(redirected)) == []


def test_every_state_file_the_plugin_writes_is_gitignored():
    """Same scan. sportsdb_thumb_cache.json and gamethumbs_cache.json were
    written beside the code and not ignored, so a local run left them ready
    to be committed."""
    with open(os.path.join(os.path.dirname(HERE), ".gitignore"), encoding="utf-8") as f:
        ignored = set(f.read().split())
    state_files = _module_level_json_paths()
    assert state_files[("plugin", "GAMETHUMBS_CACHE_PATH")] == "gamethumbs_cache.json"
    assert sorted(n for n in state_files.values() if n not in ignored) == []


class TestKeptByGuide:
    """The fallback's rule, pure (#264). Literal titles, as apply writes them."""

    NOW = datetime(2026, 10, 11, 20, 0, tzinfo=timezone.utc)

    def _p(self, start_h, end_h, title):
        return (self.NOW + timedelta(hours=start_h), self.NOW + timedelta(hours=end_h), title)

    def test_live_entry_keeps_even_a_stamped_channel(self, plugin):
        assert plugin._kept_by_guide({"x": 1}, [self._p(-1, 2, "Alabama vs Georgia ᴸᶦᵛᵉ")], self.NOW)

    def test_past_entry_is_still_on_air(self, plugin):
        assert plugin._kept_by_guide({"x": 1}, [self._p(-1, 1, "Past: Alabama vs Georgia")], self.NOW)

    def test_upcoming_entry_keeps_only_an_unstamped_channel(self, plugin):
        up = [self._p(-2, 1, "Upcoming: Alabama vs Georgia, Sat 7:30 PM"),
              self._p(1, 4, "Alabama vs Georgia ᴸᶦᵛᵉ")]
        assert plugin._kept_by_guide(None, up, self.NOW) is True
        assert plugin._kept_by_guide({"x": 1}, up, self.NOW) is False

    def test_recording_dot_upcoming_is_still_upcoming(self, plugin):
        up = [self._p(-2, 1, "🔴 Upcoming: Alabama vs Georgia")]
        assert plugin._kept_by_guide({"x": 1}, up, self.NOW) is False

    def test_nothing_left_keeps_nothing(self, plugin):
        assert plugin._kept_by_guide(None, [], self.NOW) is False
