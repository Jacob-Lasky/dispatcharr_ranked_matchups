"""Apply on an empty slate (#241), driven through the real ORM.

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
OUTAGE_MESSAGE_PART = "could not rule out a source outage, so no channels were removed"


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


def scenario_reaper_vouches_for_the_slate_it_empties(env):
    """A refresh that could not vouch for an empty result still writes a
    NON-empty slate as usual (it is published either way), marked
    `empty_slate_known: false`. The reaper emptying that slate knows exactly
    what it removed, finished games, so its empty result must be published."""
    user_ch, _ = _seed_source_channel(env)
    _write_cache(env, [_row(401, _now() - timedelta(hours=10), user_ch.id)],
                 empty_slate_known=False)
    assert env.plugin._action_apply(_settings())["status"] == "ok"
    assert set(_owned(env)) == {"ranked_matchups:CFB:401"}

    result = env.plugin._action_reap(_settings())

    assert result["status"] == "ok" and result["reaped"] == 1, result
    assert _owned(env) == {}
    assert _read_cache(env)["empty_slate_known"] is True


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


def _game_row(home, away, start):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(
        sport_prefix="CFB", sport_label="NCAA Football", home=home, away=away,
        rank_home=None, rank_away=None, start_time=start, extra={"cfbd_id": 777},
    )


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
    """The case the #240 hold cannot cover: cache.json is gone (the plugin
    folder was replaced on upgrade) and the first refresh hits an outage. With
    no previous games to carry it writes `games == []`, and publishing that
    would delete a live channel. It is marked instead, and apply leaves the
    guide alone until a refresh with games writes a normal cache."""
    user_ch, _ = _seed_source_channel(env)
    owned = _publish(env, [_row(401, _now() - timedelta(minutes=30), user_ch.id)])
    os.remove(env.plugin.CACHE_PATH)

    with _sources(env, _StubSource(error=RuntimeError("CFBD 525"))):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is False

    result = env.plugin._action_apply(_settings())

    assert result["status"] == "ok", result
    assert OUTAGE_MESSAGE_PART in result["message"], result
    assert set(_owned(env)) == set(owned)

    # The source recovers: a normal cache again, and apply publishes it, which
    # takes the game it no longer lists off air as usual.
    other = _game_row("Texas", "Oklahoma", _now() + timedelta(days=1))
    with _sources(env, _StubSource([other])):
        assert env.plugin._action_refresh(_settings())["status"] == "ok"
    assert _read_cache(env)["empty_slate_known"] is True
    assert env.plugin._action_apply(_settings())["status"] == "ok"
    assert "ranked_matchups:CFB:401" not in _owned(env)


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

SCENARIOS = sorted(
    name[len("scenario_"):] for name, obj in list(globals().items())
    if name.startswith("scenario_") and callable(obj)
)


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    out = tmp_path_factory.mktemp("orm_harness") / "results.json"
    proc = subprocess.run(
        [sys.executable, RUNNER, str(out), os.path.abspath(__file__), *SCENARIOS],
        capture_output=True, text=True, timeout=600,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0 and out.exists(), (
        f"harness did not run (rc={proc.returncode})\n{proc.stdout[-4000:]}\n{proc.stderr[-4000:]}"
    )
    return json.loads(out.read_text(encoding="utf-8"))


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
