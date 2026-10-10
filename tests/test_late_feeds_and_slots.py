"""Scheduled slots that lose the run lock, and feeds that arrive between
refreshes (2026-10-10).

Two failures stacked up on one live game that day:

1. At 13:00:00 the reaper's re-apply held the run lock for eight seconds.
   Every worker's scheduler fired in that window, saw the lock held, logged
   "skipping", and slept until 16:00. The slot was simply lost.
2. The provider's M3U (on a 12-hour refresh) renamed its reused event slots to
   the noon game at 12:22, after our 11:42 refresh. Nothing re-matched until a
   person clicked Refresh + apply, so the curated channel had no stream and
   Dispatcharr answered 503 when it was tuned.

These tests pin the fixes: one worker claims each slot and retries the lock;
the reaper loop re-runs the pipeline once per M3U refresh that lands after
ours while a curated game is near; and the pipeline child starts no threads.
"""

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

UTC = timezone.utc


@pytest.fixture
def store(monkeypatch):
    """A fake core.utils.RedisClient shared by every "worker" in a test."""
    data = {}

    class FakeClient:
        def set(self, key, value, nx=False, ex=None):
            if nx and key in data:
                return None
            data[key] = value
            return True

        def delete(self, key):
            return 1 if data.pop(key, None) is not None else 0

    fake_core = MagicMock()
    fake_core.RedisClient.get_client.return_value = FakeClient()
    monkeypatch.setitem(sys.modules, "core", MagicMock())
    monkeypatch.setitem(sys.modules, "core.utils", fake_core)
    return data


@pytest.fixture
def redis_down(monkeypatch):
    fake_core = MagicMock()
    fake_core.RedisClient.get_client.side_effect = RuntimeError("redis down")
    monkeypatch.setitem(sys.modules, "core", MagicMock())
    monkeypatch.setitem(sys.modules, "core.utils", fake_core)


class _Plugin:
    """Stands in for the Plugin instance the loops read settings through."""

    def __init__(self, settings):
        self.settings = settings

    def get_current_settings(self):
        return dict(self.settings)


class _Stop:
    def __init__(self):
        self.fired = False

    def is_set(self):
        return self.fired


def _sleeps(monkeypatch, plugin, *, stop_after=None):
    """Record _scheduler_sleep calls instead of sleeping. Returns the list."""
    calls = []

    def fake(stop_event, timeout):
        calls.append(timeout)
        # The fake does not advance the clock, so a retry loop that ignores its
        # own exit conditions would spin forever: fail it instead of hanging.
        if len(calls) > 50:
            raise AssertionError("retry loop did not stop")
        return stop_after is not None and len(calls) >= stop_after

    monkeypatch.setattr(plugin, "_scheduler_sleep", fake)
    return calls


def _pipeline(monkeypatch, plugin, outcomes):
    """Script run_scheduled_pipeline's ran flag per attempt. Returns calls."""
    calls = []
    seq = list(outcomes)

    def fake(settings, *, record_skip=True):
        calls.append(record_skip)
        return seq.pop(0) if seq else seq_last[0]

    seq_last = [outcomes[-1]]
    monkeypatch.setattr(plugin.tasks, "run_scheduled_pipeline", fake)
    return calls


def _skips(monkeypatch, plugin):
    calls = []
    monkeypatch.setattr(plugin.tasks, "record_skipped_run", lambda kind: calls.append(kind))
    return calls


SETTINGS = {"auto_refresh_enabled": True, "local_timezone": "UTC"}
TIMES = [(4, 0), (10, 0), (13, 0), (16, 0), (19, 0)]


def _now_slot():
    # A slot that has just fired, so the retry window is still open.
    return datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=1)


def _only(slot):
    """A schedule holding just this slot's time of day, so the next slot is a
    day away and the retry deadline is the 30-minute window whatever the wall
    clock says. (TIMES would put the next slot inside the window near 4, 10,
    13, 16 or 19 UTC and make these tests fail at those hours.)"""
    return [(slot.hour, slot.minute)]


class TestScheduledSlot:
    def test_lock_held_at_the_slot_is_retried_not_skipped(self, plugin, store, monkeypatch):
        # WITNESS: before the fix the scheduler ran once, lost the lock to the
        # reaper, and slept until the next slot. Now it waits and runs.
        attempts = _pipeline(monkeypatch, plugin, [False, True])
        sleeps = _sleeps(monkeypatch, plugin)
        skips = _skips(monkeypatch, plugin)
        slot = _now_slot()
        stopped = plugin._run_scheduled_slot(
            _Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert stopped is False
        assert len(attempts) == 2
        assert sleeps == [60]
        assert skips == []
        # Per-attempt skips are not recorded; the slot owner records one at the end.
        assert attempts == [False, False]

    def test_only_one_worker_runs_each_slot(self, plugin, store, monkeypatch):
        # Eight schedulers wake together. The claim, not the lock, keeps the
        # other seven from each running the pipeline once the lock frees.
        attempts = _pipeline(monkeypatch, plugin, [True])
        _sleeps(monkeypatch, plugin)
        slot = _now_slot()
        for _worker in range(8):
            plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert len(attempts) == 1

    def test_next_slot_is_claimable_again(self, plugin, store, monkeypatch):
        attempts = _pipeline(monkeypatch, plugin, [True])
        _sleeps(monkeypatch, plugin)
        slot = _now_slot()
        plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        later = slot + timedelta(hours=3)
        plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, later, _only(later), UTC)
        assert len(attempts) == 2

    def test_gives_up_at_the_deadline_and_records_one_skip(self, plugin, store, monkeypatch):
        attempts = _pipeline(monkeypatch, plugin, [False])
        sleeps = _sleeps(monkeypatch, plugin)
        skips = _skips(monkeypatch, plugin)
        # 29m30s into the 30-minute window: one attempt, no room for another.
        slot = datetime.now(UTC) - timedelta(minutes=29, seconds=30)
        plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert len(attempts) == 1
        assert sleeps == []
        assert skips == ["scheduled_auto_pipeline"]

    def test_retry_never_runs_into_the_next_slot(self, plugin):
        slot = datetime(2026, 10, 10, 13, 0, tzinfo=UTC)
        assert plugin._scheduled_retry_deadline(slot, [(13, 0), (13, 15)], UTC) == \
            datetime(2026, 10, 10, 13, 14, tzinfo=UTC)
        assert plugin._scheduled_retry_deadline(slot, TIMES, UTC) == \
            datetime(2026, 10, 10, 13, 30, tzinfo=UTC)

    def test_stop_while_waiting_exits_the_loop(self, plugin, store, monkeypatch):
        _pipeline(monkeypatch, plugin, [False])
        _sleeps(monkeypatch, plugin, stop_after=1)
        slot = _now_slot()
        assert plugin._run_scheduled_slot(
            _Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC) is True

    def test_turning_auto_refresh_off_while_waiting_cancels(self, plugin, store, monkeypatch):
        attempts = _pipeline(monkeypatch, plugin, [False, True])
        _sleeps(monkeypatch, plugin)
        live = _Plugin({**SETTINGS, "auto_refresh_enabled": False})
        slot = _now_slot()
        plugin._run_scheduled_slot(live, _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert len(attempts) == 1

    def test_redis_down_makes_one_attempt_and_no_retries(self, plugin, redis_down, monkeypatch):
        # Every worker is an "owner" when the claim cannot be checked. If they
        # all retried, Redis returning mid-window would let each take the lock
        # in turn: eight full pipelines.
        assert plugin._claim_scheduled_slot(_now_slot()) == plugin._SLOT_UNKNOWN
        attempts = _pipeline(monkeypatch, plugin, [False])
        sleeps = _sleeps(monkeypatch, plugin)
        slot = _now_slot()
        plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert attempts == [True]          # one attempt, recording its own skip
        assert sleeps == []

    def test_a_late_wake_does_not_start_past_the_deadline(self, plugin, store, monkeypatch):
        attempts = _pipeline(monkeypatch, plugin, [True])
        skips = _skips(monkeypatch, plugin)
        slot = datetime.now(UTC) - timedelta(minutes=31)
        plugin._run_scheduled_slot(_Plugin(SETTINGS), _Stop(), SETTINGS, slot, _only(slot), UTC)
        assert attempts == []
        assert skips == ["scheduled_auto_pipeline"]

    def test_next_fire_time_on_the_fall_back_night(self, plugin):
        # 2026-11-01, America/New_York falls back at 02:00 EDT. At 01:00 EST
        # (06:00 UTC) the 01:30 EDT slot (05:30 UTC) has ALREADY passed. Wall-time
        # comparison picked it again, fired immediately and re-ran the slot.
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
        now = datetime(2026, 11, 1, 6, 0, tzinfo=UTC).astimezone(ny)
        nxt = plugin._next_fire_time([(1, 30), (4, 0)], ny, now=now)
        assert nxt.astimezone(UTC) == datetime(2026, 11, 1, 9, 0, tzinfo=UTC)

    def test_retry_window_is_elapsed_time_across_fall_back(self, plugin):
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
        slot = datetime(2026, 11, 1, 1, 30, tzinfo=ny)          # 01:30 EDT = 05:30 UTC
        assert plugin._scheduled_retry_deadline(slot, [(1, 30), (6, 0)], ny) == \
            datetime(2026, 11, 1, 6, 0, tzinfo=UTC)              # 30 real minutes

    def test_scheduler_loop_goes_through_the_slot_runner(self, plugin):
        # The loop must not call run_scheduled_pipeline directly any more: a
        # direct call is the one-shot that lost the 13:00 slot.
        import inspect
        body = inspect.getsource(plugin._scheduler_loop)
        assert "tasks.run_scheduled_pipeline(" not in body
        assert "_run_scheduled_slot(plugin_ref, stop_event, settings, target, times, tz)" in body


def _cache(matched, games, refreshed=None):
    return {"matched_at": matched.isoformat(),
            "refreshed_at": (refreshed or matched).isoformat(), "games": games}


def _game(start):
    return {"home": "NC State", "away": "Wake Forest", "start_time_utc": start.isoformat()}


# The 2026-10-10 timeline, in UTC.
REFRESHED = datetime(2026, 10, 10, 15, 42, tzinfo=UTC)   # 11:42 EDT
M3U = datetime(2026, 10, 10, 16, 22, tzinfo=UTC)         # 12:22 EDT
KICKOFF = datetime(2026, 10, 10, 16, 0, tzinfo=UTC)      # 12:00 EDT
NOW = datetime(2026, 10, 10, 16, 25, tzinfo=UTC)         # 12:25 EDT


@pytest.fixture
def rematch(plugin, store, monkeypatch):
    """Wire _maybe_rematch_after_m3u's inputs. Returns (state, runs)."""
    state = {"cache": _cache(REFRESHED, [_game(KICKOFF)]), "m3u": M3U, "ran": True}
    runs = []
    monkeypatch.setattr(plugin, "_read_cache", lambda: state["cache"])
    monkeypatch.setattr(plugin, "_latest_m3u_refresh", lambda: state["m3u"])

    def fake_run(settings):
        runs.append(settings)
        return state["ran"]

    monkeypatch.setattr(plugin.tasks, "run_m3u_rematch_pipeline", fake_run)
    return state, runs


class TestRematchAfterM3U:
    def test_m3u_refresh_after_ours_with_a_live_game_runs_once(self, plugin, rematch):
        # WITNESS: nothing re-matched before; the channel stayed empty (503).
        _state, runs = rematch
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is True
        assert len(runs) == 1
        # Every other worker's reaper, and this one's next tick: cooled down.
        for _ in range(8):
            assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        assert len(runs) == 1

    def test_a_failing_pipeline_is_not_rerun_for_every_m3u_refresh(self, plugin, store, rematch):
        # The run "ran" but failed, so the cache never moved past the M3U
        # refresh. An M3U refreshing every five minutes must not start a full
        # failing pipeline each time: the cooldown holds it to one per hour.
        state, runs = rematch
        plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW)
        for i in range(1, 12):
            state["m3u"] = M3U + timedelta(minutes=5 * i)
            plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW + timedelta(minutes=5 * i))
        assert len(runs) == 1
        store.pop(plugin._REMATCH_COOLDOWN_KEY)                  # the hour is up
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW + timedelta(hours=1))
        assert len(runs) == 2

    def test_m3u_refresh_during_matching_is_not_missed(self, plugin, rematch):
        # Matching read the streams at 11:40, the M3U finished at 11:41, the
        # cache was written at 11:42. Against refreshed_at the M3U looks older
        # and its feeds would never be rematched.
        state, runs = rematch
        state["cache"] = _cache(REFRESHED - timedelta(minutes=2), [_game(KICKOFF)],
                                refreshed=REFRESHED)
        state["m3u"] = REFRESHED - timedelta(minutes=1)
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is True
        assert len(runs) == 1

    def test_cache_from_before_matched_at_falls_back_to_refreshed_at(self, plugin, rematch):
        state, runs = rematch
        state["cache"] = {"refreshed_at": REFRESHED.isoformat(), "games": [_game(KICKOFF)]}
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is True

    def test_m3u_older_than_our_refresh_does_nothing(self, plugin, rematch):
        state, runs = rematch
        state["m3u"] = REFRESHED - timedelta(hours=11)
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        assert runs == []

    def test_no_game_near_does_nothing(self, plugin, rematch):
        state, runs = rematch
        state["cache"] = _cache(REFRESHED, [_game(NOW + timedelta(days=2)),
                                            _game(NOW - timedelta(hours=5))])
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        assert runs == []

    def test_window_edges(self, plugin):
        games = [_game(NOW - timedelta(hours=4)), _game(NOW + timedelta(hours=6)),
                 _game(NOW - timedelta(hours=4, minutes=1)),
                 _game(NOW + timedelta(hours=6, minutes=1)), {"start_time_utc": None}]
        assert len(plugin._games_in_rematch_window(games, NOW)) == 2

    def test_auto_refresh_off_does_nothing(self, plugin, rematch):
        _state, runs = rematch
        assert plugin._maybe_rematch_after_m3u({"auto_refresh_enabled": False}, now=NOW) is False
        assert runs == []

    def test_a_feed_just_after_our_refresh_is_not_held_back(self, plugin, rematch):
        # Refresh at 12:00, feed at 12:01, kickoff 12:02: rematch on the next
        # tick, not half an hour later.
        state, runs = rematch
        state["cache"] = _cache(NOW - timedelta(minutes=2), [_game(NOW)])
        state["m3u"] = NOW - timedelta(minutes=1)
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is True

    def test_lock_held_gives_the_claim_back(self, plugin, rematch):
        state, runs = rematch
        state["ran"] = False
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        state["ran"] = True
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is True
        assert len(runs) == 2

    def test_no_cache_timestamp_does_nothing(self, plugin, rematch):
        state, runs = rematch
        state["cache"] = {"games": [_game(KICKOFF)]}
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        assert runs == []
        assert runs == []

    def test_redis_down_does_not_run(self, plugin, monkeypatch, redis_down):
        runs = []
        monkeypatch.setattr(plugin, "_read_cache", lambda: _cache(REFRESHED, [_game(KICKOFF)]))
        monkeypatch.setattr(plugin, "_latest_m3u_refresh", lambda: M3U)
        monkeypatch.setattr(plugin.tasks, "run_m3u_rematch_pipeline", lambda s: runs.append(s))
        assert plugin._maybe_rematch_after_m3u(SETTINGS, now=NOW) is False
        assert runs == []

    def test_reaper_loop_checks_after_every_wake(self, plugin):
        import inspect
        body = inspect.getsource(plugin._reaper_loop)
        stops = body.index("_run_due_stops(settings)")
        check = body.index("_maybe_rematch_after_m3u(settings)")
        reap_gate = body.index("if reap_delay is None or delay + 1 < reap_delay:")
        # After the wake, and BEFORE the gate that skips non-reap ticks, or it
        # would only ever run on the rare tick that also reaps.
        assert stops < check < reap_gate

    def test_rematch_runs_the_full_pipeline_under_the_lock(self, tasks_mod, monkeypatch):
        seen = {}

        def fake_rul(kind, task_id, target, settings, *, record_skip=True):
            seen.update(kind=kind, record_skip=record_skip, target=target)
            return True

        monkeypatch.setattr(tasks_mod, "_run_under_lock", fake_rul)
        rps = MagicMock(return_value={"status": "ok"})
        monkeypatch.setattr(tasks_mod, "run_pipeline_subprocess", rps)
        assert tasks_mod.run_m3u_rematch_pipeline({"x": 1}) is True
        assert seen["kind"] == "m3u_rematch"
        assert seen["record_skip"] is False
        assert seen["kind"] in tasks_mod.RUN_KIND_LABELS
        seen["target"]({"x": 1})
        rps.assert_called_once_with(tasks_mod.ACTION_AUTO_PIPELINE, {"x": 1})


class TestRunUnderLockRetryContract:
    def test_lost_lock_with_retry_returns_false_and_records_nothing(self, tasks_mod, monkeypatch):
        plugin_stub = type(sys)("stub")
        plugin_stub._try_acquire_scheduler_lock = lambda destructive=True: None
        plugin_stub._release_scheduler_lock = lambda token: None
        monkeypatch.setitem(sys.modules, f"{tasks_mod.__name__.rsplit('.', 1)[0]}.plugin", plugin_stub)
        skips = []
        monkeypatch.setattr(tasks_mod, "record_skipped_run", lambda kind: skips.append(kind))
        target = MagicMock()
        assert tasks_mod._run_under_lock("scheduled_auto_pipeline", "t", target, {},
                                         record_skip=False) is False
        assert skips == []
        assert tasks_mod._run_under_lock("auto_pipeline", "t", target, {}) is False
        assert skips == ["auto_pipeline"]
        target.assert_not_called()


class TestNoThreadsInThePipelineChild:
    def test_child_env_is_set_on_the_subprocess(self, tasks_mod, monkeypatch):
        seen = {}

        def fake_run(argv, **kw):
            seen["env"] = kw["env"]
            raise OSError("stop here")

        monkeypatch.setattr(tasks_mod.subprocess, "run", fake_run)
        tasks_mod.run_pipeline_subprocess(tasks_mod.ACTION_REFRESH, {})
        assert seen["env"][tasks_mod.PIPELINE_CHILD_ENV] == "1"

    def test_plugin_in_the_child_starts_no_threads(self, plugin, monkeypatch):
        monkeypatch.setenv(plugin.tasks.PIPELINE_CHILD_ENV, "1")
        started = []
        monkeypatch.setattr(plugin.threading, "Thread",
                            lambda *a, **k: started.append(k.get("name")) or MagicMock())
        plugin.Plugin()
        assert started == []

    def test_plugin_outside_the_child_still_starts_them(self, plugin, monkeypatch):
        # Control for the test above: the same construction without the marker
        # must start threads, or the guard test passes vacuously.
        monkeypatch.delenv(plugin.tasks.PIPELINE_CHILD_ENV, raising=False)
        sreg, rreg = plugin._scheduler_registry(), plugin._reaper_registry()
        saved = (sreg.thread, sreg.stop_event, rreg.thread, rreg.stop_event)
        sreg.thread = rreg.thread = None
        started = []
        monkeypatch.setattr(plugin.threading, "Thread",
                            lambda *a, **k: started.append(k.get("name")) or MagicMock())
        try:
            plugin.Plugin()
        finally:
            sreg.thread, sreg.stop_event, rreg.thread, rreg.stop_event = saved
        assert sorted(started) == ["ranked_matchups-reaper", "ranked_matchups-scheduler"]
