"""Finished-game reaping (#196) and bench backfill (#197).

The reaper deletes Channels and EPG rows, so these tests care most about the
cases where it must NOT act: a game it cannot date, a bench entry that has
itself expired, and a bench entry with no broadcast to point at.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest


def _game(prefix="CFB", start=None, channel_id=1, **extra):
    g = {
        "sport_prefix": prefix,
        "home": extra.pop("home", "Home"),
        "away": extra.pop("away", "Away"),
        "start_time_utc": start.isoformat() if start else None,
        "channel_id": channel_id,
        "channel_ids": [channel_id] if channel_id else [],
    }
    g.update(extra)
    return g


class TestGameEndEstimate:
    """End time reuses the EPG matcher's per-sport window. A SECOND duration
    table would be free to drift from the one the guide entry is written
    against, and the reaper would then delete a channel whose own programme
    still claimed the game was on air."""

    def test_uses_the_epg_match_window(self, plugin):
        start = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
        _pre, post_hours = plugin._epg_match_window("CFB")
        end = plugin._game_end_utc(_game("CFB", start))
        assert end == start + timedelta(hours=float(post_hours))

    def test_soccer_ends_sooner_than_gridiron(self, plugin):
        start = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
        assert plugin._game_end_utc(_game("EPL", start)) < \
            plugin._game_end_utc(_game("CFB", start))

    def test_undated_game_has_no_end(self, plugin):
        assert plugin._game_end_utc(_game("CFB", None)) is None

    def test_unparseable_start_has_no_end(self, plugin):
        assert plugin._game_end_utc({"sport_prefix": "CFB",
                                     "start_time_utc": "not-a-date"}) is None


class TestReapDeadline:
    def test_adds_the_grace_period_to_the_end(self, plugin):
        start = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
        g = _game("CFB", start)
        assert plugin._game_reap_at_utc(g, 30) == \
            plugin._game_end_utc(g) + timedelta(minutes=30)

    def test_zero_grace_reaps_at_the_end(self, plugin):
        start = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
        g = _game("CFB", start)
        assert plugin._game_reap_at_utc(g, 0) == plugin._game_end_utc(g)

    def test_negative_grace_is_clamped_not_subtracted(self, plugin):
        """A negative setting must never reap a game BEFORE it ends."""
        start = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
        g = _game("CFB", start)
        assert plugin._game_reap_at_utc(g, -600) == plugin._game_end_utc(g)

    def test_undated_game_is_never_reapable(self, plugin):
        """A row we cannot date is a row we must not delete on a guess."""
        assert plugin._game_reap_at_utc(_game("CFB", None), 30) is None


class TestPartitionExpired:
    @staticmethod
    def _now():
        return datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)

    def test_finished_game_is_expired(self, plugin):
        old = self._now() - timedelta(hours=9)
        live, expired = plugin._partition_expired([_game("CFB", old)], self._now(), 30)
        assert not live and len(expired) == 1

    def test_in_progress_game_is_live(self, plugin):
        recent = self._now() - timedelta(minutes=30)
        live, expired = plugin._partition_expired([_game("CFB", recent)], self._now(), 30)
        assert len(live) == 1 and not expired

    def test_game_inside_the_grace_window_is_still_live(self, plugin):
        """Ended, but not yet past the grace period."""
        start = self._now() - timedelta(hours=4, minutes=10)
        live, expired = plugin._partition_expired([_game("CFB", start)], self._now(), 30)
        assert len(live) == 1 and not expired

    def test_undated_game_stays_live(self, plugin):
        live, expired = plugin._partition_expired([_game("CFB", None)], self._now(), 30)
        assert len(live) == 1 and not expired

    def test_future_game_stays_live(self, plugin):
        future = self._now() + timedelta(days=2)
        live, expired = plugin._partition_expired([_game("CFB", future)], self._now(), 30)
        assert len(live) == 1 and not expired

    def test_order_is_preserved(self, plugin):
        now = self._now()
        games = [_game("CFB", now + timedelta(hours=i), home=f"H{i}") for i in range(4)]
        live, _ = plugin._partition_expired(games, now, 30)
        assert [g["home"] for g in live] == ["H0", "H1", "H2", "H3"]


class TestPromotable:
    @staticmethod
    def _now():
        return datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)

    def test_future_matched_game_is_promotable(self, plugin):
        g = _game("CFB", self._now() + timedelta(hours=3))
        assert plugin._promotable([g], self._now(), 30) == [g]

    def test_expired_bench_entry_is_not_promoted(self, plugin):
        """Promoting it would create a channel the reaper deletes on its very
        next tick."""
        g = _game("CFB", self._now() - timedelta(hours=9))
        assert plugin._promotable([g], self._now(), 30) == []

    def test_unmatched_bench_entry_is_not_promoted(self, plugin):
        """Apply would turn it into a placeholder rather than a watchable
        channel, so it does not count as backfill."""
        g = _game("CFB", self._now() + timedelta(hours=3), channel_id=None)
        assert plugin._promotable([g], self._now(), 30) == []

    def test_channel_ids_alone_is_enough(self, plugin):
        g = _game("CFB", self._now() + timedelta(hours=3), channel_id=None)
        g["channel_ids"] = [77]
        assert plugin._promotable([g], self._now(), 30) == [g]

    def test_best_first_order_is_preserved(self, plugin):
        now = self._now()
        gs = [_game("CFB", now + timedelta(hours=2), home=f"H{i}") for i in range(3)]
        assert [g["home"] for g in plugin._promotable(gs, now, 30)] == ["H0", "H1", "H2"]


class TestActionReap:
    """The reap action itself. Deletion is delegated to _action_apply, so these
    assert on the CACHE it hands over plus the fact that apply was invoked."""

    @pytest.fixture
    def spy_apply(self, plugin, monkeypatch):
        calls = []

        def fake_apply(settings):
            calls.append(settings)
            return {"status": "ok", "message": "applied"}

        monkeypatch.setattr(plugin, "_action_apply", fake_apply)
        return calls

    @pytest.fixture
    def cache_file(self, plugin, monkeypatch, tmp_path):
        path = tmp_path / "cache.json"
        monkeypatch.setattr(plugin, "CACHE_PATH", str(path), raising=False)

        state = {}

        def read():
            return json.loads(path.read_text()) if path.exists() else {"games": []}

        def write(data):
            state["last"] = data
            path.write_text(json.dumps(data, default=str))

        monkeypatch.setattr(plugin, "_read_cache", read)
        monkeypatch.setattr(plugin, "_write_cache", write)
        return path, state

    def test_disabled_when_zero(self, plugin, spy_apply, cache_file):
        r = plugin._action_reap({"remove_finished_after_minutes": 0})
        assert "off" in r["message"].lower()
        assert spy_apply == [], "apply must not run when reaping is disabled"

    def test_empty_cache_is_a_noop(self, plugin, spy_apply, cache_file):
        path, _ = cache_file
        path.write_text(json.dumps({"games": []}))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert spy_apply == []
        assert "empty" in r["message"].lower()

    def test_nothing_expired_does_not_apply(self, plugin, spy_apply, cache_file):
        """The whole point is to be cheap. A tick with nothing to do must not
        drag the full apply (LLM descriptions, logo fetches) along with it."""
        path, _ = cache_file
        future = datetime.now(timezone.utc) + timedelta(hours=3)
        path.write_text(json.dumps({"games": [_game("CFB", future)]}))
        plugin._action_reap({"remove_finished_after_minutes": 30})
        assert spy_apply == []

    def test_expired_game_is_dropped_and_apply_runs(self, plugin, spy_apply, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        future = datetime.now(timezone.utc) + timedelta(hours=3)
        path.write_text(json.dumps({
            "games": [_game("CFB", old, home="Done"), _game("CFB", future, home="Later")],
        }))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["reaped"] == 1
        assert [g["home"] for g in state["last"]["games"]] == ["Later"]
        assert len(spy_apply) == 1, "apply must run so the channel is deleted"

    def test_bench_backfills_the_freed_slot(self, plugin, spy_apply, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        future = datetime.now(timezone.utc) + timedelta(hours=3)
        path.write_text(json.dumps({
            "games": [_game("CFB", old, home="Done")],
            "bench": [_game("CFB", future, home="Promoted")],
        }))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["reaped"] == 1 and r["promoted"] == 1
        assert [g["home"] for g in state["last"]["games"]] == ["Promoted"]
        assert state["last"]["bench"] == [], "promoted game must leave the bench"

    def test_promotes_at_most_the_number_reaped(self, plugin, spy_apply, cache_file):
        """Jake's window: the applied slice slides, it does not grow."""
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        future = datetime.now(timezone.utc) + timedelta(hours=3)
        path.write_text(json.dumps({
            "games": [_game("CFB", old, home="Done")],
            "bench": [_game("CFB", future, home=f"B{i}") for i in range(5)],
        }))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["promoted"] == 1
        assert len(state["last"]["games"]) == 1
        assert len(state["last"]["bench"]) == 4

    def test_shrinks_when_the_bench_is_empty(self, plugin, spy_apply, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        path.write_text(json.dumps({"games": [_game("CFB", old)], "bench": []}))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["reaped"] == 1 and r["promoted"] == 0
        assert state["last"]["games"] == []

    def test_expired_bench_entries_are_not_used_as_backfill(self, plugin, spy_apply, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        path.write_text(json.dumps({
            "games": [_game("CFB", old, home="Done")],
            "bench": [_game("CFB", old, home="AlsoDone")],
        }))
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["promoted"] == 0
        assert state["last"]["games"] == []


class TestNextReapDelay:
    def test_never_returns_zero_or_negative(self, plugin, monkeypatch):
        """A past-due game must still round-trip through one sleep, or a
        failing reap spins the thread against the lock."""
        past = datetime.now(timezone.utc) - timedelta(days=1)
        monkeypatch.setattr(plugin, "_read_cache",
                            lambda: {"games": [_game("CFB", past)]})
        assert plugin._next_reap_delay({"remove_finished_after_minutes": 30}) >= 1.0

    def test_capped_at_the_heartbeat(self, plugin, monkeypatch):
        far = datetime.now(timezone.utc) + timedelta(days=30)
        monkeypatch.setattr(plugin, "_read_cache",
                            lambda: {"games": [_game("CFB", far)]})
        d = plugin._next_reap_delay({"remove_finished_after_minutes": 30})
        assert d == float(plugin._REAPER_MAX_SLEEP_SECONDS)

    def test_uses_the_earliest_deadline(self, plugin, monkeypatch):
        now = datetime.now(timezone.utc)
        soon = now + timedelta(minutes=1)      # ends ~4h later for CFB
        later = now + timedelta(hours=5)
        monkeypatch.setattr(plugin, "_read_cache", lambda: {
            "games": [_game("CFB", later), _game("CFB", soon)],
        })
        d = plugin._next_reap_delay({"remove_finished_after_minutes": 30})
        # Must track the SOONER game, not whichever came first in the list.
        assert d < (later - now).total_seconds()

    def test_unreadable_cache_falls_back_to_the_heartbeat(self, plugin, monkeypatch):
        def boom():
            raise OSError("cache gone")
        monkeypatch.setattr(plugin, "_read_cache", boom)
        assert plugin._next_reap_delay({"remove_finished_after_minutes": 30}) == \
            float(plugin._REAPER_MAX_SLEEP_SECONDS)

    def test_disabled_still_returns_the_heartbeat(self, plugin):
        assert plugin._next_reap_delay({"remove_finished_after_minutes": 0}) == \
            float(plugin._REAPER_MAX_SLEEP_SECONDS)


class TestReapWiringContracts:
    """Source-level contracts. Every behavioural test above passes even if the
    reaper is never started, never locked, or never dispatched."""

    def test_reap_runs_out_of_process(self, plugin):
        import inspect
        src = inspect.getsource(plugin._action_reap_locked)
        assert "run_pipeline_subprocess" in src, (
            "reap ends by calling apply, whose bulk DB writes wedge a gevent "
            "worker; it must not run inline"
        )

    def test_reap_takes_the_same_lock_as_apply(self, plugin):
        import inspect
        src = inspect.getsource(plugin._action_reap_locked)
        assert "_try_acquire_scheduler_lock" in src
        assert "_release_scheduler_lock" in src

    def test_runner_dispatches_the_bare_action(self):
        """The parent holds the lock and the token is a parent-process global,
        so the child must call the UNLOCKED function or silently skip."""
        import os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        src = open(os.path.join(here, "_pipeline_runner.py")).read()
        assert "tasks.ACTION_REAP: plugin._action_reap," in src
        assert "plugin._action_reap_locked" not in src

    def test_action_name_has_one_source_of_truth(self):
        import os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        tasks_src = open(os.path.join(here, "tasks.py")).read()
        assert 'ACTION_REAP = "reap"' in tasks_src

    def test_manifest_exposes_the_action(self):
        import json as _json, os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        m = _json.load(open(os.path.join(here, "plugin.json")))
        assert any(a["id"] == "reap_now" for a in m["actions"])

    def test_manifest_exposes_both_settings(self):
        import json as _json, os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        m = _json.load(open(os.path.join(here, "plugin.json")))
        ids = [f["id"] for f in m["fields"]]
        assert "remove_finished_after_minutes" in ids
        assert "bench_size" in ids

    def test_both_settings_default_off(self):
        """Published-plugin safety: neither may change behaviour on upgrade."""
        import json as _json, os
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        m = _json.load(open(os.path.join(here, "plugin.json")))
        by_id = {f["id"]: f for f in m["fields"]}
        assert by_id["remove_finished_after_minutes"]["default"] == 0
        assert by_id["bench_size"]["default"] == 0

    def test_refresh_splits_games_from_bench(self, plugin):
        import inspect
        src = inspect.getsource(plugin._action_refresh)
        assert '"bench": bench_payload' in src
        assert "games_payload[:max_games]" in src

    def test_stop_tears_down_the_reaper(self, plugin):
        import inspect
        src = inspect.getsource(plugin.Plugin.stop)
        assert "_reaper_registry()" in src, (
            "a reaper left running after disable keeps taking a DB connection "
            "per worker, which is the #82 lock-up under another name"
        )


class TestReaperLifecycle:
    """The reaper is a second daemon thread beside the scheduler. Its start and
    stop paths have their own failure modes, and the interesting one is silent:
    a reaper that never starts looks exactly like a reaper with nothing to do."""

    @pytest.fixture(autouse=True)
    def _isolate(self, plugin):
        import threading
        reg, rreg = plugin._scheduler_registry(), plugin._reaper_registry()
        saved = (reg.thread, reg.stop_event, rreg.thread, rreg.stop_event)
        reg.thread = reg.stop_event = None
        rreg.thread = rreg.stop_event = None
        yield
        for r in (reg, rreg):
            if r.stop_event is not None:
                r.stop_event.set()
        reg.thread, reg.stop_event, rreg.thread, rreg.stop_event = saved

    @staticmethod
    def _tracking(plugin, monkeypatch, spawned):
        import threading

        class TrackingThread(threading.Thread):
            def __init__(self, *a, **kw):
                spawned.append(kw.get("name") or "<unnamed>")
                super().__init__(*a, **kw)

            def start(self):
                pass

        monkeypatch.setattr(plugin.threading, "Thread", TrackingThread)

    def test_reaper_starts_when_the_scheduler_is_already_alive(
        self, plugin, monkeypatch,
    ):
        """THE upgrade case, and the one that made the feature inert.

        __init__ returns early when a live scheduler is already in the
        registry, which is the COMMON path (the loader rebuilds Plugin on
        every discovery). With _start_reaper below that return, upgrading from
        a version that had no reaper never started one until the worker
        restarted."""
        import threading

        class _Live:
            def is_alive(self):
                return True

        reg = plugin._scheduler_registry()
        reg.thread, reg.stop_event = _Live(), threading.Event()

        spawned = []
        self._tracking(plugin, monkeypatch, spawned)
        plugin.Plugin()

        assert "ranked_matchups-reaper" in spawned, (
            "reaper did not start while a live scheduler short-circuited __init__"
        )
        assert "ranked_matchups-scheduler" not in spawned, (
            "the live scheduler must still be left alone"
        )
        assert plugin._reaper_registry().thread is not None

    def test_reaper_start_is_idempotent(self, plugin, monkeypatch):
        import threading

        class _Live:
            def is_alive(self):
                return True

        rreg = plugin._reaper_registry()
        rreg.thread, rreg.stop_event = _Live(), threading.Event()
        spawned = []
        self._tracking(plugin, monkeypatch, spawned)
        plugin.Plugin()
        assert "ranked_matchups-reaper" not in spawned

    def test_stop_does_not_clear_a_still_running_thread(self, plugin):
        """Clearing the slot while the thread lives makes the next __init__
        spawn a SECOND reaper. The reaper can be parked on a subprocess for
        minutes, well past the 5s join."""
        import threading

        class _Stubborn:
            def __init__(self):
                self.joined = 0

            def is_alive(self):
                return True

            def join(self, timeout=None):
                self.joined += 1

        rreg = plugin._reaper_registry()
        ev = threading.Event()
        stubborn = _Stubborn()
        rreg.thread, rreg.stop_event = stubborn, ev

        plugin._clear_if_exited(rreg, "test")
        assert ev.is_set(), "the thread must still be signalled"
        assert stubborn.joined == 1
        assert rreg.thread is stubborn, "a live thread must stay in the registry"

    def test_stop_clears_a_thread_that_did_exit(self, plugin):
        import threading

        class _Exited:
            def is_alive(self):
                return False

            def join(self, timeout=None):
                pass

        rreg = plugin._reaper_registry()
        rreg.thread, rreg.stop_event = _Exited(), threading.Event()
        plugin._clear_if_exited(rreg, "test")
        assert rreg.thread is None and rreg.stop_event is None


class TestApplyFailureIsNotSilent:
    """The cache is rewritten BEFORE apply runs, so an apply failure leaves the
    desired state published and the DB behind it. That must be visible and it
    must be retried, or the finished games stay on air with the cache no longer
    listing them, which is a state nothing else can detect."""

    @pytest.fixture
    def cache_file(self, plugin, monkeypatch, tmp_path):
        path = tmp_path / "cache.json"
        state = {}

        def read():
            return json.loads(path.read_text()) if path.exists() else {"games": []}

        def write(data):
            state["last"] = data
            path.write_text(json.dumps(data, default=str))

        monkeypatch.setattr(plugin, "_read_cache", read)
        monkeypatch.setattr(plugin, "_write_cache", write)
        return path, state

    def test_failed_apply_reports_error(self, plugin, monkeypatch, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        path.write_text(json.dumps({"games": [_game("CFB", old)]}))
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: {"status": "error", "message": "boom"})
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["status"] == "error", "a failed apply must not report success"
        assert r["apply_pending"] is True

    def test_failed_apply_leaves_a_pending_flag(self, plugin, monkeypatch, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        path.write_text(json.dumps({"games": [_game("CFB", old)]}))
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: {"status": "error", "message": "boom"})
        plugin._action_reap({"remove_finished_after_minutes": 30})
        assert json.loads(path.read_text())["reap_apply_pending"] is True

    def test_successful_apply_clears_the_flag(self, plugin, monkeypatch, cache_file):
        path, state = cache_file
        old = datetime.now(timezone.utc) - timedelta(hours=9)
        path.write_text(json.dumps({"games": [_game("CFB", old)]}))
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: {"status": "ok", "message": "applied"})
        plugin._action_reap({"remove_finished_after_minutes": 30})
        assert json.loads(path.read_text())["reap_apply_pending"] is False

    def test_next_tick_retries_a_pending_apply(self, plugin, monkeypatch, cache_file):
        """The expired games are already gone from the cache, so nothing else
        would ever notice they still need removing."""
        path, state = cache_file
        future = datetime.now(timezone.utc) + timedelta(hours=5)
        path.write_text(json.dumps({
            "games": [_game("CFB", future)], "reap_apply_pending": True,
        }))
        calls = []
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: (calls.append(s), {"status": "ok"})[1])
        plugin._action_reap({"remove_finished_after_minutes": 30})
        assert len(calls) == 1, "pending apply was not retried"
        assert json.loads(path.read_text())["reap_apply_pending"] is False

    def test_retry_failure_stays_pending(self, plugin, monkeypatch, cache_file):
        path, state = cache_file
        future = datetime.now(timezone.utc) + timedelta(hours=5)
        path.write_text(json.dumps({
            "games": [_game("CFB", future)], "reap_apply_pending": True,
        }))
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: {"status": "error", "message": "still down"})
        r = plugin._action_reap({"remove_finished_after_minutes": 30})
        assert r["status"] == "error" and r["apply_pending"] is True
        assert json.loads(path.read_text())["reap_apply_pending"] is True

    def test_pending_retry_happens_even_with_an_empty_games_list(
        self, plugin, monkeypatch, cache_file,
    ):
        """The worst shape: everything expired, apply failed, cache now empty.
        A bare 'nothing to reap' early return would strand those channels."""
        path, state = cache_file
        path.write_text(json.dumps({"games": [], "reap_apply_pending": True}))
        calls = []
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: (calls.append(s), {"status": "ok"})[1])
        plugin._action_reap({"remove_finished_after_minutes": 30})
        assert len(calls) == 1


class TestLockBusyBackoff:
    def test_lock_busy_is_flagged_for_the_loop(self, plugin, monkeypatch):
        """An overdue game pins the next delay to its 1s floor, so without a
        flag the loop asks Redis for the lock once a second from every worker
        for the whole of a long refresh."""
        monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda destructive=True: None)
        r = plugin._action_reap_locked({"remove_finished_after_minutes": 30})
        assert r.get("lock_busy") is True

    def test_loop_backs_off_on_a_blocked_tick(self, plugin):
        import inspect
        src = inspect.getsource(plugin._reaper_loop)
        assert "_REAPER_BACKOFF_SECONDS" in src
        assert 'result.get("lock_busy")' in src
        assert plugin._REAPER_BACKOFF_SECONDS >= 30
