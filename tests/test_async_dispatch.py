"""Tests for the daemon-thread async action dispatch added in #84.

Covers three surfaces:
  - The inflight Redis helpers serialize/deserialize as expected and
    degrade safely when Redis is unavailable (work must NEVER fail
    because Redis is down -- inflight is best-effort UX, not control).
  - The HTTP-facing _action_refresh_async / _action_auto_pipeline_async
    return a queued envelope WITHOUT running the underlying work.
  - show_status surfaces the inflight Redis key when present.

These tests cover the dispatch surface in isolation; the actual
background thread is mocked. End-to-end behavior (thread acquires lock,
runs work, clears inflight) is verified live against the running
Dispatcharr container during deploy."""

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_tasks_module():
    if f"{PKG_NAME}.tasks" in sys.modules:
        return sys.modules[f"{PKG_NAME}.tasks"]
    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.tasks", os.path.join(REPO_ROOT, "tasks.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.tasks"] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_plugin_module():
    """Same pattern as test_plugin_helpers -- sidestep package __init__."""
    if f"{PKG_NAME}.plugin" in sys.modules:
        return sys.modules[f"{PKG_NAME}.plugin"]
    util_spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py")
    )
    util_mod = importlib.util.module_from_spec(util_spec)
    sys.modules[f"{PKG_NAME}._util"] = util_mod
    util_spec.loader.exec_module(util_mod)

    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.plugin", os.path.join(REPO_ROOT, "plugin.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def tasks_mod():
    return _load_tasks_module()


@pytest.fixture(scope="module")
def plugin_mod():
    return _load_plugin_module()


class TestInflightHelpers:
    """The inflight Redis key is what show_status reads to tell the
    UI 'work is still happening'. The helpers must (a) round-trip
    JSON correctly, (b) return None / no-op when Redis is unavailable
    so the actual work continues even if Redis is down."""

    def test_read_inflight_returns_none_when_redis_unavailable(self, tasks_mod):
        with patch.object(tasks_mod, "_redis", return_value=None):
            assert tasks_mod.read_inflight() is None

    def test_set_inflight_noop_when_redis_unavailable(self, tasks_mod):
        # Must not raise.
        with patch.object(tasks_mod, "_redis", return_value=None):
            tasks_mod._set_inflight("auto_pipeline", "abc123", "refresh")
            tasks_mod._update_inflight_phase("apply")
            tasks_mod._clear_inflight()

    def test_set_inflight_writes_expected_payload(self, tasks_mod):
        fake_redis = MagicMock()
        with patch.object(tasks_mod, "_redis", return_value=fake_redis):
            tasks_mod._set_inflight("auto_pipeline", "abc-uuid", "refresh")
        fake_redis.set.assert_called_once()
        args, kwargs = fake_redis.set.call_args
        key, raw = args[0], args[1]
        assert key == tasks_mod._INFLIGHT_KEY
        payload = json.loads(raw)
        assert payload["kind"] == "auto_pipeline"
        assert payload["task_id"] == "abc-uuid"
        assert payload["phase"] == "refresh"
        assert "started_at" in payload
        assert "pid" in payload
        assert kwargs.get("ex") == tasks_mod._INFLIGHT_TTL_SECONDS

    def test_update_phase_preserves_other_fields(self, tasks_mod):
        existing = {
            "kind": "auto_pipeline",
            "task_id": "abc-uuid",
            "phase": "refresh",
            "started_at": "2026-05-26T14:00:00+00:00",
            "pid": 1234,
        }
        fake_redis = MagicMock()
        fake_redis.get.return_value = json.dumps(existing)
        with patch.object(tasks_mod, "_redis", return_value=fake_redis):
            tasks_mod._update_inflight_phase("apply")
        args, _ = fake_redis.set.call_args
        updated = json.loads(args[1])
        assert updated["phase"] == "apply"
        assert updated["kind"] == "auto_pipeline"
        assert updated["task_id"] == "abc-uuid"
        assert updated["started_at"] == "2026-05-26T14:00:00+00:00"

    def test_update_phase_noop_when_no_existing_key(self, tasks_mod):
        # If the inflight key has expired between set and update, we
        # should NOT recreate a partial entry -- that would lie about
        # which task is in flight.
        fake_redis = MagicMock()
        fake_redis.get.return_value = None
        with patch.object(tasks_mod, "_redis", return_value=fake_redis):
            tasks_mod._update_inflight_phase("apply")
        fake_redis.set.assert_not_called()

    def test_read_inflight_handles_bytes_payload(self, tasks_mod):
        # Some redis clients return bytes; others return str depending
        # on `decode_responses` config. Helper must handle either.
        payload = {"kind": "refresh", "task_id": "x", "phase": "refresh"}
        fake_redis = MagicMock()
        fake_redis.get.return_value = json.dumps(payload).encode("utf-8")
        with patch.object(tasks_mod, "_redis", return_value=fake_redis):
            result = tasks_mod.read_inflight()
        assert result == payload


class TestThreadLaunchers:
    """The background-thread launchers must (a) start the thread and
    return immediately, (b) return a fresh UUID per call so the UI can
    correlate, (c) NOT raise even if the underlying action would."""

    def test_auto_pipeline_returns_uuid_task_id(self, tasks_mod):
        fake_thread = MagicMock()
        with patch.object(tasks_mod.threading, "Thread", return_value=fake_thread):
            task_id = tasks_mod.run_auto_pipeline_background({"max_games": 5})
        fake_thread.start.assert_called_once()
        # Loose UUID4 shape check: 36 chars with hyphens at the standard
        # positions. We don't pin to a specific value because run_*_background
        # mints a fresh UUID each call.
        assert len(task_id) == 36
        assert task_id.count("-") == 4

    def test_refresh_returns_distinct_task_ids_per_call(self, tasks_mod):
        fake_thread = MagicMock()
        with patch.object(tasks_mod.threading, "Thread", return_value=fake_thread):
            t1 = tasks_mod.run_refresh_background({})
            t2 = tasks_mod.run_refresh_background({})
        assert t1 != t2

    def test_thread_marked_daemon(self, tasks_mod):
        # The thread MUST be daemon=True; otherwise a uwsgi worker
        # restart would hang waiting for the auto_pipeline to finish.
        captured_kwargs = {}

        def capture_thread(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        with patch.object(tasks_mod.threading, "Thread", side_effect=capture_thread):
            tasks_mod.run_auto_pipeline_background({})
        assert captured_kwargs.get("daemon") is True


class TestRunUnderLock:
    """The shared thread body acquires the scheduler lock, publishes
    inflight, runs work, clears inflight, releases lock. Errors must
    NEVER escape the thread (they'd crash an arbitrary uwsgi worker)."""

    def test_lock_held_skips_work(self, tasks_mod, plugin_mod):
        target = MagicMock()
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value=False):
            with patch.object(plugin_mod, "_release_scheduler_lock") as release:
                tasks_mod._run_under_lock("refresh", "tid", target, {})
        target.assert_not_called()
        # If lock was held, we never acquired it -- so we must NOT
        # release it. Releasing someone else's lock is a correctness
        # bug.
        release.assert_not_called()

    def test_happy_path_acquires_publishes_runs_clears_releases(self, tasks_mod, plugin_mod):
        calls = []
        target = MagicMock(return_value={"status": "ok", "message": "done"})

        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value=True) as acquire:
            with patch.object(plugin_mod, "_release_scheduler_lock") as release:
                with patch.object(tasks_mod, "_set_inflight", side_effect=lambda *a, **k: calls.append("set")):
                    with patch.object(tasks_mod, "_clear_inflight", side_effect=lambda: calls.append("clear")):
                        # When target is called, record that too.
                        target.side_effect = lambda s: (calls.append("work"), {"status": "ok"})[1]
                        tasks_mod._run_under_lock("auto_pipeline", "tid", target, {"x": 1})
        acquire.assert_called_once()
        release.assert_called_once()
        assert calls == ["set", "work", "clear"]
        target.assert_called_once_with({"x": 1})

    def test_target_exception_still_clears_and_releases(self, tasks_mod, plugin_mod):
        target = MagicMock(side_effect=RuntimeError("boom"))
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value=True):
            with patch.object(plugin_mod, "_release_scheduler_lock") as release:
                with patch.object(tasks_mod, "_clear_inflight") as clear:
                    # Must not raise -- daemon threads that escape exceptions
                    # take down the uwsgi worker with them in some configs.
                    tasks_mod._run_under_lock("refresh", "tid", target, {})
        clear.assert_called_once()
        release.assert_called_once()


class TestHttpDispatchSurface:
    """The HTTP-facing action handlers (called from Plugin.run) must
    return a queued envelope with a task_id WITHOUT executing the
    underlying ~40s work synchronously. This is the entire point of
    #84 -- without it the action runs inline and the browser times out."""

    def test_auto_pipeline_async_returns_queued_envelope(self, plugin_mod):
        with patch.object(
            plugin_mod.tasks, "run_auto_pipeline_background", return_value="task-abc-123"
        ) as launcher:
            result = plugin_mod._action_auto_pipeline_async({"max_games": 10})
        launcher.assert_called_once_with({"max_games": 10})
        assert result["status"] == "queued"
        assert result["task_id"] == "task-abc-123"
        assert "task-abc-123" in result["message"]

    def test_refresh_async_returns_queued_envelope(self, plugin_mod):
        with patch.object(
            plugin_mod.tasks, "run_refresh_background", return_value="task-refresh-1"
        ) as launcher:
            result = plugin_mod._action_refresh_async({"enable_epl": True})
        launcher.assert_called_once_with({"enable_epl": True})
        assert result["status"] == "queued"
        assert result["task_id"] == "task-refresh-1"

    def test_plugin_run_routes_auto_pipeline_through_async(self, plugin_mod):
        # The bug is that Plugin.run("auto_pipeline") used to call the
        # sync helper; verify the dispatch now goes through the async
        # wrapper instead.
        instance = plugin_mod.Plugin.__new__(plugin_mod.Plugin)  # skip __init__
        with patch.object(
            plugin_mod.tasks, "run_auto_pipeline_background", return_value="tid"
        ):
            with patch.object(plugin_mod, "_action_auto_pipeline_sync") as sync:
                result = instance.run("auto_pipeline", {}, {"settings": {}})
        sync.assert_not_called()
        assert result["status"] == "queued"

    def test_plugin_run_routes_refresh_through_async(self, plugin_mod):
        instance = plugin_mod.Plugin.__new__(plugin_mod.Plugin)
        with patch.object(
            plugin_mod.tasks, "run_refresh_background", return_value="tid"
        ):
            with patch.object(plugin_mod, "_action_refresh") as sync:
                result = instance.run("refresh", {}, {"settings": {}})
        sync.assert_not_called()
        assert result["status"] == "queued"


class TestShowStatusSurfacesInflight:
    """show_status is the UI's only progress signal during a queued
    run. It must surface the inflight Redis key when present, both
    when cache.json is empty (first-run) and when it has games."""

    def test_empty_cache_shows_inflight_when_present(self, plugin_mod):
        inflight = {
            "kind": "auto_pipeline",
            "task_id": "0123456789abcdef",
            "phase": "refresh",
            "started_at": "2026-05-26T14:00:00+00:00",
            "pid": 1,
        }
        with patch.object(plugin_mod, "_read_cache", return_value={"games": []}):
            with patch.object(plugin_mod.tasks, "read_inflight", return_value=inflight):
                result = plugin_mod._action_show_status({})
        assert result["status"] == "ok"
        assert "in flight" in result["message"]
        assert "auto_pipeline" in result["message"]
        assert "refresh" in result["message"]
        assert "01234567" in result["message"]  # truncated task id

    def test_empty_cache_no_inflight_keeps_original_message(self, plugin_mod):
        with patch.object(plugin_mod, "_read_cache", return_value={"games": []}):
            with patch.object(plugin_mod.tasks, "read_inflight", return_value=None):
                result = plugin_mod._action_show_status({})
        assert result["status"] == "ok"
        assert "Cache empty" in result["message"]
        assert "in flight" not in result["message"]

    def test_populated_cache_with_inflight_prepends_progress_line(self, plugin_mod):
        cache = {
            "games": [
                {
                    "sport_prefix": "CFB",
                    "home": "X",
                    "away": "Y",
                    "rank_home": 1,
                    "rank_away": 2,
                    "score": 7.5,
                    "score_breakdown": {"rank": 5.0},
                    "kickoff_local": "Sat 7:00 PM",
                    "channel_name_current": "ESPN",
                }
            ],
            "refreshed_at": "2026-05-26T13:00:00+00:00",
            "summary": ["CFB: 1 game"],
        }
        inflight = {
            "kind": "auto_pipeline",
            "task_id": "abc",
            "phase": "apply",
            "started_at": "2026-05-26T14:00:00+00:00",
            "pid": 1,
        }
        with patch.object(plugin_mod, "_read_cache", return_value=cache):
            with patch.object(plugin_mod.tasks, "read_inflight", return_value=inflight):
                result = plugin_mod._action_show_status({})
        lines = result["message"].split("\n")
        # First line is the inflight marker, second is blank, then the
        # original cache header.
        assert "in flight" in lines[0]
        assert "apply" in lines[0]
        assert lines[1] == ""
        assert any(line.startswith("Refreshed:") for line in lines[2:])
