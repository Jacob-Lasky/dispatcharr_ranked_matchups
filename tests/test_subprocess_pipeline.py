"""Tests for the subprocess isolation of the heavy pipeline (gevent fix).

The scoring is pure-Python Monte Carlo that holds the GIL; running it in
the gevent uwsgi worker froze the hub and hung login + live streams
(prod outage 2026-06-10). run_pipeline_subprocess forks a fresh
interpreter so the work runs in its own GIL. These tests pin:
  - the subprocess command shape, stdin payload, and sentinel parsing;
  - that every failure mode (non-zero exit, timeout, no result line)
    returns an error dict and NEVER raises (the caller is a daemon
    thread whose escape crashes a worker);
  - that the HTTP launchers route their supervised target through the
    subprocess for the right action.

The child interpreter itself (_pipeline_runner.py) needs Django and is
exercised live against the running container during deploy, not here."""

import importlib.util
import json
import os
import subprocess
import sys
from types import SimpleNamespace
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


@pytest.fixture(scope="module")
def tasks_mod():
    return _load_tasks_module()


def _completed(returncode=0, stdout="", stderr=""):
    """Stand-in for subprocess.CompletedProcess (only the attrs we read)."""
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


class TestRunPipelineSubprocess:
    def test_happy_path_returns_parsed_result(self, tasks_mod):
        result = {"status": "ok", "message": "refresh: 12 | apply: 12"}
        stdout = "some django.setup() noise\n" + tasks_mod._RESULT_SENTINEL + json.dumps(result) + "\n"
        with patch.object(tasks_mod, "_python_executable", return_value="/venv/bin/python"):
            with patch.object(tasks_mod.subprocess, "run", return_value=_completed(0, stdout)) as run:
                out = tasks_mod.run_pipeline_subprocess("auto_pipeline", {"max_games": 5})
        assert out == result
        # Command shape: the resolved interpreter + the runner in the package dir.
        (cmd,), kwargs = run.call_args
        assert cmd[0] == "/venv/bin/python"
        assert cmd[1].endswith("_pipeline_runner.py")
        # Action + settings handed off as JSON on stdin.
        assert json.loads(kwargs["input"]) == {"action": "auto_pipeline", "settings": {"max_games": 5}}
        # Bounded so a hung child can't outlive the scheduler lock.
        assert kwargs["timeout"] == tasks_mod._SUBPROCESS_TIMEOUT_SECONDS
        # Parent sys.path forwarded so the child can import `dispatcharr`.
        assert "PYTHONPATH" in kwargs["env"]
        assert any(p and p in kwargs["env"]["PYTHONPATH"] for p in sys.path)

    def test_python_executable_never_returns_uwsgi_binary(self, tasks_mod):
        # The whole bug: under uWSGI sys.executable is the uwsgi binary, which
        # makes the child try to parse the script as a uwsgi config (rc=1).
        with patch.object(tasks_mod.sys, "executable", "/dispatcharrpy/bin/uwsgi"):
            # sys.prefix/bin/python exists in the test env, so we get that...
            exe = tasks_mod._python_executable()
            assert "uwsgi" not in os.path.basename(exe)
            # ...and with no venv python present, we must still avoid uwsgi.
            with patch.object(tasks_mod.os.path, "exists", return_value=False):
                with patch.object(tasks_mod.shutil, "which", return_value="/usr/bin/python3"):
                    assert tasks_mod._python_executable() == "/usr/bin/python3"

    def test_sentinel_ignores_leading_stdout_noise(self, tasks_mod):
        # A stray print before the sentinel must not corrupt the result.
        result = {"status": "ok"}
        stdout = "WARNING blah\nDEBUG something\n" + tasks_mod._RESULT_SENTINEL + json.dumps(result)
        with patch.object(tasks_mod.subprocess, "run", return_value=_completed(0, stdout)):
            out = tasks_mod.run_pipeline_subprocess("refresh", {})
        assert out == result

    def test_nonzero_exit_returns_error_dict(self, tasks_mod):
        with patch.object(tasks_mod.subprocess, "run", return_value=_completed(1, "", "boom")):
            out = tasks_mod.run_pipeline_subprocess("refresh", {})
        assert out["status"] == "error"
        assert "rc=1" in out["message"]

    def test_timeout_returns_error_dict(self, tasks_mod):
        with patch.object(
            tasks_mod.subprocess, "run",
            side_effect=subprocess.TimeoutExpired(cmd="x", timeout=1),
        ):
            out = tasks_mod.run_pipeline_subprocess("auto_pipeline", {})
        assert out["status"] == "error"
        assert "timed out" in out["message"]

    def test_no_sentinel_line_returns_error_dict(self, tasks_mod):
        # Child exited 0 but emitted nothing parseable -- treat as failure,
        # do NOT pretend success.
        with patch.object(tasks_mod.subprocess, "run", return_value=_completed(0, "just logs\n")):
            out = tasks_mod.run_pipeline_subprocess("refresh", {})
        assert out["status"] == "error"
        assert "no result" in out["message"]

    def test_unexpected_exception_never_escapes(self, tasks_mod):
        # The caller is a daemon thread; an escape crashes a uwsgi worker.
        with patch.object(tasks_mod.subprocess, "run", side_effect=OSError("fork failed")):
            out = tasks_mod.run_pipeline_subprocess("refresh", {})
        assert out["status"] == "error"


class TestLaunchersRouteThroughSubprocess:
    """The HTTP launchers must hand _run_under_lock a target that runs the
    work in the subprocess for the correct action -- NOT the in-process
    action function (which would freeze the gevent hub)."""

    def _run_and_capture(self, tasks_mod, launcher, settings):
        """Patch run_pipeline_subprocess BEFORE the launcher runs so the
        target partial binds the mock (partial captures the function at
        build time, so patching after the fact would be a no-op), then
        capture the args handed to threading.Thread."""
        captured = {}

        def capture_thread(*args, **kwargs):
            captured["args"] = kwargs.get("args")
            captured["daemon"] = kwargs.get("daemon")
            return MagicMock()

        rps = MagicMock(return_value={"status": "ok"})
        with patch.object(tasks_mod, "run_pipeline_subprocess", rps):
            with patch.object(tasks_mod.threading, "Thread", side_effect=capture_thread):
                launcher(settings)
            kind, _task_id, target, passed_settings = captured["args"]
            target_result = target(settings)
        return captured, rps, kind, passed_settings, target_result

    def test_auto_pipeline_target_routes_to_subprocess(self, tasks_mod):
        settings = {"max_games": 9}
        captured, rps, kind, passed_settings, target_result = self._run_and_capture(
            tasks_mod, tasks_mod.run_auto_pipeline_background, settings
        )
        assert kind == "auto_pipeline"
        assert passed_settings == settings
        assert captured["daemon"] is True
        assert target_result == {"status": "ok"}
        rps.assert_called_once_with(tasks_mod.ACTION_AUTO_PIPELINE, settings)

    def test_refresh_target_routes_to_subprocess(self, tasks_mod):
        settings = {"enable_epl": True}
        _captured, rps, kind, passed_settings, _result = self._run_and_capture(
            tasks_mod, tasks_mod.run_refresh_background, settings
        )
        assert kind == "refresh"
        assert passed_settings == settings
        rps.assert_called_once_with(tasks_mod.ACTION_REFRESH, settings)

    def test_scheduled_pipeline_routes_through_lock_and_subprocess(self, tasks_mod):
        # The scheduler tick is the ORIGINAL outage path. Guard that it runs
        # under the cross-worker lock AND out of process -- i.e. it must NOT
        # call the in-process _action_auto_pipeline_sync directly.
        settings = {"max_games": 3}
        with patch.object(tasks_mod, "run_pipeline_subprocess", return_value={"status": "ok"}) as rps:
            with patch.object(tasks_mod, "_run_under_lock") as rul:
                tasks_mod.run_scheduled_pipeline(settings)
            (kind, task_id, target, passed_settings), _ = rul.call_args
            # Distinguishable scheduled-run identity for show_status.
            assert kind == "scheduled_auto_pipeline"
            assert task_id.startswith("scheduler-")
            assert passed_settings == settings
            # The supervised target must run the auto_pipeline in the subprocess.
            target(settings)
        rps.assert_called_once_with(tasks_mod.ACTION_AUTO_PIPELINE, settings)

    def test_action_constants_match_manifest_ids(self, tasks_mod):
        # The subprocess action selectors must equal the wire/manifest action
        # ids the UI and plugin.run dispatch on, or a scheduled/HTTP run would
        # hand the runner a name it doesn't know.
        assert tasks_mod.ACTION_REFRESH == "refresh"
        assert tasks_mod.ACTION_AUTO_PIPELINE == "auto_pipeline"
