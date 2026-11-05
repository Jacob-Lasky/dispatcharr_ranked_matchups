"""A failed scoring child must still pick up feeds for the scored slate (#327)."""
import json
import subprocess
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("failure", ["timeout", "crash", "error"])
def test_failed_pipeline_rematches_in_fresh_child(tasks_mod, monkeypatch, failure):
    calls = []

    def child(argv, input, **kw):
        action = json.loads(input)["action"]
        calls.append(action)
        if len(calls) == 1:
            if failure == "timeout":
                raise subprocess.TimeoutExpired(argv, 1500)
            if failure == "crash":
                return SimpleNamespace(returncode=1, stderr="", stdout="")
            result = {"status": "error", "message": "scoring failed"}
        else:
            result = {"status": "ok", "message": "late feed attached"}
        return SimpleNamespace(returncode=0, stderr="", stdout=tasks_mod._RESULT_SENTINEL + json.dumps(result))

    monkeypatch.setattr(tasks_mod.subprocess, "run", child)
    monkeypatch.setattr(tasks_mod, "_update_inflight_phase", lambda _phase: None)
    result = tasks_mod.run_pipeline_subprocess("auto_pipeline", {})
    assert calls == ["auto_pipeline", "rematch"]
    # Scoring is still broken; recovery must not hide it from status/monitoring.
    assert result["status"] == "error"
    assert result["matching_recovery"]["status"] == "ok"
    assert "late feed attached" in result["message"]


def test_recovery_failure_is_bounded(tasks_mod, monkeypatch):
    calls = []
    def child(action, settings):
        calls.append(action)
        return {"status": "error", "message": "failed"}
    monkeypatch.setattr(tasks_mod, "_run_pipeline_child", child)
    monkeypatch.setattr(tasks_mod, "_update_inflight_phase", lambda _phase: None)
    result = tasks_mod.run_pipeline_subprocess("auto_pipeline", {})
    assert calls == ["auto_pipeline", "rematch"]
    assert result["matching_recovery"]["status"] == "error"
