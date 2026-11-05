"""Heartbeat contracts and the parent-supervisor lifecycle, without live network."""
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import Mock

import pytest

from dispatcharr_ranked_matchups import health, _http
from .conftest import stub_requests_get


def test_push_overrides_copied_kuma_query_and_redacts_credentials(monkeypatch, caplog):
    seen = []
    def get(url, **kw):
        seen.append((url, kw))
        return SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None)
    stub_requests_get(monkeypatch, get)
    caplog.set_level("INFO")
    assert health.push("http://localhost/api/push/SECRET?status=up&msg=OK&ping=12&extra=yes",
                       ["timeout at https://provider.test/account?password=PRIVATE"], "")
    query = parse_qs(urlsplit(seen[0][0]).query)
    assert query == {"status": ["down"], "msg": ["timeout at [URL redacted]"], "extra": ["yes"]}
    assert seen[0][1]["timeout"] == 5
    assert seen[0][1]["allow_redirects"] is False
    assert "SECRET" not in caplog.text and "PRIVATE" not in caplog.text


@pytest.mark.parametrize("failure", [RuntimeError("http://secret/token"), None])
def test_push_failure_does_not_leak_url_or_raise(monkeypatch, caplog, failure):
    def get(*a, **kw):
        if failure:
            raise failure
        return SimpleNamespace(status_code=200, headers={"Content-Type": "application/json"},
                               raise_for_status=lambda: None, json=lambda: {"ok": False})
    monkeypatch.setattr(_http, "get_within_deadline", get)
    assert not health.push("http://localhost/api/push/SECRET", [], "healthy")
    assert "SECRET" not in caplog.text and "secret/token" not in caplog.text


@pytest.mark.parametrize("message", ["Authorization: Bearer PRIVATE", "authorization: Basic PRIVATE",
                                     "GET /api/push/PRIVATE?status=up", "/api/v1/json/PRIVATE/lookup",
                                     "password=PRIVATE", "apiKey: PRIVATE", 'password="PRIVATE"',
                                     "token='PRIVATE'", "api-key=PRIVATE", '"password": "PRIVATE WITH_SPACE"'])
def test_relative_credentials_are_absent_from_both_wire_and_log(monkeypatch, caplog, message):
    seen = []
    def get(url, **kw):
        seen.append(url)
        return SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None)
    stub_requests_get(monkeypatch, get)
    caplog.set_level("INFO")
    assert health.push("http://localhost/api/push/MONITOR_TOKEN", [message], "")
    assert "PRIVATE" not in parse_qs(urlsplit(seen[0]).query)["msg"][0]
    assert "WITH_SPACE" not in parse_qs(urlsplit(seen[0]).query)["msg"][0]
    assert "PRIVATE" not in caplog.text and "MONITOR_TOKEN" not in caplog.text


class Timer:
    def __init__(self, interval, callback):
        self.interval, self.callback = interval, callback
        self.cancelled = False
    def start(self):
        pass
    def cancel(self):
        self.cancelled = True


def test_slow_warning_then_final_up_cannot_be_overwritten_by_late_timer(monkeypatch):
    monkeypatch.setattr(health.threading, "Timer", Timer)
    monkeypatch.setattr(health, "pipeline_problems", lambda *a: [])
    pushes = Mock(return_value=True)
    monkeypatch.setattr(health, "push", pushes)
    watch = health.PipelineHealth({"health_push_url": "url"}, datetime.now(timezone.utc), 1500)
    assert watch.timer.interval == 1050
    watch.timer.callback()
    assert "70%" in pushes.call_args.args[1][0]
    watch.finish({"status": "ok"})
    assert pushes.call_args.args[1] == []
    assert watch.timer.cancelled
    watch.timer.callback()  # callback already queued when cancel() happened
    assert pushes.call_count == 2


def test_health_query_failure_is_down_and_preserves_pipeline_cause(monkeypatch):
    monkeypatch.setattr(health.threading, "Timer", Timer)
    monkeypatch.setattr(health, "pipeline_problems", Mock(side_effect=RuntimeError("credential")))
    pushes = Mock()
    monkeypatch.setattr(health, "push", pushes)
    watch = health.PipelineHealth({"health_push_url": "url"}, datetime.now(timezone.utc), 1500)
    watch.finish({"status": "error", "message": "scoring timed out"})
    assert pushes.call_args.args[1] == ["pipeline error: scoring timed out", "pipeline health check failed (RuntimeError)"]


@pytest.mark.parametrize("kind", ["auto_pipeline", "scheduled_auto_pipeline", "m3u_rematch"])
@pytest.mark.parametrize("crash", [False, True])
def test_supervisor_reports_every_pipeline_exit_and_releases_lock(tasks_mod, plugin, monkeypatch, kind, crash):
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda *a: "token")
    release = Mock()
    monkeypatch.setattr(plugin, "_release_scheduler_lock", release)
    monkeypatch.setattr(tasks_mod, "_set_inflight", Mock())
    monkeypatch.setattr(tasks_mod, "_clear_inflight", Mock())
    watch = Mock()
    start = Mock(return_value=watch)
    monkeypatch.setattr(health, "PipelineHealth", start)
    def target(settings):
        if crash:
            raise RuntimeError("forced failure")
        return {"status": "ok", "message": "fine"}
    assert tasks_mod._run_under_lock(kind, "test", target, {"health_push_url": "url"})
    assert start.call_count == 1
    assert watch.finish.call_args.args[0]["status"] == ("error" if crash else "ok")
    release.assert_called_once_with("token")


def test_no_heartbeat_for_skipped_runs_or_non_pipeline_actions(tasks_mod, plugin, monkeypatch):
    start = Mock()
    monkeypatch.setattr(health, "PipelineHealth", start)
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda *a: None)
    monkeypatch.setattr(plugin, "_run_lock_unavailable_reason", lambda: "busy")
    assert not tasks_mod._run_under_lock("auto_pipeline", "test", Mock(), {"health_push_url": "url"})
    start.assert_not_called()
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda *a: "token")
    monkeypatch.setattr(plugin, "_release_scheduler_lock", Mock())
    monkeypatch.setattr(tasks_mod, "_set_inflight", Mock())
    monkeypatch.setattr(tasks_mod, "_clear_inflight", Mock())
    for kind in ["refresh", "rematch"]:
        assert tasks_mod._run_under_lock(kind, "test", lambda _: {"status": "ok"}, {"health_push_url": "url"})
    start.assert_not_called()


def test_monitor_failure_cannot_strand_lock(tasks_mod, plugin, monkeypatch):
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda *a: "token")
    release = Mock()
    monkeypatch.setattr(plugin, "_release_scheduler_lock", release)
    monkeypatch.setattr(tasks_mod, "_set_inflight", Mock())
    monkeypatch.setattr(tasks_mod, "_clear_inflight", Mock())
    monkeypatch.setattr(health, "PipelineHealth", Mock(return_value=Mock(finish=Mock(side_effect=RuntimeError()))))
    assert tasks_mod._run_under_lock("auto_pipeline", "test", lambda _: {"status": "ok"}, {"health_push_url": "url"})
    release.assert_called_once_with("token")


def test_monitor_start_failure_does_not_skip_pipeline(tasks_mod, plugin, monkeypatch):
    monkeypatch.setattr(plugin, "_try_acquire_scheduler_lock", lambda *a: "token")
    monkeypatch.setattr(plugin, "_release_scheduler_lock", Mock())
    monkeypatch.setattr(tasks_mod, "_set_inflight", Mock())
    monkeypatch.setattr(tasks_mod, "_clear_inflight", Mock())
    monkeypatch.setattr(health, "PipelineHealth", Mock(side_effect=RuntimeError()))
    target = Mock(return_value={"status": "ok"})
    assert tasks_mod._run_under_lock("auto_pipeline", "test", target, {"health_push_url": "url"})
    target.assert_called_once()


@pytest.mark.parametrize("elected", [True, False])
def test_refresh_push_is_separate_and_elected(tasks_mod, monkeypatch, elected):
    redis = Mock()
    redis.set.return_value = elected
    monkeypatch.setattr(tasks_mod, "_redis", lambda: redis)
    monkeypatch.setattr(health, "refresh_problems", lambda: ["EPG 7: refresh error"])
    push = Mock()
    monkeypatch.setattr(health, "push", push)
    health.report_refresh({"health_push_url": "pipeline", "refresh_health_push_url": "feeds"})
    assert push.call_count == int(elected)
    if elected:
        assert push.call_args.args[:2] == ("feeds", ["EPG 7: refresh error"])


def test_refresh_query_failure_pushes_down_without_credentials(tasks_mod, monkeypatch):
    monkeypatch.setattr(tasks_mod, "_redis", lambda: Mock())
    monkeypatch.setattr(health, "refresh_problems", Mock(side_effect=RuntimeError("SECRET")))
    push = Mock()
    monkeypatch.setattr(health, "push", push)
    health.report_refresh({"refresh_health_push_url": "feeds"})
    assert push.call_args.args[1] == ["refresh health check failed (RuntimeError)"]


def test_missing_refresh_url_and_redis_do_not_push(tasks_mod, monkeypatch):
    monkeypatch.setattr(tasks_mod, "_redis", lambda: None)
    push = Mock()
    monkeypatch.setattr(health, "push", push)
    health.report_refresh({})
    health.report_refresh({"refresh_health_push_url": "feeds"})
    push.assert_not_called()
