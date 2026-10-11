"""The run lock fails closed for every caller (#199) and says who holds it (#239).

#199: refresh used to take the lock FAIL-OPEN when Redis was unreachable, on
the theory that it is non-destructive. It publishes cache.json, though, and
cache.json is what apply and reap delete by, so an unlocked refresh could swap
the cache under a reap that was already running.

#239: the lock's value was a bare uuid, so a 16:00 scheduled run that waited
seven minutes could only log "another refresh / auto_pipeline is in flight, or
Redis is unreachable". The value is now a JSON holder record, a contended
attempt logs it, and the busy toast, the skip note and Show current state name
it.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


class FakeRedis:
    """Just enough of the Redis client for the lock: SET NX EX, GET, and the
    compare-and-delete EVAL. `down=True` makes every call raise, which is what
    an unreachable Redis looks like to redis-py."""

    def __init__(self, store=None, down=False):
        self.store = {} if store is None else store
        self.down = down

    def _check(self):
        if self.down:
            raise ConnectionError("Error 111 connecting to localhost:6379. Connection refused.")

    def set(self, key, value, nx=False, ex=None):
        self._check()
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def get(self, key):
        self._check()
        return self.store.get(key)

    def delete(self, key):
        self._check()
        return 1 if self.store.pop(key, None) is not None else 0

    def eval(self, script, numkeys, key, arg):
        self._check()
        if self.store.get(key) == arg:
            del self.store[key]
            return 1
        return 0


@pytest.fixture
def redis(monkeypatch):
    """Install a fake core.utils.RedisClient, reachable by default."""
    client = FakeRedis()
    fake_core = MagicMock()
    fake_core.RedisClient.get_client.return_value = client
    monkeypatch.setitem(sys.modules, "core", MagicMock())
    monkeypatch.setitem(sys.modules, "core.utils", fake_core)
    return client


def _record(raw):
    """The lock value as a holder record, or None, so a bare token fails the
    test as an assertion rather than as a JSONDecodeError."""
    try:
        rec = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return rec if isinstance(rec, dict) else None


def _status(plugin_mod, last_run=None):
    with patch.object(plugin_mod, "_read_cache", return_value={"games": [], "summary": []}):
        with patch.object(plugin_mod.tasks, "read_inflight", return_value=None):
            with patch.object(plugin_mod.tasks, "read_last_run", return_value=last_run):
                return plugin_mod._action_show_status({"dry_run": False})["message"]


class TestRefreshFailsClosed:
    """#199 witness: the refresh path itself, not the acquire helper."""

    def test_refresh_does_not_run_when_redis_is_unreachable(self, tasks_mod, plugin_mod, redis):
        redis.down = True
        target = MagicMock(return_value={"status": "ok", "message": "published cache.json"})
        ran = tasks_mod._run_under_lock("refresh", "t-1", target, {})
        assert ran is False
        target.assert_not_called()  # the old fail-open path ran it and published

    def test_the_skip_is_recorded_with_the_reason(self, tasks_mod, plugin_mod, redis):
        redis.down = True
        tasks_mod._run_under_lock("refresh", "t-1", MagicMock(), {})
        rec = tasks_mod.read_last_run() or {}
        assert "status" not in rec, "the refresh ran and recorded a verdict"
        assert rec.get("skipped_kind") == "refresh"
        assert rec.get("skipped_reason") == "Redis is unreachable, so the run lock cannot be taken"

    def test_refresh_cannot_publish_under_a_running_reap(self, tasks_mod, plugin_mod, redis):
        """The race #199 describes: a reap holds the lock, Redis drops, a
        refresh starts. It must stay out until it can see the lock."""
        reap_token = plugin_mod._try_acquire_scheduler_lock("reap", "reaper-7")
        assert reap_token
        redis.down = True
        target = MagicMock()
        assert tasks_mod._run_under_lock("refresh", "t-2", target, {}) is False
        target.assert_not_called()
        redis.down = False
        # Still the reap's lock, untouched by the refused refresh.
        assert redis.store[plugin_mod._SCHEDULER_LOCK_KEY] == reap_token

    def test_show_status_says_redis_is_unreachable(self, plugin_mod, redis):
        redis.down = True
        last = _status(plugin_mod).splitlines()[-1]
        assert last.startswith("WARNING: Redis is unreachable, so no refresh, apply or reap can run")


    def test_a_client_that_never_connected_counts_as_unreachable(self, plugin_mod, monkeypatch):
        # Dispatcharr's RedisClient.get_client returns None, not raises, once
        # its connect retries are exhausted.
        fake_core = MagicMock()
        fake_core.RedisClient.get_client.return_value = None
        monkeypatch.setitem(sys.modules, "core", MagicMock())
        monkeypatch.setitem(sys.modules, "core.utils", fake_core)
        assert plugin_mod._read_run_lock()[0] == "unreachable"
        assert plugin_mod._try_acquire_scheduler_lock("refresh") is None
        assert "Redis is unreachable" in _status(plugin_mod).splitlines()[-1]


class TestHolderRecord:
    def test_lock_value_names_the_holder(self, plugin_mod, redis, monkeypatch):
        monkeypatch.setattr(plugin_mod.os, "getpid", lambda: 411)
        token = plugin_mod._try_acquire_scheduler_lock("scheduled_auto_pipeline", "scheduler-411")
        rec = _record(redis.store[plugin_mod._SCHEDULER_LOCK_KEY])
        assert rec is not None, "the lock value is a bare token, not a holder record"
        assert rec["kind"] == "scheduled_auto_pipeline"
        assert rec["task_id"] == "scheduler-411"
        assert rec["pid"] == 411
        assert rec["process"]
        assert rec["version"] == plugin_mod.Plugin.version
        assert datetime.fromisoformat(rec["acquired_at"]).tzinfo is not None
        assert len(rec["token"]) == 32
        assert token == redis.store[plugin_mod._SCHEDULER_LOCK_KEY]

    def test_two_acquires_still_get_distinct_values(self, plugin_mod, redis):
        a = plugin_mod._try_acquire_scheduler_lock("reap", "r")
        plugin_mod._release_scheduler_lock(a)
        b = plugin_mod._try_acquire_scheduler_lock("reap", "r")
        assert a != b
        ra, rb = _record(a), _record(b)
        assert ra is not None and rb is not None, "lock values are not holder records"
        assert ra["token"] != rb["token"]

    def test_contended_attempt_logs_the_holder(self, plugin_mod, redis, caplog):
        redis.store[plugin_mod._SCHEDULER_LOCK_KEY] = json.dumps({
            "token": "f" * 32, "kind": "reap", "task_id": "reaper-403", "pid": 403,
            "process": "uwsgi worker 3", "version": "9.9.9",
            "acquired_at": "2026-10-10T19:59:30+00:00",
        })
        caplog.set_level(logging.INFO)
        assert plugin_mod._try_acquire_scheduler_lock("scheduled_auto_pipeline", "scheduler-404") is None
        lines = [r.getMessage() for r in caplog.records if "run lock wanted by" in r.getMessage()]
        assert len(lines) == 1, "a contended attempt must log the holder exactly once"
        line = lines[0]
        assert "scheduled_auto_pipeline (task scheduler-404" in line
        assert "held by Reap finished games, task reaper-403, pid 403 (uwsgi worker 3), plugin 9.9.9" in line

    def test_holder_age_is_reported(self, plugin_mod):
        raw = json.dumps({"kind": "auto_pipeline", "task_id": "abc", "pid": 7,
                          "process": "celery -A dispatcharr worker",
                          "acquired_at": "2026-10-10T20:00:00+00:00"})
        now = datetime(2026, 10, 10, 20, 6, 12, tzinfo=timezone.utc)
        assert plugin_mod._describe_lock_holder(raw, now) == (
            "Refresh + apply, task abc, pid 7 (celery -A dispatcharr worker), held 6m 12s"
        )

    @pytest.mark.parametrize("raw", [
        '{"acquired_at": 7}', '{"kind": ["x"], "pid": {"a": 1}}', '[1, 2]', b"\xff\xfe", "null",
    ])
    def test_a_malformed_holder_never_raises(self, plugin_mod, raw):
        # Written by whichever version took the lock; read on a daemon
        # thread's contended path, where an escape kills the thread before
        # it records the skip.
        assert isinstance(plugin_mod._describe_lock_holder(raw), str)

    def test_contention_with_a_malformed_holder_still_returns_none(self, plugin_mod, redis):
        redis.store[plugin_mod._SCHEDULER_LOCK_KEY] = '{"acquired_at": 7}'
        assert plugin_mod._try_acquire_scheduler_lock("auto_pipeline", "t") is None
        assert plugin_mod._run_lock_unavailable_reason().startswith("the run lock is held by")

    def test_a_bare_token_is_named_as_old_code(self, plugin_mod):
        # A process the reload did not reach still writes the old bare uuid.
        text = plugin_mod._describe_lock_holder("0123456789abcdef0123456789abcdef")
        assert text.startswith("a plugin version from before holder records")

    def test_release_logs_who_released_and_after_how_long(self, plugin_mod, redis, caplog):
        token = plugin_mod._try_acquire_scheduler_lock("reap", "reaper-9")
        caplog.set_level(logging.INFO)
        plugin_mod._release_scheduler_lock(token)
        assert plugin_mod._SCHEDULER_LOCK_KEY not in redis.store
        assert any("run lock released by reap (task reaper-9) after" in r.getMessage()
                   for r in caplog.records)

    def test_taking_the_lock_is_logged(self, plugin_mod, redis, caplog):
        caplog.set_level(logging.INFO)
        plugin_mod._try_acquire_scheduler_lock("apply", "apply")
        assert any("run lock taken by apply (task apply" in r.getMessage() for r in caplog.records)


class TestHolderSurfaces:
    HOLDER = json.dumps({"token": "e" * 32, "kind": "scheduled_auto_pipeline",
                         "task_id": "scheduler-411", "pid": 411,
                         "process": "celery --quiet -A dispatcharr worker -Q dvr",
                         "version": "1.33.2", "acquired_at": "2026-10-10T20:00:00+00:00"})

    def test_busy_apply_toast_names_the_holder(self, plugin_mod, redis, monkeypatch):
        redis.store[plugin_mod._SCHEDULER_LOCK_KEY] = self.HOLDER
        child = MagicMock()
        monkeypatch.setattr(plugin_mod.tasks, "run_pipeline_subprocess", child)
        out = plugin_mod._action_apply_locked({})
        assert out["status"] == "busy"
        assert "held by Scheduled refresh + apply, task scheduler-411, pid 411" in out["message"]
        assert "worker -Q dvr" in out["message"]
        child.assert_not_called()

    def test_show_status_names_the_holder(self, plugin_mod, redis):
        redis.store[plugin_mod._SCHEDULER_LOCK_KEY] = self.HOLDER
        msg = _status(plugin_mod)
        assert "Run lock: held by Scheduled refresh + apply, task scheduler-411, pid 411" in msg

    def test_show_status_is_quiet_when_the_lock_is_free(self, plugin_mod, redis):
        assert "Run lock" not in _status(plugin_mod)
        assert "Redis is unreachable" not in _status(plugin_mod)

    def test_skip_note_carries_the_recorded_reason(self, plugin_mod):
        rec = {"skipped_kind": "refresh", "skipped_at": "2026-10-10T20:01:00+00:00",
               "skipped_reason": "the run lock is held by Reap finished games, pid 403"}
        last = _status(plugin_mod, rec).splitlines()[-1]
        assert last.startswith("NOTE: Refresh requested")
        assert last.endswith("did not run: the run lock is held by Reap finished games, pid 403.")

    def test_skipped_run_records_the_holder(self, tasks_mod, plugin_mod, redis):
        redis.store[plugin_mod._SCHEDULER_LOCK_KEY] = self.HOLDER
        assert tasks_mod._run_under_lock("auto_pipeline", "t-3", MagicMock(), {}) is False
        reason = tasks_mod.read_last_run()["skipped_reason"]
        assert reason.startswith("the run lock is held by Scheduled refresh + apply, task scheduler-411")

    def test_reaper_and_button_reaps_are_told_apart(self, plugin_mod, redis, monkeypatch):
        seen = []

        def child(action, settings):
            seen.append((_record(redis.store[plugin_mod._SCHEDULER_LOCK_KEY]) or {}).get("task_id"))
            return {"status": "ok"}

        monkeypatch.setattr(plugin_mod.tasks, "run_pipeline_subprocess", child)
        plugin_mod._action_reap_locked({}, task_id="reaper-12")
        plugin_mod._action_reap_locked({})
        assert seen == ["reaper-12", "reap_now"]

    def test_reaper_loop_passes_its_task_id(self, plugin_mod):
        import inspect
        src = inspect.getsource(plugin_mod._reaper_loop)
        assert '_action_reap_locked(settings, task_id=f"reaper-{os.getpid()}")' in src


class TestKindsHaveLabels:
    def test_every_lock_kind_in_the_source_has_a_label(self, plugin_mod, tasks_mod):
        """The holder record is described through RUN_KIND_LABELS, so a kind
        with no entry reaches a toast as its raw id. Scans the real call sites
        rather than a list kept in this test."""
        import inspect
        import re
        src = inspect.getsource(plugin_mod) + inspect.getsource(tasks_mod)
        kinds = set(re.findall(r'_try_acquire_scheduler_lock\(\s*"(\w+)"', src))
        kinds |= set(re.findall(r'_run_under_lock,\s*args=\(\s*"(\w+)"', src))
        kinds |= set(re.findall(r'_run_under_lock\(\s*"(\w+)"', src))
        assert {"apply", "reap", "auto_pipeline", "refresh",
                "scheduled_auto_pipeline", "m3u_rematch"} <= kinds, kinds  # the scan works
        assert kinds <= set(tasks_mod.RUN_KIND_LABELS), kinds - set(tasks_mod.RUN_KIND_LABELS)


def test_parse_iso_utc_returns_none_for_a_non_string(plugin_mod):
    # Its docstring promised None "on any parse failure"; a number raised
    # AttributeError from .replace instead.
    from dispatcharr_ranked_matchups._util import parse_iso_utc
    assert parse_iso_utc(7) is None
    assert parse_iso_utc(["2026-10-10"]) is None


class TestProcessLabel:
    def test_uwsgi_worker_is_named_by_worker_id(self, plugin_mod, monkeypatch):
        monkeypatch.setitem(sys.modules, "uwsgi", MagicMock(worker_id=lambda: 3))
        assert plugin_mod._process_label() == "uwsgi worker 3"

    def test_other_processes_are_named_by_argv_without_long_arguments(self, plugin_mod, monkeypatch):
        monkeypatch.setitem(sys.modules, "uwsgi", None)  # import uwsgi -> ImportError
        script = "from apps.x import y; TOKEN='" + "s" * 60 + "'"
        monkeypatch.setattr(plugin_mod.sys, "argv",
                            ["/dispatcharrpy/bin/python", "manage.py", "shell", "-c", script])
        assert plugin_mod._process_label() == "python manage.py shell -c"
        monkeypatch.setattr(plugin_mod.sys, "argv",
                            ["/dispatcharrpy/bin/celery", "--quiet", "-A", "dispatcharr", "worker",
                             "-Q", "dvr", "-n", "dvr@%h", "--pool=threads"])
        assert plugin_mod._process_label() == "celery --quiet -A dispatcharr worker -Q dvr -n dvr@%h"

    def test_short_secrets_are_not_copied(self, plugin_mod, monkeypatch):
        monkeypatch.setitem(sys.modules, "uwsgi", None)
        monkeypatch.setattr(plugin_mod.sys, "argv",
                            ["/x/bin/python", "probe.py", "--token=abc123", "--password", "hunter2",
                             "--api-key", "k1", "run"])
        label = plugin_mod._process_label()
        assert label == "python probe.py run"
        for secret in ("abc123", "hunter2", "k1"):
            assert secret not in label
