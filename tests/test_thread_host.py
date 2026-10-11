"""Which processes run the scheduler and reaper, and noticing when none does (#252).

Measured on Tower 2026-10-10/11: Dispatcharr's celery.py runs plugin discovery
in worker_process_init, so every Celery prefork child and the thread worker
built a Plugin and started both threads, and so did daphne. Those copies took
part in every scheduled slot on code the plugin reload never reaches, and a
Celery copy that won a slot left no log line (Celery drops this plugin's INFO).

Since #252 only processes that serve plugin requests (uwsgi workers) start
them. A detection that is wrong everywhere would leave no scheduler at all,
so `_thread_host` starts the threads in any process it cannot place, and
`_scheduler_staleness` reports a slot nobody claimed.
"""

import json
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

UTC = timezone.utc
ROOT = Path(__file__).resolve().parent.parent

# Real argv of the processes on Tower, read from /proc/<pid>/cmdline
# 2026-10-11 (sys.argv drops the interpreter for a script).
CELERY_PREFORK = ["/dispatcharrpy/bin/celery", "--quiet", "-A", "dispatcharr", "worker",
                  "-Q", "celery", "-n", "default@%h", "--autoscale=6,1"]
CELERY_THREADS = ["/dispatcharrpy/bin/celery", "--quiet", "-A", "dispatcharr", "worker",
                  "-Q", "dvr", "-n", "dvr@%h", "--pool=threads", "--concurrency=20"]
DAPHNE = ["/dispatcharrpy/bin/daphne", "-b", "0.0.0.0", "-p", "8001",
          "dispatcharr.asgi:application"]


@pytest.fixture
def no_uwsgi(monkeypatch):
    monkeypatch.setitem(sys.modules, "uwsgi", None)  # import uwsgi -> ImportError


def _as_uwsgi_worker(monkeypatch, wid):
    monkeypatch.setitem(sys.modules, "uwsgi", MagicMock(worker_id=lambda: wid))


class TestThreadHost:
    def test_a_uwsgi_worker_runs_them(self, plugin, monkeypatch):
        _as_uwsgi_worker(monkeypatch, 2)
        monkeypatch.setattr(plugin.sys, "argv", ["uwsgi"])
        assert plugin._thread_host() == (True, True)

    @pytest.mark.parametrize("argv", [
        CELERY_PREFORK,
        CELERY_THREADS,
        DAPHNE,
        ["/dispatcharrpy/lib/python3.13/site-packages/celery/__main__.py", "-A", "dispatcharr", "worker"],
        ["manage.py", "shell", "-c", "print(1)"],
        ["manage.py", "migrate"],
    ], ids=["celery-prefork", "celery-threads", "daphne", "python-m-celery",
            "manage-shell", "manage-migrate"])
    def test_non_serving_processes_do_not(self, plugin, monkeypatch, no_uwsgi, argv):
        monkeypatch.setattr(plugin.sys, "argv", argv)
        assert plugin._thread_host() == (False, True)

    def test_uwsgi_variables_in_the_environment_do_not_make_a_worker(
            self, plugin, monkeypatch, no_uwsgi):
        # uwsgi exports these to its attach-daemons (Celery, daphne), not to
        # its workers: measured in /proc/<pid>/environ on Tower. An env test
        # would therefore pick exactly the wrong processes.
        monkeypatch.setenv("UWSGI_ORIGINAL_PROC_NAME", "/dispatcharrpy/bin/uwsgi")
        monkeypatch.setenv("UWSGI_RELOADS", "0")
        monkeypatch.setattr(plugin.sys, "argv", CELERY_PREFORK)
        assert plugin._thread_host() == (False, True)

    @pytest.mark.parametrize("argv", [["/x/bin/gunicorn", "dispatcharr.wsgi:application"],
                                      ["manage.py", "runserver"]])
    def test_other_web_servers_run_them(self, plugin, monkeypatch, no_uwsgi, argv):
        monkeypatch.setattr(plugin.sys, "argv", argv)
        assert plugin._thread_host() == (True, True)

    @pytest.mark.parametrize("argv", [["/tmp/probe.py"], ["-c"], [], ["/x/bin/granian"]])
    def test_an_unplaceable_process_starts_them_and_says_so(self, plugin, monkeypatch,
                                                           no_uwsgi, argv):
        # Fail-safe direction: a wrong "no" leaves no scheduler anywhere.
        monkeypatch.setattr(plugin.sys, "argv", argv)
        assert plugin._thread_host() == (True, False)

    def test_uwsgi_without_a_worker_id_starts_them_and_says_so(self, plugin, monkeypatch):
        _as_uwsgi_worker(monkeypatch, 0)
        assert plugin._thread_host() == (True, False)

        def broken():
            raise RuntimeError("no worker id")

        monkeypatch.setitem(sys.modules, "uwsgi", MagicMock(worker_id=broken))
        assert plugin._uwsgi_worker_id() == 0
        assert plugin._thread_host() == (True, False)


@pytest.fixture
def registries(plugin):
    reg, rreg = plugin._scheduler_registry(), plugin._reaper_registry()
    attrs = ("thread", "stop_event", "retiring", "stale_warned", "host_warned")
    saved = [(r, a, getattr(r, a, None)) for r in (reg, rreg) for a in attrs]
    for r, a, _ in saved:
        setattr(r, a, None)
    yield reg, rreg
    for r, a, v in saved:
        setattr(r, a, v)


@pytest.fixture
def spawned(plugin, monkeypatch):
    names = []

    class Tracking(threading.Thread):
        def __init__(self, *a, **kw):
            names.append(kw.get("name"))
            super().__init__(*a, **kw)

        def start(self):  # never run a real loop in a test
            pass

    monkeypatch.setattr(plugin.threading, "Thread", Tracking)
    monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: None)
    monkeypatch.delenv(plugin.tasks.PIPELINE_CHILD_ENV, raising=False)
    return names


class TestPluginInit:
    @pytest.mark.parametrize("argv", [CELERY_PREFORK, CELERY_THREADS, DAPHNE,
                                      ["manage.py", "shell"]],
                             ids=["celery-prefork", "celery-threads", "daphne", "manage-shell"])
    def test_non_serving_process_starts_no_thread(self, plugin, monkeypatch, no_uwsgi,
                                                  registries, spawned, argv, caplog):
        monkeypatch.setattr(plugin.sys, "argv", argv)
        with caplog.at_level("INFO"):
            plugin.Plugin()
        assert spawned == []
        reg, rreg = registries
        assert reg.thread is None and rreg.thread is None
        assert any("no scheduler or reaper in this process" in r.getMessage()
                   for r in caplog.records)

    def test_uwsgi_worker_starts_both(self, plugin, monkeypatch, registries, spawned, caplog):
        _as_uwsgi_worker(monkeypatch, 3)
        with caplog.at_level("INFO"):
            plugin.Plugin()
        assert sorted(spawned) == ["ranked_matchups-reaper", "ranked_matchups-scheduler"]
        started = [r.getMessage() for r in caplog.records if "thread started" in r.getMessage()]
        assert len(started) == 2 and all("uwsgi worker 3" in m for m in started)
        assert not [r for r in caplog.records if r.levelname == "WARNING"]

    def test_reload_restarts_them_in_the_reloading_worker(self, plugin, monkeypatch,
                                                          registries, spawned):
        # The reload endpoint calls stop() and then discovers again in the
        # worker serving it; that worker must come back with both threads.
        _as_uwsgi_worker(monkeypatch, 1)
        plugin.Plugin()
        reg, rreg = registries

        class Running:
            """Alive until its stop event is set, like a parked loop."""
            def __init__(self, ev):
                self.ev = ev

            def is_alive(self):
                return not self.ev.is_set()

            def join(self, timeout=None):
                pass

        for r in (reg, rreg):
            r.thread = Running(r.stop_event)
        spawned.clear()
        plugin.Plugin()  # a routine discovery: both still running, none started
        assert spawned == []
        plugin.Plugin.__new__(plugin.Plugin).stop()
        assert reg.thread is None and rreg.thread is None
        plugin.Plugin()
        assert sorted(spawned) == ["ranked_matchups-reaper", "ranked_matchups-scheduler"]

    def test_reload_while_a_run_is_in_progress_still_leaves_a_scheduler(
            self, plugin, monkeypatch, registries, spawned):
        # The reloading worker's scheduler is inside a 25-minute pipeline, so
        # it outlives stop()'s 5s join. It is signaled and will exit after the
        # run; the rebuilt Plugin must not count it as this worker's scheduler.
        _as_uwsgi_worker(monkeypatch, 1)
        reg, rreg = registries

        class Busy:
            """Alive whatever its event says: still inside the run."""
            def is_alive(self):
                return True

            def join(self, timeout=None):
                pass

        old = {}
        for r in (reg, rreg):
            r.thread, r.stop_event = Busy(), threading.Event()
            old[id(r)] = r.thread
        plugin.Plugin.__new__(plugin.Plugin).stop()
        assert reg.thread is old[id(reg)]  # kept: a second stop() still finds it
        assert reg.stop_event.is_set()
        plugin.Plugin()
        assert sorted(spawned) == ["ranked_matchups-reaper", "ranked_matchups-scheduler"]
        assert reg.thread is not old[id(reg)] and rreg.thread is not old[id(rreg)]
        assert not reg.stop_event.is_set() and not rreg.stop_event.is_set()

    def test_threads_stuck_after_stop_are_bounded_across_reloads(
            self, plugin, monkeypatch, registries, spawned, caplog):
        # A thread that never finishes its work in hand (a reaper blocked on a
        # row lock holds a DB connection) must not be replaced on EVERY
        # reload: that is the #82 connection leak with a new trigger.
        _as_uwsgi_worker(monkeypatch, 1)
        monkeypatch.setattr(plugin.threading, "Thread",
                            lambda *a, **k: spawned.append(k.get("name")) or Stuck())
        reg, rreg = registries

        class Stuck:
            """Started, and never exits, signaled or not."""
            def start(self):
                pass

            def is_alive(self):
                return True

            def join(self, timeout=None):
                pass

        plugin.Plugin()
        for _ in range(6):  # six reloads: stop(), then rebuild in this worker
            plugin.Plugin.__new__(plugin.Plugin).stop()
            plugin.Plugin()
        cap = plugin._MAX_RETIRING_THREADS
        for name in ("ranked_matchups-scheduler", "ranked_matchups-reaper"):
            assert spawned.count(name) == 1 + cap  # the first, plus one per allowed retiree
        assert len(reg.retiring) == cap + 1 and len(rreg.retiring) == cap + 1
        assert any("stopped ones are still finishing" in r.getMessage() for r in caplog.records)

    def test_unplaceable_process_starts_both_with_a_warning(self, plugin, monkeypatch, no_uwsgi,
                                                            registries, spawned, caplog):
        monkeypatch.setattr(plugin.sys, "argv", ["/tmp/probe.py"])
        plugin.Plugin()
        assert sorted(spawned) == ["ranked_matchups-reaper", "ranked_matchups-scheduler"]
        assert any(r.levelname == "WARNING" and "cannot tell whether this process" in r.getMessage()
                   for r in caplog.records)

    def test_a_threadless_process_migrates_and_checks_staleness(
            self, plugin, monkeypatch, no_uwsgi, registries, spawned):
        # It still migrates (unchanged) and runs the staleness check: if no
        # scheduler ran anywhere, the Celery children are what would log it.
        calls = []
        monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append("migrate"))
        monkeypatch.setattr(plugin, "_warn_if_scheduler_stale", lambda s: calls.append("stale"))
        monkeypatch.setattr(plugin.sys, "argv", CELERY_PREFORK)
        plugin.Plugin()
        assert calls == ["migrate", "stale"]
        assert spawned == []

    @pytest.mark.parametrize("argv", [["manage.py", "migrate", "--noinput"],
                                      ["/app/manage.py", "shell", "-c", "print(1)"]],
                             ids=["manage-migrate", "manage-shell"])
    def test_a_one_shot_command_skips_the_staleness_check(
            self, plugin, monkeypatch, no_uwsgi, registries, spawned, argv):
        # The AIO entrypoint runs `manage.py migrate` before uwsgi starts
        # Redis, and its post_migrate discovery builds this Plugin: a check
        # there could only sit through RedisClient's connect retries and log
        # a Redis ERROR on every container boot. Threadless, migrated, no check.
        calls = []
        monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append("migrate"))
        monkeypatch.setattr(plugin, "_warn_if_scheduler_stale", lambda s: calls.append("stale"))
        monkeypatch.setattr(plugin.sys, "argv", argv)
        plugin.Plugin()
        assert calls == ["migrate"]
        assert spawned == []

    @pytest.mark.parametrize("argv", [CELERY_THREADS, DAPHNE], ids=["celery-threads", "daphne"])
    def test_the_other_long_lived_threadless_processes_check(
            self, plugin, monkeypatch, no_uwsgi, registries, spawned, argv):
        calls = []
        monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append("migrate"))
        monkeypatch.setattr(plugin, "_warn_if_scheduler_stale", lambda s: calls.append("stale"))
        monkeypatch.setattr(plugin.sys, "argv", argv)
        plugin.Plugin()
        assert calls == ["migrate", "stale"]

    def test_a_worker_discovery_does_not_touch_redis(self, plugin, monkeypatch,
                                                     registries, spawned):
        # Discovery runs on every plugins-list fetch in a uwsgi worker; its
        # reaper tick does the check there instead.
        calls = []
        monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append("migrate"))
        monkeypatch.setattr(plugin, "_warn_if_scheduler_stale", lambda s: calls.append("stale"))
        _as_uwsgi_worker(monkeypatch, 1)
        plugin.Plugin()
        assert calls == ["migrate"]

    def test_the_cannot_tell_warning_is_said_once_per_process(
            self, plugin, monkeypatch, no_uwsgi, registries, spawned, caplog):
        monkeypatch.setattr(plugin.sys, "argv", ["/tmp/probe.py"])
        for _ in range(3):  # discovery rebuilds Plugin on every plugins-list fetch
            plugin.Plugin()
        said = [r for r in caplog.records if "cannot tell whether" in r.getMessage()]
        assert len(said) == 1


class TestPreviousFireTime:
    def test_same_day(self, plugin):
        tz = plugin._resolve_tz("America/New_York")
        now = datetime(2026, 10, 11, 14, 0, tzinfo=UTC)  # 10:00 EDT
        prev = plugin._previous_fire_time([(4, 0), (10, 0), (16, 0)], tz, now)
        assert prev.isoformat() == "2026-10-11T10:00:00-04:00"

    def test_before_the_first_slot_is_yesterdays_last(self, plugin):
        tz = plugin._resolve_tz("America/New_York")
        now = datetime(2026, 10, 11, 7, 0, tzinfo=UTC)  # 03:00 EDT
        prev = plugin._previous_fire_time([(4, 0), (22, 0)], tz, now)
        assert prev.isoformat() == "2026-10-10T22:00:00-04:00"

    def test_matches_the_slot_the_scheduler_claims(self, plugin):
        # The claim key is the slot's isoformat(): the two must agree exactly.
        tz = plugin._resolve_tz("America/New_York")
        times = [(4, 0), (10, 0), (16, 0), (22, 0)]
        before = datetime(2026, 11, 1, 9, 59, tzinfo=tz)
        slot = plugin._next_fire_time(times, tz, now=before)
        after = slot.astimezone(UTC) + timedelta(minutes=1)
        assert plugin._previous_fire_time(times, tz, after).isoformat() == slot.isoformat()

    @pytest.mark.parametrize("hhmm, before", [
        ((1, 30), datetime(2026, 11, 1, 5, 0, tzinfo=UTC)),   # fall-back: 01:30 happens twice
        ((2, 30), datetime(2026, 3, 8, 6, 0, tzinfo=UTC)),    # spring-forward: 02:30 never happens
    ], ids=["fall-back", "spring-forward"])
    def test_dst_days_agree_with_the_claimed_slot(self, plugin, hhmm, before):
        tz = plugin._resolve_tz("America/New_York")
        slot = plugin._next_fire_time([hhmm], tz, now=before.astimezone(tz))
        # Up to 22h: the spring-forward day is only 23h long, so 23h after
        # its 02:30 is already the next day's 02:30.
        # 30 min / 1 h after the fall-back 01:30 EDT are 01:00 / 01:30 EST, wall
        # times at or before it: a same-zone (wall-clock) comparison regresses there.
        for later in (timedelta(minutes=1), timedelta(minutes=30), timedelta(hours=1),
                      timedelta(hours=2), timedelta(hours=22)):
            prev = plugin._previous_fire_time([hhmm], tz, slot.astimezone(UTC) + later)
            assert prev.isoformat() == slot.isoformat()

    def test_no_times(self, plugin):
        assert plugin._previous_fire_time([], UTC, datetime(2026, 1, 1, tzinfo=UTC)) is None


@pytest.fixture
def redis(monkeypatch):
    """A fake Redis whose keys EXPIRE against clock["now"] (a datetime; None
    means nothing ever expires)."""
    data = {}
    expires = {}
    clock = {"now": None}

    def _live(key):
        at = expires.get(key)
        if at is not None and clock["now"] is not None and clock["now"] >= at:
            data.pop(key, None)
            expires.pop(key, None)
        return key in data

    class FakeClient:
        def get(self, key):
            return data.get(key) if _live(key) else None

        def set(self, key, value, nx=False, ex=None):
            if nx and _live(key):
                return None
            data[key] = value
            expires[key] = (clock["now"] + timedelta(seconds=ex)
                            if ex and clock["now"] is not None else None)
            return True

        def exists(self, key):
            return 1 if _live(key) else 0

    fake_core = MagicMock()
    fake_core.RedisClient.get_client.return_value = FakeClient()
    monkeypatch.setitem(sys.modules, "core", MagicMock())
    monkeypatch.setitem(sys.modules, "core.utils", fake_core)
    return data, clock


SCHED = {"auto_refresh_enabled": True, "scheduled_times": "0400,1000,1600,2200",
         "local_timezone": "America/New_York"}
# 2026-10-11 10:00 EDT is 14:00 UTC.
SLOT_10 = datetime(2026, 10, 11, 14, 0, tzinfo=UTC)


class TestSchedulerStaleness:
    @pytest.fixture(autouse=True)
    def _no_last_run(self, plugin, monkeypatch):
        # Never read a last_run.json lying in the checkout; tests that need a
        # record set one.
        monkeypatch.setattr(plugin.tasks, "read_last_run", lambda: None)

    def _watch_from(self, plugin, since, settings=SCHED):
        """Start watching at `since` (the first check only records the watch)."""
        assert plugin._scheduler_staleness(settings, now=since) is None

    def test_an_unclaimed_slot_is_reported(self, plugin, redis):
        self._watch_from(plugin, SLOT_10 - timedelta(hours=2))
        line = plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=20))
        assert line == (
            "WARNING: no scheduler started the 2026-10-11 10:00 EDT run: no process claimed "
            "that slot, so automatic Refresh + apply may not be running anywhere. If "
            "Dispatcharr was not running then, the next scheduled run clears this; otherwise "
            "check the log for 'scheduler thread started' (one per uwsgi worker) and restart "
            "Dispatcharr."
        )

    def test_a_slot_the_scheduler_claimed_is_not(self, plugin, redis):
        self._watch_from(plugin, SLOT_10 - timedelta(hours=2))
        tz = plugin._resolve_tz(SCHED["local_timezone"])
        times = plugin._parse_scheduled_times(SCHED["scheduled_times"])
        slot = plugin._next_fire_time(times, tz, now=(SLOT_10 - timedelta(minutes=1)).astimezone(tz))
        # The scheduler's OWN claim, so a key-format drift fails here.
        assert plugin._claim_scheduled_slot(slot) == plugin._SLOT_CLAIMED
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=20)) is None

    def test_within_the_grace_the_previous_slot_is_judged(self, plugin, redis):
        data, _ = redis
        self._watch_from(plugin, SLOT_10 - timedelta(hours=8))
        data[plugin._SCHEDULER_SLOT_KEY_PREFIX + "2026-10-11T04:00:00-04:00"] = "401"
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=10)) is None
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=16)) is not None

    def test_slots_before_the_watch_began_do_not_count(self, plugin, redis):
        # Fresh install, Redis restart, or the container down over a slot
        # with Redis wiped: nothing was expected before the watch started.
        self._watch_from(plugin, SLOT_10 + timedelta(minutes=5))
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(hours=5)) is None
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(hours=6, minutes=20)) \
            is not None  # the 16:00 slot was after it

    def test_turning_it_on_or_changing_the_times_restarts_the_watch(self, plugin, redis):
        off = dict(SCHED, auto_refresh_enabled=False)
        self._watch_from(plugin, SLOT_10 - timedelta(days=3))
        assert plugin._scheduler_staleness(off, now=SLOT_10 - timedelta(days=1)) is None
        # Enabled just after 10:00: the 10:00 slot was never due.
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=30)) is None
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(hours=1)) is None
        moved = dict(SCHED, scheduled_times="0400,1600")
        assert plugin._scheduler_staleness(moved, now=SLOT_10 + timedelta(hours=1)) is None

    def test_the_same_schedule_spelled_differently_keeps_one_watch(self, plugin, redis):
        # The reaper reads raw stored settings, Show current state the merged
        # ones: equal schedules must not reset each other's watch.
        self._watch_from(plugin, SLOT_10 - timedelta(hours=2))
        spelled = dict(SCHED, scheduled_times=" 10:00, 0400 ,2200,1600")
        assert plugin._scheduler_staleness(spelled, now=SLOT_10 + timedelta(minutes=20)) is not None

    def test_off_or_without_times_says_nothing(self, plugin, redis):
        for settings in (dict(SCHED, auto_refresh_enabled=False), dict(SCHED, scheduled_times="x")):
            self._watch_from(plugin, SLOT_10 - timedelta(days=2), settings)
            assert plugin._scheduler_staleness(settings, now=SLOT_10 + timedelta(hours=1)) is None

    def test_a_missing_key_uses_the_schedulers_default(self, plugin, redis):
        bare = {"auto_refresh_enabled": True, "local_timezone": "America/New_York"}
        self._watch_from(plugin, SLOT_10 - timedelta(hours=2))
        assert plugin._scheduler_staleness(bare, now=SLOT_10 + timedelta(minutes=20)) is not None

    def test_redis_unreachable_is_unknown_not_stale(self, plugin, monkeypatch):
        fake_core = MagicMock()
        fake_core.RedisClient.get_client.side_effect = RuntimeError("redis down")
        monkeypatch.setitem(sys.modules, "core", MagicMock())
        monkeypatch.setitem(sys.modules, "core.utils", fake_core)
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10) is None
        fake_core.RedisClient.get_client.side_effect = None
        fake_core.RedisClient.get_client.return_value = None
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10) is None

    def test_redis_down_costs_no_connect_backoff(self, plugin, monkeypatch):
        # Modelled on Dispatcharr's RedisClient.get_client: with Redis down
        # it sleeps retry_interval * 2**n between its max_retries attempts
        # (1+2+4+8 s by default) and then returns None. Show current state
        # has already waited that out once in _read_run_lock, and Celery's
        # worker_process_init runs this too: the status line must not wait.
        waited, asked = [], []

        class DownRedisClient:
            @staticmethod
            def get_client(max_retries=5, retry_interval=1, **_kw):
                asked.append(max_retries)
                waited.extend(retry_interval * 2 ** n for n in range(max_retries - 1))
                return None

        fake_core = MagicMock(RedisClient=DownRedisClient)
        monkeypatch.setitem(sys.modules, "core", MagicMock())
        monkeypatch.setitem(sys.modules, "core.utils", fake_core)
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10) is None
        assert asked == [1]  # it did ask Redis, once, without retries
        assert waited == []

    def test_a_claim_outlives_a_day_plus_the_grace(self, plugin, redis):
        # One daily time: the newest due slot can be 24h + grace old, and an
        # expired claim would read as "no scheduler ran it". The fake Redis
        # really expires keys, against the clock set here.
        _, clock = redis
        daily = dict(SCHED, scheduled_times="1000")
        self._watch_from(plugin, SLOT_10 - timedelta(hours=1), daily)
        tz = plugin._resolve_tz(SCHED["local_timezone"])
        clock["now"] = SLOT_10
        assert plugin._claim_scheduled_slot(plugin._next_fire_time(
            [(10, 0)], tz, now=SLOT_10 - timedelta(minutes=1))) == plugin._SLOT_CLAIMED
        # The latest moment the 10:00 claim is still the slot judged.
        last = SLOT_10 + timedelta(hours=24, minutes=14)
        clock["now"] = last
        assert plugin._scheduler_staleness(daily, now=last) is None
        # Control: the next day's unclaimed 10:00 is reported once it is due.
        nxt = SLOT_10 + timedelta(hours=24, minutes=16)
        clock["now"] = nxt
        assert plugin._scheduler_staleness(daily, now=nxt) is not None

    def test_busy_is_not_dead(self, plugin, redis):
        # One worker, times 15 minutes apart, still inside the 10:00 pipeline
        # when 10:15 comes due: it holds the run lock and has not claimed.
        data, _ = redis
        close = dict(SCHED, scheduled_times="1000,1015")
        self._watch_from(plugin, SLOT_10 - timedelta(hours=1), close)
        data[plugin._SCHEDULER_SLOT_KEY_PREFIX + "2026-10-11T10:00:00-04:00"] = "401"
        at = SLOT_10 + timedelta(minutes=31)
        # The holder record exactly as a scheduled run writes it (the
        # producer's own function and kind), so a renamed kind fails here.
        data[plugin._SCHEDULER_LOCK_KEY] = plugin._new_lock_value(
            plugin.tasks.SCHEDULED_RUN_KIND, "scheduler-401")
        assert '"kind": "scheduled_auto_pipeline"' in data[plugin._SCHEDULER_LOCK_KEY]
        assert plugin._scheduler_staleness(close, now=at) is None
        del data[plugin._SCHEDULER_LOCK_KEY]
        assert plugin._scheduler_staleness(close, now=at) is not None

    def test_busy_through_the_slot_is_not_dead_after_the_run_either(
            self, plugin, monkeypatch, redis):
        # Same single worker: the 10:00 pipeline ran 10:00:05-10:31, through
        # 10:15, and the scheduler then picked 16:00, never claiming 10:15.
        # Lock free again, so only the run record explains the missing claim.
        data, _ = redis
        close = dict(SCHED, scheduled_times="1000,1015")
        self._watch_from(plugin, SLOT_10 - timedelta(hours=1), close)
        data[plugin._SCHEDULER_SLOT_KEY_PREFIX + "2026-10-11T10:00:00-04:00"] = "401"
        record = {"kind": "scheduled_auto_pipeline", "status": "ok",
                  "started_at": "2026-10-11T14:00:05+00:00",
                  "finished_at": "2026-10-11T14:31:00+00:00"}
        monkeypatch.setattr(plugin.tasks, "read_last_run", lambda: dict(record))
        at = SLOT_10 + timedelta(minutes=40)
        assert plugin._scheduler_staleness(close, now=at) is None
        # Its lock release and cleanup run just past the stamped finish.
        record["finished_at"] = "2026-10-11T14:14:30+00:00"
        assert plugin._scheduler_staleness(close, now=at) is None
        # Controls: a run that ended well BEFORE 10:15, a finish in the future
        # (a bad clock or file must not hide every later slot), or a manual
        # run spanning it, does not explain an unclaimed 10:15.
        record["finished_at"] = "2026-10-11T14:10:00+00:00"
        assert plugin._scheduler_staleness(close, now=at) is not None
        record["finished_at"] = "9999-12-31T23:59:59+00:00"
        assert plugin._scheduler_staleness(close, now=at) is not None
        record.update(kind="auto_pipeline", finished_at="2026-10-11T14:31:00+00:00")
        assert plugin._scheduler_staleness(close, now=at) is not None

    @pytest.mark.parametrize("holder", [
        '{"kind": "auto_pipeline", "task_id": "manual"}',
        '{"kind": "reap", "task_id": "reaper-401"}',
        "3f2a9c",  # a pre-#239 holder: a bare token, kind unknown
    ], ids=["manual-run", "reap", "pre-239"])
    def test_any_other_holder_does_not_hide_a_dead_scheduler(self, plugin, redis, holder):
        data, _ = redis
        self._watch_from(plugin, SLOT_10 - timedelta(hours=1))
        data[plugin._SCHEDULER_LOCK_KEY] = holder
        assert plugin._scheduler_staleness(SCHED, now=SLOT_10 + timedelta(minutes=20)) is not None


class TestStalenessIsShownAndLogged:
    def test_it_is_the_last_line_of_show_current_state(self, plugin, monkeypatch):
        line = "WARNING: no scheduler started the 2026-10-11 10:00 EDT run: test."
        monkeypatch.setattr(plugin, "_scheduler_staleness", lambda s: line)
        monkeypatch.setattr(plugin, "_read_cache", lambda: {})
        monkeypatch.setattr(plugin.tasks, "read_inflight", lambda: None)
        monkeypatch.setattr(plugin.tasks, "read_last_run", lambda: {
            "kind": "scheduled_auto_pipeline", "status": "ok", "duration_s": 300,
            "finished_at": "2026-10-11T08:05:00+00:00"})
        msg = plugin._action_show_status({"local_timezone": "America/New_York"})["message"]
        lines = msg.splitlines()
        assert lines[-1] == line
        assert lines[-2].startswith("Last run OK")

    def test_run_health_without_it_is_unchanged(self, plugin):
        _, verdict = plugin._run_health_lines({}, None, UTC)
        assert verdict == []

    def test_logged_once_per_missed_slot(self, plugin, monkeypatch, registries, caplog):
        lines = iter(["WARNING: A", "WARNING: A", "WARNING: B", None, "WARNING: B"])
        monkeypatch.setattr(plugin, "_scheduler_staleness", lambda s: next(lines))
        for _ in range(5):
            plugin._warn_if_scheduler_stale({})
        logged = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert len(logged) == 2
        assert logged[0].startswith("[ranked_matchups] A (pid=")
        assert logged[1].startswith("[ranked_matchups] B (pid=")

    @pytest.mark.parametrize("reap_minutes", [0, 30], ids=["reaping-off", "not-a-reap-tick"])
    def test_the_reaper_checks_on_every_wake(self, plugin, monkeypatch, reap_minutes):
        # Including the ticks that do not reap: the check must sit above the
        # reap_delay gate, or it runs only on the rare tick that also reaps.
        settings = {"auto_refresh_enabled": True}
        checked, reaped = [], []
        wakes = iter([False, True])  # one wake, then the stop event
        monkeypatch.setattr(plugin, "_scheduler_sleep", lambda ev, t: next(wakes))
        monkeypatch.setattr(plugin, "_reap_settings", lambda s: reap_minutes)
        monkeypatch.setattr(plugin, "_next_reap_delay", lambda s: 10_000.0)
        monkeypatch.setattr(plugin, "_read_stops", lambda: {})
        monkeypatch.setattr(plugin, "_run_due_stops", lambda s: None)
        monkeypatch.setattr(plugin, "_maybe_rematch_after_m3u", lambda s: None)
        monkeypatch.setattr(plugin, "_action_reap_locked", lambda *a, **k: reaped.append(1) or {})
        monkeypatch.setattr(plugin, "_warn_if_scheduler_stale", checked.append)
        ref = MagicMock(get_current_settings=lambda: settings)
        plugin._reaper_loop(ref, threading.Event())
        assert checked == [settings]
        assert reaped == []

    def test_a_malformed_stop_entry_does_not_stop_the_reaper(self, plugin, monkeypatch, tmp_path):
        # {"7": null} made next_stop_delay raise on every tick: a 10-minute
        # crash loop with no reaping, no handoffs and no staleness check.
        path = tmp_path / "recording_stops.json"
        good = {"at": "2026-10-11T14:00:00+00:00", "marker": "m"}
        path.write_text(json.dumps({"7": None, "8": good, "9": "x"}))
        monkeypatch.setattr(plugin, "RECORDING_STOPS_PATH", str(path))
        assert plugin._read_stops() == {"8": good}
        assert plugin.recording_policy.next_stop_delay(
            plugin._read_stops(), datetime(2026, 10, 11, 13, 0, tzinfo=UTC)) == 3600.0


def test_scheduler_default_matches_the_manifest(plugin, monkeypatch):
    fields = {f["id"]: f for f in json.loads((ROOT / "plugin.json").read_text())["fields"]}
    assert fields["scheduled_times"]["default"] == plugin._DEFAULT_SCHEDULED_TIMES
    # And the live loop uses it when the stored settings lack the key: it used
    # to fall back to "0400", one run a day while the modal showed four.
    seen = []
    real_next = plugin._next_fire_time
    monkeypatch.setattr(plugin, "_next_fire_time",
                        lambda times, tz, now=None: seen.append(times) or real_next(times, tz, now))
    monkeypatch.setattr(plugin, "_scheduler_sleep", lambda ev, t: True)  # stop at the first park
    ref = MagicMock(get_current_settings=lambda: {"auto_refresh_enabled": True})
    plugin._scheduler_loop(ref, threading.Event())
    assert seen == [[(4, 0), (10, 0), (16, 0), (22, 0)]]
