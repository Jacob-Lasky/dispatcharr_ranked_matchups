"""Background runner for ranked_matchups long-running actions.

Two problems shape this module, and the fix for each is load-bearing:

1. HTTP RESPONSE TIMEOUT (#84). Dispatcharr's HTTP layer drops the
   response when a synchronous plugin action runs longer than the
   client's fetch timeout (~30s browsers, 60s axios). Auto pipeline is
   ~40s end-to-end on a populated install, so the Plugins page shows
   "failed to run plugin action" even though the work completed. The
   HTTP entry points therefore return a queued envelope immediately and
   run the work in a daemon thread that publishes progress to a Redis
   inflight key (show_status reads it).

2. GEVENT HUB FREEZE (the actual reason the work runs OUT OF PROCESS).
   DO NOT run the pipeline inline in the uwsgi worker -- not directly,
   and NOT in a daemon thread. Dispatcharr 0.26.0 runs uwsgi under gevent
   (gevent-early-monkey-patch = true). The scoring step (simulation.py)
   is a pure-Python Monte Carlo that holds the GIL for its entire run, so
   executing it anywhere in the worker process freezes every greenlet on
   that worker: login and live streams hang until it finishes (observed
   in prod 2026-06-10, a multi-restart outage). A gevent threadpool does
   NOT help -- pure-Python never yields the GIL. So the daemon thread no
   longer runs the work itself; it SUPERVISES a subprocess
   (run_pipeline_subprocess -> _pipeline_runner.py), a fresh interpreter
   that never imports the gevent monkey-patch and runs the scoring at
   full speed in its own GIL. The supervising thread only blocks on the
   child (gevent-cooperative wait), so the hub stays free.

DO NOT switch this to a Celery @shared_task without first fixing
Dispatcharr core. The natural path is `@shared_task` + `.delay()`, but
Dispatcharr's celery.py wires plugin discovery to the `worker_ready`
signal which fires AFTER the worker's consumer has called
`update_strategies()` and frozen the per-task strategy registry
(celery/worker/consumer/consumer.py:635). Plugins that register tasks in
`worker_ready` show up in `inspect.registered()` but are rejected by the
consumer as "unregistered task" (KeyError in `strategies[name]` at
consumer.py:668). Empirically verified 2026-05-26 on 0.25.1 and
RE-VERIFIED 2026-06-10 on 0.26.0: `'celery'` is still in
`apps/plugins/apps.py::_no_discovery_cmds` AND discovery is still on
`worker_ready`, so neither upstream fix has landed. The upstream fix is
either (a) move discover_plugins to `worker_init`, or (b) drop `'celery'`
from `_no_discovery_cmds` so PluginsConfig.ready() discovers plugins
before Celery's consumer initializes. If that lands, the subprocess can
be replaced by a shared_task in the `dvr` worker (which is --pool=threads
in a separate process, so the GIL freeze never reaches the uwsgi hub).

Trade-offs of the daemon-thread-supervises-subprocess approach:
  + Hub stays responsive: the CPU-bound scoring is in another process.
  + No Celery dependency, no broker round-trip, no worker_ready race.
  + Redis-backed scheduler lock still gates cross-worker mutex.
  - Worker restart kills the supervising thread (the child is reaped by
    the OS). The 30-min Redis lock TTL auto-clears so a retry succeeds.
  - ~1-2s django.setup() cost per run in the child. Acceptable for a
    6-hourly + manual action; the win is no frozen worker.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.tasks")

# Derive PLUGIN_KEY from the folder containing this file so worktrees and
# renamed clones (e.g., `dispatcharr_ranked_matchups-phase-e`) hash to the
# same Redis namespace plugin.py uses. Must stay in sync with the derivation
# at the top of plugin.py.
PLUGIN_KEY = os.path.basename(
    os.path.dirname(os.path.abspath(__file__))
).replace(" ", "_").lower()

_INFLIGHT_KEY = f"plugins:{PLUGIN_KEY}:inflight"
# Matches the scheduler lock TTL in plugin.py. If a run wedges past this we
# want show_status to stop claiming work is in progress; the lock itself
# also auto-releases at this point so a new run can start.
_INFLIGHT_TTL_SECONDS = 30 * 60

# Sentinel prefixing the single stdout line the subprocess emits with its
# JSON result. Reserving a marker (rather than parsing all of stdout) means a
# stray print during the child's django.setup() can't corrupt the result.
# _pipeline_runner.py imports this constant so there is one source of truth.
_RESULT_SENTINEL = "__RM_PIPELINE_RESULT__ "

# Hard ceiling on a single pipeline run. Kept just under the 30-min scheduler
# lock TTL so a hung child can't outlive the lock and wedge the next run; on
# timeout we return an error and the lock auto-releases.
_SUBPROCESS_TIMEOUT_SECONDS = 25 * 60

# Action selectors for the subprocess boundary -- the contract between
# run_pipeline_subprocess callers and _pipeline_runner.py's dispatch map.
# Defined once here and imported by the runner so a rename can't silently
# drift into an "unknown action" at runtime. Values match the manifest action
# ids. apply is NOT here: plugin.run("apply") and the auto_pipeline body call
# _action_apply directly, and apply is I/O-bound DB work that yields to the
# gevent hub (only the Monte Carlo scoring in refresh holds the GIL), so it
# does not need a subprocess.
ACTION_REFRESH = "refresh"
ACTION_AUTO_PIPELINE = "auto_pipeline"


def _redis():
    """Lazy-import RedisClient so tasks.py is importable in test contexts
    without Django configured. Returns None on any failure -- inflight
    publishing is best-effort; a missing Redis must not break the actual
    work."""
    try:
        from core.utils import RedisClient
        return RedisClient.get_client()
    except Exception:
        return None


def _set_inflight(kind: str, task_id: str, phase: str) -> None:
    r = _redis()
    if r is None:
        return
    payload = {
        "kind": kind,
        "task_id": task_id,
        "phase": phase,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
    }
    try:
        r.set(_INFLIGHT_KEY, json.dumps(payload), ex=_INFLIGHT_TTL_SECONDS)
    except Exception as e:
        logger.warning("[ranked_matchups] inflight redis set failed: %s", e)


def _update_inflight_phase(phase: str) -> None:
    r = _redis()
    if r is None:
        return
    try:
        raw = r.get(_INFLIGHT_KEY)
        if not raw:
            return
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        payload["phase"] = phase
        r.set(_INFLIGHT_KEY, json.dumps(payload), ex=_INFLIGHT_TTL_SECONDS)
    except Exception as e:
        logger.warning("[ranked_matchups] inflight phase update failed: %s", e)


def _clear_inflight() -> None:
    r = _redis()
    if r is None:
        return
    try:
        r.delete(_INFLIGHT_KEY)
    except Exception as e:
        logger.warning("[ranked_matchups] inflight redis clear failed: %s", e)


def read_inflight() -> Optional[Dict[str, Any]]:
    """Public read accessor. show_status uses this to surface 'still
    running' state so the UI has something to poll after a queued
    dispatch."""
    r = _redis()
    if r is None:
        return None
    try:
        raw = r.get(_INFLIGHT_KEY)
        if not raw:
            return None
        return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return None


def _run_under_lock(kind: str, task_id: str, target: Callable[[Dict[str, Any]], Dict[str, Any]], settings: Dict[str, Any]) -> None:
    """Common thread body: acquire scheduler lock, publish inflight,
    run the work, clear inflight, release lock. Logs exceptions but
    never propagates them out of the thread."""
    # Lazy import: keeps tasks.py importable without Django configured.
    from .plugin import _try_acquire_scheduler_lock, _release_scheduler_lock

    if not _try_acquire_scheduler_lock():
        logger.info(
            "[ranked_matchups.tasks] %s task %s: scheduler lock held; "
            "skipping (another refresh / auto_pipeline is in flight)",
            kind, task_id,
        )
        # Best-effort: publish a short-lived 'busy' marker so the UI's
        # show_status surfaces the skipped state for a few seconds. The
        # marker uses the SAME inflight key the active run is updating;
        # we deliberately do not overwrite an active marker.
        return
    try:
        _set_inflight(kind, task_id, "refresh")
        result = target(settings)
        logger.info(
            "[ranked_matchups.tasks] %s task %s complete: status=%s",
            kind, task_id, result.get("status"),
        )
    except Exception:
        logger.exception(
            "[ranked_matchups.tasks] %s task %s crashed", kind, task_id,
        )
    finally:
        _clear_inflight()
        _release_scheduler_lock()


def _new_task_id() -> str:
    return str(uuid.uuid4())


def _python_executable() -> str:
    """Resolve the real Python interpreter to fork.

    DO NOT use sys.executable here: uWSGI embeds Python, so inside a worker
    sys.executable is the uwsgi BINARY, not an interpreter. Running it against
    a .py script makes uwsgi try to load the script as a config
    ("unable to load configuration from ...") and the child exits rc=1.
    The venv interpreter lives at sys.prefix/bin/python (verified
    /dispatcharrpy/bin/python on the 0.26.0 image); fall back to PATH only if
    that's somehow absent."""
    candidate = os.path.join(sys.prefix, "bin", "python")
    if os.path.exists(candidate):
        return candidate
    exe = sys.executable or ""
    if exe and "uwsgi" not in os.path.basename(exe):
        return exe
    return shutil.which("python3") or shutil.which("python") or "python3"


def run_pipeline_subprocess(action: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Run a heavy pipeline action ('refresh' / 'apply' / 'auto_pipeline') in
    a SEPARATE PROCESS and return its result dict.

    This is the gevent-safety boundary (see module header): the scoring is a
    pure-Python Monte Carlo that holds the GIL, so it MUST NOT run in the uwsgi
    worker -- doing so freezes the gevent hub and hangs login + streams. We
    fork `_pipeline_runner.py` (a fresh interpreter with no gevent patch),
    feed it the action + settings as JSON on stdin, and read its result back
    from the sentinel line on stdout. Because the parent process is
    gevent-monkey-patched, `subprocess.run` yields the calling greenlet while
    the child runs, so the hub keeps serving requests.

    NEVER raises: the caller is a daemon thread / scheduler loop whose escape
    would crash a uwsgi worker. Any failure (non-zero exit, timeout,
    unparseable output) is logged and returned as an error dict."""
    runner = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_pipeline_runner.py")
    payload = json.dumps({"action": action, "settings": settings})
    # The forked plain interpreter's sys.path[0] is the plugin dir, so /app
    # (where the `dispatcharr` Django project lives) is NOT importable and
    # django.setup() dies with "No module named 'dispatcharr'". The uwsgi
    # worker we're running in CAN import it, so forward our sys.path to the
    # child via PYTHONPATH. (The child inherits our cwd by default, which is
    # the worker's /app, so we don't pass cwd explicitly.)
    env = os.environ.copy()
    forwarded = os.pathsep.join(p for p in sys.path if p)
    env["PYTHONPATH"] = (
        forwarded + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    )
    try:
        proc = subprocess.run(
            [_python_executable(), runner],
            input=payload,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            env=env,
        )
    except subprocess.TimeoutExpired:
        logger.error(
            "[ranked_matchups] pipeline subprocess action=%s timed out after %ss",
            action, _SUBPROCESS_TIMEOUT_SECONDS,
        )
        return {"status": "error", "message": "pipeline subprocess timed out"}
    except Exception as e:  # noqa: BLE001 - never let this escape the thread
        logger.exception("[ranked_matchups] pipeline subprocess action=%s crashed", action)
        return {"status": "error", "message": f"pipeline subprocess error: {e}"}

    # The child routes all its logging to stderr; surface it under our logger
    # so scheduled-run diagnostics land where they always did.
    if proc.stderr:
        for line in proc.stderr.splitlines():
            logger.info("[ranked_matchups.subproc] %s", line)

    if proc.returncode != 0:
        logger.error(
            "[ranked_matchups] pipeline subprocess action=%s exited rc=%s",
            action, proc.returncode,
        )
        return {"status": "error", "message": f"pipeline subprocess failed (rc={proc.returncode})"}

    for line in proc.stdout.splitlines():
        if line.startswith(_RESULT_SENTINEL):
            try:
                return json.loads(line[len(_RESULT_SENTINEL):])
            except json.JSONDecodeError:
                break
    logger.error(
        "[ranked_matchups] pipeline subprocess action=%s: no parseable result line", action,
    )
    return {"status": "error", "message": "pipeline subprocess produced no result"}


def run_auto_pipeline_background(settings: Dict[str, Any]) -> str:
    """Spawn a daemon thread that runs refresh + apply in the
    background. Returns the task id (a fresh UUID) the caller can
    surface to the UI.

    DO NOT block on the thread; the whole point is to return to the
    HTTP client immediately. The thread publishes progress to the
    inflight Redis key which show_status reads. The thread does NOT run
    the scoring itself -- it supervises a subprocess (see module header
    on the gevent freeze); the cooperative wait keeps the hub free."""
    task_id = _new_task_id()
    t = threading.Thread(
        target=_run_under_lock,
        args=("auto_pipeline", task_id, partial(run_pipeline_subprocess, ACTION_AUTO_PIPELINE), settings),
        name=f"ranked_matchups-auto_pipeline-{task_id[:8]}",
        daemon=True,
    )
    t.start()
    return task_id


def run_refresh_background(settings: Dict[str, Any]) -> str:
    """Spawn a daemon thread that runs refresh only. Same contract as
    run_auto_pipeline_background (work runs in a supervised subprocess)."""
    task_id = _new_task_id()
    t = threading.Thread(
        target=_run_under_lock,
        args=("refresh", task_id, partial(run_pipeline_subprocess, ACTION_REFRESH), settings),
        name=f"ranked_matchups-refresh-{task_id[:8]}",
        daemon=True,
    )
    t.start()
    return task_id


def run_scheduled_pipeline(settings: Dict[str, Any]) -> None:
    """Scheduler entry point for the auto-refresh tick.

    Runs the auto_pipeline through the SAME lock/inflight/subprocess path the
    HTTP launchers use (`_run_under_lock` -> supervised subprocess), so the
    scheduler can't drift back into running the Monte Carlo in-process and
    re-freezing the gevent hub. Blocks until the child completes; that's fine
    because the scheduler runs in its own loop thread, not a request worker.
    The kind/task_id mark this as a scheduled run so show_status can tell it
    apart from an HTTP-queued one."""
    _run_under_lock(
        "scheduled_auto_pipeline",
        f"scheduler-{os.getpid()}",
        partial(run_pipeline_subprocess, ACTION_AUTO_PIPELINE),
        settings,
    )
