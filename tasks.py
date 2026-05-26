"""Background-thread runner for ranked_matchups long-running actions.

These exist because Dispatcharr's HTTP layer drops the response when a
synchronous plugin action runs longer than the client's fetch timeout
(typically 30s for browsers, 60s for axios). Auto pipeline is ~40s
end-to-end on a populated install and reliably exceeds those defaults;
the Plugins page surfaces the dropped response as "failed to run plugin
action" even though the server-side work completed. See issue #84.

DO NOT switch this to Celery without first fixing Dispatcharr core.
The natural path is `@shared_task` + dispatch via `.delay()`, but
Dispatcharr's celery.py wires plugin discovery to the `worker_ready`
signal which fires AFTER the worker's consumer has called
`update_strategies()` and frozen the per-task strategy registry
(celery/worker/consumer/consumer.py:635). Plugins that register tasks
in `worker_ready` show up in `inspect.registered()` (which reads
`app.tasks`) but are rejected by the consumer as "unregistered task"
(KeyError in `strategies[name]` at consumer.py:668). Empirically
verified 2026-05-26 on Dispatcharr 0.25.1. The upstream fix is either
(a) move the discover_plugins call to `worker_init` instead of
`worker_ready`, or (b) drop `'celery'` from
`apps/plugins/apps.py::_no_discovery_cmds` so PluginsConfig.ready()
runs plugin discovery before Celery's consumer initializes. Until
one of those lands, daemon threads inside the uwsgi worker process
are the only viable async path.

Trade-offs of the daemon-thread approach:
  + No Celery dependency, no broker round-trip, no worker_ready race.
  + Lives in the uwsgi worker that handles the request -- consistent
    with the scheduler's existing in-process model.
  + Redis-backed scheduler lock already gates cross-worker mutex, so
    the "thread per uwsgi worker, no shared state" concern is moot.
  - Worker restart kills the in-flight thread. The 30-min Redis lock
    TTL auto-clears so a follow-up click can succeed.
  - State is in the worker, not in a broker. show_status reads the
    same Redis inflight key the thread writes, so the UI still sees
    progress even from a different worker.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
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


def run_auto_pipeline_background(settings: Dict[str, Any]) -> str:
    """Spawn a daemon thread that runs refresh + apply in the
    background. Returns the task id (a fresh UUID) the caller can
    surface to the UI.

    DO NOT block on the thread; the whole point is to return to the
    HTTP client immediately. The thread publishes progress to the
    inflight Redis key which show_status reads."""
    from .plugin import _action_auto_pipeline_sync

    task_id = _new_task_id()
    t = threading.Thread(
        target=_run_under_lock,
        args=("auto_pipeline", task_id, _action_auto_pipeline_sync, settings),
        name=f"ranked_matchups-auto_pipeline-{task_id[:8]}",
        daemon=True,
    )
    t.start()
    return task_id


def run_refresh_background(settings: Dict[str, Any]) -> str:
    """Spawn a daemon thread that runs refresh only. Same contract as
    run_auto_pipeline_background."""
    from .plugin import _action_refresh

    task_id = _new_task_id()
    t = threading.Thread(
        target=_run_under_lock,
        args=("refresh", task_id, _action_refresh, settings),
        name=f"ranked_matchups-refresh-{task_id[:8]}",
        daemon=True,
    )
    t.start()
    return task_id
