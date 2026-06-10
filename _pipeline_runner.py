#!/usr/bin/env python
"""Subprocess entry point for ranked_matchups heavy pipeline actions.

DO NOT run the pipeline (refresh / apply / auto_pipeline) inline in the uwsgi
worker. Dispatcharr 0.26.0 runs uwsgi under gevent (gevent-early-monkey-patch).
The scoring step (simulation.py) is a pure-Python Monte Carlo that holds the
GIL for its entire run, so executing it in the worker -- even in a daemon
thread -- freezes every greenlet on that worker: login and live streams hang
until the scoring finishes. A gevent threadpool does NOT help (pure-Python
never yields the GIL), and a Celery @shared_task is rejected as "unregistered
task" on this build (see tasks.py header). So tasks.run_pipeline_subprocess
forks THIS script: a fresh interpreter that never imports the gevent
monkey-patch and runs the scoring at full speed in its own GIL, fully isolated
from the worker that serves requests.

Protocol:
    python _pipeline_runner.py   < {"action": "<name>", "settings": {...}}  on stdin
The action's result dict is written to stdout as a single sentinel-prefixed
line (see tasks._RESULT_SENTINEL). ALL logging goes to stderr so a stray
print during django.setup() can never corrupt the result line.
"""
import json
import logging
import os
import sys

# stderr-only logging: stdout is reserved for the one sentinel result line.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ranked_matchups.runner")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        log.exception("could not parse stdin payload")
        return 2
    action = payload.get("action")
    settings = payload.get("settings") or {}

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
    import django

    django.setup()

    # Resolve the plugin package by its folder name (== importable package
    # name in the deployed container) and add the plugins root to sys.path,
    # matching how Dispatcharr's loader registers it. Done AFTER django.setup()
    # because plugin.py imports Django models at module load.
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    plugins_root = os.path.dirname(pkg_dir)
    if plugins_root not in sys.path:
        sys.path.insert(0, plugins_root)
    pkg_name = os.path.basename(pkg_dir)

    plugin = __import__(f"{pkg_name}.plugin", fromlist=["plugin"])
    tasks = __import__(f"{pkg_name}.tasks", fromlist=["tasks"])

    # Only the GIL-holding actions run out of process. auto_pipeline chains
    # refresh + apply internally (transitioning the inflight phase the parent
    # published); standalone apply stays inline in the worker because it's
    # I/O-bound DB work that yields to the gevent hub. Action names come from
    # tasks so the caller/runner contract has one source of truth.
    actions = {
        tasks.ACTION_REFRESH: plugin._action_refresh,
        tasks.ACTION_AUTO_PIPELINE: plugin._action_auto_pipeline_sync,
    }
    fn = actions.get(action)
    if fn is None:
        log.error("unknown action %r", action)
        result = {"status": "error", "message": f"unknown action {action!r}"}
    else:
        try:
            result = fn(settings)
        except Exception as e:  # noqa: BLE001 - report, don't crash silently
            log.exception("action %s crashed", action)
            result = {"status": "error", "message": f"{action} crashed: {e}"}

    # Single intentional stdout write: the sentinel-prefixed result line.
    sys.stdout.write(tasks._RESULT_SENTINEL + json.dumps(result) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
