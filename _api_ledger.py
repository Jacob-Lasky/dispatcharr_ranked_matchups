"""Every outbound request this plugin sends, counted per UTC day on disk (#363).

The call budgets in `_api_cache`, `sources/_college_data.py`, `sources/_boxing_data.py`
and `sources/_odds.py` are arithmetic about how often each API is asked. This ledger is
the measurement that checks the arithmetic: `_http.get_within_deadline` and
`_http.urlopen_read`, the only two functions that send a request, call `record` before
sending, so a request that times out or fails is counted too. Show current state prints
today's and yesterday's totals by host (`summary_lines`).

Each count is keyed by TRIGGER and ENDPOINT:

- The trigger is the run kind the pipeline child was started for (`TRIGGER_ENV`, set by
  `tasks._run_pipeline_child`: `scheduled`, `m3u_rematch`, `reap_now`, `apply`, ...), or
  `service` for work done in the long-lived processes (the leader service's slate and
  recording-final checks, the health pushes).
- The endpoint is the host plus the last three path segments, with anything that looks
  like a credential masked (`endpoint`). DO NOT record a query string or an unmasked
  path: TheSportsDB carries its key in the path, Uptime Kuma its push token, and The Odds
  API its key in the query. `tests/test_api_ledger.py` pins the masking.

Best effort by design: a ledger that cannot be written never blocks or fails a request.
Several processes write it (uWSGI, Celery, the pipeline child, and the child's sweep
threads), so every update is read-modify-write under an exclusive `flock` on a sidecar
lock file, plus a thread lock for the threads of one process.
"""
from __future__ import annotations

import fcntl
import json
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from . import storage

LEDGER_PATH = storage.path("api_calls.json")
TRIGGER_ENV = "RANKED_MATCHUPS_RUN_KIND"
KEEP_DAYS = 14
_VERSION = 1
_lock = threading.Lock()
# A path segment with letters AND digits and no dot, 8+ characters: a key or token.
# League ids (`usa.ncaa.m.1`), file names (`searchevents.php`) and words survive.
_TOKENISH = re.compile(r"^(?=[^.]*[A-Za-z])(?=[^.]*\d)[A-Za-z0-9_\-]{8,}$")
_LONG = re.compile(r"^[A-Za-z0-9_\-]{24,}$")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def endpoint(url: str) -> str:
    """`host a/b/c` for `url`: the last three path segments, digits folded to N and
    credential-like segments masked to `*`. Never the query string."""
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return "?"
    segments = []
    for seg in [s for s in parts.path.split("/") if s][-3:]:
        if _TOKENISH.match(seg) or _LONG.match(seg):
            segments.append("*")
        else:
            segments.append(re.sub(r"\d+", "N", seg))
    return f"{parts.hostname or '?'} {'/'.join(segments)}"


def trigger() -> str:
    return os.environ.get(TRIGGER_ENV) or "service"


def _read(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) and data.get("v") == _VERSION else {}


def record(url: str, now: Optional[datetime] = None) -> None:
    """Count one request to `url` under today's UTC date and this process's trigger."""
    now = now or _now()
    day = now.strftime("%Y-%m-%d")
    key = endpoint(url)
    kind = trigger()
    path = LEDGER_PATH
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with _lock, open(path + ".lock", "a") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            data = _read(path)
            days = data.get("days") if isinstance(data.get("days"), dict) else {}
            per_kind = days.setdefault(day, {}).setdefault(kind, {})
            per_kind[key] = int(per_kind.get(key, 0)) + 1
            oldest = (now - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
            days = {d: v for d, v in days.items() if d >= oldest and isinstance(v, dict)}
            # Imported here: _util imports _http, which imports this module.
            from ._util import write_json_atomic
            write_json_atomic(path, {"v": _VERSION, "days": days}, separators=(",", ":"))
    except (OSError, TypeError, ValueError):
        pass


def day_counts(day: str) -> Dict[str, Dict[str, int]]:
    """{trigger: {endpoint: n}} for one UTC date (`YYYY-MM-DD`)."""
    days = _read(LEDGER_PATH).get("days") or {}
    out = days.get(day) if isinstance(days, dict) else None
    return out if isinstance(out, dict) else {}


def by_host(counts: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for per_kind in counts.values():
        for key, n in (per_kind or {}).items():
            host = key.split(" ", 1)[0]
            totals[host] = totals.get(host, 0) + int(n)
    return totals


def summary_lines(now: Optional[datetime] = None) -> List[str]:
    """Show current state's API-call lines: today (UTC, so far) and yesterday, by host."""
    now = now or _now()
    lines = []
    for label, when in (("today so far", now), ("yesterday", now - timedelta(days=1))):
        totals = by_host(day_counts(when.strftime("%Y-%m-%d")))
        if not totals:
            continue
        ranked = sorted(totals.items(), key=lambda kv: (-kv[1], kv[0]))
        shown = ", ".join(f"{host} {n}" for host, n in ranked[:6])
        more = f", +{len(ranked) - 6} more host(s)" if len(ranked) > 6 else ""
        lines.append(f"Outside API calls {label} (UTC): {sum(totals.values())} ({shown}{more})")
    return lines
