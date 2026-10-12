"""The last COMPLETE season read of each points-based source, kept across
refreshes, so a failed read can simulate from it instead of from nothing
(#289 follow-up).

Why it exists. A season read that fails (a lost ESPN walk day, a failed
club schedule, season request or chunk) is not simulated with holes in it
(#289), but simulating NOTHING scores every game of the league with no
standings stakes for that refresh. During an outage those games can fall
out of the top N and be reaped, then come back on the next good refresh:
the churn #261/#264 and #283 exist to prevent. Standings stakes move only as
results come in, so a recent complete read is a far better season than none.

What is kept. One file per season key (`PointsBasedSportSource.
season_store_key`, which names the class, label and season, so a new season
never reads the last one's games) in `season_cache/` beside cache.json,
gzipped JSON. Sizes measured live 2026-10-12 with every source's real
season: NCAA baseball, the biggest (5367 games), is 919 KB of JSON, all 14
points-based seasons 3.9 MB, and 0.31 MB gzipped. One file per season means
a read writes only its own season, never rewriting the others, and files
older than MAX_AGE are deleted whenever one is written, so the directory
holds at most one recent file per source.

What is trusted. Only a read with no failure is remembered (the caller
calls `remember` on success alone), so a partial read can never overwrite a
complete one; a later complete read replaces it. A file older than MAX_AGE,
for another key, from another version, or unreadable, is ignored.

DO NOT import `requests` here: this module only reads and writes the plugin
folder. Sources run in the pipeline subprocess, one refresh at a time (the
run lock), so there is one writer.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple, Optional

from .._util import parse_iso_utc, write_json_atomic

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.season_store")

# Beside cache.json, in the plugin folder (as `_odds.CACHE_PATH`).
STORE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "season_cache",
)

# How old a complete read may be and still stand in for a failed one.
#
# 36 hours, from three measurements and one schedule. The schedule: refreshes
# run four times a day by default (`scheduled_times` 0400,1000,1600,2200), so
# 36 h covers six failed refreshes in a row, a full day's outage of a feed
# and then some. The measurements (2026-10-12, the share of each season's
# FINISHED results that kicked off in the last 36 h): NCAA men's soccer 85 of
# 1284 (6.6%), women's 143 of 2356 (6.1%), NCAA football 46 of 448 (10.3%,
# a Saturday's slate), NHL 17 of 86 (early season), NFL 12 of 77. So a 36 h
# old season misses at most about one game day: the simulator replays those
# games as still to play rather than knowing their results, which widens a
# team's outcomes but does not misstate them, and is far closer to the truth
# than no season at all. Past that a football read misses a whole week of
# results, the standings it starts from are visibly wrong, and the honest
# answer is no stakes (the #289 behaviour) until a read succeeds.
MAX_AGE = timedelta(hours=36)

_VERSION = 1


class Remembered(NamedTuple):
    read_at: datetime
    games: List[Dict[str, Any]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _path(key: str) -> str:
    """The file for one season key: a digest, so any key (labels carry
    spaces and apostrophes) is a safe file name."""
    return os.path.join(STORE_DIR, hashlib.sha256(key.encode("utf-8")).hexdigest()[:24] + ".json.gz")


def _encode(value: Any) -> Any:
    """JSON-safe copy of a season list: a datetime anywhere becomes
    {"__dt__": iso}, the one non-JSON type the season lists carry
    (`start_time`), so `_decode` restores exactly what was read."""
    if isinstance(value, datetime):
        return {"__dt__": value.isoformat()}
    if isinstance(value, dict):
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    return value


def _decode(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"__dt__"}:
            return parse_iso_utc(value["__dt__"])
        return {k: _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    return value


def remember(key: str, games: List[Dict[str, Any]], now: Optional[datetime] = None) -> None:
    """Keep `games`, a COMPLETE season read, as `key`'s last complete read.
    Call it only for a read with no failure. Never raises: a season that
    cannot be stored is logged and the refresh goes on."""
    now = now or _now()
    try:
        os.makedirs(STORE_DIR, exist_ok=True)
        write_json_atomic(
            _path(key),
            {"v": _VERSION, "key": key, "read_at": now.isoformat(), "games": _encode(games)},
            compress=True, separators=(",", ":"),
        )
    except (OSError, TypeError, ValueError) as e:
        logger.warning("[season_store] could not keep the %s season: %s", key, e)
        return
    _prune()


def recall(key: str, now: Optional[datetime] = None) -> Optional[Remembered]:
    """`key`'s last complete read, if there is one no older than MAX_AGE."""
    now = now or _now()
    try:
        with gzip.open(_path(key), "rt", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, EOFError):
        return None
    if not (isinstance(data, dict) and data.get("v") == _VERSION and data.get("key") == key
            and isinstance(data.get("games"), list)):
        return None
    read_at = parse_iso_utc(data.get("read_at")) if isinstance(data.get("read_at"), str) else None
    # Not from the future either: a clock that went backwards must not make
    # a stale file look fresh for longer than MAX_AGE.
    if read_at is None or not (now - MAX_AGE <= read_at <= now + timedelta(minutes=5)):
        return None
    return Remembered(read_at, _decode(data["games"]))


def _prune() -> None:
    """Delete season files last written more than MAX_AGE ago: they can never
    be recalled, and this is what keeps the directory to one recent file per
    source. By the file's mtime against the wall clock (not the `now` a
    caller passes, and not by opening each file for its `read_at`): a file is
    written exactly when it is read, so the two agree, and pruning a dozen
    files costs a stat each."""
    cutoff = time.time() - MAX_AGE.total_seconds()
    try:
        names = os.listdir(STORE_DIR)
    except OSError:
        return
    for name in names:
        if not name.endswith(".json.gz"):
            continue
        path = os.path.join(STORE_DIR, name)
        try:
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
        except OSError:
            pass
