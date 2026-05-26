"""Shared helpers for ESPN-derived soccer bracket sources.

`ncaa_soccer_cup` and `mls_cup` both pull bracket games from ESPN's
scoreboard. ESPN occasionally publishes two records for the same
elimination-stage match: one for the regulation 0-0 tie marked
FINISHED, and a second for the PK shootout final score (e.g., 3-2)
also marked FINISHED. Both sources have to collapse these pairs into
the single decisive record before downstream stage / matchday logic
sees them.

The helpers in this module are the canonical implementations.
DO NOT inline a parallel copy in a new source -- import from here.
See #69 (the consolidation issue) and the original duplicates in
ncaa_soccer_cup.py / mls_cup.py prior to extraction.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Tuple


def dedupe_pk_shootout_pairs(
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Collapse multiple events at the same (stage, participants) tuple
    into the single decisive event. ESPN occasionally publishes two
    records for a soccer bracket game that goes to OT / PKs: one with
    the regulation tie (e.g., 0-0) marked finished, and a second with
    the PK shootout final score (e.g., 3-2). Both are tagged complete
    in ESPN's scoreboard.

    Preference order when multiple records collide on
    (stage, frozenset(home, away)):
      1. SCHEDULED records lose to any FINISHED record (real outcomes
         beat placeholders).
      2. Among FINISHED records, non-tie outcomes beat tie outcomes
         (PK shootout score beats the regulation tie).
      3. Among non-tie FINISHED records, the LATEST `start_time` wins
         (handles a real same-day duplicate; in practice the shootout
         event is published with a later timestamp than the regulation
         tie).

    This is conservative: single-game elimination brackets cannot have
    a real second leg between the same two teams at the same stage, so
    any (stage, participants) collision IS data noise. Best-of-N stages
    (e.g., MLS Round One) should already have matchday-assigned records
    by the time they reach this helper; the (stage, participants) key
    is enough because each record carries a distinct matchday.
    """
    buckets: Dict[Tuple[str, FrozenSet[str]], List[Dict[str, Any]]] = {}
    for rec in records:
        key = (rec["stage"], frozenset((rec["home"], rec["away"])))
        buckets.setdefault(key, []).append(rec)

    out: List[Dict[str, Any]] = []
    for _key, bucket in buckets.items():
        if len(bucket) == 1:
            out.append(bucket[0])
            continue
        out.append(pick_decisive_event(bucket))
    return out


def pick_decisive_event(bucket: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply the (FINISHED > SCHEDULED, non-tie > tie, latest > earliest)
    preference order. Bucket must have 2+ records (callers should
    short-circuit single-record buckets before calling here)."""
    finished = [r for r in bucket if r.get("status") == "FINISHED"]
    if not finished:
        # All scheduled: keep the latest by start_time (most recent
        # info is the canonical one).
        return max(
            bucket,
            key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc),
        )
    non_tie = [
        r for r in finished
        if r.get("home_goals") is not None
        and r.get("away_goals") is not None
        and r["home_goals"] != r["away_goals"]
    ]
    pool = non_tie if non_tie else finished
    return max(
        pool,
        key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc),
    )
