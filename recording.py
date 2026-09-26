"""Auto-recording for the matchups plugin (#216): pure policy, no ORM.

Everything here takes plain values or row-likes (anything with the attributes a
Dispatcharr ``Recording`` has) and returns decisions. The plugin's apply does
the Django reads and writes around these calls. Keeping the policy ORM-free is
what lets it be unit-tested without a database, the same split as
``_partition_stale_for_recordings`` in plugin.py.

Phase A (this module's first cut) records games involving a *recorded team*,
inside a stream budget, earliest kickoff first. Ranked-game slots and the
preference/interruption rules are later phases; the plan shape already carries
the slot number they will need.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Key the plugin stamps into Recording.custom_properties on every recording it
# creates. It is the ONLY provenance test for "ours": every update, cancel and
# tombstone decision keys on it, so a recording the user made by hand is never
# touched. DO NOT rename it without migrating live rows: an unrecognised key
# makes the plugin create a duplicate and stop managing the old recording.
MARKER_KEY = "ranked_matchups_marker"

# Recording.custom_properties["status"] values Dispatcharr writes
# (apps/channels/tasks.py run_recording and api_views.stop). A recording in a
# terminal state is finished business: never re-created, never moved, and it
# holds no stream.
STATUS_RECORDING = "recording"
TERMINAL_STATUSES = frozenset({"completed", "stopped", "interrupted", "failed"})

# How long a created-by-us entry is kept once its game has left the slate.
# Long enough to outlive any game's window plus the lookahead the plugin plans
# over, short enough that the state file stays small.
STATE_RETENTION = timedelta(days=21)
# Tombstones are kept far longer: a postponed game can leave the slate for weeks
# and come back under the same stable marker (#217), and dropping its tombstone
# would re-create a recording the user deleted.
TOMBSTONE_RETENTION = timedelta(days=180)

DOT = "\U0001F534"  # red circle, the guide's "this game will be recorded" mark


# ---------------------------------------------------------------- settings --

def resolve_slot_budget(
    configured: Any,
    provider_limits: Iterable[int],
    reserve: Any,
) -> Optional[int]:
    """How many recordings may run at once, or None for no limit.

    ``provider_limits`` are the max_streams of the active M3U accounts (0 means
    unlimited in Dispatcharr and is ignored). The budget is the TIGHTEST
    positive limit minus ``reserve``, so a recording never takes the last
    stream someone needs to watch live TV. A positive ``configured`` value may
    lower that budget but never raise it above the provider's limit. With no
    positive provider limit the configured value stands alone, and 0 / blank
    means unlimited.
    """
    def _int(v: Any, default: int) -> int:
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    limits = [int(x) for x in provider_limits if _int(x, 0) > 0]
    reserve_n = max(0, _int(reserve, 1))
    configured_n = max(0, _int(configured, 0))
    provider_cap = (min(limits) - reserve_n) if limits else None
    if provider_cap is not None:
        provider_cap = max(0, provider_cap)
        return min(configured_n, provider_cap) if configured_n else provider_cap
    return configured_n or None


# ------------------------------------------------------------------ state ---

def load_state(path: str) -> Dict[str, Dict[str, Any]]:
    """Read the plugin's recording state file; a missing or corrupt file is an
    empty state (the worst case is one re-created recording, never a crash)."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {"created": {}, "tombstones": {}}
    if not isinstance(data, dict):
        return {"created": {}, "tombstones": {}}
    return {
        "created": dict(data.get("created") or {}),
        "tombstones": dict(data.get("tombstones") or {}),
    }


def save_state(path: str, state: Dict[str, Dict[str, Any]]) -> None:
    """Atomic write, per-writer temp name (same reasoning as plugin._write_cache:
    a shared temp path lets two writers publish a half-written file).

    Load-modify-save is only safe because every caller runs inside apply, which
    holds the cross-worker scheduler lock (#155; the reaper takes the same one).
    DO NOT write this file from anywhere that does not hold that lock: two
    writers would each replace the whole file and one's tombstones would be
    lost, re-creating a recording the user deleted."""
    import threading
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def prune_state(state: Dict[str, Dict[str, Any]], live_markers: Iterable[str], now: datetime) -> Dict[str, Dict[str, Any]]:
    """Drop entries for games no longer in the slate once they are older than
    STATE_RETENTION. An entry for a live game is always kept: dropping a live
    tombstone would re-create a recording the user deleted."""
    live = set(live_markers)

    def _keep(marker: str, stamp: Any, retention: timedelta = STATE_RETENTION) -> bool:
        cutoff = now - retention
        if marker in live:
            return True
        try:
            at = datetime.fromisoformat(str(stamp))
        except ValueError:
            return False
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        return at >= cutoff

    created = {m: v for m, v in state.get("created", {}).items()
               if _keep(m, (v or {}).get("at") if isinstance(v, dict) else None)}
    tomb = {m: at for m, at in state.get("tombstones", {}).items()
            if _keep(m, at, TOMBSTONE_RETENTION)}
    return {"created": created, "tombstones": tomb}


# --------------------------------------------------------------- row views ---

def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Treat a naive datetime as UTC. Dispatcharr runs with USE_TZ, but its own
    signal code defends against naive values, so this module does too: one
    naive row would otherwise raise TypeError and abort the whole apply."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _span(rec: Any) -> Tuple[Optional[datetime], Optional[datetime]]:
    return _aware(getattr(rec, "start_time", None)), _aware(getattr(rec, "end_time", None))


def rec_marker(rec: Any) -> Optional[str]:
    cp = getattr(rec, "custom_properties", None) or {}
    m = cp.get(MARKER_KEY)
    return str(m) if m else None


def rec_status(rec: Any) -> str:
    cp = getattr(rec, "custom_properties", None) or {}
    return str(cp.get("status") or "")


def is_terminal(rec: Any) -> bool:
    return rec_status(rec) in TERMINAL_STATUSES


def is_in_progress(rec: Any) -> bool:
    return rec_status(rec) == STATUS_RECORDING


def occupies_stream(rec: Any, now: datetime) -> bool:
    """A recording that will hold (or is holding) a provider stream."""
    end = _aware(getattr(rec, "end_time", None))
    return not is_terminal(rec) and end is not None and end > _aware(now)


# ------------------------------------------------------------------- plan ---

@dataclass(frozen=True)
class Candidate:
    """A game the plugin wants recorded. ``start``/``end`` is the full
    recording window: Live-block start (kickoff minus the pre-roll) through the
    scheduled end plus post-roll. NOT the Upcoming block (#145)."""
    marker: str
    kickoff: datetime
    start: datetime
    end: datetime


@dataclass
class Plan:
    slots: Optional[int]
    planned: Dict[str, Tuple[datetime, datetime, int]] = field(default_factory=dict)  # marker -> (start, end, slot)
    no_slot: List[str] = field(default_factory=list)   # markers that wanted a slot and got none
    skipped: Dict[str, str] = field(default_factory=dict)  # marker -> reason (tombstoned, finished)


def _overlaps(a0: datetime, a1: datetime, b0: datetime, b1: datetime) -> bool:
    return a0 < b1 and b0 < a1


def _max_concurrent(intervals: Sequence[Tuple[datetime, datetime]], s: datetime, e: datetime) -> int:
    """Peak number of intervals overlapping any instant of [s, e)."""
    points = [s] + [a for a, _b in intervals if s < a < e]
    return max((sum(1 for a, b in intervals if a <= t < b) for t in points), default=0)


def plan_recordings(
    candidates: Sequence[Candidate],
    slots: Optional[int],
    busy: Sequence[Tuple[datetime, datetime]] = (),
    held: Optional[Dict[str, Tuple[datetime, datetime]]] = None,
    skip: Optional[Dict[str, str]] = None,
) -> Plan:
    """Assign recording slots, earliest kickoff first.

    ``busy``: windows of recordings that are not ours but hold a stream (the
    user's own, series rules). They count against capacity and are never moved.
    ``held``: OUR recordings already in progress, by marker. They keep running
    whatever else is planned: a started recording is never displaced. When the
    game's window has grown (it runs later now), the plan carries the later end
    so apply extends the running recording; it never shortens one.
    ``skip``: markers that must not be planned, with the reason.

    Capacity is checked as CONCURRENCY: a game fits only when fewer than
    ``slots`` recordings overlap every instant of its window. DO NOT go back to
    checking capacity by fitting windows into lanes: a busy window that did not
    fit a lane (the user already overbooked) was silently dropped and the plan
    then overbooked further. Lanes are kept only to number our slots for the
    guide ("slot 1 of 3"). With ``slots=None`` every candidate is planned.
    """
    held = {m: (_aware(a), _aware(b)) for m, (a, b) in (held or {}).items()}
    skip = dict(skip or {})
    plan = Plan(slots=slots, skipped=dict(skip))
    occupied: List[Tuple[datetime, datetime]] = [(_aware(a), _aware(b)) for a, b in busy]
    lanes: List[List[Tuple[datetime, datetime]]] = []

    def _lane(s: datetime, e: datetime) -> int:
        for i, lane in enumerate(lanes):
            if all(not _overlaps(s, e, a, b) for a, b in lane):
                lane.append((s, e))
                return i + 1
        lanes.append([(s, e)])
        return len(lanes)

    ordered = sorted(candidates, key=lambda c: (c.kickoff, c.marker))
    by_marker = {c.marker: c for c in ordered}
    for marker, (s, e) in sorted(held.items(), key=lambda kv: (kv[1][0], kv[0])):
        c = by_marker.get(marker)
        if c is not None and c.end > e:
            e = c.end
        occupied.append((s, e))
        if c is None:
            # Still running for a game that is no longer wanted (team removed
            # mid-game). Never cut short here, so it holds a stream like any
            # other busy window, but it is not "planned".
            continue
        plan.planned[marker] = (s, e, _lane(s, e))
    for c in ordered:
        if c.marker in skip or c.marker in plan.planned:
            continue
        if slots is not None and _max_concurrent(occupied, c.start, c.end) >= slots:
            plan.no_slot.append(c.marker)
            continue
        occupied.append((c.start, c.end))
        plan.planned[c.marker] = (c.start, c.end, _lane(c.start, c.end))
    return plan


# -------------------------------------------------------------- decisions ---

CREATE = "create"
UPDATE = "update"      # not started: move start/end (full save reschedules it)
EXTEND = "extend"      # in progress: only a later end is honoured by Dispatcharr
KEEP = "keep"
SKIP = "skip"          # tombstoned / finished: do not create


def classify_existing(
    ours: Iterable[Any],
    state: Dict[str, Dict[str, Any]],
    wanted: Iterable[str],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Map marker -> our live Recording row, and marker -> skip reason.

    A wanted marker is skipped when it is tombstoned, when our recording for it
    is already in a terminal state (done, or stopped by the user: never
    restart it), or when state says we created one and it is gone. That last
    case is the user deleting it from the DVR tab, and it becomes a tombstone
    so the next apply does not bring it back.
    """
    by_marker: Dict[str, Any] = {}
    for r in ours:
        m = rec_marker(r)
        if not m:
            continue
        prev = by_marker.get(m)
        # Prefer a non-terminal row when a marker somehow has two.
        if prev is None or (is_terminal(prev) and not is_terminal(r)):
            by_marker[m] = r
    skip: Dict[str, str] = {}
    tomb = state.get("tombstones", {})
    created = state.get("created", {})
    # A recording of ours whose times or channel no longer match what we last
    # wrote was edited by the user. From then on it is theirs: never moved,
    # never cancelled. Checked for EVERY marker, not only wanted ones, because
    # the cancel step must not delete an edited recording either.
    for m, r in by_marker.items():
        if not is_terminal(r) and not is_in_progress(r) and was_edited(r, created.get(m)):
            skip[m] = "you changed it"
    for m in wanted:
        if m in skip:
            continue
        if m in tomb:
            skip[m] = "deleted by you"
        elif m in by_marker and is_terminal(by_marker[m]):
            skip[m] = rec_status(by_marker[m]) or "finished"
        elif m not in by_marker and m in created:
            skip[m] = "deleted by you"
    return by_marker, skip


def written_record(rec_id: Any, start: datetime, end: datetime, channel_id: Any, at: datetime) -> Dict[str, Any]:
    """The state entry for a recording we just wrote: what it SHOULD look like,
    so a later difference can be read as the user's edit."""
    return {"id": rec_id, "at": _aware(at).isoformat(), "start": _aware(start).isoformat(),
            "end": _aware(end).isoformat(), "channel": channel_id}


def was_edited(rec: Any, entry: Optional[Dict[str, Any]]) -> bool:
    """True when the row differs from what we last wrote. An entry from before
    this bookkeeping existed (no "start") cannot tell, so it counts as unedited."""
    if not isinstance(entry, dict) or "start" not in entry:
        return False
    s, e = _span(rec)
    return (s.isoformat() != entry.get("start") or e.isoformat() != entry.get("end")
            or getattr(rec, "channel_id", entry.get("channel")) != entry.get("channel"))


def decide(
    planned: Optional[Tuple[datetime, datetime, int]],
    existing: Any,
    channel_id: Any,
) -> str:
    """What to do for one planned game, given our existing row (or None)."""
    if planned is None:
        return KEEP
    start, end, _slot = planned
    if existing is None:
        return CREATE
    if is_terminal(existing):
        return SKIP
    ex_start, ex_end = _span(existing)
    if is_in_progress(existing):
        return EXTEND if _aware(end) > ex_end else KEEP
    if (ex_start != _aware(start) or ex_end != _aware(end)
            or getattr(existing, "channel_id", channel_id) != channel_id):
        return UPDATE
    return KEEP


def covered_by_user(rows: Iterable[Any], channel_id: Any, start: datetime, end: datetime) -> bool:
    """True when a recording that is NOT ours already covers this window on
    the game's own channel: the user scheduled it by hand. The plugin then
    leaves the game alone instead of adding a duplicate capture of the same
    channel, and the dot still shows because it is read from the user's row."""
    return any(
        getattr(r, "channel_id", None) == channel_id and not rec_marker(r)
        and not is_terminal(r) and _overlaps(_aware(start), _aware(end), *_span(r))
        for r in rows
    )


def cancellable(ours_by_marker: Dict[str, Any], plan: Plan) -> List[Any]:
    """Our recordings that are no longer planned and have not started: the
    team was removed from the list, or the game left the slate. A started or
    finished recording is never cancelled here."""
    out = []
    for m, r in ours_by_marker.items():
        if m in plan.planned or m in plan.skipped:
            continue
        if is_terminal(r) or is_in_progress(r):
            continue
        out.append(r)
    return out


# ------------------------------------------------------------------- dot ---

def recording_over(recs: Iterable[Any], start: datetime, end: datetime) -> bool:
    """True when any live (non-terminal) recording on the channel overlaps the
    window. Drives the 🔴, so it reads the ACTUAL rows after the writes,
    including recordings the user scheduled by hand."""
    return any(
        not is_terminal(r) and _overlaps(_aware(start), _aware(end), *_span(r))
        for r in recs
    )


def description_line(slot: Optional[int], slots: Optional[int]) -> str:
    """One short line for the programme description. The title carries the dot;
    this says which slot, so a user can read the plan."""
    if slot and slots:
        return f"Recording (slot {slot} of {slots})."
    return "Recording."
