"""Shared helpers for ESPN scoreboard event parsing.

`mls_standings`, `ncaa_soccer`, `ncaa_baseball`, and (via the bracket
helpers) the cup-bracket sources all consume ESPN's
`/scoreboard?dates=YYYYMMDD` shape. The per-event extraction logic is
the same across these: pull the two competitors, canonicalize team
names, classify status, parse scores, demote FINISHED-without-scores
to SCHEDULED, emit the canonical PointsBasedSportSource record.

Per-source variation is restricted to:

  - How team names are canonicalized. Soccer uses
    `team.location` -> `team.name` -> `team.abbreviation`; baseball
    uses `team.displayName`. Callers pass `team_namer`.

  - What extra metadata the source wants on each record (e.g.,
    `mls_standings` wants `season_slug` for bracket vs regular-season
    routing; ncaa_soccer wants nothing). Callers pass `extras_fn`,
    a callable that receives the full event dict and returns a dict
    of extra top-level fields (NOT inside the `extra` sub-dict --
    those are first-class fields like `season_slug`).

DO NOT inline a fourth copy of this in a new ESPN-backed source.
Import from here. See #66 for the consolidation history.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from .._util import parse_iso_utc


def extract_espn_scoreboard_event(
    event: Dict[str, Any],
    *,
    team_namer: Callable[[Dict[str, Any]], str],
    extras_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Convert one ESPN scoreboard event into the canonical
    PointsBasedSportSource game record, or return None when the event
    is malformed (missing competitors, missing teams, etc.).

    Output shape (always present):
      id, home, away, home_points, away_points, status, start_time, extra

    Plus any top-level keys returned by `extras_fn(event)` if provided
    (e.g., `season_slug` for ESPN soccer events).

    Status classification:
      - `completed=True` or `state == "post"` -> FINISHED
      - Anything else -> SCHEDULED
      - FINISHED with missing scores demotes to SCHEDULED (the
        importance simulator must not seed a 0-0 result from a
        score-less FINISHED tag, which happens on some preseason and
        forfeited rows).

    home_points and away_points are integers (parsed from
    `competitor.score`) when status is FINISHED, otherwise None.
    Tied final scores in FINISHED games are LEFT TIED -- soccer-
    flavored callers want `hp == ap` to read as a draw. Sports that
    "force a winner via overtime" (NCAAF / NCAAM) coin-flip the tie
    upstream, not here.
    """
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
    competitors = comp.get("competitors") or []
    if len(competitors) != 2:
        return None
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if home is None or away is None:
        return None
    home_team = team_namer(home.get("team") or {})
    away_team = team_namer(away.get("team") or {})
    if not home_team or not away_team:
        return None

    status_type = (comp.get("status") or {}).get("type") or {}
    completed = bool(status_type.get("completed"))
    state = (status_type.get("state") or "").lower()
    if completed or state == "post":
        status = "FINISHED"
    else:
        status = "SCHEDULED"

    try:
        hp = int(home.get("score")) if status == "FINISHED" else None
    except (TypeError, ValueError):
        hp = None
    try:
        ap = int(away.get("score")) if status == "FINISHED" else None
    except (TypeError, ValueError):
        ap = None

    # FINISHED but missing scores demotes to SCHEDULED: the importance
    # simulator must not seed a 0-0 result. This happens on some
    # preseason and forfeit rows in ESPN's data.
    if status == "FINISHED" and (hp is None or ap is None):
        status = "SCHEDULED"
        hp = None
        ap = None

    out: Dict[str, Any] = {
        "id": event.get("id"),
        "home": home_team,
        "away": away_team,
        "home_points": hp,
        "away_points": ap,
        "status": status,
        "start_time": parse_iso_utc(event.get("date")),
        "extra": {},
    }
    if extras_fn is not None:
        out.update(extras_fn(event))
    return out
