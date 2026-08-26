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

`sweep_upcoming_scoreboard` is the second shared piece: the whole
per-day sweep for sources that want UPCOMING fixtures only (drop
FINISHED, drop stale-SCHEDULED, dedupe by event id). It was extracted
from `friendlies.py` when `english_cup.py` needed the identical window,
because the two date constants below carry the reasoning for a live bug
each and duplicating that reasoning is how it drifts.

DO NOT route the bracket-flavoured sources (`ncaa_soccer_cup`,
`ncaa_baseball`, `ncaa_softball`, `mls_cup`) through it. Those sweep a
fixed CALENDAR WINDOW and deliberately KEEP finished games, because a
bracket's state is derived from results already played; handing them a
helper that drops FINISHED would silently empty their cascade. Their
sweeps differ on purpose. See #190.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from .._util import parse_iso_utc

# ESPN's soccer competition root. Every soccer slug this plugin sweeps
# (eng.1, eng.league_cup, eng.fa, fifa.friendly, club.friendly,
# usa.ncaa.m.1, ...) hangs off it.
ESPN_SOCCER_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# How many extra days BEHIND today-UTC the sweep reaches. ESPN buckets its
# scoreboard by US EASTERN calendar date, not UTC, so the `dates=YYYYMMDD`
# bucket for day D holds events with UTC timestamps running through
# D+1 04:59Z (23:59 EST). Anchoring the sweep at today-UTC therefore goes
# blind to every fixture in the 00:00Z-05:00Z window, which is prime time in
# the Americas and the Pacific. Verified live 2026-07-26T01:25Z: the
# `dates=20260725` bucket still held "Tottenham Hotspur at Auckland FC" at
# 2026-07-26T03:00Z (a favorite, 95 minutes from kickoff) and "Trinidad and
# Tobago at Louisville City FC" already in progress, while the
# `dates=20260726` bucket held neither.
#
# DO NOT "optimize" this back to starting at today: the extra bucket is one
# HTTP request, the FINISHED filter drops anything already ended, and the
# id-dedupe absorbs the overlap, so the day costs nothing. One day back fully
# covers the skew (max US Eastern offset is UTC-5). Reaching backwards does
# admit stale rows ESPN has not yet retagged FINISHED, which is what
# MAX_AGE_AFTER_KICKOFF below handles.
#
# Used to derive BOTH the sweep's start day and its length; DO NOT hardcode
# either independently or the window silently shifts when one is edited.
SCOREBOARD_LOOKBACK_DAYS = 1

# Upper bound on how long ago a kept fixture may have kicked off. The lookback
# above deliberately reaches into a past bucket to catch in-progress games, but
# ESPN sometimes leaves a finished match tagged SCHEDULED (its status lag is
# how the Wrexham v Leeds kickoff still read SCHEDULED two hours in). Without a
# floor those stale rows would sail past the FINISHED filter and land in the
# guide as "upcoming" games that ended hours ago, sorting to the top because
# channels are numbered by kickoff time. Six hours clears any football match
# (90 minutes plus halftime, stoppage, and extra time) with wide margin while
# still discarding yesterday's leftovers.
MAX_AGE_AFTER_KICKOFF = timedelta(hours=6)


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


def sweep_upcoming_scoreboard(
    *,
    slug: str,
    days_ahead: int,
    team_namer: Callable[[Dict[str, Any]], str],
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    extras_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    base: str = ESPN_SOCCER_BASE,
) -> List[Dict[str, Any]]:
    """Sweep one ESPN competition day by day and return UPCOMING games only.

    Shared by `friendlies.py` and `english_cup.py`; see the module docstring
    for why the bracket sources deliberately do NOT use it.

    ESPN's date-RANGE syntax silently caps at 25 events, so this walks ONE DAY
    AT A TIME (the same trap documented in `ncaa_baseball.py` and
    `ncaa_soccer.py`). The BUCKETS run from `SCOREBOARD_LOOKBACK_DAYS` before
    today-UTC through `days_ahead` after it.

    `days_ahead` bounds the buckets requested, NOT the kickoff timestamps
    returned, and the two are not the same thing. ESPN buckets by US Eastern
    date, so the last bucket can carry fixtures up to ~05:00Z the following
    morning; a `days_ahead=7` sweep can therefore return a kickoff a few hours
    past the 7-day mark. That is deliberate and NOT filtered: such a fixture is
    a genuine upcoming game, nothing downstream re-filters by the window
    (`plugin.py` trusts each source's `days_ahead`), and dropping it would lose
    a real match to an off-by-one on a timezone boundary. Do not "tighten" this
    into a hard timestamp cut without checking what it silently removes.

    Three filters, in order, each of which fixed a live bug:
      - FINISHED games are dropped: a match that already ended is not an
        upcoming matchup. A live (in-progress) game classifies as SCHEDULED in
        `extract_espn_scoreboard_event` and is KEPT, so "playing right now"
        still surfaces.
      - Games that kicked off more than `MAX_AGE_AFTER_KICKOFF` ago are dropped
        even when still tagged SCHEDULED, catching ESPN's status lag.
      - Events already seen (the lookback bucket overlaps the first forward
        bucket) are deduped by event id.

    `http_get` is injected rather than called directly for two reasons, and the
    second one is the load-bearing one: each source keeps logging under its OWN
    logger name, AND each source keeps its own `requests` symbol, which is the
    patch point every source's tests stub. Calling `requests.get` from in here
    would turn all of those stubs into silent no-ops that hit the real network
    rather than failing. DO NOT "simplify" this into a direct call.

    Returns the canonical `extract_espn_scoreboard_event` records (plus
    whatever `extras_fn` adds), in sweep order. Callers apply their own
    additional filtering (e.g. the friendlies favorites gate) afterwards.
    """
    now = datetime.now(timezone.utc)
    start_day = now.date() - timedelta(days=SCOREBOARD_LOOKBACK_DAYS)
    oldest_kickoff = now - MAX_AGE_AFTER_KICKOFF
    out: List[Dict[str, Any]] = []
    seen_ids: set = set()
    for offset in range(days_ahead + SCOREBOARD_LOOKBACK_DAYS + 1):
        day = start_day + timedelta(days=offset)
        data = http_get(
            f"{base}/{slug}/scoreboard?dates={day.strftime('%Y%m%d')}"
        )
        if not data:
            continue
        for event in data.get("events") or []:
            rec = extract_espn_scoreboard_event(
                event, team_namer=team_namer, extras_fn=extras_fn,
            )
            if rec is None:
                continue
            # DROP FILTERS RUN BEFORE THE DEDUPE, and the order is load-bearing.
            #
            # The original friendlies loop claimed the id first and filtered
            # afterwards, which lets a REJECTED copy poison the key: the
            # overlapping lookback bucket and the first forward bucket both
            # carry the same event, and ESPN serves those two URLs from
            # separate cache entries, so one can lag the other. A stale
            # FINISHED copy in the earlier bucket would consume the id and then
            # be discarded, silently suppressing the good SCHEDULED copy that
            # followed. Filtering first means an id is only ever claimed by an
            # event actually kept. The cost is re-checking a dropped event once
            # per bucket, which is a dict lookup.
            if rec.get("status") == "FINISHED":
                continue
            start = rec.get("start_time")
            if start is None:
                continue
            if start < oldest_kickoff:
                # Kicked off long enough ago that it cannot still be running:
                # ESPN just hasn't retagged it FINISHED. Keeping it would put a
                # finished match in the guide as an upcoming game. See
                # MAX_AGE_AFTER_KICKOFF.
                continue
            # A MISSING id must not become a dedupe key.
            # `extract_espn_scoreboard_event` permits `id: None` (it passes
            # `event.get("id")` through), and `None` is a perfectly good dict
            # key, so using it directly meant the FIRST id-less event claimed
            # the slot for ALL of them and every later one was dropped as a
            # duplicate of an unrelated match. Fall back to the natural
            # identity instead, which is what the rest of the plugin uses when
            # no id is available.
            eid = rec.get("id")
            key = eid if eid else ("no-id", rec["home"], rec["away"], start)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            out.append(rec)
    return out
