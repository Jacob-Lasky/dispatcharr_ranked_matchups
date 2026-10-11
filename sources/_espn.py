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

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.espn")

# ESPN's soccer competition root. Every soccer slug this plugin sweeps
# (eng.1, eng.league_cup, eng.fa, fifa.friendly, club.friendly,
# usa.ncaa.m.1, ...) hangs off it.
ESPN_SOCCER_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# Every ESPN scoreboard request goes ONE DAY AT A TIME with an explicit page
# size, through `scoreboard_day_url` / `fetch_scoreboard_day`. Three measured
# facts (2026-10-11, #182) decide that, and each rules out a shortcut:
#
#   1. A single day PAGES silently at 100 events. `dates=20261025` on
#      `soccer/usa.ncaa.w.1` returned 100; `&limit=1000` returned 101. Over the
#      2026 women's NCAA soccer regular season the bare per-day walk saw 3001
#      games and the limited one 3226: 225 games missing from the season the
#      importance simulator replays, with nothing logged. `limit` is why
#      SCOREBOARD_EVENT_LIMIT exists, and it applies to every per-day call.
#   2. `limit` above 1000 does NOT raise the cap. ESPN silently answers with
#      a 25-event page instead: one `soccer/usa.ncaa.w.1` query returned 999
#      events at limit=999, 1000 at limit=1000 and 25 at limit=1001. A
#      25-event page is the likely origin of the old "range queries cap at
#      25" comments. DO NOT raise this constant past 1000.
#   3. The multi-day forms are not a substitute for the per-day walk:
#        - `dates=YYYYMMDD-YYYYMMDD` (with or without `limit`) answers
#          HTTP 400 "Failed to get events endpoint." for every team sport
#          tried: NBA, NFL, NHL, MLS, Premier League, NCAA soccer. It worked
#          for MLS on 2026-08-15 (#65), and the live plugin log shows
#          `mls_standings` getting 400s on it since at least 2026-10-07 (the
#          oldest retained log). It still answers for racing and tennis, but
#          EXCLUDES the window's last day (`racing/nascar-premier`
#          20261011-20261018 omitted the Oct 18 race) and missed an
#          in-progress tournament the per-day buckets carry (`tennis/atp`,
#          the Shanghai Masters on 2026-10-11).
#        - `dates=YYYYMM` returned 742 of the 1033 games the per-day walk
#          found for the 2025-26 women's college basketball season, WITHOUT
#          reaching the limit, so "count == limit" cannot detect it.
#      A form that fails loudly costs a fallback request; a form that drops
#      events quietly costs correctness with no symptom. Neither is worth the
#      requests it saves while every per-day walk runs concurrently
#      (`fetch_scoreboard_days`).
#
# A per-day page that still comes back AT the limit is logged by
# `fetch_scoreboard_day`, since that is the only symptom truncation gives.
SCOREBOARD_EVENT_LIMIT = 1000


def scoreboard_day_url(scoreboard_url: str, day: date) -> str:
    """The one-day scoreboard URL for `day`, with the explicit page size.

    `scoreboard_url` is the league's `.../scoreboard` endpoint, no query.
    Every source builds its day URLs here; `tests/test_espn_scoreboard.py`
    fails any `scoreboard?dates=` literal elsewhere in `sources/`, because a
    URL without `limit` silently loses events past the 100th of a busy day.
    """
    return f"{scoreboard_url}?dates={day.strftime('%Y%m%d')}&limit={SCOREBOARD_EVENT_LIMIT}"


def fetch_scoreboard_day(
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    scoreboard_url: str,
    day: date,
) -> Optional[Dict[str, Any]]:
    """`http_get(scoreboard_day_url(...))`, warning when the page is full.

    `http_get` is injected for the same reason as everywhere in this module:
    it is the source's own `_http_get`, the patch point its tests stub.
    Returns whatever `http_get` returned (a dict, or None on failure).
    """
    data = http_get(scoreboard_day_url(scoreboard_url, day))
    if isinstance(data, dict):
        n = len(data.get("events") or [])
        if n >= SCOREBOARD_EVENT_LIMIT:
            logger.warning(
                "[espn] %s dates=%s returned %d events, at the page limit of "
                "%d: that day may be truncated",
                scoreboard_url, day.strftime("%Y%m%d"), n, SCOREBOARD_EVENT_LIMIT,
            )
    return data


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

    Walks ONE DAY AT A TIME through `fetch_scoreboard_day`; see
    SCOREBOARD_EVENT_LIMIT for why no multi-day form replaces that. The
    BUCKETS run from `SCOREBOARD_LOOKBACK_DAYS` before today-UTC through
    `days_ahead` after it.

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
    for _day, data in fetch_scoreboard_days(
        http_get, f"{base}/{slug}/scoreboard",
        start_day, start_day + timedelta(days=days_ahead + SCOREBOARD_LOOKBACK_DAYS),
    ):
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


# Concurrent scoreboard requests for a season walk. A full-season walk is one
# request per day (up to ~210 for an NBA season), and walking every enabled
# source sequentially measured ~0.4 s per request on Tower: several minutes per
# refresh once #229 made the walks run to the season's end. Eight in flight
# keeps it to seconds without leaning hard on an unofficial API.
_SEASON_WALK_WORKERS = 8


def fetch_days(
    start: date,
    end: date,
    fetch_day: Callable[[date], Any],
    max_workers: int = _SEASON_WALK_WORKERS,
) -> List[Tuple[date, Any]]:
    """`[(day, fetch_day(day)), ...]` for every day from `start` to `end`
    inclusive, IN DATE ORDER, fetched concurrently (#229).

    `fetch_day` is injected for the same reason `http_get` is everywhere in
    this module: each source's tests stub its own `_http_get`, and a helper
    that made requests itself would turn those stubs into silent no-ops.
    Results come back in date order so callers' "first occurrence wins"
    dedupe behaves exactly as their old sequential loops did.

    Real threads are fine here because every source fetch runs inside the
    pipeline subprocess (`_action_refresh` is reached only through
    `_pipeline_runner.py`), never in a gevent-patched uwsgi worker. DO NOT
    call a source's fetch from a worker without revisiting that.
    """
    days: List[date] = []
    day = start
    while day <= end:
        days.append(day)
        day += timedelta(days=1)
    if not days:
        return []
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(days)))) as pool:
        return list(zip(days, pool.map(fetch_day, days)))


# How many days a walk tries before giving up on a scoreboard that answers
# NOTHING. A day with no games is a 200 with an empty `events` list, so a run
# of None pages means the endpoint itself is failing (down, hung, or rejecting
# the request), not a quiet stretch of calendar. Two concurrent batches: when
# every request hangs to its 15 s timeout, a whole-season walk stops after
# ~30 s instead of ~38 batches (~570 s for MLS's 303 days), and a refresh that
# walks several such seasons in a row stays inside the pipeline's 25-minute
# ceiling (#182 review). The cost is a walk that loses its first 16 days to a
# brief outage also skips the rest; it is retried on the next refresh.
SCOREBOARD_GIVE_UP_AFTER_DAYS = 2 * _SEASON_WALK_WORKERS


def fetch_scoreboard_days(
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    scoreboard_url: str,
    start: date,
    end: date,
) -> List[Tuple[date, Optional[Dict[str, Any]]]]:
    """`[(day, page), ...]` for the scoreboard days from `start` to `end`
    inclusive, IN DATE ORDER, each fetched through `fetch_scoreboard_day`
    and run concurrently by `fetch_days`.

    The one per-day walk every ESPN source uses: upcoming sweeps, full-season
    walks and bracket walks alike. Concurrency keeps a long window
    affordable: a slow endpoint costs one timeout per `_SEASON_WALK_WORKERS`
    days instead of one per day.

    If every one of the first `SCOREBOARD_GIVE_UP_AFTER_DAYS` days comes back
    None, the walk stops there, logs a WARNING and returns just those days,
    so the result can be SHORTER than the window. Callers already treat a
    missing day as "no games that day".
    """
    fetch = lambda day: fetch_scoreboard_day(http_get, scoreboard_url, day)  # noqa: E731
    probe_end = min(end, start + timedelta(days=SCOREBOARD_GIVE_UP_AFTER_DAYS - 1))
    pages = fetch_days(start, probe_end, fetch)
    if probe_end >= end:
        return pages
    if all(page is None for _day, page in pages):
        logger.warning(
            "[espn] %s: the first %d days (%s-%s) all failed; not requesting "
            "the remaining days through %s",
            scoreboard_url, len(pages), start.strftime("%Y%m%d"),
            probe_end.strftime("%Y%m%d"), end.strftime("%Y%m%d"),
        )
        return pages
    return pages + fetch_days(probe_end + timedelta(days=1), end, fetch)
