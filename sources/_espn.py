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

`sweep_upcoming_events` is the second shared piece: the whole per-day
sweep behind EVERY ESPN `fetch_upcoming` (#256), which wants UPCOMING
fixtures only (drop FINISHED, drop stale-SCHEDULED, dedupe by event id)
and owns the US-Eastern-bucket lookback through `upcoming_window`.
`sweep_upcoming_scoreboard` is it plus the canonical extraction. It was
extracted from `friendlies.py` when `english_cup.py` needed the identical
window, because the two date constants below carry the reasoning for a
live bug each and duplicating that reasoning is how it drifts; twelve
other sources had duplicated the window anyway, each starting at
today-UTC, until #256. `upcoming_window` is also the window for the two
non-ESPN feeds that bucket by US date (NHL `schedule/{date}`, MLB
`startDate=`).

DO NOT route the bracket-flavoured sources' BRACKET sweeps
(`_fetch_bracket_games` in `ncaa_soccer_cup`, `ncaa_baseball`,
`ncaa_softball`, `mls_cup`, ...) or any `_fetch_full_season_games` walk
through it. Those sweep a fixed CALENDAR WINDOW and deliberately KEEP
finished games, because a bracket's state and a season's table are
derived from results already played; handing them a helper that drops
FINISHED would silently empty their cascade. Their sweeps differ on
purpose. See #190 and #229. A bracket source's `fetch_upcoming` (the EPG
path) is upcoming-only and uses it like every other.

`dedupe_events` is the one dedupe EVERY ESPN walk goes through, the bracket
sweeps and full-season walks included (they share the dedupe, just not the
upcoming filter): first kept copy per `event_identity`, which falls back to
the event's natural identity when ESPN omits the id. That identity is also the
`game_id` every ESPN row and record the simulator sees carries (#279).

`fetch_season_type_windows` / `season_type_at` are the third piece: a
season's phases (regular season, each tournament round) from ESPN's core
API calendar, for competitions whose events no longer name their own round
(the NCAA College Cup since 2025, #235).
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import (
    Any, Callable, Dict, Iterable, Iterator, List, NamedTuple, Optional, Tuple, TypeVar,
)

from .._util import parse_iso_utc
from .base import RankingsUnavailable, report_lost_days

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
    Returns the page, or None when the day got no answer: the request failed,
    or the body carries no `events` list (#282).

    A day with no games answers `"events": []` (measured 2026-10-11 on NFL,
    NBA, NHL, FA Cup, college baseball and softball, NCAAW, NCAA soccer, UFC,
    F1, NASCAR, PGA and friendlies, on dates without games), and ESPN's error
    bodies carry no `events` key (`{"code": 404}` on a bad path). DO NOT read
    a body without the list as a quiet day: the per-day walk would not report
    the day lost (#261) and a bracket sweep would cache it as played (#245).
    None makes both callers treat it as the failure it is.
    """
    data = http_get(scoreboard_day_url(scoreboard_url, day))
    if not isinstance(data, dict) or not isinstance(data.get("events"), list):
        if data is not None:
            logger.warning(
                "[espn] %s dates=%s answered without an events list (%.80r); "
                "treating the day as not answered",
                scoreboard_url, day.strftime("%Y%m%d"), data,
            )
        return None
    n = len(data["events"])
    if n >= SCOREBOARD_EVENT_LIMIT:
        logger.warning(
            "[espn] %s dates=%s returned %d events, at the page limit of "
            "%d: that day may be truncated",
            scoreboard_url, day.strftime("%Y%m%d"), n, SCOREBOARD_EVENT_LIMIT,
        )
    return data


def fetch_espn_poll(
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    rankings_url: str,
    team_namer: Callable[[Dict[str, Any]], str],
    prefer: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Dict[str, int]:
    """{team: rank} from one poll of an ESPN `/rankings` page: the first poll
    `prefer` accepts, else the first listed. `{}` when the page answered with
    no polls. RAISES `base.RankingsUnavailable` when it did not answer: the
    request failed, or the body has no `rankings` list, or the chosen poll no
    `ranks` list (#283). An answered page always has the list (NCAAW, NCAAM
    and both NCAA soccer polls, measured 2026-10-11); a failed one is None,
    which is what `_http_get` makes of every 4xx/5xx, including the offseason
    404 college baseball and softball answer (see `base.attach_poll_ranks`,
    which never asks then). A listed poll that is not an object, or a chosen
    poll whose rows none of which reads as a team and rank, is a failure too.
    The ONE parser for the four ESPN college polls;
    `http_get` is injected for the reason given in `sweep_upcoming_scoreboard`.
    """
    data = http_get(rankings_url)
    polls = data.get("rankings") if isinstance(data, dict) else None
    if not isinstance(polls, list):
        raise RankingsUnavailable(f"{rankings_url} did not answer with a rankings list")
    if not polls:
        return {}
    if not all(isinstance(p, dict) for p in polls):
        raise RankingsUnavailable(f"{rankings_url}: a listed poll is not an object")
    poll = next((p for p in polls if prefer is not None and prefer(p)), polls[0])
    ranks = poll.get("ranks")
    if not isinstance(ranks, list):
        raise RankingsUnavailable(f"{rankings_url}: poll {poll.get('name')!r} has no ranks list")
    out: Dict[str, int] = {}
    for r in ranks:
        if not isinstance(r, dict):
            continue
        name = team_namer(r.get("team") or {})
        try:
            rank = int(r.get("current") or 0)
        except (TypeError, ValueError):
            continue
        if name and rank > 0:
            out[name] = rank
    if ranks and not out:
        # Rows listed, none readable: the row shape changed, not a poll with
        # nobody in it. Unranked games from it would be the #283 defect again.
        raise RankingsUnavailable(f"{rankings_url}: poll {poll.get('name')!r} has no readable rank")
    return out


# How many extra days BEHIND today-UTC every upcoming sweep reaches. ESPN
# buckets its scoreboard by US EASTERN calendar date, not UTC, so the
# `dates=YYYYMMDD` bucket for day D holds events with UTC timestamps running
# through D+1 04:59Z (23:59 EST). Anchoring the sweep at today-UTC therefore
# goes blind to every fixture in the 00:00Z-05:00Z window, which is prime time
# in the Americas and the Pacific. Verified live 2026-07-26T01:25Z: the
# `dates=20260725` bucket still held "Tottenham Hotspur at Auckland FC" at
# 2026-07-26T03:00Z (a favorite, 95 minutes from kickoff) and "Trinidad and
# Tobago at Louisville City FC" already in progress, while the
# `dates=20260726` bucket held neither. Measured again for #256 on
# 2026-10-11 across every league that still started at today-UTC: the NCAA
# College Cup semifinal NC State v Saint Louis (2025-12-13T01:30Z) sits under
# `dates=20251212`, and the two US-dated non-ESPN schedules bucket the same
# way: api-web.nhle.com `schedule/2026-10-10` lists ANA at CGY
# (2026-10-11T02:00Z) under its 2026-10-10 day, and statsapi.mlb.com lists
# Diamondbacks at Padres (2026-09-27T00:40Z) under 2026-09-26, so
# `startDate=2026-09-27` never returns it even BEFORE first pitch.
#
# DO NOT "optimize" this back to starting at today: the extra bucket is one
# HTTP request, the FINISHED filter drops anything already ended, and the
# id-dedupe absorbs the overlap, so the day costs nothing. One day back fully
# covers the skew (max US Eastern offset is UTC-5; US Pacific is behind ET, so
# its late games are already inside ET's day). Reaching backwards does admit
# stale rows the feed has not yet retagged FINISHED, which is what
# MAX_AGE_AFTER_KICKOFF below handles.
#
# Read ONLY through `upcoming_window`, which derives BOTH the first bucket and
# the window's length from it; DO NOT compute today-UTC in a source, or the
# window silently shifts when one of the two is edited (#256: twelve sources
# each computed their own today-UTC and all went blind after 00:00Z).
# `tests/test_upcoming_window.py` is the class guard.
SCOREBOARD_LOOKBACK_DAYS = 1

# Upper bound on how long ago a kept fixture may have kicked off. The lookback
# above deliberately reaches into a past bucket to catch in-progress games, but
# ESPN sometimes leaves a finished match tagged SCHEDULED (its status lag is
# how the Wrexham v Leeds kickoff still read SCHEDULED two hours in). Without a
# floor those stale rows would sail past the FINISHED filter and land in the
# guide as "upcoming" games that ended hours ago, sorting to the top because
# channels are numbered by kickoff time. Six hours clears any football match
# (90 minutes plus halftime, stoppage, and extra time) with wide margin while
# still discarding yesterday's leftovers, and it clears a full NFL, NBA, NHL or
# college baseball game too.
#
# DO NOT exempt a row ESPN still calls live (`in`) from this floor, tempting as
# a rain-delayed doubleheader or a long fight card makes it. The plugin dates a
# game's end at kickoff plus its post window (`plugin._game_end_utc`: 4 h by
# default, 2.5 h for soccer), so a game six hours past kickoff is already a
# "Past:" guide entry due for reaping; admitting it again only republishes a
# finished slot. A game that was already applied stays on air through
# `plugin._rows_to_hold` until the reaper retires it either way.
MAX_AGE_AFTER_KICKOFF = timedelta(hours=6)


class UpcomingWindow(NamedTuple):
    """The day buckets an upcoming sweep requests, and what it may keep.

    Built ONLY by `upcoming_window`. `today` is the first FORWARD bucket
    (today-UTC); every bucket from `first_day` up to it is lookback.
    """

    today: date
    first_day: date
    last_day: date
    oldest_kickoff: datetime

    def admits(self, start: Optional[datetime], over: bool) -> bool:
        """Whether a game is still upcoming or in progress: not over, dated,
        and kicked off no earlier than `oldest_kickoff`.

        For a head-to-head game this is the rule in every bucket, not just the
        lookback one: a game that ended this afternoon is no more an upcoming
        matchup than one that ended yesterday, and a refresh dropping it does
        not take its channel off air early, because `plugin._rows_to_hold`
        keeps every applied game that has kicked off until the reaper retires
        it. (Field events differ: see `_field_event_current`.)
        """
        return not over and start is not None and start >= self.oldest_kickoff


def upcoming_window(days_ahead: int) -> UpcomingWindow:
    """THE window for every upcoming sweep (#256): buckets from
    `SCOREBOARD_LOOKBACK_DAYS` before today-UTC through `days_ahead` after it,
    keeping games that kicked off at most `MAX_AGE_AFTER_KICKOFF` ago.

    Every `fetch_upcoming` that requests feed days by date (ESPN `dates=`, NHL
    `schedule/{date}`, MLB `startDate=`) takes its days from here, and this is
    the only place in `sources/` that turns the clock into a date. DO NOT
    write `datetime.now(...).date()` in a source: `tests/test_upcoming_window.py`
    fails it, and pins every source's first requested day under a fixed clock.
    """
    now = datetime.now(timezone.utc)
    today = now.date()
    return UpcomingWindow(
        today=today,
        first_day=today - timedelta(days=SCOREBOARD_LOOKBACK_DAYS),
        last_day=today + timedelta(days=days_ahead),
        oldest_kickoff=now - MAX_AGE_AFTER_KICKOFF,
    )


def scoreboard_event_over(event: Dict[str, Any]) -> bool:
    """Whether ESPN says a head-to-head scoreboard event is over: its first
    competition is `completed` or in state `post`.

    The ONE reading of ESPN's game status. `extract_espn_scoreboard_event`
    classifies FINISHED with it, and the upcoming sweeps drop with it. A
    postponed or cancelled game is `post` without being `completed`, so it is
    over too: it is not going to be played at the time it is listed (DO NOT
    "keep it in case ESPN moved it": a moved game gets a new `date` and goes
    back to `pre`, while one postponed ahead of time keeps its old slot as
    `post`, and keeping that would publish a game nobody will play). A
    competition with no status falls back to the event's own, which ESPN
    carries on every scoreboard event, so a missing block cannot pass a
    finished game as upcoming.
    """
    comps = event.get("competitions") or []
    comp = comps[0] if comps and isinstance(comps[0], dict) else {}
    status_type = (
        (comp.get("status") or {}).get("type")
        or (event.get("status") or {}).get("type")
        or {}
    )
    state = (status_type.get("state") or "").lower()
    return bool(status_type.get("completed")) or state == "post"


def _field_event_current(event: Dict[str, Any], window: UpcomingWindow) -> bool:
    """Whether a field event (race, golf or tennis tournament, fight card)
    from a LOOKBACK bucket is admitted: `window.admits` on its EVENT-level
    status and `date`.

    EVENT-level, not `competitions[0]` as for a game: a fight card's first
    competition is its first prelim bout, finished long before the main event.
    A multi-day event in a lookback bucket ages out of it (its `date` is the
    first day), which loses nothing: an event still running is also filed
    under every later day it spans, so the forward bucket carries it.

    Only the lookback buckets are filtered for field events. The forward
    buckets keep everything, as they always have, because a multi-day event's
    status and `date` do not describe the event: the same morning
    `tennis/atp?dates=20261011` reported the Rolex Shanghai Masters (running
    to 2026-10-19) as `post`, `completed`, and a golf event's `date` is its
    first round days ago. `endDate` does not rescue a forward filter either:
    golf's is the START of the final day (Baycurrent Classic: `date`
    2026-10-08T04:00Z, `endDate` 2026-10-11T04:00Z), so treating it as the
    end would drop a tournament during its final round. Measured 2026-10-11.
    """
    status_type = (event.get("status") or {}).get("type") or {}
    state = (status_type.get("state") or "").lower()
    over = bool(status_type.get("completed")) or state == "post"
    return window.admits(parse_iso_utc(event.get("date")), over)


def _team_identity(team: Dict[str, Any]) -> str:
    """One side of an id-less event's natural identity: the first field the
    team carries, of every field any source names a team by (the NCAA
    sources read `location`)."""
    for field in ("id", "displayName", "name", "location", "abbreviation"):
        if team.get(field):
            return str(team[field])
    return ""


def event_identity(event: Dict[str, Any]) -> Any:
    """The identity of one scoreboard event: its id, or, when ESPN omits one,
    its natural identity, as a string. THE key every ESPN dedupe uses
    (`dedupe_events`) AND the `game_id` every ESPN row and record that reaches
    the simulator carries (#279), so the upcoming row a refresh scores and the
    season or bracket record it must match agree on it.

    A MISSING id must not become the key itself: `None` is a perfectly good
    dict key, so the first id-less event would claim it for ALL of them and
    every later one would be dropped as a duplicate of an unrelated match.

    The natural identity is a STRING, not a tuple, because a row's `game_id`
    is written to cache.json and the guide's custom_properties: a tuple would
    come back from JSON as an unhashable list. The "no-id:" prefix keeps it
    out of ESPN's all-digit id space."""
    eid = event.get("id")
    if eid:
        return eid
    comps = event.get("competitions") or []
    competitors = (comps[0].get("competitors") or []) if comps and isinstance(comps[0], dict) else []
    teams = sorted(_team_identity(c.get("team") or {}) for c in competitors if isinstance(c, dict))
    # Teams and kickoff, NOT the event's display name: the two overlapping
    # buckets are separate cache entries, and a name edited between them
    # would otherwise make one game two rows. The name stands in only for a
    # field event, which has no two competitors. The kickoff cannot go
    # either: every game of a series has the same two teams. The cost is that
    # an id-less game whose kickoff changes between two of a walk's days is
    # two games; with no id there is nothing else to tie the copies together.
    if any(teams):
        who = "teams:" + "|".join(teams)
    else:
        who = "name:" + str(event.get("name") or event.get("shortName") or "")
    # The parsed instant, not the raw text: `...T17:00Z` and `...T17:00:00Z`
    # are one kickoff, and two keys for it would count one game twice.
    kickoff = parse_iso_utc(event.get("date"))
    when = kickoff.isoformat() if kickoff is not None else str(event.get("date"))
    return f"no-id:{who}@{when}"


T = TypeVar("T")


def scoreboard_events(pages: Iterable[Any]) -> Iterator[Any]:
    """Every event on each scoreboard page, in page order. A page that is not
    a dict (a day that failed or came back empty) contributes none."""
    for page in pages:
        if isinstance(page, dict):
            yield from page.get("events") or []


def dedupe_events(
    events: Iterable[Any],
    keep: Callable[[Dict[str, Any], Any], Optional[T]],
    identity: Optional[Callable[[Dict[str, Any]], Any]] = None,
) -> List[T]:
    """`keep(event, key)` for each event, in order, keeping the FIRST kept
    copy of each event: THE dedupe for every ESPN walk, upcoming sweep,
    bracket sweep and full-season walk alike (#279).

    `key` is `event_identity(event)`, or `identity(event)` for a walk whose
    own post-processing must see copies `event_identity` would merge (the
    single-leg soccer cups: `_soccer_bracket_helpers.pk_pair_identity`).
    DO NOT key a walk on
    `event.get("id")` (or a record's `id` / `game_id` copied from it): a
    missing id is None for EVERY id-less event, so the first one claims it
    and each later one is dropped as its duplicate, or, where a walk skipped
    a None id outright, every id-less game was lost.
    `tests/test_event_identity_dedupe.py` fails on such a seen-set anywhere
    in sources/ outside this module.

    `keep` returns None to skip an event, and a skipped event claims nothing,
    so a later copy can still be kept; `sweep_upcoming_events` says why that
    order is load-bearing.

    A record that goes on to the simulator (a season walk's `id`, a bracket
    game's `game_id`) must carry `key` as its id, NOT `event.get("id")`. The
    simulator marks a game played by that id (`_applied`), and its drain
    loop re-asks `remaining_matches` until nothing is left, so a game whose
    id is None cannot be marked played: `PointsBasedSportSource` never
    simulates it, and in a bracket the first FINISHED id-less game puts None
    in `_applied`, which then hides every other id-less game. The upcoming
    row scored against that record stamps the same `event_identity(event)` as
    its `game_id`, so the two are one match (`simulation._same_match`,
    `BracketSportSource.place_upcoming`). `key` IS the id whenever ESPN sends
    one, so stamping it changes nothing for those.
    """
    identity = identity or event_identity
    out: List[T] = []
    seen: set = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        key = identity(event)
        if key in seen:
            continue
        item = keep(event, key)
        if item is None:
            continue
        seen.add(key)
        out.append(item)
    return out


def sweep_upcoming_events(
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    scoreboard_url: str,
    days_ahead: int,
    convert: Callable[[Dict[str, Any]], Optional[T]],
    *,
    field_events: bool = False,
) -> List[T]:
    """THE upcoming sweep for every ESPN scoreboard source (#256):
    `convert(event)` for each event in `upcoming_window(days_ahead)` that is
    still upcoming or live, deduped by event (`dedupe_events`), in date order.

    `convert` is the source's own extraction and filtering (season type,
    postseason headline, conference); returning None skips the event. It sees
    raw ESPN events only after the shared admission rule:

      - head-to-head sources (the default): every bucket is filtered by
        `UpcomingWindow.admits` on `scoreboard_event_over` and the event's
        kickoff, so a FINISHED game and a stale SCHEDULED one (ESPN's status
        lag) are dropped wherever they sit.
      - `field_events=True`: only the lookback buckets are filtered, by
        `_field_event_current`; see it for why the forward ones cannot be.

    Admission runs BEFORE the dedupe, and an event's key is claimed only once
    `convert` keeps it, and both orders are load-bearing. The overlapping
    lookback bucket and the first forward bucket can carry the same event, and
    ESPN serves the two URLs from separate cache entries, so one can lag the
    other: a stale FINISHED (or malformed) copy that claimed the key first and
    was then discarded would silently suppress the good copy that followed.
    The cost is re-checking a dropped event once per bucket, a dict lookup.

    `http_get` is injected for the reason given in `sweep_upcoming_scoreboard`:
    it is the source's own patch point. Days come from `fetch_scoreboard_days`.
    """
    window = upcoming_window(days_ahead)

    def admitted() -> Iterator[Dict[str, Any]]:
        for day, data in fetch_scoreboard_days(
            http_get, scoreboard_url, window.first_day, window.last_day,
        ):
            for event in scoreboard_events([data]):
                if not isinstance(event, dict):
                    continue
                if field_events:
                    if day < window.today and not _field_event_current(event, window):
                        continue
                elif not window.admits(
                    parse_iso_utc(event.get("date")), scoreboard_event_over(event),
                ):
                    continue
                yield event

    return dedupe_events(admitted(), lambda event, _key: convert(event))


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

    status = "FINISHED" if scoreboard_event_over(event) else "SCHEDULED"

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
    """Sweep one ESPN competition day by day and return UPCOMING games only,
    as canonical `extract_espn_scoreboard_event` records.

    `sweep_upcoming_events` with `extract_espn_scoreboard_event` as the
    conversion: the window, the FINISHED drop, the stale-SCHEDULED floor and
    the dedupe are all `sweep_upcoming_events`'s, the one copy every ESPN
    `fetch_upcoming` shares (#256). Used by `friendlies.py`, `english_cup.py`,
    `ncaa_soccer.py` and `ncaa_soccer_cup.py`; see the module docstring for why
    the bracket SWEEPS deliberately do NOT use it.

    Walks ONE DAY AT A TIME through `fetch_scoreboard_day`; see
    SCOREBOARD_EVENT_LIMIT for why no multi-day form replaces that. The
    BUCKETS run from `SCOREBOARD_LOOKBACK_DAYS` before today-UTC through
    `days_ahead` after it (`upcoming_window`).

    `days_ahead` bounds the buckets requested, NOT the kickoff timestamps
    returned, and the two are not the same thing. ESPN buckets by US Eastern
    date, so the last bucket can carry fixtures up to ~05:00Z the following
    morning; a `days_ahead=7` sweep can therefore return a kickoff a few hours
    past the 7-day mark. That is deliberate and NOT filtered: such a fixture is
    a genuine upcoming game, nothing downstream re-filters by the window
    (`plugin.py` trusts each source's `days_ahead`), and dropping it would lose
    a real match to an off-by-one on a timezone boundary. Do not "tighten" this
    into a hard timestamp cut without checking what it silently removes.

    What is dropped, each of which fixed a live bug:
      - FINISHED games (`scoreboard_event_over`): a match that already ended is
        not an upcoming matchup. A live (in-progress) game is not over and is
        KEPT, so "playing right now" still surfaces.
      - Games that kicked off more than `MAX_AGE_AFTER_KICKOFF` ago, even when
        still tagged SCHEDULED, catching ESPN's status lag.
      - Events already seen (the lookback bucket overlaps the first forward
        bucket), deduped by event id, or by natural identity when the id is
        missing (`event_identity`).

    `http_get` is injected rather than called directly for two reasons, and the
    second one is the load-bearing one: each source keeps logging under its OWN
    logger name, AND each source keeps its own `_http_get`, the patch point
    its tests stub. Making the call from in here would bypass those stubs and
    hit the real network rather than failing (the suite's `_no_live_network`
    guard now turns that into a failure, but the stubs would still be dead).
    DO NOT "simplify" this into a direct call.

    Returns the records (plus whatever `extras_fn` adds) in sweep order.
    Callers apply their own additional filtering (e.g. the friendlies
    favorites gate) afterwards.
    """
    return sweep_upcoming_events(
        http_get, f"{base}/{slug}/scoreboard", days_ahead,
        lambda event: extract_espn_scoreboard_event(
            event, team_namer=team_namer, extras_fn=extras_fn,
        ),
    )


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


# A walk is fetched in chunks of this many days (two concurrent batches), and
# gives up on a scoreboard that is HANGING: a chunk in which every day came
# back None AND that took at least SCOREBOARD_SLOW_CHUNK_SECONDS stops the
# walk. Both halves are load-bearing (#182 review):
#
#   - All None, because a day with no games is a 200 with an empty `events`
#     list. A whole chunk of None means the endpoint itself is failing, not a
#     quiet stretch of calendar, and it is checked on EVERY chunk, so an
#     endpoint that answers day one and then hangs is still caught.
#   - Slow, because only a hang threatens the pipeline's 25-minute ceiling.
#     Every request timing out at 15 s costs ~30 s per chunk, so a 303-day
#     MLS walk would take ~570 s and two or three such walks in a row overran
#     the ceiling. An endpoint that fails FAST (a 503, a refused connection)
#     costs nothing to keep asking, and it may recover mid-walk, so it is
#     walked to the end rather than silently truncating the season.
#
# A hang that lets one request through per chunk defeats the all-None rule, so
# every walk ALSO stops once it has spent SCOREBOARD_WALK_BUDGET_SECONDS. The
# slowest healthy walk measured 2026-10-11 took ~5 s (MLS, 303 days), so 120 s
# is ~24x headroom and only a degraded endpoint reaches it.
#
# The cost is that a walk which meets a hang loses the rest of its window for
# this refresh (a WARNING says so); the next refresh retries it.
#
# A walk re-requests its failed days ONCE, but only when they are a blip: at
# least one day answered, no more than one day in SCOREBOARD_WALK_CHUNK_DAYS
# failed (at least one allowed), no more than one batch of
# _SEASON_WALK_WORKERS days, walk budget left, and the last chunk did not hang
# (all failed, slow), which is an outage starting mid-walk.
# A single read timeout is not hypothetical: one 303-day MLS walk measured
# 2026-10-11 lost 20261006 to it, and every finished game in a lost day drops
# out of the standings the importance simulator starts from, silently. The
# share cap is what keeps the retry off a degraded endpoint: a partial hang
# fails most days, retrying those is the cost the hang rules exist to avoid,
# so it gets no retry; a blip costs at most ONE more batch, one timeout.
# An abandoned walk never qualifies: the hang rule leaves a hung chunk, and
# the budget rule leaves no budget.
SCOREBOARD_WALK_CHUNK_DAYS = 2 * _SEASON_WALK_WORKERS
SCOREBOARD_SLOW_CHUNK_SECONDS = 10.0
SCOREBOARD_WALK_BUDGET_SECONDS = 120.0


def fetch_scoreboard_days(
    http_get: Callable[[str], Optional[Dict[str, Any]]],
    scoreboard_url: str,
    start: date,
    end: date,
) -> List[Tuple[date, Optional[Dict[str, Any]]]]:
    """`[(day, page), ...]` for the scoreboard days from `start` to `end`
    inclusive, IN DATE ORDER, each fetched through `fetch_scoreboard_day`
    and run concurrently by `fetch_days`.

    The per-day walk for every ESPN upcoming sweep and full-season walk.
    ESPN bracket sweeps do NOT use it: they fetch through
    `fetch_scoreboard_day` inside `BracketSportSource._sweep_bracket_days`,
    which gives up the whole sweep at the first batch with a failed day
    (#245), where this walk skips a failed day and reports it (#261). Concurrency keeps a long window
    affordable: a slow endpoint costs one timeout per `_SEASON_WALK_WORKERS`
    days instead of one per day.

    Stops early, with a WARNING, at the first chunk that is entirely None AND
    slow, or once the walk has used SCOREBOARD_WALK_BUDGET_SECONDS (see
    SCOREBOARD_WALK_CHUNK_DAYS), so the result can be SHORTER than the
    window. A walk that ran to its end retries a blip of failed days once
    (`_retry_failed_days`). A day still None after that, or never requested,
    is LOST: callers skip it as if it had no games, so the walk reports it
    (`base.report_lost_days`, logged at WARNING) and the refresh holds the
    games it had published for that day (#261). DO NOT drop the report: a
    sweep that lost a day is otherwise indistinguishable from a quiet one.
    """
    fetch = lambda day: fetch_scoreboard_day(http_get, scoreboard_url, day)  # noqa: E731
    pages: List[Tuple[date, Optional[Dict[str, Any]]]] = []
    walk_began = time.monotonic()
    chunk_start = start
    hung = False
    while chunk_start <= end:
        chunk_end = min(end, chunk_start + timedelta(days=SCOREBOARD_WALK_CHUNK_DAYS - 1))
        began = time.monotonic()
        chunk = fetch_days(chunk_start, chunk_end, fetch)
        pages.extend(chunk)
        took = time.monotonic() - began
        hung = (all(page is None for _day, page in chunk)
                and took >= SCOREBOARD_SLOW_CHUNK_SECONDS)
        if chunk_end < end and hung:
            logger.warning(
                "[espn] %s: every day %s-%s failed and took %.0f s; not "
                "requesting the remaining days through %s",
                scoreboard_url, chunk_start.strftime("%Y%m%d"),
                chunk_end.strftime("%Y%m%d"), took, end.strftime("%Y%m%d"),
            )
            break
        spent = time.monotonic() - walk_began
        if chunk_end < end and spent >= SCOREBOARD_WALK_BUDGET_SECONDS:
            logger.warning(
                "[espn] %s: walk took %.0f s by %s, past its %.0f s budget; "
                "not requesting the remaining days through %s",
                scoreboard_url, spent, chunk_end.strftime("%Y%m%d"),
                SCOREBOARD_WALK_BUDGET_SECONDS, end.strftime("%Y%m%d"),
            )
            break
        chunk_start = chunk_end + timedelta(days=1)
    if not hung:  # a hung LAST chunk is an outage too, not a blip to retry
        _retry_failed_days(fetch, pages, scoreboard_url, walk_began)
    _report_lost_days(pages, scoreboard_url, start, end)
    return pages


def _report_lost_days(
    pages: List[Tuple[date, Optional[Dict[str, Any]]]],
    scoreboard_url: str,
    start: date,
    end: date,
) -> None:
    """Report every day from `start` to `end` the walk has no page for: one
    that failed after its retry, or one never requested because the walk was
    abandoned. See `base.report_lost_days` (#261)."""
    answered = {day for day, page in pages if page is not None}
    lost: List[date] = []
    day = start
    while day <= end:
        if day not in answered:
            lost.append(day)
        day += timedelta(days=1)
    if not lost:
        return
    shown = ", ".join(d.strftime("%Y%m%d") for d in lost[:5])
    more = f" +{len(lost) - 5} more" if len(lost) > 5 else ""
    logger.warning(
        "[espn] %s: no answer for %d of %d day(s) (%s%s); their games are "
        "missing from this walk",
        scoreboard_url, len(lost), (end - start).days + 1, shown, more,
    )
    report_lost_days(lost)


def _retry_failed_days(
    fetch: Callable[[date], Optional[Dict[str, Any]]],
    pages: List[Tuple[date, Optional[Dict[str, Any]]]],
    scoreboard_url: str,
    walk_began: float,
) -> None:
    """Re-request a walk's failed days once, in place, when they are a blip.

    A blip: at least one day answered, and at most max(1, len // CHUNK) days
    failed, capped at one batch of _SEASON_WALK_WORKERS so the retry costs at
    most one request timeout. Skipped once the walk budget is spent. See
    SCOREBOARD_WALK_CHUNK_DAYS for why the caps exist; DO NOT raise the batch
    cap to a chunk, or a walk abandoned for a hang would retry the hung chunk.
    A retried day that now answers takes its own date slot, so a caller's
    first-occurrence dedupe sees exactly what a walk with no failure would. Pages stay in
    date order because each retried day replaces its own slot.
    """
    failed = [i for i, (_day, page) in enumerate(pages) if page is None]
    allowed = min(_SEASON_WALK_WORKERS,
                  max(1, len(pages) // SCOREBOARD_WALK_CHUNK_DAYS))
    if not failed or len(failed) == len(pages) or len(failed) > allowed:
        return
    if time.monotonic() - walk_began >= SCOREBOARD_WALK_BUDGET_SECONDS:
        return
    days = [pages[i][0] for i in failed]
    with ThreadPoolExecutor(max_workers=len(days)) as pool:
        retried = list(pool.map(fetch, days))
    recovered = 0
    for i, page in zip(failed, retried):
        if page is not None:
            pages[i] = (pages[i][0], page)
            recovered += 1
    logger.info(
        "[espn] %s: retried %d failed day(s), recovered %d",
        scoreboard_url, len(failed), recovered,
    )


# ESPN's "core" API root for soccer. The site API above serves scoreboards;
# this one serves the season calendar, which is where a competition's ROUNDS
# live as season types (see `fetch_season_type_windows`).
ESPN_CORE_SOCCER_BASE = "https://sports.core.api.espn.com/v2/sports/soccer/leagues"


class SeasonTypeWindow(NamedTuple):
    """One ESPN season type: a named phase of a season with its UTC window.

    `slug` is ESPN's own name for the phase (`first-round`, `quarterfinals`,
    `2025-ncaa-division-i-men` for the regular season), `type_id` the numeric
    id an event's `season.type` carries when ESPN tags it, and `start` / `end`
    the window bounds exactly as ESPN publishes them (both inclusive, minute
    precision, adjacent windows tiling with `end` one minute before the next
    `start`).
    """
    slug: str
    type_id: Optional[int]
    start: datetime
    end: datetime


def fetch_season_type_windows(
    league_slug: str,
    year: int,
    http_get: Callable[[str], Optional[Dict[str, Any]]],
) -> Optional[List[SeasonTypeWindow]]:
    """Every season type of `league_slug`'s `year` season, sorted by start,
    or None when ESPN did not return a complete, well-formed calendar.

    ONE request: `seasons/{year}` inlines every type with its slug and dates,
    so the per-type `$ref`s never need following.

    Why this exists (#235): ESPN stopped tagging NCAA soccer tournament events
    with their round. In 2024 a College Cup event carried
    `season.slug == "first-round"` and the round's own `season.type`; in 2025
    most carried the plain season slug and type, exactly like an October
    game (men's: 5 of 47 tagged, all third round; women's: 3 of 63), with empty
    `notes`, a generic `altGameNote` and no `competitions[0].type`, on the
    scoreboard, the summary and the core event alike. The season calendar
    still publishes each round as a season type with a UTC window, and
    routing every 2024 and 2025 tournament event by those windows reproduced
    every slug ESPN did publish and the exact bracket shape (47 men's games,
    63 women's) where it did not. Measured 2026-10-11.

    `http_get` is injected for the reason given in `sweep_upcoming_scoreboard`:
    each source's tests patch its OWN `_http_get`, and a request made from in
    here would turn those patches into silent live calls.
    """
    data = http_get(f"{ESPN_CORE_SOCCER_BASE}/{league_slug}/seasons/{year}")
    if not isinstance(data, dict):
        return None
    types = data.get("types")
    if not isinstance(types, dict):
        return None
    items = types.get("items")
    if not isinstance(items, list) or not items:
        return None
    # ALL OR NOTHING. A calendar missing a phase is worse than none: drop the
    # regular season and the first round becomes `windows[0]`, so
    # ncaa_soccer.py would end the regular season after the first round and
    # file its games as regular-season ones. So a short page (`count` says
    # more types exist than arrived) or any unusable item voids the whole
    # calendar, and callers take their no-calendar path.
    count = types.get("count")
    if isinstance(count, int) and count != len(items):
        return None
    out: List[SeasonTypeWindow] = []
    for item in items:
        if not isinstance(item, dict):
            return None
        raw_slug, raw_start, raw_end = item.get("slug"), item.get("startDate"), item.get("endDate")
        # Type-checked BEFORE parsing: `.strip()` on a numeric slug or
        # `parse_iso_utc` on a numeric date raises AttributeError, which would
        # turn a malformed calendar into a crashed refresh instead of the
        # no-calendar path this function promises.
        if not all(isinstance(v, str) for v in (raw_slug, raw_start, raw_end)):
            return None
        slug = raw_slug.strip().lower()
        start = parse_iso_utc(raw_start)
        end = parse_iso_utc(raw_end)
        if not slug or start is None or end is None or end < start:
            return None
        try:
            type_id: Optional[int] = int(item.get("type"))
        except (TypeError, ValueError):
            type_id = None
        out.append(SeasonTypeWindow(slug, type_id, start, end))
    out.sort(key=lambda w: w.start)
    return out


def season_type_at(
    windows: List[SeasonTypeWindow], when: Optional[datetime],
) -> Optional[SeasonTypeWindow]:
    """The season type whose window holds `when`, or None.

    The end bound is widened by one minute because ESPN's windows tile at
    minute precision (`...04:59Z` then `...05:00Z`): a kickoff stamped with
    seconds inside that last minute must not fall between two rounds.
    """
    if when is None:
        return None
    for w in windows:
        if w.start <= when < w.end + timedelta(minutes=1):
            return w
    return None
