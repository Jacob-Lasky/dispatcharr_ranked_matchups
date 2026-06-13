"""Tiny shared utilities. Module sits at the package root so both `plugin.py`
and `sources/*.py` can import without circulars."""

from __future__ import annotations

import hashlib
import math
import random
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional


def parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp (with trailing Z or offset) into a tz-aware
    datetime. Returns None on any parse failure or if `s` is falsy.

    Centralizes the `s.replace("Z", "+00:00")` dance used by every API client.
    """
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def stable_hash_int(s: str) -> int:
    """Process-stable hash of a string. Python's builtin hash() is salted by
    PYTHONHASHSEED: same input gives different output across restarts. This
    uses md5 truncated to 16 hex chars (~64 bits) which is plenty for marker
    uniqueness and is identical across processes / restarts."""
    digest = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


# Fixed anchor for stable_channel_number. DO NOT change this date: every virtual
# channel's number is measured in minutes from this origin, so moving it shifts
# EVERY channel number by the same delta in one apply (a one-time mass renumber).
# It only needs to sit on or before the earliest kickoff the plugin will ever
# schedule; the plugin fetches upcoming games, so any date in the recent past
# works. 2026-01-01 keeps the resulting numbers ~7 digits rather than ~8.
CHANNEL_NUMBER_ORIGIN = date(2026, 1, 1)

# Tiebreak slots reserved per kickoff minute. The channel number is
# minutes-since-origin * this + (stable hash % this), so two games at the SAME
# minute get distinct, stable numbers as long as their hashes differ mod this.
# 16 keeps numbers compact while making same-minute hash collisions uncommon
# (~6% per simultaneous pair); the rare residual is resolved by the deterministic
# nudge in plugin._assign_channel_numbers. Because the value is < the per-minute
# stride, it never reorders games across minute boundaries.
CHANNEL_NUMBER_TIEBREAK_SLOTS = 16


def stable_channel_number(
    virtual_base: int, start_utc: datetime, marker: str, tz
) -> int:
    """Stable, chronological channel number for a virtual game channel.

    PURE FUNCTION of the game's immutable kickoff time and its `marker`: the
    SAME game always maps to the SAME integer for its whole life, regardless of
    how the slate is ranked or which other games are present. That stability is
    the whole point. Both Dispatcharr's default M3U/XMLTV output AND the Xtream
    Codes API bind the EPG to a channel BY its (integer) channel number, so a
    number that MOVES pairs the wrong programme with a channel after the slate
    changes (see #117). A number derived from kickoff time never moves, so the
    guide binds correctly with no client configuration, in either output mode.

      number = virtual_base
             + minutes-since-origin (in the user's local tz) * TIEBREAK_SLOTS
             + (stable hash of marker) % TIEBREAK_SLOTS

    The number increases monotonically with kickoff time, so the channel list
    sorts strictly by day then start time (today's/soonest games first) — which
    is exactly the "renumber every refresh, by start time" behaviour, achieved
    WITHOUT any game's number ever changing: finished games drop off and new
    games slot into their time-position on their own. The per-minute tiebreak
    gives simultaneous kickoffs distinct, stable slots. Integer throughout, so
    the Xtream Codes integer-channel-number requirement is satisfied natively
    (no flooring/rounding by the XC layer, which is what scrambled the prior
    fractional scheme).
    """
    local = start_utc.astimezone(tz)
    day_offset = max(0, (local.date() - CHANNEL_NUMBER_ORIGIN).days)
    minutes_since_origin = day_offset * 1440 + local.hour * 60 + local.minute
    slots = CHANNEL_NUMBER_TIEBREAK_SLOTS
    return (
        virtual_base
        + minutes_since_origin * slots
        + (stable_hash_int(marker) % slots)
    )


def extract_game_number_after_marker(headline: str, marker: str) -> Optional[int]:
    """Extract the integer game number that immediately follows `marker` in
    `headline`. Strips an "(if necessary)" trailer that ESPN attaches to
    Game-3 placeholders on best-of-3 series. Returns None on any parse
    failure so the caller can skip the event gracefully.

    Shared between NCAA Baseball / Softball playoff sources where ESPN
    encodes the game index in headlines like
    "...Super Regional - Game 3 (if necessary)" or
    "...Championship Final - Game 2".

    Example:
        >>> extract_game_number_after_marker(
        ...     "NCAA Baseball Championship - Auburn Super Regional - Game 3 (if necessary)",
        ...     "Super Regional - Game ",
        ... )
        3
    """
    if not headline or marker not in headline:
        return None
    tail = headline.split(marker, 1)[1].strip()
    digits = []
    for ch in tail:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    if not digits:
        return None
    try:
        return int("".join(digits))
    except ValueError:
        return None


def poisson_sample(lam: float, rng: random.Random) -> int:
    """Draw one Poisson(lam) sample via Knuth's algorithm. Pure-Python (no
    numpy dependency, the plugin runs in Dispatcharr's lean container).

    Shared by every points-based / goals-based sport source. Soccer uses
    lam ~ 1.4 (goals/match). NCAAF / NCAAM use lam ~ 28 / 75 (points/team).
    Knuth's algorithm is O(lam): for lam > ~50 a normal-approximation
    would be measurably faster, but the per-refresh sim cost is dominated
    by season iteration, not the inner Poisson, so we keep one
    implementation for simplicity. Swap in a normal approx here if profiling
    shows it matters.
    """
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= rng.random()
    return k - 1


# Soccer-style trailing club tags. football-data.org canonical names end in
# 'FC' / 'AFC' (e.g. 'Brentford FC', 'Wrexham AFC') but provider channel and
# program titles usually drop the tag. Both scoring.format_channel_name and
# matcher._team_keywords need to know these to match against shortened forms.
TEAM_SUFFIX_TOKENS = ("afc", "fc", "cf", "sc")

# Common second-word soccer suffixes that look distinctive but aren't:
# 'United' alone false-matches Manchester/West Ham/Newcastle/Leeds/Sheffield;
# 'City' alone false-matches Manchester/Leicester/Hull/Cardiff/Swansea/etc.
# matcher._team_keywords excludes these from the last-word fallback so
# 'Manchester United' never gets reduced to 'United' as a match key (which
# would collide with channels like 'Brentford v West Ham United').
GENERIC_TEAM_SECOND_WORDS = (
    "united", "city", "town", "county", "athletic", "albion", "rovers",
    "forest", "wanderers", "rangers", "palace", "hotspur", "villa",
    "wednesday", "real",
)


# ---------- playoff-series rendering ----------
#
# The sport-agnostic `extra["series"]` schema that best-of-N sources populate
# (NHL today; NBA / MLB / NCAA series can follow the same shape). Both the
# deterministic EPG description (plugin._build_description) and the LLM context
# (llm_descriptions.build_llm_context) render from these helpers, so the wording
# and the model's grounding never drift apart:
#
#   {
#     "title":       str,   # "Stanley Cup Final" (optional, may be "")
#     "game_number": int,   # this game's number within the series (1-based)
#     "best_of":     int,   # series length (7 for an NHL round)
#     "home_wins":   int,   # wins by THIS game's home team
#     "away_wins":   int,   # wins by THIS game's away team
#     "results":     [ {game_number, home, away, home_goals, away_goals, ot}, ... ],
#   }
#
# These functions are the ONLY place series state becomes prose. Grounding the
# LLM with `series_record_text` is what stops Haiku inventing "facing
# elimination" on a Game 1 (the bug these helpers exist to kill): do not write
# a parallel series-phrasing path in the description or context builders.


def series_phase_text(series: Optional[Dict[str, Any]]) -> str:
    """Headline phrase for a series game: "Stanley Cup Final, Game 2 of 7", or
    "Game 2 of 7" when no series title is set. Returns "" when the series dict
    lacks a usable game_number / best_of."""
    if not isinstance(series, dict):
        return ""
    gnum = series.get("game_number")
    best_of = series.get("best_of")
    if not isinstance(gnum, int) or not isinstance(best_of, int) or best_of <= 0:
        return ""
    core = f"Game {gnum} of {best_of}"
    title = (series.get("title") or "").strip()
    return f"{title}, {core}" if title else core


def series_record_text(
    series: Optional[Dict[str, Any]], home: str, away: str
) -> str:
    """One-line series record: "Series tied 1-1" or
    "Carolina Hurricanes lead the series 2-1". Returns "" when win counts are
    missing. `home` / `away` are this game's team names (the schema's win
    counts are already keyed to them)."""
    if not isinstance(series, dict):
        return ""
    hw = series.get("home_wins")
    aw = series.get("away_wins")
    if not isinstance(hw, int) or not isinstance(aw, int):
        return ""
    if hw == aw:
        return f"Series tied {hw}-{aw}"
    leader, hi, lo = (home, hw, aw) if hw > aw else (away, aw, hw)
    return f"{leader} lead the series {hi}-{lo}"


def series_result_lines(series: Optional[Dict[str, Any]]) -> List[str]:
    """Per-completed-game recap lines, oldest first, e.g.
    ["Game 1: Carolina Hurricanes 3, Vegas Golden Knights 2 (OT)", ...].
    Returns [] when no results are recorded. Skips malformed result rows
    rather than raising: a bad recap should degrade to the record line, never
    break the refresh."""
    out: List[str] = []
    if not isinstance(series, dict):
        return out
    for r in series.get("results") or []:
        if not isinstance(r, dict):
            continue
        gn = r.get("game_number")
        home = r.get("home")
        away = r.get("away")
        hg = r.get("home_goals")
        ag = r.get("away_goals")
        if None in (gn, home, away, hg, ag):
            continue
        tag = " (OT)" if r.get("ot") else ""
        out.append(f"Game {gn}: {home} {hg}, {away} {ag}{tag}")
    return out


# ---------- group-stage rendering ----------
#
# The soccer analog of the series schema above, for international-tournament
# group stages (World Cup, EUROs). A group is a 4-team mini-league where the
# concrete facts -- who has played whom, the current points table, what it
# takes to advance -- are exactly what the LLM needs to stop inventing
# narratives ("shock opening loss", "needs a win to survive"). Populated by
# `sources/soccer.py:GroupStageSoccerSource.fetch_upcoming`; rendered by BOTH
# `plugin._build_description` and `llm_descriptions.build_llm_context`, so the
# deterministic prose and the model's grounding stay in lockstep (same
# contract the series helpers above hold).
#
#   {
#     "tournament":      str,   # "FIFA World Cup" (competition label)
#     "group":           str,   # "C" (group letter)
#     "matchday":        int,   # this game's matchday within the group (1-3)
#     "matchdays_total": int,   # group length (3: each team plays 3)
#     "standings": [            # current table, FINISHED matches only, in
#                               # finishing order (0 = top of group)
#       {position, name, played, points, goal_difference}, ...
#     ],
#     "results": [              # FINISHED group matches, oldest first
#       {home, away, home_goals, away_goals}, ...
#     ],
#     "advance":         str,   # "Top 2 of 4 advance, plus the best ..." (rule)
#   }
#
# These are the ONLY place group state becomes prose. As with the series
# helpers: do not author a parallel group-phrasing path in the description or
# context builders.


def group_phase_text(group_stage: Optional[Dict[str, Any]]) -> str:
    """Headline phrase for a group-stage game: "FIFA World Cup Group C,
    Matchday 2 of 3". Falls back gracefully when pieces are missing
    ("Group C" alone, or "" when there's no group letter)."""
    if not isinstance(group_stage, dict):
        return ""
    group = (group_stage.get("group") or "").strip()
    if not group:
        return ""
    tournament = (group_stage.get("tournament") or "").strip()
    head = f"{tournament} Group {group}" if tournament else f"Group {group}"
    md = group_stage.get("matchday")
    total = group_stage.get("matchdays_total")
    if isinstance(md, int) and isinstance(total, int) and total > 0:
        return f"{head}, Matchday {md} of {total}"
    return head


def group_standings_lines(group_stage: Optional[Dict[str, Any]]) -> List[str]:
    """Per-team table lines, top of group first, e.g.
    ["#1 Argentina - 6 pts, 2 played, +3 GD", ...]. Returns [] when no
    standings are recorded. Skips malformed rows rather than raising."""
    out: List[str] = []
    if not isinstance(group_stage, dict):
        return out
    for i, row in enumerate(group_stage.get("standings") or []):
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if not name:
            continue
        pos = row.get("position")
        pos = pos if isinstance(pos, int) else i + 1
        pts = row.get("points")
        played = row.get("played")
        parts: List[str] = []
        if isinstance(pts, int):
            parts.append(f"{pts} pts")
        if isinstance(played, int):
            parts.append(f"{played} played")
        gd = row.get("goal_difference")
        if isinstance(gd, int):
            parts.append(f"{gd:+d} GD")
        suffix = " - " + ", ".join(parts) if parts else ""
        out.append(f"#{pos} {name}{suffix}")
    return out


def group_results_lines(group_stage: Optional[Dict[str, Any]]) -> List[str]:
    """Per-completed-match recap lines for the group, oldest first, e.g.
    ["Argentina 2-1 Saudi Arabia", ...]. Returns [] when no results are
    recorded. Skips malformed rows rather than raising."""
    out: List[str] = []
    if not isinstance(group_stage, dict):
        return out
    for r in group_stage.get("results") or []:
        if not isinstance(r, dict):
            continue
        home = r.get("home")
        away = r.get("away")
        hg = r.get("home_goals")
        ag = r.get("away_goals")
        if None in (home, away, hg, ag):
            continue
        out.append(f"{home} {hg}-{ag} {away}")
    return out


def group_advance_text(group_stage: Optional[Dict[str, Any]]) -> str:
    """The group's advancement rule sentence, or "" when not set."""
    if not isinstance(group_stage, dict):
        return ""
    return (group_stage.get("advance") or "").strip()
