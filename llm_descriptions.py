"""Optional LLM-rewritten EPG descriptions (Claude Haiku 4.5 by default).

When `llm_descriptions_enabled` is on AND a valid Anthropic API key is present,
the apply step calls `llm_describe_or_fallback` per matched game and writes the
returned prose to `ProgramData.description` instead of the deterministic
`_build_description` output.

Failure modes (missing key, API error, non-200, malformed body, network
timeout, JSON decode) all return `fallback_description` unchanged. The cache
file is a sidecar: cache.json's structured fields (score, breakdown,
score_notes) stay deterministic and untouched.

The HTTP call is intentionally `urllib` (stdlib only): Dispatcharr does not
ship the `anthropic` SDK and we do not want to add a transitive dependency
for a 30-line wrapper.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple

from ._util import (
    SEED_PLAYED_THRESHOLD,
    current_standings_table,
    group_advance_text,
    group_phase_text,
    group_results_lines,
    group_standings_lines,
    is_bottom_outcome,
    ordinal,
    series_phase_text,
    series_record_text,
    series_result_lines,
    trusted_impact_narratives,
)
from .honours import honours_lines

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_VERSION = "2023-06-01"
ANTHROPIC_MAX_TOKENS = 200
ANTHROPIC_TIMEOUT_S = 30

# Lifted verbatim from /tmp/haiku_demo.py (the four-game validation set in
# TUNING_REPORT.md, sample outputs section), with one addition: the explicit
# plain-text rule. The demo run produced one markdown emphasis (`*must*`) that
# would render as literal asterisks in TiviMate / Plex / Jellyfin.
SYSTEM_PROMPT = """You write 2-3 sentence previews for a personal TV guide.
Goal: make a casual fan want to watch this match.

Hard rules:
- No numbers like "★9.9" or "rank=8.12". Natural prose only.
- Don't mention "favorite", "stakes", "leverage", "score breakdown". Describe
  what they mean (the team's predicament, what's at stake) instead.
- 2-3 sentences. Max ~50 words.
- Skip generic openings ("This match features..."). Drop the reader into the
  stakes.
- If a favorite team for this user is playing, that's a "personal interest":
  ground the preview in their angle.
- Plain text only. Output ONLY the preview itself. No markdown, no headings,
  no asterisks, no bullet points, no lists, no preamble, no sign-off.
- ALWAYS write the preview. Thin context means write a SIMPLER preview, never
  a refusal and never a request for more data. Some fixtures legitimately
  carry nothing but two team names, a competition and a kickoff time, and that
  is enough for two sentences about the matchup itself. NEVER reply with
  "I don't have", "I need more information", "Could you provide", or any other
  message addressed to whoever is running this. There is no one to answer you:
  your reply is written verbatim into a TV guide that a viewer reads. Silence
  about a fact you were not given is correct; asking for it is not.
- GROUND EVERY FACT in the lines above. A team's record, points, group/league
  position, who they have already played, and any prior result must come from
  the standings, results, group, or series lines provided. If a fact is not
  listed, you do not know it: never invent a scoreline, a win or loss, a points
  total, or a standing. Do not say a team "lost their opener", "needs a win to
  survive", or "has yet to score" unless the given lines make it literally true.
- Do NOT fabricate playoff series facts. The only series facts you may use are
  the "Series", "Series record", and "Results so far" lines above. Never invent
  a series score, a game number, or a best-of-N length.
- Do not write "facing elimination", "must win to force a Game 7", or "their
  season ends tonight" unless the given Series record makes it literally true
  (a team one loss from elimination in a best-of-N). Never assert a series
  standing or game number.
- "Win or go home" framing requires EVIDENCE THAT THIS IS A KNOCKOUT: a Series
  line, a "Tournament round" line, or a competition that is plainly a cup. The
  ABSENCE of series lines is NOT that evidence, because a league fixture has
  none either. A Premier League match in October is not an elimination game,
  and calling it one is exactly the kind of invented drama these rules exist
  to prevent.
- For group-stage matches, the "Current group standings", "Group results so
  far", and "Advancement" lines are the actual current state. Reason about
  advancement only from them (e.g. a team already on 6 points after 2 games is
  through; a team that has played both games and sits last is in trouble).
  Before any game has been played in the group, there are no results: do not
  imply otherwise.
- NEVER state a league position, a points total, or a gap that is not written
  in the lines above. "Where the two teams stand this season" already contains
  each team's position, points, which outcome bands they are in, and the exact
  points to the leader and to the relegation zone. Use those numbers verbatim.
  Do not recompute them, and do not infer a position from where a team appears
  in the table slice.
- Do NOT claim a team is near, in, or clear of any zone (relegation, European
  places, playoff spots, the title race) unless a line above says so. If the
  posture line does not mention the relegation zone, do not mention it either.
- "Last season's final table" is LAST season. Never describe it as the current
  standing. It is the right thing to lean on when few matchdays have been
  played, but say so as history ("last season's runners-up", "after finishing
  17th last year"), never as where they sit now.
- When "Season progress" says the table is barely formed, do not write about
  cushions, leads, gaps or hierarchy as though the table were settled. Two or
  three games is noise. Reach for last season's finish and the previous
  meetings instead.
- A team marked "newly promoted" has no LAST-SEASON record in this league.
  It may still have a current-season record, and that record is in the lines
  above and is usable. What you must not do is give it a position or a finish
  for last season, or describe it as having slipped or climbed from one.
- "Previous meetings" are real results, and each one names its winner in
  brackets. Use that verdict; do not work out who won from the scoreline
  yourself, and do not reverse it. Do not invent any other past meeting, and
  do not describe a run of form beyond what is listed.
- Say NOTHING about last season unless a "Last season" line is present. Not
  where they finished, not whether they were ranked, not whether they made a
  postseason. If no such line appears you have no last-season information at
  all, and stating any is inventing it, however plausible it sounds.
- NEVER name a conference or division that is not written above. Conference
  membership changes and you will get it wrong: "Pac-12", "Patriot League" and
  "Ivy League" were each attached to teams that are in none of them. The
  competition named at the top of the context is the only competition you may
  name.
- Do NOT describe WHERE IN THE SEASON this game falls unless a "Season
  progress" line is present. With no such line you do not know whether this is
  the opener or the run-in, so "down the stretch", "the home stretch" and
  "early season" are all guesses.
- When the only lines you have are the match, the competition and perhaps a
  ranking, write about exactly those: who is playing, what the fixture is, and
  what the ranking implies. Two honest sentences beat three padded with
  invented context.
- Do NOT name a city, stadium or region that is not written above. If a
  "Venue" or "Neutral site at" line is present, that is where the game is
  played; if none is present, do not say where it is. A neutral-site game is
  NOT at either team's home ground, so placing it in either team's town is
  always wrong.
- Rank adjectives must match the number. "Top-ranked" and "number one" mean
  ranked #1 and nothing else; a #4 team is "fourth-ranked" or "a top-five
  side". Do not inflate a ranking you were given.
- "Outcome bands in play" lists what is mathematically still reachable, NOT
  what is urgent. Early in a season everything is reachable and naming a band
  says nothing: "both teams chasing bowl eligibility" is true of every team in
  week 1. Let "Season progress" decide how to use them. Late in a season a
  band is real drama; at the opener it is not a story, so write about the
  matchup, the previous meetings, or last season instead.
- Match the framing to where the season is. At the opener there is no form
  and no record yet, so do not write "needs a win to stay in the hunt" or
  "must start fast to reach the postseason". In the run-in you may be concrete
  about what a result would settle, BUT ONLY where a posture line actually
  puts a band within reach: two safe mid-table sides in April have little at
  stake, and saying otherwise is an invented claim like any other. When
  nothing is on the line, say what the game IS rather than inventing what it
  decides.
- When a "Where the two teams stand this season" line gives a record and a
  distance to a threshold ("5-4. 3 games left. 1 more win for bowl eligible"),
  use those numbers. Never state a record, a number of games remaining, or a
  distance to a threshold that is not in those lines.
"""

# How many standings rows above/below each team to include in the context.
# 2 captures the immediate threshold (e.g. relegation cutoff is one row below
# 17th place) without flooding the prompt with bottom-table teams.
_STANDINGS_CONTEXT_WINDOW = 2


# Type alias: the seam used by tests. A `Caller` takes (context, api_key,
# model) and returns the assistant text. Production wires this to
# `_call_anthropic`; tests pass a stub.
Caller = Callable[[str, str, str], str]


def _format_team_row(row: Dict[str, Any]) -> str:
    name = row.get("name") or "?"
    pos = row.get("position") or "?"
    pts = row.get("points")
    played = row.get("played")
    qualifiers: List[str] = []
    if pts is not None:
        qualifiers.append(f"{pts} pts")
    if played is not None:
        qualifiers.append(f"{played} games played")
    base = f"  - #{pos} {name}"
    if qualifiers:
        return base + " (" + ", ".join(qualifiers) + ")"
    return base


def _table_by_name(table: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Name → row. Names come from the same FD.org payload as the fixture, so
    exact matching is correct; see `sources.soccer.build_h2h_entries` for why
    fuzzy matching is forbidden here."""
    return {r.get("name"): r for r in table if r.get("name")}


def _max_played(table: List[Dict[str, Any]]) -> int:
    """Most games any team in the table has played. 0 for an empty or
    pre-season table."""
    return max(
        (r.get("played") or 0 for r in table if isinstance(r.get("played"), int)),
        default=0,
    )


# Where a season sits, as a fraction of its scheduled length. The prose a
# viewer wants is completely different in each third: early, nothing has been
# decided and last season is the only form guide; late, the table IS the story
# and a single result can settle a band.
_EARLY_SEASON_FRACTION = 0.25
_LATE_SEASON_FRACTION = 0.75


def season_phase(played: int, total: Optional[int]) -> str:
    """"opening" / "early" / "midseason" / "late" for a season `played` games
    in, or "" when there is nothing to judge from.

    Separate from the raw counts because the counts alone did not stop the
    model treating a week-1 game as a run-in: it needs to be TOLD that nothing
    has been decided yet. See #209 and Jake's note that "chasing bowl
    eligibility" is vacuous in September and the whole story in November.
    """
    if played < 0:
        return ""
    if played == 0:
        return "opening"
    # An inconsistent denominator (a cup fixture stamped with a league's season
    # length, a rescheduled backlog) would give a fraction above 1.0 and label
    # a mid-season game as the run-in. Treat the total as unknown instead.
    if not total or total <= 0 or played > total:
        return "early" if played < SEED_PLAYED_THRESHOLD else ""
    fraction = played / total
    if fraction < _EARLY_SEASON_FRACTION:
        return "early"
    # Inclusive: 9 of 12 games is three to play with a bowl on the line, which
    # is unambiguously the run-in. An exclusive bound put it in midseason.
    if fraction >= _LATE_SEASON_FRACTION:
        return "late"
    return "midseason"


_PHASE_GUIDANCE = {
    "opening": (
        "Nothing has been decided and no team has a record. Season-long "
        "outcomes are a whole season away: do not frame them as immediate "
        "stakes. Last season's finish and the previous meetings are the only "
        "real form guide."
    ),
    "early": (
        "Very little has been decided. Positions and records are a small "
        "sample, and season-long outcomes are still distant: do not frame "
        "them as urgent. Last season's finish still carries information."
    ),
    "midseason": (
        "Enough has been played for the table and the records to mean "
        "something, but there is still time to recover from a bad result."
    ),
    "late": (
        "The run-in. Records and positions are close to final, and the "
        "season-long outcomes below are genuinely on the line in this game."
    ),
}


def season_progress_line(
    played: int, total: Any, unit: str = "matchdays"
) -> List[str]:
    """Where this game sits in its season, as one or two lines.

    Takes the counts directly rather than a table, so a win-count sport
    (college football, which has a week number and no league table) gets the
    same treatment as a league. Returns [] when there is nothing to say.
    """
    if played < 0:
        return []
    total_i = total if isinstance(total, int) and total > 0 else None
    # Inconsistent inputs (a cup fixture stamped with a league's season length,
    # a rescheduled backlog) can put played past total. Rendering "20 of 12
    # matchdays played" is worse than dropping the denominator, and a fraction
    # above 1.0 would also mislabel the phase.
    if total_i is not None and played > total_i:
        total_i = None
    phase = season_phase(played, total_i)
    if played <= 0:
        base = f"Season progress: no {unit} played yet, this is the opener."
    elif total_i:
        base = f"Season progress: {played} of {total_i} {unit} played."
    else:
        base = f"Season progress: {played} {unit} played."
    guidance = _PHASE_GUIDANCE.get(phase)
    # With no season length there is no phase to judge, but the count itself is
    # still worth stating: suppressing the whole line meant 3 matchdays got a
    # line and 5 got nothing, purely because the second had no phase.
    return [base, guidance] if guidance else [base]


def _played_from_record(extra: Dict[str, Any]) -> Optional[int]:
    """Games played, taken from whichever team's record is present.

    Preferred over `week - 1` because a team can have a bye or an unplayed
    postponement, so the week number overstates games played. Falls back to
    the week only when neither side has a record yet.
    """
    best: Optional[int] = None
    for key in ("record_home", "record_away"):
        rec = extra.get(key)
        if not isinstance(rec, dict):
            continue
        w, l = rec.get("wins"), rec.get("losses")
        if isinstance(w, int) and isinstance(l, int):
            best = max(best or 0, w + l)
    return best


def _record_text(rec: Optional[Dict[str, Any]]) -> Optional[str]:
    """"5-4" for a record dict, or None when the team has not played."""
    if not isinstance(rec, dict):
        return None
    w, l = rec.get("wins"), rec.get("losses")
    if not isinstance(w, int) or not isinstance(l, int):
        return None
    return f"{w}-{l}"


def win_count_posture_lines(
    g: Dict[str, Any], thresholds: Optional[List[Any]], total_games: Any
) -> List[str]:
    """For a win-count sport: each team's record, and its exact distance from
    every win threshold that is still reachable.

    This is the fix for the emptiest sentence #209 turned up: "both teams
    chasing bowl eligibility" in week 1, which is true of all 130 teams and
    tells a viewer nothing. What a viewer actually wants is "5-4, one win from
    a bowl with three to play", and that is arithmetic, not judgement, so it
    is computed here rather than left to the model.

    A threshold already MET is stated as met; one that can no longer be
    reached is omitted, because a preview should not dangle an outcome that is
    already gone.
    """
    total_i = total_games if isinstance(total_games, int) and total_games > 0 else None
    out: List[str] = []
    for team, key in ((g.get("home"), "record_home"), (g.get("away"), "record_away")):
        if not team:
            continue
        rec = (g.get("extra") or {}).get(key)
        text = _record_text(rec)
        wins = rec.get("wins") if isinstance(rec, dict) else None
        losses = rec.get("losses") if isinstance(rec, dict) else None
        # A 0-0 record and a negative one both mean "no usable record": the
        # producer emits None for a team with no completed games, so 0-0 can
        # only arrive from a malformed row, and reporting it as a record
        # contradicts the opener rule in the same prompt.
        if (
            text is None
            or not isinstance(wins, int) or not isinstance(losses, int)
            or wins < 0 or losses < 0
            or wins + losses == 0
        ):
            out.append(f"  - {team}: has not played yet this season.")
            continue
        played = wins + losses
        # Same inconsistency guard as season_progress_line: a record showing
        # more games than the season length means the length is wrong, and
        # "0 games left" would then be asserted as fact.
        left = total_i - played if total_i is not None and total_i >= played else None
        parts = [f"  - {team}: {text}"]
        if left is not None:
            parts.append(f"{left} game{'s' if left != 1 else ''} left")
        for entry in thresholds or []:
            try:
                cutoff, label, _w = entry
            except (TypeError, ValueError):
                continue
            if not isinstance(cutoff, int):
                continue
            needed = cutoff - wins
            if needed <= 0:
                parts.append(f"already has {label.replace('_', ' ')}")
            elif left is None:
                continue
            elif needed <= left:
                parts.append(
                    f"{needed} more win{'s' if needed != 1 else ''} for "
                    f"{label.replace('_', ' ')}"
                )
            # needed > left: unreachable, say nothing rather than dangle it.
        out.append(". ".join(parts) + ".")
    return out


def _band_membership(position: int, thresholds: List[Any], table_size: int) -> List[str]:
    """Labels of the outcome bands this position currently sits in.

    `thresholds` are (cutoff, label, weight) triples for format="league",
    where a TOP band means position <= cutoff and a BOTTOM band (relegation /
    demotion / drop) means position > cutoff. That asymmetry mirrors
    `sources.soccer.SoccerSource._is_bottom_outcome`; getting it backwards
    reports a mid-table side as relegation-bound, which is exactly the false
    claim in #209 ("Osasuna sits just two points above the relegation zone"
    while 6th).

    Top bands are NESTED, not parallel: a 3rd-placed side satisfies both the
    UCL (top 4) and Europa (top 7) cutoffs. Reporting both reads as "Arsenal
    are in the Europa places", which is wrong in the way that matters. Only
    the TIGHTEST satisfied top band is returned. Bottom bands are reported
    whenever they apply, since a team is either in the drop zone or is not.
    """
    best_top: Optional[Tuple[int, str]] = None
    bottom: List[str] = []
    for entry in thresholds or []:
        try:
            cutoff, label, _weight = entry
        except (TypeError, ValueError):
            continue
        if not isinstance(cutoff, int):
            continue
        if is_bottom_outcome(label):
            if position > cutoff:
                bottom.append(label)
        elif position <= cutoff:
            if best_top is None or cutoff < best_top[0]:
                best_top = (cutoff, label)
    out: List[str] = []
    if best_top is not None:
        out.append(best_top[1])
    out.extend(bottom)
    return out


def _rows_by_position(table: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """position -> one representative row, preferring a row that HAS points.

    FD.org shares a position between tied teams, so several rows can claim the
    same position. A plain last-wins dict comprehension therefore picked an
    arbitrary one, and if that one happened to be missing `points` the gap
    computed against it silently vanished, making the output depend on row
    order in the payload. Prefer a row with usable points so the arithmetic
    stays available whenever ANY tied row can supply it.
    """
    out: Dict[int, Dict[str, Any]] = {}
    for row in table:
        pos = row.get("position")
        if not isinstance(pos, int):
            continue
        existing = out.get(pos)
        if existing is None or (
            not isinstance(existing.get("points"), int)
            and isinstance(row.get("points"), int)
        ):
            out[pos] = row
    return out


def _first_row_below(
    by_pos: Dict[int, Dict[str, Any]], cutoff: Optional[int]
) -> Optional[Dict[str, Any]]:
    """The highest-placed row strictly below `cutoff`, i.e. the first team in
    the drop zone.

    DO NOT look up `cutoff + 1` directly. FD.org uses competition ranking, so
    tied teams SHARE a position and the next one is SKIPPED: a real Premier
    League table on 2026-09-06 had two clubs on 17 and then jumped to 19, with
    no row at 18 at all. An exact lookup silently returned None and every
    "N points clear of relegation" line vanished from the prompt.
    """
    if not isinstance(cutoff, int):
        return None
    below = [p for p in by_pos if p > cutoff]
    return by_pos[min(below)] if below else None


def _last_row_at_or_above(
    by_pos: Dict[int, Dict[str, Any]], cutoff: Optional[int]
) -> Optional[Dict[str, Any]]:
    """The lowest-placed row at or above `cutoff`, i.e. the last safe team.

    Tie-tolerant for the same reason as `_first_row_below`: an exact lookup of
    `cutoff` misses when the position is shared and skipped.
    """
    if not isinstance(cutoff, int):
        return None
    at_or_above = [p for p in by_pos if p <= cutoff]
    return by_pos[max(at_or_above)] if at_or_above else None


def _gap_text(
    points: Any,
    other: Optional[Dict[str, Any]],
    phrase: str,
    level_phrase: str,
) -> Optional[str]:
    """"N pts <phrase>" for an exact points difference, or `level_phrase` when
    the two are level. None when either side lacks a points value.

    The level case needs its own wording rather than "level on points " +
    phrase: that composed into "level on points behind the leader" for a
    co-leader and "level on points clear of the relegation zone" for a team
    level with the first side in it, both of which state the opposite of the
    situation.

    Computed here rather than left to the model: every false "two points clear
    of the drop zone" in #209 was the model doing this subtraction against
    rows it had to guess at.
    """
    if other is None or not isinstance(points, int):
        return None
    other_pts = other.get("points")
    if not isinstance(other_pts, int):
        return None
    diff = points - other_pts
    if diff == 0:
        return level_phrase
    return f"{abs(diff)} pt{'s' if abs(diff) != 1 else ''} {phrase}"


def _with_places(gap: Optional[str], other: Optional[Dict[str, Any]], pos: int) -> Optional[str]:
    """Fold the PLACES gap into a points gap, e.g. "2 pts and 5 places clear
    of the relegation zone".

    Early in a season the whole table is bunched, so a points gap alone reads
    as far tighter than the standing is. Measured on the live guide
    2026-09-06: Marseille sat 11th of 18 and two points clear of the drop
    zone on matchday 3, and the preview called them "just outside the drop
    zone". Two points IS small; five places is not, and the second number is
    what stops the first being misread.
    """
    if not gap or other is None:
        return gap
    other_pos = other.get("position")
    if not isinstance(other_pos, int):
        return gap
    places = abs(other_pos - pos)
    if places <= 1:
        return gap
    label = f"{places} places"
    # "2 pts clear of X" -> "2 pts and 5 places clear of X"; a level gap has no
    # points count to join, so it gets the places clause appended instead.
    for connector in (" clear of ", " from ", " with "):
        if connector in gap:
            head, tail = gap.split(connector, 1)
            return f"{head} and {label}{connector}{tail}"
    return f"{gap} ({label})"


def team_posture_lines(
    table: List[Dict[str, Any]],
    teams: List[str],
    thresholds: Optional[List[Any]] = None,
) -> List[str]:
    """One precomputed line per team: position, points, which bands they are
    in, and exact points gaps to the leader and to the relegation cutoff.

    This is the primary defence against #209's failure mode. The model is bad
    at reading a sliced table and computing "how far from the drop zone"; it
    is fine at repeating a sentence. So compute the arithmetic and hand it
    over as prose. Returns [] when there is no table to reason from.
    """
    if not table:
        return []
    by_name = _table_by_name(table)
    by_pos = _rows_by_position(table)
    leader = by_pos.get(min(by_pos)) if by_pos else None
    # The highest POSITION, not len(table). FD.org shares a position between
    # tied teams and skips the next, so positions stay bounded by the team
    # count, but a partial table would otherwise render "3rd of 2".
    size = max(by_pos) if by_pos else len(table)

    # The first relegation place, if this league has a bottom band.
    releg_cutoff = None
    for entry in thresholds or []:
        try:
            cutoff, label, _w = entry
        except (TypeError, ValueError):
            continue
        if isinstance(cutoff, int) and is_bottom_outcome(label):
            releg_cutoff = cutoff
            break
    first_releg = _first_row_below(by_pos, releg_cutoff)
    last_safe = _last_row_at_or_above(by_pos, releg_cutoff)

    out: List[str] = []
    for team in teams:
        row = by_name.get(team)
        if row is None:
            out.append(f"  - {team}: not in this season's table.")
            continue
        pos = row.get("position")
        pts = row.get("points")
        played = row.get("played")
        if not isinstance(pos, int):
            out.append(f"  - {team}: no league position listed.")
            continue
        head = f"  - {team}: {ordinal(pos)} of {size}"
        if isinstance(pts, int):
            head += f", {pts} pt{'s' if pts != 1 else ''}"
            if isinstance(played, int):
                head += f" from {played} game{'s' if played != 1 else ''}"
        parts = [head]
        bands = _band_membership(pos, thresholds or [], size)
        if bands:
            parts.append(f"Currently in: {', '.join(bands)}")
        if leader is not None and row is not leader:
            g = _gap_text(pts, leader, "behind the leader",
                          "level on points with the leader")
            if g:
                parts.append(g)
        # A safe team is measured against the top of the drop zone; a team
        # already IN it is measured against the last safe place. Measuring a
        # relegation-bound side against the zone it is already in produced
        # "2 pts into the relegation zone", which reads as a depth rather than
        # as the distance out and is the number a viewer actually wants.
        if isinstance(releg_cutoff, int):
            if pos <= releg_cutoff:
                if first_releg is not None and row is not first_releg:
                    g = _gap_text(
                        pts, first_releg, "clear of the relegation zone",
                        "level on points with the drop zone")
                    g = _with_places(g, first_releg, pos)
                    if g:
                        parts.append(g)
            elif last_safe is not None and row is not last_safe:
                g = _gap_text(pts, last_safe, "from safety",
                              "level on points with safety")
                g = _with_places(g, last_safe, pos)
                if g:
                    parts.append(g)
        out.append(". ".join(parts) + ".")
    return out


def prev_season_lines(
    prev_table: List[Dict[str, Any]], teams: List[str]
) -> List[str]:
    """One line per team describing LAST season's finish, including an
    explicit "newly promoted" for a team absent from it.

    Saying the absence out loud is the point. In #209 the seeded (previous
    season) table was the only table the model saw, and a promoted side
    (Monza, Malaga, Troyes) simply had no row, so the model invented a league
    position for them. An explicit "did not play in this league last season"
    is a fact the model can use instead of a hole it has to fill.
    """
    if not prev_table:
        return []
    by_name = _table_by_name(prev_table)
    size = len(prev_table)
    out: List[str] = []
    for team in teams:
        row = by_name.get(team)
        if row is None:
            out.append(f"  - {team}: did not play in this league last season (newly promoted).")
            continue
        pos = row.get("position")
        pts = row.get("points")
        if not isinstance(pos, int):
            continue
        line = f"  - {team}: finished {ordinal(pos)} of {size}"
        if isinstance(pts, int):
            line += f" ({pts} pts)"
        out.append(line + ".")
    return out


def h2h_tally_line(
    entries: List[Dict[str, Any]], home: str, away: str
) -> str:
    """The head-to-head record across the listed meetings, as one sentence.

    Naming the winner on each row was not enough. Given two meetings, one won
    by each side, the model still wrote "last season's derby victories" and
    attributed them to the club that had won ONE of the two. Reading a row is
    a different task from aggregating several rows, and the second is
    arithmetic, so it is done here. See #209.

    Returns "" for fewer than two meetings, where the rows already say it.
    """
    if len(entries or []) < 2:
        return ""
    hw = aw = draws = 0
    for e in entries:
        hg, ag = e.get("home_goals"), e.get("away_goals")
        if not isinstance(hg, int) or not isinstance(ag, int):
            continue
        if hg == ag:
            draws += 1
            continue
        winner = e.get("home") if hg > ag else e.get("away")
        if winner == home:
            hw += 1
        elif winner == away:
            aw += 1
    if hw + aw + draws < 2:
        return ""
    if hw == aw and draws == 0:
        return f"Across those meetings: one win each for {home} and {away}."
    parts = [f"{home} {hw}", f"{away} {aw}"]
    if draws:
        parts.append(f"{draws} draw{'s' if draws != 1 else ''}")
    return "Across those meetings: " + ", ".join(parts) + "."


def h2h_lines(entries: List[Dict[str, Any]]) -> List[str]:
    """Prior meetings, one line each, most recent first. Entries come from
    `sources.soccer.build_h2h_entries` and are already filtered to this exact
    pair and to FINISHED matches with a real scoreline.

    THE WINNER IS NAMED EXPLICITLY, and that is not decoration. Measured on the
    live guide 2026-09-06: given "Santos FC 1-2 SC Internacional" the model
    wrote "a Santos side that beat them earlier this season", inverting the
    result. A bare scoreline asks the model to work out who won from which
    number sits on which side of a hyphen, and it got that wrong on one of the
    three head-to-head claims in the slate. Same principle as
    `team_posture_lines`: compute the fact, do not make the model derive it.
    """
    out: List[str] = []
    for e in entries or []:
        hg, ag = e.get("home_goals"), e.get("away_goals")
        if not isinstance(hg, int) or not isinstance(ag, int):
            continue
        home, away = e.get("home", "?"), e.get("away", "?")
        date = e.get("date") or "?"
        season = e.get("season")
        when = f"{date} ({season})" if season else date
        if hg > ag:
            verdict = f"{home} won"
        elif ag > hg:
            verdict = f"{away} won"
        else:
            verdict = "a draw"
        out.append(f"  - {when}: {home} {hg}-{ag} {away} ({verdict})")
    return out


def poll_rank_lines(g: Dict[str, Any], has_standings: bool = False) -> List[str]:
    """Poll ranking for each side, for the sports whose rank is a national
    poll position rather than a league-table position.

    The ranks were already in every cache row and the prompt simply never
    mentioned them, so a #9-vs-#24 top-25 meeting (Ole Miss vs Louisville,
    2026-09-06) was described as "two programs chasing bowl eligibility" and
    nothing else. This is the cheapest available fix to #209's fourth root
    cause: a college-football prompt that carried a week number and nothing
    a fan would care about.

    Skipped for `rank_source == "standings"` (the soccer leagues), where the
    position is already covered in far more detail by `team_posture_lines`
    and repeating it as a "ranking" would invite title-race framing for a
    17th-placed side.
    """
    extra = g.get("extra") or {}
    if extra.get("rank_source", "poll") == "standings":
        return []
    # `rank_source` defaults to "poll", so a league row that predates the key
    # (or any source that forgets to stamp it) would have its LEAGUE POSITION
    # relabelled as a national poll ranking. The presence of a standings table
    # is the reliable signal, and where one exists `team_posture_lines` has
    # already described the position properly.
    if has_standings:
        return []
    pairs = [(g.get("home"), g.get("rank_home")), (g.get("away"), g.get("rank_away"))]
    if not any(isinstance(r, int) for _team, r in pairs):
        return []
    pool = g.get("rank_pool_size")
    pool_text = f" of {pool}" if isinstance(pool, int) and pool > 0 else ""
    out: List[str] = []
    for team, rank in pairs:
        if not team:
            continue
        if isinstance(rank, int):
            out.append(f"  - {team}: ranked #{rank}{pool_text} in the national poll.")
        else:
            out.append(f"  - {team}: unranked.")
    return out


def venue_context_lines(g: Dict[str, Any]) -> List[str]:
    """Venue, conference and rivalry facts, all already in `extra`.

    Conferences are SUPPLIED rather than forbidden. An earlier guard rejected
    any conference the context did not mention, which was right for a model
    guessing from stale priors ("Pac-12" on UCLA, which joined the Big Ten in
    2024) and wrong for the many cases where the true answer was sitting in
    the CFBD payload all along. Give it the fact and the guard passes.
    """
    extra = g.get("extra") or {}
    out: List[str] = []
    home, away = g.get("home"), g.get("away")
    ch, ca = extra.get("conference_home"), extra.get("conference_away")
    if ch and ca and home and away:
        if ch == ca:
            out.append(f"Conference: both teams are in the {ch}.")
        else:
            out.append(f"Conference: {home} is in the {ch}, {away} in the {ca}.")
    # ALWAYS state the venue when we have it, including for neutral sites.
    # Suppressing it there left a hole the model filled: a neutral-site game
    # at Nissan Stadium in Nashville was previewed as being played "in
    # Oxford", Ole Miss's home town, because "neither team is at home" told it
    # where the game was NOT and nothing told it where it was.
    venue = g.get("venue")  # top-level GameRow field, not an `extra` key
    if extra.get("neutral"):
        if venue:
            out.append(f"Neutral site at {venue}: neither team is at home.")
        else:
            out.append("Neutral site: neither team is at home.")
    elif venue:
        out.append(f"Venue: {venue}.")
    if extra.get("conference_game"):
        out.append("This is a conference game.")
    trophy = extra.get("rivalry_trophy")
    if trophy:
        out.append(f"This is a rivalry game, played for {trophy}.")
    elif g.get("is_rivalry"):
        out.append("This is a rivalry game.")
    return out


def _focus_positions(table: List[Dict[str, Any]], g: Dict[str, Any]) -> List[Any]:
    """Current-table positions for the two teams, falling back to the cached
    scoring ranks when a team has no current row (e.g. a legacy cache row, or
    a competition with no flat table).

    Pairs each side with ITS OWN fallback rank rather than zipping against a
    pre-filtered team list, so a missing or "?" team name cannot shift the
    away team onto the home team's rank.
    """
    by_name = _table_by_name(table)
    out: List[Any] = []
    for team, fallback in (
        (g.get("home"), g.get("rank_home")),
        (g.get("away"), g.get("rank_away")),
    ):
        row = by_name.get(team) if team else None
        pos = row.get("position") if row else None
        out.append(pos if isinstance(pos, int) else fallback)
    return out


def _standings_window(table: List[Dict[str, Any]], focus_positions: List[Any]) -> List[Dict[str, Any]]:
    """Return standings rows within ±_STANDINGS_CONTEXT_WINDOW of any focus
    position, plus the leader (#1) so the model can frame the title race.
    Deduplicated and sorted by position.
    """
    if not table:
        return []
    by_pos: Dict[int, Dict[str, Any]] = {}
    for r in table:
        pos = r.get("position")
        if isinstance(pos, int):
            by_pos[pos] = r
    if not by_pos:
        return []
    wanted = set()
    wanted.add(min(by_pos))  # leader
    for fp in focus_positions:
        if not isinstance(fp, int):
            continue
        for p in range(fp - _STANDINGS_CONTEXT_WINDOW, fp + _STANDINGS_CONTEXT_WINDOW + 1):
            if p in by_pos:
                wanted.add(p)
    return [by_pos[p] for p in sorted(wanted)]


def build_llm_context(
    g: Dict[str, Any],
    tagline: str,
    boundary_summary: str = "",
    league_context: Any = None,
) -> str:
    """Build the user-message context block for the LLM.

    Pulls only from data already in the cache row: no new API calls.

    `league_context` is the `scoring.LeagueContext` for this competition when
    one exists. It supplies the outcome-band cutoffs used to precompute each
    team's posture (which bands they are in, exact points to the leader and to
    the relegation line). Optional so callers without one still get a valid,
    if thinner, context.

    THE STANDINGS RULE, and it is load-bearing (#209): the table described as
    "this season" MUST come from `extra["standings_table_current"]`. The plain
    `extra["standings_table"]` is the SCORING table, which early in a season is
    deliberately replaced with LAST season's final standings as a ranking prior
    (`sources.soccer._fetch_standings_with_seed`). Rendering that as the live
    table produced confidently false prose on 14 of 25 games: last season's
    points against this season's fixture, with nothing marking the swap.
    """
    extra = g.get("extra") or {}
    home = g.get("home") or "?"
    away = g.get("away") or "?"
    sport_label = g.get("sport_label") or g.get("sport_prefix") or "?"
    kickoff_local = g.get("kickoff_local") or "?"

    lines: List[str] = []
    lines.append(f"Match: {away} at {home}, {kickoff_local}")
    lines.append(f"Competition: {sport_label}")

    # Playoff series grounding (best-of-N sources populate extra["series"]).
    # This is the load-bearing fix for the false-"elimination" bug: without the
    # record and game number, the model invents playoff drama. The SYSTEM_PROMPT
    # hard rule below forbids guessing series state when these lines are absent.
    series = extra.get("series")
    series_phase = series_phase_text(series)
    if series_phase:
        lines.append(f"Series: {series_phase}")
    series_record = series_record_text(series, home, away)
    if series_record:
        lines.append(f"Series record: {series_record}")
    series_recap = series_result_lines(series)
    if series_recap:
        lines.append("Results so far:")
        for recap_line in series_recap:
            lines.append(f"  - {recap_line}")

    # Group-stage grounding (WC / EURO group games populate
    # extra["group_stage"]). The soccer analog of the series block above and
    # the load-bearing fix for the false-"shock opening loss" bug: a group
    # game has no flat standings table (FD.org publishes none for
    # tournaments), so without these lines the model had nothing but team
    # names and invented the table. The SYSTEM_PROMPT grounding rule forbids
    # asserting any record / result not listed here.
    group_stage = extra.get("group_stage")
    group_phase = group_phase_text(group_stage)
    if group_phase:
        lines.append(f"Tournament round: {group_phase}")
    group_standings = group_standings_lines(group_stage)
    if group_standings:
        lines.append("Current group standings:")
        for standing_line in group_standings:
            lines.append(f"  - {standing_line}")
    group_results = group_results_lines(group_stage)
    if group_results:
        lines.append("Group results so far:")
        for result_line in group_results:
            lines.append(f"  - {result_line}")
    group_advance = group_advance_text(group_stage)
    if group_advance:
        lines.append(f"Advancement: {group_advance}")

    # Trophy grounding for knockout games in tracked international/continental
    # competitions. Load-bearing against the "going for their third crown"
    # hallucination: without the real title counts the model invents them. The
    # existing "GROUND EVERY FACT in the lines above" rule then keeps the model
    # honest about the numbers we supply. See honours.py.
    for honours_line in honours_lines(
        home, away, extra.get("fd_competition_code"), g.get("tournament_stage")
    ):
        lines.append(honours_line)

    matchday = extra.get("matchday")
    # Fall back to the league context's season length so a source that does
    # not stamp `matchdays_total` (college football stamps only `week`) still
    # gets "week 1 of 12" rather than a bare week number.
    matchdays_total = extra.get("matchdays_total") or getattr(
        league_context, "matchdays_total", None
    )
    week = extra.get("week")
    if matchday and matchdays_total:
        lines.append(f"Matchday: {matchday} of {matchdays_total}")
    elif week:
        lines.append(
            f"Week: {week} of {matchdays_total}" if matchdays_total
            else f"Week: {week}"
        )

    if boundary_summary:
        lines.append(f"League boundaries: {boundary_summary}")

    thresholds = getattr(league_context, "thresholds", None) or []
    teams = [t for t in (home, away) if t and t != "?"]
    current = current_standings_table(extra)
    is_win_count = getattr(league_context, "format", None) == "win_count"

    # How far into the season we are, and what that means for how the stakes
    # should be framed. Applies to BOTH sport shapes: a league reads its
    # progress off the table, a win-count sport off the week number. Without
    # it the model framed a week-1 opener as a run-in ("chasing bowl
    # eligibility"), which is true of every team in September and is the whole
    # story in November. See #209.
    if is_win_count:
        played_games = _played_from_record(extra)
        if played_games is None and isinstance(week, int):
            played_games = max(0, week - 1)
        if played_games is not None:
            lines.extend(season_progress_line(played_games, matchdays_total, "games"))
    else:
        lines.extend(season_progress_line(_max_played(current), matchdays_total)
                     if current else [])

    if is_win_count:
        records = win_count_posture_lines(g, thresholds, matchdays_total)
        if records:
            lines.append("Where the two teams stand this season:")
            lines.extend(records)

    # A table where nobody has played is an ORDERING, not a standing: FD.org
    # still assigns positions, so rendering it lets the model read "Currently
    # in: title" off a team that has played nothing, while the opener guidance
    # in the same prompt says nothing has been decided. Contradictory hard
    # facts are worse than fewer facts, and last season plus the previous
    # meetings carry the preview at that point.
    table_is_meaningful = _max_played(current) > 0

    posture = team_posture_lines(current, teams, thresholds) if table_is_meaningful else []
    if posture:
        lines.append("Where the two teams stand this season:")
        lines.extend(posture)

    # Focus the slice on where the teams actually are NOW. `rank_home` /
    # `rank_away` are the SEEDED scoring ranks, so using them centred the
    # window on last season's positions: the Osasuna / Alaves fixture (5th and
    # 6th today, 14th and 16th last season) rendered a slice of rows 12-18
    # containing neither team. Fall back to the ranks only when a team is
    # missing from the current table.
    window = (
        _standings_window(current, _focus_positions(current, g))
        if table_is_meaningful else []
    )
    if window:
        lines.append("This season's table (relevant slice):")
        for row in window:
            lines.append(_format_team_row(row))

    # Last season's finish. Early in a season this is the only real form guide
    # there is, which is exactly Jake's ask: two matchdays say nothing, last
    # season's 38 say a lot. Rendered under an explicit label so it can never
    # be mistaken for the current table again (#209).
    prev = prev_season_lines(extra.get("standings_prev_final") or [], teams)
    if prev:
        lines.append("Last season's final table:")
        lines.extend(prev)

    ranks = poll_rank_lines(g, has_standings=bool(current))
    if ranks:
        lines.append("National poll ranking:")
        lines.extend(ranks)

    lines.extend(venue_context_lines(g))

    h2h_entries = extra.get("h2h") or []
    meetings = h2h_lines(h2h_entries)
    if meetings:
        lines.append("Previous meetings between these two, most recent first:")
        lines.extend(meetings)
        tally = h2h_tally_line(h2h_entries, home, away)
        if tally:
            lines.append(tally)

    favorites_matched = g.get("favorites_matched") or []
    if favorites_matched:
        lines.append(f"User's favorite teams playing: {', '.join(favorites_matched)}")

    # Gated: a pre-#209 cache row's narratives were built from the seeded
    # table and state a false gap. The legacy top-level shape is checked the
    # same way, so an old row cannot slip the falsehood in through either key.
    impact_narratives = (
        trusted_impact_narratives(extra)
        or trusted_impact_narratives({**extra, "impact_narratives": g.get("impact_narratives")})
    )
    if impact_narratives:
        lines.append("Affects user's other favorites:")
        for narrative in impact_narratives:
            lines.append(f"  - {narrative}")

    # Older cache files stored the band list under `stakes_thresholds_hit`;
    # accept either key so a cache.json written by an older plugin version
    # still produces a reasonable prompt during the one-cycle migration
    # window.
    thresholds_hit = (
        g.get("importance_thresholds_hit")
        or g.get("stakes_thresholds_hit")
        or []
    )
    if thresholds_hit:
        lines.append(f"Outcome bands in play: {', '.join(thresholds_hit)}")

    if tagline:
        lines.append(f"Editorial frame (use as a hint, do not quote): {tagline}")

    closeness = g.get("closeness")
    if isinstance(closeness, (int, float)) and closeness >= 0.7:
        lines.append("Bookmaker view: toss-up")

    return "\n".join(lines)


def prompt_hash(context: str, model: str) -> str:
    """Stable short hash used as part of the cache key. Folds in the model AND
    the SYSTEM_PROMPT so that a model swap OR a system-prompt edit invalidates
    cached prose without a manual cache-bust. The system prompt matters because
    it carries the behavioral rules (e.g. the anti-"elimination" guardrail): a
    cache keyed only on the user-message context would keep serving prose
    written under the OLD rules after a prompt tightening.
    """
    h = hashlib.sha256()
    h.update(model.encode("utf-8"))
    h.update(b"\x00")
    h.update(SYSTEM_PROMPT.encode("utf-8"))
    h.update(b"\x00")
    h.update(context.encode("utf-8"))
    return h.hexdigest()[:16]


def _call_anthropic(context: str, api_key: str, model: str) -> str:
    """Call Anthropic /v1/messages and return the assistant text block.

    Raises on any non-200, missing key, network error, malformed body, or
    empty response. The caller (`llm_describe_or_fallback`) catches and
    falls back to the deterministic description.
    """
    body = json.dumps({
        "model": model,
        "max_tokens": ANTHROPIC_MAX_TOKENS,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": context}],
    }).encode("utf-8")
    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_API_VERSION,
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=ANTHROPIC_TIMEOUT_S) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    blocks = data.get("content") or []
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str):
            return b["text"].strip()
    raise ValueError("anthropic response had no text block")


# Openings and phrases that mean the model addressed the OPERATOR rather than
# writing the preview. Matched case-insensitively against the response.
#
# DO NOT loosen these into single common words. "I need" as a bare substring
# fires on the legitimate "Milan need a win", which is exactly the prose this
# feature exists to produce; every entry here is either first-person meta or a
# direct request, neither of which can occur inside a match preview.
_NON_PREVIEW_MARKERS = (
    "i don't have",
    "i do not have",
    "i need more information",
    "i need the actual",
    "i need to write",
    "could you provide",
    "can you provide",
    "please provide",
    "i'd be happy to write",
    "i would be happy to write",
    "i appreciate you providing",
    "i can't write",
    "i cannot write",
    "to write this preview",
    "to ground this preview",
    "to ground the preview",
    "once i have those",
)


# Conference and division names the model has been observed to invent. Checked
# against the CONTEXT rather than a fixed allowlist, so naming the Premier
# League in a Premier League preview is fine while "Pac-12" on a Big Ten team
# is not.
#
# Measured on the live guide 2026-09-06: "Patriot League" on Howard (MEAC) vs
# Manhattan (MAAC), "Pac-12" on UCLA (Big Ten since 2024) and on California
# (ACC), "Ivy League" on a Brown vs Saint Peter's (MAAC) fixture. Every one
# read as authoritative and every one was wrong.
_CONFERENCE_NAMES = (
    "pac-12", "pac 12", "big ten", "big 10", "big 12", "big east",
    "ivy league", "patriot league", "summit league", "horizon league",
    "sun belt", "conference usa", "american athletic", "mountain west",
    "atlantic 10", "colonial athletic", "missouri valley",
    "maac", "meac", "swac", "wcc", "acc", "sec",
)


def names_an_ungrounded_conference(prose: str, context: str) -> Optional[str]:
    """The first conference name that appears in `prose` but not in `context`,
    or None.

    Conference affiliation is exactly the kind of fact a model holds with
    confidence and gets wrong, because it changes between seasons and the
    training data spans many of them. If the context did not supply it, the
    preview may not assert it.
    """
    low_prose = prose.lower()
    low_ctx = context.lower()
    for name in _CONFERENCE_NAMES:
        if name in low_prose and name not in low_ctx:
            # Guard the short acronyms against matching inside a word
            # ("SECond", "ACCra") by requiring a non-letter on each side.
            if len(name) <= 4:
                import re as _re
                if not _re.search(rf"(?<![a-z]){_re.escape(name)}(?![a-z])", low_prose):
                    continue
            return name
    return None


# Phrases that place a game somewhere in its season. Only legitimate when a
# "Season progress" line told the model where the season actually is.
#
# Measured on the live guide 2026-09-06: an NCAA soccer fixture whose source
# supplies no season length ("matchdays_total": None, so no progress line) was
# previewed as "late-season matches like this can reshape the postseason
# conversation". It was the 6th of September.
_SEASON_STAGE_PHRASES = (
    "late-season", "late season", "down the stretch", "home stretch",
    "the run-in", "run-in", "final stretch", "closing weeks", "closing stretch",
    "early season", "early-season", "midseason", "mid-season",
    "season opener", "opening weekend", "start of the season",
    "this stage of the season", "business end",
)

_SEASON_PROGRESS_MARKER = "Season progress:"


def names_an_ungrounded_season_stage(prose: str, context: str) -> Optional[str]:
    """The first season-stage phrase used without a "Season progress" line to
    justify it, or None.

    When the context carries a progress line the model has been told where the
    season is and may say so. When it does not, any such phrase is a guess,
    and a guess about the calendar is as wrong as a guess about the table.
    """
    if _SEASON_PROGRESS_MARKER in context:
        return None
    low = prose.lower()
    for phrase in _SEASON_STAGE_PHRASES:
        if phrase in low:
            return phrase
    return None


def looks_like_non_preview(prose: str) -> bool:
    """True when the model answered the operator instead of writing a preview.

    Two shapes, both observed live:
      - a refusal or a request for data ("I don't have the standings ... I'd
        need:"), which reads as a bug report in the middle of a TV guide;
      - markdown structure (a `# heading`, or a bulleted list), which the
        prompt forbids and which renders as literal `#` and `-` characters in
        TiviMate, Plex and Jellyfin.

    Deliberately conservative: it only fires on first-person meta phrasing and
    on structural markdown, never on a word that could appear in real prose
    about a football match.
    """
    if not prose:
        return True
    low = prose.lower()
    if any(marker in low for marker in _NON_PREVIEW_MARKERS):
        return True
    stripped = prose.lstrip()
    if stripped.startswith("#"):
        return True
    # A bulleted list: a line starting with "- " or "* " after the first line.
    for line in prose.split("\n")[1:]:
        t = line.lstrip()
        if t.startswith("- ") or t.startswith("* "):
            return True
    return False


# Phrases that assert a team is ranked FIRST. Only usable when a supplied
# ranking actually says #1.
_TOP_RANK_CLAIMS = ("top-ranked", "top ranked", "number one", "no. 1", "#1 team")


def claims_a_rank_it_does_not_have(prose: str, context: str) -> Optional[str]:
    """A "top-ranked" claim with no #1 in the supplied rankings, or None.

    Measured on the live guide 2026-09-06: "Top-ranked Notre Dame opens
    against Wisconsin ... carrying a #4 national ranking", which contradicts
    itself inside one sentence. The prompt now forbids it; this is what makes
    it stick.

    Only fires when a poll line was actually supplied, so a preview for a
    competition with no rankings is unaffected.
    """
    if "National poll ranking:" not in context:
        return None
    if "ranked #1 " in context or "ranked #1." in context:
        return None
    low = prose.lower()
    for phrase in _TOP_RANK_CLAIMS:
        if phrase in low:
            return phrase
    return None


_TITLE_CLAIM_RE = re.compile(
    r"\b(?:won\s+(?:it|the\s+(?:competition|title|trophy|tournament))\s+"
    r"(?:once|twice|three|four|five|\d+)"
    r"|(?:two|three|four|five|six|seven|\d+)[-\s]time\s+(?:champion|winner)s?"
    r"|their\s+(?:first|second|third|fourth|fifth|\d+(?:st|nd|rd|th))\s+"
    r"(?:title|crown|trophy))\b",
    re.IGNORECASE,
)


def claims_a_title_count(prose: str, context: str) -> Optional[str]:
    """A count of past titles with no "Honours" line to support it, or None.

    Measured on the live guide 2026-09-07: a Champions League league-phase
    preview said Porto "has won the competition twice before". True, and
    entirely ungrounded: `honours.honours_lines` fires only on KNOCKOUT games
    by deliberate scope, so the prompt carried no honours line at all and the
    model supplied the number from its own knowledge. That is the same class
    as the invented conferences, and being right by luck is not the standard.
    """
    if "Honours (" in context:
        return None
    m = _TITLE_CLAIM_RE.search(prose)
    return m.group(0) if m else None


def reject_reason(prose: str, context: str) -> Optional[str]:
    """Why this response must not reach the guide, or None if it may.

    THE SINGLE GATE. Every check lives here so that the cached path and the
    fresh path cannot diverge: they used to, and a response that predated a
    guard was served from cache without ever meeting it.

    A rule the model can decline to follow is not a guarantee for text a
    viewer reads. The SYSTEM_PROMPT asks for all of this; this function is
    what makes it true.
    """
    if looks_like_non_preview(prose):
        # Seven NCAA soccer filler games came back as "I don't have the
        # standings ... I'd need:" and that text went verbatim into the EPG.
        # Tightening the grounding rules made the model refuse rather than
        # invent, which is the right instinct pointed at the wrong output.
        return "not a preview"
    conference = names_an_ungrounded_conference(prose, context)
    if conference:
        return f"ungrounded conference: {conference}"
    stage = names_an_ungrounded_season_stage(prose, context)
    if stage:
        return f"ungrounded season stage: {stage}"
    rank = claims_a_rank_it_does_not_have(prose, context)
    if rank:
        return f"inflated ranking: {rank}"
    titles = claims_a_title_count(prose, context)
    if titles:
        return f"ungrounded title count: {titles}"
    return None


def llm_describe_or_fallback(
    g: Dict[str, Any],
    tagline: str,
    fallback_description: str,
    api_key: str,
    model: str,
    cache: Dict[str, str],
    boundary_summary: str = "",
    marker: str = "",
    caller: Optional[Caller] = None,
    league_context: Any = None,
) -> str:
    """Return LLM-rewritten prose, or `fallback_description` on any failure.

    Mutates `cache` in place on a successful call (caller is responsible for
    persisting the cache dict to disk). The enable-flag, the API-key existence
    check, and the placeholder skip are the caller's job: this function
    assumes it should attempt the call.
    """
    context = build_llm_context(g, tagline, boundary_summary, league_context)
    # Use `|` separator: markers contain ':' (`ranked_matchups:EPL:538161`),
    # so a single-':' split would clip everything before the last colon.
    cache_key = f"{marker}|{prompt_hash(context, model)}"
    cached = cache.get(cache_key)
    if cached:
        reason = reject_reason(cached, context)
        if reason is None:
            return cached
        # A cached response is NOT exempt from the guards. They used to run
        # only on a fresh call, so prose that predated a guard (or predated a
        # tightening of one) was served straight from the cache and never
        # re-examined: "late-season matches like this" survived on the live
        # guide through a deploy that had already added the check meant to
        # catch it. Evict and re-ask.
        logger.warning(
            "[ranked_matchups] cached description for %s rejected (%s); "
            "re-requesting", marker or "?", reason,
        )
        cache.pop(cache_key, None)
    fn = caller or _call_anthropic
    try:
        prose = fn(context, api_key, model)
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError) as exc:
        logger.warning("[ranked_matchups] LLM describe failed for %s: %s", marker or "?", exc)
        return fallback_description
    if not prose:
        return fallback_description
    reason = reject_reason(prose, context)
    if reason is not None:
        logger.warning(
            "[ranked_matchups] LLM response rejected for %s (%s); using the "
            "deterministic description", marker or "?", reason,
        )
        return fallback_description
    cache[cache_key] = prose
    return prose


# ---------- cache file I/O ----------

def read_cache(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    # Only keep string-valued entries; defensively drop anything else.
    return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def write_cache(path: str, cache: Dict[str, str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True, ensure_ascii=False)
    os.replace(tmp, path)


def prune_cache(cache: Dict[str, str], live_markers: set) -> Dict[str, str]:
    """Drop cache entries whose marker is not in the current refresh's
    `seen_markers` set. Bounds the file to live games only.

    Cache key shape is `<marker>|<prompt_hash>`. The marker itself contains
    `:` (`ranked_matchups:EPL:538161`), which is why this splits on `|`
    rather than `:`.
    """
    return {k: v for k, v in cache.items() if k.split("|", 1)[0] in live_markers}
