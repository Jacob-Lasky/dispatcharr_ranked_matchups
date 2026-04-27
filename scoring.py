"""Transparent interestingness scoring with per-signal breakdown + helpers.

Design principle: the score MUST show its work. Every game's final score is the
sum of per-signal contributions, each visible in the cache. When the user says
"this game should be ranked higher", they can see exactly which signal needs
to be tuned.

Signals (Phase 1 — rank + favorites only):
  - rank_pair: both teams ranked → score from sum_of_ranks (lower = higher score)
  - one_ranked: one team ranked, one unranked → score from the ranked team's rank
  - favorite: at least one favorite team involved → flat boost

Signals added in Phase 3:
  - close_game: tight betting spread → score inversely proportional to spread
  - rivalry: known rivalry game → flat boost
  - narrative: LLM-judged narrative score (playoff race, history, stakes)
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# Trailing club-tag tokens (FC/AFC/etc) and generic second-words (United/City/
# etc) live in _util so matcher.py can use them without importing scoring.
from ._util import GENERIC_TEAM_SECOND_WORDS, TEAM_SUFFIX_TOKENS

# Sentinel rank for unranked teams. Picked so that "unranked vs unranked" sums
# to a clearly worst value but doesn't dominate finite scoring.
UNRANKED = 26


@dataclass
class Weights:
    """Per-signal weights. Tweakable via plugin settings.

    Defaults reflect priority order: standings > rank > favorites > tournament/spread.
    Narrative defaults to 0 because heuristics (stakes/tournament/impact-on-favorite)
    cover what LLM narrative scoring would surface. Enable by setting weight > 0.
    """
    rank: float = 1.0
    spread: float = 0.5
    favorite: float = 4.0
    rivalry: float = 2.0
    stakes: float = 2.0          # standings near meaningful league threshold
    tournament: float = 1.5      # knockout-stage cup games
    impact_favorite: float = 1.0  # non-favorite game shifts favorite's table
    narrative: float = 0.0       # LLM narrative score (disabled by default)


@dataclass
class GameSignals:
    """Per-game signal inputs. Sport-agnostic."""
    rank_a: Optional[int] = None      # team A's rank (None if unranked)
    rank_b: Optional[int] = None      # team B's rank
    team_a: str = ""
    team_b: str = ""
    favorite_match: List[str] = field(default_factory=list)  # which favorites match
    spread: Optional[float] = None    # absolute betting spread

    # Phase 3 — standings + stage signals (sport-aware, computed by plugin from
    # source-provided context).
    stakes_a: float = 0.0             # 0-3 ish, how close team A is to a league threshold
    stakes_b: float = 0.0
    stakes_thresholds_hit: List[str] = field(default_factory=list)  # ['playoff','relegation']
    season_progress: float = 0.0      # 0.0-1.0 — late-season amplifies stakes
    tournament_stage: Optional[str] = None  # 'FINAL', 'SEMI_FINALS', 'QUARTER_FINALS', etc.
    impact_on_favorites: List[str] = field(default_factory=list)  # favorites this game affects

    is_rivalry: bool = False
    narrative_score: Optional[float] = None  # 0-10 from LLM (last)


# ---------- League-specific thresholds ----------

@dataclass
class LeagueContext:
    """Per-league data the plugin uses to compute stakes signals.

    `boundary_summary` is a one-line human description of how
    standings translate to outcomes (UCL spots, relegation, playoff
    qualification). Rendered in the EPG description as a reminder of
    WHY a position-based race matters.
    """
    code: str                    # 'PL', 'ELC', 'CL', etc.
    matchdays_total: int         # season length (38 for EPL, 46 for ELC, etc.)
    thresholds: List[Tuple[int, str]] = field(default_factory=list)
    # List of (position, label) — e.g., [(1,'title'),(4,'UCL'),(17,'relegation')]
    boundary_summary: str = ""   # e.g. "Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation"


LEAGUE_CONTEXTS: Dict[str, LeagueContext] = {
    "PL": LeagueContext(
        code="PL", matchdays_total=38,
        thresholds=[(1, "title"), (4, "UCL"), (7, "Europa/Conference"), (17, "relegation")],
        boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
    ),
    "ELC": LeagueContext(
        code="ELC", matchdays_total=46,
        thresholds=[(2, "auto-promotion"), (6, "playoff"), (21, "relegation")],
        boundary_summary="Top 2 → auto-promotion · 3-6 → promotion playoff · bottom 3 → relegation",
    ),
    # UCL handled via tournament_stage signal, not league position
}


@dataclass
class GameScore:
    """Score with both raw signal sum and a 0-10 normalized version.

    `raw` is the unbounded sum of all signal contributions; useful for sorting
    when ties matter. `final` is the smooth-compressed 0-10 score we display
    everywhere user-facing. `breakdown` shows the per-signal raw contributions
    so the user can see WHY a game scored what it did.
    """
    raw: float                        # unbounded sum
    final: float                      # 0-10 (smooth compression of raw)
    breakdown: Dict[str, float]
    notes: List[str]


# Compression knee: tweak this to slide the asymptote earlier/later. Lower N
# = scores saturate faster; higher N = more headroom for high-raw games.
_FINAL_KNEE = 8.0


def _compress_to_10(raw: float) -> float:
    """Smooth 0-10 normalization. Preserves ordering, asymptotes at 10.

    Anchor values (knee = 8.0):
      raw=2  → 2.45
      raw=4  → 4.62
      raw=8  → 7.62
      raw=12 → 9.05
      raw=16 → 9.64
      raw=24 → 9.96
    """
    if raw <= 0:
        return 0.0
    return 10.0 * math.tanh(raw / _FINAL_KNEE)


# Trailing tokens that mean "same team" (typically the team-type / club suffix).
# When these follow a favorite name, we allow the match. Superset of the
# matcher's GENERIC_TEAM_SECOND_WORDS — adds the dotted club-tag variants and
# a few extras that show up in compound club names ("Brighton & Hove Albion").
TEAM_QUALIFIER_TOKENS = {
    *TEAM_SUFFIX_TOKENS, "f.c.", "a.f.c.",
    *GENERIC_TEAM_SECOND_WORDS,
    "hove", "end", "north", "olympic", "olympique",
    "&", "stadium",
}


def match_favorites(home: str, away: str, favorites: List[str]) -> List[str]:
    """Match a list of favorite-team names against home/away with word-boundary
    rules that avoid false positives.

    Rules:
      - Favorite must appear at word boundary (no letter on either side).
      - If a capitalized word follows the favorite, only allow the match when
        that word is in TEAM_QUALIFIER_TOKENS (e.g., "Hull" + "City" → match
        "Hull City"). Otherwise reject (e.g., "UNC Pembroke", "North Carolina A&T").
    """
    if not favorites:
        return []
    text = f"{home} | {away}"
    matched: List[str] = []
    for fav in favorites:
        pat = re.compile(r"(?<![A-Za-z])" + re.escape(fav) + r"(?![A-Za-z])", re.IGNORECASE)
        for m in pat.finditer(text):
            tail = text[m.end():m.end() + 32]
            tail_match = re.match(r"\s+([A-Z][A-Za-z&\.\-]+)", tail)
            if tail_match:
                trailing_token = tail_match.group(1).lower().rstrip(".")
                if trailing_token not in TEAM_QUALIFIER_TOKENS:
                    continue
            matched.append(fav)
            break
    return matched


def score_game(signals: GameSignals, weights: Weights) -> GameScore:
    """Compute interestingness score with full breakdown."""
    breakdown: Dict[str, float] = {}
    notes: List[str] = []

    rank_a = signals.rank_a if signals.rank_a is not None else UNRANKED
    rank_b = signals.rank_b if signals.rank_b is not None else UNRANKED
    both_ranked = signals.rank_a is not None and signals.rank_b is not None
    any_ranked = signals.rank_a is not None or signals.rank_b is not None

    if both_ranked:
        # Both ranked: more points the lower the sum (1+5=6 great, 24+25=49 OK).
        # Map sum [2..50] to score [10..0]. Linear, weighted.
        sum_ranks = signals.rank_a + signals.rank_b
        # 2 → 10, 26 → 5, 50 → 0
        rank_pts = max(0.0, (50 - sum_ranks) / 4.8) * weights.rank
        breakdown["rank_pair"] = round(rank_pts, 2)
        notes.append(f"both ranked: #{signals.rank_a} vs #{signals.rank_b} (sum={sum_ranks})")
    elif any_ranked:
        # One ranked, one unranked: scale by the ranked team's rank.
        # rank 1 → 4.0, rank 25 → 0.5
        rank = signals.rank_a if signals.rank_a is not None else signals.rank_b
        rank_pts = max(0.0, (26 - rank) / 6.0) * weights.rank
        breakdown["one_ranked"] = round(rank_pts, 2)
        notes.append(f"one ranked: #{rank} vs unranked")

    if signals.favorite_match:
        fav_pts = weights.favorite
        breakdown["favorite"] = round(fav_pts, 2)
        notes.append(f"favorite involved: {', '.join(signals.favorite_match)}")

    if signals.spread is not None and signals.spread >= 0:
        # Close game bonus. Spread 0 → max, spread 14+ → 0.
        spread_pts = max(0.0, (14 - signals.spread) / 2.0) * weights.spread
        breakdown["close_game"] = round(spread_pts, 2)
        notes.append(f"betting spread: {signals.spread:+.1f} pts")

    # Phase 3 — stakes (proximity to meaningful league threshold), with
    # late-season multiplier so games matter more when season is winding down.
    stakes_total = signals.stakes_a + signals.stakes_b
    if stakes_total > 0:
        late_mult = 1.0
        if signals.season_progress >= 0.85:
            late_mult = 2.0
        elif signals.season_progress >= 0.70:
            late_mult = 1.5
        stakes_pts = stakes_total * late_mult * weights.stakes
        breakdown["stakes"] = round(stakes_pts, 2)
        notes.append(
            f"standings stakes: thresholds={signals.stakes_thresholds_hit}, "
            f"season_progress={signals.season_progress:.0%}, late_mult={late_mult:.1f}x"
        )

    if signals.tournament_stage:
        ts = signals.tournament_stage.upper()
        stage_score = {
            "FINAL": 5.0,
            "SEMI_FINALS": 3.5, "SEMI_FINAL": 3.5,
            "QUARTER_FINALS": 2.5, "QUARTER_FINAL": 2.5,
            "ROUND_OF_16": 1.5, "LAST_16": 1.5,
            "ROUND_OF_32": 1.0, "LAST_32": 1.0,
            "PLAYOFF_ROUND": 1.0, "PLAYOFFS": 1.0,
        }.get(ts, 0.0)
        if stage_score > 0:
            tourn_pts = stage_score * weights.tournament
            breakdown["tournament_stage"] = round(tourn_pts, 2)
            notes.append(f"tournament stage: {ts.lower().replace('_', ' ')}")

    if signals.impact_on_favorites:
        # Each affected favorite contributes (more affected favorites = bigger
        # downstream impact, but capped to avoid double-counting).
        impact_pts = min(3.0, len(signals.impact_on_favorites)) * weights.impact_favorite
        breakdown["impact_on_favorite"] = round(impact_pts, 2)
        notes.append(
            f"affects favorite{'s' if len(signals.impact_on_favorites) > 1 else ''}: "
            f"{', '.join(signals.impact_on_favorites)}"
        )

    if signals.is_rivalry:
        breakdown["rivalry"] = round(weights.rivalry, 2)
        notes.append("rivalry game")

    if signals.narrative_score is not None:
        narr_pts = signals.narrative_score / 10.0 * weights.narrative
        breakdown["narrative"] = round(narr_pts, 2)
        notes.append(f"LLM narrative score: {signals.narrative_score:.1f}/10")

    raw = sum(breakdown.values())
    return GameScore(
        raw=round(raw, 2),
        final=round(_compress_to_10(raw), 2),
        breakdown=breakdown,
        notes=notes,
    )


def compute_team_stakes(
    team_position: Optional[int],
    league_thresholds: List[Tuple[int, str]],
    proximity: int = 2,
) -> Tuple[float, List[str]]:
    """How close is this team to a meaningful league threshold?

    Returns (points, hit_labels). Points: 3 if exactly at threshold, 2 if
    adjacent (±1), 1 if ±2, 0 otherwise. Stacks across multiple thresholds
    (e.g., a 4th-place EPL side is at the UCL line AND 3 spots from title).
    """
    if team_position is None:
        return 0.0, []
    pts = 0.0
    hit: List[str] = []
    for cutoff, label in league_thresholds:
        d = abs(team_position - cutoff)
        if d <= proximity:
            pts += float(proximity + 1 - d)  # adjacent → proximity, exact → proximity+1
            hit.append(label)
    return pts, hit


def compute_impact_on_favorites(
    rank_a: Optional[int], rank_b: Optional[int],
    team_a: str, team_b: str,
    favorites_in_league: List[Tuple[str, int]],  # [(name, position), ...]
    proximity: int = 3,
) -> List[str]:
    """List favorites whose table position would be affected by this game's outcome.

    A game has 'impact on favorite' when one of its teams is within `proximity`
    positions of a favorite's spot — a win/loss can swap them.
    """
    affected: List[str] = []
    a_lc, b_lc = team_a.lower(), team_b.lower()
    for fav_name, fav_pos in favorites_in_league:
        fav_lc = fav_name.lower()
        # Skip games where the favorite IS playing (already covered by 'favorite' signal)
        if fav_lc in a_lc or fav_lc in b_lc:
            continue
        for r in [rank_a, rank_b]:
            if r is None:
                continue
            if abs(r - fav_pos) <= proximity:
                affected.append(fav_name)
                break
    return affected


def strip_team_suffix(name: str) -> str:
    """Drop trailing club-tag suffixes ('FC', 'AFC', 'CF', 'SC') from a team
    name. 'Manchester United FC' → 'Manchester United'. Idempotent. Used in
    channel names + descriptions to keep the visible string scannable."""
    if not name:
        return name
    parts = name.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].lower() in TEAM_SUFFIX_TOKENS:
        return parts[0]
    return name


def pick_tagline(
    score_breakdown: Dict[str, float],
    favorites_matched: List[str],
    spread: Optional[float],
    stakes_thresholds: Optional[List[str]],
    tournament_stage: Optional[str],
    season_progress: Optional[float],
    rank_a: Optional[int],
    rank_b: Optional[int],
    rank_source: str = "poll",
) -> str:
    """Pick a single dominant tagline for the channel name. Priority:
       tournament-stage → stakes → poll-rank-pair → toss-up → favorite.

    `rank_source` distinguishes poll-based ranks (NCAAF / NCAAM AP Top 25)
    where 'top-N' framing is meaningful, from standings-position ranks
    (EPL / EFL where every team in the league is automatically 'top-N').
    Stakes covers the league-position case with proper labels (title race,
    relegation, etc.) so we drop the rank-pair tagline for standings.
    """
    if tournament_stage:
        ts = tournament_stage.upper()
        stage_labels = {
            "FINAL": "Final",
            "SEMI_FINALS": "Semifinal", "SEMI_FINAL": "Semifinal",
            "QUARTER_FINALS": "Quarterfinal", "QUARTER_FINAL": "Quarterfinal",
            "ROUND_OF_16": "Round of 16", "LAST_16": "Round of 16",
            "ROUND_OF_32": "Round of 32", "LAST_32": "Round of 32",
            "PLAYOFF_ROUND": "Playoff", "PLAYOFFS": "Playoff",
        }
        if ts in stage_labels:
            return stage_labels[ts]

    if "stakes" in score_breakdown and stakes_thresholds:
        labels = list(dict.fromkeys(stakes_thresholds))[:2]
        if labels:
            return " / ".join(labels) + " race"

    if rank_source == "poll" and rank_a is not None and rank_b is not None:
        lo, hi = sorted([rank_a, rank_b])
        if hi <= 5:
            return "top-5 showdown"
        if hi <= 10:
            return "top-10 matchup"
        if lo <= 5:
            return f"#{lo} ranked"

    if "close_game" in score_breakdown and spread is not None and spread <= 3:
        return "toss-up"

    if favorites_matched:
        return "favorite"

    return ""


def format_channel_name(
    sport_prefix: str,
    signals: GameSignals,
    score: GameScore,
    home: str,
    away: str,
    tagline: str = "",
) -> str:
    """Build the Dispatcharr channel name in the "B" format:

        EPL 3v9 ★10.0 · Brentford at Manchester United · title race
        CFB 1v5 ★8.5 · Ohio State at Penn State · top-5 showdown
        EFL 4v6 ⭐ ★10.0 · Middlesbrough at Wrexham · playoff race

    The rank pair is normalized so the better (lower-number) rank always
    appears first — "1v5" not "5v1" — for at-a-glance scanning.

    Team-name suffixes (FC / AFC / CF / SC) are stripped: 'Manchester
    United FC' renders as 'Manchester United'.
    """
    parts = [sport_prefix]
    a, b = signals.rank_a, signals.rank_b
    if a is not None and b is not None:
        lo, hi = (a, b) if a <= b else (b, a)
        parts.append(f"{lo}v{hi}")
    elif a is not None or b is not None:
        rank = a if a is not None else b
        parts.append(f"{rank}vUR")

    if signals.favorite_match:
        parts.append("⭐")

    parts.append(f"★{score.final:.1f}")
    head = " ".join(parts)

    matchup = f"{strip_team_suffix(away)} at {strip_team_suffix(home)}"
    name = f"{head} · {matchup}"
    if tagline:
        name = f"{name} · {tagline}"
    if len(name) > 250:
        name = name[:247] + "..."
    return name


def render_favorite_impact(
    fav_name: str, fav_pos: int, fav_points: Optional[int],
    nearby_team: str, nearby_pos: int, nearby_points: Optional[int],
) -> str:
    """Action-oriented narrative: name the favorite's rooting interest in
    THIS game, with both spot-delta and point-delta in the gap.

    Pattern:
      <Favorite> fans: rooting against <Nearby> (<spots> and <pts> <dir>).
      [Optional outcome clause when the gap is interesting, ≤ 9 pts.]

    Examples (the favorite isn't playing — these games are between OTHER
    teams whose result moves the favorite's standings):

      Manchester City fans: rooting against Manchester United (1 spot and
        12 pts back).
        # No outcome clause — 12 pts is too large for a single result.

      Manchester City fans: rooting against Manchester United (1 spot and
        3 pts back). A Manchester United win flips them past you.
        # Win-erasable gap → flip clause.

      Wrexham fans: rooting against Southampton (1 spot and 6 pts ahead).
        A Southampton loss could narrow the gap to 3 pts.
        # Catchable gap → conditional outcome clause.
    """
    fav_short = strip_team_suffix(fav_name)
    nearby_short = strip_team_suffix(nearby_team)
    spot_diff = nearby_pos - fav_pos  # >0 = nearby is below fav (chasing)

    spots = abs(spot_diff)
    spots_word = f"{spots} spot{'' if spots == 1 else 's'}"
    direction = "back" if spot_diff > 0 else "ahead"

    if fav_points is not None and nearby_points is not None:
        pt_diff_abs = abs(fav_points - nearby_points)
        pts_word = f"{pt_diff_abs} pt{'' if pt_diff_abs == 1 else 's'}"
        gap_str = f"{spots_word} and {pts_word} {direction}"
    else:
        gap_str = f"{spots_word} {direction}"

    sentence = f"{fav_short} fans: rooting against {nearby_short} ({gap_str})."

    # Outcome clause: only fires when the gap is interesting enough to
    # narrate (≤ 9 pts in either direction). For huge gaps the rooting
    # framing alone carries the message; saying "narrows to 9 pts" of a
    # 12-pt gap is still huge and clutters the description.
    if fav_points is not None and nearby_points is not None:
        pt_diff = fav_points - nearby_points  # >0 = fav has more pts
        if spot_diff > 0:  # fav leading
            new_gap = pt_diff - 3
            if pt_diff <= 3:
                sentence += f" A {nearby_short} win flips them past you."
            elif pt_diff <= 9:
                sentence += f" A {nearby_short} win narrows the gap to {new_gap} pts."
        elif spot_diff < 0:  # fav chasing
            current_gap = abs(pt_diff)
            if 0 < current_gap <= 9:
                potential_gap = current_gap - 3
                if potential_gap <= 0:
                    sentence += f" A {nearby_short} loss could put you level."
                else:
                    sentence += f" A {nearby_short} loss could narrow the gap to {potential_gap} pts."

    return sentence


def build_impact_narratives(
    rank_home: Optional[int], rank_away: Optional[int],
    home: str, away: str,
    favorites_with_standings: List[Dict],
    standings_table: List[Dict],
    proximity: int = 3,
) -> List[str]:
    """For each favorite within proximity of either game team, build one
    natural-language sentence (via render_favorite_impact). Skips favorites
    that are themselves playing in this game (handled by the 'favorite'
    signal already).

    `favorites_with_standings`: [{"name": str, "position": int, "points": int|None}, ...]
    `standings_table`: same shape, the full league.
    """
    out: List[str] = []
    home_lc, away_lc = home.lower(), away.lower()
    pts_lookup = {e.get("name"): e.get("points") for e in standings_table}

    for fav in favorites_with_standings:
        fav_name = fav["name"]
        fav_lc = fav_name.lower()
        if fav_lc in home_lc or fav_lc in away_lc:
            continue  # favorite is playing — skip impact narrative
        fav_pos = fav.get("position")
        if fav_pos is None:
            continue
        # Find which game team is closest (by spots) to the favorite within proximity.
        candidates = []
        for game_name, game_rank in [(home, rank_home), (away, rank_away)]:
            if game_rank is None:
                continue
            d = abs(game_rank - fav_pos)
            if d <= proximity:
                candidates.append((game_name, game_rank, d))
        if not candidates:
            continue
        # Closest first; tie-break = home team (gives a stable order).
        candidates.sort(key=lambda c: (c[2], 0 if c[0] == home else 1))
        team_name, team_pos, _ = candidates[0]
        out.append(render_favorite_impact(
            fav_name, fav_pos, fav.get("points"),
            team_name, team_pos, pts_lookup.get(team_name),
        ))
    return out


def build_why_text(
    rank_home: Optional[int],
    rank_away: Optional[int],
    favorites_matched: List[str],
    score_breakdown: Dict[str, float],
    spread: Optional[float] = None,
    stakes_thresholds: Optional[List[str]] = None,
    tournament_stage: Optional[str] = None,
    impact_on_favorites: Optional[List[str]] = None,
    season_progress: Optional[float] = None,
    rank_source: str = "poll",
) -> str:
    """Human-readable explanation of why this game made the cut. Used for
    the score-breakdown one-liner at the bottom of the EPG description.

    For poll-based sports (rank_source='poll', e.g. NCAAF AP Top 25), rank
    pair gets a 'top-N' label. For standings-based sports
    (rank_source='standings', e.g. EPL where every team is ranked), the
    rank-pair label is dropped — the stakes signal carries league-aware
    semantics ('title race', 'playoff race', 'relegation battle') and is
    what users actually care about.
    """
    parts: List[str] = []

    if rank_source == "poll":
        if "rank_pair" in score_breakdown and rank_home is not None and rank_away is not None:
            lo, hi = sorted([rank_home, rank_away])
            if hi <= 5:
                parts.append(f"both top-5 (#{lo} vs #{hi})")
            elif hi <= 10:
                parts.append(f"both top-10 (#{lo} vs #{hi})")
            elif lo <= 5:
                parts.append(f"top-5 ranked (#{lo} vs #{hi})")
            else:
                parts.append(f"both ranked (#{lo} vs #{hi})")
        elif "one_ranked" in score_breakdown:
            rank = rank_home if rank_home is not None else rank_away
            if rank is not None:
                parts.append(f"#{rank} ranked")

    if favorites_matched:
        if len(favorites_matched) == 1:
            parts.append(f"favorite ({favorites_matched[0]})")
        else:
            parts.append(f"favorites ({', '.join(favorites_matched)})")

    if "stakes" in score_breakdown and stakes_thresholds:
        labels = list(dict.fromkeys(stakes_thresholds))[:2]
        if labels:
            stakes_str = " / ".join(labels) + " stakes"
            if season_progress is not None and season_progress >= 0.85:
                stakes_str += " (final stretch ×2)"
            elif season_progress is not None and season_progress >= 0.70:
                stakes_str += " (late season ×1.5)"
            parts.append(stakes_str)

    if "tournament_stage" in score_breakdown and tournament_stage:
        parts.append(f"{tournament_stage.lower().replace('_', ' ')}")

    if "impact_on_favorite" in score_breakdown and impact_on_favorites:
        names = [strip_team_suffix(n) for n in impact_on_favorites]
        if len(names) == 1:
            parts.append(f"impact on {names[0]}")
        else:
            parts.append(f"impact on {', '.join(names)}")

    if "close_game" in score_breakdown and spread is not None:
        if spread <= 3:
            parts.append(f"toss-up (line {spread:+.1f})")
        elif spread <= 7:
            parts.append(f"close spread ({spread:+.1f})")

    if "rivalry" in score_breakdown:
        parts.append("rivalry game")

    if "narrative" in score_breakdown:
        parts.append("LLM narrative bonus")

    return ", ".join(parts) if parts else "interesting matchup"
