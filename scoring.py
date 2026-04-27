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
    """Per-league data the plugin uses to compute stakes signals."""
    code: str                    # 'PL', 'ELC', 'CL', etc.
    matchdays_total: int         # season length (38 for EPL, 46 for ELC, etc.)
    thresholds: List[Tuple[int, str]] = field(default_factory=list)
    # List of (position, label) — e.g., [(1,'title'),(4,'UCL'),(17,'relegation')]


LEAGUE_CONTEXTS: Dict[str, LeagueContext] = {
    "PL": LeagueContext(
        code="PL", matchdays_total=38,
        thresholds=[(1, "title"), (4, "UCL"), (7, "Europa/Conference"), (17, "relegation")],
    ),
    "ELC": LeagueContext(
        code="ELC", matchdays_total=46,
        thresholds=[(2, "auto-promotion"), (6, "playoff"), (21, "relegation")],
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

    raw=2  → 2.4
    raw=4  → 4.6
    raw=8  → 7.6
    raw=12 → 8.8
    raw=16 → 9.5
    raw=24 → 9.96
    """
    if raw <= 0:
        return 0.0
    return 10.0 * math.tanh(raw / _FINAL_KNEE)


# Trailing tokens that mean "same team" (typically the team-type / club suffix).
# When these follow a favorite name, we allow the match.
# Not exhaustive but covers EPL/EFL + most UCL teams + some general qualifiers.
TEAM_QUALIFIER_TOKENS = {
    "fc", "afc", "cf", "sc", "f.c.", "a.f.c.",
    # English football suffixes / second-words
    "city", "united", "town", "county", "athletic", "albion", "rovers",
    "forest", "wanderers", "rangers", "palace", "hotspur", "villa",
    "wednesday", "hove", "end", "north", "olympic", "olympique", "real",
    "&",
    # Common stadium/branding tokens
    "stadium",
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


def _score_for_display(score: GameScore) -> float:
    """Backwards-compatible helper: returns the user-facing 0-10 score.
    Older callers might construct a GameScore directly with `raw=...` only;
    in that case we surface raw as the display value."""
    return getattr(score, "final", None) or score.raw


def format_channel_name(
    sport_prefix: str,
    signals: GameSignals,
    score: GameScore,
    home: str,
    away: str,
    why: str = "",
) -> str:
    """Build the Dispatcharr channel name. Format puts the most-load-bearing
    info first so it's visible in tight UIs.

    The rank pair is normalized so the better (lower-number) rank always
    appears first — "1v5" not "5v1" — so the user can scan a list and see
    matchup quality at a glance regardless of home/away.

      CFB 1v5 ★8.5: Ohio State at Penn State — both ranked top-5, close spread
      CFB ⭐ ★6.1: NC State at UNC — favorite involved
      EPL ★5.2: Hull v Wrexham — playoff race
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

    parts.append(f"★{_score_for_display(score):.1f}")
    head = " ".join(parts)

    matchup = f"{away} at {home}"
    name = f"{head}: {matchup}"
    if why:
        name = f"{name} — {why}"
    # Channel.name is varchar(255). Truncate defensively.
    if len(name) > 250:
        name = name[:247] + "..."
    return name


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
) -> str:
    """Human-readable explanation of why this game made the cut.

    Walks the score breakdown and emits a comma-separated list of reasons,
    most-impactful first. Designed to be appended to the channel name so
    the user can see WHY at a glance in their guide.
    """
    parts: List[str] = []

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
            if rank <= 5:
                parts.append(f"#{rank} ranked vs unranked")
            else:
                parts.append(f"#{rank} ranked")

    if favorites_matched:
        if len(favorites_matched) == 1:
            parts.append(f"favorite ({favorites_matched[0]})")
        else:
            parts.append(f"favorites ({', '.join(favorites_matched)})")

    # Phase 3 — standings/stakes signal first, since it's most actionable
    if "stakes" in score_breakdown and stakes_thresholds:
        # Render top-2 thresholds + late-season cue
        labels = list(dict.fromkeys(stakes_thresholds))[:2]  # dedupe, keep order
        if labels:
            stakes_str = " / ".join(labels) + " race"
            if season_progress is not None and season_progress >= 0.85:
                stakes_str += " (final stretch)"
            elif season_progress is not None and season_progress >= 0.70:
                stakes_str += " (late season)"
            parts.append(stakes_str)

    if "tournament_stage" in score_breakdown and tournament_stage:
        parts.append(f"{tournament_stage.lower().replace('_', ' ')}")

    if "impact_on_favorite" in score_breakdown and impact_on_favorites:
        if len(impact_on_favorites) == 1:
            parts.append(f"affects {impact_on_favorites[0]}")
        else:
            parts.append(f"affects {', '.join(impact_on_favorites)}")

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
