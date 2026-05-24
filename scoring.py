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
from typing import Any, Dict, List, Optional, Tuple

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
    # spread/closeness signal: applied to a [0, 1] coinflip-ness measure.
    # 1.0 = pick'em → full contribution; 0.0 = blowout → no contribution.
    # B.3 reformulated the underlying signal from raw spread to devigged
    # bookmaker probabilities (for soccer; NCAAF / NCAAM still convert
    # spread to coinflip-ness via score_game's fallback). Default bumped
    # from 0.1 to 3.0 because the underlying range is now [0, 1] instead
    # of [0, 7] — same per-game magnitude (~3 raw for a pick'em).
    spread: float = 3.0
    favorite: float = 6.0
    rivalry: float = 2.0
    # stakes was 2.0 in the pre-Phase-A.5 era when compute_team_stakes
    # returned un-weighted proximity points (0-3). After Phase A.5/A.6
    # the function returns leverage_in_[0,1] times consequence_weight
    # (2-5 range). To keep the combined stakes contribution from
    # saturating the score, reduce the user-tunable multiplier here.
    # Net effect: a relegation six-pointer (both teams at d=0,
    # weight 5.0, late_mult 2.0) now contributes 1.0*5*2*0.5 = 5 raw
    # per team = 10 raw total, instead of 60 (pre-fix).
    stakes: float = 0.5
    tournament: float = 1.5      # knockout-stage cup games
    impact_favorite: float = 1.0  # non-favorite game shifts favorite's table
    narrative: float = 0.0       # LLM narrative score (disabled by default)
    # Phase C: Lahvička Monte Carlo importance. Per-game raw points are
    # sum_over_(team,outcome) of leverage × consequence_weight (leverage in
    # [0,1] from Kendall tau-c, weight from LEAGUE_CONTEXTS thresholds —
    # relegation=5, UCL=4, title=5, etc.). Default 1.0 is the C.3 starting
    # value; the structural fix replaces stakes + impact_on_favorites +
    # late_mult in C.4, so this weight will likely climb to ~3.0-5.0 once
    # the legacy signals come out. For now, both signals fire alongside
    # each other so we can compare rankings before flipping the cutover.
    importance: float = 1.0


@dataclass
class GameSignals:
    """Per-game signal inputs. Sport-agnostic."""
    rank_a: Optional[int] = None      # team A's rank (None if unranked)
    rank_b: Optional[int] = None      # team B's rank
    team_a: str = ""
    team_b: str = ""
    favorite_match: List[str] = field(default_factory=list)  # which favorites match
    spread: Optional[float] = None    # absolute betting spread (NCAAF / NCAAM path)
    # B.3 coinflip-ness signal in [0, 1]. Soccer populates this from
    # devigged moneyline probabilities; NCAAF / NCAAM still populate
    # `spread`. score_game prefers closeness when present, falls back
    # to a normalized spread otherwise. DO NOT populate BOTH on the
    # same signal — pick the right path per source. See sources/base.py.
    closeness: Optional[float] = None

    # Phase 3 — standings + stage signals (sport-aware, computed by plugin from
    # source-provided context).
    stakes_a: float = 0.0             # 0-3 ish, how close team A is to a league threshold
    stakes_b: float = 0.0
    stakes_thresholds_hit: List[str] = field(default_factory=list)  # ['playoff','relegation']
    season_progress: float = 0.0      # 0.0-1.0 — late-season amplifies stakes
    tournament_stage: Optional[str] = None  # 'FINAL', 'SEMI_FINALS', 'QUARTER_FINALS', etc.
    # Rich impact-on-favorite tuples: (favorite_name, inherited_stakes_raw,
    # distance). `inherited_stakes_raw` is the favorite's own
    # compute_team_stakes result (pre-late_mult, pre-weights.stakes) — so a
    # non-favorite game that pivots a relegation-threshold favorite inherits
    # the same magnitude that the favorite gets in their own game, scaled by
    # how close the game's teams sit to the favorite's slot. See issue #11.
    # DO NOT collapse to List[str]; the names-only shape lost the magnitude
    # signal and made West Ham vs Leeds (Tottenham relegation-pivot) score
    # identically to a Bournemouth-Forest dead-rubber.
    impact_on_favorites: List[Tuple[str, float, int]] = field(default_factory=list)

    is_rivalry: bool = False
    narrative_score: Optional[float] = None  # 0-10 from LLM (last)

    # Phase C: Lahvička Monte Carlo importance points (pre-weight). The
    # plugin's _action_refresh calls compute_match_importance for sources
    # that support it (currently SoccerSource only) and stashes the
    # result here. Sources that don't support importance leave this at 0.0
    # and score_game's importance block falls through without contributing.
    importance_points: float = 0.0
    importance_notes: List[str] = field(default_factory=list)


# ---------- League-specific thresholds ----------

@dataclass
class LeagueContext:
    """Per-league data the plugin uses to compute stakes signals.

    `boundary_summary` is a one-line human description of how
    standings translate to outcomes (UCL spots, relegation, playoff
    qualification). Rendered in the EPG description as a reminder of
    WHY a position-based race matters.

    `thresholds` is a list of (position, label, weight) triples. The
    weight is the cross-sport consequence weight from the
    leverage-times-consequence calibration: relegation = 5, UCL = 4,
    Europa = 2, etc. See TUNING_REPORT.md's cross-sport calibration
    section in /coding/dispatcharr_ranked_matchups_sim/ for the
    rationale; the numbers encode Jake's intuition that "relegation
    matters more than Europa" within a single league and that the
    same outcome band can weigh differently across leagues (top-25
    means more in CFB than mid-table in EPL).
    """
    code: str                    # 'PL', 'ELC', 'CL', etc.
    matchdays_total: int         # season length (38 for EPL, 46 for ELC, etc.)
    thresholds: List[Tuple[int, str, float]] = field(default_factory=list)
    # List of (position, label, weight)
    # e.g., [(1,'title',5.0),(4,'UCL',4.0),(17,'relegation',5.0)]
    boundary_summary: str = ""   # e.g. "Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation"


LEAGUE_CONTEXTS: Dict[str, LeagueContext] = {
    "PL": LeagueContext(
        code="PL", matchdays_total=38,
        thresholds=[
            (1,  "title",             5.0),
            (4,  "UCL",               4.0),
            (7,  "Europa/Conference", 2.0),
            (17, "relegation",        5.0),
        ],
        boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
    ),
    "ELC": LeagueContext(
        code="ELC", matchdays_total=46,
        thresholds=[
            (2,  "auto-promotion", 4.5),
            (6,  "playoff",        3.0),
            (21, "relegation",     4.0),
        ],
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


# Compression knee. Lower N = scores saturate faster; higher N = more
# headroom. Set to 16.0 because sim runs across EPL+ELC 2025-26 showed
# 70% of games ending up at score >= 9.5 with knee=8.0 — the 0-10 scale
# became indistinguishable noise above 9. With knee=16.0, a typical good
# game (raw=20-30) lands at 7.7-8.6 and the differentiation comes back.
# See TUNING_REPORT.md finding #1 (score saturation) in
# /coding/dispatcharr_ranked_matchups_sim/ for the full distribution.
_FINAL_KNEE = 16.0


def _compress_to_10(raw: float) -> float:
    """Smooth 0-10 normalization. Preserves ordering, asymptotes at 10.

    Anchor values (knee = 16.0):
      raw=2  → 1.24
      raw=4  → 2.45
      raw=8  → 4.62
      raw=16 → 7.62
      raw=24 → 9.05
      raw=32 → 9.64
      raw=48 → 9.96
    """
    if raw <= 0:
        return 0.0
    return 10.0 * math.tanh(raw / _FINAL_KNEE)


# Spread (point-spread sports) fallback: scale where 0 = full
# closeness, _SPREAD_BLOWOUT = zero closeness. Anchored to 14 because
# the pre-B.3 formula maxed at spread=14 too — keeps NCAAF / NCAAM
# magnitudes continuous through the B.3 weight bump.
_SPREAD_BLOWOUT = 14.0


def _effective_closeness(closeness: Optional[float], spread: Optional[float]) -> Optional[float]:
    """Unify the two close-game signals into a single [0, 1] coinflip-ness
    measure that score_game multiplies by weights.spread.

    Precedence: closeness (probability-based, B.3 soccer path) wins
    when populated. Spread (point-based, NCAAF / NCAAM path) is the
    fallback for sources that haven't migrated to moneylines. Returns
    None when neither is available — score_game then skips the signal.
    """
    if closeness is not None:
        if closeness < 0:
            return 0.0
        if closeness > 1:
            return 1.0
        return closeness
    if spread is not None and spread >= 0:
        return max(0.0, (_SPREAD_BLOWOUT - spread) / _SPREAD_BLOWOUT)
    return None


def _late_season_multiplier(season_progress: float) -> float:
    """Boost on stakes/impact signals as the season runs out.

    1.0x normally; 1.5x past 70% of matchdays; 2.0x past 85%. Shared
    by the stakes block AND the impact-on-favorites block in
    score_game so they amplify identically when both fire late.

    DO NOT inline this back into score_game. Two call sites need
    identical thresholds; an inline copy will silently drift and
    break the "impact magnitude tracks own-game magnitude" invariant
    that issue #11 fixed.
    """
    if season_progress >= 0.85:
        return 2.0
    if season_progress >= 0.70:
        return 1.5
    return 1.0


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

    ra, rb = signals.rank_a, signals.rank_b
    if ra is not None and rb is not None:
        # Both ranked: more points the lower the sum (1+5=6 great, 24+25=49 OK).
        # Map sum [2..50] to score [10..0]. Linear, weighted.
        sum_ranks = ra + rb
        # 2 → 10, 26 → 5, 50 → 0
        rank_pts = max(0.0, (50 - sum_ranks) / 4.8) * weights.rank
        breakdown["rank_pair"] = round(rank_pts, 2)
        notes.append(f"both ranked: #{ra} vs #{rb} (sum={sum_ranks})")
    elif ra is not None or rb is not None:
        # One ranked, one unranked: scale by the ranked team's rank.
        # rank 1 → 4.0, rank 25 → 0.5
        rank = ra if ra is not None else rb
        assert rank is not None  # narrowing: at least one is not None by elif
        rank_pts = max(0.0, (26 - rank) / 6.0) * weights.rank
        breakdown["one_ranked"] = round(rank_pts, 2)
        notes.append(f"one ranked: #{rank} vs unranked")

    if signals.favorite_match:
        fav_pts = weights.favorite
        breakdown["favorite"] = round(fav_pts, 2)
        notes.append(f"favorite involved: {', '.join(signals.favorite_match)}")

    # Close-game signal: prefer B.3 closeness (devigged bookmaker
    # probabilities, [0,1]) when present; fall back to a spread-derived
    # normalization for sources that still emit raw point spreads.
    # Both paths produce a [0, 1] effective closeness so the weight
    # multiplies into the same magnitude range across sports.
    effective_closeness = _effective_closeness(signals.closeness, signals.spread)
    if effective_closeness is not None and effective_closeness > 0:
        close_pts = effective_closeness * weights.spread
        breakdown["close_game"] = round(close_pts, 2)
        if signals.closeness is not None:
            notes.append(f"implied coinflip-ness: {effective_closeness:.2f}")
        else:
            notes.append(f"betting spread: {signals.spread:+.1f} pts")

    # Phase 3 — stakes (proximity to meaningful league threshold), with
    # late-season multiplier so games matter more when season is winding down.
    late_mult = _late_season_multiplier(signals.season_progress)
    stakes_total = signals.stakes_a + signals.stakes_b
    if stakes_total > 0:
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
        # Inherit the affected favorite's own stakes contribution, scaled
        # by how close the game's teams sit to the favorite's slot
        # (distance=0 → full inheritance, distance=cap → 1/(cap+1)).
        # Late-season multiplier amplifies impact the same way it
        # amplifies the favorite's own stakes block, so an impact game
        # at MD37 tracks the urgency of the favorite's own MD37 game.
        # See issue #11 for the West Ham vs Leeds (Tottenham relegation)
        # worked example; flat +1 per favorite buried it at #8.
        leverage_denom = float(_IMPACT_PROXIMITY_CAP + 1)
        inherited_raw = 0.0
        for _, fav_stakes_raw, d in signals.impact_on_favorites:
            leverage = (_IMPACT_PROXIMITY_CAP + 1 - d) / leverage_denom
            inherited_raw += fav_stakes_raw * leverage
        impact_pts = inherited_raw * late_mult * weights.impact_favorite
        breakdown["impact_on_favorite"] = round(impact_pts, 2)
        names = [t[0] for t in signals.impact_on_favorites]
        notes.append(
            f"affects favorite{'s' if len(names) > 1 else ''}: "
            f"{', '.join(names)}"
        )

    if signals.is_rivalry:
        breakdown["rivalry"] = round(weights.rivalry, 2)
        notes.append("rivalry game")

    if signals.narrative_score is not None:
        narr_pts = signals.narrative_score / 10.0 * weights.narrative
        breakdown["narrative"] = round(narr_pts, 2)
        notes.append(f"LLM narrative score: {signals.narrative_score:.1f}/10")

    # Phase C: Monte Carlo importance (Lahvička). Already pre-weighted by
    # consequence inside compute_match_importance; multiply only by the
    # user's weight_importance tunable here. Sources that don't support
    # importance (NCAAFSource, NCAAMSource, knockout-only soccer) leave
    # importance_points at 0.0 and this block falls through. Gating BOTH
    # the points AND the weight keeps the breakdown clean when the user
    # disables the signal via weight_importance=0 — no 0.0 stub entries.
    if signals.importance_points > 0 and weights.importance > 0:
        imp_pts = signals.importance_points * weights.importance
        breakdown["importance"] = round(imp_pts, 2)
        # Add the top-contributor note lines, capped so the cache.json
        # notes block doesn't bloat. 3 leading lines = the 3 most-leveraging
        # (team, outcome) tuples; sufficient to explain why a game scored
        # high without dumping all 8 entries.
        for line in signals.importance_notes[:3]:
            notes.append(f"importance: {line}")

    raw = sum(breakdown.values())
    return GameScore(
        raw=round(raw, 2),
        final=round(_compress_to_10(raw), 2),
        breakdown=breakdown,
        notes=notes,
    )


def compute_team_stakes(
    team_position: Optional[int],
    league_thresholds: List[Tuple[int, str, float]],
    proximity: int = 2,
    *,
    team_points: int = 0,
    matches_remaining: int = 0,
    standings_points_by_position: Optional[Dict[int, int]] = None,
) -> Tuple[float, List[str]]:
    """How close is this team to a meaningful league threshold, weighted
    by the threshold's consequence weight, with mathematical
    elimination gating.

    Returns (points, hit_labels). Points per threshold:
      (proximity + 1 - d) * weight

    With weight=1.0 this reproduces the old un-weighted behavior:
    3 if exactly at threshold, 2 if adjacent (±1), 1 if ±2, 0 otherwise.

    Stacks across thresholds: a 4th-place EPL side is at the UCL line
    AND within proximity-2 of the title line; both fire.

    Elimination gating (only active when `standings_points_by_position`
    is provided — the soccer plugin has it; CFB/CBB / knockout comps
    don't, and pass nothing):

    - If the team is BELOW the threshold position (climbing toward
      promotion / a title / a higher band), drop the threshold when
      team_points + matches_remaining * 3 < threshold_team_points.
      That's "I cannot mathematically catch the team currently at the
      cutoff" — the band is locked out, no points should fire.

    - If the team is ABOVE the threshold position (defending a higher
      band against teams below), drop the threshold when
      threshold_team_points + matches_remaining * 3 < team_points.
      That's "no team can catch me down to this band" — locked in,
      the band stops being a live race for me.

    Without standings/remaining info, falls back to pure proximity
    (matches the pre-Phase-A behavior).
    """
    if team_position is None:
        return 0.0, []
    # Gate whenever the caller provided standings data. matches_remaining
    # may legitimately be 0 (final whistle of the season) — in that case
    # the math below treats current points as final, and locked positions
    # correctly drop out. Older signature didn't gate without standings;
    # that path is kept for knockout comps that have no league table.
    gating = standings_points_by_position is not None
    # Proximity is in [0, proximity+1]; normalize to [0, 1] before
    # multiplying by the consequence weight so the contribution stays
    # in the same magnitude as the report's calibration ("leverage in
    # [0,1] times weight" — see TUNING_REPORT.md's cross-sport
    # calibration section). Without this, a relegation match at d=0
    # would contribute (3 * 5) * late_mult * weights.stakes = 60 raw
    # which saturates _FINAL_KNEE long before other signals can speak.
    leverage_denom = float(proximity + 1)
    pts = 0.0
    hit: List[str] = []
    for cutoff, label, weight in league_thresholds:
        d = abs(team_position - cutoff)
        if d > proximity:
            continue
        if gating:
            # standings_points_by_position guaranteed non-None by the
            # gating bool above; assert reassures the type checker.
            assert standings_points_by_position is not None
            if team_position > cutoff:
                # Climber: below the cutoff (numerically larger
                # position number = worse standing in soccer tables).
                # The live opponent is the team currently AT the cutoff.
                threshold_team_points = standings_points_by_position.get(cutoff, 0)
                if team_points + matches_remaining * 3 < threshold_team_points:
                    continue  # locked out from above
            else:
                # Defender (team_position <= cutoff): on the winning
                # side of the cutoff line, including AT the cutoff
                # (the marginal team). The live opponent is the team
                # at cutoff+1 — the chaser who would push us over the
                # line if they catch.
                chaser_points = standings_points_by_position.get(cutoff + 1, 0)
                if chaser_points + matches_remaining * 3 < team_points:
                    continue  # locked in, race is decided
        leverage = float(proximity + 1 - d) / leverage_denom
        pts += leverage * weight
        hit.append(label)
    return pts, hit


# Max distance (in standings positions) between a game's team and a
# favorite's position for the game to count as "impacting" the favorite.
# Used by both compute_impact_on_favorites (to filter) AND score_game's
# impact block (to compute leverage decay). Keep in sync — divergence
# would mean the function emits tuples score_game can't normalize.
_IMPACT_PROXIMITY_CAP = 3


def compute_impact_on_favorites(
    rank_a: Optional[int], rank_b: Optional[int],
    team_a: str, team_b: str,
    favorites_in_league: List[Tuple[str, int, float]],  # [(name, position, own_stakes_raw), ...]
    proximity: int = _IMPACT_PROXIMITY_CAP,
) -> List[Tuple[str, float, int]]:
    """Favorites whose table position this game's outcome would move,
    paired with the stakes the favorite would earn in their own game.

    Returns [(fav_name, fav_own_stakes_raw, distance), ...] where
    distance is `min(|game_team_rank - fav_pos|)` across both game
    teams. distance=0 means a game team sits exactly in the favorite's
    slot — maximal swap risk.

    `fav_own_stakes_raw` is the favorite's compute_team_stakes result
    BEFORE late_mult and weights.stakes (so score_game can apply
    late_mult uniformly across stakes + impact). The caller pre-computes
    it; this function just carries it through.
    """
    affected: List[Tuple[str, float, int]] = []
    a_lc, b_lc = team_a.lower(), team_b.lower()
    for fav_name, fav_pos, fav_stakes_raw in favorites_in_league:
        fav_lc = fav_name.lower()
        # Skip games where the favorite IS playing (already covered by 'favorite' signal)
        if fav_lc in a_lc or fav_lc in b_lc:
            continue
        best_d: Optional[int] = None
        for r in [rank_a, rank_b]:
            if r is None:
                continue
            d = abs(r - fav_pos)
            if d <= proximity and (best_d is None or d < best_d):
                best_d = d
        if best_d is not None:
            affected.append((fav_name, fav_stakes_raw, best_d))
    return affected


def compute_match_importance(
    source: Any,                 # SportSource; Any-typed to avoid a circular import
    match: Any,                  # GameRow
    league_ctx: "LeagueContext",
    n_sims: int = 500,
    rng: Optional[Any] = None,   # random.Random; deferred for the same circular reason
) -> Tuple[float, List[str]]:
    """Lahvička Monte Carlo match importance, summed across the match's
    two teams and the league's outcome bands, weighted by consequence.

    Returns `(raw_points, notes)` where `raw_points` is
    `sum over (team in {home, away}) over band in league_ctx.thresholds of
    |tau_c(match, team, band.label)| * band.weight`.

    `notes` is a per-(team, label) line like "Tottenham relegation: 0.42
    × 5.0 = 2.10" — only the nonzero contributions are listed so the
    breakdown stays readable in cache.json.

    Returns (0.0, []) immediately when the source doesn't support
    importance simulation (caller should also gate, but defense in depth).
    Returns (0.0, []) when the league has no outcome bands (e.g., a
    knockout-only competition routed into this path by mistake).
    """
    from .simulation import monte_carlo_importance_batch
    if not getattr(source, "supports_importance", False):
        return 0.0, []
    if not league_ctx.thresholds:
        return 0.0, []

    # Build the (team, outcome_label) query list — 2 teams × N bands.
    queries: List[Tuple[str, str]] = []
    for team in (match.home, match.away):
        for _, label, _ in league_ctx.thresholds:
            queries.append((team, label))

    leverages = monte_carlo_importance_batch(
        source, match, queries, n_sims=n_sims, rng=rng,
    )

    # Map label → weight once so the contribution loop is single-pass.
    weight_by_label: Dict[str, float] = {
        label: weight for _, label, weight in league_ctx.thresholds
    }

    raw = 0.0
    notes: List[str] = []
    for (team, label), leverage in leverages.items():
        if leverage <= 0:
            continue
        weight = weight_by_label.get(label, 0.0)
        if weight <= 0:
            continue
        contrib = leverage * weight
        raw += contrib
        # Format: "Tottenham FC relegation: 0.42 leverage × 5.0 = 2.10".
        # Strip the team suffix for the note so the line stays readable —
        # the underlying signal still uses the canonical name.
        notes.append(
            f"{strip_team_suffix(team)} {label}: "
            f"{leverage:.2f} leverage × {weight:.1f} = {contrib:.2f}"
        )
    # Sort notes by descending contribution so the biggest signals lead.
    # Parse the trailing "= X.XX" since the contrib isn't in scope here;
    # cheap enough at 2-8 lines per game.
    notes.sort(key=lambda s: -float(s.rsplit("= ", 1)[1]))
    return raw, notes


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
    rank_a: Optional[int],
    rank_b: Optional[int],
    rank_source: str = "poll",
    closeness: Optional[float] = None,
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

    if "close_game" in score_breakdown:
        # closeness >= 0.7 (each team ≥35% to win in a 3-way) is the
        # B.3 equivalent of the old spread <= 3 threshold for "toss-up".
        if closeness is not None and closeness >= 0.7:
            return "toss-up"
        if spread is not None and 0 <= spread <= 3:
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
