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

    Defaults reflect priority order: importance > rank > favorites > tournament/spread.
    Narrative defaults to 0 because the importance signal covers the structural
    "stakes" ground (champion / UCL / relegation leverage on each match). Enable
    LLM narrative by setting weight > 0 for game-flavor / storyline coverage that
    Monte Carlo can't capture.
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
    tournament: float = 1.5      # knockout-stage cup games
    narrative: float = 0.0       # LLM narrative score (disabled by default)
    # Phase C: Lahvička Monte Carlo importance. Per-game raw points are
    # sum_over_(team,outcome) of leverage × consequence_weight (leverage in
    # [0,1] from Kendall tau-c, weight from LEAGUE_CONTEXTS thresholds —
    # relegation=5, UCL=4, title=5, etc.). C.4 calibration target: 3.0.
    # That matches the typical "high-leverage relegation game" magnitude
    # (0.5 leverage × 5 weight × 2 teams = 5 raw, times 3.0 weight = 15
    # raw — the same order as a Phase-A relegation six-pointer with the
    # legacy stakes signal). Replaces the legacy stakes + impact_favorite
    # + _late_season_multiplier signal family (Phase C.4 cutover).
    importance: float = 3.0


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

    tournament_stage: Optional[str] = None  # 'FINAL', 'SEMI_FINALS', 'QUARTER_FINALS', etc.

    is_rivalry: bool = False
    narrative_score: Optional[float] = None  # 0-10 from LLM (last)

    # Phase C: Lahvička Monte Carlo importance, pre-weight. The plugin's
    # _action_refresh calls compute_match_importance for sources that
    # support it (currently SoccerSource only) and stashes the result here.
    # Sources that don't support importance leave the points at 0.0 and
    # score_game's importance block falls through without contributing.
    #
    # `importance_thresholds_hit` is the deduped list of outcome bands
    # with nonzero leverage on any queried team (the playing teams plus
    # in-league favorites). pick_tagline uses it for editorial taglines
    # ("relegation race" / "title race"). Replaces the pre-C.4
    # stakes_thresholds_hit field which was derived from the legacy
    # proximity heuristic.
    importance_points: float = 0.0
    importance_notes: List[str] = field(default_factory=list)
    importance_thresholds_hit: List[str] = field(default_factory=list)


# ---------- League-specific thresholds ----------

@dataclass
class LeagueContext:
    """Per-league data the plugin uses to compute stakes signals.

    `boundary_summary` is a one-line human description of how
    standings translate to outcomes (UCL spots, relegation, playoff
    qualification). Rendered in the EPG description as a reminder of
    WHY a position-based race matters.

    `thresholds` is a list of (cutoff, label, weight) triples. The
    weight is the cross-sport consequence weight from the
    leverage-times-consequence calibration: relegation = 5, UCL = 4,
    Europa = 2, knockout final = 5, UCL winner = 10, etc. See
    TUNING_REPORT.md's cross-sport calibration section in
    /coding/dispatcharr_ranked_matchups_sim/ for the rationale.

    `cutoff` is polymorphic by `format`:
      - format="league": int league position (e.g., 4 = top-4 UCL spot)
      - format="knockout": str round identifier matching the bracket
        source's stage labels (e.g., "QUARTER_FINALS"). A team is
        considered "in" the band if their `round_reached` equals or
        exceeds this round. The bracket source ranks rounds by depth.
      - format="win_count": int MINIMUM win count (e.g., 11 = 11+ wins
        gets the "11_wins" label). Cumulative: 11 wins gets "11_wins"
        AND every lower-cutoff band that's still set. NCAAF / NCAAM /
        NBA / MLB regular seasons use this.
      - format="points_count": int MINIMUM standings-points count.
        NHL uses this because OT / SO losses bank a partial point,
        making raw win count a misleading playoff predictor. Bands at
        95 / 100 / 110 / 125 points.
    """
    code: str                    # 'PL', 'ELC', 'CL', etc.
    matchdays_total: int         # season length (38 for EPL, 46 for ELC). 0 = N/A (knockouts).
    thresholds: List[Tuple[Any, str, float]] = field(default_factory=list)
    boundary_summary: str = ""   # e.g. "Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation"
    format: str = "league"       # "league" | "knockout" | "win_count" | "points_count"


# Knockout-round depth ordering. Higher = deeper. The bracket source's
# `terminal_outcomes` uses this to assign every band a team has reached
# (a finalist reached the final AND the semis AND the quarters etc.).
# DO NOT use stage names not present here as knockout thresholds — the
# round-rank lookup would silently return -1 and no team would qualify.
KNOCKOUT_ROUND_DEPTH: Dict[str, int] = {
    # UEFA cup soccer (UCL / UEL / UECL):
    "PLAYOFFS":        0,  # UCL play-off round (pos 9-24 entrants)
    # International tournaments (Phase I) use LAST_32 as the entry-level
    # knockout round in the new 48-team World Cup 2026 format. Same depth
    # as PLAYOFFS because both are "before LAST_16"; no competition uses
    # both stages, so they don't collide in the bracket cascade.
    "LAST_32":         0,
    "LAST_16":         1,
    "QUARTER_FINALS":  2,
    "SEMI_FINALS":     3,
    "FINAL":           4,
    "WINNER":          5,  # synthetic: only the cup winner reaches this
    # NHL Stanley Cup Playoffs (best-of-7 each round, 4 rounds):
    "R1":              0,  # First round (8 series, 16 teams)
    "R2":              1,  # Second round / Division finals (4 series)
    "CONF_FINAL":      2,  # Conference finals (2 series)
    "CUP_FINAL":       3,  # Stanley Cup Final (1 series)
    "CUP_WINNER":      4,  # Stanley Cup champion
    # MLB postseason (Phase F): Wild Card best-of-3 (6 series across both
    # leagues), Division Series best-of-5 (4 series), LCS best-of-7 (2
    # series), World Series best-of-7. WC and LDS depths differ — a team
    # that wins the WC reaches LDS, etc.
    "WC":              0,  # Wild Card Series (entry round)
    "LDS":             1,  # Division Series
    "LCS":             2,  # League Championship Series
    "WS":              3,  # World Series
    "WS_WINNER":       4,  # World Series champion
    # NBA playoffs (Phase G): 4 rounds, best-of-7 each. R1 depth 0 is
    # already populated by NHL above; that's fine — the bracket source's
    # `terminal_outcomes` reads per-league band cutoffs, so R1 sharing a
    # depth label across NHL and NBA doesn't cause cross-contamination.
    "CSF":             1,  # Conference Semifinals (4 series)
    "CF":              2,  # Conference Finals (2 series)
    "FINALS":          3,  # NBA Finals (1 series) / WNBA Finals (1 series)
    "FINALS_WINNER":   4,  # NBA Champion
    # WNBA playoffs (Phase K): 3 rounds, mixed series lengths.
    # R1 (depth 0) and FINALS (depth 3) reuse the labels already in
    # this table — terminal_outcomes reads per-league bands so no
    # cross-contamination. SF needs its own depth between R1 and FINALS.
    "SF":              1,  # WNBA Semifinals (between R1 and FINALS)
    "WNBA_WINNER":     4,  # WNBA Champion (same synthetic depth as FINALS_WINNER)
    # NCAA Women's March Madness (Phase L): 6 rounds, all single-game
    # elimination. SERIES_LENGTH=1 per stage; first to 1 win clinches.
    "R64":             0,  # 1st Round (32 games)
    "R32":             1,  # 2nd Round (16 games)
    "S16":             2,  # Sweet 16 (8 games)
    "E8":              3,  # Elite 8 (4 games)
    "F4":              4,  # Final Four (2 games)
    "NCG":             5,  # National Championship Game
    "NCG_WINNER":      6,  # National Champion
    # NFL playoffs (Phase P): 4 rounds, single-game elimination.
    # SERIES_LENGTH=1 per stage; clinches at 1 win. The "WC" label
    # entry above (depth 0) is shared between MLB Wild Card Series
    # and NFL Wild Card Playoffs — both happen to be at depth 0 in
    # their respective brackets, and terminal_outcomes reads
    # per-league bands so there's no cross-contamination (same
    # pattern as R1 shared across NHL and NBA, FINALS shared across
    # NBA and WNBA).
    "DIV":             1,  # Divisional Playoffs
    "CONF":            2,  # Conference Championship
    "SB":              3,  # Super Bowl
    "SB_WINNER":       4,  # Super Bowl Champion
}


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
    "CL": LeagueContext(
        code="CL", matchdays_total=0, format="knockout",
        thresholds=[
            ("LAST_16",        "round_of_16",   1.0),
            ("QUARTER_FINALS", "quarterfinal",  2.0),
            ("SEMI_FINALS",    "semifinal",     3.0),
            ("FINAL",          "final",         5.0),
            ("WINNER",         "winner",       10.0),
        ],
        boundary_summary="R16 → QF → SF → Final → Champion",
    ),
    # Phase H: top-flight European leagues. Slot semantics differ by league:
    #   - UCL slots: Bundesliga 4, La Liga 4, Serie A 4, Ligue 1 3 (one
    #     fewer; FL1's 4th place enters UEL/UECL playoff).
    #   - Bundesliga: 16th plays relegation playoff vs Bundesliga 2's 3rd;
    #     17-18 directly relegated. We use cutoff=15 (i.e. position > 15
    #     == positions 16/17/18) to fire the relegation band for all three.
    #   - La Liga / Serie A (20 teams): bottom 3 (18-20) relegated.
    #   - Ligue 1 (18 teams): 16-18 relegated (same shape as BL1).
    # Cross-sport weights mirror EPL's slot-by-slot importance because the
    # consequences (UCL prize money, relegation revenue hit) are roughly
    # comparable across the Big Five. Tuning each weight per-league
    # would be premature without season-replay data.
    "BL1": LeagueContext(
        code="BL1", matchdays_total=34,
        thresholds=[
            (1,  "title",             5.0),
            (4,  "UCL",               4.0),
            (6,  "Europa/Conference", 2.0),
            (15, "relegation",        5.0),
        ],
        boundary_summary="Top 4 → UCL · 5-6 → Europa · bottom 3 → relegation",
    ),
    "PD": LeagueContext(
        code="PD", matchdays_total=38,
        thresholds=[
            (1,  "title",             5.0),
            (4,  "UCL",               4.0),
            (7,  "Europa/Conference", 2.0),
            (17, "relegation",        5.0),
        ],
        boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
    ),
    "SA": LeagueContext(
        code="SA", matchdays_total=38,
        thresholds=[
            (1,  "title",             5.0),
            (4,  "UCL",               4.0),
            (7,  "Europa/Conference", 2.0),
            (17, "relegation",        5.0),
        ],
        boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
    ),
    "FL1": LeagueContext(
        code="FL1", matchdays_total=34,
        thresholds=[
            (1,  "title",             5.0),
            (3,  "UCL",               4.0),
            (5,  "Europa/Conference", 2.0),
            (15, "relegation",        5.0),
        ],
        boundary_summary="Top 3 → UCL · 4-5 → Europa · bottom 3 → relegation",
    ),
    # Phase Q: Eredivisie (Netherlands). 18 teams, 34 matchdays. Top 1
    # direct UCL group stage; 2-3 UCL qualifying; 4-5 Europa/Conference
    # qualifying; bottom 1 direct relegation, 16-17 relegation playoff
    # (we use cutoff=15 to fire the relegation band for 16/17/18 like
    # Bundesliga).
    "DED": LeagueContext(
        code="DED", matchdays_total=34,
        thresholds=[
            (1,  "title",             5.0),
            (3,  "UCL",               4.0),
            (5,  "Europa/Conference", 2.0),
            (15, "relegation",        5.0),
        ],
        boundary_summary="Top 3 → UCL · 4-5 → Europa · bottom 3 → relegation",
    ),
    # Phase Q: Primeira Liga (Portugal). 18 teams, 34 matchdays. Same
    # slot semantics as Eredivisie — 2 direct UCL spots, 3rd UCL
    # qualifying, 4-5 Europa, bottom 3 relegation.
    "PPL": LeagueContext(
        code="PPL", matchdays_total=34,
        thresholds=[
            (1,  "title",             5.0),
            (3,  "UCL",               4.0),
            (5,  "Europa/Conference", 2.0),
            (15, "relegation",        5.0),
        ],
        boundary_summary="Top 3 → UCL · 4-5 → Europa · bottom 3 → relegation",
    ),
    # Phase Q: Brazilian Série A. 20 teams, 38 matchdays. Different
    # slot semantics from Euro leagues — continental qualifications
    # are for Copa Libertadores (top 6 in modern era) and Copa
    # Sudamericana (7-12). No UCL line. Bottom 4 relegated to Série B.
    "BSA": LeagueContext(
        code="BSA", matchdays_total=38,
        thresholds=[
            (1,  "title",             5.0),
            (6,  "libertadores",      4.0),
            (12, "sudamericana",      2.0),
            (16, "relegation",        5.0),
        ],
        boundary_summary="Top 6 → Libertadores · 7-12 → Sudamericana · bottom 4 → relegation",
    ),
    # Phase I: international tournaments.
    #
    # World Cup 2026 onward uses the 48-team format: 12 groups of 4, top
    # 2 + best 8 third-place advance to a LAST_32 round. Pre-2026 World
    # Cups used 32 teams entering at LAST_16. The LEAGUE_CONTEXTS entry
    # below describes the post-2026 bracket; if you point this at an
    # older WC season FD.org will publish no LAST_32 matches and the
    # bracket source will treat LAST_16 as the entry round automatically.
    #
    # Weights ramp aggressively for international finals because they're
    # the highest-viewership soccer matches by an order of magnitude.
    # WC Final / Winner outweigh UCL equivalents (5.0 / 10.0) because
    # the WC happens once every 4 years vs UCL every year, concentrating
    # consequence.
    #
    # NB: GROUP_STAGE importance isn't modeled in V1 — the bracket
    # source only tracks knockout-round eligibility. Group-stage
    # "survive and advance" games still pick up favorite + closeness
    # signal but importance reads 0. Filed as follow-up.
    "WC": LeagueContext(
        code="WC", matchdays_total=0, format="knockout",
        thresholds=[
            ("LAST_32",        "last_32",       0.5),
            ("LAST_16",        "round_of_16",   1.5),
            ("QUARTER_FINALS", "quarterfinal",  3.0),
            ("SEMI_FINALS",    "semifinal",     5.0),
            ("FINAL",          "final",         8.0),
            ("WINNER",         "winner",       15.0),
        ],
        boundary_summary="R32 → R16 → QF → SF → Final → Champion (FIFA World Cup)",
    ),
    "EC": LeagueContext(
        code="EC", matchdays_total=0, format="knockout",
        thresholds=[
            ("LAST_16",        "round_of_16",   1.0),
            ("QUARTER_FINALS", "quarterfinal",  2.5),
            ("SEMI_FINALS",    "semifinal",     4.0),
            ("FINAL",          "final",         6.0),
            ("WINNER",         "winner",       10.0),
        ],
        boundary_summary="R16 → QF → SF → Final → Champion (UEFA EURO)",
    ),
    # Phase M: NCAA Division I baseball regular season. Win-count
    # thresholds tuned against historical NCAA Tournament selection
    # criteria — ~35 wins is the rough at-large cutoff, 45+ wins puts
    # a team in national-seed (top 16) contention. Seasons run Feb-June
    # with ~55 regular-season games + conference tournament; matchdays_total
    # is approximate because non-conference scheduling varies by team.
    # CWS postseason bracket is not modeled in V1 (filed as follow-up).
    "BSB": LeagueContext(
        code="BSB", matchdays_total=55, format="win_count",
        thresholds=[
            (30, "tournament_bubble", 1.0),  # bid uncertainty
            (35, "at_large_lock",     2.0),  # likely tournament bid
            (40, "regional_top_seed", 3.0),  # top regional seed contender
            (45, "national_seed",     4.0),  # top-16 / national seed
            (50, "overall_one_seed",  5.0),  # #1 overall seed conversation
        ],
        boundary_summary="30+ wins → tournament bubble · 35+ → at-large lock · 45+ → national seed",
    ),
    # Phase O: NCAA Division I soccer (Men's and Women's). Standings-points
    # format (3 W / 1 D / 0 L) because draws are common in college soccer
    # and a draw-heavy team's WIN count understates their tournament
    # position. A team going 13-3-8 has 13 wins but 47 standings points
    # and is well above the tournament bubble. Bands tuned against the
    # United Soccer Coaches Top 25 historical NCAA Tournament cutoffs:
    # ~25 points is the bubble; 45+ puts a team in top-regional / seed
    # contention. Postseason College Cup bracket is not modeled in V1
    # (single-elimination — filed as follow-up; reuses the existing
    # KnockoutSoccerSource machinery once API bracket data is parsed).
    "NCAA_MSOC": LeagueContext(
        code="NCAA_MSOC", matchdays_total=20, format="points_count",
        thresholds=[
            (25, "tournament_bubble", 1.0),
            (35, "at_large_lock",     2.0),
            (45, "top_regional",      3.0),
            (50, "national_seed",     4.0),
        ],
        boundary_summary="25+ pts → bubble · 35+ → at-large lock · 45+ → top regional · 50+ → national seed (Men's)",
    ),
    "NCAA_WSOC": LeagueContext(
        code="NCAA_WSOC", matchdays_total=20, format="points_count",
        thresholds=[
            (25, "tournament_bubble", 1.0),
            (35, "at_large_lock",     2.0),
            (45, "top_regional",      3.0),
            (50, "national_seed",     4.0),
        ],
        boundary_summary="25+ pts → bubble · 35+ → at-large lock · 45+ → top regional · 50+ → national seed (Women's)",
    ),
    # Phase D.2 / D.3: NCAA football and men's basketball. Format is
    # "win_count" — threshold cutoffs are MINIMUM win counts rather than
    # position cutoffs (the SoccerSource interpretation). The points-
    # based source's `terminal_outcomes` assigns a label when a team's
    # win total >= cutoff. Bands chosen to differentiate marquee from
    # middling seasons.
    "CFB": LeagueContext(
        code="CFB", matchdays_total=12, format="win_count",
        thresholds=[
            (6,  "bowl_eligible", 2.0),  # 6 wins = bowl game lock
            (8,  "8_wins",        3.0),  # strong season, NY6 talk
            (10, "10_wins",       4.0),  # top-25 / CFP edge
            (11, "11_wins",       5.0),  # near-certain CFP, elite season
        ],
        boundary_summary="6+ wins → bowl eligible · 8+ → strong · 10+ → CFP contender",
    ),
    "CBB": LeagueContext(
        code="CBB", matchdays_total=30, format="win_count",
        thresholds=[
            (15, "15_wins", 1.5),  # bubble / NIT consideration
            (20, "20_wins", 3.0),  # near-lock NCAA tournament bid
            (25, "25_wins", 5.0),  # elite season, top seed candidate
        ],
        boundary_summary="15+ wins → NIT bubble · 20+ → NCAA bid · 25+ → elite",
    ),
    # Phase E: NHL regular season uses STANDINGS POINTS (regulation win = 2,
    # OT/SO loss = 1, regulation loss = 0) rather than raw wins because the
    # OT-loss bonus point makes a team with many OTLs look bubble-bound by
    # win count when they are actually playoff-locked. Bands chosen against
    # the 82-game season's historical playoff cutoffs (~95 pts has been the
    # average Eastern Conference wild-card line over the salary-cap era).
    "NHL": LeagueContext(
        code="NHL", matchdays_total=82, format="points_count",
        thresholds=[
            (95,  "playoff_bubble",     1.5),  # roughly the wild-card line
            (100, "playoff_secured",    2.5),  # comfortable in
            (110, "division_pace",      4.0),  # division-winner pace
            (125, "presidents_trophy",  5.0),  # Presidents' Trophy contender
        ],
        boundary_summary="95+ pts → playoff bubble · 100+ → comfortable · 110+ → div lead · 125+ → Presidents'",
    ),
    # Phase E: NHL Stanley Cup Playoffs (4 rounds, best-of-7 each).
    # Round structure is identical across both conferences: 16 teams in,
    # 1 champion out. Weights ramp aggressively into the Cup Final because
    # a Game 6 / Game 7 SCF is the single highest-leverage game any NHL
    # season produces.
    "NHL_PO": LeagueContext(
        code="NHL_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("R2",         "round_2",       1.0),
            ("CONF_FINAL", "conf_final",    2.5),
            ("CUP_FINAL",  "cup_final",     5.0),
            ("CUP_WINNER", "cup_winner",   10.0),
        ],
        boundary_summary="R1 → R2 → Conf Final → Cup Final → Champion",
    ),
    # Phase F: MLB regular season. 162-game season; the playoff bubble has
    # historically settled around 85-86 wins (3rd Wild Card cutoff post-
    # 2022 expansion), with division winners typically in the 90-100 win
    # range. 105+ has been a "best record in baseball" outlier most years.
    # Threshold field is `wins` (LEAGUE_CONTEXTS["MLB"].format="win_count").
    "MLB": LeagueContext(
        code="MLB", matchdays_total=162, format="win_count",
        thresholds=[
            (85,  "playoff_bubble",   1.5),  # 3rd wild card cutoff
            (90,  "playoff_secured",  2.5),  # comfortable in
            (95,  "division_pace",    4.0),  # division-winner pace
            (105, "elite",            5.0),  # best-record-in-baseball pace
        ],
        boundary_summary="85+ wins → bubble · 90+ → comfortable · 95+ → div lead · 105+ → elite",
    ),
    # Phase F: MLB postseason. Series lengths vary (WC=3, LDS=5, LCS=7,
    # WS=7), so the leverage ramp into the World Series is sharper than
    # NHL's Stanley Cup ramp — fewer total games means each single game
    # carries more series-decision weight. Weights tuned to put a Game 7
    # World Series at the top of the season's importance distribution
    # while keeping Wild Card games meaningful (otherwise a single-elim
    # WC game would read as low-stakes despite being a season-on-the-
    # line moment).
    "MLB_PO": LeagueContext(
        code="MLB_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("LDS",       "division_series",  1.0),
            ("LCS",       "championship",     2.5),
            ("WS",        "world_series",     5.0),
            ("WS_WINNER", "ws_winner",       10.0),
        ],
        boundary_summary="WC → LDS → LCS → WS → Champion",
    ),
    # Phase G: NBA regular season. 82-game season; play-in tournament
    # (since 2020) opens 9th and 10th seeds onto the bracket via a
    # mini-playoff, but the play-in is structurally between the
    # regular season and the bracket — we treat it as separate from
    # the 16-team bracket the playoff source models. Threshold field
    # is `wins` (LEAGUE_CONTEXTS["NBA"].format="win_count").
    # Modern NBA (post-2010): the play-in cutoff has hovered around
    # 38-42 wins; 50 has been a comfortable playoff floor; 55+ tends
    # to clinch a top-3 seed; 65+ is historically great (Warriors
    # 73-9 era / OKC Thunder 68-14 in 2024-25).
    "NBA": LeagueContext(
        code="NBA", matchdays_total=82, format="win_count",
        thresholds=[
            (40, "play_in_bubble",   1.5),  # play-in tournament line
            (50, "playoff_secured",  2.5),  # comfortable in
            (55, "top_seed_pace",    4.0),  # top-3 seed pace
            (65, "elite",            5.0),  # historic-team pace
        ],
        boundary_summary="40+ wins → play-in · 50+ → comfortable · 55+ → top seed · 65+ → elite",
    ),
    # Phase G: NBA playoffs. 4 rounds (R1 / CSF / CF / FINALS),
    # best-of-7 each. Same structural shape as NHL Stanley Cup
    # Playoffs — weights mirror NHL's ramp because the
    # consequence-weight calibration is consistent across pro
    # sports' bracket leverage.
    "NBA_PO": LeagueContext(
        code="NBA_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("CSF",            "conf_semis",        1.0),
            ("CF",             "conf_finals",       2.5),
            ("FINALS",         "nba_finals",        5.0),
            ("FINALS_WINNER",  "finals_winner",    10.0),
        ],
        boundary_summary="R1 → Conf Semis → Conf Finals → NBA Finals → Champion",
    ),
    # Phase K: WNBA regular season. 40-game season; 8 of 12-13 teams
    # make playoffs (since 2022's expansion). The playoff bubble has
    # historically settled around 19-21 wins; top seed pace is ~28-32
    # in the current era. Thresholds are tighter than NBA's because
    # the season is shorter — equal "strength" of a 25-win WNBA team
    # ≈ 50-win NBA team relative to the league.
    "WNBA": LeagueContext(
        code="WNBA", matchdays_total=40, format="win_count",
        thresholds=[
            (20, "playoff_bubble",   1.5),  # 8th seed line
            (25, "playoff_secured",  2.5),  # comfortable in
            (30, "top_seed_pace",    4.0),  # top-2 seed pace
            (35, "elite",            5.0),  # historic
        ],
        boundary_summary="20+ wins → bubble · 25+ → comfortable · 30+ → top seed · 35+ → elite",
    ),
    # Phase K: WNBA playoffs. Three rounds with variable series
    # lengths (R1=3, SF=5, FINALS=5 in 2024 or 7 in 2025+). Weights
    # mirror NBA's playoff ramp; bracket structure differs (no
    # conference reseeding, fewer total games).
    "WNBA_PO": LeagueContext(
        code="WNBA_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("SF",           "wnba_semis",     1.0),
            ("FINALS",       "wnba_finals",    5.0),
            ("WNBA_WINNER",  "wnba_winner",   10.0),
        ],
        boundary_summary="R1 → Semis → WNBA Finals → Champion",
    ),
    # Phase L: NCAA Women's Basketball regular season. ~32-game D1 season;
    # 64 teams make the NCAA Tournament. Threshold bands cover the
    # selection / seeding lines: 20 wins is the rough at-large bubble
    # for power-conf teams, 25+ is a comfortable at-large lock, 28+
    # tends to anchor a top-4 seed line, 32+ contends for #1 overall.
    "NCAAW_BBALL": LeagueContext(
        code="NCAAW_BBALL", matchdays_total=32, format="win_count",
        thresholds=[
            (20, "tournament_bubble",   1.5),
            (25, "at_large_lock",       2.5),
            (28, "top_4_seed",          4.0),
            (32, "no_1_seed",           5.0),
        ],
        boundary_summary="20+ wins → bubble · 25+ → at-large lock · 28+ → top-4 seed · 32+ → #1 seed",
    ),
    # Phase L: NCAA Women's March Madness. 6 rounds, single-game
    # elimination at each step. Weights ramp steeper than the pro
    # bracket leagues because single-game elim concentrates leverage
    # — every game IS the series for the round.
    "NCAAW_BBALL_PO": LeagueContext(
        code="NCAAW_BBALL_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("R32",        "round_of_32",    1.0),
            ("S16",        "sweet_16",       2.0),
            ("E8",         "elite_8",        3.5),
            ("F4",         "final_four",     5.0),
            ("NCG",        "national_final", 7.0),
            ("NCG_WINNER", "national_champ", 10.0),
        ],
        boundary_summary="R64 → R32 → Sweet 16 → Elite 8 → Final Four → NCG → Champion",
    ),
    # Phase N: NCAA Division I Softball regular season. ~55-game season,
    # 64-team NCAA Tournament. Selection bands mirror BSB but tuned
    # slightly tighter — softball's RPI bar tends to clear at lower
    # win totals because the field is smaller / more concentrated.
    # WCWS bracket is double-elimination and not modeled in V1
    # (follow-up).
    "SBL": LeagueContext(
        code="SBL", matchdays_total=55, format="win_count",
        thresholds=[
            (30, "tournament_bubble",    1.5),
            (35, "at_large_lock",        2.5),
            (40, "top_regional_seed",    3.5),
            (45, "national_seed",        4.5),
            (50, "no_1_overall",         5.0),
        ],
        boundary_summary="30+ wins → bubble · 35+ → at-large lock · 40+ → top regional · 45+ → national seed · 50+ → #1 overall",
    ),
    # Phase P: NFL regular season. 17-game season since 2021;
    # 14 teams make playoffs (7 per conference, including a 7th seed
    # added in 2020). 9 wins has been the modern playoff bubble line;
    # 11 wins typically locks a division; 13+ is #1-seed pace.
    "NFL": LeagueContext(
        code="NFL", matchdays_total=17, format="win_count",
        thresholds=[
            (7,  "playoff_bubble",  1.5),  # 7-seed line late season
            (9,  "playoff_secured", 2.5),  # comfortable in
            (11, "division_winner", 4.0),  # division-clinching pace
            (13, "no_1_seed",       5.0),  # #1 seed contention
        ],
        boundary_summary="7+ wins → bubble · 9+ → comfortable · 11+ → division · 13+ → #1 seed",
    ),
    # Phase P: NFL playoffs. 4 rounds, single-game elimination.
    # Weights ramp aggressively because single-elim concentrates
    # leverage — a Wild Card upset eliminates a 12-win team in one
    # game. Same shape as March Madness (Phase L) but 4 rounds
    # instead of 6 because the field is 14 teams (7 per conference)
    # not 64.
    "NFL_PO": LeagueContext(
        code="NFL_PO", matchdays_total=0, format="knockout",
        thresholds=[
            ("DIV",       "divisional",   1.5),
            ("CONF",      "conf_champ",   3.5),
            ("SB",        "super_bowl",   6.0),
            ("SB_WINNER", "sb_winner",   10.0),
        ],
        boundary_summary="WC → Divisional → Conf Championship → Super Bowl → Champion",
    ),
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

    if signals.tournament_stage:
        ts = signals.tournament_stage.upper()
        stage_score = {
            "FINAL": 5.0,
            "SEMI_FINALS": 3.5, "SEMI_FINAL": 3.5,
            "QUARTER_FINALS": 2.5, "QUARTER_FINAL": 2.5,
            "ROUND_OF_16": 1.5, "LAST_16": 1.5,
            "ROUND_OF_32": 1.0, "LAST_32": 1.0,
            "PLAYOFF_ROUND": 1.0, "PLAYOFFS": 1.0,
            # Phase R: field events (F1 GP, NASCAR race, golf tour
            # weekly events). These have no two-team head-to-head
            # structure so the rank / favorite / closeness signals
            # don't apply — the tournament_stage band is what gets
            # them into the guide at all. "MAJOR" is for golf's four
            # majors (Masters / PGA / US Open / British Open) and
            # any future marquee racing event we want bumped above
            # the regular tour.
            "EVENT": 1.5,
            "MAJOR": 4.5,
        }.get(ts, 0.0)
        if stage_score > 0:
            tourn_pts = stage_score * weights.tournament
            breakdown["tournament_stage"] = round(tourn_pts, 2)
            notes.append(f"tournament stage: {ts.lower().replace('_', ' ')}")

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


def compute_match_importance(
    source: Any,                 # SportSource; Any-typed to avoid a circular import
    match: Any,                  # GameRow
    league_ctx: "LeagueContext",
    n_sims: int = 500,
    rng: Optional[Any] = None,   # random.Random; deferred for the same circular reason
    favorites_in_league: Optional[List[str]] = None,
) -> Tuple[float, List[str], List[str]]:
    """Lahvička Monte Carlo match importance, summed across the queried
    teams and the league's outcome bands, weighted by consequence.

    Queries cover:
      - The two teams playing the target match (home, away)
      - Every favorite in `favorites_in_league` who ISN'T already in the
        match (cross-team importance — this match's result affects the
        favorite's standings outcome, even though the favorite isn't
        playing). Restores the impact-on-favorites signal structurally —
        the legacy `compute_impact_on_favorites` proximity heuristic is
        retired in Phase C.4 because Monte Carlo handles it correctly.

    Returns `(raw_points, notes, thresholds_hit)` where:
      - `raw_points` is the sum of (leverage × weight) over all queries.
      - `notes` lists nonzero contributions sorted by descending magnitude,
        formatted "{team} {label}: 0.42 leverage × 5.0 = 2.10".
      - `thresholds_hit` is the deduped list of band labels with nonzero
        leverage on any queried team. Used by pick_tagline to surface
        "relegation race" / "title race" type editorial taglines.

    Returns (0.0, [], []) immediately when the source doesn't support
    importance simulation (caller should also gate, but defense in depth).
    Returns (0.0, [], []) when the league has no outcome bands (e.g., a
    knockout-only competition routed into this path by mistake).
    """
    from .simulation import monte_carlo_importance_batch
    if not getattr(source, "supports_importance", False):
        return 0.0, [], []
    if not league_ctx.thresholds:
        return 0.0, [], []

    # Teams to query: the two playing teams plus favorites in this league
    # who aren't already playing. Skipping the in-match favorites avoids
    # double-counting (their leverage is already in the home/away queries).
    teams_to_query: List[str] = [match.home, match.away]
    in_match_lc = {match.home.lower(), match.away.lower()}
    if favorites_in_league:
        for fav in favorites_in_league:
            if fav.lower() in in_match_lc:
                continue
            teams_to_query.append(fav)

    # (team, outcome_label) query list — len(teams) × N bands.
    queries: List[Tuple[str, str]] = []
    for team in teams_to_query:
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
    labels_hit: List[str] = []
    seen_labels: set = set()
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
        if label not in seen_labels:
            seen_labels.add(label)
            labels_hit.append(label)
    # Sort notes by descending contribution so the biggest signals lead.
    # Parse the trailing "= X.XX" since the contrib isn't in scope here;
    # cheap enough at 2-32 lines per game (depends on favorite count).
    notes.sort(key=lambda s: -float(s.rsplit("= ", 1)[1]))
    return raw, notes, labels_hit


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
    importance_thresholds: Optional[List[str]],
    tournament_stage: Optional[str],
    rank_a: Optional[int],
    rank_b: Optional[int],
    rank_source: str = "poll",
    closeness: Optional[float] = None,
) -> str:
    """Pick a single dominant tagline for the channel name. Priority:
       tournament-stage → importance bands → poll-rank-pair → toss-up → favorite.

    `rank_source` distinguishes poll-based ranks (NCAAF / NCAAM AP Top 25)
    where 'top-N' framing is meaningful, from standings-position ranks
    (EPL / EFL where every team in the league is automatically 'top-N').
    The importance signal's threshold list covers the league-position case
    with proper labels (title race, relegation, etc.) so we drop the
    rank-pair tagline for standings.
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
            "EVENT": "Event",
            "MAJOR": "Major",
        }
        if ts in stage_labels:
            return stage_labels[ts]

    if "importance" in score_breakdown and importance_thresholds:
        labels = list(dict.fromkeys(importance_thresholds))[:2]
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
    importance_thresholds: Optional[List[str]] = None,
    tournament_stage: Optional[str] = None,
    rank_source: str = "poll",
) -> str:
    """Human-readable explanation of why this game made the cut. Used for
    the score-breakdown one-liner at the bottom of the EPG description.

    For poll-based sports (rank_source='poll', e.g. NCAAF AP Top 25), rank
    pair gets a 'top-N' label. For standings-based sports
    (rank_source='standings', e.g. EPL where every team is ranked), the
    rank-pair label is dropped — the importance signal carries league-aware
    semantics ('title race', 'playoff race', 'relegation battle') and is
    what users actually care about. Phase C.4 retired the separate
    season-progress / impact_on_favorites parameters because the Monte
    Carlo importance signal subsumes both (late season → naturally higher
    leverage swings; cross-team impact → favorite-team importance queries).
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

    if "importance" in score_breakdown and importance_thresholds:
        labels = list(dict.fromkeys(importance_thresholds))[:2]
        if labels:
            parts.append(" / ".join(labels) + " stakes")

    if "tournament_stage" in score_breakdown and tournament_stage:
        parts.append(f"{tournament_stage.lower().replace('_', ' ')}")

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
