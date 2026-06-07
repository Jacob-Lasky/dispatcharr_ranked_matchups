# How matchups are scored

A transparent walk-through of how this plugin decides which games are
"top matchups." If a game ended up higher (or lower) on your guide than
you expected, this doc lets you trace the math back to the input
signals.

The scoring pipeline has four stages:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  per-sport  │     │   per-      │     │  raw sum    │     │  channel    │
│   adapter   │ ──> │   signal    │ ──> │     +       │ ──> │   layout    │
│ fetches the │     │  scoring    │     │ compress to │     │ (today      │
│  games      │     │ (6 signals) │     │  0-10 star  │     │  first)     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

Stages 1 and 4 are mechanical. Stages 2 and 3 are where the
interestingness math lives.


## Stage 1: per-sport adapter

Each sport / league has a `SportSource` adapter that fetches upcoming
games and emits a `GameRow`:

```python
GameRow(
    sport_prefix="EPL",                        # "CFB", "NHL", "WC", etc.
    sport_label="English Premier League",
    home="Brentford FC",
    away="Manchester United FC",
    rank_home=None,                            # Top-25 / Top-N rank (None if unranked)
    rank_away=None,
    start_time=datetime(2026, 5, 24, 14, 0),   # kickoff in UTC
    spread=None,                               # pre-game point spread (NCAAF / NCAAM)
    closeness=0.72,                            # devigged bookmaker coin-flip-ness [0, 1]
    is_rivalry=False,                          # match against the rivalry DB
    extra={"fd_id": 503811, "matchday": 38},   # source-specific metadata
)
```

Adapters live under `sources/`. Each one knows its sport's API, its
ranking system, and its outcome bands (title race, playoff bubble,
relegation, knockout cup, etc.). The rest of the plugin treats every
`GameRow` identically.


## Stage 2: per-signal scoring

Each game gets a per-signal contribution measured in **raw points**.
Six signals fire (when applicable); raw points sum to a single number
that stage 3 compresses to ★0-10.

| Signal | Default weight | Range per game (typical) |
|---|---|---|
| `rank_pair` | 1.0 | 0 → 10 |
| `close_game` (closeness) | 3.0 | 0 → 3 |
| `favorite` | 6.0 | 0 or 6 (flat) |
| `tournament_stage` | 1.5 | 0 → 7.5 |
| `rivalry` | 2.0 | 0 or 2 (flat) |
| `importance` (Monte Carlo) | 3.0 | 0 → ~50 (peaks on relegation six-pointers, WC final) |
| `narrative` (LLM) | 0.0 (off) | 0 → 10 |

All weights are user-tunable from the Plugins settings page. The
defaults reflect priority order: importance > rank > favorites >
tournament/spread.

### Signal: rank_pair

For sports with a ranking (top-25 polls in NCAAF / NCAAM / NCAA
Baseball; standings position in soccer leagues; etc.) the rank-pair
signal rewards games featuring ranked teams.

```python
# Both teams ranked:
sum_ranks = rank_home + rank_away
rank_pts  = max(0, (50 - sum_ranks) / 4.8) * weights.rank
# Maps sum=2  ("1 vs 1") → 10 pts
#      sum=26 ("13 vs 13") → 5
#      sum=50 ("25 vs 25") → 0

# Only one team ranked:
rank_pts = max(0, (26 - rank) / 6.0) * weights.rank
# Maps rank=1  → 4.0
#      rank=25 → 0.17
```

Unranked vs unranked: signal contributes 0.

### Signal: close_game

A coin-flip game between evenly-matched teams is more interesting than
a 30-point favorite. Two paths into this signal depending on the sport:

**Soccer path** — sources populate `GameRow.closeness` directly from
the bookmaker's devigged h2h moneyline. The vig (the bookie's edge) is
stripped via a normalizing pass over the three outcomes (home / draw /
away), and the result is converted to coin-flip-ness:

```
closeness = 2 * min(p_home, p_away)
```

Range: [0, 1]. 1.0 means the bookies see this as a coin-flip; 0.0
means one side is heavily favored.

**NCAAF / NCAAM path** — sources populate `GameRow.spread` with the
absolute point spread. Conversion:

```
closeness = max(0, (14 - |spread|) / 14)
```

Spread 0 → 1.0; spread 14+ → 0.0.

Final contribution: `closeness × weights.spread` (default 3.0). A
true pick'em adds ~3 raw points.

### Signal: favorite

Flag your teams in the plugin settings. When any flagged team plays,
the game gets a flat bonus equal to `weights.favorite` (default 6.0).

The match is word-boundary aware (no false positives like "UNC
Pembroke" matching a "UNC" favorite) and respects club suffixes
(e.g., "Hull" matches "Hull City").

The EPG description calls out which favorite triggered the bonus.

### Signal: tournament_stage

Knockout cup / playoff games rank higher than regular season games of
the same kind. Per-stage scalar (multiplied by `weights.tournament`,
default 1.5):

| Stage | Scalar | Default contribution |
|---|---|---|
| `FINAL` | 5.0 | 7.5 |
| `SEMI_FINALS` | 3.5 | 5.25 |
| `QUARTER_FINALS` | 2.5 | 3.75 |
| `ROUND_OF_16` / `LAST_16` | 1.5 | 2.25 |
| `ROUND_OF_32` / `LAST_32` | 1.0 | 1.5 |
| `PLAYOFFS` (entry round) | 1.0 | 1.5 |
| `MAJOR` (golf majors, F1 Monaco, etc) | 4.5 | 6.75 |
| `EVENT` (regular-season field events) | 1.5 | 2.25 |

### Signal: rivalry

Known rivalry games get a flat bonus equal to `weights.rivalry`
(default 2.0). The shipped rivalry DB covers college football, EPL,
NHL, and NBA matchups. New rivalries are added to `rivalries.json`.

### Signal: narrative (off by default)

If you enable the LLM narrative bonus (set `weight_narrative > 0` and
provide an Anthropic API key), the plugin asks Claude to score a game
0-10 based on backstory: recent form, rivalries the rivalry DB
doesn't know about, ongoing storylines (manager firings, transfer
drama, player streaks). Contribution:

```
narrative_pts = (llm_score / 10.0) * weights.narrative
```

Default `weights.narrative = 0` so this signal is opt-in. The
structural "stakes" ground (championship leverage, relegation, playoff
positioning) is covered by the importance signal below; the narrative
signal is for the things Monte Carlo can't model.

### Signal: importance (Lahvička Monte Carlo)

This is the most important signal in practice — and the most
complex. It gets its own section below.


## The Lahvička Monte Carlo importance algorithm

Following Lahvička (2012), match importance is measured as the
**strength of association between a match's W/D/L result and whether
each team reaches each outcome at season's end** — averaged over all
the possible ways the rest of the season could play out.

The high-level loop:

```
for each (target_match, team, outcome) tuple:
    table = [[0, 0], [0, 0], [0, 0]]   # 3 result rows × 2 outcome cols
    for sim in range(1000):
        result = sample_result(target_match)               # W / D / L for target team
        for game in remaining_matches:
            game_result = sample_result(game)
            state = apply_result(state, game, game_result)
        outcome_happened = team in terminal_outcomes(state)[outcome]
        table[_result_row(result)][0 if outcome_happened else 1] += 1
    leverage = |kendall_tau_c(table)|     # in [0, 1]
    contribution += leverage * outcome_weight
```

For each candidate outcome (title, UCL spot, relegation, Stanley Cup
winner, R16 advancement, etc.), this asks the question: "in 1,000
simulated alternate universes, how much does the way this match goes
shift the probability of this team hitting this outcome?"

A handful of design choices matter here:

### The 3×2 contingency table

Rows are W / D / L from the target team's perspective. Columns are
"outcome happened" / "outcome did not happen." A typical table after
1,000 sims:

```
              outcome    outcome
              happened   didn't
Win    [team]   320         80
Draw   [team]   100        100
Loss   [team]    50        350
```

Read down a row: "when this team won the target match, they advanced
in 320 / (320+80) = 80% of seasons." Read across rows: "winning
correlates with advancing; losing correlates with not advancing."
Kendall tau-c on this table measures how STRONG that correlation is.

### Kendall tau-c

Stuart's 1953 tau-c statistic measures ordinal association between
two ordered categorical variables. Formula:

```
tau_c = 2m(P - Q) / (n² (m-1))

where:
  m = min(rows, cols) = 2 here
  n = sum of all cells
  P = concordant pairs   (both rankings move the same direction)
  Q = discordant pairs   (rankings move opposite directions)
```

Range: -1 to +1. The plugin takes `|tau_c|` because a "relegation"
outcome where winning REDUCES the probability is just as informative
as a "title" outcome where winning INCREASES it. Importance is the
*magnitude* of association, not its sign.

The choice of tau-c (rather than tau-a or tau-b) is deliberate: tau-c
handles square ties (cells on the diagonal where both teams' fortunes
are tied) correctly for non-square tables, which we always have
(3×2). Lahvička used tau-c for the same reason.

### Per-team strengths

`sample_result` needs to know each team's strength to draw a plausible
outcome. Each `SportSource` implements `estimate_strengths`:

- **Soccer:** rolling-window goals-for and goals-against averages per
  team (home / away separately). Poisson with rate `(home_attack +
  away_defense) / 2` per side.
- **NCAA football, NCAA basketball, NFL, NBA:** points-for and
  points-against averages. Poisson on points, with a coin-flip OT
  resolver for ties (NCAA games can't end in regulation ties; pro
  leagues handle this differently per sport).
- **NHL:** goals-for / goals-against averages, with an OT-loss-point
  resolver to populate `standings_points` correctly under the 2-pt
  regulation / 1-pt OT-loss rule.
- **MLB:** runs-for / runs-against averages, Poisson on runs.

Strengths are estimated once per refresh and reused across the 1,000
sims; the per-sim cost is just the per-match sampling.

### Outcome bands

Each `LeagueContext` declares which outcomes the simulator should
track. Examples:

**EPL (`LEAGUE_CONTEXTS["PL"]`):**
```
(1, "title",       5.0)   ← top-1 in final standings; consequence weight 5
(4, "UCL",         4.0)   ← top-4 (UCL group stage spot)
(5, "Europa",      2.0)   ← top-5/6 (Europa League)
(17, "relegation", 5.0)   ← positions 18-20 (relegation)
```

**Stanley Cup Playoffs (`LEAGUE_CONTEXTS["NHL_PO"]`):**
```
("R2",         "conf_semifinal",  1.5)
("CONF_F",     "conf_final",      2.5)
("CUP_FINAL",  "cup_final",       4.5)
("CHAMPION",   "winner",         10.0)
```

The `weight` column is the cross-sport **consequence weight** —
calibrated so that a relegation six-pointer scores comparably to a
title-race showdown scores comparably to a Stanley Cup conference
final. See the calibration notes in the legacy `TUNING_REPORT.md`
for the per-band rationale.

Final per-game importance contribution:

```
importance_points = Σ over (team, outcome) of leverage(team, outcome) × consequence_weight(outcome)
final_contribution = importance_points × weights.importance
```

Both teams in the match get queried for every band; favorites NOT in
the match also get queried for every band so their leverage carries
across the league (a Manchester United fan whose team isn't playing
still cares about the title race between Liverpool and Arsenal). The
EPG description shows the top 3 (team, outcome) leverage lines.

### N_sims = 1000

Lahvička's original paper used 10,000 sims for academic-grade
convergence. The plugin uses 1,000 because:

- Tau-c standard error scales as `1 / sqrt(N)`. Going from 1k → 10k
  halves the SE but 10x's runtime.
- The SE at 1k is already finer than the consequence-weight grid the
  rankings sort by. Rankings stay stable.
- 1k × 10 games per matchday × 2 competitions = 20k full-season
  simulations per refresh, ~50ms per sim in pure-Python Poisson. Fits
  comfortably in the daily-refresh budget.


## Cross-source chains (the World Cup 2026 case)

The Monte Carlo importance algorithm above runs single-source: it
samples the rest of THIS competition forward to its outcome bands.
That's the wrong shape for tournaments with a group→knockout
structure, where a group-stage match's "real" importance is mostly
how it shifts the team's chances of winning the bracket, not just of
advancing to it.

For FIFA World Cup 2026 the plugin implements a **cross-source chain**:

```
group game's importance
   = importance under the group-stage bands (advance / eliminated)
   + importance under the knockout-stage bands (R32 / R16 / QF / SF / Final / Champion)
```

The chain works by passing the simulated final group standings into a
"seeding function" that produces the LAST_32 bracket entry pairings;
the knockout source then takes over and sims forward through the
bracket. Tau-c is computed once per band (group bands from the group
source, knockout bands from the chained knockout source), and the
contributions sum.

The LAST_32 bracket seed has a wrinkle: FIFA's published Annex C
specifies a **deterministic 495-row lookup table** that maps every
possible combination of 8 advancing 3rd-placers (one per scenario
across the 12 groups) to a specific slot assignment in the bracket.
The plugin ships the full 495-row table at
`_WC2026_THIRD_PLACER_SLOT_TABLE` in `sources/soccer.py`, parsed from
FIFA's regulations via Wikipedia's machine-readable transcription.

Without the Annex C lookup, the simulator could pick a structurally
valid but non-canonical slot assignment — leverage would have the
right direction but possibly wrong magnitude because the simulated
R16 opponent might not match the canonical one. With the lookup, the
chain produces leverage that mirrors the real bracket.

The same chain machinery is wired for UEFA EURO 2028 once UEFA
publishes the bracket structure.


## Stage 3: raw sum + compress to 0-10 ★

After all six signals run, raw points sum:

```
raw = rank_pair + close_game + favorite + tournament_stage + rivalry + narrative + importance
```

Raw points are unbounded — a relegation six-pointer between two
ranked teams in a Champions League knockout could theoretically score
50+. To produce a comparable ★0-10 across sports and tournament
phases, raw points compress through `tanh`:

```
final_score = 10 * tanh(raw / knee)
```

The knee is the value of `raw` that produces ★~7.6 (`tanh(1) ≈ 0.76`).
Two compression modes:

**Absolute compression** (knee = 16.0):
```
raw = 2  → ★1.2
raw = 4  → ★2.5
raw = 8  → ★4.6
raw = 16 → ★7.6
raw = 24 → ★9.0
raw = 32 → ★9.6
raw = 48 → ★10.0
```

**Adaptive compression** (knee = batch median × 1.6) — used when the
refresh produces ≥5 games. The batch median maps to ★~5.5 regardless
of where in the season the refresh runs. Effect: top games in any
given refresh feel like top games, whether you're refreshing in a
low-stakes August week or a championship-decision December weekend.
Early-season low-stakes weeks no longer compress to all-low; late-
season saturation no longer flattens every game to ★10.

Adaptive compression falls back to absolute when batches are smaller
than 5 games (the median is too noisy on tiny batches).


## Stage 4: channel layout

After every game has a ★ score:

1. **Sort within today**: today's games sort by ★ descending and take
   the lowest channel numbers in the target group (so they appear at
   the top of any IPTV client).
2. **Sort across days**: tomorrow's games follow today's; day-after-
   tomorrow follows; etc. Within each day, ★ ordering.
3. **Cap**: `max_games` setting limits the total number of curated
   channels (default 25). Beyond that, games drop off the bottom.
4. **Names**: channel names follow the template `<SPORT> <RANKS>
   <FAV?> ★<SCORE>: <AWAY> at <HOME>`, e.g. `EPL 3v9 ⭐ ★8.4:
   Brentford at Manchester United`. ⭐ marks games with a favorite.
5. **Descriptions**: each channel's EPG description shows kickoff
   time, the matchup, the sport, the raw + ★ scores, the per-signal
   breakdown, and (when LLM enabled) a narrative paragraph.


## How to read the EPG description

A typical description for a high-stakes game:

```
Kickoff: Today 2:00 PM CDT 🔴 TODAY
Matchup: Brentford FC @ Manchester United FC
Sport: English Premier League
Score: 8.4/10  (raw 21.8)
Score breakdown:
  rank_pair: +7.92
  close_game: +2.16        (devigged coinflip-ness 0.72)
  importance: +11.7
Importance notes (top 3):
  Brentford UCL: 0.65 leverage × 4.0 = 2.60
  Manchester United UCL: 0.58 leverage × 4.0 = 2.32
  Brentford title: 0.40 leverage × 5.0 = 2.00
Source channel: Manchester United
```

- **Score: 8.4/10**: the final ★ rating after compression.
- **(raw 21.8)**: pre-compression raw points, useful for debugging.
- **Score breakdown**: each signal's contribution. Sum equals raw.
- **Importance notes (top 3)**: the three (team, outcome) tuples with
  the highest leverage. Each line is `team outcome: leverage × weight
  = contribution`. Leverage is `|tau_c|` from the Monte Carlo loop;
  weight is the consequence weight from the league context.
- **Source channel**: the EPG-matched broadcast channel the virtual
  matchup channel pulls its stream from.

If a game scored unexpectedly low: scan the breakdown. Common
patterns:
- No `importance` line → the sport's source doesn't implement Monte
  Carlo importance, or `weights.importance = 0`.
- `importance` very small → both teams' outcomes are locked in (early
  September: nothing yet decided; late May: lots locked in).
- `rank_pair` missing → at least one team is unranked or the sport
  doesn't use rankings.
- `close_game` missing → no spread / closeness data from the source.


## Per-sport adapter quirks

A few sports need special handling that the generic pipeline above
doesn't capture:

- **NHL Stanley Cup Playoffs:** best-of-7 series at every round
  modeled with the `BestOfNSeriesSource`. Series-state metadata
  (`series_wins`, `series_to_win`) lives in the bracket state. The
  outcome bands are stage-based (R2 / Conf Final / Cup Final /
  Champion) rather than position-based.
- **NCAA Baseball postseason:** mixed-format bracket — regional
  (4-team double-elimination per site) → super regional (best-of-3)
  → MCWS (8-team double-elim) → MCWS Finals (best-of-3). The plugin
  ships a `DoubleEliminationSource` for the double-elim stages and
  `BestOfNSeriesSource` for the best-of-N rounds, composed via a
  staged bracket.
- **MLS:** per-conference standings (top 9 East, top 9 West), since
  playoff seeding is conference-local; cross-conference regular-season
  games are filtered out of the standings simulation but still emit
  in `fetch_upcoming`. MLS Cup playoffs use a mixed format
  (best-of-3 Round One, single-leg everything else) handled by the
  `MlsCupSource`.
- **NCAA Football OT:** the Poisson model can produce ties in
  regulation, but NCAA games can't end tied. The simulator coin-flips
  the tied result to give one side a +1 win, preserving the W/L
  classification the bands cascade reads.
- **Field events (F1, golf, NASCAR, UFC):** no two-team head-to-head
  structure, so rank / favorite / closeness signals don't fire. The
  `tournament_stage` band (`EVENT` for regular weekly events, `MAJOR`
  for golf majors / Monaco GP / etc) is what gets them onto the
  guide at all.
- **NCAA Women's Basketball / March Madness:** ESPN scoreboard for
  the regular season, dedicated `NcaawSource` knows the 6-round
  single-game elimination shape (R64 → R32 → S16 → E8 → F4 → NCG).
  Uses the same `KNOCKOUT_ROUND_DEPTH` cascade the FIFA / UEFA
  knockouts use.


## Tuning

You do not have to understand the math to get the guide you want. There
are two layers of control, and most people never leave the first:

1. **Curation presets** (the `curation_preset` setting): one dropdown
   that bundles every weight plus `max_games`. Pick one and you are done.
2. **Manual weights**: set `curation_preset` to `manual` and the nine
   individual `weight_*` numbers (plus `max_games`) take over, so you can
   nudge a single signal.

Picking any preset other than `manual` IGNORES the individual `weight_*`
settings and `max_games`. If you have hand-tuned weights you want to keep,
stay on `manual`.

### Recipes: "I want X, so I change Y"

Each recipe is a starting point, not a law. The arrows show the change
from the default (`balanced`) values. Re-apply after changing settings,
or wait for the next scheduled refresh, to see the new ordering.

| I want... | Change | Why it works |
|---|---|---|
| **Just my teams, short list** | set `favorites`, then `curation_preset` → `high_curation` | Favorites are always included regardless of score; `high_curation` caps the rest at ~10 so your teams sit at the top of a tight list. |
| **My teams to outrank everything** (manual) | `weight_favorite` 6 → 12 | Each favorite game gains +6 raw (~+1 to +2 ★ depending on compression), enough to jump a favorite above all but the highest-stakes neutral games. |
| **Nail-biters, I don't care about stakes** | `weight_spread` 3 → 8, `weight_importance` 3 → 0 | Close-game contribution is `closeness × weight`, so a coin-flip now adds up to 8 raw instead of 3; zeroing importance removes the season-stakes signal entirely. (Needs an Odds API key for the closeness data.) |
| **Marquee names only, ignore standings context** | `weight_rank` 1 → 3, `weight_importance` 3 → 0 | A 1-vs-1 ranked pair jumps from 10 → 30 raw; with importance off, ranking is the dominant driver. Good for a "big names" guide. |
| **Fewer random cup knockouts crowding the league** | `weight_tournament` 1.5 → 0.5 | A Final drops from 7.5 → 2.5 raw, a Round-of-16 from 2.25 → 0.75, so league games with real stakes stop getting pushed down by early-round cup ties. |
| **Importance feels too dominant** | `weight_importance` 3 → 1 | The Monte Carlo signal scales linearly with this knob; thirding it lets rank, closeness, and favorites compete on a more even footing. |
| **Early-season weeks feel flat / late-season everything is ★10** | turn ON `adaptive_scoring` | Switches compression from a fixed curve to a per-refresh one keyed off the batch median, so the top games of any week always read as top games. |
| **More games on the guide, including smaller stakes** | `curation_preset` → `high_coverage` (or on `manual`, raise `max_games`) | `high_coverage` widens the cap to ~50 and softens the weights so lower-leverage games clear the bar. |
| **Same curation, just a different list length** | on `manual`, change only `max_games` | `max_games` is the only list-length lever; it trims from the bottom of the sorted list without changing how anything is scored. |

A note on "fewer games": the only length control is `max_games` (or a
tighter preset). There is intentionally no "hide games below score N"
floor. Low-scored games filling otherwise-empty slots on a quiet week is
the design, not a bug: this is the one place games show up.

### What the presets actually set

If you want to start from a preset and then hand-tune, these are the
exact bundles each preset applies (so you know your baseline before
switching to `manual`):

| Preset | rank | close | favorite | rivalry | tournament | importance | max_games |
|---|---|---|---|---|---|---|---|
| `high_curation` | 1.5 | 2.0 | 4.0 | 1.5 | 2.0 | 4.0 | 10 |
| `balanced` (default) | 1.0 | 3.0 | 6.0 | 2.0 | 1.5 | 3.0 | 25 |
| `high_coverage` | 0.7 | 4.0 | 8.0 | 2.5 | 1.0 | 2.5 | 50 |

`narrative` is 0.0 (off) in every preset; the LLM narrative signal is
opt-in regardless of preset. `high_curation` tilts toward stakes
(importance 4.0) and away from breadth (favorite 4.0, max 10);
`high_coverage` does the reverse (importance 2.5, favorite 8.0, max 50).

### Knob reference

The recipes above cover the common goals. For reference, each individual
weight on `manual`:

- **`weight_rank`** (default 1.0): multiplier on the rank-pair signal.
  Bump to 2.0 and top-25 matchups score ~2x.
- **`weight_spread`** (default 3.0): multiplier on closeness `[0, 1]`. A
  true pick'em adds `weight_spread` raw points.
- **`weight_favorite`** (default 6.0): flat points added per favorite
  game. Each +6 is roughly +1 to +2 ★ after compression.
- **`weight_tournament`** (default 1.5): multiplier on the per-stage
  scalar (Final 5.0, SF 3.5, QF 2.5, R16 1.5, ...).
- **`weight_rivalry`** (default 2.0): flat bonus on rivalry games.
- **`weight_importance`** (default 3.0): multiplier on the Monte Carlo
  leverage. Set to 0 to disable stakes scoring entirely.
- **`weight_narrative`** (default 0.0, off): multiplier on the LLM
  narrative score. Needs an Anthropic key; set > 0 to enable.

The weights compose multiplicatively with the per-signal magnitudes,
so a `weight_rank = 3.0, weight_importance = 0` config aggressively
elevates rank-pair games while ignoring stakes leverage entirely:
useful for purely "marquee matchup" guides without season-context
sensitivity.

### Seeing the effect of a change

Every game's pre-compression breakdown is stored per-refresh in
`cache.json` (`score_breakdown`, `score_raw`, `score`, and the
per-signal `score_notes`). After you change a weight and re-apply, open
`cache.json` (or read a curated channel's EPG description, which prints
the same breakdown) and the per-signal contributions will reflect the
new weights. That is the fastest way to confirm a knob did what you
expected before judging the whole guide.


## Limitations and known gaps

- **MLS mid-season importance** reads near zero for marginal games
  because ESPN publishes only ~1-2 weeks of future MLS fixtures
  (other leagues publish months ahead). With a thin
  `remaining_matches` list, the simulator can't propagate the season
  forward far enough to differentiate end-of-season outcomes. Signal
  sharpens as the season's end window narrows.
- **UEFA EURO 2028 bracket leverage:** the cross-source chain
  machinery that makes WC 2026 group games show R16+ leverage is
  WC-specific. EURO 2028 needs analogous wiring once UEFA publishes
  the bracket structure (~12-18 months pre-tournament).
- **In-progress games** are treated as SCHEDULED (not FINISHED) for
  state-building so the simulator doesn't seed wins/losses from
  mid-game state. Once the game ends and the source's API updates,
  the next refresh picks up the FINISHED result.
- **The narrative LLM signal** is sensitive to prompt drift. The
  prompts in `llm_descriptions.py` are pinned and version-controlled,
  but Claude's behavior on a given prompt can shift between model
  updates. Caching by `(marker, prompt_hash)` mitigates this for any
  individual game.


## References

- Lahvička, J. (2012). [Football match importance via a contingency-
  table coefficient](https://www.researchgate.net/publication/233775017_The_Importance_of_Football_Matches_Calculated_using_Monte-Carlo_Simulation).
  Source for the Monte Carlo + tau-c formulation.
- Stuart, A. (1953). The estimation and comparison of strengths of
  association in contingency tables. *Biometrika*, 40(1/2), 105-110.
  Source for the tau-c statistic itself.
- FIFA. *FIFA World Cup 2026 Regulations*, May 2025. Annex C, third-
  placed teams allocation table. Source for the 495-row WC 2026 slot
  lookup.

Source code for everything described above:

- `scoring.py` — signal computation, raw sum, compression, tunable weights
- `simulation.py` — Monte Carlo loop, `kendall_tau_c`, the chain variant
- `sources/base.py` — `SportSource` abstract contract
- `sources/soccer.py` — soccer leagues + group/knockout + Annex C
- `sources/bracket.py` — generic best-of-N + double-elim + aggregate-leg bracket machinery
- `sources/points_based.py` — generic threshold-band sport sources
- `sources/<sport>.py` — per-sport adapters
