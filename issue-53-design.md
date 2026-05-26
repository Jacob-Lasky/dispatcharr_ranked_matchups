# Issue #53 — Cross-source feeds_from: group winner → knockout opponent

**Status**: Design proposal (no production code in this PR)
**Target competitions**: FIFA World Cup, UEFA European Championship
**Time pressure**: WC 2026 group stage starts 2026-06-11 (Mexico vs South Africa MD1 = Group A opener)

## Problem statement

`GroupStageSoccerSource` (#20, PR #54) and `KnockoutSoccerSource` (#16/Phase D.1) ship as siblings for the same competition: one models the group-stage mini-leagues, the other models the post-group bracket. They currently run independent Monte Carlo simulations and never communicate.

The structural gap: in WC / EURO, **a group winner advances to a different LAST_32 / LAST_16 opponent than a group runner-up**. Two examples from the WC 2026 bracket (officially published by FIFA):

- Group A winner plays Group B runner-up in LAST_32
- Group A runner-up plays Group B winner in LAST_32

For a group-stage Mexico vs South Africa MD1 game (Group A opener):
- If Mexico wins Group A, they face whoever finishes 2nd in Group B (potentially a weaker team)
- If Mexico finishes 2nd in Group A, they face the Group B winner (likely stronger)
- These are **structurally different bracket paths** with different downstream odds of reaching R16, QF, SF, F, Champion

**Today**: group-stage leverage covers the advance/eliminate decision only. R16 / QF / SF / Final / Winner leverage reads 0 on every group-stage match, regardless of how decisive that match is for a team's downstream path.

## The 4 options

Jake's original sketch + the design-session option:

| # | Approach | Scope | Risk |
|---|----------|-------|------|
| 1 | `InternationalTournamentSource` consolidation | ~600 LOC: combine group + knockout into one class, merge state shapes, update plugin fan-out, all ~100+ existing tests | Breaks symmetry with the established `SoccerSource` / `KnockoutSoccerSource` pattern other comps use; pattern divergence makes future bracket-shaped soccer competitions painful |
| 2 | Simulator cross-source state at `monte_carlo_importance_batch` core | ~400 LOC in `simulation.py` core, touching the batch evaluator | Risk to every other Monte Carlo path (NCAAF / NCAAM / NHL / MLB / NBA / WNBA / NCAA softball / NCAA baseball / MLS — anything that uses the batch path) |
| 3 | Group source learns the knockout bracket internally | ~200 LOC in `GroupStageSoccerSource`: embed a mini-bracket simulator with cross-group opponent strengths | Duplicates `KnockoutSoccerSource` logic — every fix in one must be mirrored to the other |
| **4** | **Additive `cross_source_chain` registry at simulator-batch level** | **~100-150 LOC of new simulator function + per-source hook (~50 LOC)** | **Smallest blast radius — adds a NEW function path rather than modifying the existing one. Risk concentrated in the seeding-rule implementation, which is competition-specific.** |

**Recommendation: Option 4.** Smallest blast radius; the existing single-source Monte Carlo paths (NCAAF, NHL, MLB, etc.) are not touched at all. The chain function is a new entry point used only when a source explicitly registers a downstream consumer.

## Option 4 implementation sketch

### Architecture

```
GroupStageSoccerSource (primary)
  │
  │  declares cross_source_chain() → KnockoutSoccerSource
  │
  ▼
KnockoutSoccerSource (downstream)
  │
  │  exposes seed_from_chain(seed_dict) → state
  │
  ▼
Combined importance simulation
  - Sample group target match + drain group games
  - Translate group standings → bracket seed (per-tournament rule)
  - Sample knockout bracket with seeded participants
  - Roll up combined terminal_outcomes
```

### New simulator function

```python
# simulation.py — new function, existing monte_carlo_importance_batch
# is unchanged.

def monte_carlo_importance_batch_chain(
    primary_source: SportSource,
    target_match: GameRow,
    queries: Sequence[Tuple[str, str]],
    downstream_chain: Tuple[SportSource, Callable[[Dict[str, Any]], Dict[str, Any]]],
    n_sims: int = DEFAULT_N_SIMS,
    rng: Optional[random.Random] = None,
) -> Dict[Tuple[str, str], float]:
    """Importance batch with cross-source chain.

    For each sim:
      1. Sample target match + drain primary's remaining_matches
      2. Read primary.terminal_outcomes(primary_state)
      3. seed_fn(primary_state) → downstream initial state
      4. Drain downstream's remaining_matches with that seed
      5. Merge primary + downstream terminal_outcomes per team
      6. Tally each query's contingency table

    Returns: same shape as monte_carlo_importance_batch — {(team, outcome): |tau_c|}
    """
    ...
```

### Per-source hooks

**On `GroupStageSoccerSource`** (~30 LOC):

```python
def cross_source_chain(self) -> Optional[Tuple[SportSource, Callable]]:
    """Return (downstream_source, seed_fn) for tournaments with a paired
    bracket source. None for tournaments without a follow-on bracket."""
    knockout = self._get_paired_knockout_source()  # plugin sets via DI
    if knockout is None:
        return None
    return (knockout, self._build_bracket_seed)

def _build_bracket_seed(self, primary_state: Dict[str, Any]) -> Dict[str, Any]:
    """Walk primary_state to determine final group standings, then map
    to bracket-slot assignments per the tournament's seeding rules.

    Output shape (consumed by KnockoutSoccerSource.seed_from_chain):
      {
        "LAST_32": [(team_name, seed), ...],  # 32 entries for WC 2026
        "best_third_advancers": [...],         # WC: best 8 of 12 3rd-place
      }
    """
    ...
```

**On `KnockoutSoccerSource`** (~30 LOC):

```python
def seed_from_chain(self, seed: Dict[str, Any]) -> Dict[str, Any]:
    """Build the bracket's initial_state from explicit slot assignments
    rather than from FD.org-published matches (which are empty during
    group stage week). Synthesizes the bracket structure using the
    competition's published seeding rule + the resolved slot list.

    For WC 2026: 12 group winners + 12 runners-up + 8 best 3rd-placers
    fill 32 slots; the bracket has a published cross-group pairing
    (Group A1 vs Group B2, etc.).
    """
    ...
```

### Plugin wiring

```python
# plugin.py — _build_sources, where world_cup is enabled

if settings.get("enable_world_cup", False) and fd_key:
    wc_groups = GroupStageSoccerSource("world_cup", fd_api_key=fd_key, ...)
    wc_knockout = KnockoutSoccerSource("world_cup", fd_api_key=fd_key, ...)
    wc_groups.set_paired_knockout_source(wc_knockout)  # NEW
    sources.append(wc_groups)
    sources.append(wc_knockout)
```

### Routing in `compute_match_importance`

```python
# scoring.py

def compute_match_importance(source, match, league_ctx, ...):
    chain = getattr(source, "cross_source_chain", lambda: None)()
    if chain is not None:
        leverages = monte_carlo_importance_batch_chain(
            source, match, queries, downstream_chain=chain, ...
        )
    else:
        leverages = monte_carlo_importance_batch(source, match, queries, ...)
    ...
```

## The hard part: seeding rules

The seeding-rule implementation is the bulk of the design risk. Each tournament has a different rule:

### WC 2026 (48 teams)
- 12 groups of 4 (A-L)
- Top 2 from each group advance (24 teams) + 8 best 3rd-place teams (8 teams) = 32-team LAST_32
- Bracket pairings: officially published cross-group mapping (FIFA's bracket diagram)
- Best-3rd-place tiebreaker: points → goal differential → goals scored → fair-play → drawing of lots

### EURO 2024 (24 teams) — pre-2028 format
- 6 groups of 4
- Top 2 from each group advance (12 teams) + 4 best 3rd-place teams (4 teams) = 16-team LAST_16
- Bracket pairings: published cross-group mapping
- Best-3rd-place: same tiebreaker chain as WC

### EURO 2028 (32 teams) — projected format
- 8 groups of 4
- Top 2 from each group advance = 16-team LAST_16, no best-3rd needed

Each of these is ~50 LOC of seed-translation logic. WC 2026's best-3rd-place tiebreaker is genuinely thorny because of the cross-group comparison (which 3rd-place teams from which groups go through depends on the relative records).

## Implementation budget (Option 4)

| Component | LOC | Risk |
|-----------|-----|------|
| `monte_carlo_importance_batch_chain` in simulation.py | ~120 | Low — mirrors existing batch shape |
| `GroupStageSoccerSource.cross_source_chain` + `_build_bracket_seed` | ~60 | Medium — group-standings → bracket-slot mapping |
| `KnockoutSoccerSource.seed_from_chain` + synthetic bracket build | ~80 | Medium — interacts with existing `_build_bracket` path |
| Plugin wiring (`set_paired_knockout_source` on both genders) | ~10 | Low |
| `compute_match_importance` routing | ~15 | Low |
| WC 2026 seed mapping | ~50 | High — best-3rd tiebreaker is non-trivial |
| EURO 2024 seed mapping | ~40 | High — same as WC but smaller field |
| Tests (~10-15 new test classes) | ~200 | Medium |
| **Total** | **~575 LOC** | |

The "~100 lines" figure in Jake's deferral comment was for the simulator extension alone. Adding the per-source hooks + seeding-rule implementations + tests pushes the realistic budget to ~575 LOC.

## Test plan

The acceptance criterion from the issue is verifiable:

> After fix: a group-stage `Mexico vs South Africa MD1` game should report nonzero leverage on `round_of_16` (because Mexico's bracket path depends on whether they finish 1st or 2nd in their group). Today it reports 0 on every downstream band.

Tests would include:

1. **End-to-end leverage test**: stub a known WC group stage state where Group A is tight (Mexico, Portugal, South Africa, Saudi Arabia within 1 point of each other), confirm `compute_match_importance` on a Mexico match returns nonzero leverage on `round_of_16`, `quarterfinal`, `semifinal`, `final`, `winner` bands.

2. **Seeding-rule unit tests**: for WC 2026, test the best-3rd-place advancement against the published tiebreaker. Edge cases: tied points, tied goal differential, tied goals scored.

3. **Bracket-slot mapping test**: confirm Group A winner faces Group B runner-up at LAST_32 per FIFA's published bracket diagram.

4. **No-chain regression test**: confirm existing `monte_carlo_importance_batch` path is unaffected for sources without a `cross_source_chain` (NCAAF, NHL, MLB, etc.).

5. **Strength-sharing across the chain**: the knockout source's strength estimates should reflect each team's group-stage results (a team that beat strong opponents in groups gets a higher implied playoff strength).

## Open questions for the design session

1. **Where does the seeding rule live?** On the `GroupStageSoccerSource` (per-competition method)? Or as a separate `BracketSeedingRule` strategy class? The former is concrete; the latter is testable in isolation. Recommend: strategy class with WC2026 / EURO2024 / EURO2028 concrete implementations.

2. **How does FD.org publish the bracket structure?** Need to verify whether `/v4/competitions/WC/matches?stage=LAST_32` returns placeholder matches (with team names like "1A" / "2B") before the group stage completes. If yes, we can read the cross-group pairings structurally. If no, we hardcode them per tournament.

3. **Does `_build_bracket_seed` need to know about ALREADY-PLAYED group games?** Yes — primary's state includes finished MD1/MD2 games before the target match. The seed function reads primary_state's standings, which reflects those games.

4. **What about partial knockout state?** If we're MID-knockout (say, during the LAST_32 week, after some R32 games have played), how does the chain handle that? The simpler path: only run the chain BEFORE the bracket fully resolves. Once FD.org publishes complete LAST_32 participants, fall back to the existing flow.

5. **Strength sharing across sources?** Should the knockout source's `estimate_strengths` aggregate group + knockout games? Currently it only uses knockout games, which are sparse pre-LAST_32. Recommend: aggregate both via a `set_strengths_from_primary` hook so the cup sampling reflects group-stage form.

## Why this is being deferred from autopilot

Jake's autopilot directive: "do them all on autopilot unless you specifically need my input." This issue meets the explicit-out criterion because:

- 4 options have meaningfully different tradeoffs that warrant a deliberate choice
- The Option-4 implementation budget (~575 LOC) is ~2-3x the ~100-line sketch in Jake's deferral comment
- The seeding-rule implementations carry real complexity (WC best-3rd-place tiebreaker)
- The risk of getting it wrong before WC 2026 is high (1-2 weeks to design, implement, test, and ship before kickoff)

## Recommendation

Take 1-2 hours in a focused design session to:
1. Confirm Option 4 is the right path (vs the consolidation paths)
2. Decide on seeding-rule layering (strategy class vs per-source method)
3. Verify FD.org's pre-tournament bracket data via a real probe
4. Bound the implementation to a single competition's seeding rules (WC 2026 first, EURO 2028 / WC 2030 later)

Once those questions resolve, the actual implementation should fit ~1-2 days of focused work and could ship before WC kickoff on June 11.

## Impact while deferred

Group-stage leverage still fires correctly for the advance/eliminate decision (the highest-value group importance — knockout-bound vs not). The R16 / QF / SF / Final / Winner cascade reads 0 leverage on group games — an incremental gap that affects the secondary signal during the ~12-day WC 2026 group stage and the ~12-day EURO 2028 group stage. Most viewers tuning into a WC group game already see it on the guide via favorite + close-game signals; the missing importance signal sharpens the ranking but doesn't determine surfacing.
