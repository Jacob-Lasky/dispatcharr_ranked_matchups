# Contributor / AI session onboarding — dispatcharr_ranked_matchups

If you're an LLM or new contributor opening this repo cold, read this
first. It tells you what the plugin does, how it's structured, how to
extend it for new sports, and the design decisions worth respecting.

## What this is

A [Dispatcharr](https://github.com/dispatcharr/dispatcharr) plugin that
pulls upcoming sports games from per-sport APIs, scores each by
**interestingness** (transparent per-signal breakdown), matches them to
the user's Dispatcharr channels via EPG, and clones into a "Top Matchups"
group. Each virtual channel's EPG description shows WHY the game made
the cut — TiviMate / Plex / Jellyfin display this natively.

Inside a Dispatcharr container the plugin lives at
`/data/plugins/dispatcharr_ranked_matchups/`.

## Architecture (sport-agnostic by design)

```
plugin.py         ← orchestrator: refresh + apply + show_status + scheduler
  ↓ uses
sources/          ← per-sport adapters (drop-in extensible)
  base.py         ← GameRow + SportSource interface
  bracket.py      ← shared knockout/playoff state machine
  points_based.py ← shared round-robin Monte Carlo
  <sport>.py      ← one adapter per sport; DO NOT enumerate them here.
                    The file map below is the single source of truth, and
                    this diagram drifted to listing 2 of 24 adapters once
                    already by duplicating it.
  __init__.py
  ↓ produce
List[GameRow]     ← {sport_prefix, home, away, rank_home, rank_away, start_time, spread, extra}
  ↓ scored by
scoring.py        ← GameSignals + score_game + per-signal weight contributions
  ↓ matched by
matcher.py        ← regex pre-filter + Claude fallback for ambiguous EPG matches
  ↓ written by
plugin.py         ← creates virtual Channels + dummy EPGSource + ProgramData
```

**Adding a new sport is a new file in `sources/`** — implement
`SportSource.fetch_upcoming()` and return `GameRow` records. Everything
downstream is sport-agnostic.

## File map

| File | What it does |
|---|---|
| `plugin.json` | Manifest: settings (sports toggles, weights, favorites, schedule), actions. Read by Dispatcharr loader. |
| `plugin.py` | Plugin class + 5 actions: `refresh`, `apply`, `auto_pipeline`, `show_status`, `preview_names` (renders the name template against sample games). Daemon scheduler. EPG lookup closure. Channel cloning + dummy EPGSource management. |
| `scoring.py` | `GameSignals`, `Weights`, `GameScore`. `score_game()` sums per-signal contributions, `_compress_to_10()` does the tanh squash. Helpers: `match_favorites`, `compute_match_importance` (Lahvička Monte Carlo), `format_channel_name` (delegates to `naming`), `render_importance_tagline` + `STAGE_BANDS` (bracket-band prettify), `tournament_stage_label`. League thresholds in `LEAGUE_CONTEXTS` dict (position, label, consequence_weight). |
| `naming.py` | Channel-name templating (Sonarr/Radarr `{group}`-collapse convention). `render_name`, `validate_template`, `build_context`, `preview_lines`, `DEFAULT_NAME_TEMPLATE`, `TOKENS`. Pure: no Django, no `scoring` import. |
| `simulation.py` | Sport-agnostic Monte Carlo importance per Lahvička (2012). `monte_carlo_importance()` for single (team, outcome); `monte_carlo_importance_batch()` shares one set of N season simulations across K queries. `kendall_tau_c()` is the ordinal-association measure. |
| `matcher.py` | `match_games_to_channels()` resolves cached `GameRow` → Dispatcharr channel via EPG `ProgramData`. Two-stage: regex (both team keywords in EPG title) → Claude batched fallback. |
| `sources/base.py` | `GameRow` + `MatchResult` dataclasses + abstract `SportSource`. ABC declares the Monte Carlo importance interface (`supports_importance` flag + 7 optional methods); sources opt in by overriding. |
| `sources/bracket.py` | `BracketSportSource` shared state machine for all knockout/playoff sports (bracket inference, `_round_reached`, `terminal_outcomes` cascade). Three concrete tie shapes: `AggregateLegSource` (two-leg, used by `KnockoutSoccerSource`), `BestOfNSeriesSource` (best-of-N series, used by `NhlPlayoffSource`, `MlbPlayoffSource`, `NcaaBaseballPlayoffSource`, `NcaaSoftballPlayoffSource` for their Super Regional + Finals stages), and `DoubleEliminationSource` (4-team double-elim base class for NCAA Baseball / Softball Regional + 8-team MCWS/WCWS bracket; plumbing into the sport sources is tracked in #43). `BestOfNSeriesSource._series_length_for_stage(stage)` hook lets each sport map stages to series lengths — NHL uses uniform 7; MLB uses 3/5/7/7 (WC/LDS/LCS/WS). |
| `sources/points_based.py` | Shared Monte Carlo for round-robin point-scoring sports (NCAAF, NCAAM, NHL regular, MLB regular). Subclasses set `_count_field` to choose between `"wins"` (CFB/CBB/MLB) and `"standings_points"` (NHL) for terminal_outcomes bucketing. |
| `sources/ncaaf.py` | CFBD `/rankings`, `/games`, `/lines` calls. CFBD uses **camelCase** (homeTeam, awayTeam) — easy gotcha. |
| `sources/nhl.py` | api-web.nhle.com (official, no key). `NhlRegularSource` uses standings points (regulation win = 2, OT/SO loss = 1, regulation loss = 0). `NhlPlayoffSource` is a best-of-7 BracketSportSource. The two cross-feed: NhlPlayoffSource borrows regular-season strengths via `set_regular_season_strengths()` so cup-final sampling uses the 82-game baseline. |
| `sources/mlb.py` | statsapi.mlb.com (official, no key). `MlbRegularSource` uses raw win count (no OT-loss bonus). `MlbPlayoffSource` is a BestOfNSeriesSource with per-stage series lengths (WC=3, LDS=5, LCS=7, WS=7). Same regular-season → playoff strength-sharing pattern as NHL. |
| `sources/nba.py` | ESPN unofficial API (stats.nba.com is WAF-blocked from most homelab egress). `NbaRegularSource` uses raw win count. `NbaPlayoffSource` is a BestOfNSeriesSource with uniform SERIES_LENGTH=7 (R1 / CSF / CF / FINALS). Stage routing comes from parsing the ESPN headline ("East 1st Round - Game 3" etc.) — the only place ESPN exposes the playoff round. All-Star Tournament games (which ESPN tags `season.type=2` but `competition.type.abbreviation=ALLSTAR`) are filtered out so the regular-season team list stays at 30. |
| `sources/mls.py` | ESPN unofficial API for the schedule + The Odds API (`soccer_usa_mls`) for closeness. Intentionally thin: `MlsSource` does NOT inherit `PointsBasedSportSource` and `supports_importance=False`. MLS surfaces with favorite + closeness signals only; standings-based importance and the mixed-format MLS Cup bracket (best-of-3 R1 + single-leg subsequent rounds) are tracked in #30. Team-name fuzzy matching reuses `_util.TEAM_SUFFIX_TOKENS` — never add "united" to that list (it's a substantive body word for Atlanta United / D.C. United / Minnesota United). |
| `sources/soccer.py` | Football-Data.org for fixtures+standings, The Odds API for spreads. League position used as rank. `SoccerSource` is league-shaped (PL, ELC); `KnockoutSoccerSource` is bracket-shaped (UCL) — multi-inherits from `AggregateLegSource` (bracket state machine) + `SoccerSource` (FD.org fetch / strengths) with the MRO `K → AggregateLegSource → BracketSportSource → SoccerSource → SportSource`. Routed by `LEAGUE_CONTEXTS[fd_code].format`. |
| `sources/_espn.py` | Shared ESPN helpers. `extract_espn_scoreboard_event` normalises one scoreboard event (its docstring forbids inlining a fourth copy). `sweep_upcoming_scoreboard` is the whole per-day sweep for UPCOMING-only sources, owning the US-Eastern-bucket lookback (`SCOREBOARD_LOOKBACK_DAYS`), the FINISHED drop, the stale-SCHEDULED floor (`MAX_AGE_AFTER_KICKOFF`) and the id dedupe. Used by `friendlies.py` + `english_cup.py`. Takes `http_get` INJECTED so each source keeps its own patchable `requests` symbol. The bracket sources deliberately do NOT use it: they sweep a fixed calendar window and KEEP finished games because bracket state comes from results already played. |
| `sources/friendlies.py` | ESPN exhibition soccer: `InternationalFriendliesSource` (`fifa.friendly` / `fifa.friendly.w`, parametrized on gender) and `ClubFriendliesSource` (`club.friendly`). `supports_importance=False` deliberately: no table to simulate. Gated to Favorites by default (`friendlies_favorites_only`) because an exhibition's only claim to a slot is the favorite signal. `CLUBFRIENDLY` is a distinct prefix from `FRIENDLY` and its absence from `rivalries.json` is load-bearing: a pre-season kickabout is not a derby. |
| `sources/english_cup.py` | EFL (Carabao) Cup + FA Cup via ESPN (`eng.league_cup` / `eng.fa`). Exists because Football-Data.org gates EVERY domestic cup behind a paid plan (FLC=TIER_THREE, FAC=TIER_TWO; verified 403 on the free-tier key), so a `soccer.py::COMPETITIONS` entry would 403 every refresh and contribute zero games silently. Rounds come from `season.slug`; the slug->stage maps are PER-COMPETITION because the two cups' round names overlap at different depths (FA Cup 4th round = R32, EFL Cup 4th round = R16). `supports_importance=False` and ranks always `None` — the latter is what stops a giant-killing tie being penalised for being lopsided. Early rounds score via the `CUP_R*` band in `scoring.py`, ramping to just under the shared `QUARTER_FINALS`. #190. |
| `logos.py` | TheSportsDB matchup-thumbnail resolver. Looks up the curated game via `searchevents.php` (team-name pair, date-tolerance ±2 days, sport-hint disambiguation), downloads the 960x540 graphic to `/data/logos/ranked_matchups_<sha1>.jpg`, and registers a `Logo` row pointing at it. Persistent per-marker cache (`sportsdb_thumb_cache.json`, 14d positive TTL / 1d negative TTL) means apply only HTTP-probes each fixture once. Field-event sources (`away=="Field"`) and dry_run short-circuit the lookup. Stale-file sweep at the end of each apply prunes JPGs whose marker isn't in the live set. Also resolves the league/tournament BADGE fallback (#102): `SPORTSDB_LEAGUE_IDS` (`sport_prefix` -> verified league id), `league_id_for`, `resolve_league_badge_url` (`lookupleague.php` -> `strBadge`), cached as `ranked_matchups_badge_<id>.png` (distinct prefix the sweep skips). |

State (gitignored, lives in `<plugin_dir>/`):
- `cache.json` — last refresh result (curated game list with score breakdowns)
- `cfbd_api_key`, `football_data_api_key`, `odds_api_key`, `anthropic_api_key` — file fallback when settings field is blank.

## How channels are produced

Source channels are **never modified**. Apply creates virtual channels in a
target `ChannelGroup` (default `Top Matchups` — user has it as `!Top Matchups`
to sort to the top of group lists):

- `tvg_id` = `ranked_matchups:<SPORT>:<source_id>` — used for cleanup detection
  on next apply (any channel with this prefix in any group is "ours")
- `channel_number` — TWO schemes, `channel_numbering_mode`. Default `kickoff`:
  `base + minutes-since-CHANNEL_NUMBER_ORIGIN × SLOTS + hash%SLOTS`, so a game's
  number never moves (#121) and the list sorts by start time. `compact`:
  `allocate_compact_numbers` keeps every number inside
  `[base, base+compact_band_size)`, holds a published game on its slot, and
  allocates new games ABOVE the highest in use so a wide band delays slot reuse.
  Compact CAN omit a marker when the band is narrower than the slate; the apply
  loop skips those games. `9000 + cache_index` was the pre-#119 scheme, gone
- `streams` = cloned via `ChannelStream` from the matched source channel
- `epg_data` = a per-channel `EPGData` row in our dummy `EPGSource` with same
  name as the group; `ProgramData` description carries the WHY breakdown

A 2-phase renumber dance avoids the unique constraint on
`(channel_group, channel_number)`: park existing channels at 19000+ first,
then assign target numbers.

## The interestingness signals (priority order)

User's stated priority: **standings > narrative > odds**, plus favorites and
end-of-season excitement. Each signal contributes raw points; the sum gets
compressed to 0-10 via `tanh(raw / 8)` so top games asymptote without losing
differentiation in the typical 4-15 range.

| Signal | Triggered when | Default weight |
|---|---|---|
| `rank_pair` | Both teams ranked / one ranked | 1.0 |
| `favorite` | One of user's favorite teams plays | 6.0 (flat) |
| `close_game` | Coinflip-ness in [0, 1]: devigged h2h moneyline probabilities for soccer, normalized point spread for NCAAF / NCAAM | 3.0 |
| **`importance`** | Lahvička Monte Carlo: \|Kendall tau-c\| × consequence weight, summed over playing teams AND in-league favorites' outcome bands. Soccer leagues (PL/ELC): title/UCL/Europa/relegation/promotion (format=`league`). UCL knockouts: round_of_16/QF/SF/F/winner (format=`knockout`). NCAAF/NCAAM: win-count bands (format=`win_count`). NHL regular season: standings-point bands (95+/100+/110+/125+, format=`points_count`). NHL Stanley Cup Playoffs: R2/Conf Final/Cup Final/Champion (format=`knockout`). Locked-in outcomes contribute 0 (mathematical elimination respected). | **3.0** |
| `tournament_stage` | Knockout cup game flat boost (R16 / QF / SF / FINAL), plus the `CUP_PRELIM`/`CUP_R1`-`CUP_R5` band for domestic-cup early rounds (`_CUP_ROUND_STAGE_SCORES`, ramping to just under the shared `QUARTER_FINALS`). For UCL the importance signal already encodes round depth via consequence weights, so this is mostly redundant; for the English cups it is the ONLY structural signal, since those sources don't simulate. The note this signal writes into the EPG description goes through `tournament_stage_label`, not the lower-cased raw enum. | 1.5 |
| `rivalry` | Known rivalry game. Populated from the bundled `rivalries.json` via `rivalries.py` (see also `honours.py`); `sources/friendlies.py` and `plugin.py` are the consumers. | 2.0 (flat) |
| `narrative` | LLM-judged narrative score | 0.0 (off by default) |

The structural Monte Carlo importance signal subsumes the old
hand-rolled "stakes / impact_on_favorite / late_season_multiplier"
signal family: mathematical elimination falls out for free,
cross-team impact comes from extending each query batch to include
in-league favorites, and natural end-of-season leverage rise emerges
from tau-c growing as outcomes lock in.

User asked us to default narrative weight to 0 because the structural
`importance` signal covers what LLM narrative would surface anyway. Don't
enable narrative without explicit user buy-in.

## Sport adapter extension contract

Three steps, using NCAAM (College Basketball) as the worked example.
NOTE: NCAAM already ships as `sources/ncaam.py`, so read that file as the
reference implementation rather than writing it; the shape below is what
a NEW sport needs.

1. Create `sources/<sport>.py` with a class implementing `SportSource`:
   ```python
   class NcaamSource(SportSource):
       sport_prefix = "CBB"
       sport_label = "NCAA Basketball"

       def fetch_upcoming(self, days_ahead=7) -> List[GameRow]:
           # call api.collegebasketballdata.com/rankings + /games
           # return GameRow with rank_home/rank_away from AP Top 25
   ```

2. Register in `sources/__init__.py`.

3. Add an `enable_<sport>` toggle to `plugin.json` and wire it in
   `_build_sources(settings)` in `plugin.py`. Every toggle must be wired
   in both places or the setting renders but does nothing.

Shared CFBD key already covers basketball (same Bearer token).

For league-based sports (where standings position is the "rank"), populate
`extra["fd_competition_code"]` to a code in `scoring.LEAGUE_CONTEXTS` so
the `importance` signal knows your thresholds and consequence weights.
Four formats exist for the threshold cutoffs:
- `format="league"` (PL, ELC): int league position (1=top, lower=better).
- `format="knockout"` (CL, NHL_PO): str round id (e.g., `LAST_16`, `CUP_FINAL`).
- `format="win_count"` (CFB, CBB): int MINIMUM win count.
- `format="points_count"` (NHL): int MINIMUM standings points — needed
  for sports with OT/SO bonus points where raw wins mislead.

`PointsBasedSportSource` subclasses set `_count_field` to choose between
`"wins"` (default; CFB/CBB/NBA/MLB) and `"standings_points"` (NHL). The
state's `_teams[team][<_count_field>]` is what `terminal_outcomes`
buckets against the threshold cutoff.

### Every emitted row costs ~7s, and the refresh budget is 1500s

**A source that flips `supports_importance = True` is buying a Monte Carlo
season replay PER EMITTED GAME. Measured 2026-08-29 for NCAAF: ~7.0s each.**
`tasks.py` kills the pipeline subprocess at 1500s, so the whole refresh
affords roughly 200 importance-bearing rows across ALL sources combined.

This is the budget that decides whether a `fetch_upcoming` filter is
cosmetic or load-bearing, and it is invisible at authoring time because the
cost lands in `plugin._action_refresh`, not in the source. Two rules:

- **Bound what `fetch_upcoming` emits to what can actually score.** NCAAF
  shipped without a division filter and pulled all of CFBD's D-II/D-III
  feed: 157 games where 27 involved an FBS team, ~1100s of the budget, and
  `auto_pipeline` timed out on every run from the day the season opened.
- **The emitted universe and the simulated universe must be the same set.**
  A team the simulator has never heard of has no strength estimate and no
  win-count band, so its games score exactly 0.00 while still paying the
  full 7s. Derive both from one predicate and one cached fetch; two call
  sites that "agree" are a convention, and conventions drift silently.

Separately, `outcome_eligible_teams()` on `PointsBasedSportSource` exists
because game inclusion and outcome eligibility are different questions. An
FBS-vs-FCS game belongs in the population (the win counts toward the FBS
team's record) while the FCS opponent does NOT belong in CFB's outcome
space: it arrives on a one-game schedule and gets bucketed against
"6+ wins = bowl eligible". Return `None` (the default) unless your sport
has that asymmetry.

### Two daemon threads, not one

`Plugin.__init__` starts BOTH a scheduler (periodic refresh+apply) and a
reaper (removes finished games between refreshes, #196). They have SEPARATE
reload-stable registries and must both be torn down in `stop()`.

**The trap that already bit once: `_start_reaper()` must not sit behind the
scheduler's early return.** `__init__` returns early when a live scheduler is
already in the registry, and that is the COMMON path, because the loader
rebuilds `Plugin` on every discovery. Anything placed after that return only
runs on a cold worker, so the reaper shipped inert on upgrade until it was
moved above the return. Any future third thread has the same hazard.

`stop()` uses `_clear_if_exited`, which only nulls a registry slot whose
thread actually died. Clearing a slot whose thread is still alive (the reaper
can be parked on a subprocess well past the 5s join) makes the next
`__init__` spawn a duplicate.

### cache.json has TWO lists: `games` and `bench`

`games` is what apply puts on air; `bench` is scored-but-not-applied stock the
reaper promotes from as games finish (#197). Splitting them at write time
rather than tagging rows is deliberate: `_action_apply` needs no knowledge of
the bench at all, it still applies exactly the list it is handed.

`reap_apply_pending` is a third key. The reaper rewrites the cache BEFORE
calling apply, so a failed apply leaves the desired state published and the DB
behind it. The flag is how the next tick knows to re-apply; without it the
expired games are already absent from the cache and nothing could ever notice
they still need removing.

**Reaping delegates deletion to `_action_apply` on purpose.** Apply already
reaps owned channels missing from the cache, with DVR-recording protection,
the archive re-home, ChannelStream/EPGData cleanup, and signal-safe queryset
deletes. Do NOT add a second deletion path; it would be a copy that has to
stay in agreement with that one.

## Known gotchas / lessons learned

- **A game's end time is ESTIMATED, and there is exactly one estimate.**
  No feed publishes one. `_game_end_utc` reuses `_epg_match_window`'s
  per-sport post-game hours (~4h gridiron, 2.5h soccer). DO NOT add a second
  duration table for reaping: the guide entry is written against the EPG
  window, so a shorter reap window would delete a channel while its own
  programme still claimed the game was on air.
- **CFBD `/games` carries EVERY NCAA division**, not just Division I:
  `homeClassification` / `awayClassification` are `fbs` / `fcs` / `ii` /
  `iii`, and are occasionally absent entirely. A time-window filter alone
  returns mostly Division II and III. See the refresh-budget section above.
- **A "0 games" summary from a poll-driven source usually means the POLL
  call failed, not that it is the offseason.** `_fetch_rankings` returning
  `None` makes `fetch_upcoming` return `[]`, and NCAAF logged that as
  "offseason (?)" regardless of cause, which cost a day of wrong diagnosis
  on 2026-08-28. Distinguish request failure from unpublished poll.
- **CFBD API is camelCase**: `homeTeam`, `awayTeam`, `startDate`,
  `neutralSite`, `excitementIndex`. We hit this once already — got 0 games
  because we used `home_team`. Their `/games` response also exposes
  `excitementIndex` on completed games, which is interesting raw data.
- **EPG self-matching bug**: matcher must exclude channels with our
  `tvg_id` prefix `ranked_matchups:` — otherwise it matches games against
  our own virtual channels (whose EPG titles literally contain the team
  names). Fixed in `_build_epg_lookup()`.
- **Group rename migration MUST preserve `Channel.id`**: when user changes
  the target group name, apply detects old virtual channels (any group, by
  tvg_id prefix) and **moves** them into the new target group via
  `.update(channel_group=target_group)`, NOT delete + recreate. Channel.id
  is the stable handle that `ChannelProfileMembership` (Dispatcharr only
  auto-adds new channels to profiles at PROFILE-creation time, never on
  channel-creation, so a recreate orphans every membership) and IPTV-client
  playlist caches both key off. A delete-then-create cycle silently makes
  the channel disappear from the user's IPTV guide until they manually
  refresh — exactly the regression that bit #1 live-verify. The user has a
  habit of renaming `Top Matchups` → `!Top Matchups` etc; every rename
  cycle must be a no-op for downstream consumers.
- **Soccer team-name suffixes**: Football-Data.org returns "Hull City AFC",
  "Manchester City FC". The favorites matcher uses a `TEAM_QUALIFIER_TOKENS`
  whitelist (FC, AFC, City, United, etc.) so the bare name "Hull" matches
  "Hull City AFC" but **doesn't** match "UNC Pembroke" (Pembroke isn't a
  qualifier).
- **Postgres connection pool**: long-running plugin code can exhaust the
  default Postgres connection pool. Monitor with `SELECT count(*) FROM
  pg_stat_activity` in the Dispatcharr container if you add worker-heavy
  code.

## Development loop

Inside the Dispatcharr container:

```bash
docker exec dispatcharr git -C /data/plugins/dispatcharr_ranked_matchups pull
# or rsync from your dev box → /data/plugins/ in the container
```

Smoke-test refresh:

```bash
docker exec dispatcharr python -c "
import django, os, sys
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
django.setup()
from apps.plugins.loader import PluginManager
pm = PluginManager.get()
pm.discover_plugins(sync_db=False, force_reload=True, use_cache=False)
r = pm.run_action('dispatcharr_ranked_matchups', 'refresh', {
    'enable_epl': True, 'max_games': 10,
})
print(r.get('message', r))
"
```

Inspect what the cache contains:

```bash
docker exec dispatcharr cat /data/plugins/dispatcharr_ranked_matchups/cache.json | head -80
```

Read live container logs:

```bash
docker logs --since 5m dispatcharr 2>&1 | grep ranked_matchups | tail -30
```

## Current state

**Working:**
- NCAAF source (CFBD) and NCAAM source (CollegeBasketballData) — both via
  the same Bearer token, both auto-skip during their respective offseason.
  Both implement Monte Carlo importance via the shared
  `PointsBasedSportSource` base (Poisson on points, win-count outcome
  bands per LEAGUE_CONTEXTS["CFB"]/["CBB"]).
- EPL + EFL Championship sources (Football-Data.org + Odds API, league-shaped)
- UCL source (`KnockoutSoccerSource`, bracket-shaped with ET + penalty
  sampling and feeds_from-via-participant bracket inference)
- EFL Cup + FA Cup sources (ESPN, keyless; stage-scored only, no importance
  simulation and no ranks — see `sources/english_cup.py` for why all three of
  those are deliberate)
- Soccer friendlies, international (both genders) + club pre-season (ESPN,
  keyless; Favorites-gated by default)
- All scoring signals (rank, favorite, close_game, importance,
  tournament-stage, optional LLM narrative)
- Today-first sort + channel renumbering, with auto / fixed virtual base
- Placeholder channels for unmatched-high-scoring games
- Group-rename auto-cleanup
- Multi-time scheduler (`scheduled_times = "0400,1000,1600,2200"`)
- Both file-based and settings-based API keys (settings preferred, masked UI)
- Description includes kickoff time + WHY breakdown
- Per-matchup channel logos from TheSportsDB (`logos.py`): each virtual
  channel gets a pre-rendered 960x540 graphic showing both team crests +
  league badge, replacing the inherited source-channel logo. Free public
  test tier (`api_key="3"`) is the default. Fallback order (#102): matchup
  thumbnail -> league/tournament badge (`SPORTSDB_LEAGUE_IDS`, cached once
  per league) -> source-channel logo (last resort). So an offseason /
  no-thumbnail / untracked fixture shows the sport badge, not the provider logo.

**Known limitations & open work:** read the
[open issues](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues?q=is%3Aissue+is%3Aopen).

**DO NOT paste a summary of them back into this file.** A previous
revision kept an inline "quick map" here, prefaced with the claim that
issues were used "so they don't drift" — and then every single entry
drifted. All eight (#3, #4, #5, #6, #7, #8, #9, #10) had shipped and
closed while this file still described them as open, including two
adapters listed as "not yet implemented" that exist as
`sources/ncaa_baseball.py` and `sources/ncaa_soccer.py`. An inline list
cannot be kept honest; the tracker is the only source of truth.

## Design principles worth respecting

- **Transparency over magic**: the score breakdown is always shown, in
  cache.json AND in the EPG description. If a user disagrees with how a
  game ranked, they should be able to see exactly which signal to tune.
- **Source channels are never modified**: the apply pipeline only creates
  virtual channels in the target group; the user's real channels and
  groups are untouched. Stale virtuals are detected by the `tvg_id`
  marker prefix and cleaned up automatically.
- **Sport-agnostic core**: `scoring.py` and `matcher.py` know nothing
  about specific sports. Adding a sport is a new file in `sources/`,
  full stop. If you find yourself adding a sport-specific branch outside
  `sources/`, push it back into the adapter.

## Publishing to the official Dispatcharr Plugin Repository

Dispatcharr maintains a central plugin repo that auto-packages,
versions, and distributes community plugins. Per the announcement in
the Dispatcharr Discord plugins channel
(https://discord.com/channels/1340492560220684331/1483922477611614208):

- **Repo**: https://github.com/Dispatcharr/Plugins
- **Listing**: https://dispatcharr.github.io/Dispatcharr-Docs/plugin-listing/
- **Releases branch**: https://github.com/Dispatcharr/Plugins/tree/releases
- **Contributing guide**:
  https://github.com/Dispatcharr/Plugins/blob/main/CONTRIBUTING.md

### Submission checklist

1. Develop and test in this repo (the canonical upstream).
2. Fork `Dispatcharr/Plugins` and add the plugin under
   `plugins/dispatcharr_ranked_matchups/`.
3. Confirm `plugin.json` has all required fields: `name`, `version`,
   `description`, `author`, `license`, `repo_url`, `help_url`. (Already
   present.)
4. Open a PR against `Dispatcharr/Plugins`.
5. Merge triggers automated packaging + versioning into the releases
   branch — no manual release step.

### Constraints worth knowing

- **Open-source license required**. We ship MIT (`LICENSE`), already
  satisfied. Submission grants Dispatcharr a license to redistribute.
- **The Dispatcharr team can decline or remove low-quality, abandoned,
  or otherwise unsuitable submissions** — keep code clean, respond to
  issues, cut releases when bugs are reported.
- **Manifests will be GPG-signed** in the near future; the bundled
  public key lets Dispatcharr verify integrity before install.

### When the upstream version bumps

- Bump `version` in `plugin.json` AND `__version__` in `__init__.py`
  (must stay in sync).
- Tag the release in this repo (`git tag v0.2.0 && git push --tags`).
- Open a PR against the upstream `Plugins` repo bumping the version
  reference for our plugin.

### Future: built-in plugin hub

Dispatcharr is shipping an in-app plugin browser. Once that lands,
users will install/update without leaving the app — the listing page
and CONTRIBUTING.md are the source of truth.
