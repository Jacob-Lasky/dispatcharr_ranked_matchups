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
matcher.py        ← EPG-programme / channel-name / stream-name candidates, resolved
                    by regex tiers + Claude fallback when ambiguous
  ↓ written by
plugin.py         ← creates virtual Channels + inactive XMLTV EPGSource + ProgramData
```

**Adding a new sport is a new file in `sources/`** — implement
`SportSource.fetch_upcoming()` and return `GameRow` records. Everything
downstream is sport-agnostic.

## File map

| File | What it does |
|---|---|
| `plugin.json` | Manifest: settings (sports toggles, weights, favorites, schedule), actions. Read by Dispatcharr loader. |
| `plugin.py` | Plugin class and action handlers. Daemon scheduler, EPG lookup closure, virtual-channel and stream linking, and inactive XMLTV EPG source management. |
| `scoring.py` | `GameSignals`, `Weights`, `GameScore`. `score_game()` sums per-signal contributions, `_compress_to_10()` does the tanh squash. Helpers: `match_favorites`, `compute_match_importance` (Lahvička Monte Carlo, one game at a time) and `compute_league_importance` (every game of one source from one shared set of seasons, #230) — both built from the same `importance_queries` + `importance_points` pair so the two paths ask the same questions and weight the answers identically, `format_channel_name` (delegates to `naming`), `render_importance_tagline` + `STAGE_BANDS` (bracket-band prettify), `tournament_stage_label`. League thresholds in `LEAGUE_CONTEXTS` dict (position, label, consequence_weight). |
| `naming.py` | Channel-name templating (Sonarr/Radarr `{group}`-collapse convention). `render_name`, `validate_template`, `build_context`, `preview_lines`, `DEFAULT_NAME_TEMPLATE`, `TOKENS`. Pure: no Django, no `scoring` import — which is why `_RECORDING_DOT` is a literal here rather than an import of `recording.DOT`; the two must stay the same glyph (`tests/test_autorecord.py` pins it). `{recording_dot}` is deliberately absent from `DEFAULT_NAME_TEMPLATE`. |
| `simulation.py` | Sport-agnostic Monte Carlo importance per Lahvička (2012). `monte_carlo_importance()` for single (team, outcome); `monte_carlo_importance_batch()` shares one set of N season simulations across K queries for ONE game; `monte_carlo_importance_shared()` shares one set across EVERY game of a source (#230), the production path for sources with `shared_season_importance = True` (state-free result models; knockouts opt out). `kendall_tau_c()` is the ordinal-association measure. |
| `matcher.py` | `match_games_to_channels()` resolves cached `GameRow` to Dispatcharr channels and streams. `_build_epg_lookup` (plugin.py) supplies three candidate paths: A, EPG `ProgramData` title, sub-title or description (whole-channel); B, channel name (whole-channel); C, stream name (stream-granular, so a stream need not be on a channel at all). Three tiers: `regex_strict` (channel or stream name names both teams, merging Tier-2 programme matches behind it), `regex_unique` (exactly one non-preview programme match), then Claude batched fallback or first candidate. `_partition_attach_targets` splits the result into `channel_ids` and `stream_ids`. |
| `sources/base.py` | `GameRow` + `MatchResult` dataclasses + abstract `SportSource`. ABC declares the Monte Carlo importance interface (`supports_importance` flag + 7 optional methods); sources opt in by overriding. A second, independent flag `shared_season_importance` (default False, #230) says the source's `sample_result` ignores `state`, so one set of simulated seasons can serve every one of its games — see the cost section below. |
| `sources/bracket.py` | `BracketSportSource` shared state machine for all knockout/playoff sports (bracket inference, `_round_reached`, `terminal_outcomes` cascade). Three concrete tie shapes: `AggregateLegSource` (two-leg, used by `KnockoutSoccerSource`), `BestOfNSeriesSource` (best-of-N series, used by `NhlPlayoffSource`, `MlbPlayoffSource`, `NcaaBaseballPlayoffSource`, `NcaaSoftballPlayoffSource` for their Super Regional + Finals stages), and `DoubleEliminationSource` (4-team double-elim base class for NCAA Baseball / Softball Regional + 8-team MCWS/WCWS bracket; plumbing into the sport sources is tracked in #43). `BestOfNSeriesSource._series_length_for_stage(stage)` hook lets each sport map stages to series lengths — NHL uses uniform 7; MLB uses 3/5/7/7 (WC/LDS/LCS/WS). `place_upcoming` stamps each fetched upcoming row with its bracket identity via `_sim_extra` (each tie shape's ONE builder of a simulated row's `extra`), and `ALL_ROWS_POSTSEASON` marks the playoff-only shapes — see the #243 gotcha below. |
| `sources/points_based.py` | Shared Monte Carlo for round-robin point-scoring sports (NCAAF, NCAAM, NHL regular, MLB regular). Subclasses set `_count_field` to choose between `"wins"` (CFB/CBB/MLB) and `"standings_points"` (NHL) for terminal_outcomes bucketing. Sets `shared_season_importance = True` for the whole family: the result model is Poisson on fixed strengths and never reads `state`. |
| `sources/ncaaf.py` | CFBD `/rankings`, `/games`, `/lines` calls. CFBD uses **camelCase** (homeTeam, awayTeam) — easy gotcha. |
| `sources/nhl.py` | api-web.nhle.com (official, no key). `NhlRegularSource` uses standings points (regulation win = 2, OT/SO loss = 1, regulation loss = 0). `NhlPlayoffSource` is a best-of-7 BracketSportSource. The two cross-feed: NhlPlayoffSource borrows regular-season strengths via `set_regular_season_strengths()` so cup-final sampling uses the 82-game baseline. |
| `sources/mlb.py` | statsapi.mlb.com (official, no key). `MlbRegularSource` uses raw win count (no OT-loss bonus). `MlbPlayoffSource` is a BestOfNSeriesSource with per-stage series lengths (WC=3, LDS=5, LCS=7, WS=7). Same regular-season → playoff strength-sharing pattern as NHL. |
| `sources/nba.py` | ESPN unofficial API (stats.nba.com is WAF-blocked from most homelab egress). `NbaRegularSource` uses raw win count. `NbaPlayoffSource` is a BestOfNSeriesSource with uniform SERIES_LENGTH=7 (R1 / CSF / CF / FINALS). Stage routing comes from parsing the ESPN headline ("East 1st Round - Game 3" etc.) — the only place ESPN exposes the playoff round. All-Star Tournament games (which ESPN tags `season.type=2` but `competition.type.abbreviation=ALLSTAR`) are filtered out so the regular-season team list stays at 30. |
| `sources/mls.py` | ESPN unofficial API for the schedule + The Odds API (`soccer_usa_mls`, requested through `_odds.py`) for closeness. Intentionally thin: `MlsSource` does NOT inherit `PointsBasedSportSource` and `supports_importance=False`. MLS surfaces with favorite + closeness signals only; standings-based importance and the mixed-format MLS Cup bracket (best-of-3 R1 + single-leg subsequent rounds) are tracked in #30. Team-name fuzzy matching reuses `_util.TEAM_SUFFIX_TOKENS` — never add "united" to that list (it's a substantive body word for Atlanta United / D.C. United / Minnesota United). |
| `sources/soccer.py` | Football-Data.org for fixtures+standings, The Odds API (through `_odds.py`, region `eu`) for spreads. League position used as rank. `SoccerSource` is league-shaped (PL, ELC); `KnockoutSoccerSource` is bracket-shaped (UCL) — multi-inherits from `AggregateLegSource` (bracket state machine) + `SoccerSource` (FD.org fetch / strengths) with the MRO `K → AggregateLegSource → BracketSportSource → SoccerSource → SportSource`. Routed by `LEAGUE_CONTEXTS[fd_code].format`. `_fetch_standings_with_seed` returns a `StandingsBundle` (position map, scoring table, current table, previous-season final table, seeded flag), never one merged table — see the standings gotcha below. `build_h2h_entries` builds the previous-meetings list; name matching there is EXACT on purpose. `_fetch_season_matches_for_code` (the list the importance simulator replays) still never caches a FAILURE as an empty season, but it does remember the failure COUNT: `_SEASON_FETCH_MAX_ATTEMPTS` (2) attempts per refresh, `allow_scoring_retry()` hands scoring one of them, `season_fetch_failures()` reports the ones that gave up — see the season-fetch gotcha below. |
| `sources/_espn.py` | Shared ESPN helpers. `extract_espn_scoreboard_event` normalises one scoreboard event (its docstring forbids inlining a fourth copy). `sweep_upcoming_scoreboard` is the whole per-day sweep for UPCOMING-only sources, owning the US-Eastern-bucket lookback (`SCOREBOARD_LOOKBACK_DAYS`), the FINISHED drop, the stale-SCHEDULED floor (`MAX_AGE_AFTER_KICKOFF`) and the id dedupe. Used by `friendlies.py` + `english_cup.py`. Takes `http_get` INJECTED so each source keeps its own patchable `requests` symbol. The bracket sources deliberately do NOT use it: they sweep a fixed calendar window and KEEP finished games because bracket state comes from results already played. `fetch_days(start, end, fetch_day)` is the separate helper the full-season walks use (#229): it runs the per-day requests on a `ThreadPoolExecutor` (`_SEASON_WALK_WORKERS = 8`) and returns `[(day, result), ...]` in DATE order, so each caller's "first occurrence wins" dedupe behaves as its old sequential loop did. `fetch_day` is injected for the same reason `http_get` is. The module still imports no `requests` (`tests/test_full_season_window.py` pins that). |
| `sources/_odds.py` | The ONE place the plugin calls The Odds API (#200). `fetch_odds_events(sport_key, api_key, region, log_tag=)` returns raw events with h2h odds; the per-sport parsers (`_h2h_to_closeness`) stay in their sources on purpose, because soccer.py devigs over three outcomes and mls.py over two. Owns the credit budget: ONE region per request, a disk cache (`odds_cache.json`, `ODDS_CACHE_TTL` 24h, keyed `sport_key|region`), an `EXHAUSTED_BACKOFF` that stops asking for a day when the plan answers 401 `OUT_OF_USAGE_CREDITS` (keyed by a hashed `_key_id` so a NEW key is not held by the old one's backoff), and a stale-but-current fallback (`_current` drops events more than 6h past kickoff, so an old line is never lent to the same pair's next meeting). `tests/test_odds_budget.py` pins all of it, including a repo-wide scan that no other file mentions `the-odds-api.com`. |
| `sources/friendlies.py` | ESPN exhibition soccer: `InternationalFriendliesSource` (`fifa.friendly` / `fifa.friendly.w`, parametrized on gender) and `ClubFriendliesSource` (`club.friendly`). `supports_importance=False` deliberately: no table to simulate. Gated to Favorites by default (`friendlies_favorites_only`) because an exhibition's only claim to a slot is the favorite signal. `CLUBFRIENDLY` is a distinct prefix from `FRIENDLY` and its absence from `rivalries.json` is load-bearing: a pre-season kickabout is not a derby. |
| `sources/english_cup.py` | EFL (Carabao) Cup + FA Cup via ESPN (`eng.league_cup` / `eng.fa`). Exists because Football-Data.org gates EVERY domestic cup behind a paid plan (FLC=TIER_THREE, FAC=TIER_TWO; verified 403 on the free-tier key), so a `soccer.py::COMPETITIONS` entry would 403 every refresh and contribute zero games silently. Rounds come from `season.slug`; the slug->stage maps are PER-COMPETITION because the two cups' round names overlap at different depths (FA Cup 4th round = R32, EFL Cup 4th round = R16). `supports_importance=False` and ranks always `None` — the latter is what stops a giant-killing tie being penalised for being lopsided. Early rounds score via the `CUP_R*` band in `scoring.py`, ramping to just under the shared `QUARTER_FINALS`. #190. |
| `recording.py` | Auto-record policy (#216), pure and ORM-free: `resolve_slot_budget` (tightest positive M3U `max_streams` minus reserve), `plan_recordings` (kickoff order, capacity checked as concurrency; busy = other recordings, held = ours in progress, cut only as a `Plan.stops` STOP when `live_stops` (phase C; never dropped whole), extended only when the budget allows; post-roll yield and tier-1/tier-2 interruption via `priority_key` / `can_interrupt`, both PLANNED as `Plan.handoffs` by moving the earlier recording's end before it starts, keeping only the cuts the new game needs and rolling back an attempt that cannot free enough), `classify_existing` (user-deleted -> tombstone, user-edited or terminal -> hands off), `decide` (create / full-save update / extend-only while recording), `cancellable` (takes the cache `slate` + a `reason_holds` predicate, #224), `covered_by_user`, `recording_over` (the 🔴, read from real rows), `description_line`, the live-stop helpers `stop_entries` / `due_stops` / `next_stop_delay` / `stopped_properties` (mirrors Dispatcharr's Stop) / `strip_dot` / `stopped_description`, the WHY-we-recorded keys (`REASON_KEY` / `TEAMS_KEY` / `LEAGUE_KEY` / `GAME_KEY`) with `reason_properties` / `reason_changed` / `reason_checker`, and the `recordings_state.json` load/save/prune. `MARKER_KEY` in `Recording.custom_properties` is the only "ours" test. The ORM around it is `_autorecord_*` in plugin.py, which writes with `.create()` / FULL `.save()` on purpose (the Recording signals are wanted; `.update()` or a narrow `update_fields` leaves the capture task stale) — the ONE exception is `_backfill_reason`, which stamps the reason with `save(update_fields=["custom_properties"])` precisely because no time changed and nothing needs rescheduling. |
| `logos.py` | TheSportsDB matchup-thumbnail resolver. Looks up the curated game via `searchevents.php` (team-name pair, date-tolerance ±2 days, sport-hint disambiguation), downloads the 960x540 graphic to `/data/logos/ranked_matchups_<sha1>.jpg`, and registers a `Logo` row pointing at it. Persistent per-marker cache (`sportsdb_thumb_cache.json`, 14d positive TTL / 1d negative TTL) means apply only HTTP-probes each fixture once. Field-event sources (`away=="Field"`) and dry_run short-circuit the lookup. Stale-file sweep at the end of each apply prunes JPGs whose marker isn't in the live set. Also resolves the league/tournament BADGE fallback (#102): `SPORTSDB_LEAGUE_IDS` (`sport_prefix` -> verified league id), `league_id_for`, `resolve_league_badge_url` (`lookupleague.php` -> `strBadge`), cached as `ranked_matchups_badge_<id>.png` (distinct prefix the sweep skips). |

State (gitignored, lives in `<plugin_dir>/`):
- `cache.json` — last refresh result (curated game list with score breakdowns). Carries TWO timestamps: `refreshed_at` (the cache write) and `matched_at` (stamped as matching BEGAN: after scoring, immediately before the EPG/stream sweep). `_match_snapshot_time` reads `matched_at` and falls back to `refreshed_at` for a pre-upgrade cache; anything asking "were these matches taken before X?" must use it, because an M3U refresh that lands DURING a long refresh is older than `refreshed_at` and would never be noticed
- `recording_stops.json` — planned stops of RUNNING recordings (#216 phase C): written whole by apply under the scheduler lock, only READ by the live handoff timer in `_reaper_loop` (`_run_due_stops`), so the timer never races apply
- `recordings_state.json` — auto-record bookkeeping (#216): what the plugin last wrote for each recording it created (so a user edit is recognised and left alone), and tombstones for ones the user deleted so they are not re-created
- `odds_cache.json` — The Odds API responses (#200), one entry per `sport_key|region` with its `fetched_at`, plus `exhausted_until` / `exhausted_key` for the out-of-credits backoff and the last `credits_remaining` header. Written by `sources/_odds.py` with the same per-writer temp + atomic replace as the cache. Holds no API key, and a hand-edited or wrong-shaped file is started over rather than failing every league's fetch
- `last_run.json` — outcome of the most recent background run (#227): kind, status, message, start/finish, duration, `consecutive_failures`, `last_ok_at`, plus a `skipped_kind`/`skipped_at` note when a request never got the lock. Written by `tasks._record_last_run` in `_run_under_lock`'s `finally`, read by `tasks.read_last_run` for the Show-current-state verdict AND by `_slot_already_covered` to decide a waiting scheduled slot has already been done for it (see the scheduler section) — so `kind`, `status` and `started_at` steer behaviour, not just display. `kind` is one of `RUN_KIND_LABELS` (`auto_pipeline`, `scheduled_auto_pipeline`, `m3u_rematch`, `refresh`) — the label is what names WHY a run happened, so a new background trigger needs an entry there or Show current state cannot explain itself, plus a decision about whether it belongs in `_PIPELINE_RUN_KINDS`. A FILE, not Redis: it must survive a restart and be readable by every uwsgi worker
- `cfbd_api_key`, `football_data_api_key`, `odds_api_key`, `anthropic_api_key` — file fallback when settings field is blank.

## How channels are produced

Source channels are **never modified**. Apply creates virtual channels in a
target `ChannelGroup` (default `Top Matchups` — user has it as `!Top Matchups`
to sort to the top of group lists):

- `tvg_id` = `ranked_matchups:<SPORT>:<tag><source_id>` (`_build_marker_key`,
  first key present in `_MARKER_ID_KEYS`) — the game's channel identity, and
  used for cleanup detection on next apply (any channel with this prefix in any
  group is "ours"). It MUST NOT include the kickoff time: a rescheduled game
  would get a second channel (#217). Only a row with no source id falls back to
  the legacy teams+kickoff hash; `_rekey_legacy_virtuals` renames pre-#217
  channels in place on the first apply after upgrade
- `channel_number` — TWO schemes, `channel_numbering_mode`. Default `kickoff`:
  `base + minutes-since-CHANNEL_NUMBER_ORIGIN × SLOTS + hash%SLOTS`, so a game's
  number does not move while its kickoff holds (#121; a slot already held in the
  kickoff minute is kept, #217) and the list sorts by start time. `compact`:
  `allocate_compact_numbers` keeps every number inside
  `[base, base+compact_band_size)`, holds a published game on its slot, and
  allocates new games ABOVE the highest in use. Handing a freed number to the
  next game is safe since #204 (every programme published for a game ends by the
  moment its channel goes away — see the gotcha below), so an exact band is a
  supported configuration and the docs must NOT tell users to widen it for
  safety. Compact CAN omit a marker when the band is narrower than the slate;
  the apply loop skips those games. `9000 + cache_index` was the pre-#119
  scheme, gone
- `streams` = cloned via `ChannelStream` from the matched source channel, plus
  the Path C stream-name matches. `_stream_sort_key` orders the stack
  outside-in: group policy (`fallback_only_groups` demotes a group below
  everything else) → provenance (a stream the user attached to a channel of
  their own beats one only the Path C sweep found) → language
  (`language_preferences`, an ordered code list, default `en`) → quality
  (ffprobe resolution/bitrate, then name hints). `excluded_groups` removes
  streams outright; see the gotcha below for where that filter has to run
- `epg_data` = a per-channel `EPGData` row in our dummy `EPGSource` with same
  name as the group; `ProgramData` description carries the WHY breakdown

A 2-phase renumber dance avoids the unique constraint on
`(channel_group, channel_number)`: park existing channels at
`_resolve_park_base` (highest target number this apply will write + 1000)
first, then assign target numbers.

In `compact` mode only, apply also collects `_reserved_channel_numbers`:
every number in the band already published by a channel we do NOT own
(effective numbers, so a `ChannelOverride` counts; the recordings-archive
channel counts as somebody else's because it lives in another group). The DB
constraint is per-group so a duplicate is legal, but the M3U, XMLTV and Xtream
Codes outputs all key the guide on the bare channel number, so a shared number
binds the guide arbitrarily for BOTH channels. Reserved numbers are skipped; if
the whole band is taken, apply returns an error rather than publishing nothing
and reaping the group.

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

Four steps, using NCAAM (College Basketball) as the worked example.
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

4. If your feed publishes a per-game id, stamp it into `extra` and add that
   key + a tag to `_MARKER_ID_KEYS` in `plugin.py` (#217) — that id is the
   channel's identity, so a source that skips this silently falls back to the
   teams+kickoff hash and gets a second channel whenever a game is
   rescheduled. Append at the END; the order is part of every live channel's
   tvg_id. `tests/test_marker_identity.py` fails if a source stamps an `*_id`
   key that is neither listed there nor exempted in the test.

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

### The importance budget: one set of seasons per league, 1500s for the lot

`tasks.py` kills the pipeline subprocess at 1500s, and importance simulation
is what fills it. Since #230 there are TWO cost models, and which one a source
gets is decided by `shared_season_importance`:

- **Shared (`shared_season_importance = True`)**: `_shared_league_importance`
  in `plugin.py` groups the slate by (source, competition), simulates each
  group's remaining season `importance_seasons` times (default 2000) ONCE, and
  reads every game of that group off those seasons. **Cost is per league, not
  per game.** Measured on a 499-game autumn slate: 125 CPU-s total.
- **Per game (the fallback)**: `compute_match_importance` replays the whole
  remaining season for ONE game, at `PER_GAME_IMPORTANCE_SIMS` (500, pinned at
  its historical value on purpose — applying the per-league 2000 to it would
  quadruple exactly the cost #230 removed). Measured 2026-08-29 for NCAAF:
  ~7.0s per row. This path takes knockout sources, an active group→knockout
  chain, and any game `monte_carlo_importance_shared` returns `None` for.

**Only a source whose `sample_result` ignores `state` may opt in.** The shared
simulator samples each target at its natural place in the schedule rather than
first, which draws from the same distribution only when the result model does
not read the season state. `KnockoutSoccerSource` reads leg 1 to sample leg 2
and therefore sets the flag back to False.
`tests/test_shared_importance.py` enforces this as a CLASS guard over every
`SportSource` subclass (AST check on `sample_result`), so a new source that
inherits the opt-in while reading state fails without anyone remembering it.

`None` from the shared path is "fall back", never a zero: a target not played
in EVERY simulated season (absent from the pool, or reachable only on some
bracket paths) would otherwise be measured conditional on reaching it.

The 1500s ceiling is still what decides whether a `fetch_upcoming` filter is
cosmetic or load-bearing — the per-game fallback is unchanged, and a wider
emitted set widens the simulated season for everyone. Three rules bound what
gets simulated:

- **Bound what `fetch_upcoming` emits to what can actually score.** NCAAF
  shipped without a division filter and pulled all of CFBD's D-II/D-III
  feed: 157 games where 27 involved an FBS team, ~1100s of the budget under
  the then-current per-game model, and `auto_pipeline` timed out on every run
  from the day the season opened. Sharing makes those rows cheaper, not free:
  they still lengthen every simulated season.
- **The emitted universe and the simulated universe must be the same set.**
  A team the simulator has never heard of has no strength estimate and no
  win-count band, so its games score exactly 0.00 while still being played in
  every simulated season. Derive both from one predicate and one cached fetch;
  two call sites that "agree" are a convention, and conventions drift silently.
- **A `_fetch_full_season_games` walk runs to the season's END, and must never
  read the clock** (#229). That list IS the simulator's season: the fixtures
  still to play are what decide who finishes where, so capping the walk at
  `now + 7 days` makes the simulated season end next week, nobody can change
  band, and importance reads exactly 0 (measured 2026-10-10: every NFL game,
  with 80 of 272 fixtures seen). Seven ESPN sources shipped with that cap;
  #65 had already fixed the same bug for MLS alone in `mls_standings.py`.
  `tests/test_full_season_window.py` is the CLASS guard: an AST check that no
  `_fetch_full_season_games` in `sources/` calls `now` / `today` / `utcnow`,
  plus a behavioural half that walks each source under two pinned clocks and
  asserts the last day requested is identical. Walking the whole season is
  affordable because the days are fetched concurrently via
  `_espn.fetch_days` — and because the walk is concurrent, a test must assert
  on `max(days)`, not on the last call made.

  The season's end is a CALENDAR constant per source, so pick one that ends
  the walk where the thing being simulated ends. NCAA soccer's is
  `_REGULAR_SEASON_END` (`{"m": (11, 18), "w": (11, 11)}`) rather than
  December: ESPN gives NCAA tournament games the same season slug, type and
  note as regular-season games, so they cannot be filtered by field, and
  walking into the tournament would add its results to the regular-season
  table the win-count bands are scored on. Those two dates sit inside the gap
  ESPN showed in 2025 between the last conference final and the first
  tournament round; the NCAA calendar shifts by weekday each year, so re-check
  them against the scoreboard when a season's rounds move.

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

The reaper loop does triple duty: besides reaping it runs the live handoff
timer (`_run_due_stops`, #216 phase C) and the post-M3U rematch check
(`_maybe_rematch_after_m3u`, see below). So it must NOT early-return when
reaping is off — it sleeps until whichever comes first, the next expiry, the
next planned stop (`next_stop_delay`), or `_HANDOFF_RECHECK_SECONDS`, and on
EVERY wake runs the due stops and the rematch check, **above** the
`reap_delay` gate that `continue`s past non-reap ticks; only a tick that woke
for a reap calls `_action_reap_locked`. Anything put below that gate runs only
on the rare tick that also reaps. Every worker runs this loop; each stop is a
locked, idempotent write, so exactly one of them stops a given recording.

**Matching is a SNAPSHOT, and providers rename their feeds on their own
clock.** Many M3Us reuse fixed event slots ("ESPN UNLTD 052") and rename each
per game, so a slot that named something else at refresh time can name
tonight's game after the next M3U refresh — and between scheduled refreshes
nothing noticed, leaving a curated channel as a zero-stream placeholder that
answers 503 when tuned (measured 2026-10-10: M3U renamed at 12:22, our refresh
was 11:42, next scheduled run 16:00). `_maybe_rematch_after_m3u` compares
`_latest_m3u_refresh()` (active `M3UAccount.updated_at`, which Dispatcharr sets
only on a successful refresh — NOT `status`, which flips mid-refresh, and NOT
`Stream.updated_at`) against `_match_snapshot_time(cache)`, and when a curated
game is inside `_games_in_rematch_window` (live up to 4h back, or starting
within 6h) runs the FULL auto_pipeline via `tasks.run_m3u_rematch_pipeline`.
Three things hold it down and each is load-bearing: it is gated on
`auto_refresh_enabled` (a user who turned background runs off gets none); ONE
Redis cooldown key for all workers and all M3U refreshes
(`_REMATCH_COOLDOWN_KEY`), shortened to 15 min after a success so a second late
feed is picked up promptly and left at an hour after a failure so a broken
pipeline is not re-run on every M3U refresh — do NOT key it per M3U refresh
time, an M3U refreshing every few minutes would then start a failing pipeline
each time; and Redis unreachable means no run at all, since this is destructive
work the schedule still covers. A lost run lock is not a failure: the cooldown
is deleted so a later tick retries, and the holder is usually a refresh that
makes this one unnecessary.

**A scheduled slot is CLAIMED by one worker, which then waits for the lock.**
Every uwsgi worker runs its own scheduler and they all fire at the same second.
Before `_run_scheduled_slot` each fired once, the cross-worker lock let one
through, and the rest logged "skipping" — fine until something ELSE held the
lock at that second (the reaper's re-apply, a manual run), at which point every
copy skipped and the slot was lost until the next one hours later. Now
`_claim_scheduled_slot` does a Redis `SET NX` keyed by the slot time and only
the winner retries (`_SCHEDULED_RETRY_SECONDS` apart, until
`_scheduled_retry_deadline`: the 30-minute window or one interval before the
NEXT slot, whichever is sooner). The CLAIM, not the lock, is what stops the
other seven each running the pipeline once the lock frees — dropping it and
retrying everywhere runs the pipeline eight times back to back. When Redis is
unreachable (`_SLOT_UNKNOWN`) every worker is an "owner", so that path makes
exactly ONE attempt and records the skip, never retries. Per-attempt losses are
not recorded as skips (`run_scheduled_pipeline(record_skip=False)`); the owner
calls `tasks.record_skipped_run` once when it gives up.

**Waiting for the lock means somebody else is doing work, and some of that work
IS this slot's.** After each retry sleep the owner asks `_slot_already_covered`:
if `last_run.json` records a refresh + apply (`_PIPELINE_RUN_KINDS` —
`auto_pipeline`, `scheduled_auto_pipeline`, `m3u_rematch`, NOT a bare `refresh`,
which does not apply) that STARTED at or after the slot and finished `ok`, the
slot's job is done and the owner returns without running a second pipeline back
to back. The common causes are a manual Refresh + apply clicked just after the
slot, an M3U rematch, and a deploy: the reload endpoint restarts the scheduler
thread only in the worker that serves it, so the other workers keep running the
previous version's threads until the container restarts and one of those can
hold the lock through the whole window. Both halves of the condition are
load-bearing: a FAILED run does not cover the slot (nothing was published), and
`started_at` must be compared, not `finished_at` — a run that began BEFORE the
slot used a pre-slot view of the world, which is the whole reason the slot
exists.

**Zoned datetime arithmetic is WALL-CLOCK arithmetic.** Two datetimes sharing
one `ZoneInfo` compare and subtract by wall time, so on a fall-back night
`_next_fire_time` picked a slot that had already run and fired it again, and a
30-minute retry window became 90. Every comparison, sleep length and deadline
in the scheduler converts to UTC first (`_next_fire_time`, `_scheduler_loop`'s
`sleep_s`, `_scheduled_retry_deadline`). Anything new that does clock maths on
the user's `local_timezone` has the same hazard.

**No threads in the pipeline child.** The subprocess runs `django.setup()`,
which discovers plugins and builds a `Plugin` — so every pipeline run used to
start a second scheduler and reaper inside itself (visible in the forwarded
child log as "scheduler thread started (pid=<child>)"), extra actors that could
fire a schedule or a handoff from inside a run. `tasks.run_pipeline_subprocess`
sets `PIPELINE_CHILD_ENV=1` and `Plugin.__init__` returns before
`_start_reaper()` when it sees it.

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

**Settings read before a sleep are not settings** (#223). `_scheduler_loop`
reads settings at the top of the loop to work out WHEN to fire, then sleeps
until the next `scheduled_times` slot — hours. It must re-read
(`plugin_ref.get_current_settings()`, and re-check `auto_refresh_enabled`)
AFTER `_scheduler_sleep` returns and before handing settings to
`_run_scheduled_slot`. Measured 2026-09-26: Recorded teams added at
5:48 PM were invisible to the 7 PM run, which then cancelled a scheduled
recording. Anything else that sleeps and then acts on a snapshot has the same
hazard — including `_run_scheduled_slot`'s own retry wait, which re-reads
settings and bails after each `_scheduler_sleep` so turning auto-refresh off
while a slot is waiting for the lock cancels it.

### cache.json has TWO lists: `games` and `bench`

`games` is what apply puts on air; `bench` is scored-but-not-applied stock the
reaper promotes from as games finish (#197). Splitting them at write time
rather than tagging rows is deliberate: `_action_apply` still applies exactly
the list it is handed. It reads the bench for exactly one thing — the
auto-record *slate* (`_autorecord_prepare(..., bench=...)`, #224), which is the
set of markers the cache knows about at all; nothing on the bench is published.

`_split_applied` is what does the split, and it is NOT a `payload[:max_games]`
slice. The first `max_games` rows go on air PLUS every **forced** row past them
(`_is_forced_row`: a favorite or a recorded team). The refresh cap already kept
favorites past `retain`, but the slice then benched any favorite that sorted
below `max_games` — the sort is today-first, so a favorite playing tomorrow
lands after a full slate of today's games — and it got no channel at all,
contradicting the Favorites help text. A recorded team benched that way would
never be recorded.

`reap_apply_pending` is a third key. The reaper rewrites the cache BEFORE
calling apply, so a failed apply leaves the desired state published and the DB
behind it. The flag is how the next tick knows to re-apply; without it the
expired games are already absent from the cache and nothing could ever notice
they still need removing.

**A refresh never takes a game off air; only the reaper does (#240).** The
refresh rebuilds `games` from the fetch, and apply deletes every channel the
list omits, but `ncaaf.py` and `mlb.py` list only games that have not kicked
off, and a failed source degrades to `[]`. So `_rows_to_hold` keeps every
previously applied row that has kicked off, or whose `sport_label` came back
empty or raised, until `_partition_expired` would reap it. Held rows the fetch
still lists are protected from the cap and the bench; held rows it omits are
carried by `_carry_forward`, re-matched (an empty re-match keeps the old match),
and flagged `held_on_air` so the reaper does not backfill a cap slot they never
took. While a held game is on air, a fetched game of the same matchup
(`_series_key`) waits, keeping one channel per series.

**Reaping delegates deletion to `_action_apply` on purpose.** Apply already
reaps owned channels missing from the cache, with DVR-recording protection,
the archive re-home, ChannelStream/EPGData cleanup, and signal-safe queryset
deletes. Do NOT add a second deletion path; it would be a copy that has to
stay in agreement with that one.

## Known gotchas / lessons learned

- **A game's end time is ESTIMATED, and there is exactly one estimate.**
  No feed publishes one. `_game_end_utc` reuses `_epg_match_window`'s
  per-sport post-game hours (~4h gridiron, 2.5h soccer, 24h boxing). DO NOT add
  a second duration table: the apply path had one (a flat `EPG_POST_HOURS` for
  `prog_end`) while the reaper used the per-sport window, so every soccer
  channel was reaped 30 minutes before its own live programme said the match
  had ended (#204). Both sides now come from `_game_end_utc`. The live row's
  end is `min(_game_end_utc, start + EPG_POST_HOURS)`: the match window is a
  match-RECALL tolerance, not a duration, so using it raw would label a boxing
  card live for a day. Taking the minimum also preserves the invariant
  everything below rests on, `prog_end <= reap_at`. That whole computation is
  `_live_window(g, start_dt)`, ONE definition shared by the EPG writer and the
  auto-record window (#216), so a recording can never cover a different span
  than the block the guide calls live. Recording the Upcoming block instead is
  #145.
- **No programme we publish may outlive the channel it describes** (#204).
  The post-game "Final: ..." row bridges the whistle to the apply that drops the
  channel, and `_compute_past_slot_end` clamps it to the reap deadline (floored
  at `prog_end`; the floor is a separate step because `_next_fire_time` can
  land before `prog_end` on a DST fall-back day and produce a negative-width
  row). Callers pass `reap_at=None` when reaping is off — `_game_reap_at_utc(g, 0)`
  is game-END, not None, so passing it through would collapse the past row to
  zero width on the DEFAULT config — and skip the row entirely when the window
  comes out empty, since Dispatcharr serialises a zero-width programme
  verbatim rather than rejecting it. Symmetrically, `_partition_expired` takes
  `settings` and holds a game until `_published_guide_end` has passed: the
  deadline baked into a published row is a snapshot, while the reaper re-reads
  the setting every tick, so lowering it would otherwise delete a channel out
  from under its own live guide entry. This invariant is what makes compact
  number reuse safe.
- **A marker is a channel identity, so nothing mutable may go into it** (#217).
  `_build_marker_key` takes the first id in `_MARKER_ID_KEYS`; bare `game_id` is
  deliberately absent (it is `simulation._MATCH_ID_KEYS`' key and is synthetic
  on some pool rows), and the two lists must NOT be merged. Three consequences
  live in `_action_apply` and have to stay: `_unique_by_marker` runs before
  anything else, because two cached rows for one event (two sources listing it,
  names differing) would otherwise create two channels on one number and fail
  the whole transaction on the unique constraint; `_rekey_legacy_virtuals` runs
  after `existing_virtuals` is built and BEFORE numbering, and persists via
  queryset `.update(tvg_id=...)` (never `ch.save()`, see the `post_save` trap
  below) so an upgraded install renames its channels instead of recreating them
  — `Recording.channel` is `on_delete=CASCADE`, so a recreate loses the user's
  recordings; and orphan-EPG cleanup excludes rows a live channel still POINTS
  at, since a re-keyed channel the write loop skipped (held for an active
  recording) still carries its `EPGData` under the OLD marker. The re-key
  refuses to guess: an ambiguous legacy hash (two TBD-vs-TBD slots at one
  kickoff) is left to the normal stale path, which preserves recordings.
- **A recording that has already STARTED is cut only by a STOP, never by a
  shorter end** (#216 phase B/C). Dispatcharr ignores a shorter `end_time` on a
  recording in progress, so a yield or interruption of a `held` marker is only
  planned with `live_stops=True` and comes back in `Plan.stops`; apply writes
  it to `recording_stops.json` and `_run_due_stops` (in `_reaper_loop`) makes
  the Stop-button write at the handoff moment, after checking the taker's
  channel has a stream. A held recording is never dropped whole. The symmetric
  half is easy to miss: a recording cut on
  the previous apply comes back as `held` with the SHORT end, so
  `plan_recordings` must extend it only when the extra time still fits the
  budget once everything else is placed — extending unconditionally overbooks
  the slot its taker already holds. A whole attempt to free capacity is rolled
  back when it does not free enough, and any cut the new game turned out not to
  need is undone (post-roll yields first), so nobody loses recording time for
  no capacity gain.
- **A game missing from the cache is not evidence that its recording is
  unwanted** (#224). A source can return zero games from one failed call, and
  reading "absent from the slate" as "no longer wanted" then deletes scheduled
  recordings for a whole sport (measured 2026-09-26: one CFBD failure, one
  cancelled Recorded-team recording). So every recording stores WHY it exists
  in its own `custom_properties` — `REASON_TEAM` with the matched teams and the
  game's `[home, away]`, or `REASON_RANKED` with its league — and `cancellable`
  drops a marker missing from the slate only when `reason_checker` says that
  reason no longer holds under the CURRENT settings. A marker still IN the
  slate but unplanned is cancelled as before: that is real evidence. Three
  things this rests on: the slate is games **plus bench**, or a demoted game
  would read as vanished; the team question is re-asked through
  `scoring.match_favorites` against the stored game rather than by comparing
  stored list entries, so fixing a typo in **Recorded teams** does not look
  like removing the team; and a row with no stored reason (written by 1.29.0 /
  1.30.0) answers **keep** — `_backfill_reason` stamps it on the next apply,
  but only on a row that has not started, because `run_recording` owns
  `custom_properties` on a recording in progress.
- **Re-read the Recording row, locked, before writing or cancelling it.** The
  rows in `ar.ours` were loaded when the plan was built, and apply's LLM / logo
  pre-pass can take minutes; a recording that started in between would be
  decided as "scheduled" and saved back with its live `status` overwritten.
  `_autorecord_for_game` and `_autorecord_cancel` both re-fetch via
  `select_for_update()` inside apply's transaction (a plain re-read only
  narrows the race) and re-check `is_in_progress` / `is_terminal`.
- **Apply re-derives `recorded_matched`, it does not trust the cache** (#221).
  Refresh stamps the match into each cached row, so an edit to **Recorded
  teams** would otherwise sit inert until the next scheduled refresh hours
  later. `_rematch_recorded_teams` runs at the top of `_action_apply`, BEFORE
  `_autorecord_prepare`, through the same `scoring.match_favorites` refresh
  uses so the two cannot disagree. A team whose games are not cached at all
  still needs a refresh to fetch them.
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
- **Apply MUST invalidate the `/output/epg` chunk cache itself** (#201).
  Dispatcharr caches the XMLTV for 300s and drops that cache from exactly one
  place: the `post_save` receiver on `Channel`. We never reach that receiver
  on purpose (channels are created without `epg_data`, the EPG is attached with
  a queryset `.update()`) because the same receiver's other half wipes our
  `ProgramData` — and dodging the destructive half also drops the beneficial
  one. So `_action_apply` calls `_invalidate_epg_output_cache()` AFTER the
  transaction commits (inside it, a concurrent `/output/epg` would repopulate
  the cache from pre-commit rows). This covers reap too, since reap delegates
  deletion to apply. It reaches into Dispatcharr internals that are not a
  documented API, so it is best-effort: a build that moves the function must
  cost a stale-guide window, not the apply.
- **`excluded_groups` must drop streams ABOVE `seen_markers.add(marker)`**
  (#206). Every `continue` in the apply loop has to stay above that line: a
  marker added and then skipped leaves its existing channel neither published
  nor reaped, so it commits at phase 0's temporary parking number (measured
  live 2026-08-29, 22 channels stranded at 1400-1421). The exclusion therefore
  runs in the pre-pass, which drops a fully-excluded game into the existing
  "no source, no streams" path — placeholder or skip, DVR recordings preserved.
  The invariant check below that line LOGS and publishes; it must never
  `continue`. The same policy also has to reach candidate LOOKUP
  (`_build_epg_lookup(excluded_stream_ids=...)`, and `_action_diagnose` passes
  it too), or the matcher can still pick an excluded stream as a game's primary
  and apply then strips it, leaving the channel with no streams at all. A
  source channel is dropped only when EVERY stream it would donate is excluded;
  a partially-excluded one still contributes the rest.
- **Provenance ordering is self-reinforcing if `_curated_stream_ids` counts our
  own channels** (#206). Our virtual matchup channels are ordinary `Channel`
  rows holding ordinary `ChannelStream` links, so a naive "attached to any
  channel" test promotes whatever the previous apply attached and an uncurated
  feed reads as curated from its second run onward. Hence the
  `_owned_tvg_id_q("channel__")` exclude, and hence the query goes through
  `ChannelStream` rather than `Stream.objects.filter(channels__isnull=False)
  .exclude(...)`: `channels` is multi-valued, so that exclude would also drop
  streams attached to BOTH a real channel and one of ours — exactly the curated
  ones. `tools/export_snapshot.py` mirrors the prefix AND the legacy markers,
  or an offline replay disagrees with production about what is curated.
- **`extra["standings_table"]` is the SCORING table, and early in a season it
  is LAST season's** (#209). `SoccerSource._fetch_standings_with_seed`
  deliberately substitutes the previous season's final table as a ranking prior
  while the current one is younger than `_util.SEED_PLAYED_THRESHOLD`
  matchdays, so that key is a good prior AND a false statement about the league
  right now. Anything that writes a sentence a human reads — the LLM prompt,
  `_build_standings_posture_line`, the impact-on-favorites narrative,
  `_is_catchup_matchday` — must go through
  `_util.current_standings_table(extra)`, which prefers the real
  `extra["standings_table_current"]` and returns `[]` rather than let a prior
  be described as live. Swapping the scoring path onto the current table is a
  different change (it re-ranks every early-season game) and is not what this
  fix did. Two follow-on traps: cache rows written before #209 carry neither
  `standings_seeded` nor `standings_table_current`, so `_util.looks_seeded`
  catches them by SHAPE (every team on zero games played while somebody has
  points, which no real table can be) or an upgraded install keeps serving the
  old falsehood from every apply until the next refresh; and the "rooting
  against X" narratives are pre-rendered INTO the cache row, so both consumers
  read them through `_util.trusted_impact_narratives` (the `prompt_hash` change
  invalidates the LLM's cached OUTPUT, not its cached INPUT). Facts the scoring
  path and the prose path must agree on — `SEED_PLAYED_THRESHOLD`,
  `is_bottom_outcome`, `ordinal` — live in `_util` as single definitions,
  because #209 shipped precisely because those two paths disagreed.
- **A failed run never rewrites `cache.json`, so nothing in the cache can
  report one** (#227). Every caller of `tasks._run_under_lock` discards the
  result dict, so before `last_run.json` a failure left no trace outside the
  container log and **Show current state** served the last GOOD cache (or
  "Cache empty. Run refresh.") as if all were well. Four pieces hold the
  answer together and each has a trap: `_record_last_run` / `record_skipped_run`
  run inside `finally` **ahead of** the inflight clear and the lock release,
  so neither may ever raise (hence `_as_int` and the blanket `except`) or a
  corrupt record wedges every later run until the lock TTL; a skipped request
  (lock held / Redis down) writes only `skipped_*` and never a `status`,
  because it did not run and must not overwrite the real verdict — and a
  caller that will RETRY the lock passes `record_skip=False` and calls
  `record_skipped_run` itself only once it gives up, since `_run_under_lock`'s
  bool return ("did the work run?") is what a retry loop steers on and a lock
  that frees a minute later must not leave a stale "did not run" note; the
  per-writer temp name in `_write_last_run` exists because refresh fails open
  when Redis is down, so two workers can write at once; and `_run_health_lines`
  orders the lines data warnings → config notices → verdict LAST, with a
  running pipeline stated IN the verdict so the last line never presents the
  previous run's outcome as the answer to the click just made.
  `_slate_problems` is the ONE definition of "upstream broke" (sources that
  errored, nothing playable), shared by the refresh result and show_status —
  and it reads the summary strings by `_SOURCE_ERROR_MARK`, which is why
  `_source_error_entry` is the only place that formats them.
- **"Never cache a failure" is not "retry forever"** (#228).
  `sources/soccer.py::_fetch_season_matches_for_code` deliberately does not
  cache a failed full-season fetch as an empty season (that would silently zero
  importance for the whole competition), and the importance simulator reads
  that list INSIDE its per-season loop — so one transient football-data.org
  error became hundreds of paced, backed-off refetches and a refresh that hit
  the 1500s ceiling. The failure COUNT is now remembered (never the empty
  result): `_SEASON_FETCH_MAX_ATTEMPTS` attempts per refresh, cleared by
  `_clear_fd_caches`, after which the fetcher returns `[]` with no network and
  no sleep. Two halves make the bound safe. `allow_scoring_retry()`, called by
  `_action_refresh` as the scoring loop begins, resets every failed
  competition to one attempt remaining, because the early head-to-head reads
  (#209) — one per source sharing an `fd_code`, e.g. a World Cup's group AND
  knockout sources — can otherwise spend the whole budget before the simulator
  ever asks, while FD.org recovers in between. And `season_fetch_failures()`
  feeds `_action_refresh` one `_source_error_entry` per given-up competition,
  appended to `src_summary` BEFORE the cache write so `_slate_problems` and
  **Show current state** see it. The label says "stakes not simulated", not
  "scored 0": the other signals still ran, so a knockout tie keeps its round
  premium.
- **An Odds API request costs a credit per region per market, and the free
  plan has 500 a month** (#200). Three sources asking three regions on every
  refresh spent the allowance in days, after which every call answered 401
  `OUT_OF_USAGE_CREDITS` — logged as "401 Unauthorized", so two issues went
  hunting a key that was never bad. Everything now goes through
  `sources/_odds.py`: one region, cached 24h per `sport_key|region`, about 300
  credits a month for a dozen leagues. Adding a region, a market, or a
  competition with an `odds_sport_key` spends more of that budget, so redo the
  arithmetic in that module's docstring first. The out-of-credits path is
  deliberately distinct from a bad key in the log, and serves the last cached
  line rather than dropping closeness to nothing.
- **`datetime.fromordinal` is a classmethod and returns a NAIVE datetime**
  (#213). `now.fromordinal(now.toordinal() + days)` reads like "now plus N
  days" and is neither: it drops `tzinfo`, so comparing the result against an
  aware `start_time` raised `TypeError` and MLB Postseason reported an error on
  every refresh for the whole postseason. Use `now + timedelta(days=N)`.
- **The child log's LEADING LEVEL NAME is a contract.** `_pipeline_runner.py`
  sets `format="%(levelname)s %(name)s: %(message)s"`, and
  `tasks._child_line_level` parses that first token so a subprocess WARNING is
  re-logged as a WARNING instead of flattened to INFO, where a warnings-only
  log view would never show it. Changing either side silently hides upstream
  breakage. Relatedly, a TIMED-OUT run forwards its child's partial stderr
  (bytes on `TimeoutExpired`, even under `text=True`) bounded to
  `_TIMEOUT_LOG_TAIL_LINES` and names the last step reached — that tail is the
  only evidence of where the 1500s ceiling hit; a COMPLETED run still forwards
  every line.
- **A name that matches NOW may not have existed when we matched, and
  Diagnose must say so.** `_action_diagnose` reads the live DB while the
  verdict it explains came from the cached match, so a feed that arrived (or a
  reused slot renamed) after the last refresh looked like a matcher bug and
  Diagnose told the user "unexpected; please send us this". `_late_feed_note`
  replaces that verdict when `_candidate_changed_at` is later than
  `_match_snapshot_time`. Two deliberate choices there: it takes the OLDEST
  candidate, not the newest — if any one of them already named the game at
  match time, the matcher saw it and missed it, and that IS the bug to report,
  however many feeds turned up afterwards — and a candidate with no known time
  makes the whole answer `None`, because it could have been there all along.
  The per-candidate times are `Stream.updated_at` (an M3U refresh bumps it only
  for streams whose content changed, so a renamed slot carries its rename time)
  and `Channel.created_at` — never `Channel.updated_at`, which any save bumps,
  an EPG assignment or a profile change included. The whole note is wrapped in
  a `try`: a diagnostic must never raise.
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
- Today-first sort + channel renumbering, with auto / fixed virtual base and
  either numbering mode (`kickoff` default, `compact` band)
- Placeholder channels for unmatched-high-scoring games, and for every game
  involving a recorded team whatever its score
- Auto-record for the **Recorded teams** list (#216, phase A): those games are
  force-included past `max_games` and the favorites-only gates, and scheduled
  in Dispatcharr's DVR over the Live block plus `recording_post_roll_minutes`,
  inside a stream budget (`resolve_slot_budget`: tightest active M3U
  `max_streams` minus `recording_stream_reserve`). Policy in `recording.py`,
  ORM in `_autorecord_*`
- Ranked games in spare recording slots (#216, phase B): `record_ranked_games`
  (off by default) plus `record_leagues` (league codes in priority order, blank
  = every league) make a stream-matched ranked game a slot candidate
  (`_ranked_eligible`; a favorite is preferred, never eligible). Slots still
  fill in kickoff order; `recording_preference` (`earliest` default,
  `favorites` / `league` / `rating`) decides what happens when a game kicks off
  with every slot full, via `priority_key` / `can_interrupt`. Every handoff is
  PLANNED (`Plan.handoffs`, written into the guide line by
  `description_line(until=, taker=)`); a recording that has already started
  is stopped at the handoff by the live timer (phase C)
- Recordings survive a source outage (#224): each one stores why it exists, and
  a game that vanished from the whole cache is only cancelled once that reason
  stops holding under the current settings
- Failed and upstream-starved runs are visible (#227): every background run's
  outcome is persisted to `last_run.json`, **Show current state** ends with that
  verdict (OK / FAILED with the streak and the last good refresh / RUNNING NOW /
  skipped), warns above it when sources errored or nothing on the list is
  playable, and a timed-out run forwards its child's log tail and names the step
  it reached
- A football-data.org season fetch cannot eat the refresh budget or go quiet
  (#228): the season list the importance simulator replays is retried at most
  twice per refresh (one of them guaranteed to scoring), and a competition that
  gives up is reported as a failed source, "stakes not simulated"
- Feeds that arrive between refreshes are picked up: when an active M3U account
  refreshes after our matches were taken and a curated game is live or starts
  within 6 hours, the reaper loop runs Refresh + apply once
  (`_maybe_rematch_after_m3u`, gated on auto-refresh, one run per cooldown
  across all workers). Show current state names it "Refresh + apply after an
  M3U update"; Diagnose explains a late feed instead of calling it a bug
- A refresh cannot take a live game off air (#240): a game that has kicked off,
  or one whose source returned nothing this fetch, is held on the applied list
  (outside the `max_games` cap) and re-matched against the current feeds until
  the reaper would remove it anyway
- A scheduled time is no longer lost when something else holds the run lock at
  that second: one worker claims the slot and retries the lock up to 30 minutes
  (never into the next slot), and stands down without running if a refresh +
  apply that started after the slot finishes OK while it waits. Scheduler clock
  maths is DST-safe, and the
  pipeline subprocess starts no scheduler or reaper of its own
- Group-rename auto-cleanup
- Multi-time scheduler (`scheduled_times = "0400,1000,1600,2200"`)
- Both file-based and settings-based API keys (settings preferred, masked UI)
- Description includes kickoff time + WHY breakdown
- LLM descriptions grounded in the CURRENT season (#209): the prompt carries
  each team's precomputed posture (position, points, which outcome bands they
  sit in, exact points to the leader and to the relegation line), last season's
  final table with an explicit "newly promoted" for absent teams, previous
  meetings, the season phase (opener / early / midseason / run-in) with
  guidance, and for win-count sports the record plus distance to each still
  reachable win threshold. The arithmetic is precomputed in
  `llm_descriptions.py` rather than left to the model, and `SYSTEM_PROMPT`
  forbids stating any position, record, gap or zone proximity that is not in
  those lines. `show_status` suffixes a seeded rank pair with `~`
- Stream-stack ordering and per-group stream policy (#206):
  `language_preferences` (ordered codes, default `en`), `fallback_only_groups`
  (demote but keep playable), `excluded_groups` (never attach), plus
  zero-config provenance ordering. `widen_stream_pool` widens the candidate
  pool only; it no longer affects ordering
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

- Bump `version` in `plugin.json`, `__version__` in `__init__.py` AND
  `Plugin.version` in `plugin.py` — all three must stay in sync
  (`tests/test_version_consistency.py` enforces the three-way match).
- Tag the release in this repo (`git tag v0.2.0 && git push --tags`).
- Open a PR against the upstream `Plugins` repo bumping the version
  reference for our plugin.

### Future: built-in plugin hub

Dispatcharr is shipping an in-app plugin browser. Once that lands,
users will install/update without leaving the app — the listing page
and CONTRIBUTING.md are the source of truth.
