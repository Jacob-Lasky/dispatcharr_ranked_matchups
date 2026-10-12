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
| `plugin.json` | Manifest: settings (sports toggles, weights, favorites, schedule), actions. Read by Dispatcharr loader. The ONLY home of the field list: the `Plugin` class declares no `fields`. Dispatcharr renders it as one flat list with no grouping primitive, so the modal only gets shorter by having fewer fields (#208). |
| `merged_settings.py` | The settings #208 merged to shorten the modal, and the legacy fallback that keeps pre-#208 saved settings behaving identically: `LEAGUE_PAIRS` (six four-way Off/A/B/Both selects replacing pairs of `enable_*` toggles), the `ncaaf` select (replacing `enable_ncaaf` + `ncaaf_divisions`), the `scoring_weights` text (replacing seven `weight_<name>` numbers). `league_enabled(settings, "enable_<x>")` is how `_build_sources` asks about EVERY league, merged or not, so the legacy toggle ids remain the logical league ids. One precedence rule: a new key wins unless it is absent or at its default and legacy keys are stored, because `context["settings"]` fills manifest defaults in and so cannot tell "picked the default" from "never saved". `migrate_stored_settings` (written once by `plugin._migrate_stored_settings` from `Plugin.__init__`, compare-and-swap on the settings JSON) makes the modal SHOW what the runtime does, stamps `settings_schema: 2` after which legacy keys are ignored, and KEEPS the legacy keys so a downgrade restores the pre-upgrade setup (edits made after the upgrade are not carried back). A new key already present in the RAW stored dict was saved by the user, so migration keeps it even at its default. `get_current_settings` migrates on the way through too, so the scheduler and reaper always read the migrated dict. Pure, no Django. |
| `plugin.py` | Plugin class and action handlers. Daemon scheduler, EPG lookup closure, virtual-channel and stream linking, and inactive XMLTV EPG source management. |
| `scoring.py` | `GameSignals`, `Weights`, `GameScore`. `score_game()` sums per-signal contributions, `_compress_to_10()` does the tanh squash. Helpers: `match_favorites`, `compute_match_importance` (Lahvička Monte Carlo, one game at a time) and `compute_league_importance` (every game of one source from one shared set of seasons, #230) — both built from the same `importance_queries` + `importance_points` pair so the two paths ask the same questions and weight the answers identically, `format_channel_name` (delegates to `naming`), `render_importance_tagline` + `STAGE_BANDS` (bracket-band prettify), `tournament_stage_label`. League thresholds in `LEAGUE_CONTEXTS` dict (position, label, consequence_weight). |
| `naming.py` | Channel-name templating (Sonarr/Radarr `{group}`-collapse convention). `render_name`, `validate_template`, `build_context`, `preview_lines`, `DEFAULT_NAME_TEMPLATE`, `TOKENS`. Pure: no Django, no `scoring` import — which is why `_RECORDING_DOT` is a literal here rather than an import of `recording.DOT`; the two must stay the same glyph (`tests/test_autorecord.py` pins it). `{recording_dot}` is deliberately absent from `DEFAULT_NAME_TEMPLATE`. |
| `simulation.py` | Sport-agnostic Monte Carlo importance per Lahvička (2012). `monte_carlo_importance()` for single (team, outcome); `monte_carlo_importance_batch()` shares one set of N season simulations across K queries for ONE game; `monte_carlo_importance_shared()` shares one set across EVERY game of a source (#230), the production path for sources with `shared_season_importance = True` (state-free result models; knockouts opt out). `kendall_tau_c()` is the ordinal-association measure. |
| `matcher.py` | `match_games_to_channels()` resolves cached `GameRow` to Dispatcharr channels and streams. `_build_epg_lookup` (plugin.py) supplies three candidate paths: A, EPG `ProgramData` title, sub-title or description (whole-channel); B, channel name (whole-channel); C, stream name (stream-granular, so a stream need not be on a channel at all). Three tiers: `regex_strict` (channel or stream name names both teams, merging Tier-2 programme matches behind it), `regex_unique` (exactly one non-preview programme match), then Claude batched fallback or first candidate. `_partition_attach_targets` splits the result into `channel_ids` and `stream_ids`. |
| `sources/base.py` | `GameRow` + `MatchResult` dataclasses + abstract `SportSource`. ABC declares the Monte Carlo importance interface (`supports_importance` flag + 7 optional methods); sources opt in by overriding. A second, independent flag `shared_season_importance` (default False, #230) says the source's `sample_result` ignores `state`, so one set of simulated seasons can serve every one of its games — see the cost section below. `rank_field_size` (default None, #198) is how many teams a POLL source's ranks are drawn from; league sources leave it None (see step 5 of the extension contract). |
| `sources/bracket.py` | `BracketSportSource` shared state machine for all knockout/playoff sports (bracket inference, `_round_reached`, `terminal_outcomes` cascade). Three concrete tie shapes: `AggregateLegSource` (two-leg, used by `KnockoutSoccerSource`), `BestOfNSeriesSource` (best-of-N series, used by `NhlPlayoffSource`, `MlbPlayoffSource`, `NcaaBaseballPlayoffSource`, `NcaaSoftballPlayoffSource` for their Super Regional + Finals stages), and `DoubleEliminationSource` (4-team double-elim base class for NCAA Baseball / Softball Regional + 8-team MCWS/WCWS bracket; plumbing into the sport sources is tracked in #43). `BestOfNSeriesSource._series_length_for_stage(stage)` hook lets each sport map stages to series lengths — NHL uses uniform 7; MLB uses 3/5/7/7 (WC/LDS/LCS/WS). `place_upcoming` stamps each fetched upcoming row with its bracket identity via `_sim_extra` (its unplaced count asks `_unplaced_row_reaches_a_tie`: a stage, and for `DoubleEliminationSource` the same `_find_tie_meta` lookup apply_result uses, so a College World Series row the fetch left with `grouping_key=None` counts only when no tie holds its teams, #256) (each tie shape's ONE builder of a simulated row's `extra`), and `ALL_ROWS_POSTSEASON` marks the playoff-only shapes — see the #243 gotcha below. `ForcedWinnerPoissonSampler` is the ONE playoff `sample_result` (Poisson from `_strength_for`, a tie broken by a coin-flipped +1; `LAMBDA_FLOOR` 0.05 for soccer), mixed in FIRST by every series/double-elim source: a source claiming `supports_importance` without a sampler raised `NotImplementedError` per game (#249; `tests/test_playoff_sampler.py` scans for it, and checks every tie winner's `round_reached` is the next round's `KNOCKOUT_ROUND_DEPTH`, which WNBA's SF->FINALS jump from 1 to 3 broke). The bracket-fetch failure accounting (`_sweep_bracket_days`, `_cache_bracket_games`, `BRACKET_FETCH_MAX_ATTEMPTS`, `allow_bracket_scoring_retry`, `bracket_fetch_failure`) also lives here — see the #245 part of the #243 gotcha. |
| `sources/points_based.py` | Shared Monte Carlo for round-robin point-scoring sports (NCAAF, NCAAM, NHL regular, MLB regular). Subclasses set `_count_field` to choose between `"wins"` (CFB/CBB/MLB) and `"standings_points"` (NHL) for terminal_outcomes bucketing. Sets `shared_season_importance = True` for the whole family: the result model is Poisson on fixed strengths and never reads `state`. `_fetch_games_cached` reads the season ONCE per source instance inside its own `recording_lost_days()` record, and a season that could not be fetched whole (a walk day lost, or `base.SeasonUnavailable` / `RankingsUnavailable` raised by the walk: NHL's per-club schedules, MLB's and NCAAF's one season request, NCAAM's three chunks, MLS's conference map) is NOT simulated: `[]`, `season_fetch_failure()` says why, and the refresh reports `<label> season (stakes not simulated)` for a source with a game in the slate (#289). Not retried within the refresh: an ESPN walk already re-asks its blips, and a second whole-season walk per league would multiply an outage against the pipeline ceiling. |
| `sources/ncaaf.py` | CFBD `/rankings`, `/games`, `/lines` calls. CFBD uses **camelCase** (homeTeam, awayTeam) — easy gotcha. |
| `sources/nhl.py` | api-web.nhle.com (official, no key). `NhlRegularSource` uses standings points (regulation win = 2, OT/SO loss = 1, regulation loss = 0). `NhlPlayoffSource` is a best-of-7 BracketSportSource. The two cross-feed: NhlPlayoffSource borrows regular-season strengths via `set_regular_season_strengths()` so cup-final sampling uses the 82-game baseline. Both `fetch_upcoming` read `_upcoming_schedule_games`, which walks `_espn.upcoming_window` one returned `gameWeek` at a time (api-web answers seven days per `schedule/{date}`, filed by US Eastern date), takes only the run of days contiguous from the one requested (a gapped answer neither skips the gap nor pre-empts a later day's own answer) and keeps only days inside the window (#256). |
| `sources/mlb.py` | statsapi.mlb.com (official, no key). `MlbRegularSource` uses raw win count (no OT-loss bonus). `MlbPlayoffSource` is a BestOfNSeriesSource with per-stage series lengths (WC=3, LDS=5, LCS=7, WS=7). Same regular-season → playoff strength-sharing pattern as NHL. `MlbRegularSource.fetch_upcoming` takes `startDate`/`endDate` from `_espn.upcoming_window`, because statsapi files a game under its US date (#256), and skips the listing a moved game left behind by statsapi's own marker (`rescheduleDate` for a postponement, `resumeDate` for a suspension; the listing it moved to carries `rescheduledFrom` / `resumedFrom`), and keeps one listing per `gamePk` (only a kept listing claims its `gamePk`, so a skipped one never hides the live one). DO NOT replace the marker with "first" or "last listing wins": a game moves earlier as well as later, so either order publishes the abandoned slot in one direction. |
| `sources/nba.py` | ESPN unofficial API (stats.nba.com is WAF-blocked from most homelab egress). `NbaRegularSource` uses raw win count. `NbaPlayoffSource` is a BestOfNSeriesSource with uniform SERIES_LENGTH=7 (R1 / CSF / CF / FINALS). Stage routing comes from parsing the ESPN headline ("East 1st Round - Game 3" etc.) — the only place ESPN exposes the playoff round. All-Star Tournament games (which ESPN tags `season.type=2` but `competition.type.abbreviation=ALLSTAR`) are filtered out so the regular-season team list stays at 30. |
| `sources/mls.py` | ESPN unofficial API for the schedule + The Odds API (`soccer_usa_mls`, requested through `_odds.py`) for closeness. Intentionally thin: `MlsSource` does NOT inherit `PointsBasedSportSource` and `supports_importance=False`. MLS surfaces with favorite + closeness signals only; standings-based importance and the mixed-format MLS Cup bracket (best-of-3 R1 + single-leg subsequent rounds) are tracked in #30. Team-name fuzzy matching reuses `_util.TEAM_SUFFIX_TOKENS` — never add "united" to that list (it's a substantive body word for Atlanta United / D.C. United / Minnesota United). |
| `sources/soccer.py` | Football-Data.org for fixtures+standings, The Odds API (through `_odds.py`, region `eu`) for spreads. League position used as rank; the table is read only once there is a fixture to rank, and a failed `/standings` (this season's, or the previous season's seed) raises `RankingsUnavailable` (#283). `SoccerSource` is league-shaped (PL, ELC); `KnockoutSoccerSource` is bracket-shaped (UCL) — multi-inherits from `AggregateLegSource` (bracket state machine) + `SoccerSource` (FD.org fetch / strengths) with the MRO `K → AggregateLegSource → BracketSportSource → SoccerSource → SportSource`. Routed by `LEAGUE_CONTEXTS[fd_code].format`. `_fetch_standings_with_seed` returns a `StandingsBundle` (position map, scoring table, current table, previous-season final table, seeded flag), never one merged table — see the standings gotcha below. `build_h2h_entries` builds the previous-meetings list; name matching there is EXACT on purpose. `_fetch_season_matches_for_code` (the list the importance simulator replays) still never caches a FAILURE as an empty season, but it does remember the failure COUNT: `_SEASON_FETCH_MAX_ATTEMPTS` (2) attempts per refresh, `allow_scoring_retry()` hands scoring one of them, `season_fetch_failures()` reports the ones that gave up — see the season-fetch gotcha below. `GroupStageSoccerSource` (WC, EURO group stages) is paired with its competition's knockout source via `set_paired_knockout_source`, and `cross_source_chain` seeds the published bracket in `_CHAIN_BRACKETS` (one `_TournamentBracket` per FD.org code: WC 2026 enters at LAST_32, EURO 2028 at LAST_16) from each simulated group table while any group game is unplayed; the chain stays off when the season's group letters or group sizes do not match that bracket (#53, #78). Group tables rank head-to-head before overall goal difference (`_break_points_tie`). |
| `sources/_espn.py` | Shared ESPN helpers. `extract_espn_scoreboard_event` normalises one scoreboard event (its docstring forbids inlining a fourth copy). `fetch_scoreboard_day` / `scoreboard_day_url` build EVERY ESPN scoreboard request: one day, `&limit=1000` (`SCOREBOARD_EVENT_LIMIT`, whose comment carries the 2026-10-11 measurements, #182). A bare one-day URL pages silently at 100 events; `limit` above 1000 silently drops to 25; `dates=<range>` answers HTTP 400 for team sports and drops the last day for racing; `dates=YYYYMM` silently returned 742 of 1033 NCAAW games. `tests/test_espn_scoreboard.py` fails a `scoreboard?dates=` literal (outside docstrings) anywhere else in `sources/`. `fetch_scoreboard_day` returns None for a 200 whose body has no `events` list (#282): a real empty day measures as `"events": []` (2026-10-11, 13 leagues, team sports and field events), and ESPN's error bodies carry `code` and no list, so such a page is a day the per-day walk reports lost and a bracket sweep gives up on, never a quiet day. `fetch_espn_poll` is the one parser for the four ESPN college polls (#283): `{}` for an answered page listing no poll, `base.RankingsUnavailable` for a failed request, a body without a `rankings` list, or a chosen poll without `ranks`. `sweep_upcoming_events(http_get, url, days_ahead, convert)` is the whole per-day sweep behind EVERY ESPN `fetch_upcoming` (#256), owning the US-Eastern-bucket lookback, the FINISHED drop (`scoreboard_event_over`, which also reads a postponed `post`-not-`completed` game as over), the stale-SCHEDULED floor (`MAX_AGE_AFTER_KICKOFF`) and the event dedupe (`dedupe_events`, claimed only once `convert` keeps the event); `convert` is the source's own extraction and filter, None to skip. `sweep_upcoming_scoreboard` is it plus `extract_espn_scoreboard_event` (friendlies, English cups, NCAA soccer). Field events pass `field_events=True`: only their LOOKBACK bucket is filtered (the same `admits` rule, on event-level status), because a multi-day event's status and `date` are the current day's and the first day's (measured 2026-10-11: the Shanghai Masters, running to Oct 19, read `post`; golf's `endDate` is the START of the final day, so it cannot bound one either). The age floor has NO live exemption: past kickoff + 6 h the plugin's own `_game_end_utc` already calls the game over, so readmitting it would only republish a Past slot. The NCAA baseball/softball bracket `fetch_upcoming` do NOT guess a College World Series sub-bracket: it is read off opening-day pairings no upcoming window can see, so the row carries `grouping_key=None` and `place_upcoming` stamps the bracket's own key (a row it cannot place resolves its tie by team set in `_find_tie_meta`, which a wrong guessed key would defeat). The old window-relative guess dropped any game whose teams it had not seen. `upcoming_window(days_ahead)` is the ONE place `sources/` turns the clock into a date (first bucket today-UTC minus `SCOREBOARD_LOOKBACK_DAYS`, last today-UTC plus `days_ahead`, plus `admits(start, over)`); NHL (`schedule/{date}`) and MLB regular (`startDate=`) use it too, because both bucket by US date as ESPN does (measured 2026-10-11), as does NCAAM (CollegeBasketballData `startDateRange=`; its bucketing was not probed, it needs a key, but the lookback day only widens the request and `admits` drops what it adds) and boxing (only `today`, to report the days of its `days=` request). Twelve sources had computed their own today-UTC and lost every evening-Eastern game from 00:00Z; `tests/test_upcoming_window.py` is the class guard (AST: no `now().date()` outside `upcoming_window`, no `fetch_upcoming` walking `fetch_scoreboard_days` itself; behavioural: every built source's first dated URL request, ESPN `dates=`, NHL `schedule/{date}` and MLB `startDate=`, under a pinned 22:00-Eastern clock is the lookback day; NCAAM's lookback, sent as a `params=` range, is pinned by its own witness in `tests/test_hold_failed_days.py`; Football-Data's window comes from `upcoming_window` too (#285), pinned by `tests/test_football_data_window.py`: FD files a match by its UTC kickoff instant and treats `dateTo` as an EXCLUSIVE day bound (measured 2026-10-11), so `soccer._fetch_tier_fixtures` asks from the lookback day up to the day AFTER the last one, in chunks of at most 10 days that each start ON the previous `dateTo`, dedupes a boundary-instant match by id, and keeps only what `admits` keeps (FD's over statuses are `soccer._FD_OVER_STATUSES`); before #285 the window's last day and, past a 10-day lookahead, the day between two chunks were never asked for (live: 28 of 29 Sunday matches missing at the default 7-day lookahead), and a match FINISHED or CANCELLED earlier today was published as upcoming. Boxing's `days=` still starts at today-UTC; the feed's dates are naive and its window is a day count, not a date range. Dropping a FINISHED game from a refresh does not take a live channel off air early: `plugin._rows_to_hold` keeps every applied game that has kicked off until the reaper retires it. Takes `http_get` INJECTED so each source keeps its own patchable `requests` symbol. The bracket SWEEPS (`_fetch_bracket_games`) and full-season walks deliberately do NOT use it: they sweep a fixed calendar window and KEEP finished games because bracket state comes from results already played. They DO share the dedupe: every ESPN walk (upcoming sweep, bracket sweep, full-season walk) goes through `dedupe_events(events, keep)` (`scoreboard_events(pages)` flattens the pages), keyed on `event_identity`: the id, else a `no-id:` string of teams + kickoff, so id-less events neither collapse into one (the NCAA baseball/softball bracket sweeps did) nor vanish (every other walk skipped a None id). EVERY ESPN row and record that reaches the simulator carries that identity as its `game_id` / `id` (a walk's `keep` gets it as `key`; upcoming rows and the cup record builders call `event_identity(event)`), because `_applied` and the drain loop track games by id (a None id is never simulated by `PointsBasedSportSource`, and in a bracket the first FINISHED id-less game puts None in `_applied` and hides every other one), and because the upcoming row a refresh scores must match its season or bracket record (`simulation._same_match`, `place_upcoming`). It is a string, not a tuple, since `game_id` is written to cache.json and custom_properties; the kickoff in it is the parsed instant, and a team is named by its first of id / displayName / name / location / abbreviation. The single-leg soccer cup sweeps dedupe on `_soccer_bracket_helpers.pk_pair_identity` instead (`dedupe_events`' `identity` argument): it adds an id-less event's outcome to the key, so both records of an id-less PK pair reach `dedupe_pk_shootout_pairs`, which keeps the decisive one; MLS Cup's best-of-3 round keeps plain `event_identity`, since a second copy there would count as another game. The sport-specific marker ids (`espn_event_id`, `nba_game_id`, ...) stay the raw id (#279). `tests/test_event_identity_dedupe.py` holds the class guards (AST: no seen-set keyed on `id` / `game_id`, no dict literal stamping a raw id as `game_id`, and every walk's `keep` reads its key, across `sources/` and `plugin.py` outside `_espn.py`; the non-ESPN exemptions, NHL, Football-Data, CFBD and the simulator's own bookkeeping, are named and checked for staleness). `fetch_scoreboard_days(http_get, url, start, end)` is the per-day walk for the upcoming sweep and full-season walks (#182; ESPN bracket sweeps instead fetch through `fetch_scoreboard_day` inside `BracketSportSource._sweep_bracket_days`, in concurrent date-ordered batches of 8 that give up the whole sweep at the first batch with a failed day, #245), fetched in 16-day chunks and abandoned, with a WARNING, at the first chunk that entirely fails AND takes ≥10 s (a hang, which would otherwise push several season walks past the 25-minute pipeline ceiling; fast failures walk on), or once the walk has used 120 s (`SCOREBOARD_WALK_BUDGET_SECONDS`, checked between chunks, so a walk can overrun it by the chunk in flight plus a retry batch; a healthy season walk takes ~5 s). A walk then re-requests its failed days ONCE when they are a blip (some day answered, at most max(1, days/16) failed, capped at 8 so the retry is one batch, one timeout, walk budget left, and a last chunk that did not hang; an abandoned walk never qualifies), because one read timeout otherwise drops that day's finished games from the simulator's standings. Every day still without a page after that (failed, or never requested) is LOGGED and REPORTED as lost (`base.report_lost_days`, #261; see "A lost day is reported" below), as is a failed `schedule/{date}` request in NHL's `_upcoming_schedule_games` (ranged requests outside ESPN report through `base.report_lost_day_range`, see the same section); built on `fetch_days(start, end, fetch_day)` (#229), which runs the per-day requests on a `ThreadPoolExecutor` (`_SEASON_WALK_WORKERS = 8`) and returns `[(day, result), ...]` in DATE order, so each caller's "first occurrence wins" dedupe behaves as its old sequential loop did. `fetch_day` is injected for the same reason `http_get` is. `fetch_season_type_windows(league_slug, year, http_get)` reads a season's phases from ESPN's CORE API (`seasons/{year}`, one request, every type inline with its slug and UTC window) and `season_type_at` finds the phase holding a kickoff. That calendar is the ONLY place ESPN still names an NCAA soccer tournament round: since 2025 the events themselves mostly carry the plain season slug and type (#235), so `ncaa_soccer_cup.py` routes by event slug, then event season-type id, then round window, and `ncaa_soccer.py` ends its regular season (walk AND `fetch_upcoming`) at the regular-season type's end. The module still imports and calls no `requests` (`tests/test_match_identity_contract.py::TestSharedSweepDoesNotOwnTheHttpCall` pins that by AST). |
| `sources/_odds.py` | The ONE place the plugin calls The Odds API (#200). `fetch_odds_events(sport_key, api_key, region, log_tag=)` returns raw events with h2h odds; the per-sport parsers (`_h2h_to_closeness`) stay in their sources on purpose, because soccer.py devigs over three outcomes and mls.py over two. Owns the credit budget: ONE region per request, a disk cache (`odds_cache.json`, `ODDS_CACHE_TTL` 24h, keyed `sport_key|region`), an `EXHAUSTED_BACKOFF` that stops asking for a day when the plan answers 401 `OUT_OF_USAGE_CREDITS` (keyed by a hashed `_key_id` so a NEW key is not held by the old one's backoff), and a stale-but-current fallback (`_current` drops events more than 6h past kickoff, so an old line is never lent to the same pair's next meeting). `tests/test_odds_budget.py` pins all of it, including a repo-wide scan that no other file mentions `the-odds-api.com`. |
| `sources/friendlies.py` | ESPN exhibition soccer: `InternationalFriendliesSource` (`fifa.friendly` / `fifa.friendly.w`, parametrized on gender) and `ClubFriendliesSource` (`club.friendly`). `supports_importance=False` deliberately: no table to simulate. Gated to Favorites by default (`friendlies_favorites_only`) because an exhibition's only claim to a slot is the favorite signal. `CLUBFRIENDLY` is a distinct prefix from `FRIENDLY` and its absence from `rivalries.json` is load-bearing: a pre-season kickabout is not a derby. |
| `sources/english_cup.py` | EFL (Carabao) Cup + FA Cup via ESPN (`eng.league_cup` / `eng.fa`). Exists because Football-Data.org gates EVERY domestic cup behind a paid plan (FLC=TIER_THREE, FAC=TIER_TWO; verified 403 on the free-tier key), so a `soccer.py::COMPETITIONS` entry would 403 every refresh and contribute zero games silently. Rounds come from `season.slug`; the slug->stage maps are PER-COMPETITION because the two cups' round names overlap at different depths (FA Cup 4th round = R32, EFL Cup 4th round = R16). `supports_importance=False` and ranks always `None` — the latter is what stops a giant-killing tie being penalised for being lopsided. Early rounds score via the `CUP_R*` band in `scoring.py`, ramping to just under the shared `QUARTER_FINALS`. #190. |
| `recording.py` | Auto-record policy (#216), pure and ORM-free: `resolve_slot_budget` (tightest positive M3U `max_streams` minus reserve), `plan_recordings` (kickoff order, capacity checked as concurrency; busy = other recordings, held = ours in progress, cut only as a `Plan.stops` STOP when `live_stops` (phase C; never dropped whole), extended only when the budget allows; post-roll yield and tier-1/tier-2 interruption via `priority_key` / `can_interrupt`, both PLANNED as `Plan.handoffs` by moving the earlier recording's end before it starts, keeping only the cuts the new game needs and rolling back an attempt that cannot free enough), `classify_existing` (user-deleted -> tombstone, user-edited or terminal -> hands off), `decide` (create / full-save update / extend-only while recording), `cancellable` (takes the cache `slate` + a `reason_holds` predicate, #224), `covered_by_user`, `recording_over` (the 🔴, read from real rows), `description_line`, the live-stop helpers `stop_entries` / `due_stops` / `next_stop_delay` / `stopped_properties` (mirrors Dispatcharr's Stop) / `strip_dot` / `stopped_description`, the WHY-we-recorded keys (`REASON_KEY` / `TEAMS_KEY` / `LEAGUE_KEY` / `GAME_KEY`) with `reason_properties` / `reason_changed` / `reason_checker`, and the `recordings_state.json` load/save/prune. `MARKER_KEY` in `Recording.custom_properties` is the only "ours" test. The ORM around it is `_autorecord_*` in plugin.py, which writes with `.create()` / FULL `.save()` on purpose (the Recording signals are wanted; `.update()` or a narrow `update_fields` leaves the capture task stale) — the ONE exception is `_backfill_reason`, which stamps the reason with `save(update_fields=["custom_properties"])` precisely because no time changed and nothing needs rescheduling. |
| `logos.py` | TheSportsDB matchup-thumbnail resolver. Looks up the curated game via `searchevents.php` (team-name pair, date-tolerance ±2 days, sport-hint disambiguation), downloads the 960x540 graphic to `/data/logos/ranked_matchups_<sha1>.jpg`, and registers a `Logo` row pointing at it. Persistent per-marker cache (`sportsdb_thumb_cache.json`, 14d positive TTL / 1d negative TTL) means apply only HTTP-probes each fixture once. Field-event sources (`away=="Field"`) and dry_run short-circuit the lookup. Stale-file sweep at the end of each apply prunes JPGs whose marker isn't in the live set (`live_markers`: every game the pre-pass decided on plus every channel of ours still in the group, so a channel kept for its recording keeps its logo) and that no live channel's `Logo` still points at (`keep_filenames`: a channel re-keyed by #217 and kept shows the file named after its old marker). Also resolves the league/tournament BADGE fallback (#102): `SPORTSDB_LEAGUE_IDS` (`sport_prefix` -> verified league id), `league_id_for`, `resolve_league_badge_url` (`lookupleague.php` -> `strBadge`), cached as `ranked_matchups_badge_<id>.png` (distinct prefix the sweep skips). Every key of `_SPORT_HINT`, `SPORTSDB_LEAGUE_IDS` and `SPORTSDB_TOURNAMENT_LEAGUE_IDS` must be a `sport_prefix` some source emits: `tests/test_mapped_prefixes.py` derives the emitted set by building every source through `plugin._build_sources` (not by grepping literals, since several sources compute their prefix) and fails on a dead key (#183). The same test covers every OTHER table keyed on `sport_prefix` too (plugin.py's soccer match-window sets, the gamethumbs maps, the rivalries index), finds new ones by their contents so an unregistered table fails, checks the raw top-level keys of `rivalries.json` and `gamethumbs_aliases.json`, and checks every `scoring.LEAGUE_CONTEXTS` key is a context code some source writes. |
| `_util.py` | Shared helpers at the package root (importable from `plugin.py` and `sources/` without circulars): time parsing, channel-number allocation, field-event and series/group text, `redact_secrets`, and `http_get_json`, the ONE body behind every source module's `_http_get` (#193). Each source keeps a thin `_http_get` wrapper (a one-line module function; boxing's is a method, because its key rides in `self._headers()`) passing its OWN `requests` symbol and logger, because `<module>._http_get` and `<module>.requests` are patch points its tests stub; `_util.py` imports no `requests` and `tests/test_sources_http.py` pins both halves. |
| `_http.py` | Every outbound HTTP call, bounded by a WALL-CLOCK deadline equal to the call's `timeout` (#271). `run_with_deadline` runs the request on a daemon worker and stops waiting at the deadline, then hangs up every socket that worker connected with `shutdown()` on a `dup()`. Sockets are captured at the TRANSPORT: `get_within_deadline` uses `requests_mod.Session()` with `_TrackedAdapter` mounted (urllib3 pools whose connections hand over their socket from `_new_conn`, before TLS), and `urlopen_read` uses an opener whose handlers swap in `http.client` connections that do the same from `_create_connection`. DO NOT capture them with `sys.addaudithook`: the plugin runs inside Dispatcharr's uwsgi/Celery/daphne processes, a hook can never be removed and runs for every audit event there; `tests/test_http_deadline.py` fails if any module installs one. DO NOT hang up with the response's `close()` from the caller (it blocks on the reader's lock until the trickle ends), and DO NOT stream with `iter_content` and check the clock (the buffered read blocks until the chunk is full: measured 200s for a 1000-byte trickle; trickled headers arrive before any body iterator). Callers keep their failure contract: `requests.Timeout` from `get_within_deadline`, `ResponseDeadlineExceeded` (a TimeoutError) from `urlopen_read`, None from `http_get_json`; image downloads pass `max_bytes` because the body is held in memory until validated. Tests stub the transport with `tests/conftest.py::stub_requests_get` (`requests.Session.get`) and `stub_urlopen` (`OpenerDirector.open`); a stub on `requests.get` or `urlopen` is no longer reached, and the autouse `_no_live_network` fixture fails any test that reaches past loopback. `NetworkBudget` is the whole-pre-pass allowance apply's cosmetic fetches share: `APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME` x games, capped inside the pipeline child at its remaining life (`tasks.pipeline_child_seconds_left`, a `time.monotonic()` deadline the parent passes in `RANKED_MATCHUPS_PIPELINE_DEADLINE`) less `APPLY_WRITE_RESERVE_SECONDS`; a refused fetch takes its failure's fallback and caches nothing, and nothing that decides which channels publish or their streams may ever be gated on it. Known limits, documented in `_http.py`: DNS, a TCP connect per resolved address, and a SOCKS negotiation run before any socket is handed over, so they end on their own timeouts while the caller still returns at the deadline. `tests/test_http_deadline.py` also fails on any `requests.get/post`, `urlopen`, `build_opener` or `urllib3` use outside `_http.py`. |

State (gitignored, lives in `<plugin_dir>/`):
- `cache.json` — last refresh result (curated game list with score breakdowns). Carries TWO timestamps: `refreshed_at` (the cache write) and `matched_at` (stamped as matching BEGAN: after scoring, immediately before the EPG/stream sweep). `_match_snapshot_time` reads `matched_at` and falls back to `refreshed_at` for a pre-upgrade cache; anything asking "were these matches taken before X?" must use it, because an M3U refresh that lands DURING a long refresh is older than `refreshed_at` and would never be noticed. Also carries `empty_slate_known` (written by refresh, the reaper, and apply after a real publish), which decides whether apply may publish an EMPTY `games` list (#241, see "An empty `games` list can be a slate" below)
- `recording_stops.json` — planned stops of RUNNING recordings (#216 phase C): written whole by apply under the scheduler lock, only READ by the live handoff timer in `_reaper_loop` (`_run_due_stops`), so the timer never races apply
- `recordings_state.json` — auto-record bookkeeping (#216): what the plugin last wrote for each recording it created (so a user edit is recognised and left alone), and tombstones for ones the user deleted so they are not re-created
- `numbering_state.json` — `{"kickoff_base": N, "numbered_for": {marker: [offset, number, floor]}}`: the floor kickoff numbering last used (#219), and for each published channel the kickoff offset (`stable_channel_number(0, ...)`), number and floor it was numbered for (#278; per entry, because a channel kept for a recording keeps an earlier floor's number across a floor change, and only entries naming the current floor are trusted, `_numbered_for_floor`, which also moves an entry to a #217 re-keyed marker). Written by apply only (under the scheduler lock, after the transaction commits, never on a dry run), rebuilt each time from the numbers the run WROTE plus kept channels still on their recorded number, so a game no longer published drops out. Read back so auto mode does not re-derive the floor from the lineup, and so a channel nudged past its block keeps its number (see `channel_number` below). A missing or malformed file is not an error: the next apply infers the floor from the published channels, and a missing or bad record entry falls back to the chain rule for that channel. A failed save REMOVES the old file when it names another floor (that floor would otherwise be read back as current) and keeps it when it names the same one (its record is only older, and each entry is checked against the live number). `kickoff_base` keeps its pre-#278 shape, so older code still reads the floor
- `odds_cache.json` — The Odds API responses (#200), one entry per `sport_key|region` with its `fetched_at`, plus `exhausted_until` / `exhausted_key` for the out-of-credits backoff and the last `credits_remaining` header. Written by `sources/_odds.py` with the same per-writer temp + atomic replace as the cache. Holds no API key, and a hand-edited or wrong-shaped file is started over rather than failing every league's fetch
- `last_run.json` — outcome of the most recent background run (#227): kind, status, message, start/finish, duration, `consecutive_failures`, `last_ok_at`, plus a `skipped_kind`/`skipped_at` note when a request never got the lock, with `skipped_reason` (naming the holder, #239) stamped by `skipped_reason_at`. Written by `tasks._record_last_run` in `_run_under_lock`'s `finally`, read by `tasks.read_last_run` for the Show-current-state verdict AND by `_slot_already_covered` to decide a waiting scheduled slot has already been done for it (see the scheduler section) — so `kind`, `status` and `started_at` steer behaviour, not just display. `kind` is one of the `_run_under_lock` kinds in `RUN_KIND_LABELS` (`auto_pipeline`, `scheduled_auto_pipeline`, `m3u_rematch`, `refresh`; its `apply` and `reap` entries only describe lock holders) — the label is what names WHY a run happened, so a new background trigger needs an entry there or Show current state cannot explain itself, plus a decision about whether it belongs in `_PIPELINE_RUN_KINDS`. A FILE, not Redis: it must survive a restart and be readable by every uwsgi worker
- `cfbd_api_key`, `football_data_api_key`, `odds_api_key`, `anthropic_api_key` — file fallback when settings field is blank.

Every JSON state file above (and the LLM-description and SportsDB thumbnail
caches) is written through `_util.write_json_atomic`: a temp name unique per
writer (pid AND thread) plus an atomic `os.replace`, with the temp removed on
any failure. It is the ONE implementation; do not hand-roll another, because a
shared temp path lets one writer publish another's half-written file.

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
  kickoff minute is kept, #217; a channel a same-minute collision nudged PAST its
  minute's block is kept too, while its number is free, when `numbered_for`
  says it was numbered for its game's current kickoff minute and still carries
  that number, #278, so a game below it leaving no longer moves it (a
  reschedule gets no recorded hold; one whose old number lands inside its NEW
  minute's block keeps it by the #217 in-block hold, as before); with no record for it, the
  #267 chain rule decides: kept only while every number from its computed one
  up to its own is still taken by a channel that stays or a reserved number. A
  looser "within 16 above" rule without the record would also hold a game
  rescheduled one minute earlier, see the DO NOT in `_place_kickoff_numbers`)
  and the list sorts by start time. `compact`:
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

A 2-phase renumber dance keeps every in-group number distinct at every step
(Dispatcharr has NO database constraint on `(channel_group, channel_number)`:
measured against Postgres on 2026-10-11, the only unique constraints on the
channel table are `id` and `uuid`; per-group uniqueness lives only in
`Channel.clean()`, which `.update()` and a plain `save()` never run, so a
duplicate is written silently and the UI then refuses to save either channel):
park existing channels at
`_resolve_park_base` (above both the highest target number this apply will write and the highest number any channel in the group holds, + 1000)
first, then assign target numbers.

In BOTH modes apply collects `_reserved_channel_numbers` (compact: the band;
kickoff: everything at or above the lowest number the slate can take under any
floor, no upper bound, collected BEFORE the floor so the inference sees it, #219):
every number already published by a channel we do NOT own
(effective numbers, so a `ChannelOverride` counts; the recordings-archive
channel counts as somebody else's because it lives in another group). Nothing
in the database refuses a duplicate number, in or across groups, but the M3U, XMLTV and Xtream
Codes outputs all key the guide on the bare channel number, so a shared number
binds the guide arbitrarily for BOTH channels. The numbers of stale channels
this apply will KEEP (an active recording, or kept by their guide;
`_numbers_held_for_recordings`, the reap step's own policy run early) are
reserved too, so a new game cannot take the number such a channel is restored
to after parking. "Stale" is `_unwritten_channels`, the ONE set the reap also
uses: every channel the write loop will not write, including an in-slate game
it skips (no feed, too low for a placeholder). That set is only known after
apply's pre-pass, so NUMBERING RUNS AFTER THE PRE-PASS, over only the games it
decided to write (`prep_by_marker`), in both modes: a skipped game numbered
anyway lost a kept recording channel its number in kickoff mode (#286) and
held a slot a new game needed in a full compact band (#288). The floor
inference sees the same decided games and reservation. `seen_markers` is set
from the plans that got a number, right after. All of it reads
through `_published_numbers` (effective, floored, upper bound `< top + 1` so a
400.5 reserves 400), which `_ensure_archive_channel` also uses so the archive
never takes a number published in ANY group. Within our own group it also
reserves the RAW numbers of channels that are not ours (`Channel.clean()`
compares raw numbers, so a hand-added channel overridden elsewhere would
otherwise share a raw number with a game, which the UI then refuses to save;
integral raw numbers only). A ChannelOverride on one of OUR
game channels is deliberately NOT reserved (it would refuse that channel its own
slot). Reserved numbers are skipped
(kickoff: never kept by either hold, a fresh game nudges +1 past them); if
the whole compact band is taken, apply returns an error rather than publishing
nothing and reaping the group.

**The kickoff floor is remembered, NOT re-derived from the lineup (#219).**
`_resolve_kickoff_base`: explicit `virtual_channel_base` > the floor in
`numbering_state.json` > `_infer_kickoff_base` (the floor the published channels
were numbered from, the floor keeping the most published numbers, so upgrading or
losing the file normally moves nothing; with no record, a channel nudged past
its block over a gap is the exception) > highest
other channel + 1 (fresh install, nothing published). The inference judges each
candidate floor by RUNNING `_place_kickoff_numbers`, the one kickoff placement
`_assign_channel_numbers` also uses, over the games apply will write (the
pre-pass's plans, #288) and every number apply will reserve (collected before the floor for that reason); counting channels inside their block instead let a same-minute
nudge past the block's end pick a floor that moved a channel. Each channel's
candidates include every floor an upward nudge could have reached it from, as
far as the slate and the reserved numbers could push it (`longest_nudge`, #267:
the hold keeps a nudge of any length), with at most `_INFER_RESERVED_RUN` (64)
reserved numbers walked per channel in total, since every number walked is a candidate scored by
two placements that each scan the run. The inference runs the placement with no
`numbered_for` record, which is what apply runs then too: it only infers when no
floor is remembered, and the loader then returns no record either. DO NOT route kickoff mode
through `_resolve_virtual_base` again: with the time term in the millions the
floor buys no separation, and following the lineup renumbered every published
game by the same delta whenever the top of the lineup changed (measured live
2026-09-26, -53 on all seven unchanged games). The reservation above is what
keeps kickoff numbers off the user's channels now. Compact auto still follows
the lineup, because there the base IS the first number. The floor and the
record are saved only AFTER apply's transaction commits: saved earlier, a failed
apply would leave a floor no published channel uses, and the next apply would
renumber every game onto it.

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
| `rivalry` | Known rivalry game. Populated from the bundled `rivalries.json` via `rivalries.py` (see also `honours.py`); `sources/friendlies.py` and `plugin.py` are the consumers. A prefix maps to its own pair list OR to the name of a shared list under `"_pools"` (`rivalries._resolve_entries`); `EPL`, `EFL`, `EFLCUP` and `FACUP` all name `ENGLISH_CLUBS`, so a derby is written once and fires in league and cup alike (#192). Never give a cup its own copy, and never point `CLUBFRIENDLY` at the pool. | 2.0 (flat) |
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

Five steps, using NCAAM (College Basketball) as the worked example.
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
   `_build_sources(settings)` in `plugin.py` through
   `league_enabled(settings, "enable_<sport>")` (never a bare
   `settings.get`: merged leagues only resolve through it). Every toggle must
   be wired in both places or the setting renders but does nothing. A league
   that genuinely pairs with an existing one (the other gender of the same
   competition) can join a four-way select in `merged_settings.LEAGUE_PAIRS`
   instead of adding a field; unrelated leagues must not be paired just to
   save a row.

4. If your feed publishes a per-game id, stamp it into `extra` and add that
   key + a tag to `_MARKER_ID_KEYS` in `plugin.py` (#217) — that id is the
   channel's identity, so a source that skips this silently falls back to the
   teams+kickoff hash and gets a second channel whenever a game is
   rescheduled. Append at the END; the order is part of every live channel's
   tvg_id. `tests/test_marker_identity.py` fails if a source stamps an `*_id`
   key that is neither listed there nor exempted in the test.

5. If your ranks come from a POLL (a top-N list drawn from a larger
   field), declare `rank_field_size` on the source: the number of teams
   the poll picks from (138 FBS programmes for the AP football poll).
   Without it every rank scores against the 25-entry list, so AP #25 reads
   as 0.04 instead of 0.82 (#198). A league source leaves it None: its
   standings table is exhaustive and already is the field.
   `tests/test_scoring.py::TestPollSourcesDeclareTheirField` fails for any
   source class that defines or inherits `_fetch_rankings`, calls a
   `/rankings` endpoint, or emits a non-None `rank_home` / `rank_away`
   without a `standings_table`, and is not listed there.

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
  `_espn.fetch_scoreboard_days` (built on `fetch_days`) — and because the walk is concurrent, a test must assert
  on `max(days)`, not on the last call made.

  The season's end is a CALENDAR date per source, so pick one that ends
  the walk where the thing being simulated ends. NCAA soccer's is the end of
  the regular-season phase on ESPN's core-API season calendar
  (`NcaaSoccerSource._regular_season_cutoff`, #235), not December: since
  2025 ESPN gives most NCAA tournament games the same season slug, type and
  note as regular-season games, so they cannot be filtered by field, and
  walking into the tournament would add its results to the regular-season
  table the win-count bands are scored on. The same cutoff keeps tournament
  games out of `fetch_upcoming`, where they would shadow the College Cup
  rows in `_dedup_series_games`. Only when that calendar is unusable
  (unreachable, short, malformed, or naming no single regular-season phase)
  does it fall back to `_REGULAR_SEASON_END`, a per-gender table keyed by the
  first season each `(month, day)` applies to (the NCAA moved the women's
  bracket a week later from 2026); add a row when a season's rounds move.

Separately, `outcome_eligible_teams()` on `PointsBasedSportSource` exists
because game inclusion and outcome eligibility are different questions. An
FBS-vs-FCS game belongs in the population (the win counts toward the FBS
team's record) while the FCS opponent does NOT belong in CFB's outcome
space: it arrives on a one-game schedule and gets bucketed against
"6+ wins = bowl eligible". Return `None` (the default) unless your sport
has that asymmetry.

### Two daemon threads, not one

`Plugin.__init__` starts BOTH a scheduler (periodic refresh+apply) and a
reaper (removes finished games between refreshes, #196), in uwsgi workers only
(#252, see "The plugin is LOADED in more processes" below). They have SEPARATE
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
thread actually died, so a second `stop()` still finds and joins a thread that
outlived the 5s join (a scheduled pipeline or a reap runs for minutes). But
`__init__` asks `_thread_running`, which counts only a thread that is alive AND
not signaled: the reload endpoint calls `stop()` and then rebuilds the Plugin in
the same worker, and keeping a signaled mid-run thread as "the" scheduler left
that worker with none once it finished (#252; since only uwsgi workers run
threads, a single-worker install then had no scheduler anywhere). The signaled
thread finishes the work in hand and exits without firing another slot, so the
replacement overlaps it harmlessly (each run takes the run lock). Replacement
is BOUNDED (`_may_start`, `_MAX_RETIRING_THREADS = 2` per registry): a signaled
thread that never finishes (a reaper blocked on a row lock holds a DB
connection) must not be replaced on every reload, which would be the #82
connection leak with a new trigger. Past the bound the worker logs a WARNING
and starts no replacement until one exits.

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

### The run lock: every run takes it, and it says who holds it

`_try_acquire_scheduler_lock(kind, task_id)` is the ONE cross-worker mutex for
refresh, apply, reap and every refresh + apply (manual, scheduled, M3U
rematch). Two properties are load-bearing:

- **It fails CLOSED for every caller when Redis is unreachable, refresh
  included** (#199). Refresh used to fail open on the theory that it is
  non-destructive, but it PUBLISHES `cache.json`, and `cache.json` is what apply
  and reap delete by: a reap reads the cache, rewrites it, runs apply (which
  reads it again) and writes its own copy back, so a refresh landing in between
  makes apply delete against a generation the reap never saw and is then erased
  by the reap's write. Redis dropping mid-run is exactly when a destructive
  holder is still running, so "no lock" is not "nobody is writing". DO NOT
  reintroduce a fail-open path (`tests/test_run_lock.py` and the
  `_LOCK_NOT_HELD` check in `tests/test_async_dispatch.py` pin this). Show
  current state says "Redis is unreachable" (`_read_run_lock`, which must not
  go through `tasks._redis()`: that folds a `None` client into "no client" and
  hides the outage) so the refusal is never silent. The guarantee holds only
  once EVERY process runs this code: a process whose plugin module was never
  reloaded keeps the fail-open refresh, so shipping this needs a container
  restart, not just a plugin reload (see the process paragraph below). A
  process that DID reload but kept a running thread is covered: that thread's
  old `_run_under_lock` imports the lock function lazily, gets the new one, and
  calls it as `_try_acquire_scheduler_lock(destructive=...)`. DO NOT drop that
  legacy keyword while an in-place upgrade from a pre-#199 release is
  possible: the TypeError lands after the old thread has claimed the scheduled
  slot, so the slot runs nowhere (`TestOlderCallers` pins it).
- **The lock VALUE is a JSON holder record** (#239): `kind`, `task_id`, `pid`,
  `process` (`uwsgi worker N`, or an allowlisted summary of a Celery/daphne
  process's argv), `thread`, plugin `version`, `acquired_at`, plus a unique
  `token`. `_process_label` copies out of argv only the program name, plain
  script file names (`_SCRIPT_NAME_RE`), the words in `_PROCESS_LABEL_WORDS`
  and a `-Q` queue list (`_QUEUE_LIST_RE`), because the label is copied into
  Redis, logs, toasts and `last_run.json`; do not loosen it to a length or
  looks-like-a-flag filter, `-p hunter2` and `--token=abc` are both short.
  Release compares the WHOLE string, so ownership works as before. A contended
  acquire logs the holder at INFO, take and release are logged at INFO, the
  busy apply toast and the skipped-run note (`skipped_reason` in
  `last_run.json`) name the holder (`_run_lock_unavailable_reason`), and Show
  current state prints a `Run lock: held by ...` line. A skip reason is stamped
  with its skip (`skipped_reason_at`), because older code records a skip
  without touching the reason, and a scheduled run given up without an attempt
  (it was already past its deadline) says so instead of describing a lock
  nobody tried. A value that is not JSON is a holder running code from before
  #239. `apply` and `reap` have `RUN_KIND_LABELS` entries for this description
  only; they never reach `last_run.json`'s `kind`.

**The plugin is LOADED in more processes than the uwsgi workers, and only the
workers run its threads (#252).** Every process whose Django start runs plugin
discovery builds a `Plugin`: Dispatcharr's `celery.py` discovers in
`worker_process_init` (every Celery prefork child and the thread worker), daphne
at Django start, and so does any `django.setup()` exec. Before #252 each of
those started its own scheduler and reaper (measured on Tower 2026-10-10/11:
daphne pid 415 and Celery pool children 422501 and 533697 took part in every
scheduled slot, and 533697 ran the whole 10:00 slot on 2026-10-11), on code the
plugin reload never reaches. Now `Plugin.__init__` asks `_thread_host()`:

- **uwsgi worker** (`import uwsgi` works and `uwsgi.worker_id() > 0`): start
  both. The `uwsgi` module is built into the uwsgi binary; the venv python that
  runs Celery, daphne and `manage.py` has none (measured: pip's uWSGI ships
  only `uwsgidecorators.py`). DO NOT detect by environment: uwsgi exports
  `UWSGI_ORIGINAL_PROC_NAME` and `UWSGI_RELOADS` to its attach-daemons, so
  Celery and daphne carry them and the workers do not (`/proc/<pid>/environ`,
  measured).
- **`celery`, `daphne` (also `python -m celery`), `manage.py <anything but
  runserver>`**: start neither, and log one INFO line saying so.
- **gunicorn, `manage.py runserver`**: start both (other ways to serve the app).
- **Anything else** (uwsgi with worker id 0, an unknown program, a probe script
  calling `django.setup()`): START both and log a WARNING (once per process).
  A wrong "no" means no
  scheduler anywhere; an extra copy is what every process did before, and the
  slot claim makes it harmless. DO NOT invert this default. A probe that must
  stay threadless sets `RANKED_MATCHUPS_PIPELINE_CHILD=1`
  (`tasks.PIPELINE_CHILD_ENV`), which also skips the settings migration.

Settings migration still runs in every process (the pipeline child excepted);
only the threads are gated.

**No scheduler anywhere is made visible** (`_scheduler_staleness`). The slot
claim every firing scheduler makes doubles as a heartbeat: if the most recent
slot that should have fired, allowing `_SCHEDULER_STALE_GRACE_SECONDS` (15 min),
has no claim key, no process ran its scheduler then. Show current state ends
with that WARNING (after the run verdict, because "Last run OK" is true and
still the wrong answer), and `_warn_if_scheduler_stale` logs it once per missed
slot per process from every reaper tick and from `Plugin.__init__` in Celery
and daphne, the long-lived processes that start no threads (Celery children
start throughout the day and their WARNING lines do reach the log, so they
still report it when every uwsgi copy is gone). NOT from `__init__` in a uwsgi
worker: discovery runs there on every plugins-list fetch, and that is no place
for Redis round trips. NOT from a `manage.py` command either: the AIO
entrypoint runs `manage.py migrate` (whose post_migrate discovery builds the
Plugin) before uwsgi starts Redis, so a check there only fails, on every boot.
The check makes ONE Redis connect attempt (`get_client(max_retries=1)`):
Dispatcharr's RedisClient otherwise sleeps 1+2+4+8 s between retries when Redis
is down, on top of the same wait `_read_run_lock` already spent in Show current
state. A Redis watch key (`..:scheduler:watch`, `{sig, since}`)
records when the check started expecting claims for the current schedule
(auto-refresh on/off, parsed times, resolved zone), so a fresh install, a Redis
restart, or turning the schedule on or changing it never counts earlier slots.
The signature is built from RESOLVED values because the reaper reads the raw
stored dict and Show current state the merged one. Claim keys now live 48 h, not
24 h: with one daily time the newest due slot can be 24 h + grace old, and an
expired claim would read as "nobody ran it". Redis unreachable means unknown,
never stale, and so does a run lock held by a SCHEDULED run
(`scheduled_auto_pipeline` in the holder record): the scheduler can be inside
an earlier slot's pipeline when the next one comes due (one worker, close
times), which is busy, not missing. So, after that run ends, is a
`last_run.json` record of a scheduled run that started before the slot and
finished after it (within a minute; a finish in the future never counts): the
busy scheduler picks its next slot from the run's end and never claims the one
it ran through. `last_run.json` holds only the latest run, so a manual run that
finishes before the check overwrites that evidence and the slot reads as
missed until the next one is judged: a false warning in an already rare case
(one worker, times closer together than a run), never a hidden dead scheduler. Any other holder (a manual run, a reap, a
pre-#239 bare token) does not suppress it, or clicking Refresh would hide a
dead scheduler. Known blind spots, by design: the first slot after
the watch (re)starts is never judged (a Redis restart, which Dispatcharr's
non-persistent Redis has on every container restart, delays the warning by one
slot), and a claim proves a scheduler WOKE for the slot, not that the run
succeeded (the run verdict and the skipped-run note cover that). Missing `scheduled_times` falls back to `_DEFAULT_SCHEDULED_TIMES`
(the manifest default, pinned by a test) everywhere; the scheduler used to fall
back to `"0400"` while the modal showed four times.

**Why the Celery copies were silent.** Dispatcharr starts the aio image's
Celery workers without `-l` (`docker/uwsgi.ini` attach-daemons), so the CLI's
`--loglevel` default of WARNING reaches the ROOT logger (Celery 5.6's
`setup_logging_subsystem` sets root to the worker's level when no
`setup_logging` receiver exists, and Dispatcharr connects none). That is the
mechanism the source shows; the measurements below fit it. `worker_log_level` in Dispatcharr's
`celery.py` is not a Celery setting and changes nothing. Loggers under `apps.*`
carry their own INFO level in `settings.LOGGING`, so they show; this plugin's
`plugins.*` loggers inherit root and do not. Measured on Tower 2026-10-11: no
`Memory usage after <task>` line (`dispatcharr.celery`, INFO, root-propagated)
in 48 h of M3U refreshes, no `ranked_matchups ... thread started` line from any
prefork pid, while `apps.plugins.m3u_watchdog` start lines from the SAME pids
appear. The modular image's `entrypoint.celery.sh` passes `-l info`, so there
Celery INFO is visible. With the threads gone from Celery, nothing of this
plugin that matters runs there any more, so the loggers keep their
`plugins.<key>` names (renaming ~30 loggers would change every line Jake and
the skills grep for, for no remaining benefit). The upstream fix is `-l
$(DISPATCHARR_LOG_LEVEL)` on the attach-daemons.

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
empty or raised, or that is dated on a day its source's sweep LOST (#261),
until `_partition_expired` would reap it. Held rows the fetch
still lists are protected from the cap and the bench; held rows it omits are
carried by `_carry_forward`, re-matched (an empty re-match keeps the old match),
and flagged `held_on_air` so the reaper does not backfill a cap slot they never
took. While a held game is on air, a fetched game of the same matchup
(`_series_key`) waits, keeping one channel per series. Apply honours the
same rule when a game's feed vanishes mid-game: a row inside the hold's
window (kicked off, before its reap deadline) that already has a channel is
published as a placeholder whatever its score, instead of being skipped and
its live channel reaped as stale. Not one with an active recording: the
stale path keeps that one for its recording with its stream links (#146),
where a placeholder would strip them mid-recording.

**A lost day is reported, not read as a quiet one (#261).** A per-day sweep
that lost a day still returns the other days, so its label is not empty.
`_espn.fetch_scoreboard_days` (and NHL's own `/schedule/{date}` loop) report
every day they asked for and got nothing for, failed after the blip retry or
never requested because the walk was abandoned, through
`sources.base.report_lost_days`; the refresh opens `recording_lost_days()`
around each source's `fetch_upcoming` (a ContextVar, reported from the
calling thread after the pool joins, a no-op outside a record; the
points-based season walks open their own record in
`PointsBasedSportSource._fetch_games_cached`, #289). The label goes in the summary as `<label>:
partial (...)` (`_SOURCE_PARTIAL_MARK`, which `_slate_problems` keys on),
counts as an outage for `_slate_known`, and `_rows_to_hold` keeps its prior
rows on a lost day (`_on_lost_day` checks the UTC date and the date five
hours earlier, because ESPN buckets by US Eastern date). Only the lost day's
games: a game on a day that answered and no longer lists it is gone. A new
`fetch_upcoming` with its own per-day request loop must call
`report_lost_days` (`tests/test_hold_failed_days.py` checks by AST).

A RANGED request (one call for several days) loses all of its days at once
and reports them with `base.report_lost_day_range(first, last)`: MLB's
statsapi `startDate`/`endDate` schedule, NCAAM's CollegeBasketballData
`/games` `startDateRange`/`endDateRange` (now over `_espn.upcoming_window`,
with `admits` dropping finished and stale games, like MLB), boxing's
`days=` schedule (today-UTC through `days`, an in-band `error` counting as a
failure; read page by page since #284, `BoxingSource._fetch_schedule`, and a
failed page, a full page whose `pagination` cannot say what follows, or more
than `_MAX_PAGES` pages report the whole schedule while keeping the cards
that arrived), and Football-Data's 10-day `dateFrom`/`dateTo` chunks
(`soccer._fetch_tier_fixtures`: each failed chunk's own days, `dateFrom`
through the day before its exclusive `dateTo`, NOT the chunks that
answered; the walk goes on past a failed chunk, so no day is lost
without a request that covered it; the ranges are kept in
`_TIER_FIXTURES_CACHE` and re-reported on every cache hit, because each FD
competition reads the shared result inside its own record). Report only on a
FAILED request: an answer with no games is a quiet day, so test `is None`,
never `not data`. A 200 whose body is not the answer's shape (no `matches`
list from FD, no `dates` list from statsapi, no `gameWeek` list from NHL
api-web, no `data` list from boxing, not a list of poll rows or games from
CBB, no `children` from ESPN MLS standings) is a failure, not an empty
answer: each API's real empty answer carries its list empty (measured
2026-10-11 for FD, statsapi, NHL, and ESPN's scoreboard, whose page without
an `events` list `_espn.fetch_scoreboard_day` turns into None, #282). Read the window
BEFORE the request, so a call failing across UTC midnight reports the days
it was sent for. A failed request that GATES the dated one raises instead,
the outage the refresh holds the label for (#240), as
`NcaaSoccerCupSource`'s `CalendarUnavailable` does: NCAAM's `/rankings`
(`RankingsUnavailable` through `_poll`; None still means no poll yet, see the
rankings paragraph below) and MLS's `/standings` conference map (`_fetch_conference_map` is None on
failure or on a map missing either conference; East and West share the label
"MLS", so one conference's quiet [] hid its days behind the other's games).
The map is fetched ONCE per source, a failure included: the source reads its
season through it when it is BUILT, and a retry that recovered in
`fetch_upcoming` would publish games scored against the empty season. Season-wide schedules that fail
whole (NCAAF `/games?year=`, the MLB postseason) leave their label empty,
which #240 already holds. The behavioural guard
`test_every_built_source_reports_the_days_it_could_not_fetch` reads dates off
the URL AND the `params=` kwarg, expands ranges into their days, fails a day
reported that no request of that source covered as well as a day it asked
for and did not report, and has a second variant failing only DATED requests
so a source gated on an undated one (CBB) is reached.

**A failed rankings poll is an outage, not "no poll" (#283).** Every source
that ranks its games from a feed fetched separately RAISES
`base.RankingsUnavailable` out of `fetch_upcoming` when that feed fails or
answers something that is not a poll: the four ESPN college polls (NCAA
baseball, softball, women's basketball, men's and women's soccer, through
`_espn.fetch_espn_poll`), CFBD and CBBD `/rankings` (NCAAF, NCAAM), and the
Football-Data league table that ranks `SoccerSource` leagues
(`_fetch_standings`, the previous season's seed table included). The refresh
counts the label as an outage and holds its previous games (#240), scored
against the last poll it had, instead of republishing them unranked. An
ANSWERED poll with nothing in it (preseason: ESPN `{"rankings": []}`, CFBD and
CBBD `[]`, an empty FD table) stays quiet. The ESPN polls and the FD table are
read only once there is a game to rank (`base.attach_poll_ranks`; FD after
`_fetch_fixtures`): ESPN's college baseball and softball `/rankings` answer
HTTP 404 all offseason (measured 2026-10-11, when neither scoreboard lists a
game from the end of June to February), so asking with nothing to rank would
report an outage every offseason refresh. NCAAM reads its poll ONCE per
source (`_poll`, a failure kept too), because its importance-side season walk
picks the simulated population from the same poll: two reads could rank the
slate against one poll and simulate a population of none. Class guard:
`tests/test_rankings_failures.py::test_every_built_source_that_reads_a_rank_feed_raises_when_it_fails`
drives every built source with one game tomorrow and every rank feed failing.

**The hold's previous slate survives a lost cache.json (#264).** The hold
reads the PREVIOUS slate, and Dispatcharr's plugin import replaces the plugin
folder (cache.json with it) on every upgrade. So apply stamps the cache row
each channel was published from on its LIVE ProgramData entry
(`custom_properties["ranked_matchups_row"]`, `_published_row_stamp`; the
XMLTV writer reads only its own keys from that dict), and when the cache is
not vouched `_prior_games` adds the rows `_published_rows` reads back
(`_published_guide`: only entries whose ProgramData.tvg_id is the channel's
own marker, and a stamp only when its row builds that marker). A vouched
cache is used alone, as before. A DB error there fails the refresh on
purpose. For channels with no usable stamp (published before the stamp
existed: the first refresh after upgrading to it) apply has a fallback: on a
slate it cannot vouch for, a stale channel is kept by its own guide
(`_kept_by_guide`: a current Live or Past entry for any channel, any entry
not yet ended for an UNSTAMPED one), like one kept for a recording
(`keep_ids` in `_partition_stale_for_recordings`, number reserved and
restored), and the slate stays unvouched until it goes. A dry run counts
`stale_deleted` with the same keep policy (recordings and `guide_kept_ids`,
leaving out recordings auto-record would cancel), so a rehearsal never
reports deleting a channel a real apply keeps. The set
(`guide_kept_ids`) is read BEFORE apply's group-rename cleanup, which leaves
a kept channel's old guide entries for a later apply. `_published_rows`
counts the channels that fallback would keep, and `_slate_known` vouches an
outage-free result only when that count is 0 (a healthy ncaaf.py fetch still
omits a game that has kicked off, and a vouched slate turns the fallback
off). DO NOT keep a STAMPED channel that is not on air: the refresh had its
row and dropped it on purpose, and keeping it would freeze cap-dropped games
until they end, with the unvouched state.

**Reaping delegates deletion to `_action_apply` on purpose.** Apply already
reaps owned channels missing from the cache, with DVR-recording protection,
the archive re-home, ChannelStream/EPGData cleanup, and signal-safe queryset
deletes. Do NOT add a second deletion path; it would be a copy that has to
stay in agreement with that one.

**An empty `games` list can be a slate, and apply must publish it (#241).**
Reaping the last game, or a refresh that finds nothing to air, leaves
`games == []`, and the stale reap, orphan-EPG sweep, recording stops file and
auto-record state all depend on apply running on it. But publishing an empty
slate deletes every channel the plugin owns, and the only defence against
doing that during a source outage is `_rows_to_hold` carrying the previous
games forward, which needs the previous cache. So an empty list counts only
when its writer set `empty_slate_known: true` (`_cache_has_slate`): a refresh
does when the previous cache was vouched or every source answered with games
(`_slate_known`), the reaper carries the answer of the slate it empties, and
apply sets it once it has published a slate for real (`_vouch_published_slate`):
its stale reap leaves nothing on air that the slate omits. DO NOT drop that
last writer. A real install always has a source answering with no games (a
tournament out of season), so "every source answered" never holds there, and
without apply's stamp the doubt from a lost cache.json (Dispatcharr's plugin
import replaces the folder on every upgrade) would never clear: every later
refresh and reap would inherit it and the last game's channel would stay.
"Vouched" (`_cache_vouched`) is stricter than "publishable": a non-empty
slate written `false` is still published, but its doubt travels to whatever
is built on it until an apply publishes it (a dry run, or one that kept a
stale channel for its recording, does not vouch), or an empty slate on top
would delete the channel it omitted. Refused: a missing, unreadable or non-object file, `games` not a list, a
pre-#241 empty cache, and an empty refresh after cache.json was lost (the
plugin folder is replaced on upgrade) with a source failing. Apply's toast
says which.

## Known gotchas / lessons learned

- **A game's end time is ESTIMATED, and there is exactly one estimate.**
  No feed publishes one. `_game_end_utc` reuses `_epg_match_window`'s
  per-game post-game hours (~4h gridiron, 24h boxing; soccer 2.5h for a league
  or group game and 3h for a tie that can go to extra time and penalties, which
  ends ~2h45m after kickoff, #257). DO NOT add
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
- **A soccer game's window is chosen from its ROW, not only its prefix** (#257).
  `_is_postseason_extra`, the same rule the favorites-only gate uses, reads a
  postseason `extra["stage"]` (`_NON_POSTSEASON_STAGES`: WC / EURO / UCL
  rounds, MLS Cup, the College Cup) or, for NWSL and Liga MX, which carry only
  an ESPN `season_slug`, any slug outside `_NON_POSTSEASON_SEASON_SLUGS` (so
  their playoff games also pass the 'postseason' favorites gate). WC, EURO, UCL,
  MLS, NWSL and Liga MX are mostly league or group games, so they must stay
  per-game. The English cups need nothing extra: every row carries a cup round,
  and an unmapped round is skipped, not emitted stage-less.
  `_SOCCER_EVERY_GAME_LONG_WINDOW` gives every game the 3h window where the row
  cannot tell: NCAAMSOC / NCAAWSOC, whose regular-season source also carries
  the conference tournaments (two overtimes and penalties) with nothing on the
  ESPN event to mark them. Friendlies never play extra time and keep 2.5h. Every soccer
  prefix a source emits must be in `_SOCCER_PREFIXES`: `tests/
  test_mapped_prefixes.py` classifies each built source by the soccer API it
  fetches (an ESPN `/sports/soccer` or football-data.org URL in its code) and
  fails on a soccer prefix missing from the set or a non-soccer one in it.
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
  names differing) would otherwise create two channels on one number, both
  published, with the guide bound arbitrarily between them (no database
  constraint refuses it); `_rekey_legacy_virtuals` runs
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
  would read as vanished (since #291 the plan's CANDIDATES are only the games
  apply will write a channel for, but its slate stays the whole cache: a
  cached game it skips, or one left without a compact slot, is known and
  unplanned, so its scheduled recording is cancelled and planned again once
  it has a channel; treated as missing instead, a recording was judged by its
  stored reason against a cache that had moved on, and sat outside the slot
  budget. No exception for a channel the guide keeps while the cache cannot
  vouch (#264): a recorded team's game always gets a channel (placeholder),
  and a ranked game with no stream attached is never eligible, so a skipped
  guide-kept game's scheduled recording is cancelled too (an exception judged
  by the stored reason was tried and kept captures through corrected rows and
  excluded streams). And every scheduled recording
  the reason rule keeps for a game missing from the slate counts as `busy` in
  the plan, so a one-slot plan never schedules a second capture beside it); the team question is re-asked through
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
  `select_for_update()` inside a transaction (a plain re-read only narrows the
  race) and re-check `is_in_progress` / `is_terminal`. `_autorecord_cancel`
  runs through `_autorecord_cancel_committed`, a transaction of its OWN, in
  two steps. The plan is built after the pre-pass's deciding half over the
  games apply will write (#291: a channel-less game planned anyway took the
  only slot from one with a channel, and nothing recorded). Step one, BEFORE
  numbering, cancels only recordings on channels apply will not write (#288
  review: the numbers held back for kept channels are read after it, so a
  recording it cancels holds no number; in a full compact band one once made
  apply abort with the unwanted recording still scheduled). Numbering may
  leave a compact game without a slot; the plan is then built again over the
  placed games, and step two cancels the rest by it, before the network half
  writes the 🔴 into names. Each step's state change (`created` entries
  dropped) is saved right after it commits: left to `_autorecord_finish`, an
  apply that returned or failed in between kept the entries, and the next
  plan read each missing row as deleted by the user and tombstoned it for
  good.
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
- **A failed poll is an ERROR in the summary, not "0 games" (#283).** It used
  to be: `_fetch_rankings` returned `None` for a failed request too, NCAAF
  returned `[]` and logged "offseason (?)" regardless of cause, which cost a
  day of wrong diagnosis on 2026-08-28. A failed or malformed poll now raises
  `RankingsUnavailable` (`<label>: error` in the summary, previous games
  held), and `None` / `{}` means only an answered poll with nothing in it. A
  "0 games" from a poll-driven source today is a real empty slate or a
  preseason with no poll.
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
  The same holds for the guide (#263): our EPGData rows on the old group's
  EPGSource are MOVED onto the new source (`_move_owned_epg_rows`, same row
  id), and the old source is deleted only once it is empty. DO NOT go back to
  `old_src.delete()`: its CASCADE took the ProgramData of a channel kept for
  an active recording, and a kept channel never passes through the write loop,
  so its guide stayed blank for the rest of the recording. A moved row is
  reused by the write loop for a listed game and reaped with its channel once
  the recording ends. On a marker clash (`unique_together (tvg_id,
  epg_source)`, renamed away and back) the new source's row keeps its id and
  every FK to the old row is repointed to it, found with
  `_meta.get_fields(include_hidden=True)` because `ChannelOverride.epg_data`
  is `related_name="+"` and `_meta.related_objects` silently omits hidden
  relations. The programmes kept are the ones one of OUR channels shows, so a
  second stale old source processed after the live one cannot overwrite the
  recording's guide. A source with a `url`, a `file_path` or the Schedules
  Direct type (which fetches with credentials and leaves both blank) is never
  ours, whatever markers its rows carry (`_never_fetches_q`): it is never swept as
  an old source, and apply refuses, before any write, a group name that is
  also the name of such a source, since step 2 would otherwise adopt it.
  `tests/test_apply_group_rename.py` drives all of it through the real apply.
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
- **`excluded_groups` must drop streams ABOVE the pre-pass's
  `prep_by_marker[marker] = ...` entry** (#206; since #288 a game is seen
  exactly when it has a plan and a number). Every `continue` in the pre-pass
  has to stay above that line: a game given a plan and then skipped is marked
  seen, and its existing channel, neither published nor reaped, commits at
  phase 0's temporary parking number (measured
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
  per-writer temp name in `_write_last_run` exists because a skip note is
  written by a caller that did NOT get the lock, so it can race the holder's
  verdict or another worker's skip; and `_run_health_lines`
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
  premium. The points-based leagues report the same way (#289,
  `PointsBasedSportSource.season_fetch_failure`), with one difference that is
  deliberate: their failure is NOT retried in the refresh. Most of their
  seasons are walks (an ESPN day walk of up to ~300 requests with its blips
  already re-asked inside it, or NHL's 32 clubs), and a re-walk per league
  multiplies an outage against the pipeline ceiling; MLB and NCAAF are one
  request and NCAAM three, kept to the same rule so the family has one.
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
- **The simulator's TARGET is the row `fetch_upcoming` built, not the one the
  bracket emits, and a bracket needs more on it than a source supplies**
  (#243). Nothing fails when it is missing; the importance is just wrong:
  - no `extra["stage"]` (the NFL/NBA/NHL/MLB/WNBA/NCAAW playoff rows): the
    sampled result is dropped, live MLB LCS ~0.15 importance instead of ~3,
    and `_is_postseason_game` reads the row as regular season;
  - a `matchday` that is not the leg number: Football-Data gives a UCL final
    0 or None, 2026 World Cup knockouts None, EURO 2024 knockouts 4-7, and
    anything but 1 was filed as leg 2, so the tie never completed (0.0);
  - no `is_decisive_leg`: a final or second leg sampled with no extra time
    or penalties, so it can end level.

  `BracketSportSource.place_upcoming` fixes the row. It matches each row to
  its bracket game by `game_id` (else `fd_id`), exactly, and only when the
  team pair agrees and the id is unique. It then applies `_sim_extra`, the ONE
  builder of a simulated row's extra, shared with
  `_emit_remaining_games_for_tie`: the bracket overrides the simulator's keys
  (a fetched stage can be None or stale, a fetched double-elim `grouping_key`
  can name the wrong sub-bracket), the source keeps its pass-through keys and
  its `matchday` (the EPG text shows it), and the bracket's game number goes
  in `bracket_game_index`, which `apply_result` prefers.

  The refresh calls it twice through `_place_in_bracket`: at fetch, for the
  favorites filter, on a throwaway bracket (reading `initial_state()` there
  would cache an empty bracket if the season fetch failed that early and
  defeat scoring's retry), and again after `_soccer_allow_scoring_retry()`
  with `for_scoring=True`, on `initial_state()`'s bracket, the one importance
  uses, so the two never disagree. Both passes warn, naming which. For the
  same reason a `_fetch_bracket_games` must NOT cache a fetch in which any
  request failed, as `[]` (MLB and NHL did) or as a partial ESPN day sweep
  (a missing day is a wrong series score, #245). `BracketSportSource` owns
  the accounting, the soccer season-fetch rule applied to brackets:
  `_sweep_bracket_days` (stops at the first failed day, keeps the days that
  answered for the retry), `_bracket_fetch_failed` / `_cache_bracket_games`
  (the only writer of the cache), `BRACKET_FETCH_MAX_ATTEMPTS` per refresh,
  `allow_bracket_scoring_retry()` called before the scoring placement, and
  `bracket_fetch_failure()` reported as "<label> bracket (stakes not
  simulated)" in the summary, one entry per `sport_label`. Given up, the
  fetch returns `[]`, not the partial sweep. NHL's per-series schedule can
  404 for a series not yet published, which `_http_get` cannot tell from a
  503, so a 0-0 series is accepted as empty only after failing on an earlier
  fetch too. Playoff-only shapes (`ALL_ROWS_POSTSEASON`) flag every row
  `postseason` first, so a failed bracket fetch cannot demote them.

  A new bracket source needs an id on its upcoming rows equal to its bracket
  games' (`tests/test_match_identity_contract.py` checks `game_id`, including
  `extra = {...}` built as a variable). Tests that take the target from
  `remaining_matches` cannot see any of this: build it the way
  `fetch_upcoming` does.
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

Smoke-test refresh (the `-e` keeps this exec from starting a scheduler and
reaper of its own: `python -c` is a process `_thread_host` cannot place, so it
would otherwise start both, with a WARNING):

```bash
docker exec -e RANKED_MATCHUPS_PIPELINE_CHILD=1 dispatcharr python -c "
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

The third argument overrides saved settings key by key (`Plugin.run` merges
`params` over `context["settings"]`). Name a merged setting by its NEW id
(`'tennis': 'atp'`, `'ncaaf': 'fbs'`, `'scoring_weights': 'favorite=8'`). An
old id such as `'enable_atp'` or `'weight_favorite'` is ignored once the
install is migrated (`settings_schema: 2`), so the override does nothing and
says nothing (#208).

Inspect what the cache contains:

```bash
docker exec dispatcharr cat /data/plugins/dispatcharr_ranked_matchups/cache.json | head -80
```

Run apply and reap against a real ORM, with no container: `tests/orm_harness`
holds field-for-field copies of the Dispatcharr models apply touches and a
runner that drives the real `plugin.py` over in-memory sqlite, in a subprocess
so Django never loads into the rest of the suite. `tests/test_apply_empty_slate.py`
is the worked example: write `scenario_<name>(env)` functions and the module's
driver runs each against a fresh database. A new harness-driven file reuses its
helpers and driver (`collect_scenarios`, `run_harness`) by loading it by path,
as `tests/test_apply_group_rename.py` does. Every file the plugin writes beside
its code must be listed in `_REDIRECTED_PATHS` in `run_scenarios.py` (and in
`.gitignore`); a test scans for public module-level `*_PATH` `.json` constants
and fails on one that is missing from either. `numbering_state.json` was
missed: a clean checkout failed every scenario with "harness wrote into the
repo", and a dirty one fed the file's kickoff floor to every scenario. Prefer
it to a MagicMock queryset for any claim about which rows apply leaves behind.

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
  (`_ranked_eligible`; a favorite is preferred, never eligible; in apply
  "stream-matched" means the pre-pass attached a stream to its channel, not
  that the cache names a match, #291 review). Slots still
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
  gives up is reported as a failed source, "stakes not simulated". Playoff
  bracket fetches follow the same rule (#245): a fetch with any failed request
  is never cached, is retried (once guaranteed to scoring), and a bracket that
  gives up is reported as "<label> bracket (stakes not simulated)"
- Feeds that arrive between refreshes are picked up: when an active M3U account
  refreshes after our matches were taken and a curated game is live or starts
  within 6 hours, the reaper loop runs Refresh + apply once
  (`_maybe_rematch_after_m3u`, gated on auto-refresh, one run per cooldown
  across all workers). Show current state names it "Refresh + apply after an
  M3U update"; Diagnose explains a late feed instead of calling it a bug
- A refresh cannot take a live game off air (#240): a game that has kicked off,
  or one whose source returned nothing this fetch, or one dated on a day its
  source's per-day sweep lost (#261), is held on the applied list (outside the
  `max_games` cap) and re-matched against the current feeds until the reaper
  would remove it anyway. It holds after cache.json is lost too (#264): apply
  stamps each published row on its live guide entry and the refresh reads
  those back
- A scheduled time is no longer lost when something else holds the run lock at
  that second: one worker claims the slot and retries the lock up to 30 minutes
  (never into the next slot), and stands down without running if a refresh +
  apply that started after the slot finishes OK while it waits. Scheduler clock
  maths is DST-safe, and the
  pipeline subprocess starts no scheduler or reaper of its own
- Only the uwsgi workers run the scheduler and reaper (#252): Celery, daphne
  and `manage.py` processes load the plugin but start neither, and a process
  that cannot be placed starts both with a WARNING rather than risk none
  running. When no scheduler claimed the latest due slot, Show current state
  ends with a WARNING and the log says so once per missed slot
- Every run takes the cross-worker run lock and refuses to start without it,
  a plain refresh included (#199), and the lock names its holder (#239) in the
  log, the busy toast, the skipped-run note and Show current state
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
