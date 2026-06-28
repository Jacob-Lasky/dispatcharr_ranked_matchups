# Changelog

All notable changes to this plugin are documented here. Format roughly
follows [Keep a Changelog](https://keepachangelog.com/) with semver.

## [Unreleased]

## [1.10.0] - 2026-06-28

### Added

- **DVR recording preservation (#146).** Apply no longer lets the stale-channel
  reap take your recordings with it. `Recording.channel` is `on_delete=CASCADE`,
  so deleting a past game's virtual channel used to CASCADE-delete any DVR
  recording made on it (the file was orphaned on disk and vanished from the DVR
  tab). Apply now: skips reaping a channel whose recording is still active
  (in progress or ending in the future); re-homes completed recordings onto a
  persistent archive channel before reaping; and keeps the channel rather than
  destroying recordings if it can't preserve them.
- New **Recordings group name** setting (`recordings_group_name`, default
  `Matchups Recordings`). The archive group is created lazily only when a
  recording needs preserving and removed again once empty, so it exists only
  while it holds recordings. Must differ from the live target group; if they
  match, preservation is disabled and channels with recordings are kept.

## [1.9.0] - 2026-06-22

### Added

- **Favorites-only curation (#144).** New "Favorites only" setting with three
  modes: **Off** (default, curate every enabled sport); **Favorites only** (keep
  only games involving a Favorites-list team across all sports, e.g. USMNT-only
  World Cup); and **Favorites only, postseason shown** (favorites everywhere,
  plus any playoff/knockout game regardless of favorite, while regular-season and
  World Cup / EURO group-stage games stay favorites-gated). No-ops with a warning
  when no Favorites are configured.

### Fixed

- Corrected the World Cup / EURO source help text, which claimed "pure knockout"
  while group-stage matches are produced.

## [1.8.0] - 2026-06-16

### Added

- **Stream-name matching (Path C).** The matcher now also keys on the names of
  individual STREAMS, not just channel names and EPG programme titles. Providers
  spin up dedicated per-match feeds whose matchup lives in the stream name
  ("USA Soccer10: ... Iran vs New Zealand") on a generically-named channel with
  no EPG; Path A (EPG title) and Path B (channel name) both miss those. A stream
  whose name names both teams (or, for field events, the event) is now attached
  **stream-granular**: only that one stream lands on the matchup channel, not its
  parent channel's unrelated streams. Match results carry a new `stream_ids`
  alongside `channel_ids`, threaded through the cache and apply.
- A **feed-prefix guard** for stream-name matching: both teams must co-occur in a
  single `:`/`|`-delimited segment of the name. Without it the network label
  "USA Soccer09" supplied a bogus "USA" hit for United States while the real
  opponent appeared in a different matchup ("Australia vs Turkey"),
  cross-matching games. Kickoff times ("Iran 02:00 New Zealand") are not treated
  as segment boundaries.

### Fixed

- **Tier-1 no longer drops EPG-confirmed broadcasters when a dedicated feed
  exists.** When a channel name named both teams (Tier-1), the matcher used to
  return only those channels and discard every broadcaster whose EPG programme
  title named the game (FOX/TSN/BBC). It now MERGES the program-title both-team
  matches behind the channel-name matches as fallback streams. Both sets are
  gated on both teams, so the merge is high-precision and needs no LLM call.

## [1.7.2] - 2026-06-14

### Fixed

- **Scheduler no longer leaks a Postgres connection, which could lock up the
  whole container (#82 / #136).** The background scheduler thread reads settings
  from the DB each tick but never closed its connection, so a parked scheduler
  pinned one Postgres backend open; and because Dispatcharr re-instantiates the
  plugin on every discovery (opening the Plugins page, running an action, saving
  settings, reloading), each re-instantiation churned a new scheduler thread and
  orphaned the previous one's connection. Connections accumulated until Postgres
  `max_connections` was hit and every request (including login) blocked, which
  presented as the server locking up. This was independent of channel count, so
  it hit small installs too. The scheduler now closes its DB connection before
  every sleep and on exit, and `Plugin.__init__` is idempotent: a healthy
  scheduler thread is left running instead of being restarted on every
  discovery.

## [1.7.1] - 2026-06-14

### Fixed

- **Apply no longer holds a DB transaction open across network I/O (#136).** The
  apply step wrapped its per-game writes in a single `transaction.atomic()` block
  and, inside it, made a Claude LLM-description call and a SportsDB logo lookup
  per game. On a large channel lineup (or whenever new, uncached games appear,
  e.g. right after enabling a sport) that held one Postgres transaction open
  across dozens of sequential network calls, then committed all at once, which
  starved the login/token worker and could make the server's login time out. All
  network-backed values are now resolved in a pre-pass BEFORE the transaction;
  the transaction does only fast in-memory to DB writes. The park step also uses
  a single `bulk_update` instead of one save per existing virtual channel. No
  change to apply output (channel names, EPG, logos are identical).

## [1.7.0] - 2026-06-13

### Fixed

- **Field-event sports (UFC, F1, golf, NASCAR, ATP/WTA) now match channels
  (#127).** These single-event sports have no opponent, so their source emits an
  away-side `"Field"` sentinel. The matcher's both-teams gate fed that sentinel
  into its keyword logic, and since no channel or EPG title ever contains the
  word "Field" the gate could never be satisfied: every field-event game fell
  through to a placeholder with no streams. The matcher and the EPG candidate
  lookup now detect the single-event shape and match on the event name alone,
  dropping the away-side requirement. Two-team sports are unchanged.

### Changed

- **"Diagnose matching" now diagnoses field events too.** Previously it skipped
  them as unmatchable (#127); now that they match, an unmatched field event is an
  ordinary diagnosable target, with the window scan keyed on the event name
  instead of a head-to-head separator.

### Internal

- The `"Field"` away sentinel and field-event detection are consolidated into a
  single `_util.is_field_event()` / `FIELD_AWAY_SENTINEL`, replacing three
  independent copies (`sources/field_event.py`, `logos.py`, an inline literal in
  `plugin.py`).

## [1.6.0] - 2026-06-13

### Added

- **"Diagnose matching" now logs a verbose report.** Alongside the short toast,
  the action writes the full detail (every matchup listing in the game's window,
  all unmatched games, and the matched set) to the container logs, so a user can
  paste the toast AND hand over the logs (`docker logs Dispatcharr`, grep
  "diagnose (verbose)") for deeper troubleshooting.

### Changed

- **"Diagnose matching" output trimmed to fit the result toast.** The UI shows
  an action result as a single bottom-anchored notification that clips long
  messages, so the toast is now at most 3 short lines (game, one naming listing
  if any, a one-line verdict). The full detail moved to the verbose log above.

## [1.5.0] - 2026-06-13

### Added

- **"Diagnose matching" action (#128).** A copy-pasteable troubleshooting
  report explaining, per curated game, why it did or did not match a channel:
  the exact team keywords searched, which of your channels named one team /
  both / neither in the time window, and why a near-miss was skipped (preview
  card, only one team, or an ambiguous match the LLM tie-break did not
  resolve). Field-event sports get a plain known-limitation note (#127). Built
  for users who cannot read container logs; read-only, no DB writes.

### Changed

- Internal: extracted `matcher._kw_hit` as the single substring-hit test
  shared by every matcher tier and the diagnostic (removes 4x duplication).
- Internal: `run()` dispatches through a single `_ACTION_HANDLERS` table; a
  contract test asserts the manifest's action ids match the table exactly, so
  a button with no handler (or a handler with no button) fails CI.

## [1.4.0] - 2026-06-13

### Changed

- **Stable, kickoff-time channel numbers (#121, supersedes #119).** Virtual
  channel numbers are a pure function of each game's start time:
  `virtual_base + minutes-since-a-fixed-origin × slots + a small per-game hash
  tiebreak`, as an **integer**. The list therefore sorts strictly by day then
  start time (live/upcoming first, no ★-score ordering), and every game keeps
  the same number for its whole life — finished games drop off and new games
  slot into their time-position without any existing number moving. Because the
  number is stable, the guide binds to the right game with no client setup in
  **both** the default M3U/EPG output and the **Xtream Codes API** (both bind by
  the integer channel number), fixing the #117 name↔guide mismatch at the source.
  Replaces #119's day-offset-plus-hash-fraction scheme, whose *fractional*
  numbers were floored and collision-bumped by the Xtream Codes layer (XC
  requires integer channel numbers), scrambling the order. The "set TVG-ID
  Source = TVG-ID" requirement remains removed. Numbers are large (time-encoded)
  by design; that is the cost of stable, integer, chronological numbering.

### Added

- **Tuning recipes in SCORING.md** plus an in-settings link to them. The
  scoring doc now leads with a goal-first cookbook ("I want X, so I change
  Y") covering favorites-only, nail-biters, marquee-names, fewer cup games,
  importance balance, adaptive scoring, and coverage, alongside the exact
  preset bundles and a knob reference. The "Interestingness Weights" settings
  section now links straight to the recipes so users tuning weights can find
  worked examples without leaving the plugin. A new `TestScoringDocMatchesCode`
  pins the documented preset and default-weight tables to `_CURATION_PRESETS`
  and `scoring.Weights`, so the doc fails its test suite instead of silently
  drifting when a number changes in code.
- **Customizable channel-name template** (#100). A new "Channel Naming"
  setting (`name_template`) lets you reshape the channel name with
  Sonarr/Radarr-style variables: plain text is literal, a `{group}` holds one
  variable plus any glued literal characters, and the whole group collapses
  when the variable is blank. Variables include `{league_short}`,
  `{away_team}`, `{home_team}`, `{rank_away}`, `{rank_home}`, `{rank_pair}`,
  `{score}`, `{favorite_star}`, `{tagline}`, `{tournament}`, `{venue}`,
  `{game_date}`, `{start_time}`, `{kickoff}`, and `{rivalry}`. Leave blank for
  the default. Implemented in the new `naming.py`.
- **"Test naming convention" action** (`preview_names`, #100). Renders the
  current template against sample games with no DB writes, reports template
  errors, and lists every variable, so a template can be checked before it
  reaches live channels.

### Changed

- **Channel names now show poll ranks inline** after each team, e.g.
  `Ohio State (5) at Penn State (1)`, replacing the compact `NvN` head prefix
  that could not say which team held which rank (#99). Ranks only render for
  poll-ranked leagues (AP / Coaches Top 25); standings-position leagues are
  unaffected.
- **Tournament / bracket taglines are humanized** (#98): `omaha_bound` now
  reads "Road to Omaha", `round_of_32` reads "Round of 32", `elite_8` reads
  "Elite Eight", and so on, with the incorrect "race" suffix dropped for
  bracket and championship-event bands. Season-long standings bands keep their
  "race" framing (title race, relegation race).
- **Logo fallback now prefers a league/tournament badge** over the provider
  channel logo (#102). When no team-vs-team matchup thumbnail exists, the
  channel gets the league's badge from TheSportsDB (tournament badge first when
  a competition id is mapped, then the sport/league badge), keyed by
  `sport_prefix` and cached once per league. The source-channel logo is now the
  last resort. Unmapped sports still fall back to the channel logo.

## [1.0.0] — 2026-05-26

First stable release. The plugin has been running daily in production
for several months; this release marks the point where the public
contract (settings, action surface, channel naming, EPG description
shape) is stable enough to commit to semver promises.

### What this plugin does

Curates the most interesting upcoming sports games from across
20+ sports / leagues into a single "Top Matchups" channel group in
your Dispatcharr guide. Every channel description shows the *why* —
ranks, closeness, rivalry, favorite team, tournament stakes, race
implications — so you can pick what to watch without scrolling
through your full guide.

### Sports supported

**Americas** — NFL, NHL, MLB, NBA, MLS, NWSL, Liga MX, NCAA Football,
NCAA Men's Basketball, NCAA Women's Basketball (with March Madness),
NCAA Baseball (regular + postseason), NCAA Men's & Women's Soccer
(regular + College Cup), NCAA Softball.

**European soccer** — English Premier League, EFL Championship, UEFA
Champions League, Bundesliga, La Liga, Serie A, Ligue 1, Eredivisie,
Primeira Liga, Brazilian Série A.

**International tournaments** — FIFA World Cup 2026 (with full Annex
C 495-row 3rd-placer slot table for accurate bracket leverage), UEFA
European Championship.

### Scoring signals (each tunable on the settings page)

- **Rank pair** — both teams in their sport's top-25 poll, or one ranked
- **Close game** — bookmaker-implied coin-flip-ness (devigged moneylines
  in soccer, normalized point spread in NCAAF / NCAAM)
- **Favorite team alert** — flag your teams; their games auto-rank
  higher and the EPG description calls them out
- **Importance** — Monte Carlo simulation of how much each game moves
  each team's chance of advancing / winning the title / making
  playoffs / getting relegated. Locked games score lower; do-or-die
  games rank to the top
- **Tournament stage** — knockout cup games (R16, QF, SF, F) rank
  higher than regular season
- **Rivalry** — known rivalry games (initial DB ships with CFB / EPL
  / NHL / NBA pairings)

Raw signals sum and compress to a 0-10 ★ score using a tanh curve so
top games asymptote without losing differentiation among the rest.

### Curated channels

Virtual channels live in a configurable target group (default
"Top Matchups") with names like:

```
CFB 1v5 ★10.0: Texas at Oklahoma — both top-5, rivalry, toss-up
EPL 3v9 ⭐ ★8.4: Brentford at Manchester United (favorite: Brentford)
NHL Stanley Cup F ★9.7: Game 5 Avalanche at Golden Knights
```

Each channel's EPG description shows kickoff time, the matchup,
the sport, the raw score, the score breakdown, and (when enabled) an
LLM-rewritten narrative with rivalry / stakes / form context.

Today's games are auto-sorted to the front (lowest channel numbers)
so they appear first in any IPTV client (TiviMate, Plex, Jellyfin,
the Dispatcharr UI itself, etc).

### Behavior

- **Daily auto-refresh** runs at the time(s) you choose (default
  `0400` local). On-demand "Refresh + apply now" button returns
  within ~100ms; progress shows under "Show current state" while
  the pipeline works in the background.
- **Dry-run mode** previews channel-profile changes before applying.
- **EPG channel matching** finds the actual broadcast channel airing
  each game (across however many IPTV providers you have) and pulls
  its stream into the virtual matchup channel.
- **AI-written descriptions** (optional, Claude-powered) — rivalry
  framing, recent form, what's at stake. Off by default.
- **SportsDB matchup logos** when available.
- Saved state caches across refreshes so iterations are fast and
  survive Dispatcharr restarts.

### API keys

Most data sources offer free tiers and the plugin's fetch volume stays
inside them comfortably. EPG matching uses Claude and is the only
paid requirement.

| Source | Tier | Required for |
|---|---|---|
| Anthropic (Claude) | Paid | EPG channel matching (required), narrative descriptions (optional) |
| CollegeFootballData | Free 1k req/day | NCAA football + men's basketball |
| Football-Data.org | Free 10 req/min, 12 free comps | EPL / EFL / UCL / Bundesliga / La Liga / Serie A / Ligue 1 / WC / EURO |
| The Odds API | Free 500 req/mo | Spread / closeness on any sport |
| SportsDB | Free key `3` works | Matchup logos (optional) |
| ESPN / NHL / MLB | No key required | NHL, MLB, NBA, MLS, NWSL, Liga MX, NCAA Baseball, NCAA Soccer, NCAA Softball, NCAA Women's Basketball |

### Compatibility

- **Dispatcharr** v0.25.1+ (tested; older versions may work but aren't tested)
- **Platforms** linux / docker
- **Python** 3.13+ (matches Dispatcharr's bundled interpreter)

### Known limitations

Two upstream-blocked gaps tracked as GitHub issues:

- **MLS mid-season importance** — ESPN publishes only ~1-2 weeks of
  future MLS fixtures (other leagues publish months ahead), so the
  Monte Carlo importance signal reads close to 0 for marginal
  mid-season MLS games. Signal sharpens as the season-end window
  narrows.
- **UEFA EURO 2028 bracket leverage** — the cross-source bracket
  wiring that makes WC 2026 group games show R16+ leverage is WC-
  specific; EURO 2028 needs analogous wiring once UEFA publishes the
  bracket structure (~12-18 months pre-tournament).

## [0.1.0] — 2026-04-27

Initial release. Phases 1-4 of the design shipped together:

- **Sport-agnostic scaffold**: per-sport adapters in `sources/`, transparent
  scoring with per-signal breakdown, EPG-to-channel matcher.
- **NCAAF adapter** (`sources/ncaaf.py`) — CFBD API: AP Top-25, weekly games,
  betting lines.
- **EPL / EFL Championship / UCL adapter** (`sources/soccer.py`) — Football-Data.org
  fixtures + standings, The Odds API for spreads.
- **Scoring signals**: rank pair, favorites (with team-qualifier whitelist for
  soccer suffixes), close-game spread, **stakes** (proximity to league
  thresholds — title / playoff / relegation — with late-season multiplier),
  **tournament stage** (knockout cup games), **impact-on-favorite** (non-favorite
  game that shifts a favorite's table position), narrative (LLM, off by default).
- **0-10 score** with smooth tanh compression so top games asymptote without
  losing differentiation in the typical 4-12 raw range.
- **Today-first sort + channel renumbering**: today's games occupy the lowest
  channel numbers (9000+) so they appear first in TiviMate / Plex / Jellyfin's
  default sort. Local timezone configurable.
- **Channel cloning**: virtual channels created in a target ChannelGroup
  (default `Top Matchups`) pointing at the source channel's streams. Source
  channels are never touched.
- **Dummy EPGSource** (created by the plugin) carries `ProgramData` entries
  whose `description` field explains WHY each game made the cut. Format:
  `Kickoff: Today 2:00 PM CDT 🔴 TODAY` + signal breakdown.
- **Placeholder channels** for unmatched but high-scored games — surfaces big
  upcoming matchups in the guide before the provider EPG publishes broadcast
  info. Threshold tunable.
- **Group-rename auto-cleanup**: changing the target group name detects old
  virtual channels by tvg_id marker and migrates them.
- **Multi-time scheduler**: `scheduled_times` in `HHMM` comma-separated format
  (e.g., `0000,0600,1200,1800`). Cross-worker Redis lock.
