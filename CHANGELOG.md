# Changelog

All notable changes to this plugin are documented here. Format roughly
follows [Keep a Changelog](https://keepachangelog.com/) with semver.

## [Unreleased]

## [1.22.0] - 2026-08-29

### Fixed

- **NCAA Football pulled every NCAA division, which timed out the whole
  refresh once the season opened.** `auto_pipeline` had been failing on most
  runs since 2026-08-21 with `pipeline subprocess timed out after 1500s`, so
  the guide stopped updating entirely and the cache went stale. NCAAF itself
  looked fine in the summaries (134-136 games), which is why it was not the
  obvious suspect.

  **Root cause:** `NcaafSource.fetch_upcoming` filtered CFBD's `/games` by
  time window only, never by division, so every Division II and III fixture
  entered the pipeline. Each emitted row costs a Monte Carlo importance
  simulation, and those rows cannot even score: the season replay population
  was already FBS-only, so a team outside it has no estimated strength and
  returns 0.00 every time. Measured against the live 2026 season on
  2026-08-29:

  ```
  next 7 days: 157 games   FBS-involved 27 | FCS-only 45 | D-II/D-III 85
  compute_match_importance: ~7.0s per game
    -> 157 x 7s = ~1100s of the 1500s budget, ~600s of it on games
       structurally incapable of scoring above zero
  ```

  All summer this was inert: the offseason returned no upcoming games, so the
  cost was zero. Week 1 filled the lookahead window and the pipeline went over
  budget on the first day of the season.

  The filter now gates BOTH the emitted games and the simulated season
  population from one predicate, so the two cannot drift apart.

- **A failed `/rankings` request was logged as "offseason".** Any transient
  CFBD failure zeroes the entire NCAAF slate for that run, and the log line
  claimed the season was over. Request failure, unpublished poll, and
  missing-poll-in-snapshot are now three distinct messages at the right
  levels.

- **FCS opponents were bucketed into CFB win-count bands off a one-game
  schedule.** The division filter for GAMES is an either-side rule, because
  an FBS team's win over an FCS opponent counts toward its bowl eligibility
  and the game has to stay in the population. That pulled the FCS opponents
  into the simulated state as a side effect: measured 2026-08-29, 100 of the
  238 teams in the FBS-only population had 4 or fewer games and most had
  exactly 1, so they were scored against "6+ wins = bowl eligible" on a
  schedule that cannot reach it. Their contingency tables degenerated and
  their leverage read 0, the right number for the wrong reason.

  `PointsBasedSportSource.outcome_eligible_teams()` now separates "games
  needed to count a selected team's record" from "teams eligible for this
  league's outcome bands". Defaults to None (no filtering), so MLB, NBA,
  NHL, WNBA and the NCAA baseball/softball/basketball sources are
  unchanged.

- **The season schedule was fetched twice per refresh.** The upcoming-window
  path and the Monte Carlo population made separate identical `/games`
  calls, so they could disagree (a classification edited between them, a
  season-year boundary crossed mid-refresh) with no way to see it. One
  cached fetch now feeds both, which makes "the simulated universe matches
  the emitted universe" structural instead of a convention, and halves the
  request cost against the 1k/day free tier. A failed fetch is deliberately
  not cached.

### Added

- **`NCAA Football divisions` setting** (`ncaaf_divisions`): `D1 FBS only`
  (default) or `D1 FBS + FCS`. Division II and III are never pulled. FCS
  teams stay unranked because the AP Top 25 is an FBS poll, so under
  `D1 FBS + FCS` those games enter as low-scored filler and an FBS-vs-FCS
  game is ranked on its FBS team.


## [1.21.0] - 2026-08-26

### Added

- **English domestic cups: EFL Cup (Carabao Cup) and FA Cup (#190).** Two new
  toggles, both off by default, backed by ESPN's keyless site API.

  Reported as "Tottenham v Charlton is on tonight and it isn't in my Top
  Matchups". The game was carried on six-plus channels in the guide (beIN Sports
  AU/FR/Arabic, ESPN Brasil, Nova Sport CZ) and the matcher never went looking
  for it, because no cup fixture had ever entered the pipeline.

  **Root cause:** the soccer competition catalog is bounded by what
  Football-Data.org's free tier serves, so every competition FD.org gates behind
  a paid plan is structurally absent regardless of channel availability. That is
  all four English domestic cups. Verified against the live API with the
  plugin's own key on 2026-08-26:

  ```
  code=FLC  Football League Cup  plan=TIER_THREE   -> HTTP 403
  code=FAC  FA Cup               plan=TIER_TWO
  code=PL   Premier League       plan=TIER_ONE     -> 200, 4 matches
  ```

  Same key for both, so it is plan scope and not a bad key or a rate-limit.
  FD.org's free tier is exactly 12 competitions and no cup of any country is
  among them. Hence ESPN, and hence a new source rather than a
  `soccer.py::COMPETITIONS` entry, which would 403 on every refresh and
  contribute zero games silently, i.e. reproduce the reported symptom.

  **Rounds** come from ESPN's per-event `season.slug`, the same mechanism
  `ncaa_soccer_cup` uses. Both cups publish 8 ordered rounds and the maps are
  deliberately per-competition: the EFL Cup has a preliminary round the FA Cup
  does not, the FA Cup has a fifth round the EFL Cup does not, and the shared
  names sit at different depths (the FA Cup's fourth round is its round of 32,
  the EFL Cup's is its round of 16).

  **No importance simulation and no ranks**, both deliberate. A cup has no
  league table; simulating the bracket instead would need the whole bracket,
  which ESPN publishes one day at a time, and the EFL Cup semifinal is two legs
  while every other round is single-leg. Ranks stay `None` because that is what
  makes the giant-killing shape score correctly: `score_game` awards rank points
  only when a rank is present, so a Premier League side drawn against a League
  One side gets no rank-gap penalty for being lopsided. A tier-derived
  pseudo-rank would actively hurt.

  Early-round ties score LOW on purpose (a new `CUP_R*` stage band ramping to
  just under the shared `QUARTER_FINALS` score) and sort to the bottom, where
  `max_games` trims them. An EFL Cup second-round Wednesday is ~25 fixtures;
  they are thin-slate filler, which this plugin intends, not noise to floor
  away.

  **No favorites gate**, unlike the friendlies sources: a friendly is an
  exhibition whose only claim to a slot is the favorite signal, whereas a cup
  tie has real elimination stakes and a quarterfinal between two clubs you do
  not follow is a genuine top matchup.

### Changed

- **The per-day ESPN scoreboard sweep is now shared** between `friendlies.py`
  and the new `english_cup.py`, as `_espn.sweep_upcoming_scoreboard`. It owns
  the US-Eastern-bucket lookback, the FINISHED drop, the stale-SCHEDULED age
  floor, and the event-id dedupe. Both date constants encode the reasoning for a
  live bug each, and duplicating that reasoning is how it drifts. The
  bracket-flavoured sources are deliberately NOT routed through it: they sweep a
  fixed calendar window and KEEP finished games, because a bracket's state is
  derived from results already played.

### Fixed

- **The tournament-stage note in the EPG description no longer leaks the
  internal stage enum.** These notes are user-facing (they are the "why did this
  game rank here" text), and the note was the lower-cased raw label, so
  `SEMI_FINALS` read as "semi finals" and the new cup labels would have read as
  "cup r2". It now reuses `tournament_stage_label`, the same prettifier the
  `{tournament}` naming token uses, so each stage has one spelling across the
  EPG text and the channel name: "Semifinal", "Round 2".

## [1.20.1] - 2026-08-17

### Fixed

- **Match identity for eleven sources no longer depends on kickoff time (#181).**
  `simulation._same_match` consults an id only when BOTH rows carry the same key,
  and the fixture pool from `remaining_matches` stamps `game_id`. Eleven sources'
  `fetch_upcoming` stamped only their own `<sport>_game_id` / `espn_event_id`, so
  the target row shared no key with the pool and identity silently degraded to the
  `(home, away, start_time)` fallback.

  That fallback is not equivalent. `points_based.remaining_matches` substitutes a
  2099-01-01 sentinel for a fixture with no published start time, and a
  **rescheduled** game (routine in MLB and NFL) simply has a different kickoff in
  the two separate fetches. Either way the target fails to match itself, is
  sampled a second time as a "remaining" match, and the contingency table
  describes a season the recorded W/D/L row does not. Nothing raises; the
  importance number is just wrong. MLS shipped this live and it was fixed in #65;
  this is the same defect everywhere else.

  Fixed by stamping `game_id` alongside the existing sport-specific id in:
  `mlb`, `mls_cup`, `nba`, `ncaa_baseball`, `ncaa_soccer`, `ncaa_soccer_cup`,
  `ncaa_softball`, `ncaaw_basketball`, `nfl`, `nhl`, `wnba`.

  **Scope correction:** the issue named four sources; the real population is
  eleven, found by generalising the check across `sources/` instead of
  enumerating. `mls.py` and `friendlies.py` also stamp a sport-specific id and are
  deliberately NOT changed: both inherit `SportSource` directly, whose
  `remaining_matches` raises `NotImplementedError`, so they never reach the
  simulator and the key would be inert. That exemption is itself pinned by a test.

  Deliberately not auto-mirrored in `GameRow.__post_init__`: the mirror is only
  correct where the sport-specific id really is per-GAME identity, and an event id
  covering several games would map wrongly and silently. The justification now
  lives once on `GameRow.extra`, with a one-line pointer at each site.

  Guarded by `tests/test_match_identity_contract.py`, an AST rule over the whole
  `sources/` tree, so a source written later cannot reintroduce it. Verified by
  reverting `sources/` to pre-fix with the tests kept: exactly 11 assertion
  failures, one per affected file, and nothing else moved.


## [1.20.0] - 2026-08-17

### Changed

- **The game-thumbs logo tier is now OPT-IN and off by default.** 1.19.0 shipped
  `game-thumbs base URL` defaulting to the vendor's public instance, which opted
  every installation into a third-party service that then sees which fixtures the
  user curates and whose uptime their guide's logos depend on. Nobody chose that;
  it was a default. A capability worth having is not a dependency worth imposing.

  The default is now blank, which disables the tier outright: no HTTP is issued
  to anyone, and a game with no SportsDB thumbnail falls straight through to the
  league/tournament badge exactly as it did before 1.19.0.

  Nothing is removed. The 30 verified league slugs, the 27 alias mappings, the
  rate-limit pacing and the ~80 tests all remain, and the tier works the moment a
  base URL is set. **Self-hosting is the recommended way in**
  (`ghcr.io/sethwv/game-thumbs`), since it also escapes the public instance's
  30 requests/minute cap. The public URL is still recorded in the help text and
  as `gamethumbs.PUBLIC_INSTANCE_URL` for anyone who deliberately opts into it.

  What it costs when off, measured on a live 23-game slate: 6 channels keep a
  real SportsDB matchup thumbnail and the other 17 show a league badge, instead
  of all 23 showing a real per-game image. That trade is the user's to make, not
  the plugin's.

  Four new tests assert the default cannot drift back on, including one that
  fails if the default base URL ever reaches the network.


## [1.19.1] - 2026-08-17

### Fixed

- **The SportsDB API key no longer reaches a log call at all.** `_http_get_json`
  logged `redact_secrets(url)`, and SportsDB carries the key as a PATH SEGMENT,
  so the URL is key-bearing by construction. The redaction was working, so this
  was not a live leak — but the safety depended on the redactor staying correct,
  and neither a reader nor a static analyser can confirm that from the call site.
  CodeQL's `py/clear-text-logging-sensitive-data` flagged exactly this shape and
  blocked the 1.19.0 listing in `Dispatcharr/Plugins`.

  `_http_get_json` now takes a STATIC endpoint label (`"searchevents"` /
  `"lookupleague"`) and logs that instead, so the key has no path to the log.
  An exception object still goes through the redactor, because urllib's
  `HTTPError` stringifies the request URL and that cannot be restructured away.

  This is a preventive/structural change: 0 tests witness a defect (there was
  none), 3 guard the property, and the one that fails under the structural
  mutation is `test_call_sites_pass_a_static_label`, which asserts both call
  sites pass a literal.


## [1.19.0] - 2026-08-15

### Added

- **game-thumbs bridges team-name vocabulary gaps (#175).** The tier added in
  #174 resolves a matchup from the team pair, so a league whose team names
  differ from ESPN's vocabulary degraded to the league badge even though
  game-thumbs carries the fixture. `CA Paranaense` and `Athletico-PR` share no
  substring, so upstream's own partial matching cannot bridge them.
  - New `gamethumbs_aliases.json` maps our source's name to the league's ESPN
    name, per league. A definitive miss is now retried **once** with the aliased
    name before being negative-cached.
  - The alias goes **second**, after the raw name has already missed. That
    ordering means an entry can only ever add a logo: an alias that goes stale
    cannot break a pair that currently resolves, and an unaliased miss still
    costs exactly one request.
  - A 429 is still never a miss and never spends the alias retry. Rate limiting
    says nothing about vocabulary, and treating it as a miss would blank a whole
    slate of logos for the negative-cache TTL.
  - 27 aliases across 6 leagues, none of them guessed. game-thumbs answers an
    unknown team with a 400 whose body enumerates the entire league, which makes
    it an oracle: all 30 league lists were pulled that way and diffed against
    every one of the 346 team names our sources actually emit, then both halves
    of each pair were confirmed with real requests (left side `400 Team not
    found`, right side `200 image/png`). Brazilian Serie A, La Liga, Ligue 1,
    Bundesliga, Primeira Liga, Serie A and the Champions League needed entries;
    the Premier League, Championship, Eredivisie, World Cup, Euros, MLB and NHL
    swept clean and deliberately have none.
  - Kept out of `team_aliases.json` on purpose. That file belongs to the
    matcher, its values become **strong** EPG-matching keywords, and it is flat
    where this has to be per-league — a bare club name is not unique worldwide,
    and the same club needs its own entry in each competition it plays in.

## [1.18.0] - 2026-08-15

### Added

- **game-thumbs as a second matchup-logo tier (#174).** TheSportsDB only has a
  thumbnail if it pre-rendered one for that specific indexed event, so its
  coverage tracks how well it indexes a league. Measured on a live instance:
  9 hits and 9 misses across 18 fixtures, with every MLS, Brazilian Serie A and
  NCAA soccer game missing and falling through to a league badge that looks
  identical for every game in that league.
  [game-thumbs](https://github.com/sethwv/game-thumbs) composites two ESPN
  crests on request, so it resolves from the team pair alone whether or not the
  fixture is indexed anywhere. It slots in between the SportsDB thumb and the
  badge, leaving the rest of the #102 order intact, and the composite is
  downloaded to `/data/logos/` exactly like a SportsDB thumb so an outage cannot
  break already-applied channels.
  - New setting **game-thumbs base URL**, defaulting to the public instance.
    Blank disables the tier. Self-host `ghcr.io/sethwv/game-thumbs` to escape
    the public instance's 30 requests/minute cap.
  - 30 leagues mapped, every slug confirmed live by rendering a real fixture and
    getting back a `200 image/png`. The slugs are ESPN-style and not guessable
    (men's college basketball is `mens-college-basketball`; `ncaa/basketball/mens`
    404s).
  - Rate limiting is paced under rather than discovered: requests are spaced to
    stay below 30/min and a 429 is retried once honouring `Retry-After`. A 429 is
    never negative-cached, because doing so would turn a one-minute throttle into
    a day of league badges for every game caught in it.
  - The apply summary now reports both tiers, e.g.
    `Matchup logos: 9 SportsDB + 8 game-thumbs, 3 league badge, 1 source-channel`.

### Fixed

- **Sweep no longer depends on the logo file being a JPG.** `marker_to_filename`
  gained an extension parameter for the PNGs game-thumbs returns, and
  `sweep_stale_logo_files` now builds its live set across every extension the
  module can write. Without that pairing the first apply after a composite was
  written would have treated the live game's own PNG as stale and deleted it.
- Documentation rot: the README described the plugin's `EPGSource` as `dummy`,
  which contradicts the `DO NOT use source_type="dummy"` constraint in
  `plugin.py` (a dummy source makes Dispatcharr overlay generated filler on top
  of the real programming). Two settings help texts still named the
  source-channel logo as the only fallback, predating the #102 badge tier.
## [1.17.3] - 2026-08-15

### Fixed

- **MLS importance read ~0 for the whole season (#65).** The conference
  standings sources swept the season fixture list only as far as `now + 7
  days`, then handed that to the Monte Carlo simulator, which projects each
  simulated season to its END. With a week of fixtures left to play no
  simulated season separates a bubble team's final standing from any other,
  so Kendall tau-c collapsed to 0 on every threshold band and MLS games
  scored no importance points.

  The sweep now covers the whole Feb-Nov season window. Measured live
  mid-season (2026-08-15), Eastern Conference: remaining fixtures went from
  13 to 95, games scoring nonzero importance from 9/18 to 18/18, and mean
  importance points from 0.37 to 0.87. 16 of 18 emitted Eastern games and 15
  of 16 Western games now register `playoff_bubble` leverage.

  This was diagnosed in #65 as ESPN publishing only 1-2 weeks of future MLS
  fixtures. That was wrong: the thin endpoint is `/teams/{id}/schedule`,
  while the scoreboard endpoint the plugin actually uses had all 241
  remaining fixtures published. No fixture synthesis was needed; the code was
  simply asking for one week of a season it needed all of.

- **The MLS season fetch is one HTTP call instead of ~300.** ESPN's
  scoreboard accepts a `dates=YYYYMMDD-YYYYMMDD` range but paginates it,
  silently returning only the first 100 events unless an explicit `limit` is
  passed. With the limit, the whole season arrives at once: verified live
  that the range response and the 303-call per-day sweep it replaces produce
  identical event-ID sets with identical kickoff times. A response that comes
  back at the limit is now logged as possibly truncated. Whole-season fetch
  time measured at ~1.0s per conference.

- **The target match could be simulated twice, silently corrupting its
  importance score (#65).** `PointsBasedSportSource.apply_result` records
  `extra["game_id"]` into the applied set that `remaining_matches` filters on,
  but the MLS conference sources stamped their emitted rows with
  `espn_event_id` only. A target row carrying neither `game_id` nor `fd_id` is
  invisible to that dedup, leaving `simulation._same_match`'s
  `(home, away, start_time)` fallback as the only guard. That fallback holds
  only while the fixture pool and the emitted row agree on a kickoff time; a
  pool fixture with no published date gets the 2099-01-01 sentinel
  `points_based.remaining_matches` substitutes, the two dates disagree, and the
  simulator plays the target a second time as a "remaining" match. The
  contingency table is then built from a season that the recorded W/D/L row
  does not describe. Nothing raised: the importance number was just wrong.

  `_same_match` now resolves identity from `game_id` as well as `fd_id`
  (consulting a key only when both rows carry it, so ids from different
  namespaces cannot collide), and the MLS sources emit `game_id` alongside the
  `espn_event_id` that cache.json consumers already read.
## [1.17.2] - 2026-08-15

### Added

- **Field-event sports now get a league badge instead of the channel logo
  (#104).** Formula 1, NASCAR, PGA, UFC, ATP, WTA and boxing have no head-to-head
  opponent pair, so the matchup-thumbnail path can never produce anything for
  them: their `away` is the "Field" sentinel and the `"<home> vs Field"` search
  matches no SportsDB event. That left every one of these games falling all the
  way through to the provider's channel logo ("ESPN"), which is the least
  useful image a curated matchup can carry. The league badge is the best logo
  these sports will ever get, so they are now mapped:

  | Prefix | SportsDB id | `strLeague` |
  | --- | --- | --- |
  | `F1` | 4370 | Formula 1 |
  | `NASCAR` | 4393 | NASCAR Cup Series |
  | `PGA` | 4425 | PGA Tour |
  | `UFC` | 4443 | UFC |
  | `ATP` | 4464 | ATP World Tour |
  | `WTA` | 4517 | WTA Tour |
  | `BOX` | 4445 | Boxing |

  Every id was resolved live through `lookupleague.php` and confirmed to return
  both the expected `strLeague` and a non-empty `strBadge`, with a known-good
  control (4328) re-run in the same batch — the free API key answers a burst
  with HTTP 429 or an empty body, and both are indistinguishable from "no such
  league" unless you check.

  `BOX` (4445) survived a challenge worth recording: an independent bulk sweep
  of ids 4380-4900 reported that no generic boxing league existed in that range,
  which would have meant 4445 was wrong. Re-checking found the opposite — that
  sweep reported only 3 Fighting leagues where paced per-id lookups with a
  passing control found at least 16, so it was missing ~80% of the range and
  reporting the gaps as absences. 4445 was confirmed three separate times, and
  its badge downloads as a real 474KB PNG. The `DO NOT trust a bulk sweep's
  absences` note now sits above the map.

  `BOX` maps to SportsDB's **generic** `Boxing` league, not to a promotion.
  The source is cross-promoter, so a Top Rank or Matchroom badge would be wrong
  on every card the other promoters put on — the id carries a `DO NOT` comment
  saying so.

  Still unmapped, deliberately: the NCAA sub-sport prefixes (`NCAAW`,
  `NCAAMSOC`, `NCAAWSOC`, `NCAABSB`, `NCAASBL`) and the friendlies prefixes
  (`FRIENDLY`, `FRIENDLYW`, `CLUBFRIENDLY`). The free tier caps every league
  listing at five rows with no way to page past them, so their leagues could
  not be confirmed, and an unverified id is worse than a missing one: a missing
  prefix falls through to the channel logo, a wrong one silently paints a
  wrong-sport badge on every game of that sport.

### Fixed

- **Dropped five dead `_SPORT_HINT` entries.** `CWBB`, `NCAAB`, `NCAAS`,
  `NCAAMS` and `NCAAWS` are prefixes no source has emitted for some time; they
  matched nothing and only made the hint map look like a registry of live
  prefixes when it is not.
## [1.17.1] - 2026-08-15

### Fixed

- **Tennis and golf events no longer match unrelated sports' channels (#169).**
  Reported by a user on v1.16.0: `WTA Warsaw T-Mobile Polish Open` and
  `ATP Cincinnati Open` were both matching a PDC darts channel
  (`European Tour 12 _ Czech Darts Open`) as `regex_strict`.

  Field-event sources put the EVENT name in `home`, and it runs through the
  same splitter as a team name. The last-word relaxation therefore emitted a
  tournament title's final token as a STRONG keyword, one that admits a
  candidate on its own, but that token is a generic competition noun shared
  across every sport. Measured over the 22137 real channel and stream names in
  a live snapshot: `Championship` matched 383 of them, `Open` 157, `Prix` 93,
  `Masters` 87. Same family as the numeric case `_is_weak_last_word` already
  blocks (`UFC 329: ... Holloway 2` -> `2`), which only covers short and
  numeric last words.

  Replayed through the real lookup against a 6708-channel snapshot, the two
  reported events had been attaching **14 channels and ~140 streams** of darts
  coverage each. After the fix `Cincinnati Open` matches its genuine broadcast
  (`DAZN CA 26: Cincinnati Open - Day 2 - Session 2`) and
  `New Zealand Darts Masters` matches the real PDC feed, so removing the
  wildcard recovered correct matches rather than only deleting wrong ones.

  Suppressing the last word also must not promote the two-word prefix, which
  would trade one wildcard for another: `New Zealand Darts Masters` would have
  promoted the bare country `New Zealand`, going from 87 corpus hits to 121.

  Corpus diff over both populations: 26 event names narrowed, 0 widened, and
  **0 of 184 real team names changed**, so the two-team leagues are untouched.


## [1.17.0] - 2026-08-12

### Added

- **Curated channels now land in your Channel Profiles (#172).** Reported by a
  user: "the visibility for the created channels is disabled for all my profiles
  except for All". Dispatcharr creates `ChannelProfileMembership` rows in its API
  view layer, for channels made through its own UI, so channels a plugin creates
  through the ORM get none, and the m3u/XC output filters a named profile on
  `channelprofilemembership__enabled=True`. The result is a channel that is
  present under `All` (which applies no membership filter at all, and is not a
  profile) and switched off in every named profile. Measured on a live instance
  before the fix: 24 plugin channels, 19 profiles, 0 membership rows.

  Apply now sets that membership. **Blank, the default, means every profile**,
  matching what Dispatcharr itself does for a hand-created channel. The new
  "Channel Profiles to enable" setting takes a comma-separated list of profile
  names to narrow it, matched case-insensitively against the names shown in the
  UI. The setting converges: profiles listed are turned on and profiles left out
  are turned off, so removing a name removes the channels from that profile on
  the next apply. A name matching no profile is reported in the Apply result and
  logged next to the list of names that do exist, rather than being guessed at
  or silently ignored.

  Every channel the plugin owns is covered, not just the ones a given run
  creates, so an existing install heals on its first apply after upgrading.

### Changed

- **`channel_profile_name`'s help text now says it is a channel GROUP**, not a
  Channel Profile. The id is a long-standing misnomer that cannot be renamed
  without dropping every existing install's saved value, and it became actively
  confusing the moment a real Channel Profile setting landed beside it.
## [1.16.1] - 2026-08-12

### Fixed

- **A dry-run apply no longer looks like a silent success (#170).** Reported by
  a user whose group and channels never appeared: his log ended with
  `auto_pipeline task <id> complete: status=ok` and nothing else, because
  `dry_run` defaults to true and three things conspired to hide that. On an
  install with no group yet, apply returns from its dry-run branch before
  logging anything; "Refresh + apply now" runs in the background, so the
  "[dry] Would create..." message it returns is discarded and never reaches a
  toast; and the thread body logged only `status=`, not the message. All three
  are closed. The background runner now logs the result message alongside the
  status, both of apply's dry-run exits state the consequence and the remedy
  ("Apply writes nothing. Untick it in this plugin's settings...") in the
  message AND in the log, and **Show current state** reports when dry run is on.

### Changed

- **Show current state prints its warnings at the BOTTOM.** An action result
  renders as a toast anchored bottom-right that grows upward and cannot be
  scrolled, so a 30-line status dump pushed its own opening lines off the top of
  the screen: a banner at the top was a banner nobody could read. The broken
  name-template warning (#126) moves down there with the new dry-run notice.

## [1.16.0] - 2026-07-30

### Added

- **European public broadcasters now match (#143).** ORF, ARD, ZDF and SRF put
  the competition in the programme title and the actual fixture in the
  sub-title, in the local language: title `FIFA Fußball WM 2026`, sub-title
  `Gruppe F: Schweden - Tunesien`. The matcher read only the title, so those
  broadcasts produced no candidates and the games never matched at all. The
  candidate pre-filter now also reads the sub-title and the description, and the
  both-teams regex tier reads them too, so an ORF programme matches on the regex
  tier instead of falling through to the LLM for a fixture its sub-title states
  outright. The three fields are gated differently on purpose: a title or
  sub-title admits on one team name, but a description is free prose that
  name-drops other teams in passing, so it requires both sides.
- **German team names** in `team_aliases.json` (`Frankreich`, `Schweden`,
  `Tunesien`, `Kroatien`, `Elfenbeinküste`, …), without which the above changes
  nothing, since `Frankreich` never matched `France`. Both umlaut and ASCII
  spellings are listed because matching does no unicode folding.

### Fixed

- **Stale feeds from previous nights no longer stack behind a game (#164).**
  Streams carry no schedule, so stream-name matching had no time filter at all,
  and the team-pair gate that was supposed to keep it tight is blind to dates.
  Baseball plays the same opponent three nights running, so every night's
  dedicated feed names the same two teams: roughly half the 22 streams attached
  to one Yankees game were the previous two nights' finished broadcasts. The
  date, where a provider puts one in the name, is now read and a feed dated
  before its game is dropped. Undated feeds are unaffected, and a future-dated
  feed is kept.
- **The main card, not the pre-show, is now the primary feed for an event
  (#135).** Matching stacked every channel naming a UFC card, which mixes the
  main card with the pre-show, the prelims, the post-fight press conference and
  the multiview, and picked whichever it saw first. Ancillary feeds now sort
  behind a plain feed. They are still stacked as fallbacks, never dropped.
- **An invalid channel-name template now says so (#126).** A stray brace made
  apply silently fall back to the default template, so channels kept showing the
  score even though the saved template had no score token, with nothing in the
  UI explaining why. Apply and **Show current state** now both report it and
  point at where to fix it. Note the plugin cannot reject a bad template at the
  moment you save it: Dispatcharr writes plugin settings with no validation hook
  available to the plugin.

## [1.15.0] - 2026-07-30

### Changed

- **Apply now runs in the pipeline subprocess, not in the web worker (#142).**
  Standalone `apply` was the last plugin action doing heavy DB work inside the
  gevent uwsgi worker, and it kept intermittently freezing one worker: nginx
  round-robins into it, so login (a multi-request flow) failed for roughly a
  quarter of attempts, and the `3/minute` throttle then locked out the retries.
  It survived both earlier fixes aimed at it — after 1.7.1 shrank the apply
  transaction to sub-second and 1.8.x closed the scheduler DB-connection leak, a
  0.55s apply on a warm container still produced a 20s login timeout.

  Rather than keep hunting the exact non-yielding call with tools that cannot
  see individual greenlets, the surface is gone: `apply` forks the same
  `_pipeline_runner.py` child that `refresh` and `auto_pipeline` already use, so
  no plugin action does bulk DB work in the worker any more.

  **This is not a UX change.** Apply stays synchronous and still returns the
  real summary in the toast, unlike refresh which returns a queued envelope: the
  subprocess wait is gevent-cooperative, so the hub keeps serving requests while
  the child works. The only visible difference is roughly one to two extra
  seconds for the child's Django startup. `auto_pipeline` is unaffected — it
  still chains refresh and apply inside ONE child, not two. The destructive
  cross-worker lock is still taken in the parent for the whole run.

## [1.14.2] - 2026-07-30

Matching precision, part two. Two long-standing loose-match defects that
attached channels and streams carrying a different fixture (#129).

### Fixed

- **A team abbreviation in a provider's feed label no longer pairs with an
  opponent named in the matchup body.** Tier-1's channel-name both-teams check
  accepted the two sides anywhere in the name, so `USA Soccer07: Australia vs
  Turkey` — the Australia-vs-**Turkey** feed — matched the Australia vs United
  States game, with `USA` supplied by the network label. Channel names are now
  gated on `both_teams_in_one_segment`, the same rule stream names have had
  since 1.8.0; the two paths were built for the same class of text and now
  agree. Programme titles (Tier 2) are deliberately NOT gated: feed labels are
  a channel/stream naming convention and XMLTV titles do not carry them.
- **Short team aliases must now match as whole tokens.** `team_aliases.json`
  carries broadcast abbreviations, and up to four characters they were matched
  as bare substrings, which made them near-wildcards: `NE` (New England) hit
  `Sportsnet` and `Tennessee`, `OM` hit `Roma`, `BOS` hit `Bosnia`, `GB` hit
  `GBN`, `PIT` hit `Jupiter`, `CLE` hit `Clearwater`, and `Real` hit
  `Montreal`. On Path A's single-keyword title pre-filter this handed the LLM
  tier an arbitrary candidate pool for every game involving such a team.
  Longer keywords keep substring semantics, so `Yankees' bullpen` still hits.

  Measured cost: a differential over a live 6588-channel / 16415-stream corpus
  across 33 fixtures dropped exactly two Tier-1 matches, both false positives
  (a WTA tennis card matched to Cowboys-Eagles via `Dalibor`, a MiLB game
  matched to Steelers-Browns via `Jupiter` and `Clearwater`), and gained none.

## [1.14.1] - 2026-07-29

Matching precision. Two defects that composed to attach a different sport's
dead feeds to a game channel (#162).

### Fixed
- **A bare city can no longer admit a candidate on its own.** `New York Yankees`
  expanded to the keywords `New York Yankees`, `Yankees`, **`New York`**, `NYY`,
  and the EPG title pre-filter admits a candidate on any ONE keyword, so every
  same-city franchise entered the pool: an MLB Yankees game came back carrying
  the NFL Giants and Jets, the NBA Knicks, the WNBA Liberty and the Mets. The
  sport prefix never guarded against this (it only sizes the match time window).

  Keywords are now split into **strong** (identify the team alone) and **weak**
  (bare place-name relaxations). Single-keyword admissions use strong keywords
  only; the both-teams gates keep using everything, because requiring both sides
  in one segment is specific enough that a city-only feed name is a correct
  match. That distinction matters in both directions: dropping the city
  everywhere also broke a real Tier-1 match, since providers do name feeds
  `(Apple) (MLS) 006 | Cincinnati vs. San Jose`. Two related keyword leaks went
  with it: `New York City FC` re-emitted the bare `New York` via its
  suffix-stripped form, and `Red Bull New York` reduced to `York`, which
  substring-matches every New York franchise (it now yields the actually
  discriminating `Red Bull`). `North Carolina State` still keeps `North Carolina`
  and `UFC 329: McGregor vs. Holloway 2` still keeps `UFC 329:`, because for
  those the prefix is the only relaxed form left.

  Verified by replaying the real slate of a 6594-channel instance against the
  pre-fix baseline: candidate pool 28 to 17 for the affected game with zero
  cross-sport entries left, the correctly matched game keeping the identical 14
  channels and 22 streams, and exactly one behaviour change across the whole
  slate, a UFC event that had matched a *different* event's preview show
  (`UFC Fight Night Preview Show: Gamrot vs. Salkilld`) via the `UFC Fight`
  prefix and is now correctly unmatched.
- **A whole-channel match no longer donates streams for other games.** Apply
  attached every stream on a matched channel with no name check, so one bad
  match delivered every feed bundled onto that channel rather than one wrong
  feed. Streams are now gated per channel: only when some stream on the channel
  names one of this game's sides are the streams naming neither side dropped, so
  a generic broadcaster channel (`MLB Network HD` / `FHD`) still contributes all
  of them. Dropped counts are logged and reported in the apply summary rather
  than being silent. Field events keep every stream, since they are single-sided
  and their feed naming is tracked separately (#135).

## [1.14.0] - 2026-07-26

Security and correctness pass over the whole plugin. No new features.

### Security
- **API keys no longer reach the logs.** Two shapes, both arriving inside strings
  the plugin never builds, so there was no parameter to sanitize:
  the Odds API takes `apiKey` as a **query param** and `requests` embeds the full
  URL in its exception text (`HTTPError` renders "... for url: ...?apiKey=...",
  `ConnectionError` renders "Max retries exceeded with url: ...?apiKey=..."), and
  TheSportsDB carries its key as a **path segment** (`/api/v1/json/<KEY>/...`)
  which was logged whole on every failed lookup. The Odds leak was at WARNING,
  visible at the default INFO log level; the SportsDB one was at DEBUG. Both key
  fields are `input_type: password`, which masks the form input and does nothing
  for logs, and Dispatcharr logs go to container stdout. Added
  `_util.redact_secrets`, applied at every affected site in `logos.py`,
  `sources/soccer.py`, `sources/mls.py`, and `sources/mls_standings.py`, with a
  source-level contract test so a new log line cannot reintroduce it. (#156)

### Fixed
- **The scheduler lock could be released by a different holder, allowing two
  concurrent destructive applies.** The lock value was the constant `"1"` and
  release was an unconditional `DELETE`, so a run that outlived the TTL deleted
  whichever holder had replaced it and a third run could start alongside the
  second. Two applies interleaving `Channel.delete()`, `ProgramData.delete()` and
  guide renumbering can drop rows the other just wrote. Now a unique per-run
  token with an atomic compare-and-delete release. The TTL also moved from 30 to
  90 minutes: the refresh subprocess alone may legitimately take 25, and apply's
  per-game network pre-pass could exhaust the remaining 5 at *default* settings.
  Redis being unreachable now fails **closed** for apply/auto_pipeline (declining
  to run beats running unserialized) and still fails **open** for refresh, which
  only reads upstream and rewrites `cache.json`. (#155)
- **`apply` took no lock at all.** It was dispatched straight to the bare
  `_action_apply`, so clicking Apply during a scheduled `auto_pipeline` raced it
  directly, with no TTL overrun needed. The manifest action now points at
  `_action_apply_locked`; the bare function stays lock-free on purpose so
  `_action_auto_pipeline_sync` can call it from inside the already-locked
  subprocess. (#155)
- **Rename cleanup could delete a group the user created.** An empty group was
  deleted when its name merely *contained* `"top"` or `"matchup"`, which also
  matches Rooftop, Stop, Laptop, Topical and Utopia; and emptiness is a state the
  sweep itself creates by migrating our channels out. This was the one delete in
  the plugin not gated on provenance. Now an exact, case-insensitive allowlist of
  names the plugin itself creates. A custom-to-custom rename now leaves the old
  empty group behind, which is the safe direction, and says so in the log. (#157)
- **Football-Data.org calls now pace and retry.** Three call sites fired back to
  back against a 10 req/min free tier with no throttle, backoff, or retry, so a
  single 429 or connection drop zeroed whatever had not been fetched and the
  soccer slate silently read "0 games" until the next refresh six hours later,
  indistinguishable from an off-season. All three now route through one paced
  `_fd_get` (7.0s spacing, at most nine requests in any rolling 60s window) that
  retries 429s and transport drops with backoff, honoring FD.org's
  `X-Requestcounter-Reset` header. A contract test rejects any new bare
  `requests.get` against `FD_BASE`. (#159)
- **`max_games` is validated.** It was read with a bare `int()`, so a non-numeric
  stored value raised `ValueError` out of refresh and killed the action, and an
  unbounded large value stretched apply's pre-pass past the lock TTL. Now clamped
  to `[1, 60]` with a fallback and a warning, and the manifest declares matching
  bounds. The ceiling is derived from the lock budget, not chosen for roundness.
  (#158)

### Changed
- Test suite gained an autouse fixture neutralizing FD.org pacing. Leaving the
  real sleeps in took the suite from 7 seconds to 105; the fixture is autouse so
  a new test touching an FD path cannot silently reintroduce that cost.

## [1.13.0] - 2026-07-25

### Added
- **Club friendlies source (`ClubFriendliesSource`, setting `enable_club_friendlies`).**
  Pre-season tours and mid-season club exhibitions from ESPN's `club.friendly`
  competition. Closes a coverage hole that made an entire class of game
  unreachable: a live Wrexham v Leeds United pre-season friendly never appeared
  in Top Matchups despite Wrexham being the top entry in Favorites, because
  `InternationalFriendliesSource` sweeps `fifa.friendly` (national teams only)
  and Football-Data.org's competition codes (PL, ELC, CL, ...) carry no
  friendlies at all. The blind spot spanned the whole European pre-season
  window, roughly July to mid-August, which is exactly when every league source
  in the plugin returns zero games. Off by default; shares the existing
  `friendlies_favorites_only` gate with the international sources. (#153)

### Fixed
- **Friendlies sweep went blind to fixtures in the 00:00Z-05:00Z window.**
  ESPN buckets its scoreboard by US **Eastern** calendar date, not UTC, so the
  `dates=YYYYMMDD` bucket for day D holds events running through `D+1 04:59Z`.
  The sweep anchored on `now(utc).date()` and walked forward, so for those five
  hours it never queried the bucket holding the games in progress or about to
  start. Verified live at 2026-07-26T01:25Z: `dates=20260725` still held
  Tottenham Hotspur at Auckland FC (2026-07-26T03:00Z, a favorite, 95 minutes
  from kickoff) while `dates=20260726` held nothing before 12:00Z. The sweep now
  starts one day earlier. Worst for the Americas and the Pacific, where prime
  time falls inside the blind window. Roughly a dozen other sources share this
  anchor and are tracked separately in #154.
- **Stale fixtures could enter the guide as upcoming games.** Reaching one
  bucket backwards exposed matches that ESPN leaves tagged SCHEDULED long after
  full time (its status lag is why the Wrexham kickoff still read SCHEDULED two
  hours in). Those sailed past the FINISHED filter and would have sorted to the
  top of the guide, since channels are numbered by kickoff time. Fixtures that
  kicked off more than six hours ago are now dropped, which keeps in-progress
  games while discarding yesterday's leftovers.

### Changed
- **`sources/friendlies.py` refactored onto a shared `_EspnFriendliesBase`.**
  The per-day ESPN sweep, the FINISHED drop, the favorites gate, and the
  `GameRow` shape now live in one place, with `InternationalFriendliesSource`
  and `ClubFriendliesSource` supplying only the competition slug, prefix, and
  label. Aside from the two sweep fixes above (which the international sources
  also receive), no behavior change to the international sources.
- **`friendlies_favorites_only` relabelled** from "favorite national teams only"
  to "favorites only", since it now governs the club toggle too.
- **Friendlies test fixtures now use relative timestamps.** They carried
  hardcoded 2026-05-31 / 2026-06-09 dates that had silently aged into "eight
  weeks in the past", which the new staleness floor would have filtered out.
  Absolute dates in a fixture are a time bomb; these are now offsets from now.

## [1.12.0] - 2026-07-19

### Added
- **Trophy grounding for knockout previews (`honours.py` + `honours.json`).**
  For knockout games in the three tracked competitions (World Cup, Euros,
  Champions League) the preview context now carries each finalist's real title
  count, e.g. `Honours (World Cup): Spain — 1 title (2010); Argentina — 3 titles
  (1978, 1986, 2022)`. This is the fix for an LLM preview of the Spain v
  Argentina World Cup final that claimed a side was "going for their third
  crown" — false for both (Argentina already had three, Spain one). The model
  now has the true numbers to ground on; the existing "ground every fact"
  system-prompt rule does the rest. For a final the line also spells out the
  conclusion ("a win would be their 4th") because, given only raw counts, Haiku
  4.5 still miscounted (it wrote "three titles apiece" for Spain 1 / Argentina
  3); stating the outcome removes the arithmetic the model got wrong.
  National-team competitions list every
  winner, so a trophyless finalist is stated as "no World Cup titles yet"
  rather than left silent; the Champions League list is partial, so unlisted
  clubs are omitted rather than falsely called titleless.

### Changed
- **A final's subtitle/headline no longer appends the closeness descriptor.**
  The Spain v Argentina final rendered as `Final · close spread`; for a final
  the stage is the headline and "close spread" is redundant (and a category
  error for soccer, which has no point spread). The descriptor is now
  suppressed for finals only (any sport); semifinals and earlier keep it.

## [1.11.1] - 2026-07-11

### Fixed
- **Field-event titles ending in a number no longer match every stream.** The
  EPG keyword pre-filter relaxes a name down to its last word so abbreviated
  titles still hit, but a bare number is a substring of a huge fraction of
  channel names and numbers. A UFC rematch card, `UFC 329: McGregor vs.
  Holloway 2`, reduced to the keyword `2` and matched 8214 unrelated streams
  (NASCAR, BMX, MLB feeds, `TVA Sports 2`, `Dota 2`, ...). The last-word
  fallback now skips weak tokens (a bare number, or a 1-2 char token such as a
  Roman-numeral rematch marker), so the card matches only genuine `UFC 329`
  feeds and the broadcasters actually airing it. This is a shared-matcher
  backstop, so every field-event source is protected, not just the boxing
  source that sanitised its own titles. Not gated on `enable_boxing`; the bug
  affected UFC (and any field event) independently.

## [1.11.0] - 2026-07-10

### Added
- **Boxing source** (`enable_boxing`). Professional boxing cards surface as
  one field-event entry per card (same shape as UFC), sourced from the Boxing
  Data API (RapidAPI) via a new `boxing_data_api_key` setting. ESPN has no
  boxing feed, so boxing requires this key; enabled-but-unkeyed is a logged
  no-op. Cards default to "Event" tier; explicit world-title cards get "Major".
  Cancelled cards are dropped. The free tier looks ahead about 7 days, so the
  boxing lookahead is clamped to 7. Boxing gets a widened EPG match window
  because the feed's start times are unreliable (date-only placeholders + naive
  datetimes); name specificity, not the clock, drives matching. Covers boxing
  only: UFC/MMA remains the separate `enable_ufc` toggle, and kickboxing /
  bare-knuckle have no free feed.

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
