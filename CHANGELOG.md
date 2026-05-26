# Changelog

All notable changes to this plugin are documented here. Format roughly
follows [Keep a Changelog](https://keepachangelog.com/) with semver.

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
