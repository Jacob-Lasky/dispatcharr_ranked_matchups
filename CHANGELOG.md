# Changelog

All notable changes to this plugin are documented here.

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
