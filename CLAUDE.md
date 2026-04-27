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
  ncaaf.py        ← NCAA Football via CFBD
  soccer.py       ← EPL/EFL Championship/UCL via Football-Data.org + Odds API
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
| `plugin.py` | Plugin class + 4 actions: `refresh`, `apply`, `auto_pipeline`, `show_status`. Daemon scheduler. EPG lookup closure. Channel cloning + dummy EPGSource management. |
| `scoring.py` | `GameSignals`, `Weights`, `GameScore`. `score_game()` sums per-signal contributions, `_compress_to_10()` does the tanh squash. Helpers: `match_favorites`, `compute_team_stakes`, `compute_impact_on_favorites`, `build_why_text`, `format_channel_name`. League thresholds in `LEAGUE_CONTEXTS` dict. |
| `matcher.py` | `match_games_to_channels()` resolves cached `GameRow` → Dispatcharr channel via EPG `ProgramData`. Two-stage: regex (both team keywords in EPG title) → Claude batched fallback. |
| `sources/base.py` | `GameRow` dataclass + abstract `SportSource`. |
| `sources/ncaaf.py` | CFBD `/rankings`, `/games`, `/lines` calls. CFBD uses **camelCase** (homeTeam, awayTeam) — easy gotcha. |
| `sources/soccer.py` | Football-Data.org for fixtures+standings, The Odds API for spreads. League position used as rank. UCL doesn't use position-as-rank (knockout) — handled via tournament_stage signal. |

State (gitignored, lives in `<plugin_dir>/`):
- `cache.json` — last refresh result (curated game list with score breakdowns)
- `cfbd_api_key`, `football_data_api_key`, `odds_api_key`, `anthropic_api_key` — file fallback when settings field is blank.

## How channels are produced

Source channels are **never modified**. Apply creates virtual channels in a
target `ChannelGroup` (default `Top Matchups` — user has it as `!Top Matchups`
to sort to the top of group lists):

- `tvg_id` = `ranked_matchups:<SPORT>:<source_id>` — used for cleanup detection
  on next apply (any channel with this prefix in any group is "ours")
- `channel_number` = `9000 + cache_index` — today's games occupy lowest
  numbers, so any IPTV client's default sort puts them first
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
| `favorite` | One of user's favorite teams plays | 4.0 (flat) |
| `close_game` | Tight betting spread | 0.5 |
| **`stakes`** | Team near a league cutoff (playoff/relegation/title/UCL); 1.5–2× late-season multiplier | **2.0** |
| `tournament_stage` | Knockout cup game | 1.5 |
| `impact_on_favorite` | Non-favorite game whose outcome shifts a favorite's table position | 1.0 |
| `narrative` | LLM-judged narrative score | 0.0 (off by default) |

Late-season multiplier kicks in past 70% of season, doubles past 85%. EPL +
EFL Championship are 38 / 46 matchdays. The multiplier is a major reason
top games saturate at ★10 in late April / early May.

User asked us to default narrative weight to 0 because heuristic stakes /
tournament / impact-on-favorite cover what LLM narrative would surface
anyway. Don't enable narrative without explicit user buy-in.

## Sport adapter extension contract

To add NCAAM (College Basketball — same author as CFBD, free, same API key),
you'd:

1. Create `sources/ncaam.py` with a `NcaamSource` class implementing
   `SportSource`:
   ```python
   class NcaamSource(SportSource):
       sport_prefix = "CBB"
       sport_label = "NCAA Basketball"

       def fetch_upcoming(self, days_ahead=7) -> List[GameRow]:
           # call api.collegebasketballdata.com/rankings + /games
           # return GameRow with rank_home/rank_away from AP Top 25
   ```

2. Register in `sources/__init__.py`.

3. Add an `enable_ncaam` toggle to `plugin.json` and wire it in
   `_build_sources(settings)` in `plugin.py`.

Shared CFBD key already covers basketball (same Bearer token).

For league-based sports (where standings position is the "rank"), populate
`extra["fd_competition_code"]` to a code in `scoring.LEAGUE_CONTEXTS` so the
stakes signal knows your thresholds. NCAAF doesn't need this — AP Top-25
implicitly handles "near the top."

## Known gotchas / lessons learned

- **CFBD API is camelCase**: `homeTeam`, `awayTeam`, `startDate`,
  `neutralSite`, `excitementIndex`. We hit this once already — got 0 games
  because we used `home_team`. Their `/games` response also has the goldmine
  `excitementIndex` field for completed games (potential Phase 3+ training
  signal).
- **EPG self-matching bug**: matcher must exclude channels with our
  `tvg_id` prefix `ranked_matchups:` — otherwise it matches games against
  our own virtual channels (whose EPG titles literally contain the team
  names). Fixed in `_build_epg_lookup()`.
- **Group rename migration**: when user changes the target group name,
  apply detects old virtual channels (any group, by tvg_id prefix) and
  cleans them up + the orphaned dummy EPGSource. Don't break this — the user
  has a habit of renaming `Top Matchups` → `!Top Matchups` etc.
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
  the same Bearer token, both auto-skip during their respective offseason
- EPL + EFL Championship + UCL source (Football-Data.org + Odds API)
- All scoring signals (rank, favorite, spread, stakes, tournament-stage,
  impact-on-favorite, optional LLM narrative)
- Today-first sort + channel renumbering, with auto / fixed virtual base
- Placeholder channels for unmatched-high-scoring games
- Group-rename auto-cleanup
- Multi-time scheduler (`scheduled_times = "0400,1000,1600,2200"`)
- Both file-based and settings-based API keys (settings preferred, masked UI)
- Description includes kickoff time + WHY breakdown

**Known limitations & open work:** tracked as GitHub issues so they don't
drift. Quick map (most user-facing first):

- [#4 — Matcher v2: team aliases, time-window tightening, non-English
  titles](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/4)
- [#7 — Season-aware score normalization (★10 inflation +
  early-season flatness)](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/7)
- [#8 — Rivalry signal (`weight_rivalry` is wired but no source populates
  `is_rivalry`)](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/8)
- [#9 — Curation presets (high-curation / high-coverage instead of 9
  weight knobs)](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/9)
- [#10 — Standings deltas in EPG description (data is in
  `cache.json`, just not surfaced)](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/10)
- [#1 — Per-channel matchup JPG as virtual channel
  logo](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/1)
- [#3 — Label catch-up matchdays (40-of-46 rendering on a backlog
  fixture)](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/3)

Sport adapters not yet implemented:

- [#5 — NCAA Baseball
  adapter](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/5)
- [#6 — NCAA Soccer
  adapter](https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/issues/6)

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
