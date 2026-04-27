# Dispatcharr Ranked Matchups

A [Dispatcharr](https://github.com/dispatcharr/dispatcharr) plugin that curates
upcoming sports games into a single "Top Matchups" group based on
**interestingness**, with the WHY shown directly in each channel's EPG
description.

What "interesting" means is computed transparently from a handful of signals
you can tune:

| Signal | Fires when | Default weight |
|---|---|---|
| `rank_pair` | Both teams ranked / one ranked | 1.0 |
| `favorite` | One of YOUR favorite teams is playing | 4.0 (flat) |
| `close_game` | Tight betting spread (smaller = tighter) | 0.5 |
| `stakes` | Either team near a meaningful league cutoff (playoff, relegation, title, UCL) — multiplied 1.5–2× late in the season | 2.0 |
| `tournament_stage` | Knockout cup game (R16, QF, SF, F) | 1.5 |
| `impact_on_favorite` | Non-favorite game whose outcome shifts a favorite's table position | 1.0 |
| `rivalry` | Known rivalry game (rivalry DB pending) | 2.0 (flat) |
| `narrative` | LLM-judged narrative bonus (off by default) | 0.0 |

Raw signal contributions are summed and compressed to a 0-10 ★ score using a
tanh curve so top games asymptote without losing differentiation among the
rest.

## Example output

```
#9000  EPL 3v9 ★10.0: Brentford FC at Manchester United FC
       — both top-10 (#3 vs #9), title / UCL race (final stretch),
         affects Manchester City FC, toss-up (line +0.5)

       Description (what TiviMate/Plex/Jellyfin show):
         Kickoff: Today 2:00 PM CDT 🔴 TODAY
         Matchup: Brentford FC @ Manchester United FC
         Sport: English Premier League
         Score: 10.0/10  (raw 28.3)
         Score breakdown:
           rank_pair: +7.92
           close_game: +3.38
           stakes: +16.0
           impact_on_favorite: +1.0
         Source channel: Manchester United

#9002  EFL 4v6 ⭐ ★10.0: Middlesbrough FC at Wrexham AFC
       — both top-10 (#4 vs #6), favorite (Wrexham),
         playoff / auto-promotion race (final stretch), toss-up (line +0.2)
```

Today's games are sorted to the front (lowest channel numbers) so they appear
first in any IPTV client.

## Sports supported

| Sport | Source | Free tier? |
|---|---|---|
| NCAA Football | [CollegeFootballData.com](https://collegefootballdata.com/) | Yes (1k req/day) |
| NCAA Men's Basketball | [CollegeBasketballData.com](https://collegebasketballdata.com/) (same key as CFBD) | Yes |
| EPL / EFL Championship / UCL | [Football-Data.org](https://www.football-data.org/) | Yes (10 req/min, 12 free comps) |
| Spreads (any sport above) | [The Odds API](https://the-odds-api.com/) | Yes (500 req/mo) |

Adding a sport is a new file in `sources/` implementing the `SportSource`
interface; everything else (scoring, matching, channel cloning, EPG
descriptions) is sport-agnostic.

## Roadmap

Sports / leagues on the to-do list (PRs welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for a step-by-step guide; for
requests, [open an issue](../../issues/new/choose) and the form will
collect everything needed to scope it):

- **NCAA Baseball** — no clean public API yet; options being evaluated:
  D1Baseball.com scrape, ESPN's hidden API, or a future
  CollegeBaseballData.com from the same author as CFBD/CBB.
- **NCAA Soccer** — same author publishes CollegeSoccerData; needs an
  adapter file + AP poll mapping.
- **MLB / NBA / NFL** — would use The Odds API for spreads + a
  rankings/standings source (TBD per sport).
- **More European football leagues** — La Liga, Serie A, Bundesliga,
  Ligue 1 are all on Football-Data.org's free tier. Each needs a
  `LEAGUE_CONTEXTS` entry in `scoring.py` with the right thresholds
  (UCL/Europa cutoff, relegation line) plus a one-line addition to the
  `COMPETITIONS` dict in `sources/soccer.py`.
- **Rivalry signal** — a `rivalries.json` shipped with the plugin (NCAA
  rivalries are well-known) and/or an LLM "is this a rivalry?" call
  cached per team-pair.
- **Matcher v2** — soccer EPG match rate is currently low because UK
  providers publish broadcast EPG only 24-48h ahead and team-name
  variants ("Wrexham" / "Wrexham AFC" / "AFC Wrexham") trip the regex
  pre-filter. Plan: team-alias dictionary, tighter time window, broader
  fuzzy match.

## Install

1. Clone the repo into your Dispatcharr plugins directory:

   ```bash
   docker exec dispatcharr git clone https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups.git \
       /data/plugins/dispatcharr_ranked_matchups
   ```

2. Stage your API keys (each as a file with `chmod 600`):

   ```bash
   docker exec dispatcharr sh -c '
     echo "<CFBD key>"          > /data/plugins/dispatcharr_ranked_matchups/cfbd_api_key
     echo "<Football-Data key>" > /data/plugins/dispatcharr_ranked_matchups/football_data_api_key
     echo "<Odds API key>"      > /data/plugins/dispatcharr_ranked_matchups/odds_api_key
     echo "<Anthropic key>"     > /data/plugins/dispatcharr_ranked_matchups/anthropic_api_key
     chmod 600 /data/plugins/dispatcharr_ranked_matchups/*_api_key
   '
   ```

   Anthropic key is only needed if you set `weight_narrative > 0` OR want
   LLM-resolved EPG matching when the regex pre-filter is ambiguous. For
   regex-only matching it's optional.

3. Open Dispatcharr → Plugins → enable **Ranked Matchups (Top Games)**, then
   in the plugin's settings:
   - Toggle which sports you want under **Sport Sources**
   - Set your **Favorite teams** (comma-separated) under **Curation**
   - Pick your **Local timezone** so "today" classification + EPG dates are
     right
   - Tune signal **Weights** if any feel under/over-weighted

4. Run **Refresh + apply now** to populate.

## Pipeline

| Action | What it does | Writes |
|---|---|---|
| `refresh` | Pull upcoming games from each enabled sport, score each, run EPG-to-channel matching, save curated list. | `cache.json` |
| `apply` | Create / update virtual channels in the target group, link to source-channel streams, write `ProgramData` descriptions, delete stale ones. | DB (honors `dry_run`) |
| `auto_pipeline` | `refresh` + `apply`. The scheduler runs this; the button triggers it on demand. | Both |
| `show_status` | Print the current curated list with per-game score breakdown. No writes. | — |

## How channels are created

The plugin keeps your source channels untouched. Instead it creates **virtual
channels** in a target ChannelGroup (default `Top Matchups`; tip: prefix with
`!` to sort to the top of your group list):

- Channel name: `<SPORT> <RANKS> ★<SCORE>: <AWAY> at <HOME> — <WHY>`
- Streams: cloned via `ChannelStream` from the matched source channel, so
  playback works
- EPG: a dummy `EPGSource` (auto-created with the same name as the group)
  holds one `EPGData` row per virtual channel, with a `ProgramData` entry
  whose `description` shows the full WHY breakdown — TiviMate, Plex, and
  Jellyfin all surface this natively
- Channel number: `9000 + cache_index`, so today's games occupy the lowest
  numbers and appear first in any IPTV client's default sort

If you rename the target group, the next apply detects the old group + its
virtual channels (by tvg_id marker `ranked_matchups:`) and migrates them.

## Placeholder channels

Games scoring above `placeholder_min_score` (default 5.0) get a virtual
channel **even if no Dispatcharr channel currently has an EPG entry for the
game**. The description marks it `[NOTE] No EPG match found yet — this is a
placeholder channel`. When the provider EPG eventually publishes the
broadcast info, the next refresh adds streams to the virtual channel and it
becomes playable.

This is what surfaces big upcoming games (e.g., Wrexham vs Middlesbrough on
Saturday) before the EPG catches up — typically UK soccer EPG is published
24-48 hours before kickoff.

## Sport-agnostic adapter interface

Adding a new sport is a new file in `sources/` that implements `SportSource`:

```python
from .base import GameRow, SportSource

class MyNewSource(SportSource):
    sport_prefix = "MLB"          # used in channel name
    sport_label = "MLB Baseball"  # used in EPG description

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        # ... return GameRow with ranks, start_time, spread, extra context
```

For league-based sports, populate `extra["fd_competition_code"]` to a code
in `LEAGUE_CONTEXTS` (e.g., `"PL"`, `"ELC"`) so the stakes signal knows your
thresholds.

## Scoring transparency

Every game's per-signal breakdown is in `cache.json`:

```json
{
  "home": "Manchester United FC",
  "away": "Brentford FC",
  "score": 10.0,
  "score_raw": 28.3,
  "score_breakdown": {
    "rank_pair": 7.92,
    "close_game": 3.38,
    "stakes": 16.0,
    "impact_on_favorite": 1.0
  },
  "score_notes": [
    "both ranked: #3 vs #9 (sum=12)",
    "betting spread: +0.5 pts",
    "standings stakes: thresholds=['title', 'UCL'], season_progress=98%, late_mult=2.0x",
    "affects favorite: Manchester City FC"
  ]
}
```

If a game ranks higher / lower than your gut says, the breakdown shows you
exactly which signal to nudge.

## License

MIT — see LICENSE.
