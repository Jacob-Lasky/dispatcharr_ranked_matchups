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
| `favorite` | One of YOUR favorite teams is playing | 6.0 (flat) |
| `close_game` | Coinflip-ness in [0, 1] — soccer uses devigged h2h moneylines, NCAAF/NCAAM normalize point spread | 3.0 |
| `importance` | Lahvička Monte Carlo: \|Kendall tau-c\| × consequence weight, summed over playing teams AND in-league favorites' outcome bands. Soccer leagues: title / UCL / Europa / relegation / promotion. UCL knockouts: round_of_16 → quarterfinal → semifinal → final → winner. NCAAF / NCAAM: win-count bands (bowl_eligible / 10+ / 11+; 15+ / 20+ / 25+). NHL: standings-point bands (95+ bubble / 100+ secured / 110+ division / 125+ Presidents'); Stanley Cup Playoffs: R2 → Conf Final → Cup Final → Champion. Locked outcomes contribute 0; uncertainty drives leverage. | 3.0 |
| `tournament_stage` | Knockout cup game (R16, QF, SF, F) | 1.5 |
| `rivalry` | Known rivalry game (rivalry DB pending) | 2.0 (flat) |
| `narrative` | LLM-judged narrative bonus (off by default) | 0.0 |

Raw signal contributions are summed and compressed to a 0-10 ★ score using a
tanh curve so top games asymptote without losing differentiation among the
rest.

## Why I built this

My background is in data science and I love sports, and I'm pretty agnostic to
what sports I watch. But I'm busy enough that it's hard to keep track of every
league and every championship and... well, you get it. So I figured it would
be cool to get a curated list of the best games that are on right now, and
thus this plugin was born.

It's **deterministic** (it uses math to get the same answer each time) and
**tunable** so you can increase or decrease the weight of specific signals.

The most interesting part, for my fellow nerds, is from a fascinating paper
(Lahvička, J. (2012). *Football match importance via a contingency-table
coefficient*) that strives to answer how important game X is to team Y's
season. Or, in plain language: "in 1,000 simulated alternate universes, how
much does the way this match goes shift the probability of this team hitting
this outcome?" The plugin runs that simulation per match per refresh. Full
math walk-through in [SCORING.md](SCORING.md).

I wanted to release this before the World Cup and I am completely open to
feedback. I hope you use this plugin and that it helps raise your favorite
teams and interesting games to the very top of your focus. Happy watching!

### Community

Released on the Dispatcharr Discord: [tools-and-addons announcement post](https://discord.com/channels/1340492560220684331/1508938899865604167/1508938899865604167).
Feedback, sport-coverage requests, signal-tuning experiences, and bug reports
all welcome there, or as a GitHub issue here.

### AI disclaimer

This was written with help from AI. The math and ideas are mine;
implementation and testing are done by AI.

## Example output

```
#9000  EPL ★10.0 · Brentford at Manchester United · title / UCL race
       — both top-10 (#3 vs #9), title / UCL race, toss-up (line +0.5)

       Description (what TiviMate/Plex/Jellyfin show):
         Kickoff: Today 2:00 PM CDT 🔴 TODAY
         Matchup: Brentford FC @ Manchester United FC
         Sport: English Premier League
         Score: 10.0/10  (raw 28.3)
         Score breakdown:
           rank_pair: +7.92
           close_game: +3.38
           importance: +17.0
         Source channel: Manchester United

#9002  EFL ⭐★10.0 · Middlesbrough at Wrexham · playoff / auto-promotion race
       — both top-10 (#4 vs #6), favorite (Wrexham),
         playoff / auto-promotion race, toss-up (line +0.2)

#9100  CFB ★9.2 · Ohio State (5) at Penn State (1) · top-5 showdown
       — poll-ranked leagues show the rank inline after each team
```

Today's games are sorted to the front (lowest channel numbers) so they appear
first in any IPTV client.

The channel name is fully customizable (see "Channel Naming" in settings). The
default renders as above: poll ranks appear inline after each team, and any
empty field (an unranked team, a game with no tagline) collapses cleanly. Use
the **Test naming convention** action to preview a template before applying it.

## Sports supported

| Sport | Source | Free tier? |
|---|---|---|
| NCAA Football | [CollegeFootballData.com](https://collegefootballdata.com/) | Yes (1k req/day) |
| NCAA Men's Basketball | [CollegeBasketballData.com](https://collegebasketballdata.com/) (same key as CFBD) | Yes |
| EPL / EFL Championship / UCL / Bundesliga / La Liga / Serie A / Ligue 1 / FIFA World Cup / UEFA EURO | [Football-Data.org](https://www.football-data.org/) | Yes (10 req/min, 12 free comps) |
| NHL (regular + Stanley Cup Playoffs) | [api-web.nhle.com](https://api-web.nhle.com/) (official, undocumented) | Yes (no key required) |
| MLB (regular + postseason) | [statsapi.mlb.com](https://statsapi.mlb.com/) (official, undocumented) | Yes (no key required) |
| NBA (regular + playoffs) | [site.api.espn.com](https://site.api.espn.com/) (unofficial — stats.nba.com WAF-blocks most homelab egress) | Yes (no key required) |
| MLS (regular + Cup playoffs) | [site.api.espn.com](https://site.api.espn.com/) for schedule + [The Odds API](https://the-odds-api.com/) (`soccer_usa_mls`) for closeness | Yes (Odds API free tier; ESPN no key) |
| NCAA Baseball (D1 regular season) | [site.api.espn.com](https://site.api.espn.com/) (unofficial) + D1Baseball.com poll | Yes (no key required) |
| NCAA Soccer — Men's + Women's (D1 regular season) | [site.api.espn.com](https://site.api.espn.com/) (unofficial) + United Soccer Coaches Top 25 | Yes (no key required) |
| Spreads (any sport above) | [The Odds API](https://the-odds-api.com/) | Yes (500 req/mo) |

Adding a sport is a new file in `sources/` implementing the `SportSource`
interface; everything else (scoring, matching, channel cloning, EPG
descriptions) is sport-agnostic.

## Roadmap

Open work is tracked in [GitHub issues](../../issues). Notable themes:

- **Rivalry signal** — the `weight_rivalry` weight is wired but no
  source populates `is_rivalry`. See [#8](../../issues/8).
- **Matcher v2** — soccer EPG match rate is low because UK providers
  publish broadcast EPG only 24-48h ahead and team-name variants
  ("Wrexham" / "Wrexham AFC" / "AFC Wrexham") trip the regex
  pre-filter. See [#4](../../issues/4).
- **NCAA Baseball / Softball postseason brackets** — Regional
  double-elim and the 8-team MCWS/WCWS brackets need chronological
  bracket inference. See [#43](../../issues/43).
- **WC 2026 group stage importance** — group-stage games currently
  read importance=0. See [#20](../../issues/20).

PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for a step-by-step
guide. For requests, [open an issue](../../issues/new/choose) and the
form will collect everything needed to scope it.

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

5. **Point your client at the right URLs (required).** On the Channels page,
   use the **M3U** and **EPG** buttons to generate URLs, and on BOTH set
   **TVG-ID Source = TVG-ID** (not the default *Channel Number*). Then load
   those URLs in TiviMate / Plex / your client.

   > **Why this matters.** These channels are renumbered by ★ score on every
   > refresh, so a given channel *number* maps to a different game over time.
   > With the default `tvg_id_source=channel_number`, your client binds the
   > guide to channels by that volatile number, and because Dispatcharr caches
   > the EPG separately from the playlist, a post-refresh renumber pairs the
   > new channel's name with the previous cycle's program for that number
   > (e.g. an "Ole Miss at North Carolina" channel showing the "Iran vs New
   > Zealand" guide entry). The plugin already writes a stable per-game
   > `tvg_id`, so `TVG-ID Source = TVG-ID` binds name and guide by that stable
   > id and is immune to the reshuffle. Both the M3U and EPG URLs must use the
   > same source.

## Pipeline

| Action | What it does | Writes |
|---|---|---|
| `refresh` | Pull upcoming games from each enabled sport, score each, run EPG-to-channel matching, save curated list. | `cache.json` |
| `apply` | Create / update virtual channels in the target group, link to source-channel streams, write `ProgramData` descriptions, delete stale ones. | DB (honors `dry_run`) |
| `auto_pipeline` | `refresh` + `apply`. The scheduler runs this; the button triggers it on demand. | Both |
| `show_status` | Print the current curated list with per-game score breakdown. No writes. | — |
| `preview_names` | Render the channel-name template against sample games so you can check the layout before applying. Reports template errors and lists every variable. No writes. | — |

## How channels are created

The plugin keeps your source channels untouched. Instead it creates **virtual
channels** in a target ChannelGroup (default `Top Matchups`; tip: prefix with
`!` to sort to the top of your group list):

- Channel name: rendered from a customizable template (default:
  `{league_short} {favorite_star}★{score} · {away_team}{ (rank_away)} at {home_team}{ (rank_home)}{ · tagline}`).
  Plain text is literal; a `{group}` collapses entirely when its variable is
  blank. Set your own under "Channel Naming"; preview with `preview_names`.
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
in `LEAGUE_CONTEXTS` (e.g., `"PL"`, `"ELC"`) so the importance signal knows
your league's outcome thresholds and consequence weights. The simulator
needs `supports_importance=True` on the `SportSource` plus the 7-method
Monte Carlo interface (`estimate_strengths`, `initial_state`,
`remaining_matches`, `sample_result`, `apply_result`, `terminal_outcomes`,
`outcome_labels`) — see `sources/soccer.py` for the canonical impl.

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
    "close_game": 2.89,
    "importance": 17.0
  },
  "score_notes": [
    "both ranked: #3 vs #9 (sum=12)",
    "implied coinflip-ness: 0.96",
    "importance: Manchester United UCL: 0.65 leverage × 4.0 = 2.60",
    "importance: Brentford Europa/Conference: 0.42 leverage × 2.0 = 0.84",
    "importance: Manchester City title: 0.51 leverage × 5.0 = 2.55"
  ]
}
```

If a game ranks higher / lower than your gut says, the breakdown shows you
exactly which signal to nudge.

## License

MIT — see LICENSE.
