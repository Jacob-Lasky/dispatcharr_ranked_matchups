# Dispatcharr Ranked Matchups

**Never miss a good game.**

A [Dispatcharr](https://github.com/dispatcharr/dispatcharr) plugin that watches
39 leagues, tours and competitions (22 of them soccer, plus NFL, NBA, MLB, NHL,
NCAA, UFC, boxing, tennis, golf and motorsport), scores every upcoming game on
**interestingness**, and curates the best of them into a single "Top Matchups"
group, with the WHY shown directly in each channel's EPG description.

What "interesting" means is computed transparently from a handful of signals
you can tune:

| Signal | Fires when | Default weight |
|---|---|---|
| `rank_pair` | Both teams ranked / one ranked | 1.0 |
| `favorite` | One of YOUR favorite teams is playing | 6.0 (flat) |
| `close_game` | Coinflip-ness in [0, 1] — soccer uses devigged h2h moneylines, NCAAF/NCAAM normalize point spread | 3.0 |
| `importance` | Lahvička Monte Carlo: \|Kendall tau-c\| × consequence weight, summed over playing teams AND in-league favorites' outcome bands. Soccer leagues: title / UCL / Europa / relegation / promotion. UCL knockouts: round_of_16 → quarterfinal → semifinal → final → winner. NCAAF / NCAAM: win-count bands (bowl_eligible / 10+ / 11+; 15+ / 20+ / 25+). NHL: standings-point bands (95+ bubble / 100+ secured / 110+ division / 125+ Presidents'); Stanley Cup Playoffs: R2 → Conf Final → Cup Final → Champion. Locked outcomes contribute 0; uncertainty drives leverage. | 3.0 |
| `tournament_stage` | Knockout cup game (R16, QF, SF, F). Domestic-cup early rounds (EFL Cup, FA Cup) have their own lower band ramping to just under the quarterfinal, so a second-round tie sorts below a marquee league game | 1.5 |
| `rivalry` | Known rivalry game, from the bundled `rivalries.json` | 2.0 (flat) |
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
#5548720  EPL ★10.0 · Brentford at Manchester United · title / UCL race
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

#5550642  EFL ⭐★10.0 · Middlesbrough at Wrexham · playoff / auto-promotion race
          — both top-10 (#4 vs #6), favorite (Wrexham),
            playoff / auto-promotion race, toss-up (line +0.2)

#5553521  CFB ★9.2 · Ohio State (5) at Penn State (1) · top-5 showdown
          — poll-ranked leagues show the rank inline after each team
```

Today's games are sorted to the front (lowest channel numbers) so they appear
first in any IPTV client. The numbers above are the default kickoff-time
scheme; see [Channel numbering](#channel-numbering) for the compact alternative
(`400-424` instead of seven digits) and what it costs.

The channel name is fully customizable (see "Channel Naming" in settings). The
default renders as above: poll ranks appear inline after each team, and any
empty field (an unranked team, a game with no tagline) collapses cleanly. Use
the **Test naming convention** action to preview a template before applying it.

## Sports supported

| Sport | Source | Free tier? |
|---|---|---|
| NCAA Football (D1 FBS; FCS optional) | [CollegeFootballData.com](https://collegefootballdata.com/) | Yes (1k req/day) |
| NCAA Men's Basketball | [CollegeBasketballData.com](https://collegebasketballdata.com/) (same key as CFBD) | Yes |
| EPL / EFL Championship / UCL / Bundesliga / La Liga / Serie A / Ligue 1 / Eredivisie / Primeira Liga / Brazilian Série A / FIFA World Cup / UEFA EURO | [Football-Data.org](https://www.football-data.org/) | Yes (10 req/min, and those 12 competitions ARE the free tier — every domestic cup is paid-only) |
| NHL (regular + Stanley Cup Playoffs) | [api-web.nhle.com](https://api-web.nhle.com/) (official, undocumented) | Yes (no key required) |
| MLB (regular + postseason) | [statsapi.mlb.com](https://statsapi.mlb.com/) (official, undocumented) | Yes (no key required) |
| NBA (regular + playoffs) | [site.api.espn.com](https://site.api.espn.com/) (unofficial — stats.nba.com WAF-blocks most homelab egress) | Yes (no key required) |
| MLS (regular + Cup playoffs) | [site.api.espn.com](https://site.api.espn.com/) for schedule + [The Odds API](https://the-odds-api.com/) (`soccer_usa_mls`) for closeness | Yes (Odds API free tier; ESPN no key) |
| NCAA Baseball (D1 regular season) | [site.api.espn.com](https://site.api.espn.com/) (unofficial) + D1Baseball.com poll | Yes (no key required) |
| NCAA Soccer — Men's + Women's (D1 regular season) | [site.api.espn.com](https://site.api.espn.com/) (unofficial) + United Soccer Coaches Top 25 | Yes (no key required) |
| EFL Cup (Carabao Cup) / FA Cup | [site.api.espn.com](https://site.api.espn.com/) (unofficial) — Football-Data.org gates every domestic cup behind a paid plan | Yes (no key required) |
| Soccer friendlies — international (men's + women's) and club pre-season | [site.api.espn.com](https://site.api.espn.com/) (unofficial) | Yes (no key required) |
| Field events — Formula 1 / NASCAR / PGA Golf / ATP + WTA Tennis / UFC | [site.api.espn.com](https://site.api.espn.com/) (unofficial) | Yes (no key required) |
| Boxing | [Boxing Data API](https://rapidapi.com/bengroves1993/api/boxing-data-api) (RapidAPI — ESPN has no boxing feed) | Yes (free RapidAPI tier; ~7-day lookahead) |
| Spreads (any sport above) | [The Odds API](https://the-odds-api.com/) | Yes (500 req/mo) |

### NCAA Football divisions

CFBD's feed carries every NCAA division, not just Division I. The
**NCAA Football divisions** setting selects what actually reaches the guide:

| Setting | What is pulled |
|---|---|
| `D1 FBS only` (default) | Games where either side is FBS, including FBS-vs-FCS |
| `D1 FBS + FCS` | The above, plus FCS-vs-FCS |

Division II and III are never pulled under either setting. They are not
merely uninteresting: the Monte Carlo importance simulation replays only the
selected divisions' season, so a team outside it scores 0.00 every time while
still costing roughly seven seconds of simulation. Opening week 2026 returned
157 games in a seven-day window of which 27 involved an FBS team, so the
unfiltered feed spent about eighteen minutes per refresh to add nothing.

FCS teams stay unranked because the AP Top 25 is an FBS poll, and an
FBS-vs-FCS game is ranked on its FBS team. Note that unranked does not mean
unscored: the Monte Carlo importance signal is independent of the poll, so
an FCS game can still score on stakes alone. Under `D1 FBS + FCS` those
stakes are measured with FBS win-count bands (6 wins = bowl eligible,
10+ = playoff contender), which FCS's own postseason does not work that
way — treat FCS importance as an approximation rather than a like-for-like
comparison with FBS. This is why FBS-only is the default.


Adding a sport is a new file in `sources/` implementing the `SportSource`
interface; everything else (scoring, matching, channel cloning, EPG
descriptions) is sport-agnostic.

## Roadmap

Open work is tracked in
[GitHub issues](../../issues?q=is%3Aissue+is%3Aopen), which is the single
source of truth. DO NOT restate the current roadmap here: an inline list
of issue numbers goes stale silently and every item in the previous one
(#4, #8, #20, #43) had shipped and closed while the README still
advertised it as pending.

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
     echo "<Boxing Data key>"   > /data/plugins/dispatcharr_ranked_matchups/boxing_data_api_key
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

5. **Load the M3U/EPG URLs (or connect via Xtream Codes) in your client.** On
   the Channels page, use the **M3U** and **EPG** buttons (or point your client
   at the Xtream Codes API). No special TVG-ID setting is required, in either
   the default M3U/EPG output or the Xtream Codes API.

   > **Why it sorts soonest-first and the guide stays correct.** (This describes
   > the default **Kickoff time** numbering; if you switch to **Compact range**,
   > read [Channel numbering](#channel-numbering) first — the guarantee below is
   > exactly what that mode trades away.) Each channel's
   > *number* is derived from its kickoff time: `virtual_base + minutes-since-a-
   > fixed-origin × slots + a small per-game tiebreak`. So the list sorts
   > strictly by day then start time (live/upcoming games first), and — crucially
   > — every game keeps the **same number for its whole life**. Finished games
   > drop off and new games slot into their time-position on their own, which
   > looks like "renumbering every refresh" but no existing game's number ever
   > moves. Because the number is stable, the guide binds to the right game even
   > though clients cache the EPG separately from the channel list, in both the
   > default output and the Xtream Codes API (both bind by the integer channel
   > number). The numbers are large (time-encoded) by design; that is what keeps
   > them stable and integer-clean (Xtream Codes requires integer channel
   > numbers and floors fractional ones, which would scramble the order).

## Pipeline

| Action | What it does | Writes |
|---|---|---|
| `refresh` | Pull upcoming games from each enabled sport, score each, run EPG-to-channel matching, save curated list. | `cache.json` |
| `apply` | Create / update virtual channels in the target group, link to source-channel streams, write `ProgramData` descriptions, delete stale ones. | DB (honors `dry_run`) |
| | *Waits for the result and shows the real summary. Runs in a separate process so it cannot stall the web worker, which costs a second or two of startup.* | |
| `auto_pipeline` | `refresh` + `apply`. The scheduler runs this; the button triggers it on demand. | Both |
| `show_status` | Print the current curated list with per-game score breakdown. No writes. | — |
| `preview_names` | Render the channel-name template against sample games so you can check the layout before applying. Reports template errors and lists every variable. No writes. | — |
| `reap_now` | Drop games that finished more than `remove_finished_after_minutes` ago and backfill from the bench. Re-fetches nothing. | `cache.json` + DB |

### Clearing out finished games

By default a game's channel stays in the group until the next scheduled
refresh, which on a five-a-day schedule can be hours after the final whistle.
Two settings change that, both off by default:

| Setting | Effect |
|---|---|
| `remove_finished_after_minutes` | Minutes past a game's estimated end before its channel is removed. `0` disables reaping. |
| `bench_size` | How many extra scored games to keep in reserve, ready to replace ones that finish. `0` means the group just shrinks through the day. |

The end of a game is **estimated**, because no feed publishes one: the plugin
reuses the same per-sport window the EPG matcher uses, roughly 4 hours for
gridiron and 2.5 for soccer. So `remove_finished_after_minutes = 30` means
about four and a half hours after a college football kickoff, not thirty
minutes. A game whose start time cannot be parsed is never reaped.

That same estimate bounds the game's guide entry — or four hours, whichever is
shorter, since a few of those windows are a generous matching tolerance rather
than an event length (boxing's is 24 hours) — and the post-game "Final: ..."
listing ends exactly when the channel is removed rather than lingering until the
next refresh. It matters more than it sounds: the guide identifies a channel by
its number, so a listing that outlived its channel would be read as belonging to
whichever game took that number next. For the same reason, lowering
`remove_finished_after_minutes` does not strand anything: a game is held until
the entry already published for it has ended.

With a bench, finishing games are replaced by the next-best scored games that
have not started yet and do have a broadcast to point at, so the group holds
its size instead of draining. Those games were already fetched and scored
during the last refresh, so promotion costs no API calls and no simulation.

A background thread handles this: it sleeps until the next game's deadline
rather than polling, and falls back to a half-hourly heartbeat when there is
nothing pending. Use the `reap_now` action to trigger it by hand.

## How games are matched to your lineup

A scored game is matched against three independent sources, all of which can
contribute (results are merged and stacked as fallback streams):

| Path | Keys on | Typical shape |
|---|---|---|
| A | EPG **programme title, sub-title or description**, inside the game's broadcast window | `FOX Sports 1` airing "MLB: Yankees at White Sox"; or ORF's `FIFA Fußball WM 2026` whose sub-title reads `Gruppe F: Schweden - Tunesien` |
| B | **Channel name** | `EPL01: Manchester United 20:00 Brentford` |
| C | **Stream name**, whether or not the stream is on a channel | `USA Soccer10: Iran vs New Zealand` on a generic parent channel |

Path C is why you do not have to curate a stream into a channel first: streams
are queried directly, and only the matched stream is attached, not its parent
channel's unrelated feeds.

### Controlling which streams get used, and in what order

Because Path C sweeps your whole M3U, a matchup channel can pick up feeds you
never promoted to channels: foreign-language backups are the common case. Three
settings decide what happens to them, and none of them removes a stream unless
you say so:

| Setting | Effect |
|---|---|
| **Preferred languages** | Ordered list of language codes (`en`, or `de, en`). Streams in an earlier-listed language play first. Nothing is removed. |
| **Demote stream groups** | Streams from these channel groups sort behind everything else. They stay playable as a last resort. |
| **Exclude stream groups** | Streams from these groups are never attached at all, not even as a fallback. |

Streams you have attached to a channel of your own always sort ahead of ones
found only by the Path C sweep, so the lineup you curated leads by default and
raw M3U inventory backs it up. Your own matchup channels do not count as
curation.

**Language is read from the stream name, so it is best-effort.** A `DE:` or
`MX |` tag, a broadcaster like ZDF or Telemundo, an `English Feed` label, or
accented spelling all resolve; a name with no such marker does not. The honest
limit: a German Bundesliga feed named `Bayern Munich vs Dortmund` is
character-for-character what an English one would be called, because the
canonical English club names *are* the German ones. Measured against one real
provider's German sports group, 8 of 10 per-match Bundesliga feeds carried no
detectable language signal at all. **For those, use Demote stream groups**,
which keys on the group rather than the name and is exact.

Path A reads the sub-title and description as well as the title because
European public broadcasters (ORF, ARD, ZDF, SRF) title the programme with the
competition and put the fixture underneath it. Team names are matched in German
and Spanish as well as English, so `Frankreich` and `Francia` both find
`France`. A **description** must name *both* sides to count, since match reports
name other teams in passing; a title or sub-title only needs one.

Three rules keep this from over-matching, all worth knowing if a channel you
expected does not appear:

- **Both teams must appear in one segment of the name.** Names are split on `:`
  and `|` (a `:` inside a clock time does not split), and a channel or stream
  only matches when a single segment names both sides. Providers put a network
  label in front of the matchup, and without this rule the label supplies a
  bogus hit: `USA Soccer07: Australia vs Turkey` is the Australia-vs-*Turkey*
  feed, but the `USA` in its label made it match Australia vs United States.
- **Short abbreviations match as whole words.** `team_aliases.json` carries
  broadcast abbreviations, and up to four characters they must stand alone as a
  token. As loose substrings they behave like wildcards: `NE` is inside
  Sportsnet and Tennessee, `PIT` is inside Jupiter, `CLE` is inside Clearwater.
  Longer names still match as substrings.
- **A stream whose name is dated before the game is skipped.** Providers spin up
  a dedicated feed per night, and a three-game series has every night's feed
  naming the same two teams, so without this the channel stacks finished games
  behind the live one. Feeds with no date in the name are unaffected, and a
  feed dated later than the game is kept.

When several feeds carry the same event, a plain feed is preferred over one that
labels itself as the pre-show, the prelims, a press conference or a multiview.
Those still attach as fallbacks; they just do not become the primary stream.

Games with no match above the placeholder threshold still get a channel; see
[Placeholder channels](#placeholder-channels).

## How channels are created

The plugin keeps your source channels untouched. Instead it creates **virtual
channels** in a target ChannelGroup (default `Top Matchups`; tip: prefix with
`!` to sort to the top of your group list):

- Channel name: rendered from a customizable template (default:
  `{league_short} {favorite_star}★{score} · {away_team}{ (rank_away)} at {home_team}{ (rank_home)}{ · tagline}`).
  Plain text is literal; a `{group}` collapses entirely when its variable is
  blank. Set your own under "Channel Naming"; preview with `preview_names`.
- Streams: cloned via `ChannelStream` from the matched source channel, so
  playback works. Some providers bundle dedicated per-matchup feeds onto one
  channel, so when a matched channel's stream names mention teams, only the
  streams naming *this* game are attached. A plain broadcaster channel whose
  streams name no team at all (`MLB Network HD`, `MLB Network FHD`) contributes
  every stream, unchanged
- EPG: an inactive `xmltv` `EPGSource` (auto-created with the same name as the
  group) holds one `EPGData` row per virtual channel, with a `ProgramData` entry
  whose `description` shows the full WHY breakdown — TiviMate, Plex, and
  Jellyfin all surface this natively. It is deliberately not a `dummy` source:
  Dispatcharr overlays generated filler programming on every channel attached to
  one of those, which would bury the real descriptions
- Logo: a real per-game matchup image rather than the provider's channel logo,
  downloaded once to `/data/logos/`. TheSportsDB's pre-rendered event thumbnail
  first, then the league or tournament badge, then, last, the matched source
  channel's logo. Field-event sports (F1, golf, UFC, tennis, boxing) have no
  opponent pair, so they start at the badge tier.

  **Optionally** a fourth tier slots in between the thumbnail and the badge:
  [game-thumbs](https://github.com/sethwv/game-thumbs) composites the two teams'
  crests on demand, covering the leagues SportsDB never indexed a graphic for
  (MLS, Brazilian Serie A, NCAA soccer). On one live 23-game slate it took the
  list from 6 real matchup images to 23. It is **off by default** and enabling it
  points your instance at a third-party service, so the recommended way in is to
  self-host `ghcr.io/sethwv/game-thumbs` and set "game-thumbs base URL" to your
  own instance. Leave the field blank and no request is made to anyone
- Channel number: two schemes, picked by the "Channel numbering" setting.
  **Kickoff time** (default) derives the number from the game's start time, so
  the list sorts soonest-first and no game's number ever changes. The numbers
  are large, around 7 digits. **Compact range** keeps every channel inside a
  band you name, e.g. 400-424, provided that stretch of your lineup is free —
  see "Channel numbering" below
- Channel Profiles: the channels are enabled in every Channel Profile by
  default. Narrow that with "Channel Profiles to enable" (see below)

If you rename the target group, the next apply detects the old group + its
virtual channels (by tvg_id marker `ranked_matchups:`) and migrates them.

### Channel numbering

Two schemes, set by **Channel numbering**. Both keep the guide honest; what
differs is what the numbers look like and how much of your lineup they need.

**Kickoff time** (default) builds the number out of the game's start time:
`starting number + minutes-since-a-fixed-origin × slots + a small per-game
tiebreak`. Two consequences. The list sorts strictly by day then start time, and
a game's number is fixed for its entire life, so no number is ever handed on. The
cost is that the numbers are large, around 7 digits, and the **Starting channel
number** setting is only a floor: the time-based term is in the millions, so
setting it to 400 lands you near 5,548,000 rather than at 400.

**Compact range** keeps every channel inside a band you name. Set **Starting
channel number** to 400 and the group runs 400, 401, 402 and up. **Compact range
size** left at its default of 0 reserves exactly as many numbers as you have
channels, so 400 with 25 channels is 400-424 and nothing else; raise it only if
you want headroom. A published game holds its slot until it finishes, so numbers
do not shuffle underneath you on a refresh.

**Pick a stretch of your lineup that is empty.** Channel numbers identify a
channel to the guide *on their own*, with no group attached, so if one of your
existing channels already sits at 401 and we put a game there too, the guide
cannot tell them apart and both break. We skip any number in the range that is
already taken and log how many, but if the range you chose is crowded there will
not be room for every game.

When a game finishes, its channel is removed and the next game takes that
number. That is safe: every programme we publish for a game ends no later than
the moment its channel goes away, so a player holding a guide it downloaded
earlier will show *nothing* on that number until it refreshes, never the wrong
match. Getting that wrong is what made channel numbers seven digits long in the
first place (see #117, #204).

If you use the M3U and XMLTV URLs rather than the Xtream Codes API, you can take
the channel number out of the guide entirely: set **TVG-ID Source = TVG-ID** on
both URLs (the M3U and EPG buttons on the Channels page). The guide then binds by
a stable per-game id, so even a number shared with one of your existing channels
stops mattering. Xtream Codes has no equivalent setting; it binds the guide to
the integer channel number and that is not configurable.

### Channel Profiles

Dispatcharr creates profile memberships in its API layer, for channels made
through its own UI. A plugin creates channels through the ORM, so it gets none,
and a channel with no membership row is filtered out of every **named** profile
— it shows up only under `All`, which applies no membership filter at all. That
is why the curated channels used to appear switched off in each of your
profiles.

Apply now sets that membership itself:

- **Blank (the default): every profile**, which is what a hand-created channel
  gets in Dispatcharr.
- **A comma-separated list** (`Sports, Soccer`) restricts them to those
  profiles. Names are matched case-insensitively against the names shown under
  Channel Profiles.

The setting is the source of truth and it converges: profiles you list are
turned on and profiles you leave out are turned off, so removing a name here
removes the channels from that profile on the next apply. If you toggle one of
these channels by hand in the profile editor, the next apply will set it back.
A name matching no profile is reported in the Apply result rather than guessed
at, and nothing else is changed.

Only channels the plugin owns (tvg_id marker `ranked_matchups:`) are touched,
including ones created by earlier versions, so an existing install fixes itself
on the first apply after upgrading.

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
