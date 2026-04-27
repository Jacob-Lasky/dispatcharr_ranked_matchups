# Contributing

Thanks for the interest. Most contributions fall into two buckets:

1. **Adding a soccer league** that Football-Data.org already covers
   (5-line config change).
2. **Adding a new sport** (writing a new adapter, ~150 lines).

Both paths below.

If you're not sure what's needed, file an
[Add a league issue](../../issues/new/choose) and we can scope it
together first.

---

## Adding a soccer league

Soccer leagues that Football-Data.org covers are config-only. Everything
downstream (scoring, EPG matching, channel cloning, descriptions) is
sport-agnostic. Two files, ~10 lines total.

Football-Data's free tier covers EPL, EFL Championship, UCL, La Liga,
Bundesliga, Serie A, Ligue 1, Eredivisie, Primeira Liga, and more. Full
list at https://www.football-data.org/coverage.

### Step 1: register the competition (`sources/soccer.py`)

Add an entry to the `COMPETITIONS` dict:

```python
COMPETITIONS["la_liga"] = SoccerCompetitionConfig(
    fd_code="PD",                          # Football-Data.org code
    sport_prefix="LIGA",                   # appears in channel name
    sport_label="La Liga",                 # appears in EPG description
    odds_sport_key="soccer_spain_la_liga", # optional, enables spread signal
    rank_cap=20,                           # league size (matters for the WHY)
    total_matchdays=38,                    # round-robin home + away
)
```

### Step 2: register the stakes thresholds (`scoring.py`)

Add a `LEAGUE_CONTEXTS` entry. This is what powers the "title race",
"playoff race", "relegation battle" labels and the EPG description's
"why is this a race?" reminder:

```python
LEAGUE_CONTEXTS["PD"] = LeagueContext(
    code="PD",
    matchdays_total=38,  # same value as SoccerCompetitionConfig.total_matchdays
    thresholds=[
        (1,  "title"),
        (4,  "UCL"),
        (7,  "Europa"),
        (18, "relegation"),
    ],
    boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
)
```

The `thresholds` list is `(position, label)` tuples. The plugin's stakes
signal fires for any team within ±2 positions of any threshold. Late in
the season the contribution doubles, so a relegation-line game in
matchday 36 outranks a relegation-line game in matchday 5.

`boundary_summary` is rendered verbatim in the EPG description as a
one-line reminder of what each standings position translates to. Use
the `→` arrow notation and `·` separator to match the existing style.

### Step 3: add the user-facing toggle (`plugin.json`)

Inside the `fields` array, under the "Sport Sources" section:

```json
{
  "id": "enable_la_liga",
  "type": "boolean",
  "label": "La Liga (Spain)",
  "default": false,
  "help_text": "Spanish top flight. Title, UCL, Europa, relegation all live signals. Requires the Football-Data.org API key below."
}
```

### Step 4: wire it in (`plugin.py`)

Inside `_build_sources`, add:

```python
if settings.get("enable_la_liga", False) and fd_key:
    sources.append(SoccerSource("la_liga", fd_api_key=fd_key, odds_api_key=odds_key))
```

### Step 5: test

1. Enable the toggle in the Dispatcharr plugin settings, paste your
   Football-Data.org key.
2. Run `Refresh curated list`.
3. Open `cache.json` (in the plugin dir) and confirm games come back
   with proper ranks + stakes labels.
4. Run `Apply to Dispatcharr` (with `Dry run on Apply` enabled first).

If the score breakdown for a known marquee fixture (a top-of-table
clash, a relegation 6-pointer) makes sense, ship it.

---

## Adding a new sport

A new sport means a new file in `sources/` implementing the
`SportSource` interface. Existing examples:

- `sources/ncaaf.py` — poll-based ranks (AP Top 25 from a 130-team
  field). Season identifier is the START year.
- `sources/ncaam.py` — poll-based ranks (AP Top 25 from a 350-team
  field). Season identifier is the END year. Date-range pagination
  on `/games`.
- `sources/soccer.py` — standings-position ranks (every team has a
  rank). Multi-competition adapter with a `COMPETITIONS` dict.

### The contract

`sources/base.py` defines:

```python
class SportSource(ABC):
    sport_prefix: str    # short tag for channel names (e.g. "NHL")
    sport_label: str     # human label (e.g. "NHL Regular Season")

    @abstractmethod
    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]: ...
```

`GameRow` carries: home, away, rank_home, rank_away, start_time (UTC),
optional spread, and an `extra` dict for sport-specific metadata.

### Pick your ranking model

- **Poll-based** (NCAAF/NCAAM-style): only the top N of a much larger
  field is "ranked". Most teams have `rank=None`. Set
  `extra["rank_source"] = "poll"` (or omit, that's the default).
  The "both top-10" and "both top-5" WHY labels fire for these.

- **Standings-position-based** (soccer-style): every team in the
  competition has a position. Set
  `extra["rank_source"] = "standings"`. The "top-N" WHY labels are
  suppressed (everyone is "top-20"); the `stakes` signal carries
  league-aware semantics instead.

### Required `extra` fields

For the score signals to work fully:

- `extra["rank_source"]` — `"poll"` or `"standings"`, see above.
- `extra["fd_competition_code"]` — set if you want the `stakes` signal.
  Maps to a `LEAGUE_CONTEXTS` entry in `scoring.py` (add one for your
  sport: matchday count + threshold positions).
- `extra["season_progress"]` — float 0.0-1.0; powers the late-season
  multiplier. Skip for sports without a clear season arc (knockouts).
- `extra["stage"]` — `"FINAL"` / `"SEMI_FINALS"` / etc. for knockouts;
  fires the tournament-stage signal.
- `extra["standings_table"]` — for standings-based sports, full league
  with `{"name", "position", "points", "played"}`. Needed for the
  impact-on-favorites narrative ("Wrexham sits #6 (70 pts), 1 spot
  and 6 pts behind ...").

### Wire it in

Same as soccer's step 3 + step 4: add the toggle to `plugin.json`,
import the source in `sources/__init__.py`, and wire it in
`plugin._build_sources`.

### A note on API rate limits

Most sports APIs have free tiers but they're tight. Cache between
refreshes — the plugin already only refreshes 4× per day by default.
Don't issue per-game lookups when one season-wide call does the same
thing (this is what bit `NcaamSource` initially: pagination silently
capped at 3000 games and we only realized late-season was missing).
Always test against a past week's data before relying on the adapter.

---

## Style

- No em-dashes (—) or double-dashes (--). Commas, periods, or rewrite.
- New scoring signals go in `scoring.py`. Sport-specific logic goes in
  `sources/<sport>.py`. If you find yourself adding a sport branch in
  `scoring.py` or `plugin.py`, push it back into the adapter.
- Run `pytest tests/` before submitting. Add tests for any new pure
  helpers you write.

---

## License

MIT. By contributing you agree your contributions ship under MIT.
