**[Plugin] Dispatcharr Ranked Matchups v1.0.0**

**What it does**
Curates the most interesting upcoming sports games from across 20+ leagues into a single "Top Matchups" channel group in your Dispatcharr guide, ranked by transparent signals (rank-pair, closeness, rivalry, favorite team, tournament stakes, standings race) with the *why* shown in each channel's EPG description. Daily auto-refresh keeps the curated list fresh; FIFA World Cup 2026 ships with the full Annex C bracket modeling so group games carry accurate R16+ leverage.

[screenshots]

---

✅ **Compatibility**
App: Dispatcharr v0.25.1+
Platforms: Linux/amd64, Linux/arm64

📦 **Download (ZIP)**
ZIP: https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups/releases/download/v1.0.0/plugin-dispatcharr_ranked_matchups-v1.0.0.zip
SHA256: `32c2a5ae31850fa921270615f33edf811a99324a2920baf6106600a9b7a69d0a`
File name: `plugin-dispatcharr_ranked_matchups-v1.0.0.zip`

🗂 **ZIP Contents**
```
dispatcharr_ranked_matchups/
├── CHANGELOG.md
├── LICENSE
├── README.md
├── __init__.py
├── _util.py
├── llm_descriptions.py
├── logos.py
├── matcher.py
├── plugin.json
├── plugin.py
├── rivalries.json
├── rivalries.py
├── scoring.py
├── simulation.py
├── tasks.py
├── team_aliases.json
└── sources/
    ├── __init__.py
    ├── _espn.py
    ├── _soccer_bracket_helpers.py
    ├── base.py
    ├── bracket.py
    ├── field_event.py
    ├── liga_mx.py
    ├── mlb.py
    ├── mls.py
    ├── mls_cup.py
    ├── mls_standings.py
    ├── nba.py
    ├── ncaa_baseball.py
    ├── ncaa_soccer.py
    ├── ncaa_soccer_cup.py
    ├── ncaa_softball.py
    ├── ncaaf.py
    ├── ncaam.py
    ├── ncaaw_basketball.py
    ├── nfl.py
    ├── nhl.py
    ├── nwsl.py
    ├── points_based.py
    ├── soccer.py
    └── wnba.py
```

ZIP must be named `dispatcharr_ranked_matchups.zip` for the plugin to load — rename the downloaded file before importing.

Source: https://github.com/Jacob-Lasky/dispatcharr_ranked_matchups (MIT)
