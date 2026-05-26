"""WC 2026 simulation probe.

Closes the "going-in-blind" gap before WC kicks off on 2026-06-11 by
driving the WC group->knockout cross-source chain through realistic
tournament states using REAL FD.org schedule data with patched match
statuses. Captures the importance signal at every key inflection point
and emits a markdown report.

Why this exists: the Annex C 495-row lookup table, the daemon-thread
async dispatch, the typed soccer state, and the cross-source chain
machinery all landed in the last 24 hours. The unit tests cover their
correctness in isolation; this probe exercises their composition end-
to-end against the real FD.org WC 2026 draw (12 groups A-L, 48 teams,
72 group matches, 16 LAST_32, 8 LAST_16, 4 QF, 2 SF, 1 final).

What the probe verifies, per scenario:
  - The simulator produces a leverage signal of plausible magnitude
    for the target match.
  - Group-stage matches accumulate downstream-cascade contributions
    from the cross-source chain (R32 / R16 / QF / SF / Final / Winner
    bands all show up in the breakdown when the chain is active).
  - The Annex C lookup hits the canonical mapping when the group
    stage is fully decided (vs the backtracking fallback).
  - The leverage signal evolves sensibly across the tournament
    timeline: early group games < MD3 do-or-die < knockout games <
    deep knockouts.

Usage:
  docker exec Dispatcharr python /data/plugins/dispatcharr_ranked_matchups/tools/probe_wc_simulation.py

Reads:
  - /data/plugins/dispatcharr_ranked_matchups/.wc_2026_fixture.json
    (the cached FD.org WC 2026 match schedule)

Writes:
  - /data/plugins/dispatcharr_ranked_matchups/.wc_probe_report.md
    (markdown report of all scenario outputs)
"""
from __future__ import annotations

import copy
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Bootstrap Django so model imports inside the plugin work.
sys.path.insert(0, "/app")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
import django
django.setup()

# The Dispatcharr plugin loader registers the package under both
# `_dispatcharr_plugin_dispatcharr_ranked_matchups` and the alias
# `dispatcharr_ranked_matchups`. Force a discovery so the alias is
# available for our imports.
from apps.plugins.loader import PluginManager
PluginManager.get().discover_plugins(sync_db=False)

from dispatcharr_ranked_matchups.sources.soccer import (  # noqa: E402
    GroupStageSoccerSource,
    KnockoutSoccerSource,
    SoccerSource,
)
from dispatcharr_ranked_matchups.scoring import (  # noqa: E402
    LEAGUE_CONTEXTS,
    compute_match_importance,
)
from dispatcharr_ranked_matchups.sources.base import GameRow  # noqa: E402

# ---------- fixture loading ----------

# Fixture + report live in the deployed plugin folder so the script
# works whether it's run from /tmp/ inside the container or from the
# repo's tools/ directory.
PLUGIN_DIR = "/data/plugins/dispatcharr_ranked_matchups"
if not os.path.isdir(PLUGIN_DIR):
    PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE_PATH = os.path.join(PLUGIN_DIR, ".wc_2026_fixture.json")
REPORT_PATH = os.path.join(PLUGIN_DIR, ".wc_probe_report.md")


def load_fixture() -> List[Dict[str, Any]]:
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("matches", [])


# ---------- scenario helpers ----------


def mark_finished(match: Dict[str, Any], home_goals: int, away_goals: int) -> Dict[str, Any]:
    """Return a copy of `match` patched to a FINISHED state with the
    given final score. Leaves other fields (id, group, teams, date)
    intact so the plugin's state-building consumes a realistic shape."""
    m = copy.deepcopy(match)
    m["status"] = "FINISHED"
    m["score"] = {
        "winner": "HOME_TEAM" if home_goals > away_goals else ("AWAY_TEAM" if away_goals > home_goals else "DRAW"),
        "duration": "REGULAR",
        "fullTime": {"home": home_goals, "away": away_goals},
        "halfTime": {"home": max(0, home_goals - 1), "away": max(0, away_goals - 1)},
    }
    return m


def find_match(matches: List[Dict[str, Any]], home: str, away: str, stage: str = "GROUP_STAGE") -> Dict[str, Any]:
    """First match where home/away names match (either direction) and
    stage matches. Raises KeyError if not found."""
    for m in matches:
        if m.get("stage") != stage:
            continue
        h, a = m["homeTeam"]["name"], m["awayTeam"]["name"]
        if (h == home and a == away) or (h == away and a == home):
            return m
    raise KeyError(f"no {stage} match {home} vs {away}")


def group_matches(matches: List[Dict[str, Any]], group_letter: str) -> List[Dict[str, Any]]:
    """All matches in group `letter`, ordered by matchday then date."""
    grp = f"GROUP_{group_letter}"
    out = [m for m in matches if m.get("group") == grp and m.get("stage") == "GROUP_STAGE"]
    out.sort(key=lambda m: (m.get("matchday", 0), m.get("utcDate", "")))
    return out


def apply_md_results(matches: List[Dict[str, Any]], scripted: List[Tuple[str, str, int, int]]) -> List[Dict[str, Any]]:
    """Given a list of (home, away, hg, ag) tuples, return a new
    matches list with those specific games patched to FINISHED.

    A scripted entry matches a match in either orientation; goals are
    re-oriented to the match's published home/away when the scripted
    entry was reversed. Idempotent on misses."""
    forward: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for h, a, hg, ag in scripted:
        forward[(h, a)] = (hg, ag)
    out: List[Dict[str, Any]] = []
    for m in matches:
        if m.get("stage") != "GROUP_STAGE":
            out.append(m)
            continue
        h, a = m["homeTeam"]["name"], m["awayTeam"]["name"]
        if (h, a) in forward:
            hg, ag = forward[(h, a)]
            out.append(mark_finished(m, hg, ag))
        elif (a, h) in forward:
            # Scripted entry has the orientation reversed; swap goals to
            # the match's published home/away orientation.
            hg, ag = forward[(a, h)]
            out.append(mark_finished(m, ag, hg))
        else:
            out.append(m)
    return out


# ---------- probe core ----------


def make_sources(matches: List[Dict[str, Any]]) -> Tuple[GroupStageSoccerSource, KnockoutSoccerSource]:
    """Build a paired WC source pair and pre-load both with the
    scenario's match list so neither hits FD.org."""
    fd_key = os.environ.get("FD_API_KEY", "x")
    knockout = KnockoutSoccerSource("world_cup", fd_api_key=fd_key, odds_api_key="")
    groups = GroupStageSoccerSource("world_cup", fd_api_key=fd_key, odds_api_key="")
    # Pre-load match caches so the simulator works from the scenario data.
    knockout._all_matches_cache = matches
    groups._all_matches_cache = matches
    groups._initial_state_cache = None
    groups._team_group_cache = None
    knockout._initial_state_cache = None
    knockout._bracket_games_cache = None
    # Wire the cross-source chain (the same setup _build_sources does).
    groups.set_paired_knockout_source(knockout)
    return groups, knockout


def probe_match(
    groups: GroupStageSoccerSource,
    matches: List[Dict[str, Any]],
    target_fd_match: Dict[str, Any],
    n_sims: int = 1000,
    rng_seed: int = 0,
) -> Dict[str, Any]:
    """Drive compute_match_importance for one target match and return
    the structured outputs the report consumes."""
    # Convert the FD.org match dict to a GameRow the simulator expects.
    target = GameRow(
        sport_prefix="WC",
        sport_label="FIFA World Cup",
        home=target_fd_match["homeTeam"]["name"],
        away=target_fd_match["awayTeam"]["name"],
        rank_home=None,
        rank_away=None,
        start_time=datetime.fromisoformat(target_fd_match["utcDate"].replace("Z", "+00:00")),
        extra={"fd_id": target_fd_match["id"], "matchday": target_fd_match.get("matchday")},
    )
    ctx = LEAGUE_CONTEXTS["WC_GS"]
    rng = random.Random(rng_seed)
    t0 = time.time()
    raw, notes, labels_hit = compute_match_importance(
        groups, target, ctx, n_sims=n_sims, rng=rng,
    )
    elapsed = time.time() - t0
    return {
        "match": f"{target.home} vs {target.away}",
        "stage": target_fd_match.get("stage"),
        "group": target_fd_match.get("group"),
        "matchday": target_fd_match.get("matchday"),
        "raw": raw,
        "notes": notes,
        "labels_hit": labels_hit,
        "elapsed_s": elapsed,
        "n_sims": n_sims,
    }


def probe_bracket_seed(groups: GroupStageSoccerSource) -> Dict[str, Any]:
    """Run _build_bracket_seed on the current state and return the
    LAST_32 entry ties so we can verify Annex C canonical assignments
    fire (vs the backtracking fallback)."""
    state = groups.initial_state()
    seed = groups._build_bracket_seed(state)
    l32 = seed["stages"][0]["ties"]
    pairings = []
    for i, tie in enumerate(l32):
        pairings.append({
            "l32_idx": i,
            "home": tie["home"],
            "away": tie["away"],
        })
    return {"l32_pairings": pairings}


# ---------- scenarios ----------


def scenario_t_minus_15(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """All 104 matches in their natural pre-tournament state. Nothing
    patched. Tournament-eve baseline."""
    return copy.deepcopy(matches)


def scenario_group_a_md1_done(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group A's two MD1 games are FINISHED. Other groups untouched.
    Lets us probe an MD2 game and compare leverage to the T-15 baseline."""
    ga = group_matches(matches, "A")
    md1 = [m for m in ga if m.get("matchday") == 1]
    if len(md1) < 2:
        return copy.deepcopy(matches)
    # Mexico 2-0 South Africa (MD1 game 1), Czechia 1-1 South Korea (MD1 game 2)
    scripted = []
    for m in md1:
        h, a = m["homeTeam"]["name"], m["awayTeam"]["name"]
        if {h, a} == {"Mexico", "South Africa"}:
            scripted.append((h, a, 2, 0) if h == "Mexico" else (h, a, 0, 2))
        elif {h, a} == {"Czechia", "South Korea"}:
            scripted.append((h, a, 1, 1))
    return apply_md_results(copy.deepcopy(matches), scripted)


def scenario_group_a_md3_do_or_die(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """All Group A MD1+MD2 played, all 4 teams alive (each on 3 pts)
    going into MD3. Both MD3 games are do-or-die for everyone. Plus
    every OTHER group is finished so the Annex C lookup has a key to
    hit on."""
    # Make every NON-A group fully finished with deterministic results.
    scripted: List[Tuple[str, str, int, int]] = []
    for letter in "BCDEFGHIJKL":
        teams = sorted({(m["homeTeam"]["name"], m["awayTeam"]["name"]) for m in group_matches(matches, letter)})
        # Pick the alphabetically-first 4 teams and stage a clean 1-1-1-1
        # result set so all 4 finish with 3 pts and the Annex C lookup
        # gets a maximally-uncertain (deterministic-by-alphabet) outcome.
        # Actually: produce 1st > 2nd > 3rd > 4th by points, deterministically.
        all_teams = sorted({n for h, a in teams for n in (h, a)})
        if len(all_teams) < 4:
            continue
        # Each team plays the other 3. Give all_teams[0] (1st) 9 pts,
        # all_teams[1] (2nd) 6, all_teams[2] (3rd) 3, all_teams[3] (4th) 0.
        # That means: 0 beats 1, 2, 3; 1 beats 2, 3; 2 beats 3. Each 1-0.
        t1, t2, t3, t4 = all_teams[0], all_teams[1], all_teams[2], all_teams[3]
        wins = [
            (t1, t2, 1, 0),
            (t1, t3, 1, 0),
            (t1, t4, 1, 0),
            (t2, t3, 1, 0),
            (t2, t4, 1, 0),
            (t3, t4, 1, 0),
        ]
        scripted.extend(wins)

    # Group A MD1+MD2: every team gets 1 win, 1 loss (3 pts each on 2 games).
    # MD1: Mex 2-0 SA, Cze 2-0 SKor
    # MD2: SA 2-0 Mex (lol unlikely but it's a test), SKor 2-0 Cze
    # That gives all four teams 1W-1L = 3 pts going into MD3.
    ga_scripted = [
        ("Mexico", "South Africa", 2, 0),
        ("Czechia", "South Korea", 2, 0),
        ("South Africa", "Mexico", 2, 0),  # MD2 in our scripted state
        ("South Korea", "Czechia", 2, 0),
    ]
    scripted.extend(ga_scripted)
    return apply_md_results(copy.deepcopy(matches), scripted)


def scenario_all_groups_done(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every group fully decided. Probe inspects the bracket seed dict
    to verify Annex C canonical assignments. No target match probed
    (probe focuses on the seed structure)."""
    scripted: List[Tuple[str, str, int, int]] = []
    for letter in "ABCDEFGHIJKL":
        all_teams = sorted({n for h, a in [(m["homeTeam"]["name"], m["awayTeam"]["name"])
                                          for m in group_matches(matches, letter)] for n in (h, a)})
        if len(all_teams) < 4:
            continue
        t1, t2, t3, t4 = all_teams[0], all_teams[1], all_teams[2], all_teams[3]
        wins = [
            (t1, t2, 1, 0),
            (t1, t3, 1, 0),
            (t1, t4, 1, 0),
            (t2, t3, 1, 0),
            (t2, t4, 1, 0),
            (t3, t4, 1, 0),
        ]
        scripted.extend(wins)
    return apply_md_results(copy.deepcopy(matches), scripted)


def scenario_all_groups_done_thirds_abcdefgh(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every group fully decided, AND the advancing-thirds set is exactly
    {A,B,C,D,E,F,G,H}. This isolates Annex C row 495, where the canonical
    mapping diverges from the greedy fallback enough to be testable.

    To force the advancing set: give groups A-H's 3rd-placers slightly
    better goal differential (GD=0, 1W-1L-0D over 3 games) and I-L's
    3rd-placers worse (GD=-2, 0W-2L-1D). The FIFA tiebreaker chain
    (points -> GD -> GF) then sorts A-H above I-L cleanly.

    Per Annex C row 495 the canonical M77 (l32_idx=4) away slot gets
    group F's 3rd-placer (Sweden, in this fixture). The greedy fallback
    would assign group G's 3rd-placer (Iran) instead -- Iran comes
    before Sweden alphabetically and G is the first matching slot in
    {C,D,F,G,H}. M77 == Sweden proves the canonical lookup fired."""
    scripted: List[Tuple[str, str, int, int]] = []

    # Groups A-H: rig to give each group's 3rd-placer 3 pts with GD=0
    # (1W 1D 1L: beat 4th, draw 2nd, lose to 1st).
    for letter in "ABCDEFGH":
        all_teams = sorted({n for h, a in [(m["homeTeam"]["name"], m["awayTeam"]["name"])
                                          for m in group_matches(matches, letter)] for n in (h, a)})
        if len(all_teams) < 4:
            continue
        t1, t2, t3, t4 = all_teams
        # 1st (t1): 9 pts, GD=+5; 2nd (t2): 5 pts, GD=+1; 3rd (t3): 3 pts, GD=0;
        # 4th (t4): 1 pt, GD=-6.
        wins = [
            (t1, t2, 2, 0),   # t1 beats t2  (t1 +2, t2 0)
            (t1, t3, 2, 0),   # t1 beats t3  (t1 +2, t3 0)
            (t1, t4, 3, 0),   # t1 thrashes t4 (t1 +3)
            (t2, t3, 1, 1),   # t2 draws t3
            (t2, t4, 2, 0),   # t2 beats t4
            (t3, t4, 2, 0),   # t3 beats t4
        ]
        scripted.extend(wins)

    # Groups I-L: rig 3rd-placer to 1 pt with GD=-3 (worse than A-H 3rds).
    for letter in "IJKL":
        all_teams = sorted({n for h, a in [(m["homeTeam"]["name"], m["awayTeam"]["name"])
                                          for m in group_matches(matches, letter)] for n in (h, a)})
        if len(all_teams) < 4:
            continue
        t1, t2, t3, t4 = all_teams
        # 1st (t1): 9 pts, GD=+6; 2nd (t2): 6 pts, GD=+2; 3rd (t3): 1 pt, GD=-3;
        # 4th (t4): 1 pt, GD=-5. Crucially t3 is here with GD=-3 (worse than A-H's GD=0).
        wins = [
            (t1, t2, 2, 0),
            (t1, t3, 3, 0),
            (t1, t4, 2, 0),
            (t2, t3, 2, 0),
            (t2, t4, 2, 0),
            (t3, t4, 1, 1),   # t3 draws t4 (only point either gets aside from 4th's loss to 1/2)
        ]
        scripted.extend(wins)

    return apply_md_results(copy.deepcopy(matches), scripted)


# ---------- report writer ----------


def format_notes(notes: List[str], max_lines: int = 8) -> str:
    if not notes:
        return "  _(no nonzero contributions)_"
    shown = notes[:max_lines]
    extra = len(notes) - max_lines
    out = "\n".join(f"  - `{line}`" for line in shown)
    if extra > 0:
        out += f"\n  - _(+{extra} more)_"
    return out


def write_report(sections: List[Tuple[str, str]]) -> None:
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("# WC 2026 simulation probe report\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write("Data source: real FD.org WC 2026 schedule (104 matches) with patched FINISHED statuses per scenario.\n")
        f.write("All probes use `compute_match_importance` with the WC_GS league context, n_sims=1000 per query.\n")
        f.write("Leverage notes format: `{team} {label}: {leverage} × {weight} = {contribution}`.\n\n")
        f.write("---\n\n")
        for title, body in sections:
            f.write(f"## {title}\n\n{body}\n\n---\n\n")
    print(f"wrote {REPORT_PATH}")


# ---------- main ----------


def run() -> None:
    raw_matches = load_fixture()
    print(f"loaded {len(raw_matches)} matches from fixture")

    sections: List[Tuple[str, str]] = []

    # Scenario 1: T-15 baseline
    print("\n=== S1: T-15 baseline ===")
    s1_matches = scenario_t_minus_15(raw_matches)
    groups, _ = make_sources(s1_matches)
    target = find_match(s1_matches, "Mexico", "South Africa", "GROUP_STAGE")
    r = probe_match(groups, s1_matches, target)
    body = (
        f"All 104 matches in natural FD.org TIMED state. Target: MD1 opener.\n\n"
        f"- match: **{r['match']}** ({r['group']} MD{r['matchday']})\n"
        f"- raw importance: **{r['raw']:.3f}**\n"
        f"- labels hit: `{r['labels_hit']}`\n"
        f"- sim time: {r['elapsed_s']:.2f}s for {r['n_sims']} sims\n"
        f"- notes (top contributions):\n{format_notes(r['notes'])}\n"
    )
    sections.append(("Scenario 1: Tournament eve (T-15) — Mexico vs South Africa MD1", body))

    # Scenario 2: Group A MD1 done
    print("\n=== S2: Group A MD1 done ===")
    s2_matches = scenario_group_a_md1_done(raw_matches)
    groups, _ = make_sources(s2_matches)
    # Probe MD2 Mexico vs South Korea (typically; first MD2 game in GROUP_A)
    ga_md2 = [m for m in group_matches(s2_matches, "A") if m.get("matchday") == 2]
    target = ga_md2[0] if ga_md2 else find_match(s2_matches, "Mexico", "Czechia")
    r = probe_match(groups, s2_matches, target)
    body = (
        f"Group A MD1 played: Mexico 2-0 South Africa, Czechia 1-1 South Korea. "
        f"All other groups still TIMED.\n\n"
        f"- match: **{r['match']}** ({r['group']} MD{r['matchday']})\n"
        f"- raw importance: **{r['raw']:.3f}**\n"
        f"- labels hit: `{r['labels_hit']}`\n"
        f"- sim time: {r['elapsed_s']:.2f}s for {r['n_sims']} sims\n"
        f"- notes:\n{format_notes(r['notes'])}\n"
    )
    sections.append(("Scenario 2: Group A MD1 done — MD2 leverage", body))

    # Scenario 3: Group A MD3 do-or-die (all groups B-L finished)
    print("\n=== S3: Group A MD3 do-or-die, other groups done ===")
    s3_matches = scenario_group_a_md3_do_or_die(raw_matches)
    groups, _ = make_sources(s3_matches)
    # Probe one of the two MD3 games (Mexico vs Czechia typically).
    ga_md3 = [m for m in group_matches(s3_matches, "A") if m.get("matchday") == 3]
    if ga_md3:
        target = ga_md3[0]
        r = probe_match(groups, s3_matches, target)
        body = (
            f"Group A enters MD3 with all 4 teams alive (each on 3 pts). "
            f"Groups B-L are fully decided with deterministic standings, "
            f"so the Annex C lookup should hit a canonical key for any "
            f"advancing-thirds set Group A's outcome can produce.\n\n"
            f"- match: **{r['match']}** ({r['group']} MD{r['matchday']})\n"
            f"- raw importance: **{r['raw']:.3f}**\n"
            f"- labels hit: `{r['labels_hit']}`\n"
            f"- sim time: {r['elapsed_s']:.2f}s for {r['n_sims']} sims\n"
            f"- notes:\n{format_notes(r['notes'])}\n"
        )
        sections.append(("Scenario 3: Group A MD3 do-or-die (Annex C lookup live)", body))

    # Scenario 4: All groups done — inspect the L32 bracket seed
    print("\n=== S4: All groups done, inspect bracket seed ===")
    s4_matches = scenario_all_groups_done(raw_matches)
    groups, _ = make_sources(s4_matches)
    seed = probe_bracket_seed(groups)
    pairings_lines = []
    for p in seed["l32_pairings"]:
        pairings_lines.append(f"  - M{p['l32_idx']+73} (l32_idx={p['l32_idx']}): **{p['home']}** vs **{p['away']}**")
    # Also surface which groups produced 3rd-placers and what the
    # advancing set is.
    state = groups.initial_state()
    standings = groups._compute_group_standings(state)
    advancing_thirds = [
        (team, state["_team_group"][team][-1])  # team -> group letter
        for team, _ in standings.best_third_order[:standings.n_best_third_advance]
    ]
    body = (
        f"All 12 groups fully decided with deterministic alphabetical "
        f"standings (each group's alphabetically-first team wins all 3 "
        f"games, second wins 2, third wins 1, fourth loses all). The "
        f"`_build_bracket_seed` output below shows the LAST_32 entry "
        f"pairings the cross-source chain produces.\n\n"
        f"### Advancing 3rd-placers (top 8 across all groups):\n"
        + "\n".join(f"  - {t} (group {g})" for t, g in advancing_thirds) + "\n\n"
        f"### LAST_32 pairings:\n" + "\n".join(pairings_lines) + "\n"
    )
    sections.append(("Scenario 4: All groups decided — LAST_32 bracket seed", body))

    # Scenario 5: Advancing thirds rigged to {A-H} so canonical Annex C
    # diverges from the greedy fallback. Distinguishing test.
    print("\n=== S5: All groups done, thirds = {A,B,C,D,E,F,G,H} (canonical-distinguishing) ===")
    s5_matches = scenario_all_groups_done_thirds_abcdefgh(raw_matches)
    groups, _ = make_sources(s5_matches)
    state = groups.initial_state()
    standings = groups._compute_group_standings(state)
    advancing_thirds = [
        (team, state["_team_group"][team][-1])
        for team, _ in standings.best_third_order[:standings.n_best_third_advance]
    ]
    advancing_set = "".join(sorted({g for _, g in advancing_thirds}))
    seed = probe_bracket_seed(groups)
    # The canonical mapping for {ABCDEFGH} (Annex C row 495):
    #   M74 (l32_idx=1) away = group C's 3rd-placer
    #   M77 (l32_idx=4) away = group F's 3rd-placer  <-- the discriminator
    #   M79 (l32_idx=6) away = group H's 3rd-placer
    #   M80 (l32_idx=7) away = group E's 3rd-placer
    #   M81 (l32_idx=8) away = group B's 3rd-placer
    #   M82 (l32_idx=9) away = group A's 3rd-placer
    #   M85 (l32_idx=12) away = group G's 3rd-placer
    #   M87 (l32_idx=14) away = group D's 3rd-placer
    # Greedy (alphabetical-by-team) would put group G (Iran) at M77,
    # not group F (Sweden), so M77 == Sweden proves canonical fired.
    third_by_group: Dict[str, str] = {}
    for team, group_letter in advancing_thirds:
        third_by_group[group_letter] = team
    canonical_expected: Dict[int, str] = {
        1: "C", 4: "F", 6: "H", 7: "E", 8: "B", 9: "A", 12: "G", 14: "D",
    }
    matches_canonical: List[Tuple[int, str, str, bool]] = []
    if advancing_set == "ABCDEFGH":
        for idx, expected_group in canonical_expected.items():
            tie = seed["l32_pairings"][idx]
            expected_team = third_by_group.get(expected_group, "?")
            actual_team = tie["away"]
            ok = actual_team == expected_team
            matches_canonical.append((idx, expected_group, actual_team, ok))
    pairings_lines = []
    for p in seed["l32_pairings"]:
        pairings_lines.append(f"  - M{p['l32_idx']+73}: **{p['home']}** vs **{p['away']}**")
    canonical_lines = []
    if matches_canonical:
        for idx, exp_grp, actual, ok in matches_canonical:
            mark = "✓" if ok else "✗"
            canonical_lines.append(
                f"  - M{idx+73} (l32_idx={idx}): canonical says group {exp_grp}'s 3rd "
                f"(`{third_by_group.get(exp_grp, '?')}`); actual `{actual}` {mark}"
            )
    body = (
        f"All 12 groups fully decided, with groups A-H's 3rd-placers "
        f"given better goal differential than I-L's so the advancing "
        f"set is exactly {{A,B,C,D,E,F,G,H}}. This is Annex C row 495 "
        f"in `_WC2026_THIRD_PLACER_SLOT_TABLE`. The canonical assignment "
        f"diverges from the greedy fallback at M77: canonical puts "
        f"group F (Sweden) there; greedy with alphabetical-by-team "
        f"`qualifying_thirds` puts group G (Iran) there.\n\n"
        f"### Advancing set: `{advancing_set}` "
        f"({'expected ABCDEFGH' if advancing_set == 'ABCDEFGH' else 'NOT the target set!'})\n\n"
        f"### 3rd-placers by group:\n"
        + "\n".join(f"  - group {g}: {t}" for t, g in sorted(advancing_thirds, key=lambda x: x[1])) + "\n\n"
        f"### Canonical vs actual (per slot):\n"
        + ("\n".join(canonical_lines) if canonical_lines else "  _(advancing set not ABCDEFGH; can't run the distinguishing check)_") + "\n\n"
        f"### LAST_32 pairings:\n" + "\n".join(pairings_lines) + "\n"
    )
    sections.append(("Scenario 5: Annex C canonical-vs-greedy discriminator", body))

    write_report(sections)


if __name__ == "__main__":
    run()
