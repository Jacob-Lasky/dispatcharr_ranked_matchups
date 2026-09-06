"""Tests for the optional LLM-rewritten EPG descriptions feature.

The implementation lives in `llm_descriptions.py`; the wiring into the apply
pipeline lives in `plugin.py:_action_apply`. The cache is a sidecar JSON file
(separate from the deterministic cache.json) so the structural data stays
untouched.

Test surface (in dependency order):
  - prompt_hash determinism + model sensitivity
  - build_llm_context shape (favorites, standings window, threshold tagging,
    closeness tag)
  - llm_describe_or_fallback caching (hit short-circuits the call)
  - fallback on raised exception
  - fallback on empty response
  - read/write/prune cache file
"""

from __future__ import annotations

import json
import os

import pytest

from dispatcharr_ranked_matchups import llm_descriptions as llm


def _sample_game():
    """Realistic game dict shape lifted from a live cache.json row."""
    return {
        "sport_prefix": "EPL",
        "sport_label": "English Premier League",
        "home": "Tottenham Hotspur FC",
        "away": "Everton FC",
        "rank_home": 17,
        "rank_away": 13,
        "start_time_utc": "2026-05-24T15:00:00+00:00",
        "kickoff_local": "Today 11:00 AM EDT",
        "spread": None,
        "closeness": 0.82,
        "favorites_matched": ["Tottenham"],
        "importance_thresholds_hit": ["relegation"],
        "extra": {
            "matchday": 38,
            "matchdays_total": 38,
            "fd_competition_code": "PL",
            "standings_table": [
                {"name": "Arsenal FC", "position": 1, "points": 85, "played": 38},
                {"name": "Manchester City FC", "position": 2, "points": 78, "played": 38},
                {"name": "Manchester United FC", "position": 3, "points": 71, "played": 38},
                {"name": "Newcastle United FC", "position": 12, "points": 49, "played": 38},
                {"name": "Everton FC", "position": 13, "points": 49, "played": 38},
                {"name": "Leeds United FC", "position": 14, "points": 47, "played": 38},
                {"name": "Crystal Palace FC", "position": 15, "points": 45, "played": 38},
                {"name": "Nottingham Forest FC", "position": 16, "points": 44, "played": 38},
                {"name": "Tottenham Hotspur FC", "position": 17, "points": 41, "played": 38},
                {"name": "West Ham United FC", "position": 18, "points": 39, "played": 38},
                {"name": "Burnley FC", "position": 19, "points": 22, "played": 38},
            ],
            "impact_narratives": [],
        },
    }


# ---------- prompt_hash ----------

class TestPromptHash:
    def test_deterministic(self):
        a = llm.prompt_hash("hello world", "claude-haiku-4-5")
        b = llm.prompt_hash("hello world", "claude-haiku-4-5")
        assert a == b
        assert len(a) == 16

    def test_context_change_invalidates(self):
        a = llm.prompt_hash("hello world", "claude-haiku-4-5")
        b = llm.prompt_hash("hello world!", "claude-haiku-4-5")
        assert a != b

    def test_model_change_invalidates(self):
        a = llm.prompt_hash("hello world", "claude-haiku-4-5")
        b = llm.prompt_hash("hello world", "claude-sonnet-4-6")
        assert a != b


# ---------- build_llm_context ----------

class TestBuildLlmContext:
    def test_includes_teams_and_kickoff(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="relegation race")
        assert "Tottenham Hotspur FC" in ctx
        assert "Everton FC" in ctx
        assert "Today 11:00 AM EDT" in ctx
        # Away listed first, "at" home: broadcast convention.
        assert "Everton FC at Tottenham Hotspur FC" in ctx

    def test_includes_competition_and_matchday(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "English Premier League" in ctx
        assert "Matchday: 38 of 38" in ctx

    def test_standings_window_includes_neighbors(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        # Window should include Tottenham (#17) ± 2 = #15-19, plus Everton
        # (#13) ± 2 = #11-15, plus leader (#1).
        assert "#1 Arsenal FC" in ctx
        assert "#17 Tottenham Hotspur FC" in ctx
        assert "#18 West Ham United FC" in ctx  # the relegation rival
        assert "#13 Everton FC" in ctx
        assert "#15 Crystal Palace FC" in ctx
        # Should NOT include far-away teams like #2 or #3.
        assert "Manchester City FC" not in ctx
        assert "Manchester United FC" not in ctx

    def test_favorites_surfaced(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "User's favorite teams playing: Tottenham" in ctx

    def test_series_state_surfaced(self):
        # Playoff grounding: without these lines the model invents
        # "elimination" framing (the bug this feature fixes).
        g = _sample_game()
        g["home"] = "Carolina Hurricanes"
        g["away"] = "Vegas Golden Knights"
        g["extra"]["series"] = {
            "title": "Stanley Cup Final", "game_number": 3, "best_of": 7,
            "home_wins": 2, "away_wins": 0,
            "results": [
                {"game_number": 1, "home": "Carolina Hurricanes",
                 "away": "Vegas Golden Knights", "home_goals": 3,
                 "away_goals": 2, "ot": True},
            ],
        }
        ctx = llm.build_llm_context(g, tagline="")
        assert "Series: Stanley Cup Final, Game 3 of 7" in ctx
        assert "Series record: Carolina Hurricanes lead the series 2-0" in ctx
        assert "Results so far:" in ctx
        assert "Game 1: Carolina Hurricanes 3, Vegas Golden Knights 2 (OT)" in ctx

    def test_no_series_state_emits_no_series_lines(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "Series:" not in ctx
        assert "Series record:" not in ctx

    def test_system_prompt_forbids_inventing_series_facts(self):
        # The guardrail string must be present: it's the model-facing half of
        # the fix (grounding lines are the data half).
        assert "Do NOT fabricate playoff series facts" in llm.SYSTEM_PROMPT
        assert "facing elimination" in llm.SYSTEM_PROMPT

    def test_win_or_go_home_requires_knockout_evidence(self):
        """The rule used to say the ABSENCE of series lines still permitted
        "win or go home" framing. A regular-season league fixture also has no
        series lines, so that licensed calling an October Premier League match
        an elimination game. Knockout framing now needs positive evidence."""
        assert "Win or go home" in llm.SYSTEM_PROMPT
        assert "requires EVIDENCE THAT THIS IS A KNOCKOUT" in llm.SYSTEM_PROMPT
        assert "ABSENCE of series lines is NOT that evidence" in llm.SYSTEM_PROMPT

    def test_run_in_framing_requires_a_band_within_reach(self):
        """"Be concrete about what this result settles" contradicted the
        grounding rules for two safe mid-table sides in April."""
        assert "ONLY where a posture line actually" in llm.SYSTEM_PROMPT
        # Fragment chosen to sit inside a single wrapped line.
        assert "nothing is on the line, say what the game IS" in llm.SYSTEM_PROMPT

    def test_promoted_rule_does_not_deny_the_current_record(self):
        """"has NO record in this league" contradicted the current-season
        record supplied in the same prompt."""
        assert "no LAST-SEASON record in this league" in llm.SYSTEM_PROMPT
        assert "may still have a current-season record" in llm.SYSTEM_PROMPT

    def test_system_prompt_grounds_all_facts(self):
        # The general grounding rule is what kills the WC "shock opening loss"
        # fabrication: every record / result / standing must come from the
        # provided lines, not just series facts.
        assert "GROUND EVERY FACT" in llm.SYSTEM_PROMPT
        assert "lost their opener" in llm.SYSTEM_PROMPT

    def test_group_stage_surfaced(self):
        # WC / EURO group grounding: without these lines the model invents a
        # group narrative ("shock opening loss") from team names alone.
        g = _sample_game()
        g["home"] = "Argentina"
        g["away"] = "Mexico"
        g["extra"]["group_stage"] = {
            "tournament": "FIFA World Cup",
            "group": "C",
            "matchday": 2,
            "matchdays_total": 3,
            "standings": [
                {"position": 1, "name": "Argentina", "played": 1, "points": 3,
                 "goal_difference": 1},
                {"position": 2, "name": "Mexico", "played": 1, "points": 1,
                 "goal_difference": 0},
            ],
            "results": [
                {"home": "Argentina", "away": "Saudi Arabia",
                 "home_goals": 2, "away_goals": 1},
            ],
            "advance": "The top 2 teams in each group advance, plus the 8 "
                       "best third-placed teams across all groups.",
        }
        ctx = llm.build_llm_context(g, tagline="")
        assert "Tournament round: FIFA World Cup Group C, Matchday 2 of 3" in ctx
        assert "Current group standings:" in ctx
        assert "#1 Argentina - 3 pts, 1 played, +1 GD" in ctx
        assert "Group results so far:" in ctx
        assert "Argentina 2-1 Saudi Arabia" in ctx
        assert "Advancement: The top 2 teams in each group advance" in ctx

    def test_no_group_stage_emits_no_group_lines(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "Tournament round:" not in ctx
        assert "Current group standings:" not in ctx
        assert "Group results so far:" not in ctx

    def test_importance_thresholds_surfaced(self):
        # Phase C.4 renamed the prompt label "Stakes thresholds" →
        # "Outcome bands" to match the importance-signal vocabulary.
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "Outcome bands in play: relegation" in ctx

    def test_closeness_high_tags_toss_up(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "Bookmaker view: toss-up" in ctx

    def test_closeness_low_does_not_tag(self):
        g = _sample_game()
        g["closeness"] = 0.3
        ctx = llm.build_llm_context(g, tagline="")
        assert "toss-up" not in ctx

    def test_honours_surfaced_for_wc_final(self):
        """Regression: the WC final where the model invented 'their third
        crown'. The real counts must reach the context so it can't."""
        g = _sample_game()
        g["home"] = "Spain"
        g["away"] = "Argentina"
        g["tournament_stage"] = "FINAL"
        g["extra"]["fd_competition_code"] = "WC"
        ctx = llm.build_llm_context(g, tagline="Final")
        assert "Honours (World Cup):" in ctx
        assert "Spain — 1 title (2010)" in ctx
        assert "Argentina — 3 titles (1978, 1986, 2022)" in ctx

    def test_no_honours_for_league_game(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="")
        assert "Honours (" not in ctx

    def test_closeness_none_does_not_tag(self):
        g = _sample_game()
        g["closeness"] = None
        ctx = llm.build_llm_context(g, tagline="")
        assert "toss-up" not in ctx

    def test_tagline_surfaced_as_hint(self):
        ctx = llm.build_llm_context(_sample_game(), tagline="relegation race")
        assert "Editorial frame" in ctx
        assert "relegation race" in ctx

    def test_boundary_summary_passed_through(self):
        ctx = llm.build_llm_context(
            _sample_game(), tagline="", boundary_summary="top 4 UCL, bottom 3 relegated"
        )
        assert "top 4 UCL, bottom 3 relegated" in ctx

    def test_impact_narratives_from_extra(self):
        g = _sample_game()
        g["extra"]["impact_narratives"] = [
            "West Ham's result swings Tottenham's relegation fate.",
        ]
        ctx = llm.build_llm_context(g, tagline="")
        assert "Affects user's other favorites" in ctx
        assert "West Ham's result swings Tottenham's relegation fate." in ctx

    def test_impact_narratives_legacy_top_level(self):
        # Older cache.json rows had impact_narratives at top level; the
        # normalizer in build_llm_context falls through to that path.
        g = _sample_game()
        g["impact_narratives"] = ["legacy-shape narrative"]
        ctx = llm.build_llm_context(g, tagline="")
        assert "legacy-shape narrative" in ctx

    def test_no_extra_handles_gracefully(self):
        g = {"home": "A", "away": "B", "kickoff_local": "Tomorrow noon"}
        ctx = llm.build_llm_context(g, tagline="")
        # Doesn't raise on missing extra/standings/etc.
        assert "A" in ctx and "B" in ctx


# ---------- llm_describe_or_fallback ----------

class TestLlmDescribeOrFallback:
    def test_cache_hit_short_circuits(self):
        g = _sample_game()
        calls = []

        def boom_caller(context, api_key, model):
            calls.append(1)
            raise AssertionError("should not have been called")

        # Pre-populate cache with the exact key the function will compute.
        ctx = llm.build_llm_context(g, tagline="")
        key = f"marker-abc|{llm.prompt_hash(ctx, 'claude-haiku-4-5')}"
        cache = {key: "Cached prose."}
        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="k", model="claude-haiku-4-5", cache=cache,
            marker="marker-abc", caller=boom_caller,
        )
        assert out == "Cached prose."
        assert calls == []

    def test_miss_calls_and_caches(self):
        g = _sample_game()
        cache = {}

        def caller(context, api_key, model):
            assert api_key == "secret"
            assert model == "claude-haiku-4-5"
            assert "Tottenham" in context
            return "Fresh prose from the model."

        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="secret", model="claude-haiku-4-5", cache=cache,
            marker="marker-xyz", caller=caller,
        )
        assert out == "Fresh prose from the model."
        # Second call should now be a cache hit.
        out2 = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="secret", model="claude-haiku-4-5", cache=cache,
            marker="marker-xyz",
            caller=lambda *a, **k: pytest.fail("expected cache hit"),
        )
        assert out2 == "Fresh prose from the model."

    def test_api_error_falls_back(self):
        g = _sample_game()
        cache = {}

        def caller(context, api_key, model):
            raise ValueError("anthropic 500")

        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="k", model="claude-haiku-4-5", cache=cache,
            marker="m", caller=caller,
        )
        assert out == "FALLBACK"
        assert cache == {}  # nothing cached on failure

    def test_network_timeout_falls_back(self):
        g = _sample_game()

        def caller(context, api_key, model):
            raise TimeoutError("connect timeout")

        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="k", model="claude-haiku-4-5", cache={},
            marker="m", caller=caller,
        )
        assert out == "FALLBACK"

    def test_empty_response_falls_back(self):
        g = _sample_game()

        def caller(context, api_key, model):
            return ""

        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description="FALLBACK",
            api_key="k", model="claude-haiku-4-5", cache={},
            marker="m", caller=caller,
        )
        assert out == "FALLBACK"

    def test_fallback_returns_same_object_for_change_detection(self):
        """plugin.py uses `description is before` to detect fallback. Must
        return the EXACT object passed in, not a copy."""
        g = _sample_game()
        sentinel = "FALLBACK-SENTINEL"

        def caller(context, api_key, model):
            raise ValueError("nope")

        out = llm.llm_describe_or_fallback(
            g=g, tagline="", fallback_description=sentinel,
            api_key="k", model="claude-haiku-4-5", cache={},
            marker="m", caller=caller,
        )
        assert out is sentinel


# ---------- cache file I/O ----------

class TestCacheFileIO:
    def test_read_missing_returns_empty(self, tmp_path):
        path = str(tmp_path / "no_such_file.json")
        assert llm.read_cache(path) == {}

    def test_read_malformed_returns_empty(self, tmp_path):
        path = str(tmp_path / "broken.json")
        with open(path, "w") as f:
            f.write("{not valid json")
        assert llm.read_cache(path) == {}

    def test_read_non_dict_returns_empty(self, tmp_path):
        path = str(tmp_path / "wrong_shape.json")
        with open(path, "w") as f:
            json.dump([1, 2, 3], f)
        assert llm.read_cache(path) == {}

    def test_read_drops_non_string_values(self, tmp_path):
        path = str(tmp_path / "mixed.json")
        with open(path, "w") as f:
            json.dump({"good": "prose", "bad": 42, "also_bad": None}, f)
        assert llm.read_cache(path) == {"good": "prose"}

    def test_write_then_read_round_trips(self, tmp_path):
        path = str(tmp_path / "cache.json")
        llm.write_cache(path, {"a:1234": "prose A", "b:5678": "prose B"})
        loaded = llm.read_cache(path)
        assert loaded == {"a:1234": "prose A", "b:5678": "prose B"}

    def test_write_is_atomic(self, tmp_path):
        """Writes go through a .tmp file and then os.replace. Should leave no
        partial .tmp behind on success."""
        path = str(tmp_path / "cache.json")
        llm.write_cache(path, {"x:1": "y"})
        assert os.path.exists(path)
        assert not os.path.exists(path + ".tmp")


class TestPruneCache:
    def test_keeps_live_markers(self):
        cache = {
            "marker-A|abc": "alive prose",
            "marker-B|def": "stale prose",
            "marker-C|ghi": "also alive",
        }
        pruned = llm.prune_cache(cache, live_markers={"marker-A", "marker-C"})
        assert pruned == {"marker-A|abc": "alive prose", "marker-C|ghi": "also alive"}

    def test_empty_live_markers_drops_all(self):
        assert llm.prune_cache({"a|1": "x"}, live_markers=set()) == {}

    def test_marker_with_internal_colons_preserved(self):
        # Regression: real markers look like 'ranked_matchups:EPL:538161'.
        # A naive split-on-first-colon would clip 'ranked_matchups' as the
        # marker and prune everything. Splitting on '|' avoids the collision.
        cache = {"ranked_matchups:EPL:538161|hash123": "alive prose"}
        pruned = llm.prune_cache(cache, live_markers={"ranked_matchups:EPL:538161"})
        assert pruned == {"ranked_matchups:EPL:538161|hash123": "alive prose"}

    def test_handles_marker_without_separator(self):
        # Defensive: malformed key with no '|'. Treated as the whole-string
        # being the marker; only kept if that's in live_markers.
        assert llm.prune_cache({"weird": "x"}, live_markers={"weird"}) == {"weird": "x"}
        assert llm.prune_cache({"weird": "x"}, live_markers={"other"}) == {}


# ---------- system prompt invariants ----------

class TestSystemPrompt:
    def test_says_plain_text_only(self):
        # The demo run produced markdown asterisks; the prompt was hardened
        # against that before shipping. Regression guard.
        assert "Plain text only" in llm.SYSTEM_PROMPT

    def test_forbids_signal_jargon(self):
        # The model should describe what stakes/favorites MEAN, not name the
        # signals. Regression guard against the prompt drifting back toward
        # algorithmic vocabulary.
        assert '"favorite"' in llm.SYSTEM_PROMPT
        assert '"stakes"' in llm.SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# #209: the seeded-standings falsehood.
#
# The audit that produced #209 found 16 of 21 live descriptions asserting at
# least one false, checkable fact. The dominant cause was that early in a
# season the SCORING table is deliberately replaced with last season's final
# table (a good ranking prior), and `build_llm_context` rendered that as the
# current standings with nothing marking the swap. The model then wrote
# "Arsenal's seven-point cushion at the top" on matchday 3, when Arsenal were
# 3rd and 3 points behind the leader.
#
# These tests are written against the REAL numbers from that audit so a
# regression reproduces the exact false sentence rather than an abstraction.
# ---------------------------------------------------------------------------

# Last season's final Premier League table, which is what the seeded scoring
# table actually contained on 2026-09-06 (points kept, played forced to 0).
_PL_SEED_TABLE = [
    {"name": "Arsenal FC", "position": 1, "points": 85, "played": 0},
    {"name": "Manchester City FC", "position": 2, "points": 78, "played": 0},
    {"name": "Manchester United FC", "position": 3, "points": 71, "played": 0},
    {"name": "Chelsea FC", "position": 10, "points": 52, "played": 0},
    {"name": "Everton FC", "position": 13, "points": 49, "played": 0},
]

# The real matchday-3 table for the same clubs, from /v4/competitions/PL/standings.
_PL_CURRENT_TABLE = [
    {"name": "Manchester City FC", "position": 1, "points": 9, "played": 3},
    {"name": "Hull City AFC", "position": 2, "points": 7, "played": 3},
    {"name": "Arsenal FC", "position": 3, "points": 6, "played": 2},
    {"name": "Chelsea FC", "position": 4, "points": 6, "played": 2},
    {"name": "Everton FC", "position": 10, "points": 4, "played": 2},
    {"name": "Manchester United FC", "position": 12, "points": 3, "played": 2},
    {"name": "Aston Villa FC", "position": 17, "points": 1, "played": 3},
    {"name": "Tottenham Hotspur FC", "position": 18, "points": 1, "played": 3},
    {"name": "Fulham FC", "position": 19, "points": 0, "played": 3},
    {"name": "Coventry City FC", "position": 20, "points": 0, "played": 3},
]

# Matches scoring.LEAGUE_CONTEXTS["PL"].thresholds.
_PL_THRESHOLDS = [
    (1, "title", 5.0),
    (4, "UCL", 4.0),
    (7, "Europa/Conference", 2.0),
    (17, "relegation", 5.0),
]


class _FakeLeagueContext:
    """Duck-typed stand-in for scoring.LeagueContext. Only `thresholds` is
    read by build_llm_context, so importing the real dataclass (and with it
    the scoring module) would buy nothing."""

    def __init__(self, thresholds):
        self.thresholds = thresholds


def _seed_window_game():
    """Chelsea at Arsenal, matchday 3 — the exact fixture from the #209 audit,
    carrying both the seeded scoring table and the real current table."""
    return {
        "sport_prefix": "EPL",
        "sport_label": "English Premier League",
        "home": "Arsenal FC",
        "away": "Chelsea FC",
        "rank_home": 1,
        "rank_away": 10,
        "kickoff_local": "Today 11:30 AM EDT",
        "extra": {
            "matchday": 3,
            "matchdays_total": 38,
            "fd_competition_code": "PL",
            "standings_table": _PL_SEED_TABLE,
            "standings_table_current": _PL_CURRENT_TABLE,
            "standings_prev_final": [
                {"name": "Arsenal FC", "position": 1, "points": 85, "played": 38},
                {"name": "Chelsea FC", "position": 10, "points": 52, "played": 38},
            ],
            "standings_seeded": True,
            "h2h": [
                {"date": "2026-04-12", "home": "Arsenal FC", "away": "Chelsea FC",
                 "home_goals": 2, "away_goals": 1, "season": "last season"},
            ],
        },
    }


class TestSeededStandingsNeverPresentedAsCurrent:
    def test_last_season_points_do_not_appear_as_current_standings(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            boundary_summary="Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        # The seeded table's giveaway numbers must be absent from every line
        # that is not explicitly labelled as last season.
        current_section = ctx.split("Last season's final table:")[0]
        assert "85 pts" not in current_section
        assert "78 pts" not in current_section
        # And the "0 games played" incoherence that made the seed readable as
        # a live table is gone entirely.
        assert "0 games played" not in ctx

    def test_current_positions_are_the_real_ones(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        # Arsenal are 3rd on 6 points, not 1st on 85.
        assert "Arsenal FC: 3rd of 20, 6 pts from 2 games" in ctx
        assert "Chelsea FC: 4th of 20, 6 pts from 2 games" in ctx

    def test_gap_to_leader_is_precomputed_and_correct(self):
        """The false sentence was 'Arsenal's seven-point cushion at the top'.
        Arsenal are 3 points BEHIND, and the prompt now says so outright."""
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        assert "3 pts behind the leader" in ctx
        assert "cushion" not in ctx

    def test_relegation_gap_is_precomputed(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        # Arsenal 6 pts, first relegation place (18th, Tottenham) on 1 pt.
        assert "5 pts and 15 places clear of the relegation zone" in ctx

    def test_band_membership_uses_current_position(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        # 3rd and 4th are both inside the top 4, so both are in the UCL band,
        # and neither is in the title band (cutoff 1).
        arsenal = [l for l in ctx.splitlines() if l.startswith("  - Arsenal FC:")][0]
        assert "UCL" in arsenal
        assert "title" not in arsenal
        assert "relegation" not in arsenal.split("Currently in:")[1].split(".")[0]

    def test_early_season_is_flagged_as_noise(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        assert "Season progress: 3 of 38 matchdays played." in ctx
        assert "Very little has been decided" in ctx
        assert "do not frame them as urgent" in ctx

    def test_last_season_is_labelled_as_last_season(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        assert "Last season's final table:" in ctx
        assert "Arsenal FC: finished 1st of 2 (85 pts)." in ctx

    def test_head_to_head_is_rendered(self):
        ctx = llm.build_llm_context(
            _seed_window_game(), tagline="",
            league_context=_FakeLeagueContext(_PL_THRESHOLDS),
        )
        assert "Previous meetings between these two, most recent first:" in ctx
        assert "2026-04-12 (last season): Arsenal FC 2-1 Chelsea FC" in ctx


class TestCurrentStandingsSelection:
    def test_seeded_row_without_current_table_yields_no_standings(self):
        """An old cache row written before #209: `standings_seeded` says the
        only table present is last season's, so the prompt must go without a
        table rather than describe the prior as live."""
        g = _seed_window_game()
        del g["extra"]["standings_table_current"]
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "This season's table" not in ctx
        assert "Where the two teams stand this season:" not in ctx
        assert "85 pts" not in ctx.split("Last season's final table:")[0]

    def test_unseeded_legacy_row_still_uses_its_standings_table(self):
        """A mid-season row from before #209 has no `standings_table_current`
        and is not seeded, so its `standings_table` IS the current table and
        must keep working."""
        g = _seed_window_game()
        del g["extra"]["standings_table_current"]
        g["extra"]["standings_seeded"] = False
        g["extra"]["standings_table"] = _PL_CURRENT_TABLE
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "Arsenal FC: 3rd of 20, 6 pts from 2 games" in ctx


class TestPromotedTeamIsNamedNotInvented:
    def test_absent_from_last_season_reads_as_newly_promoted(self):
        """Monza, Malaga and Troyes each appeared in a described match with no
        row anywhere in the prompt, and the model invented a league position
        for them. Absence must be stated, not left as a hole."""
        lines = llm.prev_season_lines(
            [{"name": "AC Milan", "position": 5, "points": 70, "played": 38}],
            ["AC Monza", "AC Milan"],
        )
        assert lines[0] == "  - AC Monza: did not play in this league last season (newly promoted)."
        assert lines[1] == "  - AC Milan: finished 5th of 1 (70 pts)."


class TestBandMembershipDirection:
    """The single worst description in the #209 audit said Osasuna and Alaves
    were 'in a dogfight to avoid dropping into the bottom three' while they sat
    6th and 5th. That is a top/bottom cutoff inversion, so it gets its own
    test."""

    def test_midtable_team_is_not_reported_as_relegation_bound(self):
        table = [
            {"name": f"Team {i}", "position": i, "points": 30 - i, "played": 3}
            for i in range(1, 21)
        ]
        table[5]["name"] = "CA Osasuna"  # position 6
        lines = llm.team_posture_lines(table, ["CA Osasuna"], _PL_THRESHOLDS)
        assert "relegation" not in lines[0].split("Currently in:")[1].split(".")[0]
        assert "UCL" not in lines[0]  # 6th is outside the top 4
        assert "Europa/Conference" in lines[0]

    def test_bottom_team_is_reported_as_relegation_bound(self):
        table = [
            {"name": f"Team {i}", "position": i, "points": 30 - i, "played": 3}
            for i in range(1, 21)
        ]
        table[18]["name"] = "Sinking FC"  # position 19, below the 17 cutoff
        lines = llm.team_posture_lines(table, ["Sinking FC"], _PL_THRESHOLDS)
        assert "relegation" in lines[0]
        # A team already in the zone is measured to safety, not to the zone.
        # See TestRelegationGapDirection for why.
        assert "from safety" in lines[0]


class TestSeasonPhase:
    """Jake, on the live output: "chasing bowl eligibility" is vacuous in week
    1 and the whole story in November, so the framing has to know where in the
    season it is."""

    def test_phase_boundaries(self):
        assert llm.season_phase(0, 12) == "opening"
        assert llm.season_phase(1, 12) == "early"     # 8%
        assert llm.season_phase(2, 12) == "early"     # 17%
        assert llm.season_phase(4, 12) == "midseason"  # 33%
        assert llm.season_phase(8, 12) == "midseason"  # 67%
        assert llm.season_phase(9, 12) == "late"       # 75%, three to play
        assert llm.season_phase(10, 12) == "late"      # 83%
        assert llm.season_phase(38, 38) == "late"
        assert llm.season_phase(28, 38) == "midseason"  # 74%
        assert llm.season_phase(29, 38) == "late"       # 76%

    def test_opener_guidance_forbids_run_in_framing(self):
        lines = llm.season_progress_line(0, 12, "games")
        assert lines[0] == "Season progress: no games played yet, this is the opener."
        assert "Nothing has been decided" in lines[1]
        assert "do not frame them as immediate stakes" in lines[1]

    def test_late_season_guidance_says_the_stakes_are_real(self):
        lines = llm.season_progress_line(10, 12, "games")
        assert lines[0] == "Season progress: 10 of 12 games played."
        assert "run-in" in lines[1]
        assert "genuinely on the line" in lines[1]

    def test_midseason_is_neither(self):
        lines = llm.season_progress_line(20, 38)  # 53%
        assert "Season progress: 20 of 38 matchdays played." == lines[0]
        assert "still time to recover" in lines[1]

    def test_negative_played_yields_nothing(self):
        assert llm.season_progress_line(-1, 38) == []


class TestSystemPromptGuardrails:
    """The prompt hash folds SYSTEM_PROMPT, so these rules are also what
    invalidates every cached description written under the old rules."""

    def test_forbids_ungrounded_positions_and_zones(self):
        assert "NEVER state a league position" in llm.SYSTEM_PROMPT
        assert "unless a line above says so" in llm.SYSTEM_PROMPT

    def test_separates_last_season_from_now(self):
        assert "Last season's final table" in llm.SYSTEM_PROMPT
        # Fragments chosen to sit inside a single wrapped line.
        assert "is LAST season" in llm.SYSTEM_PROMPT
        assert "never as where they sit now" in llm.SYSTEM_PROMPT

    def test_names_the_promoted_case(self):
        assert "newly promoted" in llm.SYSTEM_PROMPT


class TestTiedPositionsDoNotHideTheRelegationGap:
    """FD.org uses competition ranking: tied teams share a position and the
    next is skipped. The real PL table on 2026-09-06 had two clubs on 17 and
    then jumped to 19, with no row at 18. An exact `cutoff + 1` lookup found
    nothing and silently dropped every relegation-gap line."""

    def test_gap_is_still_reported_when_the_cutoff_row_is_skipped(self):
        table = [
            {"name": "Leader FC", "position": 1, "points": 9, "played": 3},
            {"name": "Arsenal FC", "position": 3, "points": 6, "played": 2},
            {"name": "Tied A", "position": 17, "points": 1, "played": 3},
            {"name": "Tied B", "position": 17, "points": 1, "played": 3},
            # No position 18 exists.
            {"name": "Fulham FC", "position": 19, "points": 0, "played": 3},
        ]
        lines = llm.team_posture_lines(table, ["Arsenal FC"], _PL_THRESHOLDS)
        assert "6 pts and 16 places clear of the relegation zone" in lines[0]


class TestNestedTopBandsReportOnlyTheTightest:
    def test_third_place_is_ucl_not_also_europa(self):
        """Top bands are nested: 3rd satisfies both the top-4 and top-7
        cutoffs. Reporting both reads as 'Arsenal are in the Europa places',
        which is the wrong half of a true statement."""
        table = [{"name": f"T{i}", "position": i, "points": 30 - i, "played": 3}
                 for i in range(1, 21)]
        table[2]["name"] = "Arsenal FC"  # 3rd
        line = llm.team_posture_lines(table, ["Arsenal FC"], _PL_THRESHOLDS)[0]
        bands = line.split("Currently in: ")[1].split(".")[0]
        assert bands == "UCL"

    def test_leader_reports_title_only(self):
        table = [{"name": f"T{i}", "position": i, "points": 30 - i, "played": 3}
                 for i in range(1, 21)]
        table[0]["name"] = "Top FC"
        line = llm.team_posture_lines(table, ["Top FC"], _PL_THRESHOLDS)[0]
        assert line.split("Currently in: ")[1].split(".")[0] == "title"

    def test_relegation_is_reported_alongside_no_top_band(self):
        table = [{"name": f"T{i}", "position": i, "points": 30 - i, "played": 3}
                 for i in range(1, 21)]
        table[19]["name"] = "Doomed FC"  # 20th
        line = llm.team_posture_lines(table, ["Doomed FC"], _PL_THRESHOLDS)[0]
        assert line.split("Currently in: ")[1].split(".")[0] == "relegation"


class TestStandingsSliceFollowsCurrentPositions:
    """`rank_home` / `rank_away` are the SEEDED scoring ranks. Centring the
    table slice on them showed a window containing neither team: Osasuna and
    Alaves sit 5th and 6th today but finished 16th and 14th last season, so
    the slice rendered rows 12-18."""

    def _table(self):
        return [{"name": f"T{i}", "position": i, "points": 40 - i, "played": 4}
                for i in range(1, 21)]

    def test_slice_contains_both_teams_when_ranks_are_stale(self):
        table = self._table()
        table[4]["name"] = "Deportivo Alaves"   # 5th now
        table[5]["name"] = "CA Osasuna"         # 6th now
        g = {
            "home": "Deportivo Alaves", "away": "CA Osasuna",
            "rank_home": 14, "rank_away": 16,   # last season's positions
            "extra": {"standings_table_current": table, "standings_seeded": True},
        }
        ctx = llm.build_llm_context(g, tagline="", league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        slice_lines = ctx.split("This season's table (relevant slice):")[1]
        assert "Deportivo Alaves" in slice_lines
        assert "CA Osasuna" in slice_lines

    def test_falls_back_to_rank_when_team_absent_from_table(self):
        g = {
            "home": "Promoted FC", "away": "T3",
            "rank_home": None, "rank_away": 3,
            "extra": {"standings_table_current": self._table()},
        }
        ctx = llm.build_llm_context(g, tagline="", league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "#3 T3" in ctx


class TestPollRankAndVenueContext:
    """#209 root cause 4: the entire CFB prompt was 'Match / Competition /
    Week / Outcome bands'. The AP ranks were sitting unused in the cache row
    the whole time."""

    def _cfb_game(self, rank_home=9, rank_away=24, neutral=True, conf=False):
        return {
            "sport_prefix": "CFB",
            "sport_label": "NCAA Football",
            "home": "Ole Miss",
            "away": "Louisville",
            "rank_home": rank_home,
            "rank_away": rank_away,
            "rank_pool_size": 25,
            "kickoff_local": "Today 7:30 PM EDT",
            "extra": {"week": 1, "neutral": neutral, "conference_game": conf,
                      "fd_competition_code": "CFB"},
        }

    def test_both_ranks_are_rendered(self):
        ctx = llm.build_llm_context(self._cfb_game(), tagline="")
        assert "National poll ranking:" in ctx
        assert "Ole Miss: ranked #9 of 25 in the national poll." in ctx
        assert "Louisville: ranked #24 of 25 in the national poll." in ctx

    def test_unranked_side_is_named_as_unranked(self):
        ctx = llm.build_llm_context(self._cfb_game(rank_away=None), tagline="")
        assert "Louisville: unranked." in ctx

    def test_no_block_when_neither_side_is_ranked(self):
        ctx = llm.build_llm_context(
            self._cfb_game(rank_home=None, rank_away=None), tagline="")
        assert "National poll ranking:" not in ctx

    def test_neutral_site_is_stated(self):
        ctx = llm.build_llm_context(self._cfb_game(), tagline="")
        assert "Neutral site: neither team is at home." in ctx

    def test_conference_game_is_stated_only_when_true(self):
        assert "conference game" not in llm.build_llm_context(self._cfb_game(), tagline="")
        assert "This is a conference game." in llm.build_llm_context(
            self._cfb_game(conf=True), tagline="")

    def test_league_sources_do_not_get_a_poll_block(self):
        """A 17th-placed league side must not be relabelled as a national
        ranking; team_posture_lines already covers position properly."""
        g = _seed_window_game()
        g["extra"]["rank_source"] = "standings"
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "National poll ranking:" not in ctx


class TestRelegationGapDirection:
    """A safe team is measured to the top of the drop zone; a team already in
    it is measured to the last safe place. Measuring the second against the
    zone it is already in produced "2 pts into the relegation zone", a depth
    where the reader wants the distance out."""

    def _table(self):
        # 17 is the PL cutoff: 1-17 safe, 18-20 down.
        return [{"name": f"T{i}", "position": i, "points": 40 - i, "played": 10}
                for i in range(1, 21)]

    def test_safe_team_is_measured_to_the_drop_zone(self):
        table = self._table()
        table[9]["name"] = "Safe FC"  # 10th, 30 pts; 18th has 22
        line = llm.team_posture_lines(table, ["Safe FC"], _PL_THRESHOLDS)[0]
        assert "8 pts and 8 places clear of the relegation zone" in line
        assert "from safety" not in line

    def test_relegated_team_is_measured_to_safety(self):
        table = self._table()
        table[18]["name"] = "Sinking FC"  # 19th, 21 pts; 17th (last safe) has 23
        line = llm.team_posture_lines(table, ["Sinking FC"], _PL_THRESHOLDS)[0]
        assert "2 pts and 2 places from safety" in line
        assert "into the relegation zone" not in line

    def test_last_safe_place_is_found_through_a_tie(self):
        """Tied teams share a position and the next is skipped, so an exact
        lookup of the cutoff misses on both sides of the line."""
        table = [
            {"name": "Leader", "position": 1, "points": 40, "played": 10},
            {"name": "Tied A", "position": 16, "points": 23, "played": 10},
            {"name": "Tied B", "position": 16, "points": 23, "played": 10},
            # No position 17 exists; the cutoff row itself is missing.
            {"name": "Sinking FC", "position": 18, "points": 20, "played": 10},
        ]
        line = llm.team_posture_lines(table, ["Sinking FC"], _PL_THRESHOLDS)[0]
        assert "3 pts and 2 places from safety" in line

    def test_level_on_points_reads_as_level(self):
        table = self._table()
        table[17]["points"] = table[16]["points"]  # 18th level with 17th
        table[17]["name"] = "Level FC"
        line = llm.team_posture_lines(table, ["Level FC"], _PL_THRESHOLDS)[0]
        assert "level on points with safety" in line


class TestPollRankWithoutAPoolSize:
    def test_rank_renders_without_a_pool_when_none_is_known(self):
        """Live CFB cache rows carry `rank_pool_size: None`, so the "of N"
        clause must be omitted rather than rendered as "of None"."""
        g = {
            "home": "Ole Miss", "away": "Louisville",
            "rank_home": 9, "rank_away": 24, "rank_pool_size": None,
            "kickoff_local": "Today", "sport_label": "NCAA Football",
            "extra": {"week": 1},
        }
        ctx = llm.build_llm_context(g, tagline="")
        assert "Ole Miss: ranked #9 in the national poll." in ctx
        assert "None" not in ctx


class TestWinCountPosture:
    """Jake's note on the live output: "they're always chasing bowl
    eligibility at the start of the season... that just feels like an
    unnecessary comment". The band list says what is REACHABLE; a viewer wants
    the record and the distance."""

    _CFB_THRESHOLDS = [
        (6, "bowl_eligible", 2.0),
        (8, "8_wins", 3.0),
        (10, "10_wins", 4.0),
        (11, "11_wins", 5.0),
    ]

    def _g(self, home_rec, away_rec):
        return {
            "home": "Washington", "away": "Washington State",
            "extra": {"record_home": home_rec, "record_away": away_rec},
        }

    def test_record_and_distance_are_precomputed(self):
        lines = llm.win_count_posture_lines(
            self._g({"wins": 5, "losses": 4}, {"wins": 3, "losses": 6}),
            self._CFB_THRESHOLDS, 12,
        )
        assert lines[0] == (
            "  - Washington: 5-4. 3 games left. 1 more win for bowl eligible. "
            "3 more wins for 8 wins."
        )
        assert lines[1] == (
            "  - Washington State: 3-6. 3 games left. 3 more wins for bowl eligible."
        )

    def test_unreachable_thresholds_are_omitted_not_dangled(self):
        """3-6 with 3 to play tops out at 6 wins, so 8/10/11 are gone. A
        preview must not dangle an outcome that is already impossible."""
        lines = llm.win_count_posture_lines(
            self._g({"wins": 3, "losses": 6}, {"wins": 0, "losses": 9}),
            self._CFB_THRESHOLDS, 12,
        )
        assert "8 wins" not in lines[0]
        assert "10 wins" not in lines[0]
        # 0-9 with 3 left cannot reach 6 either.
        assert "bowl eligible" not in lines[1]
        assert lines[1] == "  - Washington State: 0-9. 3 games left."

    def test_already_met_threshold_is_stated_as_met(self):
        lines = llm.win_count_posture_lines(
            self._g({"wins": 9, "losses": 1}, None), self._CFB_THRESHOLDS, 12,
        )
        assert "already has bowl eligible" in lines[0]
        assert "already has 8 wins" in lines[0]
        assert "1 more win for 10 wins" in lines[0]

    def test_no_record_reads_as_not_played_yet(self):
        lines = llm.win_count_posture_lines(self._g(None, None), self._CFB_THRESHOLDS, 12)
        assert lines[0] == "  - Washington: has not played yet this season."

    def test_week_one_prompt_does_not_frame_bands_as_urgent(self):
        """End to end: the exact shape that produced the vacuous sentence."""
        g = {
            "home": "Washington", "away": "Washington State",
            "rank_home": 17, "rank_away": None, "rank_pool_size": 25,
            "kickoff_local": "Today", "sport_label": "NCAA Football",
            "extra": {"week": 1, "fd_competition_code": "CFB",
                      "record_home": None, "record_away": None},
        }

        class Ctx:
            thresholds = TestWinCountPosture._CFB_THRESHOLDS
            format = "win_count"
            matchdays_total = 12

        ctx = llm.build_llm_context(g, tagline="", league_context=Ctx())
        assert "Week: 1 of 12" in ctx
        assert "this is the opener" in ctx
        assert "do not frame them as immediate stakes" in ctx
        assert "has not played yet this season" in ctx

    def test_late_season_prompt_says_the_stakes_are_live(self):
        g = {
            "home": "Washington", "away": "Washington State",
            "kickoff_local": "Today", "sport_label": "NCAA Football",
            "extra": {"week": 11, "fd_competition_code": "CFB",
                      "record_home": {"wins": 5, "losses": 4},
                      "record_away": {"wins": 3, "losses": 6}},
        }

        class Ctx:
            thresholds = TestWinCountPosture._CFB_THRESHOLDS
            format = "win_count"
            matchdays_total = 12

        ctx = llm.build_llm_context(g, tagline="", league_context=Ctx())
        assert "genuinely on the line" in ctx
        assert "1 more win for bowl eligible" in ctx

    def test_games_played_prefers_the_record_over_the_week_number(self):
        """A bye or a postponement makes week-1 overstate games played."""
        assert llm._played_from_record(
            {"record_home": {"wins": 5, "losses": 4}}) == 9
        assert llm._played_from_record({}) is None


class TestPostureEdgeCases:
    """Edge inputs found by stressing the arithmetic directly. Each produced a
    sentence that was wrong or nonsense, which is the same failure class as
    #209 even though none of them came from the seeded table."""

    def test_level_with_the_leader_does_not_read_as_behind(self):
        """"level on points behind the leader" states the opposite of the
        situation. The level case needs its own wording, not "level on points"
        composed onto a phrase written for a non-zero gap."""
        table = [
            {"name": "A", "position": 1, "points": 9, "played": 3},
            {"name": "B", "position": 2, "points": 9, "played": 3},
        ]
        line = llm.team_posture_lines(table, ["B"], _PL_THRESHOLDS)[0]
        assert "level on points with the leader" in line
        assert "behind the leader" not in line

    def test_level_with_safety_does_not_read_as_clear(self):
        table = [{"name": f"T{i}", "position": i, "points": 40 - i, "played": 10}
                 for i in range(1, 21)]
        table[17]["points"] = table[16]["points"]  # 18th level with 17th
        table[17]["name"] = "Level FC"
        line = llm.team_posture_lines(table, ["Level FC"], _PL_THRESHOLDS)[0]
        assert "level on points with safety" in line
        assert "clear of" not in line

    def test_league_size_comes_from_position_not_row_count(self):
        """A partial table rendered "3rd of 2". Under competition ranking the
        highest position is the team count; len(table) is not."""
        table = [
            {"name": "A", "position": 3, "points": 6, "played": 2},
            {"name": "B", "position": 18, "points": 1, "played": 2},
        ]
        line = llm.team_posture_lines(table, ["A"], _PL_THRESHOLDS)[0]
        assert "3rd of 18" in line
        assert "3rd of 2" not in line

    def test_missing_points_omits_gaps_rather_than_guessing(self):
        table = [{"name": "A", "position": 3, "played": 2},
                 {"name": "B", "position": 18, "played": 2}]
        line = llm.team_posture_lines(table, ["A"], _PL_THRESHOLDS)[0]
        assert "pts" not in line
        assert "behind the leader" not in line
        assert "relegation zone" not in line


class TestInconsistentSeasonLengths:
    """A cup fixture stamped with a league's season length, or a rescheduled
    backlog, can put games-played past the season length."""

    def test_played_beyond_total_drops_the_denominator(self):
        lines = llm.season_progress_line(20, 12)
        assert lines[0] == "Season progress: 20 matchdays played."
        assert "of 12" not in lines[0]

    def test_played_beyond_total_does_not_mislabel_the_phase(self):
        """A fraction above 1.0 would classify as late on a bad denominator."""
        assert llm.season_phase(20, 12) == ""

    def test_count_is_still_stated_when_the_total_is_unknown(self):
        """Suppressing the whole line meant 3 matchdays got a line and 5 got
        nothing, purely because the second had no phase to report."""
        lines = llm.season_progress_line(5, None)
        assert lines == ["Season progress: 5 matchdays played."]

    def test_win_count_omits_games_left_on_an_impossible_total(self):
        g = {"home": "H", "away": "A",
             "extra": {"record_home": {"wins": 3, "losses": 6}, "record_away": None}}
        line = llm.win_count_posture_lines(g, [(6, "bowl_eligible", 2.0)], 2)[0]
        assert line == "  - H: 3-6."
        assert "games left" not in line


class TestPollBlockDoesNotShadowALeaguePosition:
    def test_league_row_missing_rank_source_gets_no_poll_block(self):
        """`rank_source` defaults to "poll", so a cache row that predates the
        key would have its LEAGUE POSITION relabelled a national ranking. The
        standings table is the reliable signal."""
        g = _seed_window_game()
        g["extra"].pop("rank_source", None)
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "National poll ranking:" not in ctx
        # ...and the position is still described, by the posture lines.
        assert "Arsenal FC: 3rd of 20" in ctx

    def test_poll_sport_with_no_standings_still_gets_the_block(self):
        g = {
            "home": "Ole Miss", "away": "Louisville",
            "rank_home": 9, "rank_away": 24, "rank_pool_size": 25,
            "kickoff_local": "Today", "sport_label": "NCAA Football",
            "extra": {"week": 1},
        }
        assert "National poll ranking:" in llm.build_llm_context(g, tagline="")


class TestLegacyRowDoesNotReachThePrompt:
    """Companion to the _util-level tests: the whole point is that a pre-#209
    cache row cannot put last season's numbers into a prompt."""

    def test_legacy_seeded_row_renders_no_current_standings(self):
        g = _seed_window_game()
        del g["extra"]["standings_table_current"]
        del g["extra"]["standings_seeded"]  # pre-#209 shape: neither key exists
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "85 pts" not in ctx.split("Last season's final table:")[0]
        assert "Where the two teams stand this season:" not in ctx
        assert "This season's table" not in ctx

    def test_legacy_row_drops_its_cached_narrative(self):
        g = _seed_window_game()
        del g["extra"]["standings_table_current"]
        del g["extra"]["standings_seeded"]
        g["extra"]["impact_narratives"] = [
            "Manchester City fans: rooting against Arsenal (1 spot and 7 pts ahead)."
        ]
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "7 pts ahead" not in ctx
        assert "Affects user's other favorites" not in ctx


class TestUnplayedTableIsNotRenderedAsAStanding:
    """FD.org assigns positions before a ball is kicked, so an unplayed table
    let the model read "Currently in: title" off a team with no games, while
    the opener guidance in the same prompt said nothing had been decided."""

    def test_zero_played_table_yields_no_posture_and_no_slice(self):
        table = [{"name": f"T{i}", "position": i, "points": 0, "played": 0}
                 for i in range(1, 21)]
        table[0]["name"] = "Arsenal FC"
        table[1]["name"] = "Chelsea FC"
        g = {"home": "Arsenal FC", "away": "Chelsea FC",
             "kickoff_local": "Today", "sport_label": "English Premier League",
             "extra": {"matchday": 1, "matchdays_total": 38,
                       "standings_table_current": table}}
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "this is the opener" in ctx
        assert "Currently in: title" not in ctx
        assert "This season's table" not in ctx

    def test_one_game_played_is_enough_to_render(self):
        table = [{"name": f"T{i}", "position": i, "points": 0, "played": 0}
                 for i in range(1, 21)]
        table[0].update(name="Arsenal FC", points=3, played=1)
        table[1]["name"] = "Chelsea FC"
        g = {"home": "Arsenal FC", "away": "Chelsea FC",
             "kickoff_local": "Today", "sport_label": "English Premier League",
             "extra": {"matchday": 2, "matchdays_total": 38,
                       "standings_table_current": table}}
        ctx = llm.build_llm_context(g, tagline="",
                                    league_context=_FakeLeagueContext(_PL_THRESHOLDS))
        assert "Where the two teams stand this season:" in ctx


class TestTiedRowsPreferAUsablePointsValue:
    def test_gap_survives_a_tied_row_with_no_points(self):
        """A last-wins dict picked an arbitrary row among tied teams, so a
        computable gap vanished when that row lacked points, and reversing the
        payload order restored it."""
        base = [
            {"name": "Safe", "position": 17, "points": 10, "played": 10},
            {"name": "Drop A", "position": 19, "points": 9, "played": 10},
            {"name": "Drop B", "position": 19, "points": None, "played": 10},
        ]
        for table in (base, list(reversed(base))):
            line = llm.team_posture_lines(table, ["Safe"], _PL_THRESHOLDS)[0]
            assert "1 pt and 2 places clear of the relegation zone" in line, (
                "output must not depend on payload row order"
            )


class TestLiveGuideRegressions:
    """Three defects found by reading the descriptions back off the live guide
    after deploying. None was visible to the unit tests, because each was the
    MODEL misusing a line that was itself correct."""

    def test_head_to_head_names_the_winner(self):
        """Given "Santos FC 1-2 SC Internacional" the model wrote "a Santos
        side that beat them earlier this season", inverting the result. One of
        the three head-to-head claims on the slate, so a 1-in-3 error rate on
        a brand-new feature. The winner is now stated, not derived."""
        lines = llm.h2h_lines([
            {"date": "2026-03-19", "home": "Santos FC", "away": "SC Internacional",
             "home_goals": 1, "away_goals": 2, "season": "this season"},
        ])
        assert lines[0] == (
            "  - 2026-03-19 (this season): Santos FC 1-2 SC Internacional "
            "(SC Internacional won)"
        )

    def test_home_win_and_draw_are_labelled_too(self):
        lines = llm.h2h_lines([
            {"date": "2026-03-19", "home": "CR Flamengo", "away": "Clube do Remo",
             "home_goals": 3, "away_goals": 0, "season": "this season"},
            {"date": "2026-01-31", "home": "Paris FC", "away": "Olympique de Marseille",
             "home_goals": 2, "away_goals": 2, "season": "last season"},
        ])
        assert "(CR Flamengo won)" in lines[0]
        assert "(a draw)" in lines[1]

    def test_relegation_gap_carries_the_places_gap(self):
        """Marseille sat 11th of 18, two points clear of the drop zone on
        matchday 3, and the preview called them "just outside the drop zone".
        Two points is small; five places is not, and the second number is what
        stops the first being misread."""
        table = [
            {"name": "Leader", "position": 1, "points": 9, "played": 3},
            {"name": "Marseille", "position": 11, "points": 3, "played": 3},
            {"name": "Le Havre", "position": 16, "points": 1, "played": 3},
            {"name": "Auxerre", "position": 18, "points": 0, "played": 3},
        ]
        thresholds = [(1, "title", 5.0), (15, "relegation", 5.0)]
        line = llm.team_posture_lines(table, ["Marseille"], thresholds)[0]
        assert "2 pts and 5 places clear of the relegation zone" in line

    def test_adjacent_places_do_not_get_a_redundant_clause(self):
        """One place away needs no elaboration; the points gap says it."""
        table = [
            {"name": "Leader", "position": 1, "points": 9, "played": 3},
            {"name": "Safe", "position": 15, "points": 3, "played": 3},
            {"name": "Down", "position": 16, "points": 1, "played": 3},
        ]
        thresholds = [(1, "title", 5.0), (15, "relegation", 5.0)]
        line = llm.team_posture_lines(table, ["Safe"], thresholds)[0]
        assert "2 pts clear of the relegation zone" in line
        assert "places" not in line

    def test_places_gap_also_applies_from_inside_the_zone(self):
        table = [
            {"name": "Leader", "position": 1, "points": 30, "played": 20},
            {"name": "Safe", "position": 15, "points": 20, "played": 20},
            {"name": "Doomed", "position": 20, "points": 12, "played": 20},
        ]
        thresholds = [(1, "title", 5.0), (15, "relegation", 5.0)]
        line = llm.team_posture_lines(table, ["Doomed"], thresholds)[0]
        assert "8 pts and 5 places from safety" in line

    def test_prompt_forbids_ungrounded_last_season_claims(self):
        """A college-football preview asserted "Both programs finished last
        season ranked" with no last-season data anywhere in its prompt."""
        assert "Say NOTHING about last season unless" in llm.SYSTEM_PROMPT
        assert "not whether they were ranked" in llm.SYSTEM_PROMPT

    def test_prompt_tells_the_model_to_use_the_bracketed_verdict(self):
        assert "each one names its winner in" in llm.SYSTEM_PROMPT
        assert "do not reverse it" in llm.SYSTEM_PROMPT


class TestNonPreviewGuard:
    """Seven NCAA soccer filler games came back as "I don't have the standings
    ... I'd need:" and that text was written verbatim into Jake's EPG.
    Tightening the grounding rules made the model refuse rather than invent,
    which is the right instinct pointed at the wrong output."""

    REAL_REFUSALS = [
        "I don't have the standings, results, group information, or season "
        "progress data needed to write this preview. To ground the preview in "
        "facts rather than invention, I'd need:\n\n- Current standings",
        "I appreciate you providing all these details, but I need the actual "
        "standings and results context to write this preview.\n\nCould you provide:",
        "I need more information to write this preview. Please provide:\n\n"
        "- Current standings or records for both teams",
        "I need to write a 2-3 sentence preview for this NCAA Men's Soccer "
        "match between Chicago State and DePaul. However, I don't have any of "
        "the required information",
        "I'd be happy to write a preview, but I need the data lines to ground "
        "it properly. Could you provide:",
    ]

    REAL_PREVIEWS = [
        "Internacional is three points from safety with twelve games left and "
        "can't afford many more slips. Santos sits comfortably in Sudamericana "
        "territory but just lost to Internacional earlier this season.",
        "Paris FC arrive in third place and already staking a claim in the UCL "
        "zone after three matches, while Marseille sit well back in 11th.",
        "Old Dominion hosts Bucknell in a non-conference matchup at 1:00 PM "
        "EDT. This is the season opener for both teams, so there's no form to "
        "lean on.",
        # The exact false-positive risk the marker list is written to avoid.
        "Milan need a win here to stay in the Champions League places, and "
        "Juventus can't provide them one cheaply.",
        "Spurs have to provide an answer after a dismal start; they need three "
        "points and they need them today.",
    ]

    def test_every_real_refusal_is_caught(self):
        for text in self.REAL_REFUSALS:
            assert llm.looks_like_non_preview(text) is True, text[:60]

    def test_no_real_preview_is_rejected(self):
        for text in self.REAL_PREVIEWS:
            assert llm.looks_like_non_preview(text) is False, text[:60]

    def test_markdown_heading_is_caught(self):
        assert llm.looks_like_non_preview("# Michigan at Notre Dame\n\nNotre Dame hosts...")

    def test_bulleted_list_is_caught(self):
        assert llm.looks_like_non_preview("Preview:\n- one thing\n- another")

    def test_empty_is_caught(self):
        assert llm.looks_like_non_preview("") is True

    def test_a_hyphen_inside_prose_is_not_a_bullet(self):
        assert llm.looks_like_non_preview(
            "Flamengo's perfect record against Remo this season - a dominant "
            "3-0 win in March - sets up another lopsided affair."
        ) is False

    def test_a_refusal_falls_back_to_the_deterministic_description(self):
        cache = {}
        out = llm.llm_describe_or_fallback(
            g={"home": "Brown", "away": "Saint Peter's", "extra": {}},
            tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk",
            caller=lambda ctx, k, m: "I don't have the current standings.",
        )
        assert out == "DETERMINISTIC"
        assert cache == {}, "a rejected response must not be cached"

    def test_a_good_response_is_still_returned_and_cached(self):
        """Fail on the instrument: if the guard rejected everything this test
        would be the only thing to notice."""
        cache = {}
        out = llm.llm_describe_or_fallback(
            g={"home": "Brown", "away": "Saint Peter's", "extra": {}},
            tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk",
            # Deliberately free of season-stage phrasing: with no "Season
            # progress" line in the context, "a season opener" would be
            # rejected by the season-stage guard and this test would then be
            # measuring the wrong thing.
            caller=lambda ctx, k, m: "Brown host Saint Peter's at Stevenson Field.",
        )
        assert out == "Brown host Saint Peter's at Stevenson Field."
        assert len(cache) == 1

    def test_prompt_tells_the_model_never_to_refuse(self):
        assert "ALWAYS write the preview" in llm.SYSTEM_PROMPT
        assert "never a request for more data" in llm.SYSTEM_PROMPT
        assert "There is no one to answer you" in llm.SYSTEM_PROMPT

    def test_prompt_forbids_markdown_structure_explicitly(self):
        assert "Output ONLY the preview itself" in llm.SYSTEM_PROMPT
        assert "no headings" in llm.SYSTEM_PROMPT


class TestUngroundedConferenceGuard:
    """Told to always write something and given almost nothing, the model
    started inventing conference affiliations instead of refusing. All four
    observed on the live guide 2026-09-06 were wrong."""

    CTX = "Match: Howard at Manhattan, Today 12:00 PM EDT\nCompetition: NCAA Men's Soccer"

    def test_invented_patriot_league_is_caught(self):
        prose = ("Two teams hunting for ground in the Patriot League dance for "
                 "position. Howard visits Manhattan.")
        assert llm.names_an_ungrounded_conference(prose, self.CTX) == "patriot league"

    def test_invented_pac12_is_caught(self):
        prose = "UCLA's top-12 form faces an unranked Omaha side in a late-night Pac-12 showdown."
        assert llm.names_an_ungrounded_conference(prose, self.CTX) == "pac-12"

    def test_invented_ivy_league_is_caught(self):
        prose = "Brown hosts Saint Peter's in an Ivy League clash tonight."
        assert llm.names_an_ungrounded_conference(prose, self.CTX) == "ivy league"

    def test_a_conference_present_in_the_context_is_allowed(self):
        ctx = "Match: A at B\nCompetition: Big Ten Conference"
        prose = "A visits B in a Big Ten clash."
        assert llm.names_an_ungrounded_conference(prose, ctx) is None

    def test_clean_prose_passes(self):
        prose = ("Marshall hosts VCU tonight. The Thundering Herd bring top-15 "
                 "credentials to the pitch.")
        assert llm.names_an_ungrounded_conference(prose, self.CTX) is None

    def test_short_acronyms_do_not_match_inside_a_word(self):
        """"SEC" and "ACC" must not fire on "second" or "accelerate"."""
        prose = ("Milan accelerate in the second half and their defence was "
                 "impeccable; the access to space was total.")
        assert llm.names_an_ungrounded_conference(prose, self.CTX) is None

    def test_real_acronym_use_is_still_caught(self):
        prose = "Two SEC programs meet with bowl positioning on the line."
        assert llm.names_an_ungrounded_conference(prose, self.CTX) == "sec"

    def test_an_ungrounded_conference_falls_back(self):
        cache = {}
        out = llm.llm_describe_or_fallback(
            g={"home": "Manhattan", "away": "Howard", "extra": {}},
            tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk",
            caller=lambda ctx, k, m: "Howard visits Manhattan in a Patriot League clash.",
        )
        assert out == "DETERMINISTIC"
        assert cache == {}

    def test_prompt_forbids_naming_an_unsupplied_conference(self):
        assert "NEVER name a conference or division that is not written above" in llm.SYSTEM_PROMPT

    def test_prompt_forbids_guessing_the_season_stage(self):
        assert "Season\n  progress" in llm.SYSTEM_PROMPT or "Season progress" in llm.SYSTEM_PROMPT
        assert "are all guesses" in llm.SYSTEM_PROMPT


class TestUngroundedSeasonStageGuard:
    """An NCAA soccer fixture whose source supplies no season length got no
    "Season progress" line, and was previewed as "late-season matches like
    this can reshape the postseason conversation". It was 6 September."""

    THIN = "Match: Omaha at UCLA, Today 10:00 PM EDT\nCompetition: NCAA Men's Soccer"
    WITH_PROGRESS = THIN + "\nSeason progress: 3 of 38 matchdays played."

    def test_late_season_without_a_progress_line_is_caught(self):
        prose = "late-season matches like this can reshape the postseason conversation."
        assert llm.names_an_ungrounded_season_stage(prose, self.THIN) == "late-season"

    def test_down_the_stretch_is_caught(self):
        prose = "Both teams are jockeying for position down the stretch."
        assert llm.names_an_ungrounded_season_stage(prose, self.THIN) == "down the stretch"

    def test_early_season_is_caught_too(self):
        """Being right by accident is still ungrounded: the model had no way
        to know, and it said "late" for the same September date elsewhere."""
        prose = "An early season tuneup for both squads."
        assert llm.names_an_ungrounded_season_stage(prose, self.THIN) == "early season"

    def test_allowed_when_a_progress_line_justifies_it(self):
        prose = "An early season clash with plenty still to play for."
        assert llm.names_an_ungrounded_season_stage(prose, self.WITH_PROGRESS) is None

    def test_clean_thin_preview_passes(self):
        prose = ("Marshall hosts VCU tonight. The Thundering Herd bring top-15 "
                 "credentials to the pitch.")
        assert llm.names_an_ungrounded_season_stage(prose, self.THIN) is None

    def test_it_falls_back(self):
        cache = {}
        out = llm.llm_describe_or_fallback(
            g={"home": "UCLA", "away": "Omaha", "extra": {}},
            tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk",
            caller=lambda ctx, k, m: "A late-season test for the Bruins.",
        )
        assert out == "DETERMINISTIC"
        assert cache == {}

    def test_prompt_forbids_placing_the_season_without_grounds(self):
        assert "unless a \"Season" in llm.SYSTEM_PROMPT
        assert "are all guesses" in llm.SYSTEM_PROMPT


class TestGuardsApplyToTheCachedPath:
    """The guards ran only on a fresh call, so prose that predated a guard was
    served straight from cache and never re-examined. Measured on the live
    guide: "late-season matches like this" survived a deploy that had already
    added the check meant to catch it, because the prompt hash had not moved
    and the cached copy short-circuited the whole check."""

    THIN_G = {"home": "UCLA", "away": "Omaha", "sport_label": "NCAA Men's Soccer",
              "kickoff_local": "Today", "extra": {}}

    def _key(self, model="m"):
        ctx = llm.build_llm_context(self.THIN_G, "", "")
        return f"mk|{llm.prompt_hash(ctx, model)}"

    def test_a_poisoned_cache_entry_is_evicted_and_re_asked(self):
        cache = {self._key(): "A late-season test for the Bruins."}
        calls = []

        def caller(ctx, k, m):
            calls.append(1)
            return "UCLA host Omaha at Wallis Annenberg Stadium."

        out = llm.llm_describe_or_fallback(
            g=self.THIN_G, tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk", caller=caller,
        )
        assert calls, "a rejected cache entry must trigger a fresh request"
        assert out == "UCLA host Omaha at Wallis Annenberg Stadium."
        assert cache[self._key()] == out, "the good response replaces the bad one"

    def test_a_poisoned_entry_whose_retry_also_fails_falls_back(self):
        cache = {self._key(): "A late-season test for the Bruins."}
        out = llm.llm_describe_or_fallback(
            g=self.THIN_G, tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk",
            caller=lambda ctx, k, m: "Another late-season clash.",
        )
        assert out == "DETERMINISTIC"
        assert self._key() not in cache

    def test_a_clean_cache_entry_is_still_served_without_a_call(self):
        """Fail on the instrument: if eviction fired on everything the cache
        would be pointless and every apply would re-bill the API."""
        cache = {self._key(): "UCLA host Omaha at Wallis Annenberg Stadium."}
        calls = []

        def caller(ctx, k, m):
            calls.append(1)
            return "should not be reached"

        out = llm.llm_describe_or_fallback(
            g=self.THIN_G, tagline="", fallback_description="DETERMINISTIC",
            api_key="k", model="m", cache=cache, marker="mk", caller=caller,
        )
        assert out == "UCLA host Omaha at Wallis Annenberg Stadium."
        assert calls == [], "a clean cached entry must not trigger a request"

    def test_reject_reason_names_each_class(self):
        ctx = "Match: Omaha at UCLA\nCompetition: NCAA Men's Soccer"
        assert llm.reject_reason("I don't have the standings.", ctx) == "not a preview"
        assert llm.reject_reason(
            "A Pac-12 showdown tonight.", ctx) == "ungrounded conference: pac-12"
        assert llm.reject_reason(
            "A late-season test.", ctx) == "ungrounded season stage: late-season"
        assert llm.reject_reason("UCLA host Omaha tonight.", ctx) is None
