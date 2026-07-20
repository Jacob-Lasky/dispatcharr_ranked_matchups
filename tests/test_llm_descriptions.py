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
        # the fix (grounding lines are the data half). It targets fabricated
        # SERIES facts specifically, so single-game knockouts can still be
        # framed as win-or-go-home.
        assert "Do NOT fabricate playoff series facts" in llm.SYSTEM_PROMPT
        assert "facing elimination" in llm.SYSTEM_PROMPT
        assert "win or go home" in llm.SYSTEM_PROMPT

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
