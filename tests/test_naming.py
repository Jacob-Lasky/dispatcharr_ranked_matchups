"""Tests for the channel-name template engine (naming.py, issue #100), the
inline-rank default (#99), and the tagline prettifier (#98). Pure logic: no
Django, no network."""

import json
import os
from datetime import datetime, timezone, timedelta

import pytest

from dispatcharr_ranked_matchups import naming
from dispatcharr_ranked_matchups.scoring import (
    GameSignals,
    Weights,
    STAGE_BANDS,
    format_channel_name,
    pick_tagline,
    render_importance_tagline,
    score_game,
    tournament_stage_label,
)


# --------------------------------------------------------------------------- #
# render_name: the {group}-collapse contract                                   #
# --------------------------------------------------------------------------- #

class TestRenderName:
    def test_bare_text_is_literal(self):
        assert naming.render_name("hi {away_team}", {"away_team": "LSU"}) == "hi LSU"

    def test_group_keeps_literal_decoration_when_present(self):
        out = naming.render_name("{away_team}{ (rank_away)}", {"away_team": "LSU", "rank_away": "5"})
        assert out == "LSU (5)"

    def test_group_collapses_whole_when_empty(self):
        # The parens + leading space live INSIDE the group and must vanish with
        # the empty token: no orphaned "( )" or trailing space.
        out = naming.render_name("{away_team}{ (rank_away)}", {"away_team": "LSU", "rank_away": ""})
        assert out == "LSU"

    def test_multiple_optional_groups_collapse_independently(self):
        tmpl = "{league_short} {away_team}{ (rank_away)} at {home_team}{ (rank_home)}{ · tagline}"
        ctx = {"league_short": "NFL", "away_team": "Bills", "home_team": "Chiefs",
               "rank_away": "", "rank_home": "", "tagline": ""}
        assert naming.render_name(tmpl, ctx) == "NFL Bills at Chiefs"

    def test_unknown_group_emitted_literally(self):
        # No known token -> braces stripped, inner kept (validate_template warns).
        assert naming.render_name("{nope}", {}) == "nope"

    def test_longest_token_wins(self):
        # rank_home must not lose to a shorter prefix match.
        out = naming.render_name("{rank_home}", {"rank_home": "7", "rank": "X"})
        assert out == "7"


class TestDefaultTemplateNoDrift:
    def test_plugin_json_default_matches_code_constant(self):
        # plugin.json's name_template default is the pre-filled UI value; the
        # code uses DEFAULT_NAME_TEMPLATE when the field is blank. They must
        # stay equal so the UI default and the blank-field default never diverge.
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        manifest = json.load(open(os.path.join(repo_root, "plugin.json"), encoding="utf-8"))
        field = next(f for f in manifest["fields"] if f["id"] == "name_template")
        assert field["default"] == naming.DEFAULT_NAME_TEMPLATE


class TestValidateTemplate:
    def test_valid_default(self):
        assert naming.validate_template(naming.DEFAULT_NAME_TEMPLATE) == []

    def test_unbalanced_braces(self):
        assert "Unbalanced { } braces." in naming.validate_template("{away_team at {home_team}")

    def test_unknown_variable_reported(self):
        errs = naming.validate_template("{leag_short} {away_team}")
        assert any("leag_short" in e for e in errs)


# --------------------------------------------------------------------------- #
# build_context: rank_source gate, favorite star, date/time                    #
# --------------------------------------------------------------------------- #

class TestBuildContext:
    def test_poll_rank_renders(self):
        ctx = naming.build_context(rank_home=5, rank_away=12, rank_source="poll")
        assert ctx["rank_home"] == "5" and ctx["rank_away"] == "12"
        assert ctx["rank_pair"] == "5v12"

    def test_standings_rank_suppressed(self):
        # League-table position is not a poll rank: it must NOT render inline.
        ctx = naming.build_context(rank_home=3, rank_away=9, rank_source="standings")
        assert ctx["rank_home"] == "" and ctx["rank_away"] == ""
        assert ctx["rank_pair"] == ""

    def test_rank_pair_blank_unless_both_ranked(self):
        ctx = naming.build_context(rank_home=5, rank_away=None, rank_source="poll")
        assert ctx["rank_pair"] == ""
        assert ctx["rank_home"] == "5" and ctx["rank_away"] == ""

    def test_favorite_star(self):
        assert naming.build_context(favorite=True)["favorite_star"] == naming._FAVORITE_STAR
        assert naming.build_context(favorite=False)["favorite_star"] == ""

    def test_score_formatted_one_decimal(self):
        assert naming.build_context(score_final=8.0)["score"] == "8.0"

    def test_date_time_tokens_use_tz(self):
        # A fixed UTC kickoff rendered into a +0 tz is deterministic.
        dt = datetime(2026, 11, 14, 19, 30, tzinfo=timezone.utc)
        ctx = naming.build_context(start_dt=dt, tz=timezone.utc)
        assert ctx["game_date"] == "Sat Nov 14"
        assert ctx["start_time"] == "7:30 PM"

    def test_date_time_blank_without_start_dt(self):
        ctx = naming.build_context(tz=timezone.utc)
        assert ctx["game_date"] == "" and ctx["start_time"] == "" and ctx["kickoff"] == ""


# --------------------------------------------------------------------------- #
# preview_lines: the test-button data path                                     #
# --------------------------------------------------------------------------- #

class TestPreviewLines:
    def test_default_template_has_no_errors(self):
        errors, lines = naming.preview_lines(naming.DEFAULT_NAME_TEMPLATE)
        assert errors == []
        assert len(lines) == len(naming._PREVIEW_SAMPLES)

    def test_default_renders_expected_samples(self):
        _, lines = naming.preview_lines(naming.DEFAULT_NAME_TEMPLATE)
        rendered = {label: name for label, name in lines}
        assert rendered["Ranked vs unranked, favorite, college"] == (
            "CBB ⭐★8.5 · Alabama (15) at St. John's · top-25 matchup"
        )
        assert rendered["Both ranked, college football"] == (
            "CFB ★9.2 · Ohio State (5) at Penn State (1) · top-5 showdown"
        )
        # standings game: ranks suppressed, no inline parens
        assert rendered["Standings race, soccer (ranks suppressed)"] == (
            "EPL ★10.0 · Brentford at Manchester United · title race"
        )
        # pro game: every optional group collapses, no stray separators
        assert rendered["Pro game, no ranks / no tagline / no favorite"] == (
            "NFL ★7.2 · Buffalo Bills at Kansas City Chiefs"
        )

    def test_invalid_template_surfaces_errors(self):
        errors, _ = naming.preview_lines("{bogus} {away_team}")
        assert errors and any("bogus" in e for e in errors)


# --------------------------------------------------------------------------- #
# format_channel_name: default + custom template + truncation                  #
# --------------------------------------------------------------------------- #

class TestFormatChannelNameTemplate:
    def test_custom_template_reshapes_name(self):
        sig = GameSignals(rank_a=1, rank_b=5)
        score = score_game(sig, Weights())
        name = format_channel_name(
            "CFB", sig, score, "Penn State", "Ohio State",
            tagline="top-5 showdown",
            template="{away_team} v {home_team}{ - tagline}",
        )
        assert name == "Ohio State v Penn State - top-5 showdown"

    def test_custom_template_with_date(self):
        sig = GameSignals(rank_a=None, rank_b=None)
        score = score_game(sig, Weights())
        dt = datetime(2026, 11, 14, 19, 0, tzinfo=timezone.utc)
        name = format_channel_name(
            "NFL", sig, score, "Chiefs", "Bills",
            template="{game_date} {start_time} · {away_team} at {home_team}",
            start_dt=dt, tz=timezone.utc,
        )
        assert name == "Sat Nov 14 7:00 PM · Bills at Chiefs"

    def test_standings_source_suppresses_inline_rank(self):
        sig = GameSignals(rank_a=3, rank_b=9)
        score = score_game(sig, Weights())
        name = format_channel_name(
            "EPL", sig, score, "Manchester United", "Brentford", rank_source="standings",
        )
        assert "(3)" not in name and "(9)" not in name

    def test_truncates_to_250(self):
        sig = GameSignals(rank_a=1, rank_b=2)
        score = score_game(sig, Weights())
        name = format_channel_name("CFB", sig, score, "A" * 200, "B" * 200, tagline="x" * 200)
        assert len(name) <= 250 and name.endswith("...")

    def test_invalid_template_param_does_not_crash_here(self):
        # format_channel_name renders whatever template it's given; the apply
        # path validates+falls back. A no-token group degrades gracefully.
        sig = GameSignals(rank_a=None, rank_b=None)
        score = score_game(sig, Weights())
        name = format_channel_name("NFL", sig, score, "Chiefs", "Bills", template="{nope} {away_team}")
        assert "Bills" in name


# --------------------------------------------------------------------------- #
# Tagline prettifier (#98)                                                     #
# --------------------------------------------------------------------------- #

class TestTaglinePrettify:
    def test_bracket_band_is_clean_no_race(self):
        assert render_importance_tagline(["omaha_bound"]) == "Road to Omaha"
        assert render_importance_tagline(["round_of_32"]) == "Round of 32"
        assert render_importance_tagline(["elite_8"]) == "Elite Eight"
        assert render_importance_tagline(["super_bowl"]) == "Super Bowl"

    def test_no_raw_snake_case_leaks(self):
        for label in STAGE_BANDS:
            out = render_importance_tagline([label])
            assert "_" not in out
            assert not out.endswith(" race")

    def test_standings_band_keeps_race_and_humanizes(self):
        assert render_importance_tagline(["title"]) == "title race"
        assert render_importance_tagline(["relegation"]) == "relegation race"
        # snake_case standings label gets de-underscored but keeps "race"
        assert render_importance_tagline(["playoff_bubble"]) == "playoff bubble race"

    def test_empty_thresholds(self):
        assert render_importance_tagline([]) == ""

    def test_pick_tagline_routes_bracket_band_clean(self):
        tag = pick_tagline(
            score_breakdown={"importance": 5.0},
            favorites_matched=[],
            spread=None,
            importance_thresholds=["omaha_bound"],
            tournament_stage=None,
            rank_a=None, rank_b=None,
        )
        assert tag == "Road to Omaha"

    def test_tournament_stage_label(self):
        assert tournament_stage_label("ROUND_OF_16") == "Round of 16"
        assert tournament_stage_label("last_16") == "Round of 16"
        assert tournament_stage_label(None) == ""
        assert tournament_stage_label("NONSENSE") == ""
