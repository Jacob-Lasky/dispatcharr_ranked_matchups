"""Tests for the channel-name template engine (naming.py, issue #100), the
inline-rank default (#99), and the tagline prettifier (#98). Pure logic: no
Django, no network."""

import json
import os
from datetime import datetime, timezone

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

    def test_time_and_venue_variables_preview_filled_in(self):
        # Every sample used to have no kickoff, so a template built on
        # {game_date} / {start_time} / {kickoff} / {venue} previewed blank.
        _, lines = naming.preview_lines("{game_date}|{start_time}|{kickoff}")
        for _label, name in lines:
            date, time, kickoff = name.split("|")
            assert date and time == "7:00 PM" and kickoff.startswith("Tomorrow 7:00 PM")
        _, lines = naming.preview_lines("{away_team}{ @ venue}")
        assert dict(lines)["Ranked vs unranked, favorite, college"] == "Alabama @ Madison Square Garden"

    def test_preview_cleans_up_like_apply(self):
        # A sparse custom template leaves double spaces; apply squeezes them
        # (format_channel_name), so the preview must too or it shows a name
        # that will never be written. Measured on a live template, 2026-10-16.
        tmpl = "{away_team}  {rivalry}  at {home_team}"
        _, lines = naming.preview_lines(tmpl)
        assert dict(lines)["Ranked vs unranked, favorite, college"] == "Alabama at St. John's"

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


class TestResolveTemplate:
    """#126: apply and preview each decided the fallback independently and
    disagreed about how loud to be. resolve_template is the single decision.

    The repro from the issue: a stray '}' after {start_time} makes brace counts
    9-vs-10, validate_template rejects it, and every channel silently got the
    DEFAULT name (score included) even though the saved template has no score
    token. Nothing in apply, the guide, or show_status said so.
    """

    BROKEN = (
        "{league_short} {favorite_star} · {away_team}{ (rank_away)} at "
        "{home_team}{ (rank_home)} | {game_date}, {start_time}} { | tagline}"
    )

    def test_the_reported_template_is_rejected(self):
        assert naming.validate_template(self.BROKEN) == ["Unbalanced { } braces."]

    def test_broken_template_falls_back_to_default_and_reports(self):
        tmpl, errors, is_default = naming.resolve_template(self.BROKEN)
        assert tmpl == naming.DEFAULT_NAME_TEMPLATE
        assert errors == ["Unbalanced { } braces."]
        assert is_default is True

    def test_valid_template_is_used_verbatim_with_no_errors(self):
        good = "{league_short} · {away_team} at {home_team}"
        assert naming.resolve_template(good) == (good, [], False)

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_empty_means_default_and_is_NOT_an_error(self, raw):
        # An unset template is the normal case, not a misconfiguration; it must
        # not produce a warning banner on every apply.
        tmpl, errors, is_default = naming.resolve_template(raw)
        assert (tmpl, errors, is_default) == (naming.DEFAULT_NAME_TEMPLATE, [], True)

    def test_whitespace_is_stripped_before_use(self):
        tmpl, errors, is_default = naming.resolve_template("  {away_team} at {home_team}  ")
        assert tmpl == "{away_team} at {home_team}"
        assert (errors, is_default) == ([], False)

    def test_summary_is_empty_when_there_is_nothing_to_say(self):
        assert naming.template_problem_summary([]) == ""

    def test_summary_names_the_problem_and_the_fix(self):
        s = naming.template_problem_summary(["Unbalanced { } braces."])
        assert "Unbalanced { } braces." in s
        assert "DEFAULT" in s
        # Must tell the user WHERE to fix it, by the section's real UI label.
        assert _naming_section_label() in s

    def test_unknown_variable_group_also_reports(self):
        tmpl, errors, is_default = naming.resolve_template("{away_team}{ nonsense}")
        assert tmpl == naming.DEFAULT_NAME_TEMPLATE
        assert is_default is True
        assert any("nonsense" in e for e in errors)


# --------------------------------------------------------------------------- #
# Description template (#317)                                                  #
# --------------------------------------------------------------------------- #

def _manifest():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return json.load(open(os.path.join(repo_root, "plugin.json"), encoding="utf-8"))


def _naming_section_label():
    return next(f["label"] for f in _manifest()["fields"] if f["id"] == "_section_naming")


class TestDescriptionTemplate:
    def test_blank_means_built_in_and_is_not_an_error(self):
        assert naming.resolve_description_template("  ") == (None, [])
        assert naming.resolve_description_template(None) == (None, [])

    def test_valid_template_is_used(self):
        assert naming.resolve_description_template(" {home_team} - {llm} ") == ("{home_team} - {llm}", [])

    def test_broken_template_falls_back_and_reports(self):
        tmpl, errors = naming.resolve_description_template("{home_team} {bogus}")
        assert tmpl is None
        assert any("bogus" in e for e in errors)

    def test_description_variables_are_not_name_variables(self):
        # {llm} in the channel NAME template must still be rejected.
        assert naming.validate_template("{llm}")
        assert naming.validate_template("{llm}", naming.KNOWN_DESCRIPTION_TOKENS) == []

    def test_summary_names_the_setting_and_where_to_fix_it(self):
        s = naming.description_template_problem_summary(["Unbalanced { } braces."])
        assert "description_template" in s and _naming_section_label() in s
        # Apply falls back to the built-in description, not a "DEFAULT" template.
        assert "built-in description" in s and "DEFAULT" not in s
        assert naming.description_template_problem_summary([]) == ""

    def test_two_variables_in_one_group_are_rejected(self):
        # render_name substitutes only the first, so this printed the word
        # "llm" and skipped the Claude call (second-opinion finding, #317).
        errors = naming.validate_template("{source_channel: llm}", naming.KNOWN_DESCRIPTION_TOKENS)
        assert any("More than one variable" in e for e in errors)
        assert naming.validate_template("{(rank_away) v rank_home}")

    @pytest.mark.parametrize("tmpl", ["{home_team}}{", "{{home_team}}", "}{home_team}{"])
    def test_misordered_or_nested_braces_are_rejected(self, tmpl):
        # Balanced counts used to pass and render a stray "}{" into the output.
        assert naming.validate_template(tmpl)

    def test_real_templates_still_validate(self):
        # The shipped default and a live user's custom template (2026-10-16)
        # must survive the stricter validator untouched.
        live = ("{league_short} {favorite_star} · {away_team}{ (rank_away)} at {home_team}"
                "{ (rank_home)} | {game_date} at {start_time} { | tagline} ({score})")
        assert naming.validate_template(naming.DEFAULT_NAME_TEMPLATE) == []
        assert naming.validate_template(live) == []

    def test_empty_render_yields_the_built_in(self):
        assert naming.render_description("{llm}", {"llm": "", "details": "BUILT-IN"}) == "BUILT-IN"

    def test_preview_matches_apply_fallback_and_is_capped(self):
        # {llm} alone with the LLM off: apply writes the built-in text, so the
        # preview must show it rather than an empty line.
        assert naming.preview_description("{llm}", llm_on=False) == "A top-25 matchup: toss-up. / Source: ESPN+ 12."
        long = naming.preview_description("x" * 10_000 + "{home_team}", llm_on=False)
        assert len(long) == 200 and long.endswith("…")

    def test_line_breaks_are_kept_and_empty_groups_leave_no_gap(self):
        tmpl = "{away_team} at {home_team}{ · tagline}\n\n{llm}\n\n{source_channel}"
        out = naming.render_description(tmpl, {
            "away_team": "Bills", "home_team": "Chiefs", "tagline": "",
            "llm": "", "source_channel": "ESPN 2",
        })
        assert out == "Bills at Chiefs\n\nESPN 2"

    def test_a_group_can_carry_its_own_paragraph_break(self):
        tmpl = "{home_team}{\n\nllm}"
        assert naming.render_description(tmpl, {"home_team": "A", "llm": ""}) == "A"
        assert naming.render_description(tmpl, {"home_team": "A", "llm": "Big game."}) == "A\n\nBig game."

    def test_description_needs_llm(self):
        assert naming.description_needs_llm(None), "built-in layout swaps in the prose"
        assert naming.description_needs_llm("{home_team}{ - llm}")
        assert not naming.description_needs_llm("{home_team} at {away_team}")

    def test_template_uses(self):
        known = naming.KNOWN_DESCRIPTION_TOKENS
        assert naming.template_uses("{home_team}{ - llm}", "llm", known)
        assert not naming.template_uses("{home_team} llm", "llm", known), "bare text is not a variable"

    def test_preview_is_one_line_and_honours_the_llm_toggle(self):
        tmpl = "{away_team} at {home_team}\n\n{llm}"
        on = naming.preview_description(tmpl, llm_on=True)
        assert "\n" not in on and on.startswith("Alabama at St. John's / ")
        assert naming.preview_description(tmpl, llm_on=False) == "Alabama at St. John's"

    def test_name_help_text_names_every_variable(self):
        # {recording_dot} shipped (#216) without ever reaching the help text.
        field = next(f for f in _manifest()["fields"] if f["id"] == "name_template")
        assert "{away_team}" in field["help_text"], "instrument: help text not loaded"
        for token in naming.TOKENS:
            assert "{" + token + "}" in field["help_text"], token

    def test_help_text_names_every_description_only_variable(self):
        field = next(f for f in _manifest()["fields"] if f["id"] == "description_template")
        assert field["type"] == "text" and field["default"] == ""
        for token in naming.DESCRIPTION_ONLY_TOKENS:
            assert "{" + token + "}" in field["help_text"], token

