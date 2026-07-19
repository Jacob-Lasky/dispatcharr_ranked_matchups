"""Tests for the honours.json + honours.py trophy-grounding helper.

The regression this exists for: a World Cup final (Spain 1 title, Argentina 3)
whose LLM preview claimed one side was "going for their third crown" — false
for both. honours_lines() feeds the real counts so the model has truth to
ground on, and asserts an explicit zero for a trophyless finalist.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_honours_mod():
    mod_name = f"{PKG_NAME}.honours"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(REPO_ROOT, "honours.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


honours = _load_honours_mod()


class TestHonoursLines:
    def test_wc_final_names_both_counts(self):
        """The exact regression case: Spain (1) and Argentina (3), final."""
        lines = honours.honours_lines("Spain", "Argentina", "WC", "FINAL")
        assert len(lines) == 1
        line = lines[0]
        assert line.startswith("Honours (World Cup):")
        assert "Spain — 1 title (2010)" in line
        assert "Argentina — 3 titles (1978, 1986, 2022)" in line

    def test_wc_final_makes_third_crown_claim_false(self):
        """Neither finalist has exactly two titles, so 'going for a third'
        cannot be grounded in the supplied line."""
        line = honours.honours_lines("Spain", "Argentina", "WC", "FINAL")[0]
        assert "2 titles" not in line

    def test_complete_comp_states_zero(self):
        """A WC finalist absent from the (complete) winners list is stated as
        zero, not omitted — that is what kills the hallucination for the
        trophyless side."""
        line = honours.honours_lines("Croatia", "Argentina", "WC", "FINAL")[0]
        assert "Croatia — no World Cup titles yet" in line
        assert "Argentina — 3 titles" in line

    def test_group_stage_returns_nothing(self):
        assert honours.honours_lines("Spain", "Argentina", "WC", "GROUP_STAGE") == []

    def test_missing_stage_returns_nothing(self):
        assert honours.honours_lines("Spain", "Argentina", "WC", None) == []

    def test_semifinal_still_fires(self):
        """Scope is knockout, not final-only: a semifinal gets honours too."""
        lines = honours.honours_lines("Spain", "France", "WC", "SEMI_FINALS")
        assert lines and "Honours (World Cup):" in lines[0]

    def test_untracked_competition_returns_nothing(self):
        assert honours.honours_lines("Arsenal FC", "Chelsea FC", "PL", "FINAL") == []

    def test_no_competition_code(self):
        assert honours.honours_lines("Spain", "Argentina", None, "FINAL") == []

    def test_euro_maps_to_euro_key(self):
        line = honours.honours_lines("Spain", "England", "EC", "FINAL")[0]
        assert line.startswith("Honours (European Championship):")
        assert "Spain — 4 titles" in line
        assert "England — no European Championship titles yet" in line

    def test_ucl_partial_omits_unlisted_club(self):
        """Champions League list is NOT complete, so an unlisted club is
        omitted rather than falsely asserted to have zero."""
        line = honours.honours_lines("Real Madrid CF", "SomeMinnow FC", "CL", "FINAL")[0]
        # The phrase echoes the game's team name (consistent with the context's
        # "Match:" line), resolved via substring against the stored "Real Madrid".
        assert "Real Madrid CF — 15 titles" in line
        assert "SomeMinnow" not in line
        assert "no Champions League titles" not in line

    def test_ucl_both_unlisted_returns_nothing(self):
        assert honours.honours_lines("MinnowA FC", "MinnowB FC", "CL", "FINAL") == []

    def test_club_substring_match_bidirectional(self):
        """FD's 'Real Madrid CF' resolves to the stored 'Real Madrid'."""
        line = honours.honours_lines("Real Madrid CF", "AC Milan", "CL", "FINAL")[0]
        assert "Real Madrid CF — 15 titles" in line
        assert "AC Milan — 7 titles" in line

    def test_singular_title_grammar(self):
        line = honours.honours_lines("Spain", "Croatia", "WC", "FINAL")[0]
        assert "Spain — 1 title (2010)" in line  # singular, not "1 titles"

    def test_long_year_list_collapses_to_recent(self):
        """A >5-year list shows the count + most recent year, not all years."""
        assert honours._phrase("X", [1, 2, 3, 4, 5, 6], "Foo") == "X — 6 titles (most recent 6)"


class TestHonoursLoader:
    def test_load_skips_bool_nonpositive_and_underscore_keys(self, tmp_path, monkeypatch):
        """Exercise the REAL _load_honours: bool (int subclass), zero/negative
        counts, malformed years, and _-prefixed comment keys are all dropped;
        int counts and year lists survive (and years get sorted)."""
        p = tmp_path / "honours.json"
        p.write_text(json.dumps({
            "_comment": "skip me",
            "WC": {
                "_note": "skip me too",
                "BoolTeam": True,       # bool is an int subclass -> reject
                "ZeroTeam": 0,          # non-positive -> reject
                "NegTeam": -1,          # non-positive -> reject
                "BadYears": ["x"],      # non-int years -> reject
                "CountTeam": 2,         # valid int count -> keep
                "YearTeam": [2012, 2001],  # valid, gets sorted -> keep
            },
        }), encoding="utf-8")
        monkeypatch.setattr(honours, "_HONOURS_PATH", str(p))
        loaded = honours._load_honours()
        assert set(loaded.keys()) == {"WC"}
        wc = loaded["WC"]
        assert set(wc.keys()) == {"countteam", "yearteam"}
        assert wc["countteam"] == 2
        assert wc["yearteam"] == [2001, 2012]  # sorted

    def test_load_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(honours, "_HONOURS_PATH", str(tmp_path / "nope.json"))
        assert honours._load_honours() == {}

    def test_load_corrupt_json_returns_empty(self, tmp_path, monkeypatch):
        p = tmp_path / "honours.json"
        p.write_text("{ not valid json", encoding="utf-8")
        monkeypatch.setattr(honours, "_HONOURS_PATH", str(p))
        assert honours._load_honours() == {}

    def test_real_file_loads_counts_and_skips_notes(self):
        """The shipped honours.json loads: COPA as int counts, UCL present,
        and no _-prefixed note leaks in as a team."""
        assert honours._HONOURS.get("COPA", {}).get("argentina") == 16
        assert "real madrid" in honours._HONOURS.get("UCL", {})
        assert "_note" not in honours._HONOURS.get("COPA", {})

    def test_json_is_valid_and_years_sane(self):
        with open(os.path.join(REPO_ROOT, "honours.json"), encoding="utf-8") as f:
            data = json.load(f)
        for comp in ("WC", "EURO"):
            for team, years in data[comp].items():
                if team.startswith("_"):
                    continue
                assert isinstance(years, list)
                assert all(1900 <= y <= 2100 for y in years), (comp, team, years)
