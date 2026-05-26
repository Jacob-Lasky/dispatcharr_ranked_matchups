"""Tests for the rivalries.json + rivalries.py rivalry-detection helper."""
from __future__ import annotations

import importlib.util
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_rivalries_mod():
    mod_name = f"{PKG_NAME}.rivalries"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(REPO_ROOT, "rivalries.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


rivalries = _load_rivalries_mod()


class TestNormalize:
    def test_lowercases(self):
        assert rivalries._normalize("Manchester City") == "manchester city"

    def test_strips_whitespace(self):
        assert rivalries._normalize("  Arsenal   FC  ") == "arsenal fc"

    def test_empty_safe(self):
        assert rivalries._normalize("") == ""
        assert rivalries._normalize(None) == ""


class TestIsRivalry:
    def test_known_epl_pair(self):
        assert rivalries.is_rivalry("Liverpool FC", "Manchester United FC", "EPL")

    def test_order_independent(self):
        # Same pair, swapped: should still match.
        assert rivalries.is_rivalry("Manchester United FC", "Liverpool FC", "EPL")

    def test_case_insensitive(self):
        assert rivalries.is_rivalry("liverpool fc", "MANCHESTER UNITED FC", "EPL")

    def test_substring_handles_fd_org_suffixes(self):
        # FD.org names have trailing FC / AFC; the JSON stores bare names.
        assert rivalries.is_rivalry("Arsenal FC", "Tottenham Hotspur FC", "EPL")
        assert rivalries.is_rivalry("Manchester City FC", "Manchester United FC", "EPL")

    def test_unknown_pair_returns_false(self):
        assert not rivalries.is_rivalry("Brighton", "Burnley", "EPL")

    def test_unknown_sport_returns_false(self):
        assert not rivalries.is_rivalry("Anyone", "Anyone", "NOT_A_REAL_SPORT")

    def test_missing_team_names_safe(self):
        assert not rivalries.is_rivalry("", "Manchester United FC", "EPL")
        assert not rivalries.is_rivalry("Liverpool FC", "", "EPL")
        assert not rivalries.is_rivalry("", "", "EPL")

    def test_ncaa_football_pair(self):
        assert rivalries.is_rivalry("Alabama", "Auburn", "CFB")
        assert rivalries.is_rivalry("Ohio State", "Michigan", "CFB")

    def test_nba_classic(self):
        assert rivalries.is_rivalry("Boston Celtics", "Los Angeles Lakers", "NBA")

    def test_nhl_original_six(self):
        assert rivalries.is_rivalry("Montreal Canadiens", "Boston Bruins", "NHL")

    def test_mlb_yankees_red_sox(self):
        assert rivalries.is_rivalry("New York Yankees", "Boston Red Sox", "MLB")

    def test_la_classico_either_name(self):
        # Real Madrid vs Barcelona: full names + abbreviations.
        assert rivalries.is_rivalry("Real Madrid", "Barcelona", "LaLiga")
        assert rivalries.is_rivalry("Real Madrid CF", "FC Barcelona", "LaLiga")

    def test_paris_sg_abbreviation(self):
        # SportsDB returns "Paris SG"; FD.org returns "Paris Saint-Germain FC".
        # JSON has both forms: both should match Marseille.
        assert rivalries.is_rivalry("Paris SG", "Olympique de Marseille", "Ligue1")
        assert rivalries.is_rivalry("Paris Saint-Germain FC", "Marseille", "Ligue1")

    def test_cross_sport_no_false_positive(self):
        # Liverpool isn't a rivalry in MLS even if a "Liverpool" entry existed.
        assert not rivalries.is_rivalry("Liverpool FC", "Manchester United FC", "MLS")

    def test_handles_one_word_team_name_substring(self):
        # NCAA Football "Texas vs Oklahoma": bare 1-word names. Both
        # appear in many other team names ("UT-Austin Texas Longhorns"),
        # but the JSON entries are exact-bare so they only match teams
        # whose name actually contains "Texas".
        assert rivalries.is_rivalry("Texas", "Oklahoma", "CFB")
        assert rivalries.is_rivalry("Texas Longhorns", "Oklahoma Sooners", "CFB")


class TestLoadRivalries:
    def test_json_is_valid(self):
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # At least the sports listed in the issue are present.
        for sport in ("CFB", "EPL", "NBA", "NHL", "MLB", "NFL"):
            assert sport in raw, f"Missing sport {sport} in rivalries.json"
            assert isinstance(raw[sport], list)
            assert len(raw[sport]) > 0

    def test_every_pair_has_two_strings(self):
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, pairs in raw.items():
            if key.startswith("_"):
                continue
            for pair in pairs:
                assert isinstance(pair, list) and len(pair) == 2
                assert all(isinstance(s, str) and s.strip() for s in pair), \
                    f"Bad pair in {key}: {pair}"

    def test_no_self_rivalries(self):
        # A team can't be its own rival.
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, pairs in raw.items():
            if key.startswith("_"):
                continue
            for a, b in pairs:
                assert rivalries._normalize(a) != rivalries._normalize(b), \
                    f"Self-rivalry in {key}: {a} / {b}"
