"""Verify the FIFA Annex C lookup table in sources/soccer.py covers
all C(12, 8) = 495 combinations of advancing-3rd-placer group letters,
and that every row's slot assignment satisfies the L32 bracket
constraints declared in _WC2026_LAST_32_PAIRINGS.

This test is the integrity check for #77's data transcription. The
table is parsed from FIFA's published Annex C via Wikipedia
(en.wikipedia.org/wiki/Template:2026_FIFA_World_Cup_third-place_table).
A bug in the table would silently miscompute leverage signals for
WC group games on R16+ outcomes; the test fails fast on any drift.

Coverage:
  - Cardinality: exactly 495 keys (C(12, 8) combinations of A..L).
  - Per-row shape: exactly 8 (l32_match_idx, side, source_letter) triples.
  - Per-row constraint: source_letter is in the allowed_groups set
    declared for that l32_match_idx in _WC2026_LAST_32_PAIRINGS.
  - Per-row uniqueness: no two triples target the same slot, and the
    source_letters in a row match the row's key letter-for-letter.
  - Key uniqueness: implicit from dict semantics, but we also
    enumerate combinations and assert coverage.
"""
import importlib.util
import itertools
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_soccer_module():
    if f"{PKG_NAME}.sources.soccer" in sys.modules:
        return sys.modules[f"{PKG_NAME}.sources.soccer"]
    # The package alias is already registered by conftest. We need to
    # also register the `sources` subpackage before importing soccer.
    sources_pkg = sys.modules.get(f"{PKG_NAME}.sources")
    if sources_pkg is None:
        import types
        sources_pkg = types.ModuleType(f"{PKG_NAME}.sources")
        sources_pkg.__path__ = [os.path.join(REPO_ROOT, "sources")]
        sys.modules[f"{PKG_NAME}.sources"] = sources_pkg

    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.sources.soccer",
        os.path.join(REPO_ROOT, "sources", "soccer.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.sources.soccer"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def soccer():
    return _load_soccer_module()


def _build_allowed_groups_per_slot(soccer):
    """For each l32_match_idx that has a best_third slot, return the
    allowed_groups frozenset and the side ('home' / 'away') the slot
    is on. Pulls from soccer._WC2026_LAST_32_PAIRINGS so the test
    stays consistent if the pairings table is edited."""
    out = {}
    for l32_idx, (home_slot, away_slot) in enumerate(soccer._WC2026_LAST_32_PAIRINGS):
        for side, slot in (("home", home_slot), ("away", away_slot)):
            if slot[0] == "best_third":
                out[(l32_idx, side)] = slot[1]
    return out


class TestTableCardinality:
    def test_has_exactly_495_keys(self, soccer):
        assert len(soccer._WC2026_THIRD_PLACER_SLOT_TABLE) == 495

    def test_covers_every_8_of_12_combination(self, soccer):
        expected_keys = {frozenset(combo) for combo in itertools.combinations("ABCDEFGHIJKL", 8)}
        actual_keys = set(soccer._WC2026_THIRD_PLACER_SLOT_TABLE.keys())
        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys
        assert not missing, f"missing {len(missing)} combinations: {sorted(missing)[:5]}..."
        assert not extra, f"unexpected combinations: {sorted(extra)[:5]}..."

    def test_keys_have_8_letters_each_in_alphabet(self, soccer):
        alphabet = set("ABCDEFGHIJKL")
        for key in soccer._WC2026_THIRD_PLACER_SLOT_TABLE.keys():
            assert len(key) == 8, f"key {key} does not have 8 letters"
            assert key <= alphabet, f"key {key} has letters outside A-L"


class TestPerRowShape:
    def test_every_value_has_exactly_8_triples(self, soccer):
        for key, value in soccer._WC2026_THIRD_PLACER_SLOT_TABLE.items():
            assert len(value) == 8, f"key {sorted(key)} value has {len(value)} triples (expected 8)"

    def test_triple_shape_is_int_str_str(self, soccer):
        for key, value in soccer._WC2026_THIRD_PLACER_SLOT_TABLE.items():
            for t in value:
                assert isinstance(t, tuple) and len(t) == 3
                l32_idx, side, source_letter = t
                assert isinstance(l32_idx, int)
                assert side in ("home", "away")
                assert isinstance(source_letter, str) and len(source_letter) == 1


@pytest.mark.parametrize("key", sorted(
    {frozenset(c) for c in itertools.combinations("ABCDEFGHIJKL", 8)},
    key=lambda f: "".join(sorted(f)),
))
class TestPerRowConstraints:
    """Parametrized over all 495 combinations -- one test instance per
    row of the table. Fast assertions; full sweep finishes in milliseconds.
    A constraint violation here means the table data has a transcription
    bug; fix the source, regenerate, and rerun."""

    def test_row_targets_only_best_third_slots(self, soccer, key):
        allowed_slots = _build_allowed_groups_per_slot(soccer)
        row = soccer._WC2026_THIRD_PLACER_SLOT_TABLE[key]
        for l32_idx, side, _ in row:
            assert (l32_idx, side) in allowed_slots, (
                f"row {sorted(key)}: slot ({l32_idx}, {side}) is not a "
                f"reserved best_third slot in _WC2026_LAST_32_PAIRINGS"
            )

    def test_source_letters_satisfy_allowed_groups(self, soccer, key):
        allowed_slots = _build_allowed_groups_per_slot(soccer)
        row = soccer._WC2026_THIRD_PLACER_SLOT_TABLE[key]
        for l32_idx, side, source_letter in row:
            allowed = allowed_slots[(l32_idx, side)]
            assert source_letter in allowed, (
                f"row {sorted(key)}: slot ({l32_idx}, {side}) assigned to "
                f"group {source_letter} but only {sorted(allowed)} are allowed"
            )

    def test_no_two_slots_overlap(self, soccer, key):
        row = soccer._WC2026_THIRD_PLACER_SLOT_TABLE[key]
        targets = [(l32_idx, side) for l32_idx, side, _ in row]
        assert len(set(targets)) == len(targets), (
            f"row {sorted(key)}: duplicate target slot in {targets}"
        )

    def test_source_letters_match_key(self, soccer, key):
        row = soccer._WC2026_THIRD_PLACER_SLOT_TABLE[key]
        source_letters = {source for _, _, source in row}
        assert source_letters == set(key), (
            f"row {sorted(key)}: assigned source letters {sorted(source_letters)} "
            f"differ from key {sorted(key)}"
        )


class TestBuildBracketSeedUsesCanonicalMapping:
    """End-to-end check that _build_bracket_seed prefers the Annex C
    table over the strongest-first greedy fallback. The two paths
    produce different assignments for the same input -- the canonical
    mapping pins the source group per slot regardless of best-3rd-placer
    strength order, the greedy fallback walks the strongest-first
    qualifying_thirds list and grabs the first allowed match.

    For the {ABCDEFGH} key (Wikipedia's row 495), Annex C places
    group C's 3rd-placer at M74 (l32_idx=1). The greedy fallback,
    given the default test state where all 8 qualifying thirds are
    alphabetically ordered (GA3 first), would place GA3 at M74
    because A is in M74's allowed_groups {A,B,C,D,F}. This test
    asserts the canonical value, which catches a regression to the
    greedy path."""

    @staticmethod
    def _wc_source(soccer):
        return soccer.GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")

    @staticmethod
    def _state_with_thirds_abcdefgh(soccer):
        # Lifted from test_sources.py::TestBuildBracketSeedWC2026._full_wc_state
        # but inlined to avoid cross-file fixture coupling.
        team_group: dict = {}
        teams_dict: dict = {}
        for gi in range(12):
            letter = chr(ord("A") + gi)
            grp = f"GROUP_{letter}"
            # Top 8 advance their 3rd-placer; A-H get extra strength.
            third_pts = 4 if letter in "ABCDEFGH" else 1
            third_gd = 2 if letter in "ABCDEFGH" else -2
            teams = [
                (f"G{letter}1", 9, 5, 8),
                (f"G{letter}2", 6, 2, 5),
                (f"G{letter}3", third_pts, third_gd, 3),
                (f"G{letter}4", 0, -5, 1),
            ]
            for team, points, gd, gf in teams:
                team_group[team] = grp
                teams_dict[team] = {"points": points, "gf": gf, "ga": gf - gd, "played": 3}
        return {"_applied": frozenset(), "_team_group": team_group, "_teams": teams_dict}

    def test_canonical_m74_assignment(self, soccer):
        """Annex C row {ABCDEFGH}: M74's away (the slot whose home is
        1E, l32_idx=1) is group C's 3rd-placer."""
        src = self._wc_source(soccer)
        state = self._state_with_thirds_abcdefgh(soccer)
        seed = src._build_bracket_seed(state)
        l32 = seed["stages"][0]["ties"]
        m74 = l32[1]
        # Home is "GE1" (group winner E), away is the best_third per Annex C.
        assert "GE1" in {m74["home"], m74["away"]}
        third_team = m74["home"] if m74["away"] == "GE1" else m74["away"]
        # Annex C maps M74 (col 1E vs) to source group C -> team GC3.
        assert third_team == "GC3", (
            f"M74 away should be GC3 (Annex C canonical) but is {third_team}; "
            f"this indicates the table lookup didn't fire and the strongest-"
            f"first greedy fallback ran instead."
        )

    def test_canonical_m82_assignment(self, soccer):
        """Annex C row {ABCDEFGH}: M82's away (the slot whose home is
        1G, l32_idx=9) is group A's 3rd-placer. Greedy would have
        consumed GA3 earlier; this asserts canonical."""
        src = self._wc_source(soccer)
        state = self._state_with_thirds_abcdefgh(soccer)
        seed = src._build_bracket_seed(state)
        l32 = seed["stages"][0]["ties"]
        m82 = l32[9]
        assert "GG1" in {m82["home"], m82["away"]}
        third_team = m82["home"] if m82["away"] == "GG1" else m82["away"]
        assert third_team == "GA3", (
            f"M82 away should be GA3 (Annex C canonical) but is {third_team}"
        )

    def test_all_eight_canonical_assignments(self, soccer):
        """Full assignment check for {ABCDEFGH}: every best_third slot
        gets the team Annex C names."""
        src = self._wc_source(soccer)
        state = self._state_with_thirds_abcdefgh(soccer)
        seed = src._build_bracket_seed(state)
        l32 = seed["stages"][0]["ties"]
        # Pull the (l32_idx, away_team) pairs for slots with a best_third.
        away_by_idx = {}
        for idx, tie in enumerate(l32):
            home_slot, away_slot = soccer._WC2026_LAST_32_PAIRINGS[idx]
            if away_slot[0] == "best_third":
                away_by_idx[idx] = tie["away"] if tie["home"].endswith("1") else tie["home"]
        expected = {
            1: "GC3",   # M74 from row 495 col 1E vs -> 3C
            4: "GF3",   # M77 col 1I vs -> 3F
            6: "GH3",   # M79 col 1A vs -> 3H
            7: "GE3",   # M80 col 1L vs -> 3E
            8: "GB3",   # M81 col 1D vs -> 3B
            9: "GA3",   # M82 col 1G vs -> 3A
            12: "GG3",  # M85 col 1B vs -> 3G
            14: "GD3",  # M87 col 1K vs -> 3D
        }
        assert away_by_idx == expected
