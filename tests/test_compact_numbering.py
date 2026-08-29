"""Compact channel numbering (#202) and the invariants it must not break.

Assertions here are written against LITERAL channel numbers (400, 401, 424)
rather than against the constants and band arithmetic under test. A test that
says `assert n == band_start + i` re-derives the production expression and stays
green when that expression is wrong; the whole point of this scheme is that a
user typing 400 sees 400, so 400 is what the tests say.
"""

import pytest

from dispatcharr_ranked_matchups._util import (
    allocate_compact_numbers,
    _next_free_slot,
)


class TestFreshBandFillsChronologically:
    """An empty band is filled straight down the order it is given, so a first
    apply comes out sorted by kickoff with no gaps."""

    def test_twenty_five_games_occupy_400_through_424(self):
        markers = [f"g{i:02d}" for i in range(25)]
        got = allocate_compact_numbers(markers, 400, 25)
        assert got["g00"] == 400
        assert got["g01"] == 401
        assert got["g24"] == 424
        assert sorted(got.values()) == list(range(400, 425))

    def test_order_of_markers_is_the_order_of_numbers(self):
        got = allocate_compact_numbers(["late", "mid", "early"], 400, 3)
        assert got == {"late": 400, "mid": 401, "early": 402}

    def test_band_wider_than_the_slate_leaves_the_top_empty(self):
        got = allocate_compact_numbers(["a", "b", "c"], 400, 100)
        assert got == {"a": 400, "b": 401, "c": 402}


class TestPublishedGamesNeverMove:
    """The property the whole scheme rests on. A game that already has a channel
    keeps that channel's number, because Xtream Codes binds the guide to the
    integer channel number and a number that moves shows one game's name over
    another game's programme (#117)."""

    def test_existing_assignment_is_handed_back_unchanged(self):
        existing = {"a": 407, "b": 402}
        got = allocate_compact_numbers(["a", "b"], 400, 25, existing)
        assert got["a"] == 407
        assert got["b"] == 402

    def test_existing_game_keeps_its_slot_even_when_a_lower_one_is_free(self):
        # 400-406 are all empty, but 'a' is live on 407 and must stay there.
        got = allocate_compact_numbers(["a", "new"], 400, 25, {"a": 407})
        assert got["a"] == 407

    def test_a_number_outside_the_band_is_not_honoured(self):
        # A leftover from kickoff mode, or from a band the user has since moved.
        got = allocate_compact_numbers(["a"], 400, 25, {"a": 5548720})
        assert got["a"] == 400

    def test_repeat_allocation_is_idempotent(self):
        markers = [f"g{i:02d}" for i in range(10)]
        first = allocate_compact_numbers(markers, 400, 25)
        second = allocate_compact_numbers(markers, 400, 25, first)
        assert second == first


class TestNewGamesAllocateAboveTheHighestSlotInUse:
    """Reserving more numbers than you have channels is the user's lever on how
    often a slot gets reused. That lever only works if new games take fresh
    numbers instead of filling the lowest hole."""

    def test_a_promoted_game_does_not_take_the_freed_low_slot(self):
        # 400-404 were live; the 402 game finished and a new one is promoted.
        existing = {"a": 400, "b": 401, "d": 403, "e": 404}
        got = allocate_compact_numbers(["a", "b", "d", "e", "new"], 400, 100, existing)
        assert got["new"] == 405
        assert 402 not in got.values()

    def test_reuse_only_happens_once_the_band_is_exhausted(self):
        # A band of exactly 5, one hole at 402, one new game: nothing else fits.
        existing = {"a": 400, "b": 401, "d": 403, "e": 404}
        got = allocate_compact_numbers(["a", "b", "d", "e", "new"], 400, 5, existing)
        assert got["new"] == 402

    def test_a_tight_band_reuses_immediately_which_is_the_documented_cost(self):
        # base 400, size 25, 25 live games: the band is full every cycle, so a
        # finished game hands its number straight to the next one. This is what
        # the settings help text warns about; pinning it so the warning cannot
        # quietly stop being true.
        live = {f"g{i:02d}": 400 + i for i in range(25)}
        del live["g07"]
        markers = [m for m in live] + ["promoted"]
        got = allocate_compact_numbers(markers, 400, 25, live)
        assert got["promoted"] == 407


class TestBandTooNarrow:
    """A band narrower than the slate drops games rather than publishing one
    with no number: a null channel_number truncates the whole Xtream Codes feed."""

    def test_only_the_band_size_worth_of_games_get_numbers(self):
        markers = [f"g{i:02d}" for i in range(10)]
        got = allocate_compact_numbers(markers, 400, 4)
        assert len(got) == 4
        assert sorted(got.values()) == [400, 401, 402, 403]

    def test_the_games_kept_are_the_earliest_ones(self):
        got = allocate_compact_numbers(["first", "second", "third"], 400, 2)
        assert set(got) == {"first", "second"}
        assert "third" not in got

    def test_zero_or_negative_size_yields_nothing(self):
        assert allocate_compact_numbers(["a"], 400, 0) == {}
        assert allocate_compact_numbers(["a"], 400, -5) == {}


class TestNoDuplicateNumbers:
    """Two channels in one group cannot share a number: the DB has a unique
    constraint on (channel_group, channel_number) and the apply would raise."""

    def test_duplicate_existing_numbers_are_not_both_honoured(self):
        # Shouldn't happen, but a hand-edited DB could produce it.
        got = allocate_compact_numbers(["a", "b"], 400, 25, {"a": 405, "b": 405})
        assert got["a"] == 405
        assert got["b"] != 405
        assert len(set(got.values())) == 2

    @pytest.mark.parametrize("size", [1, 5, 25, 100])
    def test_allocation_is_always_injective(self, size):
        markers = [f"g{i:03d}" for i in range(40)]
        got = allocate_compact_numbers(markers, 400, size)
        assert len(set(got.values())) == len(got)

    @pytest.mark.parametrize("size", [1, 5, 25, 100])
    def test_every_number_lands_inside_the_band(self, size):
        markers = [f"g{i:03d}" for i in range(40)]
        got = allocate_compact_numbers(markers, 400, size)
        for n in got.values():
            assert 400 <= n <= 400 + size - 1


class TestReservedNumbersAreSkipped:
    """WITNESS for the collision defect found by live-testing on 2026-08-29.

    The DB's unique constraint is (channel_group, channel_number), so our
    channel 401 can legally coexist with the user's channel 401 in another
    group. The M3U, XMLTV and Xtream Codes outputs all key the guide on the bare
    channel number, so the two collapse into one <channel id="401"> and the guide
    binds arbitrarily. A band of 400-499 over a real Peacock lineup put ALL 22 of
    our channels on another channel's programme, and every unit test still
    passed. Reverting the `reserved` argument turns these red.
    """

    def test_a_reserved_number_is_never_handed_out(self):
        got = allocate_compact_numbers(["a", "b"], 400, 25, None, reserved={400, 401})
        assert 400 not in got.values()
        assert 401 not in got.values()
        assert got == {"a": 402, "b": 403}

    def test_allocation_flows_around_a_block_of_reserved_numbers(self):
        got = allocate_compact_numbers(
            ["a", "b", "c"], 400, 25, None, reserved=set(range(400, 410)),
        )
        assert sorted(got.values()) == [410, 411, 412]

    def test_a_fully_reserved_band_yields_nothing_rather_than_colliding(self):
        got = allocate_compact_numbers(
            ["a", "b"], 400, 5, None, reserved=set(range(400, 405)),
        )
        assert got == {}

    def test_reserved_numbers_outside_the_band_are_irrelevant(self):
        got = allocate_compact_numbers(["a"], 400, 25, None, reserved={9999, 5548720})
        assert got == {"a": 400}

    def test_a_published_game_sitting_on_a_now_reserved_number_is_moved(self):
        """If the user renumbers a real channel onto one of ours, holding our
        game there would keep the broken binding forever. Moving costs one
        mismatch window; staying costs a permanent one."""
        got = allocate_compact_numbers(["a"], 400, 25, {"a": 405}, reserved={405})
        assert got["a"] != 405
        assert 400 <= got["a"] <= 424

    def test_reserved_does_not_break_the_no_duplicates_invariant(self):
        markers = [f"g{i:02d}" for i in range(15)]
        got = allocate_compact_numbers(
            markers, 400, 100, None, reserved=set(range(400, 450)),
        )
        assert len(set(got.values())) == len(got)
        assert all(n >= 450 for n in got.values())


class TestNextFreeSlot:
    def test_wraps_at_the_top_of_the_band(self):
        # Cursor sits on the last slot and it is taken, so the search wraps.
        assert _next_free_slot({402}, 402, 400, 402) == 400
        # Cursor already past the band wraps to the bottom.
        assert _next_free_slot({400}, 403, 400, 402) == 401

    def test_a_free_slot_at_the_cursor_is_taken_without_wrapping(self):
        assert _next_free_slot({401}, 402, 400, 402) == 402

    def test_returns_none_when_the_band_is_full(self):
        assert _next_free_slot({400, 401, 402}, 400, 400, 402) is None

    def test_a_cursor_below_the_band_starts_at_the_band(self):
        assert _next_free_slot(set(), 1, 400, 424) == 400
