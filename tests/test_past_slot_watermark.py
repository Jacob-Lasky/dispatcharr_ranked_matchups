"""The past EPG row must not outlive the channel it describes (#204).

WHY THIS MATTERS MORE THAN IT LOOKS. Dispatcharr keys the guide on the bare
channel number (Xtream Codes hardcodes `epg_channel_id = str(channel_num_int)`;
M3U and XMLTV default to it). The plugin publishes a game-NAMED "past" row after
the final whistle. If that row is still running when the reaper deletes the
channel and compact numbering hands its number to another game, a client holding
an older guide renders "Final: <old game>" over the new one. That is #117, and
the cause is not number reuse: it is a programme that outlives its channel.

Bounding the past row by the reap deadline makes the last row we ever publish
for a game end exactly when its number becomes available, so the handover is
safe by construction. These tests pin that, plus the second-duration-table bug
that made a soccer channel get reaped before its own live row ended.

Times are asserted as literal offsets from a fixed kickoff, never re-derived
from the constants under test.
"""

from datetime import datetime, timedelta, timezone

import pytest

KICK = datetime(2026, 8, 29, 18, 0, tzinfo=timezone.utc)


def _settings(**over):
    s = {
        "auto_refresh_enabled": True,
        "local_timezone": "UTC",
        "scheduled_times": "04:00",
        "remove_finished_after_minutes": 60,
    }
    s.update(over)
    return s


class TestPastRowNeverOutlivesTheChannel:
    def test_past_end_is_clamped_to_the_reap_deadline(self, plugin):
        # Game ends 22:00, reaped 23:00, next refresh 04:00 tomorrow.
        # Without the clamp the past row would run to 04:00: five hours after
        # the channel is gone and its number reassigned.
        prog_end = KICK + timedelta(hours=4)          # 22:00
        reap_at = prog_end + timedelta(minutes=60)    # 23:00
        got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=reap_at)
        assert got == datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)

    def test_without_a_reap_deadline_it_still_runs_to_the_next_refresh(self, plugin):
        """Reaping off means the channel really does survive to the next
        refresh, so the original bound is correct and must not change."""
        prog_end = KICK + timedelta(hours=4)
        got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=None)
        assert got == datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)

    def test_the_next_refresh_still_wins_when_it_comes_first(self, plugin):
        """A reap deadline far in the future must not EXTEND the row past the
        refresh that would rewrite it anyway."""
        prog_end = KICK + timedelta(hours=4)              # 22:00
        reap_at = prog_end + timedelta(hours=48)          # two days out
        got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=reap_at)
        assert got == datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)

    def test_the_row_never_ends_before_the_live_row_does(self, plugin):
        """A zero-or-negative-width programme is either rejected or renders as
        an artifact. Clamping must floor at prog_end."""
        prog_end = KICK + timedelta(hours=4)
        reap_at = prog_end - timedelta(hours=3)   # nonsense, but must not invert
        got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=reap_at)
        assert got == prog_end

    def test_the_clamp_applies_on_the_auto_refresh_disabled_path(self, plugin):
        prog_end = KICK + timedelta(hours=4)
        reap_at = prog_end + timedelta(minutes=60)
        got = plugin._compute_past_slot_end(
            prog_end, _settings(auto_refresh_enabled=False), reap_at=reap_at,
        )
        assert got == datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)

    def test_the_clamp_applies_when_scheduled_times_is_unparseable(self, plugin):
        prog_end = KICK + timedelta(hours=4)
        reap_at = prog_end + timedelta(minutes=60)
        got = plugin._compute_past_slot_end(
            prog_end, _settings(scheduled_times="nonsense"), reap_at=reap_at,
        )
        assert got == datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)

    def test_a_naive_reap_deadline_is_treated_as_utc(self, plugin):
        prog_end = KICK + timedelta(hours=4)
        naive = datetime(2026, 8, 29, 23, 0)   # no tzinfo
        got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=naive)
        assert got == datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)


class TestNoSecondDurationTable:
    """_game_end_utc's docstring forbids a second per-sport duration table. The
    apply path had one anyway: a flat EPG_POST_HOURS for prog_end, while the
    reaper used the per-sport window. They disagreed for every soccer game."""

    @pytest.mark.parametrize("prefix,hours", [("EPL", 2.5), ("CFB", 4.0), ("BOX", 24.0)])
    def test_game_end_uses_the_per_sport_window(self, plugin, prefix, hours):
        g = {"sport_prefix": prefix, "start_time_utc": "2026-08-29T18:00:00+00:00"}
        assert plugin._game_end_utc(g) == KICK + timedelta(hours=hours)

    def test_a_soccer_channel_is_not_reaped_before_its_live_row_ends(self, plugin):
        """The failure the docstring warns about, made concrete. With a flat 4h
        prog_end and a 2.5h reap window, reap_at landed 30 minutes BEFORE the
        live programme claimed the match finished."""
        g = {"sport_prefix": "EPL", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        prog_end = plugin._game_end_utc(g)
        reap_at = plugin._game_reap_at_utc(g, 60)
        assert prog_end == KICK + timedelta(hours=2.5)     # 20:30
        assert reap_at == KICK + timedelta(hours=3.5)      # 21:30
        assert reap_at > prog_end, "the reaper must never outrun the live row"

    @pytest.mark.parametrize("prefix", ["EPL", "CFB", "BOX", "NBA", "NHL"])
    @pytest.mark.parametrize("remove_after", [0, 1, 30, 60, 240])
    def test_reap_is_never_before_prog_end_for_any_sport(self, plugin, prefix, remove_after):
        g = {"sport_prefix": prefix, "start_time_utc": "2026-08-29T18:00:00+00:00"}
        assert plugin._game_reap_at_utc(g, remove_after) >= plugin._game_end_utc(g)

    def test_the_apply_path_derives_prog_end_from_game_end_utc(self):
        """A behavioural test needs Django. This pins that the apply path stops
        re-deriving the end from the flat constant."""
        import os
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py"
        )).read()
        assert "prog_end = _game_end_utc(g) or (" in src
        assert "prog_end = start_dt + timedelta(hours=EPG_POST_HOURS)" not in src


class TestTheHandoverIsSafeByConstruction:
    """The property the whole fix exists to deliver: for any sport and any reap
    delay, every programme we publish for a game has ended by the time its
    channel number can be handed to another game."""

    @pytest.mark.parametrize("prefix", ["EPL", "CFB", "BOX", "NBA", "MLB"])
    @pytest.mark.parametrize("remove_after", [1, 15, 60, 180])
    def test_last_published_end_never_exceeds_the_reap_deadline(
        self, plugin, prefix, remove_after,
    ):
        g = {"sport_prefix": prefix, "start_time_utc": "2026-08-29T18:00:00+00:00"}
        prog_end = plugin._game_end_utc(g)
        reap_at = plugin._game_reap_at_utc(g, remove_after)
        past_end = plugin._compute_past_slot_end(
            prog_end, _settings(remove_finished_after_minutes=remove_after),
            reap_at=reap_at,
        )
        assert past_end <= reap_at, (
            f"{prefix}: a game-named programme survives {past_end - reap_at} "
            f"past the moment its channel number is reassigned"
        )
