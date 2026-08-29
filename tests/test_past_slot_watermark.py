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

    def test_the_apply_path_bounds_prog_end_by_both_estimates(self):
        """A behavioural test needs Django. This pins that the apply path takes
        the MINIMUM of the per-sport end and the display default, rather than
        either one alone: the flat default alone is what broke soccer, and the
        per-sport window alone would label a boxing card live for 24 hours."""
        import os
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py"
        )).read()
        assert "prog_end = min(_sport_end, _display_end) if _sport_end else _display_end" in src
        assert "prog_end = start_dt + timedelta(hours=EPG_POST_HOURS)" not in src
        assert "prog_end = _game_end_utc(g) or (" not in src


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


class TestReviewFindings:
    """Five defects found by /second-opinion on the first cut of this fix. Each
    reverts to a live failure, so each gets a test."""

    def _prog_end(self, plugin, g):
        """Mirror of the apply path's bound. Used only to feed the helper under
        test; the assertions below are literals."""
        start = plugin.parse_iso_utc(g["start_time_utc"])
        sport = plugin._game_end_utc(g)
        disp = start + timedelta(hours=plugin.EPG_POST_HOURS)
        return min(sport, disp) if sport else disp

    def test_reaping_off_does_not_collapse_the_past_row(self, plugin):
        """FINDING 1, and it hit the DEFAULT config. _reap_settings returns 0
        when reaping is off, and _game_reap_at_utc(g, 0) is game-END, not None.
        Passing that through clamped the past row to zero width, leaving every
        default install with no guide after the whistle."""
        g = {"sport_prefix": "CFB", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        prog_end = self._prog_end(plugin, g)
        remove_after = plugin._reap_settings({"remove_finished_after_minutes": 0})
        assert remove_after == 0
        # what the apply path must pass in that case
        got = plugin._compute_past_slot_end(
            prog_end, _settings(remove_finished_after_minutes=0), reap_at=None,
        )
        assert got == datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)
        assert got > prog_end

    def test_the_apply_path_passes_none_when_reaping_is_off(self):
        import os
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py"
        )).read()
        assert "if _remove_after > 0 else None" in src

    def test_the_row_is_floored_at_prog_end_even_when_the_bound_itself_is_earlier(self, plugin):
        """FINDING 2. min(end, max(ra, prog_end)) clamps the reap side but lets
        `end` land before prog_end, which _next_fire_time can do on a DST
        fall-back day. That produced a NEGATIVE-width row, which Dispatcharr
        neither rejects nor validates."""
        prog_end = KICK + timedelta(hours=4)
        # a schedule whose next fire resolves before prog_end would invert;
        # the floor must make that impossible for any inputs.
        for reap in (None, prog_end - timedelta(hours=5), prog_end, prog_end + timedelta(hours=1)):
            got = plugin._compute_past_slot_end(prog_end, _settings(), reap_at=reap)
            assert got >= prog_end, (reap, got)

    def test_boxing_is_not_labelled_live_for_a_day(self, plugin):
        """FINDING 5. _epg_match_window is a match-RECALL prefilter: boxing's
        +24h absorbs a feed whose start times can be a day off. Using it raw as
        a display duration made a card 'live' for 24 hours."""
        g = {"sport_prefix": "BOX", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        assert plugin._game_end_utc(g) == KICK + timedelta(hours=24)   # reaper still generous
        assert self._prog_end(plugin, g) == KICK + timedelta(hours=4)  # display is not

    @pytest.mark.parametrize("prefix,hours", [("EPL", 2.5), ("CFB", 4.0), ("BOX", 4.0), ("NBA", 4.0)])
    def test_display_duration_is_the_shorter_of_the_two_estimates(self, plugin, prefix, hours):
        g = {"sport_prefix": prefix, "start_time_utc": "2026-08-29T18:00:00+00:00"}
        assert self._prog_end(plugin, g) == KICK + timedelta(hours=hours)

    @pytest.mark.parametrize("prefix", ["EPL", "CFB", "BOX", "NBA", "MLB"])
    def test_prog_end_never_exceeds_the_reap_deadline_for_any_sport(self, plugin, prefix):
        """The invariant the whole design rests on, now that prog_end is a min
        of two things rather than equal to the reaper's estimate."""
        g = {"sport_prefix": prefix, "start_time_utc": "2026-08-29T18:00:00+00:00"}
        for remove_after in (1, 30, 60, 240):
            assert self._prog_end(plugin, g) <= plugin._game_reap_at_utc(g, remove_after)

    def test_lowering_remove_after_does_not_reap_under_a_live_guide_entry(self, plugin):
        """FINDING 3. The published deadline is a snapshot taken at apply time;
        the reaper re-reads the setting every tick. Lowering it would delete the
        channel while a client's cached guide still shows that game on its
        number, which is the exact failure this change exists to close."""
        g = {"sport_prefix": "CFB", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        published = _settings(remove_finished_after_minutes=120)
        # apply wrote a row ending 22:00 + 120min = 00:00
        assert plugin._published_guide_end(g, published) == datetime(
            2026, 8, 30, 0, 0, tzinfo=timezone.utc,
        )
        now = datetime(2026, 8, 29, 22, 30, tzinfo=timezone.utc)   # past the NEW deadline
        live, expired = plugin._partition_expired([g], now, 10, published)
        assert expired == [], "reaped while its own published guide entry was still running"
        assert live == [g]

    def test_the_hold_releases_once_the_published_entry_ends(self, plugin):
        g = {"sport_prefix": "CFB", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        published = _settings(remove_finished_after_minutes=120)
        now = datetime(2026, 8, 30, 0, 1, tzinfo=timezone.utc)
        live, expired = plugin._partition_expired([g], now, 10, published)
        assert expired == [g]

    def test_the_hold_is_a_noop_when_the_setting_has_not_changed(self, plugin):
        """In the steady state the two deadlines are equal, so this must not
        delay an ordinary reap by even one cycle."""
        g = {"sport_prefix": "CFB", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        st = _settings(remove_finished_after_minutes=60)
        now = plugin._game_reap_at_utc(g, 60) + timedelta(seconds=1)
        live, expired = plugin._partition_expired([g], now, 60, st)
        assert expired == [g]

    def test_partition_without_settings_keeps_the_old_behaviour(self, plugin):
        """The parameter is optional so existing callers and tests that only
        care about the time arithmetic keep working."""
        g = {"sport_prefix": "CFB", "start_time_utc": "2026-08-29T18:00:00+00:00"}
        now = plugin._game_reap_at_utc(g, 60) + timedelta(seconds=1)
        live, expired = plugin._partition_expired([g], now, 60)
        assert expired == [g]

    def test_an_empty_past_window_writes_no_row(self):
        """A zero-width programme is never current but IS serialised into the
        XMLTV verbatim, so the caller must skip it rather than write it."""
        import os
        src = open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py"
        )).read()
        i = src.index("# Past slot: bridges the final whistle")
        j = src.index("tvg_id=marker,", i)
        assert "if past_end > prog_end:" in src[i:j]
