"""Wiring for compact numbering (#202) and the EPG cache invalidation (#201).

The unit behaviour of the allocator lives in test_compact_numbering.py. This
file asserts the plugin actually CALLS it, and that the settings the user sees
declare what they promise. Both are the failure this repo has shipped before: a
correct helper that no code path reaches (see tests/test_stream_ids_contract.py
for the same shape).
"""

import ast
import json
import os
import re

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _manifest():
    with open(os.path.join(REPO_ROOT, "plugin.json")) as fh:
        return json.load(fh)


def _field(fid):
    for f in _manifest()["fields"]:
        if f.get("id") == fid:
            return f
    raise AssertionError(f"no field {fid!r} in plugin.json")


def _plugin_ast():
    with open(os.path.join(REPO_ROOT, "plugin.py")) as fh:
        return ast.parse(fh.read())


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"no function {name!r} in plugin.py")


class TestModeResolution:
    def test_default_is_kickoff_so_upgrades_do_not_renumber(self, plugin):
        # An existing install must not silently switch schemes on upgrade: every
        # channel would move at once, which is the #117 mismatch on every channel.
        assert plugin._resolve_numbering_mode({}) == "kickoff"

    def test_compact_is_selected_by_the_setting(self, plugin):
        assert plugin._resolve_numbering_mode({"channel_numbering_mode": "compact"}) == "compact"

    @pytest.mark.parametrize("raw", ["Compact", "  COMPACT  "])
    def test_case_and_whitespace_are_tolerated(self, plugin, raw):
        assert plugin._resolve_numbering_mode({"channel_numbering_mode": raw}) == "compact"

    @pytest.mark.parametrize("raw", ["kickof", "band", "1", None, ""])
    def test_unrecognised_values_fall_back_to_the_default(self, plugin, raw):
        assert plugin._resolve_numbering_mode({"channel_numbering_mode": raw}) == "kickoff"


class TestBandSizeResolution:
    def test_sentinel_zero_sizes_the_band_to_the_slate(self, plugin):
        assert plugin._resolve_compact_band_size({"compact_band_size": 0}, 25) == 25

    def test_an_explicit_size_wins(self, plugin):
        assert plugin._resolve_compact_band_size({"compact_band_size": 100}, 25) == 100

    def test_a_string_from_the_settings_blob_is_accepted(self, plugin):
        assert plugin._resolve_compact_band_size({"compact_band_size": "100"}, 25) == 100

    @pytest.mark.parametrize("raw", ["abc", None, -3])
    def test_junk_falls_back_to_auto(self, plugin, raw):
        assert plugin._resolve_compact_band_size({"compact_band_size": raw}, 25) == 25

    def test_never_returns_less_than_one(self, plugin):
        assert plugin._resolve_compact_band_size({"compact_band_size": 0}, 0) == 1


def _games(n):
    """n games, each 10 minutes after the last. Minutes rather than hours so a
    25-game slate stays inside a single valid day: an hour past 23 parses as
    None and the game is silently skipped, which reads as a band-too-narrow
    failure and sent the first run of these tests chasing production code."""
    return [
        {
            "sport": "TEST",
            "away": f"Away{i}",
            "home": f"Home{i}",
            "start_time_utc": f"2026-08-29T12:{i * 2:02d}:00+00:00",
            "extra": {"espn_id": f"e{i}"},
        }
        for i in range(n)
    ]


class TestAssignChannelNumbersHonoursTheMode:
    def test_kickoff_mode_produces_the_large_time_encoded_numbers(self, plugin):
        from datetime import timezone
        got = plugin._assign_channel_numbers(_games(3), 400, timezone.utc)
        # Late August 2026 is ~5.5M minutes*slots past the origin. The literal
        # matters: this is exactly the number the user complained about in #202,
        # and it is the correct behaviour for the DEFAULT mode.
        assert all(v > 5_000_000 for v in got.values()), got

    def test_compact_mode_produces_the_band_the_user_asked_for(self, plugin):
        from datetime import timezone
        got = plugin._assign_channel_numbers(
            _games(25), 400, timezone.utc, mode="compact", band_size=25,
        )
        assert sorted(got.values()) == list(range(400, 425))

    def test_compact_mode_sorts_a_fresh_band_by_kickoff(self, plugin):
        from datetime import timezone
        games = _games(3)
        games.reverse()  # hand them over in the WRONG order on purpose
        got = plugin._assign_channel_numbers(
            games, 400, timezone.utc, mode="compact", band_size=25,
        )
        by_number = sorted(got.items(), key=lambda kv: kv[1])
        earliest_marker = plugin._build_marker_key(_games(3)[0])
        assert by_number[0][0] == earliest_marker

    def test_compact_mode_keeps_a_published_game_on_its_number(self, plugin):
        from datetime import timezone
        games = _games(3)
        marker = plugin._build_marker_key(games[1])
        got = plugin._assign_channel_numbers(
            games, 400, timezone.utc, mode="compact", band_size=25,
            existing_numbers={marker: 417},
        )
        assert got[marker] == 417

    def test_a_narrow_band_omits_markers_rather_than_duplicating(self, plugin):
        from datetime import timezone
        got = plugin._assign_channel_numbers(
            _games(10), 400, timezone.utc, mode="compact", band_size=3,
        )
        assert len(got) == 3
        assert len(set(got.values())) == 3


class TestApplyCallsTheAllocator:
    """A behavioural test of the apply path needs Django, so these read the
    source. Every one of them goes red if the wiring is removed while the
    helpers keep working, which is exactly how a fix ships inert."""

    def test_assign_channel_numbers_is_called_with_the_mode_and_band(self):
        tree = _plugin_ast()
        fn = _function(tree, "_action_apply")
        calls = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "_assign_channel_numbers"
        ]
        assert len(calls) == 1, "expected exactly one numbering call site in _action_apply"
        kwargs = {kw.arg for kw in calls[0].keywords}
        assert {"mode", "band_size", "existing_numbers"} <= kwargs, kwargs

    def test_the_write_loop_tolerates_a_missing_marker(self):
        """A narrow band legitimately omits a game. Subscripting would raise
        KeyError mid-transaction and abort the whole apply."""
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        assert "chnum_by_marker.get(marker)" in src
        assert "target_chnum = chnum_by_marker[marker]" not in src

    def test_apply_invalidates_the_epg_output_cache(self):
        """#201. Without this the XMLTV serves the previous slate for 300s."""
        fn = _function(_plugin_ast(), "_action_apply")
        names = [
            n.func.id for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        ]
        assert "_invalidate_epg_output_cache" in names

    def test_the_invalidation_is_outside_the_atomic_block(self):
        """Invalidating inside the transaction lets a concurrent /output/epg
        rebuild from pre-commit rows and re-populate the cache we are evicting."""
        fn = _function(_plugin_ast(), "_action_apply")
        inside = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.With):
                for sub in ast.walk(node):
                    if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
                            and sub.func.id == "_invalidate_epg_output_cache"):
                        inside.add(sub.lineno)
        assert not inside, f"invalidation called inside a with-block at {inside}"

    def test_the_invalidator_swallows_a_missing_core_function(self):
        """It reaches into Dispatcharr internals that are not a documented API.
        A build that moves the function must cost a stale guide, not the apply."""
        fn = _function(_plugin_ast(), "_invalidate_epg_output_cache")
        assert any(isinstance(n, ast.Try) for n in ast.walk(fn))


class TestApplyPassesTheReservedNumbers:
    """The allocator can only skip a colliding number if apply hands it the set.
    A behavioural test needs Django, so this reads the call site."""

    def test_reserved_numbers_is_passed_to_the_allocator(self):
        fn = _function(_plugin_ast(), "_action_apply")
        call = next(
            n for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id == "_assign_channel_numbers"
        )
        assert "reserved_numbers" in {kw.arg for kw in call.keywords}

    def test_the_reserved_query_excludes_our_own_game_channels(self):
        """Including the channels we are about to renumber would make every game
        collide with itself and the band would come back empty. The exclusion
        lives in _reserved_channel_numbers; see TestReviewFindings for the
        narrower rule about which of our channels stay reserved."""
        fn = _function(_plugin_ast(), "_reserved_channel_numbers")
        src = ast.get_source_segment(
            open(os.path.join(REPO_ROOT, "plugin.py")).read(), fn
        )
        assert "_owned_tvg_id_q()" in src
        assert "exclude" in src


class TestUnplaceableGamesAreDroppedBeforeThePrePass:
    """WITNESS for the stranding defect found live on 2026-08-29.

    seen_markers is populated by the pre-pass, so a game that reaches the
    pre-pass and is skipped in the write loop is neither renumbered nor reaped:
    it keeps the phase-0 parking number. That left 22 live channels sitting at
    1400-1421 with no test complaining.
    """

    def test_games_are_filtered_before_the_park_base_is_resolved(self):
        """Order matters: the filter has to run before _resolve_park_base, which
        is the first thing that depends on the final slate."""
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        filt = src.index("placeable = [g for g in games if _build_marker_key(g) in chnum_by_marker]")
        park = src.index("park_base = _resolve_park_base(max_target)")
        assert filt < park, "the placeable filter must precede park_base resolution"

    def test_the_filter_reassigns_games(self):
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        i = src.index("placeable = [g for g in games")
        j = src.index("park_base = _resolve_park_base", i)
        assert "games = placeable" in src[i:j], "filtering without rebinding does nothing"

    def test_a_completely_full_band_aborts_instead_of_reaping_the_group(self):
        """Publishing nothing would make every existing channel stale and reap
        the whole group over a mis-set range."""
        fn = _function(_plugin_ast(), "_action_apply")
        src = ast.get_source_segment(
            open(os.path.join(REPO_ROOT, "plugin.py")).read(), fn
        )
        i = src.index("if not placeable:")
        j = src.index("games = placeable")
        block = src[i:j]
        assert "return" in block, "must return early, not fall through to the writes"
        assert '"status": "error"' in block

    def test_the_abort_names_the_setting_to_change(self):
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        i = src.index("if not placeable:")
        j = src.index("games = placeable", i)
        block = src[i:j]
        assert "Starting channel number" in block
        assert "Compact range size" in block

    def test_kickoff_mode_is_not_filtered(self):
        """The filter is compact-only; kickoff numbers cannot fail to allocate,
        and running it there would change behaviour for every existing user."""
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        i = src.index("placeable = [g for g in games")
        head = src[:i]
        guard = head[head.rindex("if numbering_mode"):]
        assert "NUMBERING_MODE_COMPACT" in guard


class TestReviewFindings:
    """Six defects found by /second-opinion after the live test. Each of these
    goes red if the corresponding fix is reverted."""

    def test_reserved_numbers_use_effective_not_raw_values(self):
        """A ChannelOverride can renumber a channel without touching
        Channel.channel_number, and every output path resolves the override
        first. Reading the raw column misses a channel overridden INTO the band."""
        fn = _function(_plugin_ast(), "_reserved_channel_numbers")
        src = ast.get_source_segment(
            open(os.path.join(REPO_ROOT, "plugin.py")).read(), fn
        )
        assert "with_effective_values" in src
        assert "effective_channel_number" in src

    def test_the_reserved_query_keeps_the_recordings_archive_reserved(self):
        """ARCHIVE_TVG_ID carries TVG_ID_PREFIX, so a bare _owned_tvg_id_q()
        exclude would drop it. It lives in another group and publishes
        globally, so a band covering its number would collide with it."""
        fn = _function(_plugin_ast(), "_reserved_channel_numbers")
        src = ast.get_source_segment(
            open(os.path.join(REPO_ROOT, "plugin.py")).read(), fn
        )
        assert "channel_group=target_group" in src, (
            "must exclude only OUR channels in the target group, not everything we own"
        )

    def test_seen_markers_is_added_after_the_start_time_guard(self):
        """A marker added before a later `continue` leaves its channel parked
        forever. Applies to kickoff mode too; this predates compact numbering.

        Matches the STATEMENT, not any mention of it: the rule is important
        enough to be discussed in nearby comments, and an earlier version of
        this test used a bare `src.index("seen_markers.add(marker)")` that a
        comment quoting the call broke while the code was still correct.
        """
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        guard = src.index('logger.warning("[ranked_matchups] bad start_time_utc on %s", marker)')
        statements = [
            m.start() for m in re.finditer(
                r"^[ \t]*seen_markers\.add\(marker\)[ \t]*$", src, re.M)
        ]
        assert statements, "the seen_markers.add(marker) statement has gone missing"
        assert min(statements) > guard, (
            "seen_markers.add must come after the start-time guard; found at "
            f"{statements} vs guard at {guard}"
        )

    def test_the_restore_uses_written_numbers_not_the_plan(self):
        """chnum_by_marker keeps entries for games that were filtered out or
        skipped. Treating those as occupied refuses a kept-for-recording
        channel its own original number and leaves it at park_base."""
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        assert "assigned_now = written_numbers" in src
        assert "assigned_now = set(chnum_by_marker.values())" not in src

    def test_written_numbers_is_populated_where_a_channel_is_actually_numbered(self):
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        i = src.index("target_chnum = chnum_by_marker.get(marker)")
        j = src.index("prog_start =", i)
        assert "written_numbers.add(target_chnum)" in src[i:j]

    def test_the_full_band_error_does_not_claim_nothing_changed(self):
        """By that point apply may already have created the target group,
        migrated channels after a rename, and created the EPG source."""
        src = open(os.path.join(REPO_ROOT, "plugin.py")).read()
        i = src.index("Compact numbering found no free channel numbers")
        block = src[i:i + 700]
        assert "Nothing was changed" not in block
        assert "added, renumbered or removed" in block


class TestSettingsModalExplainsTheTradeoff:
    """Jake asked for the failure mode to be explicit in the settings modal.
    These pin that it stays explicit, not merely that the fields exist."""

    def test_the_mode_field_is_a_select_with_both_schemes(self):
        f = _field("channel_numbering_mode")
        assert f["type"] == "select"
        assert {o["value"] for o in f["options"]} == {"kickoff", "compact"}

    def test_the_manifest_default_matches_the_code_default(self, plugin):
        assert _field("channel_numbering_mode")["default"] == plugin.DEFAULT_NUMBERING_MODE
        assert _field("compact_band_size")["default"] == plugin.DEFAULT_COMPACT_BAND_SIZE

    def test_the_mode_help_text_says_what_each_mode_does(self):
        help_text = _field("channel_numbering_mode")["help_text"].lower()
        assert "kickoff" in help_text and "compact" in help_text
        assert "400" in help_text, "give the reader a concrete example number"

    def test_the_mode_help_text_warns_about_sharing_a_number(self):
        """The one hazard the user can still walk into, because they choose the
        range: the guide identifies a channel by its number alone, so a range
        overlapping their existing lineup breaks both channels."""
        help_text = _field("channel_numbering_mode")["help_text"].lower()
        assert "guide" in help_text
        assert "skip" in help_text or "already use" in help_text

    def test_the_band_size_help_text_says_zero_means_exact(self):
        help_text = _field("compact_band_size")["help_text"].lower()
        assert "0" in help_text
        assert "400-424" in help_text or "400, 401" in help_text

    def test_the_settings_do_not_advise_reserving_extra_room_for_safety(self):
        """#204 made an exact range safe: every programme published for a game
        now ends when its channel does, so number reuse cannot be misread. The
        old text told users to widen the range to make reuse "rare", which is
        both unnecessary and scatters their channels. If that advice reappears,
        the fix has probably been reverted."""
        blob = " ".join(
            _field(f)["help_text"].lower()
            for f in ("channel_numbering_mode", "compact_band_size", "virtual_channel_base")
        )
        for phrase in ("reuse becomes rare", "buys the safety", "worst for guide accuracy",
                       "delays slot reuse", "reuse anything"):
            assert phrase not in blob, f"stale widen-the-range advice is back: {phrase!r}"

    def test_the_settings_warn_about_overlapping_the_existing_lineup(self):
        """Found live: a band over an existing Peacock lineup silently broke the
        guide for every channel. The user picks the band, so the user has to be
        told that numbers already in use are skipped."""
        blob = (
            _field("compact_band_size")["help_text"]
            + " " + _field("channel_numbering_mode")["help_text"]
            + " " + _field("virtual_channel_base")["help_text"]
        ).lower()
        assert "already" in blob and ("skip" in blob or "in use" in blob)
        assert "room" in blob or "occupied" in blob

    def test_the_starting_number_help_text_no_longer_promises_a_known_location(self):
        """#202: it used to say you could pin a number 'if you want them at a
        known location', which is false in the default mode."""
        help_text = _field("virtual_channel_base")["help_text"].lower()
        assert "known location" not in help_text
        assert "compact" in help_text and "kickoff" in help_text, (
            "must say which mode makes the number literal"
        )
