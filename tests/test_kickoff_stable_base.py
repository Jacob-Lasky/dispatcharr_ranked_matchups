"""Kickoff numbering must not follow the top of the user's lineup (#219).

In the default KICKOFF mode the channel number is ``floor + time term``, with
the time term in the millions. Auto mode (`virtual_channel_base = 0`) used to
re-derive the floor as "highest other channel + 1" on EVERY apply, so when a
provider group shrank (live, 2026-09-26: 6891 -> 6838) every published game moved
by the same -53, with no kickoff changing. Dispatcharr's outputs bind the guide
by the bare channel number (#117), so that is the mismatch kickoff mode exists
to prevent.

The rows here are built the way production builds them: a real `GameRow` (as
a source's fetch_upcoming returns it) serialised with the same fields the
refresh writes into cache.json, so the marker and kickoff parse exactly as
apply sees them.
"""

import ast
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _gamerow_cls():
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow


def _cache_row(game):
    """The fields the refresh writes for a game (plugin.py games_payload) that
    numbering reads: sport_prefix, teams, start_time_utc (isoformat) and extra."""
    return {
        "sport_prefix": game.sport_prefix,
        "sport_label": game.sport_label,
        "home": game.home,
        "away": game.away,
        "rank_home": game.rank_home,
        "rank_away": game.rank_away,
        "start_time_utc": game.start_time.isoformat(),
        "extra": game.extra,
    }


def _slate():
    """Seven NHL games, as on the live slate in the issue: two share a kickoff
    minute (so the tiebreak slot matters), the rest are spread over two days."""
    GameRow = _gamerow_cls()
    t0 = datetime(2026, 9, 26, 23, 0, tzinfo=timezone.utc)
    kickoffs = [t0, t0, t0 + timedelta(minutes=30), t0 + timedelta(hours=2),
                t0 + timedelta(days=1), t0 + timedelta(days=1, hours=3),
                t0 + timedelta(days=2)]
    rows = []
    for i, start in enumerate(kickoffs):
        gid = 2025010100 + i
        rows.append(_cache_row(GameRow(
            sport_prefix="NHL", sport_label="NHL",
            home=f"Home{i}", away=f"Away{i}", rank_home=None, rank_away=None,
            start_time=start, extra={"nhl_game_id": gid, "game_id": gid},
        )))
    return rows


def _published(plugin, games, numbers):
    """What apply hands _resolve_kickoff_base: (live number, kickoff, marker)."""
    out = []
    for g in games:
        m = plugin._build_marker_key(g)
        if m in numbers:
            out.append((numbers[m], plugin.parse_iso_utc(g["start_time_utc"]), m))
    return out


def _apply_numbers(plugin, games, settings, highest_other, existing, tz=timezone.utc):
    """The numbering half of _action_apply in kickoff mode, minus the ORM:
    resolve the floor (reading and writing the state file exactly as apply
    does), then assign."""
    remembered = plugin._load_kickoff_base()
    base = plugin._resolve_kickoff_base(
        settings, highest_other, remembered, _published(plugin, games, existing), tz,
    )
    if base != remembered:
        plugin._save_kickoff_base(base)
    return base, plugin._assign_channel_numbers(
        games, base, tz, existing_numbers=existing,
    )


class TestLineupChangeDoesNotRenumber:
    """WITNESSES. Each fails on the pre-#219 behaviour (floor = highest + 1 on
    every apply): the second apply's floor would be 6839, not 6892."""

    def test_the_live_repro_with_the_floor_remembered(self, plugin):
        games = _slate()
        base1, first = _apply_numbers(plugin, games, {}, 6891, {})
        assert base1 == 6892
        base2, second = _apply_numbers(plugin, games, {}, 6838, first)
        assert base2 == 6892
        assert second == first

    def test_the_upgrade_apply_infers_the_floor_from_published_channels(self, plugin):
        """The first apply after upgrading has no state file. It must read the
        floor off the channels the old version published, not off the lineup,
        or the upgrade itself is the renumber."""
        games = _slate()
        # Published by the old code at floor 6892 (highest other was 6891).
        old = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        assert plugin._load_kickoff_base() is None
        base, again = _apply_numbers(plugin, games, {}, 6838, old)
        assert base == 6892
        assert again == old
        assert plugin._load_kickoff_base() == 6892

    def test_a_lineup_that_grows_does_not_move_them_either(self, plugin):
        games = _slate()
        _, first = _apply_numbers(plugin, games, {}, 6891, {})
        _, second = _apply_numbers(plugin, games, {}, 9500, first)
        assert second == first

    def test_a_lost_state_file_moves_nothing(self, plugin):
        games = _slate()
        _, first = _apply_numbers(plugin, games, {}, 6891, {})
        os.remove(plugin.NUMBERING_STATE_PATH)
        _, second = _apply_numbers(plugin, games, {}, 100, first)
        assert second == first


class TestReviewFindings:
    """Second-opinion findings on the first cut, each reproduced red first."""

    def test_two_games_one_rescheduled_keeps_the_unchanged_one(self, plugin):
        """Two channels name two floors that each keep one channel. The
        rescheduled game's floor is off by whole hours; the lineup's is not."""
        games = _slate()[3:5]
        old = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        moved = dict(games[0])
        moved["start_time_utc"] = (
            plugin.parse_iso_utc(moved["start_time_utc"]) + timedelta(hours=2)
        ).isoformat()
        now = [moved, games[1]]
        base, got = _apply_numbers(plugin, now, {}, 6891, old)
        assert base == 6892
        m = plugin._build_marker_key(games[1])
        assert got[m] == old[m]

    def test_no_channel_on_its_hash_slot_still_recovers_a_floor_that_moves_nothing(self, plugin):
        """Channels re-keyed from the legacy marker (#217) sit on slots their new
        marker does not name, so no channel names the floor exactly."""
        games = _slate()[3:5]
        old = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        held = {}
        for m, n in old.items():
            block = n - (n - 6892) % 16
            held[m] = block + ((n - block + 5) % 16)
        _, got = _apply_numbers(plugin, games, {}, 6838, held)
        assert got == held

    def test_parking_clears_a_hand_added_channel_above_our_targets(self, plugin):
        """A channel the user put in our group at exactly max_target + 1000
        made the park UPDATE hit the per-group unique constraint."""
        assert plugin._resolve_park_base(6546427, 6547427.0) > 6547427
        assert plugin._resolve_park_base(6546427, 0) == 6547427
        assert plugin._resolve_park_base(6546427, None) == 6547427


class TestFloorResolution:
    def test_explicit_base_wins_over_everything(self, plugin):
        """Explicit `virtual_channel_base` behaves as it always did."""
        games = _slate()
        published = _published(plugin, games, plugin._assign_channel_numbers(games, 6892, timezone.utc))
        assert plugin._resolve_kickoff_base(
            {"virtual_channel_base": 400}, 6891, 6892, published, timezone.utc,
        ) == 400

    def test_remembered_floor_wins_over_inference_and_lineup(self, plugin):
        games = _slate()
        published = _published(plugin, games, plugin._assign_channel_numbers(games, 6892, timezone.utc))
        assert plugin._resolve_kickoff_base({}, 100, 7000, published, timezone.utc) == 7000

    def test_fresh_install_falls_back_to_after_the_lineup(self, plugin):
        """Nothing remembered and nothing published: no number can move, so the
        documented auto rule (highest other + 1) still picks the first floor."""
        assert plugin._resolve_kickoff_base({}, 6891, None, [], timezone.utc) == 6892
        assert plugin._resolve_kickoff_base({}, 0, None, [], timezone.utc) == 9000

    @pytest.mark.parametrize("raw", [0, "0", "", None, "abc", -5])
    def test_auto_spellings_are_not_explicit(self, plugin, raw):
        assert plugin._explicit_virtual_base({"virtual_channel_base": raw}) is None

    def test_garbage_remembered_value_is_ignored(self, plugin):
        assert plugin._resolve_kickoff_base({}, 6891, 0, [], timezone.utc) == 6892


class TestInferKickoffBase:
    def test_exact_on_a_clean_slate(self, plugin):
        games = _slate()
        nums = plugin._assign_channel_numbers(games, 12345, timezone.utc)
        assert plugin._infer_kickoff_base(_published(plugin, games, nums), timezone.utc) == 12345

    def test_one_rescheduled_game_does_not_drag_the_floor(self, plugin):
        """A game rescheduled since its channel was numbered names a wildly
        wrong candidate; the floor the rest agree on must win."""
        games = _slate()
        nums = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        moved = dict(games[3])
        moved["start_time_utc"] = (
            plugin.parse_iso_utc(moved["start_time_utc"]) + timedelta(hours=5)
        ).isoformat()
        games = games[:3] + [moved] + games[4:]
        assert plugin._infer_kickoff_base(_published(plugin, games, nums), timezone.utc) == 6892

    def test_a_held_off_hash_channel_does_not_drag_the_floor(self, plugin):
        """#217's in-block hold leaves a channel on a slot its hash no longer
        names. The floor the other channels name exactly still wins."""
        games = _slate()
        nums = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        m = plugin._build_marker_key(games[4])
        block = nums[m] - (nums[m] - 6892) % 16
        nums[m] = block + ((nums[m] - block + 7) % 16)
        assert plugin._infer_kickoff_base(_published(plugin, games, nums), timezone.utc) == 6892

    def test_compact_numbers_are_not_a_kickoff_floor(self, plugin):
        games = _slate()
        compact = {plugin._build_marker_key(g): 400 + i for i, g in enumerate(games)}
        assert plugin._infer_kickoff_base(_published(plugin, games, compact), timezone.utc) is None

    def test_nothing_published(self, plugin):
        assert plugin._infer_kickoff_base([], timezone.utc) is None


class TestStateFile:
    def test_round_trip(self, plugin):
        plugin._save_kickoff_base(6892)
        assert plugin._load_kickoff_base() == 6892
        with open(plugin.NUMBERING_STATE_PATH, encoding="utf-8") as f:
            assert json.load(f) == {"kickoff_base": 6892}

    @pytest.mark.parametrize("content", [
        "not json", "[]", '{"kickoff_base": "6892"}', '{"kickoff_base": true}',
        '{"kickoff_base": 0}', '{"kickoff_base": -3}', '{"kickoff_base": 6892.5}', "{}",
    ])
    def test_a_bad_file_reads_as_nothing(self, plugin, content):
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        assert plugin._load_kickoff_base() is None

    def test_missing_file(self, plugin):
        assert plugin._load_kickoff_base() is None

    def test_an_unwritable_path_is_logged_not_raised(self, plugin, tmp_path):
        plugin._save_kickoff_base(6892, path=str(tmp_path / "no" / "such" / "dir.json"))

    def test_the_state_file_is_gitignored(self):
        with open(os.path.join(REPO_ROOT, ".gitignore"), encoding="utf-8") as f:
            assert "numbering_state.json" in f.read().split()


class TestKickoffNeverSharesANumberWithAnotherChannel:
    """With the floor no longer following the lineup, the reservation is what
    keeps a kickoff number off a number the user's own channels publish under.
    WITNESSES: kickoff mode ignored `reserved_numbers` before #219."""

    def test_a_fresh_game_steps_off_a_reserved_number(self, plugin):
        games = _slate()
        plain = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        m = plugin._build_marker_key(games[5])
        taken = plain[m]
        got = plugin._assign_channel_numbers(
            games, 6892, timezone.utc, reserved_numbers={taken},
        )
        assert taken not in got.values()
        assert got[m] == taken + 1
        assert {k: v for k, v in got.items() if k != m} == {
            k: v for k, v in plain.items() if k != m
        }

    def test_a_held_number_now_reserved_is_released(self, plugin):
        games = _slate()
        plain = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        m = plugin._build_marker_key(games[2])
        got = plugin._assign_channel_numbers(
            games, 6892, timezone.utc, existing_numbers=plain,
            reserved_numbers={plain[m]},
        )
        assert got[m] != plain[m]
        assert plain[m] not in got.values()
        assert len(set(got.values())) == len(games)

    def test_reserved_numbers_elsewhere_change_nothing(self, plugin):
        games = _slate()
        plain = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        got = plugin._assign_channel_numbers(
            games, 6892, timezone.utc, reserved_numbers={1, 2, 6891, 99999999},
        )
        assert got == plain


def _apply_fn():
    src = open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_action_apply"
    )
    return src, fn


def _calls(node, name):
    return [
        n for n in ast.walk(node)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name
    ]


class TestApplyWiring:
    """_action_apply needs Django, so pin the call sites by source. Each goes red
    if the wiring is removed while the helpers above keep passing."""

    def _kickoff_branch(self):
        _src, fn = _apply_fn()
        for n in ast.walk(fn):
            if (isinstance(n, ast.If) and isinstance(n.test, ast.Compare)
                    and isinstance(n.test.left, ast.Name)
                    and n.test.left.id == "numbering_mode"
                    and any(isinstance(c, ast.Name) and c.id == "NUMBERING_MODE_KICKOFF"
                            for c in n.test.comparators)
                    and _calls(n, "_resolve_kickoff_base")):
                return n
        raise AssertionError("no kickoff branch resolving the floor in _action_apply")

    def test_kickoff_resolves_through_the_remembered_floor(self):
        branch = self._kickoff_branch()
        body = ast.Module(body=branch.body, type_ignores=[])
        assert _calls(body, "_resolve_kickoff_base")
        assert _calls(body, "_load_kickoff_base")
        assert not _calls(body, "_resolve_virtual_base"), (
            "kickoff must not re-derive the floor from the lineup (#219)"
        )

    def test_compact_keeps_the_lineup_rule(self):
        branch = self._kickoff_branch()
        orelse = ast.Module(body=branch.orelse, type_ignores=[])
        assert _calls(orelse, "_resolve_virtual_base")

    def test_the_floor_is_saved_only_after_commit_and_never_on_a_dry_run(self):
        """Saved before the channel transaction, a failed apply would leave a
        floor on disk no published channel uses, and the next auto apply would
        renumber every game onto it (review finding)."""
        src, fn = _apply_fn()
        saves = _calls(fn, "_save_kickoff_base")
        assert len(saves) == 1
        guards = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.If) and _calls(n, "_save_kickoff_base")
        ]
        assert guards and "not dry_run" in ast.unparse(guards[-1].test)
        assert "NUMBERING_MODE_KICKOFF" in ast.unparse(guards[-1].test)
        for w in ast.walk(fn):
            if isinstance(w, ast.With):
                assert not _calls(w, "_save_kickoff_base"), "must run after the transaction"
        (invalidate,) = _calls(fn, "_invalidate_epg_output_cache")
        assert saves[0].lineno > invalidate.lineno

    def test_parking_clears_every_number_in_the_group(self):
        src, _fn = _apply_fn()
        i = src.index("highest_in_group = (")
        block = src[i:src.index("park_base = _resolve_park_base(max_target, highest_in_group)", i)]
        assert "channel_group=target_group" in block
        assert '_Max("channel_number")' in block

    def test_reserved_numbers_are_collected_in_both_modes(self):
        _src, fn = _apply_fn()
        for n in ast.walk(fn):
            if isinstance(n, ast.If) and _calls(n, "_reserved_channel_numbers"):
                yes = _calls(ast.Module(body=n.body, type_ignores=[]), "_reserved_channel_numbers")
                no = _calls(ast.Module(body=n.orelse, type_ignores=[]), "_reserved_channel_numbers")
                assert yes and no, "both numbering modes must reserve other channels' numbers"
                return
        raise AssertionError("no _reserved_channel_numbers call in _action_apply")

    def test_the_floor_is_resolved_after_existing_channels_are_rekeyed(self):
        src, _fn = _apply_fn()
        rekey = src.index("marker_renames = _rekey_legacy_virtuals(existing_virtuals, games)")
        resolve = src.index("virtual_base = _resolve_kickoff_base(")
        assert rekey < resolve
