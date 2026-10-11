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
    resolve the floor, assign with the record when it was written under that
    floor, then save the floor and the record of what was numbered (reading
    and writing the state file as apply does; every game counts as written)."""
    remembered, record = plugin._load_numbering_state()
    base = plugin._resolve_kickoff_base(
        settings, highest_other, remembered, _published(plugin, games, existing), tz,
    )
    got = plugin._assign_channel_numbers(
        games, base, tz, existing_numbers=existing,
        numbered_for=plugin._numbered_for_floor(record, base),
    )
    offsets = dict(plugin._kickoff_offsets(games, tz))
    plugin._save_numbering_state(base, {m: (offsets[m], n, base) for m, n in got.items()})
    return base, got


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
        assert plugin._load_numbering_state() == (None, {})
        base, again = _apply_numbers(plugin, games, {}, 6838, old)
        assert base == 6892
        assert again == old
        assert plugin._load_numbering_state()[0] == 6892

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
        plugin._save_numbering_state(
            6892, {"ranked_matchups:EPL:fd_8": (6672015, 6678908, 6892)},
        )
        assert plugin._load_numbering_state() == (
            6892, {"ranked_matchups:EPL:fd_8": (6672015, 6678908, 6892)},
        )
        with open(plugin.NUMBERING_STATE_PATH, encoding="utf-8") as f:
            assert json.load(f) == {
                "kickoff_base": 6892,
                "numbered_for": {"ranked_matchups:EPL:fd_8": [6672015, 6678908, 6892]},
            }

    @pytest.mark.parametrize("content", [
        "not json", "[]", '{"kickoff_base": "6892"}', '{"kickoff_base": true}',
        '{"kickoff_base": 0}', '{"kickoff_base": -3}', '{"kickoff_base": 6892.5}', "{}",
    ])
    def test_a_bad_file_reads_as_nothing(self, plugin, content):
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        assert plugin._load_numbering_state() == (None, {})

    def test_missing_file(self, plugin):
        assert plugin._load_numbering_state() == (None, {})

    def test_an_unwritable_path_is_logged_not_raised(self, plugin, tmp_path):
        plugin._save_numbering_state(6892, {}, path=str(tmp_path / "no" / "such" / "dir.json"))

    def test_a_failed_save_removes_the_old_file(self, plugin, monkeypatch):
        """Second opinion (#278 round 1): the file says floor 11, an apply
        under explicit base 100 commits and its save fails. Left in place, the
        old file would be read back by the next auto apply as the current
        floor, renumbering every game from 100 onto 11."""
        plugin._save_numbering_state(11, {})

        def fail(path, data, **kw):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(plugin, "write_json_atomic", fail)
        plugin._save_numbering_state(100, {})
        assert not os.path.exists(plugin.NUMBERING_STATE_PATH)
        assert plugin._load_numbering_state() == (None, {})

    def test_a_failed_save_under_the_same_floor_keeps_the_old_file(self, plugin, monkeypatch):
        """Second opinion (#278 round 2): deleting a file whose floor is still
        right threw away the record, and the next apply's chain rule moved
        fd_8 from 6672116 to 6672115 once fd_61 left. The old record is only
        older, so it is kept, and the next apply still holds fd_8."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116}
        old = {m: (off, n, 100) for m, (off, n) in _record(plugin, games, live).items()}
        plugin._save_numbering_state(100, old)

        def fail(path, data, **kw):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(plugin, "write_json_atomic", fail)
        plugin._save_numbering_state(100, {})
        base, record = plugin._load_numbering_state()
        assert (base, record) == (100, old)
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live,
            numbered_for=plugin._numbered_for_floor(record, 100),
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672116}

    def test_a_failed_removal_is_logged_not_raised(self, plugin, monkeypatch, tmp_path):
        def fail(path, data, **kw):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(plugin, "write_json_atomic", fail)
        plugin._save_numbering_state(100, {}, path=str(tmp_path))  # a directory

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
        assert _calls(body, "_load_numbering_state")
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
        saves = _calls(fn, "_save_numbering_state")
        assert len(saves) == 1
        guards = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.If) and _calls(n, "_save_numbering_state")
        ]
        assert guards and "not dry_run" in ast.unparse(guards[-1].test)
        assert "NUMBERING_MODE_KICKOFF" in ast.unparse(guards[-1].test)
        for w in ast.walk(fn):
            if isinstance(w, ast.With):
                assert not _calls(w, "_save_numbering_state"), "must run after the transaction"
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
        resolve = src.index("virtual_base, numbered_for = _kickoff_floor(reserved_numbers)")
        assert rekey < resolve


class _FakeQS:
    """Just enough queryset for _published_numbers: exclude/filter on one
    numeric field with __isnull / __gte / __lt / __lte, then values_list."""

    def __init__(self, values):
        self.values = list(values)

    def exclude(self, *args, **kw):
        if any(k.endswith("__isnull") for k in kw):
            return _FakeQS(v for v in self.values if v is not None)
        return self

    def filter(self, **kw):
        out = self.values
        for k, bound in kw.items():
            op = k.rsplit("__", 1)[1]
            test = {"gte": lambda v: v >= bound, "lt": lambda v: v < bound,
                    "lte": lambda v: v <= bound}[op]
            out = [v for v in out if test(v)]
        return _FakeQS(out)

    def values_list(self, *_a, **_k):
        return list(self.values)


class TestPublishedNumbers:
    """The reservation query, driven with a fake queryset (no Django; the
    effective-number annotation is unavailable here, so the raw column is read,
    which is what this tests: the bounds and the floor)."""

    def test_a_fraction_at_the_top_of_the_band_reserves_its_floor(self, plugin):
        """Review finding: `<= band_end` dropped 400.5, which Xtream Codes
        publishes as 400, so a game could be handed 400 beside it."""
        got = plugin._published_numbers(_FakeQS([399.0, 400.5, 401.0, None]), 400, 400)
        assert got == {400}

    def test_no_upper_bound(self, plugin):
        got = plugin._published_numbers(_FakeQS([5.0, 6892.0, 7000000.25]), 6892)
        assert got == {6892, 7000000}


class TestKeptRecordingChannelsHoldTheirNumbers:
    """Review finding: a stale channel kept for an active recording went back
    to its pre-park number only if no new game had been handed it; otherwise it
    stayed at a parking number mid-recording."""

    def test_only_kept_channels_are_held(self, plugin, monkeypatch):
        import sys
        import types
        from types import SimpleNamespace as NS

        now = datetime.now(timezone.utc)
        recording = NS(id=11, channel_id=1, start_time=now - timedelta(minutes=30),
                       end_time=now + timedelta(hours=1), custom_properties={"status": "recording"})
        done = NS(id=12, channel_id=2, start_time=now - timedelta(hours=3),
                  end_time=now - timedelta(hours=1), custom_properties={"status": "completed"})

        class _Objects:
            def filter(self, channel_id__in):
                return [r for r in (recording, done) if r.channel_id in channel_id__in]

        models = types.ModuleType("apps.channels.models")
        models.Recording = NS(objects=_Objects())
        for name in ("apps", "apps.channels"):
            monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
        monkeypatch.setitem(sys.modules, "apps.channels.models", models)

        stale = [NS(id=1, channel_number=6203700.0), NS(id=2, channel_number=6203701.0),
                 NS(id=3, channel_number=6203702.0), NS(id=4, channel_number=None)]
        assert plugin._numbers_held_for_recordings(stale, True) == {6203700}
        # Archive disabled: a channel with only finished recordings is kept too.
        assert plugin._numbers_held_for_recordings(stale, False) == {6203700, 6203701}

    def test_apply_adds_them_to_the_reservation_in_both_modes(self):
        src, _fn = _apply_fn()
        held = src.index("held_for_recordings = _numbers_held_for_recordings(")
        kickoff = src.index("_reserved_channel_numbers(target_group, lowest_kickoff) | held_for_recordings")
        resolve = src.index("virtual_base, numbered_for = _kickoff_floor(reserved_numbers)")
        compact = src.index(") | held_for_recordings", resolve)
        assert held < kickoff < resolve < compact
        assert compact < src.index("chnum_by_marker = _assign_channel_numbers(")

    def test_a_held_number_is_not_handed_to_a_new_game(self, plugin):
        games = _slate()
        plain = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        m = plugin._build_marker_key(games[0])
        got = plugin._assign_channel_numbers(
            games, 6892, timezone.utc, reserved_numbers={plain[m]},
        )
        assert plain[m] not in got.values()


class TestArchivePlacementIsGlobal:
    def test_the_archive_avoids_numbers_published_anywhere(self):
        """Review finding: the archive picked the first free number in ITS
        group, so a compact game (or a user channel) at 9999 elsewhere shared
        its guide key."""
        src = open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8").read()
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_ensure_archive_channel")
        body = ast.get_source_segment(src, fn)
        assert "_published_numbers(" in body
        assert "Channel.objects.exclude(tvg_id=ARCHIVE_TVG_ID), _ARCHIVE_CHANNEL_NUMBER" in body


class TestReservedNumbersCoverOurOwnGroup:
    """Third review round: the per-group unique constraint sees RAW numbers."""

    def _run(self, plugin, monkeypatch):
        import sys
        import types
        from unittest.mock import MagicMock

        models = types.ModuleType("apps.channels.models")
        models.Channel = MagicMock(name="Channel")
        for name in ("apps", "apps.channels"):
            monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
        monkeypatch.setitem(sys.modules, "apps.channels.models", models)

        calls = []
        results = iter([{500}, {400}])

        def fake(qs, lo, hi=None, effective=True):
            calls.append(effective)
            return next(results)

        monkeypatch.setattr(plugin, "_published_numbers", fake)
        return plugin._reserved_channel_numbers(object(), 400, 401), calls

    def test_raw_numbers_of_strangers_in_our_group_are_reserved(self, plugin, monkeypatch):
        got, calls = self._run(plugin, monkeypatch)
        assert got == {500, 400}
        assert calls == [True, False], "the in-group query must read RAW numbers"

    def test_effective_false_reads_the_raw_column(self, plugin):
        got = plugin._published_numbers(_FakeQS([400.0, 401.0]), 400, 400, effective=False)
        assert got == {400}

    def test_a_fractional_raw_number_does_not_block_its_floor(self, plugin):
        """Round 4: raw 400.5 is not raw 400 to the unique constraint."""
        got = plugin._published_numbers(_FakeQS([400.5, 401.0]), 400, 401, effective=False)
        assert got == {401}


def _epl_row(gid, start):
    GameRow = _gamerow_cls()
    return _cache_row(GameRow(
        sport_prefix="EPL", sport_label="Premier League",
        home=f"Home{gid}", away=f"Away{gid}", rank_home=None, rank_away=None,
        start_time=start, extra={"fd_id": gid},
    ))


def _saturday_slate(first_id=500000):
    """Ten Premier League games kicking off in the same minute (a Saturday
    3pm), then three a minute apart. Same-minute games collide on the 16 hash
    slots, and some collisions nudge a game past the end of its minute's block."""
    t0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
    return [_epl_row(first_id + i, t0) for i in range(10)] + [
        _epl_row(first_id + 10 + i, t0 + timedelta(minutes=i + 1)) for i in range(3)
    ]


def _newcomer_trial(plugin, trial, floor=None):
    """One slate of the #219 / #267 generator: 3 to 14 Premier League games,
    mostly in one minute, published from floor 6892 with 0 to 2 of them held
    back as newcomers and 0 to 3 numbers reserved (channels kept for a
    recording). Returns (published numbers, the next apply's numbers, the floor
    it used). `floor=None` runs the inference, as the upgrade apply does; an
    int pins the floor, as a remembered one does."""
    import random
    tz = timezone.utc
    t0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
    rng = random.Random(trial)
    n = rng.randint(3, 14)
    games = [
        _epl_row(800000 + trial * 20 + i, t0 + timedelta(minutes=rng.choice([0, 0, 0, 1])))
        for i in range(n)
    ]
    plain = plugin._assign_channel_numbers(games, 6892, tz)
    k = rng.randint(0, 2)
    newcomers, rest = games[:k], games[k:]
    held = set(rng.sample(sorted(plain.values()), rng.randint(0, 3)))
    old = plugin._assign_channel_numbers(rest, 6892, tz, reserved_numbers=held)
    unpublished = [
        (plugin.parse_iso_utc(g["start_time_utc"]), plugin._build_marker_key(g))
        for g in newcomers
    ]
    base = floor if floor is not None else plugin._resolve_kickoff_base(
        {}, 6838, None, _published(plugin, rest, old), tz,
        unpublished=unpublished, reserved=held,
    )
    again = plugin._assign_channel_numbers(
        games, base, tz, existing_numbers=old, reserved_numbers=held,
    )
    return old, again, base


class TestInferenceRunsTheRealPlacement:
    """Review finding (verifier, #219): the inference scored candidates by how
    many channels sat inside their block. A same-minute collision nudges a
    channel past its block's end, and the true floor reproduces that nudge every
    apply, so the channel does not move; the proxy counted it as lost and let a
    floor one or two higher win, which moved the channel on its block's first
    slot. Measured: 8 of 3000 random slates moved one channel on the upgrade
    apply; the Saturday slate below is the first realistic one tried."""

    def test_witness_a_saturday_slate_upgrades_without_moving(self, plugin):
        games = _saturday_slate()
        old = plugin._assign_channel_numbers(games, 6892, timezone.utc)
        # Precondition: at least one channel was nudged out of its block, the
        # shape that fooled the in-block proxy.
        out_of_block = 0
        for g in games:
            m = plugin._build_marker_key(g)
            off = plugin.stable_channel_number(0, plugin.parse_iso_utc(g["start_time_utc"]), m, timezone.utc)
            blk = 6892 + off - off % 16
            out_of_block += not (blk <= old[m] < blk + 16)
        assert out_of_block >= 1
        base, again = _apply_numbers(plugin, games, {}, 6838, old)
        assert again == old
        assert base == 6892

    def test_witness_random_slates_upgrade_without_moving(self, plugin):
        import random
        tz = timezone.utc
        t0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
        moved_slates = []
        for trial in range(400):
            rng = random.Random(trial)
            games = [
                _epl_row(700000 + trial * 100 + i, t0 + timedelta(
                    minutes=rng.choice([0, 0, 0, 30, 60, 180, 1440]) + 15 * rng.randint(0, 2)))
                for i in range(rng.randint(2, 30))
            ]
            floor = rng.randint(100, 20000)
            old = plugin._assign_channel_numbers(games, floor, tz)
            published = _published(plugin, games, old)
            base = plugin._resolve_kickoff_base({}, floor - 1 + rng.randint(-200, 200), None, published, tz)
            again = plugin._assign_channel_numbers(games, base, tz, existing_numbers=old)
            if again != old:
                moved_slates.append(trial)
        assert moved_slates == []

    @pytest.mark.parametrize("trial", [38, 84, 105, 135])
    def test_witness_new_games_and_held_numbers_are_part_of_the_placement(self, plugin, trial):
        """The upgrade apply also has games with no channel yet, and numbers it
        holds back for channels kept for a recording. Both change where a nudged
        channel lands, so a floor judged without them moved one or two channels
        on these slates (found by search; 38 turns on the held numbers, 84 on
        the newcomer) while the floor judged with them moves none. Only the
        PUBLISHED numbers are pinned: the newcomer has none yet, and on 84 the
        floor that keeps every published channel puts it elsewhere than the
        seeding floor would, which is the point."""
        old, again, _base = _newcomer_trial(plugin, trial)
        assert {m: again[m] for m in old} == old

    def test_a_strangers_number_is_part_of_the_placement(self, plugin):
        """Second-opinion repro: a channel we do not own sits between two of our
        published numbers. Judged without it, the inference picked floor 100 and
        the placement then nudged fd_8 off 6672116; judged with it, nothing moves."""
        tz = timezone.utc
        t0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
        rows = {gid: _epl_row(gid, t0) for gid in (11, 6, 8, 14, 29, 19)}
        old = {plugin._build_marker_key(rows[gid]): n
               for gid, n in ((11, 6672114), (6, 6672109), (8, 6672116))}
        published = [(n, t0, m) for m, n in old.items()]
        unpublished = [(t0, plugin._build_marker_key(rows[gid])) for gid in (14, 29, 19)]
        stranger = {6672115}
        base = plugin._resolve_kickoff_base(
            {}, 6672115, None, published, tz, unpublished=unpublished, reserved=stranger,
        )
        again = plugin._assign_channel_numbers(
            list(rows.values()), base, tz, existing_numbers=old, reserved_numbers=stranger,
        )
        assert {m: again[m] for m in old} == old

    def test_a_channel_nudged_past_its_block_still_names_its_floor(self, plugin):
        """Second-opinion repro, round two: a lone published channel nudged off
        a stranger's number past the end of its block named only floors above
        the true one, so the hint could not pick it. Nothing published moved
        either way; the true floor keeps later games where they would have been."""
        tz = timezone.utc
        t0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
        game = _epl_row(8, t0)
        m = plugin._build_marker_key(game)
        natural = plugin.stable_channel_number(6892, t0, m, tz)
        stranger = {natural}
        old = plugin._assign_channel_numbers([game], 6892, tz, reserved_numbers=stranger)
        off = natural - 6892
        block = 6892 + off - off % 16
        assert not (block <= old[m] < block + 16), "precondition: nudged past its block"
        base = plugin._resolve_kickoff_base(
            {}, 6891, None, [(old[m], t0, m)], tz, reserved=stranger,
        )
        assert base == 6892

    def test_apply_hands_the_inference_its_slate_and_held_numbers(self):
        _src, fn = _apply_fn()
        (call,) = _calls(fn, "_resolve_kickoff_base")
        kw = {k.arg: ast.unparse(k.value) for k in call.keywords}
        assert kw.get("unpublished") == "unpublished"
        assert kw.get("reserved") == "reserved"
        # ...and the closure around it is always handed the full reservation,
        # in the first placement and in the #286 second one.
        floors = _calls(fn, "_kickoff_floor")
        assert len(floors) == 2
        assert all([ast.unparse(a) for a in c.args] == ["reserved_numbers"] for c in floors)

    def test_one_placement_implementation(self):
        """_assign_channel_numbers and the inference must share the placement:
        a second copy would let the floor be judged by rules apply does not run."""
        tree = ast.parse(open(os.path.join(REPO_ROOT, "plugin.py")).read())
        fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        for name in ("_assign_channel_numbers", "_infer_kickoff_base"):
            assert _calls(fns[name], "_place_kickoff_numbers"), name
            assert not _calls(fns[name], "_first_free_number"), name


# Hash slots of the Premier League rows below (marker ranked_matchups:EPL:fd_N)
# at the 14:00 kickoff, from floor 100, whose block is 6672100-6672115 (the
# next minute's block starts at 6672116): fd_29, fd_61, fd_11 -> slot 14
# (6672114); fd_8, fd_16, fd_105 -> slot 15 (6672115); fd_26 -> 0, fd_2 -> 1,
# fd_74 -> 2 (6672116-6672118 at 14:01).
_T0 = datetime(2026, 10, 17, 14, 0, tzinfo=timezone.utc)
_T1 = _T0 + timedelta(minutes=1)


def _rows(plugin, spec):
    """{gid: kickoff} -> (games, {gid: marker})."""
    games = [_epl_row(gid, start) for gid, start in spec.items()]
    return games, {gid: plugin._build_marker_key(g) for gid, g in zip(spec, games)}


def _numbers(got, markers):
    return {gid: got[m] for gid, m in markers.items()}


class TestANudgedChannelPastItsBlockIsHeld:
    """#267: a same-minute collision near the last slot nudges a game past the
    end of its minute's 16-number block. The hold only kept numbers inside the
    block, so on the next apply that channel was placed as a fresh game, and a
    NEW game in the same minute placed before it took its number: a published
    channel moved, and the guide binds by the bare number (#117). It is now held
    when its number is exactly where an upward nudge from its computed number
    lands over the numbers that stay. A game rescheduled one minute EARLIER
    must still be released. These run with NO record of what each channel was
    numbered for (#278): the first apply after upgrading, or a lost file, where
    the chain rule is all apply has."""

    def test_the_fixture_slots_are_what_the_literals_assume(self, plugin):
        tz = timezone.utc
        for gid, start, slot_number in (
            (29, _T0, 6672114), (61, _T0, 6672114), (11, _T0, 6672114),
            (8, _T0, 6672115), (16, _T0, 6672115), (105, _T0, 6672115),
            (26, _T1, 6672116), (2, _T1, 6672117), (74, _T1, 6672118),
        ):
            m = plugin._build_marker_key(_epl_row(gid, start))
            assert plugin.stable_channel_number(100, start, m, tz) == slot_number, gid

    def test_witness_a_newcomer_with_a_lower_number_does_not_take_it(self, plugin):
        """The issue's shape: fd_8 was nudged off 6672115 (fd_61, itself nudged
        off fd_29) to 6672116, past its block. Newcomer fd_11 computes 6672114,
        so it was placed first and nudged onto 6672116, moving fd_8 to 6672117."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0, 11: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116}
        got = plugin._assign_channel_numbers(games, 100, timezone.utc, existing_numbers=live)
        assert _numbers(got, mk) == {29: 6672114, 61: 6672115, 8: 6672116, 11: 6672117}

    def test_witness_a_chain_through_another_held_channel(self, plugin):
        """fd_16 sits two past its computed 6672115, over fd_61 and fd_8, and
        fd_8 is itself held past its block. The lower one has to be decided
        first: decided the other way round, fd_16 saw a gap at 6672116 and was
        released to 6672118 behind the newcomer."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0, 16: _T0, 11: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116, mk[16]: 6672117}
        got = plugin._assign_channel_numbers(games, 100, timezone.utc, existing_numbers=live)
        assert _numbers(got, mk) == {
            29: 6672114, 61: 6672115, 8: 6672116, 16: 6672117, 11: 6672118,
        }

    def test_witness_a_reserved_number_explains_the_nudge(self, plugin):
        """fd_8 was nudged off a stranger's 6672115. Newcomer fd_105 computes
        the same number and its marker sorts first, so it took 6672116."""
        games, mk = _rows(plugin, {8: _T0, 105: _T0})
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers={mk[8]: 6672116},
            reserved_numbers={6672115},
        )
        assert _numbers(got, mk) == {8: 6672116, 105: 6672117}

    @pytest.mark.parametrize("trial", [686, 782, 1360, 1455, 1955, 2075, 2197, 2887])
    def test_witness_the_issues_slates_keep_every_published_number(self, plugin, trial):
        """The 8 slates the issue names, where no floor kept every published
        channel: a newcomer always displaced a nudged one. Pinned at the true
        floor and at the floor the upgrade apply infers."""
        for floor in (6892, None):
            old, again, _base = _newcomer_trial(plugin, trial, floor=floor)
            assert {m: again[m] for m in old} == old, floor

    def test_witness_no_slate_of_the_generator_moves_a_published_number(self, plugin):
        """All 3000 slates of the issue's search (about 3 s). On the base this
        moved a published channel on 11 at the inferred floor and on 58 at the
        true floor."""
        moved = []
        for trial in range(3000):
            for floor in (6892, None):
                old, again, _base = _newcomer_trial(plugin, trial, floor=floor)
                if {m: again[m] for m in old} != old:
                    moved.append((trial, floor))
        assert moved == []

    def test_a_game_rescheduled_one_minute_earlier_is_released(self, plugin):
        """fd_8 was numbered for 14:01 and sat on 6672119 (slot 3 of that
        minute, below its hash slot 15, as a #217 re-key leaves it). It now
        kicks off at 14:00 and computes 6672115, four below its old number: a
        rule holding anything within 16 above would keep it in 14:01's block.
        Nothing between is taken, so it is released to its new number."""
        games, mk = _rows(plugin, {8: _T0, 26: _T1})
        live = {mk[8]: 6672119, mk[26]: 6672116}
        got = plugin._assign_channel_numbers(games, 100, timezone.utc, existing_numbers=live)
        assert _numbers(got, mk) == {8: 6672115, 26: 6672116}

    def test_witness_a_held_rescheduled_game_keeps_its_number_from_a_newcomer(self, plugin):
        """fd_8 rescheduled from 14:01 to 14:00 as above, but every number from
        its new 6672115 up to its old 6672119 is taken (fd_16 on its slot,
        14:01's first three on theirs), so a nudge from its new number walks to
        6672119 anyway and the hold puts it exactly where a release would. The
        one difference is fd_105, new at 14:00 on the same slot: released, fd_8
        lost 6672119 to it and moved to 6672120 (both in 14:01's block, so
        second-opinion round 1's "held in the wrong minute" is true of the
        release too)."""
        spec = {16: _T0, 26: _T1, 2: _T1, 74: _T1, 8: _T0, 105: _T0}
        games, mk = _rows(plugin, spec)
        live = {mk[16]: 6672115, mk[26]: 6672116, mk[2]: 6672117, mk[74]: 6672118,
                mk[8]: 6672119}
        got = plugin._assign_channel_numbers(games, 100, timezone.utc, existing_numbers=live)
        assert _numbers(got, mk) == {16: 6672115, 26: 6672116, 2: 6672117, 74: 6672118,
                                     8: 6672119, 105: 6672120}

    def test_a_gap_below_it_still_releases_it(self, plugin):
        """fd_61 left the slate, so 6672115 is free and fd_8's nudge is no longer
        explained: with no record it is placed afresh, as before #267 (without
        one it cannot be told from a game rescheduled earlier, see the DO NOT in
        _place_kickoff_numbers). TestTheRecordHoldsANudgedChannel has the same
        slate with the record."""
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[8]: 6672116}
        got = plugin._assign_channel_numbers(games, 100, timezone.utc, existing_numbers=live)
        assert _numbers(got, mk) == {29: 6672114, 8: 6672115}

    def test_a_held_number_that_is_now_reserved_is_released(self, plugin):
        """The reservation still wins over this hold, as over the in-block one."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116}
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, reserved_numbers={6672116},
        )
        assert _numbers(got, mk) == {29: 6672114, 61: 6672115, 8: 6672117}

    def test_a_reserved_held_number_does_not_push_a_held_neighbour(self, plugin):
        """fd_29's 6672114 is now reserved, so it is released and placed from its
        computed number AFTER the held channels. Treated as held instead, it was
        placed first, walked off its reserved number onto fd_8's 6672115 and
        pushed fd_8 out."""
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[8]: 6672115}
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, reserved_numbers={6672114},
        )
        assert _numbers(got, mk) == {29: 6672116, 8: 6672115}

    def test_witness_a_nudge_of_a_whole_block_still_names_its_floor(self, plugin):
        """Second-opinion round 1: sixteen reserved numbers pushed fd_8 from
        6672115 to 6672131. The hold keeps it there, but the inference only
        tried nudges of 1..15, never named floor 100, and settled on 101, which
        would have been remembered and put every later game one higher."""
        tz = timezone.utc
        games, mk = _rows(plugin, {8: _T0, 29: _T0})
        reserved = set(range(6672115, 6672131))
        base = plugin._resolve_kickoff_base(
            {}, 99, None, [(6672131, _T0, mk[8])], tz,
            unpublished=[(_T0, mk[29])], reserved=reserved,
        )
        assert base == 100
        got = plugin._assign_channel_numbers(
            games, base, tz, existing_numbers={mk[8]: 6672131}, reserved_numbers=reserved,
        )
        assert _numbers(got, mk) == {8: 6672131, 29: 6672114}

    def test_witness_a_crowded_minute_keeps_every_number_on_the_upgrade_apply(self, plugin):
        """Twenty-one games in one minute (a Saturday 3pm with the Championship
        on top of the Premier League), all on the block's last four slots (ids
        picked for that), so the last one sits 16 or more past its computed
        number. A newcomer joins on the upgrade apply, with no floor
        remembered; every published channel keeps its number."""
        tz = timezone.utc
        crowd = (4, 25, 50, 66, 75, 88, 1, 82, 84, 87, 122, 123,  # slots 12, 13
                 11, 29, 61, 90, 99, 107, 8, 16, 23)              # slots 14, 15
        games, mk = _rows(plugin, {gid: _T0 for gid in crowd})
        old = plugin._assign_channel_numbers(games, 100, tz)
        nudges = [
            old[m] - plugin.stable_channel_number(100, _T0, m, tz) for m in old
        ]
        assert max(nudges) >= 16, "precondition: a nudge of a whole block"
        newcomer, nmk = _rows(plugin, {105: _T0})
        base = plugin._resolve_kickoff_base(
            {}, 99, None, [(n, _T0, m) for m, n in old.items()], tz,
            unpublished=[(_T0, nmk[105])],
        )
        again = plugin._assign_channel_numbers(games + newcomer, base, tz, existing_numbers=old)
        assert {m: again[m] for m in old} == old
        assert base == 100

    def test_a_long_reserved_run_does_not_stall_the_inference(self, plugin, monkeypatch):
        """Second opinion, round 2: 20,000 consecutive reserved numbers under a
        published channel made one candidate floor per number, each scored with
        two placements that scan the run (208 s). The walk stops after
        _INFER_RESERVED_RUN of them, so the placements run a bounded number of
        times."""
        calls = []
        real = plugin._place_kickoff_numbers

        def counted(*a, **kw):
            calls.append(1)
            return real(*a, **kw)

        monkeypatch.setattr(plugin, "_place_kickoff_numbers", counted)
        m = plugin._build_marker_key(_epl_row(8, _T0))
        plugin._infer_kickoff_base(
            [(6692115, _T0, m)], timezone.utc, hint=100,
            reserved=set(range(6672115, 6692115)),
        )
        assert plugin._INFER_RESERVED_RUN == 64
        assert 0 < len(calls) <= 2 * (16 + 64 + 1)



def _record(plugin, games, live, tz=timezone.utc):
    """What apply records after publishing `live`: marker -> (offset, number)."""
    offsets = dict(plugin._kickoff_offsets(games, tz))
    return {m: (offsets[m], n) for m, n in live.items()}


class TestTheRecordHoldsANudgedChannel:
    """#278: with the chain rule alone, a channel nudged past its block is
    released as soon as a game in the chain below it leaves the slate (same-
    minute games finish at different times), so it renumbers while its own game
    is on. Apply now records the (kickoff offset, number) each channel was
    numbered for, and a channel whose game still has that offset keeps its
    number while it is free. Offsets at floor 100: fd_29 / fd_61 / fd_11
    6672014, fd_8 / fd_16 / fd_105 6672015, the 14:01 games 6672016 up."""

    def test_witness_a_gap_below_it_no_longer_releases_it(self, plugin):
        """The slate test_a_gap_below_it_still_releases_it pins, with the
        record of the apply that published fd_61 too."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116}
        record = _record(plugin, games, live)
        assert record[mk[8]] == (6672015, 6672116)
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672116}

    def test_witness_the_freed_number_goes_to_a_newcomer(self, plugin):
        """fd_61 leaves as fd_850 (same slot as fd_8, marker sorting after it)
        arrives: fd_850 takes the 6672115 fd_61 freed. Released, fd_8 took it
        and fd_850 got 6672116, so two numbers changed hands at once."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116}
        record = _record(plugin, games, live)
        games, mk = _rows(plugin, {29: _T0, 8: _T0, 850: _T0})
        assert plugin.stable_channel_number(100, _T0, mk[850], timezone.utc) == 6672115
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672116, 850: 6672115}

    def test_witness_two_recorded_channels_above_one_gap(self, plugin):
        """fd_8 and fd_16 were both nudged past the block over fd_61, which
        leaves. Both keep their numbers; released, they slid down to 6672115
        and 6672116."""
        games, mk = _rows(plugin, {29: _T0, 61: _T0, 8: _T0, 16: _T0})
        live = {mk[29]: 6672114, mk[61]: 6672115, mk[8]: 6672116, mk[16]: 6672117}
        record = _record(plugin, games, live)
        games, mk = _rows(plugin, {29: _T0, 8: _T0, 16: _T0})
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672116, 16: 6672117}

    def test_witness_no_slate_of_the_generator_moves_a_published_number_when_one_leaves(self, plugin):
        """The issue's measurement: the #267 generator published at floor 6892,
        then one random published game leaves. Without the record a published
        channel moved in 91 of these 3000 slates (the PR #274 verifier's own
        variant counted 56); with it, in none."""
        import random
        tz = timezone.utc
        moved = []
        for trial in range(3000):
            rng = random.Random(trial)
            n = rng.randint(3, 14)
            games = [
                _epl_row(800000 + trial * 20 + i, _T0 + timedelta(minutes=rng.choice([0, 0, 0, 1])))
                for i in range(n)
            ]
            plain = plugin._assign_channel_numbers(games, 6892, tz)
            rest = games[rng.randint(0, 2):]
            held = set(rng.sample(sorted(plain.values()), rng.randint(0, 3)))
            old = plugin._assign_channel_numbers(rest, 6892, tz, reserved_numbers=held)
            record = _record(plugin, rest, old)
            drop = rng.randrange(len(rest))
            gone = plugin._build_marker_key(rest[drop])
            live = {m: v for m, v in old.items() if m != gone}
            again = plugin._assign_channel_numbers(
                [g for i, g in enumerate(rest) if i != drop], 6892, tz,
                existing_numbers=live, reserved_numbers=held, numbered_for=record,
            )
            if any(again[m] != v for m, v in live.items()):
                moved.append(trial)
        assert moved == []

    def test_a_rescheduled_game_is_still_released(self, plugin):
        """The case the DO NOT exists for, now WITH a record: fd_8 was numbered
        for 14:01 (offset 6672031, slot 15 of that minute) and sits on 6672119
        after a #217 re-key left it below its hash slot. It now kicks off at
        14:00 (offset 6672015). The record's offset no longer matches, so it is
        not a nudge: released to its new number, as without a record."""
        games, mk = _rows(plugin, {8: _T0, 26: _T1})
        live = {mk[8]: 6672119, mk[26]: 6672116}
        record = {mk[8]: (6672031, 6672119), mk[26]: (6672016, 6672116)}
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {8: 6672115, 26: 6672116}

    def test_a_reschedule_into_the_block_holding_its_number_keeps_it(self, plugin):
        """Second opinion (#278 round 4): fd_8 was nudged to 6672116 at 14:00,
        then moves to 14:01, whose block (6672116-6672131) holds that number.
        The record's minute no longer matches, but the #217 in-block hold keeps
        a number already inside the game's own minute, record or not: the
        channel sorts correctly there and moving it would rebind the guide."""
        games, mk = _rows(plugin, {8: _T1})
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers={mk[8]: 6672116},
            numbered_for={mk[8]: (6672015, 6672116)},
        )
        assert _numbers(got, mk) == {8: 6672116}

    def test_a_record_for_another_number_is_not_trusted(self, plugin):
        """The record says fd_8 was given 6672116 but its channel carries
        6672117 (moved by hand, or by an older release that rewrote the file's
        floor and left a stale record). Not what was recorded, so the chain rule
        decides: a gap at 6672115, released."""
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[8]: 6672117}
        record = {mk[29]: (6672014, 6672114), mk[8]: (6672015, 6672116)}
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672115}

    def test_a_recorded_number_now_reserved_is_released(self, plugin):
        """The reservation wins over the recorded hold too: another channel now
        publishes 6672116, so fd_8 is placed afresh around it."""
        games, mk = _rows(plugin, {29: _T0, 8: _T0})
        live = {mk[29]: 6672114, mk[8]: 6672116}
        record = _record(plugin, games, live)
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, reserved_numbers={6672116},
            numbered_for=record,
        )
        assert _numbers(got, mk) == {29: 6672114, 8: 6672115}

    def test_a_recorded_number_a_kept_channel_holds_is_released(self, plugin):
        """Nothing in the database stops two channels sharing a number
        (_PER_GROUP_NUMBER_NOTE). fd_26 (14:01, on its own slot 6672116) and
        fd_8 (recorded on 6672116) both carry it: the in-block hold is decided
        first, so fd_8 does not keep it and is placed afresh on 6672115."""
        games, mk = _rows(plugin, {26: _T1, 8: _T0})
        live = {mk[26]: 6672116, mk[8]: 6672116}
        record = {mk[8]: (6672015, 6672116)}
        got = plugin._assign_channel_numbers(
            games, 100, timezone.utc, existing_numbers=live, numbered_for=record,
        )
        assert _numbers(got, mk) == {26: 6672116, 8: 6672115}

    def test_the_inference_runs_the_placement_without_a_record(self, plugin):
        """The inference only runs with no floor remembered, when the loader
        hands back no record either, so it judges each floor by the record-free
        placement, which is then also what apply runs."""
        tree = ast.parse(open(os.path.join(REPO_ROOT, "plugin.py")).read())
        fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        calls = _calls(fns["_infer_kickoff_base"], "_place_kickoff_numbers")
        assert calls and all(len(c.args) == 4 and not c.keywords for c in calls)
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write('{"numbered_for": {"m": [6672015, 6678908, 6892]}}')
        assert plugin._load_numbering_state() == (None, {})

    def test_apply_passes_the_record_for_its_floor(self):
        _src, fn = _apply_fn()
        (filt,) = _calls(fn, "_numbered_for_floor")
        assert [ast.unparse(a) for a in filt.args] == [
            "remembered_record", "base", "marker_renames",
        ]
        assigns = _calls(fn, "_assign_channel_numbers")
        assert len(assigns) == 2  # the placement, and its #286 second pass
        for assign in assigns:
            kw = {k.arg: ast.unparse(k.value) for k in assign.keywords}
            assert kw.get("numbered_for") == "numbered_for"
            assert kw.get("existing_numbers") == "existing_numbers"

    def test_only_entries_numbered_from_this_floor_are_trusted(self, plugin):
        """A kept recording channel keeps the number floor 6892 gave it after
        the user moves the floor to 7000; its entry stays in the file, but is
        trusted only when the floor is 6892 again."""
        record = {"a": (6672015, 6678908, 6892), "b": (6672016, 6679016, 7000)}
        assert plugin._numbered_for_floor(record, 6892) == {"a": (6672015, 6678908)}
        assert plugin._numbered_for_floor(record, 7000) == {"b": (6672016, 6679016)}
        assert plugin._numbered_for_floor(record, 100) == {}

    def test_a_re_keyed_channel_keeps_its_entry(self, plugin):
        """Second opinion (#278 round 1): a channel published under its legacy
        marker (teams + kickoff hash, before #217) and nudged to 6672116 over a
        number reserved then is re-keyed to its source-id marker on the apply
        where that reservation is gone. Its entry follows it through the real
        re-key, and is matched on the kickoff MINUTE: the two markers hash to
        different slots, so the offsets differ although the kickoff did not."""
        from types import SimpleNamespace
        tz = timezone.utc
        game = _cache_row(_gamerow_cls()(
            sport_prefix="EPL", sport_label="Premier League", home="Home23",
            away="Away23", rank_home=None, rank_away=None, start_time=_T0,
            extra={"espn_event_id": 17},
        ))
        new, legacy = plugin._build_marker_key(game), plugin._legacy_marker_key(game)
        assert new == "ranked_matchups:EPL:espn_17"
        channel = SimpleNamespace(tvg_id=legacy, channel_number=6672116)
        existing = {legacy: channel}
        renames = plugin._rekey_legacy_virtuals(existing, [game])
        assert renames == [(channel, legacy, new)]
        legacy_offset = plugin.stable_channel_number(0, _T0, legacy, tz)
        assert legacy_offset != plugin.stable_channel_number(0, _T0, new, tz)
        record = plugin._numbered_for_floor({legacy: (legacy_offset, 6672116, 100)}, 100, renames)
        assert record == {new: (legacy_offset, 6672116)}
        got = plugin._assign_channel_numbers(
            [game], 100, tz, existing_numbers={new: 6672116}, numbered_for=record,
        )
        assert got == {new: 6672116}
        released = plugin._assign_channel_numbers([game], 100, tz, existing_numbers={new: 6672116})
        assert released == {new: 6672114}  # espn_17's slot is 14: the chain rule moves it


class TestTheRecordFile:
    """numbering_state.json grows a `numbered_for` key (#278) beside
    `kickoff_base`, and both directions of an upgrade must keep working."""

    def test_a_file_from_before_the_record_keeps_its_floor(self, plugin):
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write('{"kickoff_base": 6892}')
        assert plugin._load_numbering_state() == (6892, {})

    def test_the_pre_record_loader_still_reads_the_floor(self, plugin):
        """A downgrade: the loader as it was before #278 (copied verbatim),
        reading a file this version wrote, still gets the floor."""
        plugin._save_numbering_state(
            6892, {"ranked_matchups:EPL:fd_8": (6672015, 6678908, 6892)},
        )

        def old_load_kickoff_base(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, ValueError):
                return None
            base = data.get("kickoff_base") if isinstance(data, dict) else None
            if isinstance(base, bool) or not isinstance(base, int) or base < 1:
                return None
            return base

        assert old_load_kickoff_base(plugin.NUMBERING_STATE_PATH) == 6892

    def test_damaged_entries_are_dropped_one_by_one(self, plugin):
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"kickoff_base": 6892, "numbered_for": {
                "good": [6672015, 6678908, 6892],
                "string": "6678908", "short": [6672015, 6678908], "long": [1, 2, 3, 4],
                "bool": [True, 6678908, 6892], "float": [6672015, 6678908.0, 6892],
                "negative_offset": [-1, 6678908, 6892], "zero_number": [6672015, 0, 6892],
                "zero_floor": [6672015, 6678908, 0], "zero_offset": [0, 6892, 6892],
            }}, f)
        assert plugin._load_numbering_state() == (
            6892, {"good": (6672015, 6678908, 6892), "zero_offset": (0, 6892, 6892)},
        )

    @pytest.mark.parametrize("raw", ['[]', '"x"', 'null', '7'])
    def test_a_record_that_is_not_a_map_reads_as_none(self, plugin, raw):
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write('{"kickoff_base": 6892, "numbered_for": %s}' % raw)
        assert plugin._load_numbering_state() == (6892, {})

    def test_a_record_without_a_floor_is_not_trusted(self, plugin):
        """The record only means something under the floor it was written with."""
        with open(plugin.NUMBERING_STATE_PATH, "w", encoding="utf-8") as f:
            f.write('{"kickoff_base": 0, "numbered_for": {"m": [6672015, 6678908, 6892]}}')
        assert plugin._load_numbering_state() == (None, {})


class TestKeptChannelsKeepTheirNumbers:
    """#286, the class: a channel apply keeps without writing it (an active
    recording, #146, or kept by its guide, #264) must not lose its number to
    another game, whatever the reason it is not written (its game left the
    slate, or the write loop skipped it). The numbers held back come from the
    SAME stale set and the SAME keep policy as the reap, so a new keep reason
    or a new skip in the write loop is covered without touching this."""

    def test_the_reap_and_the_second_placement_share_the_stale_set(self):
        _src, fn = _apply_fn()
        stale_calls = _calls(fn, "_unwritten_channels")
        assert len(stale_calls) == 2
        for call in stale_calls:
            assert [ast.unparse(a) for a in call.args] == ["existing_virtuals", "seen_markers"]
        held = [c for c in _calls(fn, "_numbers_held_for_recordings")
                if ast.unparse(c.args[0]).startswith("_unwritten_channels(")]
        assert len(held) == 1
        assert [ast.unparse(a) for a in held[0].args[1:]] == ["archive_enabled", "guide_kept_ids"]
        partitions = _calls(fn, "_partition_stale_for_recordings")
        assert partitions and all(
            ast.unparse(c.args[3]) == "archive_enabled" and ast.unparse(c.args[4]) == "guide_kept_ids"
            for c in partitions
        )

    def test_every_keep_reason_of_the_reap_is_held(self, plugin, monkeypatch):
        """Second opinion on #286: the AST guard above shows both sites pass
        the keep arguments, not that the helper honours them. Channel 1 is
        recording, channel 2 is kept by its guide (#264), channel 3 neither:
        exactly what _partition_stale_for_recordings keeps is held."""
        import sys
        import types
        from types import SimpleNamespace as NS

        now = datetime.now(timezone.utc)
        recording = NS(id=11, channel_id=1, start_time=now - timedelta(minutes=30),
                       end_time=now + timedelta(hours=1), custom_properties={"status": "recording"})

        class _Objects:
            def filter(self, channel_id__in):
                return [r for r in (recording,) if r.channel_id in channel_id__in]

        models = types.ModuleType("apps.channels.models")
        models.Recording = NS(objects=_Objects())
        for name in ("apps", "apps.channels"):
            monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
        monkeypatch.setitem(sys.modules, "apps.channels.models", models)

        stale = [NS(id=1, channel_number=33772827.0), NS(id=2, channel_number=33772840.0),
                 NS(id=3, channel_number=33772841.0)]
        held = plugin._numbers_held_for_recordings(stale, True, keep_ids={2})
        _reapable, kept, _rehome = plugin._partition_stale_for_recordings(
            stale, {1: [recording]}, now, True, {2},
        )
        assert held == {int(c.channel_number) for c in kept} == {33772827, 33772840}

    def test_the_stale_set_is_every_channel_not_written(self, plugin):
        kept, written = object(), object()
        assert plugin._unwritten_channels(
            {"a": kept, "b": written}, {"b"},
        ) == [kept]
