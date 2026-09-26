"""A game's channel identity must survive a kickoff-time change (#217).

`_build_marker_key` is the channel's tvg_id and the key apply uses to find the
game's existing Channel row. When it hashed `away|home|start_time_utc` for every
game without a cfbd_id / fd_id, a rescheduled NFL/NBA/NHL/MLB/ESPN game got a new
marker, so apply created a SECOND channel and orphaned the first (and, with a DVR
recording on the old channel, recorded the wrong window).
"""

import ast
import os
from datetime import timezone

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _row(sport, extra, start="2026-10-04T17:00:00Z", home="Home FC", away="Away FC"):
    return {
        "sport_prefix": sport,
        "home": home,
        "away": away,
        "start_time_utc": start,
        "extra": extra,
    }


class TestRescheduleKeepsIdentity:
    @pytest.mark.parametrize("sport,extra", [
        ("NFL", {"nfl_game_id": "401671789", "game_id": "401671789"}),
        ("NBA", {"nba_game_id": "401704999", "game_id": "401704999"}),
        ("NHL", {"nhl_game_id": 2025020123, "game_id": 2025020123}),
        ("MLB", {"mlb_game_id": 778899, "game_id": 778899}),
        ("WNBA", {"wnba_game_id": "401700001", "game_id": "401700001"}),
        ("NCAAW", {"ncaaw_game_id": "401700002", "game_id": "401700002"}),
        ("NCAASBL", {"ncaasbl_game_id": "401700003", "game_id": "401700003"}),
        ("NCAAMSOC", {"espn_event_id": "401700004", "game_id": "401700004"}),
        ("EFLCUP", {"espn_event_id": "401700005"}),
        ("F1", {"is_field_event": True, "espn_event_id": "600051234"}),
        ("CBB", {"cbb_id": 55555}),
        ("BOXING", {"is_field_event": True, "boxing_event_id": "bx-77"}),
    ])
    def test_same_game_new_kickoff_same_marker(self, plugin, sport, extra):
        before = _row(sport, extra, start="2026-10-04T17:00:00Z")
        after = _row(sport, extra, start="2026-10-04T20:25:00Z")
        assert plugin._build_marker_key(before) == plugin._build_marker_key(after)

    def test_teams_filled_in_later_keep_identity(self, plugin):
        # A bracket slot published as TBD and filled once the feeder game ends
        # is the same event: the channel should be renamed, not replaced.
        tbd = _row("NCAAMSOC", {"espn_event_id": "401700009"}, home="TBD", away="TBD")
        known = _row("NCAAMSOC", {"espn_event_id": "401700009"}, home="Duke", away="UNC")
        assert plugin._build_marker_key(tbd) == plugin._build_marker_key(known)

    def test_existing_cfbd_and_fd_formats_unchanged(self, plugin):
        # These channels already had stable identity; their tvg_id must not move.
        assert plugin._build_marker_key(_row("CFB", {"cfbd_id": 12345})) == "ranked_matchups:CFB:12345"
        assert plugin._build_marker_key(_row("EPL", {"fd_id": 99})) == "ranked_matchups:EPL:fd_99"

    def test_different_games_differ(self, plugin):
        a = _row("NFL", {"nfl_game_id": "1"})
        b = _row("NFL", {"nfl_game_id": "2"})
        assert plugin._build_marker_key(a) != plugin._build_marker_key(b)

    def test_id_spaces_do_not_collide(self, plugin):
        # The same number in two different id spaces is two different games.
        a = _row("X", {"espn_event_id": "42"})
        b = _row("X", {"cbb_id": 42})
        assert plugin._build_marker_key(a) != plugin._build_marker_key(b)

    @pytest.mark.parametrize("blank", [None, "", "   "])
    def test_blank_id_falls_back_to_legacy_hash(self, plugin, blank):
        g = _row("NFL", {"nfl_game_id": blank})
        assert plugin._build_marker_key(g) == plugin._legacy_marker_key(g)

    def test_bare_game_id_is_not_identity(self, plugin):
        # game_id is the simulator's key and is synthetic on some rows
        # (nhl.py / mlb.py stamp -(100000 + matchday)), so it must never be
        # used as a channel identity on its own.
        g = _row("NHL", {"game_id": -100005})
        assert plugin._build_marker_key(g) == plugin._legacy_marker_key(g)


class TestLegacyRekey:
    """Upgrading must rename existing channels in place, not recreate them:
    a recreate CASCADE-deletes any Recording on the old row."""

    class _Ch:
        def __init__(self, tvg_id, number=None):
            self.tvg_id = tvg_id
            self.channel_number = number

    def test_legacy_channel_is_rekeyed_to_new_marker(self, plugin):
        g = _row("NFL", {"nfl_game_id": "401671789"})
        legacy = plugin._legacy_marker_key(g)
        new = plugin._build_marker_key(g)
        assert legacy != new
        ch = self._Ch(legacy)
        existing = {legacy: ch}
        renames = plugin._rekey_legacy_virtuals(existing, [g])
        assert renames == [(ch, legacy, new)]
        assert existing == {new: ch}

    def test_no_rekey_when_new_marker_already_exists(self, plugin):
        g = _row("NFL", {"nfl_game_id": "401671789"})
        legacy, new = plugin._legacy_marker_key(g), plugin._build_marker_key(g)
        old_ch, new_ch = self._Ch(legacy), self._Ch(new)
        existing = {legacy: old_ch, new: new_ch}
        assert plugin._rekey_legacy_virtuals(existing, [g]) == []
        # The legacy duplicate is left for the normal stale path (which
        # preserves recordings); it is not silently merged or dropped here.
        assert existing == {legacy: old_ch, new: new_ch}

    def test_unchanged_marker_is_a_no_op(self, plugin):
        g = _row("CFB", {"cfbd_id": 1})
        ch = self._Ch(plugin._build_marker_key(g))
        existing = {ch.tvg_id: ch}
        assert plugin._rekey_legacy_virtuals(existing, [g]) == []

    def test_game_with_no_existing_channel_is_ignored(self, plugin):
        g = _row("NFL", {"nfl_game_id": "7"})
        assert plugin._rekey_legacy_virtuals({}, [g]) == []

    def test_ambiguous_legacy_marker_is_not_rekeyed(self, plugin):
        # Two TBD-vs-TBD bracket slots at one kickoff share a legacy hash, so
        # the old channel cannot be attributed to either. Guessing would move a
        # recording onto the wrong game; leave it for the stale path instead.
        g1 = _row("NCAAMSOC", {"espn_event_id": "1"}, home="TBD", away="TBD")
        g2 = _row("NCAAMSOC", {"espn_event_id": "2"}, home="TBD", away="TBD")
        legacy = plugin._legacy_marker_key(g1)
        assert legacy == plugin._legacy_marker_key(g2)
        ch = self._Ch(legacy)
        existing = {legacy: ch}
        assert plugin._rekey_legacy_virtuals(existing, [g1, g2]) == []
        assert existing == {legacy: ch}


class TestUniqueByMarker:
    """Two cached rows for one source event must not both reach apply: the
    write loop would create two channels with one tvg_id and one number, and
    the unique (group, number) constraint fails the whole transaction."""

    def test_same_event_listed_twice_keeps_the_first(self, plugin):
        a = _row("NCAAMSOC", {"espn_event_id": "9"}, home="Duke Blue Devils", away="UNC")
        b = _row("NCAAMSOC", {"espn_event_id": "9"}, home="Duke", away="North Carolina")
        c = _row("NCAAMSOC", {"espn_event_id": "10"})
        out, dropped = plugin._unique_by_marker([a, b, c])
        assert out == [a, c]
        assert dropped == 1

    def test_distinct_games_untouched(self, plugin):
        games = [_row("NFL", {"nfl_game_id": str(i)}) for i in range(3)]
        assert plugin._unique_by_marker(games) == (games, 0)


class TestMarkerKeyOrderIsPinned:
    def test_order_is_exactly_this(self, plugin):
        # Literal on purpose, NOT derived from _MARKER_ID_KEYS: precedence is
        # part of every live channel's identity, so reordering or inserting a
        # key above an existing one must be a deliberate, visible edit here.
        assert plugin._MARKER_ID_KEYS == (
            ("cfbd_id", ""),
            ("fd_id", "fd_"),
            ("espn_event_id", "espn_"),
            ("nfl_game_id", "nfl_"),
            ("nba_game_id", "nba_"),
            ("nhl_game_id", "nhl_"),
            ("mlb_game_id", "mlb_"),
            ("wnba_game_id", "wnba_"),
            ("ncaaw_game_id", "ncaaw_"),
            ("ncaasbl_game_id", "ncaasbl_"),
            ("cbb_id", "cbb_"),
            ("boxing_event_id", "boxing_"),
        )


class TestKickoffNumberSurvivesRekey:
    """The kickoff tiebreak slot hashes the marker, so a marker change alone
    would move a channel's number within its minute and could hand the vacated
    number to another same-minute game (the #117 wrong-programme binding). A
    game whose existing number is still inside its kickoff minute keeps it."""

    def _games(self):
        start = "2026-10-04T17:00:00Z"
        return [
            _row("NFL", {"nfl_game_id": str(i)}, start=start, home=f"H{i}", away=f"A{i}")
            for i in range(6)
        ]

    def test_existing_in_block_number_is_kept(self, plugin):
        games = self._games()
        fresh = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        # Pretend every channel held a DIFFERENT slot in the same minute block
        # (what the legacy marker hash gave them): reverse the assignment.
        markers = [plugin._build_marker_key(g) for g in games]
        nums = [fresh[m] for m in markers]
        held = dict(zip(markers, reversed(nums)))
        again = plugin._assign_channel_numbers(
            games, 1000, timezone.utc, existing_numbers=held,
        )
        assert again == held

    def test_number_outside_the_kickoff_block_is_not_kept(self, plugin):
        # A reschedule moves the game to a new minute: its old number belongs
        # to the old kickoff and must be released, not carried.
        g = self._games()[0]
        m = plugin._build_marker_key(g)
        stale = {m: 1000}  # minute 0 of the origin, nowhere near 2026-10-04
        got = plugin._assign_channel_numbers([g], 1000, timezone.utc, existing_numbers=stale)
        expected = plugin._assign_channel_numbers([g], 1000, timezone.utc)
        assert got == expected

    def test_kept_number_wins_ties_over_a_fresh_hash(self, plugin):
        games = self._games()[:2]
        m0, m1 = (plugin._build_marker_key(g) for g in games)
        fresh = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        # m1 already holds the number m0 would hash to.
        held = {m1: fresh[m0]}
        got = plugin._assign_channel_numbers(games, 1000, timezone.utc, existing_numbers=held)
        assert got[m1] == fresh[m0]
        assert got[m0] != got[m1]


class TestKeptNumberCannotBeDisplaced:
    """A fresh game nudged off a neighbour must not take a slot a kept game
    holds (second-opinion finding: two fresh games preferring slot h pushed
    the kept holder of h+1 to h+2, out of its minute block when h+1 was the
    block's last slot)."""

    def test_two_fresh_games_on_one_slot_do_not_move_the_holder_of_the_next(self, plugin):
        from dispatcharr_ranked_matchups._util import CHANNEL_NUMBER_TIEBREAK_SLOTS as SLOTS
        start = "2026-10-04T17:00:00Z"
        base = 1000
        by_slot = {}
        i = 0
        # Deterministic search for two fresh ids sharing a hash slot h, with
        # h+1 still inside the block, plus a third game to hold h+1.
        while True:
            g = _row("NFL", {"nfl_game_id": f"s{i}"}, start=start, home=f"H{i}", away=f"A{i}")
            n = plugin._assign_channel_numbers([g], base, timezone.utc)[plugin._build_marker_key(g)]
            h = (n - base) % SLOTS
            by_slot.setdefault(h, []).append((g, n))
            pick = [k for k, v in by_slot.items() if len(v) >= 2 and k + 1 < SLOTS and (k + 1) in by_slot]
            if pick:
                break
            i += 1
        h = pick[0]
        (f1, n_h), (f2, _) = by_slot[h][:2]
        holder, _ = by_slot[h + 1][0]
        hm = plugin._build_marker_key(holder)
        got = plugin._assign_channel_numbers(
            [f1, f2, holder], base, timezone.utc, existing_numbers={hm: n_h + 1},
        )
        assert got[hm] == n_h + 1
        assert len(set(got.values())) == 3


class TestApplyWiring:
    """plugin.py needs Django to run apply, so pin the call sites by source."""

    def _apply_src(self):
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as f:
            return f.read()

    def test_apply_dedupes_games_by_marker_before_anything_else(self):
        body = self._apply_src()
        body = body[body.index("def _action_apply(settings"):]
        load = body.index('games, dup_dropped = _unique_by_marker(cache.get("games", []))')
        assert load < body.index("existing_virtuals = {")

    def test_rekey_runs_before_numbers_are_assigned(self):
        src = self._apply_src()
        built = src.index("existing_virtuals = {")
        rekey = src.index("_rekey_legacy_virtuals(existing_virtuals, games)")
        numbering = src.index("chnum_by_marker = _assign_channel_numbers(")
        assert built < rekey < numbering

    def test_orphan_epg_cleanup_keeps_rows_a_channel_still_points_at(self):
        # A re-keyed channel skipped by the write loop still points at EPGData
        # under its OLD marker; the cleanup must not delete it by tvg_id alone.
        src = self._apply_src()
        i = src.index("orphan_epg_deleted = 0")
        block = src[i:src.index("orphan_epg_deleted, _ = orphans.delete()", i)]
        assert 'values_list("epg_data_id", flat=True)' in block
        assert ".exclude(id__in=attached_ids)" in block

    def test_rename_is_persisted_inside_the_transaction_via_update(self):
        src = self._apply_src()
        atomic = src.index("with transaction.atomic():", src.index("marker_renames = _rekey_legacy_virtuals("))
        persist = src.index("for ch, old_marker, new_marker in marker_renames:")
        park = src.index("# Phase 0: park existing virtual channels")
        # Inside the apply transaction, before anything else in it runs.
        assert atomic < persist < park
        block = src[persist:park]
        # Queryset .update() (stays off Channel post_save), guarded on the old
        # value, and skipped on a dry run.
        assert "Channel.objects.filter(pk=ch.pk, tvg_id=old_marker).update(tvg_id=new_marker)" in block
        assert "if dry_run:" in block


class TestEverySourceIdKeyIsKnown:
    """Every *_id key a source stamps into GameRow.extra must be either a
    marker identity key or explicitly exempt, so a new source with a new id
    field cannot silently fall back to the kickoff-time hash."""

    # Keys that are deliberately NOT channel identity, with the reason.
    EXEMPT = {
        # Simulator identity (#181). Always stamped alongside a specific id on
        # upcoming rows, and synthetic (negative matchday) on some pool rows.
        "game_id",
    }

    def _stamped_id_keys(self):
        keys = {}
        src_dir = os.path.join(REPO_ROOT, "sources")
        for fn in sorted(os.listdir(src_dir)):
            if not fn.endswith(".py"):
                continue
            tree = ast.parse(open(os.path.join(src_dir, fn), encoding="utf-8").read())
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Dict):
                    names = [k.value for k in node.keys
                             if isinstance(k, ast.Constant) and isinstance(k.value, str)]
                elif isinstance(node, ast.Call) and (
                    getattr(node.func, "id", "") == "field_event_extra"
                    or getattr(node.func, "attr", "") == "update"
                ):
                    # field_event_extra(**extra) and extra.update(key=...) both
                    # stamp keys as keywords; a dict passed to update() is
                    # already covered by the ast.Dict branch.
                    names = [kw.arg for kw in node.keywords if kw.arg]
                for n in names:
                    if n.endswith("_id"):
                        keys.setdefault(n, set()).add(fn)
        return keys

    def test_positive_control_finds_known_keys(self):
        keys = self._stamped_id_keys()
        for k in ("espn_event_id", "nfl_game_id", "cfbd_id", "fd_id", "boxing_event_id"):
            assert k in keys, k

    def test_every_key_is_identity_or_exempt(self, plugin):
        identity = {k for k, _ in plugin._MARKER_ID_KEYS}
        unknown = {k: sorted(f) for k, f in self._stamped_id_keys().items()
                   if k not in identity and k not in self.EXEMPT}
        assert not unknown, (
            f"source id keys not in _MARKER_ID_KEYS or EXEMPT: {unknown}. "
            "Add them to _MARKER_ID_KEYS (plugin.py) if they identify one event, "
            "or to EXEMPT here with the reason they do not."
        )
