"""Tests for the DVR-recording preservation policy (#146).

`Recording.channel` is `on_delete=CASCADE`, so reaping a stale matchups game
channel would CASCADE-delete any recording made on it. `_action_apply` avoids
that by re-homing completed recordings onto a persistent archive channel and by
NOT reaping a channel whose recording is still active. The decision is isolated
in two pure (ORM-free) helpers so it can be tested without a Django DB, mirroring
`test_plugin_helpers.py`. The ORM wiring (`_ensure_archive_channel`,
`_cleanup_empty_archive`, and the apply integration) is exercised live against
the running container in the PR's live-verification section.
"""

import ast
import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

PLUGIN_PY = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "plugin.py"))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = os.path.basename(REPO_ROOT)


def _load_plugin_module():
    """Load plugin.py without exec-ing the package __init__ (which would start
    the scheduler thread and import Django). plugin.py does its Django imports
    lazily inside functions, so a top-level load is safe."""
    if f"{PKG_NAME}.plugin" in sys.modules:
        return sys.modules[f"{PKG_NAME}.plugin"]
    util_spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py")
    )
    util_mod = importlib.util.module_from_spec(util_spec)
    sys.modules[f"{PKG_NAME}._util"] = util_mod
    util_spec.loader.exec_module(util_mod)

    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.plugin", os.path.join(REPO_ROOT, "plugin.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def plugin():
    return _load_plugin_module()


NOW = datetime(2026, 6, 28, 18, 0, tzinfo=timezone.utc)


def _rec(rec_id, status=None, end_offset_hours=None):
    """A recording-like with the only two attrs the policy reads."""
    end = None if end_offset_hours is None else NOW + timedelta(hours=end_offset_hours)
    cp = {"status": status} if status is not None else {}
    return SimpleNamespace(id=rec_id, custom_properties=cp, end_time=end)


def _chan(chan_id):
    return SimpleNamespace(id=chan_id)


class TestRecordingIsActive:
    def test_in_progress_is_active(self, plugin):
        # status=recording means active regardless of end_time.
        assert plugin._recording_is_active(_rec(1, status="recording", end_offset_hours=-2), NOW)
        assert plugin._recording_is_active(_rec(1, status="recording", end_offset_hours=None), NOW)

    def test_completed_in_past_is_inactive(self, plugin):
        assert not plugin._recording_is_active(_rec(1, status="completed", end_offset_hours=-1), NOW)

    def test_stopped_in_past_is_inactive(self, plugin):
        assert not plugin._recording_is_active(_rec(1, status="stopped", end_offset_hours=-3), NOW)

    def test_scheduled_future_is_active(self, plugin):
        # No status yet (scheduled, not started) but end_time still ahead.
        assert plugin._recording_is_active(_rec(1, status=None, end_offset_hours=+2), NOW)

    def test_no_status_no_end_is_inactive(self, plugin):
        assert not plugin._recording_is_active(_rec(1, status=None, end_offset_hours=None), NOW)

    def test_none_custom_properties_is_handled(self, plugin):
        r = SimpleNamespace(id=1, custom_properties=None, end_time=NOW - timedelta(hours=1))
        assert not plugin._recording_is_active(r, NOW)


class TestPartitionStaleForRecordings:
    def test_channel_without_recordings_is_reapable(self, plugin):
        ch = _chan(10)
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], {}, NOW, archive_enabled=True
        )
        assert reapable == [ch]
        assert kept == []
        assert rehome == []

    def test_completed_recording_rehomed_then_reapable(self, plugin):
        ch = _chan(10)
        recs = {10: [_rec(100, status="completed", end_offset_hours=-1)]}
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], recs, NOW, archive_enabled=True
        )
        assert reapable == [ch]
        assert kept == []
        assert rehome == [100]

    def test_active_recording_keeps_channel(self, plugin):
        ch = _chan(10)
        recs = {10: [_rec(100, status="recording", end_offset_hours=+1)]}
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], recs, NOW, archive_enabled=True
        )
        assert reapable == []
        assert kept == [ch]
        assert rehome == []

    def test_mixed_active_and_done_keeps_channel_and_rehomes_nothing(self, plugin):
        # An active recording on the channel pins the whole channel; we do not
        # re-home its siblings mid-cycle (they reconcile once everything is done).
        ch = _chan(10)
        recs = {10: [
            _rec(100, status="completed", end_offset_hours=-2),
            _rec(101, status="recording", end_offset_hours=+1),
        ]}
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], recs, NOW, archive_enabled=True
        )
        assert reapable == []
        assert kept == [ch]
        assert rehome == []

    def test_archive_disabled_keeps_channel_with_recordings(self, plugin):
        # With no archive to move them to, a channel with recordings must be
        # kept rather than reaped (reaping would CASCADE the recordings away).
        ch = _chan(10)
        recs = {10: [_rec(100, status="completed", end_offset_hours=-1)]}
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], recs, NOW, archive_enabled=False
        )
        assert reapable == []
        assert kept == [ch]
        assert rehome == []

    def test_archive_disabled_still_reaps_channels_without_recordings(self, plugin):
        ch = _chan(10)
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [ch], {}, NOW, archive_enabled=False
        )
        assert reapable == [ch]
        assert kept == []
        assert rehome == []

    def test_multiple_channels_partition_independently(self, plugin):
        empty = _chan(1)                      # reap, nothing to move
        done = _chan(2)                       # reap, re-home its recordings
        active = _chan(3)                      # keep
        recs = {
            2: [_rec(200, status="completed", end_offset_hours=-1),
                _rec(201, status="stopped", end_offset_hours=-2)],
            3: [_rec(300, status="recording", end_offset_hours=+1)],
        }
        reapable, kept, rehome = plugin._partition_stale_for_recordings(
            [empty, done, active], recs, NOW, archive_enabled=True
        )
        assert reapable == [empty, done]
        assert kept == [active]
        assert sorted(rehome) == [200, 201]


class TestApplyPreservationWiring:
    """Static contract that `_action_apply` actually wires the preservation
    policy in: the unit tests above prove the pure helpers, but a future
    refactor could drop the call site and leave the bare CASCADE delete back in
    while every unit test stays green. This guards the integration the way
    test_apply_no_network_in_transaction.py guards call ordering."""

    @staticmethod
    def _apply_source():
        tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read(), filename=PLUGIN_PY)
        lines = open(PLUGIN_PY, encoding="utf-8").read().splitlines()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_action_apply":
                return "\n".join(lines[node.lineno - 1: node.end_lineno])
        raise AssertionError("_action_apply not found")

    def test_apply_invokes_preservation_helpers(self):
        src = self._apply_source()
        assert "_partition_stale_for_recordings(" in src
        assert "_ensure_archive_channel(" in src
        assert "_cleanup_empty_archive(" in src

    def test_preservation_precedes_channel_delete(self):
        # Recordings must be partitioned/re-homed BEFORE the stale channels are
        # deleted, or the CASCADE takes them first. Assert ordering by source
        # position within the reap block.
        src = self._apply_source()
        i_partition = src.index("_partition_stale_for_recordings(")
        i_delete = src.index("Channel.objects.filter(id__in=reap_ids).delete()")
        assert i_partition < i_delete, "partition/re-home must run before the reap delete"

    def test_no_unguarded_stale_delete(self):
        # The pre-fix code deleted every stale channel directly via
        # `Channel.objects.filter(id__in=stale_ids).delete()`. That exact path
        # must be gone: channel deletes now key off `reap_ids` (post-partition).
        # (Matching the full Channel-delete pattern, not a bare `stale_ids`
        # substring, so the legitimate Recording query on `stale_ids_all` that
        # gathers recordings across all stale channels doesn't false-trip this.)
        src = self._apply_source()
        assert "Channel.objects.filter(id__in=stale_ids)" not in src, (
            "unguarded all-stale channel delete must not return"
        )
