"""Tests for #172: put the curated channels into Dispatcharr Channel Profiles.

Reported by a user: "the visibility for the created channels is disabled for
all my profiles except for All".

The cause is structural, not a bug in either project. Dispatcharr creates
`ChannelProfileMembership` rows in its API VIEW layer
(`apps/channels/api_views.py`, `ChannelViewSet.create`), NOT in a model signal.
A plugin creates channels straight through the ORM, so no membership rows are
ever made, and `apps/output/views.py` filters a named profile on
`channelprofilemembership__enabled=True`. A channel with no row is therefore
absent from every named profile. `All` is not a profile at all: it is the
`profile_name is None` branch, which applies no membership filter.

Confirmed on a live instance before the fix: 24 plugin channels, 19 profiles,
0 membership rows.
"""

import ast
import importlib.util
import logging
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = os.path.basename(REPO_ROOT)


def _load_plugin_module():
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


class TestParseProfileNames:
    def test_blank_is_empty(self, plugin):
        for raw in ("", "   ", None, ",,,", "  ,  ,"):
            assert plugin._parse_profile_names(raw) == []

    def test_splits_and_strips(self, plugin):
        assert plugin._parse_profile_names(" Sports , Soccer ") == ["Sports", "Soccer"]

    def test_preserves_order(self, plugin):
        assert plugin._parse_profile_names("Soccer,Sports") == ["Soccer", "Sports"]

    def test_dedupes_case_insensitively_keeping_first_spelling(self, plugin):
        # The value is echoed back to the user; a list repeating their entry
        # reads as a bug.
        assert plugin._parse_profile_names("Sports, sports, SPORTS") == ["Sports"]

    def test_non_string_is_empty(self, plugin):
        # Settings come out of a JSON blob, so a number or a list is reachable.
        assert plugin._parse_profile_names(["Sports"]) == []
        assert plugin._parse_profile_names(7) == []

    def test_name_containing_spaces_survives(self, plugin):
        assert plugin._parse_profile_names("USL Super League, NHL") == [
            "USL Super League", "NHL",
        ]


class TestSelectProfiles:
    AVAILABLE = ["Sports", "Soccer", "NHL", "USL Super League"]

    def test_empty_request_selects_everything(self, plugin):
        # Blank means all profiles, mirroring what core does when a channel is
        # created with channel_profile_ids omitted.
        selected, unknown = plugin._select_profiles(self.AVAILABLE, [])
        assert selected == self.AVAILABLE
        assert unknown == []

    def test_matches_case_insensitively_but_returns_the_db_spelling(self, plugin):
        # The user copies the name by eye; the caller needs the real spelling to
        # look the profile back up.
        selected, unknown = plugin._select_profiles(self.AVAILABLE, ["sports", "nhl"])
        assert selected == ["Sports", "NHL"]
        assert unknown == []

    def test_unknown_names_are_reported_not_guessed(self, plugin):
        selected, unknown = plugin._select_profiles(self.AVAILABLE, ["Sports", "Sprots"])
        assert selected == ["Sports"]
        assert unknown == ["Sprots"]

    def test_all_names_unknown_selects_NOTHING_not_everything(self, plugin):
        # The load-bearing case. Falling back to "all profiles" on a typo would
        # push the channels into the very profiles the user was excluding, and
        # it would look like the setting worked.
        selected, unknown = plugin._select_profiles(self.AVAILABLE, ["Sprots"])
        assert selected == []
        assert unknown == ["Sprots"]

    def test_no_profiles_exist_at_all(self, plugin):
        assert plugin._select_profiles([], []) == ([], [])
        assert plugin._select_profiles([], ["Sports"]) == ([], ["Sports"])

    def test_duplicate_request_selects_once(self, plugin):
        selected, _ = plugin._select_profiles(self.AVAILABLE, ["Sports", "SPORTS"])
        assert selected == ["Sports"]


class TestProfileSummary:
    def test_silent_when_nothing_ran(self, plugin):
        assert plugin._profile_summary(None) == ""

    def test_silent_on_the_default_no_op(self, plugin):
        # Blank setting, everything already correct: a line on every apply
        # saying "all profiles" is noise in a message that already clips.
        stats = {"selected": ["Sports"], "unknown": [], "enabled": 0, "disabled": 0, "channels": 24}
        assert plugin._profile_summary(stats) == ""

    def test_reports_counts_when_something_changed(self, plugin):
        stats = {"selected": ["Sports"], "unknown": [], "enabled": 24, "disabled": 3, "channels": 24}
        out = plugin._profile_summary(stats)
        assert "24 enabled" in out and "3 disabled" in out and "24 channel(s)" in out

    def test_unknown_name_is_always_reported(self, plugin):
        stats = {"selected": ["Sports"], "unknown": ["Sprots"], "enabled": 24, "disabled": 0, "channels": 24}
        assert "'Sprots'" in plugin._profile_summary(stats)

    def test_unknown_names_are_capped_with_a_count(self, plugin):
        # The toast clips, so echoing a long typo list back costs the lines that
        # matter.
        stats = {
            "selected": [], "unknown": ["a", "b", "c", "d", "e"],
            "enabled": 0, "disabled": 0, "channels": 5,
        }
        out = plugin._profile_summary(stats)
        assert "+2 more" in out
        assert "'d'" not in out

    def test_total_typo_says_nothing_changed(self, plugin):
        # Otherwise the user gets "no such profile" with no hint that their
        # channels are consequently in no profile at all.
        stats = {"selected": [], "unknown": ["Sprots"], "enabled": 0, "disabled": 0, "channels": 24}
        out = plugin._profile_summary(stats)
        assert "'Sprots'" in out
        assert "left unchanged" in out


class TestManifestMatchesCode:
    """plugin.json is read by the loader WITHOUT executing plugin code, so the
    field id and its default are a second source of truth for the same concept.
    Same guard as test_manifest_stream_priority_matches_code."""

    def _field(self, plugin):
        import json
        repo = os.path.dirname(os.path.abspath(plugin.__file__))
        manifest = json.load(open(os.path.join(repo, "plugin.json"), encoding="utf-8"))
        return next(
            f for f in manifest["fields"] if f["id"] == plugin._ENABLED_PROFILES_SETTING
        )

    def test_the_field_the_code_reads_exists_in_the_manifest(self, plugin):
        # A rename on one side only means the UI writes a key nothing reads, and
        # the setting silently does nothing.
        assert self._field(plugin)["type"] == "string"

    def test_manifest_default_is_blank_which_the_code_reads_as_all_profiles(self, plugin):
        # Asserted against the literal "" rather than against the code's own
        # default, so flipping both in lockstep still trips this.
        field = self._field(plugin)
        assert field["default"] == ""
        assert plugin._parse_profile_names(field["default"]) == []
        assert plugin._select_profiles(["Sports"], [])[0] == ["Sports"]

    def test_the_group_setting_says_it_is_not_a_profile(self, plugin):
        # `channel_profile_name` is a ChannelGroup despite its id. With a real
        # Channel Profile setting beside it, the help text has to disambiguate
        # or the two are indistinguishable in the form.
        import json
        repo = os.path.dirname(os.path.abspath(plugin.__file__))
        manifest = json.load(open(os.path.join(repo, "plugin.json"), encoding="utf-8"))
        grp = next(f for f in manifest["fields"] if f["id"] == "channel_profile_name")
        assert "GROUP" in grp["help_text"]


@pytest.fixture(scope="module")
def apply_fn():
    """The `_action_apply` AST node. Module-scoped free function, matching
    test_apply_no_network_in_transaction."""
    tree = ast.parse(open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_action_apply":
            return node
    pytest.fail("_action_apply not found in plugin.py")


class TestApplyActuallyCallsTheSync:
    """Source-level contract. `_action_apply` needs Django to run, so every
    behavioural test above can pass with the sync never called from apply --
    which is exactly how a fix ships inert. Mirrors the rationale in
    test_apply_no_network_in_transaction and test_stream_ids_contract.
    """

    def test_apply_calls_sync_profile_memberships(self, apply_fn):
        calls = [
            n for n in ast.walk(apply_fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id == "_sync_profile_memberships"
        ]
        assert len(calls) == 1, f"expected exactly one call, found {len(calls)}"

    def test_the_call_forwards_dry_run(self, apply_fn):
        # Without this it either writes on a rehearsal or goes silent on one.
        call = next(
            n for n in ast.walk(apply_fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id == "_sync_profile_memberships"
        )
        kwargs = {k.arg for k in call.keywords}
        assert "dry_run" in kwargs

    def test_the_summary_includes_the_profile_line(self, apply_fn):
        # The sync can run correctly and still be invisible if its line never
        # reaches the message the user reads.
        names = {n.id for n in ast.walk(apply_fn) if isinstance(n, ast.Name)}
        assert "profile_msg" in names
        src = open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8").read()
        assert "{profile_msg}" in src, "profile_msg is computed but never interpolated"


class _Row:
    """A ChannelProfileMembership stand-in: the four attrs the sync reads."""

    def __init__(self, row_id, profile_id, channel_id, enabled):
        self.id = row_id
        self.channel_profile_id = profile_id
        self.channel_id = channel_id
        self.enabled = enabled


@pytest.fixture
def orm(request):
    """Fake `apps.channels.models` for `_sync_profile_memberships`.

    Records the writes rather than performing them, so the test asserts on WHAT
    the sync decided to change. Configure with
    `@pytest.mark.orm_state(profiles=..., channels=..., rows=...)`.
    """
    marker = request.node.get_closest_marker("orm_state")
    cfg = marker.kwargs if marker else {}
    profiles = cfg.get("profiles", [(1, "Sports"), (2, "Soccer")])
    channel_ids = cfg.get("channels", [10, 11])
    rows = cfg.get("rows", [])

    calls = {"created": [], "enabled_ids": [], "disabled_ids": []}

    profile_objs = [types.SimpleNamespace(id=pid, name=name) for pid, name in profiles]

    models = types.ModuleType("apps.channels.models")
    models.Channel = MagicMock(name="Channel")
    models.Channel.objects.filter.return_value.values_list.return_value = channel_ids
    models.ChannelProfile = MagicMock(name="ChannelProfile")
    models.ChannelProfile.objects.all.return_value = profile_objs

    membership = MagicMock(name="ChannelProfileMembership")
    membership.side_effect = lambda **kw: types.SimpleNamespace(**kw)
    membership.objects.filter.return_value = rows

    def _bulk_create(objs, ignore_conflicts=False):
        calls["created"].extend(objs)
        calls["ignore_conflicts"] = ignore_conflicts
        return objs
    membership.objects.bulk_create.side_effect = _bulk_create

    class _UpdateQS:
        def __init__(self, ids):
            self._ids = ids

        def update(self, enabled):
            (calls["enabled_ids"] if enabled else calls["disabled_ids"]).extend(self._ids)

    def _filter(**kw):
        if "id__in" in kw:
            return _UpdateQS(kw["id__in"])
        return rows
    membership.objects.filter.side_effect = _filter
    models.ChannelProfileMembership = membership

    pkgs = {"apps": types.ModuleType("apps"), "apps.channels": types.ModuleType("apps.channels")}
    pkgs["apps.channels.models"] = models
    saved = {n: sys.modules.get(n) for n in pkgs}
    sys.modules.update(pkgs)
    try:
        yield calls
    finally:
        for name, prev in saved.items():
            if prev is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


class TestSyncProfileMemberships:
    """The reported repro is the no-rows case: channels exist, profiles exist,
    not a single membership row between them."""

    def test_blank_setting_enables_every_profile(self, plugin, orm):
        stats = plugin._sync_profile_memberships({})
        # 2 profiles x 2 channels, none existing.
        assert len(orm["created"]) == 4
        assert all(row.enabled for row in orm["created"])
        assert stats["enabled"] == 4
        assert stats["disabled"] == 0

    def test_named_profile_enables_only_that_one(self, plugin, orm):
        plugin._sync_profile_memberships({"enabled_channel_profiles": "Soccer"})
        assert {row.channel_profile_id for row in orm["created"]} == {2}
        assert len(orm["created"]) == 2

    def test_unselected_profiles_get_no_disabled_rows(self, plugin, orm):
        # Core represents "not in this profile" as an ABSENT row and never
        # bulk-creates disabled ones. Manufacturing a row per channel per
        # profile would write thousands to say nothing.
        plugin._sync_profile_memberships({"enabled_channel_profiles": "Soccer"})
        assert all(row.enabled for row in orm["created"])

    def test_bulk_create_tolerates_a_concurrent_insert(self, plugin, orm):
        # A user toggling the same channel in the UI mid-apply must not abort
        # the whole transaction on the unique (profile, channel) constraint.
        plugin._sync_profile_memberships({})
        assert orm["ignore_conflicts"] is True

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, False), _Row(101, 2, 10, False)])
    def test_existing_disabled_rows_are_flipped_on(self, plugin, orm):
        plugin._sync_profile_memberships({})
        assert set(orm["enabled_ids"]) == {100, 101}

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, True), _Row(101, 2, 10, True)])
    def test_narrowing_the_setting_turns_the_others_off(self, plugin, orm):
        # THE convergence case. Without it, a profile removed from the setting
        # keeps the channels forever and the setting is write-once.
        plugin._sync_profile_memberships({"enabled_channel_profiles": "Sports"})
        assert orm["disabled_ids"] == [101]
        assert orm["enabled_ids"] == []

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, True), _Row(101, 2, 10, True)])
    def test_already_correct_state_writes_nothing(self, plugin, orm):
        # Idempotence: apply runs every 6 hours, and a no-op run must be a
        # genuine no-op, not 24 pointless UPDATEs.
        stats = plugin._sync_profile_memberships({})
        assert orm["enabled_ids"] == [] and orm["disabled_ids"] == []
        # channel 11 still has no rows at all, so those two are created.
        assert {r.channel_id for r in orm["created"]} == {11}
        assert stats["disabled"] == 0

    @pytest.mark.orm_state(profiles=[])
    def test_no_profiles_configured_is_a_no_op(self, plugin, orm):
        stats = plugin._sync_profile_memberships({})
        assert orm["created"] == []
        assert stats["enabled"] == 0

    @pytest.mark.orm_state(channels=[])
    def test_no_owned_channels_is_a_no_op(self, plugin, orm):
        stats = plugin._sync_profile_memberships({})
        assert orm["created"] == []
        assert stats["channels"] == 0

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, True)])
    def test_dry_run_writes_nothing_but_reports_the_real_numbers(self, plugin, orm):
        # A rehearsal that says nothing about profiles reads as one that had
        # nothing to say -- the #170 failure mode, in a new place. Counts come
        # from the PLAN, so they are the same numbers a real run would produce.
        stats = plugin._sync_profile_memberships(
            {"enabled_channel_profiles": "Sports"}, dry_run=True,
        )
        assert orm["created"] == []
        assert orm["enabled_ids"] == [] and orm["disabled_ids"] == []
        assert stats["enabled"] == 1   # channel 11 needs a Sports row
        assert stats["disabled"] == 0  # channel 10's Sports row is already on
        assert plugin._profile_summary(stats) != ""

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, True), _Row(101, 2, 10, True)])
    def test_dry_run_and_real_run_agree_on_the_numbers(self, plugin, orm):
        # The rehearsal is only worth printing if it predicts the real run.
        dry = plugin._sync_profile_memberships(
            {"enabled_channel_profiles": "Sports"}, dry_run=True,
        )
        wet = plugin._sync_profile_memberships({"enabled_channel_profiles": "Sports"})
        assert (dry["enabled"], dry["disabled"]) == (wet["enabled"], wet["disabled"])

    def test_typo_selects_nothing_and_reports(self, plugin, orm, caplog):
        with caplog.at_level(logging.WARNING):
            stats = plugin._sync_profile_memberships({"enabled_channel_profiles": "Sprots"})
        assert orm["created"] == []
        assert stats["unknown"] == ["Sprots"]
        # The log must name what DOES exist, or the user cannot fix the typo.
        assert "Sports" in caplog.text

    @pytest.mark.orm_state(rows=[
        _Row(100, 1, 10, True), _Row(101, 2, 10, True),
        _Row(102, 1, 11, True), _Row(103, 2, 11, True),
    ])
    def test_a_total_typo_disables_NOTHING(self, plugin, orm):
        # Regression guard for a bug the fake ORM hid and the live probe found.
        # With no pre-existing enabled rows, "select nothing" and "change
        # nothing" are indistinguishable, so the original version of this test
        # passed while the real behavior was: one misspelled name switched all
        # 24 channels off across all 19 profiles. Seed the enabled rows and the
        # two readings come apart.
        stats = plugin._sync_profile_memberships({"enabled_channel_profiles": "Sprots"})
        assert orm["disabled_ids"] == []
        assert orm["enabled_ids"] == []
        assert orm["created"] == []
        assert stats["disabled"] == 0

    @pytest.mark.orm_state(rows=[
        _Row(100, 1, 10, True), _Row(101, 2, 10, True),
        _Row(102, 1, 11, True), _Row(103, 2, 11, True),
    ])
    def test_a_PARTIAL_typo_still_applies_the_names_that_matched(self, plugin, orm):
        # The other half of the rule: "Sports, Sprots" states a legible intent,
        # so it is applied rather than discarded.
        stats = plugin._sync_profile_memberships(
            {"enabled_channel_profiles": "Sports, Sprots"}
        )
        assert set(orm["disabled_ids"]) == {101, 103}   # Soccer off
        assert orm["enabled_ids"] == []                  # Sports already on
        assert stats["unknown"] == ["Sprots"]

    @pytest.mark.orm_state(rows=[_Row(100, 1, 10, True)])
    def test_summary_for_a_total_typo_does_not_claim_a_change(self, plugin, orm):
        stats = plugin._sync_profile_memberships({"enabled_channel_profiles": "Sprots"})
        out = plugin._profile_summary(stats)
        assert "left unchanged" in out
        assert "disabled" not in out

