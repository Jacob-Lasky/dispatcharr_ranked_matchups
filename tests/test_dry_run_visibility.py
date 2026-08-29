"""Tests for #170: a dry_run apply must not look like a silent success.

The reported failure, from a user's log: refresh output, then

    [ranked_matchups.tasks] auto_pipeline task <id> complete: status=ok

and no group, no channels, and nothing anywhere saying why. Three things
combined to hide it:

  1. `dry_run` DEFAULTS TO TRUE, so this is the state every fresh install
     starts in.
  2. On an install with no group yet, `_action_apply` returns from its
     dry-run branch before it has logged anything at all.
  3. "Refresh + apply now" is async, so its result dict (which did carry a
     "[dry] Would create..." message) was discarded by the background thread
     and never rendered in a toast.

So the user's only surface was a log line that said `status=ok`. These tests
pin the three fixes: the message reaches the log, the notice states the
remedy, and show_status reports the setting.
"""

import ast
import importlib.util
import json
import logging
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = os.path.basename(REPO_ROOT)
PLUGIN_PY = os.path.join(REPO_ROOT, "plugin.py")


class TestDryRunEnabled:
    """ONE reader for the setting. apply decides whether to write from it and
    show_status tells the user what apply will do, so two private copies of the
    default is show_status able to report the opposite of what apply does."""

    def test_unset_defaults_to_on(self, plugin):
        assert plugin._dry_run_enabled({}) is True

    @pytest.mark.parametrize("settings", [None, {}])
    def test_missing_settings_defaults_to_on(self, plugin, settings):
        # show_status can be invoked with no settings at all; defaulting to
        # False there would tell the user apply is about to write when it isn't.
        assert plugin._dry_run_enabled(settings) is True

    @pytest.mark.parametrize("raw,expected", [(True, True), (False, False), ("", False), (1, True)])
    def test_coerces_to_bool(self, plugin, raw, expected):
        assert plugin._dry_run_enabled({"dry_run": raw}) is expected

    def test_code_default_matches_the_manifest_default(self, plugin):
        # Third source of truth for the same default: plugin.json cannot import
        # the constant (the loader reads the manifest WITHOUT executing plugin
        # code), so this text-level pairing is the only thing stopping the form
        # and the code disagreeing about the initial state of a fresh install.
        # Asserted against the literal True as well, so flipping BOTH sides in
        # lockstep still trips it -- that flip is a product decision, not a
        # refactor, and it should have to come here and say so.
        with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        field = next(f for f in manifest["fields"] if f["id"] == "dry_run")
        assert field["default"] is True
        assert plugin._dry_run_enabled({}) is field["default"]


class TestDryRunNotice:
    """The constant every surface shares. It has to name the CONSEQUENCE and
    the REMEDY: '[dry]' alone reads as a label, and the user in #170 read it
    as one."""

    def test_names_the_setting_by_its_ui_label(self, plugin):
        # The user has to find this control in the settings form, so the notice
        # must use the label the form shows, not the field id.
        assert "Dry run on Apply" in plugin.DRY_RUN_NOTICE

    def test_states_the_consequence_and_the_remedy(self, plugin):
        notice = plugin.DRY_RUN_NOTICE
        assert "writes nothing" in notice
        assert "Untick" in notice


class TestDryRunReport:
    """_dry_run_report is the shared exit path for both of apply's dry-run
    returns: it appends the notice and puts it in the log."""

    def test_off_returns_message_untouched_and_logs_nothing(self, plugin, caplog):
        with caplog.at_level(logging.INFO):
            out = plugin._dry_run_report("Group 'Top Matchups': created=12", False)
        assert out == "Group 'Top Matchups': created=12"
        assert plugin.DRY_RUN_NOTICE not in caplog.text

    def test_on_appends_the_notice_to_the_message(self, plugin):
        out = plugin._dry_run_report("[dry] Would create ChannelGroup", True)
        assert out.startswith("[dry] Would create ChannelGroup")
        assert plugin.DRY_RUN_NOTICE in out

    def test_on_also_logs_the_notice(self, plugin, caplog):
        # The log is NOT redundant with the return value: under auto_pipeline
        # the return value is discarded by the background thread, so this is
        # the only surface the user ever sees. This assertion is the #170
        # regression guard.
        with caplog.at_level(logging.INFO):
            plugin._dry_run_report("[dry] Would create ChannelGroup", True)
        assert plugin.DRY_RUN_NOTICE in caplog.text


@pytest.fixture
def fake_orm():
    """Stand `apps.channels.models` / `apps.epg.models` up as mock modules so
    `_action_apply` is callable without a Django DB.

    Only the no-group early return is driven this way -- it is the branch the
    #170 reporter actually hit (fresh install, no group yet, dry_run defaulted
    on), and everything it touches before returning is a get-or-none plus two
    emptiable sweep queries. The queryset chains are given explicit terminal
    values because a bare MagicMock `.first()` is truthy, which would silently
    route the test down the create-the-group path instead.
    """
    created = {}
    for mod_name, attrs in (
        ("apps", None),
        ("apps.channels", None),
        ("apps.channels.models", ("Channel", "ChannelGroup", "ChannelStream", "Recording", "Stream")),
        ("apps.epg", None),
        ("apps.epg.models", ("EPGSource", "EPGData", "ProgramData")),
    ):
        mod = types.ModuleType(mod_name)
        for attr in attrs or ():
            setattr(mod, attr, MagicMock(name=attr))
        created[mod_name] = mod

    models = created["apps.channels.models"]
    epg_models = created["apps.epg.models"]
    # No group with the target name exists yet.
    models.ChannelGroup.objects.filter.return_value.first.return_value = None
    # No leftovers from a previous target name.
    models.ChannelGroup.objects.exclude.return_value.filter.return_value.distinct.return_value = []
    epg_models.EPGSource.objects.exclude.return_value.filter.return_value.distinct.return_value = []

    saved = {name: sys.modules.get(name) for name in created}
    sys.modules.update(created)
    try:
        yield models
    finally:
        for name, prev in saved.items():
            if prev is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


class TestApplyEarlyReturnIsTheReportedBranch:
    """The #170 repro, driven through `_action_apply` itself rather than
    through the helper it calls. Without this, deleting the `_dry_run_report`
    call from apply leaves every other test in this file green while the user's
    bug comes back whole."""

    CACHE = {"games": [{"home": "A", "away": "B", "channel_id": 7}]}

    def _apply(self, plugin, settings):
        with patch.object(plugin, "_read_cache", return_value=self.CACHE):
            return plugin._action_apply(settings)

    def test_message_states_the_remedy(self, plugin, fake_orm):
        result = self._apply(plugin, {"dry_run": True})
        assert result["status"] == "ok"
        assert plugin.DRY_RUN_NOTICE in result["message"]

    def test_nothing_is_created(self, plugin, fake_orm):
        # The point of dry run. If this ever fires, the notice is the least of
        # the problems.
        self._apply(plugin, {"dry_run": True})
        fake_orm.ChannelGroup.objects.create.assert_not_called()

    def test_the_reported_log_line_is_emitted(self, plugin, fake_orm, caplog):
        # This is the exact gap in the user's paste: the run logged nothing at
        # all between the refresh output and `complete: status=ok`.
        with caplog.at_level(logging.INFO):
            self._apply(plugin, {"dry_run": True})
        assert plugin.DRY_RUN_NOTICE in caplog.text

    def test_unset_dry_run_takes_the_same_branch(self, plugin, fake_orm):
        # A fresh install has never touched the setting, which is precisely how
        # the reporter got here.
        assert plugin.DRY_RUN_NOTICE in self._apply(plugin, {})["message"]


@pytest.fixture(scope="module")
def apply_fn():
    """The `_action_apply` AST node. Module-scoped free function, matching
    test_apply_no_network_in_transaction: a class-scoped fixture defined as an
    instance method is deprecated in pytest 8 and removed in 10."""
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read(), filename=PLUGIN_PY)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_action_apply":
            return node
    pytest.fail("_action_apply not found in plugin.py")


class TestApplySummaryExitAlsoReports:
    """The second dry-run exit -- the end-of-run summary, reached when the
    group already exists -- runs several hundred lines of ORM work that the
    fake_orm fixture deliberately does not stand up. An AST contract covers it
    instead, mirroring test_apply_no_network_in_transaction's rationale: what
    matters is that the exit routes through the shared helper, and that is a
    structural property the AST states exactly.
    """

    def test_both_exits_route_through_the_shared_helper(self, apply_fn):
        calls = [
            n for n in ast.walk(apply_fn)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "_dry_run_report"
        ]
        assert len(calls) == 2, (
            "apply has two dry-run exits (the no-group early return and the "
            "end-of-run summary) and both must report; found "
            f"{len(calls)} _dry_run_report call(s)"
        )

    def test_apply_never_hand_rolls_the_notice(self, apply_fn):
        # Guards the drift the helper exists to prevent: a third exit added
        # later that pastes the constant inline picks up the message but not
        # the log line, which is the half that mattered in #170.
        inline = [
            n for n in ast.walk(apply_fn)
            if isinstance(n, ast.Name) and n.id == "DRY_RUN_NOTICE"
        ]
        assert not inline, (
            "_action_apply must reach DRY_RUN_NOTICE through _dry_run_report, "
            "which also logs it, not by interpolating the constant directly"
        )


class TestRunUnderLockLogsMessage:
    """The background thread body discards the result dict. Whatever it does
    not log is lost, which is why `status=ok` was the whole story."""

    def _run(self, tasks_mod, plugin, result):
        with patch.object(plugin, "_try_acquire_scheduler_lock", return_value=True):
            with patch.object(plugin, "_release_scheduler_lock"):
                with patch.object(tasks_mod, "_set_inflight"):
                    with patch.object(tasks_mod, "_clear_inflight"):
                        tasks_mod._run_under_lock(
                            "auto_pipeline", "tid", MagicMock(return_value=result), {},
                        )

    def test_result_message_reaches_the_log(self, tasks_mod, plugin, caplog):
        msg = "refresh: 76 games scored | apply: [dry] Would create ChannelGroup"
        with caplog.at_level(logging.INFO):
            self._run(tasks_mod, plugin, {"status": "ok", "message": msg})
        assert msg in caplog.text

    def test_status_is_still_logged(self, tasks_mod, plugin, caplog):
        with caplog.at_level(logging.INFO):
            self._run(tasks_mod, plugin, {"status": "ok", "message": "done"})
        assert "status=ok" in caplog.text

    def test_missing_message_does_not_crash_the_thread(self, tasks_mod, plugin, caplog):
        # An escaping exception here takes a uwsgi worker with it, so a result
        # dict without a message must degrade, not raise.
        with caplog.at_level(logging.INFO):
            self._run(tasks_mod, plugin, {"status": "ok"})
        assert "(no message)" in caplog.text


class TestShowStatusReportsDryRun:
    """show_status is the action users are told to click when nothing seems to
    have happened, so it has to be able to answer 'why is there no group'."""

    CACHE = {
        "refreshed_at": "2026-08-12T00:35:36+00:00",
        "summary": ["UFC: 2 games"],
        "games": [
            {
                "sport_prefix": "UFC",
                "home": "Fighter A",
                "away": "Fighter B",
                "rank_home": None,
                "rank_away": None,
                "score": 6.2,
                "score_breakdown": {"importance": 3},
                "kickoff_local": "Tue 10:00 PM",
                "is_today": False,
                "channel_name_current": "UFC HD",
            }
        ],
    }

    def _status(self, plugin, settings, cache=None):
        with patch.object(plugin, "_read_cache", return_value=cache if cache is not None else self.CACHE):
            with patch.object(plugin.tasks, "read_inflight", return_value=None):
                return plugin._action_show_status(settings)["message"]

    def test_notice_present_when_dry_run_on(self, plugin):
        assert plugin.DRY_RUN_NOTICE in self._status(plugin, {"dry_run": True})

    def test_notice_absent_when_dry_run_off(self, plugin):
        assert plugin.DRY_RUN_NOTICE not in self._status(plugin, {"dry_run": False})

    def test_unset_dry_run_is_treated_as_on(self, plugin):
        # Must mirror _action_apply's own `settings.get("dry_run", True)`. If
        # these defaults ever disagree, show_status lies about whether the next
        # apply writes -- and the manifest default is True, so "unset" is the
        # state every new install is in.
        assert plugin.DRY_RUN_NOTICE in self._status(plugin, {})

    def test_notice_present_when_cache_is_empty(self, plugin):
        # The very first thing a new user does is click show_status before any
        # refresh has completed. That path returns early, so it needs the
        # notice too.
        msg = self._status(plugin, {"dry_run": True}, cache={})
        assert "Cache empty" in msg
        assert plugin.DRY_RUN_NOTICE in msg

    def test_notice_is_the_last_line(self, plugin):
        # PLACEMENT IS THE POINT. An action result renders as a toast anchored
        # bottom-right that grows upward and cannot be scrolled, so a 30-line
        # status dump pushes its own opening lines off the top of the screen. A
        # notice at the top is a notice nobody reads. DO NOT let this drift
        # back to a leading banner.
        msg = self._status(plugin, {"dry_run": True})
        assert msg.rstrip().splitlines()[-1].endswith(plugin.DRY_RUN_NOTICE)

    def test_both_notices_render_with_dry_run_last(self, plugin):
        # The two can fire together, and the ordering claim is only meaningful
        # in that case: whichever notice is last is the one guaranteed to be on
        # screen, and for a user with no group at all that has to be dry run.
        msg = self._status(
            plugin, {"dry_run": True, "name_template": "{away_team} at {home_team}}"},
        )
        tail = msg.rstrip().splitlines()
        assert tail[-2].startswith("WARNING:")
        assert tail[-1].startswith("NOTE:")
        assert plugin.DRY_RUN_NOTICE in tail[-1]

    def test_template_warning_also_lands_at_the_bottom(self, plugin):
        # Same toast reasoning for the #126 banner, which used to lead.
        msg = self._status(
            plugin, {"dry_run": False, "name_template": "{away_team} at {home_team}}"},
        )
        tail = msg.rstrip().splitlines()[-1]
        assert tail.startswith("WARNING:")

    def test_game_dump_still_renders(self, plugin):
        # Guard against the notices displacing the actual status content.
        msg = self._status(plugin, {"dry_run": True})
        assert "Fighter B at Fighter A" in msg
        assert "Total games: 1" in msg
