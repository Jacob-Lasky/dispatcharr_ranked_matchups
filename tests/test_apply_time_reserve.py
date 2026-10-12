"""Apply never starts an irreversible step without the time to finish (#271 follow-up).

The pipeline child is killed 25 minutes after it starts, and auto_pipeline runs
refresh and apply in that one child. A slow refresh could leave apply seconds
to live. The #271 budget only cuts apply's cosmetic fetches to zero; apply
still started, and a kill landing after its committed recording cancellation
(`_autorecord_cancel_committed`) but before the main transaction wrote the
replacement left neither recording scheduled.

Driven through the real `_action_apply` against a real Django ORM (see
tests/orm_harness/run_scenarios.py). The child's remaining life comes from
`tasks.pipeline_child_seconds_left()`, which reads `tasks.time.monotonic`;
each scenario replaces `tasks.time` with a fake clock and sets the deadline
in its terms, so nothing sleeps.

Literals on purpose: 120 is APPLY_WRITE_RESERVE_SECONDS. Deriving it from the
plugin would let a wrong constant move the expectation with it.
"""

import importlib.util
import json
import os
import types
from datetime import timedelta

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load_kit():
    spec = importlib.util.spec_from_file_location(
        "_apply_harness_kit", os.path.join(HERE, "test_apply_empty_slate.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_kit = _load_kit()
_now, _settings, _seed_source_channel, _row, _write_cache, _publish, _owned = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish, _kit._owned,
)

G411, G431 = "ranked_matchups:CFB:411", "ranked_matchups:CFB:431"
RESERVE_S = 120


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now


class _Child:
    """Pretend apply runs in a pipeline child with `seconds_left` to live, on a
    fake clock the scenario can move. Restores everything on exit."""

    def __init__(self, env, seconds_left):
        self.env, self.seconds_left = env, seconds_left
        self.clock = _FakeClock()
        self._patches = []

    def patch(self, obj, attr, value):
        self._patches.append((obj, attr, getattr(obj, attr)))
        setattr(obj, attr, value)

    def __enter__(self):
        tasks = self.env.plugin.tasks
        self.patch(tasks, "time", self.clock)
        self._saved_env = os.environ.get(tasks.PIPELINE_DEADLINE_ENV)
        os.environ[tasks.PIPELINE_DEADLINE_ENV] = repr(self.clock.now + self.seconds_left)
        return self

    def __exit__(self, *exc):
        for obj, attr, original in reversed(self._patches):
            setattr(obj, attr, original)
        name = self.env.plugin.tasks.PIPELINE_DEADLINE_ENV
        if self._saved_env is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = self._saved_env
        return False


def _recording_411_on_air(env):
    """411 published with a recording the plugin scheduled for Home411."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    soon = _now() + timedelta(hours=6)
    owned = _publish(env, [_row(411, soon, user_ch.id, home="Home411", away="Away411")],
                     settings=_settings(recorded_teams="Home411"))
    rec = m.Recording.objects.get(channel=owned[G411])
    assert rec.custom_properties.get("ranked_matchups_marker") == G411
    return user_ch, soon, rec


def _swap_slate(env, user_ch, soon, drop_411=False):
    """Recorded teams move from Home411 to Home431: the plan cancels 411's
    recording and schedules 431's. With `drop_411` the 411 game also loses
    its feed, so its cancellation is the FIRST step's (undecided games)."""
    g411 = _row(411, soon, 999999 if drop_411 else user_ch.id, home="Home411", away="Away411")
    if drop_411:
        g411["score"] = 1.0
    _write_cache(env, [g411, _row(431, soon, user_ch.id, home="Home431", away="Away431")])


def _recordings(env):
    return sorted(
        r.custom_properties.get("ranked_matchups_marker")
        for r in env.m.Recording.objects.all()
    )


def _state(env):
    with open(env.plugin.RECORDING_STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_witness_apply_with_no_time_left_changes_nothing(env):
    """A refresh that finished with 60s of the child left: apply must not
    start. Before, it cancelled 411's recording and went on, and the parent's
    kill could land anywhere after that. The same apply also renames the live
    group, the write apply makes FIRST (step 1 moves our channels into the
    new group): with no time it must not happen either."""
    m = env.m
    user_ch, soon, rec = _recording_411_on_air(env)
    channels_before = sorted(_owned(env))
    _swap_slate(env, user_ch, soon)
    with _Child(env, seconds_left=60):
        result = env.plugin._action_apply(
            _settings(recorded_teams="Home431", channel_profile_name="!Top Matchups"))
    assert result["status"] == "error", result
    assert "not enough time" in result["message"], result
    assert m.Recording.objects.filter(pk=rec.pk).exists(), "a recording was cancelled"
    assert _recordings(env) == [G411]
    assert sorted(_owned(env)) == channels_before
    assert not m.ChannelGroup.objects.filter(name="!Top Matchups").exists(), (
        "apply started its group rename with no time to finish"
    )


def scenario_witness_a_slow_decide_half_stops_before_the_first_cancellation(env):
    """200s left at the start passes the first check, but the deciding half
    (stream pools over every game) spends 100s, leaving 100s when the first
    cancellation would commit. It must stop there, with nothing cancelled."""
    user_ch, soon, rec = _recording_411_on_air(env)
    _swap_slate(env, user_ch, soon, drop_411=True)
    with _Child(env, seconds_left=200) as child:
        prepare = env.plugin._autorecord_prepare

        def slow_prepare(*a, **k):
            child.clock.now += 100
            return prepare(*a, **k)

        child.patch(env.plugin, "_autorecord_prepare", slow_prepare)
        result = env.plugin._action_apply(_settings(recorded_teams="Home431"))
    assert result["status"] == "error", result
    assert "not enough time" in result["message"], result
    assert env.m.Recording.objects.filter(pk=rec.pk).exists(), (
        "the cancellation committed with less than the reserve left"
    )


def scenario_apply_with_time_left_runs_normally(env):
    """Guard: ten minutes left is plenty; the swap happens."""
    user_ch, soon, rec = _recording_411_on_air(env)
    _swap_slate(env, user_ch, soon)
    with _Child(env, seconds_left=600):
        result = env.plugin._action_apply(_settings(recorded_teams="Home431"))
    assert result["status"] == "ok", result
    assert _recordings(env) == [G431]


class _Killed(BaseException):
    """Stands in for the parent's kill: not an Exception, so nothing in apply
    that catches Exception can swallow it."""


def scenario_a_kill_after_the_cancellation_is_repaired_by_the_next_apply(env):
    """Guard for the decision to keep the cancellation where it is: if apply
    dies after a cancellation committed and before the replacement was
    written, the next apply converges. It schedules the replacement, and does
    NOT read the cancelled recording as one the user deleted."""
    user_ch, soon, rec = _recording_411_on_air(env)
    _swap_slate(env, user_ch, soon)
    http = importlib.import_module(env.plugin.__name__.rsplit(".", 1)[0] + "._http")
    saved = http.NetworkBudget

    def killed(*a, **k):
        raise _Killed()

    http.NetworkBudget = killed  # the network half starts right after the cancellation
    try:
        try:
            env.plugin._action_apply(_settings(recorded_teams="Home431"))
        except _Killed:
            pass
        else:
            raise AssertionError("the simulated kill never happened")
    finally:
        http.NetworkBudget = saved
    assert _recordings(env) == [], "the kill should leave the split this test repairs"
    assert G411 not in _state(env)["created"]

    result = env.plugin._action_apply(_settings(recorded_teams="Home431"))
    assert result["status"] == "ok", result
    assert _recordings(env) == [G431]
    state = _state(env)
    assert G431 in state["created"]
    assert G411 not in state["tombstones"], state


def _kill_cancellation_bookkeeping(env, after_write):
    """Make the FIRST state-file save during apply die like a kill: before
    the write (`after_write=False`) or right after it."""
    policy = env.plugin.recording_policy
    real = policy.save_state
    calls = []

    def save_then_maybe_die(*a, **k):
        calls.append(1)
        if len(calls) == 1 and not after_write:
            raise _Killed()
        real(*a, **k)
        if len(calls) == 1:
            raise _Killed()

    policy.save_state = save_then_maybe_die
    return lambda: setattr(policy, "save_state", real)


def _apply_until_killed(env, **settings):
    try:
        env.plugin._action_apply(_settings(**settings))
    except _Killed:
        return
    raise AssertionError("the simulated kill never happened")


def scenario_witness_a_kill_before_the_bookkeeping_tombstones_nothing(env):
    """Codex review: the cancellation committed, then the state file was
    written. A kill between the two left 411 in "created" with its row gone,
    and the next apply that WANTED 411 read the gap as the user deleting it:
    tombstoned, never recorded again. Written ahead inside the transaction,
    the kill rolls the deletion back instead."""
    user_ch, soon, rec = _recording_411_on_air(env)
    _swap_slate(env, user_ch, soon)
    restore = _kill_cancellation_bookkeeping(env, after_write=False)
    try:
        _apply_until_killed(env, recorded_teams="Home431")
    finally:
        restore()
    # The user adds Home411 back: 411 is wanted again.
    result = env.plugin._action_apply(_settings(recorded_teams="Home411,Home431"))
    assert result["status"] == "ok", result
    assert G411 not in _state(env)["tombstones"], _state(env)
    assert G411 in _recordings(env), _recordings(env)


def scenario_a_kill_right_after_the_bookkeeping_tombstones_nothing(env):
    """Guard for the write-ahead order: the state file says the recording is
    no longer ours to track, the deletion rolls back, and the row is read as
    ours and unedited, never as a user deletion."""
    user_ch, soon, rec = _recording_411_on_air(env)
    _swap_slate(env, user_ch, soon)
    restore = _kill_cancellation_bookkeeping(env, after_write=True)
    try:
        _apply_until_killed(env, recorded_teams="Home431")
    finally:
        restore()
    result = env.plugin._action_apply(_settings(recorded_teams="Home411,Home431"))
    assert result["status"] == "ok", result
    assert G411 not in _state(env)["tombstones"], _state(env)
    assert sorted(_recordings(env)) == [G411, G431], _recordings(env)


def scenario_a_reap_whose_apply_ran_short_is_finished_by_the_next_reap(env):
    """Reap's apply stops for time: reap reports the apply pending, and the
    next reap re-applies through its pending path and clears
    reap_apply_pending. Nothing re-runs it in a fresh child (tasks.py)."""
    user_ch, _ = _seed_source_channel(env)
    _publish(env, [_row(401, _now() - timedelta(hours=10), user_ch.id)])
    with _Child(env, seconds_left=60):
        result = env.plugin._action_reap(_settings())
    assert result["status"] == "error", result
    assert result["apply_pending"] is True
    assert "not enough time" in result["message"], result
    rerun = env.plugin._action_reap(_settings())  # the next reap, no deadline pressure
    assert rerun["status"] == "ok", rerun
    with open(env.plugin.CACHE_PATH, encoding="utf-8") as f:
        assert json.load(f)["reap_apply_pending"] is False
    assert _owned(env) == {}, "the finished game's channel is still on air"


# ----------------------------------------------------- the check itself ---

class TestApplyTimeShort:
    """`_apply_time_short` against a fake child clock. 120 is the reserve."""

    def _child(self, plugin, monkeypatch, seconds_left):
        clock = _FakeClock()
        monkeypatch.setattr(plugin.tasks, "time", clock)
        monkeypatch.setenv(plugin.tasks.PIPELINE_DEADLINE_ENV, repr(clock.now + seconds_left))

    def test_outside_a_pipeline_child_it_never_stops(self, plugin, monkeypatch):
        monkeypatch.delenv(plugin.tasks.PIPELINE_DEADLINE_ENV, raising=False)
        assert plugin._apply_time_short("before starting", False) is None

    def test_the_reserve_itself_is_enough(self, plugin, monkeypatch):
        self._child(plugin, monkeypatch, RESERVE_S)
        assert plugin._apply_time_short("before starting", False) is None

    def test_just_under_the_reserve_stops_and_says_why(self, plugin, monkeypatch):
        self._child(plugin, monkeypatch, RESERVE_S - 0.5)
        out = plugin._apply_time_short("before starting", False)
        assert out["status"] == "error"
        assert "not enough time" in out["message"] and "applied again" in out["message"]
        assert "(119s;" in out["message"], out  # rounded down, never shown as the reserve

    def test_a_child_already_past_its_deadline_stops(self, plugin, monkeypatch):
        self._child(plugin, monkeypatch, -5)
        out = plugin._apply_time_short("before cancelling recordings", False)
        assert out["status"] == "error" and "(0s;" in out["message"], out

    def test_the_stop_asks_for_a_fresh_child(self, plugin, monkeypatch):
        self._child(plugin, monkeypatch, 10)
        out = plugin._apply_time_short("before starting", False)
        assert out[plugin.tasks.APPLY_STOPPED_FOR_TIME] is True

    def test_a_dry_run_is_never_stopped(self, plugin, monkeypatch):
        """A dry run writes nothing, so a kill costs nothing (Codex review)."""
        self._child(plugin, monkeypatch, 10)
        assert plugin._apply_time_short("before starting", True) is None

    def test_auto_pipeline_passes_the_stop_on(self, plugin, monkeypatch):
        flag = plugin.tasks.APPLY_STOPPED_FOR_TIME
        monkeypatch.setattr(plugin, "_action_refresh", lambda s: {"status": "ok", "message": "r"})
        monkeypatch.setattr(plugin, "_action_apply",
                            lambda s: {"status": "error", "message": "short", flag: True})
        monkeypatch.setattr(plugin.tasks, "_update_inflight_phase", lambda *a: None)
        out = plugin._action_auto_pipeline_sync({})
        assert out["status"] == "error" and out[flag] is True
        monkeypatch.setattr(plugin, "_action_apply", lambda s: {"status": "ok", "message": "a"})
        assert plugin.tasks.APPLY_STOPPED_FOR_TIME not in plugin._action_auto_pipeline_sync({})


class TestFreshChildFollowUp:
    """tasks.run_pipeline_subprocess re-runs a stopped apply once, in a fresh
    child, with the action's own follow-up. subprocess.run is faked: each call
    returns the next scripted result line."""

    def _script(self, tasks_mod, monkeypatch, results):
        calls = []

        def fake_run(argv, input=None, **kw):
            calls.append(json.loads(input)["action"])
            out = results[len(calls) - 1]
            return types.SimpleNamespace(
                returncode=0, stderr="",
                stdout=tasks_mod._RESULT_SENTINEL + json.dumps(out) + "\n",
            )

        monkeypatch.setattr(tasks_mod.subprocess, "run", fake_run)
        return calls

    def _stopped(self, tasks_mod, message="apply: short"):
        return {"status": "error", "message": message, tasks_mod.APPLY_STOPPED_FOR_TIME: True}

    def test_a_stopped_apply_runs_once_more_in_a_fresh_child(self, tasks_mod, monkeypatch):
        action, follow_up = "auto_pipeline", "apply"
        calls = self._script(tasks_mod, monkeypatch,
                             [self._stopped(tasks_mod), {"status": "ok", "message": "done"}])
        phases = []
        monkeypatch.setattr(tasks_mod, "_update_inflight_phase", phases.append)
        out = tasks_mod.run_pipeline_subprocess(action, {})
        assert calls == [action, follow_up]
        assert out["status"] == "ok"
        # The follow-up's outcome leads: show_status cuts at 240 characters.
        assert out["message"].startswith(f"{follow_up} in a fresh child: done")
        assert "apply: short" in out["message"]
        # The inflight key is renewed for the second child (Codex review).
        assert phases == [f"{follow_up} (fresh child)"]

    def test_only_one_follow_up(self, tasks_mod, monkeypatch):
        calls = self._script(tasks_mod, monkeypatch,
                             [self._stopped(tasks_mod), self._stopped(tasks_mod, "again")])
        out = tasks_mod.run_pipeline_subprocess("auto_pipeline", {})
        assert calls == ["auto_pipeline", "apply"]
        assert out["status"] == "error"

    def test_any_other_result_runs_once(self, tasks_mod, monkeypatch):
        calls = self._script(tasks_mod, monkeypatch, [{"status": "error", "message": "band full"}])
        assert tasks_mod.run_pipeline_subprocess("auto_pipeline", {})["message"] == "band full"
        assert calls == ["auto_pipeline"]

    @pytest.mark.parametrize("action", ["apply", "reap"])
    def test_a_standalone_apply_or_reap_is_not_rerun(self, tasks_mod, monkeypatch, action):
        """Their child had its whole life: short there means wedged, and the
        HTTP Apply and Reap now are synchronous (Codex review)."""
        calls = self._script(tasks_mod, monkeypatch, [self._stopped(tasks_mod)])
        assert tasks_mod.run_pipeline_subprocess(action, {})["status"] == "error"
        assert calls == [action]

    def test_refresh_has_no_follow_up(self, tasks_mod, monkeypatch):
        calls = self._script(tasks_mod, monkeypatch, [self._stopped(tasks_mod)])
        tasks_mod.run_pipeline_subprocess("refresh", {})
        assert calls == ["refresh"]


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_time_reserve(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
