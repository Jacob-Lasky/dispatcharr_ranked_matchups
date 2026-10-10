"""A failed or upstream-starved pipeline run must be visible (#227).

Before this, a run that timed out left one log line ("timed out after
1500s") and no record of where it stalled, and Show current state kept
presenting the last good cache, or "Cache empty. Run refresh.", as if
nothing were wrong. These tests pin the three surfaces that fix that:
  - the timeout path forwards the child's partial log and names its last step;
  - every background run records its outcome to last_run.json;
  - Show current state ends with a verdict line built from that record and
    from the cache (sources that errored, games with no playable stream).
"""

import json
import subprocess
import sys
from unittest.mock import MagicMock, patch


class TestTimeoutKeepsTheChildLog:
    def test_real_timeout_forwards_partial_stderr_and_names_last_step(self, tasks_mod, caplog):
        # A REAL child and a REAL TimeoutExpired, not a hand-built exception:
        # the bug was that CPython attaches the partial stderr (as bytes, even
        # under text=True) and this branch ignored it. A mock would only prove
        # the code handles the shape the mock was given.
        child = (
            "import sys, time\n"
            "print('INFO plugins.x: [ranked_matchups] NHL: pulled 98 games', file=sys.stderr, flush=True)\n"
            "print('INFO plugins.x: [ranked_matchups] scoring NHL importance', file=sys.stderr, flush=True)\n"
            "time.sleep(30)\n"
        )
        real_run = subprocess.run

        def run_child(cmd, **kwargs):
            return real_run([sys.executable, "-c", child], **{**kwargs, "timeout": 2})

        caplog.set_level("INFO")
        with patch.object(tasks_mod.subprocess, "run", side_effect=run_child):
            out = tasks_mod.run_pipeline_subprocess("auto_pipeline", {})
        assert out["status"] == "error"
        assert "timed out" in out["message"]
        assert "last step reached: [ranked_matchups] scoring NHL importance" in out["message"]
        forwarded = [r.getMessage() for r in caplog.records if "[ranked_matchups.subproc]" in r.getMessage()]
        assert any("NHL: pulled 98 games" in m for m in forwarded)

    def test_timeout_with_no_child_output_still_reports(self, tasks_mod):
        with patch.object(
            tasks_mod.subprocess, "run",
            side_effect=subprocess.TimeoutExpired(cmd="x", timeout=1, stderr=None),
        ):
            out = tasks_mod.run_pipeline_subprocess("refresh", {})
        assert out["status"] == "error"
        assert "last step" not in out["message"]

    def test_forward_is_bounded_and_says_so(self, tasks_mod, caplog):
        caplog.set_level("INFO")
        lines = [f"line {i}" for i in range(tasks_mod._TIMEOUT_LOG_TAIL_LINES + 50)]
        tasks_mod._forward_child_log(lines, tail=tasks_mod._TIMEOUT_LOG_TAIL_LINES)
        msgs = [r.getMessage() for r in caplog.records]
        assert any("50 earlier lines omitted" in m for m in msgs)
        assert sum("[ranked_matchups.subproc] line" in m for m in msgs) == tasks_mod._TIMEOUT_LOG_TAIL_LINES


class TestLastRunRecord:
    def _run(self, tasks_mod, plugin_mod, target):
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value="tok"):
            with patch.object(plugin_mod, "_release_scheduler_lock"):
                with patch.object(tasks_mod, "_set_inflight"), patch.object(tasks_mod, "_clear_inflight"):
                    tasks_mod._run_under_lock("auto_pipeline", "tid", target, {})
        return tasks_mod.read_last_run()

    def test_failures_count_up_and_success_resets(self, tasks_mod, plugin_mod):
        fail = MagicMock(return_value={"status": "error", "message": "pipeline subprocess timed out"})
        ok = MagicMock(return_value={"status": "ok", "message": "fine"})
        first = self._run(tasks_mod, plugin_mod, fail)
        assert first["status"] == "error" and first["consecutive_failures"] == 1
        assert first["last_ok_at"] is None
        second = self._run(tasks_mod, plugin_mod, fail)
        assert second["consecutive_failures"] == 2
        third = self._run(tasks_mod, plugin_mod, ok)
        assert third["status"] == "ok" and third["consecutive_failures"] == 0
        assert third["last_ok_at"] == third["finished_at"]
        fourth = self._run(tasks_mod, plugin_mod, fail)
        assert fourth["last_ok_at"] == third["finished_at"]

    def test_crash_is_recorded_as_a_failure(self, tasks_mod, plugin_mod):
        rec = self._run(tasks_mod, plugin_mod, MagicMock(side_effect=RuntimeError("boom")))
        assert rec["status"] == "error" and "boom" in rec["message"]

    def test_skipped_run_writes_no_verdict(self, tasks_mod, plugin_mod):
        # A run that never acquired the lock did not run: it may note the skip,
        # but must not write a status, which would overwrite the real outcome
        # of the run holding the lock, or on a fresh install read as a failure.
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value=None):
            tasks_mod._run_under_lock("refresh", "tid", MagicMock(), {})
        rec = tasks_mod.read_last_run()
        assert "status" not in rec and rec["skipped_kind"] == "refresh"
        msg = _status(plugin_mod, {"games": [], "summary": []}, rec)
        assert "FAILED" not in msg and "did not run" in msg

    def test_unreadable_file_reads_as_none(self, tasks_mod):
        with open(tasks_mod.LAST_RUN_PATH, "w") as f:
            f.write("{not json")
        assert tasks_mod.read_last_run() is None


def _status(plugin_mod, cache, last_run, settings=None, inflight=None):
    with patch.object(plugin_mod, "_read_cache", return_value=cache):
        with patch.object(plugin_mod.tasks, "read_inflight", return_value=inflight):
            with patch.object(plugin_mod.tasks, "read_last_run", return_value=last_run):
                return plugin_mod._action_show_status(settings or {"dry_run": False})["message"]


_FAILED = {
    "kind": "scheduled_auto_pipeline", "status": "error",
    "message": "pipeline subprocess timed out after 25 min; last step reached: [ranked_matchups] NHL",
    "finished_at": "2026-10-10T08:25:00+00:00", "duration_s": 1500, "consecutive_failures": 9,
}
_GAME = {
    "sport_prefix": "NHL", "home": "A", "away": "B", "score": 5.0,
    "channel_id": None, "stream_ids": [], "score_breakdown": {},
}


class TestShowStatusVerdict:
    def test_failed_run_is_the_last_line_with_streak_and_reason(self, plugin_mod):
        msg = _status(plugin_mod, {"games": [], "summary": []}, _FAILED)
        last = msg.splitlines()[-1]
        assert last.startswith("LAST RUN FAILED")
        assert "9 failures in a row" in last
        assert "timed out after 25 min" in last
        assert "Last successful refresh: never" in last

    def test_failed_run_over_a_stale_cache_names_the_stale_time(self, plugin_mod):
        cache = {"games": [dict(_GAME, channel_id=7)], "summary": ["NHL: 1 games"],
                 "refreshed_at": "2026-10-09T14:22:54+00:00"}
        msg = _status(plugin_mod, cache, _FAILED, {"dry_run": False, "local_timezone": "America/New_York"})
        last = msg.splitlines()[-1]
        assert last.startswith("LAST RUN FAILED")
        assert "Last successful refresh: 2026-10-09 10:22 EDT" in last

    def test_no_playable_stream_is_called_out(self, plugin_mod):
        cache = {"games": [dict(_GAME), dict(_GAME, home="C")], "summary": ["NHL: 2 games"]}
        msg = _status(plugin_mod, cache, None)
        assert "none of the 2 listed games matched a stream" in msg

    def test_stream_granular_match_counts_as_playable(self, plugin_mod):
        cache = {"games": [dict(_GAME, stream_ids=[42])], "summary": ["NHL: 1 games"]}
        assert "matched a stream" not in _status(plugin_mod, cache, None)

    def test_errored_sources_are_named(self, plugin_mod):
        cache = {"games": [], "refreshed_at": "2026-10-10T08:00:00+00:00",
                 "summary": ["NFL: error (401 Unauthorized)", "NHL: 0 games"]}
        msg = _status(plugin_mod, cache, None)
        assert "1 source(s) failed to fetch (NFL)" in msg
        assert "found no games to list in the lookahead window" in msg

    def test_ok_run_reports_duration(self, plugin_mod):
        ok = dict(_FAILED, status="ok", duration_s=1260, consecutive_failures=0)
        cache = {"games": [dict(_GAME, channel_id=7)], "summary": ["NHL: 1 games"]}
        assert msg_last(_status(plugin_mod, cache, ok)).startswith("Last run OK")


def msg_last(msg):
    return msg.splitlines()[-1]


class TestSlateProblems:
    """The one check both the refresh result and Show current state use."""

    def test_clean_slate_has_no_problems(self, plugin_mod):
        assert plugin_mod._slate_problems(["NHL: 3 games"], [dict(_GAME, channel_id=1)]) == []

    def test_errored_sources_are_capped_with_a_count(self, plugin_mod):
        summary = [f"S{i}: error (boom)" for i in range(5)]
        (problem,) = plugin_mod._slate_problems(summary, [])
        assert "5 source(s) failed to fetch (S0, S1, S2 +2 more)" in problem

    def test_empty_list_is_not_a_no_stream_problem(self, plugin_mod):
        # No games is the off-day case, reported by the empty-refresh path;
        # "none of the 0 listed games matched" would be nonsense.
        assert plugin_mod._slate_problems(["NHL: 0 games"], []) == []

    def test_refresh_result_leads_with_the_warning(self, plugin_mod):
        # Pins that _action_refresh routes its result through the shared
        # helper rather than an inline copy that could drift.
        import inspect
        src = inspect.getsource(plugin_mod._action_refresh)
        assert "_slate_problems(src_summary, games_payload)" in src
        assert 'f"WARNING: {p}. "' in src


class TestReviewFollowups:
    def test_completed_run_forwards_every_line(self, tasks_mod, caplog):
        # The tail cap is for TIMED-OUT runs only. A completed run's log is its
        # whole diagnostic record and must not be truncated.
        caplog.set_level("INFO")
        n = tasks_mod._TIMEOUT_LOG_TAIL_LINES + 50
        stderr = "\n".join(f"line {i}" for i in range(n))
        stdout = tasks_mod._RESULT_SENTINEL + json.dumps({"status": "ok"})
        from types import SimpleNamespace
        done = SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr)
        with patch.object(tasks_mod.subprocess, "run", return_value=done):
            tasks_mod.run_pipeline_subprocess("refresh", {})
        msgs = [r.getMessage() for r in caplog.records]
        assert sum("[ranked_matchups.subproc] line" in m for m in msgs) == n
        assert not any("earlier lines omitted" in m for m in msgs)

    def test_source_error_entry_is_what_the_warning_reads(self, plugin_mod):
        # Contract between the refresh summary writer and _slate_problems.
        entry = plugin_mod._source_error_entry("NFL", RuntimeError("401 Unauthorized: bad key"))
        assert entry == "NFL: error (401 Unauthorized: bad key)"
        (problem,) = plugin_mod._slate_problems([entry, "NHL: 3 games"], [])
        assert "1 source(s) failed to fetch (NFL)" in problem

    def test_refresh_writes_errors_through_the_shared_helper(self, plugin_mod):
        import inspect
        src = inspect.getsource(plugin_mod._action_refresh)
        assert "_source_error_entry(src.sport_label, e)" in src
        assert ': error (' not in src

    def test_unparseable_timestamp_is_shown_raw(self, plugin_mod):
        from datetime import timezone
        assert plugin_mod._fmt_run_time("not-a-time", timezone.utc) == "not-a-time"
        assert plugin_mod._fmt_run_time(None, timezone.utc) == "never"
        assert plugin_mod._fmt_run_time("2026-10-10T08:25:00", timezone.utc) == "2026-10-10 08:25 UTC"

    def test_verdict_uses_human_run_names(self, plugin_mod):
        msg = _status(plugin_mod, {"games": [], "summary": []}, _FAILED)
        assert "LAST RUN FAILED: Scheduled refresh + apply at" in msg
        assert "scheduled_auto_pipeline" not in msg

    def test_scoring_logs_progress(self, plugin_mod):
        # A timeout's "last step reached" is the child's last log line; scoring
        # must log or that line points at the fetch phase.
        import inspect
        src = inspect.getsource(plugin_mod._action_refresh)
        assert '"[ranked_matchups] scoring %d games"' in src
        assert "_SCORING_PROGRESS_EVERY" in src



class TestSecondOpinionFindings:
    """Defects an outside review (gpt-6-sol) found in the first cut, each
    verified against the code before fixing."""

    def _locked_run(self, tasks_mod, plugin_mod, target):
        release = MagicMock()
        clear = MagicMock()
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value="tok"):
            with patch.object(plugin_mod, "_release_scheduler_lock", release):
                with patch.object(tasks_mod, "_set_inflight"), patch.object(tasks_mod, "_clear_inflight", clear):
                    tasks_mod._run_under_lock("auto_pipeline", "tid", target, {})
        return release, clear

    def test_corrupt_record_cannot_skip_the_lock_release(self, tasks_mod, plugin_mod):
        # A non-numeric streak in a hand-edited or truncated file used to raise
        # inside `finally`, ahead of the inflight clear and the lock release.
        with open(tasks_mod.LAST_RUN_PATH, "w") as f:
            json.dump({"status": "error", "consecutive_failures": "oops"}, f)
        release, clear = self._locked_run(
            tasks_mod, plugin_mod, MagicMock(return_value={"status": "error", "message": "x"}))
        release.assert_called_once()
        clear.assert_called_once()
        assert tasks_mod.read_last_run()["consecutive_failures"] == 1

    def test_unwritable_record_cannot_skip_the_lock_release(self, tasks_mod, plugin_mod, monkeypatch):
        monkeypatch.setattr(tasks_mod, "LAST_RUN_PATH", "/nonexistent-dir/last_run.json")
        release, clear = self._locked_run(tasks_mod, plugin_mod, MagicMock(return_value={"status": "ok"}))
        release.assert_called_once()
        clear.assert_called_once()

    def test_corrupt_streak_does_not_break_show_status(self, plugin_mod):
        bad = dict(_FAILED, consecutive_failures="oops", duration_s="x")
        assert "LAST RUN FAILED" in _status(plugin_mod, {"games": [], "summary": []}, bad)

    def test_temp_file_is_writer_unique(self, tasks_mod, monkeypatch):
        # Refresh runs without the lock when Redis is down, so two workers can
        # write at once; a shared temp name lets one replace the other's file.
        seen = []
        real_replace = tasks_mod.os.replace
        monkeypatch.setattr(tasks_mod.os, "replace", lambda a, b: (seen.append(a), real_replace(a, b)))
        tasks_mod._write_last_run({"status": "ok"})
        (tmp,) = seen
        assert str(tasks_mod.os.getpid()) in tmp and tmp != tasks_mod.LAST_RUN_PATH + ".tmp"

    def test_skipped_request_is_recorded_beside_the_verdict(self, tasks_mod, plugin_mod):
        ok = MagicMock(return_value={"status": "ok", "message": "fine"})
        self._locked_run(tasks_mod, plugin_mod, ok)
        with patch.object(plugin_mod, "_try_acquire_scheduler_lock", return_value=None):
            tasks_mod._run_under_lock("auto_pipeline", "tid2", MagicMock(), {})
        rec = tasks_mod.read_last_run()
        assert rec["status"] == "ok"          # the verdict is untouched
        assert rec["skipped_kind"] == "auto_pipeline"
        msg = _status(plugin_mod, {"games": [], "summary": []}, rec)
        assert msg.splitlines()[-1].startswith("NOTE: Refresh + apply requested")
        assert "did not run" in msg.splitlines()[-1]

    def test_ok_verdict_counts_the_warnings_above_it(self, plugin_mod):
        ok = dict(_FAILED, status="ok", consecutive_failures=0)
        cache = {"games": [dict(_GAME)], "summary": ["NFL: error (401)", "NHL: 1 games"]}
        last = _status(plugin_mod, cache, ok).splitlines()[-1]
        assert last.startswith("Last run OK with 2 warning(s) above")

    def test_running_now_is_the_last_line_not_the_previous_verdict(self, plugin_mod):
        ok = dict(_FAILED, status="ok", consecutive_failures=0)
        inflight = {"kind": "auto_pipeline", "phase": "refresh", "task_id": "abcdef0123456789",
                    "started_at": "2026-10-10T13:56:27+00:00"}
        lines = _status(plugin_mod, {"games": [dict(_GAME, channel_id=1)], "summary": ["NHL: 1 games"]},
                        ok, inflight=inflight).splitlines()
        assert lines[-2].startswith("RUNNING NOW: Refresh + apply (refresh) since 2026-10-10 13:56 UTC")
        assert lines[-1].startswith("Previous run OK")

    def test_child_warnings_keep_their_level(self, tasks_mod, caplog):
        import logging
        caplog.set_level("INFO")
        tasks_mod._forward_child_log([
            "WARNING plugins.x: [ranked_matchups] refresh: 1 source(s) failed",
            "INFO plugins.x: [ranked_matchups] NHL: pulled 3 games",
            "Traceback (most recent call last):",
        ])
        levels = [r.levelno for r in caplog.records]
        assert levels == [logging.WARNING, logging.INFO, logging.INFO]

    def test_progress_reports_games_finished_not_started(self, plugin_mod):
        import inspect
        src = inspect.getsource(plugin_mod._action_refresh)
        assert "for done_n, (g, src) in enumerate(zip(all_games, game_sources)):" in src
        assert "if done_n and done_n % _SCORING_PROGRESS_EVERY == 0:" in src

    def test_apply_phase_is_marked_in_the_log(self, plugin_mod, caplog, monkeypatch):
        caplog.set_level("INFO")
        monkeypatch.setattr(plugin_mod, "_action_refresh", lambda s: {"status": "ok", "message": "r"})
        monkeypatch.setattr(plugin_mod, "_action_apply", lambda s: {"status": "ok", "message": "a"})
        monkeypatch.setattr(plugin_mod.tasks, "_update_inflight_phase", lambda p: None)
        plugin_mod._action_auto_pipeline_sync({})
        assert any("[ranked_matchups] apply: starting" in r.getMessage() for r in caplog.records)
