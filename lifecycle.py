"""Dispatcharr v3 adapter. Legacy entrypoints stay in plugin.py/tasks.py.

One host-owned service handles clocks and short maintenance work. Heavy work
is dispatched through the host queue, still protected by our Redis run/slot
locks and subprocess boundary. A leadership lease alone is not a fence.
"""
from datetime import datetime, timezone
import time
import uuid

from . import tasks

PIPELINE_ACTIONS = {
    'refresh': tasks.ACTION_REFRESH,
    'apply': tasks.ACTION_APPLY,
    'auto_pipeline': tasks.ACTION_AUTO_PIPELINE,
    'rematch': tasks.ACTION_REMATCH,
    'reap_now': tasks.ACTION_REAP,
}
SLOT_PARAM = '_scheduled_slot'


def _outcome():
    record = tasks.read_last_run() or {}
    return {'status': record.get('status', 'error'),
            'message': record.get('message', 'Run ended without a recorded outcome.')}


def _pipeline(action, settings, kind, *, record_skip=True):
    from . import plugin as implementation
    result_box = []

    def execute(current):
        result = tasks.run_pipeline_subprocess(PIPELINE_ACTIONS[action], current)
        result_box.append(result)
        return result

    ran = tasks._run_under_lock(kind, str(uuid.uuid4()), execute, settings,
                               record_skip=record_skip)
    result = (result_box[0] if result_box else _outcome()) if ran else {
        'status': 'partial', 'lock_busy': True,
        'message': implementation._run_lock_unavailable_reason(),
    }
    return ran, result


def run(plugin, action, settings, context):
    from . import plugin as implementation
    report = context.get('report_progress', lambda *args: None)
    report(5, f'Running {action}')
    if action == 'auto_pipeline' and settings.get(SLOT_PARAM):
        slot = implementation.parse_iso_utc(settings[SLOT_PARAM])
        if slot is None:
            return {'status': 'error', 'message': 'Invalid scheduled slot.'}
        if not settings.get('auto_refresh_enabled', False):
            return {'status': 'partial', 'message': 'Scheduled refresh is disabled.'}
        tz = implementation._resolve_tz(settings.get('local_timezone', 'UTC'))
        times = implementation._parse_scheduled_times(
            settings.get('scheduled_times', implementation._DEFAULT_SCHEDULED_TIMES))
        result = {'status': 'partial', 'message': 'Slot already claimed, covered, or past its retry deadline.'}

        def scheduled(current, *, record_skip=True):
            nonlocal result
            ran, result = _pipeline('auto_pipeline', current, tasks.SCHEDULED_RUN_KIND,
                                    record_skip=record_skip)
            return ran

        # This finite queue job retries a busy run lock. The leader service
        # remains free to check slates and recording handoffs meanwhile.
        import threading
        implementation._run_scheduled_slot(plugin, threading.Event(), settings, slot, times, tz,
                                            run_pipeline=scheduled)
    elif action in PIPELINE_ACTIONS:
        kind = 'm3u_rematch' if settings.pop('_m3u_rematch', False) else action
        ran, result = _pipeline(action, settings, kind)
        if kind == 'm3u_rematch':
            implementation._finish_m3u_rematch(ran, result)
    else:
        handler = implementation._ACTION_HANDLERS.get(action)
        result = handler(settings) if handler else {
            'status': 'error', 'message': f'Unknown action: {action!r}',
        }
    report(100, result.get('message') or str(result.get('status')))
    return result


def serve(plugin, context, stop_event):
    from . import plugin as implementation, slate
    dispatch = context['dispatch_scheduled_task']
    get_settings = context['get_settings']
    last_slot = None
    next_maintenance = 0
    implementation.logger.info('[ranked_matchups] leader-managed scheduler started (pid=%s)',
                               implementation.os.getpid())
    try:
        while not stop_event.is_set():
            try:
                settings = get_settings()
                now = datetime.now(timezone.utc)
                if settings.get('auto_refresh_enabled', False):
                    tz = implementation._resolve_tz(settings.get('local_timezone', 'UTC'))
                    times = implementation._parse_scheduled_times(
                        settings.get('scheduled_times', implementation._DEFAULT_SCHEDULED_TIMES))
                    slot = implementation._previous_fire_time(times, tz, now)
                    if (slot is not None and slot != last_slot
                            and now <= implementation._scheduled_retry_deadline(slot, times, tz)):
                        dispatch('auto_pipeline', {SLOT_PARAM: slot.isoformat()})
                        last_slot = slot
                if stop_event.is_set():
                    break
                # Uses per-channel Redis locks/cooldowns, including across the
                # lease overlap window. No listener or new HTTP server is bound.
                slate.check(implementation, settings, scheduled=True)
                if stop_event.is_set():
                    break
                implementation._run_due_stops(settings)
                if time.monotonic() >= next_maintenance:
                    tasks.health.report_refresh(settings)

                    def rematch(current):
                        dispatch('auto_pipeline', {'_m3u_rematch': True})
                        return None  # completion, including lock contention, owns the cooldown

                    implementation._maybe_rematch_after_m3u(settings, run_pipeline=rematch)
                    if (implementation._reap_settings(settings) > 0
                            and implementation._next_reap_delay(settings) <= 60):
                        dispatch('reap_now', {})
                    next_maintenance = time.monotonic() + 60
            except Exception:
                implementation.logger.exception('[ranked_matchups] managed scheduler tick failed')
            finally:
                implementation._scheduler_close_db()
            stop_event.wait(slate.INTERVAL)
    finally:
        implementation._scheduler_close_db()
        implementation.logger.info('[ranked_matchups] leader-managed scheduler stopped')
