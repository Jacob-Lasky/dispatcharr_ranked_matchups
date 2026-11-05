"""Optional push-monitor health, with no credentials in messages or logs.

Only completed pipelines send their heartbeat. The reaper's separate refresh
heartbeat must never keep a dead pipeline scheduler looking healthy.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ._util import parse_iso_utc, redact_secrets

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.health")
HEALTH_PUSH_URL_SETTING = "health_push_url"
REFRESH_HEALTH_PUSH_URL_SETTING = "refresh_health_push_url"
_PUSH_TIMEOUT_SECONDS = 5
_REFRESH_PUSH_COOLDOWN_SECONDS = 240
_REFRESH_GRACE_SECONDS = 25 * 60


def _safe_message(message):
    # An upstream exception may include a provider URL with credentials.
    message = re.sub(r"https?://\S+", "[URL redacted]", redact_secrets(message), flags=re.IGNORECASE)
    return " ".join(message.split())[:1000]


def push(url, problems, healthy_message):
    """Bounded, best-effort Kuma-compatible GET. Never log a URL/exception body."""
    if not url:
        return False
    status = "down" if problems else "up"
    message = _safe_message("; ".join(problems) if problems else healthy_message)
    try:
        import requests
        from ._http import get_within_deadline
        parts = urlsplit(str(url).strip())
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise ValueError("health push URL must be HTTP(S)")
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                 if k not in {"status", "msg", "ping"}]
        query.extend([("status", status), ("msg", message)])
        target = urlunsplit(parts._replace(query=urlencode(query), fragment=""))
        response = get_within_deadline(requests, target, timeout=_PUSH_TIMEOUT_SECONDS,
                                     allow_redirects=False)
        response.raise_for_status()
        if not 200 <= response.status_code < 300:
            raise ValueError("health endpoint returned a redirect")
        if "application/json" in response.headers.get("Content-Type", ""):
            body = response.json()
            if isinstance(body, dict) and body.get("ok") is False:
                raise ValueError("health endpoint rejected the heartbeat")
    except Exception as exc:
        logger.warning("[ranked_matchups.health] push failed (%s)", type(exc).__name__)
        return False
    logger.info("[ranked_matchups.health] %s: %s", status.upper(), message)
    return True


def _live_empty_channels(plugin, cache, now):
    """Read actual links and the published guide, including channels absent from cache."""
    from apps.channels.models import Channel, ChannelStream
    channels = list(Channel.objects.filter(plugin._owned_tvg_id_q())
                    .exclude(tvg_id=plugin.ARCHIVE_TVG_ID)
                    .exclude(pk__in=ChannelStream.objects.values("channel_id")))
    guide = plugin._published_guide(channels, now)
    live_markers = set()
    for row in cache.get("games") or []:
        start = parse_iso_utc(row.get("start_time_utc"))
        end = plugin._game_end_utc(row)
        if start is not None and end is not None and start <= now < end:
            live_markers.add(plugin._build_marker_key(row))
    return [f"{ch.id} {ch.name}" for ch in channels
            if ch.tvg_id in live_markers or plugin._guide_shows_on_air(guide[ch.id][1], now)]


def pipeline_problems(result, started, ceiling_seconds, elapsed, now=None):
    from . import plugin
    now = now or datetime.now(timezone.utc)
    problems = []
    if result.get("status") != "ok":
        problems.append(f"pipeline error: {result.get('message') or 'no result'}")
    if elapsed >= ceiling_seconds:
        problems.append(f"run exceeded {ceiling_seconds:g}s ceiling ({elapsed:.0f}s)")
    cache = plugin._read_cache()
    matched = plugin._match_snapshot_time(cache)
    latest = plugin._latest_m3u_refresh()
    if matched is None or matched < started or (latest is not None and matched < latest):
        problems.append("stale match cache: matching did not cover this run/latest M3U refresh")
    problems.extend(plugin._slate_problems(cache.get("summary") or [], cache.get("games") or []))
    empty = _live_empty_channels(plugin, cache, now)
    if empty:
        problems.append("live channel(s) with 0 streams: " + plugin._first_few(empty))
    return problems


class PipelineHealth:
    """One early warning and one final verdict, ordered even if the timer races finish."""

    def __init__(self, settings, started, ceiling_seconds):
        self.url = settings[HEALTH_PUSH_URL_SETTING]
        self.started = started
        self.ceiling = ceiling_seconds
        self.clock_started = time.monotonic()
        self.lock = threading.Lock()
        self.finished = False
        self.timer = threading.Timer(0.7 * ceiling_seconds, self.warn)
        self.timer.daemon = True
        self.timer.start()

    def warn(self):
        with self.lock:
            if not self.finished:
                push(self.url, [f"pipeline slow: run exceeded 70% of {self.ceiling:g}s ceiling "
                                f"({time.monotonic() - self.clock_started:.0f}s); still running"], "")

    def finish(self, result):
        with self.lock:
            self.finished = True
            self.timer.cancel()
            elapsed = time.monotonic() - self.clock_started
            try:
                problems = pipeline_problems(result, self.started, self.ceiling, elapsed)
            except Exception as exc:
                problems = [f"pipeline health check failed ({type(exc).__name__})"]
                if result.get("status") != "ok":
                    problems.insert(0, f"pipeline error: {result.get('message') or 'no result'}")
            push(self.url, problems, f"pipeline healthy; completed in {elapsed:.0f}s")


def refresh_problems(now=None):
    """Core refresh errors and overdue scheduled feeds; manual feeds have no age deadline."""
    from apps.m3u.models import CUSTOM_M3U_ACCOUNT_NAME, M3UAccount
    from apps.epg.models import EPGSource
    now = now or datetime.now(timezone.utc)
    problems = []
    groups = [("M3U", M3UAccount.objects.filter(is_active=True)
               .exclude(name=CUSTOM_M3U_ACCOUNT_NAME, locked=True)),
              ("EPG", EPGSource.objects.filter(is_active=True).exclude(source_type="dummy"))]
    for kind, sources in groups:
        for row in sources.values("id", "name", "status", "updated_at", "refresh_interval"):
            label = f"{kind} {row['id']} {row['name']}"
            if row["status"] in {"error", "pending_setup", "disabled"}:
                problems.append(f"{label}: refresh {row['status']}")
                continue
            hours = row["refresh_interval"] or 0
            if hours > 0:
                stamp = row["updated_at"]
                if stamp is None:
                    problems.append(f"{label}: refresh never completed")
                elif now - stamp > timedelta(hours=hours, seconds=_REFRESH_GRACE_SECONDS):
                    problems.append(f"{label}: refresh overdue ({row['status']}; last success {stamp.isoformat()})")
    return problems


def report_refresh(settings):
    """Elect one reaper per tick. No push on Redis failure: Kuma expires the heartbeat."""
    url = settings.get(REFRESH_HEALTH_PUSH_URL_SETTING)
    if not url:
        return
    from . import tasks
    redis = tasks._redis()
    try:
        if redis is None or not redis.set(f"plugins:{tasks.PLUGIN_KEY}:refresh_health", "1",
                                          nx=True, ex=_REFRESH_PUSH_COOLDOWN_SECONDS):
            return
    except Exception:
        return
    try:
        problems = refresh_problems()
    except Exception as exc:
        problems = [f"refresh health check failed ({type(exc).__name__})"]
    push(url, problems, "M3U and EPG refreshes healthy")
