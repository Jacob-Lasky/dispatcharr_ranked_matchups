"""Stop only our running DVR captures whose exact ESPN fixture is final.

EPG windows are conservative fallbacks, not game clocks. Never interpret a
missing, postponed or cancelled fixture as final. Network I/O precedes row
locks; the recording identity is checked again before revoking its DVR task.
"""
from datetime import datetime, timedelta, timezone
import logging
import re
from zoneinfo import ZoneInfo

import requests

from . import _util, recording
from .sources._espn import scoreboard_day_url

logger = logging.getLogger(__name__)
TARGET_KEY = 'ranked_matchups_final_target'
INTERVAL = 60
# Only documented ESPN site-api leagues. Uncovered sources retain their DVR
# window. CBB ids belong to CollegeBasketballData, so they require exact teams
# AND kickoff, never an ESPN event-id lookup using the cbb id.
LEAGUES = {
    'CBB': 'basketball/mens-college-basketball',
    'NCAAW': 'basketball/womens-college-basketball',
    'CFB': 'football/college-football', 'NFL': 'football/nfl',
    'NBA': 'basketball/nba', 'WNBA': 'basketball/wnba',
    'NHL': 'hockey/nhl', 'MLB': 'baseball/mlb',
    'NCAAMSOC': 'soccer/usa.ncaa.m.1', 'NCAAWSOC': 'soccer/usa.ncaa.w.1',
}


def target(game, marker):
    league = str(game.get('sport_prefix', '')).upper()
    kickoff = _util.parse_iso_utc(game.get('start_time_utc'))
    home, away = game.get('home'), game.get('away')
    if league not in LEAGUES or kickoff is None or not home or not away:
        return None
    extra = game.get('extra') or {}
    event_id = extra.get('espn_event_id')
    # CFBD's game id is ESPN's event id; unlike CBB's separate id namespace.
    if league == 'CFB':
        event_id = extra.get('cfbd_id')
    suffix = marker.rsplit(':', 1)[-1]
    tags = {'NFL': 'nfl_', 'NBA': 'nba_', 'WNBA': 'wnba_', 'NCAAW': 'ncaaw_'}
    tag = tags.get(league)
    if not event_id and tag and suffix.startswith(tag):
        event_id = suffix.removeprefix(tag)
    if not event_id and suffix.startswith('espn_'):
        event_id = suffix.removeprefix('espn_')
    if not event_id and league == 'CFB' and suffix.isdigit():
        event_id = suffix
    return {'marker': marker, 'league': league, 'home': home, 'away': away,
            'kickoff': kickoff.isoformat(), 'event_id': str(event_id) if event_id else None}


def _name(value):
    return re.sub(r'[^a-z0-9]', '', str(value).casefold())


def matching_final(wanted, events):
    """One exact fixture, with an affirmative played-to-final status.

    Exact kickoff also rejects an old final when this event was rescheduled.
    No fuzzy team matching: uncertainty keeps the file recording to its window.
    """
    matches = []
    for event in events:
        if not isinstance(event, dict) or not event.get('id'):
            continue
        if wanted.get('event_id') and str(event['id']) != wanted['event_id']:
            continue
        if _util.parse_iso_utc(event.get('date')) != _util.parse_iso_utc(wanted.get('kickoff')):
            continue
        competitions = event.get('competitions') or []
        if len(competitions) != 1:
            continue
        competitors = competitions[0].get('competitors') or []
        sides = {c.get('homeAway'): c.get('team') or {} for c in competitors}
        if len(competitors) != 2 or set(sides) != {'home', 'away'}:
            continue
        if not all(_name(wanted[side]) in {
                _name(sides[side].get(key, ''))
                for key in ('location', 'displayName', 'shortDisplayName')}
                for side in ('home', 'away')):
            continue
        matches.append(event)
    if len(matches) != 1:
        return None
    event = matches[0]
    status = (event.get('status') or {}).get('type') or {}
    if (status.get('state') == 'post' and status.get('completed') is True
            and status.get('name') in {'STATUS_FINAL', 'STATUS_FULL_TIME', 'STATUS_FINAL_OVERTIME',
                                       'STATUS_FINAL_PEN', 'STATUS_FINAL_AET'}):
        return event
    return None


def _events(wanted):
    kickoff = _util.parse_iso_utc(wanted['kickoff'])
    day = kickoff.astimezone(ZoneInfo('America/New_York')).date()
    league = wanted['league']
    url = scoreboard_day_url(
        'https://site.api.espn.com/apis/site/v2/sports/' + LEAGUES[league] + '/scoreboard', day)
    # ESPN defaults to ranked teams in college basketball; NC State's game
    # was absent from that response and present with groups=50 (W332).
    if league in {'CBB', 'NCAAW'}:
        url += '&groups=50'
    payload = _util.http_get_json(requests, url, log=logger, tag='recording-final', timeout=15)
    return (payload or {}).get('events') or []


def _identity(rec):
    return (recording.rec_marker(rec), rec.channel_id, rec.start_time, rec.end_time,
            (rec.custom_properties or {}).get(TARGET_KEY))


def _managed(rec, state):
    entry = state.get('created', {}).get(recording.rec_marker(rec))
    return (isinstance(entry, dict) and str(entry.get('id')) == str(rec.pk)
            and 'start' in entry and not recording.was_edited(rec, entry))


def _finish_stop(rid, identity):
    """Let run_recording interrupt ffmpeg and remux before transport teardown."""
    from apps.channels.models import Recording
    try:
        current = Recording.objects.filter(pk=rid).first()
        if (current is None or _identity(current) != identity
                or recording.rec_status(current) != 'stopped'):
            return
        from apps.channels.signals import revoke_task
        # Native revocation removes a queued task without terminating an active
        # worker. Its recorder polls our committed stopped flag every 2 seconds,
        # sends SIGINT to ffmpeg (as at scheduled end), then finalizes the MKV.
        # RecordingViewSet.stop disconnects proxy clients FIRST, causing a 503
        # reconnect and unexpected_exit retry before that poll can run (W332).
        revoke_task(current.task_id)
    except Exception as exc:
        # The committed flag still tells run_recording to interrupt ffmpeg
        # and retain the file even if task revocation fails.
        logger.warning('[ranked_matchups] task revocation for recording %s failed (%s)',
                       rid, type(exc).__name__)


def check(plugin, settings, *, now=None, scheduled=False):
    """Same guarded path for the minute timer and the authenticated action.

    The Redis lease/cooldown is independent of the long pipeline lock: a
    simulation must never delay final-status checks. Manual checks bypass only
    the cooldown. Row locks make concurrent/manual calls idempotent too.
    """
    if settings.get('dry_run'):
        return 0
    from apps.channels.models import Recording
    from django.db import transaction
    now = now or datetime.now(timezone.utc)
    lease = None
    if scheduled:
        from core.utils import RedisClient
        redis = RedisClient.get_client()
        lease = redis.lock('ranked_matchups:recording-finals', timeout=180, blocking=False)
        if not lease.acquire():
            return 0
        if not redis.set('ranked_matchups:recording-finals:checked', '1', nx=True, ex=INTERVAL):
            lease.release()
            return 0
    try:
        rows = Recording.objects.filter(
            custom_properties__has_key=recording.MARKER_KEY,
            custom_properties__status=recording.STATUS_RECORDING).select_related('channel')
        if settings.get('recording_id'):
            rows = rows.filter(pk=int(settings['recording_id']))
        state = recording.load_state(plugin.RECORDING_STATE_PATH)
        games = None
        responses = {}
        stopped = 0
        for rec in rows:
            marker = recording.rec_marker(rec)
            if not marker or not recording.is_in_progress(rec) or not _managed(rec, state):
                continue
            wanted = (rec.custom_properties or {}).get(TARGET_KEY)
            if wanted is None:
                # Upgrade coverage for existing recordings, including 60/61/63.
                # ProgramData carries the same full row if it has left cache.
                if games is None:
                    cache = plugin._read_cache()
                    games = {plugin._build_marker_key(g): g
                             for g in (cache.get('games') or []) + (cache.get('bench') or [])}
                game = games.get(marker)
                if game is None and getattr(rec, 'channel', None) is not None:
                    from apps.epg.models import ProgramData
                    for programme in ProgramData.objects.filter(epg_id=rec.channel.epg_data_id):
                        row = (programme.custom_properties or {}).get('ranked_matchups_row')
                        if isinstance(row, dict) and plugin._build_marker_key(row) == marker:
                            game = row
                            break
                wanted = target(game or {}, marker)
                original = _util.parse_iso_utc(
                    ((rec.custom_properties or {}).get('program') or {}).get('start_time'))
                # Legacy captures stored the original Live-block start even
                # after an empty-pregame retry. Do not bind a moved fixture to
                # a recording still targeting its old window.
                if (original is None or (wanted is not None
                        and _util.parse_iso_utc(wanted['kickoff']) != original + timedelta(minutes=30))):
                    continue
            if not isinstance(wanted, dict) or wanted.get('marker') != marker:
                continue
            kickoff = _util.parse_iso_utc(wanted.get('kickoff'))
            if (wanted.get('league') not in LEAGUES or kickoff is None or now < kickoff
                    or not (rec.start_time - recording.STOP_GRACE <= kickoff < rec.end_time)):
                continue
            key = (wanted['league'], kickoff.astimezone(ZoneInfo('America/New_York')).date())
            try:
                if key not in responses:
                    responses[key] = _events(wanted)
                event = matching_final(wanted, responses[key])
                if event is None:
                    continue
                identity = _identity(rec)
                with transaction.atomic():
                    current = Recording.objects.select_for_update().filter(pk=rec.pk).first()
                    if (current is None or _identity(current) != identity
                            or not recording.is_in_progress(current)
                            or not _managed(current, recording.load_state(plugin.RECORDING_STATE_PATH))):
                        continue
                    # Publish the native stopped flag before revocation. The
                    # active recorder must close ffmpeg itself, while its proxy
                    # connection is still healthy, exactly as at scheduled end.
                    current.custom_properties = recording.stopped_properties(
                        current.custom_properties, now)
                    current.save(update_fields=['custom_properties'])
                    transaction.on_commit(
                        lambda rid=current.pk, expected=identity: _finish_stop(rid, expected))
                stopped += 1
                logger.info('[ranked_matchups] ESPN final %s: requested graceful stop for recording %s (partial file kept)',
                            event['id'], rec.pk)
            except Exception as exc:
                logger.warning('[ranked_matchups] final check for recording %s failed (%s)',
                               rec.pk, type(exc).__name__)
        return stopped
    finally:
        if lease is not None:
            lease.release()
