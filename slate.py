"""Matchup outage slates, served only inside Dispatcharr's network namespace.

Provider membership and order remain authoritative. A slate is admitted only
when every real feed has failed a bounded video probe; missing capacity is
UNKNOWN, not failure. Recovery uses the proxy's confirmed change-stream path,
so viewers and the DVR keep their original connection and output segments.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import select
import shutil
import signal
import sys
import tempfile
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
ROUTE = 'plugins/ranked-matchups/slate'
INTERVAL = 10
PROBE_SECONDS = 6
OWNER = 'ranked_matchups_slate'
def cache_dir(data_dir=None):
    """Persistent data root; v3 can pass context['data_dir'] here directly."""
    return Path(data_dir or Path(__file__).resolve().parent) / 'slates'


class _Process:
    """posix_spawn avoids gevent/uWSGI's hanging fork handlers (0.33.0)."""
    def __init__(self, args, output):
        with open(os.devnull, 'rb+') as null:
            self.pid = os.posix_spawn(shutil.which(args[0]) or args[0], args, os.environ,
                file_actions=[(os.POSIX_SPAWN_DUP2, null.fileno(), 0),
                              (os.POSIX_SPAWN_DUP2, output, 1),
                              (os.POSIX_SPAWN_DUP2, null.fileno(), 2)])
        self.code = None

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        while self.code is None:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.code = os.waitstatus_to_exitcode(status)
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('ffmpeg deadline exceeded')
            time.sleep(0.05)
        return self.code

    def close(self):
        if self.code is None:
            try:
                os.kill(self.pid, signal.SIGTERM)
                self.wait(2)
            except TimeoutError:
                os.kill(self.pid, signal.SIGKILL)
                self.wait(2)
            except ProcessLookupError:
                self.wait(2)


def _run(args, timeout):
    with tempfile.TemporaryFile() as output:
        child = _Process(args, output.fileno())
        try:
            code = child.wait(timeout)
            output.seek(0)
            return code, output.read(1024)
        finally:
            child.close()


def _key(channel):
    return str(channel.uuid)


def _owned(stream, channel):
    return (stream.custom_properties or {}).get(OWNER) == _key(channel)


def caption(game, now):
    from ._util import parse_iso_utc
    start = parse_iso_utc(game.get('start_time_utc'))
    title = f"{game.get('away', '')} vs {game.get('home', '')}"
    if start and now < start:
        local = start.astimezone(ZoneInfo('America/New_York'))
        status = f"Live at {local.strftime('%I:%M %p').lstrip('0')} ET"
    else:
        status = 'Feed unavailable, retrying'
    return '\n'.join(textwrap.wrap(title, width=34)[:3]), status


def _draw_card(spec, destination):
    from PIL import Image, ImageDraw, ImageFont, ImageOps
    width, height = 1920, 1080
    # A one-pixel gradient expanded in C keeps the background inexpensive.
    gradient = Image.new('RGB', (1, height))
    gradient.putdata([(int(20 - y * 9 / height), int(32 - y * 12 / height),
                       int(52 - y * 15 / height)) for y in range(height)])
    image = gradient.resize((width, height))
    draw = ImageDraw.Draw(image)
    font_path = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'
    def text(value, x, y, size, fill, max_width=740):
        font = ImageFont.truetype(font_path, size)
        while draw.textbbox((0, 0), value, font=font)[2] > max_width and size > 28:
            size -= 2
            font = ImageFont.truetype(font_path, size)
        draw.text((x, y), value, font=font, fill=fill, anchor='mm')
    draw.rounded_rectangle((96, 90, 1824, 990), radius=44, outline='#33465f', width=2)
    text((spec.get('league') or 'RANKED MATCHUPS').upper(), 960, 165, 28, '#91aacb', 1600)
    for side, name, x in (('away', spec['away'], 535), ('home', spec['home'], 1385)):
        box = (x - 165, 285, x + 165, 615)
        draw.ellipse(box, fill='#1e3049', outline='#456180', width=3)
        crop = None
        try:
            with Image.open((spec.get('logos') or {}).get(side) or '') as source:
                crop = source.convert('RGBA')
                bounds = crop.getchannel('A').getbbox()
                crop = crop.crop(bounds) if bounds else None
        except (OSError, ValueError, KeyError, TypeError):
            pass
        if crop is not None:
            # The bounding-box diagonal fits a 280px circle, leaving 25px
            # padding inside the 330px badge even for square/tall crests.
            ratio = 280 / math.hypot(crop.width, crop.height)
            crop = ImageOps.contain(crop, (max(1, round(crop.width * ratio)),
                                         max(1, round(crop.height * ratio))))
            image.paste(crop, (x - crop.width // 2, 450 - crop.height // 2), crop)
        else:
            initials = ''.join(part[0] for part in name.split() if part)[:3].upper() or '?'
            text(initials, x, 450, 94, '#e6f1ff', 285)
        lines = textwrap.wrap(name, width=24) or ['Team']
        if len(lines) > 2:
            lines = [lines[0], lines[1][:-1] + '…']
        for i, line in enumerate(lines):
            text(line, x, 690 + i * 58 - (len(lines) - 1) * 29, 50, '#f2f6fc')
    text('VS', 960, 455, 56, '#7995b8', 180)
    draw.line((390, 805, 1530, 805), fill='#33465f', width=2)
    text(spec['status'], 960, 870, 52, '#a8d2ff', 1600)
    image.save(destination)


def media_format(streams):
    """Use the first ranked provider's known format; constrain encoder inputs."""
    stats = next((s.stream_stats for s in streams if getattr(s, 'stream_stats', None)), {})
    video = {'h264': 'libx264', 'hevc': 'libx265', 'mpeg2video': 'mpeg2video'}.get(stats.get('video_codec'), 'libx264')
    audio = stats.get('audio_codec', 'ac3')
    if audio not in {'aac', 'ac3', 'eac3', 'mp2'}:
        audio = 'ac3'
    layout = str(stats.get('audio_channels', 'stereo'))
    layout = {'1': 'mono', '2': 'stereo', '6': '5.1', '8': '7.1'}.get(layout, layout)
    if layout not in {'mono', 'stereo', '5.1', '5.1(side)', '7.1'}:
        layout = 'stereo'
    match = re.fullmatch(r'(\d+)x(\d+)', str(stats.get('resolution', '1920x1080')))
    width, height = map(int, match.groups()) if match else (1920, 1080)
    if not (320 <= width <= 3840 and 240 <= height <= 2160):
        width, height = 1920, 1080
    rate = stats.get('sample_rate', 48000)
    if rate not in {32000, 44100, 48000}:
        rate = 48000
    return {'video': video, 'audio': audio, 'layout': layout, 'rate': rate,
            'width': width // 2 * 2, 'height': height // 2 * 2}


def artwork_spec(game, now, streams=(), settings=None):
    """The production card specification, also used by artwork proof tools."""
    from . import logos, plugin
    settings = settings or {}
    _, status = caption(game, now)
    badges, reasons = {}, {}
    key = plugin._resolve_key(settings, 'sportsdb_api_key', plugin.SPORTSDB_KEY_PATH) or plugin.SPORTSDB_DEFAULT_KEY
    for side in ('away', 'home'):
        if settings.get('enable_matchup_logos', True):
            badges[side], reasons[side] = logos.cached_team_badge(
                game.get(side, ''), game.get('sport_prefix', ''), str(cache_dir() / 'team-logos'), key)
        else:
            badges[side], reasons[side] = None, 'matchup logos disabled'
    return {'card_version': 3, 'home': game.get('home', ''), 'away': game.get('away', ''),
            'league': game.get('sport_label') or game.get('sport_prefix', ''),
            'status': status, 'logos': badges, 'logo_reasons': reasons, 'media': media_format(streams)}


def render(game, now, streams=(), settings=None):
    from . import plugin
    spec = artwork_spec(game, now, streams, settings)
    # The cache changes only for the matchup or status, never on a monitor tick.
    digest = hashlib.sha256(json.dumps({k:v for k,v in spec.items()
        if k != 'logo_reasons'}, sort_keys=True).encode()).hexdigest()
    root = cache_dir()
    root.mkdir(parents=True, exist_ok=True)
    target = root / f'{digest}.mp4'
    if not target.exists():
        for side, path in spec['logos'].items():
            if not path:
                logger.info('[ranked_matchups] slate initials for %s: %s',
                            spec[side], spec['logo_reasons'][side])
        with tempfile.TemporaryDirectory(dir=root) as directory:
            work = Path(directory)
            (work / 'spec.json').write_text(json.dumps(spec), encoding='utf-8')
            code, _ = _run([sys.prefix + '/bin/python', __file__, 'draw',
                            str(work / 'spec.json'), str(work / 'card.png')], timeout=15)
            if code != 0:
                raise RuntimeError('Slate artwork rendering failed')
            media = spec['media']
            encoder = ['-preset', 'ultrafast', '-tune', 'stillimage', '-x264-params', 'repeat-headers=1:scenecut=0:keyint=5:min-keyint=5'] if media['video'] == 'libx264' else []
            if media['video'] == 'libx265':
                encoder = ['-preset', 'ultrafast', '-x265-params', 'pools=1:frame-threads=1:repeat-headers=1']
            code, _ = _run(['ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error',
                '-loop', '1', '-framerate', '5', '-i', str(work / 'card.png'),
                '-f', 'lavfi', '-i', f"anullsrc=r={media['rate']}:cl={media['layout']}",
                '-t', '4', '-vf', f"scale={media['width']}:{media['height']}",
                '-c:v', media['video'], '-threads', '1', *encoder, '-pix_fmt', 'yuv420p',
                '-g', '5', '-c:a', media['audio'], '-b:a', '192k',
                '-f', 'mp4', str(work / 'clip.mp4')], timeout=30)
            if code != 0:
                raise RuntimeError('Slate video rendering failed')
            os.replace(work / 'clip.mp4', target)
    end = plugin._game_end_utc(game)
    return f'http://127.0.0.1:5656/{ROUTE}/{int(end.timestamp())}/{digest}.ts'


def packets(clip, until):
    """A request-scoped remux; MP4 looping keeps timestamps advancing at 1x.

    Looping a short TS with ffmpeg -re slows below real time at every wrap,
    which makes Dispatcharr declare a healthy slate to be buffering.
    """
    code, info = _run(['ffprobe', '-v', 'error', '-show_entries',
        'stream=codec_type,codec_name,sample_rate,avg_frame_rate', '-of', 'json', str(clip)], 5)
    if code:
        raise RuntimeError('Cached slate probe failed')
    streams = json.loads(info)['streams']
    video = next(s for s in streams if s['codec_type'] == 'video')
    audio = next(s for s in streams if s['codec_type'] == 'audio')
    numerator, denominator = map(int, video['avg_frame_rate'].split('/'))
    samples = {'aac': 1024, 'ac3': 1536, 'eac3': 1536, 'mp2': 1152}[audio['codec_name']]
    # Demuxer loop padding is not always an integral video frame. Quantize
    # each output clock, otherwise 5fps can be reported as 45fps to Jellyfin,
    # which sizes its HLS GOP from that value and stalls startup for a minute.
    video_clock = f"N*{denominator}/({numerator}*TB)"
    audio_clock = f"N*{samples}/({int(audio['sample_rate'])}*TB)"
    read_fd, write_fd = os.pipe()
    child = _Process(['ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error',
        '-re', '-stream_loop', '-1', '-i', str(clip), '-c', 'copy',
        '-bsf:v', f'setts=pts={video_clock}:dts={video_clock}',
        '-bsf:a', f'setts=pts={audio_clock}:dts={audio_clock}',
        '-mpegts_flags', '+resend_headers+initial_discontinuity', '-flush_packets', '1', '-f', 'mpegts', 'pipe:1'], write_fd)
    os.close(write_fd)
    try:
        while time.time() < until:
            if not select.select([read_fd], [], [], 10)[0]:
                break
            data = os.read(read_fd, 188 * 7)
            if not data:
                break
            yield data
    finally:
        child.close()
        os.close(read_fd)


def view(request, until, digest):
    from django.http import HttpResponse, StreamingHttpResponse
    # No provider credentials or external listener. Nginx may forward the
    # originating address, so reject external forwarded callers as well.
    peer = request.META.get('REMOTE_ADDR', '')
    forwarded = request.META.get('HTTP_X_FORWARDED_FOR', peer).split(',')[0].strip()
    if peer not in {'127.0.0.1', '::1'} or forwarded not in {'127.0.0.1', '::1'}:
        return HttpResponse(status=403)
    if request.method != 'GET' or not re.fullmatch('[0-9a-f]{64}', digest):
        return HttpResponse(status=404)
    clip = cache_dir() / f'{digest}.mp4'
    if not clip.is_file() or until <= time.time():
        return HttpResponse(status=404)
    response = StreamingHttpResponse(packets(clip, until), content_type='video/mp2t')
    response['Cache-Control'] = 'no-store'
    response['X-Accel-Buffering'] = 'no'
    return response


def install_route():
    """0.33 has no plugin route hook: use its existing Django HTTP server.

    Update the callback on discovery without adding a server or daemon loop.
    This adapter is the only part v3's native plugin routing needs to replace.
    """
    from django.conf import settings
    if not settings.configured:
        return
    from django.urls import get_resolver, path, clear_url_caches
    patterns = get_resolver().url_patterns
    name = 'ranked-matchups-slate'
    for route in patterns:
        if getattr(route, 'name', None) == name:
            route.callback = view
            return
    patterns.insert(0, path(ROUTE + '/<int:until>/<str:digest>.ts', view, name=name))
    clear_url_caches()


def active_stream(channel):
    from core.utils import RedisClient
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    redis = RedisClient.get_client()
    data = redis.hgetall(RedisKeys.channel_metadata(_key(channel)))
    if not data:
        return None
    return {(k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v) for k, v in data.items()}


def probe(channel, stream):
    """True = decodable video, False = failed feed, None = no safe probe slot."""
    from apps.proxy.live_proxy.url_utils import get_stream_info_for_switch
    from apps.m3u.connection_pool import reserve_profile_slot, release_profile_slot
    from core.utils import RedisClient
    if not stream.m3u_account:
        return False
    redis = RedisClient.get_client()
    profiles = stream.m3u_account.profiles.filter(is_active=True).order_by('-is_default', 'pk')
    profile = next((p for p in profiles if reserve_profile_slot(p, redis)[0]), None)
    if profile is None:
        return None
    # This raw pool reservation belongs only to this probe. Publishing a user
    # preview key would let a concurrent preview reuse it and lose its slot
    # when the probe finishes. No preview/channel assignments are touched.
    try:
        info = get_stream_info_for_switch(_key(channel), stream.pk, profile.pk)
        if 'error' in info:
            return None
        try:
            code, data = _run(
                ['ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error',
                 '-threads', '1', '-rw_timeout', '4000000', '-user_agent', info['user_agent'],
                 '-i', info['url'], '-map', '0:v:0', '-frames:v', '1', '-vf', 'scale=16:16',
                 '-pix_fmt', 'gray', '-threads', '1', '-f', 'rawvideo', 'pipe:1'],
                timeout=PROBE_SECONDS)
            return code == 0 and len(data) == 256
        except TimeoutError:
            return False
    finally:
        release_profile_slot(profile.pk, redis)


def switch(channel, stream_id):
    from apps.proxy.live_proxy.services.channel_service import ChannelService
    result = ChannelService.change_stream_url(_key(channel), target_stream_id=stream_id)
    return result.get('status') != 'error' and result.get('success') is True


def _recent_real_data(channel, active, real_ids):
    if not active or active.get('state') != 'active' or str(active.get('stream_id')) not in {str(x) for x in real_ids}:
        return False
    from core.utils import RedisClient
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    last = RedisClient.get_client().get(RedisKeys.last_data(_key(channel)))
    try:
        return last is not None and 0 <= time.time() - float(last) < INTERVAL
    except (TypeError, ValueError):
        return False


def reconcile(plugin, channel, game, now, settings=None):
    from apps.channels.models import Channel, ChannelStream, Stream
    from apps.m3u.models import M3UAccount, CUSTOM_M3U_ACCOUNT_NAME
    from django.db import transaction

    links = list(channel.streams.all().order_by('channelstream__order'))
    real = [s for s in links if not _owned(s, channel)]
    slates = [s for s in links if _owned(s, channel)]
    end = plugin._game_end_utc(game)
    if end is None or now >= end:
        ChannelStream.objects.filter(channel=channel, stream_id__in=[s.pk for s in slates]).delete()
        return 'finished'
    active = active_stream(channel)
    current_slate = any(str(s.pk) == str((active or {}).get('stream_id')) for s in slates)
    if active and not current_slate:
        # Apply can replace ChannelStreams while a viewer still consumes the
        # old slate. Ownership survives detachment; membership alone does not.
        current = Stream.objects.filter(pk=active.get('stream_id')).first()
        current_slate = current is not None and _owned(current, channel)
    if _recent_real_data(channel, active, [s.pk for s in real]):
        live, unknown = next(s for s in real if str(s.pk) == str(active['stream_id'])), False
    else:
        live, unknown = None, False
        deadline = time.monotonic() + 120
        for stream in real:
            if time.monotonic() + PROBE_SECONDS > deadline:
                return 'unknown'  # never outlive the 180s coordination lock
            verdict = probe(channel, stream)
            if verdict is True:
                live = stream
                break
            unknown |= verdict is None
    if live:
        fresh_ids = {s.pk for s in channel.streams.all() if not _owned(s, channel)}
        if fresh_ids != {s.pk for s in real}:
            return 'changed'
        if current_slate and not switch(channel, live.pk):
            return 'switch_pending'
        # Only plugin-created slate links are removed. Provider links/order and
        # Channel.epg_data are never saved, so the destructive EPG signal is skipped.
        ChannelStream.objects.filter(channel=channel, stream_id__in=[s.pk for s in slates]).delete()
        return 'live'
    if unknown:
        return 'unknown'
    url = render(game, now, real, settings=settings)
    with transaction.atomic():
        # Serialize with apply/reap, and discard a probe if apply changed the
        # provider membership while it ran. No Channel.save(), no EPG signals.
        if not Channel.objects.select_for_update().filter(pk=channel.pk).exists():
            return 'removed'
        fresh = list(channel.streams.all())
        if {s.pk for s in fresh if not _owned(s, channel)} != {s.pk for s in real}:
            return 'changed'
        account, _ = M3UAccount.objects.get_or_create(name=CUSTOM_M3U_ACCOUNT_NAME)
        slate, _ = Stream.objects.get_or_create(
            tvg_id=f'{OWNER}:{_key(channel)}',
            defaults={'name': 'Matchup outage slate', 'url': url,
                      'm3u_account': account, 'custom_properties': {OWNER: _key(channel)}})
        if not _owned(slate, channel):
            raise ValueError('Slate ownership collision')
        if slate.url != url:
            Stream.objects.filter(pk=slate.pk).update(url=url)  # skip stream signals
        from django.db.models import F
        provider_links = ChannelStream.objects.filter(channel=channel).exclude(stream=slate)
        if provider_links.filter(order=0).exists():
            provider_links.update(order=F('order') + 1)  # retain relative real-feed ordering
        ChannelStream.objects.update_or_create(channel=channel, stream=slate, defaults={'order': 0})
    if active and (str(active.get('stream_id')) != str(slate.pk) or active.get('url') != url):
        if not switch(channel, slate.pk):
            return 'switch_pending'
    return 'placeholder'


def check(plugin, settings, scheduled=False):
    from apps.channels.models import Channel
    from core.utils import RedisClient
    from django.db import close_old_connections
    if settings.get('dry_run'):
        return {'status': 'ok', 'message': 'Dry run: no feed checks or slate writes.'}
    games = {plugin._build_marker_key(g): g for g in plugin._read_cache().get('games', [])}
    if settings.get('channel_id'):
        channels = Channel.objects.filter(pk=settings['channel_id'], tvg_id__in=games)
    else:
        channels = Channel.objects.filter(tvg_id__in=games)
    counts = {}
    redis = RedisClient.get_client()
    checked = 'ranked_matchups:slate:checked_at'
    # Cooldowns keep active channels eligible every 10s and idle ones every
    # 60s. Oldest-first across both classes prevents a slow active outage
    # from consuming every tick and starving an idle pre-tune slate.
    channels = sorted(channels, key=lambda ch: redis.zscore(checked, str(ch.pk)) or 0)
    started = time.monotonic()
    for channel in channels:
        if scheduled and time.monotonic() - started >= 20:
            break
        lock = redis.lock(f'ranked_matchups:slate:{channel.pk}', timeout=180, blocking=False)
        if not lock.acquire():
            continue
        try:
            cooldown = f'ranked_matchups:slate:checked:{channel.pk}'
            if scheduled and redis.exists(cooldown):
                continue
            result = reconcile(plugin, channel, games[channel.tvg_id], datetime.now(timezone.utc), settings)
            counts[result] = counts.get(result, 0) + 1
            redis.setex(cooldown, INTERVAL if active_stream(channel) else 60, '1')
            redis.zadd(checked, {str(channel.pk): time.time()})
            redis.expire(checked, 2 * 86400)
        except Exception as exc:
            # Exception text from transports can contain credential-bearing URLs.
            logger.warning('[ranked_matchups] slate channel %s failed (%s)', channel.pk, type(exc).__name__)
            counts['error'] = counts.get('error', 0) + 1
        finally:
            lock.release()
            close_old_connections()
    # The stream row belongs exclusively to this feature. Never collect a
    # provider stream, or an owned slate still linked to another channel.
    from apps.channels.models import Stream
    for orphan in Stream.objects.filter(tvg_id__startswith=OWNER + ':', channels__isnull=True,
                                        custom_properties__has_key=OWNER):
        owner = (orphan.custom_properties or {}).get(OWNER)
        if orphan.tvg_id != f'{OWNER}:{owner}':
            continue
        channel = Channel.objects.filter(uuid=owner).first()
        active = active_stream(channel) if channel else None
        if not active or str(active.get('stream_id')) != str(orphan.pk):
            orphan.delete()
    root = cache_dir()
    if root.is_dir():
        for clip in root.glob('*.mp4'):
            if re.fullmatch(r'[0-9a-f]{64}\.mp4', clip.name) and time.time() - clip.stat().st_mtime > 2 * 86400:
                clip.unlink(missing_ok=True)
    return {'status': 'error' if counts.get('error') else 'ok', 'feeds': counts,
            'message': f'Matchup feed check: {counts}'}


if __name__ == '__main__':
    if len(sys.argv) == 4 and sys.argv[1] == 'draw':
        _draw_card(json.loads(Path(sys.argv[2]).read_text()), sys.argv[3])
