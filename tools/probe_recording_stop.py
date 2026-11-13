"""Isolated W332 stop probe: real core recorder, ffmpeg, HLS and MKV remux.

Run ONLY in a disposable container with --network none, a read-only plugin
mount, and no production data mounts. Pass tasks.py and api_views.py copied
from the target Dispatcharr image; optionally pass a deployed plugin directory
as the third argument. ORM, Redis, Celery and notifications are
in-memory fakes; the capture loop, Stop method, ffmpeg arguments, SIGINT and
remux functions come from those actual core sources. The loopback TS proxy
models client teardown by disconnecting and then returning HTTP 503.
"""
import ast
import copy
import importlib
import io
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
from collections import deque
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def module(name, **values):
    result = types.ModuleType(name)
    result.__dict__.update(values)
    sys.modules[name] = result
    return result


def load_functions(path, env):
    tree = ast.parse(Path(path).read_text())
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    for node in functions:
        node.decorator_list = []
    code = ast.Module(body=functions, type_ignores=[])
    # Core annotations can name models that this isolated process never loads.
    import __future__
    exec(compile(code, str(path), 'exec', flags=__future__.annotations.compiler_flag), env)


def main():
    tasks_path, views_path = sys.argv[1:3]
    plugin_path = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).resolve().parents[1]
    logs = io.StringIO()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s',
                        handlers=[logging.StreamHandler(logs), logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger('core_recorder')
    now = datetime.now(timezone.utc)
    marker = 'ranked_matchups:NCAAMSOC:espn_401929736'
    poll = threading.Event()
    lock = threading.RLock()
    stored = {'id': 987654332, 'channel_id': 987654332, 'start_time': now-timedelta(minutes=30),
              'end_time': now+timedelta(hours=3), 'task_id': 'sandbox-dvr-987654332',
              'custom_properties': {'status': 'scheduled', 'ranked_matchups_marker': marker,
                                    'program': {'title': 'Isolated final replay'}}}

    class Row:
        def __init__(self):
            self.refresh_from_db()
        @property
        def pk(self):
            return self.id
        @property
        def channel(self):
            return channel
        def refresh_from_db(self):
            with lock:
                self.__dict__.update(copy.deepcopy(stored))
        def save(self, update_fields=None):
            with lock:
                for key in update_fields or stored:
                    stored[key] = copy.deepcopy(getattr(self, key))

    class Rows:
        def filter(self, **kw):
            return self
        def only(self, *fields):
            if fields == ('custom_properties', 'end_time'):
                poll.set()
            return self
        def select_related(self, *args):
            return self
        def select_for_update(self):
            return self
        def first(self):
            return Row()
        def get(self, **kw):
            return Row()
        def exists(self):
            return True
        def __iter__(self):
            return iter([Row()])

    recording = types.SimpleNamespace(objects=Rows())
    channel = types.SimpleNamespace(id=987654332, uuid='sandbox-987654332', name='Sandbox',
        logo_id=None, get_stream_profile=lambda: types.SimpleNamespace(is_redirect=lambda: False))
    module('apps', __path__=[])
    module('apps.channels', __path__=[])
    module('apps.channels.models', Recording=recording, Logo=object)
    revoked = []
    module('apps.channels.signals', revoke_task=revoked.append,
           _delete_periodic_task_named=lambda name: None, _dvr_task_name=lambda rid: str(rid))
    module('core', __path__=[])
    redis = types.SimpleNamespace(set=lambda *a, **k: True, expire=lambda *a: None,
        exists=lambda *a: False, delete=lambda *a: None, hgetall=lambda *a: {})
    module('core.utils', RedisClient=types.SimpleNamespace(get_client=lambda **kw: redis),
           send_websocket_update=lambda *a, **k: None, log_system_event=lambda *a, **k: None,
           dispatcharr_dvr_user_agent=lambda rid: f'Dispatcharr-DVR/recording-{rid}')
    from django.conf import settings
    settings.configure(USE_TZ=True, TIME_ZONE='UTC')
    from django.db import transaction
    transaction.atomic = nullcontext
    transaction.on_commit = lambda callback: callback()

    with tempfile.TemporaryDirectory(prefix='recording-stop-') as tmp:
        root = Path(tmp)
        fixture = root/'fixture.ts'
        subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i',
                        'testsrc2=size=160x90:rate=25', '-t', '60', '-c:v', 'mpeg2video',
                        '-g', '25', '-f', 'mpegts', str(fixture)], check=True)
        body = fixture.read_bytes()
        disconnected = threading.Event()
        connections = []
        class Proxy(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                if disconnected.is_set():
                    logging.getLogger('django.request').error('Service Unavailable: %s', self.path)
                    self.send_error(503)
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'video/mp2t')
                self.end_headers()
                connections.append(self.connection)
                try:
                    while not disconnected.is_set():
                        for offset in range(0, len(body), 1316):
                            if disconnected.is_set():
                                return
                            self.wfile.write(body[offset:offset+1316])
                            self.wfile.flush()
                            time.sleep(.003)
                except (BrokenPipeError, ConnectionResetError):
                    pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        def disconnect(*args, **kw):
            disconnected.set()
            for conn in connections:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            return 1
        env = dict(__name__='apps.channels.sandbox', logger=logger, os=os, time=time,
                   threading=threading, subprocess=subprocess, signal=signal, shutil=shutil,
                   datetime=datetime, timedelta=timedelta, deque=deque, json=json,
                   OperationalError=RuntimeError, InterfaceError=RuntimeError,
                   _DB_RETRY_TRANSIENT=(RuntimeError,),
                   _DVR_SHUTTING_DOWN=False, _DVR_HLS_REMUX_TIMEOUT_SECONDS=30)
        load_functions(tasks_path, env)
        final = root/'final.mkv'
        hls = root/'hls'
        env.update(Channel=types.SimpleNamespace(objects=types.SimpleNamespace(get=lambda **k: channel)),
            CoreSettings=types.SimpleNamespace(get_dvr_output_profile_id=lambda: None,
                                              get_dvr_comskip_enabled=lambda: False),
            get_channel_layer=lambda: types.SimpleNamespace(group_send=lambda *a: None),
            async_to_sync=lambda f: f,
            _build_output_paths=lambda *a: (str(final), str(hls), final.name),
            _resolve_poster_for_program=lambda *a, **kw: (None, None),
            get_dvr_stream_base_url=lambda: f'http://127.0.0.1:{server.server_port}',
            _dvr_emit_recording_end=lambda *a, **kw: None)
        # The OLD plugin calls this actual Stop method, which tears down before
        # the recorder polls its committed status. No transport is mocked away.
        stop = next(n for n in ast.walk(ast.parse(Path(views_path).read_text()))
                    if isinstance(n, ast.FunctionDef) and n.name == 'stop')
        stop.decorator_list = []
        stop_env = dict(logger=logger, threading=threading, _stop_dvr_clients=disconnect,
            timezone=types.SimpleNamespace(now=lambda: datetime.now(timezone.utc)),
            Response=lambda data, **kw: types.SimpleNamespace(status_code=200))
        exec(compile(ast.Module(body=[stop], type_ignores=[]), views_path, 'exec'), stop_env)
        view = type('RecordingViewSet', (), {'stop': stop_env['stop']})
        module('apps.channels.api_views', RecordingViewSet=view)
        package = module('dispatcharr_ranked_matchups', __path__=[str(plugin_path)])
        finals = importlib.import_module(package.__name__+'.recording_finals')
        game = {'sport_prefix': 'NCAAMSOC', 'home': 'North Carolina', 'away': 'NC State',
                'start_time_utc': now.isoformat()}
        state_path = root/'state.json'
        state_path.write_text(json.dumps({'created': {marker: finals.recording.written_record(
            stored['id'], stored['start_time'], stored['end_time'], stored['channel_id'], now)}}))
        plugin = types.SimpleNamespace(RECORDING_STATE_PATH=str(state_path))
        stored['custom_properties'][finals.TARGET_KEY] = finals.target(game, marker)
        event = {'id': '401929736', 'date': now.isoformat(),
                 'status': {'type': {'state': 'post', 'completed': True, 'name': 'STATUS_FINAL'}},
                 'competitions': [{'competitors': [
                     {'homeAway': 'home', 'team': {'location': game['home']}},
                     {'homeAway': 'away', 'team': {'location': game['away']}}]}]}
        finals._events = lambda wanted: [event]
        errors = []
        def capture():
            try:
                env['run_recording'](stored['id'], channel.id, stored['start_time'].isoformat(),
                                     stored['end_time'].isoformat())
            except Exception as exc:
                errors.append(exc)
                logger.exception('Recorder probe crashed')
        thread = threading.Thread(target=capture)
        try:
            thread.start()
            assert poll.wait(25), 'native recorder never reached its live stop poll'
            assert not errors, errors
            time.sleep(.05)  # put Stop just after a poll, exposing the old race
            assert finals.check(plugin, {}, now=datetime.now(timezone.utc)) == 1
            thread.join(35)
            assert not thread.is_alive(), 'recorder did not finish within the stop/remux bound'
            assert not errors, errors
            assert stored['custom_properties']['remux_success'] is True, stored
            assert final.stat().st_size > 0
            probe = json.loads(subprocess.check_output(['ffprobe', '-v', 'error', '-show_format',
                                                       '-show_streams', '-of', 'json', str(final)]))
            assert probe['streams'] and float(probe['format']['duration']) > 0
            text = logs.getvalue()
            error_lines = [line for line in text.splitlines() if line.startswith('ERROR')]
            result = {'status': stored['custom_properties']['status'], 'bytes': final.stat().st_size,
                'duration': probe['format']['duration'], 'errors': error_lines,
                'unexpected_exit_retry': 'reason=unexpected_exit' in text,
                'proxy_disconnected_by_plugin': disconnected.is_set(), 'revoked': revoked}
            print('RESULT', json.dumps(result), flush=True)
            assert not error_lines, error_lines
            assert not result['unexpected_exit_retry'], 'deliberate final entered outage retry path'
            assert not disconnected.is_set(), 'plugin disconnected the proxy before ffmpeg exited'
            assert revoked == [stored['task_id']]
        finally:
            # Only synthetic fixture files exist in this disposable container.
            if thread.is_alive():
                with lock:
                    stored['custom_properties']['status'] = 'stopped'
                thread.join(15)
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
