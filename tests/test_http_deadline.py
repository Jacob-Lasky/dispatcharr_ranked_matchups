"""#271: every outbound HTTP response is bounded by a wall-clock deadline.

`timeout=` bounds the connect and each socket read, so a server that trickles
a byte at a time never trips it and one request could hold a refresh to the
25-minute pipeline kill. These tests run against a REAL local HTTP server (not
a mock of `requests`), because the defect lives in socket behaviour no stub
reproduces.

Witnesses (fail on the pre-#271 code, as assertions, because each call runs on
a thread the test joins with its own bound): a trickled body and trickled
headers both give up within deadline + slack, and a body cut at the deadline is
never parsed. Guards (pass on old and new code, and pin what must not regress):
fast and large responses come back whole, the response is closed, and no module
makes an HTTP call outside the shared helper.

Module attributes are looked up inside each test, so the witnesses still run
(and fail as assertions) against a `_util` that predates the helpers.
"""
from __future__ import annotations

import ast
import http.server
import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import requests

from dispatcharr_ranked_matchups import _util

REPO = Path(__file__).resolve().parent.parent

# The trickle cadence, and how long past the deadline a call may run before it
# counts as not bounded. The slack covers thread start-up and a loaded CI box;
# it is still far below what the old code takes (the whole trickle).
TRICKLE_INTERVAL_S = 0.2
TRICKLE_BYTES = 100  # 20s of trickle: long enough to outlast every bound below
DEADLINE_S = 0.8
SLACK_S = 1.5


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disconnects: "dict[str, threading.Event]" = {}

    def log_message(self, *args):  # keep pytest output clean
        pass

    def _trickle(self, data: bytes, key: str) -> None:
        for i in range(len(data)):
            try:
                self.wfile.write(data[i:i + 1])
                self.wfile.flush()
            except OSError:
                # The client closed the connection under us: that is the
                # behaviour the "closed" guard looks for.
                self.disconnects.setdefault(key, threading.Event()).set()
                return
            time.sleep(TRICKLE_INTERVAL_S)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's name
        route = self.path.lstrip("/")
        if route.startswith("trickle-body"):
            body = b"[" + b"1," * TRICKLE_BYTES + b"1]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._trickle(body, route)
        elif route.startswith("trickle-headers"):
            self.wfile.write(b"HTTP/1.1 200 OK\r\n")
            self._trickle(b"X-Slow: " + b"a" * TRICKLE_BYTES, route)
        elif route == "digits":
            # Any prefix of a run of digits is itself valid JSON (an int), so a
            # helper that parsed whatever had arrived at the deadline would
            # return a number here instead of failing.
            body = b"1" * TRICKLE_BYTES
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._trickle(body, route)
        elif route == "fast":
            self._send(json.dumps({"events": [1, 2, 3]}).encode())
        elif route == "large":
            self._send(json.dumps({"events": list(range(600_000))}).encode())
        elif route == "cut":
            # No Content-Length and a closed connection: the body ends where
            # the server stopped, half way through a JSON object.
            self.protocol_version = "HTTP/1.0"
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"events": [1, 2')
            self.close_connection = True
        elif route == "redirect-trickle":
            # urllib reads a 3xx body before following it: a slow one stalls
            # the worker before any final response exists.
            body = b"x" * TRICKLE_BYTES
            self.send_response(302)
            self.send_header("Location", "/fast")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._trickle(body, route)
        elif route == "missing":
            self.send_error(404)
        else:
            self.send_error(500)

    def _send(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            pass  # the oversized-body test hangs up after max_bytes, by design


@pytest.fixture(scope="module")
def server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    srv.daemon_threads = True
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def _bounded(fn, bound_s):
    """Run fn on a daemon thread; return (finished, elapsed, result, error).

    The bound is what keeps a regression from hanging the suite: the old code
    blocks for the whole 20s trickle, and this reports it as a failed
    assertion at `bound_s` instead."""
    box = {}

    def run():
        t0 = time.monotonic()
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 - reported to the test
            box["error"] = exc
        box["elapsed"] = time.monotonic() - t0

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(bound_s)
    return (not t.is_alive(), box.get("elapsed"), box.get("result"), box.get("error"))


def _get_json(url, timeout):
    return _util.http_get_json(
        requests, url, log=logging.getLogger("test.http_deadline"), tag="t", timeout=timeout,
    )


class TestWitnesses:
    """Each of these hung (for the whole trickle) before #271."""

    def test_trickled_body_gives_up_at_the_deadline(self, server, caplog):
        with caplog.at_level(logging.WARNING):
            finished, elapsed, result, error = _bounded(
                lambda: _get_json(f"{server}/trickle-body-a", DEADLINE_S), DEADLINE_S + SLACK_S,
            )
        assert finished, (
            f"http_get_json still reading a trickled body {DEADLINE_S + SLACK_S}s in: "
            "a per-read timeout is not a response deadline (#271)"
        )
        assert error is None and result is None
        assert elapsed >= DEADLINE_S * 0.9, "gave up before its deadline"
        assert any("within" in r.getMessage() for r in caplog.records), (
            "the deadline failure must be logged like any other feed failure"
        )

    def test_trickled_headers_give_up_at_the_deadline(self, server):
        """The status line and headers arrive before any body iterator exists,
        so a deadline checked between body chunks never sees this one."""
        finished, _elapsed, result, error = _bounded(
            lambda: _get_json(f"{server}/trickle-headers", DEADLINE_S), DEADLINE_S + SLACK_S,
        )
        assert finished, "http_get_json still reading trickled headers past its deadline"
        assert error is None and result is None

    def test_a_body_cut_at_the_deadline_is_not_parsed(self, server):
        finished, _elapsed, result, error = _bounded(
            lambda: _get_json(f"{server}/digits", DEADLINE_S), DEADLINE_S + SLACK_S,
        )
        assert finished, "http_get_json still reading past its deadline"
        assert error is None
        assert result is None, (
            f"the partial body was parsed as JSON ({result!r}); a response cut "
            "at the deadline must fail, not return its prefix"
        )


class TestGuards:
    def test_a_fast_response_parses(self, server):
        assert _get_json(f"{server}/fast", 5.0) == {"events": [1, 2, 3]}

    def test_a_large_fast_response_is_not_cut(self, server):
        out = _get_json(f"{server}/large", 10.0)
        assert out is not None and len(out["events"]) == 600_000
        assert out["events"][-1] == 599_999

    def test_an_error_status_is_still_none(self, server):
        assert _get_json(f"{server}/missing", 5.0) is None

    def test_a_connection_cut_mid_json_is_none(self, server, caplog):
        with caplog.at_level(logging.WARNING):
            assert _get_json(f"{server}/cut", 5.0) is None
        assert any("failed" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("route,call", [
        ("trickle-body-close", "requests"),
        ("trickle-headers-close", "requests"),
        ("trickle-body-close-urllib", "urllib"),
        ("trickle-headers-close-urllib", "urllib"),
        ("redirect-trickle", "urllib"),
    ])
    def test_the_connection_is_hung_up_at_the_deadline(self, server, route, call):
        """Giving up must also hang up, in every phase: an abandoned worker
        would otherwise keep draining the trickle (and a thread and a socket)
        for the rest of the run. The server sees the hang-up as a failed write
        on its next trickled byte."""
        _Handler.disconnects[route] = threading.Event()
        if call == "requests":
            fn = lambda: _get_json(f"{server}/{route}", DEADLINE_S)  # noqa: E731
        else:
            def fn():
                with pytest.raises(TimeoutError):
                    _util.urlopen_read(urllib.request.Request(f"{server}/{route}"), timeout=DEADLINE_S)
        finished, _e, _r, err = _bounded(fn, DEADLINE_S + SLACK_S)
        assert finished and err is None, err
        assert _Handler.disconnects[route].wait(SLACK_S + TRICKLE_INTERVAL_S * 5), (
            f"the server is still trickling into a connection the client gave up on ({route})"
        )

    def test_get_within_deadline_raises_the_callers_timeout(self, server):
        """Direct callers (odds, CFBD, CBB, Football-Data) catch
        RequestException; the deadline must arrive as one."""
        t0 = time.monotonic()
        with pytest.raises(requests.Timeout):
            _util.get_within_deadline(requests, f"{server}/trickle-body-b", timeout=DEADLINE_S)
        assert time.monotonic() - t0 < DEADLINE_S + SLACK_S

    def test_get_within_deadline_returns_a_full_response(self, server):
        r = _util.get_within_deadline(requests, f"{server}/fast", timeout=5.0)
        r.raise_for_status()
        assert r.status_code == 200 and r.json() == {"events": [1, 2, 3]}

    def test_urlopen_read_is_bounded(self, server):
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            _util.urlopen_read(
                urllib.request.Request(f"{server}/trickle-body-c"), timeout=DEADLINE_S,
            )
        assert time.monotonic() - t0 < DEADLINE_S + SLACK_S

    def test_urlopen_read_returns_status_and_body(self, server):
        status, body = _util.urlopen_read(urllib.request.Request(f"{server}/fast"), timeout=5.0)
        assert status == 200 and json.loads(body) == {"events": [1, 2, 3]}

    def test_urlopen_read_passes_http_errors_through(self, server):
        """gamethumbs reads Retry-After off the HTTPError, so it must arrive
        as itself, not wrapped."""
        with pytest.raises(urllib.error.HTTPError) as info:
            _util.urlopen_read(urllib.request.Request(f"{server}/missing"), timeout=5.0)
        assert info.value.code == 404

    def test_urlopen_read_refuses_an_oversized_body(self, server):
        with pytest.raises(ValueError):
            _util.urlopen_read(urllib.request.Request(f"{server}/large"), timeout=5.0, max_bytes=1024)
        status, body = _util.urlopen_read(
            urllib.request.Request(f"{server}/fast"), timeout=5.0, max_bytes=1024)
        assert status == 200 and body

    def test_expiry_hangs_up_every_tracked_connection(self):
        """Early and late hand-overs alike: a socket connected AFTER expiry (a
        slow redirect hop) is refused and hung up on the spot, or nothing ever
        would hang it up."""
        pairs = [socket.socketpair() for _ in range(2)]
        tracked = _util._WorkerConnections()
        tracked.track(pairs[0][0])
        tracked.expire()
        with pytest.raises(_util.ResponseDeadlineExceeded):
            tracked.track(pairs[1][0])  # refused (aborts its connect) AND hung up
        for ours, peer in pairs:
            peer.settimeout(2.0)
            assert peer.recv(1) == b"", "the peer did not see the connection shut down"
            ours.close()
            peer.close()

    def test_release_closes_only_the_duplicates(self):
        """A finished call must not shut down a socket the library still owns
        (requests may hand it back to a pool), and must not leak the dup."""
        ours, peer = socket.socketpair()
        tracked = _util._WorkerConnections()
        tracked.track(ours)
        dup = tracked._open[0]
        tracked.release()
        assert dup.fileno() == -1, "the duplicate descriptor leaked"
        ours.sendall(b"x")
        peer.settimeout(2.0)
        assert peer.recv(1) == b"x"
        ours.close()
        peer.close()

    def test_only_deadline_workers_are_tracked(self, server):
        """The audit hook is process-wide; a connect on any other thread (the
        caller, Dispatcharr's own code) must pass through untouched."""
        _util._install_connect_hook()
        seen = []

        class Spy:
            def track(self, sock):
                seen.append(sock)

        _util._deadline_worker.connections = None
        requests.get(f"{server}/fast", timeout=5)
        assert seen == []
        _util._deadline_worker.connections = Spy()
        try:
            requests.get(f"{server}/fast", timeout=5)
        finally:
            _util._deadline_worker.connections = None
        assert len(seen) == 1

    def _connect_as_worker(self, connections, fn):
        """Run fn on a thread that the audit hook treats as a deadline worker."""
        _util._install_connect_hook()
        box = {}

        def run():
            _util._deadline_worker.connections = connections
            try:
                box["result"] = fn()
            except BaseException as exc:  # noqa: BLE001
                box["error"] = exc

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(10)
        return box

    def test_a_connection_that_cannot_be_tracked_is_not_made(self, server):
        """dup() failing (EMFILE) must not let an unbounded connection through."""
        class Broken:
            def track(self, sock):
                raise OSError("EMFILE")

        box = self._connect_as_worker(Broken(), lambda: requests.get(f"{server}/fast", timeout=5))
        assert isinstance(box.get("error"), requests.ConnectionError), box

    def test_a_connect_after_expiry_is_refused(self, server):
        """A late redirect hop must not open a connection nobody will hang up."""
        connections = _util._WorkerConnections()
        connections.expire()
        box = self._connect_as_worker(connections, lambda: requests.get(f"{server}/fast", timeout=5))
        assert isinstance(box.get("error"), requests.ConnectionError), box

    def test_a_socket_shut_down_before_it_connects_cannot_read(self, server):
        """Expiry can land between the audit event and the connect syscall.
        Pins the Linux behaviour that covers it: shutdown() on a not-yet-
        connected TCP socket returns ENOTCONN but still poisons it."""
        host, port = server.rsplit("/", 1)[-1].split(":")
        sock = socket.socket()
        sock.settimeout(5)
        connections = _util._WorkerConnections()
        connections.track(sock)
        connections.expire()
        sock.connect((host, int(port)))
        try:
            sock.sendall(b"GET /fast HTTP/1.1\r\nHost: x\r\n\r\n")
        except OSError:
            pass  # EPIPE is the other half of the same poisoning
        assert sock.recv(1) == b""
        sock.close()

    def test_the_hook_is_installed_once(self, monkeypatch):
        added = []
        monkeypatch.setattr(_util.sys, "addaudithook", added.append)
        monkeypatch.setattr(_util, "_connect_hook_installed", False)
        _util._install_connect_hook()
        _util._install_connect_hook()
        assert added == [_util._track_worker_connect]

    def test_no_descriptor_leaks_across_calls(self, server):
        fd_dir = Path("/proc/self/fd")
        if not fd_dir.is_dir():
            pytest.skip("needs /proc")
        _get_json(f"{server}/fast", 5.0)  # warm up: hook installed, imports done
        before = len(list(fd_dir.iterdir()))
        for _ in range(20):
            _get_json(f"{server}/fast", 5.0)
            _util.urlopen_read(urllib.request.Request(f"{server}/fast"), timeout=5.0)
        assert len(list(fd_dir.iterdir())) <= before + 2

    def test_worker_errors_reach_the_caller(self):
        def work():
            raise KeyError("boom")

        with pytest.raises(KeyError):
            _util.run_with_deadline(work, 5.0)

    @pytest.mark.parametrize("bad", [None, 0, -1])
    def test_a_missing_deadline_is_refused(self, bad):
        with pytest.raises(ValueError):
            _util.run_with_deadline(lambda: None, bad)


# ---------- class guard: no HTTP call outside the shared helper ----------

# Names on the `requests` module that open a connection. Flagged on any
# REFERENCE, not only a call: `get = requests.get; get(url)` and
# `requests.api.get(url)` make the same unbounded request.
_REQUESTS_NAMES = {"get", "post", "put", "patch", "delete", "head", "options", "request",
                   "Session", "session", "api", "sessions"}
# Names in the stdlib that open a connection, wherever they are referenced.
_STDLIB_NAMES = {"urlopen", "build_opener", "OpenerDirector", "HTTPConnection", "HTTPSConnection"}
# Files allowed to make the raw call, each with its reason.
_ALLOWED = {
    # The helper itself: run_with_deadline is what bounds these calls, and its
    # requests calls go through the CALLER'S module (`requests_mod`), never a
    # `requests` import, so only the urlopen reference needs the exemption.
    "_util.py",
}


def _raw_http_calls(source: str, filename: str = "<src>") -> list:
    """Lines in `source` that reach an HTTP client without the deadline helper."""
    tree = ast.parse(source, filename=filename)
    requests_names = {"requests"}
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name == "requests" and a.asname:
                    requests_names.add(a.asname)
                if a.name.startswith("requests."):
                    hits.append(node.lineno)  # import requests.api / requests.sessions
        elif isinstance(node, ast.ImportFrom):
            names = {a.name for a in node.names}
            mod = node.module or ""
            if mod == "requests" and names & _REQUESTS_NAMES:
                hits.append(node.lineno)
            if mod.startswith("requests."):
                hits.append(node.lineno)
            if names & _STDLIB_NAMES:
                hits.append(node.lineno)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            base = node.value
            if isinstance(base, ast.Name) and base.id in requests_names and node.attr in _REQUESTS_NAMES:
                hits.append(node.lineno)
            elif node.attr in _STDLIB_NAMES:
                hits.append(node.lineno)
        elif isinstance(node, ast.Name) and node.id in _STDLIB_NAMES:
            hits.append(node.lineno)
    return sorted(set(hits))


def _plugin_modules():
    for path in sorted(REPO.rglob("*.py")):
        rel = path.relative_to(REPO)
        if rel.parts[0] == "tests":
            continue
        yield rel, path


class TestNoHttpCallBypassesTheDeadline:
    def test_no_module_calls_http_directly(self):
        offenders = {}
        for rel, path in _plugin_modules():
            if str(rel) in _ALLOWED:
                continue
            hits = _raw_http_calls(path.read_text(), filename=str(rel))
            if hits:
                offenders[str(rel)] = hits
        assert not offenders, (
            f"raw HTTP calls with no response deadline: {offenders}. Use "
            "_util.get_within_deadline / http_get_json (requests) or "
            "_util.urlopen_read (urllib); see #271"
        )

    def test_the_scan_covers_the_sources(self):
        scanned = {str(rel) for rel, _ in _plugin_modules()}
        assert {"sources/ncaam.py", "sources/soccer.py", "logos.py", "matcher.py"} <= scanned

    def test_every_allowlisted_file_exists(self):
        assert all((REPO / name).is_file() for name in _ALLOWED)

    @pytest.mark.parametrize("snippet", [
        "import requests\nrequests.get(u, timeout=5)",
        "import requests\nrequests.post(u, json={})",
        "import requests\nrequests.Session().get(u)",
        "import requests as rq\nrq.get(u)",
        "from requests import get",
        "import urllib.request\nurllib.request.urlopen(req, timeout=5)",
        "from urllib.request import urlopen\nurlopen(req)",
        "import http.client\nhttp.client.HTTPSConnection('h')",
        "import requests\nrequests.api.get(u)",
        "import requests\nget = requests.get\nget(u)",
        "from requests.api import get",
        "import requests.sessions",
        "import urllib.request\nurllib.request.build_opener().open(req)",
        "import urllib.request\nf = urllib.request.urlopen",
    ])
    def test_the_scan_flags_a_raw_call(self, snippet):
        """Instrument check: a scan that cannot see the pattern would pass
        every file vacuously."""
        assert _raw_http_calls(snippet), f"scan missed: {snippet!r}"

    def test_the_scan_passes_the_helpers(self):
        ok = (
            "from .._util import get_within_deadline, urlopen_read\n"
            "import requests\n"
            "r = get_within_deadline(requests, u, timeout=15)\n"
            "status, body = urlopen_read(req, timeout=10)\n"
            "requests.RequestException\n"
            "def f(r: requests.Response) -> None: raise requests.HTTPError('x', response=r)\n"
            "import http.client\nhttp.client.HTTPException\n"
        )
        assert _raw_http_calls(ok) == []
