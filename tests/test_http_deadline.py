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
fast and large responses come back whole, the connection is hung up in every
phase (TLS handshake, headers, body, redirect), no module makes an HTTP call
outside `_http.py`, and no module installs an audit hook.

`_http` is imported inside each test (`_h()`), so the witnesses still run, and
fail as assertions, against a tree that predates it.
"""
from __future__ import annotations

import ast
import http.server
import importlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import requests

from dispatcharr_ranked_matchups import _util

REPO = Path(__file__).resolve().parent.parent


def _h():
    return importlib.import_module("dispatcharr_ranked_matchups._http")

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
        elif route == "redirect-ftp":
            self.send_response(302)
            self.send_header("Location", "ftp://127.0.0.1:1/slow.bin")
            self.send_header("Content-Length", "0")
            self.end_headers()
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


@pytest.fixture(scope="module")
def stalled_tls():
    """A server that answers a TLS ClientHello with a record header promising
    16 KiB and then trickles it a byte at a time, so the client sits in its
    handshake. A silent server would not do: the per-read timeout ends that on
    its own, and the test would pass with no deadline at all. Records when
    each client hangs up (its socket reads EOF or a write fails)."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    hung_up = []

    def serve():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            event = threading.Event()
            hung_up.append(event)

            def drain(c=conn, done=event):
                try:
                    while c.recv(4096):
                        pass
                except OSError:
                    pass
                done.set()

            def trickle(c=conn, done=event):
                try:
                    c.sendall(b"\x16\x03\x03\x40\x00")  # handshake record, 0x4000 bytes
                    for _ in range(TRICKLE_BYTES):
                        if done.is_set():
                            break
                        c.sendall(b"\x00")
                        time.sleep(TRICKLE_INTERVAL_S)
                except OSError:
                    done.set()
                finally:
                    done.wait(TRICKLE_BYTES * TRICKLE_INTERVAL_S)
                    c.close()

            threading.Thread(target=drain, daemon=True).start()
            threading.Thread(target=trickle, daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    yield f"https://127.0.0.1:{srv.getsockname()[1]}", hung_up
    srv.close()


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
                    _h().urlopen_read(urllib.request.Request(f"{server}/{route}"), timeout=DEADLINE_S)
        finished, _e, _r, err = _bounded(fn, DEADLINE_S + SLACK_S)
        assert finished and err is None, err
        assert _Handler.disconnects[route].wait(SLACK_S + TRICKLE_INTERVAL_S * 5), (
            f"the server is still trickling into a connection the client gave up on ({route})"
        )

    @pytest.mark.parametrize("via", ["requests", "urllib"])
    def test_a_stalled_tls_handshake_is_hung_up(self, stalled_tls, via):
        """The handshake happens after the TCP connect and before any request
        is sent; the socket must already be tracked by then. The handshake is
        TRICKLED, so only the deadline can end it."""
        url, hung_up = stalled_tls
        before = len(hung_up)
        if via == "requests":
            def fn():
                with pytest.raises(requests.Timeout):
                    _h().get_within_deadline(requests, url, timeout=DEADLINE_S)
        else:
            def fn():
                with pytest.raises(TimeoutError):
                    _h().urlopen_read(urllib.request.Request(url), timeout=DEADLINE_S)
        finished, _e, _r, err = _bounded(fn, DEADLINE_S + SLACK_S)
        assert finished and err is None, err
        deadline = time.monotonic() + SLACK_S
        while len(hung_up) <= before and time.monotonic() < deadline:
            time.sleep(0.05)
        assert len(hung_up) > before, "the client never connected"
        assert hung_up[before].wait(SLACK_S), "the stalled handshake was never hung up"

    @pytest.mark.parametrize("url", [
        "ftp://127.0.0.1:1/slow.bin", "file:///etc/hostname", "data:,hello",
    ])
    def test_urlopen_read_refuses_non_http_schemes(self, url):
        """No tracked class sees an FTP socket, and a file:// URL reads disk."""
        with pytest.raises(urllib.error.URLError, match="unknown url type"):
            _h().urlopen_read(url, timeout=5.0)

    def test_a_redirect_to_ftp_is_refused(self, server):
        """urllib follows 3xx to ftp:// by default; that hop would escape the
        deadline's hang-up."""
        with pytest.raises(urllib.error.URLError, match="unknown url type"):
            _h().urlopen_read(urllib.request.Request(f"{server}/redirect-ftp"), timeout=5.0)

    def test_a_socks_proxied_connection_is_hung_up(self, server, monkeypatch):
        """requests takes a SOCKS proxy from the environment, and urllib3's
        SOCKS pools are separate classes. The proxy is a stand-in `socks`
        module that connects straight to the target (no PySocks or SOCKS
        server needed); the code under test is urllib3's real SOCKS pool and
        connection, with the plugin's tracking on top."""
        import types
        fake = types.ModuleType("socks")
        fake.PROXY_TYPE_SOCKS4, fake.PROXY_TYPE_SOCKS5 = 1, 2
        fake.socksocket = socket.socket

        class ProxyError(Exception):
            socket_err = None

        fake.ProxyError = ProxyError
        fake.create_connection = lambda dest, timeout=None, **kw: socket.create_connection(dest, timeout)
        had_contrib = "urllib3.contrib.socks" in sys.modules
        monkeypatch.setitem(sys.modules, "socks", fake)
        import requests.adapters
        import urllib3.contrib.socks as urllib3_socks
        monkeypatch.setattr(urllib3_socks, "socks", fake)
        monkeypatch.setattr(requests.adapters, "SOCKSProxyManager", urllib3_socks.SOCKSProxyManager)
        _h()._tracked_socks_pools.cache_clear()
        route = "trickle-body-socks"
        _Handler.disconnects[route] = threading.Event()
        try:
            def fn():
                with pytest.raises(requests.Timeout):
                    _h().get_within_deadline(
                        requests, f"{server}/{route}", timeout=DEADLINE_S,
                        proxies={"http": "socks5h://127.0.0.1:1"},
                    )
            finished, _e, _r, err = _bounded(fn, DEADLINE_S + SLACK_S)
            assert finished and err is None, err
            assert _Handler.disconnects[route].wait(SLACK_S + TRICKLE_INTERVAL_S * 5), (
                "the SOCKS-proxied connection was never hung up"
            )
        finally:
            _h()._tracked_socks_pools.cache_clear()
            if not had_contrib:
                sys.modules.pop("urllib3.contrib.socks", None)

    def test_get_within_deadline_raises_the_callers_timeout(self, server):
        """Direct callers (odds, CFBD, CBB, Football-Data) catch
        RequestException; the deadline must arrive as one."""
        t0 = time.monotonic()
        with pytest.raises(requests.Timeout):
            _h().get_within_deadline(requests, f"{server}/trickle-body-b", timeout=DEADLINE_S)
        assert time.monotonic() - t0 < DEADLINE_S + SLACK_S

    def test_get_within_deadline_returns_a_full_response(self, server):
        r = _h().get_within_deadline(requests, f"{server}/fast", timeout=5.0)
        r.raise_for_status()
        assert r.status_code == 200 and r.json() == {"events": [1, 2, 3]}

    def test_urlopen_read_is_bounded(self, server):
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            _h().urlopen_read(
                urllib.request.Request(f"{server}/trickle-body-c"), timeout=DEADLINE_S,
            )
        assert time.monotonic() - t0 < DEADLINE_S + SLACK_S

    def test_urlopen_read_returns_status_and_body(self, server):
        status, body = _h().urlopen_read(urllib.request.Request(f"{server}/fast"), timeout=5.0)
        assert status == 200 and json.loads(body) == {"events": [1, 2, 3]}

    def test_urlopen_read_passes_http_errors_through(self, server):
        """gamethumbs reads Retry-After off the HTTPError, so it must arrive
        as itself, not wrapped."""
        with pytest.raises(urllib.error.HTTPError) as info:
            _h().urlopen_read(urllib.request.Request(f"{server}/missing"), timeout=5.0)
        assert info.value.code == 404

    def test_urlopen_read_refuses_an_oversized_body(self, server):
        with pytest.raises(ValueError):
            _h().urlopen_read(urllib.request.Request(f"{server}/large"), timeout=5.0, max_bytes=1024)
        status, body = _h().urlopen_read(
            urllib.request.Request(f"{server}/fast"), timeout=5.0, max_bytes=1024)
        assert status == 200 and body

    def test_expiry_hangs_up_every_tracked_connection(self):
        """Early and late hand-overs alike: a socket connected AFTER expiry (a
        slow redirect hop) is refused and hung up on the spot, or nothing ever
        would hang it up."""
        pairs = [socket.socketpair() for _ in range(2)]
        tracked = _h()._WorkerConnections()
        tracked.track(pairs[0][0])
        tracked.expire()
        with pytest.raises(_h().ResponseDeadlineExceeded):
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
        tracked = _h()._WorkerConnections()
        tracked.track(ours)
        dup = tracked._open[0]
        tracked.release()
        assert dup.fileno() == -1, "the duplicate descriptor leaked"
        ours.sendall(b"x")
        peer.settimeout(2.0)
        assert peer.recv(1) == b"x"
        ours.close()
        peer.close()

    @staticmethod
    def _tracked_get(url):
        """A GET through the tracked transport, exactly as get_within_deadline
        builds it, without the deadline worker around it."""
        with requests.Session() as session:
            adapter = _h()._TrackedAdapter()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            return session.get(url, timeout=5)

    @staticmethod
    def _tracked_open(url):
        with _h()._opener().open(url, timeout=5) as resp:
            return resp.read()

    def _as_worker(self, connections, fn):
        """Run fn on a thread the tracked classes treat as a deadline worker."""
        box = {}

        def run():
            _h()._deadline_worker.connections = connections
            try:
                box["result"] = fn()
            except BaseException as exc:  # noqa: BLE001
                box["error"] = exc

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(10)
        return box

    class _Spy:
        def __init__(self):
            self.seen = []

        def track(self, sock):
            self.seen.append(sock.getpeername())

    @pytest.mark.parametrize("via", ["requests", "urllib"])
    def test_a_workers_connection_is_tracked_and_no_other(self, server, via):
        """Only a deadline worker's connections are handed over; the same
        classes on any other thread leave the socket alone."""
        fetch = self._tracked_get if via == "requests" else self._tracked_open
        spy = self._Spy()
        box = self._as_worker(spy, lambda: fetch(f"{server}/fast"))
        assert "error" not in box, box
        assert len(spy.seen) == 1 and spy.seen[0][0] == "127.0.0.1"
        _h()._deadline_worker.connections = None  # this (non-worker) thread
        fetch(f"{server}/fast")  # must not raise or track anything

    @pytest.mark.parametrize("via", ["requests", "urllib"])
    def test_a_connection_that_cannot_be_tracked_is_not_made(self, server, via):
        """dup() failing (EMFILE) must not let an unbounded connection through."""
        class Broken:
            def track(self, sock):
                raise OSError("EMFILE")

        fetch = self._tracked_get if via == "requests" else self._tracked_open
        box = self._as_worker(Broken(), lambda: fetch(f"{server}/fast"))
        assert isinstance(box.get("error"), (requests.ConnectionError, OSError)), box

    @pytest.mark.parametrize("via", ["requests", "urllib"])
    def test_a_connect_after_expiry_is_refused(self, server, via):
        """A connect that finishes late, or a late redirect hop, must not open
        a connection nobody will hang up."""
        connections = _h()._WorkerConnections()
        connections.expire()
        fetch = self._tracked_get if via == "requests" else self._tracked_open
        box = self._as_worker(connections, lambda: fetch(f"{server}/fast"))
        assert isinstance(box.get("error"), (requests.ConnectionError, OSError)), box

    def test_no_descriptor_leaks_across_calls(self, server):
        fd_dir = Path("/proc/self/fd")
        if not fd_dir.is_dir():
            pytest.skip("needs /proc")
        _get_json(f"{server}/fast", 5.0)  # warm up: imports done
        before = len(list(fd_dir.iterdir()))
        for _ in range(20):
            _get_json(f"{server}/fast", 5.0)
            _h().urlopen_read(urllib.request.Request(f"{server}/fast"), timeout=5.0)
        assert len(list(fd_dir.iterdir())) <= before + 2

    def test_worker_errors_reach_the_caller(self):
        def work():
            raise KeyError("boom")

        with pytest.raises(KeyError):
            _h().run_with_deadline(work, 5.0)

    @pytest.mark.parametrize("bad", [None, 0, -1])
    def test_a_missing_deadline_is_refused(self, bad):
        with pytest.raises(ValueError):
            _h().run_with_deadline(lambda: None, bad)


class TestNetworkBudget:
    """#271: the one allowance apply's cosmetic fetches share."""

    class _Clock:
        def __init__(self):
            self.now = 100.0

        def monotonic(self):
            return self.now

    def test_allows_until_spent_then_counts_refusals(self, monkeypatch):
        clock = self._Clock()
        monkeypatch.setattr(_h(), "time", clock)
        budget = _h().NetworkBudget(10)
        assert budget.allow() and budget.skipped == 0
        clock.now += 9.9
        assert budget.allow()
        clock.now += 0.1
        assert not budget.allow() and not budget.allow()
        assert budget.skipped == 2

    def test_a_zero_budget_allows_nothing(self, monkeypatch):
        monkeypatch.setattr(_h(), "time", self._Clock())
        budget = _h().NetworkBudget(0)
        assert not budget.allow() and budget.skipped == 1


# ---------- class guard: no HTTP call outside the shared helper ----------

# Names on the `requests` module that open a connection. Flagged on any
# REFERENCE, not only a call: `get = requests.get; get(url)` and
# `requests.api.get(url)` make the same unbounded request.
_REQUESTS_NAMES = {"get", "post", "put", "patch", "delete", "head", "options", "request",
                   "Session", "session", "api", "sessions"}
# Names in the stdlib that open a connection, wherever they are referenced.
_STDLIB_NAMES = {"urlopen", "build_opener", "OpenerDirector", "HTTPConnection", "HTTPSConnection"}
# Files allowed to touch an HTTP client directly, each with its reason.
_ALLOWED = {
    # The deadline transport itself: its tracked urllib3 / http.client
    # connection classes and its opener ARE the bounded path every other module
    # goes through. Its requests session comes from the caller's `requests_mod`.
    "_http.py",
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
            if mod == "urllib3" or mod.startswith("urllib3."):
                hits.append(node.lineno)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(a.name == "urllib3" or a.name.startswith("urllib3.") for a in node.names):
                hits.append(node.lineno)  # a PoolManager of its own bypasses the tracked pools
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
            "_util.http_get_json / _http.get_within_deadline (requests) or "
            "_http.urlopen_read (urllib); see #271"
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
        "import urllib3\nurllib3.PoolManager().request('GET', u)",
        "from urllib3 import PoolManager",
    ])
    def test_the_scan_flags_a_raw_call(self, snippet):
        """Instrument check: a scan that cannot see the pattern would pass
        every file vacuously."""
        assert _raw_http_calls(snippet), f"scan missed: {snippet!r}"

    def test_the_scan_passes_the_helpers(self):
        ok = (
            "from .._http import get_within_deadline, urlopen_read\n"
            "import requests\n"
            "r = get_within_deadline(requests, u, timeout=15)\n"
            "status, body = urlopen_read(req, timeout=10)\n"
            "requests.RequestException\n"
            "def f(r: requests.Response) -> None: raise requests.HTTPError('x', response=r)\n"
            "import http.client\nhttp.client.HTTPException\n"
        )
        assert _raw_http_calls(ok) == []


# ---------- no audit hook: the plugin lives in someone else's process ----------

def _audit_hook_references(source: str, filename: str = "<src>") -> list:
    tree = ast.parse(source, filename=filename)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "addaudithook":
            hits.append(node.lineno)
        elif isinstance(node, ast.Name) and node.id == "addaudithook":
            hits.append(node.lineno)
        elif isinstance(node, ast.ImportFrom) and any(a.name == "addaudithook" for a in node.names):
            hits.append(node.lineno)
    return sorted(set(hits))


# Runs in a fresh interpreter: a hook already installed in the pytest process
# could never be removed, and would make the count meaningless. The package is
# registered the way tests/conftest.py does it, then every module that makes an
# HTTP call is imported and used against a local server. Each `sys.addaudithook`
# call raises the "sys.addaudithook" audit event, which the probe's own hook
# (installed first) counts.
_AUDIT_PROBE = r"""
import http.server, importlib, logging, sys, threading, types, urllib.request
added = []
sys.addaudithook(lambda event, args: added.append(1) if event == "sys.addaudithook" else None)
repo = sys.argv[1]
pkg = types.ModuleType("dispatcharr_ranked_matchups")
pkg.__path__ = [repo]
sys.modules["dispatcharr_ranked_matchups"] = pkg
import requests
util = importlib.import_module("dispatcharr_ranked_matchups._util")
logos = importlib.import_module("dispatcharr_ranked_matchups.logos")
for name in ("sources", "matcher", "llm_descriptions", "gamethumbs", "sources._odds"):
    importlib.import_module("dispatcharr_ranked_matchups." + name)

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def do_GET(self):
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
url = "http://127.0.0.1:%d/x" % srv.server_address[1]
assert util.http_get_json(requests, url, log=logging.getLogger("p"), tag="p", timeout=5.0) == {"ok": True}
assert logos._http_get_json(url, "probe") == {"ok": True}
print("ADDAUDITHOOK_CALLS=%d" % len(added))
"""


class TestNoAuditHook:
    def test_no_module_references_addaudithook(self):
        offenders = {
            str(rel): hits for rel, path in _plugin_modules()
            if (hits := _audit_hook_references(path.read_text(), filename=str(rel)))
        }
        assert not offenders, (
            f"sys.addaudithook in plugin code: {offenders}. A hook can never be "
            "removed and runs for every audit event in Dispatcharr's own "
            "processes; capture at the transport instead (see _http.py)"
        )

    @pytest.mark.parametrize("snippet", [
        "import sys\nsys.addaudithook(f)",
        "from sys import addaudithook\naddaudithook(f)",
        "import sys\nhook = sys.addaudithook",
    ])
    def test_the_scan_flags_a_hook(self, snippet):
        assert _audit_hook_references(snippet), f"scan missed: {snippet!r}"

    def test_using_the_plugins_http_installs_no_hook(self, tmp_path):
        """Behavioural, so a hook installed some way the AST scan cannot see
        (a helper, an eval, a dependency) is still caught."""
        script = tmp_path / "probe.py"
        script.write_text(_AUDIT_PROBE)
        proc = subprocess.run(
            [sys.executable, str(script), str(REPO)],
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        assert proc.returncode == 0, proc.stderr[-3000:]
        assert "ADDAUDITHOOK_CALLS=0" in proc.stdout, (
            f"the plugin installed an audit hook: {proc.stdout.strip()}"
        )


def test_the_network_guard_removes_ambient_proxies(monkeypatch):
    """tests/conftest.py::_no_live_network clears the proxy variables, or an
    unstubbed call would reach the internet through a loopback proxy."""
    from tests.conftest import PROXY_ENV_VARS, clear_proxy_env
    for name in PROXY_ENV_VARS:
        monkeypatch.setenv(name, "http://127.0.0.1:3128")
    clear_proxy_env(monkeypatch)
    assert not [n for n in PROXY_ENV_VARS if n in os.environ]
    assert requests.utils.get_environ_proxies("https://site.api.espn.com/x") == {}


_SOCKET_LAYER = {"getaddrinfo", "create_connection", "connect", "connect_ex", "socket"}
_SOCKET_TARGET = re.compile(
    r"^socket\.(socket\.)?(%s)$" % "|".join(sorted(_SOCKET_LAYER)))


def _socket_layer_patches(source):
    """Lines that replace a socket-layer function, however the receiver is
    spelled: any call taking (socket-ish receiver, "layer name") — setattr,
    patch.object, ... — or a "socket.layer" string target (monkeypatch's
    string form, mock.patch), and plain `receiver.layer = ...` assignments.
    A receiver is the socket module or its `socket` class under any import
    alias (`import socket as s`, `from socket import socket as S`)."""
    tree = ast.parse(source)
    modules, classes = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {a.asname or a.name for a in node.names if a.name == "socket"}
        elif isinstance(node, ast.ImportFrom) and node.module == "socket":
            classes |= {a.asname or a.name for a in node.names if a.name == "socket"}

    def is_socket(node):
        if isinstance(node, ast.Name):
            return node.id in modules or node.id in classes
        return (isinstance(node, ast.Attribute) and node.attr == "socket"
                and isinstance(node.value, ast.Name) and node.value.id in modules)

    def arg(call, index, *keywords):
        if len(call.args) > index:
            return call.args[index]
        return next((k.value for k in call.keywords if k.arg in keywords), None)

    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # setattr(target, name, value) / patch.object(target, attribute=...)
            # / patch(target="socket.x"), positional or keyword.
            first = arg(node, 0, "target")
            second = arg(node, 1, "name", "attribute")
            if first is not None and is_socket(first) and (
                    (isinstance(second, ast.Constant) and second.value in _SOCKET_LAYER)
                    # patch.multiple(socket, getaddrinfo=f)
                    or any(k.arg in _SOCKET_LAYER for k in node.keywords)):
                hits.append(node.lineno)
            elif (isinstance(first, ast.Constant) and isinstance(first.value, str)
                    and _SOCKET_TARGET.match(first.value)):
                hits.append(node.lineno)
        elif isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Attribute) and t.attr in _SOCKET_LAYER and is_socket(t.value)
                for t in node.targets):
            hits.append(node.lineno)
    return hits


class TestNoTestReplacesTheSocketLayer:
    """`_no_live_network` records a live attempt from its own socket hooks. A
    test that replaces those hooks (an "offline" getaddrinfo, say) silently
    switches the guard off for itself, so a call that missed the stubs reads
    as "feed unreachable" and the test passes (1.34.0 batch review, Codex:
    tests/test_upcoming_window.py built every source under such a hook with
    its stub installed only afterwards). Refuse at the HTTP seam instead:
    tests/conftest.py::refuse_requests."""

    @staticmethod
    def _test_files():
        return [p for p in sorted((REPO / "tests").rglob("*.py")) if p.name != "conftest.py"]

    def test_no_test_replaces_the_socket_layer(self):
        offenders = {}
        for path in self._test_files():
            hits = _socket_layer_patches(path.read_text())
            if hits:
                offenders[str(path.relative_to(REPO))] = hits
        assert not offenders, (
            f"tests replace the socket layer, which disables _no_live_network: "
            f"{offenders}. Use tests/conftest.py::refuse_requests."
        )

    def test_the_scan_reads_the_test_tree(self):
        """Fail on the instrument first: an empty or wrong glob passes the
        scan vacuously. The files named are the three that used to replace
        getaddrinfo, and this one."""
        names = {p.name for p in self._test_files()}
        assert {"test_upcoming_window.py", "test_hold_failed_days.py",
                "test_mapped_prefixes.py", "test_http_deadline.py"} <= names
        assert len(names) > 50

    @pytest.mark.parametrize("snippet", [
        'import socket\nmonkeypatch.setattr(socket, "getaddrinfo", f)',
        'import socket\nmp.setattr(socket.socket, "connect", f)',
        "import socket\nsocket.getaddrinfo = f",
        "import socket\nsocket.socket.connect_ex = f",
        'import socket as s\nmonkeypatch.setattr(s, "getaddrinfo", f)',
        'from socket import socket as S\nmp.setattr(S, "connect", f)',
        'monkeypatch.setattr("socket.getaddrinfo", f)',
        '@mock.patch("socket.socket.connect")\ndef t(m): pass',
        'import socket\nmock.patch.object(socket, "create_connection", f)',
        'import socket\n@mock.patch.object(socket, attribute="getaddrinfo", new=f)\ndef t(): pass',
        'import socket\n@mock.patch.object(target=socket, attribute="getaddrinfo", new=f)\ndef t(): pass',
        'mock.patch(target="socket.getaddrinfo", new=f)',
        'import socket\n@mock.patch.multiple(socket, getaddrinfo=f)\ndef t(): pass',
    ])
    def test_the_scan_flags_a_replacement(self, snippet):
        assert _socket_layer_patches(snippet), f"scan missed: {snippet!r}"
