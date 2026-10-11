"""Outbound HTTP with a whole-response wall-clock deadline (#271).

`timeout=` on requests.get / urlopen bounds the connect and each socket READ,
not the response. A server that sends a byte every few seconds never trips
it, so one request could hold a refresh until the pipeline child is killed at
its 25-minute ceiling, and every source on that run lost its games.

DO NOT "fix" this by streaming the body with `iter_content` and checking the
clock between chunks. Measured 2026-10-11 against a local server sending one
byte per 0.2s: `iter_content(chunk_size=65536)` returned its first chunk after
199.95s, because the buffered read under it blocks until the chunk is FULL. A
trickle can also stall the TLS handshake, status line or headers, which all
arrive before any body iterator exists. Only a wait the caller controls bounds
every phase, so the request runs on a worker thread and the caller stops
waiting at the deadline (`run_with_deadline`).

The deadline IS the call's existing `timeout`, as a total. Every real response
this plugin fetches finishes far inside it (statsapi's 3.2MB MLB season
schedule in 0.18-0.23s over 10 runs, ESPN scoreboards under 0.2s, measured
from Tower 2026-10-11), and plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME
budgets each call at its timeout. mlb keeps 20s for the reason in its
`_http_get`.

Giving up must also HANG UP, or the abandoned worker reads on. The worker's
sockets are captured at the TRANSPORT layer, by connection classes only this
module's sessions and openers use (`_TrackedAdapter` for requests,
`_TrackedHTTPHandler` / `_TrackedHTTPSHandler` for urllib), which hand each
socket over the moment its TCP connect returns: before TLS, before the request
is sent, before any read. DO NOT capture them with `sys.addaudithook`: this
code runs inside Dispatcharr's uwsgi, Celery and daphne processes, a hook can
never be removed, and it is called for every audit event in the host process
(open, import, exec, ...), a permanent cost imposed on someone else's app.
`tests/test_http_deadline.py` fails if any plugin module installs one.

This module is the only one that touches an HTTP client directly; the same
test fails on a raw `requests.get` / `urlopen` / `build_opener` anywhere else.
"""
from __future__ import annotations

import functools
import http.client
import socket
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional

from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection as _Urllib3HTTPConnection
from urllib3.connection import HTTPSConnection as _Urllib3HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool


class ResponseDeadlineExceeded(TimeoutError):
    """The whole response did not arrive within its deadline (#271).

    A TimeoutError, so every urllib caller's existing `except Exception` (and
    any `except OSError`) treats it as the network failure it is. The requests
    path re-raises it as the caller's `requests.Timeout` instead, see
    `get_within_deadline`.

    The message names the budget only, never the URL: several URLs carry an
    API key, and this text reaches log lines."""


class _WorkerConnections:
    """Every socket a deadline worker connects, so the caller can hang up.

    DO NOT hang up by calling the response's `close()` from the caller's
    thread. The worker is blocked inside the response's buffered read, which
    holds the reader's lock, and close() waits for that lock: measured
    2026-10-11, the caller sat ~40s past an 0.8s deadline until the trickle
    ended. shutdown() acts on the connection, takes no Python-level lock, and
    wakes the blocked read at once (it returns EOF, the library raises, the
    worker exits).

    `track` keeps a `dup()` of the socket, not the socket: TLS wrapping
    detaches the original object from its descriptor, and once the worker
    closes its own descriptor the number can be reused by an unrelated
    connection. The duplicate pins the connection itself.

    `track` RAISES in two cases, and the tracked connection classes close the
    socket and abort the connection when it does: the deadline has already
    passed (a connect that finished late, or a late redirect hop), and `dup()`
    failed (EMFILE). Both would otherwise leave a connection nothing can hang
    up, and no request at all is better than an unbounded one.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._open: List[socket.socket] = []
        self._expired = False

    def track(self, sock: socket.socket) -> None:
        dup = sock.dup()  # an OSError here aborts the connection, see docstring
        with self._lock:
            if not self._expired:
                self._open.append(dup)
                return
        _hang_up(dup)
        raise ResponseDeadlineExceeded("deadline passed before the connection opened")

    def expire(self) -> None:
        """The deadline passed: hang up every connection, without waiting."""
        with self._lock:
            self._expired = True
            opened, self._open = self._open, []
        for dup in opened:
            _hang_up(dup)

    def release(self) -> None:
        """The worker finished: close the duplicates only. The connections
        belong to the library (requests has released them, urllib's `with`
        has closed its own), and a duplicate left open would hold each one
        open until garbage collection."""
        with self._lock:
            opened, self._open = self._open, []
        for dup in opened:
            dup.close()


def _hang_up(dup: socket.socket) -> None:
    try:
        dup.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass  # already closed by the peer: nothing left to stop
    finally:
        dup.close()


# Set only on threads run_with_deadline starts. The tracked connection classes
# read it, so a connection opened anywhere else is never touched.
_deadline_worker = threading.local()


def _track(sock: socket.socket) -> None:
    """Hand a freshly connected socket to the current deadline worker, or
    close it and abort the connection if the worker cannot take it."""
    connections = getattr(_deadline_worker, "connections", None)
    if connections is None:
        return
    try:
        connections.track(sock)
    except BaseException:
        sock.close()
        raise


# ---------- requests: urllib3 connections that hand their socket over ----------

class _TrackedNewConnMixin:
    """For every urllib3 connection class: `_new_conn` returns the connected
    TCP socket (through the proxy, for SOCKS), and HTTPSConnection.connect
    wraps TLS around it afterwards, so handing it over here covers the
    handshake too."""

    def _new_conn(self) -> socket.socket:
        sock = super()._new_conn()
        _track(sock)
        return sock


class _TrackedUrllib3HTTPConnection(_TrackedNewConnMixin, _Urllib3HTTPConnection):
    pass


class _TrackedUrllib3HTTPSConnection(_TrackedNewConnMixin, _Urllib3HTTPSConnection):
    pass


class _TrackedHTTPPool(HTTPConnectionPool):
    ConnectionCls = _TrackedUrllib3HTTPConnection


class _TrackedHTTPSPool(HTTPSConnectionPool):
    ConnectionCls = _TrackedUrllib3HTTPSConnection


_TRACKED_POOLS = {"http": _TrackedHTTPPool, "https": _TrackedHTTPSPool}


@functools.lru_cache(maxsize=None)
def _tracked_socks_pools() -> Dict[str, type]:
    """Tracked twins of urllib3's SOCKS pools, built on first use.

    Imported lazily because urllib3.contrib.socks needs PySocks, which is
    optional; requests only builds a SOCKS proxy manager when it is present
    (it is, in Dispatcharr's image: PySocks 1.7.1, checked 2026-10-11), and
    takes a SOCKS proxy from the environment (`HTTPS_PROXY=socks5h://...`)
    without the plugin asking for one.

    Known limit: the socket is handed over when PySocks' `create_connection`
    returns, i.e. AFTER the SOCKS negotiation. A proxy that trickles its
    negotiation reply (up to ~266 bytes, each read bounded only by the socket
    timeout) is not hung up: the caller still returns at the deadline, and the
    abandoned worker ends with the reply or the pipeline child. Capturing it
    earlier means rebinding `socks.create_connection` in urllib3's module (a
    process-wide patch of a library Dispatcharr's own code may use, the same
    objection as the audit hook) or copying PySocks' and urllib3's connection
    code. Neither is worth it for a proxy the user configured and that
    misbehaves; Dispatcharr's container sets no proxy (checked 2026-10-11).
    """
    from urllib3.contrib import socks as urllib3_socks

    class _TrackedSOCKSConnection(_TrackedNewConnMixin, urllib3_socks.SOCKSConnection):
        pass

    class _TrackedSOCKSHTTPSConnection(_TrackedNewConnMixin, urllib3_socks.SOCKSHTTPSConnection):
        pass

    class _TrackedSOCKSHTTPPool(urllib3_socks.SOCKSHTTPConnectionPool):
        ConnectionCls = _TrackedSOCKSConnection

    class _TrackedSOCKSHTTPSPool(urllib3_socks.SOCKSHTTPSConnectionPool):
        ConnectionCls = _TrackedSOCKSHTTPSConnection

    return {"http": _TrackedSOCKSHTTPPool, "https": _TrackedSOCKSHTTPSPool}


class _TrackedAdapter(HTTPAdapter):
    """requests' own adapter, with every pool built from the tracked classes:
    direct, through an HTTP(S) proxy, and through a SOCKS proxy."""

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = _TRACKED_POOLS

    def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
        manager = super().proxy_manager_for(proxy, **proxy_kwargs)
        manager.pool_classes_by_scheme = (
            _tracked_socks_pools() if proxy.lower().startswith("socks") else _TRACKED_POOLS
        )
        return manager


# ---------- urllib: http.client connections that hand their socket over ----------

class _TrackedConnectionMixin:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        create = self._create_connection  # socket.create_connection, per instance

        def _create_and_track(*a: Any, **kw: Any) -> socket.socket:
            sock = create(*a, **kw)
            _track(sock)
            return sock

        # connect() opens the TCP socket through this attribute, and
        # HTTPSConnection.connect wraps TLS around what it returns.
        self._create_connection = _create_and_track


class _TrackedHTTPLibConnection(_TrackedConnectionMixin, http.client.HTTPConnection):
    pass


class _TrackedHTTPSLibConnection(_TrackedConnectionMixin, http.client.HTTPSConnection):
    pass


def _tracked_class(http_class: type) -> type:
    if issubclass(http_class, http.client.HTTPSConnection):
        return _TrackedHTTPSLibConnection
    return _TrackedHTTPLibConnection


class _TrackedHTTPHandler(urllib.request.HTTPHandler):
    # do_open, not http_open/https_open: those pass version-specific keyword
    # arguments (3.11's https_open adds check_hostname, 3.12+ dropped it).
    def do_open(self, http_class: type, req: Any, **kwargs: Any) -> Any:
        return super().do_open(_tracked_class(http_class), req, **kwargs)


class _TrackedHTTPSHandler(urllib.request.HTTPSHandler):
    def do_open(self, http_class: type, req: Any, **kwargs: Any) -> Any:
        return super().do_open(_tracked_class(http_class), req, **kwargs)


def _opener() -> urllib.request.OpenerDirector:
    """An opener for http and https ONLY, every connection tracked.

    DO NOT use `urllib.request.build_opener` here: it also installs the FTP,
    file and data handlers, and urllib follows a redirect to ftp://, whose
    control and data sockets no tracked class sees (a slow FTP download would
    outlive the deadline). A file:// URL would read the local disk. Nothing
    this plugin fetches is either, so those schemes now fail as "unknown url
    type" instead.
    """
    opener = urllib.request.OpenerDirector()
    for handler in (
        urllib.request.ProxyHandler(),
        urllib.request.UnknownHandler(),
        _TrackedHTTPHandler(),
        _TrackedHTTPSHandler(),
        urllib.request.HTTPDefaultErrorHandler(),
        urllib.request.HTTPRedirectHandler(),
        urllib.request.HTTPErrorProcessor(),
    ):
        opener.add_handler(handler)
    return opener


# ---------- the deadline ----------

def run_with_deadline(work: Any, deadline_s: float) -> Any:
    """Run `work()` on a daemon thread and return what it returns, re-raising
    whatever it raised, or raise ResponseDeadlineExceeded once `deadline_s`
    seconds of wall clock have passed.

    THE one deadline implementation; `get_within_deadline` and `urlopen_read`
    are its only callers, and `work` must open its connections through this
    module's tracked classes. On expiry every socket the worker connected is
    shut down under it (see `_WorkerConnections`), so its blocked read returns
    at once and the thread exits instead of draining the trickle, whether it
    was in the TLS handshake, the headers or the body. Whatever the worker
    produced after the deadline is discarded, so a body cut at the deadline is
    never parsed.

    Two phases have no socket to hang up yet, and both end on their own. A TCP
    connect in progress is bounded by the connect timeout (the same
    `timeout`) for each address the name resolves to, and the socket it
    produces after expiry is refused; handing the socket over before connect()
    would mean copying urllib3's and socket's connect loops. The DNS
    lookup runs before any socket exists: the caller still returns at the
    deadline, and the abandoned worker ends when the resolver gives up, which
    glibc bounds at timeout x attempts (5s x 2 by default; Dispatcharr's
    resolv.conf sets no options). Measured 2026-10-11 against a blackholed
    nameserver: the bare lookup took 10.0s, the OLD path held the caller those
    10.0s (requests' `timeout=` never covered DNS), four 2s-deadline calls
    returned in 8.0s, and their four abandoned workers were gone 8.5s after
    the last call. So stalled lookups cannot pile up: at most about
    10s / deadline live at once.

    The thread is a daemon because HTTP runs in the pipeline child
    (tasks.run_pipeline_subprocess), which must be able to exit even if a
    worker is still inside a call.
    """
    if not deadline_s or deadline_s <= 0:
        # requests reads timeout=None as "no timeout at all"; as a deadline it
        # would wait forever, and zero or less is no budget. Both are caller bugs.
        raise ValueError(f"deadline must be a positive number of seconds, got {deadline_s!r}")
    connections = _WorkerConnections()
    outcome: Dict[str, Any] = {}
    done = threading.Event()

    def _run() -> None:
        _deadline_worker.connections = connections
        try:
            outcome["value"] = work()
        except BaseException as exc:  # noqa: BLE001 - handed to the caller's thread
            outcome["error"] = exc
        finally:
            done.set()

    threading.Thread(target=_run, name="http-deadline", daemon=True).start()
    if not done.wait(deadline_s):
        connections.expire()
        raise ResponseDeadlineExceeded(f"response not complete within {deadline_s:g}s")
    connections.release()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def get_within_deadline(requests_mod: Any, url: str, *, timeout: float, **kwargs: Any) -> Any:
    """`GET url` with `timeout=timeout, **kwargs`, the WHOLE response
    (connect, TLS, status line, headers, body) bounded by `timeout`.

    Returns the same fully-read Response `requests.get` returns, so callers
    keep `raise_for_status()`, `.status_code`, `.headers` and `.json()`. On
    the deadline it raises `requests_mod.Timeout`, a RequestException, which
    is what every caller already catches for a read timeout.

    The session is `requests_mod.Session()`, the CALLER's module-level
    `requests` symbol looked up per call (the patch-point reason is in
    `_util.http_get_json`), with `_TrackedAdapter` mounted. Tests stub the
    transport with `tests/conftest.py::stub_requests_get`, which patches
    `requests.Session.get`; a stub on `requests.get` is no longer reached.

    DO NOT call `requests.get` / `requests.post` anywhere else in the plugin;
    `tests/test_http_deadline.py` fails on any call site outside this module.
    """
    def _work() -> Any:
        with requests_mod.Session() as session:
            adapter = _TrackedAdapter()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            return session.get(url, timeout=timeout, **kwargs)

    try:
        return run_with_deadline(_work, timeout)
    except ResponseDeadlineExceeded as exc:
        raise requests_mod.Timeout(str(exc)) from None


def urlopen_read(request: Any, *, timeout: float, max_bytes: Optional[int] = None) -> tuple:
    """`(status, body_bytes)` for `request` (a urllib Request or URL), with the
    whole response bounded by `timeout`.

    Opens through `_opener()`: the tracked handlers, and http/https only.
    Raises what urlopen
    raises (HTTPError for a 4xx/5xx, carrying its headers, so gamethumbs can
    still read Retry-After), ResponseDeadlineExceeded on the deadline, and
    ValueError when the body exceeds `max_bytes`. The body is read whole, in
    memory, so a caller downloading a file passes `max_bytes`; every caller
    either parses the body or validates its first bytes before writing it, so
    a partial body must never reach them.

    Tests stub the transport with `tests/conftest.py::stub_urlopen`, which
    patches `urllib.request.OpenerDirector.open`.
    """
    def _work() -> tuple:
        with _opener().open(request, timeout=timeout) as resp:
            if max_bytes is None:
                return resp.status, resp.read()
            body = resp.read(max_bytes + 1)
            if len(body) > max_bytes:
                raise ValueError(f"response larger than {max_bytes} bytes")
            return resp.status, body

    return run_with_deadline(_work, timeout)


class NetworkBudget:
    """A wall-clock allowance shared by a run's OPTIONAL network fetches.

    `allow()` says whether there is time left for one more fetch, and counts
    the fetches it refuses so the run can report them once. A refused fetch
    must take the path its caller already takes when that fetch FAILS, so the
    budget never changes anything a failed fetch would not; and it is only
    for fetches whose failure is harmless (see plugin.py's apply pre-pass).

    Reads `time.monotonic` per call, so a test can drive it with a fake clock
    by replacing this module's `time`.
    """

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self._ends_at = time.monotonic() + seconds
        self.skipped = 0

    def allow(self) -> bool:
        if time.monotonic() < self._ends_at:
            return True
        self.skipped += 1
        return False
