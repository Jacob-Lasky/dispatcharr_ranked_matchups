"""Regression guards for the scheduler DB-connection leak (#82/#136).

The scheduler thread reads settings from Postgres each tick. Two bugs caused a
connection leak that eventually hit `max_connections` and locked up the server:

1. The loop slept WITHOUT closing its DB connection, so a parked scheduler
   pinned one backend open indefinitely.
2. `Plugin.__init__` (re-run on every loader discovery) stopped+started the
   scheduler each time, orphaning the old greenlet's connection.

This file locks in the connection-lifecycle half of the fix: every park closes
the connection, and no bare `stop_event.wait` slips back into the loop. The
idempotent-__init__ half (no restart when a healthy thread is already running)
is covered in test_plugin_helpers.py::TestPluginStopTeardown against the real
reload-stable registry.
"""

import ast
import importlib.util
import os
import sys
import types

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "dispatcharr_ranked_matchups"
PLUGIN_PY = os.path.join(REPO_ROOT, "plugin.py")


@pytest.fixture(scope="module")
def plugin():
    if f"{PKG}._util" not in sys.modules:
        s = importlib.util.spec_from_file_location(f"{PKG}._util", os.path.join(REPO_ROOT, "_util.py"))
        m = importlib.util.module_from_spec(s)
        sys.modules[f"{PKG}._util"] = m
        s.loader.exec_module(m)
    if PKG not in sys.modules:
        pkg = types.ModuleType(PKG)
        pkg.__path__ = [REPO_ROOT]
        sys.modules[PKG] = pkg
    if f"{PKG}.plugin" not in sys.modules:
        s = importlib.util.spec_from_file_location(f"{PKG}.plugin", PLUGIN_PY)
        m = importlib.util.module_from_spec(s)
        sys.modules[f"{PKG}.plugin"] = m
        s.loader.exec_module(m)
    return sys.modules[f"{PKG}.plugin"]


class _RecordingConn:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


def _install_fake_db(monkeypatch, conn):
    db = types.ModuleType("django.db")
    db.connection = conn
    monkeypatch.setitem(sys.modules, "django", types.ModuleType("django"))
    monkeypatch.setitem(sys.modules, "django.db", db)


class TestSchedulerClosesConnection:
    def test_close_db_closes_the_connection(self, plugin, monkeypatch):
        conn = _RecordingConn()
        _install_fake_db(monkeypatch, conn)
        plugin._scheduler_close_db()
        assert conn.closed == 1

    def test_close_db_swallows_errors(self, plugin, monkeypatch):
        # A DB layer that raises on close must not crash the scheduler.
        boom = types.ModuleType("django.db")

        class _Boom:
            def close(self):
                raise RuntimeError("nope")
        boom.connection = _Boom()
        monkeypatch.setitem(sys.modules, "django", types.ModuleType("django"))
        monkeypatch.setitem(sys.modules, "django.db", boom)
        plugin._scheduler_close_db()  # must not raise

    def test_sleep_closes_then_waits(self, plugin, monkeypatch):
        conn = _RecordingConn()
        _install_fake_db(monkeypatch, conn)
        order = []

        class _Evt:
            def wait(self, timeout=None):
                order.append(("wait", conn.closed))  # capture closed-count AT wait time
                return True
        out = plugin._scheduler_sleep(_Evt(), 5)
        assert out is True
        # The connection was already closed BEFORE the wait happened.
        assert order == [("wait", 1)]


class TestNoBareWaitInSchedulerLoop:
    """Every sleep in _scheduler_loop must route through _scheduler_sleep (which
    closes the connection). A bare `stop_event.wait(...)` would reintroduce the
    leak, so fail if one appears."""

    def test_no_bare_stop_event_wait(self):
        tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read(), filename=PLUGIN_PY)
        loop = next((n for n in ast.walk(tree)
                     if isinstance(n, ast.FunctionDef) and n.name == "_scheduler_loop"), None)
        assert loop is not None, "_scheduler_loop not found"
        bare = []
        for node in ast.walk(loop):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "wait"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "stop_event"):
                bare.append(node.lineno)
        assert not bare, (
            f"bare stop_event.wait() in _scheduler_loop at lines {bare}; "
            f"route every sleep through _scheduler_sleep so it closes the DB "
            f"connection first (#82/#136)")
