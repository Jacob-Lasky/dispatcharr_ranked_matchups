"""Lookup-side coverage for #127: `_build_epg_lookup` must match field events
(away == 'Field') on the event name alone.

`_build_epg_lookup` does its Django imports lazily inside the closure, so we
inject a tiny in-memory fake ORM into sys.modules before calling it. The fake
`Q` builds an AND/OR predicate tree and evaluates it against plain row objects,
which is enough to prove the query SHAPE the lookup builds:

  - two-team games keep the Path-B `home AND away` channel-name gate, and
  - field events drop it to `home` alone (the #127 fix).

Before the fix, Path B ANDed the unmatchable 'Field' sentinel, so a field-event
broadcast advertised only in the channel NAME (no program data) was invisible.
"""

import importlib.util
import os
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


# --------------------------------------------------------------------------- #
# Minimal fake ORM
# --------------------------------------------------------------------------- #
class FakeQ:
    """AND/OR predicate tree over Django-style `field__lookup=value` kwargs."""

    def __init__(self, **kwargs):
        self._tests = list(kwargs.items())
        self._or = []
        self._and = []

    def _empty(self):
        return not self._tests and not self._or and not self._and

    def __or__(self, other):
        if self._empty():
            return other
        if other._empty():
            return self
        n = FakeQ()
        n._or = [self, other]
        return n

    __ior__ = __or__

    def __and__(self, other):
        if self._empty():
            return other
        if other._empty():
            return self
        n = FakeQ()
        n._and = [self, other]
        return n

    __iand__ = __and__

    def matches(self, row):
        if self._or:
            return any(q.matches(row) for q in self._or)
        if self._and:
            return all(q.matches(row) for q in self._and)
        if not self._tests:
            return True
        for key, val in self._tests:
            field, _, lookup = key.partition("__")
            actual = getattr(row, field, "" if "time" not in field else None)
            if lookup == "icontains":
                if str(val).lower() not in str(actual or "").lower():
                    return False
            elif lookup == "startswith":
                if not str(actual or "").startswith(str(val)):
                    return False
            elif lookup == "in":
                if actual not in val:
                    return False
            elif lookup == "lt":
                if not (actual is not None and actual < val):
                    return False
            elif lookup == "gt":
                if not (actual is not None and actual > val):
                    return False
            else:  # exact
                if actual != val:
                    return False
        return True


class FakeManager:
    def __init__(self, rows, includes=None, excludes=None):
        self._rows = rows
        self._inc = list(includes or [])
        self._exc = list(excludes or [])

    def filter(self, *qs, **kw):
        inc = self._inc + list(qs) + ([FakeQ(**kw)] if kw else [])
        return FakeManager(self._rows, inc, self._exc)

    def exclude(self, *qs, **kw):
        exc = self._exc + list(qs) + ([FakeQ(**kw)] if kw else [])
        return FakeManager(self._rows, self._inc, exc)

    def only(self, *a):
        return self

    def __iter__(self):
        for r in self._rows:
            if all(q.matches(r) for q in self._inc) and not any(q.matches(r) for q in self._exc):
                yield r


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _install_fake_orm(monkeypatch, channels, programs):
    """Register fake apps.channels.models / apps.epg.models / django.db.models."""
    chan_mod = types.ModuleType("apps.channels.models")
    chan_mod.Channel = types.SimpleNamespace(objects=FakeManager(channels))
    epg_mod = types.ModuleType("apps.epg.models")
    epg_mod.ProgramData = types.SimpleNamespace(objects=FakeManager(programs))
    dj_mod = types.ModuleType("django.db.models")
    dj_mod.Q = FakeQ
    for name, mod in [
        ("apps", types.ModuleType("apps")),
        ("apps.channels", types.ModuleType("apps.channels")),
        ("apps.channels.models", chan_mod),
        ("apps.epg", types.ModuleType("apps.epg")),
        ("apps.epg.models", epg_mod),
        ("django", types.ModuleType("django")),
        ("django.db", types.ModuleType("django.db")),
        ("django.db.models", dj_mod),
    ]:
        monkeypatch.setitem(sys.modules, name, mod)


@pytest.fixture(scope="module")
def plugin():
    if f"{PKG_NAME}._util" not in sys.modules:
        uspec = importlib.util.spec_from_file_location(
            f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py"))
        umod = importlib.util.module_from_spec(uspec)
        sys.modules[f"{PKG_NAME}._util"] = umod
        uspec.loader.exec_module(umod)
    if f"{PKG_NAME}.plugin" in sys.modules:
        return sys.modules[f"{PKG_NAME}.plugin"]
    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.plugin", os.path.join(REPO_ROOT, "plugin.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


def _game(home, away, *, prefix="UFC", extra=None):
    start = datetime(2026, 6, 15, 20, 0, tzinfo=timezone.utc)
    return types.SimpleNamespace(
        home=home, away=away, sport_prefix=prefix, start_time=start,
        extra=extra if extra is not None else {})


class TestFieldEventLookup:
    def test_field_event_matches_channel_named_for_event(self, plugin, monkeypatch):
        # The #127 case: a broadcast advertised ONLY in the channel name, no
        # program data. Old code ANDed the 'Field' sentinel and found nothing.
        chans = [_Row(id=1, name="UFC 250: Topuria vs Gaethje (PPV)", epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("UFC 250: Topuria vs Gaethje", "Field",
                     extra={"is_field_event": True})
        out = plugin._build_epg_lookup()(game)
        assert [c.channel_id for c in out] == [1]

    def test_field_event_matches_program_title(self, plugin, monkeypatch):
        # Path A: a generic channel carrying a program that names the event.
        win = datetime(2026, 6, 15, 20, 0, tzinfo=timezone.utc)
        chans = [_Row(id=7, name="BT Sport 1", epg_data_id=99)]
        progs = [_Row(id=1, title="UFC 250: Topuria vs Gaethje",
                      start_time=win, end_time=win + timedelta(hours=2), epg_id=99)]
        _install_fake_orm(monkeypatch, chans, progs)
        game = _game("UFC 250: Topuria vs Gaethje", "Field")  # sentinel only, no extra
        out = plugin._build_epg_lookup()(game)
        assert any(c.channel_id == 7 and "UFC 250" in c.program_title for c in out)

    def test_two_team_game_keeps_both_sides_gate(self, plugin, monkeypatch):
        # Regression: a channel naming only ONE team must NOT match a two-team
        # game (the AND gate is intact for non-field events).
        chans = [_Row(id=3, name="Arsenal TV", epg_data_id=None)]  # only home
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Arsenal", "Chelsea", prefix="EPL")
        out = plugin._build_epg_lookup()(game)
        assert out == []

    def test_two_team_game_matches_channel_with_both(self, plugin, monkeypatch):
        chans = [_Row(id=4, name="EPL01: Arsenal v Chelsea", epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Arsenal", "Chelsea", prefix="EPL")
        out = plugin._build_epg_lookup()(game)
        assert [c.channel_id for c in out] == [4]
