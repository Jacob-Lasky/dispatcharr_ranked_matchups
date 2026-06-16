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


def _install_fake_orm(monkeypatch, channels, programs, streams=None):
    """Register fake apps.channels.models / apps.epg.models / django.db.models."""
    chan_mod = types.ModuleType("apps.channels.models")
    chan_mod.Channel = types.SimpleNamespace(objects=FakeManager(channels))
    chan_mod.Stream = types.SimpleNamespace(objects=FakeManager(streams or []))
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


class TestStreamNameLookup:
    """Path C: streams whose NAME names both teams become stream-granular
    candidates even when no channel (name or EPG) names the game. This is the
    'matchup lives only in the stream name' failure mode."""

    def test_stream_named_both_teams_becomes_candidate(self, plugin, monkeypatch):
        # Generic channel, no EPG, but a stream names both teams.
        chans = [_Row(id=9, name="USA Sports 24/7", epg_data_id=None)]
        streams = [
            _Row(id=500, name="USA Soccer10: FIFA World Cup : Iran vs New Zealand @ 9pm"),
            _Row(id=501, name="Random Movie Stream"),  # noise
        ]
        _install_fake_orm(monkeypatch, chans, programs=[], streams=streams)
        game = _game("Iran", "New Zealand", prefix="WC")
        out = plugin._build_epg_lookup()(game)
        # Only the both-team stream becomes a candidate, carrying its stream_id
        # and a negative sentinel channel_id (never a real PK).
        sc = [c for c in out if c.stream_id is not None]
        assert [c.stream_id for c in sc] == [500]
        assert sc[0].channel_id == -500
        assert "Iran" in sc[0].channel_name and "New Zealand" in sc[0].channel_name

    def test_stream_naming_one_team_does_not_match(self, plugin, monkeypatch):
        streams = [_Row(id=600, name="Iran National Team 24/7")]  # only home
        _install_fake_orm(monkeypatch, [], programs=[], streams=streams)
        game = _game("Iran", "New Zealand", prefix="WC")
        out = plugin._build_epg_lookup()(game)
        assert [c for c in out if c.stream_id is not None] == []

    def test_feed_prefix_does_not_fake_a_match(self, plugin, monkeypatch):
        # Confirmed false positive: the feed label 'USA Soccer09' supplies a
        # bogus 'USA' home hit for United States, while the away token (Australia,
        # the real opponent) appears in a DIFFERENT matchup. The two tokens are
        # in different ':'-segments, so the stream must NOT become a candidate.
        streams = [_Row(id=800, name="USA Soccer09: Australia vs Turkey ( TSN5 ) @ 12am")]
        _install_fake_orm(monkeypatch, [], programs=[], streams=streams)
        game = _game("United States", "Australia", prefix="WC")
        out = plugin._build_epg_lookup()(game)
        assert [c for c in out if c.stream_id is not None] == []

    def test_feed_prefix_with_real_matchup_still_matches(self, plugin, monkeypatch):
        # The same feed-label shape is legitimate when the matchup names BOTH the
        # game's teams in one segment: 'USA Soccer10: ... Iran vs New Zealand'.
        streams = [_Row(id=801, name="USA Soccer10: FIFA World Cup : Iran vs New Zealand @ 9pm")]
        _install_fake_orm(monkeypatch, [], programs=[], streams=streams)
        game = _game("Iran", "New Zealand", prefix="WC")
        out = plugin._build_epg_lookup()(game)
        assert [c.stream_id for c in out if c.stream_id is not None] == [801]

    def test_kickoff_time_colon_does_not_split_matchup(self, plugin, monkeypatch):
        # 'FIFA World Cup 2026 18: Iran 02:00 New Zealand': the kickoff time
        # '02:00' contains a colon, but it must NOT split the matchup segment
        # (else both teams land in different segments and the feed is wrongly
        # rejected). The label colon after '18' is the only real boundary.
        streams = [_Row(id=802, name="FIFA World Cup 2026 18: Iran 02:00 New Zealand")]
        _install_fake_orm(monkeypatch, [], programs=[], streams=streams)
        game = _game("Iran", "New Zealand", prefix="WC")
        out = plugin._build_epg_lookup()(game)
        assert [c.stream_id for c in out if c.stream_id is not None] == [802]

    def test_field_event_stream_single_sided(self, plugin, monkeypatch):
        streams = [
            _Row(id=700, name="PPV: UFC 250 Topuria vs Gaethje 4K"),
            _Row(id=701, name="Premier League Arsenal v Chelsea"),  # noise
        ]
        _install_fake_orm(monkeypatch, [], programs=[], streams=streams)
        game = _game("UFC 250: Topuria vs Gaethje", "Field",
                     extra={"is_field_event": True})
        out = plugin._build_epg_lookup()(game)
        sc = [c for c in out if c.stream_id is not None]
        assert [c.stream_id for c in sc] == [700]
