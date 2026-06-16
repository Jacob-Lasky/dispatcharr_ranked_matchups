#!/usr/bin/env python3
"""Offline replay harness for the matcher/lookup, NO Dispatcharr runtime.

Runs the plugin's REAL code (`matcher.match_games_to_channels` +
`plugin._build_epg_lookup`) against a JSON *snapshot* of a Dispatcharr
instance's channels + EPG, served through a tiny in-memory ORM. This produces
the exact candidate set and match verdict the plugin would produce in-process,
without ever touching the live container (whose plugin reloads/discovery wedge
the worker, see the dispatcharr skill).

This is a faithful replay, not a mock: the matching code is the production code;
only the ORM backend is swapped for an in-memory store over the snapshot.

Snapshot format (see tools/export_snapshot.py / the skill):
  {"channels":[{"id","name","tvg_id","epg_data_id"}...],
   "programs":[{"id","title","start_time"(iso),"end_time"(iso),"epg_id"}...]}

Usage:
  python tools/replay_match.py SNAPSHOT.json --cache CACHE.json
      Replay every scored game from a plugin cache.json and compare each
      match verdict against the channel_name_current the cache recorded.
  python tools/replay_match.py SNAPSHOT.json \
      --game "UFC Freedom 250: Topuria vs. Gaethje" --away Field \
      --prefix UFC --start 2026-06-15T00:00:00Z
      Replay a single synthetic game (away defaults to the Field sentinel
      when omitted, i.e. a field event).
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PKG = "dispatcharr_ranked_matchups"


# --------------------------------------------------------------------------- #
# In-memory ORM (Django-shaped subset the lookup uses)
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
            actual = getattr(row, field, None)
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
    def __init__(self, rows, inc=None, exc=None):
        self._rows = rows
        self._inc = list(inc or [])
        self._exc = list(exc or [])

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


def _dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# --------------------------------------------------------------------------- #
# Wiring
# --------------------------------------------------------------------------- #
def load_snapshot(path):
    data = json.load(open(path))
    chans = [_Row(id=c["id"], name=c["name"] or "", tvg_id=c.get("tvg_id") or "",
                  epg_data_id=c["epg_data_id"]) for c in data["channels"]]
    progs = [_Row(id=p["id"], title=p["title"] or "", epg_id=p["epg_id"],
                  start_time=_dt(p["start_time"]), end_time=_dt(p["end_time"]))
             for p in data["programs"]]
    # Streams power Path C (stream-name matching). Older snapshots predate the
    # key; default to empty so they still replay (Path C just finds nothing).
    streams = [_Row(id=s["id"], name=s["name"] or "")
               for s in data.get("streams", [])]
    return chans, progs, streams


def install_orm(chans, progs, streams):
    chan_mod = types.ModuleType("apps.channels.models")
    chan_mod.Channel = SimpleNamespace(objects=FakeManager(chans))
    chan_mod.Stream = SimpleNamespace(objects=FakeManager(streams))
    epg_mod = types.ModuleType("apps.epg.models")
    epg_mod.ProgramData = SimpleNamespace(objects=FakeManager(progs))
    dj = types.ModuleType("django.db.models")
    dj.Q = FakeQ
    for name, mod in [
        ("apps", types.ModuleType("apps")),
        ("apps.channels", types.ModuleType("apps.channels")),
        ("apps.channels.models", chan_mod),
        ("apps.epg", types.ModuleType("apps.epg")),
        ("apps.epg.models", epg_mod),
        ("django", types.ModuleType("django")),
        ("django.db", types.ModuleType("django.db")),
        ("django.db.models", dj),
    ]:
        sys.modules[name] = mod


def load_pkg():
    """Load real _util + matcher + plugin by file path (no Django import)."""
    if f"{PKG}._util" not in sys.modules:
        s = importlib.util.spec_from_file_location(f"{PKG}._util", os.path.join(REPO_ROOT, "_util.py"))
        m = importlib.util.module_from_spec(s)
        sys.modules[f"{PKG}._util"] = m
        s.loader.exec_module(m)
    if PKG not in sys.modules:
        pkg = types.ModuleType(PKG)
        pkg.__path__ = [REPO_ROOT]
        sys.modules[PKG] = pkg
    out = {}
    for sub in ("matcher", "plugin"):
        full = f"{PKG}.{sub}"
        if full not in sys.modules:
            s = importlib.util.spec_from_file_location(full, os.path.join(REPO_ROOT, f"{sub}.py"))
            m = importlib.util.module_from_spec(s)
            sys.modules[full] = m
            s.loader.exec_module(m)
        out[sub] = sys.modules[full]
    return out["matcher"], out["plugin"]


def game_from_cache(g):
    return SimpleNamespace(
        home=g.get("home", ""), away=g.get("away", ""),
        sport_prefix=g.get("sport_prefix", ""), sport_label=g.get("sport_label", ""),
        start_time=_dt(g.get("start_time_utc")) or datetime.now(timezone.utc),
        extra=g.get("extra") or {})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("snapshot")
    ap.add_argument("--cache", help="plugin cache.json: replay every scored game")
    ap.add_argument("--game", help="single synthetic game home/event name")
    ap.add_argument("--away", default="Field", help="away team (default: Field sentinel)")
    ap.add_argument("--prefix", default="UFC")
    ap.add_argument("--start", default="2026-06-15T00:00:00Z")
    args = ap.parse_args()

    chans, progs, streams = load_snapshot(args.snapshot)
    install_orm(chans, progs, streams)
    matcher, plugin = load_pkg()
    print(f"snapshot: {len(chans)} channels, {len(progs)} programs, "
          f"{len(streams)} streams\n")

    if args.cache:
        cache = json.load(open(args.cache))
        games = [game_from_cache(g) for g in cache.get("games", [])]
        cached_match = [g.get("channel_name_current") for g in cache.get("games", [])]
        results = matcher.match_games_to_channels(
            [(g, None, None) for g in games], plugin._build_epg_lookup(),
            api_key="", model="m")  # no key -> LLM tier degrades to fallback_first
        for g, r, was in zip(games, results, cached_match):
            tag = "OK " if bool(r.channel_id) == bool(was) else "DIFF"
            print(f"[{tag}] {g.sport_prefix} {g.home[:40]!r}")
            print(f"       replay: {r.method} -> {r.channel_name!r} "
                  f"({len(r.channel_ids)} chans, {len(r.stream_ids)} streams)")
            print(f"       cache : {was!r}")
        return

    home = args.game or "UFC Freedom 250: Topuria vs. Gaethje"
    extra = {"is_field_event": True} if args.away == "Field" else {}
    game = SimpleNamespace(home=home, away=args.away, sport_prefix=args.prefix,
                           sport_label=args.prefix, start_time=_dt(args.start), extra=extra)
    print(f"GAME: {game.sport_prefix} {home!r} vs {game.away!r}  @ {game.start_time.isoformat()}")
    print("home keywords:", matcher._team_keywords(home))
    cands = plugin._build_epg_lookup()(game)
    print(f"\ncandidates in window: {len(cands)}")
    field = game.extra.get("is_field_event") or game.away == "Field"
    away = None if field else game.away
    t1 = matcher._regex_filter_channel_name(cands, home, away)
    print(f"tier-1 (channel name) matches: {len(t1)}")
    for c in t1[:15]:
        print(f"   [{c.channel_id}] {c.channel_name!r}")
    res = matcher.match_games_to_channels([(game, None, None)], plugin._build_epg_lookup(),
                                          api_key="", model="m")[0]
    print(f"\nVERDICT: method={res.method} primary={res.channel_name!r} "
          f"channels={res.channel_ids} streams={res.stream_ids}")


if __name__ == "__main__":
    main()
