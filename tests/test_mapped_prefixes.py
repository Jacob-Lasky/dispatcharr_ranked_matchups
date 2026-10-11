"""Every sport_prefix a lookup table is keyed on must be one a source emits (#183).

Five `_SPORT_HINT` keys in logos.py (`CWBB`, `NCAAB`, `NCAAS`, `NCAAMS`,
`NCAAWS`) named prefixes no source has ever emitted. Nothing noticed: a dead
key is simply never looked up, so it reads exactly like a live one. They
survived long enough to be quoted verbatim in #104's title and sent that ticket
after work that could not be done.

WHY THIS DOES NOT GREP sources/*.py for string literals: several sources do not
return one. `field_event.py` returns `self.SPORT_PREFIX`, `mls.py` returns
`self._SPORT_PREFIX`, and `soccer.py` returns `self.config.sport_prefix`, so a
literal scan reports their real prefixes as stale. The authoritative set comes
from the registry production uses instead: `plugin._build_sources` with every
`enable_*` toggle the manifest declares switched on, reading `sport_prefix` off
each instance it builds.

The check is one-directional on purpose. Every MAPPED key must be real; a real
prefix with no mapping is fine (the friendlies and NCAA sub-sports are unmapped
deliberately, see logos.py).
"""
from __future__ import annotations

import ast
import importlib
import inspect
import json
import os
import pkgutil
import socket

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"

# The logos.py tables keyed on sport_prefix. SPORTSDB_TOURNAMENT_LEAGUE_IDS is
# empty today, and is here so its first entry is checked like the others.
LOGO_MAPS = ("_SPORT_HINT", "SPORTSDB_LEAGUE_IDS", "SPORTSDB_TOURNAMENT_LEAGUE_IDS")

# Concrete SportSource classes that `_build_sources` never instantiates
# directly, each for a stated reason. Anything else missing from the build is a
# source the registry does not reach, which would make the emitted set short.
NOT_BUILT_DIRECTLY = {
    # Base for NwslSource and LigaMxSource; MLS itself is emitted by
    # MlsEastSource / MlsWestSource / MlsCupSource (#30).
    "MlsSource",
    # Shared base of MlsEastSource / MlsWestSource; its `_conference` is "".
    "MlsStandingsSourceBase",
    # Base of F1 / NASCAR / golf / UFC / tennis; its SPORT_PREFIX is "".
    "FieldEventSource",
}


def _logos():
    return importlib.import_module(f"{PKG_NAME}.logos")


def _all_on_settings():
    """Every source toggle the manifest declares, on, with every API key
    present, so no source is skipped for want of a key."""
    with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as f:
        fields = json.load(f)["fields"]
    settings = {}
    for field in fields:
        fid = field["id"]
        if fid.startswith("enable_"):
            settings[fid] = True
        elif fid.endswith("_api_key"):
            settings[fid] = "test-key-not-real"
    return settings


@pytest.fixture(scope="module")
def built_sources(plugin):
    """`_build_sources` with everything on and the network unreachable, built
    once per module (it is the slow part, and nothing here mutates it).

    Several sources seed playoff strengths from a live fetch at build time;
    each wraps that in try/except, so a refused DNS lookup just leaves the
    prior in place. Refusing it here keeps the test offline and fast, and a
    source that does NOT tolerate it fails loudly rather than reaching out.
    """
    def _offline(*_a, **_kw):
        raise socket.gaierror("network disabled in tests/test_mapped_prefixes.py")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(socket, "getaddrinfo", _offline)
        # Module scope runs before the function-scoped autouse fixture that
        # removes Football-Data.org pacing, so remove it here too.
        soccer = importlib.import_module(f"{PKG_NAME}.sources.soccer")
        mp.setattr(soccer, "_fd_sleep", lambda _seconds: None)
        return plugin._build_sources(_all_on_settings())


def _emitted(sources):
    return {s.sport_prefix for s in sources}


def _stale_keys(emitted):
    """{map name: [keys no source emits]}, empty when every key is live."""
    logos = _logos()
    out = {}
    for name in LOGO_MAPS:
        dead = sorted(set(getattr(logos, name)) - emitted)
        if dead:
            out[name] = dead
    return out


def test_every_mapped_key_is_a_prefix_some_source_emits(built_sources):
    stale = _stale_keys(_emitted(built_sources))
    assert stale == {}, (
        f"logos.py maps sport_prefix keys that no source emits: {stale}. "
        "A dead key is never looked up and reads exactly like a live one "
        "(#104 chased five of them). Remove it, or fix the prefix to the one "
        "the source's `sport_prefix` actually returns."
    )


def test_the_maps_are_populated():
    """Instrument check: an empty map would pass the subset test above
    vacuously and forever."""
    logos = _logos()
    assert len(logos._SPORT_HINT) >= 20
    assert len(logos.SPORTSDB_LEAGUE_IDS) >= 20


def test_derivation_reaches_sources_that_return_no_literal(built_sources):
    """Positive control on the derivation itself. Each of these comes from a
    source whose `sport_prefix` is NOT a string literal (the reason #183 could
    not just grep): a class attribute, a config object, a gender switch."""
    emitted = _emitted(built_sources)
    expected = {
        "NWSL", "LigaMX",                 # mls.py: self._SPORT_PREFIX
        "F1", "NASCAR", "PGA", "UFC",     # field_event.py: self.SPORT_PREFIX
        "ATP", "WTA",
        "EPL", "EFL", "UCL", "BL1",       # soccer.py: self.config.sport_prefix
        "LaLiga", "SerieA", "Ligue1",
        "WC", "EURO", "Eredivisie", "PrimeiraLiga", "BSA",
        "FRIENDLY", "FRIENDLYW",          # friendlies.py: gender switch
        "NCAAMSOC", "NCAAWSOC",           # ncaa_soccer.py: gender switch
        "CLUBFRIENDLY", "EFLCUP", "FACUP",
        "CFB", "CBB", "BOX",              # need an API key to be built
    }
    missing = expected - emitted
    assert not missing, f"the registry build did not reach: {sorted(missing)}"


def test_derivation_is_not_over_inclusive(built_sources):
    """The other direction of the instrument check: the five keys #104
    removed must NOT be in the derived set, or the guard could never fire."""
    emitted = _emitted(built_sources)
    for dead in ("CWBB", "NCAAB", "NCAAS", "NCAAMS", "NCAAWS"):
        assert dead not in emitted, dead


@pytest.mark.parametrize("map_name", LOGO_MAPS)
def test_the_guard_fires_on_an_injected_stale_key(built_sources, monkeypatch, map_name):
    """Mutation: reintroduce one of #104's dead keys into each map in turn and
    the check must name it."""
    logos = _logos()
    value = "NCAA" if map_name == "_SPORT_HINT" else 4607
    monkeypatch.setitem(getattr(logos, map_name), "NCAAB", value)
    assert _stale_keys(_emitted(built_sources)) == {map_name: ["NCAAB"]}


def _concrete_source_classes():
    """Every non-abstract SportSource subclass defined in the sources package.
    Imports each module first so `__subclasses__` sees all of them."""
    sources_pkg = importlib.import_module(f"{PKG_NAME}.sources")
    for info in pkgutil.iter_modules(sources_pkg.__path__):
        importlib.import_module(f"{PKG_NAME}.sources.{info.name}")
    from dispatcharr_ranked_matchups.sources.base import SportSource

    seen, stack = set(), [SportSource]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            if sub not in seen:
                seen.add(sub)
                stack.append(sub)
    return {
        c for c in seen
        if c.__module__.startswith(f"{PKG_NAME}.sources.") and not inspect.isabstract(c)
    }


def test_every_source_class_is_in_the_build(built_sources):
    """Completeness: the emitted set is only authoritative if the build
    reached every source. A concrete class the registry never instantiates
    would leave its prefix out, so a real mapped key would fail above for
    the wrong reason (or, worse, someone would "fix" it by deleting the key)."""
    built = {type(s).__name__ for s in built_sources}
    concrete = {c.__name__ for c in _concrete_source_classes()}
    assert len(concrete) >= 30, "instrument check: class discovery found too few"
    unbuilt = concrete - built - NOT_BUILT_DIRECTLY
    assert not unbuilt, (
        f"concrete sources _build_sources never instantiates: {sorted(unbuilt)}. "
        "Wire them in, or add them to NOT_BUILT_DIRECTLY with the reason."
    )
    # And the allowlist must not hide a class that IS built or does not exist.
    assert NOT_BUILT_DIRECTLY <= concrete - built


_ALLOWED_ROW_PREFIX = {"self.sport_prefix", "self.config.sport_prefix"}


def _odd_row_prefixes(paths):
    """Every place under `paths` (files) that could give a row a prefix other
    than its source's own, plus how many GameRow prefixes were seen.

    Three shapes are reported:
      - `GameRow(sport_prefix=...)` whose value is not the source's own
        prefix (`self.sport_prefix` / `self.config.sport_prefix`);
      - a positional or `**mapping` GameRow, which cannot be checked;
      - `replace(row, sport_prefix=...)` or `replace(row, **mapping)`, the
        dataclasses re-stamp;
      - any assignment to an attribute named `sport_prefix`, including
        `setattr(row, "sport_prefix", ...)`, which re-stamps a row AFTER it
        was built correctly and so slips past the constructor check.
    """
    odd, seen = [], 0
    for path in paths:
        fname = os.path.basename(path)
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(func, ast.Name) else (
                    func.attr if isinstance(func, ast.Attribute) else "")
                # A positional GameRow puts sport_prefix FIRST (its first
                # field), and a `**mapping` can carry it under any value, so
                # neither can be checked: both are reported, which makes the
                # keyword form the only one a source can use.
                if name == "GameRow" and node.args:
                    odd.append((fname, node.lineno, ast.unparse(node)))
                for kw in node.keywords:
                    if kw.arg is None and name in ("GameRow", "replace"):
                        odd.append((fname, node.lineno, ast.unparse(node)))
                    if kw.arg != "sport_prefix":
                        continue
                    if name == "GameRow":
                        seen += 1
                        text = ast.unparse(kw.value)
                        if text not in _ALLOWED_ROW_PREFIX:
                            odd.append((fname, node.lineno, text))
                    elif name == "replace":
                        odd.append((fname, node.lineno, ast.unparse(node)))
                if (name == "setattr" and len(node.args) >= 2
                        and isinstance(node.args[1], ast.Constant)
                        and node.args[1].value == "sport_prefix"):
                    odd.append((fname, node.lineno, ast.unparse(node)))
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
                targets = [node.target]
            for t in targets:
                for sub in ast.walk(t):
                    if isinstance(sub, ast.Attribute) and sub.attr == "sport_prefix":
                        odd.append((fname, node.lineno, ast.unparse(node)))
    return odd, seen


def _source_files():
    src_dir = os.path.join(REPO_ROOT, "sources")
    return [os.path.join(src_dir, f) for f in sorted(os.listdir(src_dir))
            if f.endswith(".py")]


def test_a_row_carries_its_sources_own_prefix():
    """What makes "the instance's `sport_prefix`" equal "the prefix it emits":
    every GameRow a source builds takes its prefix from the source itself, and
    nothing re-stamps a row afterwards. A row carrying some other prefix
    would emit one the registry build above cannot see."""
    odd, seen = _odd_row_prefixes(_source_files())
    assert seen >= 15, "instrument check: found too few GameRow calls"
    assert not odd, f"a row's prefix is not taken from its source: {odd}"


def test_plugin_never_restamps_a_rows_prefix():
    """plugin.py builds exactly one GameRow, `_gamerow_from_row`, which
    re-hydrates a cache row a SOURCE wrote, so its prefix is one this file
    already accounts for and its constructor is not checked. What plugin.py
    must never do is overwrite a row's prefix between fetch and lookup."""
    odd, _seen = _odd_row_prefixes([os.path.join(REPO_ROOT, "plugin.py")])
    odd = [o for o in odd if o[2] != "row.get('sport_prefix') or ''"]
    assert not odd, f"plugin.py re-stamps a row's prefix: {odd}"


def test_the_row_prefix_scan_flags_every_restamp_shape(tmp_path):
    """Mutation for the scan: each way of giving a row a foreign prefix is
    reported, and the two allowed forms are not."""
    fake = tmp_path / "fake.py"
    fake.write_text(
        'GameRow(sport_prefix=self.sport_prefix)\n'
        'GameRow(sport_prefix=self.config.sport_prefix)\n'
        'GameRow(sport_prefix="NCAAB")\n'
        'row.sport_prefix = "NCAAB"\n'
        'setattr(row, "sport_prefix", "NCAAB")\n'
        'dataclasses.replace(row, sport_prefix="NCAAB")\n'
        'GameRow("NCAAB", "NCAA", "A", "B", None, None, start)\n'
        'dataclasses.replace(row, **{"sport_prefix": "NCAAB"})\n'
        'GameRow(**fields)\n'
    )
    odd, seen = _odd_row_prefixes([str(fake)])
    assert seen == 3
    assert sorted(line for _f, line, _t in odd) == [3, 4, 5, 6, 7, 8, 9]
