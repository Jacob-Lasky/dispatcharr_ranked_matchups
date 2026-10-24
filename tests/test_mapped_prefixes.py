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

It covers EVERY table keyed on sport_prefix, not only the logos.py ones the
issue named: plugin.py's soccer match-window set carried two of #104's dead
keys ("NCAAMS", "NCAAWS") from #4 until this guard was widened to it, which is
the same rot one file over. `test_every_prefix_keyed_table_is_registered`
finds such tables by their contents, so a new one cannot be missed silently.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import json
import os
import pkgutil
import re
import sys
import types

import pytest

from tests.conftest import guard_live_network, refuse_requests

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"

# Every module-level table keyed on (or a set of) sport_prefix, as (module,
# attribute). SPORTSDB_TOURNAMENT_LEAGUE_IDS is empty today, and is here so its
# first entry is checked like the others.
PREFIX_TABLES = (
    ("logos", "_SPORT_HINT"),
    ("logos", "SPORTSDB_LEAGUE_IDS"),
    ("logos", "SPORTSDB_TOURNAMENT_LEAGUE_IDS"),
    ("plugin", "_SOCCER_PREFIXES"),
    ("plugin", "_SOCCER_EVERY_GAME_LONG_WINDOW"),
    ("plugin", "_MATCH_WINDOW_OVERRIDE_PRE_MIN"),
    ("plugin", "_MATCH_WINDOW_OVERRIDE_POST_HOURS"),
    ("gamethumbs", "GAMETHUMBS_LEAGUE_SLUGS"),
    ("gamethumbs", "_TEAM_ALIASES"),
    ("rivalries", "_RIVALRIES_BY_SPORT"),
    ("matcher", "_PREFIX_SPORT"),
)

# Tables whose keys overlap the prefix space but are a DIFFERENT key space, so
# a key that is not an emitted prefix is correct there. Each with its reason.
NOT_PREFIX_KEYED = {
    # Keyed on a league-CONTEXT code: the FD.org competition code for soccer
    # ("PL", "ELC", "CL", "EC"), a source's `league_context_code` or
    # `_league_context_code()` otherwise ("MLS_EAST", "NHL_PO"). Several equal
    # the source's prefix by convention ("CBB", "NHL", "WC"), never by lookup.
    # Its own dead-key guard is test_every_league_context_is_one_a_source_uses.
    ("scoring", "LEAGUE_CONTEXTS"),
    # Keyed on the honours.json competition key ("WC", "EURO", "COPA", "UCL"),
    # reached through honours._COMPETITION_MAP from an FD.org code.
    ("honours", "_HONOURS"),
    # Keyed on the FD.org competition code ("WC", "EC", "CL").
    ("honours", "_COMPETITION_MAP"),
    # Keyed on the FD.org competition code of a chained group stage ("WC",
    # "EC"), read by GroupStageSoccerSource.cross_source_chain (#53, #78).
    ("sources.soccer", "_CHAIN_BRACKETS"),
    # Keyed on the FD.org competition code of an international tournament
    # ("WC", "EC") granted the knockout premium (#250); its own coverage guard
    # is tests/test_knockout_premium.py::test_every_chained_tournament_gets_the_premium.
    ("scoring", "_KNOCKOUT_PREMIUM_COMPETITIONS"),
    # Keyed on a LEAGUE_CONTEXTS outcome-band label; "UCL" is the
    # Champions-League-places band, not the UCL prefix.
    ("plugin", "_BAND_DISPLAY"),
    # Keyed on a knockout stage name; "WC" there is a stage, not the World Cup.
    ("scoring", "KNOCKOUT_ROUND_DEPTH"),
    # Keyed on an MLB postseason round; "WC" is the Wild Card Series.
    ("sources.mlb", "_MLB_SERIES_LENGTHS"),
}

# JSON data files whose top-level keys are sport prefixes, with the metadata
# keys each one legitimately carries. Their loaders skip every "_" key, so a
# league key accidentally written with an underscore would silently load as
# nothing; checking the RAW keys, not the loaded table, is what catches that.
PREFIX_KEYED_JSON = {
    "rivalries.json": {"_comment", "_pools"},
    "gamethumbs_aliases.json": {"_comment", "_method", "_not_needed"},
}

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


def _module(name):
    """A package module by its name relative to the package. plugin.py and
    tasks.py cannot be imported normally (the package __init__ pulls Django),
    so they come from the conftest loaders like everywhere else."""
    if name in ("plugin", "tasks"):
        from tests.conftest import _load_plugin_module, _load_tasks_module
        return _load_plugin_module() if name == "plugin" else _load_tasks_module()
    return importlib.import_module(f"{PKG_NAME}.{name}")


def _table(module, attr):
    return getattr(_module(module), attr)


def _logos():
    return _module("logos")


def _all_on_settings():
    """Every source the manifest can switch on, on, with every API key
    present, so no source is skipped for want of a key.

    A league is switched on by its own `enable_*` toggle, or (since #208) by a
    pair select or the NCAAF select in `merged_settings`. Both are read off
    the modules that define them, not listed here, so a league moved into a
    select stays covered without anyone editing this test."""
    merged = _module("merged_settings")
    ncaaf = _module("sources.ncaaf")
    with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as f:
        fields = json.load(f)["fields"]
    settings = {}
    for field in fields:
        fid = field["id"]
        if fid.startswith("enable_"):
            settings[fid] = True
        elif fid.endswith("_api_key"):
            settings[fid] = "test-key-not-real"
    for select_id in merged.LEAGUE_PAIRS:
        settings[select_id] = merged.PAIR_BOTH
    # The widest division set, so every division's games are built.
    settings[merged.NCAAF_SETTING] = max(
        ncaaf.DIVISION_SETS, key=lambda k: len(ncaaf.DIVISION_SETS[k]),
    )
    settings[merged.SETTINGS_SCHEMA_KEY] = merged.SETTINGS_SCHEMA_VERSION
    return settings


@pytest.fixture(scope="module")
def built_sources(plugin):
    """`_build_sources` with everything on and the network unreachable, built
    once per module (it is the slow part, and nothing here mutates it).

    Several sources seed playoff strengths from a live fetch at build time;
    each wraps that in try/except, so a refused request just leaves the
    prior in place. Refusing it here keeps the test offline and fast, and a
    source that does NOT tolerate it fails loudly rather than reaching out.
    """
    with pytest.MonkeyPatch.context() as mp:
        # Module scope runs before the autouse `_no_live_network`, so this
        # build installs the same guard itself.
        _, check_network = guard_live_network(mp)
        refuse_requests(mp, "network disabled in tests/test_mapped_prefixes.py")
        # Module scope runs before the function-scoped autouse fixture that
        # removes Football-Data.org pacing, so remove it here too.
        soccer = importlib.import_module(f"{PKG_NAME}.sources.soccer")
        mp.setattr(soccer, "_fd_sleep", lambda _seconds: None)
        sources = plugin._build_sources(_all_on_settings())
        check_network()
        return sources


def _emitted(sources):
    return {s.sport_prefix for s in sources}


def _stale_keys(emitted):
    """{"module.attr": [keys no source emits]}, empty when every key is live."""
    out = {}
    for module, attr in PREFIX_TABLES:
        dead = sorted(set(_table(module, attr)) - emitted)
        if dead:
            out[f"{module}.{attr}"] = dead
    return out


def test_every_mapped_key_is_a_prefix_some_source_emits(built_sources):
    stale = _stale_keys(_emitted(built_sources))
    assert stale == {}, (
        f"tables keyed on sport_prefix carry keys no source emits: {stale}. "
        "A dead key is never looked up and reads exactly like a live one "
        "(#104 chased five of them). Remove it, or fix the prefix to the one "
        "the source's `sport_prefix` actually returns."
    )


def test_the_soccer_window_holds_none_of_104s_dead_keys(plugin):
    """#183's own class, one file over from logos.py: the soccer match-window
    set carried "NCAAMS"/"NCAAWS" from #4, never matching the NCAA soccer
    sources' real prefixes. Literal, so it does not lean on the derivation."""
    for dead in ("NCAAMS", "NCAAWS"):
        assert dead not in plugin._SOCCER_PREFIXES, dead
        assert dead not in plugin._MATCH_WINDOW_OVERRIDE_PRE_MIN, dead
        assert dead not in plugin._MATCH_WINDOW_OVERRIDE_POST_HOURS, dead


def test_the_maps_are_populated():
    """Instrument check: an empty map would pass the subset test above
    vacuously and forever."""
    logos = _logos()
    assert len(logos._SPORT_HINT) >= 20
    assert len(logos.SPORTSDB_LEAGUE_IDS) >= 20
    assert len(_table("plugin", "_SOCCER_PREFIXES")) >= 10
    assert len(_table("gamethumbs", "GAMETHUMBS_LEAGUE_SLUGS")) >= 20
    assert len(_table("rivalries", "_RIVALRIES_BY_SPORT")) >= 15


# A soccer API endpoint, as a FULL URL. Docstrings that mention an endpoint
# without its scheme ("sports.core.api.espn.com/v2/sports/soccer/...") do not
# count, so a non-soccer module explaining soccer cannot be classified as one.
_SOCCER_API_URL = re.compile(
    r"https://[^\s\"']*(/sports/soccer|football-data\.org)")
# An ESPN "sport/league" slug a class appends to the generic site API base
# (mls.py's `_ESPN_SLUG = "soccer/usa.1"`, overridden per league by NWSL and
# Liga MX).
_SOCCER_ESPN_SLUG = re.compile(r"^soccer/[\w.-]+$")


def _code_strings(code):
    """Every string constant in a code object and the ones nested in it (an
    f-string URL built inside a method, a lambda, a comprehension)."""
    for const in code.co_consts:
        if isinstance(const, str):
            yield const
        elif isinstance(const, types.CodeType):
            yield from _code_strings(const)


def _soccer_api_evidence(src):
    """Where a built source's own code names a soccer API endpoint, or None.

    Read off the classes in the source's MRO that live in the sources
    package: their string attributes (a full URL or an ESPN "soccer/..."
    slug), their method constants, and their modules' globals, including the
    string defaults of helpers they import
    (friendlies.py and english_cup.py reach ESPN soccer only through
    `_espn.sweep_upcoming_scoreboard`'s default `base`). This is the sport
    the source FETCHES, which is what makes it soccer, so it needs no list of
    soccer sources that a new one could be missing from."""
    own = [c for c in type(src).__mro__ if c.__module__.startswith(f"{PKG_NAME}.sources.")]
    # String attributes are read RESOLVED, so a subclass that overrides its
    # base's slug is judged by the slug it actually fetches.
    for name in sorted({n for c in own for n, v in vars(c).items() if isinstance(v, str)}):
        val = getattr(type(src), name)
        if isinstance(val, str) and (_SOCCER_API_URL.search(val) or _SOCCER_ESPN_SLUG.match(val)):
            return f"{type(src).__name__}.{name}"
    for cls in own:
        for name, attr in vars(cls).items():
            fn = getattr(attr, "fget", None) or getattr(attr, "__func__", None) or attr
            code = getattr(fn, "__code__", None)
            if code is not None and any(_SOCCER_API_URL.search(c) for c in _code_strings(code)):
                return f"{cls.__name__}.{name}"
        mod = sys.modules[cls.__module__]
        for name, val in vars(mod).items():
            if name.startswith("__"):
                continue
            if isinstance(val, str) and _SOCCER_API_URL.search(val):
                return f"{cls.__module__}.{name}"
            if inspect.isfunction(val):
                for param in inspect.signature(val).parameters.values():
                    if isinstance(param.default, str) and _SOCCER_API_URL.search(param.default):
                        return f"{cls.__module__}.{name}({param.name}=)"
    return None


def _soccer_prefixes_emitted(sources):
    return {s.sport_prefix for s in sources if _soccer_api_evidence(s)}


def test_every_soccer_prefix_a_source_emits_gets_the_soccer_window(built_sources, plugin):
    """#257: NCAAMSOC, NCAAWSOC, EFLCUP, FACUP and the three friendly prefixes
    were emitted for months while the soccer window never heard of them, so
    every one fell back to the 4 h gridiron window #4 said lets soccer
    false-match the day's preview shows. The emitted soccer set comes from
    what each built source fetches, and must equal the window's set in both
    directions: a soccer prefix outside it gets the gridiron window, and a
    non-soccer prefix inside it gets a soccer one."""
    soccer = _soccer_prefixes_emitted(built_sources)
    assert soccer == set(plugin._SOCCER_PREFIXES), (
        f"missing from plugin._SOCCER_PREFIXES: {sorted(soccer - plugin._SOCCER_PREFIXES)}; "
        f"in it but not soccer: {sorted(plugin._SOCCER_PREFIXES - soccer)}"
    )
    # Literal windows, so a prefix dropped from the post-hours table (or
    # given the wrong one) fails here too. NCAA soccer is 3 h for every game:
    # see _SOCCER_EVERY_GAME_LONG_WINDOW.
    for prefix in soccer:
        want = (5, 3.0) if prefix in ("NCAAMSOC", "NCAAWSOC") else (5, 2.5)
        assert plugin._epg_match_window(prefix, {}) == want, prefix


def test_the_soccer_derivation_sorts_the_known_leagues(built_sources):
    """Instrument check, in literals: an over- or under-inclusive derivation
    would make the test above agree with a wrong table."""
    soccer = _soccer_prefixes_emitted(built_sources)
    assert {
        "EPL", "UCL", "WC", "EURO", "BSA",          # FD.org
        "MLS", "NWSL", "LigaMX",                     # ESPN usa.1 / nwsl / mex.1
        "NCAAMSOC", "NCAAWSOC",                      # ESPN NCAA soccer
        "EFLCUP", "FACUP",                           # only via the sweep's default base
        "FRIENDLY", "FRIENDLYW", "CLUBFRIENDLY",
    } <= soccer
    assert not soccer & {"NFL", "CFB", "CBB", "NHL", "MLB", "NBA", "WNBA",
                         "F1", "PGA", "UFC", "BOX", "ATP"}


def test_the_soccer_derivation_finds_a_new_soccer_source(built_sources, monkeypatch):
    """Mutation: a new source whose only soccer marker is a URL built inside
    its fetch method is classified as soccer, and its prefix is reported
    missing from the window."""
    mod = types.ModuleType(f"{PKG_NAME}.sources._probe_new_soccer")

    class NewSoccerSource:
        sport_prefix = "SPL"

        def fetch_upcoming(self, days_ahead=7):
            return f"https://site.api.espn.com/apis/site/v2/sports/soccer/sco.1?d={days_ahead}"

    NewSoccerSource.__module__ = mod.__name__
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    src = NewSoccerSource()
    assert _soccer_api_evidence(src) == "NewSoccerSource.fetch_upcoming"
    assert "SPL" in _soccer_prefixes_emitted(list(built_sources) + [src])


def test_the_soccer_derivation_follows_an_overridden_slug(monkeypatch):
    """Mutation (codex round 2): a subclass of the MLS family that fetches
    another sport must not be called soccer because its base class's module
    or default slug is. The resolved `_ESPN_SLUG` decides."""
    from dispatcharr_ranked_matchups.sources.nwsl import NwslSource

    mod = types.ModuleType(f"{PKG_NAME}.sources._probe_not_soccer")

    class NotSoccer(NwslSource):
        _ESPN_SLUG = "basketball/nba"

    NotSoccer.__module__ = mod.__name__
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    assert _soccer_api_evidence(object.__new__(NotSoccer)) is None
    assert _soccer_api_evidence(object.__new__(NwslSource)) == "NwslSource._ESPN_SLUG"


def _prefix_like_tables(emitted):
    """{id(obj): "module.attr"} for every module-level dict/set/tuple/list in
    the package with ANY emitted prefix among its string members: the
    contents test for "this table may be keyed on sport_prefix". One is
    enough on purpose: a new table holding one live key and one dead one is
    exactly the case to catch, and a threshold of two let it through. The few
    tables where a prefix-shaped string means something else are classified
    in NOT_PREFIX_KEYED. Keyed on identity so a table re-exported under a
    second module (soccer.py imports scoring.LEAGUE_CONTEXTS) counts once."""
    names = ["plugin", "tasks"] + sorted(
        f[:-3] for f in os.listdir(REPO_ROOT)
        if f.endswith(".py") and f not in ("__init__.py", "plugin.py", "tasks.py")
    )
    sources_pkg = importlib.import_module(f"{PKG_NAME}.sources")
    names += [f"sources.{i.name}" for i in pkgutil.iter_modules(sources_pkg.__path__)]
    found = {}
    for name in names:
        for attr, obj in vars(_module(name)).items():
            if not isinstance(obj, (dict, set, frozenset, tuple, list)):
                continue
            members = {k for k in obj if isinstance(k, str)}
            if members & emitted:
                found.setdefault(id(obj), f"{name}.{attr}")
    return found


def test_every_prefix_keyed_table_is_registered(built_sources):
    """Completeness for PREFIX_TABLES: a table found by its contents must be
    either checked above or listed as a different key space. Without this the
    guard covers only the tables someone remembered, which is how the soccer
    window's dead keys outlived the logos.py ones."""
    found = _prefix_like_tables(_emitted(built_sources))
    registered = {id(_table(m, a)) for m, a in PREFIX_TABLES + tuple(NOT_PREFIX_KEYED)}
    unregistered = sorted(found[i] for i in set(found) - registered)
    assert not unregistered, (
        f"tables that look keyed on sport_prefix but are not checked: "
        f"{unregistered}. Add each to PREFIX_TABLES, or to NOT_PREFIX_KEYED "
        "with the key space it really uses."
    )
    # Instrument check: discovery must see the known tables, or the
    # assertion above is vacuous. An EMPTY table (SPORTSDB_TOURNAMENT_LEAGUE_IDS
    # today) has no members to be found by, so it is the one exception.
    checked = 0
    for module, attr in PREFIX_TABLES + tuple(NOT_PREFIX_KEYED):
        if not _table(module, attr):
            continue
        assert id(_table(module, attr)) in found, f"{module}.{attr}"
        checked += 1
    assert checked >= 10, "instrument check: too few known tables were found"


def test_the_table_discovery_flags_a_new_prefix_table(built_sources, monkeypatch):
    """Mutation for the completeness check: a new prefix-keyed table in some
    module is reported until it is registered."""
    monkeypatch.setattr(
        _module("naming"), "_NEW_PREFIX_TABLE", {"EPL": 1, "NCAAB": 2}, raising=False)
    found = _prefix_like_tables(_emitted(built_sources))
    assert "naming._NEW_PREFIX_TABLE" in found.values()


# Context codes a source writes into `extra["fd_competition_code"]` as a bare
# literal rather than from an attribute, so _context_codes cannot read them off
# the instance. Each is checked to still appear in its file.
LITERAL_CONTEXT_CODES = {"BOXING": "boxing.py"}


def _context_codes(sources):
    """Every code a built source can put in `extra["fd_competition_code"]`,
    which is what plugin.py looks LEAGUE_CONTEXTS up by, read from each way
    the sources name one: an attribute (points_based's `league_context_code`,
    mls.py's `_FD_CODE`, field_event.py's `FD_COMPETITION_CODE`), a method
    (bracket's `_league_context_code`, soccer's `_group_stage_context_code`),
    soccer's `config.fd_code`, or a literal listed in LITERAL_CONTEXT_CODES."""
    used = set(LITERAL_CONTEXT_CODES)
    for src in sources:
        for attr in ("league_context_code", "_FD_CODE", "FD_COMPETITION_CODE"):
            code = getattr(src, attr, None)
            if isinstance(code, str) and code:
                used.add(code)
        for method in ("_league_context_code", "_group_stage_context_code"):
            fn = getattr(src, method, None)
            if callable(fn):
                used.add(fn())
        config = getattr(src, "config", None)
        if getattr(config, "fd_code", None):
            used.add(config.fd_code)
    used.discard("")
    return used


def _dead_contexts(sources):
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
    return sorted(set(LEAGUE_CONTEXTS) - _context_codes(sources))


def test_every_league_context_is_one_a_source_uses(built_sources):
    """LEAGUE_CONTEXTS is the other table #183's rot could hide in: it shares
    keys with the prefix space ("CBB", "NHL") but is looked up by context
    code, so the prefix check above cannot judge it. A context no built source
    names is never consulted and reads exactly like a live one."""
    used = _context_codes(built_sources)
    assert {"PL", "ELC", "CBB", "NHL_PO", "MLS_EAST", "EC_GS", "NCAA_WSOC_CUP"} <= used, \
        "instrument check: the context derivation missed a lookup shape"
    dead = _dead_contexts(built_sources)
    assert not dead, f"LEAGUE_CONTEXTS keys no source looks up: {dead}"


def test_the_context_guard_fires_on_a_dead_context(built_sources, monkeypatch):
    """Mutation (the round-2 review's input): a context under a prefix no
    source uses is reported."""
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
    monkeypatch.setitem(LEAGUE_CONTEXTS, "NCAAB", LEAGUE_CONTEXTS["CBB"])
    assert _dead_contexts(built_sources) == ["NCAAB"]


def test_the_context_guard_does_not_call_a_live_code_dead(built_sources, monkeypatch):
    """The opposite mutation (round-3 review): a context for a code a source
    DOES write, through each attribute shape the derivation reads, must not be
    reported. NWSL comes from mls.py's `_FD_CODE`, F1 from field_event.py's
    `FD_COMPETITION_CODE`, BOXING from a literal; none has a context today."""
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
    for code in ("NWSL", "F1", "BOXING"):
        assert code not in LEAGUE_CONTEXTS, f"instrument check: {code} already has one"
        monkeypatch.setitem(LEAGUE_CONTEXTS, code, LEAGUE_CONTEXTS["CBB"])
    assert _dead_contexts(built_sources) == []


def test_the_literal_context_codes_are_still_written():
    for code, fname in LITERAL_CONTEXT_CODES.items():
        with open(os.path.join(REPO_ROOT, "sources", fname), encoding="utf-8") as f:
            assert f'"{code}"' in f.read(), f"{fname} no longer writes {code!r}"


def _json_prefix_keys(fname):
    with open(os.path.join(REPO_ROOT, fname), encoding="utf-8") as f:
        raw = json.load(f)
    assert isinstance(raw, dict) and raw, f"instrument check: {fname} did not load"
    return set(raw)


def _bad_json_keys(fname, emitted):
    keys = _json_prefix_keys(fname)
    meta = PREFIX_KEYED_JSON[fname]
    return sorted(k for k in keys if k not in meta and k not in emitted)


@pytest.mark.parametrize("fname", sorted(PREFIX_KEYED_JSON))
def test_every_raw_json_key_is_a_prefix_or_known_metadata(built_sources, fname):
    """The loaded tables above cannot see a key their loader DROPPED: a league
    renamed "_BL1" is skipped as metadata, and every alias for it goes dark
    with only the remaining leagues to look at. Every raw key must be an
    emitted prefix or one of the file's listed metadata keys."""
    emitted = _emitted(built_sources)
    assert _json_prefix_keys(fname) & emitted, f"instrument check: {fname} has no live key"
    assert PREFIX_KEYED_JSON[fname] <= _json_prefix_keys(fname), \
        f"{fname}: a listed metadata key is gone; drop it from PREFIX_KEYED_JSON"
    bad = _bad_json_keys(fname, emitted)
    assert not bad, f"{fname}: top-level keys that are neither a prefix nor metadata: {bad}"


def test_the_raw_json_check_fires_on_a_hidden_league(built_sources, monkeypatch):
    """Mutation: "_BL1" in place of "BL1" is reported."""
    real = _json_prefix_keys
    monkeypatch.setitem(
        globals(), "_json_prefix_keys",
        lambda fname: (real(fname) - {"BL1"}) | {"_BL1"})
    assert _bad_json_keys("gamethumbs_aliases.json", _emitted(built_sources)) == ["_BL1"]


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


@pytest.mark.parametrize("module,attr", PREFIX_TABLES)
def test_the_guard_fires_on_an_injected_stale_key(built_sources, monkeypatch, module, attr):
    """Mutation: reintroduce one of #104's dead keys into each table in turn
    and the check must name it (and only it)."""
    table = _table(module, attr)
    if isinstance(table, dict):
        monkeypatch.setitem(table, "NCAAB", next(iter(table.values()), 4607))
    else:
        monkeypatch.setattr(_module(module), attr, frozenset(table) | {"NCAAB"})
    assert _stale_keys(_emitted(built_sources)) == {f"{module}.{attr}": ["NCAAB"]}


def _concrete_source_classes():
    """Every non-abstract SportSource subclass defined in the sources package.
    Imports each module first so `__subclasses__` sees all of them.

    A class counts only if its module really holds it, under ANY name (an
    exported alias still counts, so a real source cannot slip out). The
    probe classes other tests here build (NotSoccer, NewSoccerSource) set
    `__module__` to a fake `sources.` module and stay in `__subclasses__`
    until the garbage collector frees them, which on Python 3.13 (the live
    container's) is after this test runs: it failed on the base branch, 2 of 2
    runs, naming NotSoccer."""
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
        and _held_by_its_module(c)
    }


def _held_by_its_module(cls):
    """True if `cls`'s module reaches it: as a module global under any name,
    or nested at any depth in a class the module reaches (`Alias.Nested`
    counts). A class built inside a function, or claiming a module that was
    never imported, is a test probe and is not reached."""
    module = sys.modules.get(cls.__module__)
    if module is None:
        return False
    seen, stack = set(), list(vars(module).values())
    while stack:
        obj = stack.pop()
        if obj is cls:
            return True
        if isinstance(obj, type) and id(obj) not in seen:
            seen.add(id(obj))
            stack.extend(vars(obj).values())
    return False



def test_discovery_counts_nested_and_aliased_classes_and_drops_probes(monkeypatch):
    """Codex (1.34.0 batch review): reading only module globals dropped a
    source nested in another class, so the completeness test would have
    missed it unbuilt."""
    mod = types.ModuleType(f"{PKG_NAME}.sources._probe_discovery")

    class Outer:
        class Nested:
            pass

    class Aliased:
        pass

    class Unheld:
        pass

    for cls, qualname in ((Outer, "Outer"), (Outer.Nested, "Outer.Nested"),
                          (Aliased, "Aliased"), (Unheld, "Unheld")):
        cls.__module__, cls.__qualname__ = mod.__name__, qualname
    class Hidden:
        class Nested:
            pass

    Hidden.Nested.__module__ = mod.__name__
    Hidden.Nested.__qualname__ = "Hidden.Nested"
    mod.Outer, mod.Exported, mod.Alias = Outer, Aliased, Hidden
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    assert _held_by_its_module(Outer.Nested)
    assert _held_by_its_module(Hidden.Nested)  # reached only as Alias.Nested
    assert _held_by_its_module(Aliased)
    assert not _held_by_its_module(Unheld)
    monkeypatch.delitem(sys.modules, mod.__name__)
    assert not _held_by_its_module(Outer)

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
        # `from .base import GameRow as Row` / `from dataclasses import
        # replace as copy_row`: an alias is the same callable under a new name.
        alias = {"GameRow": "GameRow", "replace": "replace", "setattr": "setattr"}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for a in node.names:
                    if a.name in ("GameRow", "replace") and a.asname:
                        alias[a.asname] = a.name
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                raw_name = func.id if isinstance(func, ast.Name) else (
                    func.attr if isinstance(func, ast.Attribute) else "")
                name = alias.get(raw_name, "")
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
                # setattr naming sport_prefix, or hiding its arguments
                # behind *args where the name cannot be read.
                if name == "setattr" and (
                    any(isinstance(a, ast.Starred) for a in node.args)
                    or (len(node.args) >= 2
                        and isinstance(node.args[1], ast.Constant)
                        and node.args[1].value == "sport_prefix")):
                    odd.append((fname, node.lineno, ast.unparse(node)))
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign, ast.For,
                                   ast.AsyncFor, ast.comprehension)):
                targets = [node.target]
            elif isinstance(node, (ast.With, ast.AsyncWith)):
                targets = [i.optional_vars for i in node.items if i.optional_vars]
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
        'from .base import GameRow as Row\n'
        'Row("NCAAB", "NCAA", "A", "B", None, None, start)\n'
        'from dataclasses import replace as copy_row\n'
        'copy_row(row, **{"sport_prefix": "NCAAB"})\n'
        'setattr(row, *("sport_prefix", "NCAAB"))\n'
        'for row.sport_prefix in ["NCAAB"]: pass\n'
        'with ctx() as row.sport_prefix: pass\n'
    )
    odd, seen = _odd_row_prefixes([str(fake)])
    assert seen == 3
    assert sorted(line for _f, line, _t in odd) == [
        3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 15, 16]
