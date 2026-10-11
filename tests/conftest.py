"""Make `import dispatcharr_ranked_matchups.scoring` etc. resolve when pytest
is invoked from the repo root (this directory is the package itself, not its
parent). We expose the package under its own name by adding the parent dir to
sys.path with a symlink-like alias."""

import json
import os
import sys
import types
import importlib.util

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARENT = os.path.dirname(REPO_ROOT)

# Register the package under the canonical name so absolute imports
# work even when the repo lives in a worktree whose directory name
# isn't `dispatcharr_ranked_matchups` (e.g., `dispatcharr_ranked_matchups-phase-e`).
PKG_NAME = "dispatcharr_ranked_matchups"
if PKG_NAME not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        PKG_NAME,
        os.path.join(REPO_ROOT, "__init__.py"),
        submodule_search_locations=[REPO_ROOT],
    )
    # We don't actually exec the package __init__ (it imports `.plugin` which
    # pulls Django models). Tests import the leaf submodules they need.
    pkg = types.ModuleType(PKG_NAME)
    pkg.__path__ = [REPO_ROOT]
    pkg.__file__ = os.path.join(REPO_ROOT, "__init__.py")
    pkg.__spec__ = spec
    sys.modules[PKG_NAME] = pkg


import pytest  # noqa: E402  (must follow the package registration above)


# ---------------------------------------------------------------------------
# Shared module loaders.
#
# plugin.py and tasks.py cannot be imported normally: the package __init__
# imports `.plugin`, which pulls Django models and starts the scheduler and
# reaper threads. Loading the leaf modules by file path sidesteps that.
#
# These live HERE and nowhere else. Five test modules used to carry their own
# byte-identical copy of the plugin loader and three carried the tasks loader,
# which is one fact in eight places: if any copy drifted, that file's tests
# would quietly load a differently-constructed module and nothing would say so.
# ---------------------------------------------------------------------------

def _load_plugin_module():
    if f"{PKG_NAME}.plugin" in sys.modules:
        return sys.modules[f"{PKG_NAME}.plugin"]
    util_spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py")
    )
    util_mod = importlib.util.module_from_spec(util_spec)
    sys.modules[f"{PKG_NAME}._util"] = util_mod
    util_spec.loader.exec_module(util_mod)

    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.plugin", os.path.join(REPO_ROOT, "plugin.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_tasks_module():
    if f"{PKG_NAME}.tasks" in sys.modules:
        return sys.modules[f"{PKG_NAME}.tasks"]
    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.tasks", os.path.join(REPO_ROOT, "tasks.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.tasks"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def plugin():
    return _load_plugin_module()


@pytest.fixture(scope="module")
def plugin_mod():
    """Alias kept for test_async_dispatch's existing parameter name."""
    return _load_plugin_module()


@pytest.fixture(scope="module")
def tasks_mod():
    return _load_tasks_module()


@pytest.fixture(autouse=True)
def _no_real_sleeps(monkeypatch):
    """Neutralize Football-Data.org request pacing for the whole suite.

    `sources/soccer._fd_get` paces calls at _FD_MIN_INTERVAL_S (6.5s) and backs
    off on 429, which is correct against the live 10-req/min free tier and
    catastrophic in tests: leaving it live took the suite from 7 seconds to 105.

    Autouse so a NEW test that happens to exercise an FD.org path cannot
    silently reintroduce that cost. Tests that assert on pacing behavior patch
    `soccer._fd_sleep` themselves to record durations; this fixture only removes
    the wall-clock wait, it does not change control flow.
    """
    try:
        from dispatcharr_ranked_matchups.sources import soccer
    except Exception:
        # Django-less contexts where soccer isn't importable: nothing to patch.
        return
    monkeypatch.setattr(soccer, "_fd_sleep", lambda _seconds: None)


@pytest.fixture(autouse=True)
def _isolated_last_run(tmp_path, monkeypatch):
    """Point tasks.LAST_RUN_PATH at a per-test temp file (#227).

    Every _run_under_lock call records its outcome to last_run.json beside the
    code, and show_status reads it back. Left live, a test that runs a fake
    pipeline would write a real file into the repo checkout, and a later
    show_status test would read whatever an earlier test left there, so its
    result would depend on test order. Autouse so a new test cannot forget."""
    tasks = _load_tasks_module()
    monkeypatch.setattr(tasks, "LAST_RUN_PATH", str(tmp_path / "last_run.json"))


@pytest.fixture(autouse=True)
def _isolated_odds_cache(tmp_path, monkeypatch):
    """Point sources/_odds.CACHE_PATH at a per-test temp file (#200), for the
    same reason as _isolated_last_run: left live, a test would write
    odds_cache.json into the checkout and a later test would read it."""
    import importlib
    odds = importlib.import_module(f"{PKG_NAME}.sources._odds")
    monkeypatch.setattr(odds, "CACHE_PATH", str(tmp_path / "odds_cache.json"))


@pytest.fixture
def refresh_env(plugin, tmp_path, monkeypatch):
    """_action_refresh with no network, no DB and a matcher that matches every
    game to channel 777. Returns (write_prior_cache, run, read_cache); `run`
    takes the source objects to refresh and any settings to override.

    Lives here, not in one test module, because more than one module drives a
    real refresh end to end (test_keep_on_air for #240, test_sources for the
    cup rivalry bonus, #192, test_plugin_helpers for the #198 rank-field
    wiring), and a per-module copy of this stubbing is one fact in several
    places that would drift."""
    import importlib
    matcher = importlib.import_module("dispatcharr_ranked_matchups.matcher")

    cache_path = tmp_path / "cache.json"
    monkeypatch.setattr(plugin, "CACHE_PATH", str(cache_path))
    monkeypatch.setattr(plugin, "_streams_in_groups", lambda _ids: set())
    def build_lookup(**_kw):
        def lookup(_game):
            return []
        lookup.stats = {}
        return lookup
    monkeypatch.setattr(plugin, "_build_epg_lookup", build_lookup)
    monkeypatch.setattr(plugin, "_resolve_key", lambda *_a, **_kw: "")
    # The DB half of the previous slate (#264): no DB here, and nothing on air
    # that the prior cache does not list. The #264 scenarios in
    # tests/test_apply_empty_slate.py drive the real readback through the ORM
    # harness.
    monkeypatch.setattr(plugin, "_published_rows", lambda: ([], 0))

    def fake_match(scored, _lookup, _key, _model, widen=False):
        return [
            matcher.MatchResult(game_index=i, channel_id=777, channel_ids=[777],
                                stream_ids=[], channel_name="NCAAF 080",
                                method="regex_strict")
            for i in range(len(scored))
        ]
    monkeypatch.setattr(matcher, "match_games_to_channels", fake_match)

    def write_prior(games):
        cache_path.write_text(json.dumps({"games": games, "refreshed_at": None}))

    def run(sources, **settings):
        monkeypatch.setattr(plugin, "_build_sources", lambda _s: sources)
        base = {"lookahead_days": 7, "weight_importance": 0, "max_games": 10}
        base.update(settings)
        return plugin._action_refresh(base)

    def read():
        return json.loads(cache_path.read_text())

    return write_prior, run, read


@pytest.fixture(autouse=True)
def _isolated_numbering_state(tmp_path, monkeypatch):
    """Point plugin.NUMBERING_STATE_PATH at a per-test temp file (#219), for the
    same reason as _isolated_last_run: apply remembers its kickoff floor there,
    and a test driving apply must neither write into the checkout nor inherit a
    floor an earlier test left behind."""
    plugin = _load_plugin_module()
    monkeypatch.setattr(
        plugin, "NUMBERING_STATE_PATH", str(tmp_path / "numbering_state.json"),
    )


# ---------------------------------------------------------------------------
# The HTTP transport seam (#271).
#
# Every outbound call goes through _http.get_within_deadline (requests, as
# `requests_mod.Session().get`) or _http.urlopen_read (urllib, as
# `OpenerDirector.open`). A stub on `requests.get` or `urllib.request.urlopen`
# is no longer reached, so tests stub the transport here. Both helpers keep the
# old stubs' signatures: `fake(url, **kwargs)` and `fake(request, timeout=...)`.
# ---------------------------------------------------------------------------

def stub_requests_get(monkeypatch, fake):
    """Answer every requests GET in the plugin with `fake(url, **kwargs)`."""
    import requests
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: fake(url, **kw))


def stub_urlopen(monkeypatch, fake):
    """Answer every urllib open in the plugin with `fake(request, timeout=...)`."""
    import urllib.request
    monkeypatch.setattr(
        urllib.request.OpenerDirector, "open",
        lambda self, fullurl, data=None, timeout=None: fake(fullurl, timeout=timeout),
    )



def refuse_requests(monkeypatch, why):
    """Every plugin HTTP call fails as an unreachable feed would, at the seam.

    DO NOT refuse by replacing `socket.getaddrinfo` instead: that replaces
    `_no_live_network`'s own hook, so a call that slipped past the stubs is
    no longer recorded and the guard passes vacuously (1.34.0 batch review).
    `test_no_test_replaces_the_socket_layer` enforces it."""
    import urllib.error

    import requests

    def refuse_get(url, **_kw):
        raise requests.ConnectionError(why)

    def refuse_open(request, timeout=None):
        raise urllib.error.URLError(why)

    stub_requests_get(monkeypatch, refuse_get)
    stub_urlopen(monkeypatch, refuse_open)


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
PROXY_ENV_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
                  "http_proxy", "https_proxy", "all_proxy", "no_proxy")


def clear_proxy_env(monkeypatch):
    """A proxy in the environment would make a live call look local (the
    socket goes to 127.0.0.1:3128 and the proxy fetches the internet), so every
    call in a test goes direct and the guard checks the real target."""
    for name in PROXY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def guard_live_network(monkeypatch):
    """Install the live-network hooks on `monkeypatch`. Returns (attempts,
    check): `attempts` records every non-loopback lookup or connect (each is
    also refused), and `check()` fails if any happened OR if something
    replaced the hooks since, which would have switched the recording off.

    Shared by the autouse `_no_live_network` and by module-scoped fixtures
    (`tests/test_mapped_prefixes.py::built_sources`), which run before any
    function-scoped fixture and so would otherwise build with no guard."""
    import socket

    clear_proxy_env(monkeypatch)
    attempts = []
    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def _host(address):
        return address[0] if isinstance(address, tuple) else address

    def getaddrinfo(host, *args, **kwargs):
        if host is not None and str(host) not in _LOOPBACK_HOSTS:
            attempts.append(f"getaddrinfo({host!r})")
            raise socket.gaierror(socket.EAI_NONAME, "live network refused in tests")
        return real_getaddrinfo(host, *args, **kwargs)

    def _guard(real):
        def connect(self, address):
            if self.family in (socket.AF_INET, socket.AF_INET6) and _host(address) not in _LOOPBACK_HOSTS:
                attempts.append(f"connect({address!r})")
                raise OSError("live network refused in tests")
            return real(self, address)
        return connect

    hooks = {
        (socket, "getaddrinfo"): getaddrinfo,
        (socket.socket, "connect"): _guard(real_connect),
        (socket.socket, "connect_ex"): _guard(real_connect_ex),
    }
    for (owner, name), hook in hooks.items():
        monkeypatch.setattr(owner, name, hook)

    def check():
        replaced = [name for (owner, name), hook in hooks.items()
                    if getattr(owner, name) is not hook]
        # Behavioural half of tests/test_http_deadline.py's AST scan: catches a
        # replacement however it was spelled, while it is still in place.
        assert not replaced, (
            f"the live-network hooks were replaced ({replaced}), which stops "
            "them recording; refuse at the HTTP seam with refuse_requests instead"
        )
        assert not attempts, f"test reached the live network: {attempts}"

    return attempts, check


@pytest.fixture(autouse=True)
def _no_live_network(monkeypatch):
    """Fail any test that reaches past this machine.

    Moving the HTTP seam (#271) turned every stub on the OLD one into a live
    call, and a live call that fails comes back as "no data from this feed",
    which plenty of assertions accept. Refusing here would be just as quiet,
    for the same reason, so the attempt is RECORDED and the test fails at
    teardown instead. Loopback stays open for the real-server tests.
    Teardown runs before `monkeypatch` undoes the test's own patches, so a
    test that replaced a hook through the shared `monkeypatch` is caught."""
    _, check = guard_live_network(monkeypatch)
    yield
    check()


def pytest_configure(config):
    """Register custom markers so `-W error` / `--strict-markers` runs stay
    clean and a typo'd marker name fails loudly instead of silently doing
    nothing."""
    config.addinivalue_line(
        "markers",
        "orm_state(profiles=..., channels=..., rows=...): seed the fake "
        "ChannelProfile ORM used by tests/test_channel_profiles.py",
    )
