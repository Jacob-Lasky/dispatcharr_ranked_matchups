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
    # that the prior cache does not list. tests/test_hold_lost_cache.py drives
    # the real readback against the ORM harness.
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


def pytest_configure(config):
    """Register custom markers so `-W error` / `--strict-markers` runs stay
    clean and a typo'd marker name fails loudly instead of silently doing
    nothing."""
    config.addinivalue_line(
        "markers",
        "orm_state(profiles=..., channels=..., rows=...): seed the fake "
        "ChannelProfile ORM used by tests/test_channel_profiles.py",
    )
