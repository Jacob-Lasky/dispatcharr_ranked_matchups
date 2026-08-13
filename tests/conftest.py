"""Make `import dispatcharr_ranked_matchups.scoring` etc. resolve when pytest
is invoked from the repo root (this directory is the package itself, not its
parent). We expose the package under its own name by adding the parent dir to
sys.path with a symlink-like alias."""

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


def pytest_configure(config):
    """Register custom markers so `-W error` / `--strict-markers` runs stay
    clean and a typo'd marker name fails loudly instead of silently doing
    nothing."""
    config.addinivalue_line(
        "markers",
        "orm_state(profiles=..., channels=..., rows=...): seed the fake "
        "ChannelProfile ORM used by tests/test_channel_profiles.py",
    )
