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

# Register the package under its directory name so absolute imports work.
PKG_NAME = os.path.basename(REPO_ROOT)
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
