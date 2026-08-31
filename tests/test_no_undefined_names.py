"""Static gate against undefined names in the PRODUCTION modules.

WHY THIS FILE EXISTS, and it is not hypothetical: on 2026-08-31 a refactor of
`_action_apply` deleted the line binding `source` and every apply thereafter
died with `name 'source' is not defined`. It shipped through 4281 passing tests,
a byte-compile gate, two external review rounds, a corpus sweep, and a deploy,
and was caught only when the action was finally driven end-to-end against the
live container.

Nothing else in this suite can catch that class. `plugin.py` imports Django, so
no test imports it and no test executes `_action_apply`; `compileall` only
checks syntax, and an undefined name is perfectly valid syntax. The result is a
NameError that is invisible until the code path actually runs, which for a
scheduled action means hours later in production.

Scoped to undefined names ON PURPOSE. Unused imports and shadowed variables are
style; a name that does not exist is a guaranteed runtime crash on the first
execution of its branch. Widening this to full pyflakes would bury the signal
under pre-existing noise and get the file skipped, which is how a gate dies.
"""

import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# EVERY .py in the repo. The gate started scoped to the shipped modules because
# tests/ carried 23 pre-existing undefined names that would have buried the
# signal; those were fixed in the same change (missing `typing` imports behind
# PEP 526 local annotations, unevaluated at runtime and therefore invisible), so
# the scope is now the whole tree and this class cannot re-enter anywhere.
def _production_files():
    out = []
    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs
                   if d not in {"__pycache__", ".git", ".planning", "node_modules"}]
        for name in sorted(files):
            if name.endswith(".py"):
                out.append(os.path.relpath(os.path.join(root, name), REPO_ROOT))
    return sorted(out)


@pytest.fixture(scope="module")
def pyflakes_available():
    try:
        import pyflakes  # noqa: F401
    except ImportError:
        pytest.skip(
            "pyflakes not installed; add it to the test-run deps so this gate "
            "actually runs (see the module docstring for the outage it exists "
            "to prevent)"
        )
    return True


def test_the_file_list_is_not_empty(pyflakes_available):
    """Positive control. An empty list would make the gate below vacuous and
    permanently green, which is the exact failure mode it exists to catch."""
    files = _production_files()
    assert len(files) > 40, f"only found {len(files)}: {files[:10]}"
    assert "plugin.py" in files
    assert any(f.startswith("sources/") for f in files)
    assert any(f.startswith("tests/") for f in files)


def test_no_undefined_names_anywhere(pyflakes_available):
    files = _production_files()
    proc = subprocess.run(
        [sys.executable, "-m", "pyflakes", *files],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    bad = [
        ln for ln in (proc.stdout + proc.stderr).splitlines()
        if "undefined name" in ln or "referenced before assignment" in ln
    ]
    assert not bad, (
        "undefined names in production code, each one a NameError waiting for "
        "its branch to execute:\n  " + "\n  ".join(bad)
    )


def test_the_gate_detects_a_planted_undefined_name(pyflakes_available, tmp_path):
    """Fail the INSTRUMENT before trusting it.

    If pyflakes were missing, mis-invoked, or its output format changed, the
    test above would report a clean run forever. This plants the exact defect
    that caused the outage and asserts the detector sees it.
    """
    f = tmp_path / "planted.py"
    f.write_text("def g(items):\n    got = [i for i in items]\n    return use(got, missing_binding)\n")
    proc = subprocess.run(
        [sys.executable, "-m", "pyflakes", str(f)],
        capture_output=True, text=True,
    )
    combined = proc.stdout + proc.stderr
    assert "undefined name" in combined, (
        f"pyflakes did not flag a planted undefined name; output was: {combined!r}"
    )
