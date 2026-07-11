"""The plugin version lives in three places that must never drift.

- plugin.json "version": must be a static literal (the loader reads the
  manifest WITHOUT executing plugin code).
- __init__.py __version__: the package version.
- plugin.py `Plugin.version`: the class attr the loader DISPLAYS (it wins over
  the manifest).

They can't be collapsed to one Python source: __init__ imports plugin before it
defines __version__ (circular import), and plugin.json has to stay literal. So
this text-only test (no imports, runs without Django) is the backstop. It exists
because a bump that touched plugin.json + __init__ but not the class attr shipped
a plugin that displayed the wrong version (1.11.1 release, caught in prod deploy).
"""
import json
import os
import re

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(_ROOT, name), encoding="utf-8") as fh:
        return fh.read()


def _manifest_version():
    return json.loads(_read("plugin.json"))["version"]


def _init_version():
    m = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', _read("__init__.py"), re.M)
    assert m, "__version__ not found in __init__.py"
    return m.group(1)


def _class_version():
    # First `version = "..."` inside plugin.py is the Plugin class attr.
    m = re.search(r'^\s{4}version\s*=\s*["\']([^"\']+)["\']', _read("plugin.py"), re.M)
    assert m, "Plugin.version class attr not found in plugin.py"
    return m.group(1)


def test_all_three_versions_match():
    manifest = _manifest_version()
    init = _init_version()
    klass = _class_version()
    assert manifest == init == klass, (
        f"version drift: plugin.json={manifest!r}, __init__={init!r}, "
        f"Plugin.version={klass!r} - bump all three together"
    )


def test_version_is_semver():
    assert re.fullmatch(r"\d+\.\d+\.\d+", _manifest_version()), (
        "plugin.json version must be strict X.Y.Z semver (the official-repo "
        "validator rejects a 'v' prefix or non-semver)"
    )
