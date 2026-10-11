"""Run `_action_apply` / `_action_reap` against a REAL Django ORM (sqlite, in memory).

Why this exists: plugin.py imports Django models at call time, so every other
test either stops at an early return (the MagicMock fixtures in
test_dry_run_visibility / test_channel_profiles) or reads apply's source with
`ast`. Neither runs the stale reap, the orphan-EPG sweep or the auto-record
bookkeeping, and a MagicMock queryset agrees with whatever shape the test gave
it. This harness swaps only the database: the models are field-for-field copies
of Dispatcharr's (tests/orm_harness/apps), and the plugin code is the real file.

Run in a SUBPROCESS, never imported by pytest: `django.setup()` and the `apps`
package would otherwise leak into every other test module in the run, several
of which install their own fake `apps.*` modules and expect to own them.

Usage: python run_scenarios.py <out.json> <test_file.py> <scenario_name> [...]
Each `scenario_<name>(env)` in the test file gets a fresh database and a fresh
temp directory, and signals failure by raising. Writes `out.json` as
{name: null on success | the traceback string}. A file, not stdout, so nothing
Django or the plugin prints can be mistaken for the result.
"""

import hashlib
import importlib
import importlib.util
import json
import os
import sys
import tempfile
import traceback
from types import SimpleNamespace

HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.dirname(HARNESS_DIR)
REPO_ROOT = os.path.dirname(TESTS_DIR)

# Every file the plugin writes next to its own code, as (module, attribute).
# Each is redirected into the scenario's temp dir. Anything missed here lands
# in the repo checkout, which the before/after snapshot in main() turns into a
# failure rather than a stray or silently overwritten file. The snapshot sees
# WRITES only: a missed file the checkout already holds is READ instead, and
# passes. numbering_state.json (#219) was missed that way, so this harness
# failed on a clean checkout and passed beside a stale copy whose floor
# matched, which apply then did not rewrite.
_REDIRECTED_PATHS = (
    ("plugin", "CACHE_PATH"),
    ("plugin", "RECORDING_STATE_PATH"),
    ("plugin", "RECORDING_STOPS_PATH"),
    ("plugin", "LLM_DESCRIPTIONS_CACHE_PATH"),
    ("plugin", "SPORTSDB_THUMB_CACHE_PATH"),
    ("plugin", "GAMETHUMBS_CACHE_PATH"),
    ("plugin", "NUMBERING_STATE_PATH"),
    ("sources._odds", "CACHE_PATH"),
    ("tasks", "LAST_RUN_PATH"),
)


def _setup_django():
    sys.path.insert(0, HARNESS_DIR)
    import django
    from django.conf import settings

    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=["apps.m3u", "apps.channels", "apps.epg"],
        USE_TZ=True,
        TIME_ZONE="UTC",
        DEFAULT_AUTO_FIELD="django.db.models.AutoField",
    )
    django.setup()


def _load_plugin():
    # The ONE plugin loader lives in conftest.py (see the comment there on why
    # it must not be copied); load that file and use it.
    spec = importlib.util.spec_from_file_location(
        "_orm_harness_conftest", os.path.join(TESTS_DIR, "conftest.py"),
    )
    conftest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conftest)
    plugin = conftest._load_plugin_module()
    pkg = conftest.PKG_NAME
    redirects = [
        (plugin if mod == "plugin" else importlib.import_module(f"{pkg}.{mod}"), attr)
        for mod, attr in _REDIRECTED_PATHS
    ]
    return plugin, importlib.import_module(f"{pkg}.logos"), redirects


def _models():
    from django.apps import apps as registry
    return SimpleNamespace(**{m.__name__: m for m in registry.get_models()})


def _fresh_schema(create: bool):
    from django.apps import apps as registry
    from django.db import connection
    models = list(registry.get_models())
    with connection.schema_editor() as editor:
        for model in (models if create else reversed(models)):
            (editor.create_model if create else editor.delete_model)(model)


def _repo_snapshot():
    """Top-level entries of the checkout with each file's content hash: the
    plugin only ever writes beside its own code, and a NAME listing alone would
    miss it overwriting a gitignored file that was already there."""
    snap = {}
    for name in os.listdir(REPO_ROOT):
        path = os.path.join(REPO_ROOT, name)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                snap[name] = hashlib.sha256(f.read()).hexdigest()
        else:
            snap[name] = "dir"
    return snap


def main(argv):
    out_path, test_file, names = argv[0], argv[1], argv[2:]
    _setup_django()
    plugin, logos, redirects = _load_plugin()

    spec = importlib.util.spec_from_file_location("_orm_harness_scenarios", test_file)
    scenarios = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scenarios)

    before = _repo_snapshot()
    results = {}
    for name in names:
        tmp = tempfile.mkdtemp(prefix="orm_harness_")
        logo_dir = os.path.join(tmp, "logos")
        saved = [(mod, attr, getattr(mod, attr)) for mod, attr in redirects]
        saved_logo = (logos.LOGO_DIR, logos.sweep_stale_logo_files.__defaults__)
        for mod, attr, original in saved:
            setattr(mod, attr, os.path.join(tmp, os.path.basename(original)))
        # sweep_stale_logo_files binds LOGO_DIR as a DEFAULT at def time, so
        # patching the module constant alone would still sweep /data/logos.
        logos.LOGO_DIR = logo_dir
        logos.sweep_stale_logo_files.__defaults__ = (logo_dir,)
        _fresh_schema(create=True)
        try:
            getattr(scenarios, f"scenario_{name}")(
                SimpleNamespace(plugin=plugin, m=_models(), tmp=tmp)
            )
            results[name] = None
        except BaseException:
            results[name] = traceback.format_exc()
        finally:
            _fresh_schema(create=False)
            for mod, attr, original in saved:
                setattr(mod, attr, original)
            logos.LOGO_DIR, logos.sweep_stale_logo_files.__defaults__ = saved_logo

    after = _repo_snapshot()
    if after != before:
        changed = sorted(n for n in set(before) | set(after) if before.get(n) != after.get(n))
        for name in results:
            results[name] = results[name] or f"harness wrote into the repo: {changed}"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f)


if __name__ == "__main__":
    main(sys.argv[1:])
