"""Durable storage on v3; the installed folder on Dispatcharr 0.33."""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import shutil
import sys
import tempfile

CODE_DIR = Path(__file__).resolve().parent
DATA_ENV = 'RANKED_MATCHUPS_DATA_DIR'
KEY_ENV = 'RANKED_MATCHUPS_PLUGIN_KEY'
_root = Path(os.environ.get(DATA_ENV) or CODE_DIR)
_identity = None

# Only runtime state and legacy key files migrate. Bundled fixtures/resources stay
# with the code. A completion marker prevents a deliberately deleted cache from
# being resurrected from the old install on the next action.
FILES = (
    'cache.json', 'recordings_state.json', 'recording_stops.json',
    'numbering_state.json', 'llm_descriptions_cache.json',
    'sportsdb_thumb_cache.json', 'gamethumbs_cache.json', 'last_run.json',
    'odds_cache.json', 'cfbd_api_key', 'football_data_api_key', 'odds_api_key',
    'anthropic_api_key', 'boxing_data_api_key', 'sportsdb_api_key',
)
DIRECTORIES = ('season_cache', 'college_cache', 'slates')


def managed_services():
    """Feature detection, not an app-version comparison or a guessed path."""
    try:
        from apps.plugins.service_runner import PluginServiceRunner
    except ImportError:
        return False
    return callable(getattr(PluginServiceRunner, 'start', None))


def plugin_key():
    global _identity
    if os.environ.get(KEY_ENV):
        return os.environ[KEY_ENV]
    try:
        from apps.plugins.context import current_plugin_key
        key = current_plugin_key()
        if key:
            _identity = key
            return key
    except ImportError:
        pass
    return _identity or CODE_DIR.name.replace(' ', '_').lower()


def path(name):
    return str(_root / name)


def _copy_state(source, target):
    """Publish a complete file; a failed import must remain retryable."""
    fd, staged = tempfile.mkstemp(prefix=f'.{target.name}.', dir=target.parent)
    os.close(fd)
    try:
        shutil.copy2(source, staged)
        os.replace(staged, target)
    finally:
        Path(staged).unlink(missing_ok=True)


def _import_legacy(destination):
    destination.mkdir(parents=True, exist_ok=True)
    with (destination / '.legacy-import.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        marker = destination / '.legacy-imported'
        if marker.exists():
            return
        for name in FILES + DIRECTORIES:
            source = CODE_DIR / name
            target = destination / name
            if not source.exists():
                continue
            if source.is_dir():
                # Copy only absent files, preserving state already in data_dir.
                for old in source.rglob('*'):
                    new = target / old.relative_to(source)
                    if old.is_file() and not new.exists():
                        new.parent.mkdir(parents=True, exist_ok=True)
                        _copy_state(old, new)
            elif not target.exists():
                _copy_state(source, target)
        marker.touch()


def configure(context):
    global _root
    directory = context.get('data_dir')
    if not directory:
        return
    destination = Path(directory).resolve()
    if destination == _root:
        return
    _import_legacy(destination)
    _root = destination
    # These modules expose path constants for existing callers/tests. Rebind the
    # loaded copies; subprocess imports read the same root from their own env.
    package = __package__
    bindings = {
        'plugin': {
            'CACHE_PATH': 'cache.json', 'RECORDING_STATE_PATH': 'recordings_state.json',
            'RECORDING_STOPS_PATH': 'recording_stops.json', 'NUMBERING_STATE_PATH': 'numbering_state.json',
            'LLM_DESCRIPTIONS_CACHE_PATH': 'llm_descriptions_cache.json',
            'SPORTSDB_THUMB_CACHE_PATH': 'sportsdb_thumb_cache.json', 'GAMETHUMBS_CACHE_PATH': 'gamethumbs_cache.json',
            'CFBD_KEY_PATH': 'cfbd_api_key', 'FD_KEY_PATH': 'football_data_api_key',
            'ODDS_KEY_PATH': 'odds_api_key', 'ANTHROPIC_KEY_PATH': 'anthropic_api_key',
            'BOXING_KEY_PATH': 'boxing_data_api_key', 'SPORTSDB_KEY_PATH': 'sportsdb_api_key',
        },
        'tasks': {'LAST_RUN_PATH': 'last_run.json'},
        'sources._odds': {'CACHE_PATH': 'odds_cache.json'},
        'sources._season_store': {'STORE_DIR': 'season_cache'},
        'sources._college_data': {'CACHE_DIR': 'college_cache'},
    }
    for module_name, names in bindings.items():
        module = sys.modules.get(f'{package}.{module_name}')
        if module is not None:
            for attribute, name in names.items():
                setattr(module, attribute, path(name))
