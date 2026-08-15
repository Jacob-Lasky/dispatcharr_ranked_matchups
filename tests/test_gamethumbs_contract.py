"""Producer/consumer contract for the game-thumbs logo tier (#174).

Every behavioural test in test_gamethumbs.py can pass while the tier is never
reached from the apply path: gamethumbs.py is a standalone module, and the one
line that wires it in lives inside `_action_apply`, which is too Django-coupled
to import in a unit test. That is exactly how a feature ships inert.

These are AST/source-level checks over plugin.py, matching the approach in
test_stream_ids_contract.py and test_apply_no_network_in_transaction.py.
"""

import ast
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGIN_PY = os.path.join(REPO_ROOT, "plugin.py")
PLUGIN_JSON = os.path.join(REPO_ROOT, "plugin.json")


@pytest.fixture(scope="module")
def plugin_src():
    return open(PLUGIN_PY, encoding="utf-8").read()


@pytest.fixture(scope="module")
def apply_src(plugin_src):
    """Source text of _action_apply, where the whole logo chain lives."""
    tree = ast.parse(plugin_src, filename="plugin.py")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_action_apply":
            return ast.get_source_segment(plugin_src, node)
    pytest.fail("_action_apply not found in plugin.py")


class TestTierIsWiredIn:
    def test_apply_imports_the_module(self, apply_src):
        assert "from . import gamethumbs" in apply_src

    def test_apply_calls_the_resolver(self, apply_src):
        assert "fetch_matchup_composite(" in apply_src

    def test_resolver_is_reached_from_the_sportsdb_miss_path(self, apply_src):
        # The tier is only useful if a SportsDB miss falls INTO it. If this ever
        # reverts to _badge_or_channel(), every game-thumbs lookup goes dead
        # while both modules still exist and every other test stays green.
        assert "_gamethumbs_or_badge()" in apply_src
        assert apply_src.count("_gamethumbs_or_badge()") >= 3

    def test_outcome_is_tallied(self, apply_src):
        # A tier nobody counts is a tier nobody notices going to zero.
        assert 'logo_outcome == "gamethumbs"' in apply_src
        assert "matchup_logos_gamethumbs" in apply_src

    def test_summary_reports_the_tier(self, apply_src):
        assert "game-thumbs" in apply_src


class TestCacheHygiene:
    def test_separate_cache_file(self, plugin_src):
        # Sharing the SportsDB cache file would make each provider's prune()
        # delete the other's entries.
        assert "GAMETHUMBS_CACHE_PATH" in plugin_src
        assert "gamethumbs_cache.json" in plugin_src

    def test_cache_is_pruned_and_saved(self, apply_src):
        assert "gamethumbs_cache.prune(" in apply_src
        assert "gamethumbs_cache.save()" in apply_src

    def test_only_definitive_misses_are_cached(self, apply_src):
        # The load-bearing branch: caching a transient 429 as a negative would
        # blank a whole slate of logos for the negative TTL.
        assert "if definitive_miss:" in apply_src


class TestSettingsSurface:
    def test_field_declared_in_manifest(self):
        import json
        fields = json.load(open(PLUGIN_JSON, encoding="utf-8"))["fields"]
        ids = [f.get("id") for f in fields]
        assert "gamethumbs_base_url" in ids

    def test_field_default_matches_module_default(self):
        import json
        import re
        fields = json.load(open(PLUGIN_JSON, encoding="utf-8"))["fields"]
        declared = next(
            f for f in fields if f.get("id") == "gamethumbs_base_url"
        )["default"]
        src = open(os.path.join(REPO_ROOT, "gamethumbs.py"), encoding="utf-8").read()
        module_default = re.search(
            r'^DEFAULT_BASE_URL\s*=\s*"([^"]+)"', src, re.M,
        ).group(1)
        assert declared == module_default

    def test_setting_is_read_by_apply(self, apply_src):
        assert 'settings.get("gamethumbs_base_url"' in apply_src
