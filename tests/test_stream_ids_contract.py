"""Producer/consumer contract for the `stream_ids` key (Path C stream-granular).

`stream_ids` is a runtime string key crossing three layers:
  - MatchResult.stream_ids (matcher produces it)
  - the refresh cache payload writes "stream_ids" (serialised)
  - _action_apply reads g.get("stream_ids") and attaches those streams

Nothing else couples these by type, so a rename on one side would silently
break stream attachment with every unit test still green. These static checks
fail loudly if any layer drops the key. AST/source-level (not runtime) because
the refresh + apply paths are deeply Django-coupled, matching the approach in
test_apply_no_network_in_transaction.py.
"""

import ast
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGIN_PY = os.path.join(REPO_ROOT, "plugin.py")
MATCHER_PY = os.path.join(REPO_ROOT, "matcher.py")


@pytest.fixture(scope="module")
def plugin_src():
    return open(PLUGIN_PY, encoding="utf-8").read()


def _func(src, name):
    tree = ast.parse(src, filename="plugin.py")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    pytest.fail(f"{name} not found in plugin.py")


def test_matchresult_declares_stream_ids():
    src = open(MATCHER_PY, encoding="utf-8").read()
    assert "stream_ids" in src, "MatchResult must carry stream_ids"


def test_refresh_writes_stream_ids_to_cache(plugin_src):
    # Producer: the cache payload built in _action_refresh must serialise
    # match.stream_ids under the "stream_ids" key.
    fn = _func(plugin_src, "_action_refresh")
    body = ast.get_source_segment(plugin_src, fn)
    assert '"stream_ids"' in body, "_action_refresh must write the stream_ids key"
    assert "match.stream_ids" in body, "cache must serialise match.stream_ids"


def test_apply_reads_stream_ids(plugin_src):
    # Consumer: _action_apply must read the stream_ids key back and attach those
    # streams (Stream.objects.filter on the explicit ids).
    fn = _func(plugin_src, "_action_apply")
    body = ast.get_source_segment(plugin_src, fn)
    assert 'get("stream_ids")' in body, "_action_apply must read g.get('stream_ids')"
    assert "explicit_stream_ids" in body, "apply must attach the explicit streams"
