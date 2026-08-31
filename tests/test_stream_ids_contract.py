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


def test_apply_gates_whole_channel_streams_on_this_game(plugin_src):
    # A whole-channel match donates every stream the channel carries, so without
    # this gate one match drags in other games' broadcasts (#162: dedicated NFL
    # feeds attached to an MLB game). The gate lives in matcher, but the WIRING
    # is here in apply and is what a refactor would silently drop: the unit tests
    # for select_streams_for_game would all still pass with apply not calling it.
    fn = _func(plugin_src, "_action_apply")
    body = ast.get_source_segment(plugin_src, fn)
    assert "select_streams_for_game" in body, (
        "_action_apply must gate whole-channel streams through "
        "matcher.select_streams_for_game"
    )
    assert "foreign_streams_dropped" in body, (
        "dropped streams must be counted so the gate is never silent"
    )
    # The gate admits a stream on ONE side's hit, so it must be fed STRONG
    # keywords. Feeding it _team_keywords would re-admit the bare metro and the
    # gate would silently stop working with every unit test still green (#162).
    assert "_strong_team_keywords" in body, (
        "the stream gate must be fed _strong_team_keywords, not _team_keywords"
    )


class TestPathCRejectsStaleDatedStreams:
    """#164: the wiring, not the predicate.

    matcher-level tests can all pass with the filter never called from the
    lookup, which is exactly how the stale feeds shipped: the predicate is
    useless unless Path C consults it.
    """

    def _lookup_src(self):
        import os
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        start = src.index("def _build_epg_lookup(")
        return src[start:src.index("\ndef ", start + 10)]

    def test_path_c_consults_the_staleness_filter(self):
        assert "stream_is_stale_for_game(s.name or \"\", earliest_game_date)" in self._lookup_src()

    def test_the_reference_date_is_the_EARLIER_of_utc_and_local(self):
        # Taking min() is what absorbs the provider's timezone (names are
        # stamped ET) without a fixed fudge factor. Picking one zone would drop
        # the LIVE feed for any night game whose UTC date has rolled over.
        body = self._lookup_src()
        assert "earliest_game_date = min(" in body
        assert "astimezone(timezone.utc).date()" in body
        assert "astimezone(local_tz).date()" in body

    def test_drops_are_counted_and_reported(self):
        import os
        body = self._lookup_src()
        assert 'stats["stale_dated_streams_dropped"] += 1' in body
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        assert "stale_dated_streams_dropped" in src.split("def _build_epg_lookup(")[0], (
            "refresh must read the counter back and log it, or silent truncation "
            "reads as 'nothing was dropped'"
        )

    def test_refresh_threads_the_users_timezone_in(self):
        import os
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        # Kept as a source-level assertion rather than a behavioural one because
        # plugin.py needs Django to import: every matcher-level timezone test
        # passes with the refresh never threading the tz in at all.
        assert 'local_tz=_resolve_tz(settings.get("local_timezone", "UTC"))' in src

    def test_refresh_threads_the_excluded_groups_policy_in(self):
        """#206: excluded_groups must reach the LOOKUP, not just the apply.

        If it only reached the apply, the matcher could still choose an excluded
        stream as a game's primary and the apply would then strip it, leaving
        the matchup channel with no streams. Source-level for the same reason as
        the test above.
        """
        import os
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        assert "excluded_stream_ids=_excluded_stream_ids" in src
        assert "def _build_epg_lookup(local_tz=timezone.utc, excluded_stream_ids=frozenset()):" in src

    def test_diagnose_applies_the_same_exclusion_policy(self):
        """#206: diagnose must not report candidates the apply would drop.

        Contract-level: the stub in test_diagnose.py accepts **kw, so a
        regression that stopped passing the policy would still pass there.
        """
        import os
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        assert "excluded_stream_ids=_streams_in_groups(_group_policy(settings)[1])" in src


class TestPathAReadsSubtitleAndDescription:
    """#143: the wiring. Every matcher-level test for the European-broadcaster
    path passes with the lookup never populating program_extra and the DB query
    never selecting the columns, which would ship the exact bug it fixes.
    """

    def _lookup_src(self):
        import os
        with open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8") as fh:
            src = fh.read()
        start = src.index("def _build_epg_lookup(")
        return src[start:src.index("\ndef ", start + 10)]

    def test_query_admits_on_title_or_subtitle(self):
        body = self._lookup_src()
        assert '_or_icontains("title", all_kws) | _or_icontains("sub_title", all_kws)' in body

    def test_description_requires_BOTH_sides(self):
        # One keyword over free prose would drag half the guide into the Tier-3
        # pool: match reports name other teams in passing.
        body = self._lookup_src()
        assert '_or_icontains("description", strong_home)' in body
        assert '& _or_icontains("description", strong_away)' in body

    def test_the_columns_are_actually_selected(self):
        # .only() without them makes every access a deferred-field query at
        # best, and the fields are silently empty in the fake-ORM path.
        assert '.only("id", "title", "sub_title", "description",' in self._lookup_src()

    def test_candidates_carry_the_extra_text(self):
        body = self._lookup_src()
        assert "program_extra=" in body
        assert 'getattr(p, "sub_title", "")' in body, (
            "must tolerate row stubs from the replay harness and fake ORM"
        )

    def test_snapshot_exporter_carries_the_columns(self):
        # A snapshot without them replays as if no programme had text below the
        # headline, silently exercising the pre-fix path.
        import os
        with open(os.path.join(REPO_ROOT, "tools", "export_snapshot.py"), encoding="utf-8") as fh:
            src = fh.read()
        assert '"sub_title": p.sub_title, "description": p.description,' in src
