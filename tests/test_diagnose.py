"""Tests for the "Diagnose matching" action (`_action_diagnose`, #128).

The action re-runs the matcher's candidate lookup against the cached slate and
explains, per game, why a match did or did not land. The live verification on a
real instance exercised the 0-candidates branch thoroughly; these tests pin the
branches that real data did not hit at ship time: the near-miss "named only one
team" path, the preview-card skip path, and the field-event (#127) short-circuit.

`plugin.py` does its Django imports lazily inside functions, so the module loads
without a database. We stub `_read_cache` (the slate) and `_build_epg_lookup`
(the candidate source) so the whole action runs in-process with no ORM.
"""

import importlib.util
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load(submodule):
    """Load a leaf submodule of the (conftest-registered) package without
    exec-ing __init__.py. _util must be registered before matcher/plugin import
    it. Idempotent across the two submodules this test needs."""
    if f"{PKG_NAME}._util" not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py")
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules[f"{PKG_NAME}._util"] = mod
        spec.loader.exec_module(mod)
    full = f"{PKG_NAME}.{submodule}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(
        full, os.path.join(REPO_ROOT, f"{submodule}.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[full] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def plugin():
    return _load("plugin")


@pytest.fixture(scope="module")
def ChannelCandidate():
    return _load("matcher").ChannelCandidate


def _game(away, home, *, prefix="NBA", matched=None, start="2026-06-15T20:00:00Z"):
    return {
        "sport_prefix": prefix,
        "away": away,
        "home": home,
        "kickoff_local": "Today 8:00 PM EDT",
        "start_time_utc": start,
        "channel_name_current": matched,
    }


def _run(plugin, ChannelCandidate, games, candidates_by_home):
    """Drive _action_diagnose with a stubbed cache + lookup. `candidates_by_home`
    maps a game's home-team string to the ChannelCandidate list the lookup
    should return for it (the matcher keys nothing on home; this is just a
    convenient test handle)."""
    plugin._read_cache = lambda: {"games": games}

    def fake_lookup_factory():
        def lookup(game):
            return candidates_by_home.get(game.home, [])
        return lookup

    plugin._build_epg_lookup = fake_lookup_factory
    out = plugin._action_diagnose({})
    assert out["status"] == "ok"
    return out["message"]


def _cand(ChannelCandidate, channel_name, program_title):
    from datetime import datetime, timezone
    now = datetime(2026, 6, 15, 20, 0, tzinfo=timezone.utc)
    return ChannelCandidate(
        channel_id=1,
        channel_name=channel_name,
        program_title=program_title,
        program_start=now,
        program_end=now,
    )


class TestDiagnoseBranches:
    def test_zero_candidates(self, plugin, ChannelCandidate):
        games = [_game("New York Knicks", "San Antonio Spurs")]
        msg = _run(plugin, ChannelCandidate, games, {"San Antonio Spurs": []})
        assert "channels naming either team in this window: 0" in msg
        assert "feed likely is not in your lineup" in msg

    def test_near_miss_one_team_only(self, plugin, ChannelCandidate):
        # A channel whose guide names ONLY the away team. Both-team gate fails,
        # so it is a near-miss the user can act on (likely an alias gap).
        cands = [_cand(ChannelCandidate, "Sports 5", "Lakers Game Tonight")]
        games = [_game("Los Angeles Lakers", "Boston Celtics")]
        msg = _run(plugin, ChannelCandidate, games, {"Boston Celtics": cands})
        assert "channels naming either team in this window: 1" in msg
        assert "named: away" in msg                 # hit Lakers, not Celtics
        assert "name-spelling gap" in msg           # the alias-gap conclusion

    def test_preview_card_is_flagged_and_skipped(self, plugin, ChannelCandidate):
        # A preview card names BOTH teams but is not the live broadcast. The
        # matcher strips it; the diagnostic must say so rather than imply a match.
        cands = [
            _cand(ChannelCandidate, "ESPN", "NBA Tonight"),                  # neither
            _cand(ChannelCandidate, "NBA TV", "Next Game: Knicks vs Spurs"), # both, preview
        ]
        games = [_game("New York Knicks", "San Antonio Spurs")]
        msg = _run(plugin, ChannelCandidate, games, {"San Antonio Spurs": cands})
        assert "[PREVIEW card - skipped on purpose]" in msg
        assert "preview / pre-game cards" in msg
        assert "named: neither" in msg              # the ESPN line

    def test_field_event_short_circuit(self, plugin, ChannelCandidate):
        # away == "Field" sentinel (#127): say it is a known limitation, do not
        # run the lookup or report a misleading "0 candidates".
        games = [_game("Field", "UFC Freedom 250: Topuria vs. Gaethje", prefix="UFC")]
        msg = _run(plugin, ChannelCandidate, games, {})
        assert "known limitation" in msg
        assert "#127" in msg
        assert "channels naming either team" not in msg

    def test_matched_games_shown_for_contrast(self, plugin, ChannelCandidate):
        games = [
            _game("New York Knicks", "San Antonio Spurs"),                    # unmatched
            _game("Scotland", "Haiti", prefix="WC", matched="Peacock 12"),    # matched
        ]
        msg = _run(plugin, ChannelCandidate, games, {"San Antonio Spurs": []})
        assert "1 matched" in msg and "1 unmatched" in msg
        assert "MATCHED (for contrast" in msg
        assert "Peacock 12" in msg

    def test_empty_cache_prompts_refresh(self, plugin, ChannelCandidate):
        msg = _run(plugin, ChannelCandidate, [], {})
        assert "Cache empty" in msg and "refresh" in msg.lower()

    def test_ambiguous_multiple_live_both_team_candidates(self, plugin, ChannelCandidate):
        # Two NON-preview channels both name both teams: not a preview situation,
        # not an alias gap. The matcher could not auto-pick (LLM off/abstained).
        cands = [
            _cand(ChannelCandidate, "Feed A", "Knicks vs Spurs - Feed A"),
            _cand(ChannelCandidate, "Feed B", "Knicks vs Spurs - Feed B"),
        ]
        games = [_game("New York Knicks", "San Antonio Spurs")]
        msg = _run(plugin, ChannelCandidate, games, {"San Antonio Spurs": cands})
        assert "ambiguous" in msg
        assert "2 channel(s) named both teams" in msg
        assert "PREVIEW card" not in msg            # not the preview branch
        assert "name-spelling gap" not in msg       # not the alias branch

    def test_candidate_list_is_capped_and_counts_remainder(self, plugin, ChannelCandidate):
        cap = plugin._DIAGNOSE_MAX_CANDIDATES
        cands = [_cand(ChannelCandidate, f"Ch {i}", "Lakers Tonight")
                 for i in range(cap + 3)]                  # only away team named
        games = [_game("Los Angeles Lakers", "Boston Celtics")]
        msg = _run(plugin, ChannelCandidate, games, {"Boston Celtics": cands})
        assert f"channels naming either team in this window: {cap + 3}" in msg
        assert f"and 3 more (omitted" in msg
        assert msg.count('- "Ch ') == cap               # exactly `cap` lines shown

    def test_matched_list_is_capped_and_counts_remainder(self, plugin, ChannelCandidate):
        cap = plugin._DIAGNOSE_MAX_CANDIDATES
        games = [_game(f"Away {i}", f"Home {i}", prefix="WC", matched=f"Ch {i}")
                 for i in range(cap + 2)]
        msg = _run(plugin, ChannelCandidate, games, {})
        assert "Every curated game matched a channel" in msg   # no unmatched
        assert "MATCHED (for contrast" in msg
        assert "and 2 more matched." in msg


class TestActionContract:
    """The dispatch table and the manifest must declare the SAME action ids.
    A manifest button with no handler returns 'Unknown action' at runtime; a
    handler with no button is dead. Catch both here instead of in production."""

    def test_manifest_actions_match_dispatch_table(self, plugin):
        import json
        manifest = json.load(open(os.path.join(REPO_ROOT, "plugin.json")))
        manifest_ids = {a["id"] for a in manifest["actions"]}
        handler_ids = set(plugin._ACTION_HANDLERS)
        assert manifest_ids == handler_ids, (
            f"manifest-only: {manifest_ids - handler_ids}; "
            f"handler-only: {handler_ids - manifest_ids}"
        )

    def test_every_handler_is_callable(self, plugin):
        assert all(callable(h) for h in plugin._ACTION_HANDLERS.values())
