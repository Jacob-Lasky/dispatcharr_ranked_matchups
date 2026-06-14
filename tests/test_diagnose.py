"""Tests for the "Diagnose matching" action (`_action_diagnose`, #128).

The action deep-dives the single highest-scored UNMATCHED game: it shows the
team spellings the matcher searched, dumps the sports programming actually
airing in that game's window (so a human can spot the game under a spelling we
miss), gives a verdict, and lists the rest of the unmatched slate one line each.

`plugin.py` does its Django imports lazily, so the module loads without a DB.
The two DB touchpoints (`_diagnose_window_sample`, `_build_epg_lookup`) are
stubbed so the whole action runs in-process with no ORM.
"""

import importlib.util
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load(submodule):
    if f"{PKG_NAME}._util" not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py"))
        mod = importlib.util.module_from_spec(spec)
        sys.modules[f"{PKG_NAME}._util"] = mod
        spec.loader.exec_module(mod)
    full = f"{PKG_NAME}.{submodule}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(full, os.path.join(REPO_ROOT, f"{submodule}.py"))
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


def _game(away, home, *, prefix="WC", score=5.0, matched=None,
          start="2026-06-15T20:00:00Z", label="FIFA World Cup"):
    return {
        "sport_prefix": prefix, "sport_label": label,
        "away": away, "home": home,
        "kickoff_local": "Today 8:00 PM EDT",
        "start_time_utc": start, "score": score,
        "channel_name_current": matched,
    }


def _run(plugin, games, *, window_rows=None, window_capped=False, lookup_cands=None):
    """Drive _action_diagnose with the two DB helpers stubbed."""
    plugin._read_cache = lambda: {"games": games}
    plugin._diagnose_window_sample = lambda *a, **k: (list(window_rows or []), window_capped)
    plugin._build_epg_lookup = lambda: (lambda shim: list(lookup_cands or []))
    out = plugin._action_diagnose({})
    assert out["status"] == "ok"
    return out["message"]


def _cand(ChannelCandidate, channel_name, program_title=""):
    from datetime import datetime, timezone
    now = datetime(2026, 6, 15, 20, 0, tzinfo=timezone.utc)
    return ChannelCandidate(channel_id=1, channel_name=channel_name,
                            program_title=program_title, program_start=now, program_end=now)


class TestDeepDive:
    def test_empty_cache_prompts_refresh(self, plugin):
        msg = _run(plugin, [])
        assert "Cache empty" in msg and "refresh" in msg.lower()

    def test_all_matched_nothing_to_diagnose(self, plugin):
        games = [_game("Scotland", "Haiti", matched="Peacock 12")]
        msg = _run(plugin, games)
        assert "Every curated game matched" in msg

    def test_picks_soonest_unmatched_not_highest_scored(self, plugin):
        # Far-future distinct starts so "upcoming" holds regardless of wall clock.
        # The LATER-starting game has the HIGHER score, to prove soonest wins
        # over score (the user's live game scores low; we must still pick it).
        games = [_game("Egypt", "Belgium", score=9.9, start="2099-01-02T20:00:00Z"),
                 _game("Japan", "Netherlands", score=0.1, start="2099-01-01T20:00:00Z")]
        msg = _run(plugin, games, window_rows=[])
        assert "DEEP DIVE" in msg
        deep = msg.split("Other unmatched")[0]
        assert "Japan at Netherlands" in deep          # soonest, despite low score
        assert "Egypt at Belgium" not in deep

    def test_searched_spellings_shown(self, plugin):
        # _team_keywords expands aliases, so the searched line proves what we look for.
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=[])
        assert "Spellings searched" in msg
        assert "Japan" in msg and "Netherlands" in msg

    def test_no_sports_programming_means_not_carried(self, plugin):
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=[])
        assert "NONE" in msg
        assert "not carrying this game" in msg

    def test_both_teams_row_yields_not_picked_verdict(self, plugin):
        rows = [("beIN SPORTS 1", "FIFA World Cup: Japan vs Netherlands", " [BOTH TEAMS]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)   # lookup returns no strict match
        assert "[BOTH TEAMS]" in msg
        assert "names BOTH teams but was not picked" in msg

    def test_one_team_row_yields_alias_scan_verdict(self, plugin):
        # The classic alias gap: the game is there under a Spanish spelling our
        # keywords miss, so it reads as only one team (or neither).
        rows = [("beIN SPORTS 1", "Copa Mundial: Japon vs Paises Bajos", " [Japan]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert "SCAN THE LIST" in msg
        assert "alias" in msg

    def test_unexpected_when_channel_name_has_both(self, plugin, ChannelCandidate):
        # A candidate whose NAME has both teams should have matched; flag it.
        cands = [_cand(ChannelCandidate, "Knicks vs Spurs HD")]
        rows = [("Knicks vs Spurs HD", "", " [BOTH TEAMS]")]
        games = [_game("New York Knicks", "San Antonio Spurs", prefix="NBA", score=8.0)]
        msg = _run(plugin, games, window_rows=rows, lookup_cands=cands)
        assert "Unexpected" in msg

    def test_window_sample_capped_with_remainder(self, plugin):
        cap = plugin._DIAGNOSE_WINDOW_SHOW
        rows = [(f"Ch {i}", "Soccer", "") for i in range(cap + 4)]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows, window_capped=True)
        assert f"+4 more" in msg
        assert "scan limit hit" in msg
        assert msg.count('Ch ') >= cap

    def test_all_field_events_short_circuits(self, plugin):
        games = [_game("Field", "UFC Freedom 250: Topuria vs Gaethje", prefix="UFC", score=2.0)]
        msg = _run(plugin, games)
        assert "field-event" in msg and "#127" in msg
        assert "DEEP DIVE" not in msg

    def test_other_unmatched_lists_field_events_tagged(self, plugin):
        games = [_game("Japan", "Netherlands", score=9.0),
                 _game("Field", "UFC Freedom 250", prefix="UFC", score=2.0)]
        msg = _run(plugin, games, window_rows=[])
        other = msg.split("Other unmatched")[1]
        assert "field event #127" in other

    def test_matched_shown_as_working_examples(self, plugin):
        games = [_game("Japan", "Netherlands", score=9.0),
                 _game("Scotland", "Haiti", matched="Peacock 12")]
        msg = _run(plugin, games, window_rows=[])
        assert "Working matches (1)" in msg
        assert "Peacock 12" in msg


class TestActionContract:
    """The dispatch table and the manifest must declare the SAME action ids."""

    def test_manifest_actions_match_dispatch_table(self, plugin):
        import json
        manifest = json.load(open(os.path.join(REPO_ROOT, "plugin.json")))
        manifest_ids = {a["id"] for a in manifest["actions"]}
        handler_ids = set(plugin._ACTION_HANDLERS)
        assert manifest_ids == handler_ids, (
            f"manifest-only: {manifest_ids - handler_ids}; "
            f"handler-only: {handler_ids - manifest_ids}")

    def test_every_handler_is_callable(self, plugin):
        assert all(callable(h) for h in plugin._ACTION_HANDLERS.values())
