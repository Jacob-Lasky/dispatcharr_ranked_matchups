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


@pytest.fixture(autouse=True)
def _restore_plugin_attrs(plugin):
    """`_run` stubs module-level callables by direct assignment; snapshot and
    restore them so the stubs don't leak into other test files sharing this
    process (notably test_field_event_lookup, which drives the real
    _build_epg_lookup)."""
    names = ("_read_cache", "_diagnose_window_sample", "_build_epg_lookup", "logger")
    saved = {n: getattr(plugin, n) for n in names}
    yield
    for n, v in saved.items():
        setattr(plugin, n, v)


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


def _run(plugin, games, *, window_rows=None, lookup_cands=None):
    """Drive _action_diagnose with the two DB helpers stubbed."""
    plugin._read_cache = lambda: {"games": games}
    plugin._diagnose_window_sample = lambda *a, **k: list(window_rows or [])
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
        assert "all matched" in msg

    def test_picks_soonest_unmatched_not_highest_scored(self, plugin):
        # Far-future distinct starts so "upcoming" holds regardless of wall clock.
        # The LATER-starting game has the HIGHER score, to prove soonest wins
        # over score (the user's live game scores low; we must still pick it).
        games = [_game("Egypt", "Belgium", score=9.9, start="2099-01-02T20:00:00Z"),
                 _game("Japan", "Netherlands", score=0.1, start="2099-01-01T20:00:00Z")]
        msg = _run(plugin, games, window_rows=[])
        assert "Closest of 2 unmatched: WC Japan at Netherlands" in msg  # soonest, low score
        assert "Egypt" not in msg                                        # non-target not shown

    def test_output_is_short(self, plugin):
        # Toast constraint: keep it to a few short lines. No game with evidence
        # should exceed 3 lines (game + Saw + verdict).
        rows = [("beIN SPORTS 1", "Copa Mundial: Japon vs Paises Bajos", " [Japan]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert len(msg.splitlines()) <= 3

    def test_no_naming_listing_means_not_carried(self, plugin):
        # An unrelated head-to-head airing then names neither team: not evidence.
        rows = [("ACC Network", "CFP: Miami vs. Ohio State", "")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert "Saw " not in msg                       # the unrelated game is NOT surfaced
        assert "not carried" in msg

    def test_both_teams_listing_yields_not_picked_verdict(self, plugin):
        rows = [("beIN SPORTS 1", "FIFA World Cup: Japan vs Netherlands", " [BOTH TEAMS]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)   # lookup returns no strict match
        assert "Saw beIN SPORTS 1" in msg
        assert "BOTH teams but wasn't auto-picked" in msg

    def test_one_team_listing_yields_alias_verdict(self, plugin):
        # The classic alias gap: the game is there under a Spanish spelling our
        # keywords miss, so it reads as only one team.
        rows = [("beIN SPORTS 1", "Copa Mundial: Japon vs Paises Bajos", " [Japan]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert "Saw " in msg
        assert "spelling gap" in msg

    def test_unexpected_when_channel_name_has_both(self, plugin, ChannelCandidate):
        # A candidate whose NAME has both teams should have matched; flag it.
        cands = [_cand(ChannelCandidate, "Knicks vs Spurs HD")]
        rows = [("Knicks vs Spurs HD", "", " [BOTH TEAMS]")]
        games = [_game("New York Knicks", "San Antonio Spurs", prefix="NBA", score=8.0)]
        msg = _run(plugin, games, window_rows=rows, lookup_cands=cands)
        assert "unexpected" in msg

    def test_long_title_is_truncated(self, plugin):
        long_title = "FIFA World Cup Quarterfinal Extravaganza Presented by Sponsor: Japan vs"
        rows = [("Some Very Long Channel Name Here FHD", long_title, " [Japan]")]
        games = [_game("Japan", "Netherlands", score=9.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert "…" in msg                              # title/name got truncated
        saw_line = next(l for l in msg.splitlines() if l.startswith("Saw "))
        assert len(saw_line) < 90

    def test_field_event_is_diagnosed_not_skipped(self, plugin):
        # #127: field events now match single-sided, so an unmatched one is an
        # ordinary diagnosable target (previously short-circuited as
        # "unmatchable").
        games = [_game("Field", "UFC Freedom 250: Topuria vs Gaethje", prefix="UFC", score=2.0)]
        msg = _run(plugin, games, window_rows=[])
        assert "Closest of 1 unmatched" in msg
        assert "UFC Freedom 250" in msg          # rendered as the event name...
        assert "Field at" not in msg             # ...not the misleading "Field at"
        assert "names this event" in msg         # field-specific verdict

    def test_field_event_with_event_listing_yields_spelling_gap(self, plugin):
        rows = [("BT Sport 1", "UFC 250: Topuria vs Gaethje", " [event]")]
        games = [_game("Field", "UFC 250: Topuria vs Gaethje", prefix="UFC", score=2.0)]
        msg = _run(plugin, games, window_rows=rows)
        assert "Saw BT Sport 1" in msg
        assert "spelling gap" in msg

    def test_field_event_channel_name_match_is_unexpected(self, plugin, ChannelCandidate):
        # A channel NAME carries the event but it didn't auto-match: flag it,
        # with field-specific wording ("has the event", not "has both teams").
        cands = [_cand(ChannelCandidate, "UFC 250: Topuria vs Gaethje HD")]
        rows = [("UFC 250: Topuria vs Gaethje HD", "", " [event]")]
        games = [_game("Field", "UFC 250: Topuria vs Gaethje", prefix="UFC", score=2.0)]
        msg = _run(plugin, games, window_rows=rows, lookup_cands=cands)
        assert "has the event but it didn't match" in msg

    def test_non_target_games_not_listed(self, plugin):
        # The full unmatched slate is NOT dumped (toast length): only the target.
        games = [_game("Japan", "Netherlands", score=9.0),
                 _game("Field", "UFC Freedom 250", prefix="UFC", score=2.0)]
        msg = _run(plugin, games, window_rows=[])
        assert "UFC Freedom 250" not in msg

    def test_verbose_report_logged_alongside_toast(self, plugin, monkeypatch):
        # The full detail the toast omits goes to the logs, so a user can paste
        # the toast AND provide logs.
        captured = []

        class _FakeLogger:
            def info(self, *a, **k):
                captured.append(" ".join(str(x) for x in a))
            def warning(self, *a, **k):
                pass
            def exception(self, *a, **k):
                pass
            def error(self, *a, **k):
                pass

        monkeypatch.setattr(plugin, "logger", _FakeLogger())
        rows = [("beIN 1", "Japan vs Netherlands", " [BOTH TEAMS]"),
                ("Some Other Ch", "Unrelated vs Game", "")]
        games = [_game("Japan", "Netherlands", score=9.0),
                 _game("Egypt", "Belgium", score=3.0, start="2099-09-09T20:00:00Z")]
        toast = _run(plugin, games, window_rows=rows)

        v = "\n".join(captured)
        assert "diagnose (verbose)" in v          # the marker
        assert "all unmatched" in v
        assert "Egypt" in v                        # non-target game IS in the log
        assert "Egypt" not in toast                # ...but NOT in the toast
        assert "Unrelated vs Game" in v            # full window listings in the log
        assert "Unrelated vs Game" not in toast    # ...not in the toast (names neither)


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
