"""Tests for plugin.py helpers that DON'T touch Django.

We import the helpers individually so we can sidestep the package's
__init__.py (which imports Plugin → starts the scheduler thread → imports
Django models). The conftest registers the package without exec-ing
__init__.py."""

import importlib.util
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = os.path.basename(REPO_ROOT)


def _load_plugin_module():
    """Load `plugin.py` as a submodule of the (already-stub-registered)
    package. Need to stub the Django imports it does at top-level too — but
    actually plugin.py only does Django imports lazily inside functions, so
    top-level load is safe."""
    if f"{PKG_NAME}.plugin" in sys.modules:
        return sys.modules[f"{PKG_NAME}.plugin"]
    # _util is imported at module top by plugin; load it first.
    util_spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}._util", os.path.join(REPO_ROOT, "_util.py")
    )
    util_mod = importlib.util.module_from_spec(util_spec)
    sys.modules[f"{PKG_NAME}._util"] = util_mod
    util_spec.loader.exec_module(util_mod)

    spec = importlib.util.spec_from_file_location(
        f"{PKG_NAME}.plugin", os.path.join(REPO_ROOT, "plugin.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG_NAME}.plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def plugin():
    return _load_plugin_module()


class TestParseScheduledTimes:
    def test_simple_csv(self, plugin):
        out = plugin._parse_scheduled_times("0400,1000,1600,2200")
        assert out == [(4, 0), (10, 0), (16, 0), (22, 0)]

    def test_three_digit_padded(self, plugin):
        out = plugin._parse_scheduled_times("400,1000")
        assert (4, 0) in out

    def test_colon_form_accepted(self, plugin):
        out = plugin._parse_scheduled_times("04:00,10:30")
        assert (4, 0) in out
        assert (10, 30) in out

    def test_dedupes_and_sorts(self, plugin):
        out = plugin._parse_scheduled_times("1000,0400,1000")
        assert out == [(4, 0), (10, 0)]

    def test_skips_garbage(self, plugin):
        out = plugin._parse_scheduled_times("0400,foo,2500,99,99")
        # "0400" → (4,0). "foo" rejected. "2500" rejected (h out of range).
        # "99" → padded to "0099" → 4 digits but only valid if HH<24, MM<60.
        # 0099 = h=0, m=99 → rejected.
        assert (4, 0) in out
        assert all(0 <= h < 24 and 0 <= m < 60 for h, m in out)

    def test_empty(self, plugin):
        assert plugin._parse_scheduled_times("") == []
        assert plugin._parse_scheduled_times(None) == []


class TestParseFavorites:
    def test_simple(self, plugin):
        assert plugin._parse_favorites("Wrexham, Barcelona") == ["Wrexham", "Barcelona"]

    def test_strips_whitespace(self, plugin):
        assert plugin._parse_favorites("  A  ,   B  ") == ["A", "B"]

    def test_skips_empty_tokens(self, plugin):
        assert plugin._parse_favorites("A,,B,  ,C") == ["A", "B", "C"]

    def test_empty(self, plugin):
        assert plugin._parse_favorites("") == []


class TestBuildMarkerKey:
    """The fallback path MUST be process-stable (issue: Python's hash() is
    salted by PYTHONHASHSEED, causing every soccer game to look like a new
    game on each restart and trigger spurious delete+recreate cycles)."""

    def test_uses_cfbd_id_when_present(self, plugin):
        g = {"sport_prefix": "CFB", "extra": {"cfbd_id": 12345}}
        assert plugin._build_marker_key(g) == "ranked_matchups:CFB:12345"

    def test_uses_fd_id_when_present(self, plugin):
        g = {"sport_prefix": "EPL", "extra": {"fd_id": 99999}}
        assert plugin._build_marker_key(g) == "ranked_matchups:EPL:fd_99999"

    def test_cfbd_id_wins_over_fd_id(self, plugin):
        g = {"sport_prefix": "CFB", "extra": {"cfbd_id": 1, "fd_id": 2}}
        assert plugin._build_marker_key(g) == "ranked_matchups:CFB:1"

    def test_fallback_is_stable_across_calls(self, plugin):
        g = {
            "sport_prefix": "EPL",
            "home": "Manchester United FC",
            "away": "Brentford FC",
            "start_time_utc": "2026-04-27T19:30:00Z",
            "extra": {},
        }
        a = plugin._build_marker_key(g)
        b = plugin._build_marker_key(g)
        assert a == b
        assert a.startswith("ranked_matchups:EPL:")

    def test_fallback_differs_for_different_games(self, plugin):
        g1 = {"sport_prefix": "EPL", "home": "A", "away": "B",
              "start_time_utc": "2026-04-27T19:30:00Z", "extra": {}}
        g2 = {"sport_prefix": "EPL", "home": "A", "away": "B",
              "start_time_utc": "2026-04-28T19:30:00Z", "extra": {}}
        assert plugin._build_marker_key(g1) != plugin._build_marker_key(g2)


class TestBuildWeights:
    def test_defaults(self, plugin):
        w = plugin._build_weights({})
        assert w.rank == 1.0
        assert w.spread == 0.5
        assert w.favorite == 4.0
        assert w.stakes == 2.0
        assert w.tournament == 1.5
        assert w.impact_favorite == 1.0
        assert w.narrative == 0.0

    def test_overrides(self, plugin):
        w = plugin._build_weights({"weight_rank": 3.0, "weight_narrative": 1.5})
        assert w.rank == 3.0
        assert w.narrative == 1.5

    def test_string_inputs_coerced(self, plugin):
        # Plugin settings UI sometimes hands us strings.
        w = plugin._build_weights({"weight_rank": "2.5"})
        assert w.rank == 2.5


class TestResolveKey:
    def test_setting_value_wins(self, plugin, tmp_path):
        f = tmp_path / "key"
        f.write_text("from-disk")
        out = plugin._resolve_key({"my_key": "from-settings"}, "my_key", str(f))
        assert out == "from-settings"

    def test_falls_back_to_disk(self, plugin, tmp_path):
        f = tmp_path / "key"
        f.write_text("from-disk")
        out = plugin._resolve_key({}, "my_key", str(f))
        assert out == "from-disk"

    def test_chain_of_fallbacks(self, plugin, tmp_path):
        first = tmp_path / "missing"  # doesn't exist
        second = tmp_path / "fallback"
        second.write_text("from-second")
        out = plugin._resolve_key({}, "my_key", str(first), str(second))
        assert out == "from-second"

    def test_blank_setting_falls_through(self, plugin, tmp_path):
        f = tmp_path / "key"
        f.write_text("from-disk")
        out = plugin._resolve_key({"my_key": "   "}, "my_key", str(f))
        assert out == "from-disk"

    def test_all_missing_returns_empty(self, plugin):
        out = plugin._resolve_key({}, "my_key", "/nonexistent/path")
        assert out == ""


class TestBuildSignalsScoreFromPayload:
    def test_legacy_payload_without_score_raw(self, plugin):
        # Old caches don't have score_raw; we fall back to summing the
        # breakdown (same scale as raw) — NOT to `score` (which is 0-10 scale).
        g = {
            "score": 7.6,
            "score_breakdown": {"rank_pair": 5.0, "favorite": 4.0},
        }
        signals, score = plugin._build_signals_score_from_payload(g)
        assert score.raw == 9.0  # 5 + 4, NOT 7.6
        assert score.final == 7.6


class TestResolveVirtualBase:
    def test_explicit_positive_value_wins(self, plugin):
        assert plugin._resolve_virtual_base({"virtual_channel_base": 9000}, 999) == 9000

    def test_string_input_coerced(self, plugin):
        assert plugin._resolve_virtual_base({"virtual_channel_base": "5000"}, 0) == 5000

    def test_zero_means_auto(self, plugin):
        # Auto = highest_other + 1
        assert plugin._resolve_virtual_base({"virtual_channel_base": 0}, 1234) == 1235

    def test_missing_key_means_auto(self, plugin):
        assert plugin._resolve_virtual_base({}, 50) == 51

    def test_garbage_means_auto(self, plugin):
        assert plugin._resolve_virtual_base({"virtual_channel_base": "abc"}, 50) == 51

    def test_fresh_install_uses_fallback(self, plugin):
        # No existing channels → highest_other=0 → +1=1, but we don't want to
        # return 1 (squats on prime real estate). Should bump to fallback.
        result = plugin._resolve_virtual_base({"virtual_channel_base": 0}, 0)
        assert result == plugin._AUTO_BASE_FALLBACK

    def test_negative_value_means_auto(self, plugin):
        # Negative is unparseable as a "fixed base" → auto.
        assert plugin._resolve_virtual_base({"virtual_channel_base": -5}, 100) == 101


class TestResolvParkBase:
    def test_park_above_target_range(self, plugin):
        # park must be past base + N (the highest target we'll write).
        base = 9000
        n_games = 25
        park = plugin._resolve_park_base(base, n_games)
        assert park > base + n_games

    def test_dynamic_with_huge_n(self, plugin):
        # Even with a giant cache, park stays past targets.
        assert plugin._resolve_park_base(100, 5000) > 100 + 5000

    def test_zero_games(self, plugin):
        # Empty cache shouldn't crash; we still want a sane park base.
        park = plugin._resolve_park_base(9000, 0)
        assert park > 9000

    def test_negative_games_clamped(self, plugin):
        # Defensive: never compute a park base below the target base.
        park = plugin._resolve_park_base(9000, -10)
        assert park > 9000


class TestBuildSourcesToggles:
    """Make sure each enable_* toggle actually wires its source AND that
    keys-required sources are skipped when the key is absent."""

    def test_no_key_skips_ncaaf(self, plugin, tmp_path, monkeypatch):
        # Force the on-disk fallback to also miss
        monkeypatch.setattr(plugin, "CFBD_KEY_PATH", str(tmp_path / "missing"))
        sources = plugin._build_sources({"enable_ncaaf": True})
        assert sources == []

    def test_ncaam_wired_up(self, plugin, tmp_path, monkeypatch):
        # Same Bearer token as NCAAF.
        keyfile = tmp_path / "cfbd_api_key"
        keyfile.write_text("fake-key")
        monkeypatch.setattr(plugin, "CFBD_KEY_PATH", str(keyfile))
        sources = plugin._build_sources({"enable_ncaam": True})
        assert len(sources) == 1
        assert sources[0].sport_prefix == "CBB"

    def test_both_ncaa_share_one_key(self, plugin, tmp_path, monkeypatch):
        keyfile = tmp_path / "cfbd_api_key"
        keyfile.write_text("fake-key")
        monkeypatch.setattr(plugin, "CFBD_KEY_PATH", str(keyfile))
        sources = plugin._build_sources({"enable_ncaaf": True, "enable_ncaam": True})
        prefixes = sorted(s.sport_prefix for s in sources)
        assert prefixes == ["CBB", "CFB"]

    def test_all_off_by_default(self, plugin):
        # Public-release defaults: nothing enabled until the user opts in.
        sources = plugin._build_sources({})
        assert sources == []
