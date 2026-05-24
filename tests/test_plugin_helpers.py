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
        # spread is intentionally low (0.1) because the close_game signal
        # currently fires off raw spread which is noisy. A future revision
        # will reformulate around implied win probability.
        assert w.spread == 0.1
        # favorite is bumped to 6.0 to push favorite-involved games up
        # against title-race / playoff contenders.
        assert w.favorite == 6.0
        # stakes was reduced 2.0 -> 0.5 in Phase A.5/A.6 to compensate
        # for compute_team_stakes returning leverage_in_[0,1] times
        # consequence weight (2-5 range) instead of the older 0-3
        # proximity points.
        assert w.stakes == 0.5
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

    def test_empty_settings_equals_dataclass_defaults(self, plugin):
        # Regression guard against the DRY violation that bit during
        # Phase A tuning: _build_weights had hardcoded fallback values
        # that drifted from scoring.Weights's dataclass defaults. When
        # the dataclass was bumped, runtime kept the old values until a
        # test caught it. This test pins them together — any future
        # divergence here will fail loudly on every commit.
        from dispatcharr_ranked_matchups.scoring import Weights
        defaults = Weights()
        built = plugin._build_weights({})
        assert built.rank == defaults.rank
        assert built.spread == defaults.spread
        assert built.favorite == defaults.favorite
        assert built.rivalry == defaults.rivalry
        assert built.stakes == defaults.stakes
        assert built.tournament == defaults.tournament
        assert built.impact_favorite == defaults.impact_favorite
        assert built.narrative == defaults.narrative

    def test_plugin_json_defaults_match_dataclass(self, plugin):
        # The Dispatcharr loader reads plugin.json WITHOUT executing
        # plugin code, so the manifest's per-field "default" values are
        # a third source of truth alongside scoring.Weights and
        # _build_weights. All three must agree, otherwise the UI shows
        # one number while the runtime computes with another.
        #
        # If someone bumps the dataclass without bumping plugin.json,
        # new installs surface the wrong default in the form. Saved
        # settings would then override with stale UI defaults the user
        # didn't change. This test catches the divergence at CI time.
        import json
        import os
        from dispatcharr_ranked_matchups.scoring import Weights
        defaults = Weights()
        manifest_path = os.path.join(
            os.path.dirname(os.path.abspath(plugin.__file__)), "plugin.json"
        )
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        field_defaults = {
            f["id"]: f.get("default")
            for f in manifest.get("fields", [])
            if f["id"].startswith("weight_")
        }
        # weight_<name> in plugin.json must match Weights.<name>.
        weight_field_names = [
            "rank", "spread", "favorite", "rivalry", "stakes",
            "tournament", "impact_favorite", "narrative",
        ]
        for name in weight_field_names:
            assert f"weight_{name}" in field_defaults, (
                f"plugin.json missing weight_{name} field"
            )
            assert field_defaults[f"weight_{name}"] == getattr(defaults, name), (
                f"plugin.json weight_{name} default "
                f"({field_defaults[f'weight_{name}']}) disagrees with "
                f"Weights.{name} default ({getattr(defaults, name)}). "
                f"Bump both together."
            )


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

    def test_pre_b1_impact_legacy_list_of_strings_normalized(self, plugin):
        # Pre-B.1 cache had impact_on_favorites: List[str]. Apply step
        # must accept it without crashing — gracefully degrades to
        # stakes=0/distance=0 (zero contribution) until next refresh
        # writes the new shape.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "impact_on_favorites": ["Tottenham Hotspur FC", "Wrexham AFC"],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.impact_on_favorites == [
            ("Tottenham Hotspur FC", 0.0, 0),
            ("Wrexham AFC", 0.0, 0),
        ]

    def test_b1_impact_rich_tuples_round_trip(self, plugin):
        # B.1+ cache has impact_on_favorites: List[List[name, stakes, distance]]
        # (JSON-serialized tuples). Reader must rebuild as tuples with
        # correct types so downstream destructuring works.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "impact_on_favorites": [
                ["Tottenham Hotspur FC", 5.0, 1],
                ["Wrexham AFC", 3.75, 0],
            ],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.impact_on_favorites == [
            ("Tottenham Hotspur FC", 5.0, 1),
            ("Wrexham AFC", 3.75, 0),
        ]

    def test_malformed_impact_entries_dropped(self, plugin):
        # Defensive: a hand-edited or corrupt cache shouldn't crash
        # apply. Unknown shapes are skipped, valid entries preserved.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "impact_on_favorites": [
                ["Tottenham", 5.0, 1],
                {"unexpected": "dict"},
                ["TooShort"],
                ["TooLong", 1.0, 2, 3],
            ],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.impact_on_favorites == [("Tottenham", 5.0, 1)]


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


class TestStreamQualityRank:
    """Stacked streams on a virtual channel render in rank order, so UHD/4K
    must sort before FHD which must sort before HD, etc. The helper uses
    whitespace-padded substring matching to avoid false matches like 'CHD'
    matching 'HD'."""

    def test_uhd_beats_fhd(self, plugin):
        assert plugin._stream_quality_rank("Sky UHD") < plugin._stream_quality_rank("Sky FHD")

    def test_4k_treated_as_uhd(self, plugin):
        assert plugin._stream_quality_rank("Astro Sports 4K") == plugin._stream_quality_rank("Astro Sports UHD")

    def test_fhd_beats_hd(self, plugin):
        assert plugin._stream_quality_rank("ESPN FHD") < plugin._stream_quality_rank("ESPN HD")

    def test_hd_beats_sd(self, plugin):
        assert plugin._stream_quality_rank("Channel HD") < plugin._stream_quality_rank("Channel SD")

    def test_substring_protection(self):
        # 'CHD' must NOT trigger HD bucket. Same for any word containing 'HD'
        # or 'SD' as a substring without word boundaries.
        from dispatcharr_ranked_matchups import plugin as plugin_mod
        rank_chd = plugin_mod._stream_quality_rank("CHANNELCHD")
        rank_explicit_hd = plugin_mod._stream_quality_rank("Sport HD")
        assert rank_chd != rank_explicit_hd
        assert rank_chd == plugin_mod._QUALITY_RANK_UNKNOWN

    def test_unknown_sorts_between_hd_and_sd(self, plugin):
        # No quality marker → unknown bucket. We don't want unknowns sorting
        # last (they may actually be high-quality), so they sit between HD
        # and SD.
        assert plugin._stream_quality_rank("Sport HD") < plugin._stream_quality_rank("Sport")
        assert plugin._stream_quality_rank("Sport") < plugin._stream_quality_rank("Sport SD")

    def test_empty_string(self, plugin):
        assert plugin._stream_quality_rank("") == plugin._QUALITY_RANK_UNKNOWN


class TestStreamSortKey:
    """Composite sort key trusts ffprobe data over name keywords. A stream
    that the prober has confirmed is 1080p must beat one that's only
    self-described as UHD in its name (since names lie). A stream with a
    failed probe (0x0 resolution) must sort last regardless of name."""

    def test_valid_probe_beats_name_keyword(self, plugin):
        # Probed 720p vs name-only "Sport UHD" — probed wins.
        probed_720 = plugin._stream_sort_key(
            {"width": 1280, "height": 720, "resolution": "1280x720"},
            "ESPN",
        )
        name_uhd = plugin._stream_sort_key({}, "Sport UHD")
        assert probed_720 < name_uhd

    def test_higher_resolution_sorts_first(self, plugin):
        a = plugin._stream_sort_key({"width": 1920, "height": 1080}, "")
        b = plugin._stream_sort_key({"width": 1280, "height": 720}, "")
        assert a < b

    def test_higher_bitrate_breaks_ties(self, plugin):
        # Same height, different bitrate → higher bitrate wins.
        a = plugin._stream_sort_key(
            {"width": 1920, "height": 1080, "ffmpeg_output_bitrate": 8000.0}, "",
        )
        b = plugin._stream_sort_key(
            {"width": 1920, "height": 1080, "ffmpeg_output_bitrate": 3000.0}, "",
        )
        assert a < b

    def test_failed_probe_sorts_last(self, plugin):
        # 0x0 resolution → tier 2 (last).
        failed = plugin._stream_sort_key(
            {"width": 0, "height": 0, "resolution": "0x0"}, "Sport UHD",
        )
        valid = plugin._stream_sort_key({"width": 1920, "height": 1080}, "ESPN")
        no_probe = plugin._stream_sort_key({}, "ESPN")
        assert failed > valid
        assert failed > no_probe

    def test_no_probe_uses_name_keyword(self, plugin):
        # Without probe data, falls back to keyword bucket.
        a = plugin._stream_sort_key({}, "Sport FHD")
        b = plugin._stream_sort_key({}, "Sport HD")
        assert a < b

    def test_no_probe_unknown_keyword(self, plugin):
        no_probe_unknown = plugin._stream_sort_key({}, "Random Channel")
        valid = plugin._stream_sort_key({"width": 1280, "height": 720}, "")
        # Probed beats unknown-keyword no-probe.
        assert valid < no_probe_unknown

    def test_realistic_5_stream_ordering(self, plugin):
        # Reproduces the live ch 5906 inventory:
        #   EPL 07ⓧ probed 720p 4698kbps
        #   EPL01 probed 1080p 4891kbps
        #   USA Soccer01 never probed
        #   AU STAN 01 / 02 probed 0x0 (dead)
        epl07 = plugin._stream_sort_key(
            {"width": 1280, "height": 720, "ffmpeg_output_bitrate": 4698.3},
            "EPL 07ⓧ",
        )
        epl01 = plugin._stream_sort_key(
            {"width": 1920, "height": 1080, "ffmpeg_output_bitrate": 4891.1},
            "EPL01",
        )
        usa = plugin._stream_sort_key({}, "USA Soccer01")
        au_stan = plugin._stream_sort_key(
            {"width": 0, "height": 0, "resolution": "0x0"}, "AU (STAN 01)",
        )
        ordering = sorted([
            ("EPL 07ⓧ", epl07),
            ("EPL01", epl01),
            ("USA Soccer01", usa),
            ("AU STAN", au_stan),
        ], key=lambda x: x[1])
        # EPL01 (1080p) beats EPL 07ⓧ (720p) beats unprobed beats failed.
        assert [n for n, _ in ordering] == ["EPL01", "EPL 07ⓧ", "USA Soccer01", "AU STAN"]


class TestEpgSourceConfig:
    """Regression: source_type='dummy' triggers Dispatcharr's joke-filler EPG
    overlay (Rush Hour, What's For Dinner?, etc.) on top of our real
    ProgramData. The constants must NEVER drift back to dummy. See
    EPGGridAPIView in apps/epg/api_views.py — it filters on
    epg_data__epg_source__source_type='dummy' to decide which channels need
    on-the-fly placeholder text generation."""

    def test_source_type_is_not_dummy(self, plugin):
        assert plugin.EPG_SOURCE_TYPE != "dummy"

    def test_source_type_is_xmltv(self, plugin):
        # We're producing XMLTV-equivalent program data directly via ORM.
        assert plugin.EPG_SOURCE_TYPE == "xmltv"

    def test_source_is_inactive(self, plugin):
        # is_active=False so the EPG refresh task skips us — we have no URL
        # to fetch, and we write ProgramData ourselves on apply.
        assert plugin.EPG_SOURCE_IS_ACTIVE is False


class TestPluginKey:
    """PLUGIN_KEY must equal the folder name normalized exactly the way
    Dispatcharr's apps/plugins/loader.py derives keys (lowercase,
    spaces->underscores). Anything else and PluginConfig DB lookups,
    REST routes, and the Plugins-page card all miss. The scheduler
    thread runs but the settings dict is always empty.

    This caught a real regression: PLUGIN_KEY = __package__ resolved to
    the loader's wrapper namespace (_dispatcharr_plugin_<key>) instead
    of the folder name, leaving the auto-refresh idle for 18 days."""

    def test_plugin_key_matches_folder_name(self, plugin):
        # The folder name on disk IS the canonical key Dispatcharr uses.
        # Read it the same way the loader does, then assert equality.
        import os
        expected = os.path.basename(os.path.dirname(os.path.abspath(plugin.__file__))).replace(" ", "_").lower()
        assert plugin.PLUGIN_KEY == expected

    def test_plugin_key_is_canonical_string(self, plugin):
        # The package ships under this exact folder name. If anything
        # renames the folder, the test should fail loudly until both
        # this assertion and the folder name agree again.
        assert plugin.PLUGIN_KEY == "dispatcharr_ranked_matchups"

    def test_plugin_key_not_loader_wrapper(self, plugin):
        # The exact bug the comment in plugin.py warns about: don't
        # accidentally derive PLUGIN_KEY from __package__, which would
        # return the loader's internal wrapper namespace.
        assert not plugin.PLUGIN_KEY.startswith("_dispatcharr_plugin_")

    def test_source_does_not_reintroduce_package_derivation(self, plugin):
        # Source-introspection regression guard. The original bug shape
        # was `PLUGIN_KEY = __package__ or "..."`. The three assertions
        # above can't catch a re-introduction of that pattern because
        # pytest's conftest stubs the package name to the folder name,
        # so __package__ accidentally resolves to the right string in
        # tests. Under Dispatcharr's real loader, __package__ resolves
        # to the wrapper namespace and the bug bites.
        #
        # This test reads plugin.py source directly and refuses the
        # exact failing pattern. Format-fragile by design: if the line
        # is reflowed across lines, the assertion needs updating —
        # which is a feature, not a bug. Forces a re-think.
        src = open(plugin.__file__).read()
        assert "PLUGIN_KEY = __package__" not in src, (
            "PLUGIN_KEY must derive from the folder name, not __package__. "
            "See the load-bearing comment above the PLUGIN_KEY assignment "
            "in plugin.py for why."
        )
        # Affirm the correct shape is in place.
        assert "PLUGIN_KEY = os.path.basename(PLUGIN_DIR)" in src

    def test_derivation_ignores_package_namespace(self, tmp_path):
        # The actual bug had nothing to do with the folder name being
        # right or wrong; it had to do with __package__ resolving to
        # Dispatcharr's wrapper namespace at runtime. In pytest's
        # importlib setup, __package__ is the conftest stub, which is
        # already correct by accident — so the other PLUGIN_KEY tests
        # would have passed even on the broken `PLUGIN_KEY = __package__`
        # code. This test reproduces the loader-wrap scenario directly:
        # write a tiny plugin.py-equivalent into tmp_path with the
        # CORRECT derivation, register it under the buggy wrapper-style
        # __package__ name, and assert the derivation ignores
        # __package__ and reads the folder instead.
        import importlib.util
        import sys

        plugin_dir = tmp_path / "dispatcharr_ranked_matchups"
        plugin_dir.mkdir()
        (plugin_dir / "__init__.py").write_text("")
        (plugin_dir / "tiny_plugin.py").write_text(
            "import os\n"
            "PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))\n"
            "PLUGIN_KEY = os.path.basename(PLUGIN_DIR).replace(' ', '_').lower()\n"
        )
        # Stub the package under the WRONG (loader-wrapped) name to
        # poison __package__. If the derivation used __package__, the
        # resulting PLUGIN_KEY would be the wrapper string and the
        # assertion below would fail.
        wrapper_name = "_dispatcharr_plugin_dispatcharr_ranked_matchups"
        spec = importlib.util.spec_from_file_location(
            f"{wrapper_name}.tiny_plugin",
            str(plugin_dir / "tiny_plugin.py"),
            submodule_search_locations=[str(plugin_dir)],
        )
        # Pre-register a fake parent package so the relative import
        # machinery is happy.
        import types
        fake_parent = types.ModuleType(wrapper_name)
        fake_parent.__path__ = [str(plugin_dir)]
        sys.modules[wrapper_name] = fake_parent
        try:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            # The fix: PLUGIN_KEY comes from the folder, not __package__.
            assert mod.PLUGIN_KEY == "dispatcharr_ranked_matchups"
            # Decoupling check: __package__ is the wrapper-shaped string
            # the loader produces in prod, and PLUGIN_KEY ignores it.
            # If derivation slipped back to `__package__ or "..."`, this
            # assertion would fail because the two would equal.
            assert mod.PLUGIN_KEY != mod.__package__
            assert mod.__package__.startswith("_dispatcharr_plugin_")
        finally:
            sys.modules.pop(wrapper_name, None)


class TestOwnedTvgIdQ:
    """The ownership helper covers both the current TVG_ID_PREFIX scheme
    AND legacy tvg_id values from earlier plugin versions. A prefix-only
    check misses legacy-shaped rows on a target-group rename, leaving
    orphan EPGSources visible in the UI with status='error'.

    The function builds Django Q objects with lazy import (do NOT
    hoist `from django.db.models import Q` to module level — the test
    suite relies on plugin.py keeping Django imports inside function
    bodies; see tests/conftest.py)."""

    def _q_to_dict(self, q):
        """Walk a Q tree into a comparable dict. Q nodes have .connector
        ('AND'/'OR') and .children (list of (lookup, value) tuples or
        nested Q objects)."""
        from django.db.models import Q
        if isinstance(q, Q):
            return {
                "connector": q.connector,
                "children": [self._q_to_dict(c) for c in q.children],
            }
        return q  # leaf: (lookup, value) tuple

    def test_returns_a_q_object(self, plugin):
        from django.db.models import Q
        q = plugin._owned_tvg_id_q()
        assert isinstance(q, Q)

    def test_or_connector_at_top(self, plugin):
        # Top-level Q must OR the prefix-startswith branch with the
        # legacy-marker branch. AND would be a bug (no row could match
        # both at once).
        q = plugin._owned_tvg_id_q()
        assert q.connector == "OR"

    def test_includes_current_prefix_lookup(self, plugin):
        # The prefix branch must use tvg_id__startswith against
        # TVG_ID_PREFIX. If the lookup type ever changes (e.g. iexact),
        # cleanup will silently miss rows.
        shape = self._q_to_dict(plugin._owned_tvg_id_q())
        leaves = self._all_leaves(shape)
        assert ("tvg_id__startswith", plugin.TVG_ID_PREFIX) in leaves

    def test_includes_all_legacy_markers(self, plugin):
        # The legacy branch must use tvg_id__in against the full marker
        # tuple. Single-value lookups would only catch one historical
        # scheme; tuple-in catches every appended marker.
        shape = self._q_to_dict(plugin._owned_tvg_id_q())
        leaves = self._all_leaves(shape)
        assert ("tvg_id__in", plugin._OWNED_TVG_ID_LEGACY_MARKERS) in leaves

    def test_field_prefix_prepends_lookup_path(self, plugin):
        # For joined queries from EPGSource ('epgs__') or ChannelGroup
        # ('channels__'), the helper must rewrite both lookups to walk
        # the relation. If prefix application is incomplete, half the
        # check looks at the wrong table.
        shape = self._q_to_dict(plugin._owned_tvg_id_q("epgs__"))
        leaves = self._all_leaves(shape)
        assert ("epgs__tvg_id__startswith", plugin.TVG_ID_PREFIX) in leaves
        assert ("epgs__tvg_id__in", plugin._OWNED_TVG_ID_LEGACY_MARKERS) in leaves

    def test_empty_field_prefix_is_default(self, plugin):
        # The default empty prefix must not introduce a leading
        # underscore or other accidental prefix character.
        shape = self._q_to_dict(plugin._owned_tvg_id_q())
        leaves = self._all_leaves(shape)
        for lookup, _ in leaves:
            assert not lookup.startswith("_")
            assert lookup.startswith("tvg_id")

    def test_legacy_markers_tuple_is_immutable(self, plugin):
        # Tuple, not list. A mutable default would let any caller
        # silently extend the markers list across the whole process.
        assert isinstance(plugin._OWNED_TVG_ID_LEGACY_MARKERS, tuple)

    def test_legacy_markers_contains_known_history(self, plugin):
        # 'dummy_top_matchups' is the documented legacy marker from
        # earlier plugin versions. The comment in plugin.py says
        # never remove entries from this tuple; this test enforces that.
        assert "dummy_top_matchups" in plugin._OWNED_TVG_ID_LEGACY_MARKERS

    def _all_leaves(self, shape):
        """Flatten a nested Q-shape dict into a list of leaf
        (lookup, value) tuples regardless of OR/AND nesting depth."""
        if isinstance(shape, dict):
            out = []
            for child in shape["children"]:
                out.extend(self._all_leaves(child))
            return out
        return [shape]
