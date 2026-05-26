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
        # B.3 reformulated the close-game signal from raw spread to a
        # [0, 1] coinflip-ness measure (devigged bookmaker probabilities
        # for soccer; spread normalized for NCAAF / NCAAM). Default
        # bumped 0.1 → 3.0 because the underlying range shrank from
        # [0, 7] to [0, 1] — same per-game magnitude (~3 raw for a
        # pick'em) preserved across the formula change.
        assert w.spread == 3.0
        # favorite is bumped to 6.0 to push favorite-involved games up
        # against title-race / playoff contenders.
        assert w.favorite == 6.0
        assert w.tournament == 1.5
        assert w.narrative == 0.0
        # Phase C.4: weight_importance bumped 1.0 -> 3.0 when legacy
        # stakes/impact/late_mult signals were retired. The structural
        # importance signal carries the same magnitude as the legacy
        # family at this weight.
        assert w.importance == 3.0

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
        assert built.tournament == defaults.tournament
        assert built.narrative == defaults.narrative
        assert built.importance == defaults.importance

    def test_legacy_settings_ignored(self, plugin):
        # Phase C.4 removed Weights.stakes and Weights.impact_favorite. A
        # saved PluginConfig from before C.4 will still have weight_stakes
        # and weight_impact_favorite in its settings JSON; _build_weights
        # must ignore them without raising. Drop after the next plugin
        # bump cycle when no real installs have stale keys.
        w = plugin._build_weights({
            "weight_stakes": 7.0,
            "weight_impact_favorite": 5.0,
            "weight_rank": 1.0,
        })
        assert w.rank == 1.0
        # Stale keys silently dropped — no AttributeError, no surprise
        # contribution.
        assert not hasattr(w, "stakes")
        assert not hasattr(w, "impact_favorite")

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
            "rank", "spread", "favorite", "rivalry",
            "tournament", "narrative", "importance",
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

    def test_pre_c4_cache_with_legacy_impact_field_ignored(self, plugin):
        # Pre-C.4 cache.json carries `impact_on_favorites` which the new
        # GameSignals doesn't have. Reader must drop the field silently
        # so apply doesn't crash reading a cache written by older code.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "impact_on_favorites": [
                ["Tottenham Hotspur FC", 5.0, 1],
                ["Wrexham AFC", 3.75, 0],
            ],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        # No crash; the field doesn't exist on the new GameSignals.
        assert not hasattr(signals, "impact_on_favorites")

    def test_pre_c4_stakes_thresholds_hit_fallback(self, plugin):
        # Pre-C.4 cache stored thresholds under stakes_thresholds_hit; the
        # post-C.4 key is importance_thresholds_hit. Reader prefers the new
        # key but falls back to the old one for one-cycle migration.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "stakes_thresholds_hit": ["title", "UCL"],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.importance_thresholds_hit == ["title", "UCL"]

    def test_post_c4_importance_thresholds_hit_preferred(self, plugin):
        # When both keys are present (a cache rewritten mid-migration),
        # the new key wins.
        g = {
            "score": 5.0,
            "score_breakdown": {},
            "stakes_thresholds_hit": ["LEGACY"],
            "importance_thresholds_hit": ["NEW"],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.importance_thresholds_hit == ["NEW"]

    def test_phase_c_importance_round_trip(self, plugin):
        # Phase C: importance_points + importance_notes survive the
        # cache write/read cycle. apply needs them on signals so the
        # narrative-description layer (built from signals) sees the
        # same numbers refresh produced.
        g = {
            "score": 7.0,
            "score_breakdown": {"importance": 4.5},
            "importance_points": 4.5,
            "importance_notes": [
                "Tottenham relegation: 0.50 leverage × 5.0 = 2.50",
                "Everton relegation: 0.30 leverage × 5.0 = 1.50",
            ],
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.importance_points == 4.5
        assert len(signals.importance_notes) == 2
        assert "Tottenham" in signals.importance_notes[0]

    def test_pre_c_cache_lacks_importance_fields_no_crash(self, plugin):
        # Pre-C.3 caches don't have importance_points / importance_notes.
        # Reader must default cleanly to 0.0 / [] so apply doesn't choke
        # on the missing keys when reading a cache from before this
        # phase deployed.
        g = {
            "score": 5.0,
            "score_breakdown": {"rank_pair": 5.0},
        }
        signals, _ = plugin._build_signals_score_from_payload(g)
        assert signals.importance_points == 0.0
        assert signals.importance_notes == []


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


class TestBuildDescription:
    """_build_description is the sole production EPG-description renderer
    after the build_why_text deletion (PR #42, issue #15). It's a pure
    function (Dict + str + bool → str), so we can pin every output block
    here without touching Django.

    Six blocks, joined by blank lines:
      1. Placeholder note (when placeholder=True).
      2. Headline: "A/An {tagline} — {spread_desc}." (each piece optional).
      3. Matchday + league boundary summary.
      4. Favorite-impact narratives.
      5. Favorite-is-playing line ("X is your favorite." / "Your favorites: ...").
      6. Source channel line ("Source: ...").
    """

    @staticmethod
    def _g(**overrides):
        """Build a minimal game dict. Pass keyword overrides to add
        specific cache fields per test."""
        g = {}
        g.update(overrides)
        return g

    # ---------- block 1: placeholder ----------

    def test_empty_input_returns_empty_string(self, plugin):
        # No tagline, no placeholder, no extra fields → no sections.
        assert plugin._build_description(self._g(), "", False) == ""

    def test_placeholder_note_appears_first(self, plugin):
        out = plugin._build_description(self._g(), "", placeholder=True)
        # Leading underscore = markdown italic — the renderer relies on
        # the placeholder note being italic, not plain text.
        assert out.startswith("_Channel match pending")
        assert "next refresh" in out

    def test_placeholder_false_omits_note(self, plugin):
        out = plugin._build_description(self._g(), "title race", placeholder=False)
        assert "Channel match pending" not in out

    # ---------- block 2: headline ----------

    def test_headline_with_consonant_tagline_uses_A(self, plugin):
        out = plugin._build_description(self._g(), "title race", False)
        assert out.startswith("A title race.")

    def test_headline_with_vowel_tagline_uses_An(self, plugin):
        # All five vowels.
        for vowel_tagline in ["a-game decider", "elite matchup",
                              "in-form clash", "opener", "underdog story"]:
            out = plugin._build_description(self._g(), vowel_tagline, False)
            assert out.startswith(f"An {vowel_tagline}.")

    def test_headline_vowel_check_is_case_insensitive(self, plugin):
        # "Elite" must read as vowel-starting just like "elite".
        out = plugin._build_description(self._g(), "Elite matchup", False)
        assert out.startswith("An Elite matchup.")

    def test_no_tagline_skips_headline(self, plugin):
        # Empty tagline + no spread = no headline section at all.
        out = plugin._build_description(self._g(), "", False)
        assert "A " not in out and "An " not in out

    def test_headline_with_closeness_toss_up(self, plugin):
        # closeness >= 0.7 → "toss-up". Em-dash separates the two parts.
        out = plugin._build_description(self._g(closeness=0.8), "title race", False)
        assert "A title race — toss-up." in out

    def test_headline_with_only_spread_no_tagline(self, plugin):
        # spread alone (no tagline) still produces a headline line
        # because the spread descriptor is part of the headline block.
        out = plugin._build_description(self._g(spread=2.0), "", False)
        assert "toss-up." in out
        assert not out.startswith("A ")  # no article without a tagline

    def test_headline_terminates_with_period(self, plugin):
        out = plugin._build_description(self._g(), "title race", False)
        # The period is intentional — without it, EPG clients sometimes
        # run the headline into the following block.
        assert out.split("\n\n", 1)[0].endswith(".")

    # ---------- block 3: matchday + league boundary ----------

    def test_matchday_line_uses_explicit_totals(self, plugin):
        g = self._g(extra={"matchday": 7, "matchdays_total": 38})
        out = plugin._build_description(g, "", False)
        assert "Matchday 7 of 38." in out

    def test_matchday_line_falls_back_to_league_ctx_total(self, plugin):
        # When `matchdays_total` isn't in extra but fd_competition_code
        # resolves to a LEAGUE_CONTEXTS entry with a known total, that
        # total fills in. PL is 38.
        g = self._g(extra={"matchday": 5, "fd_competition_code": "PL"})
        out = plugin._build_description(g, "", False)
        assert "Matchday 5 of 38." in out

    def test_league_boundary_summary_appears(self, plugin):
        # PL's boundary_summary is the threshold mnemonic users see.
        g = self._g(extra={"fd_competition_code": "PL"})
        out = plugin._build_description(g, "", False)
        # Boundary summary content comes from LEAGUE_CONTEXTS["PL"]
        # ("Top 4 → UCL · 5-7 → Europa · bottom 3 → relegation").
        assert "UCL" in out and "relegation" in out

    def test_matchday_without_total_omits_line(self, plugin):
        # If neither extra.matchdays_total nor league_ctx.matchdays_total
        # are available, no matchday line is emitted (we don't want
        # "Matchday 7" floating without context).
        g = self._g(extra={"matchday": 7})
        out = plugin._build_description(g, "", False)
        assert "Matchday 7" not in out

    # ---------- block 4: impact narratives ----------

    def test_impact_narratives_in_extra(self, plugin):
        g = self._g(extra={"impact_narratives": [
            "City fans: every dropped point closes the title race.",
            "Arsenal supporters: this is a four-point swing.",
        ]})
        out = plugin._build_description(g, "", False)
        assert "every dropped point" in out
        assert "four-point swing" in out

    def test_impact_narratives_top_level_fallback(self, plugin):
        # Older cache entries kept narratives at the top level; the
        # fallback path keeps them rendering.
        g = self._g(impact_narratives=["Top-level narrative."])
        out = plugin._build_description(g, "", False)
        assert "Top-level narrative." in out

    def test_extra_narratives_win_over_top_level(self, plugin):
        # When both are present, the canonical (extra) location wins.
        # Top-level fallback exists only for old-shape cache files.
        g = self._g(
            extra={"impact_narratives": ["From extra."]},
            impact_narratives=["From top level."],
        )
        out = plugin._build_description(g, "", False)
        assert "From extra." in out
        assert "From top level." not in out

    def test_empty_narratives_list_omits_section(self, plugin):
        g = self._g(extra={"impact_narratives": []})
        out = plugin._build_description(g, "title race", False)
        # Only the headline survives.
        assert out == "A title race."

    # ---------- block 5: favorite-is-playing ----------

    def test_single_favorite_singular_form(self, plugin):
        g = self._g(favorites_matched=["Wrexham"])
        out = plugin._build_description(g, "", False)
        assert "Wrexham is your favorite." in out

    def test_multiple_favorites_plural_form(self, plugin):
        g = self._g(favorites_matched=["Wrexham", "Barcelona"])
        out = plugin._build_description(g, "", False)
        assert "Your favorites: Wrexham, Barcelona." in out
        # Singular form must NOT appear when there are multiple.
        assert "is your favorite" not in out

    def test_no_favorites_omits_line(self, plugin):
        g = self._g(favorites_matched=[])
        out = plugin._build_description(g, "title race", False)
        assert "favorite" not in out

    # ---------- block 6: source channel ----------

    def test_source_channel_line(self, plugin):
        g = self._g(channel_name_current="ESPN")
        out = plugin._build_description(g, "", False)
        assert "Source: ESPN." in out

    def test_no_source_channel_omits_line(self, plugin):
        g = self._g()
        out = plugin._build_description(g, "", False)
        assert "Source:" not in out

    # ---------- structure: ordering + separator ----------

    def test_blocks_are_separated_by_blank_lines(self, plugin):
        # Every present block is joined by `\n\n` (a blank line between
        # them so EPG clients render paragraph breaks). With three
        # blocks present we expect exactly two blank-line separators.
        g = self._g(
            extra={"matchday": 7, "matchdays_total": 38},
            favorites_matched=["Wrexham"],
        )
        out = plugin._build_description(g, "title race", False)
        # 3 sections (headline, matchday, favorite) → 2 separators.
        assert out.count("\n\n") == 2

    def test_all_six_blocks_in_documented_order(self, plugin):
        # Build a game that exercises every block, then assert each
        # marker substring appears in the order the docstring says.
        g = self._g(
            extra={
                "matchday": 5,
                "matchdays_total": 38,
                "impact_narratives": ["Impact narrative here."],
                "fd_competition_code": "PL",
            },
            closeness=0.8,
            favorites_matched=["Wrexham"],
            channel_name_current="ESPN",
        )
        out = plugin._build_description(g, "title race", placeholder=True)
        # Use .find() to pin order — earlier marker must have lower index.
        pos_placeholder = out.find("Channel match pending")
        pos_headline = out.find("A title race")
        pos_matchday = out.find("Matchday 5")
        pos_narrative = out.find("Impact narrative here.")
        pos_favorite = out.find("is your favorite")
        pos_source = out.find("Source: ESPN")
        # All blocks present.
        positions = [pos_placeholder, pos_headline, pos_matchday,
                     pos_narrative, pos_favorite, pos_source]
        assert all(p >= 0 for p in positions), f"missing block: {positions}"
        # Strict order.
        assert positions == sorted(positions)


# ---------- #49: polished EPG program titles + past slot ----------


class TestFormatMatchup:
    def test_basic_pair(self, plugin):
        assert plugin._format_matchup("Auburn", "Vanderbilt") == "Auburn vs Vanderbilt"

    def test_no_star_prefix(self, plugin):
        # The matchup string explicitly omits the ★ score prefix that the
        # channel name carries — the EPG title should read like a real
        # program guide entry, not a debug breadcrumb.
        result = plugin._format_matchup("Auburn", "Vanderbilt")
        assert "★" not in result
        assert "vs" in result


class TestBuildProgramTitle:
    def test_upcoming_with_kickoff(self, plugin):
        title = plugin._build_program_title(
            "upcoming", "Auburn vs Vanderbilt", "Fri 7:30 PM EDT",
        )
        assert title == "Upcoming: Auburn vs Vanderbilt, Fri 7:30 PM EDT"

    def test_upcoming_without_kickoff_omits_separator(self, plugin):
        # Empty kickoff_local string falls back to just the matchup —
        # no trailing comma.
        title = plugin._build_program_title("upcoming", "Auburn vs Vanderbilt", "")
        assert title == "Upcoming: Auburn vs Vanderbilt"

    def test_live_marker(self, plugin):
        title = plugin._build_program_title("live", "Auburn vs Vanderbilt", "ignored")
        # Unicode "ᴸᶦᵛᵉ" superscript marker matches the broadcast EPG
        # convention. Modern EPG clients (TiviMate, Plex, Jellyfin) all
        # render this glyph correctly.
        assert title == "Auburn vs Vanderbilt ᴸᶦᵛᵉ"

    def test_past_prefix(self, plugin):
        title = plugin._build_program_title("past", "Auburn vs Vanderbilt", "ignored")
        assert title == "Past: Auburn vs Vanderbilt"

    def test_unknown_state_raises(self, plugin):
        import pytest
        with pytest.raises(ValueError):
            plugin._build_program_title("inplay", "Auburn vs Vanderbilt", "")

    def test_truncation_at_255_chars(self, plugin):
        # ProgramData.title is varchar(255) — anything longer gets
        # truncated with ellipsis. The Upcoming prefix + matchup + a
        # very long team name combo must not blow the column.
        long_team = "A" * 300
        title = plugin._build_program_title("upcoming", f"{long_team} vs B", "")
        assert len(title) == 255
        assert title.endswith("...")


class TestComputePastSlotEnd:
    def test_uses_next_scheduled_fire_time(self, plugin):
        from datetime import datetime, timezone
        # Game ends Friday 22:00 UTC = Friday 18:00 ET.
        prog_end = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc)
        settings = {
            "auto_refresh_enabled": True,
            "local_timezone": "America/New_York",
            # Refreshes at 04:00, 10:00, 16:00, 22:00 ET.
            "scheduled_times": "0400,1000,1600,2200",
        }
        past_end = plugin._compute_past_slot_end(prog_end, settings)
        # Next fire after 18:00 ET is 22:00 ET same day = 02:00 UTC next day.
        assert past_end == datetime(2026, 6, 14, 2, 0, tzinfo=timezone.utc)

    def test_falls_back_to_12h_when_auto_refresh_disabled(self, plugin):
        from datetime import datetime, timezone, timedelta
        prog_end = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc)
        settings = {
            "auto_refresh_enabled": False,
            "scheduled_times": "0400,1000,1600,2200",
        }
        past_end = plugin._compute_past_slot_end(prog_end, settings)
        assert past_end == prog_end + timedelta(hours=12)

    def test_falls_back_to_12h_when_no_scheduled_times(self, plugin):
        from datetime import datetime, timezone, timedelta
        prog_end = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc)
        settings = {
            "auto_refresh_enabled": True,
            "local_timezone": "America/New_York",
            "scheduled_times": "",
        }
        past_end = plugin._compute_past_slot_end(prog_end, settings)
        assert past_end == prog_end + timedelta(hours=12)

    def test_falls_back_to_12h_when_scheduled_times_malformed(self, plugin):
        # All-garbage scheduled_times → empty list → fallback.
        from datetime import datetime, timezone, timedelta
        prog_end = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc)
        settings = {
            "auto_refresh_enabled": True,
            "local_timezone": "America/New_York",
            "scheduled_times": "abcd,9999,nope",
        }
        past_end = plugin._compute_past_slot_end(prog_end, settings)
        assert past_end == prog_end + timedelta(hours=12)

    def test_naive_prog_end_is_normalized_to_utc(self, plugin):
        # Defensive: caller passes a naive datetime → we attach UTC
        # rather than raise. Naive datetimes round-trip through some
        # pickling paths in older cache files.
        from datetime import datetime, timedelta
        prog_end_naive = datetime(2026, 6, 13, 22, 0)
        settings = {"auto_refresh_enabled": False}
        past_end = plugin._compute_past_slot_end(prog_end_naive, settings)
        # Should be 12h later, tz-aware.
        assert past_end.tzinfo is not None
        # Verify the result is exactly prog_end + 12h interpreted as UTC.
        from datetime import timezone
        expected = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc) + timedelta(hours=12)
        assert past_end == expected

    def test_returns_utc_aware_datetime(self, plugin):
        # ProgramData.end_time stores UTC; the helper must return a
        # tz-aware datetime in UTC even when the next-fire calculation
        # ran in a non-UTC local tz.
        from datetime import datetime, timezone
        prog_end = datetime(2026, 6, 13, 22, 0, tzinfo=timezone.utc)
        settings = {
            "auto_refresh_enabled": True,
            "local_timezone": "America/New_York",
            "scheduled_times": "0400,1000,1600,2200",
        }
        past_end = plugin._compute_past_slot_end(prog_end, settings)
        assert past_end.tzinfo is not None
        assert past_end.utcoffset().total_seconds() == 0


class TestOrdinal:
    def test_first(self, plugin):
        assert plugin._ordinal(1) == "1st"

    def test_second(self, plugin):
        assert plugin._ordinal(2) == "2nd"

    def test_third(self, plugin):
        assert plugin._ordinal(3) == "3rd"

    def test_fourth(self, plugin):
        assert plugin._ordinal(4) == "4th"

    def test_teens_always_th(self, plugin):
        # 11/12/13 break the simple last-digit rule.
        assert plugin._ordinal(11) == "11th"
        assert plugin._ordinal(12) == "12th"
        assert plugin._ordinal(13) == "13th"

    def test_twenties(self, plugin):
        assert plugin._ordinal(21) == "21st"
        assert plugin._ordinal(22) == "22nd"
        assert plugin._ordinal(23) == "23rd"
        assert plugin._ordinal(24) == "24th"

    def test_typical_league_positions(self, plugin):
        # Cover the full EPL range (20-team league).
        assert plugin._ordinal(20) == "20th"


class TestBuildStandingsPostureLine:
    def _table(self):
        # Minimal EPL-shaped table — 3 teams enough to exercise the path.
        return [
            {"name": "Manchester City FC", "position": 2, "points": 70, "played": 35},
            {"name": "Manchester United FC", "position": 3, "points": 69, "played": 35},
            {"name": "Bournemouth FC", "position": 14, "points": 41, "played": 35},
        ]

    def test_none_when_no_table(self, plugin):
        g = {"home": "X", "away": "Y", "extra": {}}
        assert plugin._build_standings_posture_line(g) is None

    def test_none_when_neither_team_in_table(self, plugin):
        g = {
            "home": "Promoted Club A",
            "away": "Promoted Club B",
            "extra": {"standings_table": self._table()},
        }
        assert plugin._build_standings_posture_line(g) is None

    def test_both_teams_close_in_table(self, plugin):
        g = {
            "home": "Manchester City FC",
            "away": "Manchester United FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Manchester City FC 2nd, 70 pts. Manchester United FC 3rd, 69 pts — 1 pt behind."

    def test_both_teams_wide_gap(self, plugin):
        g = {
            "home": "Manchester City FC",
            "away": "Bournemouth FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Manchester City FC 2nd, 70 pts. Bournemouth FC 14th, 41 pts — 29 pts behind."

    def test_away_team_ahead(self, plugin):
        # When home team is lower-ranked, away team's gap reads "ahead".
        g = {
            "home": "Bournemouth FC",
            "away": "Manchester City FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Bournemouth FC 14th, 41 pts. Manchester City FC 2nd, 70 pts — 29 pts ahead."

    def test_tied_on_points_no_gd_cached(self, plugin):
        # Older caches (pre-#10) won't have goal_difference; fall back to
        # the bare framing.
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35},
            {"name": "B FC", "position": 2, "points": 70, "played": 35},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts — level on points."

    def test_tied_on_points_away_gd_better(self, plugin):
        # B has the better GD — reads "... GD ahead" for the away team.
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 15},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 22},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts — level on points, 7 GD ahead."

    def test_tied_on_points_home_gd_better(self, plugin):
        # A (home) has better GD — away reads "behind on GD".
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 22},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 15},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts — level on points, 7 GD behind."

    def test_tied_on_everything(self, plugin):
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 22},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 22},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts — level on points and goal difference."

    def test_one_pt_uses_singular(self, plugin):
        # 1 → "1 pt", not "1 pts".
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35},
            {"name": "B FC", "position": 2, "points": 69, "played": 35},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert "1 pt behind" in line
        assert "1 pts" not in line

    def test_only_home_in_table(self, plugin):
        g = {
            "home": "Manchester City FC",
            "away": "Promoted Cold-Start Club",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Manchester City FC 2nd, 70 pts."

    def test_only_away_in_table(self, plugin):
        g = {
            "home": "Promoted Cold-Start Club",
            "away": "Manchester City FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Manchester City FC 2nd, 70 pts."

    def test_missing_position_skips_team(self, plugin):
        # Defensive: if FD.org returns an entry with no position, treat as
        # "team not in standings" rather than rendering "Nonepth".
        table = [
            {"name": "A FC", "position": None, "points": 70, "played": 35},
            {"name": "B FC", "position": 2, "points": 69, "played": 35},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "B FC 2nd, 69 pts."

    def test_missing_home_away_returns_none(self, plugin):
        g = {"home": "", "away": "", "extra": {"standings_table": self._table()}}
        assert plugin._build_standings_posture_line(g) is None

    def test_integration_with_build_description(self, plugin):
        # End-to-end: the new section appears between matchday and impact.
        g = {
            "home": "Manchester City FC",
            "away": "Manchester United FC",
            "sport_prefix": "EPL",
            "closeness": None,
            "spread": None,
            "extra": {
                "matchday": 35,
                "matchdays_total": 38,
                "standings_table": self._table(),
                "impact_narratives": ["Manchester City could clinch the title with a win."],
                "fd_competition_code": "PL",
            },
            "favorites_matched": [],
        }
        desc = plugin._build_description(g, tagline="title race", placeholder=False)
        sections = desc.split("\n\n")
        # Matchday should precede the standings posture line, which should
        # precede the impact narrative.
        md_idx = next(i for i, s in enumerate(sections) if s.startswith("Matchday 35"))
        st_idx = next(i for i, s in enumerate(sections) if "70 pts" in s)
        nar_idx = next(i for i, s in enumerate(sections) if "clinch the title" in s)
        assert md_idx < st_idx < nar_idx
