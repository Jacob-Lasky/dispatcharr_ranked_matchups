"""#208: the settings modal was shortened by merging fields, and installs saved
before the merge must keep behaving exactly as they did.

What was merged (see merged_settings.py for the full rationale):
  - seven `weight_<name>` numbers -> one `scoring_weights` text field;
  - `enable_ncaaf` + `ncaaf_divisions` -> one `ncaaf` select;
  - six pairs of league toggles -> one four-way select each.

The tests build settings the two ways production does:
  - the run() path: the stored dict with every manifest field's default filled
    in, exactly as Dispatcharr's loader does it (`_loader_merge` below copies
    `PluginManager._merge_settings_with_defaults` from apps/plugins/loader.py,
    0.30.0), using the REAL plugin.json fields;
  - the scheduler / reaper path: the raw stored dict (get_current_settings).

Expected values are literals, not the constants under test.
"""
import contextlib
import json
import os
import sys
import types

import pytest

from dispatcharr_ranked_matchups import merged_settings as ms

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _manifest_fields():
    with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as fh:
        return json.load(fh)["fields"]


def _loader_merge(stored):
    """apps/plugins/loader.py PluginManager._merge_settings_with_defaults
    (Dispatcharr 0.30.0), verbatim in behaviour: a field's default is filled
    in only when the stored dict lacks the key."""
    merged = dict(stored or {})
    for field_def in _manifest_fields():
        field_id = field_def.get("id")
        if not field_id:
            continue
        if field_id not in merged and "default" in field_def:
            merged[field_id] = field_def.get("default")
    return merged


# A real install's stored settings as saved by a pre-#208 release (Jake's,
# read 2026-10-11, API keys and unrelated fields dropped). Note what is
# ABSENT: enable_atp / enable_wta and weight_tournament / weight_importance were
# never saved, so they ran at their defaults.
PRE_208_STORED = {
    "enable_ncaaf": True,
    "enable_ncaam": True,
    "enable_intl_friendlies": True,
    "enable_efl_cup": True,
    "enable_fa_cup": True,
    "enable_ncaa_baseball": True,
    "enable_ncaa_mens_soccer": True,
    "enable_ncaa_womens_soccer": False,
    "enable_epl": True,
    "enable_euros": False,
    "weight_favorite": 6,
    "weight_narrative": 1,
    "weight_rank": 1,
    "weight_rivalry": 2,
    "weight_spread": 0.1,
    "max_games": 25,
}

ON_BEFORE = {
    "enable_ncaaf", "enable_ncaam", "enable_intl_friendlies", "enable_efl_cup",
    "enable_fa_cup", "enable_ncaa_baseball", "enable_ncaa_mens_soccer",
}
OFF_BEFORE = {
    "enable_ncaaw_basketball", "enable_intl_friendlies_women", "enable_atp",
    "enable_wta", "enable_ncaa_softball", "enable_ncaa_womens_soccer",
}

PATHS = {
    "run": _loader_merge,             # context["settings"]
    "scheduler": lambda s: dict(s),   # get_current_settings()
}


@pytest.fixture
def no_network(monkeypatch):
    import requests

    def _refuse(*a, **kw):
        raise AssertionError("constructing a source must not touch the network")
    monkeypatch.setattr(requests, "get", _refuse)
    monkeypatch.setattr(requests.Session, "get", _refuse)


@pytest.fixture
def cfbd_key(plugin, tmp_path, monkeypatch):
    keyfile = tmp_path / "cfbd_api_key"
    keyfile.write_text("fake-key")
    monkeypatch.setattr(plugin, "CFBD_KEY_PATH", str(keyfile))


# ---------------------------------------------------------------------------
# An install saved before #208 behaves exactly as it did.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", sorted(PATHS))
class TestPre208SettingsKeepTheirBehaviour:

    def test_merged_leagues_stay_on_and_off(self, path):
        settings = PATHS[path](PRE_208_STORED)
        assert {t for t in ON_BEFORE | OFF_BEFORE if ms.league_enabled(settings, t)} == ON_BEFORE

    def test_unmerged_league_toggles_untouched(self, path):
        settings = PATHS[path](PRE_208_STORED)
        assert ms.league_enabled(settings, "enable_epl") is True
        assert ms.league_enabled(settings, "enable_euros") is False
        assert ms.league_enabled(settings, "enable_nfl") is False

    def test_tuned_weights_still_apply(self, plugin, path):
        w = plugin._build_weights(PATHS[path](PRE_208_STORED))
        assert (w.rank, w.spread, w.favorite, w.rivalry, w.narrative) == (1.0, 0.1, 6.0, 2.0, 1.0)
        # Never saved -> still the defaults.
        assert (w.tournament, w.importance) == (1.5, 3.0)

    def test_ncaaf_divisions_still_default_to_fbs(self, plugin, cfbd_key, path):
        sources = plugin._build_sources(PATHS[path]({"enable_ncaaf": True}))
        assert [(type(s).__name__, s.divisions) for s in sources] == [("NcaafSource", "fbs")]

    def test_ncaaf_saved_divisions_still_forwarded(self, plugin, cfbd_key, path):
        stored = {"enable_ncaaf": True, "ncaaf_divisions": "fbs_fcs"}
        sources = plugin._build_sources(PATHS[path](stored))
        assert [s.divisions for s in sources] == ["fbs_fcs"]

    def test_ncaaf_divisions_without_the_toggle_stays_off(self, plugin, cfbd_key, path):
        """A saved divisions choice never switched NCAAF on by itself."""
        stored = {"enable_ncaaf": False, "ncaaf_divisions": "fbs_fcs"}
        assert plugin._build_sources(PATHS[path](stored)) == []

    def test_paired_sources_still_built(self, plugin, no_network, path):
        stored = {"enable_wta": True, "enable_fa_cup": True,
                  "enable_intl_friendlies_women": True}
        names = sorted(type(s).__name__ for s in plugin._build_sources(PATHS[path](stored)))
        assert names == ["FaCupSource", "InternationalFriendliesSource", "WtaSource"]
        friendly = next(s for s in plugin._build_sources(PATHS[path](stored))
                        if type(s).__name__ == "InternationalFriendliesSource")
        assert friendly.gender == "w"


# ---------------------------------------------------------------------------
# The new fields, used directly.
# ---------------------------------------------------------------------------

class TestPairSelects:

    @pytest.mark.parametrize("value, atp, wta", [
        ("off", False, False), ("atp", True, False), ("wta", False, True),
        ("both", True, True), (None, False, False), ("", False, False),
        ("nonsense", False, False),
    ])
    def test_tennis_select(self, value, atp, wta):
        settings = {"tennis": value}
        assert ms.league_enabled(settings, "enable_atp") is atp
        assert ms.league_enabled(settings, "enable_wta") is wta

    def test_both_builds_both_sources(self, plugin, no_network):
        names = sorted(type(s).__name__ for s in plugin._build_sources(
            _loader_merge({"english_cups": "both", "tennis": "atp"})))
        assert names == ["AtpSource", "EflCupSource", "FaCupSource"]

    def test_fresh_install_enables_nothing(self, plugin):
        assert plugin._build_sources(_loader_merge({})) == []

    def test_ncaaf_select(self, plugin, cfbd_key):
        assert plugin._build_sources(_loader_merge({"ncaaf": "off"})) == []
        sources = plugin._build_sources(_loader_merge({"ncaaf": "fbs_fcs"}))
        assert [(type(s).__name__, s.divisions) for s in sources] == [("NcaafSource", "fbs_fcs")]

    def test_ncaa_basketball_select_routes_each_gender(self, plugin, cfbd_key, monkeypatch):
        # Women's builds its regular-season source and seeds the playoff source
        # from it, which fetches; stub that seam out, the wiring is what is
        # under test.
        from dispatcharr_ranked_matchups.sources import ncaaw_basketball
        monkeypatch.setattr(ncaaw_basketball.NcaawBasketballRegularSource,
                            "estimate_strengths", lambda self: {})
        men = plugin._build_sources(_loader_merge({"ncaa_basketball": "men"}))
        assert [type(s).__name__ for s in men] == ["NcaamSource"]
        women = plugin._build_sources(_loader_merge({"ncaa_basketball": "women"}))
        assert "NcaamSource" not in {type(s).__name__ for s in women}
        assert any(type(s).__name__.startswith("NcaawBasketball") for s in women)


class TestPrecedence:

    def test_new_value_beats_legacy_on_an_unmigrated_install(self):
        settings = _loader_merge({"enable_atp": True, "tennis": "wta"})
        assert ms.league_enabled(settings, "enable_atp") is False
        assert ms.league_enabled(settings, "enable_wta") is True

    def test_after_migration_off_means_off(self):
        """The reason for the marker: once migrated, a user choosing "Off"
        (the default) must win over a lingering legacy True."""
        settings = _loader_merge({"enable_atp": True, "tennis": "off", "settings_schema": 2})
        assert ms.league_enabled(settings, "enable_atp") is False

    def test_after_migration_ncaaf_off_means_off(self, plugin, cfbd_key):
        stored = {"enable_ncaaf": True, "ncaaf": "off", "settings_schema": 2}
        assert plugin._build_sources(_loader_merge(stored)) == []

    def test_typed_weight_beats_legacy_and_legacy_fills_the_rest(self, plugin):
        stored = {"scoring_weights": "spread=5", "weight_spread": 0.1, "weight_rank": 2}
        w = plugin._build_weights(_loader_merge(stored))
        assert (w.spread, w.rank, w.favorite) == (5.0, 2.0, 6.0)

    def test_after_migration_legacy_weights_are_ignored(self, plugin):
        stored = {"scoring_weights": "", "weight_spread": 0.1, "settings_schema": 2}
        assert plugin._build_weights(_loader_merge(stored)).spread == 3.0

    def test_preset_still_overrides_weights(self, plugin):
        stored = {"curation_preset": "high_coverage", "scoring_weights": "favorite=1"}
        assert plugin._build_weights(_loader_merge(stored)).favorite == 8.0


class TestWeightText:

    def test_forms_accepted(self):
        got, problems = ms.parse_weight_overrides(
            " Favorite = 8 ;spread:1\nweight_importance=0, rank=2.5,")
        assert got == {"favorite": 8.0, "spread": 1.0, "importance": 0.0, "rank": 2.5}
        assert problems == []

    def test_bad_entries_skipped_and_reported(self):
        got, problems = ms.parse_weight_overrides("favorite=lots, stakes=3, rank, rivalry=4")
        assert got == {"rivalry": 4.0}
        assert len(problems) == 3

    def test_bad_entry_logged_not_raised(self, plugin, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            w = plugin._build_weights({"scoring_weights": "favorite=lots"})
        assert w.favorite == 6.0
        assert "favorite=lots" in caplog.text

    def test_cleared_legacy_number_no_longer_crashes_refresh(self, plugin):
        """A NumberInput cleared to "" was saved as "". Before #208,
        float("") raised ValueError straight out of _build_weights and every
        refresh failed. It now falls back to the default."""
        w = plugin._build_weights({"weight_rank": "", "weight_spread": None})
        assert (w.rank, w.spread) == (1.0, 3.0)

    def test_blank_is_every_default(self, plugin):
        w = plugin._build_weights(_loader_merge({}))
        assert (w.rank, w.spread, w.favorite, w.rivalry, w.tournament, w.narrative,
                w.importance) == (1.0, 3.0, 6.0, 2.0, 1.5, 0.0, 3.0)

    def test_format_round_trips(self):
        d = {"spread": 0.1, "rank": 1.0, "narrative": 1.0}
        text = ms.format_weight_overrides(d)
        assert text == "rank=1, spread=0.1, narrative=1"
        assert ms.parse_weight_overrides(text)[0] == d

    def test_format_is_lossless_past_six_digits(self):
        """:g alone keeps 6 significant digits, so a saved 1.244995 migrated
        as 1.245 and moved every close-game score (codex review, #208)."""
        text = ms.format_weight_overrides({"spread": 1.244995, "rank": 0.30000000000000004})
        assert text == "rank=0.30000000000000004, spread=1.244995"
        got = ms.parse_weight_overrides(text)[0]
        assert got == {"spread": 1.244995, "rank": 0.30000000000000004}

    @pytest.mark.parametrize("bad", ["nan", "inf", "-inf", "NaN", "1e999"])
    def test_non_finite_weight_rejected(self, plugin, bad):
        """float() accepts these, and a NaN weight makes every affected game's
        score NaN, which sorts arbitrarily and lands in the cache."""
        got, problems = ms.parse_weight_overrides(f"favorite={bad}")
        assert got == {} and len(problems) == 1
        w = plugin._build_weights({"scoring_weights": f"favorite={bad}"})
        assert w.favorite == 6.0

    def test_non_finite_legacy_weight_rejected(self, plugin):
        w = plugin._build_weights({"weight_favorite": float("nan")})
        assert w.favorite == 6.0


# ---------------------------------------------------------------------------
# The one-time write that makes the modal show what the runtime does.
# ---------------------------------------------------------------------------

class TestMigrateStoredSettings:

    def test_real_install(self):
        out = ms.migrate_stored_settings(PRE_208_STORED)
        added = {k: v for k, v in out.items() if k not in PRE_208_STORED}
        assert added == {
            "ncaaf": "fbs",
            "ncaa_basketball": "men",
            "intl_friendlies": "men",
            "english_cups": "both",
            "ncaa_baseball_softball": "baseball",
            "ncaa_soccer": "men",
            "scoring_weights": "rank=1, spread=0.1, favorite=6, rivalry=2, narrative=1",
            "settings_schema": 2,
        }
        # Nothing stored for tennis, so nothing is written for it.
        assert "tennis" not in out
        # Legacy keys are kept, so a downgrade restores the pre-upgrade setup.
        assert {k: out[k] for k in PRE_208_STORED} == PRE_208_STORED

    def test_idempotent(self):
        once = ms.migrate_stored_settings(PRE_208_STORED)
        assert ms.migrate_stored_settings(once) is None

    def test_nothing_to_migrate(self):
        assert ms.migrate_stored_settings({}) is None
        assert ms.migrate_stored_settings({"max_games": 10, "tennis": "atp"}) is None

    @pytest.mark.parametrize("typed", ["favorite=9,", "spread=4", ""])
    def test_saved_weights_text_kept_verbatim(self, plugin, typed):
        """Scoring weights in the raw dict was saved from a modal that showed
        it blank, not the legacy weights, so what was typed (blank included)
        is what was meant; the legacy weight_spread is not appended back
        (codex review round 2, #208)."""
        out = ms.migrate_stored_settings({"scoring_weights": typed, "weight_spread": 1})
        assert out["scoring_weights"] == typed
        assert plugin._build_weights(out).spread == (4.0 if typed == "spread=4" else 3.0)

    def test_unsaved_weights_text_carries_legacy(self):
        out = ms.migrate_stored_settings({"weight_spread": 1, "weight_rank": 0})
        assert out["scoring_weights"] == "rank=0, spread=1"

    def test_a_saved_select_is_kept_even_at_off(self):
        """A modal holding pre-migration settings saved Tennis = Off over a
        stored enable_atp. The raw dict proves "off" was saved (the UI sends
        only keys it loaded or the user touched), so migration keeps it, and
        from then on ATP is off on both paths (codex review, #208)."""
        out = ms.migrate_stored_settings({"enable_atp": True, "tennis": "off"})
        assert out["tennis"] == "off"
        for view in (out, _loader_merge(out)):
            assert ms.league_enabled(view, "enable_atp") is False

    def test_a_saved_ncaaf_is_kept_even_at_off(self):
        out = ms.migrate_stored_settings({"enable_ncaaf": True, "ncaaf": "off"})
        assert out["ncaaf"] == "off"
        assert ms.ncaaf_divisions(out) is None

    def test_ncaaf_values(self):
        assert ms.migrate_stored_settings({"enable_ncaaf": False, "ncaaf_divisions": "fbs_fcs"})["ncaaf"] == "off"
        assert ms.migrate_stored_settings({"ncaaf_divisions": "fbs_fcs"})["ncaaf"] == "off"
        assert ms.migrate_stored_settings({"enable_ncaaf": True, "ncaaf_divisions": "nonsense"})["ncaaf"] == "fbs"
        assert ms.migrate_stored_settings({"enable_ncaaf": True, "ncaaf_divisions": "fbs_fcs"})["ncaaf"] == "fbs_fcs"

    @pytest.mark.parametrize("a", [None, False, True])
    @pytest.mark.parametrize("b", [None, False, True])
    @pytest.mark.parametrize("select_id", sorted(ms.LEAGUE_PAIRS))
    def test_migration_never_changes_behaviour(self, plugin, select_id, a, b):
        """Every stored combination of a pair (each key absent, False or True),
        read through both production paths before and after migration, enables
        the same leagues."""
        (_, key_a), (_, key_b) = ms.LEAGUE_PAIRS[select_id]
        stored = {"weight_spread": 0.5}
        if a is not None:
            stored[key_a] = a
        if b is not None:
            stored[key_b] = b
        migrated = ms.migrate_stored_settings(stored)
        expect = {key_a: bool(a), key_b: bool(b)}
        for view in (stored, _loader_merge(stored), migrated, _loader_merge(migrated)):
            assert {k: ms.league_enabled(view, k) for k in expect} == expect
            assert plugin._build_weights(view).spread == 0.5


class _FakePluginConfigStore:
    """Just enough of PluginConfig.objects for _migrate_stored_settings:
    filter(key=...).first() and filter(pk=..., settings=...).update(...),
    where the settings filter is an exact JSON match like Postgres jsonb =."""

    def __init__(self, settings, on_read=None):
        self.settings = dict(settings)
        self.on_read = on_read
        self.writes = []

    def filter(self, **kw):
        store = self

        class _QS:
            def first(self_inner):
                row = types.SimpleNamespace(pk=1, settings=dict(store.settings))
                if store.on_read:
                    store.on_read(store)
                return row

            def update(self_inner, settings):
                if "settings" in kw and kw["settings"] != store.settings:
                    return 0
                store.settings = dict(settings)
                store.writes.append(settings)
                return 1
        return _QS()


@pytest.fixture
def fake_plugin_config(monkeypatch):
    def install(store):
        models = types.ModuleType("apps.plugins.models")
        models.PluginConfig = types.SimpleNamespace(objects=store)
        apps_pkg = types.ModuleType("apps")
        plugins_pkg = types.ModuleType("apps.plugins")
        apps_pkg.plugins = plugins_pkg
        plugins_pkg.models = models
        monkeypatch.setitem(sys.modules, "apps", apps_pkg)
        monkeypatch.setitem(sys.modules, "apps.plugins", plugins_pkg)
        monkeypatch.setitem(sys.modules, "apps.plugins.models", models)
        from django.db import transaction
        monkeypatch.setattr(transaction, "atomic", contextlib.nullcontext)
        return store
    return install


class TestPersistedMigration:

    def test_writes_the_migrated_dict(self, plugin, fake_plugin_config):
        store = fake_plugin_config(_FakePluginConfigStore(PRE_208_STORED))
        plugin._migrate_stored_settings()
        assert store.settings["settings_schema"] == 2
        assert store.settings["english_cups"] == "both"
        assert len(store.writes) == 1
        # And a second discovery writes nothing.
        plugin._migrate_stored_settings()
        assert len(store.writes) == 1

    def test_a_save_between_read_and_write_is_not_overwritten(self, plugin, fake_plugin_config):
        users_save = {"enable_atp": True, "tennis": "wta", "max_games": 7}

        def user_saves_the_form(store):
            store.settings = dict(users_save)
        store = fake_plugin_config(_FakePluginConfigStore(PRE_208_STORED, on_read=user_saves_the_form))
        plugin._migrate_stored_settings()
        assert store.settings == users_save
        assert store.writes == []

    def test_failure_is_logged_not_raised(self, plugin, fake_plugin_config, caplog):
        import logging

        class _Broken:
            def filter(self, **kw):
                raise RuntimeError("db down")
        fake_plugin_config(_Broken())
        with caplog.at_level(logging.WARNING):
            plugin._migrate_stored_settings()
        assert "could not migrate saved settings" in caplog.text

    def test_outside_dispatcharr_is_a_no_op(self, plugin, monkeypatch):
        monkeypatch.setitem(sys.modules, "apps.plugins.models", None)
        plugin._migrate_stored_settings()  # ImportError path: must not raise


class TestRawSettingsReadMigrates:
    """get_current_settings (scheduler + reaper) reads the raw dict, so it
    migrates on the way through: one read, at most one CAS write."""

    def test_returns_and_saves_the_migrated_dict(self, plugin, fake_plugin_config):
        store = fake_plugin_config(_FakePluginConfigStore(
            {"enable_atp": True, "tennis": "off"}))
        got = plugin.Plugin.get_current_settings(None)
        assert got["tennis"] == "off" and got["settings_schema"] == 2
        assert store.settings == got
        assert ms.league_enabled(got, "enable_atp") is False

    def test_lost_race_returns_what_the_user_saved(self, plugin, fake_plugin_config):
        """The snapshot read before the user's save is stale the moment the
        CAS misses; handing it to the scheduler would run with settings the
        user just replaced (codex review round 2, #208)."""
        users_save = {"tennis": "wta", "auto_refresh_enabled": False}
        reads = []

        def user_saves_the_form(store):
            if not reads:
                store.settings = dict(users_save)
            reads.append(1)
        store = fake_plugin_config(_FakePluginConfigStore(
            {"enable_atp": True, "auto_refresh_enabled": True},
            on_read=user_saves_the_form))
        got = plugin.Plugin.get_current_settings(None)
        assert got == users_save
        assert store.settings == users_save and store.writes == []
        assert ms.league_enabled(got, "enable_atp") is False

    def test_failed_write_returns_the_stored_dict(self, plugin, fake_plugin_config, caplog):
        import logging

        class _ReadOnly(_FakePluginConfigStore):
            def filter(self, **kw):
                qs = super().filter(**kw)
                if "pk" in kw:
                    def _boom(settings):
                        raise RuntimeError("read-only replica")
                    qs.update = _boom
                return qs
        stored = {"enable_atp": True, "tennis": "off"}
        fake_plugin_config(_ReadOnly(stored))
        with caplog.at_level(logging.WARNING):
            got = plugin.Plugin.get_current_settings(None)
        assert "could not save migrated settings" in caplog.text
        # What is STORED comes back, so the scheduler resolves exactly what
        # run() resolves from the same row (codex review round 2, #208).
        assert got == stored
        assert ms.league_enabled(got, "enable_atp") is ms.league_enabled(
            _loader_merge(stored), "enable_atp")

    def test_no_row_is_empty(self, plugin, fake_plugin_config):
        class _Empty:
            def filter(self, **kw):
                return types.SimpleNamespace(first=lambda: None)
        fake_plugin_config(_Empty())
        assert plugin.Plugin.get_current_settings(None) == {}


class TestInitRunsTheMigration:
    """Discovery rebuilds Plugin and then serves the plugins list, so __init__
    is where the modal's settings get migrated. It must sit ABOVE the
    scheduler's early return (the common path), like _start_reaper."""

    def test_runs_when_the_scheduler_is_already_alive(self, plugin, monkeypatch):
        import threading

        class _Live:
            def is_alive(self):
                return True

        reg, rreg = plugin._scheduler_registry(), plugin._reaper_registry()
        saved = (reg.thread, reg.stop_event, rreg.thread, rreg.stop_event)
        calls = []
        try:
            reg.thread, reg.stop_event = _Live(), threading.Event()
            rreg.thread, rreg.stop_event = _Live(), threading.Event()
            monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append(1))
            plugin.Plugin()
        finally:
            reg.thread, reg.stop_event, rreg.thread, rreg.stop_event = saved
        assert calls == [1]

    def test_not_in_the_pipeline_child(self, plugin, monkeypatch):
        calls = []
        monkeypatch.setenv(plugin.tasks.PIPELINE_CHILD_ENV, "1")
        monkeypatch.setattr(plugin, "_migrate_stored_settings", lambda: calls.append(1))
        plugin.Plugin()
        assert calls == []


# ---------------------------------------------------------------------------
# The manifest and the code agree.
# ---------------------------------------------------------------------------

class TestManifestMatchesTheMergeTable:

    def test_each_pair_select_offers_off_each_league_and_both(self):
        by_id = {f["id"]: f for f in _manifest_fields()}
        for select_id, ((opt_a, _), (opt_b, _)) in ms.LEAGUE_PAIRS.items():
            field = by_id[select_id]
            assert field["type"] == "select"
            assert field["default"] == "off"
            assert [o["value"] for o in field["options"]] == ["off", opt_a, opt_b, "both"]

    def test_no_retired_field_is_still_rendered(self):
        """A retired id left in the manifest would be a second control for the
        same league or weight, and the loader would merge its default into
        context["settings"], which reads as a stored legacy value."""
        ids = {f["id"] for f in _manifest_fields()}
        assert ids & set(ms.LEGACY_KEYS) == set()

