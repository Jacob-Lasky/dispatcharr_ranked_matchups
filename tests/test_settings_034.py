"""#358: the settings UI on Dispatcharr 0.34 (sections, multiselects, tables)
and the action outcomes the host's run history records.

Two upstream facts make the list conversion load-bearing rather than
cosmetic, both read from Dispatcharr's source at v0.34.1:

  - `Field.jsx` renders a multiselect with `(value ?? default ?? []).map(...)`,
    so a saved legacy STRING crashes the settings page;
  - `update_settings` rejects any multiselect value that is not an option
    and any table cell whose type does not match its column, on every save.

So saved strings are migrated to lists, every saved name stays an option,
and the readers accept both shapes. These tests pin each half.
"""
import json
import os
from unittest.mock import Mock

import pytest

from dispatcharr_ranked_matchups import lifecycle
from dispatcharr_ranked_matchups import merged_settings as ms
from tests import test_mapped_prefixes as _mapped

# The module-scoped fixture that builds every source with the network off.
built_sources = _mapped.built_sources

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _manifest():
    with open(os.path.join(REPO_ROOT, "plugin.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _fields():
    return _manifest()["fields"]


def _by_id():
    return {f["id"]: f for f in _fields()}


# What Tower stored on 2026-11-19, before the upgrade (enabled_channel_profiles
# shortened). The shape a real install migrates from.
TOWER_STORED = {
    "settings_schema": 2,
    "enabled_channel_profiles": "Sports, Baseball, MLB, Jellyfin",
    "fallback_only_groups": "Sports | ESPN Unlimited",
    "scoring_weights": "rank=1, spread=0.1, favorite=6, rivalry=2, narrative=1",
    "max_games": 25,
}


class TestNameList:
    def test_text_and_list_read_the_same(self):
        assert ms.name_list("Sports, sports ,NHL,") == ["Sports", "NHL"]
        assert ms.name_list(["Sports", "sports", " NHL ", "", None]) == ["Sports", "NHL"]

    def test_other_values_are_empty(self):
        for raw in (None, "", "  ", 7, {"a": 1}, []):
            assert ms.name_list(raw) == []

    def test_canonical_spelling(self):
        assert ms.name_list("epl, CFB, xyz", {"epl": "EPL", "cfb": "CFB"}) == ["EPL", "CFB", "xyz"]

    def test_record_leagues_reader_takes_the_list(self, plugin):
        assert plugin._record_leagues({"record_leagues": ["CFB", "EPL"]}) == ["cfb", "epl"]
        assert plugin._record_leagues({"record_leagues": "CFB, epl"}) == ["cfb", "epl"]

    def test_group_policy_reader_takes_the_list(self, plugin):
        fallback, excluded = plugin._group_policy({
            "fallback_only_groups": ["Sports | DE Sports"],
            "excluded_groups": "Adult | XXX",
        })
        assert fallback == {"sports | de sports"}
        assert excluded == {"adult | xxx"}


class TestWeightTables:
    TEXT = "rank=1, spread=0.1, Weight_Favorite=6, rivalry: 2; narrative=1"

    def test_rows_score_exactly_as_the_text(self):
        rows = ms.weight_rows(ms.SCORING_WEIGHTS_SETTING, self.TEXT)
        assert rows == [
            {"name": "rank", "weight": 1}, {"name": "spread", "weight": 0.1},
            {"name": "favorite", "weight": 6}, {"name": "rivalry", "weight": 2},
            {"name": "narrative", "weight": 1},
        ]
        assert ms.parse_weight_overrides(rows) == ms.parse_weight_overrides(self.TEXT)

    def test_league_rows_score_exactly_as_the_text(self):
        text = "NFL=1.5, laliga=0.5, NHL=0"
        rows = ms.weight_rows(ms.LEAGUE_WEIGHTS_SETTING, text)
        assert rows == [{"name": "NFL", "weight": 1.5}, {"name": "laliga", "weight": 0.5},
                        {"name": "NHL", "weight": 0}]
        assert ms.parse_league_weights(rows) == ms.parse_league_weights(text)

    def test_entries_the_runtime_ignored_are_dropped_by_the_migration(self):
        # Kept, they would make the table reject every save; the runtime
        # already skipped and reported each of them.
        assert ms.weight_rows(ms.SCORING_WEIGHTS_SETTING, "bogus=2, spread=x, =3, spread=2") == [
            {"name": "spread", "weight": 2}]
        # League weights keep any code and any finite number: the refresh
        # decides those, and reports them.
        assert ms.weight_rows(ms.LEAGUE_WEIGHTS_SETTING, "XYZ=500, NFL=nan") == [
            {"name": "XYZ", "weight": 500}]

    def test_a_list_is_returned_unchanged(self):
        rows = [{"name": "spread", "weight": 2}]
        assert ms.weight_rows(ms.SCORING_WEIGHTS_SETTING, rows) is rows

    def test_an_empty_added_row_is_skipped_and_a_half_row_reported(self):
        overrides, problems = ms.parse_weight_overrides([
            {"name": None, "weight": None},          # "Add row", untouched
            {"name": "spread", "weight": 2},
            {"name": "rank"},                        # no weight yet
            "not a row",
        ])
        assert overrides == {"spread": 2.0}
        assert len(problems) == 2

    def test_an_empty_table_lets_the_legacy_weights_decide(self, plugin):
        # The run() path fills the manifest default, now [] (#208's rule).
        stored = {"weight_spread": 0.5, "scoring_weights": []}
        assert plugin._build_weights(stored).spread == 0.5


class TestMigration:
    def test_tower_settings(self):
        out = ms.migrate_stored_settings(TOWER_STORED, ("CFB", "EPL"))
        assert out["enabled_channel_profiles"] == ["Sports", "Baseball", "MLB", "Jellyfin"]
        assert out["fallback_only_groups"] == ["Sports | ESPN Unlimited"]
        assert out["scoring_weights"][1] == {"name": "spread", "weight": 0.1}
        # Absent keys stay absent: the manifest default fills them in.
        for key in ("record_leagues", "excluded_groups", "league_weights"):
            assert key not in out
        assert out["max_games"] == 25 and out["settings_schema"] == 2

    def test_idempotent_and_none_when_current(self):
        once = ms.migrate_stored_settings(TOWER_STORED, ())
        assert ms.migrate_stored_settings(once, ()) is None
        assert ms.migrate_stored_settings({}, ()) is None

    def test_blank_strings_become_empty_lists(self):
        out = ms.migrate_stored_settings({"excluded_groups": "", "league_weights": "  "})
        assert out == {"excluded_groups": [], "league_weights": []}

    def test_record_leagues_are_canonicalised_onto_the_options(self):
        out = ms.migrate_stored_settings({"record_leagues": "cfb, epl, Gone"}, ("CFB", "EPL"))
        assert out["record_leagues"] == ["CFB", "EPL", "Gone"]

    def test_behaviour_is_unchanged(self, plugin):
        out = ms.migrate_stored_settings(TOWER_STORED, ())
        assert plugin._build_weights(out) == plugin._build_weights(TOWER_STORED)
        assert plugin._group_policy(out) == plugin._group_policy(TOWER_STORED)
        assert (plugin._parse_name_list(out["enabled_channel_profiles"])
                == plugin._parse_name_list(TOWER_STORED["enabled_channel_profiles"]))

    def test_plugin_passes_the_manifest_league_codes(self, plugin):
        codes = plugin._league_codes()
        assert "EPL" in codes and "CFB" in codes
        assert codes == tuple(o["value"] for o in _by_id()["record_leagues"]["options"])


class TestBuildFields:
    DYNAMIC = {
        "enabled_channel_profiles": ["Sports", "Soccer"],
        "fallback_only_groups": ["A", "B"],
        "excluded_groups": ["A", "B"],
    }

    def test_dynamic_options_and_saved_names_kept(self):
        saved = {
            "enabled_channel_profiles": "Sports, Renamed",     # legacy text
            "excluded_groups": ["B", "Gone"],                   # list
            "record_leagues": "epl, Mystery",
        }
        manifest = _fields()
        before = json.dumps(manifest)
        out = {f["id"]: f for f in ms.build_fields(manifest, saved, self.DYNAMIC)}
        assert json.dumps(manifest) == before, "the manifest list must not be mutated"
        assert out["enabled_channel_profiles"]["options"] == [
            {"value": "Sports", "label": "Sports"}, {"value": "Soccer", "label": "Soccer"},
            {"value": "Renamed", "label": "Renamed" + ms.MISSING_OPTION_SUFFIX},
        ]
        assert [o["value"] for o in out["excluded_groups"]["options"]] == ["A", "B", "Gone"]
        assert [o["value"] for o in out["fallback_only_groups"]["options"]] == ["A", "B"]
        leagues = [o["value"] for o in out["record_leagues"]["options"]]
        # "epl" is the EPL option, not a second entry; an unknown code is kept.
        assert leagues.count("EPL") == 1 and "epl" not in leagues
        assert leagues[-1] == "Mystery"

    def test_every_saved_value_passes_the_hosts_save_validation(self):
        # The check update_settings runs (loader._validate_multiselect_value).
        saved = ms.migrate_stored_settings(
            {"enabled_channel_profiles": "Old, Sports", "record_leagues": "nfl"}, ("NFL",))
        for f in ms.build_fields(_fields(), saved, self.DYNAMIC):
            if f["type"] == "multiselect" and f["id"] in saved:
                allowed = {o["value"] for o in f["options"]}
                assert all(str(v) in allowed for v in saved[f["id"]]), f["id"]

    def test_get_fields_never_raises(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_dynamic_field_options",
                            Mock(side_effect=RuntimeError("no db")))
        assert plugin.Plugin.get_fields(None, {"settings": {}}) == []

    def test_get_fields_uses_the_db_names(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_dynamic_field_options", lambda: self.DYNAMIC)
        out = {f["id"]: f for f in plugin.Plugin.get_fields(None, {"settings": {}})}
        assert [o["value"] for o in out["enabled_channel_profiles"]["options"]] == ["Sports", "Soccer"]
        assert [f["id"] for f in out.values()] == [f["id"] for f in _fields()]


class TestManifestLayout:
    OPEN = {"_section_sources", "_section_filter"}

    def test_every_setting_belongs_to_a_section(self):
        loose = [f["id"] for f in _fields() if f["type"] != "section" and not f.get("section")]
        # The client-setup note stays above every section on purpose.
        assert loose == ["_section_client_setup"]

    def test_which_sections_start_collapsed(self):
        sections = [f for f in _fields() if f["type"] == "section"]
        assert len(sections) == 9
        assert {s["id"] for s in sections if not s.get("collapsed")} == self.OPEN

    def test_section_notes_keep_the_old_wording(self):
        # The heading moved to the section marker; its description became the
        # section's first field, unchanged.
        by = _by_id()
        assert by["_section_sources_note"]["description"].startswith("Toggle which sports/leagues")
        assert by["_section_sources_note"]["section"] == "_section_sources"
        assert "_section_naming_note" not in by  # it never had a description

    def test_tables_repeat_their_help_under_the_table(self):
        # PluginTableField renders the label, never help_text.
        by = _by_id()
        for fid in ms.WEIGHT_TABLE_SETTINGS:
            assert by[fid + "_help"]["description"] == by[fid]["help_text"]

    def test_record_leagues_offers_every_league_code(self, built_sources):
        offered = [o["value"] for o in _by_id()["record_leagues"]["options"]]
        assert len(offered) == len(set(offered))
        assert set(offered) == {s.sport_prefix for s in built_sources}

    def test_description_template_rows_and_reap_confirm(self):
        assert _by_id()["description_template"]["rows"] == 6
        reap = next(a for a in _manifest()["actions"] if a["id"] == "reap_now")
        assert reap["confirm"]["required"] is True and reap["confirm"]["message"]


class TestHostOutcome:
    @pytest.mark.parametrize("result,status", [
        ({"status": "ok"}, "ok"),
        ({"status": "ok", "warnings": []}, "ok"),
        ({"status": "ok", "warnings": ["NFL: error"]}, "partial"),
        ({"status": "error", "matching_recovery": {"status": "ok"}}, "partial"),
        ({"status": "error", "matching_recovery": {"status": "error"}}, "error"),
        ({"status": "error"}, "error"),
        ({"status": "partial", "lock_busy": True}, "partial"),
    ])
    def test_mapping(self, result, status):
        assert lifecycle.host_outcome(result)["status"] == status

    def test_the_recorded_result_is_not_rewritten(self):
        # last_run.json keeps "error" for a recovered failure (slot coverage,
        # consecutive_failures): host_outcome copies, never mutates.
        result = {"status": "error", "matching_recovery": {"status": "ok"}}
        lifecycle.host_outcome(result)
        assert result["status"] == "error"

    def test_run_reports_partial_for_a_warned_pipeline(self, plugin, monkeypatch):
        outcome = {"status": "ok", "message": "done", "warnings": ["MLS: error"]}
        monkeypatch.setattr(lifecycle, "_pipeline", lambda *a, **k: (True, outcome))
        assert lifecycle.run(None, "refresh", {}, {})["status"] == "partial"

    def test_refresh_and_auto_pipeline_carry_warnings(self):
        src = open(os.path.join(REPO_ROOT, "plugin.py"), encoding="utf-8").read()
        assert '"warnings": problems,' in src
        assert 'out["warnings"] = r1["warnings"]' in src
