"""Tests for plugin.py helpers that DON'T touch Django.

We import the helpers individually so we can sidestep the package's
__init__.py (which imports Plugin → starts the scheduler thread → imports
Django models). The conftest registers the package without exec-ing
__init__.py."""

import importlib.util
import os
import sys
import types
from datetime import timezone

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = os.path.basename(REPO_ROOT)


def _load_plugin_module():
    """Load `plugin.py` as a submodule of the (already-stub-registered)
    package. Need to stub the Django imports it does at top-level too: but
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


class TestActionRefreshClearsFdCaches:
    """The FD.org batching refactor pins module-level caches into
    `sources/soccer.py` and relies on `_action_refresh` to wipe them at
    the top of every refresh. Without the wipe, a long-running plugin
    instance serves stale fixtures on the second and later refreshes:
    silent staleness, no error.

    This integration test asserts the contract directly: pre-populate
    the caches with sentinel data, invoke `_action_refresh` (with
    `_build_sources` stubbed to return [] so we short-circuit before
    hitting Django / FD.org), then verify both caches were cleared.

    A future refactor that removes the `_clear_fd_caches()` call from
    `_action_refresh` would silently regress to stale-cache behavior
    and pass every unit-level cache test. This test is the safety net.
    """

    def test_action_refresh_clears_tier_fixtures_cache(self, plugin, monkeypatch):
        from dispatcharr_ranked_matchups.sources import soccer

        # Pre-populate both caches with sentinel data.
        sentinel_tier = (("sentinel-key", "2026-01-01", "2026-01-08"), {"PL": [{"id": 999}]})
        soccer._TIER_FIXTURES_CACHE = sentinel_tier
        soccer._SEASON_MATCHES_CACHE["SENTINEL"] = [{"id": 888}]

        # Short-circuit _action_refresh past the cache-clear by stubbing
        # _build_sources to return [] (no sources → early return with
        # status=error).
        monkeypatch.setattr(plugin, "_build_sources", lambda _settings: [])

        result = plugin._action_refresh({"lookahead_days": 7})
        # Confirm we hit the early-return path so the only thing
        # _action_refresh did was clear caches + build empty sources.
        assert result.get("status") == "error"

        # Both caches must be wiped.
        assert soccer._TIER_FIXTURES_CACHE is None, (
            "_action_refresh did not call _clear_fd_caches(): tier "
            "fixtures cache survived the refresh boundary, which would "
            "cause stale FD.org data on every refresh after the first."
        )
        assert "SENTINEL" not in soccer._SEASON_MATCHES_CACHE, (
            "_action_refresh did not call _clear_fd_caches(): season "
            "matches cache survived the refresh boundary."
        )

    def test_clear_fd_caches_runs_before_build_sources(self, plugin, monkeypatch):
        # Pin the order: the cache wipe must precede the source loop,
        # not run after it. If it ran AFTER, the first source's fetch
        # would still hit a stale cache from the prior refresh.
        from dispatcharr_ranked_matchups.sources import soccer

        order: list = []

        def fake_clear():
            order.append("clear")
            soccer._TIER_FIXTURES_CACHE = None
            soccer._SEASON_MATCHES_CACHE.clear()

        def fake_build_sources(_settings):
            order.append("build_sources")
            return []

        monkeypatch.setattr(soccer, "_clear_fd_caches", fake_clear)
        monkeypatch.setattr(plugin, "_build_sources", fake_build_sources)

        plugin._action_refresh({"lookahead_days": 7})
        assert order == ["clear", "build_sources"], (
            f"unexpected order: {order}: cache clear must fire BEFORE "
            "source building so the first source's fetch sees fresh data"
        )


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


class TestBuildSourcesFriendliesGate:
    """_build_sources is where the friendlies favorites-gate is wired: it
    reads friendlies_favorites_only (defaulting ON) and the user's Favorites
    and hands both to InternationalFriendliesSource. Enable ONLY friendlies so
    no API keys are needed and the returned list is just the friendlies
    source(s)."""

    def _friendlies(self, plugin, settings):
        from dispatcharr_ranked_matchups.sources.friendlies import (
            InternationalFriendliesSource,
        )
        return [
            s for s in plugin._build_sources(settings)
            if isinstance(s, InternationalFriendliesSource)
        ]

    def test_default_gates_on_and_passes_favorites(self, plugin):
        # friendlies_favorites_only OMITTED must default to True (the manifest
        # default), and the user's Favorites must reach the source.
        srcs = self._friendlies(plugin, {
            "favorites": "United States, Tottenham",
            "enable_intl_friendlies": True,
        })
        assert len(srcs) == 1
        assert srcs[0].gender == "m"
        assert srcs[0].favorites_only is True
        assert srcs[0].favorites == ["United States", "Tottenham"]

    def test_explicit_opt_out(self, plugin):
        srcs = self._friendlies(plugin, {
            "favorites": "United States",
            "enable_intl_friendlies": True,
            "friendlies_favorites_only": False,
        })
        assert len(srcs) == 1
        assert srcs[0].favorites_only is False

    def test_women_source_also_gated(self, plugin):
        # The gate applies to both genders from the single toggle.
        srcs = self._friendlies(plugin, {
            "favorites": "United States",
            "enable_intl_friendlies_women": True,
        })
        assert len(srcs) == 1
        assert srcs[0].gender == "w"
        assert srcs[0].favorites_only is True
        assert srcs[0].favorites == ["United States"]


class TestIsPostseasonGame:
    """`_is_postseason_game` classifies a game from its source-set
    `extra["stage"]`. The contract is an EXCLUSION set (non-postseason stages);
    any other non-empty stage is postseason, so future bracket stages default
    to postseason without code changes."""

    def _game(self, stage):
        extra = {} if stage is None else {"stage": stage}
        return types.SimpleNamespace(home="A", away="B", extra=extra)

    def test_regular_season_league_has_no_stage(self, plugin):
        # NFL/NBA/NHL/MLB stamp a stage ONLY on playoff games; regular season
        # has no stage key at all.
        assert plugin._is_postseason_game(self._game(None)) is False
        assert plugin._is_postseason_game(types.SimpleNamespace(extra=None)) is False

    def test_explicit_non_postseason_stages(self, plugin):
        for stage in ("REGULAR_SEASON", "GROUP_STAGE", "ALLSTAR", "EVENT"):
            assert plugin._is_postseason_game(self._game(stage)) is False, stage

    def test_world_cup_group_stage_is_not_postseason(self, plugin):
        # The original complaint: WC group phase must stay favorites-gated even
        # in postseason mode. The group LETTER lives in extra["group_stage"],
        # so extra["stage"] is "GROUP_STAGE".
        g = types.SimpleNamespace(
            home="Brazil", away="Serbia",
            extra={"stage": "GROUP_STAGE", "group_stage": "GROUP_A"},
        )
        assert plugin._is_postseason_game(g) is False

    def test_team_sport_playoff_stages_are_postseason(self, plugin):
        # A representative slice across every bracket source's vocabulary.
        for stage in (
            "WC", "DIV", "CONF", "SB",          # NFL playoffs
            "FINALS", "CONF_FINAL", "CUP_FINAL",  # NBA / NHL
            "LDS", "LCS",                        # MLB
            "R64", "R32", "S16", "E8", "F4", "NCG",  # NCAA basketball
            "LAST_32", "LAST_16", "QUARTER_FINALS", "SEMI_FINALS", "FINAL",  # soccer KO
        ):
            assert plugin._is_postseason_game(self._game(stage)) is True, stage

    def test_ncaa_regionals_are_postseason_despite_reg_suffix(self, plugin):
        # BSB_REG / SB_REG are NCAA Regionals (postseason), NOT regular season.
        # A naive "_REG" suffix exclusion would wrongly drop these.
        assert plugin._is_postseason_game(self._game("BSB_REG")) is True
        assert plugin._is_postseason_game(self._game("SB_REG")) is True

    def test_golf_major_is_postseason_event_is_not(self, plugin):
        assert plugin._is_postseason_game(self._game("MAJOR")) is True
        assert plugin._is_postseason_game(self._game("EVENT")) is False

    def test_case_insensitive(self, plugin):
        assert plugin._is_postseason_game(self._game("group_stage")) is False
        assert plugin._is_postseason_game(self._game("final")) is True


class TestFilterFavoritesOnly:
    """`_filter_favorites_only` is the curation gate. It filters games and
    their parallel source list in lockstep and returns a drop count."""

    def _g(self, home, away, stage=None):
        extra = {} if stage is None else {"stage": stage}
        return types.SimpleNamespace(home=home, away=away, extra=extra)

    def _run(self, plugin, games, favorites, mode):
        # Sources are opaque to the filter; tag them so we can assert the
        # parallel association survives.
        sources = [f"src{i}" for i in range(len(games))]
        kept_g, kept_s, dropped = plugin._filter_favorites_only(
            games, sources, favorites, mode,
        )
        # Parallel association must hold: each kept source is the one that
        # originally accompanied its kept game.
        for g, s in zip(kept_g, kept_s):
            assert s == f"src{games.index(g)}"
        return kept_g, dropped

    def test_off_is_noop(self, plugin):
        games = [self._g("USA", "Wales"), self._g("Brazil", "Serbia")]
        kept, dropped = self._run(plugin, games, ["United States"], "off")
        assert dropped == 0 and kept == games

    def test_unrecognized_mode_is_noop(self, plugin):
        games = [self._g("Brazil", "Serbia")]
        kept, dropped = self._run(plugin, games, ["United States"], "bogus")
        assert dropped == 0 and kept == games

    def test_empty_favorites_is_noop_even_when_mode_on(self, plugin):
        # Strict with no favorites would blank the guide; the gate no-ops.
        games = [self._g("Brazil", "Serbia")]
        for mode in ("strict", "postseason"):
            kept, dropped = self._run(plugin, games, [], mode)
            assert dropped == 0 and kept == games, mode

    def test_strict_keeps_only_favorite_games(self, plugin):
        usa = self._g("United States", "Wales", stage="GROUP_STAGE")
        bra = self._g("Brazil", "Serbia", stage="GROUP_STAGE")
        final = self._g("France", "Argentina", stage="FINAL")  # KO, but no fav
        kept, dropped = self._run(plugin, [usa, bra, final], ["United States"], "strict")
        assert kept == [usa]
        assert dropped == 2  # strict drops the non-favorite final too

    def test_postseason_rescues_playoff_non_favorites(self, plugin):
        usa_group = self._g("United States", "Wales", stage="GROUP_STAGE")
        bra_group = self._g("Brazil", "Serbia", stage="GROUP_STAGE")   # dropped
        wc_final = self._g("France", "Argentina", stage="FINAL")        # rescued
        nfl_playoff = self._g("Bills", "Ravens", stage="DIV")          # rescued
        nfl_regular = self._g("Jets", "Texans")                         # dropped
        kept, dropped = self._run(
            plugin,
            [usa_group, bra_group, wc_final, nfl_playoff, nfl_regular],
            ["United States"],
            "postseason",
        )
        assert kept == [usa_group, wc_final, nfl_playoff]
        assert dropped == 2  # bra_group (WC group) + nfl_regular


class TestManifestFavoritesOnlyMatchesCode:
    """plugin.json's favorites_only option values must equal the code
    constants, mirroring test_manifest_stream_priority_matches_code."""

    def test_options_match_constants(self, plugin):
        import json
        with open(os.path.join(REPO_ROOT, "plugin.json")) as f:
            manifest = json.load(f)
        field = next(x for x in manifest["fields"] if x["id"] == "favorites_only")
        values = [o["value"] for o in field["options"]]
        assert values == [
            plugin._FAVORITES_ONLY_OFF,
            plugin._FAVORITES_ONLY_STRICT,
            plugin._FAVORITES_ONLY_POSTSEASON,
        ]
        assert field["default"] == plugin._FAVORITES_ONLY_OFF
        assert field["id"] == plugin._FAVORITES_ONLY_SETTING


class TestEmptyRefreshResult:
    """Shared exit for the no-games-fetched and everything-filtered-out paths.
    Must persist an EMPTY cache (so a stale prior cache stops serving dropped
    games) and return the action's ok-with-message payload."""

    def test_returns_ok_and_persists_empty_cache(self, plugin, monkeypatch):
        captured = {}
        monkeypatch.setattr(plugin, "_write_cache", lambda c: captured.update(c))
        out = plugin._empty_refresh_result("nothing here", ["NFL: 0 games"])
        assert out == {"status": "ok", "message": "nothing here"}
        assert captured["games"] == []
        assert captured["summary"] == ["NFL: 0 games"]
        assert "refreshed_at" in captured


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
        # [0, 7] to [0, 1]: same per-game magnitude (~3 raw for a
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
        # test caught it. This test pins them together: any future
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
        # Stale keys silently dropped: no AttributeError, no surprise
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
        # breakdown (same scale as raw): NOT to `score` (which is 0-10 scale).
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
    def test_park_above_max_target(self, plugin):
        # park must be past the highest target number we'll write this apply.
        max_target = 3767040
        park = plugin._resolve_park_base(max_target)
        assert park > max_target

    def test_empty_slate_uses_base(self, plugin):
        # Empty cache passes the bare virtual_base; result stays sane and above.
        assert plugin._resolve_park_base(9000) > 9000


class TestAssignChannelNumbers:
    """Stable kickoff-time channel numbering (#121). The map must be position-
    independent (the #117 fix) and collision-free."""

    @staticmethod
    def _game(marker_id, start_iso, sport="cfb"):
        # Minimal cache-shaped row: _assign_channel_numbers only reads
        # start_time_utc + whatever _build_marker_key needs (sport_prefix +
        # extra.cfbd_id here gives a deterministic marker).
        return {
            "sport_prefix": sport,
            "start_time_utc": start_iso,
            "extra": {"cfbd_id": marker_id},
        }

    def test_same_game_keeps_number_when_slate_reordered(self, plugin):
        # THE regression: under the old `virtual_base + cache_idx` scheme a
        # game's number changed when the slate re-ranked. Now reordering the
        # input list must not change any game's assigned number.
        games = [
            self._game(1, "2026-06-13T16:00:00Z"),
            self._game(2, "2026-06-13T20:00:00Z"),
            self._game(3, "2026-06-14T18:00:00Z"),
        ]
        a = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        b = plugin._assign_channel_numbers(list(reversed(games)), 1000, timezone.utc)
        assert a == b
        assert len(a) == 3

    def test_all_numbers_unique(self, plugin):
        games = [
            self._game(i, f"2026-06-{13 + (i % 5)}T{12 + (i % 8):02d}:00:00Z")
            for i in range(60)
        ]
        assigned = plugin._assign_channel_numbers(games, 5000, timezone.utc)
        assert len(assigned) == 60
        assert len(set(assigned.values())) == 60

    def test_skips_bad_start_time(self, plugin):
        games = [
            self._game(1, "2026-06-13T16:00:00Z"),
            self._game(2, "not-a-date"),
            self._game(3, None),
        ]
        assigned = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        markers = set(assigned.keys())
        assert plugin._build_marker_key(games[0]) in markers
        assert plugin._build_marker_key(games[1]) not in markers
        assert plugin._build_marker_key(games[2]) not in markers

    def test_collisions_resolved_to_unique_values(self, plugin, monkeypatch):
        # Force every game onto the SAME raw number; the resolver must still
        # hand back all-distinct numbers (nudge path) and stay deterministic.
        monkeypatch.setattr(plugin, "stable_channel_number",
                            lambda base, start, marker, tz: 1000)
        games = [self._game(i, "2026-06-13T16:00:00Z") for i in range(5)]
        assigned = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        assert len(assigned) == 5
        assert len(set(assigned.values())) == 5
        # Integer +1 nudge produces a contiguous block from the collided value.
        assert sorted(assigned.values()) == [1000, 1001, 1002, 1003, 1004]
        # Deterministic across calls.
        again = plugin._assign_channel_numbers(games, 1000, timezone.utc)
        assert assigned == again


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
        # Probed 720p vs name-only "Sport UHD": probed wins.
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
    EPGGridAPIView in apps/epg/api_views.py: it filters on
    epg_data__epg_source__source_type='dummy' to decide which channels need
    on-the-fly placeholder text generation."""

    def test_source_type_is_not_dummy(self, plugin):
        assert plugin.EPG_SOURCE_TYPE != "dummy"

    def test_source_type_is_xmltv(self, plugin):
        # We're producing XMLTV-equivalent program data directly via ORM.
        assert plugin.EPG_SOURCE_TYPE == "xmltv"

    def test_source_is_inactive(self, plugin):
        # is_active=False so the EPG refresh task skips us: we have no URL
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
        # is reflowed across lines, the assertion needs updating:
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
        # already correct by accident: so the other PLUGIN_KEY tests
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
    hoist `from django.db.models import Q` to module level: the test
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
      2. Headline: "A/An {tagline}: {spread_desc}." (each piece optional).
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
        # Leading underscore = markdown italic: the renderer relies on
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
        assert "A title race: toss-up." in out

    def test_headline_with_only_spread_no_tagline(self, plugin):
        # spread alone (no tagline) still produces a headline line
        # because the spread descriptor is part of the headline block.
        out = plugin._build_description(self._g(spread=2.0), "", False)
        assert "toss-up." in out
        assert not out.startswith("A ")  # no article without a tagline

    def test_headline_terminates_with_period(self, plugin):
        out = plugin._build_description(self._g(), "title race", False)
        # The period is intentional: without it, EPG clients sometimes
        # run the headline into the following block.
        assert out.split("\n\n", 1)[0].endswith(".")

    # ---------- block 2b: playoff series state ----------

    def test_series_phase_and_record_render(self, plugin):
        # Game 1 of a tied series: the canonical bug case. The description must
        # say "Game 1 of 7" and "Series tied", NEVER imply elimination.
        g = self._g(
            home="Carolina Hurricanes",
            away="Vegas Golden Knights",
            extra={"series": {
                "title": "Stanley Cup Final", "game_number": 1, "best_of": 7,
                "home_wins": 0, "away_wins": 0, "results": [],
            }},
        )
        out = plugin._build_description(g, "", False)
        assert "Stanley Cup Final, Game 1 of 7." in out
        assert "Series tied 0-0." in out
        assert "elimination" not in out.lower()

    def test_series_recap_line_renders(self, plugin):
        g = self._g(
            home="Carolina Hurricanes",
            away="Vegas Golden Knights",
            extra={"series": {
                "title": "Stanley Cup Final", "game_number": 3, "best_of": 7,
                "home_wins": 1, "away_wins": 1,
                "results": [
                    {"game_number": 1, "home": "Carolina Hurricanes",
                     "away": "Vegas Golden Knights", "home_goals": 3,
                     "away_goals": 2, "ot": False},
                    {"game_number": 2, "home": "Carolina Hurricanes",
                     "away": "Vegas Golden Knights", "home_goals": 1,
                     "away_goals": 2, "ot": True},
                ],
            }},
        )
        out = plugin._build_description(g, "", False)
        assert "Game 3 of 7." in out
        assert "Game 1: Carolina Hurricanes 3, Vegas Golden Knights 2" in out
        assert "(OT)" in out

    def test_no_series_key_renders_no_series_block(self, plugin):
        # League fixtures (no extra["series"]) must be unaffected.
        out = plugin._build_description(self._g(extra={"matchday": 7, "matchdays_total": 38}), "", False)
        assert "of 7." not in out
        assert "Series" not in out

    # ---------- block 2c: group-stage state ----------

    @staticmethod
    def _group_stage_extra():
        return {
            "fd_competition_code": "WC_GS",
            "matchday": 2,
            "group_stage": {
                "tournament": "FIFA World Cup",
                "group": "C",
                "matchday": 2,
                "matchdays_total": 3,
                "standings": [
                    {"position": 1, "name": "Argentina", "played": 1,
                     "points": 3, "goal_difference": 1},
                    {"position": 2, "name": "Mexico", "played": 1,
                     "points": 1, "goal_difference": 0},
                ],
                "results": [
                    {"home": "Argentina", "away": "Saudi Arabia",
                     "home_goals": 2, "away_goals": 1},
                ],
                "advance": "The top 2 teams in each group advance, plus the "
                           "8 best third-placed teams across all groups.",
            },
        }

    def test_group_stage_round_standings_results_advance_render(self, plugin):
        # The "shock opening loss" fix: a group game's description must carry
        # the real round, table, results, and advance rule, NEVER invent them.
        g = self._g(home="Argentina", away="Mexico",
                    extra=self._group_stage_extra())
        out = plugin._build_description(g, "", False)
        assert "FIFA World Cup Group C, Matchday 2 of 3." in out
        assert "#1 Argentina - 3 pts, 1 played, +1 GD" in out
        assert "Argentina 2-1 Saudi Arabia" in out
        assert "The top 2 teams in each group advance" in out

    def test_group_stage_does_not_double_render_matchday(self, plugin):
        # group_phase owns the matchday line; block 3's generic matchday
        # (WC_GS league ctx supplies matchdays_total=3) must not repeat it.
        g = self._g(home="Argentina", away="Mexico",
                    extra=self._group_stage_extra())
        out = plugin._build_description(g, "", False)
        assert out.count("Matchday 2 of 3") == 1

    def test_no_group_stage_key_renders_no_group_block(self, plugin):
        out = plugin._build_description(
            self._g(extra={"matchday": 7, "matchdays_total": 38}), "", False)
        assert "Group" not in out
        assert "advance" not in out.lower()

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
        # Use .find() to pin order: earlier marker must have lower index.
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
        # channel name carries: the EPG title should read like a real
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
        # Empty kickoff_local string falls back to just the matchup:
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
        # ProgramData.title is varchar(255): anything longer gets
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


class TestBuildSourcesBoxingGate:
    """_build_sources wires the boxing source: gated on enable_boxing AND a
    resolved Boxing Data API key (ESPN has no boxing feed, so unlike the ESPN
    field events boxing REQUIRES a key). Enabled-without-key must be a no-op,
    not a crash, mirroring the CFBD/FD-keyed sources."""

    def _boxing(self, plugin, settings):
        from dispatcharr_ranked_matchups.sources.boxing import BoxingSource
        return [s for s in plugin._build_sources(settings) if isinstance(s, BoxingSource)]

    def test_enabled_with_key_adds_source(self, plugin):
        srcs = self._boxing(plugin, {
            "enable_boxing": True,
            "boxing_data_api_key": "test-key",
        })
        assert len(srcs) == 1
        assert srcs[0].api_key == "test-key"

    def test_enabled_without_key_is_noop(self, plugin):
        # No key -> no source (and a warning), never a crash.
        assert self._boxing(plugin, {"enable_boxing": True}) == []

    def test_disabled_adds_nothing(self, plugin):
        assert self._boxing(plugin, {
            "enable_boxing": False,
            "boxing_data_api_key": "test-key",
        }) == []


class TestEpgMatchWindow:
    """#4: per-sport match-window override. Soccer uses (5, 2.5) to avoid
    false-positives on pre-game preview programs; NCAAF / NFL keep
    (30, 4) for long pre-game shows + OT."""

    def test_default_window_for_unknown_sport(self, plugin):
        pre, post = plugin._epg_match_window("UNKNOWN")
        assert pre == plugin.EPG_PRE_MIN
        assert post == plugin.EPG_POST_HOURS

    def test_default_window_for_none(self, plugin):
        pre, post = plugin._epg_match_window(None)
        assert pre == plugin.EPG_PRE_MIN
        assert post == plugin.EPG_POST_HOURS

    def test_soccer_leagues_tight_window(self, plugin):
        for prefix in ("EPL", "EFL", "UCL", "BL1", "LaLiga", "SerieA",
                       "Ligue1", "WC", "EURO", "MLS", "NWSL", "LigaMX"):
            pre, post = plugin._epg_match_window(prefix)
            assert pre == 5, f"{prefix} should use tight pre-window of 5 min"
            assert post == 2.5, f"{prefix} should use 2.5h post-window"

    def test_ncaaf_keeps_default(self, plugin):
        # NCAAF needs the long pre-show + OT window.
        pre, post = plugin._epg_match_window("CFB")
        assert pre == 30
        assert post == 4

    def test_boxing_wide_window(self, plugin):
        # Boxing (BOX) uses a WIDE window: the Boxing Data API's start times are
        # unreliable (date-only T00:00:00 placeholders + naive datetimes), so a
        # +/- ~1 day window absorbs the offset; the event-name keyword filter,
        # not the clock, is the discriminator for a rare, name-unique card.
        pre, post = plugin._epg_match_window("BOX")
        assert pre == 12 * 60
        assert post == 24.0


class TestCurationPresets:
    def test_manual_preset_uses_individual_weights(self, plugin):
        settings = {
            "curation_preset": "manual",
            "weight_rank": 99.0, "weight_favorite": 99.0,
        }
        w = plugin._build_weights(settings)
        assert w.rank == 99.0
        assert w.favorite == 99.0

    def test_manual_preset_default_when_setting_blank(self, plugin):
        # No curation_preset key at all → manual path.
        w = plugin._build_weights({"weight_rank": 99.0})
        assert w.rank == 99.0

    def test_high_curation_preset_overrides_individuals(self, plugin):
        # Even with manual weights set, preset values win.
        w = plugin._build_weights({
            "curation_preset": "high_curation",
            "weight_rank": 99.0, "weight_favorite": 99.0,
        })
        assert w.rank == 1.5
        assert w.favorite == 4.0

    def test_balanced_preset_matches_default_weights(self, plugin):
        # Balanced preset == scoring.Weights() defaults: DRY check that
        # prevents the preset drifting silently as defaults change.
        from dispatcharr_ranked_matchups.scoring import Weights
        d = Weights()
        w = plugin._build_weights({"curation_preset": "balanced"})
        assert w.rank == d.rank
        assert w.spread == d.spread
        assert w.favorite == d.favorite
        assert w.rivalry == d.rivalry
        assert w.tournament == d.tournament
        assert w.narrative == d.narrative
        assert w.importance == d.importance

    def test_high_coverage_preset_lower_rank_higher_favorite(self, plugin):
        w = plugin._build_weights({"curation_preset": "high_coverage"})
        assert w.rank == 0.7
        assert w.favorite == 8.0

    def test_unknown_preset_falls_back_to_manual(self, plugin):
        # Defensive: unrecognized preset name shouldn't crash, just use individuals.
        w = plugin._build_weights({
            "curation_preset": "best_in_show_only",
            "weight_rank": 42.0,
        })
        assert w.rank == 42.0

    def test_case_insensitive_preset_name(self, plugin):
        w = plugin._build_weights({"curation_preset": "HIGH_CURATION"})
        assert w.rank == 1.5

    # ---------- max_games resolution ----------

    def test_resolve_max_games_manual_reads_setting(self, plugin):
        assert plugin._resolve_max_games({
            "curation_preset": "manual", "max_games": 7,
        }) == 7

    def test_resolve_max_games_preset_wins(self, plugin):
        # User set max_games=999 but picked high_curation: preset wins.
        assert plugin._resolve_max_games({
            "curation_preset": "high_curation", "max_games": 999,
        }) == 10
        assert plugin._resolve_max_games({"curation_preset": "balanced"}) == 25
        assert plugin._resolve_max_games({"curation_preset": "high_coverage"}) == 50

    def test_resolve_max_games_default(self, plugin):
        # No setting at all → default 25.
        assert plugin._resolve_max_games({}) == 25


class TestScoringDocMatchesCode:
    """SCORING.md restates code constants (preset bundles, default signal
    weights) for the human reader. Those tables are a documentation source
    of truth that drifts the instant a number changes in code and nobody
    edits the doc, which is exactly the failure mode plugin.py's
    _CURATION_PRESETS comment ("DRY check pinned by tests") warns about.
    These tests parse the doc and pin it to the code, so a weight or preset
    change that forgets the doc fails CI instead of silently lying to users
    who are following the tuning recipes.
    """

    def _scoring_md_rows(self, plugin):
        import os
        path = os.path.join(
            os.path.dirname(os.path.abspath(plugin.__file__)), "SCORING.md"
        )
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("|"):
                    # Split markdown row into trimmed cells, dropping the
                    # empty leading/trailing cells the outer pipes produce.
                    yield [c.strip() for c in line.strip("|").split("|")]

    def _first_float(self, cell):
        import re
        m = re.search(r"-?\d+(?:\.\d+)?", cell)
        return float(m.group()) if m else None

    def test_preset_table_matches_curation_presets(self, plugin):
        # The "What the presets actually set" table. Columns:
        # name | rank | close | favorite | rivalry | tournament | importance | max_games
        # Doc column "close" maps to the preset key "spread".
        doc_cols = ["rank", "spread", "favorite", "rivalry",
                    "tournament", "importance", "max_games"]
        presets = plugin._CURATION_PRESETS
        seen = set()
        for cells in self._scoring_md_rows(plugin):
            if len(cells) < 8:
                continue
            name_cell = cells[0]
            for key in presets:
                if key in name_cell and key not in seen:
                    nums = [self._first_float(c) for c in cells[1:8]]
                    if any(n is None for n in nums):
                        continue
                    documented = dict(zip(doc_cols, nums))
                    expected = presets[key]
                    for col in doc_cols:
                        assert documented[col] == float(expected[col]), (
                            f"SCORING.md preset table '{key}' {col}="
                            f"{documented[col]} but _CURATION_PRESETS has "
                            f"{expected[col]}. Update SCORING.md."
                        )
                    seen.add(key)
        assert seen == set(presets), (
            f"SCORING.md preset table is missing rows for "
            f"{set(presets) - seen}. Every preset must be documented."
        )

    def test_signal_default_weight_table_matches_dataclass(self, plugin):
        # The Stage 2 "per-signal scoring" table lists a "Default weight"
        # per signal. Those must equal scoring.Weights() defaults.
        from dispatcharr_ranked_matchups.scoring import Weights
        d = Weights()
        # doc signal token (in backticks) → Weights attribute
        signal_to_attr = {
            "rank_pair": "rank",
            "close_game": "spread",
            "favorite": "favorite",
            "tournament_stage": "tournament",
            "rivalry": "rivalry",
            "importance": "importance",
            "narrative": "narrative",
        }
        seen = set()
        for cells in self._scoring_md_rows(plugin):
            # Stage 2 table rows are: signal | default | range  → 3 cells.
            if len(cells) != 3:
                continue
            token = cells[0].strip("`").split()[0].strip("`")
            attr = signal_to_attr.get(token)
            if attr is None or token in seen:
                continue
            documented = self._first_float(cells[1])
            assert documented is not None, (
                f"SCORING.md signal table '{token}' has no numeric default."
            )
            assert documented == float(getattr(d, attr)), (
                f"SCORING.md signal table '{token}' default={documented} but "
                f"Weights().{attr}={getattr(d, attr)}. Update SCORING.md."
            )
            seen.add(token)
        assert seen == set(signal_to_attr), (
            f"SCORING.md signal table is missing rows for "
            f"{set(signal_to_attr) - seen}."
        )


class TestIsCatchupMatchday:
    def _table(self, max_played):
        # Synthetic table where most teams played `max_played` and a couple
        # played slightly less (normal weekly drift).
        return [
            {"name": "A", "position": 1, "points": 70, "played": max_played},
            {"name": "B", "position": 2, "points": 69, "played": max_played},
            {"name": "C", "position": 3, "points": 65, "played": max_played - 1},
        ]

    def test_no_matchday_returns_false(self, plugin):
        g = {"extra": {"standings_table": self._table(46)}}
        assert plugin._is_catchup_matchday(g) is False

    def test_no_standings_returns_false(self, plugin):
        # Knockout fixtures (UCL etc) have matchday but no standings table.
        g = {"extra": {"matchday": 5}}
        assert plugin._is_catchup_matchday(g) is False

    def test_current_matchday_is_not_catchup(self, plugin):
        # League at MD46, fixture at MD46: normal final round.
        g = {"extra": {"matchday": 46, "standings_table": self._table(46)}}
        assert plugin._is_catchup_matchday(g) is False

    def test_one_behind_is_not_catchup(self, plugin):
        # League at MD46, fixture at MD45: normal weekly lag (midweek game).
        g = {"extra": {"matchday": 45, "standings_table": self._table(46)}}
        assert plugin._is_catchup_matchday(g) is False

    def test_two_behind_is_catchup(self, plugin):
        # League at MD46, fixture at MD44: postponed by 2 weeks.
        g = {"extra": {"matchday": 44, "standings_table": self._table(46)}}
        assert plugin._is_catchup_matchday(g) is True

    def test_southampton_ipswich_real_case(self, plugin):
        # The case from the issue: Southampton vs Ipswich MD40 while most
        # of the league is at MD46.
        g = {"extra": {"matchday": 40, "standings_table": self._table(46)}}
        assert plugin._is_catchup_matchday(g) is True

    def test_missing_played_field_returns_false(self, plugin):
        # Older caches predating #10 might not have 'played' on every row.
        table = [
            {"name": "A", "position": 1, "points": 70},
            {"name": "B", "position": 2, "points": 69},
        ]
        g = {"extra": {"matchday": 40, "standings_table": table}}
        assert plugin._is_catchup_matchday(g) is False

    def test_partial_played_uses_what_we_have(self, plugin):
        # Some entries have 'played', some don't: use the ones that do.
        table = [
            {"name": "A", "position": 1, "points": 70, "played": 46},
            {"name": "B", "position": 2, "points": 69},  # missing
            {"name": "C", "position": 3, "points": 65, "played": 46},
        ]
        g = {"extra": {"matchday": 40, "standings_table": table}}
        assert plugin._is_catchup_matchday(g) is True

    def test_integration_description_uses_catchup_label(self, plugin):
        g = {
            "home": "Southampton FC", "away": "Ipswich Town FC",
            "sport_prefix": "EPL",
            "closeness": None, "spread": None,
            "extra": {
                "matchday": 40, "matchdays_total": 46,
                "fd_competition_code": "PL",
                "standings_table": [
                    {"name": "X", "position": 1, "points": 90, "played": 46},
                    {"name": "Y", "position": 2, "points": 85, "played": 46},
                ],
                "impact_narratives": [],
            },
            "favorites_matched": [],
        }
        desc = plugin._build_description(g, tagline="", placeholder=False)
        assert "Catch-up matchday 40 of 46" in desc
        assert "Matchday 40 of 46" not in desc.replace("Catch-up matchday 40 of 46", "")

    def test_integration_subtitle_uses_catchup_prefix(self, plugin):
        g = {
            "home": "Southampton FC", "away": "Ipswich Town FC",
            "extra": {
                "matchday": 40, "matchdays_total": 46,
                "standings_table": [
                    {"name": "X", "position": 1, "points": 90, "played": 46},
                ],
            },
        }
        sub = plugin._build_subtitle(g, tagline="")
        assert "catch-up matchday 40/46" in sub


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
        # Minimal EPL-shaped table: 3 teams enough to exercise the path.
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
        assert line == "Manchester City FC 2nd, 70 pts. Manchester United FC 3rd, 69 pts: 1 pt behind."

    def test_both_teams_wide_gap(self, plugin):
        g = {
            "home": "Manchester City FC",
            "away": "Bournemouth FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Manchester City FC 2nd, 70 pts. Bournemouth FC 14th, 41 pts: 29 pts behind."

    def test_away_team_ahead(self, plugin):
        # When home team is lower-ranked, away team's gap reads "ahead".
        g = {
            "home": "Bournemouth FC",
            "away": "Manchester City FC",
            "extra": {"standings_table": self._table()},
        }
        line = plugin._build_standings_posture_line(g)
        assert line == "Bournemouth FC 14th, 41 pts. Manchester City FC 2nd, 70 pts: 29 pts ahead."

    def test_tied_on_points_no_gd_cached(self, plugin):
        # Older caches (pre-#10) won't have goal_difference; fall back to
        # the bare framing.
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35},
            {"name": "B FC", "position": 2, "points": 70, "played": 35},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts: level on points."

    def test_tied_on_points_away_gd_better(self, plugin):
        # B has the better GD: reads "... GD ahead" for the away team.
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 15},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 22},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts: level on points, 7 GD ahead."

    def test_tied_on_points_home_gd_better(self, plugin):
        # A (home) has better GD: away reads "behind on GD".
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 22},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 15},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts: level on points, 7 GD behind."

    def test_tied_on_everything(self, plugin):
        table = [
            {"name": "A FC", "position": 1, "points": 70, "played": 35, "goal_difference": 22},
            {"name": "B FC", "position": 2, "points": 70, "played": 35, "goal_difference": 22},
        ]
        g = {"home": "A FC", "away": "B FC", "extra": {"standings_table": table}}
        line = plugin._build_standings_posture_line(g)
        assert line == "A FC 1st, 70 pts. B FC 2nd, 70 pts: level on points and goal difference."

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


class _StubThread:
    """Controllable stand-in for a scheduler thread. join() simulates the loop
    exiting once its stop_event is set, exactly as the real loop does."""

    def __init__(self, stop_event):
        self._alive = True
        self.join_calls = 0
        self._stop_event = stop_event

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        self.join_calls += 1
        if self._stop_event is not None and self._stop_event.is_set():
            self._alive = False


class TestPluginStopTeardown:
    """The Plugin class spawns a daemon scheduler thread in __init__. The thread
    reads settings from Postgres each tick, so a leaked or churned thread leaks a
    Postgres connection until max_connections is hit and the API 500s with "too
    many clients" (#82).

    The scheduler (thread + its stop Event) lives in a reload-stable registry
    (#110) so a later disable can reach it even after the module that started it
    is unloaded. __init__ is IDEMPOTENT (#82/#136): the loader re-instantiates
    Plugin on EVERY discovery, so if a healthy thread is already in the registry
    it is LEFT running (no restart, no churn, no per-discovery connection leak);
    only stop() (disable/delete) tears it down. These tests drive that lifecycle.
    """

    @pytest.fixture(autouse=True)
    def _isolate_registry(self, plugin):
        # Snapshot + restore the reload-stable registry around each test so the
        # scheduler tests don't leak state into one another.
        import threading
        reg = plugin._scheduler_registry()
        saved_thread, saved_event = reg.thread, reg.stop_event
        reg.thread, reg.stop_event = None, None
        yield reg
        # Make sure no real thread we started lingers.
        if reg.stop_event is not None:
            reg.stop_event.set()
        reg.thread, reg.stop_event = saved_thread, saved_event

    def test_stop_signals_and_clears_registry(self, plugin, _isolate_registry):
        import threading
        reg = _isolate_registry
        ev = threading.Event()
        stub = _StubThread(ev)
        reg.thread, reg.stop_event = stub, ev

        inst = plugin.Plugin.__new__(plugin.Plugin)  # skip __init__'s thread spawn
        inst.stop()

        assert ev.is_set()
        assert stub.join_calls == 1
        assert not stub.is_alive()
        assert reg.thread is None and reg.stop_event is None

    def test_stop_is_safe_when_no_thread(self, plugin, _isolate_registry):
        # Defensive: stop() called with an empty registry (or twice) must not
        # raise (loader unload can race a disable signal).
        inst = plugin.Plugin.__new__(plugin.Plugin)
        inst.stop()
        inst.stop()
        assert _isolate_registry.thread is None

    def test_stop_accepts_context_argument(self, plugin, _isolate_registry):
        # Loader contract is stop(self, context); it passes a dict on lifecycle
        # stops. The signature must accept it (and None, and nothing).
        inst = plugin.Plugin.__new__(plugin.Plugin)
        inst.stop()
        inst.stop({"settings": {}})
        inst.stop(None)

    def test_init_spawns_thread_into_registry(self, plugin, monkeypatch, _isolate_registry):
        import threading
        spawned = []

        class TrackingThread(threading.Thread):
            def __init__(self, *a, **kw):
                spawned.append(kw.get("name") or "<unnamed>")
                super().__init__(*a, **kw)
            def start(self):  # don't run a live thread in tests
                pass

        monkeypatch.setattr(plugin.threading, "Thread", TrackingThread)
        plugin.Plugin()
        assert spawned == ["ranked_matchups-scheduler"]
        assert _isolate_registry.thread is not None
        assert _isolate_registry.stop_event is not None

    def test_reload_init_keeps_live_thread_idempotent(self, plugin, monkeypatch, _isolate_registry):
        # #82/#136: the loader re-instantiates Plugin on EVERY discovery
        # (plugins-list view, run, settings-save, reload). A healthy scheduler
        # from a prior incarnation is reachable via the reload-stable registry
        # and reads settings live from the DB, so __init__ must LEAVE it running:
        # no signal, no replacement, no new spawn. Restarting per discovery is
        # what churned a thread and leaked a DB connection each time (the #82
        # lock-up).
        import threading
        reg = _isolate_registry
        live_event = threading.Event()
        live = _StubThread(live_event)  # is_alive() -> True
        reg.thread, reg.stop_event = live, live_event

        spawned = []

        class TrackingThread(threading.Thread):
            def __init__(self, *a, **kw):
                spawned.append(kw.get("name"))
                super().__init__(*a, **kw)
            def start(self):  # don't run a live thread in tests
                pass

        monkeypatch.setattr(plugin.threading, "Thread", TrackingThread)
        plugin.Plugin()  # a later discovery's incarnation

        # The live thread is untouched: not signaled, not joined, not replaced,
        # and no new thread spawned.
        assert not live_event.is_set()
        assert live.join_calls == 0
        assert reg.thread is live
        assert reg.stop_event is live_event
        assert spawned == []

    def test_reload_init_replaces_dead_thread(self, plugin, monkeypatch, _isolate_registry):
        # If the registry's thread has died, __init__ starts a fresh one (the
        # idempotency check is is_alive(), not mere presence).
        import threading
        reg = _isolate_registry
        dead_event = threading.Event()
        dead = _StubThread(dead_event)
        dead._alive = False  # a thread that has exited
        reg.thread, reg.stop_event = dead, dead_event

        class TrackingThread(threading.Thread):
            def start(self):  # don't run a live thread in tests
                pass

        monkeypatch.setattr(plugin.threading, "Thread", TrackingThread)
        plugin.Plugin()

        assert reg.thread is not dead
        assert isinstance(reg.thread, TrackingThread)
        assert reg.stop_event is not dead_event

    def test_stop_still_joins_on_teardown(self, plugin, _isolate_registry):
        # The disable/delete path (stop()) is not latency-sensitive and SHOULD
        # block-join to confirm the thread exited.
        import threading
        reg = _isolate_registry
        ev = threading.Event()
        stub = _StubThread(ev)
        reg.thread, reg.stop_event = stub, ev
        plugin.Plugin.__new__(plugin.Plugin).stop()
        assert ev.is_set()
        assert stub.join_calls == 1
        assert not stub.is_alive()


class TestDedupSeriesGames:
    """Best-of-N playoff series sources (NHL / NBA / MLB / NCAA tournaments)
    return every scheduled series game from fetch_upcoming. Without
    dedup, a Carolina vs Montreal best-of-7 becomes 4-7 redundant
    virtual channels: same matchup, different dates. The user-facing
    fix Jake hit live on 2026-05-26: keep ONLY the chronologically
    earliest game per (sport_prefix, team-pair) key; let the next
    refresh after the game finishes promote the subsequent series
    game.

    The dedup MUST key on `frozenset({home, away})` not `(home, away)`
   : NHL playoff games 2 and 4 swap home-ice (the lower-seed gets
    "Carolina at Montreal" alternating with "Montreal at Carolina"),
    and a strict ordered key would treat them as distinct.
    """

    @staticmethod
    def _game(sport, home, away, start_dt):
        """Minimal GameRow-shaped stub: attribute access on sport_prefix,
        home, away, start_time is all the dedup helper touches."""
        import types
        return types.SimpleNamespace(
            sport_prefix=sport, home=home, away=away, start_time=start_dt,
        )

    def test_empty_input_returns_empty(self, plugin):
        g, s, n = plugin._dedup_series_games([], [])
        assert g == []
        assert s == []
        assert n == 0

    def test_single_game_per_pair_passes_through(self, plugin):
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Vegas", "Colorado", datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),
            self._game("NHL", "Carolina", "Montreal", datetime(2026, 5, 28, 0, 0, tzinfo=timezone.utc)),
        ]
        sources = ["src_nhl_1", "src_nhl_2"]
        out_g, out_s, n = plugin._dedup_series_games(games, sources)
        assert n == 0
        assert len(out_g) == 2
        assert out_s == sources

    def test_drops_later_games_in_same_series(self, plugin):
        # 4-game best-of-7 schedule: keep only the earliest (game 1).
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Vegas", "Colorado",  datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),   # G1
            self._game("NHL", "Colorado", "Vegas",  datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc)),   # G2 (swap home)
            self._game("NHL", "Vegas", "Colorado",  datetime(2026, 5, 31, 0, 0, tzinfo=timezone.utc)),   # G3
            self._game("NHL", "Colorado", "Vegas",  datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc)),    # G4
        ]
        sources = ["s1", "s2", "s3", "s4"]
        out_g, out_s, n = plugin._dedup_series_games(games, sources)
        assert n == 3
        assert len(out_g) == 1
        # The earliest survives.
        assert out_g[0].start_time.day == 27
        # The corresponding source survives too: parallel index preserved.
        assert out_s == ["s1"]

    def test_home_ice_swap_treated_as_same_series(self, plugin):
        # Carolina at Montreal AND Montreal at Carolina = same series.
        # Strict ordered key would treat them distinct; frozenset key
        # collapses them. This is THE key invariant Jake's bug ran into.
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Carolina", "Montreal", datetime(2026, 5, 30, 0, 0, tzinfo=timezone.utc)),
            self._game("NHL", "Montreal", "Carolina", datetime(2026, 5, 28, 0, 0, tzinfo=timezone.utc)),
        ]
        out_g, out_s, n = plugin._dedup_series_games(games, ["a", "b"])
        assert n == 1
        assert len(out_g) == 1
        # The May 28 game (with Montreal at Carolina) survives: earlier.
        assert out_g[0].home == "Montreal"
        assert out_g[0].away == "Carolina"

    def test_different_sport_prefixes_not_merged(self, plugin):
        # Hypothetical: same team-name string used by two leagues
        # (extremely unlikely but invariant should hold). NHL Edmonton
        # Oilers and a fictitious other-league "Edmonton Oilers" stay
        # separate because sport_prefix differs.
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Edmonton", "Vancouver",
                       datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),
            self._game("AHL", "Edmonton", "Vancouver",
                       datetime(2026, 5, 27, 2, 0, tzinfo=timezone.utc)),
        ]
        out_g, out_s, n = plugin._dedup_series_games(games, ["nhl", "ahl"])
        assert n == 0
        assert len(out_g) == 2

    def test_keeps_earliest_when_input_order_is_reversed(self, plugin):
        # Defensive: the dedup must not depend on input order. Even if
        # game 4 appears before game 1 in the list, game 1 must win.
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Vegas", "Colorado", datetime(2026, 6, 2,  0, 0, tzinfo=timezone.utc)),  # G4 first
            self._game("NHL", "Vegas", "Colorado", datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),  # G1 second
            self._game("NHL", "Vegas", "Colorado", datetime(2026, 5, 31, 0, 0, tzinfo=timezone.utc)),  # G3 third
        ]
        out_g, out_s, n = plugin._dedup_series_games(games, ["g4", "g1", "g3"])
        assert n == 2
        assert len(out_g) == 1
        assert out_g[0].start_time.day == 27
        assert out_s == ["g1"]

    def test_preserves_source_alignment_with_mixed_input(self, plugin):
        # When some games are deduped and others pass through, the
        # surviving games' indices into the sources list MUST stay
        # aligned. A broken alignment would mean compute_match_
        # importance ran against the wrong source for some games.
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Vegas", "Colorado",  datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),   # series A, G1
            self._game("CL",  "Real Madrid", "PSG", datetime(2026, 5, 28, 19, 0, tzinfo=timezone.utc)),   # CL pass-through
            self._game("NHL", "Colorado", "Vegas",  datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc)),   # series A, G2 (drop)
            self._game("NHL", "Carolina", "Montreal", datetime(2026, 5, 28, 0, 0, tzinfo=timezone.utc)), # series B, G1
        ]
        sources = ["src_nhl_a", "src_ucl", "src_nhl_b", "src_nhl_c"]
        out_g, out_s, n = plugin._dedup_series_games(games, sources)
        assert n == 1
        # Keep G1 of each series + the CL pass-through.
        assert len(out_g) == 3
        # Sources line up with their games (UCL with UCL game, etc.).
        for g, s in zip(out_g, out_s):
            if g.sport_prefix == "CL":
                assert s == "src_ucl"
            elif g.home == "Vegas" or g.away == "Vegas":
                assert s == "src_nhl_a"
            elif g.home == "Carolina" or g.away == "Carolina":
                assert s == "src_nhl_c"
            else:
                raise AssertionError(f"unexpected game in output: {g.home} vs {g.away}")

    def test_handles_missing_start_time_gracefully(self, plugin):
        # If a source omits start_time (None), the earlier-wins
        # comparison falls back to "keep the existing one" so we don't
        # crash on a None < datetime comparison.
        from datetime import datetime, timezone
        games = [
            self._game("NHL", "Vegas", "Colorado",
                       datetime(2026, 5, 27, 1, 0, tzinfo=timezone.utc)),
            self._game("NHL", "Vegas", "Colorado", None),
        ]
        out_g, out_s, n = plugin._dedup_series_games(games, ["a", "b"])
        assert n == 1
        assert len(out_g) == 1
        # The one with a real start_time wins.
        assert out_g[0].start_time is not None


class TestActionPreviewNames:
    """The 'Test naming convention' action (#100) renders sample games against
    the configured template so a user can eyeball the layout before applying.
    Django-free: it only touches settings, naming.py, and the tz resolver."""

    def test_default_template_previews_ok(self, plugin):
        r = plugin._action_preview_names({"local_timezone": "UTC"})
        assert r["status"] == "ok"
        assert "DEFAULT template" in r["message"]
        assert "Alabama (15) at St. John's" in r["message"]

    def test_custom_template_is_used(self, plugin):
        r = plugin._action_preview_names({"name_template": "{away_team} at {home_team}"})
        assert r["status"] == "ok"
        assert "your template" in r["message"]
        assert "Alabama at St. John's" in r["message"]

    def test_invalid_template_errors_and_falls_back_to_default(self, plugin):
        r = plugin._action_preview_names({"name_template": "{bogus} {away_team}"})
        assert r["status"] == "error"
        assert "bogus" in r["message"]
        # the DEFAULT is still previewed so the user sees working output
        assert "Alabama (15) at St. John's" in r["message"]


class TestStreamLanguageRank:
    """#111: language-preference bucket for widen-pool ordering. Cases are
    drawn from real `!Top Matchups` stream names observed on the live box."""

    def test_both_english_team_names_is_english(self, plugin):
        n = "FIFA World Cup 2026 02: [4K] Mexico 20:00 South Africa"
        assert plugin._stream_language_rank(n, "Mexico", "South Africa") == plugin._LANG_RANK_ENGLISH

    def test_accented_spelling_is_non_english(self, plugin):
        # "Turquía" carries an accent and the English "Turkey" is absent.
        n = "US (Peacock 038) |  Australia v. Turquía (2026-06-13 23:00:00)"
        assert plugin._stream_language_rank(n, "Australia", "Turkey") == plugin._LANG_RANK_NON_ENGLISH

    def test_spanish_country_without_accent_is_non_english(self, plugin):
        # No accent, but "Estados Unidos" is a curated Spanish-country hint.
        n = "US (Peacock 022) |  Estados Unidos v. Paraguay (2026-06-12 20:30:00)"
        assert plugin._stream_language_rank(n, "United States", "Paraguay") == plugin._LANG_RANK_NON_ENGLISH

    def test_accent_wins_over_partial_english_token(self, plugin):
        # Regression (caught in live verification): "Arabia Saudí v. Uruguay"
        # matches the single-word English tokens "Arabia" and "Uruguay", but the
        # "í" in "Saudí" is the reliable Spanish tell. The foreign-marker check
        # must run before the both-team-name check so this stays non-English.
        n = "US (Peacock 061) |  Arabia Saudí v. Uruguay (2026-06-15 17:00:00)"
        assert plugin._stream_language_rank(n, "Saudi Arabia", "Uruguay") == plugin._LANG_RANK_NON_ENGLISH

    def test_foreign_audio_feed_label_is_non_english(self, plugin):
        # Regression (#113, caught live): the audio is Czech/Korean even though
        # the team names are spelled in English, so the "<lang> Feed" label must
        # demote it below the plain English feed.
        czech = "TSN+ 11 : Czech Feed: FIFA World Cup 2026: Korea Republic vs. Czechia"
        korean = "TSN+ 12 : Korean Feed: FIFA World Cup 2026: Korea Republic vs. Czechia"
        assert plugin._stream_language_rank(czech, "South Korea", "Czechia") == plugin._LANG_RANK_NON_ENGLISH
        assert plugin._stream_language_rank(korean, "South Korea", "Czechia") == plugin._LANG_RANK_NON_ENGLISH

    def test_plain_english_feed_outranks_foreign_audio_feed(self, plugin):
        # The plain English FIFA feed (no "<lang> Feed" label) must sort ahead.
        czech = plugin._stream_language_rank(
            "TSN+ 11 : Czech Feed: ... Korea Republic vs. Czechia", "South Korea", "Czechia")
        english = plugin._stream_language_rank(
            "FIFA World Cup 2026 05: Korea Republic 03:00 Czechia", "South Korea", "Czechia")
        assert english < czech  # English (0) before non-English (2)

    def test_team_name_alone_does_not_trip_feed_marker(self, plugin):
        # "Czechia" contains "CZECH" but no feed noun follows, so a plain feed
        # naming the team must NOT be mislabeled foreign.
        n = "FIFA World Cup 2026 05: Korea Republic 03:00 Czechia"
        assert plugin._stream_language_rank(n, "South Korea", "Czechia") == plugin._LANG_RANK_ENGLISH

    def test_english_provider_with_single_team_is_english(self, plugin):
        # Only one team named, but "BBC" is an English provider marker.
        n = "WC2026: BBC Scotland"
        assert plugin._stream_language_rank(n, "Scotland", "Haiti") == plugin._LANG_RANK_ENGLISH

    def test_no_signal_is_unknown(self, plugin):
        assert plugin._stream_language_rank("Generic Sports Channel 12", "Foo", "Bar") == plugin._LANG_RANK_UNKNOWN

    def test_empty_name_is_unknown(self, plugin):
        assert plugin._stream_language_rank("", "Mexico", "South Africa") == plugin._LANG_RANK_UNKNOWN

    def test_peacock_alone_is_not_english(self, plugin):
        # Guard the DO-NOT: "Peacock" must not be treated as an English marker
        # (it carries Telemundo Spanish too). With no team-name match and no
        # other signal it stays unknown, never English.
        assert plugin._stream_language_rank("US (Peacock 099) | Highlights", "Foo", "Bar") != plugin._LANG_RANK_ENGLISH


class TestStreamSortKeyEnglishFirst:
    """#111: english_first prepends the language rank so all English variants
    sort ahead of all non-English ones, quality preserved within each tier."""

    def test_quality_only_when_flag_off(self, plugin):
        # Default behavior unchanged: UHD non-English sorts before SD English.
        eng_sd = plugin._stream_sort_key(None, "Mexico vs South Africa SD", home="Mexico", away="South Africa")
        non_uhd = plugin._stream_sort_key(None, "Sudáfrica vs México UHD", home="Mexico", away="South Africa")
        assert non_uhd < eng_sd  # quality wins when english_first defaults off

    def test_english_low_quality_beats_non_english_high_quality(self, plugin):
        eng_sd = plugin._stream_sort_key(
            None, "Mexico vs South Africa SD", english_first=True, home="Mexico", away="South Africa")
        non_uhd = plugin._stream_sort_key(
            None, "Sudáfrica vs México UHD", english_first=True, home="Mexico", away="South Africa")
        assert eng_sd < non_uhd  # language is primary

    def test_quality_preserved_within_english_tier(self, plugin):
        eng_uhd = plugin._stream_sort_key(
            None, "Mexico vs South Africa UHD", english_first=True, home="Mexico", away="South Africa")
        eng_sd = plugin._stream_sort_key(
            None, "Mexico vs South Africa SD", english_first=True, home="Mexico", away="South Africa")
        assert eng_uhd < eng_sd  # within English, UHD before SD

    def test_unknown_sorts_between_english_and_non_english(self, plugin):
        eng = plugin._stream_sort_key(
            None, "Mexico vs South Africa HD", english_first=True, home="Mexico", away="South Africa")
        unknown = plugin._stream_sort_key(
            None, "Generic Channel HD", english_first=True, home="Mexico", away="South Africa")
        non_eng = plugin._stream_sort_key(
            None, "Sudáfrica vs México HD", english_first=True, home="Mexico", away="South Africa")
        assert eng < unknown < non_eng


class TestUsBroadcastPreference:
    """stream_priority='us_preferred' breaks quality TIES toward US English
    feeds, but never overrides quality. Acceptance (Jake): the 4K Fox feed of
    USA vs Australia must be the top stream."""

    def test_us_rank_identifies_us_networks(self, plugin):
        assert plugin._us_broadcast_rank("FOX Sports 1") == plugin._US_RANK_US
        assert plugin._us_broadcast_rank("ESPN") == plugin._US_RANK_US
        assert plugin._us_broadcast_rank("FS1 4K") == plugin._US_RANK_US

    def test_us_rank_excludes_canada_and_uk(self, plugin):
        assert plugin._us_broadcast_rank("TSN 4") == plugin._US_RANK_NON_US
        assert plugin._us_broadcast_rank("Sportsnet") == plugin._US_RANK_NON_US
        assert plugin._us_broadcast_rank("BBC One") == plugin._US_RANK_NON_US

    def test_us_rank_foreign_language_us_feed_not_preferred(self, plugin):
        # ESPN Deportes is US-based but Spanish — not the preferred English feed.
        assert plugin._us_broadcast_rank("ESPN Deportes") == plugin._US_RANK_NON_US

    def test_us_rank_team_name_usa_not_false_positive(self, plugin):
        # A USMNT fixture on a Canadian feed must NOT read as US: 'USA'/'US' are
        # team names here, not broadcaster tokens (why they're excluded).
        assert plugin._us_broadcast_rank("TSN: USA vs Australia") == plugin._US_RANK_NON_US

    def test_us_breaks_tie_among_equal_quality(self, plugin):
        us = plugin._stream_sort_key({"width": 1920, "height": 1080}, "FOX Sports 1", prefer_us=True)
        non_us = plugin._stream_sort_key({"width": 1920, "height": 1080}, "TSN 4", prefer_us=True)
        assert us < non_us

    def test_quality_still_dominates_us(self, plugin):
        # A 1080p US feed must NOT beat a 2160p non-US feed: quality is primary.
        us_1080 = plugin._stream_sort_key({"width": 1920, "height": 1080}, "FOX Sports 1", prefer_us=True)
        non_us_2160 = plugin._stream_sort_key({"width": 3840, "height": 2160}, "TSN 4", prefer_us=True)
        assert non_us_2160 < us_1080

    def test_default_mode_has_no_us_tiebreak(self, plugin):
        # Without prefer_us the key carries no country term: equal-quality US and
        # non-US feeds tie (downstream src_order decides), preserving old behavior.
        us = plugin._stream_sort_key({"width": 1920, "height": 1080}, "FOX Sports 1")
        non_us = plugin._stream_sort_key({"width": 1920, "height": 1080}, "TSN 4")
        assert us == non_us

    def test_acceptance_4k_fox_usa_v_aus_is_top(self, plugin):
        # Real config: widen_stream_pool ON (english_first=True) AND us_preferred,
        # with realistic live feed names (no team names): "FOX 4K" is a US token
        # but NOT an English token; "TSN 1" IS an English token. Under us_preferred
        # quality leads, so the probed-4K Fox tops the 1080p TSN. The bug this
        # guards: english_first alone sank FOX below TSN (TSN-first symptom).
        kw = dict(english_first=True, prefer_us=True, home="United States", away="Australia")
        fox_4k = plugin._stream_sort_key(
            {"width": 3840, "height": 2160, "ffmpeg_output_bitrate": 12876.0}, "FOX 4K", **kw)
        tsn_1080 = plugin._stream_sort_key(
            {"width": 1920, "height": 1080, "ffmpeg_output_bitrate": 5803.0}, "TSN 1", **kw)
        bein_es_1080 = plugin._stream_sort_key(
            {"width": 1920, "height": 1080, "ffmpeg_output_bitrate": 2748.0},
            "Bein Sports en Espanol", **kw)
        ranked = sorted(
            [("fox_4k", fox_4k), ("tsn_1080", tsn_1080), ("bein_es_1080", bein_es_1080)],
            key=lambda t: t[1])
        assert ranked[0][0] == "fox_4k", f"expected fox_4k first, got {[r[0] for r in ranked]}"

    def test_us_preferred_overrides_english_first_language_ordering(self, plugin):
        # Regression for the live bug: with english_first ON, a 4K feed whose name
        # is NOT an English token must still beat a 1080p English-token feed under
        # us_preferred (quality leads; language is only a final tiebreak). Without
        # prefer_us, english_first sinks the 4K feed below the 1080p one.
        fox_4k, tsn_1080 = {"width": 3840, "height": 2160}, {"width": 1920, "height": 1080}
        hw = dict(home="United States", away="Australia")
        # us_preferred: 4K FOX wins (quality leads)
        assert plugin._stream_sort_key(fox_4k, "FOX 4K", english_first=True, prefer_us=True, **hw) < \
               plugin._stream_sort_key(tsn_1080, "TSN 1", english_first=True, prefer_us=True, **hw)
        # english_first only (the #111 behavior that produced TSN-first): TSN wins
        assert plugin._stream_sort_key(tsn_1080, "TSN 1", english_first=True, **hw) < \
               plugin._stream_sort_key(fox_4k, "FOX 4K", english_first=True, **hw)

    def test_manifest_stream_priority_matches_code(self, plugin):
        # plugin.json's stream_priority option values + default MUST match the
        # code constants, or picking "Prefer US broadcasts" in the UI would
        # silently no-op (the runtime check compares against _STREAM_PRIORITY_US).
        import json
        import os
        repo = os.path.dirname(os.path.abspath(plugin.__file__))
        manifest = json.load(open(os.path.join(repo, "plugin.json"), encoding="utf-8"))
        field = next(f for f in manifest["fields"] if f["id"] == plugin._STREAM_PRIORITY_SETTING)
        values = {o["value"] for o in field["options"]}
        assert values == {plugin._STREAM_PRIORITY_QUALITY, plugin._STREAM_PRIORITY_US}
        assert field["default"] == plugin._STREAM_PRIORITY_QUALITY
