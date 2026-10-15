"""#309: the League weights setting multiplies every game in a league.

Three layers, each pinned on its own:
  - merged_settings.parse_league_weights reads the text ("NFL=1.5, epl=0.7");
  - scoring.score_game applies the multiplier to raw before compression and
    records it as a `league_weight` breakdown entry that keeps the breakdown
    summing to raw;
  - plugin._league_weights keeps only codes an enabled source emits, and
    _action_refresh hands each game its own league's weight (end to end through
    the refresh_env harness, with real source instances).

Expected values are literals, not the constants under test.
"""
import logging
from datetime import datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups import merged_settings as ms
from dispatcharr_ranked_matchups.scoring import GameSignals, Weights, score_game


class TestParseLeagueWeights:

    def test_forms_accepted_and_codes_casefolded(self):
        got, problems = ms.parse_league_weights(" NFL = 1.5 ;laliga:2\nSerieA=0.5, EPL=0,")
        assert got == {"nfl": 1.5, "laliga": 2.0, "seriea": 0.5, "epl": 0.0}
        assert problems == []

    def test_the_bounds_themselves_are_accepted(self):
        assert ms.parse_league_weights("NFL=0, NHL=100") == ({"nfl": 0.0, "nhl": 100.0}, [])

    def test_blank_and_none_mean_no_weights(self):
        assert ms.parse_league_weights("") == ({}, [])
        assert ms.parse_league_weights("   ") == ({}, [])
        assert ms.parse_league_weights(None) == ({}, [])

    @pytest.mark.parametrize("entry", ["NFL", "=2", "NFL=lots", "NFL=nan", "NFL=inf", "NFL=-1",
                                       "NFL=100.5", "NFL=1e308"])
    def test_bad_entry_skipped_and_reported(self, entry):
        got, problems = ms.parse_league_weights(f"{entry}, NHL=2")
        assert got == {"nhl": 2.0}
        assert len(problems) == 1
        assert entry in problems[0]

    def test_scoring_weights_parser_unchanged_by_the_shared_tokenizer(self):
        # parse_weight_overrides now shares _name_number_entries with this
        # setting; its own contract (case-insensitive, weight_ prefix, both
        # separators, bad entries reported) must not move.
        got, problems = ms.parse_weight_overrides(" Favorite = 8 ;spread:1\nweight_rank=2, bogus=1, rank")
        assert got == {"favorite": 8.0, "spread": 1.0, "rank": 2.0}
        assert len(problems) == 2


class TestScoreGameLeagueWeight:

    SIG = dict(rank_a=2, rank_b=3, favorite_match=["Wrexham"], spread=2.0, importance_points=4.0)

    def test_default_weight_adds_nothing(self):
        plain = score_game(GameSignals(**self.SIG), Weights())
        same = score_game(GameSignals(**self.SIG), Weights(), league_weight=1.0, league="EPL")
        assert same == plain
        assert "league_weight" not in plain.breakdown

    def test_weight_multiplies_raw_before_compression(self):
        plain = score_game(GameSignals(**self.SIG), Weights())
        heavy = score_game(GameSignals(**self.SIG), Weights(), league_weight=2.0, league="NFL")
        assert heavy.raw == pytest.approx(2 * plain.raw, abs=0.01)
        assert heavy.breakdown["league_weight"] == pytest.approx(plain.raw, abs=0.01)
        assert heavy.final > plain.final
        assert heavy.notes[-1] == "league weight x2 (NFL)"

    def test_breakdown_still_sums_to_raw(self):
        plain = score_game(GameSignals(**self.SIG), Weights())
        for w in (0.0, 0.37, 1.5, 3.0):
            s = score_game(GameSignals(**self.SIG), Weights(), league_weight=w, league="NHL")
            # The weight really applied (an ignored weight would also sum)...
            assert s.raw == pytest.approx(plain.raw * w, abs=0.01)
            assert "league_weight" in s.breakdown
            # ...and the breakdown still accounts for every point of it.
            assert s.raw == pytest.approx(round(sum(s.breakdown.values()), 2), abs=0.001)

    def test_negative_raw_is_scaled_too(self):
        # A negative Scoring weight can make raw negative; League weights 0
        # must still zero it and 2 must still double it.
        neg = Weights(favorite=-6.0)
        sig = GameSignals(favorite_match=["Wrexham"])
        assert score_game(sig, neg).raw == -6.0
        assert score_game(sig, neg, league_weight=0.0, league="NFL").raw == 0.0
        doubled = score_game(sig, neg, league_weight=2.0, league="NFL")
        assert doubled.raw == -12.0
        assert doubled.breakdown["league_weight"] == -6.0

    def test_the_largest_weight_stays_finite(self):
        import math
        big = dict(self.SIG, importance_points=40.0)
        s = score_game(GameSignals(**big), Weights(importance=100.0),
                       league_weight=ms.MAX_LEAGUE_WEIGHT, league="NFL")
        assert math.isfinite(s.raw)
        assert s.final == 10.0

    def test_zero_weight_scores_the_game_at_zero(self):
        s = score_game(GameSignals(**self.SIG), Weights(), league_weight=0.0, league="EPL")
        assert s.raw == 0.0
        assert s.final == 0.0
        assert s.notes[-1] == "league weight x0 (EPL)"

    def test_unscored_game_gets_no_stub_entry(self):
        s = score_game(GameSignals(), Weights(), league_weight=2.0, league="NFL")
        assert s.raw == 0.0
        assert "league_weight" not in s.breakdown
        assert s.notes == []


class _Src:
    def __init__(self, prefix):
        self.sport_prefix = prefix


class TestLeagueWeightsSetting:

    def test_keeps_only_codes_an_enabled_source_emits(self, plugin, caplog):
        with caplog.at_level(logging.WARNING):
            got = plugin._league_weights(
                {"league_weights": "nfl=2, LaLiga=0.5, NBA=3, NFL=oops"},
                [_Src("NFL"), _Src("LaLiga"), _Src("EPL")],
            )
        assert got == {"nfl": 2.0, "laliga": 0.5}
        text = caplog.text
        assert "nba" in text and "no enabled league" in text
        assert "'NFL=oops'" in text

    def test_blank_setting_is_empty(self, plugin):
        assert plugin._league_weights({}, [_Src("NFL")]) == {}
        assert plugin._league_weights({"league_weights": ""}, [_Src("NFL")]) == {}


def _two_league_slate(monkeypatch, epl_games=1):
    """One ranked NCAA men's soccer game (one_ranked 4.44 at default weights)
    and `epl_games` EPL games (the first is 15th of 20, one_ranked 1.32), with
    real source instances, the same shape as test_plugin_helpers' #198
    end-to-end test."""
    from dispatcharr_ranked_matchups.sources.base import GameRow
    from dispatcharr_ranked_matchups.sources.ncaa_soccer import NcaaSoccerSource
    from dispatcharr_ranked_matchups.sources.soccer import SoccerSource
    start = datetime.now(timezone.utc) + timedelta(hours=3)
    poll_src = NcaaSoccerSource(gender="m")
    poll_game = GameRow(
        sport_prefix="NCAAMSOC", sport_label="NCAA Men's Soccer",
        home="Syracuse", away="Wake Forest", rank_home=25, rank_away=None,
        start_time=start, extra={"game_id": 9001},
    )
    league_src = SoccerSource("epl", fd_api_key="x")
    table = [
        {"name": f"Club {i}", "position": i, "points": 40 - i, "played": 10,
         "goal_difference": 10 - i}
        for i in range(1, 21)
    ]
    league_games = [
        GameRow(
            sport_prefix="EPL", sport_label="English Premier League",
            home=f"Club {pos}", away=f"Visitor {pos}", rank_home=pos, rank_away=None,
            start_time=start, extra={"game_id": 9002 + i, "standings_table": table},
        )
        for i, pos in enumerate((15, 1, 5, 10, 20)[:epl_games])
    ]
    monkeypatch.setattr(poll_src, "fetch_upcoming", lambda days_ahead=7: [poll_game])
    monkeypatch.setattr(league_src, "fetch_upcoming", lambda days_ahead=7: league_games)
    return [poll_src, league_src]


def _rows(cache):
    """sport_prefix -> row; with several EPL games the last one wins, so only
    read EPL through this on a one-EPL-game slate."""
    return {r["sport_prefix"]: r for r in cache["games"] + cache.get("bench", [])}


class TestRefreshAppliesLeagueWeights:

    def test_each_game_gets_its_own_leagues_weight(self, refresh_env, monkeypatch):
        _write_prior, run, read = refresh_env
        result = run(_two_league_slate(monkeypatch), league_weights="ncaamsoc=2")
        assert result["status"] == "ok", result
        rows = _rows(read())

        weighted = rows["NCAAMSOC"]
        assert weighted["score_breakdown"]["league_weight"] == 4.44
        assert weighted["score_raw"] == 8.88
        assert "league weight x2 (NCAAMSOC)" in weighted["score_notes"]

        untouched = rows["EPL"]
        assert "league_weight" not in untouched["score_breakdown"]
        assert untouched["score_raw"] == 1.32

    def test_adaptive_scoring_compresses_the_weighted_raw(self, refresh_env, monkeypatch):
        # Adaptive scoring re-derives `score` from the batch's raws; the raw it
        # reads must be the weighted one, or the weight would move the
        # breakdown and not the rating. Six games, so the batch is past the
        # adaptive minimum (5) and the median-relative curve really runs.
        _write_prior, run, read = refresh_env

        def poll_score(weights_text):
            result = run(_two_league_slate(monkeypatch, epl_games=5),
                         adaptive_scoring=True, league_weights=weights_text)
            assert result["status"] == "ok", result
            return _rows(read())["NCAAMSOC"]

        plain, weighted = poll_score(""), poll_score("ncaamsoc=3")
        assert weighted["score_raw"] == pytest.approx(3 * plain["score_raw"], abs=0.01)
        assert weighted["score"] > plain["score"]

    def test_a_curation_preset_does_not_override_it(self, plugin, refresh_env, monkeypatch):
        # high_curation sets rank=1.5, so the EPL row's one_ranked is
        # (5 / 19) / 2 * 10 * 1.5 = 1.97; the league weight then halves the
        # raw: -0.98 in the breakdown (1.97 * -0.5 = -0.985, rounded), 0.99 left. Its
        # importance weight is zeroed here only because importance > 0 makes
        # the refresh demand each league's full season, which needs network.
        preset = plugin._CURATION_PRESETS["high_curation"]
        monkeypatch.setitem(plugin._CURATION_PRESETS, "high_curation",
                            {**preset, "importance": 0.0})
        _write_prior, run, read = refresh_env
        result = run(_two_league_slate(monkeypatch),
                     curation_preset="high_curation", league_weights="EPL=0.5")
        assert result["status"] == "ok", result
        epl = _rows(read())["EPL"]
        assert epl["score_breakdown"]["one_ranked"] == 1.97
        assert epl["score_breakdown"]["league_weight"] == -0.98
        assert epl["score_raw"] == 0.99
