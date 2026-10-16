"""#318: season records reach the LLM context, and a season-opener claim is
rejected unless the context says nothing has been played.

The live failure (2026-10-16): a week-7 NFL game, both teams six games in,
was previewed as "Two Super Bowl-winning franchises kick off their seasons
... both start from scratch tonight". The NFL row carried no record and no
season progress, so the model had nothing to contradict the guess."""

from datetime import datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups import llm_descriptions as llm
from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
from dispatcharr_ranked_matchups.sources.base import GameRow
from dispatcharr_ranked_matchups.sources.ncaa_soccer import NcaaSoccerSource
from dispatcharr_ranked_matchups.sources.nfl import NflRegularSource
from dispatcharr_ranked_matchups.sources.nhl import NhlRegularSource

UTC = timezone.utc
T0 = datetime(2026, 9, 10, 0, 20, tzinfo=UTC)


def _final(gid, home, away, hp, ap, days):
    return {"id": gid, "home": home, "away": away, "home_points": hp, "away_points": ap,
            "status": "FINISHED", "start_time": T0 + timedelta(days=days)}


def _scheduled(gid, home, away, days):
    return {"id": gid, "home": home, "away": away, "home_points": None, "away_points": None,
            "status": "SCHEDULED", "start_time": T0 + timedelta(days=days)}


class _Nfl(NflRegularSource):
    """The real NFL source with its two network calls replaced."""

    SEASON = [
        _final("1", "Denver Broncos", "Kansas City Chiefs", 20, 17, 0),
        _final("2", "Seattle Seahawks", "Denver Broncos", 24, 10, 7),
        _final("3", "Seattle Seahawks", "Kansas City Chiefs", 13, 13, 14),  # a tie
        _scheduled("4", "Denver Broncos", "Seattle Seahawks", 35),
    ]

    def __init__(self, season=None):
        super().__init__(season_end_year=2027)
        self.season_reads = 0
        self._season = self.SEASON if season is None else season

    def _fetch_full_season_games(self):
        self.season_reads += 1
        return list(self._season)

    def fetch_upcoming(self, days_ahead=7):
        return [GameRow(sport_prefix="NFL", sport_label="NFL",
                        home="Denver Broncos", away="Seattle Seahawks",
                        rank_home=None, rank_away=None,
                        start_time=datetime.now(UTC) + timedelta(days=1),
                        extra={"nfl_game_id": "4", "game_id": "4",
                               "fd_competition_code": "NFL"})]


# ---------- the source side ----------

class TestRecordExtra:
    def test_never_fetches_a_season(self):
        src = _Nfl()
        assert src.record_extra("Denver Broncos", "Seattle Seahawks") == {}
        assert src.season_reads == 0

    def test_tallies_the_season_already_read(self):
        src = _Nfl()
        src.require_season()  # what the refresh does right after fetch_upcoming
        out = src.record_extra("Denver Broncos", "Seattle Seahawks")
        assert out == {
            "record_home": {"wins": 1, "losses": 1, "played": 2},
            # The tie is in neither W nor L: `played` and `ties` carry it.
            "record_away": {"wins": 1, "losses": 0, "played": 2, "ties": 1},
        }
        assert src.season_reads == 1

    def test_a_team_with_no_finished_game_is_none_not_0_0(self):
        src = _Nfl(season=[_scheduled("9", "Denver Broncos", "Seattle Seahawks", 1)])
        src.require_season()
        assert src.record_extra("Denver Broncos", "Seattle Seahawks") == {
            "record_home": None, "record_away": None}

    def test_points_leagues_carry_points_and_draws(self):
        nhl = NhlRegularSource()
        nhl._initial_state_cache = {"_teams": {
            # 4 wins (8 pts) + 1 overtime loss (1 pt) + 1 regulation loss.
            "Minnesota Wild": {"wins": 4, "losses": 2, "games_played": 6, "standings_points": 9},
        }}
        assert nhl.record_extra("Minnesota Wild", "Nobody")["record_home"] == {
            "wins": 4, "losses": 1, "ot_losses": 1, "played": 6, "points": 9}
        soc = NcaaSoccerSource.__new__(NcaaSoccerSource)
        soc._initial_state_cache = {"_teams": {
            "Creighton": {"wins": 8, "losses": 1, "draws": 3, "games_played": 12, "standings_points": 27},
        }}
        soc._all_games_cache = None
        soc._season_fallback_at = None
        assert soc.record_extra("Creighton", "Xavier")["record_home"] == {
            "wins": 8, "losses": 1, "draws": 3, "played": 12, "points": 27}


# ---------- the refresh stamps it ----------

class TestRefreshStampsRecords:
    def test_records_ride_on_the_cache_row(self, plugin, refresh_env):
        _write_prior, run, read = refresh_env
        src = _Nfl()
        run([src], weight_importance=1.0, importance_seasons=20)
        row = next(g for g in read()["games"] if g["sport_prefix"] == "NFL")
        assert row["extra"]["record_home"] == {"wins": 1, "losses": 1, "played": 2}
        assert row["extra"]["record_away"] == {"wins": 1, "losses": 0, "played": 2, "ties": 1}
        assert src.season_reads == 1, "no second read for the records"

    def test_importance_off_reads_no_season_for_prose(self, plugin, refresh_env):
        # With importance off the refresh never reads the season; the records
        # must not become the reason it does.
        _write_prior, run, read = refresh_env
        src = _Nfl()
        run([src], weight_importance=0)
        row = next(g for g in read()["games"] if g["sport_prefix"] == "NFL")
        assert "record_home" not in row["extra"]
        assert src.season_reads == 0


# ---------- the LLM context ----------

def _ctx(code, home_rec, away_rec, home="Denver Broncos", away="Seattle Seahawks"):
    g = {"home": home, "away": away, "sport_label": code, "sport_prefix": code,
         "start_time_utc": "2026-10-16T00:15:00+00:00",
         "extra": {"fd_competition_code": code, "record_home": home_rec, "record_away": away_rec}}
    return llm.build_llm_context(g, "", "", LEAGUE_CONTEXTS[code])


class TestContextStatesTheSeason:
    def test_nfl_week_7_reads_as_week_7(self):
        ctx = _ctx("NFL", {"wins": 3, "losses": 3, "played": 6}, {"wins": 4, "losses": 2, "played": 6})
        assert "Season progress: 6 of 17 games played." in ctx
        assert "  - Denver Broncos: 3-3. 11 games left." in ctx
        assert "this is the opener" not in ctx

    def test_a_tie_counts_as_a_game_played(self):
        ctx = _ctx("NFL", {"wins": 3, "losses": 2, "played": 6, "ties": 1}, None)
        assert "Season progress: 6 of 17 games played." in ctx
        assert "  - Denver Broncos: 3-2-1. 11 games left." in ctx

    def test_nhl_states_record_and_points(self):
        ctx = _ctx("NHL", {"wins": 4, "losses": 1, "ot_losses": 1, "played": 6, "points": 9},
                   {"wins": 3, "losses": 3, "played": 6, "points": 7},
                   home="Minnesota Wild", away="St. Louis Blues")
        assert "Season progress: 6 of 82 games played." in ctx
        assert "  - Minnesota Wild: 4-1-1 (9 pts), 76 games left" in ctx
        # Points thresholds get no "N more" arithmetic (a game is worth 0-2).
        assert "more win" not in ctx and "more point" not in ctx

    def test_soccer_draws_render(self):
        ctx = _ctx("NCAA_MSOC", {"wins": 8, "losses": 1, "draws": 3, "played": 12, "points": 27},
                   None, home="Creighton", away="Xavier")
        assert "  - Creighton: 8-1-3 (27 pts), 8 games left" in ctx
        assert "  - Xavier: has not played yet this season." in ctx

    def test_a_points_league_with_a_table_keeps_the_table_path(self):
        g = {"home": "A", "away": "B", "sport_label": "MLS", "sport_prefix": "MLS",
             "start_time_utc": "2026-10-16T00:15:00+00:00",
             "extra": {"fd_competition_code": "MLS_EAST",
                       "record_home": {"wins": 1, "losses": 0, "played": 1, "points": 3},
                       "standings_table_current": [
                           {"position": 1, "team": "A", "points": 3, "playedGames": 1},
                           {"position": 2, "team": "B", "points": 0, "playedGames": 1}]}}
        ctx = llm.build_llm_context(g, "", "", LEAGUE_CONTEXTS["MLS_EAST"])
        assert "(3 pts)" not in ctx, "a table, when present, is the source of truth"


# ---------- the guard ----------

LIVE_PROSE = ("Two Super Bowl-winning franchises kick off their seasons with a prime-time "
              "clash in Denver. The Seahawks and Broncos both start from scratch tonight.")


class TestOpenerGuard:
    def test_the_live_failure_is_rejected(self):
        ctx = _ctx("NFL", {"wins": 3, "losses": 3, "played": 6}, {"wins": 4, "losses": 2, "played": 6})
        assert llm.reject_reason(LIVE_PROSE, ctx) == "not a season opener: kick off their seasons"

    def test_rejected_with_no_season_context_at_all(self):
        # The pre-#318 NFL row: no progress line. "start from scratch" was on
        # no list, so this sentence passed every guard.
        assert llm.reject_reason("Both teams start from scratch tonight.", "Match: A at B") \
            == "not a season opener: start from scratch"

    def test_allowed_at_a_real_opener(self):
        # Records stamped, neither team has played: this IS the opener, and
        # the context must say so or the guard rejects a true preview.
        ctx = _ctx("NFL", None, None)
        assert "Season progress: no games played yet, this is the opener." in ctx
        assert llm.reject_reason(LIVE_PROSE, ctx) is None

    def test_no_records_stamped_says_nothing_about_progress(self):
        g = {"home": "A", "away": "B", "sport_label": "NFL", "sport_prefix": "NFL",
             "start_time_utc": "2026-10-16T00:15:00+00:00", "extra": {"fd_competition_code": "NFL"}}
        ctx = llm.build_llm_context(g, "", "", LEAGUE_CONTEXTS["NFL"])
        assert "Season progress" not in ctx, "absent records are unknown, not an opener"

    @pytest.mark.parametrize("prose", [
        "The Rangers and Devils kick off their season series in Newark.",
        "The Rangers kick off their season-series tonight.",
        "Seattle has been unbeaten since the start of the season.",
        "Seattle has been unbeaten since the very start of the season.",
        "The captain plays his first game of the season tonight.",
        "The rookie makes his season debut tonight.",
        "The Broncos host their home opener of the month.",
    ])
    def test_mid_season_sentences_pass(self, prose):
        ctx = _ctx("NFL", {"wins": 3, "losses": 3, "played": 6}, {"wins": 4, "losses": 2, "played": 6})
        assert llm.claims_an_opener_it_is_not(prose, ctx) is None

    @pytest.mark.parametrize("prose", [
        "It is the season opener for both sides.",
        "A season-opening clash under the lights.",
        "Seattle kicks off its 2026 season tonight.",
        "Both clubs launch their seasons tonight.",
        "This is the opening fixture of the season.",
    ])
    def test_opener_claims_fail_mid_season(self, prose):
        ctx = _ctx("NFL", {"wins": 3, "losses": 3, "played": 6}, {"wins": 4, "losses": 2, "played": 6})
        assert llm.claims_an_opener_it_is_not(prose, ctx)


class TestLastSeasonGuard:
    """The SYSTEM_PROMPT already says "Say NOTHING about last season unless a
    'Last season' line is present"; nothing enforced it. Found on the #318 live
    run: the NHL context below came back "after last season's push"."""

    NHL_PROSE = ("Colorado has stumbled to an uneven start after last season's push, "
                 "while Pittsburgh sits even further back in the early going.")

    def _nhl(self):
        return _ctx("NHL", {"wins": 4, "losses": 3, "played": 7, "points": 9},
                    {"wins": 3, "losses": 4, "played": 7, "points": 7},
                    home="Colorado Avalanche", away="Pittsburgh Penguins")

    def test_the_live_claim_is_rejected(self):
        ctx = self._nhl()
        # Instrument: the early-phase guidance mentions last season itself, so
        # a naive `"last season" in context` check would have passed this.
        assert "last season" in ctx.lower()
        assert llm.reject_reason(self.NHL_PROSE, ctx) == "ungrounded last-season claim: last season"

    @pytest.mark.parametrize("fact", [
        "Last season's final table:\n  - Colorado Avalanche: 1st",
        "Previous meetings between these two, most recent first:\n  - 2026-03-01 (last season): Colorado 3-2 Pittsburgh",
    ])
    def test_allowed_when_the_context_has_last_seasons_facts(self, fact):
        assert llm.claims_last_season_without_facts(self.NHL_PROSE, self._nhl() + "\n" + fact) is None

    def test_this_season_wording_passes(self):
        assert llm.claims_last_season_without_facts(
            "Colorado is 4-3 with 75 games left this season.", self._nhl()) is None


class TestRecordsAreNeverAGuess:
    """Second-opinion findings: a stamped None means "has not played", and
    both None means the OPENER, so nothing that is merely unknown may be
    stamped as None."""

    def test_a_team_the_season_does_not_name_is_left_out(self):
        src = _Nfl()
        src.require_season()
        out = src.record_extra("Denver Broncos", "St Louis Spellings")
        assert "record_away" not in out and out["record_home"]["played"] == 2

    def test_a_fallback_season_stamps_nothing(self):
        src = _Nfl()
        src.require_season()
        src._season_fallback_at = datetime(2026, 10, 15, tzinfo=UTC)
        assert src.record_extra("Denver Broncos", "Seattle Seahawks") == {}

    def test_one_key_alone_is_not_an_opener(self):
        g = {"home": "A", "away": "B", "sport_label": "NFL", "sport_prefix": "NFL",
             "start_time_utc": "2026-10-16T00:15:00+00:00",
             "extra": {"fd_competition_code": "NFL", "week": 7, "record_home": None}}
        ctx = llm.build_llm_context(g, "", "", LEAGUE_CONTEXTS["NFL"])
        assert "Season progress: 6 of 17 games played." in ctx
        assert "this is the opener" not in ctx
        assert "  - B:" not in ctx, "an unknown team gets no line, not 'has not played'"


class TestLastSeasonPhrasing:
    def _bare(self):
        return "Match: A at B\nSeason progress: 6 of 82 games played."

    @pytest.mark.parametrize("prose", [
        "They reached the playoffs a season ago.",
        "They won their division in the prior campaign.",
        "Better than the previous season already.",
    ])
    def test_other_phrasings_are_caught(self, prose):
        assert llm.claims_last_season_without_facts(prose, self._bare())

    def test_last_year_is_licensed_by_a_dated_meeting(self):
        ctx = self._bare() + ("\nPrevious meetings between these two, most recent first:"
                              "\n  - 2025-12-14 (this season): A 3-2 B")
        assert llm.claims_last_season_without_facts("They met last year in a thriller.", ctx) is None
        assert llm.claims_last_season_without_facts("They met last year in a thriller.", self._bare())
        # Season wording still needs last season's facts, meetings or not.
        assert llm.claims_last_season_without_facts("They finished last season strong.", ctx)
