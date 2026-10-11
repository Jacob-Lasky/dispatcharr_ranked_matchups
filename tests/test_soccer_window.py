"""Soccer's post-kickoff window: 2.5 h for a league game, 3 h for a tie that
can go to extra time and penalties (#257).

The post window is not only match recall. `_game_end_utc` dates a game's end
with it, so it is also where the Live EPG block ends (`_live_window`), where
the auto-record window ends (#216, through the same `_live_window`), and when
the reaper may delete the channel. A knockout decided on penalties ends about
2 h 45 m after kickoff (90 + stoppage + 15 half time + 5 break + 30 extra time
+ stoppage + the shootout), so 2.5 h stopped the WC / EURO / UCL knockout
recordings before the shootout and could reap the channel during it.

Every expected time below is a literal, never derived from the constants
under test.
"""

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from tests.test_field_event_lookup import _install_fake_orm, _Row

KICKOFF = "2026-12-13T15:00:00+00:00"
KICK = datetime(2026, 12, 13, 15, 0, tzinfo=timezone.utc)
AT_2H30 = datetime(2026, 12, 13, 17, 30, tzinfo=timezone.utc)
AT_2H45 = datetime(2026, 12, 13, 17, 45, tzinfo=timezone.utc)
AT_3H = datetime(2026, 12, 13, 18, 0, tzinfo=timezone.utc)


def row(prefix, **extra):
    """A cache row as _action_refresh writes it (the `extra` dict is copied
    from the GameRow, and `tournament_stage` from its stage)."""
    return {
        "sport_prefix": prefix, "home": "Arsenal", "away": "Chelsea",
        "start_time_utc": KICKOFF, "tournament_stage": extra.get("stage"),
        "extra": dict(extra),
    }


# (prefix, extra) pairs whose game can go to extra time and penalties. Stages
# and slugs are the values the sources write (and, for the ESPN slugs, what
# ESPN returned on 2026-10-11).
KNOCKOUTS = [
    ("WC", {"stage": "LAST_16"}),
    ("WC", {"stage": "THIRD_PLACE"}),
    ("EURO", {"stage": "QUARTER_FINALS"}),
    ("UCL", {"stage": "PLAYOFFS"}),
    ("UCL", {"stage": "FINAL"}),
    ("MLS", {"stage": "MLS_CSF", "season_slug": "eastern-conference-playoffs---semifinals"}),
    ("NWSL", {"season_slug": "playoffs---quarterfinals"}),
    ("LigaMX", {"season_slug": "apertura---finals"}),
    ("LigaMX", {"season_slug": "apertura---8th-seed-game"}),
    # A playoff source's own flag, whatever the stage says (#243).
    ("UCL", {"stage": "LEAGUE_STAGE", "postseason": True}),
    # Cups: every tie, with or without a mapped round.
    ("EFLCUP", {"stage": "CUP_R2"}),
    ("EFLCUP", {}),
    ("FACUP", {"stage": "FINAL"}),
    ("FACUP", {}),
    # NCAA soccer: the College Cup, and the regular-season source, which also
    # carries the conference tournaments with nothing to tell them apart.
    ("NCAAMSOC", {"stage": "R64"}),
    ("NCAAWSOC", {"stage": "NCG"}),
    ("NCAAMSOC", {}),
    ("NCAAWSOC", {}),
]

LEAGUE_GAMES = [
    ("EPL", {"stage": "REGULAR_SEASON"}),
    ("EPL", {}),
    ("UCL", {"stage": "LEAGUE_STAGE"}),
    ("WC", {"stage": "GROUP_STAGE"}),
    ("EURO", {"stage": "GROUP_STAGE"}),
    ("MLS", {"season_slug": "regular-season"}),
    ("NWSL", {"season_slug": "regular-season"}),
    ("LigaMX", {"season_slug": "torneo-apertura"}),
    ("LigaMX", {"season_slug": "torneo-clausura"}),
    # Friendlies never play extra time.
    ("FRIENDLY", {}),
    ("FRIENDLYW", {}),
    ("CLUBFRIENDLY", {}),
]


def _id(case):
    prefix, extra = case
    return f"{prefix}-{extra.get('stage') or extra.get('season_slug') or 'nostage'}"


class TestTheWindow:
    @pytest.mark.parametrize("case", KNOCKOUTS, ids=_id)
    def test_a_knockout_gets_three_hours_and_keeps_the_tight_pre_window(self, plugin, case):
        prefix, extra = case
        assert plugin._epg_match_window(prefix, extra) == (5, 3.0)

    @pytest.mark.parametrize("case", LEAGUE_GAMES, ids=_id)
    def test_a_league_game_keeps_two_and_a_half_hours(self, plugin, case):
        prefix, extra = case
        assert plugin._epg_match_window(prefix, extra) == (5, 2.5)

    def test_a_stage_on_a_non_soccer_game_changes_nothing(self, plugin):
        """The knockout rule is soccer's: an NFL Wild Card game keeps the
        gridiron window, and boxing keeps its +24 h."""
        assert plugin._epg_match_window("NFL", {"stage": "WC"}) == (30, 4.0)
        assert plugin._epg_match_window("BOX", {"stage": "FINAL"}) == (720, 24.0)

    def test_the_knockout_window_outlasts_a_shootout_and_fits_the_live_cap(self, plugin):
        """2 h 45 m is the measured length of an ET-plus-penalties tie, and
        anything at or past EPG_POST_HOURS would be clipped by _live_window's
        min(), silently giving the knockout the cap instead."""
        assert 2.75 < plugin._SOCCER_KNOCKOUT_POST_HOURS < plugin.EPG_POST_HOURS


class TestEveryConsumerEndsAKnockoutAfterTheShootout:
    """One test per consumer of _game_end_utc, each on the cache-row shape it
    really reads."""

    def test_the_live_block(self, plugin):
        start, end = plugin._live_window(row("WC", stage="LAST_16"), KICK)
        assert start == datetime(2026, 12, 13, 14, 30, tzinfo=timezone.utc)
        assert end == AT_3H and end > AT_2H45

    def test_a_league_games_live_block_still_ends_at_two_and_a_half_hours(self, plugin):
        assert plugin._live_window(row("WC", stage="GROUP_STAGE"), KICK)[1] == AT_2H30
        assert plugin._live_window(row("EPL"), KICK)[1] == AT_2H30

    def test_the_reaper(self, plugin):
        knockout = row("UCL", stage="SEMI_FINALS")
        league = row("EPL", stage="REGULAR_SEASON")
        assert plugin._game_reap_at_utc(knockout, 10) == datetime(
            2026, 12, 13, 18, 10, tzinfo=timezone.utc)
        assert plugin._game_reap_at_utc(league, 10) == datetime(
            2026, 12, 13, 17, 40, tzinfo=timezone.utc)
        # Mid-shootout, with a 1-minute delay: the league game is gone, the
        # knockout's channel is not.
        mid_shootout = datetime(2026, 12, 13, 17, 50, tzinfo=timezone.utc)
        live, expired = plugin._partition_expired([knockout, league], mid_shootout, 1)
        assert live == [knockout] and expired == [league]

    def test_the_published_guide_end(self, plugin):
        """_partition_expired's hold: the Past row is clamped to the reap
        deadline, which is the game's end plus the delay."""
        settings = {"remove_finished_after_minutes": 1}
        assert plugin._published_guide_end(row("EURO", stage="FINAL"), settings) == datetime(
            2026, 12, 13, 18, 1, tzinfo=timezone.utc)
        assert plugin._published_guide_end(row("EURO", stage="GROUP_STAGE"), settings) == datetime(
            2026, 12, 13, 17, 31, tzinfo=timezone.utc)

    def test_the_auto_record_window(self, plugin, monkeypatch, tmp_path):
        """_autorecord_prepare is the #216 recording window. Real path, with
        an empty in-memory ORM: the candidate's scheduled end is the knockout's
        live end, and the post-roll comes on top of it."""
        empty = types.SimpleNamespace(objects=_EmptyManager())
        chan_mod = types.ModuleType("apps.channels.models")
        chan_mod.Recording = chan_mod.Channel = empty
        m3u_mod = types.ModuleType("apps.m3u.models")
        m3u_mod.M3UAccount = empty
        for name, mod in [
            ("apps", types.ModuleType("apps")),
            ("apps.channels", types.ModuleType("apps.channels")),
            ("apps.channels.models", chan_mod),
            ("apps.m3u", types.ModuleType("apps.m3u")),
            ("apps.m3u.models", m3u_mod),
        ]:
            monkeypatch.setitem(sys.modules, name, mod)
        monkeypatch.setattr(plugin, "RECORDING_STATE_PATH", str(tmp_path / "rec.json"))

        knockout = dict(row("WC", stage="FINAL"), recorded_matched=["Arsenal"])
        league = dict(row("EPL", stage="REGULAR_SEASON"), recorded_matched=["Arsenal"],
                      home="Arsenal", away="Spurs")
        settings = {plugin.RECORDED_TEAMS_SETTING: "Arsenal",
                    plugin.RECORDING_POST_ROLL_SETTING: 15}
        ar = plugin._autorecord_prepare([knockout, league], settings)
        cand_ko = ar.meta[plugin._build_marker_key(knockout)][0]
        cand_league = ar.meta[plugin._build_marker_key(league)][0]
        assert cand_ko.sched_end == AT_3H and cand_ko.sched_end > AT_2H45
        assert cand_ko.end == datetime(2026, 12, 13, 18, 15, tzinfo=timezone.utc)
        assert cand_league.sched_end == AT_2H30


class _EmptyManager:
    """Every query an empty table answers: no rows, no values."""

    def filter(self, *_a, **_kw):
        return self

    def values_list(self, *_a, **_kw):
        return []

    def __iter__(self):
        return iter(())


class TestTheMatcherAndDiagnoseUseTheGamesOwnWindow:
    def test_the_matcher_window_reaches_three_hours_for_a_knockout(self, plugin, monkeypatch):
        """A listing that starts 2 h 50 m after kickoff is inside a knockout's
        window and outside a league game's: the lookup reads the GameRow's
        own `extra`, not only its prefix."""
        late = datetime(2026, 12, 13, 17, 50, tzinfo=timezone.utc)
        chans = [_Row(id=7, name="TSN 1", epg_data_id=99)]
        progs = [_Row(id=1, title="Arsenal vs Chelsea", start_time=late,
                      end_time=late + timedelta(hours=2), epg_id=99)]
        _install_fake_orm(monkeypatch, chans, progs)

        def game(extra):
            return types.SimpleNamespace(home="Arsenal", away="Chelsea", sport_prefix="UCL",
                                         start_time=KICK, extra=extra)

        lookup = plugin._build_epg_lookup()
        assert [c.channel_id for c in lookup(game({"stage": "LAST_16"}))] == [7]
        assert lookup(game({"stage": "LEAGUE_STAGE"})) == []

    def test_diagnose_passes_the_targets_extra(self, plugin, monkeypatch):
        seen = {}

        def spy(*args, **kwargs):
            seen.update(kwargs)
            return []

        target = dict(row("UCL", stage="QUARTER_FINALS"), start_time_utc="2099-04-07T19:00:00Z",
                      score=5.0, channel_name_current=None, sport_label="UEFA Champions League")
        monkeypatch.setattr(plugin, "_read_cache", lambda: {"games": [target]})
        monkeypatch.setattr(plugin, "_diagnose_window_sample", spy)
        monkeypatch.setattr(plugin, "_candidate_changed_at", lambda cands: None)
        monkeypatch.setattr(plugin, "_build_epg_lookup", lambda **kw: (lambda shim: []))
        assert plugin._action_diagnose({})["status"] == "ok"
        assert seen.get("extra") == {"stage": "QUARTER_FINALS"}
