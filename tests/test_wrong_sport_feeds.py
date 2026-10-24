"""#324: the matcher attached other-sport and previous-week feeds.

On 2026-10-23 the only stream on `CFB NC State at Stanford` was
'ESPN+ 138: College Swimming League (Indiana, Cal, Stanford, NC State) @ Oct 16
8:27PM ET'. It names both schools in one segment, so the both-teams gate passed
it, and it reached Tier 1 through Path B (channel names), which had no date
check and no sport check. A live sweep found 5 of 17 game channels carrying a
feed of this shape (swimming, WSL, a different 'Real').

These tests drive the REAL `_build_epg_lookup` (over the in-memory ORM from
test_field_event_lookup) into the REAL `match_games_to_channels`, because the
predicates alone passed while Path B never consulted them.
"""
from datetime import date, datetime, timezone
import types

import pytest

from dispatcharr_ranked_matchups.matcher import (
    _PREFIX_SPORT,
    _team_keywords,
    match_games_to_channels,
    name_conflicts_with_game,
    name_is_stale_for_game,
)
from tests.test_field_event_lookup import _Row, _install_fake_orm
from tests.test_mapped_prefixes import built_sources  # noqa: F401  (module fixture)

SWIM = ("ESPN+ 138: College Swimming League (Indiana, Cal, Stanford, NC State) "
        "@ Oct 16 8:27PM ET")


def _game(home, away, prefix, start):
    return types.SimpleNamespace(home=home, away=away, sport_prefix=prefix,
                                 sport_label=prefix, start_time=start, extra={})


def _match(plugin, game, chans, streams=(), local_tz=timezone.utc):
    lookup = plugin._build_epg_lookup(local_tz=local_tz)
    [res] = match_games_to_channels([(game, None, None)], lookup, api_key="", model="")
    return res, lookup.stats


@pytest.fixture
def eastern():
    from zoneinfo import ZoneInfo
    return ZoneInfo("America/New_York")


class TestTheReportedPair:
    """The exact channel and game from the report."""

    def test_swimming_slot_is_not_matched_to_the_football_game(
            self, plugin, monkeypatch, eastern):
        chans = [_Row(id=366207, name=SWIM, epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Stanford", "NC State", "CFB",
                     datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc))
        res, stats = _match(plugin, game, chans, local_tz=eastern)
        assert res.channel_id is None and res.channel_ids == [], res
        # Dropped at the lookup by its date, before the sport gate saw it.
        assert stats["stale_dated_channels_dropped"] == 1

    def test_swimming_slot_is_not_matched_to_the_soccer_game(
            self, plugin, monkeypatch, eastern):
        chans = [_Row(id=366207, name=SWIM, epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("NC State", "Stanford", "NCAAMSOC",
                     datetime(2026, 10, 24, 22, 0, tzinfo=timezone.utc))
        res, _ = _match(plugin, game, chans, local_tz=eastern)
        assert res.channel_ids == [], res

    def test_same_week_swimming_slot_is_rejected_by_sport_alone(
            self, plugin, monkeypatch, eastern):
        # The date gate cannot help when the other sport's meet is the same
        # night; the sport gate must.
        name = SWIM.replace("Oct 16", "Oct 23")
        chans = [_Row(id=366207, name=name, epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Stanford", "NC State", "CFB",
                     datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc))
        res, stats = _match(plugin, game, chans, local_tz=eastern)
        assert res.channel_ids == [], res
        assert stats["stale_dated_channels_dropped"] == 0
        assert "named another sport" in res.note and "swimming" in res.note

    def test_the_real_football_feed_still_wins_beside_it(
            self, plugin, monkeypatch, eastern):
        # Recall control: the gates remove the swimming slot, not the game.
        chans = [
            _Row(id=366207, name=SWIM, epg_data_id=None),
            _Row(id=1, name="ESPN+ 045: NC State at Stanford (10.23 10:30 PM ET)",
                 epg_data_id=None),
            _Row(id=2, name="ACCN 03: College Football: NC State vs Stanford",
                 epg_data_id=None),
        ]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Stanford", "NC State", "CFB",
                     datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc))
        res, _ = _match(plugin, game, chans, local_tz=eastern)
        assert sorted(res.channel_ids) == [1, 2]
        assert res.method == "regex_strict"


class TestOtherSweepFindings:
    def test_wsl_fixture_is_not_the_premier_league_game(self, plugin, monkeypatch):
        # Same weekend, so only the competition gate can tell them apart.
        chans = [
            _Row(id=10, name="AU (STAN 42) | Chelsea v Tottenham Hotspur  WSL 2026/27 "
                             "(2026-10-24 21:50:15)", epg_data_id=None),
            _Row(id=11, name="AU (STAN 97) | Chelsea v Tottenham Hotspur  Premier League "
                             "Matchweek 8 2026/2027 (2026-10-25 03:20:15)", epg_data_id=None),
        ]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Chelsea FC", "Tottenham Hotspur FC", "EPL",
                     datetime(2026, 10, 24, 16, 30, tzinfo=timezone.utc))
        res, _ = _match(plugin, game, chans)
        assert res.channel_ids == [11]

    def test_real_betis_is_not_real_madrid(self, plugin, monkeypatch):
        chans = [_Row(id=20, name="CA (TSN+ 017) | LaLiga: Real Betis vs. Barcelona",
                      epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("FC Barcelona", "Real Madrid CF", "LaLiga",
                     datetime(2026, 10, 25, 20, 0, tzinfo=timezone.utc))
        res, _ = _match(plugin, game, chans)
        assert res.channel_ids == []

    def test_paris_fc_is_not_psg(self):
        kws = [k.lower() for k in _team_keywords("Paris Saint-Germain FC")]
        assert "paris" not in kws
        # The run-on spelling 11 live feeds use, previously reached via 'Paris'.
        assert "paris saintgermain" in kws

    def test_stale_channel_name_counts_separately_from_streams(self, plugin, monkeypatch):
        chans = [_Row(id=30, name="NHL 01: Penguins vs. Avalanche (10.16 9:00 PM ET)",
                      epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Colorado Avalanche", "Pittsburgh Penguins", "NHL",
                     datetime(2026, 10, 24, 1, 0, tzinfo=timezone.utc))
        res, stats = _match(plugin, game, chans)
        assert res.channel_ids == []
        assert stats["stale_dated_channels_dropped"] == 1
        assert stats["stale_dated_streams_dropped"] == 0


class TestConflictPredicate:
    @pytest.mark.parametrize("text,prefix", [
        (SWIM, "CFB"),
        ("Big Ten+ 04: Women's Volleyball: Stanford vs NC State", "NCAAMSOC"),
        ("ESPN+ 12: NCAA Women's Soccer: Stanford vs NC State", "NCAAMSOC"),
        ("ESPN+ 12: NCAA Men's Soccer: Stanford vs NC State", "NCAAWSOC"),
        ("Chelsea v Tottenham Hotspur  WSL 2026/27", "EPL"),
        ("ESPN+ 9: Field Hockey: Duke vs North Carolina", "NHL"),
        ("ESPN+ 9: Women’s Basketball: Duke vs UNC", "CBB"),  # curly apostrophe
        ("Peacock 3: Ascent FC v Power FC  Rugby", "MLS"),
        ("NFL 04 | Packers at Bears", "MLS"),
    ])
    def test_rejects_a_name_that_says_it_is_something_else(self, text, prefix):
        assert name_conflicts_with_game(text, prefix) is not None

    @pytest.mark.parametrize("text,prefix,teams", [
        ("ESPN+ 045: NC State at Stanford", "CFB", ()),            # names no sport
        ("ACCN: College Football: NC State vs Stanford", "CFB", ()),
        ("Sky Sports Football: Chelsea v Spurs", "EPL", ()),        # 'football' is neither
        ("NCAA Football: Stanford vs NC State", "NCAAMSOC", ()),   # ditto
        ("ESPN+: NCAA Men's Soccer: Stanford vs NC State", "NCAAMSOC", ()),
        ("Big East: Soccer & Volleyball doubleheader", "NCAAMSOC", ()),  # own named too
        ("NBA TV: Celtics vs Knicks", "NBA", ()),
        ("NFL Network: Stanford vs NC State", "CFB", ()),           # same sport
        ("England Women vs Spain Women", "FRIENDLYW", ("England Women", "Spain Women")),
        ("Bowling Green vs Toledo", "CFB", ("Bowling Green", "Toledo")),
        ("anything at all", "NOT_A_PREFIX", ()),                    # unmapped: not judged
        ("Yemen vs Oman", "FRIENDLY", ()),                          # 'men' needs a word edge
    ])
    def test_keeps_a_name_with_no_contrary_evidence(self, text, prefix, teams):
        assert name_conflicts_with_game(text, prefix, teams) is None

    def test_a_team_name_cannot_veto_its_own_feed(self):
        # A women's national side named '... Women' in a men's-default source
        # would otherwise read its own name as the other gender.
        assert name_conflicts_with_game(
            "Spain Women vs England Women", "FRIENDLY",
            ("Spain Women", "England Women")) is None

    def test_field_hockey_does_not_read_as_ice_hockey(self):
        assert name_conflicts_with_game("Field Hockey: Duke v UNC", "NHL") is not None


class TestStationNamesAreNotJudged:
    def test_programme_on_a_sport_branded_station_is_judged_by_the_programme(
            self, plugin, monkeypatch):
        # Overflow coverage: an EFL game on Sky Sports Golf. The station name
        # names no team, so it is a brand, not the event, and must not veto it.
        win = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)
        chans = [_Row(id=40, name="Sky Sports Golf HD", epg_data_id=99)]
        progs = [_Row(id=1, title="EFL: Blackburn Rovers v Wrexham", sub_title="",
                      description="", start_time=win,
                      end_time=win.replace(hour=16), epg_id=99)]
        _install_fake_orm(monkeypatch, chans, progs)
        game = _game("Blackburn Rovers FC", "Wrexham AFC", "EFL", win)
        res, _ = _match(plugin, game, chans)
        assert res.channel_ids == [40]


class TestDateHelperStaysOneSided:
    def test_future_dated_channel_name_is_kept(self):
        assert not name_is_stale_for_game(
            "AU (STAN 97) | Chelsea v Tottenham Hotspur (2026-10-25 03:20:15)",
            date(2026, 10, 24))


def test_every_emitted_prefix_is_mapped(built_sources):
    """An unmapped prefix silently skips the gate, which is exactly how a new
    source would reintroduce #324. The dead-key direction is covered by
    tests/test_mapped_prefixes.py (PREFIX_TABLES)."""
    emitted = {s.sport_prefix for s in built_sources}
    missing = sorted(emitted - set(_PREFIX_SPORT))
    assert not missing, f"add these to matcher._PREFIX_SPORT: {missing}"
    assert len(emitted) >= 30, "instrument check: too few sources were built"


class TestSecondOpinionFindings:
    """Six inputs an independent review (gpt-6-sol, 2026-10-24) produced
    against the first cut of #324. Each failed before its fix."""

    def test_et_stamped_feed_survives_for_a_user_east_of_utc(self, plugin, monkeypatch):
        # 02:30Z Oct 24 is Oct 23 22:30 ET; a Berlin user's date and UTC's are
        # both Oct 24, so min(UTC, local) called the correct feed stale.
        from zoneinfo import ZoneInfo
        chans = [_Row(id=1, name="ESPN+ 045: NC State at Stanford @ Oct 23 10:30PM ET",
                      epg_data_id=None)]
        _install_fake_orm(monkeypatch, chans, programs=[])
        game = _game("Stanford", "NC State", "CFB",
                     datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc))
        res, stats = _match(plugin, game, chans, local_tz=ZoneInfo("Europe/Berlin"))
        assert res.channel_ids == [1]
        assert stats["stale_dated_channels_dropped"] == 0

    def _subtitle_case(self, plugin, monkeypatch, sub_title):
        win = datetime(2026, 10, 24, 16, 30, tzinfo=timezone.utc)
        chans = [_Row(id=5, name="BBC One HD", epg_data_id=77)]
        progs = [_Row(id=1, title="Live Sports", sub_title=sub_title, description="",
                      start_time=win, end_time=win.replace(hour=18), epg_id=77)]
        _install_fake_orm(monkeypatch, chans, progs)
        game = _game("Chelsea FC", "Tottenham Hotspur FC", "EPL", win)
        return _match(plugin, game, chans)[0]

    def test_womens_fixture_in_the_subtitle_is_rejected(self, plugin, monkeypatch):
        res = self._subtitle_case(plugin, monkeypatch, "WSL: Chelsea vs Tottenham Hotspur")
        assert res.channel_ids == []

    def test_mens_fixture_in_the_subtitle_still_matches(self, plugin, monkeypatch):
        # Recall control for the case above (#143's path).
        res = self._subtitle_case(
            plugin, monkeypatch, "Premier League: Chelsea vs Tottenham Hotspur")
        assert res.channel_ids == [5]

    def test_stale_slot_name_admitted_by_epg_cannot_win_tier1(
            self, plugin, monkeypatch, eastern):
        # Path A admits the channel on its current programme naming ONE team;
        # Tier 1 must not then match on the slot's stale name naming both.
        win = datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc)
        chans = [_Row(id=8, name="ESPN+ 138: NC State vs Stanford @ Oct 16",
                      epg_data_id=55)]
        progs = [_Row(id=1, title="Stanford vs Oregon", sub_title="", description="",
                      start_time=win, end_time=win.replace(hour=5), epg_id=55)]
        _install_fake_orm(monkeypatch, chans, progs)
        game = _game("Stanford", "NC State", "CFB", win)
        res, _ = _match(plugin, game, chans, local_tz=eastern)
        # Not a Tier 1 (channel-name) match. The one-team programme still
        # reaches the LLM tier, which declines it with a key; keyless installs
        # take the first candidate there by design, which is not this test.
        assert res.method != "regex_strict", res

    def test_a_channel_label_cannot_vouch_for_a_contrary_programme(self):
        from dispatcharr_ranked_matchups.matcher import ChannelCandidate, candidate_conflict
        now = datetime(2026, 10, 24, 2, 30, tzinfo=timezone.utc)
        c = ChannelCandidate(channel_id=1, channel_name="CFB: NC State vs Stanford",
                             program_title="College Swimming: NC State vs Stanford",
                             program_start=now, program_end=now)
        game = _game("Stanford", "NC State", "CFB", now)
        assert "swimming" in (candidate_conflict(c, game, field=False) or "")

    @pytest.mark.parametrize("text,prefix", [
        ("ESPN+ 045: Stanford vs NC State WBB", "CBB"),
        ("ESPN+ 045: Stanford vs NC State MBB", "NCAAW"),
        ("ACCNX 12: Duke vs UNC WSOC", "NCAAMSOC"),
        ("ACCNX 13: Duke vs UNC WVB", "NCAAMSOC"),
    ])
    def test_college_schedule_abbreviations(self, text, prefix):
        assert name_conflicts_with_game(text, prefix) is not None

    def test_college_abbreviation_for_the_right_game_is_kept(self):
        assert name_conflicts_with_game("ESPN+ 045: Stanford vs NC State MBB", "CBB") is None
