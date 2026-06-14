"""Tests for _util.parse_iso_utc, stable_hash_int, extract_game_number_after_marker,
and the playoff-series rendering helpers."""

from datetime import datetime, timedelta, timezone

from dispatcharr_ranked_matchups._util import (
    CHANNEL_NUMBER_ORIGIN,
    CHANNEL_NUMBER_TIEBREAK_SLOTS,
    FIELD_AWAY_SENTINEL,
    extract_game_number_after_marker,
    group_advance_text,
    group_phase_text,
    group_results_lines,
    group_standings_lines,
    is_field_event,
    parse_iso_utc,
    series_phase_text,
    series_record_text,
    series_result_lines,
    stable_channel_number,
    stable_hash_int,
)


class TestIsFieldEvent:
    """#127: single source of truth for the field-event shape (no opponent)."""

    def test_sentinel_away_is_field_event(self):
        assert is_field_event(FIELD_AWAY_SENTINEL) is True
        assert is_field_event("Field") is True

    def test_real_opponent_is_not_field_event(self):
        assert is_field_event("Ohio State") is False
        assert is_field_event("") is False
        assert is_field_event(None) is False

    def test_extra_flag_wins_even_without_sentinel(self):
        # A source could set the flag without using the literal sentinel; the
        # flag is the primary signal.
        assert is_field_event("Some Opponent", {"is_field_event": True}) is True

    def test_falsy_extra_falls_back_to_sentinel(self):
        assert is_field_event("Field", {}) is True
        assert is_field_event("Field", {"is_field_event": False}) is True
        assert is_field_event("Arsenal", {"is_field_event": False}) is False


class TestParseIsoUtc:
    def test_zulu_suffix(self):
        dt = parse_iso_utc("2026-04-27T19:30:00Z")
        assert dt == datetime(2026, 4, 27, 19, 30, tzinfo=timezone.utc)

    def test_explicit_offset(self):
        dt = parse_iso_utc("2026-04-27T15:30:00-04:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == -4 * 3600

    def test_naive_string_gets_utc(self):
        # CFBD sometimes emits naive timestamps; we must attach UTC.
        dt = parse_iso_utc("2026-04-27T19:30:00")
        assert dt is not None
        assert dt.tzinfo is timezone.utc

    def test_none_passthrough(self):
        assert parse_iso_utc(None) is None
        assert parse_iso_utc("") is None

    def test_garbage_returns_none(self):
        assert parse_iso_utc("not-a-date") is None
        assert parse_iso_utc("2026-13-99") is None


class TestStableHashInt:
    def test_deterministic_within_process(self):
        a = stable_hash_int("Wrexham AFC|Hull City|2026-04-27T19:30:00Z")
        b = stable_hash_int("Wrexham AFC|Hull City|2026-04-27T19:30:00Z")
        assert a == b

    def test_different_inputs_differ(self):
        a = stable_hash_int("Wrexham|Hull|2026-04-27T19:30:00Z")
        b = stable_hash_int("Wrexham|Hull|2026-04-28T19:30:00Z")
        assert a != b

    def test_known_md5_anchor(self):
        # Pin the hash function: if someone "improves" the hash later, the
        # marker for every existing virtual channel changes, which deletes
        # and recreates them on the next apply. That regression must be
        # caught by this assertion.
        # md5("test") = 098f6bcd4621d373cade4e832627b4f6
        assert stable_hash_int("test") == int("098f6bcd4621d373", 16)


class TestExtractGameNumberAfterMarker:
    """Shared helper extracted from NCAA Baseball / Softball playoff
    sources. Pulls the integer game number that follows a marker string
    in an ESPN headline, stripping the "(if necessary)" trailer."""

    def test_simple_game_number(self):
        assert extract_game_number_after_marker(
            "Super Regional - Game 1", "Super Regional - Game "
        ) == 1

    def test_two_digit_game_number(self):
        # Not currently observed in NCAA postseason (games are best-of-3
        # or best-of-7 max), but the parser must handle multi-digit
        # numbers if ESPN ever emits them.
        assert extract_game_number_after_marker(
            "Super Regional - Game 10", "Super Regional - Game "
        ) == 10

    def test_strips_if_necessary_trailer(self):
        assert extract_game_number_after_marker(
            "Super Regional - Game 3 (if necessary)", "Super Regional - Game "
        ) == 3

    def test_empty_headline(self):
        assert extract_game_number_after_marker("", "Super Regional - Game ") is None

    def test_marker_not_present(self):
        # If the marker substring isn't in the headline, return None
        #: caller is expected to try a different marker or skip.
        assert extract_game_number_after_marker(
            "Some other headline", "Super Regional - Game "
        ) is None

    def test_non_digit_after_marker(self):
        # If the marker is followed by something non-numeric, return
        # None rather than guess. ESPN has not been observed to do this,
        # but graceful-degrade is the contract.
        assert extract_game_number_after_marker(
            "Super Regional - Game X", "Super Regional - Game "
        ) is None

    def test_marker_at_end_of_string(self):
        # No digits to consume → None.
        assert extract_game_number_after_marker(
            "Super Regional - Game ", "Super Regional - Game "
        ) is None


# The series schema these render from is documented in _util.py. CAR is this
# game's home team, VGK the away team, in every fixture below.
def _series(**overrides):
    s = {
        "title": "Stanley Cup Final",
        "game_number": 1,
        "best_of": 7,
        "home_wins": 0,
        "away_wins": 0,
        "results": [],
    }
    s.update(overrides)
    return s


class TestSeriesPhaseText:
    def test_title_and_game_number(self):
        assert series_phase_text(_series(game_number=2)) == "Stanley Cup Final, Game 2 of 7"

    def test_no_title_drops_prefix(self):
        assert series_phase_text(_series(title="", game_number=3)) == "Game 3 of 7"

    def test_missing_game_number_returns_empty(self):
        assert series_phase_text(_series(game_number=None)) == ""

    def test_zero_best_of_returns_empty(self):
        assert series_phase_text(_series(best_of=0)) == ""

    def test_none_and_non_dict_return_empty(self):
        assert series_phase_text(None) == ""
        assert series_phase_text("nope") == ""


class TestSeriesRecordText:
    def test_tied(self):
        assert series_record_text(_series(home_wins=1, away_wins=1),
                                  "Carolina Hurricanes", "Vegas Golden Knights") == "Series tied 1-1"

    def test_home_team_leads(self):
        out = series_record_text(_series(home_wins=2, away_wins=1),
                                 "Carolina Hurricanes", "Vegas Golden Knights")
        assert out == "Carolina Hurricanes lead the series 2-1"

    def test_away_team_leads(self):
        # Win counts are keyed to home/away; when away leads, the AWAY name
        # must be the subject and the bigger number must come first.
        out = series_record_text(_series(home_wins=1, away_wins=3),
                                 "Carolina Hurricanes", "Vegas Golden Knights")
        assert out == "Vegas Golden Knights lead the series 3-1"

    def test_missing_counts_returns_empty(self):
        assert series_record_text(_series(home_wins=None), "A", "B") == ""

    def test_none_returns_empty(self):
        assert series_record_text(None, "A", "B") == ""


class TestSeriesResultLines:
    def test_empty_results(self):
        assert series_result_lines(_series()) == []

    def test_oldest_first_with_ot_tag(self):
        s = _series(results=[
            {"game_number": 1, "home": "Carolina Hurricanes", "away": "Vegas Golden Knights",
             "home_goals": 3, "away_goals": 2, "ot": False},
            {"game_number": 2, "home": "Carolina Hurricanes", "away": "Vegas Golden Knights",
             "home_goals": 1, "away_goals": 2, "ot": True},
        ])
        assert series_result_lines(s) == [
            "Game 1: Carolina Hurricanes 3, Vegas Golden Knights 2",
            "Game 2: Carolina Hurricanes 1, Vegas Golden Knights 2 (OT)",
        ]

    def test_malformed_row_skipped(self):
        # A result missing a score is skipped, not raised: a bad recap degrades
        # to the surviving rows rather than breaking the description.
        s = _series(results=[
            {"game_number": 1, "home": "A", "away": "B", "home_goals": 3, "away_goals": 2},
            {"game_number": 2, "home": "A", "away": "B"},  # missing scores
            "garbage",
        ])
        assert series_result_lines(s) == ["Game 1: A 3, B 2"]

    def test_none_returns_empty(self):
        assert series_result_lines(None) == []


# ---------- group-stage rendering ----------

def _group_stage(**overrides):
    base = {
        "tournament": "FIFA World Cup",
        "group": "C",
        "matchday": 2,
        "matchdays_total": 3,
        "standings": [
            {"position": 1, "name": "Argentina", "played": 1, "points": 3,
             "goal_difference": 1},
            {"position": 2, "name": "Mexico", "played": 1, "points": 1,
             "goal_difference": 0},
            {"position": 3, "name": "Poland", "played": 1, "points": 1,
             "goal_difference": 0},
            {"position": 4, "name": "Saudi Arabia", "played": 1, "points": 0,
             "goal_difference": -1},
        ],
        "results": [
            {"home": "Argentina", "away": "Saudi Arabia",
             "home_goals": 2, "away_goals": 1},
            {"home": "Mexico", "away": "Poland",
             "home_goals": 1, "away_goals": 1},
        ],
        "advance": "The top 2 teams in each group advance, plus the 8 best "
                   "third-placed teams across all groups.",
    }
    base.update(overrides)
    return base


class TestGroupPhaseText:
    def test_full_phrase(self):
        assert group_phase_text(_group_stage()) == (
            "FIFA World Cup Group C, Matchday 2 of 3"
        )

    def test_no_tournament_drops_to_group_only(self):
        assert group_phase_text(_group_stage(tournament="")) == (
            "Group C, Matchday 2 of 3"
        )

    def test_missing_matchday_drops_to_head(self):
        assert group_phase_text(_group_stage(matchday=None)) == "FIFA World Cup Group C"

    def test_no_group_returns_empty(self):
        assert group_phase_text(_group_stage(group="")) == ""

    def test_none_returns_empty(self):
        assert group_phase_text(None) == ""
        assert group_phase_text("nope") == ""


class TestGroupStandingsLines:
    def test_lines_in_order_with_gd_sign(self):
        lines = group_standings_lines(_group_stage())
        assert lines[0] == "#1 Argentina - 3 pts, 1 played, +1 GD"
        assert lines[3] == "#4 Saudi Arabia - 0 pts, 1 played, -1 GD"

    def test_position_falls_back_to_index(self):
        gs = _group_stage(standings=[{"name": "A", "points": 0, "played": 0,
                                       "goal_difference": 0}])
        assert group_standings_lines(gs) == ["#1 A - 0 pts, 0 played, +0 GD"]

    def test_malformed_row_skipped(self):
        gs = _group_stage(standings=[
            {"position": 1, "name": "A", "points": 3, "played": 1, "goal_difference": 1},
            {"position": 2},  # no name
            "garbage",
        ])
        assert group_standings_lines(gs) == ["#1 A - 3 pts, 1 played, +1 GD"]

    def test_none_returns_empty(self):
        assert group_standings_lines(None) == []


class TestGroupResultsLines:
    def test_scorelines(self):
        assert group_results_lines(_group_stage()) == [
            "Argentina 2-1 Saudi Arabia",
            "Mexico 1-1 Poland",
        ]

    def test_malformed_row_skipped(self):
        gs = _group_stage(results=[
            {"home": "A", "away": "B", "home_goals": 1, "away_goals": 0},
            {"home": "A", "away": "B"},  # missing scores
        ])
        assert group_results_lines(gs) == ["A 1-0 B"]

    def test_empty_before_kickoff(self):
        assert group_results_lines(_group_stage(results=[])) == []

    def test_none_returns_empty(self):
        assert group_results_lines(None) == []


class TestGroupAdvanceText:
    def test_returns_rule(self):
        assert "third-placed" in group_advance_text(_group_stage())

    def test_missing_returns_empty(self):
        assert group_advance_text(_group_stage(advance="")) == ""
        assert group_advance_text(None) == ""


# ---------- stable kickoff-time channel numbering (#121) ----------

# A fixed-offset tz stands in for a real zoneinfo zone: enough to exercise the
# local-date/time boundary logic without depending on the tz database in CI.
_ET = timezone(timedelta(hours=-5))


def _utc(y, mo, d, h=18, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


class TestStableChannelNumber:
    def test_returns_int(self):
        # Xtream Codes requires integer channel numbers; a float gets floored and
        # collision-bumped by the XC layer, scrambling order. Must be int.
        n = stable_channel_number(1000, _utc(2026, 6, 13), "m:cfb:1", _ET)
        assert isinstance(n, int)

    def test_deterministic(self):
        # Same inputs → same number, every time. The whole feature rests on this:
        # a game's number must not move across applies.
        a = stable_channel_number(1000, _utc(2026, 6, 13), "m:cfb:1", _ET)
        b = stable_channel_number(1000, _utc(2026, 6, 13), "m:cfb:1", _ET)
        assert a == b

    def test_independent_of_other_games(self):
        # Pure function of THIS game; the old scheme keyed off slate position
        # (virtual_base + cache_idx) so a re-rank moved it. Must depend only on
        # (base, kickoff, marker, tz).
        n = stable_channel_number(1000, _utc(2026, 6, 13), "m:cfb:42", _ET)
        for other in ("m:cfb:1", "m:soc:fd_9", "m:nhl:7"):
            stable_channel_number(1000, _utc(2026, 6, 13), other, _ET)
        assert stable_channel_number(1000, _utc(2026, 6, 13), "m:cfb:42", _ET) == n

    def test_earlier_kickoff_sorts_first_across_days(self):
        # Today's games get lower numbers than tomorrow's, regardless of hash.
        today = stable_channel_number(1000, _utc(2026, 6, 13, 23), "zzz", _ET)
        tomorrow = stable_channel_number(1000, _utc(2026, 6, 15, 1), "aaa", _ET)
        assert today < tomorrow

    def test_earlier_kickoff_sorts_first_within_day(self):
        # The key #121 fix vs the prior hash-within-day scheme: WITHIN a day the
        # number must increase with start time (noon < afternoon < evening),
        # independent of marker hash.
        noon = stable_channel_number(1000, _utc(2026, 6, 13, 16), "zzzz", _ET)   # 12:00 ET
        eve = stable_channel_number(1000, _utc(2026, 6, 13, 23), "aaaa", _ET)    # 19:00 ET
        assert noon < eve

    def test_same_minute_distinct_via_tiebreak(self):
        # Two games at the SAME kickoff minute get distinct numbers (different
        # hash slots) so they don't collide on the unique constraint, and both
        # stay just above the minute's base.
        a = stable_channel_number(1000, _utc(2026, 6, 13, 18), "m:cfb:1", _ET)
        b = stable_channel_number(1000, _utc(2026, 6, 13, 18), "m:cfb:2", _ET)
        assert a != b
        # Both sit within one minute-stride of each other (same minute bucket).
        assert abs(a - b) < CHANNEL_NUMBER_TIEBREAK_SLOTS

    def test_pre_origin_clamps_to_base_minute_zero(self):
        # A kickoff before the fixed origin must not produce a number below the
        # base (which could collide with the user's real channels). Clamped to
        # day_offset 0, so number is base + minute_of_day*slots + tiebreak.
        n = stable_channel_number(1000, _utc(2020, 1, 1, 5), "m:cfb:1", _ET)
        # 2020-01-01 05:00 UTC = 2020-01-01 00:00 ET → minute_of_day 0.
        assert 1000 <= n < 1000 + CHANNEL_NUMBER_TIEBREAK_SLOTS

    def test_local_timezone_decides_day_and_time(self):
        # 02:00 UTC on the 14th is 21:00 ET on the 13th. The local date+time
        # drive the number so "today" and ordering match the user's clock. Same
        # marker → same tiebreak, so the gap is exactly the minutes difference
        # times the tiebreak stride.
        early_utc = _utc(2026, 6, 14, 2, 0)
        slots = CHANNEL_NUMBER_TIEBREAK_SLOTS
        n_et = stable_channel_number(1000, early_utc, "m:cfb:1", _ET)
        n_utc = stable_channel_number(1000, early_utc, "m:cfb:1", timezone.utc)

        def mins(dt):
            return (dt.date() - CHANNEL_NUMBER_ORIGIN).days * 1440 + dt.hour * 60 + dt.minute
        et_min = mins(early_utc.astimezone(_ET))
        utc_min = mins(early_utc.astimezone(timezone.utc))
        assert n_utc - n_et == (utc_min - et_min) * slots
        assert n_utc > n_et  # UTC interpretation is later in wall-clock-from-origin

    def test_unique_across_realistic_slate(self):
        # 120 games over 10 days at staggered times: every number distinct, so no
        # two channels share a (group, channel_number).
        numbers = []
        for day in range(10):
            for i in range(12):
                start = _utc(2026, 6, 1 + day, 12 + (i % 8), (i * 7) % 60)
                numbers.append(
                    stable_channel_number(5000, start, f"m:cfb:{day}_{i}", _ET)
                )
        assert len(numbers) == len(set(numbers))
