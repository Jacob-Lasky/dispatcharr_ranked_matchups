"""Tests for _util.parse_iso_utc, stable_hash_int, extract_game_number_after_marker."""

from datetime import datetime, timezone

from dispatcharr_ranked_matchups._util import (
    extract_game_number_after_marker,
    parse_iso_utc,
    stable_hash_int,
)


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
