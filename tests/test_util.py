"""Tests for _util.parse_iso_utc and stable_hash_int."""

from datetime import datetime, timezone

from dispatcharr_ranked_matchups._util import parse_iso_utc, stable_hash_int


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
