"""Tests for the SportsDB matchup-thumbnail resolver (logos.py).

Network-dependent paths are exercised via monkey-patched urlopen: no live
HTTP. The cache-file and stale-sweep paths use tmp_path fixtures.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_logos():
    """Load logos.py as a submodule of the test-registered package without
    triggering the package __init__ (which imports Django via plugin.py)."""
    mod_name = f"{PKG_NAME}.logos"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(REPO_ROOT, "logos.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


logos = _load_logos()


# ---------- _strip_trailing_qualifier ----------

class TestStripTrailingQualifier:
    def test_no_qualifier_unchanged(self):
        assert logos._strip_trailing_qualifier("Manchester City") == "Manchester City"

    def test_fc_stripped(self):
        assert logos._strip_trailing_qualifier("Manchester City FC") == "Manchester City"

    def test_afc_stripped(self):
        assert logos._strip_trailing_qualifier("Hull City AFC") == "Hull City"

    def test_case_insensitive(self):
        assert logos._strip_trailing_qualifier("Aston Villa fc") == "Aston Villa"

    def test_only_trailing(self):
        # "FC" in the middle of a name (rare but possible) is left alone.
        assert logos._strip_trailing_qualifier("FC Barcelona") == "FC Barcelona"

    def test_iterative(self):
        # Some clubs have double qualifiers; both should strip.
        assert logos._strip_trailing_qualifier("Some Team CF FC") == "Some Team"

    def test_empty_and_none_safe(self):
        assert logos._strip_trailing_qualifier("") == ""
        assert logos._strip_trailing_qualifier(None) == ""


# ---------- _build_search_query ----------

class TestBuildSearchQuery:
    def test_basic(self):
        assert logos._build_search_query("Manchester City", "Aston Villa") == "Manchester City vs Aston Villa"

    def test_strips_both(self):
        assert (
            logos._build_search_query("Manchester City FC", "Aston Villa FC")
            == "Manchester City vs Aston Villa"
        )


# ---------- marker_to_filename ----------

class TestMarkerToFilename:
    def test_format(self):
        f = logos.marker_to_filename("ranked_matchups:EPL:fd_535345")
        assert f.startswith("ranked_matchups_")
        assert f.endswith(".jpg")
        # 16 hex chars + prefix + ext
        assert len(f) == len("ranked_matchups_") + 16 + len(".jpg")

    def test_stable(self):
        m = "ranked_matchups:NBA:12345"
        assert logos.marker_to_filename(m) == logos.marker_to_filename(m)

    def test_distinct_markers_produce_distinct_filenames(self):
        a = logos.marker_to_filename("ranked_matchups:EPL:fd_1")
        b = logos.marker_to_filename("ranked_matchups:EPL:fd_2")
        assert a != b


# ---------- _date_in_tolerance ----------

class TestDateInTolerance:
    def test_same_day(self):
        assert logos._date_in_tolerance("2026-05-24", datetime(2026, 5, 24, tzinfo=timezone.utc))

    def test_one_day_off(self):
        assert logos._date_in_tolerance("2026-05-25", datetime(2026, 5, 24, tzinfo=timezone.utc))

    def test_two_days_off(self):
        assert logos._date_in_tolerance("2026-05-26", datetime(2026, 5, 24, tzinfo=timezone.utc))

    def test_three_days_off_rejected(self):
        assert not logos._date_in_tolerance("2026-05-27", datetime(2026, 5, 24, tzinfo=timezone.utc))

    def test_malformed_date_rejected(self):
        assert not logos._date_in_tolerance("not-a-date", datetime(2026, 5, 24, tzinfo=timezone.utc))

    def test_empty_date_rejected(self):
        assert not logos._date_in_tolerance("", datetime(2026, 5, 24, tzinfo=timezone.utc))


# ---------- _hint_matches ----------

class TestHintMatches:
    def test_no_hint_accepts_anything(self):
        assert logos._hint_matches({"strLeague": "Anything"}, None)

    def test_unmapped_prefix_accepts(self):
        # Prefix not in _SPORT_HINT map => fail-open.
        assert logos._hint_matches({"strLeague": "Anything"}, "UNKNOWNPREFIX")

    def test_league_substring(self):
        ev = {"strLeague": "English Premier League", "strSport": "Soccer"}
        assert logos._hint_matches(ev, "EPL")

    def test_sport_substring(self):
        ev = {"strLeague": "Some Cup", "strSport": "NHL"}
        assert logos._hint_matches(ev, "NHL")

    def test_mismatch_rejected(self):
        ev = {"strLeague": "NCAA Division I Basketball Mens", "strSport": "Basketball"}
        # CFB hint requires "NCAA" in league: basketball league DOES contain
        # "NCAA", so it would pass. The date filter is the real discriminator.
        # Use a non-NCAA league to confirm rejection.
        ev2 = {"strLeague": "MLB", "strSport": "Baseball"}
        assert not logos._hint_matches(ev2, "CFB")

    def test_case_insensitive(self):
        ev = {"strLeague": "english premier league", "strSport": ""}
        assert logos._hint_matches(ev, "EPL")


# ---------- resolve_thumb_url ----------

class _FakeHttpResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status

    def read(self, n: int = -1):
        if n == -1:
            data, self._body = self._body, b""
        else:
            data, self._body = self._body[:n], self._body[n:]
        return data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(payload: dict, status: int = 200):
    def opener(req, timeout=None):
        return _FakeHttpResponse(json.dumps(payload).encode("utf-8"), status=status)
    return opener


class TestResolveThumbUrl:
    def test_field_event_short_circuits(self, monkeypatch):
        # away="Field" means single-event sport (F1, golf, UFC, tennis).
        called = []
        monkeypatch.setattr(logos.urllib.request, "urlopen",
                            lambda *a, **k: called.append(1))
        url = logos.resolve_thumb_url(
            "Heineken Chinese GP", "Field",
            datetime(2026, 4, 15, tzinfo=timezone.utc), "F1", "3",
        )
        assert url is None
        assert called == []  # no HTTP call

    def test_empty_team_returns_none(self):
        assert logos.resolve_thumb_url("", "Aston Villa",
                                       datetime(2026, 5, 24, tzinfo=timezone.utc),
                                       "EPL", "3") is None
        assert logos.resolve_thumb_url("Manchester City", "",
                                       datetime(2026, 5, 24, tzinfo=timezone.utc),
                                       "EPL", "3") is None

    def test_positive_match(self, monkeypatch):
        payload = {"event": [{
            "dateEvent": "2026-05-24",
            "strEvent": "Manchester City vs Aston Villa",
            "strLeague": "English Premier League",
            "strSport": "Soccer",
            "strThumb": "https://r2.thesportsdb.com/images/media/event/thumb/foo.jpg",
        }]}
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen(payload))
        url = logos.resolve_thumb_url(
            "Manchester City", "Aston Villa",
            datetime(2026, 5, 24, tzinfo=timezone.utc), "EPL", "3",
        )
        assert url == "https://r2.thesportsdb.com/images/media/event/thumb/foo.jpg"

    def test_date_mismatch_rejected(self, monkeypatch):
        # SportsDB returns a match for a different date: the date filter
        # rejects it so we don't grab the wrong leg of a season series.
        payload = {"event": [{
            "dateEvent": "2024-12-15",  # 17 months off
            "strLeague": "English Premier League",
            "strThumb": "https://example.com/wrong.jpg",
        }]}
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen(payload))
        url = logos.resolve_thumb_url(
            "Manchester City", "Aston Villa",
            datetime(2026, 5, 24, tzinfo=timezone.utc), "EPL", "3",
        )
        assert url is None

    def test_sport_hint_filters_cross_sport_collision(self, monkeypatch):
        # SportsDB returns a basketball "Alabama vs Auburn" but we asked for football.
        # The basketball league name does contain "NCAA" (the CFB hint), but we use
        # MLB instead (no NCAA in league/sport) to confirm filter actually filters.
        payload = {"event": [{
            "dateEvent": "2025-11-29",
            "strLeague": "MLB",  # missing CFB hint substring "NCAA"
            "strSport": "Baseball",
            "strThumb": "https://example.com/wrong.jpg",
        }]}
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen(payload))
        url = logos.resolve_thumb_url(
            "Some Team", "Other Team",
            datetime(2025, 11, 29, tzinfo=timezone.utc), "CFB", "3",
        )
        assert url is None

    def test_empty_event_list(self, monkeypatch):
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen({"event": None}))
        url = logos.resolve_thumb_url(
            "Garbage", "Other",
            datetime(2026, 5, 24, tzinfo=timezone.utc), "EPL", "3",
        )
        assert url is None

    def test_event_without_thumb(self, monkeypatch):
        payload = {"event": [{
            "dateEvent": "2026-05-24",
            "strLeague": "English Premier League",
            "strThumb": "",  # event exists but no thumb yet
        }]}
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen(payload))
        url = logos.resolve_thumb_url(
            "Manchester City", "Aston Villa",
            datetime(2026, 5, 24, tzinfo=timezone.utc), "EPL", "3",
        )
        assert url is None

    def test_http_failure_returns_none(self, monkeypatch):
        def boom(req, timeout=None):
            raise OSError("network down")
        monkeypatch.setattr(logos.urllib.request, "urlopen", boom)
        url = logos.resolve_thumb_url(
            "Manchester City", "Aston Villa",
            datetime(2026, 5, 24, tzinfo=timezone.utc), "EPL", "3",
        )
        assert url is None


# ---------- ThumbCache ----------

class TestThumbCache:
    def test_empty_when_missing(self, tmp_path):
        cache = logos.ThumbCache(str(tmp_path / "missing.json"))
        fresh, url = cache.get("any")
        assert not fresh and url is None

    def test_put_and_get_positive(self, tmp_path):
        path = str(tmp_path / "cache.json")
        cache = logos.ThumbCache(path)
        cache.put("m1", "https://example.com/thumb.jpg")
        fresh, url = cache.get("m1")
        assert fresh and url == "https://example.com/thumb.jpg"

    def test_put_and_get_negative(self, tmp_path):
        cache = logos.ThumbCache(str(tmp_path / "c.json"))
        cache.put("m1", None)
        fresh, url = cache.get("m1")
        assert fresh and url is None

    def test_save_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "c.json")
        c1 = logos.ThumbCache(path)
        c1.put("m1", "https://example.com/x.jpg")
        c1.put("m2", None)
        c1.save()
        c2 = logos.ThumbCache(path)
        fresh, url = c2.get("m1")
        assert fresh and url == "https://example.com/x.jpg"
        fresh, url = c2.get("m2")
        assert fresh and url is None

    def test_positive_entry_expires(self, tmp_path):
        cache = logos.ThumbCache(str(tmp_path / "c.json"))
        cache.put("m1", "https://example.com/x.jpg")
        # backdate beyond the positive TTL
        stale_ts = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
        cache._data["m1"][0] = stale_ts
        fresh, url = cache.get("m1")
        assert not fresh
        assert url == "https://example.com/x.jpg"  # stale value still returned

    def test_negative_entry_expires_faster(self, tmp_path):
        cache = logos.ThumbCache(str(tmp_path / "c.json"))
        cache.put("m1", None)
        stale_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        cache._data["m1"][0] = stale_ts
        fresh, _ = cache.get("m1")
        assert not fresh

    def test_corrupt_file_recovers(self, tmp_path):
        path = str(tmp_path / "c.json")
        with open(path, "w") as f:
            f.write("not-json{{{")
        cache = logos.ThumbCache(path)
        # No exception; starts empty.
        fresh, url = cache.get("m1")
        assert not fresh and url is None

    def test_prune_drops_stale_markers(self, tmp_path):
        cache = logos.ThumbCache(str(tmp_path / "c.json"))
        cache.put("live", "https://example.com/x.jpg")
        cache.put("stale", "https://example.com/y.jpg")
        dropped = cache.prune({"live"})
        assert dropped == 1
        assert "live" in cache._data
        assert "stale" not in cache._data


# ---------- sweep_stale_logo_files ----------

class TestSweepStaleLogoFiles:
    def test_no_dir_zero_result(self, tmp_path):
        # Directory doesn't exist.
        assert logos.sweep_stale_logo_files(set(), str(tmp_path / "nope")) == 0

    def test_keeps_live_removes_stale(self, tmp_path):
        # Create three files: two ours (one live, one stale), one foreign.
        live_marker = "ranked_matchups:EPL:fd_111"
        stale_marker = "ranked_matchups:EPL:fd_222"
        live_name = logos.marker_to_filename(live_marker)
        stale_name = logos.marker_to_filename(stale_marker)
        for name in (live_name, stale_name, "other_uploaded_logo.png"):
            with open(tmp_path / name, "w") as f:
                f.write("x")
        removed = logos.sweep_stale_logo_files({live_marker}, str(tmp_path))
        assert removed == 1
        files = set(os.listdir(tmp_path))
        assert live_name in files
        assert stale_name not in files
        assert "other_uploaded_logo.png" in files  # foreign file untouched

    def test_no_files_matches_prefix(self, tmp_path):
        # Empty dir or no matching prefix.
        with open(tmp_path / "unrelated.png", "w") as f:
            f.write("x")
        assert logos.sweep_stale_logo_files({"anything"}, str(tmp_path)) == 0


# ---------- download_thumb ----------

class TestDownloadThumb:
    def test_writes_atomically(self, tmp_path, monkeypatch):
        body = b"FAKE JPEG BYTES"
        monkeypatch.setattr(logos.urllib.request, "urlopen",
                            lambda req, timeout=None: _FakeHttpResponse(body))
        dest = str(tmp_path / "out.jpg")
        ok = logos.download_thumb("https://example.com/x.jpg", dest)
        assert ok is True
        with open(dest, "rb") as f:
            assert f.read() == body
        # tmp file gone
        assert not any(p.endswith(".tmp") or ".tmp." in p for p in os.listdir(tmp_path) if p != "out.jpg")

    def test_http_error_returns_false(self, tmp_path, monkeypatch):
        monkeypatch.setattr(logos.urllib.request, "urlopen",
                            lambda req, timeout=None: _FakeHttpResponse(b"", status=500))
        dest = str(tmp_path / "out.jpg")
        ok = logos.download_thumb("https://example.com/x.jpg", dest)
        assert ok is False
        assert not os.path.exists(dest)

    def test_network_failure_cleans_up_tmp(self, tmp_path, monkeypatch):
        def boom(req, timeout=None):
            raise OSError("dropped")
        monkeypatch.setattr(logos.urllib.request, "urlopen", boom)
        dest = str(tmp_path / "out.jpg")
        ok = logos.download_thumb("https://example.com/x.jpg", dest)
        assert ok is False
        # No partial file or .tmp left behind.
        assert os.listdir(tmp_path) == []

    def test_empty_url_returns_false(self, tmp_path):
        assert logos.download_thumb("", str(tmp_path / "out.jpg")) is False


# ---------- league/tournament badge fallback (issue #102) ----------

class TestLeagueIdFor:
    def test_sport_league_id(self):
        assert logos.league_id_for("EPL") == 4328
        assert logos.league_id_for("CFB") == 4479      # NCAA football, NOT basketball
        assert logos.league_id_for("CBB") == 4607      # NCAA men's basketball

    def test_unmapped_prefix_is_none(self):
        # Niche NCAA sub-sports are intentionally unmapped -> channel-logo fallback.
        assert logos.league_id_for("NCAAWS") is None
        assert logos.league_id_for("MADEUP") is None
        assert logos.league_id_for(None) is None

    def test_tournament_override_when_stage_set(self, monkeypatch):
        monkeypatch.setitem(logos.SPORTSDB_TOURNAMENT_LEAGUE_IDS, "CBB", 9999)
        # tournament_stage set + prefix in override -> tournament id wins
        assert logos.league_id_for("CBB", "FINAL") == 9999
        # no tournament_stage -> falls back to the sport league id
        assert logos.league_id_for("CBB", None) == 4607

    def test_every_mapped_id_is_int_and_known_prefix(self):
        for prefix, lid in logos.SPORTSDB_LEAGUE_IDS.items():
            assert isinstance(lid, int)
            # every badge-mapped prefix is a real sport hint prefix
            assert prefix in logos._SPORT_HINT


class TestBadgeFilename:
    def test_format_and_prefix(self):
        fn = logos.badge_filename(4328)
        assert fn == "ranked_matchups_badge_4328.png"
        assert fn.startswith(logos.BADGE_FILENAME_PREFIX)

    def test_distinct_from_per_game_filename(self):
        # A badge file must never collide with a per-game marker file.
        assert logos.badge_filename(4328) != logos.marker_to_filename("ranked_matchups:EPL:x")


class TestResolveLeagueBadgeUrl:
    def test_returns_badge_url(self, monkeypatch):
        payload = {"leagues": [{"strLeague": "English Premier League",
                                "strBadge": "https://example.test/epl.png"}]}
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen(payload))
        assert logos.resolve_league_badge_url(4328, "3") == "https://example.test/epl.png"

    def test_no_leagues_returns_none(self, monkeypatch):
        monkeypatch.setattr(logos.urllib.request, "urlopen", _fake_urlopen({"leagues": None}))
        assert logos.resolve_league_badge_url(4328, "3") is None

    def test_missing_badge_field_returns_none(self, monkeypatch):
        monkeypatch.setattr(logos.urllib.request, "urlopen",
                            _fake_urlopen({"leagues": [{"strLeague": "X"}]}))
        assert logos.resolve_league_badge_url(4328, "3") is None


class TestSweepKeepsBadges:
    def test_badge_file_survives_marker_sweep(self, tmp_path):
        live_marker = "ranked_matchups:EPL:live"
        # one live per-game file, one stale per-game file, one badge file
        live_file = tmp_path / logos.marker_to_filename(live_marker)
        stale_file = tmp_path / logos.marker_to_filename("ranked_matchups:EPL:stale")
        badge_file = tmp_path / logos.badge_filename(4328)
        for f in (live_file, stale_file, badge_file):
            f.write_bytes(b"x")
        removed = logos.sweep_stale_logo_files({live_marker}, logo_dir=str(tmp_path))
        assert removed == 1               # only the stale per-game file
        assert live_file.exists()
        assert badge_file.exists()        # badge is never swept
        assert not stale_file.exists()
