"""Tests for the game-thumbs matchup-composite resolver (gamethumbs.py).

No live HTTP: the download layer is monkey-patched. The behaviour that actually
matters here is the TRANSIENT-vs-DEFINITIVE classification, because getting it
backwards is silent and expensive (see TestMissClassification).
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load(mod_basename):
    """Load a plugin submodule without triggering the package __init__ (which
    imports Django via plugin.py). Mirrors tests/test_logos.py."""
    mod_name = f"{PKG_NAME}.{mod_basename}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(REPO_ROOT, f"{mod_basename}.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


# gamethumbs imports `from ._util import ...` and `from .logos import ...`, so
# the package and both dependencies must be registered first.
if PKG_NAME not in sys.modules:
    pkg = types.ModuleType(PKG_NAME)
    pkg.__path__ = [REPO_ROOT]
    sys.modules[PKG_NAME] = pkg
_load("_util")
logos = _load("logos")
gamethumbs = _load("gamethumbs")


def _no_pacing(monkeypatch):
    """Neutralise the inter-request pacing sleep so tests don't take 2s each."""
    monkeypatch.setattr(gamethumbs, "_pace", lambda: None)


class _FakeHTTPError(Exception):
    """Stands in for urllib.error.HTTPError, which carries .headers."""

    def __init__(self, headers):
        self.headers = headers


# ---------- league_slug_for ----------

class TestLeagueSlugFor:
    def test_known_prefix(self):
        assert gamethumbs.league_slug_for("MLS") == "mls"

    def test_multi_segment_slug(self):
        assert gamethumbs.league_slug_for("CFB") == "ncaa/football"

    def test_unmapped_prefix_is_none(self):
        assert gamethumbs.league_slug_for("QUIDDITCH") is None

    def test_none_and_empty_safe(self):
        assert gamethumbs.league_slug_for(None) is None
        assert gamethumbs.league_slug_for("") is None

    def test_field_event_sports_are_not_mapped(self):
        # F1/UFC/golf/tennis/boxing have no head-to-head pair to composite.
        # A mapping here would mean we issue HTTP for something that can never
        # resolve, so their absence is deliberate, not an oversight.
        for prefix in ("F1", "NASCAR", "PGA", "UFC", "ATP", "WTA", "BOX"):
            assert gamethumbs.league_slug_for(prefix) is None


# ---------- build_url ----------

class TestBuildUrl:
    def test_away_first(self):
        url = gamethumbs.build_url("https://x.test", "mls", "Away FC", "Home FC")
        assert url.startswith("https://x.test/mls/Away%20FC/Home%20FC/logo")

    def test_trailing_slash_on_base_stripped(self):
        url = gamethumbs.build_url("https://x.test/", "nfl", "A", "B")
        assert "https://x.test/nfl/" in url
        assert "//nfl" not in url.replace("https://", "")

    def test_league_slashes_are_not_escaped(self):
        # "ncaa/football" is a real two-segment path; quoting it would 404.
        url = gamethumbs.build_url("https://x.test", "ncaa/football", "A", "B")
        assert "/ncaa/football/" in url

    def test_team_names_are_escaped(self):
        url = gamethumbs.build_url("https://x.test", "bra.1", "SE Palmeiras", "São Paulo FC")
        assert " " not in url
        assert "S%C3%A3o" in url

    def test_style_and_endpoint_present(self):
        url = gamethumbs.build_url("https://x.test", "mls", "A", "B")
        assert url.endswith("/logo?style=1")


# ---------- fetch_matchup_composite: miss classification ----------

class TestMissClassification:
    """A definitive miss is cached forever; a transient one must not be.

    Getting this backwards is the expensive failure: negative-caching a 429
    turns a one-minute rate-limit window into a full negative-TTL of league
    badges for every game that happened to be in flight.
    """

    def _fetch(self, monkeypatch, status, err=None):
        _no_pacing(monkeypatch)
        monkeypatch.setattr(
            gamethumbs, "download_to_file", lambda *a, **k: (status, err),
        )
        return gamethumbs.fetch_matchup_composite(
            base_url="https://x.test", sport_prefix="MLS",
            away="Columbus Crew", home="Charlotte FC", dest_path="/tmp/x.png",
        )

    def test_success_returns_url_and_not_a_miss(self, monkeypatch):
        url, definitive = self._fetch(monkeypatch, 200)
        assert url is not None and definitive is False

    def test_400_is_definitive(self, monkeypatch):
        # Server enumerates the league's real teams: a stable fact.
        assert self._fetch(monkeypatch, 400) == (None, True)

    def test_404_and_444_are_definitive(self, monkeypatch):
        assert self._fetch(monkeypatch, 404) == (None, True)
        assert self._fetch(monkeypatch, 444) == (None, True)

    def test_429_without_retry_after_is_transient(self, monkeypatch):
        assert self._fetch(monkeypatch, 429) == (None, False)

    def test_500_is_transient(self, monkeypatch):
        assert self._fetch(monkeypatch, 500) == (None, False)

    def test_network_error_is_transient(self, monkeypatch):
        assert self._fetch(monkeypatch, None) == (None, False)


# ---------- fetch_matchup_composite: no-HTTP short circuits ----------

class TestShortCircuits:
    def _spy(self, monkeypatch):
        calls = []
        _no_pacing(monkeypatch)
        monkeypatch.setattr(
            gamethumbs, "download_to_file",
            lambda *a, **k: (calls.append(a), (200, None))[1],
        )
        return calls

    def test_field_event_issues_no_request(self, monkeypatch):
        calls = self._spy(monkeypatch)
        from dispatcharr_ranked_matchups._util import FIELD_AWAY_SENTINEL
        out = gamethumbs.fetch_matchup_composite(
            "https://x.test", "UFC", FIELD_AWAY_SENTINEL, "UFC 330", "/tmp/x.png")
        assert out == (None, True)
        assert calls == []

    def test_unmapped_league_issues_no_request(self, monkeypatch):
        calls = self._spy(monkeypatch)
        out = gamethumbs.fetch_matchup_composite(
            "https://x.test", "QUIDDITCH", "A", "B", "/tmp/x.png")
        assert out == (None, True)
        assert calls == []

    def test_blank_base_url_disables_the_tier(self, monkeypatch):
        calls = self._spy(monkeypatch)
        for base in ("", "   ", None):
            out = gamethumbs.fetch_matchup_composite(
                base, "MLS", "A", "B", "/tmp/x.png")
            assert out == (None, True)
        assert calls == []

    def test_missing_team_issues_no_request(self, monkeypatch):
        calls = self._spy(monkeypatch)
        assert gamethumbs.fetch_matchup_composite(
            "https://x.test", "MLS", "", "Home", "/tmp/x.png") == (None, True)
        assert calls == []


# ---------- 429 retry ----------

class TestRateLimitRetry:
    def test_retries_once_on_429_with_retry_after(self, monkeypatch):
        _no_pacing(monkeypatch)
        monkeypatch.setattr(gamethumbs.time, "sleep", lambda s: None)
        seq = [(429, _FakeHTTPError({"Retry-After": "3"})), (200, None)]
        monkeypatch.setattr(
            gamethumbs, "download_to_file", lambda *a, **k: seq.pop(0),
        )
        url, definitive = gamethumbs.fetch_matchup_composite(
            "https://x.test", "MLS", "A", "B", "/tmp/x.png")
        assert url is not None and definitive is False
        assert seq == []  # both responses consumed: it really retried

    def test_gives_up_after_one_retry(self, monkeypatch):
        _no_pacing(monkeypatch)
        monkeypatch.setattr(gamethumbs.time, "sleep", lambda s: None)
        calls = []

        def _always_429(*a, **k):
            calls.append(1)
            return 429, _FakeHTTPError({"Retry-After": "1"})

        monkeypatch.setattr(gamethumbs, "download_to_file", _always_429)
        out = gamethumbs.fetch_matchup_composite(
            "https://x.test", "MLS", "A", "B", "/tmp/x.png")
        # Transient, NOT definitive: the next apply must be free to try again.
        assert out == (None, False)
        assert len(calls) == 2

    def test_absurd_retry_after_is_not_honoured(self, monkeypatch):
        _no_pacing(monkeypatch)
        slept = []
        monkeypatch.setattr(gamethumbs.time, "sleep", lambda s: slept.append(s))
        monkeypatch.setattr(
            gamethumbs, "download_to_file",
            lambda *a, **k: (429, _FakeHTTPError({"Retry-After": "3600"})),
        )
        out = gamethumbs.fetch_matchup_composite(
            "https://x.test", "MLS", "A", "B", "/tmp/x.png")
        assert out == (None, False)
        assert slept == []  # never block an apply for an hour


class TestParseRetryAfter:
    def test_absent_header(self):
        assert gamethumbs._parse_retry_after(_FakeHTTPError({})) is None

    def test_no_headers_attribute(self):
        assert gamethumbs._parse_retry_after(None) is None

    def test_http_date_form_unsupported_returns_none(self):
        # Retry-After may be an HTTP-date; we only honour delta-seconds.
        err = _FakeHTTPError({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        assert gamethumbs._parse_retry_after(err) is None

    def test_negative_rejected(self):
        assert gamethumbs._parse_retry_after(_FakeHTTPError({"Retry-After": "-5"})) is None

    def test_valid(self):
        assert gamethumbs._parse_retry_after(_FakeHTTPError({"Retry-After": "2"})) == 2.0


# ---------- league map integrity ----------

class TestSweepExtensionContract:
    """gamethumbs.FILE_EXT and logos._SWEEPABLE_EXTS are two names for one fact.

    If FILE_EXT ever moves (say to webp) without the sweep learning about it,
    every composite this module writes becomes invisible to the live-file set
    and gets deleted on the next apply, immediately after being downloaded.
    Nothing else couples them, and the failure is silent.
    """

    def test_file_ext_is_sweepable(self):
        assert gamethumbs.FILE_EXT in logos._SWEEPABLE_EXTS

    def test_file_ext_has_no_leading_dot(self):
        # marker_to_filename tolerates one, but the sweep set is built from the
        # raw tuple, so a dot here would produce "..png" on one side only.
        assert not gamethumbs.FILE_EXT.startswith(".")

    def test_a_written_composite_survives_a_sweep(self, tmp_path):
        # End-to-end over the real pairing rather than the constants alone.
        marker = "ranked_matchups:MLS:contract"
        name = logos.marker_to_filename(marker, gamethumbs.FILE_EXT)
        (tmp_path / name).write_bytes(b"x")
        assert logos.sweep_stale_logo_files({marker}, logo_dir=str(tmp_path)) == 0
        assert (tmp_path / name).exists()


class TestLeagueMap:
    def test_no_empty_slugs(self):
        for prefix, slug in gamethumbs.GAMETHUMBS_LEAGUE_SLUGS.items():
            assert slug and slug.strip() == slug, prefix

    def test_slugs_have_no_leading_or_trailing_slash(self):
        # build_url joins with single slashes; a stray one yields "//" and 404s.
        for prefix, slug in gamethumbs.GAMETHUMBS_LEAGUE_SLUGS.items():
            assert not slug.startswith("/") and not slug.endswith("/"), prefix

    def test_covers_the_head_to_head_sources(self):
        # Guards against a new league source shipping with no logo coverage.
        expected = {
            "NFL", "NBA", "WNBA", "MLB", "NHL", "MLS", "NWSL", "LigaMX",
            "CFB", "CBB", "NCAAW", "NCAAMSOC", "NCAAWSOC", "NCAABSB", "NCAASBL",
            "EPL", "EFL", "UCL", "BL1", "LaLiga", "SerieA", "Ligue1",
            "Eredivisie", "PrimeiraLiga", "BSA",
            "WC", "EURO", "FRIENDLY", "FRIENDLYW", "CLUBFRIENDLY",
        }
        assert expected <= set(gamethumbs.GAMETHUMBS_LEAGUE_SLUGS)
