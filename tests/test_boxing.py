"""Tests for the Boxing source (sources/boxing.py).

Network is never called: we monkeypatch `boxing.requests.get` with a fake
response. The schedule payload below is a real capture from the Boxing Data
API (2026-07-10), trimmed to the fields the parser reads, so the test pins the
actual wire shape (naive datetimes, the '(Cancelled)' marker, the broadcast
country array) rather than an invented one.
"""

import pytest

from dispatcharr_ranked_matchups.sources import BoxingSource
from dispatcharr_ranked_matchups.sources.base import SportSource
from dispatcharr_ranked_matchups.sources import boxing as boxing_mod
from dispatcharr_ranked_matchups._util import FIELD_AWAY_SENTINEL


class FakeResp:
    status_code = 200

    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


# Real capture (2026-07-10), trimmed. Note: naive datetimes (no offset), a
# date-only card at T00:00:00, and a cancelled card carrying "(Cancelled)".
_SCHEDULE_PAYLOAD = {
    "metadata": {"timestamp": "2026-07-10T18:51:08+00:00"},
    "pagination": {"page": 1, "items": 3, "total_pages": 1, "total_items": 3},
    "error": None,
    "data": [
        {
            "id": "6a11cfa967a83631b538dd83",
            "title": "Gassiev vs. Kadiru: IBA Pro 19",
            "date": "2026-07-11T15:00:00",
            "venue": "VTB Arena",
            "location": "Moscow, Russia",
            "broadcast": [
                {"country": "US", "broadcasters": ["DAZN"]},
                {"country": "United Kingdom", "broadcasters": ["DAZN Global"]},
            ],
            "poster_image_url": "https://assets.boxing-data.com/x.jpg",
        },
        {
            "id": "69fbd6dfe91967a315c29309",
            "title": "Olascuaga vs. Dominguez (Cancelled): History in the Making",
            "date": "2026-07-12T00:00:00",
            "venue": "Civic Center",
            "location": "San Francisco, California",
            "broadcast": [{"country": "US", "broadcasters": ["ProBox TV US"]}],
            "poster_image_url": None,
        },
        {
            "id": "abc123",
            "title": "Alvarez vs. Crawford: Undisputed Super Middleweight",
            "date": "2026-07-13T02:00:00",
            "venue": "Allegiant Stadium",
            "location": "Las Vegas, Nevada",
            "broadcast": [{"country": "US", "broadcasters": ["Netflix"]}],
            "poster_image_url": None,
        },
    ],
}


class TestBoxingConstants:
    def test_implements_interface(self):
        assert issubclass(BoxingSource, SportSource)

    def test_constants(self):
        src = BoxingSource(api_key="x")
        assert src.sport_prefix == "BOX"
        assert src.sport_label == "Boxing"

    def test_not_importance(self):
        assert BoxingSource(api_key="x").supports_importance is False

    def test_no_key_returns_empty(self):
        assert BoxingSource(api_key="").fetch_upcoming() == []


class TestBoxingParse:
    def test_parses_and_drops_cancelled(self, monkeypatch):
        monkeypatch.setattr(
            boxing_mod.requests, "get", lambda *a, **kw: FakeResp(_SCHEDULE_PAYLOAD)
        )
        rows = BoxingSource(api_key="x").fetch_upcoming()
        # 3 in the payload, the cancelled one dropped -> 2 rows.
        assert len(rows) == 2
        # home is the fighters portion (matcher keys off distinctive surnames,
        # not the generic promo suffix); the full title is kept in extra.
        homes = [r.home for r in rows]
        assert "Gassiev vs. Kadiru" in homes
        assert "Gassiev vs. Kadiru: IBA Pro 19" not in homes
        full = [r.extra["full_title"] for r in rows]
        assert "Gassiev vs. Kadiru: IBA Pro 19" in full
        assert all("Cancelled" not in t for t in full)

    def test_field_event_contract(self, monkeypatch):
        monkeypatch.setattr(
            boxing_mod.requests, "get", lambda *a, **kw: FakeResp(_SCHEDULE_PAYLOAD)
        )
        row = BoxingSource(api_key="x").fetch_upcoming()[0]
        assert row.away == FIELD_AWAY_SENTINEL
        assert row.rank_home is None and row.rank_away is None
        assert row.extra["is_field_event"] is True
        assert row.extra["fd_competition_code"] == "BOXING"
        assert row.start_time.tzinfo is not None  # naive -> UTC-aware
        assert row.extra["us_broadcasters"] == ["DAZN"]

    def test_major_vs_event_tier(self, monkeypatch):
        monkeypatch.setattr(
            boxing_mod.requests, "get", lambda *a, **kw: FakeResp(_SCHEDULE_PAYLOAD)
        )
        rows = BoxingSource(api_key="x").fetch_upcoming()
        by_full = {r.extra["full_title"]: r.extra["stage"] for r in rows}
        # Plain promo card -> EVENT; explicit title/undisputed card -> MAJOR
        # (MAJOR is detected on the full title, where the belt framing lives).
        assert by_full["Gassiev vs. Kadiru: IBA Pro 19"] == "EVENT"
        assert by_full["Alvarez vs. Crawford: Undisputed Super Middleweight"] == "MAJOR"

    def test_error_envelope_returns_empty(self, monkeypatch):
        payload = {"data": [], "error": {"code": "DateOutOfRange", "message": "x"}}
        monkeypatch.setattr(
            boxing_mod.requests, "get", lambda *a, **kw: FakeResp(payload)
        )
        assert BoxingSource(api_key="x").fetch_upcoming() == []

    def test_lookahead_clamped_to_free_tier(self, monkeypatch):
        captured = {}

        def fake_get(url, params=None, headers=None, timeout=None):
            captured["params"] = params
            captured["headers"] = headers
            return FakeResp({"data": [], "error": None})

        monkeypatch.setattr(boxing_mod.requests, "get", fake_get)
        BoxingSource(api_key="x").fetch_upcoming(days_ahead=30)
        # Free tier caps the queryable range; 30 must be clamped to 7.
        assert captured["params"]["days"] == boxing_mod._FREE_TIER_MAX_DAYS
        # And the RapidAPI auth headers must be sent.
        assert captured["headers"]["x-rapidapi-key"] == "x"
        assert captured["headers"]["x-rapidapi-host"] == boxing_mod._RAPIDAPI_HOST

    def test_match_name_strips_promo_suffix(self):
        # "Fighter vs. Fighter: Promo" -> fighters only (distinctive surnames);
        # a generic promo suffix like "IBA Pro 19" must not leak into keywords.
        assert boxing_mod._match_name("Gassiev vs. Kadiru: IBA Pro 19") == "Gassiev vs. Kadiru"
        assert boxing_mod._match_name("Alvarez vs. Crawford: Undisputed") == "Alvarez vs. Crawford"

    def test_match_name_falls_back_when_no_vs(self):
        # A promo-only title (no "vs." head) has no fighters to isolate: keep
        # the full title rather than inventing a name.
        assert boxing_mod._match_name("Night of Champions XII") == "Night of Champions XII"

    def test_match_name_ignores_trailing_parenthetical_in_head(self):
        assert boxing_mod._match_name("Olascuaga vs. Dominguez (Cancelled): x") == "Olascuaga vs. Dominguez"

    def test_http_error_status_returns_empty(self, monkeypatch):
        class ErrResp:
            status_code = 429

            def json(self):
                raise AssertionError("json() must not be read on a >=400 status")

        monkeypatch.setattr(boxing_mod.requests, "get", lambda *a, **kw: ErrResp())
        assert BoxingSource(api_key="x").fetch_upcoming() == []
