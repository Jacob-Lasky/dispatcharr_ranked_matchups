"""Football-Data's upcoming window, measured (#285).

football-data.org files a match by its UTC kickoff INSTANT, and `/v4/matches`
covers dateFrom 00:00Z up to dateTo 00:00Z: dateTo is an exclusive day bound
(measured 2026-10-11: dateFrom=2026-09-19&dateTo=2026-09-20 returned the 45
matches of the 19th plus the one at exactly 2026-09-20T00:00Z, and none of the
37 later on the 20th; dateFrom=dateTo=2026-09-19 returned nothing). The source
used to ask dateFrom=today-UTC, dateTo=today+lookahead, and start each later
10-day chunk the day AFTER the previous dateTo, so:

- the window's LAST day was never asked for;
- with lookahead past 10 days, the day between two chunks was never asked for
  (a whole matchday, every refresh);
- a match that kicked off at 23:00Z yesterday (19:00 Eastern) and is still
  being played after midnight UTC was outside the window;
- a match FINISHED or CANCELLED earlier today was published as upcoming.

`_FakeFootballData` answers exactly as the measured API does, so each witness
below fails as an assertion on the pre-fix window. Dates are literals against
the pinned clock.
"""
from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.test_hold_failed_days import EVENING, NOON, _Resp, _pin_window

UTC = timezone.utc
PKG = "dispatcharr_ranked_matchups.sources"


def _match(mid, kickoff, status="TIMED", code="PL"):
    return {"id": mid, "competition": {"code": code},
            "utcDate": kickoff.strftime("%Y-%m-%dT%H:%M:%SZ"), "status": status,
            "stage": "REGULAR_SEASON", "matchday": 8,
            "homeTeam": {"name": f"Home {mid}"}, "awayTeam": {"name": f"Away {mid}"}}


class _FakeFootballData:
    """/v4/matches over `matches`, by the measured semantics: a match is
    returned when dateFrom 00:00Z <= kickoff <= dateTo 00:00Z, and a request
    whose dateTo - dateFrom exceeds 10 days is a 400."""

    def __init__(self, matches):
        self.matches = matches
        self.asked = []

    def __call__(self, url, headers=None, params=None, timeout=None):
        assert url.endswith("/v4/matches"), url
        lo = datetime.fromisoformat(params["dateFrom"]).replace(tzinfo=UTC)
        hi = datetime.fromisoformat(params["dateTo"]).replace(tzinfo=UTC)
        self.asked.append((params["dateFrom"], params["dateTo"]))
        if (hi - lo).days > 10:
            return _Resp({"message": "Specified period must not exceed 10 days."}, status=400)
        return _Resp({"matches": [m for m in self.matches
                                  if lo <= datetime.fromisoformat(m["utcDate"][:-1]).replace(tzinfo=UTC) <= hi]})


def _tier(monkeypatch, now, matches, days_ahead):
    soccer = importlib.import_module(f"{PKG}.soccer")
    soccer._clear_fd_caches()
    _pin_window(monkeypatch, now)
    fake = _FakeFootballData(matches)
    monkeypatch.setattr(soccer.requests, "get", fake)
    by_code = soccer._fetch_tier_fixtures("k", days_ahead)
    return sorted(m["id"] for ms in by_code.values() for m in ms), fake.asked


def test_the_last_day_of_the_window_is_asked_for(monkeypatch):
    """WITNESS: a 7-day lookahead at 16:00Z on 2026-10-11 asked dateTo=
    2026-10-18, which stops at 00:00Z that day, so the 18th's 15:00Z match was
    never returned. The request now runs to dateTo=2026-10-19."""
    ids, asked = _tier(monkeypatch, NOON, [
        _match(1, datetime(2026, 10, 18, 15, 0, tzinfo=UTC)),
        _match(2, datetime(2026, 10, 19, 15, 0, tzinfo=UTC)),   # past the window
    ], days_ahead=7)
    assert asked == [("2026-10-10", "2026-10-19")]
    assert ids == [1]


def test_no_day_falls_between_two_chunks(monkeypatch):
    """WITNESS: a 12-day lookahead was two chunks, 2026-10-11..21 then
    2026-10-22..23. The 21st was the first chunk's EXCLUSIVE dateTo and the
    second started on the 22nd, so nothing kicking off on the 21st after
    00:00Z was ever asked for. Chunks now start on the previous dateTo."""
    ids, asked = _tier(monkeypatch, NOON, [
        _match(1, datetime(2026, 10, 19, 15, 0, tzinfo=UTC)),
        _match(2, datetime(2026, 10, 20, 15, 0, tzinfo=UTC)),
        _match(3, datetime(2026, 10, 21, 15, 0, tzinfo=UTC)),
        _match(4, datetime(2026, 10, 23, 15, 0, tzinfo=UTC)),
    ], days_ahead=12)
    assert asked == [("2026-10-10", "2026-10-20"), ("2026-10-20", "2026-10-24")]
    assert ids == [1, 2, 3, 4]


def test_a_match_on_a_chunk_boundary_instant_is_kept_once(monkeypatch):
    """Guard: a kickoff at exactly the boundary's 00:00Z is in BOTH chunks
    (dateTo includes its own midnight, measured), and stays one fixture."""
    ids, _asked = _tier(monkeypatch, NOON, [
        _match(7, datetime(2026, 10, 20, 0, 0, tzinfo=UTC)),
    ], days_ahead=12)
    assert ids == [7]


def test_an_evening_eastern_match_still_being_played_is_found(monkeypatch):
    """WITNESS (#285, the #256 class): at 02:00Z on the 11th (22:00 Eastern
    on the 10th) a match that kicked off at 23:00Z on the 10th is in its
    second half. dateFrom=2026-10-11 could not return it; the window starts
    at the lookback day like every other upcoming sweep."""
    ids, asked = _tier(monkeypatch, EVENING, [
        _match(1, datetime(2026, 10, 10, 23, 0, tzinfo=UTC), status="IN_PLAY"),
        _match(2, datetime(2026, 10, 11, 1, 0, tzinfo=UTC), status="IN_PLAY"),
    ], days_ahead=2)
    assert asked[0][0] == "2026-10-10"
    assert ids == [1, 2]


@pytest.mark.parametrize("status", ["FINISHED", "CANCELLED", "POSTPONED", "AWARDED", "SUSPENDED"])
def test_a_match_that_is_over_is_not_upcoming(monkeypatch, status):
    """WITNESS: a match FINISHED (or CANCELLED: Rayo Vallecano v Athletic,
    2026-10-10, measured) earlier today came back in today's request and
    was published as an upcoming game. `admits` drops it, as it does for
    every ESPN sweep; a scheduled match and a live one stay."""
    ids, _asked = _tier(monkeypatch, NOON, [
        _match(1, datetime(2026, 10, 11, 12, 0, tzinfo=UTC), status=status),
        _match(2, datetime(2026, 10, 11, 14, 30, tzinfo=UTC), status="IN_PLAY"),
        _match(3, datetime(2026, 10, 11, 19, 0, tzinfo=UTC), status="TIMED"),
        _match(4, datetime(2026, 10, 12, 19, 0, tzinfo=UTC), status="SCHEDULED"),
    ], days_ahead=2)
    assert ids == [2, 3, 4]


def test_a_stale_scheduled_match_from_the_lookback_day_is_dropped(monkeypatch):
    """Guard for the lookback day: a match still tagged TIMED nine hours after
    kickoff (a feed that lags) is past `MAX_AGE_AFTER_KICKOFF`, not upcoming."""
    ids, _asked = _tier(monkeypatch, NOON, [
        _match(1, datetime(2026, 10, 11, 7, 0, tzinfo=UTC), status="TIMED"),
    ], days_ahead=2)
    assert ids == []


def test_every_request_stays_under_the_ten_day_cap(monkeypatch):
    """Guard: the measured 400 past a 10-day difference never fires, however
    long the lookahead."""
    for days_ahead in (0, 7, 8, 9, 12, 25):
        _ids, asked = _tier(monkeypatch, NOON, [], days_ahead=days_ahead)
        spans = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in asked]
        assert spans and max(spans) <= 10, (days_ahead, asked)
        assert asked[0][0] == "2026-10-10"
        assert asked[-1][1] == (date(2026, 10, 11) + timedelta(days=days_ahead + 1)).isoformat()
        assert all(asked[i][1] == asked[i + 1][0] for i in range(len(asked) - 1))


@pytest.mark.parametrize("body", [{}, {"errorCode": 429, "message": "quota"}])
def test_a_season_answer_without_a_matches_list_is_a_failure(monkeypatch, body):
    """WITNESS (#282's class, in the season fetch): a 200 without a
    `matches` list was cached as an empty season, so every game in the
    competition scored zero importance with nothing reported. It is now a
    failed attempt: not cached, counted, and reported once given up."""
    soccer = importlib.import_module(f"{PKG}.soccer")
    soccer._clear_fd_caches()
    monkeypatch.setattr(soccer.requests, "get", lambda *a, **k: _Resp(body))
    assert soccer._fetch_season_matches_for_code("k", "PL") == []
    assert "PL" not in soccer._SEASON_MATCHES_CACHE
    assert soccer._fetch_season_matches_for_code("k", "PL") == []
    assert "PL" in soccer.season_fetch_failures()


def test_a_previous_season_answer_without_a_matches_list_is_not_cached(monkeypatch):
    """WITNESS: the same for last season's list (head-to-head prose), which
    cached an error body as a season with no meetings for the whole refresh."""
    soccer = importlib.import_module(f"{PKG}.soccer")
    soccer._clear_fd_caches()
    monkeypatch.setattr(soccer.requests, "get", lambda *a, **k: _Resp({"message": "quota"}))
    assert soccer._fetch_prev_season_matches_for_code("k", "PL") == []
    assert "PL" not in soccer._PREV_SEASON_MATCHES_CACHE
