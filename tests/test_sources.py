"""Sanity tests for the sport adapters. Network isn't called — we just check
class-level constants are wired up correctly so a typo in the sport_prefix
or api_base doesn't ship silently."""

from dispatcharr_ranked_matchups.sources import (
    NcaafSource,
    NcaamSource,
    SoccerSource,
    SOCCER_COMPETITIONS,
)
from dispatcharr_ranked_matchups.sources._collegedata import CollegeDataSource
from dispatcharr_ranked_matchups.sources.base import SportSource


class TestCollegeDataInheritance:
    def test_ncaaf_inherits_collegedata(self):
        assert issubclass(NcaafSource, CollegeDataSource)
        assert issubclass(NcaafSource, SportSource)

    def test_ncaam_inherits_collegedata(self):
        assert issubclass(NcaamSource, CollegeDataSource)
        assert issubclass(NcaamSource, SportSource)


class TestNcaafConstants:
    def test_constants(self):
        assert NcaafSource.sport_prefix == "CFB"
        assert NcaafSource.sport_label == "NCAA Football"
        assert NcaafSource.api_base == "https://api.collegefootballdata.com"
        assert NcaafSource.season_pivot_month == 8


class TestNcaamConstants:
    def test_constants(self):
        assert NcaamSource.sport_prefix == "CBB"
        assert NcaamSource.sport_label == "NCAA Men's Basketball"
        assert NcaamSource.api_base == "https://api.collegebasketballdata.com"
        assert NcaamSource.season_pivot_month == 11


class TestCollegeDataInstantiation:
    def test_init_without_key(self):
        # Empty key is allowed at construction; fetch_upcoming guards.
        s = NcaamSource(api_key="")
        assert s.poll_name == "AP Top 25"

    def test_no_key_returns_empty(self):
        s = NcaamSource(api_key="")
        assert s.fetch_upcoming() == []

    def test_custom_poll_name(self):
        s = NcaafSource(api_key="abc", poll_name="Coaches Poll")
        assert s.poll_name == "Coaches Poll"

    def test_subclass_without_api_base_rejects(self):
        # Catch the "forgot to set api_base" mistake at construction time
        # rather than 400ing inside fetch_upcoming.
        class Broken(CollegeDataSource):
            pass
        try:
            Broken(api_key="x")
        except NotImplementedError:
            pass
        else:
            raise AssertionError("expected NotImplementedError")


class TestSoccerCompetitions:
    def test_known_keys(self):
        for k in ("epl", "championship", "ucl"):
            assert k in SOCCER_COMPETITIONS

    def test_epl_config(self):
        cfg = SOCCER_COMPETITIONS["epl"]
        assert cfg.fd_code == "PL"
        assert cfg.sport_prefix == "EPL"
        assert cfg.use_position_as_rank is True
        assert cfg.rank_cap == 20

    def test_ucl_does_not_use_position_as_rank(self):
        # Knockout standings don't map to a single position.
        assert SOCCER_COMPETITIONS["ucl"].use_position_as_rank is False

    def test_unknown_competition_rejected(self):
        try:
            SoccerSource("bundesliga", fd_api_key="x")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")


class TestCollegeDataSeasonPivot:
    """Pivot logic: if current month < pivot, we're in the prior year's season."""

    def test_basketball_in_january_uses_prior_year(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import _collegedata as cd
        from datetime import datetime as real_dt, timezone as real_tz

        class FakeDateTime:
            @staticmethod
            def now(tz=None):
                return real_dt(2027, 1, 15, tzinfo=real_tz.utc)

        monkeypatch.setattr(cd, "datetime", FakeDateTime)
        s = NcaamSource(api_key="x")
        # Jan 2027 with pivot=11 → pivot not yet reached this calendar year, so
        # we're in the 2026-27 season → "year=2026".
        assert s._current_season_year() == 2026

    def test_basketball_in_november_uses_current_year(self, monkeypatch):
        from dispatcharr_ranked_matchups.sources import _collegedata as cd
        from datetime import datetime as real_dt, timezone as real_tz

        class FakeDateTime:
            @staticmethod
            def now(tz=None):
                return real_dt(2026, 11, 5, tzinfo=real_tz.utc)

        monkeypatch.setattr(cd, "datetime", FakeDateTime)
        s = NcaamSource(api_key="x")
        # Nov 5 2026, pivot=11 → at/after pivot → 2026-27 season → "year=2026"
        assert s._current_season_year() == 2026
