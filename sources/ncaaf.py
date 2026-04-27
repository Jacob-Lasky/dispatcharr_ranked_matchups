"""NCAA Football source via CollegeFootballData.com."""

from __future__ import annotations

from ._collegedata import CollegeDataSource


class NcaafSource(CollegeDataSource):
    """AP Top-25 + games + betting lines from CollegeFootballData.com.

    Free tier: 1k req/day. Offseason (Feb-Aug) /games returns nothing.
    """
    api_base = "https://api.collegefootballdata.com"
    sport_prefix = "CFB"
    sport_label = "NCAA Football"
    season_pivot_month = 8  # NCAAF season runs Aug → Jan
