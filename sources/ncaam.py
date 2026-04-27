"""NCAA Men's Basketball source via CollegeBasketballData.com.

CollegeBasketballData mirrors the CollegeFootballData API surface (same
author, same Bearer-token auth, same /rankings + /games + /lines
endpoints, same camelCase JSON), so this class only sets the per-sport
constants.

Same Bearer token works for both APIs — users who already have a CFBD
key can flip on NCAAM without paying for anything new.

Season runs roughly November → early April (regular season ends, then
March Madness). We pivot at November so calls in summer return [] cleanly.
"""

from __future__ import annotations

from ._collegedata import CollegeDataSource


class NcaamSource(CollegeDataSource):
    api_base = "https://api.collegebasketballdata.com"
    sport_prefix = "CBB"
    sport_label = "NCAA Men's Basketball"
    season_pivot_month = 11  # NCAAM season opens in early November
