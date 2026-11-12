"""NCAA Men's Basketball source via CollegeBasketballData.com.

Same author and same Bearer-token auth as CollegeFootballData, but the
endpoints' wire shape differs in non-trivial ways (verified against the
live API on 2026-04-27):

  - /rankings is a FLAT list of (team, ranking, pollType, week) tuples,
    not nested under polls[].ranks[]. Field names: `team` not `school`,
    `ranking` not `rank`, `pollType` not `poll`. Filter by season.
  - /games is paginated at 3000 results without offset support. Use
    startDateRange / endDateRange query params instead: they accept any
    YYYY-MM-DD range and return only games in that window. Games have no
    `week` field. Excitement is `excitement` not `excitementIndex`.
  - /lines accepts the same date-range params and is keyed by `gameId`.

These shape differences make sharing code with NcaafSource brittle: each
source implements SportSource directly.

Free tier: ONE monthly pool of 1,000 calls shared with CFBD through the one key
(free key from collegebasketballdata.com). Every request goes through
`_college_data.get_json`, which keeps answers on disk and owns that budget (#337).
Season runs roughly November → early April (regular season ends, then
March Madness). Calls in May-October return [] cleanly.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ._college_data import CollegeQuotaUnavailable, POLL_STANDS_IN, get_answer, get_json, line_objects, require_rows
from .base import GameRow, RankingsUnavailable, SeasonUnavailable, report_lost_day_range, warn_quota
from .points_based import PointsBasedSportSource
from ._espn import upcoming_window
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaam")

CBB_BASE = "https://api.collegebasketballdata.com"


def _in_population(home: Optional[str], away: Optional[str], ranked: Dict[str, int]) -> bool:
    """Whether a game between `home` and `away` belongs to the simulated
    population: one of the two is in the poll. The ONE predicate for both
    `_fetch_full_season_games` (which games are simulated) and the first
    test in `NcaamSource.importance_covers` (which games are worth
    simulating): if they drift, a game the scorer skips would be one the simulation could
    answer, or the per-game fallback comes back for games it cannot."""
    return home in ranked or away in ranked


def _date_range(start: Any, end: Any) -> Dict[str, str]:
    """The regular-season date-range parameters /games and /lines share."""
    return {"startDateRange": start.strftime("%Y-%m-%d"),
            "endDateRange": end.strftime("%Y-%m-%d"), "seasonType": "regular"}


def _completed(game: Dict[str, Any]) -> bool:
    """Whether a CBBD /games row is a finished game. CBBD encodes completion
    via the homeWinner/awayWinner booleans being non-null AND the *Points
    fields being numeric; `status` doesn't always reflect completion, so the
    joint check. A game in progress has points and no winner yet."""
    return (game.get("homePoints") is not None and game.get("awayPoints") is not None
            and (game.get("homeWinner") is not None
                 or game.get("awayWinner") is not None))


class NcaamSource(PointsBasedSportSource):
    league_context_code = "CBB"

    # The AP Top 25 is drawn from Division I (#198): 365 programmes for the
    # 2025-26 season, measured 2026-10-11 from CBBD `/teams?season=2026`.
    # See SportSource.rank_field_size.
    rank_field_size = 365

    # NCAA men's basketball averages ~75 points/game per team. Cold-start
    # fallback for teams without any FINISHED games yet (early November).
    _DEFAULT_POINTS_FOR = 75.0
    _DEFAULT_POINTS_AGAINST = 75.0

    @property
    def sport_prefix(self) -> str:
        return "CBB"

    @property
    def sport_label(self) -> str:
        return "NCAA Men's Basketball"

    def __init__(self, api_key: str, poll_name: str = "AP Top 25"):
        super().__init__()
        self.api_key = api_key
        self.poll_name = poll_name
        # (season, poll or None) once read, or (season, RankingsUnavailable).
        # See _poll.
        self._poll_memo: Optional[tuple] = None
        # {team: most wins reachable in the simulated season}. See _win_ceiling.
        self._win_ceiling_memo: Optional[Dict[str, int]] = None
        # When the season chunks this read used were fetched (`_season_read_at`).
        self._season_answers_at: Optional[datetime] = None

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            logger.warning("[ncaam] no CBB-Data API key configured; returning []")
            return []

        season_year = self._season()
        # Raises RankingsUnavailable when the request FAILS: that is an outage
        # the refresh holds the previous games for (#240), where None, no poll
        # published yet, is a real preseason and stays quiet. DO NOT catch it
        # back into None: it was logged as "offseason" and the slate zeroed
        # silently.
        rank_by_team = self._poll(season_year)
        if rank_by_team is None:
            logger.info("[ncaam] no %s poll published for %d yet (preseason / "
                        "offseason); skipping", self.poll_name, season_year)
            return []

        # The window starts at the US-Eastern lookback day, not today-UTC
        # (#256): from 00:00Z an evening-Eastern tip-off sits on yesterday's
        # UTC date, and `startDateRange=<today-UTC>` cannot return it while it
        # is being played. `admits` then drops the finished and the stale.
        window = upcoming_window(days_ahead)
        games = self._fetch_games(window.first_day, window.last_day)
        if games is None:
            # The one request for every day of the window failed: those days
            # are lost, not quiet (#261, see base.report_lost_day_range).
            report_lost_day_range(window.first_day, window.last_day)
            return []
        games = [g for g in games
                 if window.admits(parse_iso_utc(g.get("startDate")), _completed(g))]
        if not games:
            logger.info("[ncaam] no upcoming games in next %d days", days_ahead)
            return []

        spread_by_id = self._fetch_spreads(window.first_day, window.last_day)

        rows: List[GameRow] = []
        for g in games:
            home = g.get("homeTeam")
            away = g.get("awayTeam")
            if not home or not away:
                continue
            start = parse_iso_utc(g.get("startDate"))
            cbb_id = g.get("id")
            spread = spread_by_id.get(cbb_id) if cbb_id is not None else None
            rows.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=home,
                away=away,
                rank_home=rank_by_team.get(home),
                rank_away=rank_by_team.get(away),
                start_time=start,
                venue=g.get("venue"),
                spread=spread,
                extra={
                    "cbb_id": g.get("id"),
                    "season": g.get("season"),
                    "neutral": g.get("neutralSite", False),
                    "conference_game": g.get("conferenceGame", False),
                    "excitement_index": g.get("excitement"),
                    "tournament": g.get("tournament"),
                    # Importance lookup key for LEAGUE_CONTEXTS.
                    "fd_competition_code": self.league_context_code,
                },
            ))
        return rows

    @staticmethod
    def _current_season_year() -> int:
        # CBB's ?season= is the END year of the season (?season=2025 means
        # 2024-25). Pivot at November (when the season opens). In May-Oct we
        # return last season's end-year: /games for that season is in the
        # past so fetch_upcoming filters everything out and returns [].
        now = datetime.now(timezone.utc)
        return now.year + 1 if now.month >= 11 else now.year

    def _season(self) -> int:
        """`_current_season_year()`, read ONCE per source (one refresh). The
        upcoming fetch, the season read and the key its complete read is
        kept under (`points_based.season_store_key`) must name the same
        season; three clock reads could straddle the season boundary and
        file one season's games under the other's key."""
        if getattr(self, "_season_memo", None) is None:
            self._season_memo = self._current_season_year()
        return self._season_memo

    def _season_store_id(self) -> Optional[str]:
        """The season AND the poll's teams: the stored population is the games
        of the teams the poll ranks (`_fetch_full_season_games`), so a read
        filtered by last week's poll must not stand in for this week's,
        where a newly ranked team's games would be missing and score no
        stakes (Codex, #289 follow-up round 3). None when the poll did not
        answer: then the season cannot be read either, and nothing is kept
        or recalled."""
        try:
            ranked = self._poll(self._season())
        except RankingsUnavailable:
            return None
        teams = ",".join(sorted(ranked or ()))
        digest = hashlib.sha256(teams.encode("utf-8")).hexdigest()[:12]
        return f"{self._season()}|{self.poll_name}|{digest}"

    def _poll(self, season: int) -> Optional[Dict[str, int]]:
        """`_fetch_rankings(season)`, read ONCE per source (#283).

        `fetch_upcoming` ranks its rows with it and `_fetch_full_season_games`
        picks the simulated population from it, so the two must agree: a poll
        that answered for one read and failed for the other ranked the slate
        against one poll and simulated a population of NONE (zero importance
        for every game). One read, kept on the instance, and a failure kept
        too: the season walk re-raises it rather than asking again. In a
        refresh the walk never reaches a failed poll anyway, because the
        source raised from `fetch_upcoming` and none of its games is scored.
        """
        if self._poll_memo is None or self._poll_memo[0] != season:
            try:
                self._poll_memo = (season, self._fetch_rankings(season))
            except RankingsUnavailable as e:
                self._poll_memo = (season, e)
        result = self._poll_memo[1]
        if isinstance(result, RankingsUnavailable):
            raise result
        return result

    def _fetch_rankings(self, season: int) -> Optional[Dict[str, int]]:
        """Return {team_name → rank} for the latest week of the configured
        poll. None if no poll has been published yet (preseason, before
        mid-October's preseason AP poll). RAISES `RankingsUnavailable` for a
        failed request or an answer that is not a list of poll rows, which is
        not a preseason (#283). Read through `_poll`."""
        try:
            # The parser is the check: an answer it cannot read is a failure
            # and is never kept in place of a good poll (#337).
            data = get_json(
                CBB_BASE, "/rankings", {"season": season}, self.api_key, timeout=15,
                log_tag="ncaam", stale_for=POLL_STANDS_IN, check=self._poll_from,
            )
        except CollegeQuotaUnavailable:
            raise
        except Exception as e:
            logger.error("[ncaam] /rankings failed: %s", e)
            raise RankingsUnavailable(f"/rankings failed: {e}") from e
        return self._poll_from(data)

    def _poll_from(self, data: Any) -> Optional[Dict[str, int]]:
        """{team: rank} from a /rankings answer, or None for no poll yet.
        RAISES `RankingsUnavailable` for an answer it cannot read."""
        # /rankings is a flat JSON list of poll rows, [] before the first
        # poll; anything else (an error object, null, rows without a
        # pollType) is a failure, not a preseason.
        if not isinstance(data, list) or not all(isinstance(e, dict) and "pollType" in e for e in data):
            raise RankingsUnavailable(f"/rankings answered {str(data)[:80]!r}, not a list of poll rows")
        if not data:
            return None
        # Flat list: filter by pollType, then take the latest week.
        relevant = [r for r in data if r.get("pollType") == self.poll_name]
        if not relevant:
            return None
        latest_week = max((r.get("week") or 0) for r in relevant)
        latest = [r for r in relevant if (r.get("week") or 0) == latest_week]
        out: Dict[str, int] = {}
        for entry in latest:
            team = entry.get("team")
            rank = entry.get("ranking")
            try:
                if team and rank is not None:
                    out[team] = int(rank)
            except (TypeError, ValueError):
                continue
        if not out:
            # The poll's rows are listed but none reads as a team and rank: a
            # changed row shape, not a preseason (#283).
            raise RankingsUnavailable(f"/rankings: {self.poll_name} week {latest_week} has no readable rank")
        return out

    def _fetch_games(self, start: date, end: date) -> Optional[List[Dict]]:
        """Use startDateRange/endDateRange: /games is hard-capped at 3000
        without offset support, so a season-wide call would silently miss
        late-season games.

        None when the request FAILED, distinct from [] (it answered with no
        games): the caller reports the window's days as lost on None.
        """
        try:
            # /games answers with a JSON list, empty included; anything else
            # (an error object, null) is not an answer.
            return get_json(
                CBB_BASE, "/games", _date_range(start, end), self.api_key, timeout=30,
                log_tag="ncaam", check=require_rows,
            )
        except CollegeQuotaUnavailable:
            raise
        except Exception as e:
            logger.error("[ncaam] /games failed: %s", e)
            return None

    def _fetch_spreads(self, start: date, end: date) -> Dict[int, float]:
        """Same date-range as /games. Returns {gameId: abs(spread)}."""
        out: Dict[int, float] = {}
        try:
            data = get_json(
                CBB_BASE, "/lines", _date_range(start, end), self.api_key, timeout=30,
                log_tag="ncaam", check=require_rows,
            )
        except CollegeQuotaUnavailable as e:
            warn_quota(logger, "[ncaam] /lines failed: %s", e)
            return {}
        except Exception as e:
            logger.warning("[ncaam] /lines failed: %s", e)
            return {}
        for entry in data:
            gid = entry.get("gameId")
            if gid is None:
                continue
            line_list = line_objects(entry)
            # CBB returns per-bookmaker lines without a "consensus" entry:
            # take the first non-null spread we see (mirrors what the user
            # would see at the top of any sportsbook).
            for line in line_list:
                spread = line.get("spread")
                if spread is None:
                    continue
                try:
                    out[gid] = abs(float(spread))
                    break
                except (TypeError, ValueError):
                    continue
        return out

    # ---------- Monte Carlo importance ----------

    def importance_covers(self, game: GameRow) -> bool:
        """Whether simulating `game` can produce a nonzero importance.
        Only games involving a poll team are in the simulated season
        (`_fetch_full_season_games`). For any other game the simulator
        applies the sampled result, plays out the population's remaining
        games and buckets each team by `wins >= band`: a team that cannot
        reach the lowest CBB band even by winning this game and every
        population game it has left lands in no band under either result,
        so Kendall tau-c is exactly 0 (simulation.monte_carlo_importance).
        Skipping such a game changes no score; it only saves the ~4.4s
        simulation. Measured 2026-11-04 on the live CBBD API: 116 of 126
        slate games were outside the population and every one scored 0.0000
        in the per-game fallback, which pushed every refresh past its
        timeout. See SportSource.importance_covers.

        DO NOT reduce this to `_in_population` alone: an unranked team with
        enough wins over poll teams can still reach a band from a game
        against another unranked team, and that game's importance is real
        (second-opinion counterexample, 2026-11-04: 14 population wins
        scored 1.356).

        A failed poll or season fetch answers True: the importance call
        then raises and logs the outage as before (#283), rather than every
        game silently scoring zero here."""
        from ..scoring import LEAGUE_CONTEXTS
        try:
            ranked = self._poll(self._season())
        except RankingsUnavailable:
            return True
        if _in_population(game.home, game.away, ranked or {}):
            return True
        try:
            ceiling = self._win_ceiling()
        except Exception:  # noqa: BLE001 - the simulation raises and reports it
            return True
        bands = [t[0] for t in LEAGUE_CONTEXTS[self.league_context_code].thresholds]
        if not bands:
            return False
        # +1: this game itself, which is not in the population.
        return any(ceiling.get(team, 0) + 1 >= min(bands)
                   for team in (game.home, game.away))

    def _win_ceiling(self) -> Dict[str, int]:
        """Most wins each team can finish the simulated season with: its
        FINISHED population wins plus every population game it has left.
        Read from the same initial_state/remaining_matches the simulator
        drains, and cached per refresh alongside them."""
        if self._win_ceiling_memo is None:
            state = self.initial_state()
            ceiling = {team: int(row.get(self._count_field, 0))
                       for team, row in (state.get("_teams") or {}).items()}
            for m in self.remaining_matches(state):
                for team in (m.home, m.away):
                    ceiling[team] = ceiling.get(team, 0) + 1
            self._win_ceiling_memo = ceiling
        return self._win_ceiling_memo

    def _season_read_at(self) -> Optional[datetime]:
        """The season chunks are kept up to twelve hours (#337): the season
        store dates this read by the oldest, not by now (`points_based`)."""
        return self._season_answers_at

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Return the current regular-season game list filtered to games
        involving an AP-ranked team. The full D1 schedule is ~7500 games
       : too many for the Monte Carlo simulator to iterate every refresh
        at acceptable latency. Filtering to AP-relevant games drops it to
        ~700-1000 (each of ~25 AP teams plays ~30 games, with overlap),
        which keeps per-refresh importance cost in the seconds, not
        minutes.

        Known limitation: a non-AP favorite team's games against non-AP
        opponents aren't in the cache, so the simulator misses those
        wins when computing the favorite's win-count outcomes. The
        plugin's `favorites_in_league` mechanism still queries those
        teams but the leverage is biased low.
        """
        if not self.api_key:
            return []
        season = self._season()
        # The poll `fetch_upcoming` ranked the slate with (`_poll`); a failed
        # one raises here too, so the importance call fails (and is logged)
        # instead of simulating an empty population as zero importance (#283).
        ranked = self._poll(season)
        if not ranked:
            # No AP poll yet (preseason). Importance signal returns 0
            # via terminal_outcomes' empty dict: graceful no-op.
            return []

        # Season window: Nov 1 of (season-1) through Apr 30 of season.
        # Three date-range chunks to stay under CBBD's 3000-row cap per call.
        chunks: List[tuple] = [
            (datetime(season - 1, 11, 1, tzinfo=timezone.utc),
             datetime(season - 1, 12, 31, tzinfo=timezone.utc)),
            (datetime(season, 1, 1, tzinfo=timezone.utc),
             datetime(season, 2, 15, tzinfo=timezone.utc)),
            (datetime(season, 2, 16, tzinfo=timezone.utc),
             datetime(season, 4, 30, tzinfo=timezone.utc)),
        ]
        all_games: List[Dict[str, Any]] = []
        fetched: List[datetime] = []
        for start, end in chunks:
            try:
                answer = get_answer(
                    CBB_BASE, "/games", _date_range(start, end), self.api_key, timeout=30,
                    log_tag="ncaam", check=require_rows,
                )
            except CollegeQuotaUnavailable:
                raise
            except Exception as e:
                logger.warning("[ncaam] full-season chunk %s..%s failed: %s",
                               start.date(), end.date(), e)
                # A third of the season missing is not a shorter season
                # (#289): skipped, its results and fixtures fell out of
                # every simulation with nothing reported.
                raise SeasonUnavailable(
                    f"full-season chunk {start.date()}..{end.date()} failed: {e}") from e
            all_games.extend(answer.body)
            fetched.append(answer.fetched_at)
        # The oldest chunk dates the read: each is kept up to twelve hours (#337).
        self._season_answers_at = min(fetched)

        out: List[Dict[str, Any]] = []
        for g in all_games:
            home = g.get("homeTeam")
            away = g.get("awayTeam")
            if not home or not away:
                continue
            if not _in_population(home, away, ranked):
                continue
            hp = g.get("homePoints")
            ap_pts = g.get("awayPoints")
            completed = _completed(g)
            out.append({
                "id": g.get("id"),
                "home": home,
                "away": away,
                "home_points": hp if completed else None,
                "away_points": ap_pts if completed else None,
                "status": "FINISHED" if completed else "SCHEDULED",
                "start_time": parse_iso_utc(g.get("startDate")),
            })
        return out
