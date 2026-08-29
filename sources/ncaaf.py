"""NCAA Football source via CollegeFootballData.com.

Free tier: 1k req/day. We make 3 calls per refresh:
  1) /rankings: current AP Top-25 (nested polls→ranks shape)
  2) /games  : schedule for the upcoming window (param: ?year=)
  3) /lines  : betting lines per week

Offseason (Feb-Aug) /games returns no upcoming results; fetch_upcoming
returns []. CFBD identifies a season by its START year (?year=2024 means
the 2024-25 NCAAF season).

DIVISION FILTERING IS NOT COSMETIC, IT IS A RUNTIME BUDGET CONTROL.
CFBD's /games carries every NCAA division: measured 2026-08-29, the
opening week returned 157 games in a 7-day window of which only 27
involved an FBS team (45 were FCS-only and 85 were Division II/III).
Every emitted row costs ~7s of Monte Carlo importance simulation in
plugin._action_refresh, so passing all 157 through spends ~18 minutes
against the pipeline subprocess's 1500s budget, which is what made
auto_pipeline time out on every run once the season opened. A D-II
game cannot even score: the season replay population is FBS (or
FBS+FCS) only, so a Division II team is absent from it and its
importance is always 0.00. Emitting those rows buys nothing and costs
the entire refresh.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow
from .points_based import PointsBasedSportSource
from .._util import parse_iso_utc

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaaf")

CFBD_BASE = "https://api.collegefootballdata.com"


# Classification values CFBD puts on homeClassification / awayClassification.
# "fbs" = Division I FBS (the 136-team top tier), "fcs" = Division I FCS,
# "ii" / "iii" = Division II / III. A game is kept when EITHER side is in
# the selected set, so an FBS-vs-FCS cupcake still counts for the FBS team.
DIVISION_SETS: Dict[str, frozenset] = {
    "fbs": frozenset({"fbs"}),
    "fbs_fcs": frozenset({"fbs", "fcs"}),
}
DEFAULT_DIVISIONS = "fbs"


class NcaafSource(PointsBasedSportSource):
    league_context_code = "CFB"

    # CFB per-team average scoring is ~28 points/game. Cold-start fallback
    # for teams with no FINISHED games in the current season (weeks 1-2,
    # newly-promoted FCS-to-FBS programs).
    _DEFAULT_POINTS_FOR = 28.0
    _DEFAULT_POINTS_AGAINST = 28.0

    @property
    def sport_prefix(self) -> str:
        return "CFB"

    @property
    def sport_label(self) -> str:
        return "NCAA Football"

    def __init__(
        self,
        api_key: str,
        poll_name: str = "AP Top 25",
        divisions: Optional[str] = DEFAULT_DIVISIONS,
    ):
        super().__init__()
        self.api_key = api_key
        self.poll_name = poll_name
        # Unset (None, the caller's "no saved setting") and unrecognised
        # values both fall back to the FBS-only default rather than
        # raising: a stale saved setting must not take the whole source
        # down, and FBS-only is the cheap, safe end of the range. This is
        # the ONLY place the default is spelled; callers pass through.
        if divisions is not None and divisions not in DIVISION_SETS:
            # Silently normalising a typo, a renamed manifest value, or a
            # failed settings migration would make a real misconfiguration
            # indistinguishable from the default. Say so, then degrade.
            logger.warning(
                "[ncaaf] unrecognised divisions value %r; falling back to %r "
                "(valid: %s)", divisions, DEFAULT_DIVISIONS,
                ", ".join(sorted(DIVISION_SETS)),
            )
        self.divisions = divisions if divisions in DIVISION_SETS else DEFAULT_DIVISIONS
        self._raw_season_cache: Dict[int, List[Dict[str, Any]]] = {}
        self._headers = {"Authorization": f"Bearer {api_key}"}

    @property
    def _allowed_classifications(self) -> frozenset:
        return DIVISION_SETS[self.divisions]

    def _in_selected_divisions(self, game: Dict[str, Any]) -> bool:
        """True when EITHER side is in the selected division set.

        CFBD omits the classification key on a handful of rows (a
        non-NCAA exhibition opponent). Treat a missing value as not
        selected: guessing it in would reintroduce exactly the untiered
        noise this filter exists to remove.
        """
        allowed = self._allowed_classifications
        return (
            game.get("homeClassification") in allowed
            or game.get("awayClassification") in allowed
        )

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        if not self.api_key:
            logger.warning("[ncaaf] no CFBD API key configured; returning []")
            return []

        season_year = self._current_season_year()
        rank_by_team = self._fetch_rankings(season_year)
        if rank_by_team is None:
            # _fetch_rankings already logged the specific cause at the
            # right level (ERROR for a failed request, INFO for a poll
            # that simply is not published yet). DO NOT re-log this as
            # "offseason": a transient CFBD failure in September zeroes
            # the whole slate, and calling that an offseason sent a real
            # 2026-08-28 outage to the wrong diagnosis for a full day.
            return []

        games = self._fetch_games(season_year, days_ahead)
        if not games:
            logger.info("[ncaaf] no upcoming games in next %d days", days_ahead)
            return []

        spread_by_id = self._fetch_spreads(season_year, games)

        # Ranks come from ONE poll (AP Top 25, an FBS poll), so under the
        # "fbs_fcs" setting FCS teams stay unranked and their games enter as
        # low-scored filler, which is the intended behaviour.
        # DO NOT merge CFBD's "FCS Coaches Poll" into the same rank_home /
        # rank_away space to "fix" that. scoring.score_game reads a rank as a
        # position in one global ordering: an FCS #1 vs #2 would land in the
        # both-ranked branch worth up to 10 points and outrank a genuine
        # top-5 FBS matchup. Two polls means two incomparable scales.
        rows: List[GameRow] = []
        for g in games:
            home = g.get("homeTeam") or g.get("home_team")
            away = g.get("awayTeam") or g.get("away_team")
            if not home or not away:
                continue
            start = parse_iso_utc(g.get("startDate"))
            if start is None:
                continue
            cfbd_id = g.get("id")
            spread = spread_by_id.get(cfbd_id) if cfbd_id is not None else None
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
                    "cfbd_id": g.get("id"),
                    "week": g.get("week"),
                    "season": g.get("season"),
                    "neutral": g.get("neutralSite", False),
                    "conference_game": g.get("conferenceGame", False),
                    "excitement_index": g.get("excitementIndex"),
                    # Importance signal lookup key. The plugin's
                    # compute_match_importance reads this to find the
                    # LEAGUE_CONTEXTS entry that carries the win-count
                    # thresholds and consequence weights.
                    "fd_competition_code": self.league_context_code,
                },
            ))
        return rows

    @staticmethod
    def _current_season_year() -> int:
        # NCAAF season runs Aug-Jan; CFBD's ?year= is the START year.
        # Before Aug we're in the tail end of the prior year's season.
        now = datetime.now(timezone.utc)
        return now.year if now.month >= 8 else now.year - 1

    def _fetch_rankings(self, year: int) -> Optional[Dict[str, int]]:
        """Return {team_name → rank} for the latest snapshot, or None if no
        poll has been published yet (preseason)."""
        try:
            r = requests.get(
                f"{CFBD_BASE}/rankings",
                headers=self._headers,
                params={"year": year},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error(
                "[ncaaf] /rankings request failed (%s); NCAAF contributes "
                "0 games to this refresh", e,
            )
            return None
        if not data:
            logger.info(
                "[ncaaf] no %s snapshot published for %d yet (preseason); "
                "skipping", self.poll_name, year,
            )
            return None
        latest = data[-1]
        for p in latest.get("polls", []):
            if p.get("poll") == self.poll_name:
                return {r["school"]: r["rank"] for r in p.get("ranks", [])}
        logger.info(
            "[ncaaf] poll %r absent from the latest snapshot (polls present: "
            "%s); skipping", self.poll_name,
            ", ".join(str(p.get("poll")) for p in latest.get("polls", [])) or "none",
        )
        return None

    def _fetch_raw_season(self, year: int) -> List[Dict]:
        """One CFBD /games call per season year, cached for this instance.

        BOTH the upcoming-window path and the Monte Carlo season population
        read from here, and that is what makes "the simulated universe
        matches the emitted universe" a structural guarantee rather than a
        promise two call sites have to keep. Two separate requests could
        disagree (a classification edited between them, a year boundary
        crossed mid-refresh), and the disagreement would be invisible.
        It also halves the request cost against a 1k/day free tier.
        """
        cached = self._raw_season_cache.get(year)
        if cached is not None:
            return cached
        try:
            r = requests.get(
                f"{CFBD_BASE}/games",
                headers=self._headers,
                params={"year": year, "seasonType": "regular"},
                timeout=60,
            )
            r.raise_for_status()
            data = r.json() or []
        except Exception as e:
            logger.error("[ncaaf] /games failed: %s", e)
            # DO NOT cache a failure: a transient error must not freeze an
            # empty season for the life of the source instance.
            return []
        self._raw_season_cache[year] = data
        return data

    def _fetch_games(self, year: int, days_ahead: int) -> List[Dict]:
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)
        data = self._fetch_raw_season(year)

        upcoming: List[Dict] = []
        skipped_division = 0
        unclassified = 0
        for g in data:
            start = parse_iso_utc(g.get("startDate"))
            if start is None:
                continue
            if not (now <= start <= cutoff):
                continue
            # DO NOT drop this filter to "show more football". Every row
            # returned here costs ~7s of Monte Carlo in the refresh loop,
            # and an out-of-division team is absent from the season
            # replay so it can only ever score 0.00. See module docstring.
            if not self._in_selected_divisions(g):
                skipped_division += 1
                if (g.get("homeClassification") is None
                        and g.get("awayClassification") is None):
                    unclassified += 1
                continue
            upcoming.append(g)
        if skipped_division:
            logger.info(
                "[ncaaf] division filter (%s): kept %d, dropped %d "
                "out-of-division games in the %d-day window",
                self.divisions, len(upcoming), skipped_division, days_ahead,
            )
        if unclassified:
            # Dropping an unclassified row is the safe default, but if CFBD
            # ever stops populating the field for real NCAA teams this is
            # silent data loss that looks exactly like a quiet week. Make it
            # visible rather than inferring intent from a missing key.
            logger.warning(
                "[ncaaf] %d game(s) in the window carried NO division on "
                "either side and were dropped; if this number is large, "
                "CFBD's classification field may have changed shape",
                unclassified,
            )
        return upcoming

    def _fetch_spreads(self, year: int, games: List[Dict]) -> Dict[int, float]:
        if not games:
            return {}
        weeks = {g.get("week") for g in games if g.get("week") is not None}
        out: Dict[int, float] = {}
        for week in weeks:
            try:
                r = requests.get(
                    f"{CFBD_BASE}/lines",
                    headers=self._headers,
                    params={"year": year, "week": week, "seasonType": "regular"},
                    timeout=15,
                )
                r.raise_for_status()
                lines = r.json()
            except Exception as e:
                logger.warning("[ncaaf] /lines week %s failed: %s", week, e)
                continue
            for entry in lines:
                gid = entry.get("id")
                line_list = entry.get("lines", [])
                consensus = next((l for l in line_list if l.get("provider") == "consensus"), None)
                line = consensus or (line_list[0] if line_list else None)
                if line is None:
                    continue
                spread = line.get("spread")
                if spread is None:
                    continue
                try:
                    out[gid] = abs(float(spread))
                except (TypeError, ValueError):
                    continue
        return out

    # ---------- Monte Carlo importance ----------

    def outcome_eligible_teams(self) -> Optional[frozenset]:
        """Only teams whose OWN classification is in the selected set may be
        assigned CFB win-count bands.

        The game filter is an either-side rule, because an FBS team's win
        over an FCS opponent counts toward its bowl eligibility and the game
        must stay in the population. That drags the FCS opponent into the
        simulated state as a side effect, on a schedule consisting of just
        that one game. Measured 2026-08-29 on the live 2026 season: 100 of
        the 238 teams in the FBS-only population had 4 or fewer games, most
        exactly 1. Bucketing those against "6+ wins = bowl eligible" is
        meaningless, and the degenerate contingency table it produces reads
        as zero leverage, which is the right number arrived at by accident.

        Restricting outcomes here (rather than tightening the game filter)
        keeps both facts true at once: the FBS team's record still includes
        the cupcake win, and nobody is measured against a league they are
        not in.
        """
        raw = self._fetch_raw_season(self._current_season_year())
        allowed = self._allowed_classifications
        eligible = set()
        for g in raw:
            home = g.get("homeTeam") or g.get("home_team")
            away = g.get("awayTeam") or g.get("away_team")
            if home and g.get("homeClassification") in allowed:
                eligible.add(home)
            if away and g.get("awayClassification") in allowed:
                eligible.add(away)
        return frozenset(eligible)

    def _fetch_full_season_games(self) -> List[Dict[str, Any]]:
        """Return the current season's regular-season games for the selected
        divisions as match dicts for the points-based simulator. One CFBD
        /games call covers the whole season; cached per source instance
        in the base class.

        THE SIMULATED UNIVERSE MUST MATCH THE EMITTED UNIVERSE. This uses
        the SAME division filter as _fetch_games, and that is load-bearing
        rather than tidiness: a team absent from this population has no
        estimated strength and no win-count band, so every game it plays
        scores exactly 0.00 importance while still paying the full ~7s
        simulation cost. Emitting a division here that is filtered out of
        fetch_upcoming (or the reverse) silently reintroduces that waste.

        Measured 2026-08-29 against the live 2026 season: CFBD returns
        3676 rows, of which 888 involve an FBS team and 1608 involve FBS
        or FCS. FBS-vs-FCS games are kept under both settings because
        they count toward the FBS team's win total.
        """
        if not self.api_key:
            return []
        raw = self._fetch_raw_season(self._current_season_year())
        out: List[Dict[str, Any]] = []
        for g in raw:
            if not self._in_selected_divisions(g):
                continue
            home = g.get("homeTeam") or g.get("home_team")
            away = g.get("awayTeam") or g.get("away_team")
            if not home or not away:
                continue
            hp = g.get("homePoints")
            ap = g.get("awayPoints")
            completed = bool(g.get("completed"))
            out.append({
                "id": g.get("id"),
                "home": home,
                "away": away,
                "home_points": hp if completed else None,
                "away_points": ap if completed else None,
                "status": "FINISHED" if completed else "SCHEDULED",
                "start_time": parse_iso_utc(g.get("startDate")),
            })
        return out
