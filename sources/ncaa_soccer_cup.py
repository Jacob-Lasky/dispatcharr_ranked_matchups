"""NCAA College Cup source — single-game-elim bracket via ESPN.

Issue #24: NCAA Men's + Women's D1 Soccer Tournament. Both genders use
the same structural shape (single-leg elimination across multiple
rounds); only ESPN's URL slug, the LEAGUE_CONTEXTS code, and the
gender-specific round labels differ. One source class parametrized on
gender, mirroring NcaaSoccerSource (regular season) — same pattern.

Bracket shape (6 rounds, both genders):

| ESPN season.slug                | Stage label | KNOCKOUT_ROUND_DEPTH |
|---------------------------------|-------------|----------------------|
| first-round                     | R64         | 0 (entry)            |
| second-round                    | R32         | 1                    |
| third-round                     | S16         | 2 (Sweet 16)         |
| quarterfinals                   | E8          | 3 (Elite 8)          |
| college-cup---semifinal (M's)   | F4          | 4 (College Cup SF)   |
| semifinals (W's)                | F4          | 4                    |
| college-cup---championship (M's)| NCG         | 5 (Final)            |
| college-cup (W's)               | NCG         | 5                    |

Gender-specific slug aliases for the final two rounds (M's uses "college-
cup---semifinal" / "college-cup---championship"; W's uses "semifinals" /
"college-cup"). Both map to the same canonical R64/R32/S16/E8/F4/NCG
stage labels, which match `KNOCKOUT_ROUND_DEPTH` entries already in use
by NCAA Women's Basketball March Madness (also a 6-round single-game
elim bracket — the depth dict is shared and the cascade is identical).

Field size note: Men's NCAA Soccer Tournament uses a 48-team field with
16 byes (top 16 seeds play their first match in the second round), so
"first-round" has only 16 games (32 of 48 teams), then "second-round"
has 16 games (32 teams: 16 byes + 16 R1 winners). Treating the entry
round as R64 is correct conceptually — the cascade only cares about
depth ordering, and a team that played and lost R1 has reached the
"R64" entry tier even though the tier isn't full. Women's bracket is
a full 64-team field with no byes.

Strength sharing: `set_regular_season_strengths` lets the plugin seed
playoff sampling with per-team Poisson rates from the regular-season
NcaaSoccerSource — the College Cup samples then reflect actual scoring
skill from the 20-game regular season instead of the 1.5/1.5 league-
average prior. Without the seed the source falls back to the prior.

Postseason is filed under `LEAGUE_CONTEXTS["NCAA_MSOC_CUP"]` /
`["NCAA_WSOC_CUP"]` (sibling to the regular-season `NCAA_MSOC` /
`NCAA_WSOC` contexts already shipped in Phase O). Weights ramp into
the College Cup Final because postseason is when these channels get
real TV pickup, and the National Championship Game has the highest
viewership of any D1 soccer game by an order of magnitude.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, MatchResult
from .bracket import BestOfNSeriesSource
from .._util import parse_iso_utc, poisson_sample as _poisson

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.ncaa_soccer_cup")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"

# Tournament window: M's runs late Nov through mid-Dec, W's runs mid-Nov
# through early-Dec. Walking Nov 1 through Dec 31 of season_year covers
# both with margin on either side.
TOURNAMENT_START_MONTH = 11
TOURNAMENT_END_MONTH = 12

# Per-team scoring rate priors. NCAA D1 soccer averages ~1.5 goals/team/
# game across both M's and W's. Same prior as NcaaSoccerSource so the
# regular-season → playoff strength seed lines up consistently when
# `set_regular_season_strengths` is wired.
_DEFAULT_GOALS_FOR = 1.5
_DEFAULT_GOALS_AGAINST = 1.5

# Map ESPN's per-event `season.slug` to canonical bracket stage labels.
# Stage labels are reused from NCAAW Basketball (R64/R32/S16/E8/F4/NCG)
# because the depth ordering in KNOCKOUT_ROUND_DEPTH already supports
# them and the cascade behaves identically for single-game elim brackets.
# Gender-specific keys for the last two rounds — both M's and W's map
# to the same canonical labels via slug aliases.
SLUG_TO_STAGE: Dict[str, str] = {
    "first-round":                  "R64",
    "second-round":                 "R32",
    "third-round":                  "S16",
    "quarterfinals":                "E8",
    # College Cup Semifinals — gender split:
    "college-cup---semifinal":      "F4",  # M's slug
    "semifinals":                   "F4",  # W's slug
    # National Championship Game — gender split:
    "college-cup---championship":   "NCG", # M's slug
    "college-cup":                  "NCG", # W's slug (the final, not the full event)
}


def _http_get(url: str, timeout: float = 15.0) -> Optional[Any]:
    """ESPN endpoints return dicts; the wrapper type is Optional[Any]
    rather than Optional[Dict] to keep the contract honest about the
    None failure path. Caller checks isinstance(data, dict)."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[ncaa_soccer_cup] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[ncaa_soccer_cup] %s failed: %s", url, exc)
        return None


def _dedupe_pk_shootout_pairs(
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Collapse multiple events at the same (stage, participants) tuple
    into the single decisive event. ESPN occasionally publishes two
    records for a soccer bracket game that goes to OT / PKs: one with
    the regulation tie (e.g., 0-0) marked finished, and a second with
    the PK shootout final score (e.g., 3-2). Both are tagged complete
    in ESPN's scoreboard.

    Preference order when multiple records collide on (stage,
    frozenset(home, away)):
      1. SCHEDULED records lose to any FINISHED record (real outcomes
         beat placeholders).
      2. Among FINISHED records, non-tie outcomes beat tie outcomes
         (PK shootout score beats the regulation tie).
      3. Among non-tie FINISHED records, the LATEST `start_time` wins
         (handles a real same-day duplicate; in practice the shootout
         event is published with a later timestamp than the regulation
         tie).

    This is conservative: single-game elimination brackets cannot have
    a real second leg between the same two teams at the same stage, so
    any (stage, participants) collision IS data noise.
    """
    # Bucket records by their dedup key. order matters: insertion order
    # in the candidates list is by-date, so the first record we see for
    # a (stage, participants) pair is the earliest event.
    buckets: Dict[Tuple[str, frozenset], List[Dict[str, Any]]] = {}
    for rec in records:
        key = (rec["stage"], frozenset((rec["home"], rec["away"])))
        buckets.setdefault(key, []).append(rec)

    out: List[Dict[str, Any]] = []
    for key, bucket in buckets.items():
        if len(bucket) == 1:
            out.append(bucket[0])
            continue
        out.append(_pick_decisive_event(bucket))
    return out


def _pick_decisive_event(bucket: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply the (FINISHED > SCHEDULED, non-tie > tie, latest > earliest)
    preference order. Bucket has 2+ records guaranteed.
    """
    finished = [r for r in bucket if r.get("status") == "FINISHED"]
    if not finished:
        # All scheduled — keep the latest by start_time (most recent
        # info is the canonical one).
        return max(bucket, key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc))
    non_tie = [
        r for r in finished
        if r.get("home_goals") is not None
        and r.get("away_goals") is not None
        and r["home_goals"] != r["away_goals"]
    ]
    pool = non_tie if non_tie else finished
    return max(pool, key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc))


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Prefer `team.location` (school) over `team.name` (mascot) — the
    EPG side of the matcher uses school names like "Washington at
    Stanford" rather than mascots like "Huskies at Cardinal". Same
    canonicalizer as NcaaSoccerSource so the cross-source team-name
    join (regular-season strength seed → College Cup teams) holds.
    """
    loc = (team_obj.get("location") or "").strip()
    if loc:
        return loc
    return (team_obj.get("name") or team_obj.get("abbreviation") or "").strip()


class NcaaSoccerCupSource(BestOfNSeriesSource):
    """NCAA D1 soccer College Cup bracket, parametrized on gender.

    Six single-game elim rounds (R64 → R32 → S16 → E8 → F4 → NCG).
    SERIES_LENGTH = 1 so each "series" clinches at ceil(1/2) = 1 win —
    the BestOfNSeriesSource state machine handles round_reached and
    terminal_outcomes for free at this length.

    Field-size asymmetry: M's has 48 teams (16 byes at entry), W's has
    64 teams (no byes). The bracket source doesn't care — both genders
    surface the same 6 stage labels via slug aliases in `SLUG_TO_STAGE`.
    """

    KO_STAGES = ("R64", "R32", "S16", "E8", "F4", "NCG")
    SERIES_LENGTH = 1
    supports_importance = True

    def __init__(
        self,
        gender: str = "m",
        season_year: Optional[int] = None,
    ) -> None:
        g = (gender or "").lower().strip()
        if g not in ("m", "w"):
            raise ValueError(f"gender must be 'm' or 'w', got {gender!r}")
        self.gender = g
        now = datetime.now(timezone.utc)
        # NCAA soccer seasons are named by their calendar year (2025
        # season runs Aug-Dec 2025, with the tournament wrapping mid-
        # December 2025). Default to current calendar year; pre-August
        # treats as the prior season (postseason wraps by mid-Dec, so
        # January reads as offseason → use prior year for any lingering
        # postseason data fetch).
        self.season_year = (
            season_year if season_year is not None
            else (now.year if now.month >= 8 else now.year - 1)
        )
        self._bracket_games_cache: Optional[List[Dict[str, Any]]] = None
        self._team_strengths_from_regular: Optional[
            Dict[str, Dict[str, float]]
        ] = None

    @property
    def sport_prefix(self) -> str:
        return "NCAAMSOC" if self.gender == "m" else "NCAAWSOC"

    @property
    def sport_label(self) -> str:
        return (
            "NCAA Men's College Cup"
            if self.gender == "m"
            else "NCAA Women's College Cup"
        )

    @property
    def _espn_slug(self) -> str:
        return f"usa.ncaa.{self.gender}.1"

    def _league_context_code(self) -> str:
        return "NCAA_MSOC_CUP" if self.gender == "m" else "NCAA_WSOC_CUP"

    def _winner_advance_label(self, stage: str) -> Optional[str]:
        # Champion is the synthetic depth above NCG (final game). Mirror
        # the NCAAW Basketball NCG → NCG_WINNER mapping — same depth
        # entry in KNOCKOUT_ROUND_DEPTH (already at depth 6).
        if stage == "NCG":
            return "NCG_WINNER"
        return None

    # ---------- strength sharing (regular season → playoff) ----------

    def estimate_strengths(self) -> Dict[str, Dict[str, float]]:
        """Per-team scoring/conceding rate. Seeded from the regular-
        season NcaaSoccerSource via `set_regular_season_strengths`;
        without the seed, falls back to the league-average prior."""
        if self._team_strengths_from_regular is not None:
            return self._team_strengths_from_regular
        return {}

    def set_regular_season_strengths(
        self, strengths: Dict[str, Dict[str, float]],
    ) -> None:
        """Hook for the plugin to share regular-season strength
        estimates with the College Cup source. Without this, every
        bracket team gets the league-average 1.5/1.5 prior — accurate
        enough for early-round importance but loses the team-skill
        signal that a 20-game regular season provides."""
        self._team_strengths_from_regular = strengths

    def _strength_for(
        self, strengths: Dict[str, Dict[str, float]], team: str,
    ) -> Dict[str, float]:
        if team in strengths:
            return strengths[team]
        return {
            "pf_per_game": _DEFAULT_GOALS_FOR,
            "pa_per_game": _DEFAULT_GOALS_AGAINST,
        }

    # ---------- sample_result (force a winner — bracket games) ----------

    def sample_result(
        self,
        state: Dict[str, Any],
        match: GameRow,
        strengths: Dict[str, Dict[str, float]],
        rng: random.Random,
    ) -> MatchResult:
        """Bracket games CANNOT end in a draw — NCAA postseason soccer
        goes to OT then PKs if needed. Sample Poisson goals; on a tied
        regulation result, coin-flip the +1 boost so the bracket
        cascade gets a clean winner. Distinct from NcaaSoccerSource
        regular-season sample_result which allows draws (1 standings
        point each) — bracket scenarios have no draw outcome at all.
        """
        del state  # interface-required, per-game classification only
        h = self._strength_for(strengths, match.home)
        a = self._strength_for(strengths, match.away)
        lam_home = max(0.05, (h["pf_per_game"] + a["pa_per_game"]) / 2.0)
        lam_away = max(0.05, (a["pf_per_game"] + h["pa_per_game"]) / 2.0)
        home_goals = _poisson(lam_home, rng)
        away_goals = _poisson(lam_away, rng)
        if home_goals == away_goals:
            # OT / PKs in real life; coin-flip the +1 here. Strength
            # asymmetry already biased the Poisson means — no further
            # bias added (PK shootouts are roughly coin-flips empirically
            # at the NCAA level).
            if rng.random() < 0.5:
                home_goals += 1
            else:
                away_goals += 1
        return MatchResult(home_goals=home_goals, away_goals=away_goals)

    # ---------- fetch_upcoming (EPG display side) ----------

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Emit upcoming tournament games (any stage). Per-day sweep —
        ESPN's date-range syntax silently caps at 25 events, same trap
        as ncaa_baseball.py / ncaa_soccer.py. Postseason games carry
        a `season.slug` of one of the SLUG_TO_STAGE keys; non-tournament
        games are filtered out so only bracket games surface here.
        """
        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard?"
                f"dates={day.strftime('%Y%m%d')}"
            )
            if not isinstance(data, dict):
                continue
            for event in data.get("events") or []:
                rec = self._extract_bracket_record(event)
                if rec is None:
                    continue
                eid = rec.get("game_id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                start = rec.get("start_time")
                if start is None:
                    continue
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=rec["home"],
                    away=rec["away"],
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "fd_competition_code": self._league_context_code(),
                        "stage": rec.get("stage"),
                    },
                ))
        return out

    # ---------- _fetch_bracket_games (importance side) ----------

    def _fetch_bracket_games(self) -> List[Dict[str, Any]]:
        """Pull the November-December tournament window and emit one
        canonical record per bracket game. Filters out non-tournament
        events via SLUG_TO_STAGE membership — regular-season games
        won't carry a slug like 'quarterfinals', so they fall out
        naturally.

        ESPN sometimes records two events for a single bracket game
        that goes to OT / PKs: one with the regulation 0-0 / X-X tie
        and a second with the PK shootout final score (observed live
        on the 2024 M's Marshall @ SMU QF: events 723937 with 0-0
        and 724472 with 3-2). Both are tagged status="post" and
        share the same slug, so we dedupe by (stage, frozenset
        participants) keeping the event with a non-tie outcome (or
        the latest by start_time when both are non-tie). Single-game
        elimination brackets have at most one game per (stage,
        participants); a second event at the same stage between the
        same two teams is ESPN data noise, not a real second leg.
        """
        if self._bracket_games_cache is not None:
            return self._bracket_games_cache

        candidates: List[Dict[str, Any]] = []
        seen_ids: set = set()
        # Tournament window: Nov 1 - Dec 31 of season_year.
        start = datetime(
            self.season_year, TOURNAMENT_START_MONTH, 1, tzinfo=timezone.utc,
        ).date()
        # End of December: build Jan 1 of next year, subtract one day.
        end = (
            datetime(self.season_year + 1, 1, 1, tzinfo=timezone.utc).date()
            - timedelta(days=1)
        )
        day = start
        while day <= end:
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard?"
                f"dates={day.strftime('%Y%m%d')}"
            )
            if isinstance(data, dict):
                for event in data.get("events") or []:
                    rec = self._extract_bracket_record(event)
                    if rec is None or rec["game_id"] is None:
                        continue
                    if rec["game_id"] in seen_ids:
                        continue
                    seen_ids.add(rec["game_id"])
                    candidates.append(rec)
            day += timedelta(days=1)

        self._bracket_games_cache = _dedupe_pk_shootout_pairs(candidates)
        return self._bracket_games_cache

    @staticmethod
    def _extract_bracket_record(
        event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Convert one ESPN scoreboard event into the canonical
        BracketSportSource per-game record shape, OR None if the event
        isn't a tournament bracket game.

        Stage routing: `event.season.slug` carries the round identifier
        (e.g. 'first-round', 'college-cup---semifinal'). Non-tournament
        games (regular-season) carry a different slug and are filtered
        out via SLUG_TO_STAGE membership.
        """
        season = event.get("season") or {}
        slug = (season.get("slug") or "").strip().lower()
        stage = SLUG_TO_STAGE.get(slug)
        if stage is None:
            return None

        comps = event.get("competitions") or []
        if not comps:
            return None
        comp = comps[0]
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            return None
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            return None
        home_team = _team_canonical_name(home.get("team") or {})
        away_team = _team_canonical_name(away.get("team") or {})
        if not home_team or not away_team:
            return None

        status_type = (comp.get("status") or {}).get("type") or {}
        completed = bool(status_type.get("completed"))
        state = (status_type.get("state") or "").lower()
        is_final = completed or state == "post"
        status = "FINISHED" if is_final else "SCHEDULED"

        try:
            hg = int(home.get("score")) if is_final else None
        except (TypeError, ValueError):
            hg = None
        try:
            ag = int(away.get("score")) if is_final else None
        except (TypeError, ValueError):
            ag = None

        if is_final and (hg is None or ag is None):
            status = "SCHEDULED"
            hg = None
            ag = None

        return {
            "game_id": event.get("id"),
            "stage": stage,
            # SERIES_LENGTH=1 means each game is its own one-game series.
            # matchday is always 1; BracketSportSource clinches at first win.
            "matchday": 1,
            "home": home_team,
            "away": away_team,
            "home_goals": hg,
            "away_goals": ag,
            "status": status,
            "start_time": parse_iso_utc(event.get("date")),
            "extra": {"season_slug": slug},
        }
