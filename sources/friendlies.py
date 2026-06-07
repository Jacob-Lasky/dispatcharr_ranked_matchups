"""International friendlies source: ESPN's unofficial soccer API.

Senior national-team friendlies (FIFA international windows + pre-tournament
warm-ups, e.g. a USMNT vs Senegal tune-up the week before the World Cup).

These are EXHIBITION games: no league table, no standings, no elimination
stakes. This source therefore DELIBERATELY does not implement the Monte Carlo
importance interface; `supports_importance` stays False (the base-class
default). A friendly surfaces in Top Matchups purely on the favorite / rivalry
/ narrative signals in score_game, which is honest: a USA warm-up is worth
watching because it's USA, not because it swings a standings position. DO NOT
flip supports_importance on here and fabricate a league context for it: there
is no table to simulate, and compute_match_importance would have nothing to
threshold against.

Why this source exists: the FIFA World Cup source (sources/soccer.py, config
key "world_cup") reads ONLY tournament fixtures from Football-Data.org. It
does not, and should not, contain warm-up friendlies. Before this source there
was no path for a pre-tournament national-team friendly to appear in the guide
at all (the teams aren't playing a league fixture and aren't yet in a World
Cup match). See the "USA vs Senegal didn't show up" investigation.

Parametrized on gender ("m"/"w"), mirroring NcaaSoccerSource:
  - men's   -> ESPN league slug "fifa.friendly"
  - women's -> ESPN league slug "fifa.friendly.w"

Favorites gate (favorites_only): because a friendly's ONLY claim to a guide
slot is the favorite signal, the source can be told to emit only games that
involve a configured favorite national team. A FIFA window produces dozens of
fixtures between teams a given user doesn't follow (Kenya vs Lesotho, Cambodia
vs Hong Kong); surfacing all of them buries the one game the user cares about.
With the gate on, a friendly between two non-favorite teams is dropped EVEN IF
it would otherwise pick up a rivalry/narrative signal: an exhibition between
teams you don't follow isn't a Top Matchup. The gate is opt-out (the plugin
defaults it on) so users who genuinely want every friendly can disable it.

Offseason (no friendlies scheduled in the lookahead window) returns []. No API
key required (ESPN's site API is free, same as the NFL/NHL/NCAA-soccer
sources).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .base import GameRow, SportSource
from ._espn import extract_espn_scoreboard_event

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.friendlies")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[friendlies] %s → %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[friendlies] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """National-team namer. ESPN populates `location` with the country name
    ("United States", "Senegal") for international fixtures, which is exactly
    the form the favorites list and EPG provider titles use. Fall back to
    displayName / name / abbreviation if a row is missing location."""
    for key in ("location", "displayName", "name", "abbreviation"):
        val = (team_obj.get(key) or "").strip()
        if val:
            return val
    return ""


class InternationalFriendliesSource(SportSource):
    """Senior national-team friendlies from ESPN, parametrized on gender
    ("m" or "w"). Lightweight: implements only fetch_upcoming. No importance
    simulation (exhibition games have no standings), so a friendly scores on
    favorite / rivalry / narrative signals alone. Optionally gated to favorite
    national teams only (favorites_only); see the module docstring."""

    def __init__(
        self,
        gender: str = "m",
        favorites: Optional[List[str]] = None,
        favorites_only: bool = False,
    ) -> None:
        g = (gender or "").lower().strip()
        if g not in ("m", "w"):
            raise ValueError(f"gender must be 'm' or 'w', got {gender!r}")
        self.gender = g
        # favorites_only gates fetch_upcoming to games involving a configured
        # favorite national team. Matching reuses scoring.match_favorites so
        # the word-boundary rules are IDENTICAL to how the favorite SCORING
        # signal is computed; DO NOT reimplement substring matching here and
        # risk the gate and the score disagreeing on what "involves a favorite"
        # means. self.favorites holds the user's own Favorites list (country
        # names as ESPN spells them: "United States", not "USA").
        self.favorites = list(favorites or [])
        self.favorites_only = bool(favorites_only)

    @property
    def sport_prefix(self) -> str:
        return "FRIENDLY" if self.gender == "m" else "FRIENDLYW"

    @property
    def sport_label(self) -> str:
        return (
            "International Friendly" if self.gender == "m"
            else "Women's International Friendly"
        )

    @property
    def _espn_slug(self) -> str:
        return "fifa.friendly" if self.gender == "m" else "fifa.friendly.w"

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Per-day scoreboard sweep. ESPN's date-RANGE syntax silently caps at
        25 events, so we walk one day at a time (same trap documented in
        ncaa_baseball.py / ncaa_soccer.py). Drops FINISHED games: a friendly
        that already kicked off and ended is not an upcoming Top Matchup. A
        live (in-progress) game classifies as SCHEDULED in the shared parser
        and is kept, so a game "playing right now" still surfaces.

        When favorites_only is set, drops any game not involving a favorite
        national team (see the module docstring on the favorites gate)."""
        # Lazy import to keep the source module's top-level import graph free of
        # scoring (matches the lazy-import idiom used across sources/*.py).
        from ..scoring import match_favorites

        if self.favorites_only and not self.favorites:
            # Gate is on but the user configured no favorites, so every game
            # would be dropped. Surface this once per fetch rather than fail
            # silently with an empty guide section.
            logger.warning(
                "[friendlies] favorites_only is on but no favorites are "
                "configured: all %s friendlies will be suppressed",
                self.sport_label,
            )

        today = datetime.now(timezone.utc).date()
        out: List[GameRow] = []
        seen_ids: set = set()
        for offset in range(days_ahead + 1):
            day = today + timedelta(days=offset)
            data = _http_get(
                f"{ESPN_BASE}/{self._espn_slug}/scoreboard"
                f"?dates={day.strftime('%Y%m%d')}"
            )
            if not data:
                continue
            for event in data.get("events") or []:
                rec = extract_espn_scoreboard_event(
                    event, team_namer=_team_canonical_name,
                )
                if rec is None:
                    continue
                eid = rec.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                if rec.get("status") == "FINISHED":
                    continue
                start = rec.get("start_time")
                if start is None:
                    continue
                home = rec["home"]
                away = rec["away"]
                if self.favorites_only and not match_favorites(
                    home, away, self.favorites
                ):
                    # Exhibition between teams the user doesn't follow: no
                    # standings/rank to earn a slot and no favorite to rescue
                    # it. Drop rather than let it pad the guide.
                    continue
                # No rank (national-team friendlies have no poll), no spread,
                # no closeness, no fd_competition_code: the scoring loop sees
                # no league context and contributes zero importance, which is
                # correct for an exhibition. Favorite / rivalry / narrative
                # carry the signal.
                out.append(GameRow(
                    sport_prefix=self.sport_prefix,
                    sport_label=self.sport_label,
                    home=home,
                    away=away,
                    rank_home=None,
                    rank_away=None,
                    start_time=start,
                    extra={
                        "espn_event_id": eid,
                        "espn_league_slug": self._espn_slug,
                    },
                ))
        return out
