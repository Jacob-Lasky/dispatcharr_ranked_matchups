"""English domestic cups from ESPN: the EFL (Carabao) Cup and the FA Cup.

WHY THIS FILE EXISTS AT ALL, because the obvious implementation is wrong:
these two competitions CANNOT be added to `soccer.py::COMPETITIONS` alongside
the Premier League and the Championship. Football-Data.org gates every English
domestic cup behind a paid plan, verified against the live API with the
plugin's own key on 2026-08-26:

    code=FLC  Football League Cup  type=CUP  plan=TIER_THREE
    code=FAC  FA Cup               type=CUP  plan=TIER_TWO

    GET /v4/competitions/FLC/matches -> HTTP 403
    {"message": "The resource you are looking for is restricted and
     apparently not within your permissions. Please check your
     subscription.", "errorCode": 403}

with `GET /v4/competitions/PL/matches` returning fixtures on the same key as
the control. FD.org's free tier is exactly 12 competitions (PL, ELC, CL, EC,
FL1, BL1, SA, DED, PPL, PD, BSA, WC) and no cup of any country is among them.
So the fixtures come from ESPN's keyless site API instead, the same feed the
NHL / MLS / NCAA / friendlies sources already use. DO NOT "simplify" this into
a `SoccerCompetitionConfig` entry: the fetch would 403 on every refresh and the
cup would silently contribute zero games, which is the exact symptom #190 was
filed for.

Motivating case (#190): Tottenham v Charlton, EFL Cup second round,
2026-08-26. Carried on six-plus channels in the user's guide (beIN Sports
AU/FR/Arabic, ESPN Brasil, Nova Sport CZ) and absent from Top Matchups,
because no cup fixture had ever entered the pipeline for the matcher to look
for.

ROUNDS. ESPN publishes the round on each event as `season.slug`, the same
mechanism `ncaa_soccer_cup.py` keys on. Both cups expose exactly 8 ordered
rounds, enumerated live from
`sports.core.api.espn.com/v2/sports/soccer/leagues/<slug>/seasons/<year>/types`
on 2026-08-26:

    EFL Cup  preliminary-round, first-round, second-round, third-round,
             fourth-round, quarterfinals, semifinals, final
    FA Cup   first-round, second-round, third-round, fourth-round,
             fifth-round, quarterfinals, semifinals, final

The two lists are NOT interchangeable and MUST stay per-competition: the EFL
Cup has a preliminary round the FA Cup does not, the FA Cup has a fifth round
the EFL Cup does not, and the shared names sit at different depths (the FA
Cup's fourth round is its round of 32, the EFL Cup's fourth round is its round
of 16). A single shared slug->stage map would therefore score the same slug as
two different things. DO NOT merge them.

IMPORTANCE. Both sources deliberately leave `supports_importance` at the base
class default of False, so a cup tie surfaces on stage + favorite + rivalry +
narrative, never on a simulated league table. Three reasons, in order of how
load-bearing they are:

  1. There is no table. A cup has no standings for the Monte Carlo simulator
     to threshold against, exactly as documented for the friendlies sources.
  2. Simulating the BRACKET instead would need the full bracket, and ESPN's
     scoreboard publishes one day at a time; reconstructing an 8-round English
     cup means sweeping from June, and the entry rounds are not a clean power
     of two (the EFL Cup's second round mixes 36 first-round winners with 13
     Premier League clubs that have no European commitments).
  3. The EFL Cup semifinal is TWO LEGS while every other round is single-leg,
     so a `BestOfNSeriesSource` with `SERIES_LENGTH = 1` would model that round
     wrongly. `AggregateLegSource` exists for two-legged ties but mixing leg
     counts across rounds of one bracket is not a shape any current base
     supports.

DO NOT flip `supports_importance` on without solving all three. See #190.

RANKS are likewise always None, and that is what makes the giant-killing shape
score correctly rather than perversely. `scoring.score_game` awards
`rank_pair` / `one_ranked` points only when a rank is present, so a Premier
League side drawn against a League One side produces NO rank-gap signal at
all: nothing to suppress the tie for being lopsided, which is the right answer
for a competition whose entire appeal is the upset. Supplying a pseudo-rank
from league tier would ACTIVELY HURT (a tier gap would read as "one ranked vs
unranked" and inflate the favourite's side of a mismatch). DO NOT add one.

No API key required. Offseason (no cup fixtures in the lookahead window)
returns [].
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base import GameRow, SportSource
from ._espn import sweep_upcoming_scoreboard

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.english_cup")

# Canonical stage labels for the rounds that precede the quarterfinals.
#
# The last three rounds of both cups reuse the stage labels scoring.py already
# carries for UEFA knockouts (QUARTER_FINALS / SEMI_FINALS / FINAL), because
# those genuinely ARE the same stage. The earlier rounds get their own CUP_*
# labels rather than being mapped onto LAST_16 / LAST_32, for two reasons:
#
#   - team counts drift year to year with byes and re-entries, so "which round
#     is the round of 32" is not stable enough to hardcode; and
#   - LAST_16 / LAST_32 carry a UEFA-knockout score band, and claiming an FA
#     Cup first-round tie between two League Two clubs is worth the same as a
#     Champions League round-of-16 leg is simply false.
#
# scoring.py owns the points these map to; see _CUP_ROUND_STAGE_SCORES there.
STAGE_PRELIM = "CUP_PRELIM"
STAGE_R1 = "CUP_R1"
STAGE_R2 = "CUP_R2"
STAGE_R3 = "CUP_R3"
STAGE_R4 = "CUP_R4"
STAGE_R5 = "CUP_R5"
STAGE_QF = "QUARTER_FINALS"
STAGE_SF = "SEMI_FINALS"
STAGE_FINAL = "FINAL"

# EFL Cup (Carabao Cup). Ordered as ESPN's season types list them; the tuple
# order IS the round order and is what `_round_index` reads.
EFL_CUP_ROUNDS: Tuple[Tuple[str, str], ...] = (
    ("preliminary-round", STAGE_PRELIM),
    ("first-round",       STAGE_R1),
    ("second-round",      STAGE_R2),
    ("third-round",       STAGE_R3),
    ("fourth-round",      STAGE_R4),   # the EFL Cup's round of 16
    ("quarterfinals",     STAGE_QF),
    ("semifinals",        STAGE_SF),   # two legs, see the module docstring
    ("final",             STAGE_FINAL),
)

# FA Cup. Note `fifth-round` where the EFL Cup has `preliminary-round`, and
# note that the shared slugs sit at different bracket depths. The proper
# rounds only; ESPN files the qualifying rounds under a separate competition
# (FD.org calls it FACQ) that this source does not sweep.
FA_CUP_ROUNDS: Tuple[Tuple[str, str], ...] = (
    ("first-round",   STAGE_R1),
    ("second-round",  STAGE_R2),
    ("third-round",   STAGE_R3),       # Premier League clubs enter here
    ("fourth-round",  STAGE_R4),       # the FA Cup's round of 32
    ("fifth-round",   STAGE_R5),       # the FA Cup's round of 16
    ("quarterfinals", STAGE_QF),
    ("semifinals",    STAGE_SF),
    ("final",         STAGE_FINAL),
)


def _http_get(url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    """DELIBERATELY a per-module copy, not a shared helper.

    Seventeen sources define a near-identical `_http_get`, and the similarity is
    textual rather than shared knowledge: these copies do not have to agree for
    anything to be correct, and two things make the duplication load-bearing.

      - The logger name. Each source logs under its own
        `plugins.dispatcharr_ranked_matchups.<source>` channel, which is how a
        failing feed is attributed in the Dispatcharr log.
      - The patch point. Every source's tests stub `<module>.requests.get`, so
        the module needs its OWN `requests` symbol. Routing through a shared
        getter would make those stubs silent no-ops that hit the real network
        instead of failing, which is the worst kind of test regression.

    `sweep_upcoming_scoreboard` therefore takes `http_get` INJECTED rather than
    calling requests itself. The genuinely shared knowledge (the sweep window
    and its filters) lives there; this stays local. Repo-wide consolidation of
    all seventeen is tracked separately.
    """
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            logger.warning("[english_cup] %s -> %d", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[english_cup] %s failed: %s", url, exc)
        return None


def _team_canonical_name(team_obj: Dict[str, Any]) -> str:
    """Same soccer namer as the friendlies sources, and for the same reason.

    ESPN populates `location` with the full club name on its soccer endpoints
    ("Tottenham Hotspur", "Bradford City", "Charlton Athletic"), which is the
    form the favorites list and the EPG provider titles use.

    DO NOT prefer shortDisplayName. ESPN's short forms are lossy in exactly the
    way that breaks matching: "Tottenham Hotspur" becomes "Spurs", "Bradford
    City" becomes "Bradford". Both the favorite signal and the EPG title match
    run against this string.
    """
    for key in ("location", "displayName", "name", "abbreviation"):
        val = (team_obj.get(key) or "").strip()
        if val:
            return val
    return ""


class _EnglishDomesticCupBase(SportSource):
    """Shared per-day ESPN sweep for a single-elimination English cup.

    Subclasses supply `_espn_slug`, `_rounds`, `sport_prefix`, and
    `sport_label`; everything else (the sweep, the round mapping, the GameRow
    shape) lives here so the two cups cannot drift apart. Abstract on all four,
    so this class is not instantiable on its own.

    `supports_importance` stays False, inherited from SportSource. See the
    module docstring for the three reasons; do not override it here.
    """

    @property
    @abstractmethod
    def _espn_slug(self) -> str:
        """ESPN soccer competition slug to sweep."""

    @property
    @abstractmethod
    def _rounds(self) -> Tuple[Tuple[str, str], ...]:
        """This competition's (season.slug, stage label) pairs, in round order."""

    @property
    def _slug_to_stage(self) -> Dict[str, str]:
        return {slug: stage for slug, stage in self._rounds}

    def _round_index(self, slug: str) -> Optional[int]:
        """0-based depth of `slug` within THIS competition's round order, or
        None for a slug this competition does not publish.

        Exposed (and tested) because it is the only thing that makes the two
        cups' overlapping slug names unambiguous: "fourth-round" is depth 4 in
        the EFL Cup and depth 3 in the FA Cup.
        """
        for i, (s, _stage) in enumerate(self._rounds):
            if s == slug:
                return i
        return None

    def fetch_upcoming(self, days_ahead: int = 7) -> List[GameRow]:
        """Upcoming cup ties in the window, one GameRow each.

        The sweep, the FINISHED drop, the stale-SCHEDULED age floor, and the id
        dedupe all come from `_espn.sweep_upcoming_scoreboard`. The one filter
        added here drops any event whose `season.slug` this competition does
        not publish: ESPN occasionally files a qualifying-round or otherwise
        unrecognised fixture under the competition, and a tie whose round we
        cannot identify has no stage to score and would land in the guide as an
        unlabelled game.
        """
        out: List[GameRow] = []
        unknown_slugs: set = set()
        for rec in sweep_upcoming_scoreboard(
            slug=self._espn_slug,
            days_ahead=days_ahead,
            team_namer=_team_canonical_name,
            http_get=_http_get,
            extras_fn=lambda ev: {
                "season_slug": ((ev.get("season") or {}).get("slug") or "")
                .strip()
                .lower(),
            },
        ):
            slug = rec.get("season_slug") or ""
            stage = self._slug_to_stage.get(slug)
            if stage is None:
                unknown_slugs.add(slug)
                continue
            out.append(GameRow(
                sport_prefix=self.sport_prefix,
                sport_label=self.sport_label,
                home=rec["home"],
                away=rec["away"],
                # Deliberately None: a cup has no table, and a tier-derived
                # pseudo-rank would inflate the favourite in a mismatch. See
                # the RANKS paragraph in the module docstring.
                rank_home=None,
                rank_away=None,
                start_time=rec["start_time"],
                # No `game_id`: this source never reaches the importance
                # simulator (supports_importance is False), so stamping the
                # shared identity key would be inert. Same documented
                # exemption as mls.py and friendlies.py; see #181 and
                # tests/test_match_identity_contract.py.
                extra={
                    "espn_event_id": rec.get("id"),
                    "espn_league_slug": self._espn_slug,
                    "stage": stage,
                    "season_slug": slug,
                    "round_index": self._round_index(slug),
                },
            ))
        if unknown_slugs:
            # One line per fetch, not per event: a new ESPN round slug is worth
            # knowing about (it means real fixtures are being dropped) but it
            # is not an error worth failing the refresh over.
            logger.info(
                "[english_cup] %s: skipped fixtures with unrecognised "
                "season.slug %s",
                self.sport_label, sorted(unknown_slugs),
            )
        return out


class EflCupSource(_EnglishDomesticCupBase):
    """EFL Cup, sponsored as the Carabao Cup. 8 rounds, League Cup format:
    all 92 Premier League and EFL clubs, single-leg except a two-legged
    semifinal."""

    @property
    def sport_prefix(self) -> str:
        # NOT "EFL": that prefix is already the EFL CHAMPIONSHIP (see
        # soccer.py::COMPETITIONS["championship"]), and rivalries.json plus
        # logos.SPORTSDB_TOURNAMENT_LEAGUE_IDS are keyed on sport_prefix, so
        # reusing it would cross-wire the league and the cup.
        return "EFLCUP"

    @property
    def sport_label(self) -> str:
        return "EFL Cup"

    @property
    def _espn_slug(self) -> str:
        return "eng.league_cup"

    @property
    def _rounds(self) -> Tuple[Tuple[str, str], ...]:
        return EFL_CUP_ROUNDS


class FaCupSource(_EnglishDomesticCupBase):
    """FA Cup. 8 proper rounds (the qualifying rounds are a separate ESPN
    competition and are not swept), single-leg throughout since replays were
    scrapped from the 2024/25 competition onward."""

    @property
    def sport_prefix(self) -> str:
        return "FACUP"

    @property
    def sport_label(self) -> str:
        return "FA Cup"

    @property
    def _espn_slug(self) -> str:
        return "eng.fa"

    @property
    def _rounds(self) -> Tuple[Tuple[str, str], ...]:
        return FA_CUP_ROUNDS
