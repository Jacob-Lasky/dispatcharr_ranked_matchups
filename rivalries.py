"""Rivalry detection: whole-name match against a curated JSON list.

`rivalries.json` lives next to this module and maps sport_prefix to a list of
team-name pairs. At plugin import, the JSON is read once and indexed by sport;
`is_rivalry(home, away, sport_prefix)` returns True when the (home, away) pair
appears in either order in the list for that sport.

A prefix's value may instead be a STRING naming a shared set under the
top-level `"_pools"` key, which it then resolves to exactly. That is how the
English league prefixes (EPL, EFL) and the English cups (EFLCUP, FACUP) share
one list of English club rivalries without four copies of it that must agree
(#192). `_resolve_entries` does the expansion.

Names are matched WHOLE, after normalizing case/whitespace and stripping a
club-type suffix token, so Football-Data.org's "Manchester City FC" matches
the stored "Manchester City". Spell every entry the way its source spells it:
CFBD says "Ole Miss", not "Mississippi". See #8.

To extend: edit `rivalries.json`. The match runs once per refresh per game, so
even a 10,000-entry list would barely register on the profile. No LLM-judged
path here: that's a deferred follow-up if the static list proves too narrow.
"""
from __future__ import annotations

import json
import logging
import os
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from ._util import TEAM_SUFFIX_TOKENS

logger = logging.getLogger(__name__)

_RIVALRIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rivalries.json")


def _normalize(name: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace.

    Diacritic folding is load-bearing, not cosmetic: Football-Data.org writes
    "FC Bayern Munchen" with the umlaut, "Gremio FBPA" with the circumflex and
    "Sao Paulo FC" with the tilde, while other sources and any human editing
    this file by hand will reach for the ASCII form. Verified 2026-09-06: SIX
    freshly-written entries matched nothing for exactly this reason, which is
    the same silently-dead-entry failure as the Egg Bowl.
    """
    folded = unicodedata.normalize("NFKD", name or "")
    ascii_form = "".join(c for c in folded if not unicodedata.combining(c))
    return " ".join(ascii_form.lower().split())


_POOLS_KEY = "_pools"


def _resolve_entries(raw: Dict[str, Any]) -> Dict[str, List[Any]]:
    """{sport_prefix: [raw entry, ...]}, with every pool reference expanded.

    A prefix maps either to its own list of entries or to a STRING naming a
    list under `raw["_pools"]`; the string form resolves to that pool's list
    itself, so every prefix naming one pool sees the same pairs and cannot
    drift from the others. Keys starting with "_" are metadata, never sport
    prefixes, which is also why the pools live under one.

    A reference to a pool that does not exist logs a warning and leaves that
    prefix with no rivalries, the same fail-soft stance as a corrupt file: an
    enhancement must not take the refresh down. Entries are returned as
    written (not normalized) so the tests can check them against how each
    source spells the team.
    """
    pools = raw.get(_POOLS_KEY)
    if not isinstance(pools, dict):
        pools = {}
    out: Dict[str, List[Any]] = {}
    for key, value in raw.items():
        if key.startswith("_"):
            continue  # _comment, _pools
        if isinstance(value, str):
            pool = pools.get(value)
            if not isinstance(pool, list):
                logger.warning(
                    "rivalries.json: %s names pool %r, which is not a list "
                    "under %s; no rivalries for %s", key, value, _POOLS_KEY, key,
                )
                continue
            value = pool
        if isinstance(value, list):
            out[key] = value
    return out


def _load_rivalries() -> Dict[str, List[Tuple[str, str, str]]]:
    """Load and normalize the rivalries map. Returns
    {sport_prefix: [(a, b, trophy_name), ...]} with an empty trophy name for
    two-element entries.

    Failure modes (file missing, JSON corrupt) log a warning and return an
    empty map so the plugin still works: rivalries are an enhancement, not
    a hard dependency.
    """
    try:
        with open(_RIVALRIES_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("rivalries.json missing at %s; rivalry signal disabled", _RIVALRIES_PATH)
        return {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("rivalries.json load failed (%s); rivalry signal disabled", e)
        return {}
    if not isinstance(raw, dict):
        logger.warning("rivalries.json is not an object; rivalry signal disabled")
        return {}
    out: Dict[str, List[Tuple[str, str, str]]] = {}
    for key, pairs in _resolve_entries(raw).items():
        clean: List[Tuple[str, str, str]] = []
        for entry in pairs:
            # 2 elements = a pair with no trophy; 3 = pair plus trophy name.
            # A malformed entry is dropped rather than failing the load: a
            # typo in one row must not disable the whole signal.
            if (
                isinstance(entry, list)
                and len(entry) in (2, 3)
                and all(isinstance(s, str) and s.strip() for s in entry[:2])
            ):
                trophy = entry[2].strip() if len(entry) == 3 and isinstance(entry[2], str) else ""
                clean.append((_normalize(entry[0]), _normalize(entry[1]), trophy))
        if clean:
            out[key] = clean
    return out


# Loaded once per process. Plugin reload re-imports the module so edits to
# rivalries.json land on the next .reload_token bump.
_RIVALRIES_BY_SPORT = _load_rivalries()


def is_rivalry(home: str, away: str, sport_prefix: str) -> bool:
    """True when (home, away) matches a known rivalry pair for this sport.

    Order-independent, and WHOLE-name after normalizing case, whitespace,
    diacritics and a club-type suffix: the entry 'Manchester City' matches
    Football-Data.org's 'Manchester City FC', but never 'Manchester City
    Women'. `_name_matches` carries why it must not be loosened.
    """
    return rivalry_name(home, away, sport_prefix) is not None


def rivalry_name(home: str, away: str, sport_prefix: str) -> Optional[str]:
    """The trophy or game name for this fixture, "" for a rivalry with no
    trophy recorded, or None when it is not a known rivalry.

    The three-way return matters: "" and None mean different things, and
    collapsing them would either lose the rivalry signal for the ~7 pairs with
    no trophy or invent a trophy for them. Callers that only need the boolean
    use `is_rivalry`.
    """
    pairs = _RIVALRIES_BY_SPORT.get(sport_prefix)
    if not pairs or not home or not away:
        return None
    h, a = _normalize(home), _normalize(away)
    for rival_a, rival_b, trophy in pairs:
        # Order in the JSON pair is incidental: match against both orderings.
        if (_name_matches(h, rival_a) and _name_matches(a, rival_b)) or (
            _name_matches(h, rival_b) and _name_matches(a, rival_a)
        ):
            return trophy
    return None


def _strip_suffix(name: str) -> str:
    """Drop every club-type token ('fc', 'afc', 'cf', 'sc'), wherever it sits
    in the name, so the Football-Data.org form 'manchester city fc' compares
    equal to a stored 'Manchester City' and 'fc bayern munchen' to 'bayern
    munchen'. Whole tokens only: 'fcsb' and 'sco' are untouched."""
    parts = [p for p in name.split() if p not in TEAM_SUFFIX_TOKENS]
    return " ".join(parts)


def _name_matches(team_name: str, rivalry_name: str) -> bool:
    """True when the two names denote the same team.

    WHOLE-NAME equality after normalizing and stripping club-type suffixes.
    Nothing looser. Spell every entry the way its SOURCE spells it; where two
    sources disagree (SportsDB "Paris SG" vs Football-Data.org "Paris
    Saint-Germain FC") add BOTH forms as separate entries.
    `TestEveryEntryResolvesToARealTeam` proves each one resolves against a
    real roster, so a misspelling fails a test instead of silently never
    matching.

    DO NOT loosen this, in either of the two ways it has already been
    loosened and had to be tightened again. Both were SCORING bugs, because
    `is_rivalry` feeds the score:

      substring       ["Texas", "Texas A&M"]  matched  Texas Tech vs Texas A&M
      token-subset    ["Real Madrid", "FC Barcelona"]  matched
                      Real Madrid vs RCD Espanyol de BARCELONA, returning
                      "El Clasico" for a fixture that is not one

    The second is the instructive one. It had a blocklist of tokens that mark
    a different institution ("State", "Tech", "A&M"), which is FAIL-OPEN by
    construction: it rejects only what someone thought to list, and nobody
    thinks of a city name shared by two clubs until it fires. Whole-name
    equality is fail-closed, and the cost, spelling entries out in full, is
    paid once and checked by a test.
    """
    return _strip_suffix(team_name) == _strip_suffix(rivalry_name)
