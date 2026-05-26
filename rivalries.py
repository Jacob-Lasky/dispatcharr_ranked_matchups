"""Rivalry detection: case-insensitive substring match against a curated JSON list.

`rivalries.json` lives next to this module and maps sport_prefix to a list of
team-name pairs. At plugin import, the JSON is read once and indexed by sport;
`is_rivalry(home, away, sport_prefix)` returns True when the (home, away) pair
appears in either order in the list for that sport.

Substring matching (in both directions per token) handles source-side name
variations: Football-Data.org returns "Manchester City FC" but our list
stores the bare "Manchester City"; ESPN returns "Boston Celtics" while we
store the same. See #8.

To extend: edit `rivalries.json`. The match runs once per refresh per game, so
even a 10,000-entry list would barely register on the profile. No LLM-judged
path here: that's a deferred follow-up if the static list proves too narrow.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

_RIVALRIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rivalries.json")


def _normalize(name: str) -> str:
    """Lowercase + collapse whitespace. Substring match runs against this form."""
    return " ".join((name or "").lower().split())


def _load_rivalries() -> Dict[str, List[Tuple[str, str]]]:
    """Load and normalize the rivalries map. Returns {sport_prefix: [(a, b), ...]}.

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
    out: Dict[str, List[Tuple[str, str]]] = {}
    for key, pairs in raw.items():
        if key.startswith("_"):
            continue  # skip _comment etc
        if not isinstance(pairs, list):
            continue
        clean: List[Tuple[str, str]] = []
        for entry in pairs:
            if (
                isinstance(entry, list)
                and len(entry) == 2
                and all(isinstance(s, str) and s.strip() for s in entry)
            ):
                clean.append((_normalize(entry[0]), _normalize(entry[1])))
        if clean:
            out[key] = clean
    return out


# Loaded once per process. Plugin reload re-imports the module so edits to
# rivalries.json land on the next .reload_token bump.
_RIVALRIES_BY_SPORT = _load_rivalries()


def is_rivalry(home: str, away: str, sport_prefix: str) -> bool:
    """True when (home, away) matches a known rivalry pair for this sport.

    Match is case-insensitive substring in both directions: the rivalry entry
    'Manchester City' matches 'Manchester City FC' (rivalry-string is a
    substring of team-string). The reverse: entry 'Manchester City FC FC FC'
    matching team 'Manchester City': also matches, which is a feature for
    occasional source-side abbreviations.
    """
    pairs = _RIVALRIES_BY_SPORT.get(sport_prefix)
    if not pairs or not home or not away:
        return False
    h, a = _normalize(home), _normalize(away)
    for rival_a, rival_b in pairs:
        # Order in the JSON pair is incidental: match against both orderings.
        if _name_matches(h, rival_a) and _name_matches(a, rival_b):
            return True
        if _name_matches(h, rival_b) and _name_matches(a, rival_a):
            return True
    return False


def _name_matches(team_name: str, rivalry_name: str) -> bool:
    """Substring match in either direction.

    Both arguments must already be normalized via _normalize. We allow either
    direction so e.g. 'manchester city' (rivalry) matches 'manchester city fc'
    (team) AND 'paris sg' (team) matches 'paris saint-germain' (rivalry: if
    the file stored the longer form).
    """
    return rivalry_name in team_name or team_name in rivalry_name
