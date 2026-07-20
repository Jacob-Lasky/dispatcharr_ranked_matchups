"""Curated trophy/honours grounding for match previews.

`honours.json` (next to this module) maps a competition key to
`{team_name: title_value}`, where `title_value` is either a list of winning
years or a plain integer count. At plugin import the file is read once.

`honours_lines(home, away, fd_competition_code, tournament_stage)` returns 0-1
context lines naming each finalist's title count in the competition being
played, so the preview can say something TRUE about history instead of
inventing it. This is the load-bearing fix for the "going for their third
crown" bug: a World Cup final (Spain 1 title, Argentina 3) where the LLM,
given only team names, fabricated a shared "third title" narrative that was
false for both sides.

Scope (Jake's call, 2026-07-19): KNOCKOUT games only, and only the three
competitions the plugin actually sources whose honours we track:
  WC -> World Cup, EC -> European Championship, CL -> Champions League.
Group-stage games and every other competition get nothing.

Completeness matters for the zero-title side. For the national-team
tournaments (WC, EURO) the winners set is small and FINITE, so `honours.json`
lists EVERY winner; a finalist absent from the list therefore has ZERO titles
and we state that explicitly ("no World Cup titles yet"). Saying zero out loud
is what actually kills the hallucination for the trophyless finalist. For the
Champions League the winners list is broad and NOT guaranteed complete here, so
an absent club is omitted rather than falsely asserted to have none.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

_HONOURS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "honours.json")

# A team's stored honours: a list of winning years, or a bare count.
TitleValue = Union[int, List[int]]

# fd_competition_code -> (honours.json key, complete?, display name).
# complete=True: the JSON lists EVERY winner, so an absent finalist has zero
# titles and we say so. complete=False: the list is partial, so omit absent
# teams (never assert zero for a club we simply didn't enumerate).
_COMPETITION_MAP: Dict[str, tuple] = {
    "WC": ("WC", True, "World Cup"),
    "EC": ("EURO", True, "European Championship"),
    "CL": ("UCL", False, "Champions League"),
}


def _normalize(name: str) -> str:
    """Lowercase + collapse whitespace, matching rivalries.py so team-name
    lookup behaves identically across the two curated files."""
    return " ".join((name or "").lower().split())


def _load_honours() -> Dict[str, Dict[str, TitleValue]]:
    """Load and normalize the honours map. Returns {comp_key: {norm_team: value}}.

    Failure modes (file missing, JSON corrupt) log a warning and return an
    empty map so the plugin still works: honours are an enhancement, not a
    hard dependency. Keys starting with "_" (comments/notes) are skipped at
    both nesting levels. `bool` is rejected explicitly: it is an `int`
    subclass in Python and a stray `true` must not read as one title.
    """
    try:
        with open(_HONOURS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("honours.json missing at %s; honours grounding disabled", _HONOURS_PATH)
        return {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("honours.json load failed (%s); honours grounding disabled", e)
        return {}
    out: Dict[str, Dict[str, TitleValue]] = {}
    for comp, teams in raw.items():
        if comp.startswith("_") or not isinstance(teams, dict):
            continue
        clean: Dict[str, TitleValue] = {}
        for team, val in teams.items():
            if team.startswith("_"):
                continue
            if isinstance(val, bool):
                continue
            if isinstance(val, int) and val > 0:
                clean[_normalize(team)] = val
            elif isinstance(val, list) and val and all(isinstance(y, int) for y in val):
                clean[_normalize(team)] = sorted(val)
        if clean:
            out[comp] = clean
    return out


# Loaded once per process. Plugin reload re-imports the module so edits to
# honours.json land on the next .reload_token bump (same lifecycle as
# rivalries.json / team_aliases.json).
_HONOURS = _load_honours()


def _name_matches(team_name: str, honours_name: str) -> bool:
    """Bidirectional substring match on already-normalized names, so the FD
    club name 'Real Madrid CF' matches the stored 'Real Madrid' and vice
    versa. National-team names arrive exact, so this only ever helps clubs."""
    return honours_name in team_name or team_name in honours_name


def _lookup(team: str, comp: Dict[str, TitleValue]) -> Optional[TitleValue]:
    """Return the team's title value for this competition, or None if absent.
    Exact normalized match first, then bidirectional substring."""
    t = _normalize(team)
    if not t:
        return None
    if t in comp:
        return comp[t]
    for name, val in comp.items():
        if _name_matches(t, name):
            return val
    return None


def _count(val: TitleValue) -> int:
    return val if isinstance(val, int) else len(val)


def _ordinal(n: int) -> str:
    """1 -> 1st, 2 -> 2nd, 4 -> 4th, 11 -> 11th. Standalone copy (honours.py is
    loaded by file path in tests, so it cannot import plugin._ordinal)."""
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _phrase(
    team: str, val: Optional[TitleValue], comp_label: str, is_final: bool
) -> Optional[str]:
    """One team's honours phrase. `val is None` is only passed for COMPLETE
    competitions (absent => genuinely zero); partial competitions omit absent
    teams before calling this.

    For a FINAL we spell out the conclusion ("a win would be their 4th") rather
    than only the raw count. Load-bearing: given just "Spain 1 / Argentina 3",
    Haiku 4.5 still wrote "three titles apiece" for the 2026 WC final. Stating
    the outcome removes the arithmetic the model kept getting wrong; a win in a
    final is a title, so "their Nth" is exactly count+1.
    """
    if val is None:
        base = f"{team} — no {comp_label} titles yet"
        return f"{base}, a win would be their 1st" if is_final else base
    n = _count(val)
    if n <= 0:
        return None
    noun = "title" if n == 1 else "titles"
    if isinstance(val, list):
        if len(val) <= 5:
            detail = ", ".join(str(y) for y in val)
        else:
            detail = f"most recent {max(val)}"
        base = f"{team} — {n} {noun} ({detail})"
    else:
        base = f"{team} — {n} {noun}"
    if is_final:
        return f"{base}, a win would be their {_ordinal(n + 1)}"
    return base


def honours_lines(
    home: str,
    away: str,
    fd_competition_code: Optional[str],
    tournament_stage: Optional[str],
) -> List[str]:
    """Return the honours context lines for a game, or an empty list.

    Fires only for knockout games in a tracked, sourced competition. Returns
    at most one line of the form:
        "Honours (World Cup): Spain — 1 title (2010); Argentina — 3 titles
         (1978, 1986, 2022)."
    """
    if not fd_competition_code:
        return []
    mapping = _COMPETITION_MAP.get(fd_competition_code)
    if not mapping:
        return []
    honours_key, complete, label = mapping

    # Knockout only. FD tags group games "GROUP_STAGE"; everything else on
    # these three competitions is a knockout round (incl. the final).
    stage = (tournament_stage or "").upper()
    if not stage or stage == "GROUP_STAGE":
        return []

    comp = _HONOURS.get(honours_key)
    if not comp:
        return []

    is_final = stage == "FINAL"
    phrases: List[str] = []
    for team in (home, away):
        if not team:
            continue
        val = _lookup(team, comp)
        if val is None and not complete:
            # Partial list: don't assert zero for a club we simply didn't list.
            continue
        phrase = _phrase(team, val, label, is_final)
        if phrase:
            phrases.append(phrase)

    if not phrases:
        return []
    return [f"Honours ({label}): " + "; ".join(phrases) + "."]
