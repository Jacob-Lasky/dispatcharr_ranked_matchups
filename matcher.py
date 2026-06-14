"""Match scored games to Dispatcharr channels via EPG ProgramData.

Flow per game:
  1) Query ProgramData for programs airing during [game.start_time-30m, game.start_time+4h]
     across all sports-flagged channels.
  2) Regex pre-filter: programs whose title contains BOTH team identifiers (full
     name or last word as fallback).
  3) If exactly 1 candidate → match.
  4) If multiple → Claude picks the right one given the game context.
  5) If zero candidates → log and skip (provider may not carry this game).

Batch optimization: one Claude call resolves all ambiguous-match games together.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ._util import GENERIC_TEAM_SECOND_WORDS, TEAM_SUFFIX_TOKENS, is_field_event

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.matcher")


# Team-name aliases: broadcaster-side abbreviations broadcasters use in
# EPG titles ("Man United", "Man Utd") that don't appear in Football-Data.org's
# canonical names ("Manchester United FC"). Loaded once per process from
# team_aliases.json. See #4.
_ALIASES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "team_aliases.json")


def _load_team_aliases() -> Dict[str, List[str]]:
    """Load team-name aliases from team_aliases.json.

    Missing or malformed file logs a warning and returns empty dict: the
    matcher still works without aliases, just with the v1 keyword set.
    """
    try:
        with open(_ALIASES_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("[matcher] team_aliases.json missing at %s; matcher aliases disabled", _ALIASES_PATH)
        return {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[matcher] team_aliases.json load failed (%s); matcher aliases disabled", e)
        return {}
    out: Dict[str, List[str]] = {}
    for key, vals in raw.items():
        if key.startswith("_"):
            continue
        if isinstance(vals, list) and all(isinstance(v, str) for v in vals):
            out[key] = vals
    return out


_TEAM_ALIASES = _load_team_aliases()

# Last-word tokens we never use as a standalone keyword fallback. 'state' /
# 'college' / 'university' are college-football generic; the soccer
# second-words ('united', 'city', etc.) collide across many EPL/EFL clubs.
_GENERIC_LAST_WORDS = frozenset(
    {"state", "college", "university"} | set(GENERIC_TEAM_SECOND_WORDS)
)


@dataclass
class ChannelCandidate:
    channel_id: int
    channel_name: str
    program_title: str
    program_start: datetime
    program_end: datetime


@dataclass
class MatchResult:
    game_index: int           # index back into the scored games list
    channel_id: Optional[int] = None       # primary (first) match
    channel_name: Optional[str] = None
    program_title: Optional[str] = None
    # All matched channels for this game, primary first. Allows the caller
    # to stack multiple provider variants (different qualities/regions) onto
    # the virtual channel as fallback streams. Empty list means no match.
    channel_ids: List[int] = None  # type: ignore[assignment]
    # 'regex_strict' (channel name had both teams), 'regex_unique' (program
    # title regex matched exactly one non-preview), 'llm' (Claude picked from
    # multiple), 'fallback_first' (no API key, used first candidate),
    # 'unmatched'.
    method: str = "unmatched"
    note: str = ""

    def __post_init__(self):
        if self.channel_ids is None:
            self.channel_ids = []


def _team_keywords(team_name: str) -> List[str]:
    """Build keyword variants for a team name to use in EPG title regex pre-filter.

    Returns ordered list of progressively-relaxed keywords. Always deduped.
    Drops the last-word fallback for generic-suffix names so 'Manchester
    United' never reduces to just 'United' (which would false-match
    'Brentford v West Ham United').

    Pulls broadcaster aliases from team_aliases.json: "Manchester United"
    expands to include "Man United" / "Man Utd" / "Man U" / "MUFC" so
    abbreviated EPG titles still match. Lookup tries the canonical name
    AND its trailing-suffix-stripped form to catch FD.org names that
    arrive with "FC" / "AFC" appended.
    """
    name = team_name.strip()
    keywords = [name]
    parts = name.split()

    # Strip trailing club tag for soccer-style names so 'Brentford FC' also
    # matches 'Brentford' in a channel/program title.
    stripped: Optional[str] = None
    if len(parts) >= 2 and parts[-1].lower() in TEAM_SUFFIX_TOKENS:
        stripped = " ".join(parts[:-1])
        keywords.append(stripped)
        # Re-derive parts so subsequent rules see the canonical form.
        parts = stripped.split()

    if len(parts) > 1 and parts[-1].lower() not in _GENERIC_LAST_WORDS:
        keywords.append(parts[-1])
    if len(parts) >= 2:
        # First two words (only meaningful for 3+ word names; for 2-word names
        # this duplicates the full name and gets deduped below).
        keywords.append(" ".join(parts[:2]))

    # Broadcaster aliases (#4). Look up both the original name AND the
    # FC-stripped form because the JSON has "Manchester United" but FD.org
    # returns "Manchester United FC".
    for lookup in (name, stripped):
        if lookup and lookup in _TEAM_ALIASES:
            keywords.extend(_TEAM_ALIASES[lookup])

    return list(dict.fromkeys(keywords))


def _kw_hit(text: str, keywords: List[str]) -> bool:
    """Whether any keyword (case-insensitive substring) appears in `text`.

    Single source of truth for the substring-hit test every matcher tier uses
    (and the diagnose action reuses), so the matched-vs-explained logic can
    never drift apart. Tolerates None/empty text.
    """
    t = (text or "").lower()
    return any(kw.lower() in t for kw in keywords)


def _regex_filter(
    candidates: List[ChannelCandidate],
    team_a: str,
    team_b: Optional[str] = None,
) -> List[ChannelCandidate]:
    """Programs whose title references both teams.

    `team_b=None` is the single-sided mode for field events (#127): one event,
    no opponent, so we match on the event name (`team_a`) alone. The both-teams
    gate would otherwise be unsatisfiable against the "Field" away sentinel.
    """
    a_kws = _team_keywords(team_a)
    if team_b is None:
        return [c for c in candidates if _kw_hit(c.program_title, a_kws)]
    b_kws = _team_keywords(team_b)
    return [c for c in candidates
            if _kw_hit(c.program_title, a_kws) and _kw_hit(c.program_title, b_kws)]


def _regex_filter_channel_name(
    candidates: List[ChannelCandidate],
    team_a: str,
    team_b: Optional[str] = None,
) -> List[ChannelCandidate]:
    """Stricter filter: channels whose CHANNEL NAME contains both teams.

    This is how we identify true match-broadcast channels (e.g.
    'EPL01: Manchester United 20:00 Brentford 27/04') versus team-branded
    home channels (e.g. 'Manchester United') that happen to carry a
    'Next Game: ...' preview EPG entry naming both teams. The team-branded
    channels are NEVER the live broadcast and must not be matched.

    Providers typically carry the same fixture across multiple branded
    channels (US/AU/EU regional variants, different bitrates), all of which
    name both teams in the channel name. Returning the full set lets the
    caller stack them as fallback streams on the virtual channel.

    `team_b=None` is the single-sided mode for field events (#127): the event
    name (`team_a`) alone identifies the broadcast, since there is no opponent.
    """
    a_kws = _team_keywords(team_a)
    if team_b is None:
        return [c for c in candidates if _kw_hit(c.channel_name, a_kws)]
    b_kws = _team_keywords(team_b)
    return [c for c in candidates
            if _kw_hit(c.channel_name, a_kws) and _kw_hit(c.channel_name, b_kws)]


# Keywords that mark a program as a preview/highlight wrapper rather than the
# live broadcast. Team-branded home channels frequently emit "Next Game:"
# preview cards in their EPG that name both teams, which would otherwise pass
# the program-title regex filter and get picked by the LLM.
_PREVIEW_TITLE_PATTERNS = (
    "next game:",
    "coming up:",
    "coming up next",
    "preview:",
    "pregame ",
    "pre-game ",
    "post-game",
    "postgame",
    "highlights:",
    "highlights ",
)


def _is_preview_title(program_title: str) -> bool:
    if not program_title:
        return False
    t = program_title.lower()
    return any(pat in t for pat in _PREVIEW_TITLE_PATTERNS)


def _strip_preview_titles(
    candidates: List[ChannelCandidate],
) -> List[ChannelCandidate]:
    return [c for c in candidates if not _is_preview_title(c.program_title)]


def _post_claude(
    api_key: str,
    model: str,
    system: str,
    user: str,
    timeout: int = 60,
) -> Optional[Dict[str, Any]]:
    body = json.dumps({
        "model": model,
        "max_tokens": 4096,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except Exception as e:
        logger.error("[matcher] Claude call failed: %s", e)
        return None
    elapsed = time.time() - t0
    try:
        data = json.loads(raw)
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
        usage = data.get("usage", {})
        logger.info(
            "[matcher] Claude call %.1fs in=%s out=%s",
            elapsed, usage.get("input_tokens"), usage.get("output_tokens"),
        )
        return _extract_json(text)
    except Exception as e:
        logger.error("[matcher] parse failed: %s ; raw=%.500s", e, raw)
        return None


def _extract_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        text = text[start : end + 1]
    return json.loads(text)


MATCHER_SYSTEM_PROMPT = (
    "You match sports games to broadcasting EPG entries. Given a game ('home' vs 'away' "
    "with sport context) and a list of candidate EPG entries (channel + program title), "
    "return the channel_id of the EPG entry that's broadcasting THIS game. "
    "Use full team-name disambiguation (e.g., 'Penn State Nittany Lions vs Ohio State Buckeyes' "
    "matches 'Penn State at Ohio State'; 'NC State Wolfpack vs Notre Dame Fighting Irish' "
    "matches 'NC State at Notre Dame'). "
    # #4: non-English EPG titles. Foreign-language EPG entries are common
    # for European soccer (German DAZN, Spanish Movistar, Italian Sky,
    # French Canal+, Portuguese SportTV). Match on team names even when
    # the surrounding 'matchday' vocabulary is in another language:
    "EPG titles in other languages are common: match team names even when surrounding "
    "text is foreign. Common 'matchday' translations: DE Spieltag, ES jornada, "
    "IT giornata, FR journee, PT jornada. Common 'highlights' translations: "
    "DE Zusammenfassung, ES resumen, IT sintesi, FR resume. "
    "If none of the candidates plausibly broadcasts the "
    "game, return null for that game. "
    "Output ONLY a JSON object: {\"<game_id>\": <channel_id_or_null>, ...}. "
    "No prose, no markdown."
)


def _stack_fallback_ids(
    primary_id: int, cands: List[ChannelCandidate]
) -> List[int]:
    """Primary id first, then every other candidate id, deduped in order.

    Used by the #108 widen path to stack same-fixture provider variants as
    fallback streams behind the chosen primary. A single channel can appear in
    `cands` more than once (multiple ProgramData rows), so dedupe.
    """
    out = [primary_id]
    seen = {primary_id}
    for c in cands:
        if c.channel_id not in seen:
            out.append(c.channel_id)
            seen.add(c.channel_id)
    return out


def match_games_to_channels(
    scored_games: List[Tuple[Any, Any, Any]],  # (GameRow, GameSignals, GameScore)
    epg_lookup,  # callable: GameRow -> List[ChannelCandidate]
    api_key: str,
    model: str,
    widen: bool = False,
) -> List[MatchResult]:
    """Resolve each game to a Dispatcharr channel.

    epg_lookup: callable that, given a GameRow, returns candidate channels
    broadcasting around that time. Caller (plugin.py) provides this with a
    closure over Dispatcharr's ORM.

    widen (#108): when True, the LLM-disambiguated tier stacks the non-chosen
    candidates as fallback streams behind the primary, INSTEAD of discarding
    them. Off by default. Only the both-team candidate set (the `filtered`
    tier-2 matches the LLM picks among) is stacked: a candidate that names just
    one team is a different-game risk, so the zero-both-team `wider` path is
    never widened even when `widen` is True. The tier-1 regex_strict path
    already stacks every channel-name variant and is unaffected by this flag.
    """
    results: List[MatchResult] = [MatchResult(game_index=i) for i in range(len(scored_games))]
    # Each entry: (game_index, game, candidates, both_team). `both_team` is True
    # only when every candidate named BOTH teams (tier-2 multi-match), which is
    # the precondition for #108 widening.
    ambiguous: List[Tuple[int, Any, List[ChannelCandidate], bool]] = []

    for i, (game, _signals, _score) in enumerate(scored_games):
        candidates = epg_lookup(game)
        if not candidates:
            results[i].note = "no EPG candidates in time window"
            continue

        # Field events (UFC/F1/golf/NASCAR/ATP/WTA, #127) have no opponent: the
        # away side is the "Field" sentinel, which no channel or title ever
        # names. Drop the both-teams gate and match on the event name (home)
        # alone, exactly as field_event.py's design contract assumes. Two-team
        # games keep `match_away = game.away` and the full both-teams gate.
        match_away = None if is_field_event(game.away, getattr(game, "extra", None)) else game.away

        # Tier 1 (strongest signal): channels whose NAME contains both teams
        # (or, for field events, the event name). These are dedicated match
        # channels: typically multiple regional / quality variants of the same
        # fixture. Stack all of them.
        strict = _regex_filter_channel_name(candidates, game.home, match_away)
        if strict:
            primary = strict[0]
            results[i].channel_id = primary.channel_id
            results[i].channel_name = primary.channel_name
            results[i].program_title = primary.program_title
            # De-dupe channel_ids while preserving order (a single channel can
            # have multiple ProgramData rows that pass the filter). Tier-1
            # always stacks every channel-name variant: this is the original
            # multi-stream path the #108 widen flag generalizes to lower tiers,
            # so it shares the same stacking helper.
            results[i].channel_ids = _stack_fallback_ids(primary.channel_id, strict)
            results[i].method = "regex_strict"
            continue

        # Tier 2: program-title regex, with previews ('Next Game:', 'Preview:',
        # 'Pre-game ...') stripped: those mark team-branded home channels
        # that surface upcoming-game EPG cards but don't broadcast the match.
        filtered = _strip_preview_titles(
            _regex_filter(candidates, game.home, match_away)
        )
        if len(filtered) == 1:
            c = filtered[0]
            results[i].channel_id = c.channel_id
            results[i].channel_name = c.channel_name
            results[i].program_title = c.program_title
            results[i].channel_ids = [c.channel_id]
            results[i].method = "regex_unique"
        elif len(filtered) == 0:
            # Tier 3: LLM with a wider net (all non-preview candidates in
            # the time window). Candidates are already pre-filtered upstream
            # by epg_lookup to only programs whose title or channel name
            # mentions a team keyword, so the count is naturally small.
            wider = _strip_preview_titles(candidates)
            if wider:
                # both_team=False: these matched only a team keyword, not both
                # teams, so they are NOT eligible for #108 fallback stacking.
                ambiguous.append((i, game, wider, False))
        else:
            # Multiple regex matches survived preview stripping: Claude resolves.
            # both_team=True: every candidate named both teams, so the
            # non-chosen ones are same-fixture variants safe to stack (#108).
            ambiguous.append((i, game, filtered, True))

    if ambiguous and api_key:
        # One batch Claude call.
        payload = []
        for idx, game, cands, _both in ambiguous:
            payload.append({
                "game_id": str(idx),
                "sport": game.sport_label,
                "home": game.home,
                "away": game.away,
                "start_time_utc": game.start_time.isoformat(),
                "candidates": [
                    {
                        "channel_id": c.channel_id,
                        "channel_name": c.channel_name,
                        "program_title": c.program_title,
                    }
                    for c in cands
                ],
            })
        user = "Match each game to its broadcasting channel from the candidates. JSON only.\n\n" + json.dumps(payload, ensure_ascii=False)
        parsed = _post_claude(api_key, model, MATCHER_SYSTEM_PROMPT, user) or {}
        for idx, game, cands, both_team in ambiguous:
            picked = parsed.get(str(idx))
            if picked is None:
                results[idx].note = f"LLM no match among {len(cands)} candidates"
                continue
            try:
                picked_id = int(picked)
            except (TypeError, ValueError):
                results[idx].note = f"LLM returned bad id: {picked!r}"
                continue
            chosen = next((c for c in cands if c.channel_id == picked_id), None)
            if chosen is None:
                results[idx].note = f"LLM picked id={picked_id} not in candidates"
                continue
            results[idx].channel_id = chosen.channel_id
            results[idx].channel_name = chosen.channel_name
            results[idx].program_title = chosen.program_title
            # #108: stack the other both-team variants as fallback streams when
            # widen is on; otherwise keep the historical single-channel result.
            if widen and both_team:
                results[idx].channel_ids = _stack_fallback_ids(chosen.channel_id, cands)
            else:
                results[idx].channel_ids = [chosen.channel_id]
            results[idx].method = "llm"
    elif ambiguous:
        # No API key: best-effort: pick first candidate to surface SOMETHING.
        for idx, _game, cands, both_team in ambiguous:
            if cands:
                c = cands[0]
                results[idx].channel_id = c.channel_id
                results[idx].channel_name = c.channel_name
                results[idx].program_title = c.program_title
                # #108: same widen rule as the LLM path. Without an API key we
                # cannot disambiguate, so the first candidate is primary and the
                # rest stack only when they all name both teams.
                if widen and both_team:
                    results[idx].channel_ids = _stack_fallback_ids(c.channel_id, cands)
                else:
                    results[idx].channel_ids = [c.channel_id]
                results[idx].method = "fallback_first"
                results[idx].note = "no api key; first candidate"

    return results
