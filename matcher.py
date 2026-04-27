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
import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ._util import GENERIC_TEAM_SECOND_WORDS, TEAM_SUFFIX_TOKENS

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.matcher")

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
    """
    name = team_name.strip()
    keywords = [name]
    parts = name.split()

    # Strip trailing club tag for soccer-style names so 'Brentford FC' also
    # matches 'Brentford' in a channel/program title.
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
    return list(dict.fromkeys(keywords))


def _regex_filter(
    candidates: List[ChannelCandidate],
    team_a: str,
    team_b: str,
) -> List[ChannelCandidate]:
    """Programs whose title contains references to BOTH teams."""
    a_kws = _team_keywords(team_a)
    b_kws = _team_keywords(team_b)
    out = []
    for c in candidates:
        title = c.program_title.lower()
        a_hit = any(kw.lower() in title for kw in a_kws)
        b_hit = any(kw.lower() in title for kw in b_kws)
        if a_hit and b_hit:
            out.append(c)
    return out


def _regex_filter_channel_name(
    candidates: List[ChannelCandidate],
    team_a: str,
    team_b: str,
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
    """
    a_kws = _team_keywords(team_a)
    b_kws = _team_keywords(team_b)
    out = []
    for c in candidates:
        name = (c.channel_name or "").lower()
        a_hit = any(kw.lower() in name for kw in a_kws)
        b_hit = any(kw.lower() in name for kw in b_kws)
        if a_hit and b_hit:
            out.append(c)
    return out


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
    "matches 'NC State at Notre Dame'). If none of the candidates plausibly broadcasts the "
    "game, return null for that game. "
    "Output ONLY a JSON object: {\"<game_id>\": <channel_id_or_null>, ...}. "
    "No prose, no markdown."
)


def match_games_to_channels(
    scored_games: List[Tuple[Any, Any, Any]],  # (GameRow, GameSignals, GameScore)
    epg_lookup,  # callable: GameRow -> List[ChannelCandidate]
    api_key: str,
    model: str,
) -> List[MatchResult]:
    """Resolve each game to a Dispatcharr channel.

    epg_lookup: callable that, given a GameRow, returns candidate channels
    broadcasting around that time. Caller (plugin.py) provides this with a
    closure over Dispatcharr's ORM.
    """
    results: List[MatchResult] = [MatchResult(game_index=i) for i in range(len(scored_games))]
    ambiguous: List[Tuple[int, Any, List[ChannelCandidate]]] = []

    for i, (game, _signals, _score) in enumerate(scored_games):
        candidates = epg_lookup(game)
        if not candidates:
            results[i].note = "no EPG candidates in time window"
            continue

        # Tier 1 (strongest signal): channels whose NAME contains both teams.
        # These are dedicated match channels — typically multiple regional /
        # quality variants of the same fixture. Stack all of them.
        strict = _regex_filter_channel_name(candidates, game.home, game.away)
        if strict:
            primary = strict[0]
            results[i].channel_id = primary.channel_id
            results[i].channel_name = primary.channel_name
            results[i].program_title = primary.program_title
            # De-dupe channel_ids while preserving order (a single channel
            # can have multiple ProgramData rows that pass the filter).
            seen_ids = set()
            for c in strict:
                if c.channel_id not in seen_ids:
                    results[i].channel_ids.append(c.channel_id)
                    seen_ids.add(c.channel_id)
            results[i].method = "regex_strict"
            continue

        # Tier 2: program-title regex, with previews ('Next Game:', 'Preview:',
        # 'Pre-game ...') stripped — those mark team-branded home channels
        # that surface upcoming-game EPG cards but don't broadcast the match.
        filtered = _strip_preview_titles(
            _regex_filter(candidates, game.home, game.away)
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
                ambiguous.append((i, game, wider))
        else:
            # Multiple regex matches survived preview stripping — Claude resolves.
            ambiguous.append((i, game, filtered))

    if ambiguous and api_key:
        # One batch Claude call.
        payload = []
        for idx, game, cands in ambiguous:
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
        for idx, game, cands in ambiguous:
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
            results[idx].channel_ids = [chosen.channel_id]
            results[idx].method = "llm"
    elif ambiguous:
        # No API key — best-effort: pick first candidate to surface SOMETHING.
        for idx, _game, cands in ambiguous:
            if cands:
                c = cands[0]
                results[idx].channel_id = c.channel_id
                results[idx].channel_name = c.channel_name
                results[idx].program_title = c.program_title
                results[idx].channel_ids = [c.channel_id]
                results[idx].method = "fallback_first"
                results[idx].note = "no api key; first candidate"

    return results
