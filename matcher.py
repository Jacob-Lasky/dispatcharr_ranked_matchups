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

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups.matcher")


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
    channel_id: Optional[int] = None
    channel_name: Optional[str] = None
    program_title: Optional[str] = None
    method: str = "unmatched"  # 'regex_unique', 'llm', 'unmatched'
    note: str = ""


def _team_keywords(team_name: str) -> List[str]:
    """Build keyword variants for a team name to use in EPG title regex pre-filter.

    Returns ordered list of progressively-relaxed keywords.
    """
    name = team_name.strip()
    keywords = [name]
    # last word (often distinctive: "Penn State" → "State", "Ohio State" → "State"
    # is too generic; we'll handle that in matching by requiring BOTH teams' keywords)
    parts = name.split()
    if len(parts) > 1 and parts[-1].lower() not in ("state", "college", "university"):
        keywords.append(parts[-1])
    if len(parts) >= 2:
        # First two words
        keywords.append(" ".join(parts[:2]))
    return keywords


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
        filtered = _regex_filter(candidates, game.home, game.away)
        if len(filtered) == 1:
            c = filtered[0]
            results[i].channel_id = c.channel_id
            results[i].channel_name = c.channel_name
            results[i].program_title = c.program_title
            results[i].method = "regex_unique"
        elif len(filtered) == 0:
            # Try LLM with a wider net (all candidates in the time window).
            ambiguous.append((i, game, candidates[:30]))
        else:
            # Multiple regex matches — Claude resolves.
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
            results[idx].method = "llm"
    elif ambiguous:
        # No API key — best-effort: pick first candidate to surface SOMETHING.
        for idx, _game, cands in ambiguous:
            if cands:
                c = cands[0]
                results[idx].channel_id = c.channel_id
                results[idx].channel_name = c.channel_name
                results[idx].program_title = c.program_title
                results[idx].method = "fallback_first"
                results[idx].note = "no api key; first candidate"

    return results
