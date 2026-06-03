"""Optional LLM-rewritten EPG descriptions (Claude Haiku 4.5 by default).

When `llm_descriptions_enabled` is on AND a valid Anthropic API key is present,
the apply step calls `llm_describe_or_fallback` per matched game and writes the
returned prose to `ProgramData.description` instead of the deterministic
`_build_description` output.

Failure modes (missing key, API error, non-200, malformed body, network
timeout, JSON decode) all return `fallback_description` unchanged. The cache
file is a sidecar: cache.json's structured fields (score, breakdown,
score_notes) stay deterministic and untouched.

The HTTP call is intentionally `urllib` (stdlib only): Dispatcharr does not
ship the `anthropic` SDK and we do not want to add a transitive dependency
for a 30-line wrapper.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional

from ._util import series_phase_text, series_record_text, series_result_lines

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_VERSION = "2023-06-01"
ANTHROPIC_MAX_TOKENS = 200
ANTHROPIC_TIMEOUT_S = 30

# Lifted verbatim from /tmp/haiku_demo.py (the four-game validation set in
# TUNING_REPORT.md, sample outputs section), with one addition: the explicit
# plain-text rule. The demo run produced one markdown emphasis (`*must*`) that
# would render as literal asterisks in TiviMate / Plex / Jellyfin.
SYSTEM_PROMPT = """You write 2-3 sentence previews for a personal TV guide.
Goal: make a casual fan want to watch this match.

Hard rules:
- No numbers like "★9.9" or "rank=8.12". Natural prose only.
- Don't mention "favorite", "stakes", "leverage", "score breakdown". Describe
  what they mean (the team's predicament, what's at stake) instead.
- 2-3 sentences. Max ~50 words.
- Skip generic openings ("This match features..."). Drop the reader into the
  stakes.
- If a favorite team for this user is playing, that's a "personal interest":
  ground the preview in their angle.
- Plain text only. No markdown, no asterisks, no bullet points.
- Do NOT fabricate playoff series facts. The only series facts you may use are
  the "Series", "Series record", and "Results so far" lines above. Never invent
  a series score, a game number, or a best-of-N length.
- Do not write "facing elimination", "must win to force a Game 7", or "their
  season ends tonight" unless the given Series record makes it literally true
  (a team one loss from elimination in a best-of-N). If no series lines are
  given the match may still be a one-off knockout, so general "win or go home"
  framing is fine, but never assert a series standing or game number.
"""

# How many standings rows above/below each team to include in the context.
# 2 captures the immediate threshold (e.g. relegation cutoff is one row below
# 17th place) without flooding the prompt with bottom-table teams.
_STANDINGS_CONTEXT_WINDOW = 2


# Type alias: the seam used by tests. A `Caller` takes (context, api_key,
# model) and returns the assistant text. Production wires this to
# `_call_anthropic`; tests pass a stub.
Caller = Callable[[str, str, str], str]


def _format_team_row(row: Dict[str, Any]) -> str:
    name = row.get("name") or "?"
    pos = row.get("position") or "?"
    pts = row.get("points")
    played = row.get("played")
    qualifiers: List[str] = []
    if pts is not None:
        qualifiers.append(f"{pts} pts")
    if played is not None:
        qualifiers.append(f"{played} games played")
    base = f"  - #{pos} {name}"
    if qualifiers:
        return base + " (" + ", ".join(qualifiers) + ")"
    return base


def _standings_window(table: List[Dict[str, Any]], focus_positions: List[Any]) -> List[Dict[str, Any]]:
    """Return standings rows within ±_STANDINGS_CONTEXT_WINDOW of any focus
    position, plus the leader (#1) so the model can frame the title race.
    Deduplicated and sorted by position.
    """
    if not table:
        return []
    by_pos: Dict[int, Dict[str, Any]] = {}
    for r in table:
        pos = r.get("position")
        if isinstance(pos, int):
            by_pos[pos] = r
    if not by_pos:
        return []
    wanted = set()
    wanted.add(min(by_pos))  # leader
    for fp in focus_positions:
        if not isinstance(fp, int):
            continue
        for p in range(fp - _STANDINGS_CONTEXT_WINDOW, fp + _STANDINGS_CONTEXT_WINDOW + 1):
            if p in by_pos:
                wanted.add(p)
    return [by_pos[p] for p in sorted(wanted)]


def build_llm_context(g: Dict[str, Any], tagline: str, boundary_summary: str = "") -> str:
    """Build the user-message context block for the LLM.

    Pulls only from data already in the cache row: no new API calls. The
    standings window includes the two teams playing plus their immediate
    neighbors, so the model can write "one point above the drop zone" without
    being told the threshold name.
    """
    extra = g.get("extra") or {}
    home = g.get("home") or "?"
    away = g.get("away") or "?"
    sport_label = g.get("sport_label") or g.get("sport_prefix") or "?"
    kickoff_local = g.get("kickoff_local") or "?"

    lines: List[str] = []
    lines.append(f"Match: {away} at {home}, {kickoff_local}")
    lines.append(f"Competition: {sport_label}")

    # Playoff series grounding (best-of-N sources populate extra["series"]).
    # This is the load-bearing fix for the false-"elimination" bug: without the
    # record and game number, the model invents playoff drama. The SYSTEM_PROMPT
    # hard rule below forbids guessing series state when these lines are absent.
    series = extra.get("series")
    series_phase = series_phase_text(series)
    if series_phase:
        lines.append(f"Series: {series_phase}")
    series_record = series_record_text(series, home, away)
    if series_record:
        lines.append(f"Series record: {series_record}")
    series_recap = series_result_lines(series)
    if series_recap:
        lines.append("Results so far:")
        for recap_line in series_recap:
            lines.append(f"  - {recap_line}")

    matchday = extra.get("matchday")
    matchdays_total = extra.get("matchdays_total")
    if matchday and matchdays_total:
        lines.append(f"Matchday: {matchday} of {matchdays_total}")
    elif extra.get("week"):
        lines.append(f"Week: {extra['week']}")

    if boundary_summary:
        lines.append(f"League boundaries: {boundary_summary}")

    standings = extra.get("standings_table") or []
    window = _standings_window(standings, [g.get("rank_home"), g.get("rank_away")])
    if window:
        lines.append("Standings (relevant slice):")
        for row in window:
            lines.append(_format_team_row(row))

    favorites_matched = g.get("favorites_matched") or []
    if favorites_matched:
        lines.append(f"User's favorite teams playing: {', '.join(favorites_matched)}")

    impact_narratives = (
        extra.get("impact_narratives")
        or g.get("impact_narratives")
        or []
    )
    if impact_narratives:
        lines.append("Affects user's other favorites:")
        for narrative in impact_narratives:
            lines.append(f"  - {narrative}")

    # Older cache files stored the band list under `stakes_thresholds_hit`;
    # accept either key so a cache.json written by an older plugin version
    # still produces a reasonable prompt during the one-cycle migration
    # window.
    thresholds_hit = (
        g.get("importance_thresholds_hit")
        or g.get("stakes_thresholds_hit")
        or []
    )
    if thresholds_hit:
        lines.append(f"Outcome bands in play: {', '.join(thresholds_hit)}")

    if tagline:
        lines.append(f"Editorial frame (use as a hint, do not quote): {tagline}")

    closeness = g.get("closeness")
    if isinstance(closeness, (int, float)) and closeness >= 0.7:
        lines.append("Bookmaker view: toss-up")

    return "\n".join(lines)


def prompt_hash(context: str, model: str) -> str:
    """Stable short hash used as part of the cache key. Folds in the model AND
    the SYSTEM_PROMPT so that a model swap OR a system-prompt edit invalidates
    cached prose without a manual cache-bust. The system prompt matters because
    it carries the behavioral rules (e.g. the anti-"elimination" guardrail): a
    cache keyed only on the user-message context would keep serving prose
    written under the OLD rules after a prompt tightening.
    """
    h = hashlib.sha256()
    h.update(model.encode("utf-8"))
    h.update(b"\x00")
    h.update(SYSTEM_PROMPT.encode("utf-8"))
    h.update(b"\x00")
    h.update(context.encode("utf-8"))
    return h.hexdigest()[:16]


def _call_anthropic(context: str, api_key: str, model: str) -> str:
    """Call Anthropic /v1/messages and return the assistant text block.

    Raises on any non-200, missing key, network error, malformed body, or
    empty response. The caller (`llm_describe_or_fallback`) catches and
    falls back to the deterministic description.
    """
    body = json.dumps({
        "model": model,
        "max_tokens": ANTHROPIC_MAX_TOKENS,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": context}],
    }).encode("utf-8")
    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_API_VERSION,
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=ANTHROPIC_TIMEOUT_S) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    blocks = data.get("content") or []
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str):
            return b["text"].strip()
    raise ValueError("anthropic response had no text block")


def llm_describe_or_fallback(
    g: Dict[str, Any],
    tagline: str,
    fallback_description: str,
    api_key: str,
    model: str,
    cache: Dict[str, str],
    boundary_summary: str = "",
    marker: str = "",
    caller: Optional[Caller] = None,
) -> str:
    """Return LLM-rewritten prose, or `fallback_description` on any failure.

    Mutates `cache` in place on a successful call (caller is responsible for
    persisting the cache dict to disk). The enable-flag, the API-key existence
    check, and the placeholder skip are the caller's job: this function
    assumes it should attempt the call.
    """
    context = build_llm_context(g, tagline, boundary_summary)
    # Use `|` separator: markers contain ':' (`ranked_matchups:EPL:538161`),
    # so a single-':' split would clip everything before the last colon.
    cache_key = f"{marker}|{prompt_hash(context, model)}"
    cached = cache.get(cache_key)
    if cached:
        return cached
    fn = caller or _call_anthropic
    try:
        prose = fn(context, api_key, model)
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError) as exc:
        logger.warning("[ranked_matchups] LLM describe failed for %s: %s", marker or "?", exc)
        return fallback_description
    if not prose:
        return fallback_description
    cache[cache_key] = prose
    return prose


# ---------- cache file I/O ----------

def read_cache(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    # Only keep string-valued entries; defensively drop anything else.
    return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def write_cache(path: str, cache: Dict[str, str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True, ensure_ascii=False)
    os.replace(tmp, path)


def prune_cache(cache: Dict[str, str], live_markers: set) -> Dict[str, str]:
    """Drop cache entries whose marker is not in the current refresh's
    `seen_markers` set. Bounds the file to live games only.

    Cache key shape is `<marker>|<prompt_hash>`. The marker itself contains
    `:` (`ranked_matchups:EPL:538161`), which is why this splits on `|`
    rather than `:`.
    """
    return {k: v for k, v in cache.items() if k.split("|", 1)[0] in live_markers}
