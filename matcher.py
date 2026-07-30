"""Match scored games to Dispatcharr channels / streams.

`_build_epg_lookup` (in plugin.py) supplies candidates from three sources:
  Path A - EPG ProgramData whose programme TITLE names a team, in the game's
           broadcast window (whole-channel: all the channel's streams attach).
  Path B - channels whose NAME names both teams (whole-channel).
  Path C - STREAMS whose name names both teams (stream-granular: only that one
           stream attaches, not the parent channel's others). Candidates carry
           a stream_id and a negative sentinel channel_id.

Tiers per game (match_games_to_channels):
  Tier 1 (regex_strict): a CHANNEL NAME (Path B) or STREAM NAME (Path C) names
     both teams. Highest confidence. MERGES the Tier-2 program-title both-team
     matches behind it (the live broadcasters) as fallback streams instead of
     discarding them.
  Tier 2 (regex_unique): exactly one non-preview programme title names both
     teams → match it.
  Tier 3 (llm / fallback_first): multiple or zero strict/title matches → Claude
     disambiguates (one batched call for all ambiguous games); with no API key,
     the first candidate is used.

The result splits matched targets into channel_ids (whole-channel) and
stream_ids (stream-granular Path C) via _partition_attach_targets.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
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
# 'york' is here for the same reason as the bare-city rule below (#162): MLS
# writes one club as 'Red Bull New York', whose last word 'York' is long enough
# to survive _is_weak_last_word yet substring-matches EVERY New York franchise
# in the guide. Suppressing it promotes the genuinely discriminating 'Red Bull'
# prefix instead. Extended here rather than in _util's
# GENERIC_TEAM_SECOND_WORDS because that tuple is the shared soccer
# second-word list, and this is a matcher-only keyword concern.
_GENERIC_LAST_WORDS = frozenset(
    {"state", "college", "university", "york"} | set(GENERIC_TEAM_SECOND_WORDS)
)


def _is_weak_last_word(token: str) -> bool:
    """Whether a last-word fallback keyword would substring-match wholesale.

    The last-word fallback in _team_keywords() relaxes a name down to its final
    token so abbreviated EPG titles still hit. But a bare NUMBER or a 1-2 char
    token is not a discriminator: it appears inside a huge fraction of channel
    names and numbers, so as a case-insensitive substring it matches almost
    everything. DO NOT emit it as a standalone keyword. This is exactly how a
    field-event card whose title ends in a rematch number reduced to a wildcard:
    'UFC 329: McGregor vs. Holloway 2' -> last word '2' -> 10593 tier-1 matches
    (NASCAR/BMX/MLB feeds, 'TVA Sports 2', 'Dota 2', ...). Same shape the boxing
    source pre-empts at the source ('... IBA Pro 19' -> '19'); this is the
    shared-matcher backstop so EVERY source (UFC, and any future field event) is
    protected, not just the ones that remembered to sanitise their own title.
    The full-name and first-two-words keywords remain the real discriminators,
    so dropping this bonus token only removes false positives, never the match.

    DO NOT retire this in favour of the short-keyword word-boundary rule
    (_SHORT_KEYWORD_MAX_CHARS): they solve different problems. Boundaries stop a
    short token matching INSIDE a word; they do nothing about a token that is a
    legitimate standalone word everywhere, which is what a bare digit is
    ('\\b2\\b' still hits 'UFC 2', 'Round 2', 'Group 2'). This rule drops such a
    token from the keyword set entirely; the boundary rule tightens the ones
    that survive.
    """
    t = token.strip().strip(".:,-")
    return t.isdigit() or len(t) < 3


@dataclass
class ChannelCandidate:
    channel_id: int
    channel_name: str
    program_title: str
    program_start: datetime
    program_end: datetime
    # Path C (stream-name match): when set, this candidate is a SPECIFIC stream
    # whose NAME named the game, NOT a whole channel. The apply attaches only
    # this stream, not the parent channel's other (unrelated) streams. None =
    # whole-channel candidate (Path A EPG title / Path B channel name): every
    # stream on the channel attaches. For stream candidates channel_id is a
    # negative sentinel (-stream_id), never a real PK, so the partition below
    # never expands a parent channel for them.
    stream_id: Optional[int] = None
    # Programme sub-title + description, when the source had them (#143).
    # European public broadcasters put a generic competition name in the title
    # and the actual fixture in these fields, so the both-teams regex tier has
    # to see them or the game only ever reaches the LLM. Empty for Path B / C
    # candidates, which have no programme behind them.
    program_extra: str = ""


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
    # Explicit stream IDs to attach (stream-granular: Path C stream-name
    # matches), separate from channel_ids (whole-channel matches whose every
    # stream attaches). The apply stacks BOTH, deduped by stream id. Empty
    # unless a stream-name candidate won a tier.
    stream_ids: List[int] = None  # type: ignore[assignment]
    # 'regex_strict' (channel name OR stream name had both teams), 'regex_unique'
    # (program title regex matched exactly one non-preview), 'llm' (Claude picked
    # from multiple), 'fallback_first' (no API key, used first candidate),
    # 'unmatched'.
    method: str = "unmatched"
    note: str = ""

    def __post_init__(self):
        if self.channel_ids is None:
            self.channel_ids = []
        if self.stream_ids is None:
            self.stream_ids = []


def _team_keywords_split(team_name: str) -> Tuple[List[str], List[str]]:
    """Split a team's keyword variants into (strong, weak).

    STRONG keywords identify the team on their own, so they are safe anywhere,
    including a single-keyword admission (Path A's programme-title pre-filter, a
    field event's single-sided gate, the whole-channel stream gate).

    WEAK keywords are bare place-name relaxations: 'New York' from 'New York
    Yankees', 'San Jose' from 'San Jose Earthquakes'. They are ONLY trustworthy
    when the OTHER side of the fixture also hits the same text, because a metro
    is shared by every franchise in it. Admitting on a weak keyword alone is
    what put the NFL Giants and Jets in an MLB Yankees game's candidate pool
    (#162). Requiring both sides makes them safe AND necessary: providers really
    do name feeds by city alone ('(Apple) (MLS) 006 | Cincinnati vs. San Jose'),
    and that is a correct Tier-1 match that only the weak keyword can find. DO
    NOT collapse the two lists back together at a single-keyword call site; that
    is the whole distinction.
    """
    name = team_name.strip()
    strong = [name]
    weak: List[str] = []
    parts = name.split()

    # Strip trailing club tag for soccer-style names so 'Brentford FC' also
    # matches 'Brentford' in a channel/program title.
    stripped: Optional[str] = None
    if len(parts) >= 2 and parts[-1].lower() in TEAM_SUFFIX_TOKENS:
        stripped = " ".join(parts[:-1])
        strong.append(stripped)
        # Re-derive parts so subsequent rules see the canonical form.
        parts = stripped.split()

    emitted_last_word = (
        len(parts) > 1
        and parts[-1].lower() not in _GENERIC_LAST_WORDS
        and not _is_weak_last_word(parts[-1])
    )
    if emitted_last_word:
        strong.append(parts[-1])

    if len(parts) >= 2:
        # First two words (for 2-word names this duplicates the full name and
        # gets deduped). It is STRONG only as a last resort, when no other
        # relaxed form exists: 'North Carolina State' has no club tag to strip
        # and loses 'State' as a generic, so 'North Carolina' is all that is
        # left, and 'UFC 329: McGregor vs. Holloway 2' likewise needs
        # 'UFC 329:'. Otherwise a stronger relaxation already exists (the
        # nickname, or the suffix-stripped name) and this prefix is the bare
        # metro, so it is WEAK.
        prefix = " ".join(parts[:2])
        if not emitted_last_word and stripped is None:
            strong.append(prefix)
        else:
            weak.append(prefix)

    # Broadcaster aliases (#4). Look up both the original name AND the
    # FC-stripped form because the JSON has "Manchester United" but FD.org
    # returns "Manchester United FC". Aliases are curated per team, so they are
    # strong by construction.
    for lookup in (name, stripped):
        if lookup and lookup in _TEAM_ALIASES:
            strong.extend(_TEAM_ALIASES[lookup])

    strong = list(dict.fromkeys(strong))
    weak = [k for k in dict.fromkeys(weak) if k not in strong]
    return strong, weak


def _strong_team_keywords(team_name: str) -> List[str]:
    """Keywords that may admit a candidate on their OWN. See _team_keywords_split."""
    return _team_keywords_split(team_name)[0]


def _team_keywords(team_name: str) -> List[str]:
    """Every keyword variant, strong and weak. ONLY for both-teams gates.

    A caller that admits a candidate on a single keyword hit must use
    _strong_team_keywords instead, or a bare metro name will drag in every
    franchise in town (#162).

    Drops the last-word fallback for generic-suffix names so 'Manchester
    United' never reduces to just 'United' (which would false-match
    'Brentford v West Ham United'), and for weak tokens (a bare number or a
    1-2 char token) so a field-event title ending in a rematch number never
    reduces to a wildcard like '2' (see _is_weak_last_word).

    Pulls broadcaster aliases from team_aliases.json: "Manchester United"
    expands to include "Man United" / "Man Utd" / "Man U" / "MUFC" so
    abbreviated EPG titles still match. Lookup tries the canonical name
    AND its trailing-suffix-stripped form to catch FD.org names that
    arrive with "FC" / "AFC" appended.
    """
    strong, weak = _team_keywords_split(team_name)
    return strong + weak


# Keywords at or below this many alphanumeric characters must match as a whole
# token, not as a bare substring. Team aliases are abbreviations ('NE', 'OM',
# 'GB', 'BOS', 'PIT', 'Real'), and as substrings they are close to wildcards:
# 'NE' is inside Sportsnet / Tennessee / Network, 'BOS' is inside Bosnia, 'PIT'
# is inside Capital, 'Real' is inside Montreal (#129). Longer keywords are
# discriminating on their own and keep substring semantics deliberately, so a
# possessive or run-on form ("Yankees' bullpen", "Manchester Uniteds") still
# hits. DO NOT raise this bound to cover all keywords without re-running the
# snapshot replay: a boundary on long keywords costs real matches (it would
# stop 'Inter' hitting 'Internazionale').
_SHORT_KEYWORD_MAX_CHARS = 4


@lru_cache(maxsize=4096)
def _keyword_pattern(keyword: str) -> Optional[re.Pattern]:
    """Compiled whole-token matcher for a short keyword, or None if it is long.

    Uses lookarounds rather than \\b so it behaves for keywords that begin or
    end in punctuation: \\b is defined relative to the adjacent character, so
    '\\bOM\\b' and '\\bUFC 329:\\b' disagree about what a boundary even is.
    Alphanumeric neighbours are the real signal, which is what these assert.
    """
    if len(re.sub(r"[^0-9A-Za-z]", "", keyword)) > _SHORT_KEYWORD_MAX_CHARS:
        return None
    return re.compile(
        r"(?<![0-9A-Za-z])" + re.escape(keyword.lower()) + r"(?![0-9A-Za-z])"
    )


def _kw_hit(text: str, keywords: List[str]) -> bool:
    """Whether any keyword appears in `text`, case-insensitively.

    Single source of truth for the hit test every matcher tier uses (and the
    diagnose action reuses), so the matched-vs-explained logic can never drift
    apart. Tolerates None/empty text.

    Long keywords match as substrings; short ones must match as whole tokens
    (see _SHORT_KEYWORD_MAX_CHARS). Keep new gates routed through here rather
    than writing their own `in` test, or the short-keyword rule silently stops
    applying to them.
    """
    t = (text or "").lower()
    for kw in keywords:
        pattern = _keyword_pattern(kw)
        if pattern is None:
            if kw.lower() in t:
                return True
        elif pattern.search(t):
            return True
    return False


def both_teams_in_one_segment(
    text: str, home_kws: List[str], away_kws: List[str]
) -> bool:
    """True if SOME ':'/'|'-delimited segment of `text` names BOTH sides.

    Providers prefix a feed/network label onto stream names:
    'USA Soccer09: Australia vs Turkey', 'US (Peacock 064) | Suiza v. Bosnia'.
    The matchup lives in ONE segment; a team alias appearing only in the LABEL
    (e.g. 'USA' in the feed-prefix 'USA Soccer09', which is NOT the United
    States team) must not pair with an opponent token in the matchup body to
    fake a match. Confirmed false positive: the United States vs Australia game
    matched 'USA Soccer09: Australia vs Turkey' (USA from the prefix, Australia
    from the body) before this gate. Requiring co-occurrence in a single segment
    kills that class while keeping every real feed (whose matchup names both
    teams together). Used to gate Path C stream-name candidates, where these
    feed prefixes are common.

    Splits on the label separators ':' and '|', but NOT on a ':' that is part of
    a clock time ('Iran 02:00 New Zealand'): a colon immediately followed by a
    digit is a time, not a feed-label boundary. Without that guard the kickoff
    time inside 'FIFA World Cup 2026 18: Iran 02:00 New Zealand' would split the
    matchup across segments and reject a legitimate feed.
    """
    for seg in re.split(r":(?!\d)|\|", text or ""):
        if _kw_hit(seg, home_kws) and _kw_hit(seg, away_kws):
            return True
    return False


def select_streams_for_game(
    names: List[str], home_kws: List[str], away_kws: List[str]
) -> List[bool]:
    """Which of ONE channel's stream names belong to this game. Aligned mask.

    A matched channel donates every stream it carries. On a provider that
    bundles dedicated per-matchup feeds onto one channel, that turns a single
    channel match into a pile of unrelated feeds: an MLB Yankees game came back
    carrying dedicated NFL Giants and Jets feeds, dead on a baseball night
    (#162).

    The gate is RELATIVE to the channel, which is what makes it safe. Only when
    at least one stream on this channel names one of our sides do we take the
    channel as evidence that this provider puts team names in stream names, and
    drop the streams that name neither side. A generic broadcaster channel whose
    streams are 'MLB Network HD' / 'MLB Network FHD' names no team anywhere, so
    nothing is dropped and its behaviour is unchanged.

    DO NOT invert this into "attach only streams that name our teams". The
    asymmetry is the whole point: naming ANOTHER team is evidence against a
    stream, but naming NO team is not, and most legitimate broadcaster feeds
    name no team at all. Inverting it would strip the common case bare.

    Accepts an empty `away_kws` for field events (#127), where there is no
    opponent to name; the caller decides whether the single-sided case is worth
    gating at all.

    An unnamed stream is kept unconditionally: a blank name is not evidence that
    the stream belongs to someone else's game.

    Known cost: an alternate feed for the RIGHT game that happens to name
    nobody ('Feed 2' next to 'Yankees vs White Sox HD') is dropped along with
    the wrong-game feeds. The primary feed always survives, so this trades a
    backup stream for not attaching a different sport's broadcast.
    """
    hits = [
        _kw_hit(n, home_kws) or (bool(away_kws) and _kw_hit(n, away_kws))
        for n in names
    ]
    if not any(hits):
        return [True] * len(names)
    return [hit or not (name or "").strip() for hit, name in zip(hits, names)]


def _regex_filter(
    candidates: List[ChannelCandidate],
    team_a: str,
    team_b: Optional[str] = None,
) -> List[ChannelCandidate]:
    """Programs whose title references both teams.

    `team_b=None` is the single-sided mode for field events (#127): one event,
    no opponent, so we match on the event name (`team_a`) alone. The both-teams
    gate would otherwise be unsatisfiable against the "Field" away sentinel.

    The both-teams gate reads the sub-title and description alongside the title
    (#143). European public broadcasters title the programme with the
    competition ('FIFA Fussball WM 2026') and put the fixture in the sub-title
    ('Gruppe F: Schweden - Tunesien'), so a title-only gate sent every one of
    those broadcasts to the LLM tier for a match the sub-title states outright.

    The SINGLE-SIDED branch deliberately still reads the title only. One keyword
    admits there, and a description is free prose that name-drops other teams
    and events in passing, so widening it would turn a single event keyword into
    a wildcard over the whole guide.
    """
    if team_b is None:
        # Single-sided: one keyword admits, so weak place-name keywords are
        # unsafe here (#162), and so is the description (see above).
        a_kws = _strong_team_keywords(team_a)
        return [c for c in candidates if _kw_hit(c.program_title, a_kws)]
    # Both-teams gate: weak keywords are safe and needed (city-only feed names).
    a_kws = _team_keywords(team_a)
    b_kws = _team_keywords(team_b)

    def _text(c: ChannelCandidate) -> str:
        return f"{c.program_title} {c.program_extra}" if c.program_extra else c.program_title

    return [c for c in candidates
            if _kw_hit(_text(c), a_kws) and _kw_hit(_text(c), b_kws)]


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

    The both-teams check requires the two sides in ONE segment of the name, not
    merely somewhere in it (#129). Providers prefix a feed/network label onto
    channel names, and a team alias sitting in that LABEL must not pair with an
    opponent named in the matchup body: 'USA Soccer07: Australia vs Turkey'
    carried the Australia-vs-TURKEY fixture and was matched to
    Australia-vs-United States, with 'USA' supplied by the feed label. Path C
    (stream names) has gated on this since 1.8.0; this is the same helper on the
    same class of text, so the two paths agree.

    DO NOT push the same gate onto `_regex_filter` (Tier 2, programme titles).
    Feed labels are a channel/stream naming convention; XMLTV programme titles
    do not carry them, so there is no failure of this shape to defend against
    there, and gating an unaffected path only costs recall.
    """
    if team_b is None:
        # Single-sided: see the matching note in _regex_filter.
        a_kws = _strong_team_keywords(team_a)
        return [c for c in candidates if _kw_hit(c.channel_name, a_kws)]
    a_kws = _team_keywords(team_a)
    b_kws = _team_keywords(team_b)
    return [c for c in candidates
            if both_teams_in_one_segment(c.channel_name, a_kws, b_kws)]


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


# Channel-NAME markers for a feed that legitimately names the event but is not
# the event itself: the pre-show, the prelims, the post-fight presser, the
# multiview mosaic. Tier-1 stacks every channel naming the event, so for a UFC
# card these ride along with the main card and, because the primary was just
# first-seen, one of them could BE the primary (#135).
#
# DELIBERATELY SEPARATE from _PREVIEW_TITLE_PATTERNS, which is tuned for the
# two-team case (a team-branded home channel emitting a 'Next Game:' EPG card)
# and is used to REJECT candidates. Extending that list with these would start
# rejecting real broadcasts. These only REORDER.
_NON_MAIN_CARD_NAME_MARKERS = (
    "pre show", "pre-show", "preshow",
    "prelim",                      # 'prelims', 'preliminary', 'early prelims'
    "post fight", "post-fight",
    "press conference",
    "countdown",
    "multiview", "multi view", "multi-view",
    "pre game", "pre-game", "pregame",
    "post game", "post-game", "postgame",
    "weigh in", "weigh-in",
)


def _is_non_main_card(channel_name: str) -> bool:
    """Whether a channel NAME marks itself as undercard/ancillary programming."""
    n = (channel_name or "").lower()
    return any(m in n for m in _NON_MAIN_CARD_NAME_MARKERS)


def _main_card_first(cands: List[ChannelCandidate]) -> List[ChannelCandidate]:
    """Stable-sort Tier-1 matches so a plain feed outranks an undercard one.

    DEMOTE, DO NOT DROP. A provider that labels every one of its feeds
    'Prelims' would otherwise lose the event entirely, and the whole point of
    Tier-1 stacking is that a viewer can fall through to another feed. Sorting
    on a bool is stable, so within each group the original discovery order (and
    therefore every existing expectation about variant ordering) is untouched;
    the only thing that changes is that an ancillary feed can no longer sit in
    front of a main-card one.

    Applied to BOTH the field-event and two-team paths. #135 observed it on a
    UFC card, but 'PRE SHOW: Man Utd vs Brentford' winning primary over the
    actual match feed is the same defect, and since this only reorders there is
    no recall cost to covering both.
    """
    return sorted(cands, key=lambda c: _is_non_main_card(c.channel_name))


_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

# Full names FIRST so 'june' wins over 'jun', and every branch is a complete
# month token. The naive "jan|feb|..." + "[a-z]*" form matched 'Jun' inside
# 'Junior' ('Moto3 Junior' parsed as 3 June), and 'Mar' inside 'March Madness';
# a trailing \b on this alternation is what stops that.
_MONTH_RE = (
    "january|february|march|april|june|july|august|september|october|november|december"
    "|jan|feb|mar|apr|may|jun|jul|aug|sept|sep|oct|nov|dec"
)

# Ordered most-specific-first. Every one of these shapes was counted in a live
# 16415-stream corpus; the counts are why the list is this long rather than
# just ISO (#164).
_STREAM_DATE_PATTERNS = (
    # 2026-08-01 19:25:00   (6480 streams)
    ("ymd", re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")),
    # 7.29.26 | ...         (Dirtvision-style, m.d.yy)
    ("mdy", re.compile(r"(?<!\d)(\d{1,2})[./](\d{1,2})[./](\d{2,4})(?!\d)")),
    # 29 Jul 10:00 PM ET    (1032 streams). MUST be tried before the month-first
    # shape below: in '@ 29 Jul 10:00 PM ET' a month-first pattern reads the
    # CLOCK as the day and returns July 10th.
    # The optional `q` group captures a preceding word so a SEQUENCE number can
    # be told apart from a day: 'Baltimore @ Boston - Game 2 Jul 22 7:00PM ET'
    # is the 22nd, not the 2nd, and without this the doubleheader's game number
    # wins because it sits immediately before the month. Found in the corpus,
    # not predicted.
    ("dm", re.compile(
        r"(?:\b(?P<q>[A-Za-z#]{1,6})\s+)?(?<!\d)(?P<d>\d{1,2})\s+"
        r"(?P<mon>" + _MONTH_RE + r")\b", re.I)),
    # Jul 29 8:00PM ET      (1950 streams).
    # The two lookaheads are BOTH load-bearing and neither is optional:
    #   (?!\d)   the day must be the whole number, so 'Jul 28' cannot backtrack
    #            to a 1-digit '2' when the 2-digit form is rejected below.
    #   (?!\s*:) the number must not be a clock, so '@ 29 Jul 10:00 PM' does not
    #            read the HOUR as the day and return July 10th.
    # A single combined lookahead was tried first and silently returned July 2nd
    # for 'Jul 28 7:30PM ET' across ~1950 live streams, because the regex
    # backtracked the day to one digit to satisfy it. Verified against the
    # corpus, not reasoned about.
    ("md", re.compile(r"(?<!\w)(" + _MONTH_RE + r")\b\.?\s+(\d{1,2})(?!\d)(?!\s*:)", re.I)),
    # (07.27) / 7/29        (ESPN UNLTD and MLB feed suffixes)
    ("mdn", re.compile(r"(?<!\d)(\d{1,2})[./](\d{1,2})(?!\s*:)(?!\d)")),
)

# A parsed date further than this from the game is treated as NO date rather
# than as a mismatch. Providers park undated feeds on a sentinel far-future
# timestamp -- '(PDC 50) | (2098-12-31 08:00:17)' is in the live corpus -- and
# reading that as "not this game's date" would drop a perfectly good feed.
_STREAM_DATE_SANITY_DAYS = 180

# Words that mark the number after them as a SEQUENCE, not a day of the month.
# Guards the day-first pattern against 'Game 2 Jul 22' / 'Leg 1 Aug 05'.
_DATE_SEQUENCE_WORDS = frozenset({
    "game", "gm", "g", "leg", "round", "rd", "part", "pt", "day", "week", "wk",
    "race", "heat", "set", "match", "series", "#", "no", "num", "session",
})


def parse_stream_date(name: str, reference: "date") -> "Optional[date]":
    """Best-effort broadcast date from a STREAM NAME, or None if it has none.

    Streams carry no schedule column, which is why Path C has no time-window
    filter at all; the date, when there is one, is in the name (#164). Most
    formats omit the year, so `reference` (the game's date) supplies it: the
    year chosen is whichever puts the result closest to the reference, which is
    what makes a 29 Dec game match a '29 Dec' feed across a New Year boundary.

    Returns None when nothing parses AND when what parses is implausible, so
    both cases flow into the same "no date, keep the stream" branch upstream.
    """
    text = name or ""
    for kind, rx in _STREAM_DATE_PATTERNS:
        m = rx.search(text)
        if not m:
            continue
        try:
            if kind == "ymd":
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            elif kind == "mdy":
                mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if y < 100:
                    y += 2000
            elif kind == "dm":
                q = (m.group("q") or "").strip().lower()
                if q in _DATE_SEQUENCE_WORDS:
                    # 'Game 2 Jul 22': the 2 is a game number. Fall through to
                    # the month-first pattern, which reads the real day.
                    continue
                d, mo, y = int(m.group("d")), _MONTHS[m.group("mon")[:3].lower()], None
            elif kind == "md":
                mo, d, y = _MONTHS[m.group(1)[:3].lower()], int(m.group(2)), None
            else:  # mdn
                mo, d, y = int(m.group(1)), int(m.group(2)), None
            if not (1 <= mo <= 12 and 1 <= d <= 31):
                continue
            if y is None:
                # Yearless: try the reference year and its neighbours, keep the
                # nearest. Straddles the New Year without special-casing it.
                best = None
                for cand_y in (reference.year - 1, reference.year, reference.year + 1):
                    try:
                        c = date(cand_y, mo, d)
                    except ValueError:
                        continue
                    if best is None or abs((c - reference).days) < abs((best - reference).days):
                        best = c
                parsed = best
            else:
                parsed = date(y, mo, d)
        except (ValueError, KeyError):
            continue
        if parsed is None:
            continue
        if abs((parsed - reference).days) > _STREAM_DATE_SANITY_DAYS:
            # Sentinel/garbage date. Treat as undated, NOT as a mismatch.
            return None
        return parsed
    return None


def stream_is_stale_for_game(name: str, earliest_game_date: "date") -> bool:
    """Whether a stream NAME advertises a date BEFORE this game's.

    Deliberately one-sided. Baseball plays the same opponent three nights
    running, so every night's dedicated feed names the same two teams and the
    team-pair gate -- which is tight on teams and blind to dates -- passes all
    of them. Roughly half of the 22 streams attached to one Yankees game were
    the previous two nights' finished broadcasts (#164).

    DO NOT tighten this to "date must equal the game's date". The caller passes
    the EARLIEST plausible date for the game (its date in UTC and in the user's
    configured timezone, whichever is earlier), because provider names are
    stamped in the broadcaster's local zone -- almost always ET -- which can sit
    a day behind or ahead of both. Requiring equality would drop the CORRECT
    feed for any late-evening game whose UTC date has already rolled over, and
    losing the live feed is far worse than keeping one finished one.

    A future-dated feed is kept: a provider publishing tomorrow's feed early is
    not evidence against this game, and pre-published feeds are how a viewer
    gets a stream at all when the guide runs ahead.
    """
    parsed = parse_stream_date(name, earliest_game_date)
    if parsed is None:
        return False
    return parsed < earliest_game_date


def _partition_attach_targets(
    primary: ChannelCandidate, cands: List[ChannelCandidate]
) -> Tuple[List[int], List[int]]:
    """Split an ordered candidate set into (channel_ids, stream_ids).

    `primary` leads (its target sits first in whichever list it belongs to),
    then the rest of `cands` in order. Whole-channel candidates (stream_id is
    None: Path A EPG title / Path B channel name) contribute their channel_id,
    so the apply stacks ALL of that channel's streams. Stream-name candidates
    (stream_id set: Path C) contribute ONLY their stream_id, so the apply
    attaches that one stream and NOT the parent channel's unrelated streams.
    Both lists are deduped in encounter order.

    With an all-whole-channel set (the pre-stream-name shape) this returns
    (deduped channel_ids, []), identical to the old _stack_fallback_ids it
    replaced. Used by the #108 widen path and the Tier-1 merge to stack
    same-fixture variants as fallback streams behind the chosen primary; a
    single channel can appear more than once (multiple ProgramData rows), hence
    the dedupe.
    """
    channel_ids: List[int] = []
    stream_ids: List[int] = []
    seen_ch: set = set()
    seen_st: set = set()
    for c in [primary, *cands]:
        if c.stream_id is not None:
            if c.stream_id not in seen_st:
                seen_st.add(c.stream_id)
                stream_ids.append(c.stream_id)
        elif c.channel_id not in seen_ch:
            seen_ch.add(c.channel_id)
            channel_ids.append(c.channel_id)
    return channel_ids, stream_ids


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
        # fixture. Stack all of them. Note Path C stream-name candidates also
        # land here when their name names both teams (their channel_name IS the
        # stream name), so a stream-name match is treated with the same
        # confidence as a channel-name match.
        # Main-card feeds first, so the primary is never the pre-show or the
        # multiview just because it was discovered first (#135). Reorder only:
        # every ancillary feed still stacks behind as a fallback.
        strict = _main_card_first(
            _regex_filter_channel_name(candidates, game.home, match_away)
        )
        # Tier 2: program-title regex, with previews ('Next Game:', 'Preview:',
        # 'Pre-game ...') stripped: those mark team-branded home channels
        # that surface upcoming-game EPG cards but don't broadcast the match.
        # Computed up front so Tier-1 can MERGE it in (below).
        filtered = _strip_preview_titles(
            _regex_filter(candidates, game.home, match_away)
        )
        if strict:
            primary = strict[0]
            results[i].channel_id = primary.channel_id
            results[i].channel_name = primary.channel_name
            results[i].program_title = primary.program_title
            # MERGE: a channel-name match confirms the fixture is genuinely on
            # air, so also stack the program-title both-team matches (the live
            # broadcasters: FOX/TSN/BBC whose EPG names the game) behind the
            # dedicated feeds, instead of discarding them. Before this, Tier-1
            # short-circuited on `strict` alone and silently dropped every
            # EPG-confirmed broadcaster the moment one dedicated-feed channel
            # existed. Both sets are gated on BOTH teams, so the merge is
            # high-precision and needs no LLM call. The partition routes any
            # Path C stream-name candidates in either set to stream_ids and
            # de-dupes (a channel can recur across multiple ProgramData rows).
            ch_ids, st_ids = _partition_attach_targets(primary, [*strict, *filtered])
            results[i].channel_ids = ch_ids
            results[i].stream_ids = st_ids
            results[i].method = "regex_strict"
            continue

        if len(filtered) == 1:
            c = filtered[0]
            results[i].channel_id = c.channel_id
            results[i].channel_name = c.channel_name
            results[i].program_title = c.program_title
            results[i].channel_ids, results[i].stream_ids = (
                _partition_attach_targets(c, [])
            )
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
            extra = cands if (widen and both_team) else []
            results[idx].channel_ids, results[idx].stream_ids = (
                _partition_attach_targets(chosen, extra)
            )
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
                extra = cands if (widen and both_team) else []
                results[idx].channel_ids, results[idx].stream_ids = (
                    _partition_attach_targets(c, extra)
                )
                results[idx].method = "fallback_first"
                results[idx].note = "no api key; first candidate"

    return results
