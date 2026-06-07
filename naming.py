"""Channel-name templating: Sonarr/Radarr-style ``{token}`` substitution.

Bare text in a template is literal. A ``{group}`` holds exactly one known
token plus optional literal decoration glued around it (brackets, parens,
separators, spaces). The WHOLE group collapses to nothing when its token
resolves empty, so optional fields (an unranked team, a game with no tagline)
leave no orphaned separators, parentheses, or double spaces behind.

This is deliberately the same convention Sonarr and Radarr use for media file
naming: ``{[Quality Full]}`` renders ``[1080p]`` or disappears entirely. The
self-hosted media audience this plugin serves already knows it, so a custom
naming string reads the way they expect.

The module is intentionally free of Django (and of ``scoring``) imports so it
stays a pure function set: unit-testable in isolation and safe to call from the
"test naming convention" action without booting the plugin's heavier paths.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Optional


# The default ships inline poll ranks after each team (issue #99) and drops the
# old "NvN" head prefix, since the prefix could not say WHICH team held which
# rank. Every optional group collapses when empty (issue #100), so the same
# template serves a ranked college clash and a rankless pro game.
DEFAULT_NAME_TEMPLATE = (
    "{league_short} {favorite_star}★{score} · "
    "{away_team}{ (rank_away)} at {home_team}{ (rank_home)}{ · tagline}"
)

# Every token the template understands -> a one-line, user-facing description.
# The descriptions are surfaced verbatim by the test-naming action, so keep
# them tight. Insertion order is the order they are listed to the user.
TOKENS: Dict[str, str] = {
    "league_short": "Short league code, e.g. CFB",
    "league_full": "Full league name, e.g. NCAA Football",
    "away_team": "Away team (corporate suffix stripped)",
    "home_team": "Home team (corporate suffix stripped)",
    "rank_away": "Away team poll rank, blank unless poll-ranked",
    "rank_home": "Home team poll rank, blank unless poll-ranked",
    "rank_pair": "Compact rank pair, e.g. 1v5 (blank unless both poll-ranked)",
    "score": "Interestingness score 0-10, e.g. 8.5",
    "favorite_star": "A star when one of your favorites is playing, else blank",
    "tagline": "Editorial tagline, e.g. top-5 showdown / title race / Final Four",
    "tournament": "Tournament stage only, e.g. Round of 16 (blank if none)",
    "venue": "Venue name (blank if unknown)",
    "game_date": "Local date, e.g. Sat Nov 15",
    "start_time": "Local start time, e.g. 7:00 PM",
    "kickoff": "Friendly kickoff, e.g. Today 7:00 PM EST",
    "rivalry": "The word 'rivalry' for a known rivalry, else blank",
}
KNOWN_TOKENS = set(TOKENS)

_FAVORITE_STAR = "⭐"

_GROUP_RE = re.compile(r"\{([^{}]*)\}")
# Longest-first matching so a token never loses to a shorter token that is a
# prefix of it (e.g. a future bare "rank" must not pre-empt "rank_home").
_TOKEN_IN_GROUP_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in sorted(KNOWN_TOKENS, key=len, reverse=True)) + r")\b"
)


def _find_token(inner: str) -> Optional[str]:
    """Return the known token inside a group's text, or None if there isn't one."""
    m = _TOKEN_IN_GROUP_RE.search(inner)
    return m.group(1) if m else None


def render_name(template: str, ctx: Dict[str, str]) -> str:
    """Render ``template`` against a token -> value map.

    A ``{group}`` emits its inner text with the token replaced by its value, or
    nothing at all when the value is empty (the collapse that keeps separators
    from orphaning). Bare text passes through unchanged. A group with no known
    token is emitted literally (its braces stripped); ``validate_template``
    reports that case so the test action can warn before it reaches a channel.
    """
    def _sub(m: "re.Match[str]") -> str:
        inner = m.group(1)
        token = _find_token(inner)
        if token is None:
            return inner
        value = ctx.get(token, "")
        if value == "":
            return ""
        return inner.replace(token, value, 1)

    return _GROUP_RE.sub(_sub, template)


def validate_template(template: str) -> List[str]:
    """Return human-readable problems with a template; empty list means valid."""
    errors: List[str] = []
    if template.count("{") != template.count("}"):
        errors.append("Unbalanced { } braces.")
    for m in _GROUP_RE.finditer(template):
        inner = m.group(1)
        if _find_token(inner) is None:
            errors.append(f"No known variable in group: {{{inner}}}")
    return errors


def _fmt_rank(rank: Optional[int], is_poll: bool) -> str:
    # Poll ranks (AP / Coaches Top 25) are meaningful absolute numbers; a
    # standings "rank" is just a league-table position every team has, so it
    # must NOT render as an inline rank. The rank_source == "poll" gate mirrors
    # the one pick_tagline already applies for the rank-pair tagline.
    return str(rank) if (rank is not None and is_poll) else ""


def _kickoff_phrase(local: datetime) -> str:
    """Today/Tomorrow-aware kickoff, matching the EPG description convention."""
    today = datetime.now(local.tzinfo).date()
    delta = (local.date() - today).days
    if delta == 0:
        return f"Today {local.strftime('%-I:%M %p %Z')}".strip()
    if delta == 1:
        return f"Tomorrow {local.strftime('%-I:%M %p %Z')}".strip()
    return local.strftime("%a %b %-d, %-I:%M %p %Z").strip()


def build_context(
    *,
    sport_prefix: str = "",
    sport_label: str = "",
    home: str = "",
    away: str = "",
    rank_home: Optional[int] = None,
    rank_away: Optional[int] = None,
    rank_source: str = "poll",
    score_final: Optional[float] = None,
    favorite: bool = False,
    tagline: str = "",
    tournament: str = "",
    venue: Optional[str] = None,
    is_rivalry: bool = False,
    start_dt: Optional[datetime] = None,
    tz=None,
) -> Dict[str, str]:
    """Build the token -> value map for one game.

    ``home`` / ``away`` are expected pre-stripped of corporate suffixes by the
    caller (scoring owns ``strip_team_suffix``); this keeps the module free of a
    circular import. Every value is a string; empty string is the signal that
    tells ``render_name`` to collapse the surrounding group.
    """
    is_poll = rank_source == "poll"
    rh = _fmt_rank(rank_home, is_poll)
    ra = _fmt_rank(rank_away, is_poll)
    rank_pair = ""
    if rh and ra:
        lo, hi = sorted((int(rank_home), int(rank_away)))  # type: ignore[arg-type]
        rank_pair = f"{lo}v{hi}"

    game_date = start_time = kickoff = ""
    if start_dt is not None and tz is not None:
        aware = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
        local = aware.astimezone(tz)
        game_date = local.strftime("%a %b %-d")
        start_time = local.strftime("%-I:%M %p")
        kickoff = _kickoff_phrase(local)

    return {
        "league_short": sport_prefix or "",
        "league_full": sport_label or "",
        "away_team": away or "",
        "home_team": home or "",
        "rank_away": ra,
        "rank_home": rh,
        "rank_pair": rank_pair,
        "score": f"{score_final:.1f}" if score_final is not None else "",
        "favorite_star": _FAVORITE_STAR if favorite else "",
        "tagline": tagline or "",
        "tournament": tournament or "",
        "venue": venue or "",
        "game_date": game_date,
        "start_time": start_time,
        "kickoff": kickoff,
        "rivalry": "rivalry" if is_rivalry else "",
    }


# Representative games for the test-naming action: deterministic, always
# available (no live cache required), and chosen to exercise every collapse
# path: both ranked, one ranked, none ranked, favorite present/absent, a
# tournament-stage tagline, a standings "race" tagline, and a rivalry.
_PREVIEW_SAMPLES = [
    {
        "_label": "Ranked vs unranked, favorite, college",
        "sport_prefix": "CBB", "sport_label": "NCAA Basketball",
        "away": "Alabama", "home": "St. John's",
        "rank_away": 15, "rank_home": None, "rank_source": "poll",
        "score_final": 8.5, "favorite": True, "tagline": "top-25 matchup",
    },
    {
        "_label": "Both ranked, college football",
        "sport_prefix": "CFB", "sport_label": "NCAA Football",
        "away": "Ohio State", "home": "Penn State",
        "rank_away": 5, "rank_home": 1, "rank_source": "poll",
        "score_final": 9.2, "favorite": False, "tagline": "top-5 showdown",
    },
    {
        "_label": "Standings race, soccer (ranks suppressed)",
        "sport_prefix": "EPL", "sport_label": "Premier League",
        "away": "Brentford", "home": "Manchester United",
        "rank_away": 9, "rank_home": 3, "rank_source": "standings",
        "score_final": 10.0, "favorite": False, "tagline": "title race",
    },
    {
        "_label": "Pro game, no ranks / no tagline / no favorite",
        "sport_prefix": "NFL", "sport_label": "NFL",
        "away": "Buffalo Bills", "home": "Kansas City Chiefs",
        "rank_away": None, "rank_home": None, "rank_source": "poll",
        "score_final": 7.2, "favorite": False, "tagline": "",
    },
    {
        "_label": "Tournament stage tagline",
        "sport_prefix": "CWS", "sport_label": "NCAA Baseball",
        "away": "LSU", "home": "Tennessee",
        "rank_away": None, "rank_home": None, "rank_source": "poll",
        "score_final": 9.0, "favorite": False, "tagline": "Road to Omaha",
    },
]


def preview_lines(template: str, tz=None) -> "tuple[List[str], List[tuple[str, str]]]":
    """Render the canned sample games against ``template``.

    Returns ``(errors, [(sample_label, rendered_name), ...])``. ``errors`` is
    the ``validate_template`` output so the caller can surface problems instead
    of showing a silently-wrong preview.
    """
    errors = validate_template(template)
    out: List[tuple[str, str]] = []
    for sample in _PREVIEW_SAMPLES:
        kwargs = {k: v for k, v in sample.items() if not k.startswith("_")}
        ctx = build_context(tz=tz, **kwargs)
        out.append((sample["_label"], render_name(template, ctx)))
    return errors, out
