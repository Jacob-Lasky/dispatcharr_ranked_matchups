"""Channel-name and EPG-description templating: Sonarr/Radarr-style ``{token}``
substitution.

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

import functools
import re
from datetime import datetime, timedelta, timezone
from typing import AbstractSet, Dict, List, Optional, Tuple


# The default ships inline poll ranks after each team (issue #99) and drops the
# old "NvN" head prefix, since the prefix could not say WHICH team held which
# rank. Every optional group collapses when empty (issue #100), so the same
# template serves a ranked college clash and a rankless pro game.
DEFAULT_NAME_TEMPLATE = (
    "{league_short} {favorite_star}★{score} · "
    "{away_team}{ (rank_away)} at {home_team}{ (rank_home)}{ · tagline}"
)

# Every token the template understands -> a one-line description. The names
# are listed to the user in plugin.json's name_template help text, and a test
# fails if one is missing there; the descriptions document each token here.
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
    "recording_dot": "A red dot when this game is set to record, else blank",
    "tagline": "Editorial tagline, e.g. top-5 showdown / title race / Final Four",
    "tournament": "Tournament stage only, e.g. Round of 16 (blank if none)",
    "venue": "Venue name (blank if unknown)",
    "game_date": "Local date, e.g. Sat Nov 15",
    "start_time": "Local start time, e.g. 7:00 PM",
    "kickoff": "Friendly kickoff, e.g. Today 7:00 PM EST",
    "rivalry": "The word 'rivalry' for a known rivalry, else blank",
}
KNOWN_TOKENS = frozenset(TOKENS)

# The EPG description template (#317) understands every name token plus these.
# Same shape as TOKENS; plugin.json's description_template help text names
# each one (test-enforced).
DESCRIPTION_ONLY_TOKENS: Dict[str, str] = {
    "llm": ("Claude-written 2-3 sentence preview; needs 'LLM-rewritten EPG "
            "descriptions' on, blank when it is off or the call fails"),
    "details": "The plugin's built-in description (why the game ranked, standings, source)",
    "source_channel": "Name of the channel the stream comes from (blank if unmatched)",
}
DESCRIPTION_TOKENS: Dict[str, str] = {**TOKENS, **DESCRIPTION_ONLY_TOKENS}
KNOWN_DESCRIPTION_TOKENS = frozenset(DESCRIPTION_TOKENS)

_FAVORITE_STAR = "⭐"
# Same glyph the programme titles carry (recording.DOT); kept as a literal here
# so naming.py stays import-free of the recording policy module.
_RECORDING_DOT = "\U0001F534"

_GROUP_RE = re.compile(r"\{([^{}]*)\}")


@functools.lru_cache(maxsize=None)
def _token_re(known: AbstractSet[str]) -> "re.Pattern[str]":
    # Longest-first matching so a token never loses to a shorter token that is
    # a prefix of it (e.g. a future bare "rank" must not pre-empt "rank_home").
    return re.compile(
        r"\b(" + "|".join(re.escape(t) for t in sorted(known, key=len, reverse=True)) + r")\b"
    )


def _find_token(inner: str, known: AbstractSet[str] = KNOWN_TOKENS) -> Optional[str]:
    """Return the known token inside a group's text, or None if there isn't one."""
    m = _token_re(frozenset(known)).search(inner)
    return m.group(1) if m else None


def render_name(
    template: str, ctx: Dict[str, str], known: AbstractSet[str] = KNOWN_TOKENS,
) -> str:
    """Render ``template`` against a token -> value map.

    A ``{group}`` emits its inner text with the token replaced by its value, or
    nothing at all when the value is empty (the collapse that keeps separators
    from orphaning). Bare text passes through unchanged. A group with no known
    token is emitted literally (its braces stripped); ``validate_template``
    reports that case so the test action can warn before it reaches a channel.
    """
    def _sub(m: "re.Match[str]") -> str:
        inner = m.group(1)
        token = _find_token(inner, known)
        if token is None:
            return inner
        value = ctx.get(token, "")
        if value == "":
            return ""
        return inner.replace(token, value, 1)

    return _GROUP_RE.sub(_sub, template)


def validate_template(template: str, known: AbstractSet[str] = KNOWN_TOKENS) -> List[str]:
    """Return human-readable problems with a template; empty list means valid."""
    errors: List[str] = []
    if template.count("{") != template.count("}"):
        errors.append("Unbalanced { } braces.")
    elif not _braces_well_formed(template):
        # Equal counts are not enough: "{home_team}}{" balances and still
        # renders a stray "}{" into every channel.
        errors.append("Braces out of order or nested; each { needs its own } before the next {.")
    pattern = _token_re(frozenset(known))
    for m in _GROUP_RE.finditer(template):
        inner = m.group(1)
        found = pattern.findall(inner)
        if not found:
            errors.append(f"No known variable in group: {{{inner}}}")
        elif len(found) > 1:
            # render_name substitutes only the first, so "{source_channel: llm}"
            # would print the word "llm" and never call Claude.
            errors.append(f"More than one variable in group: {{{inner}}} (use one group per variable)")
    return errors


def _braces_well_formed(template: str) -> bool:
    depth = 0
    for ch in template:
        if ch == "{":
            depth += 1
            if depth > 1:
                return False
        elif ch == "}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def resolve_template(raw: Optional[str]) -> "tuple[str, List[str], bool]":
    """Decide which template to actually use. Returns (template, errors, is_default).

    THE single place the "your template is broken, falling back to the default"
    decision is made. It exists because apply and the preview action each made
    that decision independently and disagreed about how loud to be: preview
    returned `status: error` with the brace problem listed, while apply set the
    template to None, emitted a `logger.warning` nobody reads, and named every
    channel with the default. A user who edited the template to drop the score,
    and never happened to run preview, saw the score keep appearing with no
    explanation anywhere in the UI (#126). Route every new caller through here
    rather than re-deriving it, or the two paths drift apart again.

    An EMPTY template is not an error, it just means "use the default", so
    `errors` stays empty and only `is_default` flips. A NON-empty template that
    fails validation returns the default AND the problems, and the caller is
    expected to show them.
    """
    stripped = (raw or "").strip()
    if not stripped:
        return DEFAULT_NAME_TEMPLATE, [], True
    errors = validate_template(stripped)
    if errors:
        return DEFAULT_NAME_TEMPLATE, errors, True
    return stripped, [], False


def template_problem_summary(errors: List[str]) -> str:
    """One-line, user-facing rendering of resolve_template's errors.

    Shared so the apply toast and show_status word it identically; a user who
    sees two different phrasings for the same problem reasonably assumes they
    are two different problems.
    """
    return _problem_summary("name_template", "the DEFAULT template", errors)


def _problem_summary(setting: str, fallback: str, errors: List[str]) -> str:
    if not errors:
        return ""
    return (
        f"{setting} invalid ({'; '.join(errors)}); "
        f"used {fallback} instead. Fix it under Channel Name & Description, or "
        f"run 'Test naming convention' to preview."
    )


# ---------- EPG description template (#317) ----------

def resolve_description_template(raw: Optional[str]) -> Tuple[Optional[str], List[str]]:
    """The description counterpart of resolve_template: (template, errors).

    None means "use the built-in description", for a blank setting (not an
    error) and for a broken one (errors listed, and the caller must show them,
    for the reason resolve_template's docstring gives). Apply, the preview
    action and show_status all route through here so they cannot disagree.
    """
    stripped = (raw or "").strip()
    if not stripped:
        return None, []
    errors = validate_template(stripped, KNOWN_DESCRIPTION_TOKENS)
    if errors:
        return None, errors
    return stripped, []


def description_template_problem_summary(errors: List[str]) -> str:
    """One-line rendering of resolve_description_template's errors."""
    return _problem_summary("description_template", "the built-in description", errors)


def template_uses(template: str, token: str, known: AbstractSet[str] = KNOWN_DESCRIPTION_TOKENS) -> bool:
    """True when some {group} in `template` resolves to `token`."""
    return any(_find_token(m.group(1), known) == token for m in _GROUP_RE.finditer(template))


def description_needs_llm(template: Optional[str]) -> bool:
    """Whether a description built from `template` (None = built-in) would
    show Claude's prose. The built-in layout swaps in the prose whole; a
    template shows it only through an {llm} group. Apply asks this before
    paying for a call, and the preview before warning that {llm} will be
    blank, so the two cannot disagree."""
    return template is None or template_uses(template, "llm")


def render_description(template: str, ctx: Dict[str, str]) -> str:
    """Render a description template. Unlike a channel name, line breaks are
    content here, so only runs of spaces are squeezed (and each line trimmed);
    blank lines left by collapsed groups shrink to one paragraph break.

    A template that renders to nothing (every group collapsed, say {llm} alone
    on a failed call) yields ctx["details"], the built-in description, rather
    than an empty guide entry. It lives here, not in apply, so the preview
    shows the same fallback apply writes."""
    text = render_name(template, ctx, KNOWN_DESCRIPTION_TOKENS)
    text = "\n".join(re.sub(r"[ \t]{2,}", " ", line).strip() for line in text.splitlines())
    return re.sub(r"\n{3,}", "\n\n", text).strip() or ctx.get("details", "")


_MAX_NAME_LEN = 250


def finish_name(rendered: str) -> str:
    """The cleanup every rendered channel name gets, in apply AND in the
    preview, so the preview shows exactly what apply will write: squeeze the
    double spaces a sparse custom template leaves between bare-literal
    separators, then cap the length."""
    name = re.sub(r"\s{2,}", " ", rendered).strip()
    return name if len(name) <= _MAX_NAME_LEN else name[:_MAX_NAME_LEN - 3] + "..."


def format_score(score_final: Optional[float]) -> str:
    """The 0-10 score as channel names show it ({score}), one decimal. The ONE
    formatter: the Minimum score setting (#313) compares this displayed value,
    so a game named \u26052.0 is never left out by a floor of 2."""
    return f"{score_final:.1f}" if score_final is not None else ""


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
    recording: bool = False,
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
        "score": format_score(score_final),
        "favorite_star": _FAVORITE_STAR if favorite else "",
        "recording_dot": _RECORDING_DOT if recording else "",
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
# tournament-stage tagline, a standings "race" tagline, and a known/unknown
# venue. Each kicks off tomorrow at 7 PM local (_preview_start), so the date
# and time variables preview filled in, as they render on a real channel.
_PREVIEW_SAMPLES = [
    {
        "_label": "Ranked vs unranked, favorite, college",
        "sport_prefix": "CBB", "sport_label": "NCAA Basketball",
        "away": "Alabama", "home": "St. John's",
        "rank_away": 15, "rank_home": None, "rank_source": "poll",
        "score_final": 8.5, "favorite": True, "tagline": "top-25 matchup",
        "venue": "Madison Square Garden",
    },
    {
        "_label": "Both ranked, college football",
        "sport_prefix": "CFB", "sport_label": "NCAA Football",
        "away": "Ohio State", "home": "Penn State",
        "rank_away": 5, "rank_home": 1, "rank_source": "poll",
        "score_final": 9.2, "favorite": False, "tagline": "top-5 showdown",
        "venue": "Beaver Stadium",
    },
    {
        "_label": "Standings race, soccer (ranks suppressed)",
        "sport_prefix": "EPL", "sport_label": "Premier League",
        "away": "Brentford", "home": "Manchester United",
        "rank_away": 9, "rank_home": 3, "rank_source": "standings",
        "score_final": 10.0, "favorite": False, "tagline": "title race",
        "venue": "Old Trafford",
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
        "venue": "Charles Schwab Field",
    },
]


def _preview_start(tz) -> datetime:
    """Tomorrow, 7:00 PM in `tz` (UTC when None): a kickoff every date and
    time variable renders for."""
    zone = tz or timezone.utc
    tomorrow = datetime.now(zone) + timedelta(days=1)
    return tomorrow.replace(hour=19, minute=0, second=0, microsecond=0)


def _preview_context(sample: Dict, tz) -> Dict[str, str]:
    kwargs = {k: v for k, v in sample.items() if not k.startswith("_")}
    return build_context(tz=tz or timezone.utc, start_dt=_preview_start(tz), **kwargs)


def preview_lines(template: str, tz=None) -> "tuple[List[str], List[tuple[str, str]]]":
    """Render the canned sample games against ``template``.

    Returns ``(errors, [(sample_label, rendered_name), ...])``. ``errors`` is
    the ``validate_template`` output so the caller can surface problems instead
    of showing a silently-wrong preview.
    """
    errors = validate_template(template)
    out: List[tuple[str, str]] = []
    for sample in _PREVIEW_SAMPLES:
        out.append((sample["_label"], finish_name(render_name(template, _preview_context(sample, tz)))))
    return errors, out


# Stand-ins for the description-only variables in the preview. Canned, like the
# samples above, so the preview needs no cache, no network and no API key.
_PREVIEW_DETAILS = "A top-25 matchup: toss-up.\n\nSource: ESPN+ 12."
_PREVIEW_LLM = "Alabama brings a top-15 defense into the Garden against a St. John's side unbeaten at home."


_PREVIEW_DESCRIPTION_MAX = 200


def preview_description(template: str, llm_on: bool, tz=None) -> str:
    """Render `template` for the first canned sample game, on ONE line (line
    breaks shown as " / ") and capped, so it fits the action's toast. {llm}
    renders only when `llm_on`, which is what apply will do."""
    ctx = _preview_context(_PREVIEW_SAMPLES[0], tz)
    ctx.update({
        "llm": _PREVIEW_LLM if llm_on else "",
        "details": _PREVIEW_DETAILS,
        "source_channel": "ESPN+ 12",
    })
    line = " / ".join(
        part for part in render_description(template, ctx).splitlines() if part
    )
    if len(line) <= _PREVIEW_DESCRIPTION_MAX:
        return line
    return line[:_PREVIEW_DESCRIPTION_MAX - 1] + "…"
