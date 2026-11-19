"""Settings that #208 merged to shorten the settings modal, and the fallback
that keeps installs saved before the merge behaving exactly as they did.

WHY THIS MODULE EXISTS. Up to 0.33, Dispatcharr rendered a plugin's settings
as one flat list (`PluginFieldList` mapped `plugin.fields` straight to inputs;
the only field types were string / number / boolean / select / text / info,
with no collapsing or grouping primitive, verified against 0.30.0). So the
modal could only get shorter by having FEWER fields. 0.34 added collapsible
sections, multiselects and tables (#358, the last part of this module), but
the merges stay: they are what saved installs hold. #208 merged three shapes:

  - seven `weight_<name>` numbers -> one `scoring_weights` string
    ("favorite=8, spread=1"; blank means every default);
  - `enable_ncaaf` + `ncaaf_divisions` -> one `ncaaf` select with an Off option;
  - six pairs of related league toggles (men's/women's, ATP/WTA, the two
    English cups, baseball/softball) -> one four-way select each
    (off / first / second / both).

HOW OLD SAVED SETTINGS KEEP WORKING. Three facts about the loader decide this,
and each was read from the running container's source, not assumed:

  1. `POST .../settings/` REPLACES the stored dict, but the UI sends back every
     key it loaded (`useState(plugin.settings)` then `{...prev, [id]: val}`),
     so a key with no field (a retired `weight_rank`) survives every save.
  2. `context["settings"]` (the `run()` path) is the stored dict with each
     manifest field's `default` filled in, while `get_current_settings()`
     (the scheduler and reaper path) is the raw stored dict. So a reader can
     NOT tell "the user picked the default" from "the user never saved this
     field" on the run() path: both arrive as the default.
  3. The modal shows `value ?? field.default`, so an unsaved new field shows
     its default even when a legacy key is what the runtime is honouring.

So every resolver here follows one rule: a new key wins when it holds a
non-default value; when it is absent OR at its default and the legacy keys are
present, the legacy keys decide. That reproduces the pre-#208 behaviour on both
paths. Fact 3 is why `migrate_stored_settings` also exists: it writes the
derived new keys into the stored dict once (plugin.py does the write, from
discovery and from every raw settings read), so the modal SHOWS what the
runtime does, and stamps `SETTINGS_SCHEMA_KEY`. Once that stamp is present the
legacy keys are ignored, which is what lets a user later choose a new field's
default ("Off") and have it stick.

The migration reads the RAW stored dict, which carries one fact the resolvers
cannot see: a new key PRESENT there was put there by a save (the UI sends only
keys it loaded or the user touched), so the migration keeps it as saved, even
at its default (a select at "off", Scoring weights blank). That covers a save that reached the DB before any migration did (a
modal still holding pre-migration settings) with "Off" picked: the run() path
honours the legacy toggle until the next discovery or scheduler read migrates,
and from then on the user's "Off" stands.

The legacy keys are LEFT in the stored dict, so a downgrade to a pre-#208
release restores the settings as they were at the upgrade (everything off and
default weights would be the alternative). It does not carry edits made after
the upgrade back to the old fields, and edits made in the old release are not
read again after re-upgrading (the stamp is still there). Dispatcharr's own
plugin page warns that a downgrade may break saved settings; this is the
cheapest behaviour that is right for the common case.

The #358 section at the end converts the legacy STRING values of four
name lists and the two weight texts into the list shapes 0.34's multiselect
and table fields store, and builds each multiselect's options; its section
comment says why both are required, not cosmetic.

Pure: no Django, no I/O, so the whole fallback is unit-testable.
"""
import math
from dataclasses import fields as _dataclass_fields
from typing import Any, Dict, List, Optional, Tuple

from .scoring import Weights
from .sources.ncaaf import DEFAULT_DIVISIONS, DIVISION_SETS

# Stored marker: present (== SETTINGS_SCHEMA_VERSION) once
# migrate_stored_settings has written the merged keys. Version 1 is the
# implicit pre-#208 layout (per-league toggles, one number per weight).
SETTINGS_SCHEMA_KEY = "settings_schema"
SETTINGS_SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# League pairs: one four-way select replaces two boolean toggles.
#
# Each entry is  select_id: ((option_a, legacy_toggle_a), (option_b, legacy_toggle_b)).
# The select's options are PAIR_OFF, option_a, option_b and PAIR_BOTH.
#
# The legacy toggle ids double as the LOGICAL league ids that
# plugin._build_sources asks about (`league_enabled(settings, "enable_atp")`),
# so the source wiring reads the same whether a league has its own boolean
# field or shares a select. tests/test_description_claims.py reads this
# literal with ast to count leagues, so it MUST stay a plain literal.
#
# Only genuinely paired leagues belong here (the same competition across
# genders, the two tours of one sport, the two English cups, NCAA baseball and
# its softball counterpart). DO NOT fold unrelated leagues into one select to
# save a row: the select's option list is the only place a user can see that a
# league exists, so a pairing has to read as one thing.
# ---------------------------------------------------------------------------
LEAGUE_PAIRS: Dict[str, Tuple[Tuple[str, str], Tuple[str, str]]] = {
    "ncaa_basketball": (("men", "enable_ncaam"), ("women", "enable_ncaaw_basketball")),
    "intl_friendlies": (("men", "enable_intl_friendlies"), ("women", "enable_intl_friendlies_women")),
    "english_cups": (("efl", "enable_efl_cup"), ("fa", "enable_fa_cup")),
    "tennis": (("atp", "enable_atp"), ("wta", "enable_wta")),
    "ncaa_baseball_softball": (("baseball", "enable_ncaa_baseball"), ("softball", "enable_ncaa_softball")),
    "ncaa_soccer": (("men", "enable_ncaa_mens_soccer"), ("women", "enable_ncaa_womens_soccer")),
}
PAIR_OFF = "off"
PAIR_BOTH = "both"

# legacy toggle id -> (select id, the option value that turns it on)
_PAIR_OF_TOGGLE: Dict[str, Tuple[str, str]] = {
    toggle: (select_id, option)
    for select_id, members in LEAGUE_PAIRS.items()
    for option, toggle in members
}

# ---------------------------------------------------------------------------
# NCAA Football: the enable toggle and the divisions select became one select.
# Options are NCAAF_OFF plus every key of sources.ncaaf.DIVISION_SETS.
# ---------------------------------------------------------------------------
NCAAF_SETTING = "ncaaf"
NCAAF_OFF = "off"
NCAAF_LEGACY_TOGGLE = "enable_ncaaf"
_NCAAF_LEGACY_DIVISIONS = "ncaaf_divisions"
_NCAAF_LEGACY_KEYS = (NCAAF_LEGACY_TOGGLE, _NCAAF_LEGACY_DIVISIONS)

# ---------------------------------------------------------------------------
# Scoring weights: seven number fields became one "name=value, ..." string.
# Names are Weights' field names, read off the dataclass so a new weight is
# accepted without anyone remembering to list it here.
# ---------------------------------------------------------------------------
SCORING_WEIGHTS_SETTING = "scoring_weights"
WEIGHT_NAMES: Tuple[str, ...] = tuple(f.name for f in _dataclass_fields(Weights))
LEGACY_WEIGHT_PREFIX = "weight_"
_LEGACY_WEIGHT_KEYS = tuple(LEGACY_WEIGHT_PREFIX + n for n in WEIGHT_NAMES)

LEGACY_KEYS: Tuple[str, ...] = (
    tuple(_PAIR_OF_TOGGLE) + _NCAAF_LEGACY_KEYS + _LEGACY_WEIGHT_KEYS
)


def is_migrated(settings: Dict[str, Any]) -> bool:
    """True once the stored dict carries the post-#208 marker, after which
    the legacy keys are ignored."""
    try:
        return int(settings.get(SETTINGS_SCHEMA_KEY) or 0) >= SETTINGS_SCHEMA_VERSION
    except (TypeError, ValueError):
        return False


def _legacy_decides(settings: Dict[str, Any], new_key: str, default: Any,
                    legacy_keys: Tuple[str, ...]) -> bool:
    """The one precedence rule (see the module docstring): legacy keys decide
    when the install has not been migrated, at least one of them is stored,
    and the new key is absent, cleared (None, or text that is only
    whitespace), or at its default. Shared by every merged setting, the
    selects and Scoring weights alike, so they cannot drift apart."""
    if is_migrated(settings):
        return False
    if not any(k in settings for k in legacy_keys):
        return False
    raw = settings.get(new_key)
    return _is_blank(raw) or raw == default


def _is_blank(raw: Any) -> bool:
    """None, whitespace-only text, or an empty list (a table or multiselect
    saved with nothing in it, #358). The one reading of "the user left this
    empty" for every setting, text or list."""
    return (raw is None or (isinstance(raw, str) and not raw.strip())
            or (isinstance(raw, list) and not raw))


def league_pair_value(settings: Dict[str, Any], select_id: str) -> str:
    """The effective value of one pair select: PAIR_OFF, either member's
    option, or PAIR_BOTH. A cleared select (Mantine can hand back null) reads
    as off; an unrecognised hand-edited value is returned as-is and so enables
    neither league."""
    (opt_a, key_a), (opt_b, key_b) = LEAGUE_PAIRS[select_id]
    if _legacy_decides(settings, select_id, PAIR_OFF, (key_a, key_b)):
        a, b = bool(settings.get(key_a)), bool(settings.get(key_b))
        if a and b:
            return PAIR_BOTH
        return opt_a if a else (opt_b if b else PAIR_OFF)
    raw = settings.get(select_id)
    return PAIR_OFF if raw in (None, "") else str(raw)


def ncaaf_divisions(settings: Dict[str, Any]) -> Optional[str]:
    """The NCAA Football divisions to pull, or None when NCAA Football is off.

    The returned value is handed to NcaafSource unchanged; NcaafSource owns
    the normalisation of an unrecognised value (warn, then DEFAULT_DIVISIONS),
    so a bad value is reported in one place."""
    if _legacy_decides(settings, NCAAF_SETTING, NCAAF_OFF, _NCAAF_LEGACY_KEYS):
        if not bool(settings.get(NCAAF_LEGACY_TOGGLE)):
            return None
        return settings.get(_NCAAF_LEGACY_DIVISIONS) or DEFAULT_DIVISIONS
    raw = settings.get(NCAAF_SETTING)
    if raw in (None, "", NCAAF_OFF):
        return None
    return str(raw)


def league_enabled(settings: Dict[str, Any], toggle_id: str) -> bool:
    """Is the league with this LOGICAL toggle id switched on?

    A league with its own boolean field reads that field. A league merged into
    a pair select or the NCAAF select resolves through it, legacy fallback
    included, so plugin._build_sources asks the same question either way."""
    if toggle_id == NCAAF_LEGACY_TOGGLE:
        return ncaaf_divisions(settings) is not None
    pair = _PAIR_OF_TOGGLE.get(toggle_id)
    if pair is None:
        return bool(settings.get(toggle_id, False))
    select_id, option = pair
    return league_pair_value(settings, select_id) in (option, PAIR_BOTH)


def _name_number_entries(text: Any) -> List[Tuple[str, str, str]]:
    """Split "a=1, b: 2; c=3" into (entry, name, value) triples, the one
    tokenizer behind every "name=number" setting (Scoring weights, League
    weights). Entries split on commas, semicolons or newlines and take `=` or
    `:` as the separator. `name` is stripped but keeps its case, and is ""
    when the entry has no separator at all, so each caller reports a
    malformed entry in its own vocabulary."""
    out: List[Tuple[str, str, str]] = []
    if text is None:
        return out
    if isinstance(text, list):
        # A table field's rows (#358): [{"name": ..., "weight": ...}, ...].
        # "Add row" stores a row with both cells empty, which is skipped
        # rather than reported, as a blank entry in the text form is.
        for row in text:
            if not isinstance(row, dict):
                out.append((repr(row), "", ""))
                continue
            name = str(row.get(WEIGHT_ROW_NAME) or "").strip()
            value = row.get(WEIGHT_ROW_WEIGHT)
            value = "" if value is None else str(value).strip()
            if not name and not value:
                continue
            out.append((f"{name}={value}", name, value))
        return out
    for chunk in str(text).replace(";", ",").replace("\n", ",").split(","):
        entry = chunk.strip()
        if not entry:
            continue
        name, sep, value = entry.partition("=")
        if not sep:
            name, sep, value = entry.partition(":")
        out.append((entry, name.strip() if sep else "", value.strip()))
    return out


def parse_weight_overrides(text: Any) -> Tuple[Dict[str, float], List[str]]:
    """Parse "favorite=8, spread=1" into {name: value}.

    Entries split on commas, semicolons or newlines; names are
    case-insensitive and may carry the old "weight_" prefix (so a value pasted
    from the old field names still works). Returns (overrides, problems): an
    entry with an unknown name or a non-numeric value is skipped and described
    in `problems` rather than failing the refresh."""
    overrides: Dict[str, float] = {}
    problems: List[str] = []
    for entry, name, value in _name_number_entries(text):
        name = name.lower()
        if name.startswith(LEGACY_WEIGHT_PREFIX):
            name = name[len(LEGACY_WEIGHT_PREFIX):]
        if name not in WEIGHT_NAMES:
            problems.append(f"{entry!r} (expected one of {', '.join(WEIGHT_NAMES)} as name=number)")
            continue
        number = _finite(value)
        if number is None:
            problems.append(f"{entry!r} ({value!r} is not a number)")
            continue
        overrides[name] = number
    return overrides, problems


# ---------------------------------------------------------------------------
# League weights (#309). Not a #208 merge: it lives here because it shares the
# "name=number" tokenizer with Scoring weights above, and one parser for both
# settings is what keeps their accepted syntax from drifting apart.
# ---------------------------------------------------------------------------
LEAGUE_WEIGHTS_SETTING = "league_weights"

# The largest multiplier accepted. Not a taste limit: the 0-10 compression
# saturates by raw ~48, so anything past this already pins a scored game at 10,
# while a huge finite value (1e308) overflows raw * weight to inf, which lands in
# cache.json and turns adaptive scoring's median into NaN for the whole batch.
MAX_LEAGUE_WEIGHT = 100.0


def parse_league_weights(text: Any) -> Tuple[Dict[str, float], List[str]]:
    """Parse "NFL=1.5, epl=0.7" into {casefolded league code: multiplier}.

    Codes are casefolded because the real ones are mixed case ("LaLiga",
    "SerieA") and a user typing "laliga" means the same league. Whether a code
    is a league that exists is NOT decided here (that needs the sources this
    refresh builds); this only rejects what can never be a weight: a missing
    code or value, a value that is not a finite number, a negative one (a
    negative multiplier would flip the sign of every score in the league and
    sort its best game last), and one above MAX_LEAGUE_WEIGHT. 0 is allowed
    and scores the league at 0, which still lets its games fill an otherwise
    short list."""
    weights: Dict[str, float] = {}
    problems: List[str] = []
    for entry, name, value in _name_number_entries(text):
        if not name:
            problems.append(f"{entry!r} (expected LEAGUE=number, e.g. NFL=1.5)")
            continue
        number = _finite(value)
        if number is None:
            problems.append(f"{entry!r} ({value!r} is not a number)")
            continue
        if number < 0:
            problems.append(f"{entry!r} (a league weight cannot be negative)")
            continue
        if number > MAX_LEAGUE_WEIGHT:
            problems.append(f"{entry!r} (a league weight cannot be above {MAX_LEAGUE_WEIGHT:g})")
            continue
        weights[name.casefold()] = number
    return weights, problems


def _finite(raw: Any) -> Optional[float]:
    """float(raw), or None when it is not a finite number. "nan" and "inf"
    parse as floats but would turn every affected game's score into NaN or
    inf, which then sorts arbitrarily and lands in the cache."""
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def weight_overrides(settings: Dict[str, Any]) -> Tuple[Dict[str, float], List[str]]:
    """The weights the user has set, by name, plus any parse problems.

    The same precedence as the selects, with blank as the field's default:
    a non-blank `scoring_weights` is the whole answer (weights it does not
    name take their defaults, which is what the modal showed beside it);
    while it is blank on an install not yet migrated, the stored legacy
    `weight_<name>` values are the answer. Mixing the two per weight would
    score a pre-migration refresh differently from a post-migration one,
    because the migration keeps typed text verbatim. Anything still missing
    takes the Weights default (the caller's job). A legacy value that is not
    a finite number (a NumberInput cleared to "") is skipped and reported:
    before #208 it raised ValueError out of the refresh."""
    overrides, problems = parse_weight_overrides(settings.get(SCORING_WEIGHTS_SETTING))
    if not _legacy_decides(settings, SCORING_WEIGHTS_SETTING, "", _LEGACY_WEIGHT_KEYS):
        return overrides, problems
    for name in WEIGHT_NAMES:
        legacy = LEGACY_WEIGHT_PREFIX + name
        if legacy not in settings:
            continue
        number = _finite(settings[legacy])
        if number is None:
            problems.append(f"{legacy}={settings[legacy]!r} is not a number")
            continue
        overrides[name] = number
    return overrides, problems


def _format_number(value: float) -> str:
    """The shortest text that parses back to exactly `value`: "6" rather than
    "6.0", and never the 6-significant-digit rounding of :g (1.244995 would
    otherwise migrate as 1.245 and move every score it touches)."""
    short = f"{value:g}"
    return short if float(short) == value else repr(float(value))


def format_weight_overrides(overrides: Dict[str, float]) -> str:
    """Render overrides as the `scoring_weights` text, in Weights field order.
    Lossless: parsing the result gives back exactly `overrides`."""
    return ", ".join(f"{n}={_format_number(overrides[n])}" for n in WEIGHT_NAMES if n in overrides)


def _merge_legacy_keys(stored: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The stored dict with every merged key written from its legacy keys and
    the schema marker stamped, or None when there is nothing to merge
    (already migrated, or no legacy key stored).

    Each new key is computed by the SAME resolver the runtime uses, so the
    migrated dict behaves identically to the unmigrated one, except that a new
    key the user already saved is kept as saved (the raw dict proves it was
    saved; see the module docstring). Only specs whose legacy keys are
    actually stored are written. Legacy keys are kept (module docstring:
    what that does and does not buy on a downgrade)."""
    if is_migrated(stored) or not any(k in stored for k in LEGACY_KEYS):
        return None
    out = dict(stored)
    for select_id, ((_, key_a), (_, key_b)) in LEAGUE_PAIRS.items():
        # A select already in the raw dict was saved by the user; keep it,
        # even at "off" (see "The migration reads the RAW stored dict").
        if select_id not in stored and (key_a in stored or key_b in stored):
            out[select_id] = league_pair_value(stored, select_id)
    if NCAAF_SETTING not in stored and any(k in stored for k in _NCAAF_LEGACY_KEYS):
        divisions = ncaaf_divisions(stored)
        if divisions is None:
            out[NCAAF_SETTING] = NCAAF_OFF
        else:
            # Write a value the select offers; NcaafSource would have mapped an
            # unrecognised one to the default anyway, so behaviour is unchanged.
            out[NCAAF_SETTING] = divisions if divisions in DIVISION_SETS else DEFAULT_DIVISIONS
    # Same raw-presence rule as the selects: Scoring weights already in the
    # raw dict was saved by the user, from a modal that showed it blank (the
    # default) rather than their legacy weights, so what they typed, blank
    # included, is what they meant and is kept verbatim.
    if SCORING_WEIGHTS_SETTING not in stored and any(k in stored for k in _LEGACY_WEIGHT_KEYS):
        carried, _ = weight_overrides(stored)
        if carried:
            out[SCORING_WEIGHTS_SETTING] = format_weight_overrides(carried)
    out[SETTINGS_SCHEMA_KEY] = SETTINGS_SCHEMA_VERSION
    return out


# ---------------------------------------------------------------------------
# Dispatcharr 0.34 settings types (#358): multiselects and tables.
#
# Four comma-separated name lists became `multiselect` fields (stored as a JSON
# list of option values) and the two "name=number" texts became `table` fields
# (stored as a list of {name, weight} rows). Two facts about 0.34, read from
# its source at v0.34.1, decide how saved installs are carried over:
#
#   1. The UI CRASHES on the old shape. `Field.jsx` renders a multiselect with
#      `(value ?? field.default ?? []).map(...)`, and a string has no `.map`,
#      so one saved "Sports, Soccer" breaks the whole settings page. A table
#      reads a string as no rows. So every saved string is rewritten as a
#      list, once, by `migrate_stored_settings` (plugin.py writes it from
#      discovery, before the plugins list is served).
#   2. Saving REJECTS a value that is not an option. `update_settings` runs
#      `_validate_multiselect_value` (every value must be one of `options`)
#      and `_validate_table_value` (each cell must match its column type) on
#      the whole submitted dict. So `build_fields` adds every saved value to
#      its field's options, a renamed profile included: without that, one
#      stale name would make every later save of ANY setting fail, and the
#      UI also re-saves settings before running an action.
#
# Readers still accept both shapes (`name_list`, `_name_number_entries`): the
# stored dict can hold a string until the migration's compare-and-swap lands,
# a downgrade-then-upgrade writes strings again, and `params` passed to an
# action can carry either.
# ---------------------------------------------------------------------------
RECORD_LEAGUES_SETTING = "record_leagues"
ENABLED_PROFILES_SETTING = "enabled_channel_profiles"
FALLBACK_ONLY_GROUPS_SETTING = "fallback_only_groups"
EXCLUDED_GROUPS_SETTING = "excluded_groups"
NAME_LIST_SETTINGS: Tuple[str, ...] = (
    RECORD_LEAGUES_SETTING, ENABLED_PROFILES_SETTING,
    FALLBACK_ONLY_GROUPS_SETTING, EXCLUDED_GROUPS_SETTING,
)
WEIGHT_TABLE_SETTINGS: Tuple[str, ...] = (SCORING_WEIGHTS_SETTING, LEAGUE_WEIGHTS_SETTING)
WEIGHT_ROW_NAME = "name"
WEIGHT_ROW_WEIGHT = "weight"
# Appended to the label of a saved value that is no longer an option (a
# renamed or deleted profile or group), so the picker shows it for what it is.
MISSING_OPTION_SUFFIX = " (not found)"


def name_list(raw: Any, canonical: Optional[Dict[str, str]] = None) -> List[str]:
    """A name-list setting as clean names, from a multiselect list or the
    legacy comma-separated text.

    Order-preserving (Leagues to record is a priority order) and
    case-insensitively deduped, because the value is reported back to the
    user and a list echoing a name twice reads as a bug. Blank, None or a
    non-text, non-list value yields [], which every caller reads as "no
    restriction". `canonical` maps a casefolded name to its option spelling
    ("epl" -> "EPL"), so a legacy hand-typed value migrates onto the option
    the picker offers instead of beside it."""
    if isinstance(raw, str):
        parts: List[Any] = raw.split(",")
    elif isinstance(raw, list):
        parts = raw
    else:
        return []
    out: List[str] = []
    seen = set()
    for part in parts:
        if part is None:
            continue
        name = str(part).strip()
        if not name or name.casefold() in seen:
            continue
        seen.add(name.casefold())
        out.append((canonical or {}).get(name.casefold(), name))
    return out


def _weight_number(value: float) -> Any:
    """6.0 -> 6, so a migrated row shows what the user typed."""
    return int(value) if float(value).is_integer() and abs(value) < 2 ** 53 else value


def weight_rows(setting_id: str, raw: Any) -> List[Dict[str, Any]]:
    """Legacy "name=number" text as table rows, or `raw` unchanged when it is
    already a list.

    Kept: every entry whose value is a finite number and, for Scoring
    weights, whose name is a weight (spelled as the Signal column offers it:
    lower case, the old "weight_" prefix dropped). Dropped: entries the
    runtime already ignored and reported (no name, no number, an unknown
    weight), because a row the table cannot hold would make every later save
    fail. League weights keep any code and any number: whether a code is a
    league and whether its weight is in range is decided per refresh, which
    reports it."""
    if isinstance(raw, list):
        return raw
    rows: List[Dict[str, Any]] = []
    for _entry, name, value in _name_number_entries(raw):
        number = _finite(value)
        if not name or number is None:
            continue
        if setting_id == SCORING_WEIGHTS_SETTING:
            name = name.lower()
            if name.startswith(LEGACY_WEIGHT_PREFIX):
                name = name[len(LEGACY_WEIGHT_PREFIX):]
            if name not in WEIGHT_NAMES:
                continue
        rows.append({WEIGHT_ROW_NAME: name, WEIGHT_ROW_WEIGHT: _weight_number(number)})
    return rows


def _convert_list_settings(stored: Dict[str, Any],
                           league_codes: Tuple[str, ...]) -> Dict[str, Any]:
    """`stored` with every legacy string in a #358 list setting rewritten as
    a list (see the section comment). Keys that are absent stay absent: the
    manifest default fills them in, and an absent key is not a saved one."""
    out = dict(stored)
    canonical = {code.casefold(): code for code in league_codes}
    for key in NAME_LIST_SETTINGS:
        raw = out.get(key)
        if isinstance(raw, str):
            out[key] = name_list(raw, canonical if key == RECORD_LEAGUES_SETTING else None)
    for key in WEIGHT_TABLE_SETTINGS:
        raw = out.get(key)
        if isinstance(raw, str):
            out[key] = weight_rows(key, raw)
    return out


def migrate_stored_settings(stored: Dict[str, Any],
                            league_codes: Tuple[str, ...] = ()) -> Optional[Dict[str, Any]]:
    """The stored dict brought to the current layout, or None when it is
    already there.

    Two steps, in order: the #208 merge (legacy toggles and weights into the
    merged keys, stamping SETTINGS_SCHEMA_KEY), then the #358 conversion of
    string values into multiselect lists and table rows. The second runs on
    the first's output, so seven legacy `weight_<name>` numbers end up as
    Scoring weights rows. The #358 step is keyed on the value's TYPE, not on
    a stamp: a string in one of those settings is always the old shape,
    whoever wrote it. `league_codes` are the Leagues to record options, so
    "epl" migrates as the "EPL" the picker offers."""
    merged = _merge_legacy_keys(stored)
    out = _convert_list_settings(merged if merged is not None else stored, tuple(league_codes))
    return None if out == stored else out


def _option(value: str, label: Optional[str] = None) -> Dict[str, str]:
    return {"value": value, "label": label if label is not None else value}


def build_fields(fields: List[Dict[str, Any]], saved: Dict[str, Any],
                 dynamic_options: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """The manifest's fields with each multiselect's options filled in for
    this install: `Plugin.get_fields`' body, pure so it can be tested.

    `dynamic_options` maps a field id to the names that exist right now
    (Channel Profiles, channel groups); a field not in it keeps its manifest
    options (Leagues to record's static league codes). Every value saved in
    `saved` (the raw stored dict, which may still hold the legacy string)
    that is not an option is appended, labelled MISSING_OPTION_SUFFIX when it
    is a name the install no longer has, so the picker shows it and a save
    does not fail on it (see the section comment). Option values are
    compared exactly, as `_validate_multiselect_value` compares them."""
    out: List[Dict[str, Any]] = []
    for field in fields:
        field = dict(field)
        if field.get("type") == "multiselect":
            fid = field.get("id")
            if fid in dynamic_options:
                options = [_option(n) for n in dynamic_options[fid]]
            else:
                options = [dict(o) for o in field.get("options") or []]
            offered = {o["value"] for o in options}
            canonical = ({o["value"].casefold(): o["value"] for o in options}
                         if fid == RECORD_LEAGUES_SETTING else None)
            for name in name_list(saved.get(fid), canonical):
                if name not in offered:
                    options.append(_option(name, name + MISSING_OPTION_SUFFIX))
                    offered.add(name)
            field["options"] = options
        out.append(field)
    return out
