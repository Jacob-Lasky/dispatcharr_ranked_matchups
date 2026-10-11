"""Settings that #208 merged to shorten the settings modal, and the fallback
that keeps installs saved before the merge behaving exactly as they did.

WHY THIS MODULE EXISTS. Dispatcharr renders a plugin's settings as one flat
list (`PluginFieldList` maps `plugin.fields` straight to inputs; the only
field types are string / number / boolean / select / text / info, and there is
no collapsing or grouping primitive, verified against 0.30.0). So the modal
can only get shorter by having FEWER fields. #208 merged three shapes:

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
    and the new key is absent, cleared, or at its default."""
    if is_migrated(settings):
        return False
    if not any(k in settings for k in legacy_keys):
        return False
    raw = settings.get(new_key)
    return raw is None or raw == "" or raw == default


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


def parse_weight_overrides(text: Any) -> Tuple[Dict[str, float], List[str]]:
    """Parse "favorite=8, spread=1" into {name: value}.

    Entries split on commas, semicolons or newlines; names are
    case-insensitive and may carry the old "weight_" prefix (so a value pasted
    from the old field names still works). Returns (overrides, problems): an
    entry with an unknown name or a non-numeric value is skipped and described
    in `problems` rather than failing the refresh."""
    overrides: Dict[str, float] = {}
    problems: List[str] = []
    if text is None:
        return overrides, problems
    for chunk in str(text).replace(";", ",").replace("\n", ",").split(","):
        entry = chunk.strip()
        if not entry:
            continue
        name, sep, value = entry.partition("=")
        if not sep:
            name, sep, value = entry.partition(":")
        name = name.strip().lower()
        if name.startswith(LEGACY_WEIGHT_PREFIX):
            name = name[len(LEGACY_WEIGHT_PREFIX):]
        if not sep or name not in WEIGHT_NAMES:
            problems.append(f"{entry!r} (expected one of {', '.join(WEIGHT_NAMES)} as name=number)")
            continue
        number = _finite(value.strip())
        if number is None:
            problems.append(f"{entry!r} ({value.strip()!r} is not a number)")
            continue
        overrides[name] = number
    return overrides, problems


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

    Per weight: an entry in `scoring_weights` wins; otherwise, on an install
    not yet migrated, a stored legacy `weight_<name>` value fills the gap.
    Anything still missing takes the Weights default (the caller's job). A
    legacy value that is not a number (a NumberInput cleared to "") is skipped
    and reported: before #208 it raised ValueError out of the refresh."""
    overrides, problems = parse_weight_overrides(settings.get(SCORING_WEIGHTS_SETTING))
    if is_migrated(settings):
        return overrides, problems
    for name in WEIGHT_NAMES:
        legacy = LEGACY_WEIGHT_PREFIX + name
        if name in overrides or legacy not in settings:
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


def migrate_stored_settings(stored: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The stored dict with every merged key written from its legacy keys and
    the schema marker stamped, or None when there is nothing to migrate
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
