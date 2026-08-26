"""The marketing numbers in plugin.json's description are factual claims.

The description says the plugin covers N leagues/tours/competitions, M of
them soccer. Those are assertions about the `enable_*` toggles in the same
manifest, and nothing stopped them drifting apart: add a sport and the
number silently becomes a lie on the public listing page.

This is not hypothetical drift-proofing. The README and CLAUDE.md both
carried an "open work" list that had gone 100% stale (all eight referenced
issues closed, two "not yet implemented" adapters actually shipped), and
the README advertised the rivalry signal as "rivalry DB pending" long
after rivalries.json landed. Prose about the code rots unless a test
holds it to the code.

Text-only (no imports of the plugin package, no Django) so it runs
standalone like tests/test_version_consistency.py.
"""
import json
import os
import re

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Toggles that are features, not sport sources, and so must not count
# toward the description's sport total.
NON_SPORT_TOGGLES = {"enable_matchup_logos"}

# The soccer competitions, spelled out because plugin.json does not tag a
# toggle with its sport. Adding a soccer competition means adding it here
# AND bumping the "N of them soccer" figure in the description; this test
# failing is the reminder to do both.
SOCCER_TOGGLES = {
    "enable_epl",
    "enable_championship",
    "enable_ucl",
    "enable_bundesliga",
    "enable_la_liga",
    "enable_serie_a",
    "enable_ligue_1",
    "enable_world_cup",
    "enable_euros",
    "enable_intl_friendlies",
    "enable_intl_friendlies_women",
    "enable_club_friendlies",
    "enable_eredivisie",
    "enable_primeira_liga",
    "enable_brazilian_serie_a",
    "enable_mls",
    "enable_nwsl",
    "enable_liga_mx",
    "enable_ncaa_mens_soccer",
    "enable_ncaa_womens_soccer",
    # English domestic cups (#190). ESPN-backed rather than
    # Football-Data.org, which gates every domestic cup behind a paid plan,
    # but soccer competitions all the same.
    "enable_efl_cup",
    "enable_fa_cup",
}


def _read(name):
    with open(os.path.join(_ROOT, name), encoding="utf-8") as fh:
        return fh.read()


def _manifest():
    return json.loads(_read("plugin.json"))


def _toggle_ids():
    return {
        f["id"]
        for f in _manifest()["fields"]
        if f["id"].startswith("enable_")
    }


def _sport_toggle_ids():
    return _toggle_ids() - NON_SPORT_TOGGLES


def test_description_sport_total_matches_toggle_count():
    desc = _manifest()["description"]
    m = re.search(r"(\d+)\s+leagues", desc)
    assert m, (
        "description no longer states a '<N> leagues' figure; either restore "
        "it or delete this test, but do not leave an unchecked number"
    )
    claimed = int(m.group(1))
    actual = len(_sport_toggle_ids())
    assert claimed == actual, (
        f"description claims {claimed} leagues/tours/competitions but "
        f"plugin.json has {actual} sport toggles. Update the description in "
        f"BOTH this repo and the Dispatcharr/Plugins listing entry "
        f"(description is metadata-only there, so no version bump needed)."
    )


def test_description_soccer_count_matches_soccer_toggles():
    desc = _manifest()["description"]
    m = re.search(r"(\d+)\s+of them soccer", desc)
    assert m, (
        "description no longer states a '<N> of them soccer' figure; either "
        "restore it or delete this test, but do not leave an unchecked number"
    )
    claimed = int(m.group(1))
    actual = len(SOCCER_TOGGLES)
    assert claimed == actual, (
        f"description claims {claimed} soccer competitions but "
        f"SOCCER_TOGGLES lists {actual}"
    )


def test_soccer_toggles_all_exist_in_manifest():
    missing = SOCCER_TOGGLES - _toggle_ids()
    assert not missing, (
        f"SOCCER_TOGGLES names toggles absent from plugin.json: "
        f"{sorted(missing)} - a renamed or removed toggle leaves the soccer "
        f"count overstating coverage"
    )


def test_every_toggle_is_wired_in_plugin_py():
    """A toggle in the manifest with no reader renders but does nothing."""
    plugin_src = _read("plugin.py")
    unwired = sorted(t for t in _toggle_ids() if t not in plugin_src)
    assert not unwired, (
        f"these enable_* toggles appear in plugin.json but are never read in "
        f"plugin.py, so the setting renders and silently does nothing: "
        f"{unwired}"
    )


# ---------------------------------------------------------------------------
# SCORING.md's stage table is a factual claim about scoring.py (#190).
# ---------------------------------------------------------------------------

def _scoring_py():
    return _read("scoring.py")


def _code_cup_scalars():
    """The CUP_* scalars as scoring.py actually defines them."""
    src = _scoring_py()
    i = src.index("_CUP_ROUND_STAGE_SCORES: Dict[str, float] = {")
    block = src[i:src.index("}", i) + 1]
    return {
        k: float(v)
        for k, v in re.findall(r'"(CUP_[A-Z0-9_]+)":\s*([0-9.]+)', block)
    }


def _doc_cup_scalars():
    """The CUP_* scalars as SCORING.md advertises them."""
    return {
        k: float(v)
        for k, v in re.findall(
            r"\|\s+`(CUP_[A-Z0-9_]+)`[^|]*\|\s*([0-9.]+)\s*\|",
            _read("SCORING.md"),
        )
    }


def test_scoring_doc_lists_every_cup_stage_the_code_scores():
    code, doc = _code_cup_scalars(), _doc_cup_scalars()
    assert code, "no CUP_* scalars found in scoring.py; the parse is broken"
    missing = set(code) - set(doc)
    assert not missing, (
        f"scoring.py scores {sorted(missing)} but SCORING.md's stage table "
        f"does not list them, so the documented table understates coverage"
    )


def test_scoring_doc_invents_no_cup_stage():
    code, doc = _code_cup_scalars(), _doc_cup_scalars()
    extra = set(doc) - set(code)
    assert not extra, (
        f"SCORING.md advertises {sorted(extra)} but scoring.py does not score "
        f"them, so the doc promises a signal that contributes nothing"
    )


def test_scoring_doc_cup_scalars_match_the_code():
    code, doc = _code_cup_scalars(), _doc_cup_scalars()
    drifted = {k: (code[k], doc[k]) for k in code if k in doc and code[k] != doc[k]}
    assert not drifted, (
        f"SCORING.md and scoring.py disagree on cup stage scalars "
        f"(code, doc): {drifted}"
    )


def test_cup_band_stays_below_the_shared_quarterfinal_scalar():
    """The doc's own claim: the CUP_* ramp lands UNDER QUARTER_FINALS, which
    every knockout competition shares. Overlapping them would rate an FA Cup
    first-round tie at Champions-League-knockout stakes."""
    src = _scoring_py()
    m = re.search(r'"QUARTER_FINALS":\s*([0-9.]+)', src)
    assert m, "QUARTER_FINALS scalar not found in scoring.py"
    qf = float(m.group(1))
    for stage, scalar in _code_cup_scalars().items():
        assert scalar < qf, f"{stage}={scalar} is not below QUARTER_FINALS={qf}"
