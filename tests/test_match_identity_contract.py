"""Every emitted GameRow must carry the identity key the simulator compares on.

`simulation._same_match` resolves target-match identity from `_MATCH_ID_KEYS`
(`fd_id`, `game_id`), and consults a key only when BOTH rows carry it. The
fixture pool comes from `PointsBasedSportSource.remaining_matches`, which stamps
`extra["game_id"]`. So a source whose `fetch_upcoming` stamps only its own
`<sport>_game_id` shares no key with the pool, and identity silently degrades to
the `(home, away, start_time)` fallback.

That fallback is not a safe substitute, for two reasons:

  - `points_based.remaining_matches` substitutes a 2099-01-01 sentinel for a
    fixture with no published `start_time`, so the two rows disagree outright.
  - a RESCHEDULED game (routine in MLB and NFL) has a different kickoff in the
    two separate fetches, which is the same disagreement without needing a
    missing date at all.

Either way the target fails to match itself, gets sampled a second time as a
"remaining" match, and the contingency table is built from a season the recorded
W/D/L row does not describe. Nothing raises; the importance number is just wrong.
MLS had exactly this and it was LIVE there (#65). This is the generalised guard
(#181), written as an AST check rather than four per-sport simulations because
the alternative is four ESPN response fixtures that rot independently, and
because a rule over the whole `sources/` tree also catches a source nobody has
written yet.
"""

import ast
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SOURCES_DIR = os.path.join(REPO_ROOT, "sources")

# The shared key the simulator and the fixture pool both use.
IDENTITY_KEY = "game_id"

# Bases whose subclasses implement the importance-simulation protocol, and are
# therefore the only ones this contract binds. `SportSource` itself raises
# NotImplementedError from remaining_matches / apply_result, so a source that
# inherits it directly and does not override them NEVER reaches the simulator
# and cannot exhibit the defect.
#
# That exemption is load-bearing, not laziness: `sources/mls.py` (MlsSource, the
# base for MLS / NWSL / Liga MX emit) and `sources/friendlies.py` both stamp a
# sport-specific id and both inherit SportSource directly. Stamping `game_id`
# there would be inert. DO NOT "fix" them to silence a broadened version of this
# test; widen the base list only when such a source really starts simulating.
# Names verified against the real class graph, not guessed: the bracket base is
# `BracketSportSource`, NOT `BracketSource`. `test_simulating_bases_all_exist`
# exists because a name that matches nothing empties the population and turns
# this whole file green for the wrong reason.
SIMULATING_BASES = frozenset({
    "PointsBasedSportSource",   # regular-season standings importance
    "BracketSportSource",       # bracket root
    "BestOfNSeriesSource",      # playoff series (bracket subclass)
    "DoubleEliminationSource",  # NCAA baseball / softball postseason
    "AggregateLegSource",       # two-legged soccer knockout ties
})


def _extra_dicts_with_a_sport_specific_id(path):
    """Yield (function_name, dict_keys) for every `extra={...}` literal in `path`
    that stamps a `<something>_game_id` or `<something>_event_id` key.

    Those are the rows a source hands out as its own identity, which is exactly
    the population that must also carry the shared key.
    """
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src, filename=os.path.basename(path))
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for node in ast.walk(fn):
            if not isinstance(node, ast.keyword) or node.arg != "extra":
                continue
            if not isinstance(node.value, ast.Dict):
                continue
            keys = [
                k.value for k in node.value.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)
            ]
            if any(
                k.endswith("_game_id") or k.endswith("_event_id") for k in keys
            ):
                yield fn.name, keys


def _defines_a_simulating_source(path):
    """Whether `path` defines any class inheriting a simulator-protocol base."""
    tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for base in node.bases:
            name = base.id if isinstance(base, ast.Name) else (
                base.attr if isinstance(base, ast.Attribute) else None
            )
            if name in SIMULATING_BASES:
                return True
    return False


def _source_files():
    """Source modules bound by this contract: those that actually simulate."""
    return sorted(
        p for p in (
            os.path.join(SOURCES_DIR, f)
            for f in os.listdir(SOURCES_DIR)
            if f.endswith(".py")
        )
        if _defines_a_simulating_source(p)
    )


class TestEmittedRowsCarryTheSharedIdentityKey:
    def test_the_check_finds_something(self):
        """Positive control: a rule that matches nothing would pass vacuously.

        Several sources DO stamp a sport-specific id, so an empty population
        means the AST walk is broken, not that the codebase is clean.
        """
        found = [
            (os.path.basename(p), fn)
            for p in _source_files()
            for fn, _ in _extra_dicts_with_a_sport_specific_id(p)
        ]
        assert found, "AST walk found no sport-specific id keys at all"

    @pytest.mark.parametrize(
        "path", _source_files(), ids=lambda p: os.path.basename(p),
    )
    def test_every_sport_specific_id_is_paired_with_game_id(self, path):
        offenders = [
            (fn, keys)
            for fn, keys in _extra_dicts_with_a_sport_specific_id(path)
            if IDENTITY_KEY not in keys
        ]
        assert not offenders, (
            f"{os.path.basename(path)} emits a sport-specific id without "
            f"'{IDENTITY_KEY}': {offenders}. The fixture pool and "
            "simulation._same_match both key on 'game_id', so this row cannot "
            "be recognised as the target and will be simulated twice per "
            "season whenever the pool's kickoff disagrees (a date-less "
            "fixture, or a reschedule). See #181."
        )


class TestTheSimulatorStillHonoursTheKey:
    """Pin the other half of the contract, so the fix cannot be undone from the
    simulator side while the sources stay correct."""

    def test_game_id_is_an_identity_key(self):
        from dispatcharr_ranked_matchups import simulation
        assert IDENTITY_KEY in simulation._MATCH_ID_KEYS

    def test_points_based_pool_stamps_game_id(self):
        # The pool side of the pairing. Read as source text because
        # remaining_matches needs a live fetch to run.
        src = open(
            os.path.join(SOURCES_DIR, "points_based.py"), encoding="utf-8",
        ).read()
        i = src.index("def remaining_matches")
        body = src[i:src.index("\n    def ", i + 10)]
        assert f'"{IDENTITY_KEY}"' in body, (
            "points_based.remaining_matches no longer stamps game_id; the "
            "pairing this whole contract rests on is broken"
        )


class TestTheScopeItselfIsRight:
    """The exemption above is a claim about the code, so pin it.

    If `mls.py` or `friendlies.py` ever start simulating, they silently leave
    the contract's population and the defect can return there unnoticed.
    """

    def test_simulating_bases_all_exist(self):
        """A typo'd base name would empty the population and pass vacuously."""
        seen = set()
        for f in os.listdir(SOURCES_DIR):
            if not f.endswith(".py"):
                continue
            tree = ast.parse(
                open(os.path.join(SOURCES_DIR, f), encoding="utf-8").read()
            )
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    for b in node.bases:
                        if isinstance(b, ast.Name):
                            seen.add(b.id)
        missing = SIMULATING_BASES - seen
        assert not missing, f"base names in SIMULATING_BASES never used: {missing}"

    @pytest.mark.parametrize("name", ["mls.py", "friendlies.py"])
    def test_documented_exemptions_still_do_not_simulate(self, name):
        path = os.path.join(SOURCES_DIR, name)
        assert not _defines_a_simulating_source(path), (
            f"{name} now defines a simulating source, so it is no longer exempt "
            "from the game_id contract. Stamp game_id in its emitted rows and "
            "delete it from this exemption list. See #181."
        )

    def test_population_is_not_empty(self):
        assert _source_files(), "no simulating sources found; the scope filter is broken"
