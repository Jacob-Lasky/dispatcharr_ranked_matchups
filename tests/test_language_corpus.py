"""#206 CLASS guard for language detection, driven by real stream names.

The per-case tests in test_plugin_helpers.py are INSTANCE-shaped: they pin the
codes and decoys we already know about. The defect CLASS is different and wider:
**a two-letter code added to `_LANG_TAG_TO_CODE` collides with a domain term
that is not a language.** Every false positive found while building #206 was an
instance of it, and none of them was predicted:

    NO   -> 1588 "TSN+ 08: NO EVENT" rows read as Norwegian
    SE   -> "SE Missouri" / "SE Louisiana" (Southeast) read as Swedish
    AR   -> "Little Rock, AR" (Arkansas) read as Arabic
    PL   -> "PL Saturday Wrap" / "PL Live" (Premier League) read as Polish

An instance test (`assert "PL" not in _LANG_TAG_TO_CODE`) does not cover the
class: the NEXT colliding code sails past it. So this file runs the real
detector over a fixture of real names and fails on a collision nobody has
thought of yet.

The fixture is a sample of a live 16,970-name corpus, kept to every distinct
leading two-letter tag (capped per tag+group), a spread of any-position
two-letter tokens so a future code has decoys to trip on, and the measured
false-positive shapes above.
"""

import importlib.util
import json
import os
import re
import sys
import types

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "stream_names_corpus.json")

# Substrings that are NOT language tags, each one measured in the live corpus.
# A detection on any row containing one of these is a collision.
# Each entry must LEAD with the two-letter token under test, because the guards
# below derive that token with `shape.split()[0]`. "Little Rock, AR" was in the
# first cut and silently tested nothing: its first word is "Little", which is not
# two letters, so every assertion skipped.
DECOY_SHAPES = (
    "NO EVENT",
    "No Scheduled",
    "SE Missouri",
    "SE Louisiana",
    "AR)",              # "...(IN LITTLE ROCK, AR)" -- Arkansas, not Arabic
    "PL Saturday",
    "PL Live",
    "PL Weekend",
    # Found by an external review IN THIS FIXTURE, none of them predicted, and
    # none caught by the first cut because it only looked at the start of a name.
    "MK Dons",          # Milton Keynes Dons, not Macedonian
    "CF Monterrey",     # Club de Futbol, not Central African
    "St Helens",        # Saint Helens, not Sao Tome
    "TV TBA",           # television, not Tuvalu
)

# Rows leading with these tags are US Spanish-language broadcasters, so the tag
# must never resolve to English. Kept separate from DECOY_SHAPES because a
# Spanish DETECTION here is correct (via " DEPORTES " / " TUDN "); what is wrong
# is reading the "US" tag itself as a language.
US_TAG_PREFIX = re.compile(r"^\s*US\s*(?::|\|)")

LEADING_TAG = re.compile(r"^\s*([A-Za-z]{2})\s*(?::|\|)")

# A tag can also sit straight after a '|', which is a position the detector
# genuinely matches. The first cut only looked at the start of the name, so
# "EFL14x| MK Dons 12:30 Leicester City" was invisible to every guard here.
ANY_TAG_POSITION = re.compile(r"(?:^|\|)\s*([A-Za-z]{2})\s*(?::|\||\s)")


@pytest.fixture(scope="module")
def corpus():
    with open(FIXTURE, encoding="utf-8") as fh:
        rows = json.load(fh)
    return rows


class TestCorpusIsUsable:
    """Positive controls. A guard that reads a file must fail on the INSTRUMENT
    before it can be trusted to fail on the code: a fixture that failed to load,
    or that lost its decoys, would make every assertion below vacuous and
    permanently green."""

    def test_fixture_loaded_and_is_substantial(self, corpus):
        assert len(corpus) > 300, f"fixture looks truncated: {len(corpus)} rows"
        assert all("name" in r and "group" in r for r in corpus)

    @pytest.mark.parametrize("shape", DECOY_SHAPES)
    def test_every_decoy_shape_is_actually_present(self, corpus, shape):
        hits = [r for r in corpus if shape in r["name"]]
        assert hits, (
            f"decoy shape {shape!r} is missing from the fixture, so the "
            "collision guard for it cannot fail"
        )

    def test_us_tag_rows_are_present(self, corpus):
        assert [r for r in corpus if US_TAG_PREFIX.match(r["name"])]

    def test_the_detector_fires_on_something(self, corpus, plugin):
        """A detector that returned None for everything would pass every
        collision assertion in this file."""
        detected = [r for r in corpus
                    if plugin._detect_stream_language(r["name"]) is not None]
        assert len(detected) > 100, (
            f"only {len(detected)} detections across {len(corpus)} rows; the "
            "detector looks broken, so the guards below prove nothing"
        )


class TestNoTagCollidesWithADomainTerm:
    """The class guard."""

    @pytest.mark.parametrize("shape", DECOY_SHAPES)
    def test_decoy_shapes_never_resolve_via_a_tag(self, corpus, plugin, shape):
        """A row containing a measured decoy must not be detected BY ITS TAG.

        Detection through a genuinely different signal is allowed and does
        happen: "AU (STAN 97) | PL Saturday Wrap" is correctly English via the
        leading AU tag. What must never happen is the DECOY becoming the tag.
        """
        offenders = []
        decoy_token = shape.split()[0].strip(",)").upper()
        for r in corpus:
            if shape not in r["name"]:
                continue
            tag = plugin._language_tag_code(r["name"])
            if tag is None:
                continue
            if decoy_token in plugin._LANG_TAG_TO_CODE and \
                    tag == plugin._LANG_TAG_TO_CODE[decoy_token]:
                offenders.append((r["name"][:70], tag))
        assert not offenders, (
            f"{shape!r} is being read as a language tag: {offenders[:5]}"
        )

    @pytest.mark.parametrize("shape", DECOY_SHAPES)
    def test_a_domain_term_is_only_mapped_if_the_corpus_earns_it(self, corpus, plugin, shape):
        """The real class guard. The in-situ test above is NOT enough.

        Measured: adding "PL" to the map does not trip the in-situ check,
        because every "PL Saturday Wrap" row in this corpus happens to be
        prefixed "AU (STAN 97) | " and the earliest-tag rule hands the answer to
        AU. The collision is masked by an accident of one provider's naming, and
        a provider emitting "PL Live - 5 September" unprefixed would read as
        Polish with nothing reporting it.

        So this asks a question about the MAP rather than about which rows a
        snapshot happens to hold, which is what makes it cover the class:

            a two-letter domain term may be mapped ONLY IF the corpus shows it
            used as a genuine language tag somewhere.

        "NO" passes: it collides with "NO EVENT" (1588 rows) and ALSO has 38
        real "NO:" rows in "Norway | Sports", so tag position genuinely
        separates them. "PL" and "SE" fail: zero legitimate rows, pure downside.
        Removing "SE" from the first cut of the map is this test's first catch.
        """
        token = shape.split()[0].strip(",)").upper()
        if len(token) != 2:
            pytest.skip(f"{shape!r} does not lead with a two-letter token")
        mapped = plugin._LANG_TAG_TO_CODE.get(token)
        if mapped is None:
            return  # not mapped, so it cannot collide
        # Mapped: the corpus must show it earning its place as a real tag.
        # An explicit table, and a KeyError rather than a fallback. The first
        # cut fell back to the bare language code as a substring, which is both
        # too loose ("es" matches "matches", so a generic group name could
        # "prove" Spanish) and too tight (a group named for a country whose
        # name does not contain the ISO code fails a legitimate mapping).
        expected_groups = {
            "no": "norway", "sv": "sweden", "pl": "poland", "ar": "arabic",
            "da": "denmark", "ru": "russia", "mk": "macedon", "pt": "portug",
            "cs": "czech", "de": "german", "tr": "turkey", "it": "ital",
            "nl": "netherland", "el": "gree", "es": "spain", "fr": "france",
            "ja": "japan", "ko": "korea", "zh": "chin", "en": "uk",
        }
        assert mapped in expected_groups, (
            f"{token!r} maps to {mapped!r}, which has no entry in this test's "
            "group table. Add one rather than letting the guard skip it."
        )
        marker = expected_groups[mapped]
        legit = [
            r for r in corpus
            if LEADING_TAG.match(r["name"])
            and LEADING_TAG.match(r["name"]).group(1).upper() == token
            and marker in (r["group"] or "").lower()
        ]
        assert legit, (
            f"{token!r} is mapped to {mapped!r} but is a measured domain term "
            f"({shape!r}) with NO corpus row using it as a real language tag. "
            "Remove it from _LANG_TAG_TO_CODE; the language can still resolve "
            "via its explicit feed label."
        )

    def test_pl_never_resolves_as_polish_anywhere_in_the_corpus(self, corpus, plugin):
        bad = [r["name"][:70] for r in corpus
               if plugin._detect_stream_language(r["name"]) == "pl"]
        assert not bad, f"Premier League rows read as Polish: {bad[:5]}"

    def test_the_us_tag_never_resolves_as_english(self, corpus, plugin):
        """All real "US |" rows are Spanish-language US broadcasters."""
        bad = []
        for r in corpus:
            if not US_TAG_PREFIX.match(r["name"]):
                continue
            if plugin._detect_stream_language(r["name"]) == "en":
                bad.append(r["name"][:70])
        assert not bad, f'"US" tag rows read as English: {bad[:5]}'

    def test_an_unmapped_leading_tag_produces_no_tag_detection(self, corpus, plugin):
        """A two-letter token we have NOT mapped must not resolve as a tag.

        Catches a regex change that starts matching codes the map never
        declared, which would then resolve to None and read as "no signal"
        while silently shadowing the weaker checks below it.
        """
        offenders = []
        for r in corpus:
            m = ANY_TAG_POSITION.search(r["name"])
            if not m:
                continue
            tag = m.group(1).upper()
            if tag in plugin._LANG_TAG_TO_CODE:
                continue
            if plugin._language_tag_code(r["name"]) is not None:
                # Only an offender if the unmapped LEADING token is what matched.
                mm = plugin._LANG_TAG_DELIMITED.search(r["name"]) or \
                    plugin._LANG_TAG_BARE.search(r["name"])
                if mm and mm.group(1).upper() == tag:
                    offenders.append((r["name"][:60], tag))
        assert not offenders, f"unmapped tags resolving: {offenders[:5]}"


class TestMappedTagsActuallyFire:
    """The other direction: a code in the map that the regex never matches is
    dead weight that reads as working. Catches a regex tightening that silently
    drops a tag form."""

    def test_every_mapped_tag_present_in_the_corpus_resolves(self, corpus, plugin):
        present = {}
        for r in corpus:
            m = LEADING_TAG.match(r["name"])
            if m:
                present.setdefault(m.group(1).upper(), []).append(r["name"])
        dead = []
        for tag, names in present.items():
            if tag not in plugin._LANG_TAG_TO_CODE:
                continue
            expected = plugin._LANG_TAG_TO_CODE[tag]
            if not any(plugin._detect_stream_language(n) == expected for n in names):
                dead.append((tag, expected, names[0][:60]))
        assert not dead, (
            "mapped tags that never resolve on real rows leading with them: "
            f"{dead}"
        )

    # A group-vs-detection consistency guard was written here and then REMOVED
    # rather than weakened, because its own positive control proved it had no
    # population to judge.
    #
    # The idea was: a detection that contradicts its channel group's language is
    # evidence of a bug. Two things killed it.
    #
    # 1. Tagged rows cannot be judged this way. An explicit tag is DESIGNED to
    #    outrank the group, and real providers misfile: "DE: Magenta Sport 1"
    #    sits in "Turkey | Sports" and Magenta Sport really is German, so the
    #    detector is right and the group is wrong. Including tagged rows made
    #    the guard fail on correct behaviour.
    # 2. Excluding tagged rows left ZERO rows to check: every specific-code
    #    detection in a language-named group in this corpus comes from a tag.
    #    Untagged rows in those groups resolve to None or _LANG_NOT_EN, both of
    #    which the guard skips.
    #
    # Dropping the assertion threshold to make it pass would have produced a
    # test that cannot fail, which is worse than no test. What covers this
    # ground instead: test_a_domain_term_is_only_mapped_if_the_corpus_earns_it
    # (bad map entries), the decoy guards above (collisions), and a manual
    # sweep of the FULL 16,970-name corpus recorded in PR #207, which showed
    # every specific code concentrating in its matching group.
