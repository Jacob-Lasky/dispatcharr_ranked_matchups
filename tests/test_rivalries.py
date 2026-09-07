"""Tests for the rivalries.json + rivalries.py rivalry-detection helper."""
from __future__ import annotations

import importlib.util
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG_NAME = "dispatcharr_ranked_matchups"


def _load_rivalries_mod():
    mod_name = f"{PKG_NAME}.rivalries"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(REPO_ROOT, "rivalries.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


rivalries = _load_rivalries_mod()


class TestNormalize:
    def test_lowercases(self):
        assert rivalries._normalize("Manchester City") == "manchester city"

    def test_strips_whitespace(self):
        assert rivalries._normalize("  Arsenal   FC  ") == "arsenal fc"

    def test_empty_safe(self):
        assert rivalries._normalize("") == ""
        assert rivalries._normalize(None) == ""


class TestIsRivalry:
    def test_known_epl_pair(self):
        assert rivalries.is_rivalry("Liverpool FC", "Manchester United FC", "EPL")

    def test_order_independent(self):
        # Same pair, swapped: should still match.
        assert rivalries.is_rivalry("Manchester United FC", "Liverpool FC", "EPL")

    def test_case_insensitive(self):
        assert rivalries.is_rivalry("liverpool fc", "MANCHESTER UNITED FC", "EPL")

    def test_substring_handles_fd_org_suffixes(self):
        # FD.org names have trailing FC / AFC; the JSON stores bare names.
        assert rivalries.is_rivalry("Arsenal FC", "Tottenham Hotspur FC", "EPL")
        assert rivalries.is_rivalry("Manchester City FC", "Manchester United FC", "EPL")

    def test_unknown_pair_returns_false(self):
        assert not rivalries.is_rivalry("Brighton", "Burnley", "EPL")

    def test_unknown_sport_returns_false(self):
        assert not rivalries.is_rivalry("Anyone", "Anyone", "NOT_A_REAL_SPORT")

    def test_missing_team_names_safe(self):
        assert not rivalries.is_rivalry("", "Manchester United FC", "EPL")
        assert not rivalries.is_rivalry("Liverpool FC", "", "EPL")
        assert not rivalries.is_rivalry("", "", "EPL")

    def test_ncaa_football_pair(self):
        assert rivalries.is_rivalry("Alabama", "Auburn", "CFB")
        assert rivalries.is_rivalry("Ohio State", "Michigan", "CFB")

    def test_nba_classic(self):
        assert rivalries.is_rivalry("Boston Celtics", "Los Angeles Lakers", "NBA")

    def test_nhl_original_six(self):
        assert rivalries.is_rivalry("Montreal Canadiens", "Boston Bruins", "NHL")

    def test_mlb_yankees_red_sox(self):
        assert rivalries.is_rivalry("New York Yankees", "Boston Red Sox", "MLB")

    def test_la_classico_either_name(self):
        # Real Madrid vs Barcelona: full names + abbreviations.
        assert rivalries.is_rivalry("Real Madrid", "Barcelona", "LaLiga")
        assert rivalries.is_rivalry("Real Madrid CF", "FC Barcelona", "LaLiga")

    def test_paris_sg_abbreviation(self):
        # SportsDB returns "Paris SG"; FD.org returns "Paris Saint-Germain FC".
        # JSON has both forms: both should match Marseille.
        # One entry per SOURCE spelling: Football-Data.org says
        # "Paris Saint-Germain FC" / "Olympique de Marseille", SportsDB says
        # "Paris SG" / "Marseille". Mixing the two halves in one entry
        # half-matched under the old loose rule and matches nothing now.
        assert rivalries.is_rivalry("Paris SG", "Marseille", "Ligue1")
        assert rivalries.is_rivalry(
            "Paris Saint-Germain FC", "Olympique de Marseille", "Ligue1")

    def test_cross_sport_no_false_positive(self):
        # Liverpool isn't a rivalry in MLS even if a "Liverpool" entry existed.
        assert not rivalries.is_rivalry("Liverpool FC", "Manchester United FC", "MLS")

    def test_handles_one_word_team_name_substring(self):
        # NCAA Football "Texas vs Oklahoma": bare 1-word names. Both
        # appear in many other team names ("UT-Austin Texas Longhorns"),
        # but the JSON entries are exact-bare so they only match teams
        # whose name actually contains "Texas".
        assert rivalries.is_rivalry("Texas", "Oklahoma", "CFB")
        # CFBD, the only CFB source, reports the bare school name, so the
        # mascot form never reaches the matcher. Entries are spelled its way.
        assert rivalries.is_rivalry("Texas", "Oklahoma", "CFB")
        assert not rivalries.is_rivalry("Texas Longhorns", "Oklahoma Sooners", "CFB")


class TestLoadRivalries:
    def test_json_is_valid(self):
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # At least the sports listed in the issue are present.
        for sport in ("CFB", "EPL", "NBA", "NHL", "MLB", "NFL"):
            assert sport in raw, f"Missing sport {sport} in rivalries.json"
            assert isinstance(raw[sport], list)
            assert len(raw[sport]) > 0

    def test_every_pair_has_two_strings(self):
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, pairs in raw.items():
            if key.startswith("_"):
                continue
            for pair in pairs:
                # 2 = a pair; 3 = a pair plus its trophy / game name.
                assert isinstance(pair, list) and len(pair) in (2, 3), \
                    f"Bad entry in {key}: {pair}"
                assert all(isinstance(s, str) and s.strip() for s in pair), \
                    f"Bad entry in {key}: {pair}"

    def test_no_self_rivalries(self):
        # A team can't be its own rival.
        path = os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, pairs in raw.items():
            if key.startswith("_"):
                continue
            for entry in pairs:
                a, b = entry[0], entry[1]
                assert rivalries._normalize(a) != rivalries._normalize(b), \
                    f"Self-rivalry in {key}: {a} / {b}"


class TestDisambiguatingTokens:
    """The substring matcher made every shorter school name match a longer one
    that starts with it. That is a SCORING bug, because is_rivalry feeds the
    score: ["Texas", "Texas A&M"] returned True for Texas Tech vs Texas A&M,
    scoring an ordinary fixture as the Lone Star Showdown."""

    def test_tech_is_a_different_school(self):
        assert not rivalries.is_rivalry("Texas Tech", "Texas A&M", "CFB")

    def test_state_is_a_different_school(self):
        # Washington vs Washington State IS a rivalry (the Apple Cup), but it
        # must match on the real pair, not because "Washington" is a prefix.
        assert not rivalries._name_matches("washington state", "washington")
        assert rivalries.is_rivalry("Washington", "Washington State", "CFB")

    def test_a_shorter_name_no_longer_half_matches(self):
        """Superseded: the matcher is whole-name now. Spell entries in full
        and let TestEveryEntryResolvesToARealTeam prove they resolve."""
        assert not rivalries._name_matches("texas longhorns", "texas")
        assert not rivalries._name_matches("olympique de marseille", "marseille")
        assert not rivalries._name_matches("tottenham hotspur fc", "tottenham")
        # ...while the club-type suffix is still stripped.
        assert rivalries._name_matches("tottenham hotspur fc", "Tottenham Hotspur".lower())

    def test_a_city_shared_by_two_clubs_does_not_collide(self):
        """The bug Jake's "Barcelona for example?" landed on. A token-subset
        matcher made "FC Barcelona" match "RCD Espanyol de Barcelona", so
        Real Madrid vs Espanyol returned El Clasico. That is a SCORING bug:
        is_rivalry feeds the score."""
        assert not rivalries._name_matches(
            "rcd espanyol de barcelona", "fc barcelona")
        assert rivalries.rivalry_name(
            "Real Madrid CF", "RCD Espanyol de Barcelona", "LaLiga") is None
        assert rivalries.rivalry_name(
            "Real Madrid CF", "FC Barcelona", "LaLiga") == "El Clásico"
        assert rivalries.rivalry_name(
            "FC Barcelona", "RCD Espanyol de Barcelona", "LaLiga") == "the Barcelona derby"

    def test_the_egg_bowl_was_dead_and_is_not_now(self):
        """rivalries.json said "Mississippi"; CFBD says "Ole Miss", and neither
        string contains the other, so the Egg Bowl had never been detected."""
        assert rivalries.is_rivalry("Ole Miss", "Mississippi State", "CFB")


class TestRivalryTrophyNames:
    def test_apple_cup_is_indexed(self):
        """The fixture that started this: it scored as an ordinary
        non-conference game because the pair was missing entirely."""
        assert rivalries.rivalry_name("Washington", "Washington State", "CFB") == "the Apple Cup"

    def test_order_within_the_pair_does_not_matter(self):
        assert rivalries.rivalry_name("Washington State", "Washington", "CFB") == "the Apple Cup"

    def test_a_rivalry_with_no_trophy_returns_empty_not_none(self):
        """"" and None mean different things: a known rivalry with no trophy
        recorded, versus not a rivalry. Collapsing them would either lose the
        signal or invent a trophy."""
        assert rivalries.rivalry_name("Army", "Navy", "CFB") == ""
        assert rivalries.is_rivalry("Army", "Navy", "CFB") is True

    def test_a_non_rivalry_returns_none(self):
        assert rivalries.rivalry_name("Rutgers", "Vanderbilt", "CFB") is None

    def test_two_element_entries_still_load(self):
        """Back-compat: a pair with no derby name recorded still resolves as a
        rivalry, it just has nothing to call itself. Arsenal vs Chelsea is a
        real rivalry with no established derby name, unlike Arsenal vs
        Tottenham, which is the North London derby."""
        assert rivalries.is_rivalry("Arsenal FC", "Chelsea FC", "EPL")
        assert rivalries.rivalry_name("Arsenal FC", "Chelsea FC", "EPL") == ""

    def test_every_cfb_school_name_matches_cfbd_spelling(self):
        """The Egg Bowl was dead for exactly this reason: an entry spelled the
        way a human would, not the way the source does. This guard reads the
        real file, so a future entry with a typo fails here rather than
        silently never matching.
        """
        import json as _json
        import os as _os
        path = _os.path.join(REPO_ROOT, "rivalries.json")
        with open(path, "r", encoding="utf-8") as f:
            cfb = _json.load(f)["CFB"]
        assert len(cfb) > 50, "guard is inert if the list is empty or tiny"
        # A CFBD school name is Title-Cased and never contains a lowercase
        # connector we would have introduced by hand ("of", "the").
        for entry in cfb:
            for name in entry[:2]:
                assert name == name.strip()
                assert " of " not in name and " the " not in name, \
                    f"suspicious school name: {name!r}"


class TestEveryEntryResolvesToARealTeam:
    """The guard the Egg Bowl needed and did not have.

    ["Mississippi", "Mississippi State"] was written for the Egg Bowl, but the
    source calls that school "Ole Miss", so the entry matched nothing and the
    rivalry was never once detected. Nothing reported it: a rivalry that never
    fires looks exactly like a fixture that is not a rivalry.

    So: every name in rivalries.json must resolve against a real team name as
    its SOURCE spells it, snapshotted in tests/fixtures/source_team_names.json.
    """

    # Names the fixture cannot confirm, each for a stated reason. A name here
    # is a deliberate exemption; anything else that fails is a typo.
    KNOWN_ABSENT = {
        "Sheffield Wednesday",  # below the Championship, so in no tracked roster
        "Saint-Etienne",        # Ligue 2
        # SportsDB's spelling of Paris Saint-Germain, kept as a second entry
        # because that source and Football-Data.org disagree. The fixture
        # holds only Football-Data.org names, so it cannot confirm this one.
        "Paris SG",
        "Marseille",  # SportsDB's short form, same reason as Paris SG
    }

    @staticmethod
    def _fixture():
        import json as _json
        import os as _os
        path = _os.path.join(REPO_ROOT, "tests", "fixtures", "source_team_names.json")
        with open(path, "r", encoding="utf-8") as f:
            return {k: v for k, v in _json.load(f).items() if not k.startswith("_")}

    def test_fixture_is_populated(self):
        """Fail on the instrument first: an empty fixture would make every
        assertion below pass vacuously and forever."""
        rosters = self._fixture()
        assert len(rosters) >= 9, "expected rosters for the tracked competitions"
        assert sum(len(v) for v in rosters.values()) > 250
        assert "Ole Miss" in rosters["CFB"], "the school the Egg Bowl entry got wrong"

    # Rosters a given sport's entries may resolve against. Soccer pools,
    # because promotion and relegation move a club between competitions and an
    # EPL derby entry legitimately names a club currently in the Championship.
    # Everything else stands alone: pooling let an NFL name validate a CFB
    # entry, which is how a global rename turned the Apple Cup's "Washington"
    # into "Washington Commanders" and the check said nothing.
    SOCCER = ("EPL", "EFL", "BL1", "LaLiga", "SerieA", "Ligue1",
              "BSA", "Eredivisie", "PrimeiraLiga", "MLS", "LigaMX")

    def _candidates(self, prefix, rosters):
        if prefix in self.SOCCER:
            return [n for p in self.SOCCER for n in rosters.get(p, [])]
        return rosters.get(prefix, [])

    def test_every_entry_resolves_somewhere(self):
        rosters = self._fixture()
        raw = self._raw()
        unresolved = []
        for prefix in rosters:
            candidates = self._candidates(prefix, rosters)
            assert candidates, f"no roster to check {prefix} against"
            for entry in raw.get(prefix, []):
                for name in entry[:2]:
                    if name in self.KNOWN_ABSENT:
                        continue
                    if not any(
                        rivalries._name_matches(
                            rivalries._normalize(team), rivalries._normalize(name)
                        )
                        for team in candidates
                    ):
                        unresolved.append(f"{prefix}: {name!r}")
        assert not unresolved, (
            "rivalries.json names that match no real team: "
            + ", ".join(unresolved)
            + ". Either the spelling differs from the source (the Egg Bowl "
            "bug: fix the entry), or the club left every tracked competition "
            "(add it to KNOWN_ABSENT with a reason)."
        )

    def test_the_guard_actually_fires_on_a_typo(self):
        """Mutation: the check is worthless unless a wrong name fails it."""
        rosters = self._fixture()
        everyone = [n for names in rosters.values() for n in names]
        assert not any(
            rivalries._name_matches(
                rivalries._normalize(t), rivalries._normalize("Mississippi")
            )
            for t in everyone
        ), "the original Egg Bowl spelling must NOT resolve"

    def test_a_name_from_another_sport_does_not_validate(self):
        """The scoping mutation. "Washington Commanders" is a real NFL club,
        and a global rename put it in the CFB Apple Cup entry; a pooled check
        accepted it because it resolves SOMEWHERE."""
        rosters = self._fixture()
        cfb = self._candidates("CFB", rosters)
        assert not any(
            rivalries._name_matches(
                rivalries._normalize(t), rivalries._normalize("Washington Commanders")
            )
            for t in cfb
        ), "an NFL club must not resolve against the CFB roster"
        assert any(
            rivalries._name_matches(
                rivalries._normalize(t), rivalries._normalize("Washington")
            )
            for t in cfb
        ), "the real CFBD school name must resolve"

    def test_diacritics_fold_both_ways(self):
        """Six freshly-written entries matched nothing because the source
        spells them with umlauts, circumflexes and tildes."""
        for ascii_form, source_form in (
            ("FC Bayern Munchen", "FC Bayern München"),
            ("Gremio FBPA", "Grêmio FBPA"),
            ("Sao Paulo FC", "São Paulo FC"),
            ("1. FC Koln", "1. FC Köln"),
            ("Vitoria SC", "Vitória SC"),
        ):
            assert rivalries._name_matches(
                rivalries._normalize(source_form), rivalries._normalize(ascii_form)
            ), f"{ascii_form} should match {source_form}"

    @staticmethod
    def _raw():
        import json as _json
        import os as _os
        with open(_os.path.join(REPO_ROOT, "rivalries.json"), "r", encoding="utf-8") as f:
            return _json.load(f)

    def test_named_soccer_derbies_are_indexed(self):
        """Spot-check the ones Jake would notice were missing."""
        cases = [
            ("Liverpool FC", "Everton FC", "EPL", "the Merseyside derby"),
            ("Arsenal FC", "Tottenham Hotspur FC", "EPL", "the North London derby"),
            ("Real Madrid CF", "FC Barcelona", "LaLiga", "El Clásico"),
            ("AC Milan", "FC Internazionale Milano", "SerieA", "the Derby della Madonnina"),
            ("FC Bayern München", "Borussia Dortmund", "BL1", "Der Klassiker"),
            ("Paris Saint-Germain FC", "Olympique de Marseille", "Ligue1", "Le Classique"),
            ("AFC Ajax", "Feyenoord Rotterdam", "Eredivisie", "De Klassieker"),
            ("Sport Lisboa e Benfica", "FC Porto", "PrimeiraLiga", "O Clássico"),
            ("Grêmio FBPA", "SC Internacional", "BSA", "the Grenal"),
            ("CR Flamengo", "Fluminense FC", "BSA", "the Fla-Flu"),
            ("SC Corinthians Paulista", "SE Palmeiras", "BSA", "the Derby Paulista"),
        ]
        for home, away, sport, expected in cases:
            assert rivalries.rivalry_name(home, away, sport) == expected, (
                f"{home} vs {away} ({sport})"
            )
