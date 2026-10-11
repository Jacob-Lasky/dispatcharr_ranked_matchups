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


def _raw_json():
    with open(os.path.join(REPO_ROOT, "rivalries.json"), "r", encoding="utf-8") as f:
        return json.load(f)


def _resolved():
    """{prefix: [entries as written]} with pool references expanded: what the
    loader actually indexes. Iterating the raw JSON instead would walk a pool
    reference ("EPL": "ENGLISH_CLUBS") character by character and check
    nothing, so every data test below goes through the resolver."""
    return rivalries._resolve_entries(_raw_json())


class TestLoadRivalries:
    def test_json_is_valid(self):
        resolved = _resolved()
        # At least the sports listed in the issue are present.
        for sport in ("CFB", "EPL", "NBA", "NHL", "MLB", "NFL"):
            assert sport in resolved, f"Missing sport {sport} in rivalries.json"
            assert isinstance(resolved[sport], list)
            assert len(resolved[sport]) > 0

    def test_every_value_is_a_list_or_names_a_real_pool(self):
        """A dangling pool name loads as "no rivalries" with only a log line,
        which is the silently-dead-entry failure again, so the shipped file
        must not contain one."""
        raw = _raw_json()
        pools = raw["_pools"]
        refs = 0
        for key, value in raw.items():
            if key.startswith("_"):
                continue
            if isinstance(value, str):
                refs += 1
                assert isinstance(pools.get(value), list), \
                    f"{key} names pool {value!r}, which does not exist"
            else:
                assert isinstance(value, list), f"{key}: {type(value).__name__}"
        assert refs >= 4, "instrument check: EPL, EFL, EFLCUP, FACUP are pool references"

    def test_every_pair_has_two_strings(self):
        resolved = _resolved()
        assert len(resolved) >= 15, "instrument check: the resolver returned too little"
        for key, pairs in resolved.items():
            for pair in pairs:
                # 2 = a pair; 3 = a pair plus its trophy / game name.
                assert isinstance(pair, list) and len(pair) in (2, 3), \
                    f"Bad entry in {key}: {pair}"
                assert all(isinstance(s, str) and s.strip() for s in pair), \
                    f"Bad entry in {key}: {pair}"

    def test_no_self_rivalries(self):
        # A team can't be its own rival.
        for key, pairs in _resolved().items():
            for entry in pairs:
                a, b = entry[0], entry[1]
                assert rivalries._normalize(a) != rivalries._normalize(b), \
                    f"Self-rivalry in {key}: {a} / {b}"

    def test_every_written_entry_is_what_the_loaded_index_answers(self):
        """An oracle that does NOT go through `_resolve_entries`: walk the raw
        JSON, follow a pool reference by plain lookup, and ask the loaded
        index (what `rivalry_name` reads in production) about every entry.
        A resolver that dropped or reordered an entry would pass every check
        built on the resolver itself."""
        raw = _raw_json()
        checked = 0
        for prefix, value in raw.items():
            if prefix.startswith("_"):
                continue
            entries = raw["_pools"][value] if isinstance(value, str) else value
            assert len(rivalries._RIVALRIES_BY_SPORT[prefix]) == len(entries), prefix
            for entry in entries:
                trophy = entry[2] if len(entry) == 3 else ""
                for home, away in ((entry[0], entry[1]), (entry[1], entry[0])):
                    assert rivalries.rivalry_name(home, away, prefix) == trophy, (
                        f"{prefix}: {home} v {away}"
                    )
                checked += 1
        assert checked > 250, "instrument check: walked too few entries"

    def test_no_pair_is_listed_twice_in_one_list(self):
        """The clone guard. Before the English pool (#192), the East Anglian
        and Second City derbies were each written out in BOTH the EPL and the
        EFL list; a pair held twice is a pair whose two copies can disagree
        (different trophy names, one fixed and one not)."""
        for key, pairs in _resolved().items():
            seen = {}
            for entry in pairs:
                pair = frozenset(
                    rivalries._strip_suffix(rivalries._normalize(n))
                    for n in entry[:2]
                )
                assert pair not in seen, (
                    f"{key}: {entry} repeats {seen[pair]}"
                )
                seen[pair] = entry


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

    # The ESPN English cup rosters: between them, EVERY club either cup can
    # draw, so a name in neither is a misspelling, never a club that dropped
    # out of view, and KNOWN_ABSENT does not apply to a cup prefix.
    #
    # A cup prefix is checked against BOTH cup rosters, not only its own,
    # because the cups share the ENGLISH_CLUBS pool: a non-league derby
    # (Chester FC is in the FA Cup and never in the EFL Cup) is a valid pool
    # entry that simply never meets in the EFL Cup. It is NOT checked against
    # Football-Data.org's rosters: a cup row carries ESPN's spelling, so an
    # entry resolving only against FD.org's would fire in the league and
    # never in the cup (#192).
    #
    # The ENGLISH league prefixes also accept the cup rosters, for the same
    # reason SOCCER pools: a club outside the two FD.org divisions is still
    # spelled right if ESPN confirms it, and can be promoted into view. Only
    # the English ones: an English non-league club cannot appear in LaLiga,
    # so the cup rosters must not validate a LaLiga entry.
    COMPLETE = ("EFLCUP", "FACUP")
    ENGLISH_LEAGUES = ("EPL", "EFL")

    def _candidates(self, prefix, rosters):
        if prefix in self.COMPLETE:
            return [n for p in self.COMPLETE for n in rosters.get(p, [])]
        if prefix in self.SOCCER:
            extra = self.COMPLETE if prefix in self.ENGLISH_LEAGUES else ()
            return [n for p in self.SOCCER + extra for n in rosters.get(p, [])]
        return rosters.get(prefix, [])

    def _resolves(self, name, candidates):
        return any(
            rivalries._name_matches(rivalries._normalize(t), rivalries._normalize(name))
            for t in candidates
        )

    def test_a_non_league_cup_club_can_join_the_shared_pool(self):
        """The codex finding on this change: checking each cup only against
        its own roster rejected a valid FA-Cup-only derby for EFLCUP."""
        rosters = self._fixture()
        assert "Chester FC" in rosters["FACUP"]
        assert "Chester FC" not in rosters["EFLCUP"], \
            "instrument check: needs a club only the FA Cup draws"
        for prefix in ("EFLCUP", "FACUP", "EPL", "EFL"):
            assert self._resolves("Chester FC", self._candidates(prefix, rosters)), prefix
        # ...but an English non-league club must not validate a LaLiga entry.
        assert not self._resolves("Chester FC", self._candidates("LaLiga", rosters))

    def test_a_cup_prefix_still_rejects_a_misspelling(self):
        """Pooling the two cup rosters must not loosen the spelling check."""
        cup = self._candidates("EFLCUP", self._fixture())
        assert not self._resolves("Sheffield Weds", cup)
        assert self._resolves("Sheffield Wednesday", cup)

    def test_every_entry_resolves_somewhere(self):
        rosters = self._fixture()
        resolved = rivalries._resolve_entries(self._raw())
        assert set(self.COMPLETE) <= set(resolved), \
            "instrument check: the cup prefixes must be in the resolved map"
        unresolved = []
        # Walk the RESOLVED map, not the roster keys: a prefix with rivalries
        # and no roster must fail below rather than be skipped.
        for prefix, entries in resolved.items():
            candidates = self._candidates(prefix, rosters)
            assert candidates, f"no roster to check {prefix} against"
            for entry in entries:
                for name in entry[:2]:
                    if name in self.KNOWN_ABSENT and prefix not in self.COMPLETE:
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


class TestEnglishClubPool:
    """#192. rivalries.json had no EFLCUP or FACUP key, so no cup tie could
    ever earn the rivalry bonus: an Arsenal v Tottenham Carabao Cup
    quarterfinal scored as an ordinary fixture. The fix is one shared English
    pool that the leagues AND the cups resolve against, not cup copies of the
    league lists."""

    def test_a_league_derby_fires_in_both_cups(self):
        # ESPN spellings: sources/english_cup.py names a team by its location.
        for cup in ("EFLCUP", "FACUP"):
            assert rivalries.rivalry_name(
                "Arsenal", "Tottenham Hotspur", cup) == "the North London derby", cup
            assert rivalries.rivalry_name(
                "Everton", "Liverpool", cup) == "the Merseyside derby", cup

    def test_a_cross_tier_derby_fires_in_a_cup(self):
        """The case no single league key can hold: the clubs sit in
        different divisions, so the cup is the only place they meet."""
        assert rivalries.rivalry_name(
            "Sheffield Wednesday", "Sheffield United", "FACUP") == "the Steel City derby"
        assert rivalries.rivalry_name(
            "Southampton", "Portsmouth", "EFLCUP") == "the South Coast derby"

    def test_a_club_friendly_still_never_fires(self):
        """friendlies.py: the absence of CLUBFRIENDLY is DELIBERATE and
        load-bearing. Pooling the English pairs must not reach it."""
        assert rivalries.rivalry_name(
            "Arsenal", "Tottenham Hotspur", "CLUBFRIENDLY") is None
        assert rivalries.rivalry_name(
            "Sheffield Wednesday", "Sheffield United", "CLUBFRIENDLY") is None
        assert "CLUBFRIENDLY" not in _raw_json()
        assert "CLUBFRIENDLY" not in rivalries._RIVALRIES_BY_SPORT

    def test_one_list_four_prefixes(self):
        """The shape that makes drift impossible: all four prefixes name the
        same pool, so there is no second copy to fall out of step."""
        raw = _raw_json()
        for prefix in ("EPL", "EFL", "EFLCUP", "FACUP"):
            assert raw[prefix] == "ENGLISH_CLUBS", prefix
        loaded = rivalries._RIVALRIES_BY_SPORT
        assert loaded["EFLCUP"] == loaded["FACUP"] == loaded["EPL"] == loaded["EFL"]
        assert len(loaded["EPL"]) >= 20, "instrument check: the pool is populated"

    def test_the_leagues_see_the_whole_pool(self):
        """EPL and EFL share it too, so a derby recorded once fires in
        whichever division the two clubs meet: the Steel City derby used to
        be EFL-only and would have gone dark the season both Sheffield clubs
        were promoted."""
        assert rivalries.rivalry_name(
            "Sheffield United FC", "Sheffield Wednesday FC", "EPL") == "the Steel City derby"
        assert rivalries.rivalry_name(
            "Arsenal FC", "Tottenham Hotspur FC", "EFL") == "the North London derby"

    def test_derbies_added_with_the_pool(self):
        # (Football-Data.org spelling, ESPN spelling) for each club, because
        # the league rows and the cup rows reach the matcher spelled
        # differently and the derby has to fire on both.
        for fd, espn, name in (
            (("Wolverhampton Wanderers FC", "West Bromwich Albion FC"),
             ("Wolverhampton Wanderers", "West Bromwich Albion"),
             "the Black Country derby"),
            (("Swansea City AFC", "Cardiff City FC"),
             ("Swansea City", "Cardiff City"),
             "the South Wales derby"),
            (("West Ham United FC", "Millwall FC"),
             ("West Ham United", "Millwall"),
             "the Dockers derby"),
        ):
            assert rivalries.rivalry_name(*fd, "EFL") == name
            assert rivalries.rivalry_name(*espn, "FACUP") == name

    def test_a_non_derby_cup_tie_is_not_a_rivalry(self):
        # #190's motivating fixture: a cup tie, but not a derby.
        assert rivalries.rivalry_name(
            "Tottenham Hotspur", "Charlton Athletic", "EFLCUP") is None


class TestResolveEntries:
    def test_pool_reference_expands_to_the_pool(self):
        raw = {
            "_comment": "x",
            "_pools": {"_comment": "y", "P": [["A", "B", "the A-B derby"]]},
            "X": "P",
            "Y": "P",
            "Z": [["C", "D"]],
        }
        out = rivalries._resolve_entries(raw)
        assert out == {
            "X": [["A", "B", "the A-B derby"]],
            "Y": [["A", "B", "the A-B derby"]],
            "Z": [["C", "D"]],
        }
        # The SAME list, not a copy: nothing to drift.
        assert out["X"] is out["Y"] is raw["_pools"]["P"]

    def test_a_dangling_reference_loads_nothing_and_says_so(self, caplog):
        raw = {"_pools": {"P": [["A", "B"]]}, "X": "MISSING", "Z": [["C", "D"]]}
        with caplog.at_level("WARNING"):
            out = rivalries._resolve_entries(raw)
        assert out == {"Z": [["C", "D"]]}
        assert "MISSING" in caplog.text

    def test_a_pool_is_never_a_sport_prefix(self):
        out = rivalries._resolve_entries({"_pools": {"P": [["A", "B"]]}})
        assert out == {}
        assert "_pools" not in rivalries._RIVALRIES_BY_SPORT
        assert "ENGLISH_CLUBS" not in rivalries._RIVALRIES_BY_SPORT
