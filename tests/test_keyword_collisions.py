"""#324 class guard: no match keyword of one club names a rival in its own
competition.

The instance in #324 was 'Real' (Real Madrid) matching 'LaLiga: Real Betis vs.
Barcelona'. Auditing every Football-Data.org club in the ten free-tier
competitions found 37 collisions of the same shape, all from DERIVED
relaxations: a shared city as the last word ('Madrid' for Real, Atlético and
Rayo; 'Barcelona' for Espanyol; 'Rotterdam' for Feyenoord and Sparta), a club
noun ('Calcio' for four Serie A clubs, 'Club' for Athletic Club), a club-form
prefix ('1. FC', 'Sporting Clube'), a stripped bare city ('Paris' for Paris
FC), and a cross-club alias ('Borussia' for Dortmund). The both-teams gate
cannot tell such clubs apart whenever they share an opponent, so a feed for one
attaches to the other.

The fixture is the REAL canonical name list (tests/fixtures/fd_team_names.json),
not names chosen to pass, so a new club in a new season, or a new derivation
rule, is checked against every rival without anyone predicting the collision.
"""
import json
import os

import pytest

from dispatcharr_ranked_matchups import matcher
from dispatcharr_ranked_matchups.matcher import _kw_hit, _team_keywords, _team_keywords_split

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "fd_team_names.json")

# (club, keyword) pairs that DO name a rival and are kept on purpose, each
# because it is how broadcasters name the club and dropping it costs the feeds.
# A curated alias, never a derived one: the derivation rules are what this
# test exists to police.
ACCEPTED = {
    # 'Real Betis vs. Barcelona', 'Galatasaray v Barcelona': the standard
    # broadcast name. It also hits 'RCD Espanyol de Barcelona', which feeds
    # write as 'Espanyol', and the date and sport gates cover the rest.
    ("FC Barcelona", "Barcelona"),
    # Italian feeds name AC Milan 'Milan' ('Sassuolo vs. Milan'). It is a
    # substring of 'Internazionale Milano'; Inter's feeds say 'Inter'.
    ("AC Milan", "Milan"),
}


def _competitions():
    with open(FIXTURE, encoding="utf-8") as fh:
        data = json.load(fh)
    return {k: v for k, v in data.items() if not k.startswith("_")}


def test_fixture_loaded():
    # Instrument check: a missing or emptied fixture would make every
    # collision test below pass vacuously.
    comps = _competitions()
    assert len(comps) >= 10
    assert sum(len(t) for t in comps.values()) >= 200
    assert "Club Atlético de Madrid" in comps["PD"]


def _collisions(comps):
    out = []
    for comp, teams in comps.items():
        for t in teams:
            for k in _team_keywords(t):
                for u in teams:
                    if u != t and _kw_hit(u, [k]) and (t, k) not in ACCEPTED:
                        out.append((comp, t, k, u))
    return out


def test_no_keyword_names_a_league_rival():
    found = _collisions(_competitions())
    assert not found, "\n".join(
        f"{c}: {t!r} keyword {k!r} also names {u!r}" for c, t, k, u in found)


def test_no_two_rivals_share_a_keyword():
    for comp, teams in _competitions().items():
        kws = {t: {k.lower() for k in _team_keywords(t)} for t in teams}
        for i, t in enumerate(teams):
            for u in teams[i + 1:]:
                assert not (kws[t] & kws[u]), (comp, t, u, kws[t] & kws[u])


def test_the_guard_fires_on_the_madrid_relaxation(monkeypatch):
    """Mutation: restore the shared-city last word and the guard goes red, so
    it is not passing vacuously."""
    monkeypatch.setattr(
        matcher, "_GENERIC_LAST_WORDS", matcher._GENERIC_LAST_WORDS - {"madrid"})
    found = _collisions({"PD": _competitions()["PD"]})
    assert ("PD", "Real Madrid CF", "Madrid", "Club Atlético de Madrid") in found


class TestDerivedKeywords:
    @pytest.mark.parametrize("team,must,must_not", [
        ("RCD Espanyol de Barcelona", "Espanyol", "Barcelona"),
        ("Club Atlético de Madrid", "Atletico Madrid", "Madrid"),
        ("Real Madrid CF", "Real Madrid", "Madrid"),
        ("Paris FC", "Paris FC", "Paris"),
        ("Cagliari Calcio", "Cagliari", "Calcio"),
        ("Parma Calcio 1913", "Parma", "Parma Calcio"),
        ("1. FC Köln", "Köln", "1. FC"),
        ("Sporting Clube de Braga", "Braga", "Sporting Clube"),
        ("Sporting Clube de Portugal", "Sporting CP", "Portugal"),
        ("Feyenoord Rotterdam", "Feyenoord", "Rotterdam"),
        ("FC Internazionale Milano", "Inter", "Milano"),
        ("Athletic Club", "Athletic Bilbao", "Club"),
    ])
    def test_keywords(self, team, must, must_not):
        kws = _team_keywords(team)
        assert must in kws, kws
        assert must_not not in kws, kws

    def test_unchanged_for_names_the_rules_do_not_touch(self):
        # Recall control for the common shapes.
        assert _team_keywords_split("Brentford FC")[0] == ["Brentford FC", "Brentford"]
        assert "Yankees" in _team_keywords("New York Yankees")
        assert "North Carolina" in _team_keywords("North Carolina State")


class TestAliasGroups:
    def test_a_canonical_name_listed_as_an_alias_gets_the_group(self):
        # Football-Data.org's name for the club is an ALIAS in the file, not
        # its key; before #324 the key-only lookup left the club alias-less.
        assert "Atleti" in _team_keywords("Club Atlético de Madrid")
        assert "Internazionale" in _team_keywords("FC Internazionale Milano")

    def test_an_alias_under_two_keys_resolves_to_neither(self):
        rev = matcher._alias_reverse({"A": ["X", "Y"], "B": ["X"]})
        assert rev == {"Y": "A"}

    def test_a_key_lookup_still_returns_its_list(self):
        assert matcher._alias_group("Manchester United")[:2] == ["Man United", "Man Utd"]
