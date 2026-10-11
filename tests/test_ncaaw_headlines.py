"""NCAA women's tournament headlines, in every form ESPN has used (#268).

ESPN puts the round in `competitions[0].notes[0].headline`. Through 2025 it
read "NCAA Women's Championship - ..."; from the 2026 tournament it reads
"NCAA Women's Basketball Championship - ...". A headline that does not parse
drops the game from the importance BRACKET (fetch_upcoming selects on season
type 3 and still airs it, with no stakes), so a wording change silently
flattens the whole tournament. The real strings below are copied from the
live scoreboard (fetched 2026-10-11, the `dates=` noted), not composed, and
the expected stages are literals on purpose.
"""
import pytest

from dispatcharr_ranked_matchups.sources import ncaaw_basketball as mod


@pytest.mark.parametrize("headline,stage", [
    # 2025 wording, live.
    ("NCAA Women's Championship - Regional 4 in Spokane - 1st Round", "R64"),     # 20250322
    ("NCAA Women's Championship - Regional 3 in Birmingham - 1st Round", "R64"),  # 20250322
    ("NCAA Women's Championship - Final Four", "F4"),
    # 2026 wording, live: one of every round.
    ("NCAA Women's Basketball Championship - Regional 4 in Sacramento - 1st Round", "R64"),  # 20260321
    ("NCAA Women's Basketball Championship - Regional 3 in Fort Worth - 2nd Round", "R32"),  # 20260322
    ("NCAA Women's Basketball Championship - Regional 1 in Fort Worth - Sweet 16", "S16"),   # 20260327
    ("NCAA Women's Basketball Championship - Regional 1 in Fort Worth - Elite 8", "E8"),     # 20260329
    ("NCAA Women's Basketball Championship - Final Four", "F4"),                             # 20260403
    ("NCAA Women's Basketball Championship - National Championship", "NCG"),                # 20260405
    # Composed: one typography pass away from the live form.
    ("NCAA Women's Basketball Championship - Regional 2 in Winston-Salem - Sweet 16", "S16"),
    ("NCAA Women’s Basketball Championship - Final Four", "F4"),
    ("NCAA Women's Basketball Championship - Regional 1 in Fort Worth - Sweet 16. ", "S16"),
    ("NCAA Women's Basketball Championship – Regional 2 in Fort Worth – Sweet 16", "S16"),
])
def test_every_espn_wording_parses(headline, stage):
    assert mod._parse_stage_from_headline(headline) == {"stage": stage, "matchday": 1}


@pytest.mark.parametrize("headline", [
    # Live (20260318): the play-in is not a bracket round.
    "NCAA Women's Basketball Championship - Regional 2 in Sacramento - First Four",
    # Other women's events, including the conference finals ESPN files as type 2.
    "Women's National Invitation Tournament - 1st Round",
    "NCAA Women's Volleyball Championship - 1st Round",
    "Big Ten Women's Basketball Tournament - Final",
    "CAA Championship - Final",
    # A tournament title embedded in another one, a longer round word, and a
    # First Four game whose round is in a suffix: the anchors exclude each.
    "WNIT: NCAA Women's Basketball Championship - Final Four",
    "NCAA Women's Basketball Championship - Final Fourteen",
    "NCAA Women's Basketball Championship - 1st Round (First Four)",
])
def test_other_headlines_do_not_parse(headline):
    assert mod._parse_stage_from_headline(headline) is None


def _event(eid, headline):
    return {
        "id": eid,
        "date": "2026-03-21T18:00Z",
        "season": {"type": 3},
        "competitions": [{
            "notes": [{"headline": headline}],
            "status": {"type": {"completed": True, "state": "post"}},
            "competitors": [
                {"homeAway": "home", "team": {"location": "UConn"}, "score": "90"},
                {"homeAway": "away", "team": {"location": "UTSA"}, "score": "50"},
            ],
        }],
    }


def test_a_2026_tournament_game_reaches_the_bracket(monkeypatch):
    """End to end through the bracket sweep: on main this returned []."""
    def fake_get(url, *_a, **_kw):
        if "dates=20260321" in url:
            return {"events": [_event(
                "401", "NCAA Women's Basketball Championship - "
                       "Regional 1 in Fort Worth - 1st Round")]}
        return {"events": []}
    monkeypatch.setattr(mod, "_http_get", fake_get)
    games = mod.NcaawBasketballPlayoffSource(season_end_year=2026)._fetch_bracket_games()
    assert [(g["game_id"], g["stage"]) for g in games] == [("401", "R64")]
