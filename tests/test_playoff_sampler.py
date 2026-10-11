"""Every source that claims importance can sample a result (#249).

NbaPlayoffSource, NflPlayoffSource, WnbaPlayoffSource and
NcaawBasketballPlayoffSource set `supports_importance = True` and have
thresholds in LEAGUE_CONTEXTS, so the refresh sent their games to the
simulator, which reached SportSource.sample_result and raised
NotImplementedError. The refresh catches that per game, so every playoff
game of those sports scored 0 importance with a WARNING each. The playoff
sources now share one sampler, ForcedWinnerPoissonSampler.
"""
import importlib
import inspect
import pkgutil
import random
from datetime import datetime, timedelta, timezone

import pytest

from dispatcharr_ranked_matchups import sources as sources_pkg
from dispatcharr_ranked_matchups.sources.base import GameRow, SportSource

UTC = timezone.utc


def _importance_classes():
    """{qualname: class} for every concrete importance source reachable from
    the package, the package module itself included. A re-export is the same
    class and is one entry; two DIFFERENT classes sharing a name fail here
    rather than one silently hiding the other."""
    modules = [sources_pkg] + [
        importlib.import_module(f"{sources_pkg.__name__}.{mod.name}")
        for mod in pkgutil.iter_modules(sources_pkg.__path__)
    ]
    seen = {}
    for module in modules:
        for _name, cls in inspect.getmembers(module, inspect.isclass):
            if (issubclass(cls, SportSource) and not inspect.isabstract(cls)
                    and getattr(cls, "supports_importance", False)):
                assert seen.get(cls.__qualname__, cls) is cls, f"two classes named {cls.__qualname__}"
                seen[cls.__qualname__] = cls
    return seen


def test_every_importance_source_overrides_sample_result():
    """The class guard: a new source that claims importance without a sampler
    fails here, not as one WARNING per game in production."""
    classes = _importance_classes()
    # The instrument first: the scan must find the classes it exists for.
    assert {"NbaPlayoffSource", "MlbPlayoffSource", "NcaafSource"} <= set(classes)
    missing = sorted(name for name, cls in classes.items()
                     if cls.sample_result is SportSource.sample_result)
    assert missing == []


# sources/base.py: "Sources that flip `supports_importance = True` MUST
# override all of them" (the 7 methods of the importance interface). Literal
# on purpose, so a method dropped from the interface shows here.
_IMPORTANCE_INTERFACE = (
    "outcome_labels", "estimate_strengths", "initial_state", "remaining_matches",
    "sample_result", "apply_result", "terminal_outcomes",
)


def _impl(owner, name):
    """The function behind `name`: a property's getter, a classmethod's or
    staticmethod's function, so the base implementation rewrapped still reads
    as not overridden."""
    attr = inspect.getattr_static(owner, name)
    if isinstance(attr, property):
        return attr.fget
    return getattr(attr, "__func__", attr)


def _not_overridden(cls):
    return [m for m in _IMPORTANCE_INTERFACE if _impl(cls, m) is _impl(SportSource, m)]


def test_every_importance_source_overrides_the_whole_interface():
    """The rest of the contract #249 asked for: a source missing ANY of the
    seven raises NotImplementedError per simulated game, the same failure as
    the missing sampler, so the guard covers all of them."""
    class _Incomplete(SportSource):  # the instrument must catch a gap
        supports_importance = True
        sport_prefix = sport_label = "X"

        def fetch_upcoming(self, days_ahead=7):
            return []

        def sample_result(self, state, match, strengths, rng):
            return None
        # The base implementation rewrapped is still the base's
        # NotImplementedError, so neither counts as an override.
        outcome_labels = property(SportSource.outcome_labels.fget)
        estimate_strengths = staticmethod(SportSource.estimate_strengths)
    assert _not_overridden(_Incomplete) == [m for m in _IMPORTANCE_INTERFACE if m != "sample_result"]

    classes = _importance_classes()
    # Discovery's own instrument: one class per shape the scan exists for (a
    # bracket series, a double-elim mixin, a points-based league, soccer).
    assert {"NbaPlayoffSource", "NcaaBaseballPlayoffBracketSource", "NcaafSource",
            "SoccerSource"} <= set(classes)
    assert len(classes) >= 25
    gaps = {name: _not_overridden(cls) for name, cls in sorted(classes.items())}
    assert {name: g for name, g in gaps.items() if g} == {}


def _row(src, home, away):
    return GameRow(sport_prefix=src.sport_prefix, sport_label=src.sport_label, home=home,
                   away=away, rank_home=None, rank_away=None,
                   start_time=datetime.now(UTC) + timedelta(days=1), extra={})


@pytest.mark.parametrize("module,cls,kw,mean_lo,mean_hi", [
    # Means bracket each sport's default scoring rate (sources' _DEFAULT_POINTS_FOR),
    # as literals so a wrong default or a wrong formula shows.
    ("nba", "NbaPlayoffSource", {"season_end_year": 2026}, 105, 125),
    ("nfl", "NflPlayoffSource", {"season_end_year": 2026}, 18, 27),
    ("wnba", "WnbaPlayoffSource", {"season_year": 2026}, 74, 90),
    ("ncaaw_basketball", "NcaawBasketballPlayoffSource", {"season_end_year": 2026}, 60, 80),
])
def test_playoff_samplers_produce_a_winner_on_the_sports_scale(module, cls, kw, mean_lo, mean_hi):
    mod = importlib.import_module(f"dispatcharr_ranked_matchups.sources.{module}")
    src = getattr(mod, cls)(**kw)
    rng = random.Random(3)
    row = _row(src, "Home", "Away")
    scores = []
    for _ in range(300):
        r = src.sample_result({}, row, {}, rng)
        assert r.home_goals != r.away_goals
        scores += [r.home_goals, r.away_goals]
    assert mean_lo <= sum(scores) / len(scores) <= mean_hi


def _nba_finals_event(start, n):
    return {
        "id": f"900{n}",
        "date": start.strftime("%Y-%m-%dT%H:%MZ"),
        "season": {"type": 3},
        "competitions": [{
            "type": {"abbreviation": "FINAL"},
            "notes": [{"headline": f"NBA Finals - Game {n}"}],
            "status": {"type": {"completed": False, "state": "pre"}},
            "competitors": [
                {"homeAway": "home", "team": {"displayName": "Oklahoma City Thunder"}},
                {"homeAway": "away", "team": {"displayName": "New York Knicks"}},
            ],
        }],
    }


def test_nba_finals_game_has_importance(monkeypatch):
    """The witness, through the same catch the refresh wraps importance in:
    before, NBA Finals Game 1 raised NotImplementedError and scored 0
    (8.6 measured here with the sampler)."""
    from urllib.parse import parse_qs, urlparse
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, compute_match_importance
    from dispatcharr_ranked_matchups.sources import nba
    # All seven games published, as ESPN lists "if necessary" games: the
    # simulator replays only published games, so a lone Game 1 never ends
    # the series and nothing it decides varies.
    start = datetime.now(UTC) + timedelta(days=1)
    games = [_nba_finals_event(start + timedelta(days=2 * (n - 1)), n) for n in range(1, 8)]
    upcoming = start.strftime("%Y%m%d")

    def get(url, *a, **kw):
        # The bracket day is checked first, so a clock that makes "tomorrow"
        # 2026-06-04 still gets the whole series there.
        day = parse_qs(urlparse(url).query)["dates"][0]
        if day == "20260604":
            return {"events": games}
        return {"events": games[:1] if day == upcoming else []}
    monkeypatch.setattr(nba, "_http_get", get)
    src = nba.NbaPlayoffSource(season_end_year=2026)
    rows = src.fetch_upcoming(days_ahead=2)
    assert src.place_upcoming(rows, for_scoring=True) == 0
    try:
        points, _notes, _bands = compute_match_importance(
            src, rows[0], LEAGUE_CONTEXTS["NBA_PO"], n_sims=200, rng=random.Random(5),
        )
    except NotImplementedError:
        points = 0.0
    assert points > 1.0


# Stages whose winner moves to a stage in ANOTHER source or another path, so
# "the next stage in KO_STAGES" is not where they go: the NCAA baseball and
# softball Regionals feed the Super Regionals (other source) and the Super
# Regionals feed the Finals (other source); a UEFA knockout PLAYOFFS winner
# goes to LAST_16, LAST_32 being the World Cup's round.
_CROSS_SOURCE_STAGES = {"BSB_REG", "BSB_SR", "SB_REG", "SB_SR", "PLAYOFFS"}


def test_every_bracket_winner_reaches_the_next_rounds_depth():
    """The class guard for the WNBA defect: a winner's round_reached must be
    the depth of the round it plays next, or every band at that round reads
    unreachable and the stakes are 0.0."""
    from dispatcharr_ranked_matchups.scoring import KNOCKOUT_ROUND_DEPTH
    from dispatcharr_ranked_matchups.sources.bracket import BracketSportSource
    checked, wrong = 0, []
    for mod in pkgutil.iter_modules(sources_pkg.__path__):
        module = importlib.import_module(f"{sources_pkg.__name__}.{mod.name}")
        for _name, cls in inspect.getmembers(module, inspect.isclass):
            if (not issubclass(cls, BracketSportSource) or inspect.isabstract(cls)
                    or cls.__module__ != module.__name__):
                continue
            src = cls.__new__(cls)  # the hooks read only class attributes
            for stage, nxt in zip(cls.KO_STAGES, cls.KO_STAGES[1:]):
                if stage in _CROSS_SOURCE_STAGES:
                    continue
                reached = {}
                src._advance_round_reached(reached, "W", "L", stage)
                checked += 1
                if reached.get("W") != KNOCKOUT_ROUND_DEPTH[nxt]:
                    wrong.append(f"{cls.__name__} {stage}->{nxt}")
    assert checked >= 25  # the instrument: it found the sources
    assert wrong == []


def test_a_wnba_semifinal_has_stakes():
    """The witness: 2026 semifinal, five games published, no Finals yet.
    0.0 before (a semifinal winner never reached the Finals band)."""
    from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS, compute_match_importance
    from dispatcharr_ranked_matchups.sources import wnba
    src = wnba.WnbaPlayoffSource(season_year=2026)
    start = datetime(2026, 9, 28, 23, tzinfo=UTC)
    records = [{"game_id": f"77{n}", "stage": "SF", "matchday": n,
                "home": "Las Vegas Aces" if n in (1, 2, 5) else "Minnesota Lynx",
                "away": "Minnesota Lynx" if n in (1, 2, 5) else "Las Vegas Aces",
                "home_goals": None, "away_goals": None, "status": "SCHEDULED",
                "start_time": start + timedelta(days=2 * (n - 1)), "extra": {}}
               for n in range(1, 6)]
    src._cache_bracket_games(records)
    row = GameRow(sport_prefix="WNBA", sport_label="WNBA Playoffs", home="Las Vegas Aces",
                  away="Minnesota Lynx", rank_home=None, rank_away=None, start_time=start,
                  extra={"game_id": "771"})
    assert src.place_upcoming([row], for_scoring=True) == 0
    points, _notes, _bands = compute_match_importance(
        src, row, LEAGUE_CONTEXTS["WNBA_PO"], n_sims=300, rng=random.Random(5),
    )
    assert points > 1.0
