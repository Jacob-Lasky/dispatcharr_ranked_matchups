"""Apply's network pre-pass runs inside a whole-pass budget (#271).

MAX_GAMES_CEILING is derived from APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME
times max_games, but that figure only counts the SportsDB path. A game that
also falls through to game-thumbs and the league badge spends more, so a slate
of slow cosmetic fetches could outrun the scheduler lock. The pre-pass now gets
that product as ONE wall-clock budget and skips the remaining cosmetic fetches
once it is spent.

Driven through the real `_action_apply` against a real Django ORM (see
tests/orm_harness/run_scenarios.py). Every cosmetic fetch is stubbed to "take
its full timeout" by advancing a fake clock by that timeout and failing, so the
witness costs no wall time. The budget reads `_http.time`, which the scenario
replaces; on a tree without `_http` there is no budget at all, and the witness
fails as an assertion on the total.

Literals on purpose: 55 is 30 + 10 + 15 seconds per game, 30 the longest single
call (the LLM description). Deriving them from the plugin's constants would let
a wrong constant move the expectation with it.
"""

import importlib
import logging
import os
from datetime import timedelta
from types import SimpleNamespace

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load_kit():
    spec = importlib.util.spec_from_file_location(
        "_apply_harness_kit", os.path.join(HERE, "test_apply_empty_slate.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_kit = _load_kit()
_now, _settings, _seed_source_channel, _row, _write_cache, _owned = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._owned,
)

GAMES = 4
BUDGET_S = 55 * GAMES          # the enforced total for this slate
LONGEST_CALL_S = 30            # the one call that can be in flight when it runs out
TEAMS = [("Georgia", "Alabama"), ("Texas", "Oklahoma"), ("Ohio State", "Michigan"),
         ("Oregon", "Washington")]


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now


def _install_slow_network(env, patches):
    """Stub every cosmetic fetch apply's pre-pass makes. Each advances the fake
    clock by its own timeout and fails, the worst case for the budget. Returns
    (clock, calls) where calls counts each fetch that was actually made.
    Every replaced attribute is recorded in `patches` for `_restore`: the
    harness runs all scenarios in one process."""
    pkg = env.plugin.__name__.rsplit(".", 1)[0]
    mods = SimpleNamespace(**{
        name: importlib.import_module(f"{pkg}.{name}")
        for name in ("logos", "gamethumbs", "llm_descriptions")
    })
    clock = _FakeClock()

    def patch(obj, attr, value):
        patches.append((obj, attr, getattr(obj, attr)))
        setattr(obj, attr, value)

    try:
        http = importlib.import_module(f"{pkg}._http")
        patch(http, "time", clock)  # NetworkBudget reads _http.time per call
    except ModuleNotFoundError:
        pass                        # a tree with no budget: the witness must still run
    calls = {}

    def slow(name, seconds, result):
        def fetch(*args, **kwargs):
            calls[name] = calls.get(name, 0) + 1
            clock.now += seconds
            if isinstance(result, BaseException):
                raise result
            return result
        return fetch

    patch(mods.llm_descriptions, "_call_anthropic", slow("llm", 30, TimeoutError("30s")))
    patch(mods.logos, "resolve_thumb_url", slow("thumb_search", 10, "https://thumbs.example/t.jpg"))
    patch(mods.logos, "download_thumb", slow("download", 15, False))
    patch(mods.logos, "resolve_league_badge_url", slow("badge_search", 10, "https://badges.example/b.png"))
    patch(mods.gamethumbs, "download_to_file", slow("gamethumbs", 15, (None, TimeoutError("15s"))))
    patch(mods.gamethumbs, "_pace", lambda: None)
    return clock, calls


def _restore(patches):
    for obj, attr, original in reversed(patches):
        setattr(obj, attr, original)


def _slate_settings(**over):
    return _settings(
        enable_matchup_logos=True,
        gamethumbs_base_url="https://gamethumbs.example",
        llm_descriptions_enabled=True,
        anthropic_api_key="test-key",
        **over,
    )


def _seed_slate(env):
    user_ch, _ = _seed_source_channel(env)
    start = _now() + timedelta(hours=2)
    games = [_row(500 + i, start + timedelta(minutes=i), user_ch.id, home=h, away=a)
             for i, (h, a) in enumerate(TEAMS[:GAMES])]
    _write_cache(env, games)
    return games


def _published(env):
    """What the budget must never change: which channels publish, their names,
    and the streams they carry."""
    return {
        marker: (ch.name, sorted(env.m.ChannelStream.objects.filter(channel=ch)
                                 .values_list("stream_id", flat=True)))
        for marker, ch in _owned(env).items()
    }


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_slow_cosmetic_fetches_stop_at_the_budget(env):
    """Witness. Unbudgeted, each game here spends 30 + 10 + 15 + 15 + 10 + 15
    = 95s of fetches (LLM, SportsDB search and download, game-thumbs, badge
    search and download), 380s for the slate against a 220s budget."""
    _seed_slate(env)
    patches = []
    clock, calls = _install_slow_network(env, patches)
    records = _Records()
    env.plugin.logger.addHandler(records)
    start = clock.now
    try:
        result = env.plugin._action_apply(_slate_settings())
    finally:
        env.plugin.logger.removeHandler(records)
        _restore(patches)
    spent = clock.now - start

    assert result["status"] == "ok", result
    assert {"llm", "thumb_search", "download", "gamethumbs", "badge_search"} <= set(calls), (
        f"the scenario did not exercise every cosmetic tier: {calls}"
    )
    assert spent <= BUDGET_S + LONGEST_CALL_S, (
        f"the pre-pass spent {spent:g}s on cosmetic fetches against a "
        f"{BUDGET_S}s budget ({calls})"
    )
    budget_lines = [m for m in records.messages if "network budget" in m]
    assert len(budget_lines) == 1, records.messages
    assert "skipped" in budget_lines[0]
    assert len(_owned(env)) == GAMES, "a budget skip dropped a channel"


def scenario_the_budget_fits_inside_the_pipeline_child(env):
    """Witness. tasks.py kills the pipeline child at its deadline whatever the
    budget says, so the budget is capped at the child's remaining life less
    the 120s write reserve. With 220s left, the 220s budget shrinks to 100s."""
    import time as _time
    _seed_slate(env)
    tasks = env.plugin.tasks
    patches = []
    saved = os.environ.get(tasks.PIPELINE_DEADLINE_ENV)
    os.environ[tasks.PIPELINE_DEADLINE_ENV] = repr(_time.monotonic() + 120 + 100)
    try:
        clock, calls = _install_slow_network(env, patches)
        start = clock.now
        assert env.plugin._action_apply(_slate_settings())["status"] == "ok"
        spent = clock.now - start
    finally:
        _restore(patches)
        if saved is None:
            os.environ.pop(tasks.PIPELINE_DEADLINE_ENV, None)
        else:
            os.environ[tasks.PIPELINE_DEADLINE_ENV] = saved
    assert spent <= 100 + LONGEST_CALL_S, (
        f"the pre-pass spent {spent:g}s with 100s of the child's life to spend ({calls})"
    )
    assert len(_owned(env)) == GAMES


def scenario_a_spent_budget_makes_no_cosmetic_fetch(env):
    """Witness. With no budget at all, not one cosmetic fetch is made, and
    nothing a failed fetch would not cost is lost (every channel publishes)."""
    _seed_slate(env)
    patches = []
    try:
        _clock, calls = _install_slow_network(env, patches)
        patches.append((env.plugin, "APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME",
                        env.plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME))
        env.plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME = 0
        assert env.plugin._action_apply(_slate_settings())["status"] == "ok"
        assert calls == {}, f"fetches made with no budget left: {calls}"
        assert len(_owned(env)) == GAMES
    finally:
        _restore(patches)


def scenario_the_budget_never_changes_what_publishes(env):
    """Guard (green before and after #271). A spent budget may only cost logos
    and descriptions: the same slate publishes the same channels, names and
    streams with an unlimited budget and with none at all."""
    _seed_slate(env)
    patches = []
    try:
        _clock, calls = _install_slow_network(env, patches)
        plugin = env.plugin
        patches.append((plugin, "APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME",
                        plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME))

        plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME = 10 ** 9
        assert env.plugin._action_apply(_slate_settings())["status"] == "ok"
        unlimited = _published(env)
        assert len(unlimited) == GAMES and all(streams for _n, streams in unlimited.values())
        assert calls, "the unlimited run made no fetch, so the comparison proves nothing"

        plugin.APPLY_PREPASS_WORST_CASE_SECONDS_PER_GAME = 0
        assert env.plugin._action_apply(_slate_settings())["status"] == "ok"
        assert _published(env) == unlimited
    finally:
        _restore(patches)


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_network_budget(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
