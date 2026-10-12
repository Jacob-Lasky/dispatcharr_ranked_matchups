"""Nothing commits before apply's last time check (#271 follow-up, rename residual).

`_apply_time_short` can stop apply at each of its checks, and a stopped apply
must leave nothing behind. Before this change the live-group rename (step 1:
create the new group, move our channels into it, delete the old one; step 2:
move our guide rows off the old EPG source, #263) committed BEFORE the second
check. A stopped apply then left our channel in the new group on its old
number, beside a channel the user had filed there on the same number: two
channels on 401 in one group, which the guide binds by the bare number and the
UI refuses to save.

Driven through the real `_action_apply` against a real Django ORM (see
tests/orm_harness/run_scenarios.py), with the fake child clock from
test_apply_time_reserve.py, so nothing sleeps.
"""

import ast
import hashlib
import importlib.util
import os
from datetime import timedelta

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_kit_{name}", os.path.join(HERE, name))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_kit = _load("test_apply_empty_slate.py")
_reserve = _load("test_apply_time_reserve.py")
_now, _settings, _seed_source_channel, _row, _write_cache, _publish, _owned = (
    _kit._now, _kit._settings, _kit._seed_source_channel, _kit._row,
    _kit._write_cache, _kit._publish, _kit._owned,
)
_Child = _reserve._Child
PLUGIN_PY = os.path.join(os.path.dirname(HERE), "plugin.py")

OLD, NEW = "Top Matchups", "!Top Matchups"
G401 = "ranked_matchups:CFB:401"
COMPACT = {"channel_numbering_mode": "compact", "virtual_channel_base": 401,
           "compact_band_size": 2}


def _published_on_401(env):
    """Our 401 game on air in OLD at 401, and the user's own channel filed by
    hand at 401 in NEW, the group the next apply renames to."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    soon = _now() + timedelta(hours=6)
    game = _row(401, soon, user_ch.id)
    owned = _publish(env, [game], settings=_settings(**COMPACT))
    assert int(owned[G401].channel_number) == 401
    new_group = m.ChannelGroup.objects.create(name=NEW)
    stranger = m.Channel.objects.create(name="My channel", channel_number=401, channel_group=new_group)
    return game, owned[G401], stranger


def _stop_at_the_last_check(env, child):
    """The deciding half (before the last check) spends 100 of the 200s left."""
    prepare = env.plugin._autorecord_prepare

    def slow_prepare(*a, **k):
        child.clock.now += 100
        return prepare(*a, **k)

    child.patch(env.plugin, "_autorecord_prepare", slow_prepare)


def _numbers_in(env, group_name):
    return sorted(
        int(c.channel_number) for c in env.m.Channel.objects.filter(channel_group__name=group_name)
    )


# ------------------------------------------------- scenarios (subprocess) ---

def scenario_witness_a_stopped_rename_leaves_no_two_channels_on_401(env):
    m = env.m
    game, ours, stranger = _published_on_401(env)
    _write_cache(env, [game])
    with _Child(env, seconds_left=200) as child:
        _stop_at_the_last_check(env, child)
        result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "error" and "not enough time" in result["message"], result
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the stopped apply moved our channel into {ours.channel_group.name!r}; "
        f"{NEW!r} now holds {_numbers_in(env, NEW)}"
    )
    assert _numbers_in(env, NEW) == [401]
    assert m.ChannelGroup.objects.filter(name=OLD).exists()

    # The next apply, with time, renames and renumbers around the user's 401.
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    stranger.refresh_from_db()
    assert (ours.channel_group.name, int(ours.channel_number)) == (NEW, 402)
    assert int(stranger.channel_number) == 401
    assert _numbers_in(env, NEW) == [401, 402]


def _snapshot(env):
    """Every row of every model, every state file the plugin writes, and the
    logo directory: what a stopped apply must leave exactly as it found it."""
    from django.apps import apps as registry
    rows = {
        model.__name__: sorted(repr(sorted(r.items())) for r in model.objects.values())
        for model in registry.get_models()
    }
    files = {}
    for attr in ("CACHE_PATH", "RECORDING_STATE_PATH", "NUMBERING_STATE_PATH",
                 "LLM_DESCRIPTIONS_CACHE_PATH", "SPORTSDB_THUMB_CACHE_PATH", "GAMETHUMBS_CACHE_PATH"):
        path = getattr(env.plugin, attr)
        if os.path.exists(path):
            with open(path, "rb") as f:
                files[attr] = hashlib.sha256(f.read()).hexdigest()
    logos_dir = os.path.join(env.tmp, "logos")
    files["logos"] = sorted(os.listdir(logos_dir)) if os.path.isdir(logos_dir) else []
    return rows, files


def _gate_count():
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
    apply = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_action_apply")
    return sum(1 for n in ast.walk(apply) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "_apply_time_short")


def scenario_stopping_at_any_check_leaves_nothing_behind(env):
    """Class guard, behavioural: for EVERY _apply_time_short call in
    _action_apply, stop apply there and compare the whole database, the state
    files and the logo directory with what they were. The apply renames to a
    group that does not exist yet, so every pre-check write path is armed:
    create the group, move our channel, delete the old group, create our
    EPGSource, move our guide rows off the old source and delete it (#263).
    A write moved above any check, however it is spelled, fails here."""
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(**COMPACT))
    _write_cache(env, [game])
    gates = _gate_count()
    assert gates >= 2, f"instrument: found {gates} time checks in _action_apply"
    real = env.plugin._apply_time_short
    for stop_at in range(1, gates + 1):
        before = _snapshot(env)
        calls = []

        def check(*a, **k):
            calls.append(1)
            if len(calls) == stop_at:
                return {"status": "error", "message": "stopped by the test"}
            return real(*a, **k)

        env.plugin._apply_time_short = check
        try:
            result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **COMPACT))
        finally:
            env.plugin._apply_time_short = real
        assert result.get("message") == "stopped by the test", (
            f"instrument: apply never reached check #{stop_at}: {result}"
        )
        after = _snapshot(env)
        changed = sorted(k for k in set(before[0]) | set(after[0]) if before[0].get(k) != after[0].get(k))
        changed += sorted(k for k in set(before[1]) | set(after[1]) if before[1].get(k) != after[1].get(k))
        assert not changed, f"apply stopped at check #{stop_at} of {gates} still wrote: {changed}"
    # Instrument: unstopped, the same apply DOES write all of it.
    result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **COMPACT))
    assert result["status"] == "ok", result
    m = env.m
    assert m.ChannelGroup.objects.filter(name="Renamed Matchups").exists()
    assert not m.ChannelGroup.objects.filter(name=OLD).exists()
    assert not m.EPGSource.objects.filter(name=OLD).exists()
    assert m.ProgramData.objects.filter(epg__epg_source__name="Renamed Matchups").exists()


# ----------------------------------------- class guard, by source (AST) ---

_ORM_WRITES = {"create", "get_or_create", "update_or_create", "bulk_create", "bulk_update",
               "save", "delete"}
_FILE_WRITES = {"write_json_atomic", "save_state"}
_ATOMIC = "transaction.atomic()"

# Committed writes apply makes AFTER its last time check but OUTSIDE its main
# transaction, each with why it cannot wait for that transaction. Anything new
# there must be added here, with its reason, or fail.
ALLOWED_BEFORE_MAIN_TRANSACTION = {
    "_commit_group_setup": (
        "Steps 1-2's group and guide setup (create the group, move our channels "
        "and #263 guide rows from a previous group/source). Numbering, which "
        "runs before the main transaction, reads the channels in the group, so "
        "the move must be visible to it. A kill after it is repaired by the "
        "next apply's renumbering."
    ),
    "_autorecord_cancel_committed": (
        "Recording cancellations commit before numbering on purpose (#288: a "
        "recording counted as keeping its channel held a full compact band's "
        "slot; #291: the second plan must name the channels numbering placed). "
        "Their state is written ahead of the commit, and a kill after one is "
        "repaired by the next plan (#303)."
    ),
    "_resolve_matchup_logo_id": (
        "The network half's Logo rows (get_or_create) for images it downloads. "
        "The network half must stay outside the transaction (#136); the rows "
        "are idempotent, and a kill leaves one the next apply reuses."
    ),
}


def _is_atomic(node):
    return isinstance(node, ast.With) and any(
        ast.unparse(i.context_expr) == _ATOMIC for i in node.items)


def _write_sites(fn, writers):
    """(line, label) for every write `fn` makes itself, not inside nested
    defs (those count where they are CALLED, through `writers`)."""
    querysets = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Assign) and any(k in ast.unparse(n.value) for k in (".objects", ".filter(")):
            querysets |= {t.id for t in n.targets if isinstance(t, ast.Name)}
    out = []

    def visit(n):
        if n is not fn and isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return
        if _is_atomic(n):
            out.append((n.lineno, _ATOMIC))
        if isinstance(n, ast.Call):
            f = n.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            receiver = ast.unparse(f.value) if isinstance(f, ast.Attribute) else ""
            if isinstance(f, ast.Attribute) and f.attr in _ORM_WRITES and receiver != "os.path":
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and f.attr == "update" and (
                    ".objects" in receiver or "filter(" in receiver or receiver in querysets):
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and receiver == "os" and f.attr in ("replace", "remove", "unlink"):
                out.append((n.lineno, ast.unparse(f)))
            elif name in _FILE_WRITES or name in writers:
                out.append((n.lineno, name))
        for c in ast.iter_child_nodes(n):
            visit(c)

    visit(fn)
    return out


def _writers(functions, known=()):
    """Names among `functions` that write, directly or through each other."""
    found = set(known)
    changed = True
    while changed:
        changed = False
        for name, fn in functions.items():
            if name not in found and _write_sites(fn, found):
                found.add(name)
                changed = True
    return found


def _apply_layout(source):
    tree = ast.parse(source)
    module = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    apply = module["_action_apply"]
    nested = {n.name: n for n in ast.walk(apply) if isinstance(n, ast.FunctionDef) and n is not apply}
    writers = _writers(nested, _writers(module))
    checks = [n.lineno for n in ast.walk(apply) if isinstance(n, ast.Call)
              and getattr(n.func, "id", None) == "_apply_time_short"]
    main = [n.lineno for n in apply.body if _is_atomic(n)]
    return sorted(set(_write_sites(apply, writers))), checks, main


class TestNothingCommitsBeforeTheLastCheck:
    def _layout(self):
        return _apply_layout(open(PLUGIN_PY, encoding="utf-8").read())

    def test_instrument(self):
        sites, checks, main = self._layout()
        assert len(checks) >= 2 and len(main) == 1, (checks, main)
        assert any(label == _ATOMIC for _l, label in sites)

    def test_no_write_precedes_the_last_check(self):
        sites, checks, _main = self._layout()
        early = [(line, label) for line, label in sites if line < max(checks)]
        assert not early, (
            f"committed writes before apply's last time check (line {max(checks)}): {early}. "
            "A stopped apply must leave nothing behind; move them after it."
        )

    def test_writes_outside_the_main_transaction_are_allowlisted(self):
        sites, checks, main = self._layout()
        between = {label for line, label in sites if max(checks) < line < main[0]}
        unexplained = between - set(ALLOWED_BEFORE_MAIN_TRANSACTION)
        assert not unexplained, (
            f"committed writes between the last time check and the main transaction "
            f"with no stated reason: {sorted(unexplained)}"
        )
        stale = set(ALLOWED_BEFORE_MAIN_TRANSACTION) - between
        assert not stale, f"allowlist entries apply no longer makes: {sorted(stale)}"

    @pytest.mark.parametrize("snippet", [
        "def _action_apply(s):\n    Channel.objects.create(name='x')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    qs = Channel.objects.filter(a=1)\n    qs.update(b=2)\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    with transaction.atomic():\n        pass\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def helper():\n    write_json_atomic('p', {})\ndef _action_apply(s):\n    helper()\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    def inner():\n        Logo.objects.get_or_create(url='u')\n    inner()\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    os.replace('a', 'b')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
    ])
    def test_the_scan_sees_an_early_write(self, snippet):
        sites, checks, _main = _apply_layout(snippet)
        assert [s for s in sites if s[0] < max(checks)], f"scan missed: {snippet!r}"

    def test_the_scan_ignores_dicts_sets_and_strings(self):
        snippet = ("def _action_apply(s):\n    d = {}\n    d.update(a=1)\n    ids = set()\n"
                   "    ids.update([1])\n    'a_b'.replace('_', '-')\n"
                   "    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n"
                   "    with transaction.atomic():\n        pass")
        sites, checks, _main = _apply_layout(snippet)
        assert [s for s in sites if s[0] < max(checks)] == []


# --------------------------------------------------------- pytest driver ---

SCENARIOS = _kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return _kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_apply_no_write_before_gate(harness_results, name):
    error = harness_results.get(name, "scenario did not run")
    assert error is None, error
