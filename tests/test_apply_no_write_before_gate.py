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


def scenario_witness_a_refused_rename_leaves_no_two_channels_on_401(env):
    """Codex review: the full-band refusal (compact, every slot taken) returns
    after numbering, and the group setup used to have committed by then. A
    band of ONE slot, 401, held in the new group by the user's own channel:
    the rename is refused, and must leave our channel where it was."""
    m = env.m
    game, ours, stranger = _published_on_401(env)
    _write_cache(env, [game])
    one_slot = dict(COMPACT, compact_band_size=1)
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **one_slot))
    assert result["status"] == "error" and "no free channel numbers" in result["message"], result
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the refused apply moved our channel into {ours.channel_group.name!r}; "
        f"{NEW!r} now holds {_numbers_in(env, NEW)}"
    )
    assert _numbers_in(env, NEW) == [401]
    assert m.ChannelGroup.objects.filter(name=OLD).exists()


def scenario_witness_a_case_only_archive_rename_keeps_the_archive(env):
    """Codex review: 'Recordings group name' changed only in case. The archive
    group matched the protected name exactly, so it looked like a previous
    LIVE group: its archive channel was moved into the live group and, on
    the base branch, reaped there (deleted)."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    archive_ch = m.Channel.objects.create(
        name="Matchups Recordings", channel_number=9999, channel_group=archive,
        tvg_id=env.plugin.ARCHIVE_TVG_ID,
    )
    # A finished recording lives there: the archive exists only while it
    # holds one, and an empty one is (rightly) cleaned up.
    m.Recording.objects.create(
        channel=archive_ch, start_time=_now() - timedelta(days=2),
        end_time=_now() - timedelta(days=2, hours=-3), custom_properties={"status": "completed"},
    )
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _publish(env, [game], settings=_settings(recordings_group_name="matchups recordings", **COMPACT))
    row = m.Channel.objects.filter(pk=archive_ch.pk).first()
    assert row is not None and row.channel_group_id == archive.pk, (
        "the archive channel was " + ("deleted" if row is None
                                      else f"moved into {row.channel_group.name!r}")
    )
    assert m.ChannelGroup.objects.filter(pk=archive.pk).exists()


def scenario_a_rename_keeps_our_numbers(env):
    """Guard: numbering now runs BEFORE the group move commits, so it must
    count our channels in the group being renamed away from as ours. Counted
    as somebody else's, our own 401 reserves 401 against us and the rename
    renumbers the channel to 402."""
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
    assert result["status"] == "ok", result
    ours.refresh_from_db()
    assert (ours.channel_group.name, int(ours.channel_number)) == (NEW, 401)


def scenario_a_case_only_rename_of_the_live_group_moves_our_channel(env):
    """Guard: the LIVE name is matched exactly, unlike the archive's. A
    case-only rename must still move our channel out of the old-case group
    (same Channel.id); matched ignoring case, the old group was protected,
    our channel stranded in it, and a second channel was created."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    _write_cache(env, [game])
    # Not _publish: it counts channels in the literal "Top Matchups" group.
    first = env.plugin._action_apply(_settings(channel_profile_name="top matchups", **COMPACT))
    assert first["status"] == "ok", first
    ours = m.Channel.objects.get(tvg_id=G401)
    assert ours.channel_group.name == "top matchups"
    result = env.plugin._action_apply(_settings(channel_profile_name="Top Matchups", **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [c.pk for c in mine] == [ours.pk], [(c.pk, c.channel_group.name) for c in mine]
    assert mine[0].channel_group.name == "Top Matchups"


def scenario_witness_a_failed_rename_rolls_back(env):
    """Codex review, round 2: the setup committed on its own, so an error
    later in it (an EPGSource name the database rejects, here the #263 row
    move failing) left our channels moved and their guide on the old source.
    Inside the main transaction it rolls back with everything else."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    real = env.plugin._move_owned_epg_rows

    def rejected(*a, **k):
        raise RuntimeError("the database rejected the write")

    env.plugin._move_owned_epg_rows = rejected
    try:
        try:
            env.plugin._action_apply(_settings(channel_profile_name=NEW, **COMPACT))
        except RuntimeError:
            pass
        else:
            raise AssertionError("instrument: the injected failure never happened")
    finally:
        env.plugin._move_owned_epg_rows = real
    ours.refresh_from_db()
    assert ours.channel_group.name == OLD, (
        f"the failed apply left our channel moved into {ours.channel_group.name!r}"
    )
    assert not m.ChannelGroup.objects.filter(name=NEW).exists()
    assert m.EPGSource.objects.filter(name=OLD).exists()


def scenario_witness_an_archive_name_like_the_old_live_name_strands_nothing(env):
    """Codex review, round 2: the live group goes Top Matchups -> !Top Matchups
    while Recordings group name is set to 'top matchups'. Protecting archive
    NAMES ignoring case protected the old live group: our channel stayed in it
    and the game got a second channel. The archive is protected by the archive
    channel it holds, and the names exactly."""
    m = env.m
    user_ch, _ = _seed_source_channel(env)
    game = _row(401, _now() + timedelta(hours=6), user_ch.id)
    ours = _publish(env, [game], settings=_settings(**COMPACT))[G401]
    _write_cache(env, [game])
    result = env.plugin._action_apply(_settings(
        channel_profile_name=NEW, recordings_group_name="top matchups", **COMPACT))
    assert result["status"] == "ok", result
    mine = list(m.Channel.objects.filter(tvg_id=G401))
    assert [(c.pk, c.channel_group.name, int(c.channel_number)) for c in mine] == [(ours.pk, NEW, 401)], (
        [(c.pk, c.channel_group.name, c.channel_number) for c in mine]
    )


def scenario_witness_a_case_only_archive_rename_reuses_the_archive(env):
    """Codex review, round 2: archive creation and cleanup looked the group up
    by exact name, so 'matchups recordings' made a second archive group and
    channel beside 'Matchups Recordings', splitting the recordings."""
    m = env.m
    archive = m.ChannelGroup.objects.create(name="Matchups Recordings")
    arch = m.Channel.objects.create(
        name="Matchups Recordings", channel_number=9999, channel_group=archive,
        tvg_id=env.plugin.ARCHIVE_TVG_ID,
    )
    got = env.plugin._ensure_archive_channel("matchups recordings")
    assert got.pk == arch.pk, "a second archive channel was created"
    assert list(m.ChannelGroup.objects.filter(name__iexact="matchups recordings")) == [archive]
    assert env.plugin._cleanup_empty_archive("matchups recordings") is True  # it holds no recording
    assert not m.Channel.objects.filter(pk=arch.pk).exists()


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
    if os.path.isdir(logos_dir):
        for name in sorted(os.listdir(logos_dir)):
            with open(os.path.join(logos_dir, name), "rb") as f:
                files[f"logos/{name}"] = hashlib.sha256(f.read()).hexdigest()
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
    # Auto-record armed too, so its state and recordings are in play.
    armed = dict(COMPACT, recorded_teams="Georgia", record_ranked_games=True)
    _publish(env, [game], settings=_settings(**armed))
    assert env.m.Recording.objects.exists(), "instrument: auto-record did not run"
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
            result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **armed))
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
    result = env.plugin._action_apply(_settings(channel_profile_name="Renamed Matchups", **armed))
    assert result["status"] == "ok", result
    m = env.m
    assert m.ChannelGroup.objects.filter(name="Renamed Matchups").exists()
    assert not m.ChannelGroup.objects.filter(name=OLD).exists()
    assert not m.EPGSource.objects.filter(name=OLD).exists()
    assert m.ProgramData.objects.filter(epg__epg_source__name="Renamed Matchups").exists()


# ----------------------------------------- class guard, by source (AST) ---

_ORM_WRITES = {"create", "get_or_create", "update_or_create", "bulk_create", "bulk_update",
               "save", "delete"}
_FILE_WRITES = {"write_json_atomic", "save_state", "write_text", "write_bytes"}
_ATOMIC = "transaction.atomic()"

# Committed writes apply makes AFTER its last time check but OUTSIDE its main
# transaction, each with why it cannot wait for that transaction. Anything new
# there must be added here, with its reason, or fail.
ALLOWED_BEFORE_MAIN_TRANSACTION = {
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


def _write_sites(fn, writers, aliases=None):
    """(line, label) for every write `fn` makes itself, not inside nested
    defs (those count where they are CALLED, through `writers`). `writers`
    holds plain names (functions of the same module) and "module.func" names
    (other plugin modules), resolved through `aliases` ({local name: module})."""
    aliases = aliases or {}
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
            qualified = (f"{aliases[receiver]}.{f.attr}" if isinstance(f, ast.Attribute)
                         and receiver in aliases else aliases.get(name))
            if isinstance(f, ast.Attribute) and f.attr in _ORM_WRITES and receiver != "os.path":
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and f.attr == "update" and (
                    ".objects" in receiver or "filter(" in receiver or receiver in querysets):
                out.append((n.lineno, ast.unparse(f)))
            elif isinstance(f, ast.Attribute) and (
                    (receiver == "os" and f.attr in ("replace", "remove", "unlink", "rename"))
                    or receiver == "shutil"):
                out.append((n.lineno, ast.unparse(f)))
            elif name in _FILE_WRITES or (not receiver and name in writers) or qualified in writers:
                out.append((n.lineno, qualified or name))
        for c in ast.iter_child_nodes(n):
            visit(c)

    visit(fn)
    return out


def _writers(functions, known=(), aliases=None):
    """Names among `functions` that write, directly or through each other."""
    found = set(known)
    changed = True
    while changed:
        changed = False
        for name, fn in functions.items():
            if name not in found and _write_sites(fn, found, aliases):
                found.add(name)
                changed = True
    return found


def _module_writers(repo=os.path.dirname(PLUGIN_PY)):
    """"module.func" for every module-level function of a plugin module that
    writes (a file, the database), resolved within each module."""
    found = set()
    paths = [os.path.join(repo, n) for n in os.listdir(repo) if n.endswith(".py")]
    sources = os.path.join(repo, "sources")
    paths += [os.path.join(sources, n) for n in os.listdir(sources) if n.endswith(".py")]
    for path in paths:
        mod = os.path.relpath(path, repo)[:-3].replace(os.sep, ".")
        tree = ast.parse(open(path, encoding="utf-8").read())
        fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        found |= {f"{mod}.{name}" for name in _writers(fns)}
    return found


def _aliases(tree):
    """{local name: module} for `from . import X as Y` and `from .X import f`
    anywhere in `tree` (apply imports its siblings locally)."""
    out = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.level == 1:
            for a in n.names:
                if n.module is None:
                    out[a.asname or a.name] = a.name
                else:
                    out[a.asname or a.name] = f"{n.module}.{a.name}"
    return out


def _apply_layout(source, module_writers=frozenset()):
    tree = ast.parse(source)
    aliases = _aliases(tree)
    # A `from .X import f` alias names the function itself.
    func_aliases = {k: v for k, v in aliases.items() if v in module_writers}
    module = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    apply = module["_action_apply"]
    nested = {n.name: n for n in ast.walk(apply) if isinstance(n, ast.FunctionDef) and n is not apply}
    known = set(module_writers) | set(func_aliases)
    mod_aliases = {k: v for k, v in aliases.items() if k not in func_aliases}
    writers = _writers(nested, _writers(module, known, mod_aliases), mod_aliases)
    checks = [n.lineno for n in ast.walk(apply) if isinstance(n, ast.Call)
              and getattr(n.func, "id", None) == "_apply_time_short"]
    main = [n.lineno for n in apply.body if _is_atomic(n)]
    return sorted(set(_write_sites(apply, writers, mod_aliases))), checks, main


class TestNothingCommitsBeforeTheLastCheck:
    def _layout(self):
        return _apply_layout(open(PLUGIN_PY, encoding="utf-8").read(), _module_writers())

    def test_instrument(self):
        sites, checks, main = self._layout()
        assert len(checks) >= 2 and len(main) == 1, (checks, main)
        assert any(label == _ATOMIC for _l, label in sites)
        # Writes in sibling modules are resolved, not only plugin.py's own.
        writers = _module_writers()
        assert {"logos.download_thumb", "recording.save_state", "_util.write_json_atomic"} <= writers

    def test_the_group_setup_runs_inside_the_main_transaction(self):
        """Atomic with the slate: a stopped, refused or failed apply never
        leaves a rename half done (Codex review, round 2)."""
        tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read())
        apply = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_action_apply")
        main = [n for n in apply.body if _is_atomic(n)]
        calls = [n for n in ast.walk(apply) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", None) == "_commit_group_setup"]
        assert len(main) == 1 and len(calls) == 1
        assert any(n is calls[0] for n in ast.walk(main[0])), "the setup left the main transaction"

    def test_a_write_through_an_imported_module_is_seen(self):
        snippet = ("def _action_apply(s):\n    from . import logos as matchup_logos\n"
                   "    matchup_logos.download_thumb('u', 'p')\n"
                   "    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n"
                   "    with transaction.atomic():\n        pass")
        sites, checks, _main = _apply_layout(snippet, {"logos.download_thumb"})
        assert [s for s in sites if s[0] < max(checks)], sites

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
        "def _action_apply(s):\n    Path('x').write_text('y')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
        "def _action_apply(s):\n    shutil.move('a', 'b')\n    _apply_time_short('a', False)\n    _apply_time_short('b', False)\n    with transaction.atomic():\n        pass",
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
