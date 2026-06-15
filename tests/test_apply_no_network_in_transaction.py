"""Regression guard for #136: `_action_apply` must not perform network I/O
inside its DB transaction.

The apply step holds a single `with transaction.atomic():` block. If the
per-game LLM-description call (`llm_describe_or_fallback`) or the SportsDB
logo resolution (`_resolve_matchup_logo_id`) runs INSIDE that block, the
transaction (and its DB connection) stays open across every slow network
call, which starves the login/token worker on large instances and on the
scheduled refresh. The fix resolves all network-backed values in a pre-pass
BEFORE opening the transaction.

This is a static (AST) contract rather than a runtime test because
`_action_apply` is deeply Django-coupled (Channel/EPGData/ProgramData ORM,
`transaction.atomic`) and the invariant we care about is purely about call
ORDER relative to the transaction, which the AST captures exactly. It fails
on the pre-fix code (calls inside/after the atomic block) and passes on the
fix (calls hoisted above it).
"""

import ast
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGIN_PY = os.path.join(REPO_ROOT, "plugin.py")

# The network-backed calls that must be resolved before the transaction opens.
NETWORK_CALLS = {"llm_describe_or_fallback", "_resolve_matchup_logo_id"}


def _called_name(call: ast.Call):
    """The bare callable name for a Call node (`f(...)` -> 'f', `a.b.f(...)` -> 'f')."""
    fn = call.func
    if isinstance(fn, ast.Attribute):
        return fn.attr
    if isinstance(fn, ast.Name):
        return fn.id
    return None


@pytest.fixture(scope="module")
def apply_fn():
    tree = ast.parse(open(PLUGIN_PY, encoding="utf-8").read(), filename=PLUGIN_PY)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_action_apply":
            return node
    pytest.fail("_action_apply not found in plugin.py")


def _atomic_lineno(apply_fn):
    """Line of the `with transaction.atomic():` statement inside _action_apply."""
    for node in ast.walk(apply_fn):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "atomic"):
                    return node.lineno
    return None


class TestNoNetworkInsideApplyTransaction:
    def test_transaction_atomic_present(self, apply_fn):
        assert _atomic_lineno(apply_fn) is not None, \
            "_action_apply no longer wraps writes in transaction.atomic()"

    def test_network_calls_exist_and_precede_the_transaction(self, apply_fn):
        atomic_line = _atomic_lineno(apply_fn)
        seen = {name: [] for name in NETWORK_CALLS}
        for node in ast.walk(apply_fn):
            if isinstance(node, ast.Call):
                name = _called_name(node)
                if name in NETWORK_CALLS:
                    seen[name].append(node.lineno)

        # Both calls must still exist (guards against a silent removal that would
        # make the "before the transaction" check vacuously pass).
        for name in NETWORK_CALLS:
            assert seen[name], f"{name} call disappeared from _action_apply"

        # Every network call site must be ABOVE the transaction. The invocation
        # inside `transaction.atomic()` is what wedged login (#136).
        offenders = {n: ls for n, ls in seen.items()
                     if any(line >= atomic_line for line in ls)}
        assert not offenders, (
            f"network I/O inside/after transaction.atomic() (line {atomic_line}): "
            f"{offenders} -- resolve these in the pre-pass before the transaction (#136)")


class TestPrepContract:
    """The pre-pass produces a `prep_by_marker[...] = SimpleNamespace(...)` plan
    that the write loop consumes as `prep.<attr>`. That's a producer/consumer
    data contract: every attribute the write loop reads must be one the pre-pass
    sets, or apply breaks at runtime with AttributeError. Lock it statically so a
    future edit that reads `prep.foo` without producing `foo` fails in CI."""

    def _prep_fields_produced(self, apply_fn):
        """kwargs of the SimpleNamespace assigned into prep_by_marker[...]."""
        for node in ast.walk(apply_fn):
            if (isinstance(node, ast.Assign)
                    and isinstance(node.targets[0], ast.Subscript)
                    and isinstance(node.targets[0].value, ast.Name)
                    and node.targets[0].value.id == "prep_by_marker"
                    and isinstance(node.value, ast.Call)):
                return {kw.arg for kw in node.value.keywords if kw.arg}
        return None

    def _prep_attrs_consumed(self, apply_fn):
        """every `prep.<attr>` read in _action_apply."""
        attrs = set()
        for node in ast.walk(apply_fn):
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id == "prep"):
                attrs.add(node.attr)
        return attrs

    def test_every_consumed_prep_attr_is_produced(self, apply_fn):
        produced = self._prep_fields_produced(apply_fn)
        assert produced, "prep_by_marker[...] = SimpleNamespace(...) not found in _action_apply"
        consumed = self._prep_attrs_consumed(apply_fn)
        assert consumed, "write loop reads no prep.<attr> -- contract anchor moved?"
        missing = consumed - produced
        assert not missing, (
            f"write loop reads prep attrs the pre-pass never sets: {missing}. "
            f"Produced: {sorted(produced)} (#136 prep contract)")
