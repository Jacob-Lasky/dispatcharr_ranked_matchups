"""#193: every source's `_http_get` is a thin wrapper over `_util.http_get_json`.

The wrappers stay, one per module, because they are the patch points the
source tests stub (`<module>._http_get`, `get` on `<module>.requests`) and
because each feed logs under its own logger. What these tests pin:

  - every `_http_get` in `sources/` is exactly that wrapper, so an 18th
    hand-rolled copy cannot drift back in;
  - each wrapper resolves ITS OWN module's `requests` symbol at call time,
    with its own timeout (mlb's 20s is deliberate);
  - the shared body keeps the failure contract (None plus a WARNING on the
    source's own logger) and redacts secrets from both log lines;
  - `_util.py` never imports `requests` itself.
"""
from __future__ import annotations

import ast
import importlib
import logging
from pathlib import Path

import pytest

SOURCES = Path(__file__).resolve().parent.parent / "sources"
UTIL = Path(__file__).resolve().parent.parent / "_util.py"
PKG = "dispatcharr_ranked_matchups.sources"

# module -> the timeout its wrapper defaults to. Literals on purpose: mlb's
# 20s is a decision (its season schedule is ~2MB and 15s flaked), not a typo
# for the shared default.
WRAPPERS = {
    "english_cup": 15.0,
    "field_event": 15.0,
    "friendlies": 15.0,
    "mlb": 20.0,
    "mls": 15.0,
    "mls_cup": 15.0,
    "mls_standings": 15.0,
    "nba": 15.0,
    "ncaa_baseball": 15.0,
    "ncaa_soccer": 15.0,
    "ncaa_soccer_cup": 15.0,
    "ncaa_softball": 15.0,
    "ncaaw_basketball": 15.0,
    "nfl": 15.0,
    "nhl": 15.0,
    "wnba": 15.0,
    "boxing": 15.0,  # a method: the RapidAPI key rides in self._headers()
}


def _http_get_defs():
    """{module: FunctionDef} for every `_http_get`, module-level or method."""
    out = {}
    for path in sorted(SOURCES.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=path.name)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_http_get":
                assert path.stem not in out, f"{path.name} defines _http_get twice"
                out[path.stem] = node
    return out


class FakeResponse:
    def __init__(self, status_code=200, payload=None, json_error=None):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class FakeRequests:
    """Stands in for the `requests` MODULE a source imported."""

    class RequestException(Exception):
        pass

    def __init__(self, response=None, raises=None):
        self.calls = []
        self._response = response or FakeResponse(payload={"ok": True})
        self._raises = raises

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self._raises is not None:
            raise self._raises
        return self._response


def _call(module_name, monkeypatch, fake, url="https://example.test/feed"):
    mod = importlib.import_module(f"{PKG}.{module_name}")
    monkeypatch.setattr(mod, "requests", fake)
    if module_name == "boxing":
        return mod, mod.BoxingSource(api_key="k")._http_get(url, params={"days": 7})
    return mod, mod._http_get(url)


class TestEveryWrapperIsTheSharedBody:
    def test_the_set_of_wrappers_is_known(self):
        assert set(_http_get_defs()) == set(WRAPPERS), (
            "a module gained or lost an _http_get; add it to WRAPPERS as a "
            "one-line wrapper over _util.http_get_json"
        )

    @pytest.mark.parametrize("module_name", sorted(WRAPPERS))
    def test_body_is_one_delegating_return(self, module_name):
        fn = _http_get_defs()[module_name]
        body = fn.body
        if body and isinstance(body[0], ast.Expr) and isinstance(
            getattr(body[0], "value", None), ast.Constant
        ):
            body = body[1:]  # docstring
        assert len(body) == 1 and isinstance(body[0], ast.Return), (
            f"{module_name}._http_get is a hand-rolled copy again; route it "
            "through _util.http_get_json"
        )
        call = body[0].value
        assert isinstance(call, ast.Call) and getattr(call.func, "id", None) == "http_get_json"
        first = call.args[0]
        assert isinstance(first, ast.Name) and first.id == "requests", (
            "the wrapper must pass the module's own `requests` symbol; that "
            "symbol is the patch point the source's tests stub"
        )
        kw = {k.arg: k.value for k in call.keywords}
        assert getattr(kw.get("log"), "id", None) == "logger"
        assert getattr(kw.get("tag"), "value", None) == module_name

    def test_util_never_imports_requests(self):
        tree = ast.parse(UTIL.read_text(), filename="_util.py")
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        imported |= {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert "requests" not in imported, (
            "_util.py imports requests: the shared getter must take the "
            "caller's module, or every source stub becomes a live call"
        )


@pytest.mark.parametrize("module_name", sorted(WRAPPERS))
class TestWrapperBehaviour:
    def test_uses_its_own_requests_symbol_and_timeout(self, module_name, monkeypatch):
        fake = FakeRequests(response=FakeResponse(payload={"events": [1]}))
        _mod, out = _call(module_name, monkeypatch, fake)
        assert out == {"events": [1]}
        assert len(fake.calls) == 1, (
            f"{module_name}._http_get did not go through {module_name}.requests"
        )
        url, kwargs = fake.calls[0]
        assert url == "https://example.test/feed"
        assert kwargs["timeout"] == WRAPPERS[module_name]

    def test_error_status_is_none_and_logged_on_the_sources_logger(
        self, module_name, monkeypatch, caplog,
    ):
        fake = FakeRequests(response=FakeResponse(status_code=503))
        with caplog.at_level(logging.WARNING):
            _mod, out = _call(module_name, monkeypatch, fake)
        assert out is None
        hits = [r for r in caplog.records
                if r.name == f"plugins.dispatcharr_ranked_matchups.{module_name}"]
        assert [r.getMessage() for r in hits] == [
            f"[{module_name}] https://example.test/feed -> 503"
        ]

    def test_transport_failure_is_none_and_redacted(
        self, module_name, monkeypatch, caplog,
    ):
        boom = FakeRequests.RequestException(
            "Max retries exceeded with url: /v4/odds?apiKey=SEKRIT123&regions=us"
        )
        fake = FakeRequests(raises=boom)
        with caplog.at_level(logging.WARNING):
            _mod, out = _call(
                module_name, monkeypatch, fake,
                url="https://example.test/feed?api_key=SEKRIT123",
            )
        assert out is None
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert f"[{module_name}]" in text and "failed" in text
        assert "SEKRIT123" not in text

    def test_unparseable_body_is_none(self, module_name, monkeypatch):
        fake = FakeRequests(response=FakeResponse(json_error=ValueError("no json")))
        _mod, out = _call(module_name, monkeypatch, fake)
        assert out is None


def test_params_and_headers_are_forwarded_only_when_given():
    """A wrapper that never took params must keep making the exact
    `requests.get(url, timeout=...)` call it always did: some test stubs
    elsewhere accept only (url, timeout)."""
    from dispatcharr_ranked_matchups._util import http_get_json

    fake = FakeRequests()
    log = logging.getLogger("test")
    http_get_json(fake, "u", log=log, tag="t")
    http_get_json(fake, "u", log=log, tag="t", timeout=3.0,
                  params={"a": 1}, headers={"h": "v"})
    assert fake.calls == [
        ("u", {"timeout": 15.0}),
        ("u", {"timeout": 3.0, "params": {"a": 1}, "headers": {"h": "v"}}),
    ]
