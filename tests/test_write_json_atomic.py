"""The plugin's state files are all written by ONE helper, _util.write_json_atomic.

Before #219 there were six hand-rolled copies of "temp file, then os.replace",
and they had drifted: llm_descriptions wrote through a fixed ``path + ".tmp"``
(two writers truncate each other's temp, so the replace can publish a half
file), logos used a pid-only name (two threads in one worker share a pid), and
tasks left its temp behind on a failed dump.
"""

import ast
import json
import os
import threading

import pytest

from dispatcharr_ranked_matchups import _util

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class TestWriteJsonAtomic:
    def test_round_trip_and_dump_kwargs(self, tmp_path):
        p = str(tmp_path / "s.json")
        _util.write_json_atomic(p, {"b": 1, "a": "é"}, indent=2, sort_keys=True, ensure_ascii=False)
        with open(p, encoding="utf-8") as f:
            text = f.read()
        assert json.loads(text) == {"b": 1, "a": "é"}
        assert text.index('"a"') < text.index('"b"')
        assert "é" in text
        assert os.listdir(tmp_path) == ["s.json"]

    def test_temp_name_is_unique_per_process_and_thread(self, tmp_path, monkeypatch):
        seen = []
        real = os.replace
        monkeypatch.setattr(os, "replace", lambda a, b: (seen.append(a), real(a, b)))
        p = str(tmp_path / "s.json")
        _util.write_json_atomic(p, {})
        (tmp,) = seen
        assert str(os.getpid()) in tmp
        assert str(threading.get_ident()) in tmp
        assert tmp != p + ".tmp"

    def test_a_failed_dump_leaves_no_temp_and_keeps_the_old_file(self, tmp_path):
        p = str(tmp_path / "s.json")
        _util.write_json_atomic(p, {"v": 1})
        with pytest.raises(TypeError):
            _util.write_json_atomic(p, {"v": object()})
        assert os.listdir(tmp_path) == ["s.json"]
        with open(p, encoding="utf-8") as f:
            assert json.load(f) == {"v": 1}


def _replace_calls():
    """(file, function) for every os.replace call in shipped code."""
    out = []
    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in {"tests", ".git", "__pycache__", "tools", ".planning"}]
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            tree = ast.parse(open(path, encoding="utf-8").read())
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for n in ast.walk(fn):
                    if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                            and n.func.attr == "replace"
                            and isinstance(n.func.value, ast.Name) and n.func.value.id == "os"):
                        out.append((os.path.relpath(path, REPO_ROOT), fn.name))
    return out


class TestOneWriter:
    def test_the_scan_finds_the_helper(self):
        """Fail on the instrument first: an empty scan would pass the next test
        vacuously."""
        assert ("_util.py", "write_json_atomic") in _replace_calls()

    def test_no_other_json_state_writer_hand_rolls_the_replace(self):
        """logos.download_to_file streams image BYTES, so it cannot use a JSON
        helper; it is the one allowed exception and is pinned below."""
        allowed = {("_util.py", "write_json_atomic")}
        others = [c for c in _replace_calls() if c not in allowed]
        assert others == [("logos.py", "download_to_file")], others

    def test_the_image_download_temp_is_per_thread_too(self):
        src = open(os.path.join(REPO_ROOT, "logos.py"), encoding="utf-8").read()
        assert 'tmp_path = f"{dest_path}.{os.getpid()}.{threading.get_ident()}.tmp"' in src
