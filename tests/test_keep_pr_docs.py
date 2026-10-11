"""The docs-bot guard: a docs pass must not remove markdown the PR added (#247).

On PR #246 the Auto-Document pass deleted a 37-line CLAUDE.md section the PR
itself had added. `.github/scripts/keep-pr-docs.sh` now runs in the trusted
validate step and fails the job before anything is pushed. These tests drive
the real script against throwaway git repos: a base commit, a PR commit, and
the docs pass's edits left in the working tree, as in the workflow.

Needs `git` and `bash` on PATH (the test-run command installs git).
"""

import os
import shutil
import subprocess

import pytest

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      ".github", "scripts", "keep-pr-docs.sh")


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    """base commit (tag `base`) -> PR commit at HEAD; returns a helper."""
    assert shutil.which("git"), "git is required for the docs-bot guard tests"
    r = tmp_path / "r"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "t")

    def commit(files, tag=None):
        for name, text in files.items():
            p = r / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(text.encode() if isinstance(text, str) else text)
        _git(r, "add", "-A")
        _git(r, "commit", "-q", "-m", "c")
        if tag:
            _git(r, "tag", tag)

    def write(name, text):
        (r / name).write_bytes(text.encode() if isinstance(text, str) else text)

    def guard():
        return subprocess.run(["bash", SCRIPT, "base"], cwd=r, capture_output=True, text=True)

    return commit, write, guard


OLD = "# Doc\n\nOld line one.\n\n```\ncode\n```\n"


def test_the_incident_is_refused(repo):
    commit, write, guard = repo
    commit({"CLAUDE.md": OLD}, tag="base")
    commit({"CLAUDE.md": OLD + "\n- **New gotcha** the PR adds.\n  It explains a thing.\n"})
    write("CLAUDE.md", OLD)  # the pass deletes the PR's section
    res = guard()
    assert res.returncode == 1
    assert "New gotcha" in res.stdout


def test_a_pass_that_only_adds_is_allowed(repo):
    commit, write, guard = repo
    commit({"README.md": OLD}, tag="base")
    commit({"README.md": OLD + "PR line.\n"})
    write("README.md", OLD + "PR line.\nBot line.\n")
    assert guard().returncode == 0


def test_removing_a_line_the_pr_did_not_add_is_allowed(repo):
    commit, write, guard = repo
    commit({"README.md": OLD}, tag="base")
    commit({"README.md": OLD + "PR line.\n"})
    write("README.md", OLD.replace("Old line one.\n", "") + "PR line.\n")
    assert guard().returncode == 0


def test_no_changes_is_allowed(repo):
    commit, _write, guard = repo
    commit({"README.md": OLD}, tag="base")
    commit({"README.md": OLD + "PR line.\n"})
    assert guard().returncode == 0


def test_rewriting_a_pr_line_is_refused(repo):
    commit, write, guard = repo
    commit({"README.md": OLD}, tag="base")
    commit({"README.md": OLD + "The PR's careful sentence.\n"})
    write("README.md", OLD + "The bot's paraphrase.\n")
    assert guard().returncode == 1


@pytest.mark.parametrize("line", ["++ Preserve me", "-- Preserve me"])
def test_content_that_looks_like_a_diff_header_is_still_compared(repo, line):
    commit, write, guard = repo
    commit({"a.md": OLD}, tag="base")
    commit({"a.md": OLD + line + "\n"})
    write("a.md", OLD)
    assert guard().returncode == 1


def test_a_path_git_has_to_quote_is_still_checked(repo):
    commit, write, guard = repo
    name = 'docs/quote"name.md'
    commit({name: OLD}, tag="base")
    commit({name: OLD + "Preserve me.\n"})
    write(name, OLD)
    assert guard().returncode == 1


def test_a_binary_rewrite_is_refused(repo):
    commit, write, guard = repo
    commit({"a.md": OLD}, tag="base")
    commit({"a.md": OLD + "Preserve me.\n"})
    write("a.md", b"\x00\x01binary\x00")
    assert guard().returncode == 1


def test_non_ascii_text_is_compared(repo):
    commit, write, guard = repo
    commit({"a.md": OLD}, tag="base")
    commit({"a.md": OLD + "请保留此段\n"})
    write("a.md", OLD)
    assert guard().returncode == 1


def test_the_same_text_in_another_file_does_not_pin_it(repo):
    commit, write, guard = repo
    commit({"a.md": OLD, "b.md": OLD + "Shared instruction.\n"}, tag="base")
    commit({"a.md": OLD + "Shared instruction.\n"})
    write("b.md", OLD)  # removes b.md's pre-existing copy only
    assert guard().returncode == 0


def test_punctuation_only_lines_do_not_pin_each_other(repo):
    commit, write, guard = repo
    commit({"a.md": OLD}, tag="base")
    commit({"a.md": OLD + "\n```\nnew code\n```\n"})
    write("a.md", OLD.replace("```\ncode\n```\n", "code\n") + "\n```\nnew code\n```\n")
    assert guard().returncode == 0
