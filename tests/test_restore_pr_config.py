"""The docs workflow's restore of the PR's config copies (.github/scripts/restore-pr-config.sh, PR #338).

claude-code-action replaces CLAUDE.md, .claude/, .mcp.json and friends with the BASE branch's copies
before the model runs. Each test builds a scratch repo with a base and a PR head, simulates that
restore, applies one kind of model edit, runs the script, and checks what validation would see.
"""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "restore-pr-config.sh"
pytestmark = pytest.mark.skipif(shutil.which("git") is None or shutil.which("bash") is None,
                                reason="needs git and bash")


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout


def write(repo, rel, text):
    p = repo / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "r"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    git(r, "config", "user.email", "t@t"); git(r, "config", "user.name", "t")
    write(r, "CLAUDE.md", "# Repo\n\nintro\n\n## Tools\n\nold tool line\n")
    write(r, ".claude/skills/a/SKILL.md", "skill a\n")
    write(r, ".mcp.json", '{"base": true}\n')
    write(r, "code.py", "x = 1\n")
    git(r, "add", "-A"); git(r, "commit", "-q", "-m", "base")
    git(r, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(r, "checkout", "-q", "-b", "pr")
    write(r, "CLAUDE.md", "# Repo\n\nintro\n\n## Tools\n\nold tool line\nPR ADDED LINE\n")
    write(r, ".claude/skills/b/SKILL.md", "skill b from the PR\n")
    write(r, ".mcp.json", '{"pr": true}\n')
    write(r, "code.py", "x = 2\n")
    git(r, "add", "-A"); git(r, "commit", "-q", "-m", "pr")
    # The action's restore: base copies of the protected paths, PR-only files gone.
    git(r, "checkout", "-q", "origin/main", "--", "CLAUDE.md", ".claude", ".mcp.json")
    git(r, "rm", "-q", "-r", "--cached", ".claude/skills/b")
    shutil.rmtree(r / ".claude/skills/b")
    write(r, ".claude-pr/CLAUDE.md", "the action's copy\n")
    return r


def run(repo):
    return subprocess.run(["bash", str(SCRIPT), "main"], cwd=repo, capture_output=True, text=True)


def changed(repo):
    """What the validate step reads: worktree, staged and untracked changes against the PR head."""
    out = git(repo, "status", "--porcelain")
    return sorted(line[3:] for line in out.splitlines())


def test_untouched_paths_get_the_prs_copies_back_and_nothing_is_left_to_commit(repo):
    """WITNESS (PR #338): the restored base CLAUDE.md read as the PR's lines removed."""
    assert "CLAUDE.md" in changed(repo)          # the bug, before the script
    r = run(repo)
    assert r.returncode == 0, r.stdout + r.stderr
    assert changed(repo) == []
    assert "PR ADDED LINE" in (repo / "CLAUDE.md").read_text()
    assert (repo / ".claude/skills/b/SKILL.md").read_text() == "skill b from the PR\n"
    assert (repo / ".mcp.json").read_text() == '{"pr": true}\n'
    assert not (repo / ".claude-pr").exists()


def test_a_docs_edit_to_claude_md_is_merged_onto_the_prs_copy(repo):
    text = (repo / "CLAUDE.md").read_text().replace("intro\n", "intro, updated by the docs pass\n")
    (repo / "CLAUDE.md").write_text(text)
    assert run(repo).returncode == 0
    merged = (repo / "CLAUDE.md").read_text()
    assert "updated by the docs pass" in merged and "PR ADDED LINE" in merged
    assert changed(repo) == ["CLAUDE.md"]


def test_a_docs_edit_to_a_skill_is_kept(repo):
    """Round-1 review: restoring the whole .claude tree dropped skill edits."""
    (repo / ".claude/skills/a/SKILL.md").write_text("skill a, updated by the docs pass\n")
    assert run(repo).returncode == 0
    assert (repo / ".claude/skills/a/SKILL.md").read_text() == "skill a, updated by the docs pass\n"
    assert changed(repo) == [".claude/skills/a/SKILL.md"]


def test_a_model_change_to_protected_non_markdown_fails_the_job(repo):
    """Round-1 review: resetting .mcp.json before validation hid the model's change."""
    (repo / ".mcp.json").write_text('{"model": true}\n')
    r = run(repo)
    assert r.returncode == 1 and "non-markdown file .mcp.json" in r.stdout


def test_a_conflicting_edit_keeps_the_prs_copy_and_says_so(repo):
    text = (repo / "CLAUDE.md").read_text().replace("old tool line\n", "old tool line, rewritten by the pass\n")
    (repo / "CLAUDE.md").write_text(text)
    r = run(repo)
    assert r.returncode == 0 and "conflict" in r.stdout
    assert "PR ADDED LINE" in (repo / "CLAUDE.md").read_text()
    assert changed(repo) == []


def test_a_new_markdown_file_from_the_pass_is_kept(repo):
    write(repo, ".claude/skills/c/SKILL.md", "new skill from the docs pass\n")
    assert run(repo).returncode == 0
    assert (repo / ".claude/skills/c/SKILL.md").exists()


def test_a_pr_added_claude_md_the_pass_rewrote_keeps_the_prs_copy_with_a_warning(repo):
    """Round-1 review: a PR-added file is one the pass never saw; its version must not be lost
    silently."""
    write(repo, "CLAUDE.local.md", "local notes from the PR\n")
    git(repo, "add", "CLAUDE.local.md"); git(repo, "commit", "-q", "-m", "pr adds local")
    git(repo, "rm", "-q", "--cached", "CLAUDE.local.md")
    os.remove(repo / "CLAUDE.local.md")                       # the action's restore: base has none
    write(repo, "CLAUDE.local.md", "the pass's own local notes\n")
    r = run(repo)
    assert r.returncode == 0 and "::warning::CLAUDE.local.md" in r.stdout
    assert (repo / "CLAUDE.local.md").read_text() == "local notes from the PR\n"


def test_a_pr_only_file_with_an_unusual_name_comes_back(repo):
    """Round-2 review: newline output quotes such names, so the file stayed deleted."""
    write(repo, ".claude/skills/café/SKILL.md", "unicode skill from the PR\n")
    git(repo, "add", ".claude/skills/café"); git(repo, "commit", "-q", "-m", "pr adds unicode")
    git(repo, "rm", "-q", "-r", "--cached", ".claude/skills/café")
    shutil.rmtree(repo / ".claude/skills/café")
    assert run(repo).returncode == 0
    assert (repo / ".claude/skills/café/SKILL.md").read_text() == "unicode skill from the PR\n"
    assert changed(repo) == []


def test_an_untouched_symlink_is_not_a_change(tmp_path):
    """Round-2 review: hashing the link's target, not its text, failed the job."""
    r = tmp_path / "links"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    git(r, "config", "user.email", "t@t"); git(r, "config", "user.name", "t")
    write(r, ".mcp.json", '{"base": true}\n')
    write(r, ".claude/settings.json", '{"s": 1}\n')
    os.symlink("settings.json", r / ".claude/link.json")
    git(r, "add", "-A"); git(r, "commit", "-q", "-m", "base")
    git(r, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(r, "checkout", "-q", "-b", "pr")
    write(r, "code.py", "x = 2\n")
    git(r, "add", "code.py"); git(r, "commit", "-q", "-m", "pr")
    out = run(r)
    assert out.returncode == 0, out.stdout + out.stderr
    assert (r / ".claude/link.json").is_symlink() and changed(r) == []


def test_a_staged_change_nobody_made_fails_the_job(repo):
    """Round-2 review: a staged change with the worktree back at base was reset away unseen."""
    (repo / ".mcp.json").write_text('{"staged": true}\n')
    git(repo, "add", ".mcp.json")
    git(repo, "show", "origin/main:.mcp.json")
    (repo / ".mcp.json").write_text('{"base": true}\n')
    r = run(repo)
    assert r.returncode == 1 and "staged change" in r.stdout


def test_a_missing_base_ref_fails_instead_of_passing(repo):
    """Round-2 review: enumeration errors were swallowed by process substitution."""
    r = subprocess.run(["bash", str(SCRIPT), "no-such-branch"], cwd=repo, capture_output=True, text=True)
    assert r.returncode == 1 and "no base ref" in r.stdout
