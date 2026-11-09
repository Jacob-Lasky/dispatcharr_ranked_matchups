#!/usr/bin/env bash
# Put back the PR's own copies of the paths claude-code-action restored from the base branch.
#
# Usage (in the checkout, after the Claude step, before validation): restore-pr-config.sh BASE_REF
#
# Why. claude-code-action treats a PR head's config as untrusted: before the model runs it
# replaces these paths with the BASE branch's copies ("Restoring ... from origin/main") and leaves
# them that way. The validate step then saw every CLAUDE.md line a PR added as removed, and
# keep-pr-docs.sh refused the push (PR #338); without that guard the job would have reverted the
# PR's CLAUDE.md. The model has finished here, so the PR's copies are safe to put back.
#
# Per file under the paths, comparing the base copy (what the model was given), the worktree
# (what the model left) and the PR head:
#   - untouched by the model          -> the PR's copy (or no file, if the PR has none);
#   - a non-markdown file it changed   -> error: the job must not push (this keeps validation's
#                                         "markdown only" rule for these paths too);
#   - markdown it changed              -> three-way merge of its edit onto the PR's copy; on a
#                                         conflict, or when it could not have seen the PR's copy
#                                         (the PR adds or deletes the file), the PR's version wins
#                                         and a warning says the pass's edit was dropped;
#   - a new markdown file in neither   -> kept.
# DO NOT run this before the Claude step: the restore exists to keep PR config away from the model.
set -euo pipefail
base="origin/$1"
paths=(.claude .mcp.json .claude.json .gitmodules .ripgreprc CLAUDE.md CLAUDE.local.md .husky)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail=0

files() {
  for p in "${paths[@]}"; do
    git ls-tree -r --name-only "$base" -- "$p"
    git ls-tree -r --name-only HEAD -- "$p"
    if [ -d "$p" ]; then find "$p" -type f; elif [ -f "$p" ]; then printf '%s\n' "$p"; fi
  done | sort -u
}
blob() { git rev-parse -q --verify "$1:$2" 2>/dev/null || true; }
take_head() {   # the PR's copy, mode included, in the worktree and the index; or no file at all
  if [ -n "$2" ]; then git checkout -q HEAD -- "$1"; else git rm -q -f --ignore-unmatch -- "$1"; rm -f -- "$1"; fi
}

while IFS= read -r f; do
  b=$(blob "$base" "$f"); h=$(blob HEAD "$f"); w=""
  [ -f "$f" ] && w=$(git hash-object -- "$f")
  if [ "$w" = "$b" ]; then take_head "$f" "$h"; continue; fi
  case "$f" in
    *.md) ;;
    *) echo "::error::The docs pass changed protected non-markdown file $f"; fail=1; continue ;;
  esac
  if [ -z "$b" ] && [ -z "$h" ]; then continue; fi          # a new markdown file: keep it
  if [ -z "$b" ] || [ -z "$h" ] || [ -z "$w" ]; then
    if [ "$w" != "$h" ]; then
      echo "::warning::$f: the PR adds or deletes it (or the pass deleted it), so the pass worked without the PR's copy; kept the PR's version, dropped the pass's edit"
    fi
    take_head "$f" "$h"; continue
  fi
  git show "$base:$f" > "$tmp/base"; git show "HEAD:$f" > "$tmp/head"
  if git merge-file -q "$f" "$tmp/base" "$tmp/head"; then
    echo "$f: the PR's copy with the docs pass's edits merged in"
  else
    take_head "$f" "$h"
    echo "::warning::$f: the docs pass's edits conflict with the PR's changes; kept the PR's version"
  fi
done < <(files)

# The action's copy of the PR's sensitive paths, for its review agents: never committed.
rm -rf .claude-pr
exit "$fail"
