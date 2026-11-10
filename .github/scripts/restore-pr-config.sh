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
# Per entry under the paths (files and symlinks, compared as git does: type and blob), comparing
# the base copy (what the model was given), the worktree (what it left) and the PR head:
#   - untouched by the model          -> the PR's copy (or no entry, if the PR has none);
#   - a non-markdown entry it changed  -> error: the job must not push (this keeps validation's
#                                         "markdown only" rule for these paths too);
#   - markdown it changed              -> three-way merge of its edit onto the PR's copy; on a
#                                         conflict, or when it could not have seen the PR's copy
#                                         (the PR adds or deletes the file, or either is a link),
#                                         the PR's version wins and a warning says so;
#   - a new markdown file in neither   -> kept.
# A staged change the action did not make (the index differing from both the base and the PR
# copy) is an error too: the model has no git, so nothing it did can look like that.
# Paths are NUL-delimited throughout (git quotes unusual names in newline output).
# DO NOT run this before the Claude step: the restore exists to keep PR config away from the model.
set -euo pipefail
# Every name below is a file name, never a pattern: `.claude/x*.md` must not match `.claude/x!.md`.
export GIT_LITERAL_PATHSPECS=1
base="origin/$1"
git rev-parse -q --verify "$base^{commit}" >/dev/null || { echo "::error::no base ref $base"; exit 1; }
paths=(.claude .mcp.json .claude.json .gitmodules .ripgreprc CLAUDE.md CLAUDE.local.md .husky)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail=0

# The list is built first, so a git failure stops the script (set -e) instead of hiding in a pipe.
: > "$tmp/list"
for p in "${paths[@]}"; do
  git ls-tree -r -z --name-only "$base" -- "$p" >> "$tmp/list"
  git ls-tree -r -z --name-only HEAD -- "$p" >> "$tmp/list"
  if [ -d "$p" ] && [ ! -L "$p" ]; then
    find "$p" \( -type f -o -type l \) -print0 >> "$tmp/list"
  elif [ -e "$p" ] || [ -L "$p" ]; then
    printf '%s\0' "$p" >> "$tmp/list"
  fi
done
sort -zu "$tmp/list" > "$tmp/files"

# DO NOT read git output through `< <(git ...)` or use `$(git ...)` as an argument: set -e never
# sees a failure there, and a broken index read as "no entry" let the job pass (W290 review). Each
# helper runs in `x=$(helper ...)`, so its `|| exit 1` fails that assignment and set -e stops the job.
tree_id() {   # "<l|f>:<blob>" of $2 in tree $1, or empty. Fields end at the first tab, so a
              # newline in the name cannot shift them (NUL-terminated record, read whole).
  local rec mode rest
  git ls-tree -z "$1" -- "$2" > "$tmp/rec" || exit 1
  IFS= read -r -d '' rec < "$tmp/rec" || true
  [ -n "${rec:-}" ] || return 0
  mode=${rec%% *}; rest=${rec#* * }
  if [ "$mode" = 120000 ]; then printf 'l:%s' "${rest%%$'\t'*}"; else printf 'f:%s' "${rest%%$'\t'*}"; fi
}
index_blob() {   # the index's blob for $1, or empty ("<mode> <blob> <stage>\t<name>")
  local rec rest
  git ls-files -s -z -- "$1" > "$tmp/rec" || exit 1
  IFS= read -r -d '' rec < "$tmp/rec" || true
  [ -n "${rec:-}" ] || return 0
  rest=${rec#* }; printf '%s' "${rest%% *}"
}
work_id() {   # the same for the worktree entry
  local id
  if [ -L "$1" ]; then id=$(readlink -n -- "$1" | git hash-object --stdin) || exit 1; printf 'l:%s' "$id"
  elif [ -f "$1" ]; then id=$(git hash-object -- "$1") || exit 1; printf 'f:%s' "$id"; fi
}
take_head() {   # the PR's copy (type and mode included) in the worktree and the index, or none
  if [ -n "$2" ]; then git checkout -q HEAD -- "$1"; else git rm -q -f --ignore-unmatch -- "$1"; rm -f -- "$1"; fi
}

while IFS= read -r -d '' f; do
  b=$(tree_id "$base" "$f"); h=$(tree_id HEAD "$f"); w=$(work_id "$f")
  i=$(index_blob "$f")
  if [ -n "$i" ] && [ "f:$i" != "$b" ] && [ "l:$i" != "$b" ] && [ "f:$i" != "$h" ] && [ "l:$i" != "$h" ]; then
    echo "::error::$f has a staged change neither the base nor the PR has"; fail=1; continue
  fi
  if [ "$w" = "$b" ]; then take_head "$f" "$h"; continue; fi
  case "$f" in
    *.md) ;;
    *) echo "::error::The docs pass changed protected non-markdown file $f"; fail=1; continue ;;
  esac
  if [ -z "$b" ] && [ -z "$h" ]; then continue; fi          # a new markdown file: keep it
  if [ "${b%%:*}" != f ] || [ "${h%%:*}" != f ] || [ "${w%%:*}" != f ]; then
    if [ "$w" != "$h" ]; then
      echo "::warning::$f: the PR adds or deletes it, or it is a link (or the pass deleted it), so the pass worked without the PR's copy; kept the PR's version, dropped the pass's edit"
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
done < "$tmp/files"

# The action's copy of the PR's sensitive paths, for its review agents: never committed.
rm -rf .claude-pr
exit "$fail"
