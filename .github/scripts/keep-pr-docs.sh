#!/usr/bin/env bash
# Refuse a docs pass that removes markdown the PR itself added (#247).
#
# Usage: keep-pr-docs.sh <pr.diff>   (run in the checkout, after the Claude
# step, before anything is staged; compares the working tree with HEAD)
#
# The prompt tells the docs pass to keep documentation the PR adds, and on PR
# #246 it deleted a 37-line CLAUDE.md section anyway. The workflow's boundary
# is PREVENTION, so this is enforced here rather than trusted to the prompt.
# It is strict on purpose: a line the PR added and the pass removed or
# rewrote fails the job, because the PR's own docs are the author's to edit.
# Lines with no letter or digit are ignored: a code fence or table rule the PR
# adds must not pin an identical one elsewhere in the file.
set -euo pipefail
export LC_ALL=C  # sort and comm must agree on order
pr_diff="$1"

pr_added="$(mktemp)"
bot_removed="$(mktemp)"
trap 'rm -f "$pr_added" "$bot_removed"' EXIT

# Lines the PR added to .md files.
awk '
  /^diff --git / { md = ($NF ~ /\.md$/) ; next }
  md && /^\+/ && !/^\+\+\+ / { print substr($0, 2) }
' "$pr_diff" | { grep '[[:alnum:]]' || true; } | sort -u > "$pr_added"

# Lines the docs pass removed from .md files.
git diff -U0 -- '*.md' \
  | awk '/^-/ && !/^--- / { print substr($0, 2) }' \
  | { grep '[[:alnum:]]' || true; } | sort -u > "$bot_removed"

clobbered="$(comm -12 "$pr_added" "$bot_removed")"
if [ -n "$clobbered" ]; then
  n="$(printf '%s\n' "$clobbered" | wc -l | tr -d ' ')"
  echo "::error::The docs pass removed or rewrote $n line(s) of documentation this PR added; refusing to push. First lines:"
  printf '%s\n' "$clobbered" | head -20 || true  # head closing early is fine
  exit 1
fi
