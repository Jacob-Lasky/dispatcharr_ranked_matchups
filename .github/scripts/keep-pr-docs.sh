#!/usr/bin/env bash
# Refuse a docs pass that removes markdown the PR itself added (#247).
#
# Usage: keep-pr-docs.sh <base-ref>   (e.g. origin/main; run in the checkout
# after the Claude step and before anything is staged: the pass's edits are
# the working tree against HEAD, the PR's are <base-ref>...HEAD)
#
# The prompt tells the docs pass to keep documentation the PR adds, and on PR
# #246 it deleted a 37-line CLAUDE.md section anyway. The workflow's boundary
# is PREVENTION, so this is enforced here rather than trusted to the prompt.
# It is strict on purpose: a line the PR added and the pass removed or
# rewrote fails the job, because the PR's own docs are the author's to edit.
#
# Compared per file, so a line the PR added to a.md does not pin an identical
# line in b.md. Lines made only of ASCII punctuation and whitespace (fences,
# table rules, blank lines) are ignored for the same reason within a file.
# --text, so a pass that rewrites a file into binary still shows removals.
set -euo pipefail
export LC_ALL=C  # sort and comm must agree on order
base="$1"

# Content lines of one sign ("+" or "-") from a unified diff on stdin. Only
# inside hunks, so a content line that itself starts "++ " or "-- " (shown as
# "+++ " / "--- ") is not mistaken for a file header.
hunk_lines() {
  awk -v sign="$1" '
    /^diff --git / { in_hunk = 0; next }
    /^@@ /         { in_hunk = 1; next }
    in_hunk && substr($0, 1, 1) == sign { print substr($0, 2) }
  ' | { grep -v '^[][[:space:]!"#$%&()*+,./:;<=>?@\\^_`{|}~'"'"'-]*$' || true; } | sort -u
}

failed=0
while IFS= read -r -d '' f; do
  clobbered="$(comm -12 \
    <(git diff --text -U0 "$base...HEAD" -- "$f" | hunk_lines +) \
    <(git diff --text -U0 -- "$f" | hunk_lines -))"
  if [ -n "$clobbered" ]; then
    n="$(printf '%s\n' "$clobbered" | wc -l | tr -d ' ')"
    echo "::error file=$f::The docs pass removed or rewrote $n line(s) this PR added to $f. First lines:"
    printf '%s\n' "$clobbered" | head -20 || true  # head closing early is fine
    failed=1
  fi
done < <(git diff --name-only -z -- '*.md')

if [ "$failed" -ne 0 ]; then
  echo "::error::Refusing to push: the docs pass must not remove documentation this PR added."
  exit 1
fi
