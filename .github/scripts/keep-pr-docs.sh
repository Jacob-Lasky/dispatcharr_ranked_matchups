#!/usr/bin/env bash
# Refuse a docs pass that removes markdown the PR itself added (#247).
#
# Usage: keep-pr-docs.sh <base-ref>   (e.g. origin/main; run in the checkout
# after the Claude step and before anything is staged: the pass's edits are
# the working tree, the PR's are <base-ref>...HEAD)
#
# The prompt tells the docs pass to keep documentation the PR adds, and on PR
# #246 it deleted a 37-line CLAUDE.md section anyway. The workflow's boundary
# is PREVENTION, so this is enforced here rather than trusted to the prompt.
#
# The rule, per .md file the pass touched: every line the PR added must still
# appear in the file at least as many times as the PR added it. So deleting
# or rewriting a PR-added line fails the job (strict on purpose: the PR's own
# docs are the author's to edit), and removing an older copy of a line the PR
# also added does not. Lines made only of ASCII punctuation and whitespace
# (fences, table rules, blank lines) are ignored, and so is a trailing CR (a
# CRLF checkout must compare equal to the LF diff). Fails CLOSED: any git
# error stops the script nonzero, and the workflow does not push.
#
# Known false failure, accepted because it fails closed and is rare here: if
# the PR RENAMES a .md file, the diff scoped to the new path counts the
# inherited lines as added, so the pass may not edit those either.
set -euo pipefail
export LC_ALL=C
base="$1"

git rev-parse --verify --quiet "$base^{commit}" > /dev/null \
  || { echo "::error::keep-pr-docs: base ref '$base' is not available"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Content lines of one sign ("+" or "-") from a unified diff, only inside
# hunks: a content line that starts "++ " shows as "+++ " and is not a header.
# grep -a: a NUL in the diff must not turn it into "binary file matches".
hunk_lines() {
  awk -v sign="$1" '
    /^diff --git / { in_hunk = 0; next }
    /^@@ /         { in_hunk = 1; next }
    in_hunk && substr($0, 1, 1) == sign { line = substr($0, 2); sub(/\r$/, "", line); print line }
  ' "$2" | { grep -a -v '^[][[:space:]!"#$%&()*+,./:;<=>?@\\^_`{|}~'"'"'-]*$' || true; }
}

# Files the pass changed, NUL-separated so any path survives. Written to a
# file, not read through <(...), so a git failure stops the script.
git diff --name-only -z -- '*.md' > "$work/files"

failed=0
while IFS= read -r -d '' f; do
  git diff --text -U0 "$base...HEAD" -- "$f" > "$work/pr.diff"
  hunk_lines + "$work/pr.diff" > "$work/added"
  [ -s "$work/added" ] || continue
  if [ -f "$f" ]; then cp "$f" "$work/now"; else : > "$work/now"; fi
  awk '{ sub(/\r$/, "") } NR == FNR { need[$0]++; next } { have[$0]++ }
       END { for (l in need) if (have[l] < need[l]) print l }' \
    "$work/added" "$work/now" > "$work/missing"
  if [ -s "$work/missing" ]; then
    n="$(wc -l < "$work/missing" | tr -d ' ')"
    echo "::error file=$f::The docs pass removed or rewrote $n line(s) this PR added to $f. First lines:"
    head -20 "$work/missing"
    failed=1
  fi
done < "$work/files"

if [ "$failed" -ne 0 ]; then
  echo "::error::Refusing to push: the docs pass must not remove documentation this PR added."
  exit 1
fi
