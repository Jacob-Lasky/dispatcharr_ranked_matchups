"""Parse the Annex C 495-row third-place table from Wikipedia template
wikitext and emit a Python literal that can be pasted into
sources/soccer.py.

Usage:
  curl -sL "https://en.wikipedia.org/wiki/Template:2026_FIFA_World_Cup_third-place_table?action=raw" > /tmp/raw.txt
  python3 tools/parse_annex_c.py /tmp/raw.txt > /tmp/literal.py
  # then splice /tmp/literal.py into sources/soccer.py replacing
  # the existing _WC2026_THIRD_PLACER_SLOT_TABLE body.

The script EXITS NONZERO if any of the 495 rows fails a constraint
(slot count, allowed_groups membership, advancing/assigned letter
mismatch, duplicate key). Don't paste output unless this script exits 0.

Re-run conditions: Wikipedia's table updates (rare) or if a
transcription bug is discovered. The parametrized test at
tests/test_annex_c_table.py is the regression guard against drift.

Wikipedia table column header:
  1A vs, 1B vs, 1D vs, 1E vs, 1G vs, 1I vs, 1K vs, 1L vs

These are the 8 reserved best_third slots in the LAST_32 bracket. Map
each header column to the corresponding l32_match_idx in our
_WC2026_LAST_32_PAIRINGS table:

  Col 0 (1A vs) -> M79 (l32_idx=6)   away slot is best_third from {CEFHI}
  Col 1 (1B vs) -> M85 (l32_idx=12)  away slot from {EFGIJ}
  Col 2 (1D vs) -> M81 (l32_idx=8)   from {BEFIJ}
  Col 3 (1E vs) -> M74 (l32_idx=1)   from {ABCDF}
  Col 4 (1G vs) -> M82 (l32_idx=9)   from {AEHIJ}
  Col 5 (1I vs) -> M77 (l32_idx=4)   from {CDFGH}
  Col 6 (1K vs) -> M87 (l32_idx=14)  from {DEIJL}
  Col 7 (1L vs) -> M80 (l32_idx=7)   from {EHIJK}

Each row gives:
  - 12 cells: which third-placers are advancing (bold letter or blank)
  - 8 cells: 3X assignment per column (where X is the source group)

Output literal: a dict mapping frozenset(advancing_group_letters) to
a sorted tuple of (l32_match_idx, side, group_letter) triples. Side
is always "away" because every best_third slot in our pairings is the
away slot.
"""
from __future__ import annotations

import re
import sys
from typing import Dict, FrozenSet, List, Tuple

HEADER_COL_TO_L32 = {
    0: 6,   # 1A vs -> M79
    1: 12,  # 1B vs -> M85
    2: 8,   # 1D vs -> M81
    3: 1,   # 1E vs -> M74
    4: 9,   # 1G vs -> M82
    5: 4,   # 1I vs -> M77
    6: 14,  # 1K vs -> M87
    7: 7,   # 1L vs -> M80
}

# Cross-reference: the allowed_groups frozenset for each L32 best_third
# slot (from _WC2026_LAST_32_PAIRINGS). The parser verifies each row's
# slot assignment satisfies the constraint.
L32_ALLOWED_GROUPS = {
    1: frozenset("ABCDF"),   # M74 from cols
    4: frozenset("CDFGH"),   # M77
    6: frozenset("CEFHI"),   # M79
    7: frozenset("EHIJK"),   # M80
    8: frozenset("BEFIJ"),   # M81
    9: frozenset("AEHIJ"),   # M82
    12: frozenset("EFGIJ"),  # M85
    14: frozenset("DEIJL"),  # M87
}


def parse(text: str) -> Dict[FrozenSet[str], List[Tuple[int, str, str]]]:
    # Split into row blocks. Each row starts with `! scope="row" | NNN`.
    row_re = re.compile(r'!\s*scope="row"\s*\|\s*(\d+)\s*\n((?:.+\n)+?)(?=!\s*scope="row"|\|\})', re.MULTILINE)
    out: Dict[FrozenSet[str], List[Tuple[int, str, str]]] = {}

    for m in row_re.finditer(text):
        row_num = int(m.group(1))
        body = m.group(2)

        # Flatten the body cells. Wikitext uses `|| cell` and `| cell` and `|`
        # for empty cells. Join all lines, then split on `||` and leading `|`.
        flat = re.sub(r'\n\s*', ' ', body).strip()

        # Row 1 has a single `! rowspan="495" |` cell between the 12 advancing
        # cells and the 8 slot cells. Replace it with `||` so the cell stream
        # stays uniform across all rows (it functions as a column separator,
        # not a data cell). Other rows have no such marker; we just strip.
        flat = re.sub(r'!\s*rowspan="495"\s*\|', '||', flat)

        # Strip trailing row terminator (`|-`) and the leading column delimiter.
        flat = re.sub(r'\|-\s*$', '', flat).strip()
        if flat.startswith('|'):
            flat = flat[1:]
        cells = [c.strip() for c in flat.split('||')]

        # Expected: 20 cells total -- 12 advancing-group cells + 8 slot cells.
        if len(cells) != 20:
            print(f"Row {row_num}: expected 20 cells, got {len(cells)}: {cells}", file=sys.stderr)
            sys.exit(1)

        # Parse cells 0-11: advancing-group letters. Bold = advancing.
        # Wikitext: `'''X'''` for a bold letter; empty cell = team didn't advance.
        adv_cells = cells[:12]
        advancing: List[str] = []
        # Column-to-letter map: cell 0 is group A, cell 1 is B, ..., cell 11 is L.
        for i, raw in enumerate(adv_cells):
            cell_letter = chr(ord("A") + i)
            bold_match = re.search(r"'''([A-L])'''", raw)
            if bold_match:
                if bold_match.group(1) != cell_letter:
                    print(f"Row {row_num}: cell {i} expected '{cell_letter}', got '{bold_match.group(1)}'",
                          file=sys.stderr)
                    sys.exit(1)
                advancing.append(cell_letter)

        if len(advancing) != 8:
            print(f"Row {row_num}: expected 8 advancing letters, got {len(advancing)}: {advancing}",
                  file=sys.stderr)
            sys.exit(1)

        # Parse cells 12-19: 3X slot assignments.
        slot_cells = cells[12:20]
        assignments: List[Tuple[int, str, str]] = []
        for col, raw in enumerate(slot_cells):
            slot_match = re.search(r'3\s*([A-L])', raw)
            if not slot_match:
                print(f"Row {row_num}: slot col {col} no 3X match in {raw!r}", file=sys.stderr)
                sys.exit(1)
            letter = slot_match.group(1)
            l32_idx = HEADER_COL_TO_L32[col]
            # Verify the group letter is in the slot's allowed_groups
            # constraint.
            if letter not in L32_ALLOWED_GROUPS[l32_idx]:
                print(f"Row {row_num}: slot col {col} assigns group {letter} "
                      f"to L32 {l32_idx} but allowed is {sorted(L32_ALLOWED_GROUPS[l32_idx])}",
                      file=sys.stderr)
                sys.exit(1)
            assignments.append((l32_idx, "away", letter))

        # Verify each advancing group appears exactly once in the slot
        # assignments.
        assigned_letters = [a[2] for a in assignments]
        if sorted(assigned_letters) != sorted(advancing):
            print(f"Row {row_num}: advancing {sorted(advancing)} != assigned {sorted(assigned_letters)}",
                  file=sys.stderr)
            sys.exit(1)

        key = frozenset(advancing)
        if key in out:
            print(f"Row {row_num}: duplicate key {sorted(key)}", file=sys.stderr)
            sys.exit(1)
        # Sort assignments by l32_match_idx for stable output.
        out[key] = sorted(assignments, key=lambda t: t[0])

    return out


def emit_python_literal(table: Dict[FrozenSet[str], List[Tuple[int, str, str]]]) -> str:
    """Format as a multi-line Python dict literal grouped 1 entry per line."""
    lines = ["_WC2026_THIRD_PLACER_SLOT_TABLE: Dict[FrozenSet[str], Tuple[Tuple[int, str, str], ...]] = {"]
    # Sort by key (sorted-letter representation) for deterministic output.
    for key in sorted(table.keys(), key=lambda f: sorted(f)):
        letters = "".join(sorted(key))
        triples = ", ".join(f'({a},"{b}","{c}")' for (a, b, c) in table[key])
        lines.append(f'    frozenset("{letters}"): ({triples}),')
    lines.append("}")
    return "\n".join(lines)


if __name__ == "__main__":
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        text = f.read()
    table = parse(text)
    print(f"# Parsed {len(table)} rows", file=sys.stderr)
    if len(table) != 495:
        print(f"ERROR: expected 495 unique combinations, got {len(table)}", file=sys.stderr)
        sys.exit(1)
    print(emit_python_literal(table))
