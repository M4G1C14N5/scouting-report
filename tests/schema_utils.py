"""Parse the feature-table column lists out of schema.sql.

Used by the metrics consistency tests and by tests/fakes.py, so the fakes
always carry exactly the columns BigQuery returns.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

SCHEMA = Path(__file__).resolve().parents[1] / "schema.sql"

IDENTITY_COLUMNS = frozenset(
    {"Player", "Nation", "Position", "Squad", "League", "Age", "Born", "Season", "position_group"}
)


@lru_cache(maxsize=None)
def feature_columns(table: str) -> list[str]:
    """Column names of `scouting.<table>` (a CREATE OR REPLACE TABLE ... AS SELECT)."""
    text = SCHEMA.read_text(encoding="utf-8")
    match = re.search(
        r"CREATE OR REPLACE TABLE `scouting\." + re.escape(table) + r"` AS\s+SELECT(.*?)\nFROM ",
        text,
        re.S,
    )
    if not match:
        raise ValueError(f"{table} not found in {SCHEMA}")
    body = "\n".join(line.split("--", 1)[0] for line in match.group(1).splitlines())

    # Split on top-level commas only: SPLIT(s.Position, ',') has commas inside parens.
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(ch)
    items.append("".join(current))

    columns: list[str] = []
    for item in items:
        item = item.strip()
        if not item:
            continue
        alias = re.search(r"\bAS\s+(\w+)\s*$", item, re.I)
        columns.append(alias.group(1) if alias else re.search(r"(\w+)\s*$", item).group(1))
    return columns


def numeric_columns(table: str) -> list[str]:
    return [c for c in feature_columns(table) if c not in IDENTITY_COLUMNS]
