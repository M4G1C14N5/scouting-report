"""Pure helpers: percentiles, percentile bands, ordinals and number formatting.

No Streamlit and no config imports, so everything here is unit-tested
directly (tests/test_stats.py). Fixes three old bugs: percentiles counted
NaN rows in the denominator, lower-is-better stats were never inverted, and
the suffix was always "th".
"""

from __future__ import annotations

import math
import re
from typing import Any, NamedTuple

import pandas as pd

BAND_LABELS = ("Poor", "Below avg", "Above avg", "Elite")
BAND_EDGES = (25.0, 50.0, 75.0)  # <25, 25-49, 50-74, 75+
MISSING = "—"
_SEASON = re.compile(r"^(\d{4})-(\d{4})$")


class Band(NamedTuple):
    """A percentile band. `index` (0-3) picks the colour from theme.PCT_RAMP."""

    index: int
    label: str


def is_missing(value: Any) -> bool:
    """True for None, NaN, pd.NA and NaT. Never raises."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):  # list-likes are not a single missing value
        return False


def to_float(value: Any) -> float | None:
    """`value` as a float, or None when it is missing or not numeric."""
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile_of(series: Any, value: Any, higher_is_better: bool = True) -> float | None:
    """Mid-rank percentile (0-100) of `value` within `series`.

    NaN rows are dropped before counting. Ties count half, which makes the
    two directions exact mirrors (lower = 100 - higher): a player with zero
    red cards lands mid-pool instead of near the bottom just because most
    of the pool also has zero.
    """
    v = to_float(value)
    if v is None:
        return None
    clean = pd.to_numeric(pd.Series(series), errors="coerce").dropna().astype(float)
    if clean.empty:
        return None
    below = int((clean < v).sum())
    equal = int((clean == v).sum())
    pct = (below + 0.5 * equal) / len(clean) * 100.0
    return pct if higher_is_better else 100.0 - pct


def band(pct: float) -> Band:
    for i, edge in enumerate(BAND_EDGES):
        if pct < edge:
            return Band(i, BAND_LABELS[i])
    return Band(len(BAND_EDGES), BAND_LABELS[-1])


def ordinal(n: int) -> str:
    """1 -> "1st", 2 -> "2nd", 11 -> "11th", 22 -> "22nd", 111 -> "111th"."""
    n = int(n)
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def pct_label(pct: float) -> str:
    """Percentile as an ordinal, rounded half-up and clamped to 1st-99th."""
    return ordinal(min(99, max(1, math.floor(pct + 0.5))))


def fmt_number(value: Any, fmt: str) -> str:
    """Format for display. fmt: "int" (1,234), "dec1" (12.3), "dec2" (0.45), "pct" (41.3%)."""
    v = to_float(value)
    if v is None:
        return MISSING
    if fmt == "int":
        return f"{v:,.0f}"
    if fmt == "dec1":
        return f"{v:,.1f}"
    if fmt == "dec2":
        return f"{v:,.2f}"
    if fmt == "pct":
        return f"{v:.1f}%"
    raise ValueError(f"unknown number format: {fmt!r}")


def per90(total: Any, nineties: Any) -> float | None:
    t, n = to_float(total), to_float(nineties)
    if t is None or n is None or n <= 0:
        return None
    return t / n


def short_season(season: str) -> str:
    """"2022-2023" -> "2022-23"; anything else is returned unchanged."""
    m = _SEASON.match(str(season))
    return f"{m.group(1)}-{m.group(2)[2:]}" if m else str(season)


def initials(name: str) -> str:
    words = str(name).split()
    if not words:
        return "?"
    if len(words) == 1:
        return words[0][0].upper()
    return (words[0][0] + words[-1][0]).upper()


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")
