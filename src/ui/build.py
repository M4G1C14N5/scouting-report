"""Display rows for the views, driven by metrics.py. Pure: no Streamlit."""

from __future__ import annotations

from typing import Any, Mapping, NamedTuple

import pandas as pd

from src.ui import metrics, stats

STAT_TABLE_COLUMNS = ("Stat", "Total", "Per 90")
SIMILAR_COLUMNS = ("Player", "Club", "League", "Season", "Position", "Minutes", "Similarity")


class PctRow(NamedTuple):
    key: str
    label: str
    pct: float
    value: float  # raw value, or the per-90 rate when the row is ranked per 90
    text: str = ""  # display text for `value` (two decimals for a per-90 rate)


def fmt_value(key: str, value: Any) -> str:
    return stats.fmt_number(value, metrics.fmt_of(key))


def profile_rows(pool: pd.DataFrame, player_row: Mapping[str, Any],
                 group: str) -> dict[str, list[PctRow]]:
    """Percentile rows per category for the group's curated metrics.

    Counting stats (the kinds metrics.PER90_COMPUTED_KINDS) are ranked per 90
    minutes: a raw season total mostly measures minutes played. Pool rows
    without 90s played drop out of those rankings. Stored per-90 columns,
    tallies, rates and ratios are ranked as they are.

    Metrics missing from the pool or blank for the player are skipped, and
    so are categories left empty."""
    out: dict[str, list[PctRow]] = {}
    for category, keys in metrics.PROFILE_METRICS.get(group, {}).items():
        rows: list[PctRow] = []
        for key in keys:
            if key not in pool.columns:
                continue
            higher_is_better = key not in metrics.LOWER_IS_BETTER
            if metrics.KIND.get(key) in metrics.PER90_COMPUTED_KINDS:
                if "nineties_played" not in pool.columns:
                    continue
                value = stats.per90(player_row.get(key), player_row.get("nineties_played"))
                if value is None:
                    continue
                nineties = pd.to_numeric(pool["nineties_played"], errors="coerce")
                ranked = pd.to_numeric(pool[key], errors="coerce") / nineties.where(nineties > 0)
                label = metrics.label(key)
                if not label.endswith("/ 90"):
                    label = f"{label} / 90"
                text = stats.fmt_number(value, "dec2")
            else:
                value = stats.to_float(player_row.get(key))
                if value is None:
                    continue
                ranked, label, text = pool[key], metrics.label(key), fmt_value(key, value)
            pct = stats.percentile_of(ranked, value, higher_is_better=higher_is_better)
            if pct is None:
                continue
            rows.append(PctRow(key, label, pct, value, text))
        if rows:
            out[category] = rows
    return out


def compare_lists(a_rows: Mapping[str, list[PctRow]],
                  b_rows: Mapping[str, list[PctRow]]) -> tuple[list[str], list[float], list[float]]:
    """Labels and both players' percentiles for the metrics both have, in A's order."""
    b_lookup = {row.key: row for rows in b_rows.values() for row in rows}
    labels: list[str] = []
    a_vals: list[float] = []
    b_vals: list[float] = []
    for rows in a_rows.values():
        for row in rows:
            other = b_lookup.get(row.key)
            if other is None:
                continue
            labels.append(row.label)
            a_vals.append(row.pct)
            b_vals.append(other.pct)
    return labels, a_vals, b_vals


def stat_table(row: Mapping[str, Any], category: str) -> pd.DataFrame:
    """Stat / Total / Per 90 rows for one Stats-tab card.

    A stored per-90 column is shown beside its total rather than on its own
    row. Counting stats without one get a computed per 90. Rates and tallies
    show their value under Total and leave Per 90 blank."""
    paired = set(metrics.PER90_PAIRS.values())
    nineties = stats.to_float(row.get("nineties_played"))
    records = []
    for key in metrics.keys_in_category(category):
        kind = metrics.KIND[key]
        if kind == "per90" and key in paired:
            continue
        if kind == "per90":
            total, p90 = None, stats.to_float(row.get(key))
        else:
            total = stats.to_float(row.get(key))
            p90 = stats.to_float(row.get(metrics.PER90_PAIRS[key])) if key in metrics.PER90_PAIRS else None
            if p90 is None and kind in metrics.PER90_COMPUTED_KINDS:
                p90 = stats.per90(total, nineties)  # no stored per-90 value: compute it
        if total is None and p90 is None:
            continue
        decimals = metrics.TOTAL_DECIMALS[metrics.KIND_FORMAT[kind]]
        records.append({
            "Stat": metrics.label(key),
            "Total": None if total is None else round(total, decimals),
            "Per 90": None if p90 is None else round(p90, 2),
        })
    table = pd.DataFrame(records, columns=list(STAT_TABLE_COLUMNS))
    return table.astype({"Total": "float64", "Per 90": "float64"})


def kpi_tiles(row: Mapping[str, Any], group: str) -> list[tuple[str, str, str]]:
    """(label, value text, help text) for the identity card's KPI row."""
    specs = metrics.KPI_TILES["GK" if group == "GK" else "outfield"]
    tiles = []
    for spec in specs:
        if spec.per90:
            value = stats.per90(row.get(spec.key), row.get("nineties_played"))
            text = stats.fmt_number(value, "dec2")
            help_text = f"{metrics.label(spec.key)} divided by 90s played."
        else:
            text = fmt_value(spec.key, row.get(spec.key))
            help_text = metrics.GLOSSARY.get(spec.key, "")
        tiles.append((spec.label, text, help_text))
    return tiles


def similar_table(similar: pd.DataFrame) -> pd.DataFrame:
    """find_similar_players output with readable column names."""
    out = pd.DataFrame({
        "Player": similar["Player"].astype(str),
        "Club": similar["Squad"],
        "League": similar["League"],
        "Season": similar["Season"],
        "Position": similar["position_group"].map(lambda g: metrics.POSITION_NAMES.get(g, g)),
        "Minutes": pd.to_numeric(similar["minutes"], errors="coerce"),
        "Similarity": pd.to_numeric(similar["similarity_score"], errors="coerce").round(3),
    })
    return out.reset_index(drop=True)
