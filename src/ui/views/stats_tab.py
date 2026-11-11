"""Stats tab: every stat by category, the season total next to per 90."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.ui import build, metrics, stats, theme

# The grid can't format per row and prints a missing number as "None", so the
# numbers are turned into right-aligned text here: Total as 1,234 / 12.3 / 41.3
# (already rounded per metric kind in build.stat_table), Per 90 with two
# decimals, and a blank where there is no value.
COLUMN_CONFIG = {
    "Stat": st.column_config.TextColumn("Stat", width="medium"),
    "Total": st.column_config.TextColumn(
        "Total", alignment="right",
        help="Season total. For % and per-shot stats this is the season rate itself."),
    "Per 90": st.column_config.TextColumn("Per 90", alignment="right", help="Per 90 minutes played."),
}


def _text(value: float, per90: bool) -> str:
    if pd.isna(value):
        return ""
    if per90:
        return f"{value:.2f}"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,}"


def display_table(table: pd.DataFrame) -> pd.DataFrame:
    """The Stats-tab table as text columns, blank where a value is missing.
    A row with no value at all (a family stat this player lacks) reads n/a."""
    out = table.copy()
    out["Total"] = [_text(v, False) for v in table["Total"]]
    out["Per 90"] = [_text(v, True) for v in table["Per 90"]]
    none = table["Total"].isna() & table["Per 90"].isna()
    out.loc[none, "Total"] = build.NOT_AVAILABLE
    return out


NOTE = ("Per 90 = per 90 minutes played. Percentages and per-shot stats are season rates, "
        "so they have no per-90 value.")


BIG_FIVE_TITLE = "Big five only"
BIG_FIVE_NOTE = ("From Understat, which covers only the Premier League, La Liga, Serie A, "
                 "Bundesliga and Ligue 1.")


def _tables(row, group: str) -> list[tuple[str, pd.DataFrame, str]]:
    """(card title, table, note) per card. FBref seasons: every stat by category,
    as before. Newer seasons: the family's fixed set (patchy stats such as
    offsides never appear), plus the Understat extras for big-five players."""
    family = metrics.family_of(row.get("Source"))
    if family == metrics.FBREF:
        tables = [(category, build.stat_table(row, category), "")
                  for category in metrics.stats_categories(group)]
        return [t for t in tables if not t[1].empty]
    tables = [(category, build.stat_table(row, category, keys=keys, keep_missing=True), "")
              for category, keys in metrics.stats_layout(family, group).items()]
    if group != "GK" and metrics.is_big_five(row.get("Source"), row.get("League")):
        extras = build.stat_table(row, BIG_FIVE_TITLE, keep_missing=True,
                                  keys=metrics.stats_keys(metrics.BIG_FIVE_EXTRAS))
        tables.append((BIG_FIVE_TITLE, extras, BIG_FIVE_NOTE))
    return tables


def render(row, group: str) -> None:
    tables = _tables(row, group)
    if not tables:
        st.markdown(theme.empty_state_html("No stats recorded",
                                           "This player-season has no stat columns filled in."),
                    unsafe_allow_html=True)
        return
    for start in range(0, len(tables), 2):
        cols = st.columns(2, gap="medium")
        for col, (category, table, note) in zip(cols, tables[start:start + 2]):
            with col, st.container(border=True, key=f"card_stats_{stats.slug(category)}"):
                st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                if note:
                    st.caption(note)
                st.dataframe(display_table(table), hide_index=True, width="stretch", height="content",
                             row_height=34, column_config=COLUMN_CONFIG)
    st.caption(NOTE)
