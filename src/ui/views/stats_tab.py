"""Stats tab: every stat by category, the season total next to per 90."""

from __future__ import annotations

import streamlit as st

from src.ui import build, metrics, stats, theme

# One format per column: the grid can't format per row. Total is
# "localized", so 1,234 / 12.3 / 41.3 read naturally; values are rounded per
# metric kind in build.stat_table. Per 90 is always two decimals.
COLUMN_CONFIG = {
    "Stat": st.column_config.TextColumn("Stat", width="medium"),
    "Total": st.column_config.NumberColumn(
        "Total", format="localized",
        help="Season total. For % and per-shot stats this is the season rate itself."),
    "Per 90": st.column_config.NumberColumn("Per 90", format="%.2f", help="Per 90 minutes played."),
}
NOTE = ("Per 90 = per 90 minutes played. Percentages and per-shot stats are season rates, "
        "so they have no per-90 value.")


def render(row, group: str) -> None:
    tables = [(category, build.stat_table(row, category)) for category in metrics.stats_categories(group)]
    tables = [(category, table) for category, table in tables if not table.empty]
    if not tables:
        st.markdown(theme.empty_state_html("No stats recorded",
                                           "This player-season has no stat columns filled in."),
                    unsafe_allow_html=True)
        return
    for start in range(0, len(tables), 2):
        cols = st.columns(2, gap="medium")
        for col, (category, table) in zip(cols, tables[start:start + 2]):
            with col, st.container(border=True, key=f"card_stats_{stats.slug(category)}"):
                st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                st.dataframe(table, hide_index=True, width="stretch", height="content",
                             row_height=34, column_config=COLUMN_CONFIG)
    st.caption(NOTE)
