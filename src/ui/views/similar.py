"""Similar players tab: similarity table, a head-to-head dumbbell for the
selected row, and a button that opens that player's profile."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, data, metrics, state, stats, theme, viz

METHOD = ("Method: cosine similarity on z-scored stats within the position pool "
          f"(same position and season, {MIN_MINUTES}+ minutes).")

COLUMN_CONFIG = {
    "Player": st.column_config.TextColumn("Player", width="medium"),
    "Club": st.column_config.TextColumn("Club"),
    "League": st.column_config.TextColumn("League"),
    "Season": st.column_config.TextColumn("Season"),
    "Position": st.column_config.TextColumn("Position"),
    "Minutes": st.column_config.NumberColumn("Minutes", format="localized"),
    "Similarity": st.column_config.ProgressColumn(
        "Similarity", min_value=0.0, max_value=1.0, format="%.2f", color=theme.RULE, width=120,
        help="1.00 = an identical statistical profile"),
}
# Position is the same for every row (the pool is one position), so leave it
# out to give the other columns room.
COLUMN_ORDER = ["Player", "Club", "League", "Season", "Minutes", "Similarity"]


def render(bq, pool: pd.DataFrame | None, row) -> None:
    player, season, group = str(row["Player"]), str(row["Season"]), str(row["position_group"])
    st.caption(METHOD)
    minutes = stats.to_float(row.get("minutes")) or 0.0
    if minutes < MIN_MINUTES:
        st.markdown(theme.empty_state_html(
            f"{player} is below the {MIN_MINUTES}-minute threshold",
            f"Similarity search needs at least {MIN_MINUTES} minutes in a season; "
            f"{player} played {minutes:,.0f}."),
            unsafe_allow_html=True)
        return

    top_n = st.slider("Players to show", min_value=5, max_value=data.SIMILAR_MAX, value=10, key="top_n")
    try:
        matches = data.similar_players(bq, player, season, group)
    except Exception as exc:  # noqa: BLE001 - BigQuery, auth and "not in pool" all end here
        theme.error_box(st, "Couldn't compute similar players.", exc)
        return
    if matches.empty:
        plural = metrics.POSITION_PLURALS.get(group, "players")
        st.markdown(theme.empty_state_html(
            "No similar players",
            f"No other {plural} reached {MIN_MINUTES} minutes in {stats.short_season(season)}."),
            unsafe_allow_html=True)
        return

    table = build.similar_table(matches.head(top_n))
    # A keyed dataframe keeps its selection when the data changes, so drop it
    # whenever the table now belongs to a different player-season.
    if st.session_state.get(state.SIMILAR_FOR) != (player, season):
        st.session_state.pop(state.SIMILAR_TABLE, None)
        st.session_state[state.SIMILAR_FOR] = (player, season)
    shown = table.assign(Season=table["Season"].map(stats.short_season))  # "2023-24", like the rest of the app
    event = st.dataframe(shown, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key=state.SIMILAR_TABLE,
                         column_config=COLUMN_CONFIG, column_order=COLUMN_ORDER)
    rows = list(event.selection.rows) if event is not None else []
    picked = [i for i in rows if i < len(table)]
    if not picked:
        st.caption(f"Select a row to compare that player with {player}.")
        return
    other = table.iloc[picked[0]]
    _compare(pool, row, other, group)
    st.button(f"View {other['Player']}'s profile", key="view_profile", type="primary",
              on_click=state.goto, args=(str(other["Player"]), str(other["Season"])))


def _compare(pool: pd.DataFrame | None, row, other: pd.Series, group: str) -> None:
    a_name, b_name = str(row["Player"]), str(other["Player"])
    if pool is None or pool.empty:
        st.caption("Comparison unavailable: the position pool didn't load.")
        return
    a = pool[(pool["Player"] == a_name) & (pool["Season"] == row["Season"])]
    b = pool[(pool["Player"] == b_name) & (pool["Season"] == other["Season"])]
    if a.empty or b.empty:
        st.caption("Comparison unavailable: one of the players isn't in the position pool.")
        return
    labels, a_vals, b_vals = build.compare_lists(build.profile_rows(pool, a.iloc[0], group),
                                                 build.profile_rows(pool, b.iloc[0], group))
    with st.container(border=True, key="card_compare"):
        st.markdown(theme.card_title_html(f"{a_name} vs {b_name} · percentiles"), unsafe_allow_html=True)
        st.plotly_chart(viz.compare_dumbbell(labels, a_vals, b_vals, a_name, b_name),
                        config=viz.PLOTLY_CONFIG, theme=None, width="stretch", key="compare_chart")
