"""Profile tab: percentile bars by category for the position's curated metrics."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, metrics, stats, theme, viz

BAND_NOTE = ("Stats where lower is better (errors, cards, goals against) are inverted, "
             "so a longer bar is always better. Counting stats are ranked per 90 minutes.")


def render(pool: pd.DataFrame | None, player: str, season: str, group: str) -> None:
    plural = metrics.POSITION_PLURALS.get(group, "players")
    short = stats.short_season(season)
    if pool is None or pool.empty:
        st.markdown(theme.empty_state_html(
            "No comparison pool",
            f"No {plural} reached {MIN_MINUTES} minutes in {short}, so there is nothing to rank against."),
            unsafe_allow_html=True)
        return
    me = pool[(pool["Player"] == player) & (pool["Season"] == season)]
    if me.empty:
        st.markdown(theme.empty_state_html(
            f"{player} isn't in the percentile pool",
            f"Percentiles compare {plural} with {MIN_MINUTES}+ minutes in {short}. "
            f"{player} played fewer minutes than that."),
            unsafe_allow_html=True)
        return

    by_category = build.profile_rows(pool, me.iloc[0], group)
    st.caption(f"vs {len(pool):,} {plural} in {short}, {MIN_MINUTES}+ minutes")
    cards = list(by_category.items())
    for start in range(0, len(cards), 2):
        cols = st.columns(2, gap="medium")
        for col, (category, rows) in zip(cols, cards[start:start + 2]):
            with col, st.container(border=True, key=f"card_profile_{stats.slug(category)}"):
                st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                bars = [(r.label, r.pct, r.text) for r in rows]
                st.plotly_chart(viz.percentile_bars(bars), config=viz.PLOTLY_CONFIG, theme=None,
                                width="stretch", key=f"pct_{stats.slug(category)}")
    st.markdown(theme.band_key_html(BAND_NOTE), unsafe_allow_html=True)
