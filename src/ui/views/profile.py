"""Profile tab: percentile bars by category for the position's curated metrics."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, metrics, state, stats, theme, viz

BAND_NOTE = ("Stats where lower is better (errors, cards, goals against) are inverted, "
             "so a longer bar is always better. Counting stats are ranked per 90 minutes.")


SAME_LEAGUE = "Same league"
ALL_LEAGUES = "All leagues in this season"


def _plural(plural: str, n: int) -> str:
    return plural[:-1] if n == 1 else plural


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

    player_row = me.iloc[0]
    league = player_row.get("League")
    st.markdown(theme.section_label_html("Compare with"), unsafe_allow_html=True)
    scope = st.segmented_control("Compare with", [SAME_LEAGUE, ALL_LEAGUES], key=state.COMPARE,
                                 default=SAME_LEAGUE, required=True, label_visibility="collapsed")
    in_league = scope == SAME_LEAGUE and not stats.is_missing(league) and "League" in pool.columns
    ranked = build.same_league_pool(pool, league) if in_league else pool
    where = f"{league} {short}" if in_league else f"all leagues in {short}"
    st.caption(f"Ranked against {len(ranked):,} {_plural(plural, len(ranked))} in {where} "
               f"with {MIN_MINUTES}+ minutes")

    by_category = build.profile_rows(ranked, player_row, group, min_coverage=build.MIN_COVERAGE)
    if not by_category:
        st.markdown(theme.empty_state_html(
            "Not enough data to rank",
            f"Too few {plural} in this pool have the stats for {player}'s profile."
            + ("" if scope == ALL_LEAGUES else " Try comparing with all leagues in this season.")),
            unsafe_allow_html=True)
        return
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
