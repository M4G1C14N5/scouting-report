"""
app.py
======
Scouting Hub - Streamlit entry point.

Thin shell: page config, theme, hero, sidebar filters, player search and
the three tabs. The rest lives in src/ui/ (metric config in
src/ui/metrics.py, cached BigQuery queries in src/ui/data.py).

Run locally:
    .venv/Scripts/python -m streamlit run app.py
"""

from __future__ import annotations

import streamlit as st

from src.ui import data, theme
from src.ui.views import header, picker, profile, similar, stats_tab

st.set_page_config(page_title="Scouting Hub", page_icon="⚽", layout="wide")
theme.inject(st)

HERO_TITLE = "Scouting"
HERO_HIGHLIGHT = "Hub"
HERO_SUB = "Big 5 leagues · 2017-18 to 2023-24 · 16.5k player-seasons"
HERO_META = "FBref data, frozen snapshot · BigQuery"
FOOTER_PARTS = (
    ("Data", "FBref snapshot, Big 5 leagues, 2017-18 to 2023-24"),
    ("Pipeline", "FBref → GCS → BigQuery"),
    ("Built by", "Thomas Tsangou and Ajaiah Darlington"),
)


def main() -> None:
    try:
        bq = data.get_bq_client()
        index = data.load_player_index(bq)
    except Exception as exc:  # noqa: BLE001 - any auth/BigQuery failure gets the same message
        theme.error_box(st, "Couldn't load the player list from BigQuery. "
                            "Check the Google Cloud credentials and try again.", exc)
        return

    filtered = picker.sidebar(index)
    if filtered.empty:
        st.markdown(theme.empty_state_html(
            "No players match these filters",
            "Try another season, or set League and Club back to All in the sidebar."),
            unsafe_allow_html=True)
        return

    player = picker.search(filtered)
    if player is None:
        picker.onboarding(filtered)
        return

    season = picker.season_switch(filtered, player)
    try:
        row = data.player_stats(bq, player, season)
    except Exception as exc:  # noqa: BLE001
        theme.error_box(st, f"Couldn't load stats for {player} ({season}).", exc)
        return
    group = row["position_group"]
    header.render(row)

    pool, pool_error = None, None
    try:
        pool = data.position_pool(bq, group, season)  # fetched once, shared by two tabs
    except Exception as exc:  # noqa: BLE001
        pool_error = exc

    tab_profile, tab_stats, tab_similar = st.tabs(["Profile", "Stats", "Similar players"])
    with tab_profile:
        if pool_error is not None:
            theme.error_box(st, "Couldn't load the position pool for percentiles.", pool_error)
        else:
            profile.render(pool, player, season, group)
    with tab_stats:
        stats_tab.render(row, group)
    with tab_similar:
        similar.render(bq, pool, row)


st.markdown(theme.hero_html(HERO_TITLE, HERO_HIGHLIGHT, HERO_SUB, HERO_META), unsafe_allow_html=True)
main()
st.markdown(theme.footer_html(FOOTER_PARTS), unsafe_allow_html=True)
