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

from src.similarity import OUTFIELD
from src.ui import data, stats, theme
from src.ui.views import header, picker, profile, similar, stats_tab

st.set_page_config(page_title="Scouting Hub", page_icon="⚽", layout="wide")
theme.inject(st)

HERO_TITLE = "Scouting"
HERO_HIGHLIGHT = "Hub"
HERO_SUB = "19 leagues · FBref 2017/18 to 2023/24 · 2024 to 2026 from API-Football, Understat and ASA"
HERO_META = "BigQuery"
FOOTER_PARTS = (
    ("Data", "FBref (Big 5, 2017/18 to 2023/24), API-Football, Understat (xG, Big 5), "
             "American Soccer Analysis (US leagues)"),
    ("Pipeline", "FBref → GCS → BigQuery; API-Football, Understat and ASA → soccer-data → BigQuery"),
    ("Built by", "Thomas Tsangou and Ajaiah Darlington"),
)


def _photo(bq, row) -> str | None:
    """The row's photo, else a newer season's (FBref seasons have none). A
    failed lookup just means initials, never an error on the page."""
    own = header.photo_of(row)
    if own is not None:
        return own
    born = stats.to_float(row.get("Born"))
    try:
        return data.player_photo(bq, str(row["Player"]), born)
    except Exception:  # noqa: BLE001 - a photo is decoration
        return None


def main() -> None:
    try:
        bq = data.get_bq_client()
        index = data.load_player_index(bq)
    except Exception as exc:  # noqa: BLE001 - any auth/BigQuery failure gets the same message
        theme.error_box(st, "Couldn't load the player list from BigQuery. "
                            "Check the Google Cloud credentials and try again.", exc)
        return

    scoped, chosen = picker.sidebar(index)
    if scoped.empty:
        st.markdown(theme.empty_state_html(
            "No players match these filters",
            "Set League and Club back to All in the sidebar."),
            unsafe_allow_html=True)
        return

    player = picker.search(scoped)
    if player is None:
        in_season = scoped if chosen is None else scoped[scoped["Season"] == chosen]
        picker.onboarding(in_season if not in_season.empty else scoped)
        return

    season = picker.season_switch(scoped, player, chosen)
    try:
        row = data.player_stats(bq, player, season)
    except Exception as exc:  # noqa: BLE001
        theme.error_box(st, f"Couldn't load stats for {player} ({season}).", exc)
        return
    group = row["position_group"]
    header.render(row, photo_url=_photo(bq, row))

    pool, pool_error = None, None
    try:
        # every outfield position, so the Profile tab can rank him against any of them;
        # fetched once, shared by two tabs
        pool = data.position_pool(bq, group if group == "GK" else OUTFIELD, season)
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
