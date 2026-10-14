"""Session-state keys shared by the sidebar, the search box and the Similar tab."""

from __future__ import annotations

import streamlit as st

ALL_SEASONS = "All"
ALL_LEAGUES = "All leagues"
ALL_CLUBS = "All clubs"

SEASON = "f_season"          # sidebar season pills
LEAGUE = "f_league"          # sidebar league selectbox
CLUB = "f_club"              # sidebar club selectbox
PLAYER = "player_pick"       # player search selectbox
SWITCH = "season_switch"     # per-player season switcher
SWITCH_FOR = "_switch_for"   # which player SWITCH was last set for
SIMILAR_TABLE = "similar_table"


def goto(player: str, season: str) -> None:
    """on_click callback: open `player` in `season`.

    Filters are widened to that season with all leagues and clubs, so the
    player is always in the search options when the page reruns."""
    st.session_state[SEASON] = season
    st.session_state[LEAGUE] = ALL_LEAGUES
    st.session_state[CLUB] = ALL_CLUBS
    st.session_state[PLAYER] = player
    st.session_state[SWITCH] = season
    st.session_state[SWITCH_FOR] = player
