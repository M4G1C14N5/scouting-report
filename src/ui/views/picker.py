"""Sidebar filters, the player search, onboarding and the season switcher.

Fixes the old app's stateless season buttons (every rerun snapped back to
the oldest season) and its selectbox that auto-picked the first player.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.ui import filters, state, stats, theme

EXAMPLE_PLAYERS = ("Mohamed Salah", "Kevin De Bruyne", "Erling Haaland", "Virgil van Dijk")
SEARCH_PLACEHOLDER = "Search a player, e.g. Salah"
STEPS = (
    ("Set the scope", "Pick a season, league or club in the sidebar to narrow the list."),
    ("Find a player", "Type a name in the search box above."),
    ("Read the profile", "Percentiles against his position, every stat, and his most similar players."),
)


def _init(key: str, value: object) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def _season_label(value: str) -> str:
    return value if value == state.ALL_SEASONS else stats.short_season(value)


def sidebar(index: pd.DataFrame) -> pd.DataFrame:
    seasons = filters.seasons_desc(index)
    _init(state.SEASON, seasons[0] if seasons else state.ALL_SEASONS)
    _init(state.LEAGUE, state.ALL_LEAGUES)
    _init(state.CLUB, state.ALL_CLUBS)
    with st.sidebar:
        st.markdown(theme.brand_html(), unsafe_allow_html=True)

        st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
        season = st.pills("Season", [state.ALL_SEASONS, *seasons], key=state.SEASON,
                          required=True, format_func=_season_label, label_visibility="collapsed")

        st.markdown(theme.section_label_html("League"), unsafe_allow_html=True)
        league = st.selectbox("League", [state.ALL_LEAGUES, *filters.leagues(index)],
                              key=state.LEAGUE, label_visibility="collapsed")
        league_filter = None if league == state.ALL_LEAGUES else league

        clubs = [state.ALL_CLUBS, *filters.clubs(index, league_filter)]
        if st.session_state.get(state.CLUB) not in clubs:  # league changed under the club
            st.session_state[state.CLUB] = state.ALL_CLUBS
        st.markdown(theme.section_label_html("Club"), unsafe_allow_html=True)
        club = st.selectbox("Club", clubs, key=state.CLUB, label_visibility="collapsed")

        filtered = filters.filter_index(
            index,
            season=None if season == state.ALL_SEASONS else season,
            league=league_filter,
            club=None if club == state.ALL_CLUBS else club,
        )
        st.caption(f"Showing {filtered['Player'].nunique():,} players")
    return filtered


def search(filtered: pd.DataFrame) -> str | None:
    options = filters.search_options(filtered)
    labels = {player: filters.option_label(player, club, league)
              for player, club, league in zip(options["Player"], options["Squad"], options["League"])}
    if st.session_state.get(state.PLAYER) not in labels:  # filters moved the player out of view
        st.session_state[state.PLAYER] = None
    return st.selectbox("Search player", list(labels), index=None, key=state.PLAYER,
                        format_func=labels.__getitem__, placeholder=SEARCH_PLACEHOLDER,
                        label_visibility="collapsed")


def _choose(player: str) -> None:
    st.session_state[state.PLAYER] = player


def onboarding(filtered: pd.DataFrame) -> None:
    st.markdown(theme.steps_html(STEPS), unsafe_allow_html=True)
    example = filters.pick_example(filtered, EXAMPLE_PLAYERS)
    if example:
        st.button(f"Try an example: {example}", key="try_example", type="primary",
                  on_click=_choose, args=(example,))


def season_switch(filtered: pd.DataFrame, player: str) -> str | None:
    """The season to show. Defaults to the player's latest season in view and
    stays put across reruns, until a different player is picked."""
    seasons = filters.player_seasons(filtered, player)
    if not seasons:
        return None
    if (st.session_state.get(state.SWITCH_FOR) != player
            or st.session_state.get(state.SWITCH) not in seasons):
        st.session_state[state.SWITCH] = seasons[-1]
        st.session_state[state.SWITCH_FOR] = player
    if len(seasons) == 1:
        return seasons[0]
    st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
    return st.segmented_control("Season", seasons, key=state.SWITCH, required=True,
                                format_func=stats.short_season, label_visibility="collapsed")
