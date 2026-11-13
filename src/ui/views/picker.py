"""Sidebar filters, the player search, onboarding and the season switcher.

Fixes the old app's stateless season buttons (every rerun snapped back to
the oldest season) and its selectbox that auto-picked the first player.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
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
    return value if value == state.ALL_SEASONS else stats.season_option(value)


def sidebar(index: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    """(scoped, season): the index narrowed by league and club only, every season
    kept so the search never hides a player, and the sidebar season (None for All),
    which picks the season a player opens in."""
    seasons = filters.seasons_desc(index)
    _init(state.SEASON, filters.default_season(seasons) or state.ALL_SEASONS)
    _init(state.LEAGUE, state.ALL_LEAGUES)
    _init(state.CLUB, state.ALL_CLUBS)
    with st.sidebar:
        st.markdown(theme.brand_html(), unsafe_allow_html=True)

        st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
        season = st.selectbox("Season", [state.ALL_SEASONS, *seasons], key=state.SEASON,
                              format_func=_season_label, label_visibility="collapsed")

        st.markdown(theme.section_label_html("League"), unsafe_allow_html=True)
        league = st.selectbox("League", [state.ALL_LEAGUES, *filters.leagues(index)],
                              key=state.LEAGUE, label_visibility="collapsed")
        league_filter = None if league == state.ALL_LEAGUES else league

        clubs = [state.ALL_CLUBS, *filters.clubs(index, league_filter)]
        if st.session_state.get(state.CLUB) not in clubs:  # league changed under the club
            st.session_state[state.CLUB] = state.ALL_CLUBS
        st.markdown(theme.section_label_html("Club"), unsafe_allow_html=True)
        club = st.selectbox("Club", clubs, key=state.CLUB, label_visibility="collapsed")

        scoped = filters.filter_index(
            index, league=league_filter, club=None if club == state.ALL_CLUBS else club)
        chosen = None if season == state.ALL_SEASONS else season
        in_season = scoped if chosen is None else scoped[scoped["Season"] == chosen]
        where = "" if chosen is None else f" in {stats.season_option(chosen)}"
        st.caption(f"{in_season['Player'].nunique():,} players{where}. "
                   "The search covers every season.")
    return scoped, chosen


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


def season_switch(scoped: pd.DataFrame, player: str, preferred: str | None = None) -> str | None:
    """The season to show. A newly picked player opens in the sidebar season when
    he has MIN_MINUTES there, else in his most recent season that has them
    (filters.preferred_season); it then stays put across reruns until another
    player or sidebar season is picked. The switcher lists every season he has
    in the league and club filters, newest first."""
    seasons = filters.player_seasons(scoped, player)
    if not seasons:
        return None
    rows = scoped[scoped["Player"] == player]
    minutes = rows.groupby("Season")["minutes"].max().to_dict()
    if (st.session_state.get(state.SWITCH_FOR) != player
            or st.session_state.get(state.SWITCH_PREF) != preferred
            or st.session_state.get(state.SWITCH) not in seasons):
        st.session_state[state.SWITCH] = filters.preferred_season(minutes, preferred, MIN_MINUTES)
        st.session_state[state.SWITCH_FOR] = player
        st.session_state[state.SWITCH_PREF] = preferred
    if len(seasons) == 1:
        return seasons[0]
    st.markdown(theme.section_label_html("Season"), unsafe_allow_html=True)
    return st.selectbox("Season", seasons[::-1], key=state.SWITCH,
                        format_func=stats.season_option, label_visibility="collapsed")
