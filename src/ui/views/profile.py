"""Profile tab: percentile bars by category for the position's curated metrics."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.similarity import MIN_MINUTES
from src.ui import build, metrics, state, stats, theme, viz

BAND_NOTE = ("Stats where lower is better (errors, cards, fouls committed, dribbled past, goals against) "
             "are inverted, "
             "so a longer bar is always better. Counting stats are ranked per 90 minutes.")
NA_NOTE = "n/a: the stat is missing for this player, so it can't be ranked."
BIG_FIVE_TITLE = "Big five only"
BIG_FIVE_NOTE = ("These stats come from Understat and exist only for the Premier League, La Liga, "
                 "Serie A, Bundesliga and Ligue 1, so they are ranked against big-five players only.")


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
    family = metrics.family_of(player_row.get("Source"))
    if family != metrics.FBREF and "Source" in pool.columns:
        # a newer-season pool holds one family; never rank against another one
        pool = pool[pool["Source"].map(metrics.family_of) == family]
    league = player_row.get("League")
    st.markdown(theme.section_label_html("Compare with"), unsafe_allow_html=True)
    scope = st.selectbox("Compare with", [SAME_LEAGUE, ALL_LEAGUES], key=state.COMPARE,
                         label_visibility="collapsed")
    in_league = scope == SAME_LEAGUE and not stats.is_missing(league) and "League" in pool.columns
    ranked = build.same_league_pool(pool, league) if in_league else pool
    where = f"{league} {short}" if in_league else f"all leagues in {short}"
    st.caption(f"Ranked against {len(ranked):,} {_plural(plural, len(ranked))} in {where} "
               f"with {MIN_MINUTES}+ minutes")

    if family == metrics.FBREF:  # today's FBref profile
        by_category = build.profile_rows(ranked, player_row, group, min_coverage=build.MIN_COVERAGE)
    else:  # the family's fixed set: every bar for every player, n/a when missing
        by_category = build.profile_rows(ranked, player_row, group,
                                         layout=metrics.profile_layout(family, group),
                                         keep_missing=True)
    if not by_category:
        st.markdown(theme.empty_state_html(
            "Not enough data to rank",
            f"Too few {plural} in this pool have the stats for {player}'s profile."
            + ("" if scope == ALL_LEAGUES else " Try comparing with all leagues in this season.")),
            unsafe_allow_html=True)
        return
    cards = [(category, rows, "") for category, rows in by_category.items()]
    if group != "GK" and metrics.is_big_five(player_row.get("Source"), league):
        cards.append(_big_five_card(pool, ranked, player_row, group, in_league))
    # cards dealt into two columns by height, so the columns end level instead
    # of each pair of cards being as tall as the taller one
    weights = [len(rows) + 2 + (1 if note else 0) for _, rows, note in cards]
    for col, indexes in zip(st.columns(2, gap="medium"), build.balance_columns(weights)):
        with col:
            for category, rows, note in (cards[i] for i in indexes):
                with st.container(border=True, key=f"card_profile_{stats.slug(category)}"):
                    st.markdown(theme.card_title_html(category), unsafe_allow_html=True)
                    if note:
                        st.caption(note)
                    bars = [(r.label, r.pct, r.text) for r in rows]
                    st.plotly_chart(viz.percentile_bars(bars), config=viz.PLOTLY_CONFIG, theme=None,
                                    width="stretch", key=f"pct_{stats.slug(category)}")
    note = BAND_NOTE if family == metrics.FBREF else f"{BAND_NOTE} {NA_NOTE}"
    st.markdown(theme.band_key_html(note), unsafe_allow_html=True)


def _big_five_card(pool: pd.DataFrame, ranked: pd.DataFrame, player_row, group: str,
                   in_league: bool) -> tuple[str, list, str]:
    """The Understat extras, ranked within big-five players of the same position
    and season (or the player's league when "Same league" is chosen)."""
    five = ranked if in_league else pool[pool["League"].isin(metrics.BIG_FIVE)]
    rows = build.profile_rows(five, player_row, group,
                              layout={BIG_FIVE_TITLE: metrics.BIG_FIVE_EXTRAS}, keep_missing=True)
    return BIG_FIVE_TITLE, rows[BIG_FIVE_TITLE], BIG_FIVE_NOTE
