"""Profile tab: percentile bars by category for the position's curated metrics.

Two choices sit above the bars: who to compare with (same league, the top 5
leagues, all leagues) and which position to compare against (his own by
default, or any other outfield position, to see how his numbers would rank
there). Newer seasons use the role groups from where players lined up
(main_role); FBref seasons and players without a role use the broad groups.
"""

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
TOP_FIVE = "Top 5 leagues"
ALL_LEAGUES = "All leagues in this season"

ROLE_ORDER = ("ST", "AMW", "CM", "FB", "CB")
ROLE_PLURALS = {"ST": "strikers", "AMW": "attacking mids and wingers",
                "CM": "central and defensive mids", "FB": "fullbacks and wing-backs",
                "CB": "centre-backs"}
ROLE_SINGULARS = {"strikers": "striker", "attacking mids and wingers": "attacking mid or winger",
                  "central and defensive mids": "central or defensive mid",
                  "fullbacks and wing-backs": "fullback or wing-back", "centre-backs": "centre-back"}
GROUP_ORDER = ("ATT", "MID", "DEF")


def _plural(plural: str, n: int) -> str:
    if n != 1:
        return plural
    return ROLE_SINGULARS.get(plural, plural[:-1])


def scope_options(family: str) -> list[str]:
    """Top 5 leagues only for the newer European seasons: FBref seasons hold only
    the big five already, and US players have no top-5 rows of their family."""
    if family == metrics.EUROPE:
        return [SAME_LEAGUE, TOP_FIVE, ALL_LEAGUES]
    return [SAME_LEAGUE, ALL_LEAGUES]


def position_choice(player_row, pool: pd.DataFrame, family: str, group: str):
    """(kind, options, default): kind "role" (main_role groups) for a newer-season
    outfielder with a role, else "group" (broad position groups)."""
    role = player_row.get("main_role")
    if (family != metrics.FBREF and "main_role" in pool.columns
            and not stats.is_missing(role) and str(role) in ROLE_ORDER):
        return "role", list(ROLE_ORDER), str(role)
    return "group", list(GROUP_ORDER), group


def position_plural(kind: str, key: str) -> str:
    return ROLE_PLURALS[key] if kind == "role" else metrics.POSITION_PLURALS.get(key, "players")


def position_label(kind: str, key: str, default: str) -> str:
    text = position_plural(kind, key).capitalize()
    return f"{text} (his position)" if key == default else text


def position_pool(pool: pd.DataFrame, kind: str, key: str) -> pd.DataFrame:
    if kind == "role":
        return pool[pool["main_role"] == key]
    return pool[pool["position_group"] == key]


def scope_pool(pool: pd.DataFrame, scope: str, league) -> pd.DataFrame:
    if scope == SAME_LEAGUE:
        return build.same_league_pool(pool, league)
    if scope == TOP_FIVE and "League" in pool.columns:
        return pool[pool["League"].isin(metrics.BIG_FIVE)]
    return pool


def _where(scope: str, league, short: str) -> str:
    if scope == SAME_LEAGUE and not stats.is_missing(league):
        return f"{league} {short}"
    if scope == TOP_FIVE:
        return f"the top 5 leagues in {short}"
    return f"all leagues in {short}"


def _select(label: str, options: list[str], key: str, default: str, for_key, format_func=str) -> str:
    """A selectbox that resets to `default` when `for_key` (the player-season)
    changes or its value is no longer offered."""
    marker = f"_{key}_for"
    if st.session_state.get(marker) != for_key or st.session_state.get(key) not in options:
        st.session_state[key] = default
        st.session_state[marker] = for_key
    st.markdown(theme.section_label_html(label), unsafe_allow_html=True)
    return st.selectbox(label, options, key=key, format_func=format_func, label_visibility="collapsed")


def render(pool: pd.DataFrame | None, player: str, season: str, group: str) -> None:
    """`pool`: the season's players with MIN_MINUTES+ (every outfield group for an
    outfielder, keepers for a keeper)."""
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
    for_key = (player, season)

    cols = st.columns(2, gap="medium")
    with cols[0]:
        scope = _select("Compare with", scope_options(family), state.COMPARE, SAME_LEAGUE, for_key)
    if group == "GK":
        kind, chosen = "group", "GK"
    else:
        kind, options, default = position_choice(player_row, pool, family, group)
        with cols[1]:
            chosen = _select("Compare against", options, state.PROFILE_POSITION, default, for_key,
                             format_func=lambda k: position_label(kind, k, default))
    as_position = position_pool(pool, kind, chosen)
    ranked = scope_pool(as_position, scope, league)
    who = position_plural(kind, chosen)
    st.caption(f"Ranked against {len(ranked):,} {_plural(who, len(ranked))} in "
               f"{_where(scope, league, short)} with {MIN_MINUTES}+ minutes")
    if ranked.empty:
        st.markdown(theme.empty_state_html(
            "No one to rank against",
            f"No {who} with {MIN_MINUTES}+ minutes in {_where(scope, league, short)}. "
            "Try a wider comparison."), unsafe_allow_html=True)
        return

    # FBref layouts differ by position: show the metrics of the position he is
    # compared against. The newer families use one outfield set.
    layout_group = chosen if kind == "group" else group
    if family == metrics.FBREF:  # today's FBref profile
        by_category = build.profile_rows(ranked, player_row, layout_group, min_coverage=build.MIN_COVERAGE)
    else:  # the family's fixed set: every bar for every player, n/a when missing
        by_category = build.profile_rows(ranked, player_row, layout_group,
                                         layout=metrics.profile_layout(family, layout_group),
                                         keep_missing=True)
    if not by_category:
        st.markdown(theme.empty_state_html(
            "Not enough data to rank",
            f"Too few {who} in this pool have the stats for {player}'s profile."
            + ("" if scope == ALL_LEAGUES else " Try comparing with all leagues in this season.")),
            unsafe_allow_html=True)
        return
    cards = [(category, rows, "") for category, rows in by_category.items()]
    if group != "GK" and metrics.is_big_five(player_row.get("Source"), league):
        cards.append(_big_five_card(as_position, ranked, player_row, layout_group, scope))
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
                   scope: str) -> tuple[str, list, str]:
    """The Understat extras, ranked within big-five players of the compared
    position and season (or the player's league when "Same league" is chosen)."""
    five = ranked if scope == SAME_LEAGUE else pool[pool["League"].isin(metrics.BIG_FIVE)]
    rows = build.profile_rows(five, player_row, group,
                              layout={BIG_FIVE_TITLE: metrics.BIG_FIVE_EXTRAS}, keep_missing=True)
    return BIG_FIVE_TITLE, rows[BIG_FIVE_TITLE], BIG_FIVE_NOTE
