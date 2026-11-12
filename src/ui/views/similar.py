"""Similar players tab: a compare mode, the similarity table, a head-to-head
dumbbell for the selected row, and a button that opens that player's profile."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src import similarity
from src.similarity import (MIN_CANDIDATES, MIN_FEATURES, MIN_MINUTES, IncompleteDataError,
                            NoPairedSeasonError, NotEnoughStatsError)
from src.ui import build, data, metrics, state, stats, theme, viz

METHOD = ("Method: cosine similarity on z-scored stats within the position pool "
          f"(same position and season, {MIN_MINUTES}+ minutes), on the stats this player has, "
          "compared only with players who have all of those stats.")
NOT_ENOUGH_BODY = (f"Similarity needs at least {MIN_FEATURES} of the position's stats that this player "
                   f"has and at least {MIN_CANDIDATES} other players also have. "
                   "The data for this player-season has fewer.")
INCOMPLETE_TITLE = "Incomplete data for this comparison"
NO_PAIRED_TITLE = "No trial comparison for this season"

COLUMN_CONFIG = {
    "Player": st.column_config.TextColumn("Player", width="medium"),
    "Club": st.column_config.TextColumn("Club"),
    "League": st.column_config.TextColumn("League"),
    "Season": st.column_config.TextColumn("Season"),
    "Position": st.column_config.TextColumn("Position"),
    "Role": st.column_config.TextColumn(
        "Role", help="Where he lined up most and his share of starting minutes there"),
    "Minutes": st.column_config.NumberColumn("Minutes", format="localized"),
    "Similarity": st.column_config.ProgressColumn(
        "Similarity", min_value=0.0, max_value=1.0, format="%.2f", color=theme.ACCENT, width=120,
        help="1.00 = an identical statistical profile"),
}
# Position (the data provider's broad group) is left out to give the other
# columns room; Role says where each match actually played.
COLUMN_ORDER = ["Player", "Club", "League", "Season", "Role", "Minutes", "Similarity"]


def _where(mode: str, family: str, season: str) -> str:
    short = stats.short_season(season)
    if mode == similarity.MODE_BIG_FIVE:
        return f"big-five leagues, {short}"
    if mode == similarity.MODE_TRIAL:
        paired = similarity.paired_season(season)
        return f"European and US leagues, {short} and {stats.short_season(paired)}"
    return f"every {'US' if family == metrics.US else 'European'} league, {short}"


def method_text(mode: str, family: str, group: str, season: str, role_aware: bool = False) -> str:
    """The method line for a fixed-template mode: the stats it uses and the pool."""
    template = similarity.mode_template(mode, family, group)
    names = ", ".join(metrics.label(k) for k in template)
    if role_aware:
        return (f"Method: style plus role. Every outfield player is compared on the same "
                f"{len(template)} stats ({names}; season totals per 90, z-scored and capped so "
                "no single stat decides a match) and on where he lined up: his share of starts "
                "as a striker, attacking mid or winger, central mid, fullback or centre-back. "
                "Players from a neighbouring role can still match, a step below. "
                f"Pool: {_where(mode, family, season)}, {MIN_MINUTES}+ minutes; players missing "
                "any of these stats are left out.")
    return (f"Method: cosine similarity on z-scored stats. Every player is compared on the same "
            f"{len(template)} stats: {names}. Season totals are compared per 90. "
            f"Pool: same position, {_where(mode, family, season)}, {MIN_MINUTES}+ minutes; "
            "players missing any of these stats are left out.")


def _mode_control(modes: list[str]) -> str:
    if st.session_state.get(state.SIMILAR_MODE) not in modes:
        st.session_state[state.SIMILAR_MODE] = modes[0]
    st.markdown(theme.section_label_html("Compare on"), unsafe_allow_html=True)
    return st.selectbox("Compare on", modes, key=state.SIMILAR_MODE, disabled=len(modes) == 1,
                        label_visibility="collapsed")


def render(bq, pool: pd.DataFrame | None, row) -> None:
    player, season, group = str(row["Player"]), str(row["Season"]), str(row["position_group"])
    family = metrics.family_of(row.get("Source"))
    big_five = metrics.is_big_five(row.get("Source"), row.get("League"))
    mode = _mode_control(similarity.modes_for(family, group, big_five))
    role_aware = family != metrics.FBREF and group != "GK" and similarity.has_role_mix(row)
    if family == metrics.FBREF:
        st.caption(METHOD)
    else:
        st.caption(method_text(mode, family, group, season, role_aware))
    if mode == similarity.MODE_BIG_FIVE:
        st.caption(similarity.BIG_FIVE_NOTE)
    elif mode == similarity.MODE_TRIAL:
        st.info(similarity.TRIAL_NOTE)

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
        matches = data.similar_players(bq, player, season, group, mode)
    except IncompleteDataError as exc:
        st.markdown(theme.empty_state_html(INCOMPLETE_TITLE, str(exc)), unsafe_allow_html=True)
        return
    except NoPairedSeasonError as exc:
        st.markdown(theme.empty_state_html(NO_PAIRED_TITLE, str(exc)), unsafe_allow_html=True)
        return
    except NotEnoughStatsError as exc:
        st.markdown(theme.empty_state_html(str(exc), NOT_ENOUGH_BODY), unsafe_allow_html=True)
        return
    except Exception as exc:  # noqa: BLE001 - BigQuery, auth and "not in pool" all end here
        theme.error_box(st, "Couldn't compute similar players.", exc)
        return

    compare_pool = pool
    if role_aware or mode == similarity.MODE_TRIAL:
        # role-aware matches come from every outfield group: compare within that pool
        key = similarity.OUTFIELD if role_aware else group
        try:
            compare_pool = data.position_pool(bq, key, season, paired=mode == similarity.MODE_TRIAL)
        except Exception:  # noqa: BLE001 - the table still shows; the dumbbell says it can't
            compare_pool = None

    if matches.empty:
        plural = metrics.POSITION_PLURALS.get(group, "players")
        body = (f"No other {plural} reached {MIN_MINUTES} minutes in {stats.short_season(season)}."
                if family == metrics.FBREF else
                f"No other {plural} with {MIN_MINUTES}+ minutes have every stat this comparison uses.")
        st.markdown(theme.empty_state_html("No similar players", body), unsafe_allow_html=True)
        return

    table = build.similar_table(matches.head(top_n))
    # A keyed dataframe keeps its selection when the data changes, so drop it
    # whenever the table now belongs to a different player-season or mode.
    if st.session_state.get(state.SIMILAR_FOR) != (player, season, mode):
        st.session_state.pop(state.SIMILAR_TABLE, None)
        st.session_state[state.SIMILAR_FOR] = (player, season, mode)
    shown = table.assign(Season=table["Season"].map(stats.season_option))  # "2023/24" or "2026 (US)"
    event = st.dataframe(shown, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key=state.SIMILAR_TABLE,
                         column_config=COLUMN_CONFIG, column_order=COLUMN_ORDER)
    rows = list(event.selection.rows) if event is not None else []
    picked = [i for i in rows if i < len(table)]
    if not picked:
        st.caption(f"Select a row to compare that player with {player}.")
        return
    other = table.iloc[picked[0]]
    _compare(compare_pool, row, other, group, mode, role_aware)
    st.button(f"View {other['Player']}'s profile", key="view_profile", type="primary",
              on_click=state.goto, args=(str(other["Player"]), str(other["Season"])))


def _compare(pool: pd.DataFrame | None, row, other: pd.Series, group: str, mode: str,
             role_aware: bool = False) -> None:
    a_name, b_name = str(row["Player"]), str(other["Player"])
    if pool is None or pool.empty:
        st.caption("Comparison unavailable: the position pool didn't load.")
        return
    a = pool[(pool["Player"] == a_name) & (pool["Season"] == row["Season"])]
    b = pool[(pool["Player"] == b_name) & (pool["Season"] == other["Season"])]
    if a.empty or b.empty:
        st.caption("Comparison unavailable: one of the players isn't in the position pool.")
        return
    family = metrics.family_of(row.get("Source"))
    if family == metrics.FBREF:
        a_rows = build.profile_rows(pool, a.iloc[0], group)
        b_rows = build.profile_rows(pool, b.iloc[0], group)
    else:
        # percentiles on the mode's stats, within the mode's pool
        ranked = similarity.mode_pool(pool, a.iloc[0], mode, same_group=not role_aware)
        layout = metrics.by_category(similarity.mode_template(mode, family, group), group)
        a_rows = build.profile_rows(ranked, a.iloc[0], group, layout=layout)
        b_rows = build.profile_rows(ranked, b.iloc[0], group, layout=layout)
    labels, a_vals, b_vals = build.compare_lists(a_rows, b_rows)
    with st.container(border=True, key="card_compare"):
        st.markdown(theme.card_title_html(f"{a_name} vs {b_name} · percentiles"), unsafe_allow_html=True)
        st.plotly_chart(viz.compare_dumbbell(labels, a_vals, b_vals, a_name, b_name),
                        config=viz.PLOTLY_CONFIG, theme=None, width="stretch", key="compare_chart")
