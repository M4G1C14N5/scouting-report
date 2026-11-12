"""Stats tab: every stat by category, the season total next to per 90."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.ui import build, metrics, theme

TOTAL_HELP = "Season total. For % and per-shot stats this is the season rate itself."
PER90_HELP = "Per 90 minutes played."


def _text(value: float, per90: bool) -> str:
    if pd.isna(value):
        return ""
    if per90:
        return f"{value:.2f}"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,}"


def display_table(table: pd.DataFrame) -> pd.DataFrame:
    """The Stats-tab table as text columns, blank where a value is missing.
    A row with no value at all (a family stat this player lacks) reads n/a."""
    out = table.copy()
    out["Total"] = [_text(v, False) for v in table["Total"]]
    out["Per 90"] = [_text(v, True) for v in table["Per 90"]]
    none = table["Total"].isna() & table["Per 90"].isna()
    out.loc[none, "Total"] = build.NOT_AVAILABLE
    return out


NOTE = ("Per 90 = per 90 minutes played. Percentages and per-shot stats are season rates, "
        "so they have no per-90 value.")


BIG_FIVE_TITLE = "Big five only"
BIG_FIVE_NOTE = ("From Understat, which covers only the Premier League, La Liga, Serie A, "
                 "Bundesliga and Ligue 1.")


def _tables(row, group: str) -> list[tuple[str, pd.DataFrame, str]]:
    """(card title, table, note) per card. FBref seasons: every stat by category,
    as before. Newer seasons: the family's fixed set (patchy stats such as
    offsides never appear), plus the Understat extras for big-five players."""
    family = metrics.family_of(row.get("Source"))
    if family == metrics.FBREF:
        tables = [(category, build.stat_table(row, category), "")
                  for category in metrics.stats_categories(group)]
        return [t for t in tables if not t[1].empty]
    tables = [(category, build.stat_table(row, category, keys=keys, keep_missing=True), "")
              for category, keys in metrics.stats_layout(family, group).items()]
    if group != "GK" and metrics.is_big_five(row.get("Source"), row.get("League")):
        extras = build.stat_table(row, BIG_FIVE_TITLE, keep_missing=True,
                                  keys=metrics.stats_keys(metrics.BIG_FIVE_EXTRAS))
        tables.append((BIG_FIVE_TITLE, extras, BIG_FIVE_NOTE))
    return tables


def render(row, group: str) -> None:
    tables = _tables(row, group)
    if not tables:
        st.markdown(theme.empty_state_html("No stats recorded",
                                           "This player-season has no stat columns filled in."),
                    unsafe_allow_html=True)
        return
    cards = []
    for category, table, note in tables:
        shown = display_table(table)
        rows = list(zip(shown["Stat"], shown["Total"], shown["Per 90"]))
        cards.append(theme.stat_card_html(category, rows, note, TOTAL_HELP, PER90_HELP))
    # weight: table rows plus the title (and note) so short cards still count
    weights = [len(table) + 2 + (1 if note else 0) for _, table, note in tables]
    columns = [[cards[i] for i in col] for col in build.balance_columns(weights)]
    st.markdown(theme.stat_grid_html(columns), unsafe_allow_html=True)
    st.caption(NOTE)
