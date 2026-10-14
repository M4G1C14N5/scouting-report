"""Identity card (initials avatar, name, chips) and the KPI row."""

from __future__ import annotations

from typing import Any, Mapping

import streamlit as st

from src.ui import build, metrics, stats, theme


def chips_for(row: Mapping[str, Any]) -> list[str]:
    group = row.get("position_group")
    chips = [theme.chip(metrics.POSITION_NAMES.get(group, str(group)), accent=True)]
    for key in ("Squad", "League"):
        if not stats.is_missing(row.get(key)):
            chips.append(theme.chip(str(row.get(key))))
    chips.append(theme.chip(stats.short_season(row.get("Season"))))
    age = stats.to_float(row.get("Age"))
    if age is not None:
        chips.append(theme.chip(f"Age {int(age)}"))
    nation = row.get("Nation")
    if not stats.is_missing(nation) and str(nation).strip():
        chips.append(theme.chip(str(nation).strip()))
    return chips


def render(row) -> None:
    tiles = [theme.kpi(label, text, help=help_text)
             for label, text, help_text in build.kpi_tiles(row, row["position_group"])]
    st.markdown(theme.identity_html(str(row["Player"]), chips_for(row)) + theme.kpi_row_html(tiles),
                unsafe_allow_html=True)
