"""Identity card (photo or initials, name, chips) and the KPI row."""

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
        chips.append(theme.flag_chip(str(nation).strip()))
    source = row.get("Source")  # which data provider the row comes from
    if not stats.is_missing(source) and str(source).strip():
        chips.append(theme.chip(str(source).strip()))
    return chips


def photo_of(row: Mapping[str, Any]) -> str | None:
    value = row.get("photo_url")
    return None if stats.is_missing(value) or not str(value).strip() else str(value)


def render(row, photo_url: str | None = None) -> None:
    """`photo_url` overrides the row's own (FBref seasons borrow a newer season's photo)."""
    tiles = [theme.kpi(label, text, help=help_text)
             for label, text, help_text in build.kpi_tiles(row, row["position_group"])]
    photo = photo_url or photo_of(row)
    st.markdown(theme.identity_html(str(row["Player"]), chips_for(row), photo_url=photo)
                + theme.kpi_row_html(tiles), unsafe_allow_html=True)
