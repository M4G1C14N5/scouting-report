"""Pure filtering of the player index for the sidebar and the search box.

The index has one row per player-season with the columns
Player, Season, Squad, League, Position, position_group, minutes.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def seasons_desc(index: pd.DataFrame) -> list[str]:
    return sorted(index["Season"].dropna().unique().tolist(), reverse=True)


def leagues(index: pd.DataFrame) -> list[str]:
    return sorted(index["League"].dropna().unique().tolist())


def clubs(index: pd.DataFrame, league: str | None) -> list[str]:
    rows = index if league is None else index[index["League"] == league]
    return sorted(rows["Squad"].dropna().unique().tolist())


def filter_index(index: pd.DataFrame, season: str | None = None, league: str | None = None,
                 club: str | None = None) -> pd.DataFrame:
    """Rows matching every filter that is not None."""
    mask = pd.Series(True, index=index.index)
    if season is not None:
        mask &= index["Season"] == season
    if league is not None:
        mask &= index["League"] == league
    if club is not None:
        mask &= index["Squad"] == club
    return index[mask].reset_index(drop=True)


def search_options(filtered: pd.DataFrame) -> pd.DataFrame:
    """One row per player: their latest season in view (most minutes on a tie)."""
    if filtered.empty:
        return filtered.copy()
    latest = (filtered.sort_values(["Season", "minutes"], ascending=[False, False])
              .drop_duplicates("Player", keep="first"))
    return latest.sort_values("Player", key=lambda s: s.str.casefold()).reset_index(drop=True)


def option_label(player: str, club: str, league: str) -> str:
    return f"{player} — {club}, {league}"


def player_seasons(filtered: pd.DataFrame, player: str) -> list[str]:
    return sorted(filtered.loc[filtered["Player"] == player, "Season"].unique().tolist())


def pick_example(filtered: pd.DataFrame, preferred: Iterable[str]) -> str | None:
    """First preferred name that is in view, else the player with the most minutes."""
    if filtered.empty:
        return None
    names = set(filtered["Player"])
    for name in preferred:
        if name in names:
            return name
    return str(filtered.sort_values("minutes", ascending=False).iloc[0]["Player"])
