"""Pure filtering of the player index for the sidebar and the search box.

The index has one row per player-season with the columns
Player, Season, Squad, League, Position, position_group, minutes, Source.

Seasons sort chronologically with stats.season_sort_key, so European
("2025-2026") and US ("2026") labels interleave correctly.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from src.ui.stats import season_sort_key

_EUROPEAN = re.compile(r"^\d{4}-\d{4}$")


def seasons_desc(index: pd.DataFrame) -> list[str]:
    return sorted(index["Season"].dropna().unique().tolist(), key=season_sort_key, reverse=True)


def default_season(seasons: list[str]) -> str | None:
    """The sidebar's starting season: the newest European-format season
    ("2025-2026"), else the newest season, else None. `seasons` is newest
    first (seasons_desc). A US calendar season ("2026") holds only US
    leagues, so it is not the default while a European season exists."""
    european = [s for s in seasons if _EUROPEAN.match(str(s))]
    if european:
        return european[0]
    return seasons[0] if seasons else None


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
    rank = {s: i for i, s in enumerate(sorted(filtered["Season"].dropna().unique(), key=season_sort_key))}
    latest = (filtered.sort_values(["Season", "minutes"], ascending=[False, False],
                                   key=lambda col: col.map(rank) if col.name == "Season" else col)
              .drop_duplicates("Player", keep="first"))
    return latest.sort_values("Player", key=lambda s: s.str.casefold()).reset_index(drop=True)


# Letters the search box's accent folding does not reduce to plain letters (it
# turns "é" into "e" but leaves "Ø" alone, so "odegaard" never found Ødegaard).
_PLAIN = str.maketrans({"ø": "o", "Ø": "O", "ł": "l", "Ł": "L", "ß": "ss", "æ": "ae",
                        "Æ": "Ae", "œ": "oe", "Œ": "Oe", "đ": "d", "Đ": "D", "ı": "i",
                        "þ": "th", "Þ": "Th", "ð": "d", "Ð": "D"})


def plain_alias(player: str) -> str:
    """The words of `player` that need a plain-letter spelling to be searchable
    ("Martin Ødegaard" -> "Odegaard"); "" when every word already is."""
    words = [w.translate(_PLAIN) for w in str(player).split() if w.translate(_PLAIN) != w]
    return " ".join(words)


def option_label(player: str, club: str, league: str) -> str:
    alias = plain_alias(player)
    name = f"{player} ({alias})" if alias else player
    return f"{name} — {club}, {league}"


def preferred_season(seasons_minutes: dict[str, float], preferred: str | None,
                     min_minutes: float) -> str | None:
    """The season to open a player in: `preferred` (the sidebar's season) when he
    has min_minutes there, else his most recent season with min_minutes, else his
    most recent season. `seasons_minutes`: {season: minutes}."""
    if not seasons_minutes:
        return None
    if preferred in seasons_minutes and seasons_minutes[preferred] >= min_minutes:
        return preferred
    newest_first = sorted(seasons_minutes, key=season_sort_key, reverse=True)
    for season in newest_first:
        if seasons_minutes[season] >= min_minutes:
            return season
    return newest_first[0]


def player_seasons(filtered: pd.DataFrame, player: str) -> list[str]:
    return sorted(filtered.loc[filtered["Player"] == player, "Season"].unique().tolist(), key=season_sort_key)


def pick_example(filtered: pd.DataFrame, preferred: Iterable[str]) -> str | None:
    """First preferred name that is in view, else the player with the most minutes."""
    if filtered.empty:
        return None
    names = set(filtered["Player"])
    for name in preferred:
        if name in names:
            return name
    return str(filtered.sort_values("minutes", ascending=False).iloc[0]["Player"])
