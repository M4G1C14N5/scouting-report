"""Small fake BigQuery results for tests. Columns come from schema.sql."""

from __future__ import annotations

import numpy as np
import pandas as pd

from tests.schema_utils import IDENTITY_COLUMNS, feature_columns

LATEST = "2023-2024"
PREVIOUS = "2022-2023"
INDEX_COLUMNS = ["Player", "Season", "Squad", "League", "Position", "position_group", "minutes"]

# Player, Season, Squad, League, Position, position_group, minutes, Nation, Age
INDEX_ROWS = [
    ("Mohamed Salah", LATEST, "Liverpool", "Premier League", "FW", "ATT", 2536, "EGY", 31),
    ("Mohamed Salah", PREVIOUS, "Liverpool", "Premier League", "FW", "ATT", 3290, "EGY", 30),
    ("Erling Haaland", LATEST, "Manchester City", "Premier League", "FW", "ATT", 2552, "NOR", 23),
    ("Harry Kane", LATEST, "Bayern Munich", "Bundesliga", "FW", "ATT", 2617, "ENG", 30),
    ("Victor Osimhen", LATEST, "Napoli", "Serie A", "FW", "ATT", 2002, "NGA", 24),
    ("Kylian Mbappé", LATEST, "Paris S-G", "Ligue 1", "FW", "ATT", 2621, "FRA", 24),
    ("Youssef En-Nesyri", LATEST, "Sevilla", "La Liga", "FW", "ATT", 1900, "MAR", 26),
    ("Bench Striker", LATEST, "Sevilla", "La Liga", "FW", "ATT", 120, "ESP", 19),
    ("Alisson", LATEST, "Liverpool", "Premier League", "GK", "GK", 2520, "BRA", 30),
    ("Ederson", LATEST, "Manchester City", "Premier League", "GK", "GK", 2970, "BRA", 29),
    ("Jan Oblak", LATEST, "Atlético Madrid", "La Liga", "GK", "GK", 3150, "SVN", 30),
    ("Gregor Kobel", LATEST, "Dortmund", "Bundesliga", "GK", "GK", 2700, "SUI", 25),
]


def player_index() -> pd.DataFrame:
    return pd.DataFrame([r[:7] for r in INDEX_ROWS], columns=INDEX_COLUMNS)


def _feature_frame(table: str, rows: list[tuple]) -> pd.DataFrame:
    cols = feature_columns(table)
    rng = np.random.default_rng(7 if table == "outfield_features" else 11)
    records = []
    for player, season, squad, league, pos, group, minutes, nation, age in rows:
        rec = {c: float(rng.uniform(0.05, 5.0)) for c in cols if c not in IDENTITY_COLUMNS}
        rec.update(Player=player, Nation=nation, Position=pos, Squad=squad, League=league,
                   Age=age, Born=2023 - age, Season=season, position_group=group,
                   minutes=minutes, nineties_played=round(minutes / 90, 1))
        records.append(rec)
    return pd.DataFrame(records, columns=cols)


def outfield_frame() -> pd.DataFrame:
    return _feature_frame("outfield_features", [r for r in INDEX_ROWS if r[5] != "GK"])


def goalkeeper_frame() -> pd.DataFrame:
    return _feature_frame("goalkeeper_features", [r for r in INDEX_ROWS if r[5] == "GK"])


def player_stats(player: str, season: str) -> pd.Series:
    for frame in (outfield_frame(), goalkeeper_frame()):
        hit = frame[(frame["Player"] == player) & (frame["Season"] == season)]
        if not hit.empty:
            return hit.iloc[0]
    raise ValueError(f"Player '{player}' not found for season '{season}'.")


def position_pool(group: str, season: str) -> pd.DataFrame:
    frame = goalkeeper_frame() if group == "GK" else outfield_frame()
    pool = frame[(frame["position_group"] == group) & (frame["Season"] == season)
                 & (frame["minutes"] >= 450)]
    return pool.reset_index(drop=True)


def similar_players(player: str, season: str, group: str) -> pd.DataFrame:
    pool = position_pool(group, season)
    others = pool[pool["Player"] != player].reset_index(drop=True)
    out = others[["Player", "Squad", "League", "Season", "position_group", "minutes"]].copy()
    out["similarity_score"] = np.linspace(0.95, 0.5, len(out)) if len(out) else []
    return out
