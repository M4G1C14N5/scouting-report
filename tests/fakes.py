"""Small fake BigQuery results for tests. Columns come from schema.sql, plus the
Source column the outfield_all / goalkeeper_all views add."""

from __future__ import annotations

import numpy as np
import pandas as pd

from tests.schema_utils import IDENTITY_COLUMNS, feature_columns, newer_source_columns

LATEST = "2023-2024"
PREVIOUS = "2022-2023"
INDEX_COLUMNS = ["Player", "Season", "Squad", "League", "Position", "position_group", "minutes", "Source"]
SOURCE = "FBref"

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
    return pd.DataFrame([(*r[:7], SOURCE) for r in INDEX_ROWS], columns=INDEX_COLUMNS)


def _feature_frame(table: str, rows: list[tuple]) -> pd.DataFrame:
    """FBref rows: every FBref metric filled, the newer-source metrics null."""
    cols = feature_columns(table)
    newer = newer_source_columns(table)
    rng = np.random.default_rng(7 if table == "outfield_features" else 11)
    records = []
    for player, season, squad, league, pos, group, minutes, nation, age in rows:
        rec = {c: float(rng.uniform(0.05, 5.0)) for c in cols
               if c not in IDENTITY_COLUMNS and c not in newer}
        rec.update({c: np.nan for c in newer})
        rec.update(Player=player, Nation=nation, Position=pos, Squad=squad, League=league,
                   Age=age, Born=2023 - age, Season=season, position_group=group,
                   minutes=minutes, nineties_played=round(minutes / 90, 1), Source=SOURCE)
        records.append(rec)
    return pd.DataFrame(records, columns=[*cols, "Source"])


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


def _in_group(frame: pd.DataFrame, group: str) -> pd.Series:
    """group, or "OUT" (src.similarity.OUTFIELD) for every outfield group."""
    if group == "OUT":
        return frame["position_group"].isin(["ATT", "MID", "DEF"])
    return frame["position_group"] == group


def position_pool(group: str, season: str) -> pd.DataFrame:
    frame = goalkeeper_frame() if group == "GK" else outfield_frame()
    pool = frame[_in_group(frame, group) & (frame["Season"] == season)
                 & (frame["minutes"] >= 450)]
    return pool.reset_index(drop=True)


def similar_players(player: str, season: str, group: str) -> pd.DataFrame:
    pool = position_pool(group, season)
    others = pool[pool["Player"] != player].reset_index(drop=True)
    out = others[["Player", "Squad", "League", "Season", "position_group", "minutes"]].copy()
    out["similarity_score"] = np.linspace(0.95, 0.5, len(out)) if len(out) else []
    return out


# --- newer seasons (API-Football, Understat, American Soccer Analysis) -------------

EUROPE_SEASON = "2025-2026"
US_SEASON = "2026"      # pairs with EUROPE_SEASON (the European season ending in 2026)
US_EARLY = "2024"       # pairs with 2023-2024, an FBref season: no trial partner
BIG_FIVE_SOURCE = "API-Football + Understat"
MATCH_DATA_SOURCE = "API-Football match data"
US_SOURCE = "American Soccer Analysis"

# No Dribbles attempted no dribbles, so his dribble success % is 0/0 (null).
NO_DRIBBLES = "No Dribbles Forward"

# Player, Season, Squad, League, position_group, minutes, Source
NEW_ROWS = [
    *[(f"Prem Forward {i}", EUROPE_SEASON, f"Prem Club {i}", "Premier League", "ATT",
       900 + 150 * i, BIG_FIVE_SOURCE) for i in range(8)],
    *[(f"Liga Forward {i}", EUROPE_SEASON, f"Liga Club {i}", "La Liga", "ATT",
       1000 + 120 * i, BIG_FIVE_SOURCE) for i in range(4)],
    *[(f"Two Forward {i}", EUROPE_SEASON, f"Two Club {i}", "League Two", "ATT",
       1100 + 130 * i, MATCH_DATA_SOURCE) for i in range(8)],
    ("Gappy Forward", EUROPE_SEASON, "Two Club 9", "League Two", "ATT", 1500, MATCH_DATA_SOURCE),
    (NO_DRIBBLES, EUROPE_SEASON, "Two Club 10", "League Two", "ATT", 1400, MATCH_DATA_SOURCE),
    *[(f"US Forward {i}", US_SEASON, f"US Club {i}", "MLS", "ATT", 950 + 140 * i, US_SOURCE)
      for i in range(8)],
    *[(f"Early US Forward {i}", US_EARLY, f"US Club {i}", "USL League One", "ATT",
       1200 + 100 * i, US_SOURCE) for i in range(6)],
    *[(f"Prem Keeper {i}", EUROPE_SEASON, f"Prem Club {i}", "Premier League", "GK",
       1800 + 200 * i, BIG_FIVE_SOURCE) for i in range(4)],
    *[(f"Two Keeper {i}", EUROPE_SEASON, f"Two Club {i}", "League Two", "GK",
       2000 + 150 * i, MATCH_DATA_SOURCE) for i in range(3)],
]
# Gappy Forward lacks this one stat of the European set.
GAPPY_MISSING = "dribbled_past"


def _family_values(group: str, source: str, rng) -> dict[str, float]:
    from src.ui import metrics

    if group == "GK":
        keys = metrics.EUROPE_GOALKEEPER
    elif source == US_SOURCE:
        keys = metrics.US_OUTFIELD
    else:
        keys = metrics.EUROPE_OUTFIELD + (metrics.BIG_FIVE_EXTRAS if source == BIG_FIVE_SOURCE else ())
    keys = (*keys, *metrics.stats_keys(keys))
    values = {k: float(rng.uniform(0.2, 40.0)) for k in keys}
    if group != "GK" and source != US_SOURCE:
        # patchy stats exist in the data but must never be shown or compared
        values.update(offsides=float(rng.integers(0, 20)), penalties_won=1.0,
                      penalties_conceded=1.0, penalties_missed=0.0)
    return values


def new_season_frame(table: str) -> pd.DataFrame:
    """Newer-season rows: each family's own stats filled, everything else null."""
    cols = feature_columns(table)
    want_gk = table == "goalkeeper_features"
    rng = np.random.default_rng(23 if want_gk else 29)
    records = []
    for player, season, squad, league, group, minutes, source in NEW_ROWS:
        if (group == "GK") != want_gk:
            continue
        rec = {c: np.nan for c in cols}
        rec.update(_family_values(group, source, rng))
        if player == "Gappy Forward":
            rec[GAPPY_MISSING] = np.nan
        if player == NO_DRIBBLES:
            rec.update(dribbles_attempted=0.0, dribbles_completed=0.0, dribble_success_pct=np.nan)
        rec.update(Player=player, Nation="ENG", Position="FW" if group == "ATT" else group,
                   Squad=squad, League=league, Age=25, Born=2000, Season=season,
                   position_group=group, minutes=minutes, nineties_played=round(minutes / 90, 1),
                   appearances=float(minutes // 80), Source=source)
        records.append(rec)
    return pd.DataFrame(records, columns=[*cols, "Source"])


def new_player_stats(player: str, season: str) -> pd.Series:
    for table in ("outfield_features", "goalkeeper_features"):
        frame = new_season_frame(table)
        hit = frame[(frame["Player"] == player) & (frame["Season"] == season)]
        if not hit.empty:
            return hit.iloc[0]
    raise ValueError(f"Player '{player}' not found for season '{season}'.")


def new_position_pool(group: str, season: str, paired: bool = False) -> pd.DataFrame:
    frame = new_season_frame("goalkeeper_features" if group == "GK" else "outfield_features")
    seasons = {season}
    if paired:
        from src.similarity import paired_season

        seasons.add(paired_season(season))
    pool = frame[_in_group(frame, group) & frame["Season"].isin(seasons)
                 & (frame["minutes"] >= 450)]
    return pool.reset_index(drop=True)
