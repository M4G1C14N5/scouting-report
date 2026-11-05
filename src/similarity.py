"""
similarity.py
=============
Player similarity model using cosine similarity on z-scored per-90 stats.

How it works
------------
1. Pull the outfield or goalkeeper rows from BigQuery (the outfield_all /
   goalkeeper_all views by default: FBref plus the newer sources).
2. Filter to players with >= 450 minutes (serious sample size).
3. For the queried player's position group, select a position-specific
   set of features (feature template). Keep only the features the queried
   player has and at least half the pool has: newer sources leave some
   stats empty (no progressive passing after 2023-24, no tackles in the US
   leagues). Fewer than 3 left raises NotEnoughStatsError.
4. Z-score the kept feature columns so no single stat dominates by scale.
5. Compute cosine similarity between the queried player and every other
   player in the same position pool.
6. Return the top N most similar players with their similarity score.

Position feature templates
--------------------------
Each template is a curated list of per-90 and rate stats relevant to
that position group. Raw counting stats are excluded — they favour
players with more minutes.
"""

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

GCP_PROJECT = os.environ.get("GCP_PROJECT", "scouting-466001")
BQ_DATASET  = os.environ.get("BQ_DATASET", "scouting")

# Tables (or views) the app reads. The defaults are the views that union the
# FBref tables with the soccer-data tables and add a Source column.
OUTFIELD_TABLE = os.environ.get("BQ_OUTFIELD_TABLE", "outfield_all")
GOALKEEPER_TABLE = os.environ.get("BQ_GOALKEEPER_TABLE", "goalkeeper_all")

MIN_MINUTES = 450
MIN_FEATURES = 3          # fewer usable template stats than this: no similarity
MIN_POOL_COVERAGE = 0.5   # a stat is usable when at least this share of the pool has it
NOT_ENOUGH_STATS = "Not enough comparable stats for this player in this season"


class NotEnoughStatsError(ValueError):
    """The player-season shares too few template stats with its pool."""

    def __init__(self, message: str = NOT_ENOUGH_STATS) -> None:
        super().__init__(message)

# Feature templates: one list of columns per position group.
# All columns must exist in outfield_features / goalkeeper_features.
# Rows from the newer sources may leave some of them null; see usable_features.
FEATURE_TEMPLATES = {
    "ATT": [
        # Output metrics (reduced weight by including fewer)
        "goals_per_90",
        "xg_per_90",
        "npxg_per_90",
        "goals_per_shot",
        # Playmaking & progression (increased weight — what wingers do)
        "assists_per_90",
        "xag_per_90",
        "key_passes",
        "passes_into_final_third",
        "passes_into_penalty_area",
        "crosses_into_penalty_area",
        "progressive_carries",
        "progressive_passing_distance",
        # Efficiency & involvement
        "shots_total_per_90",
        "passes_completion_pct_total",
        "npxg_plus_xag_per_90",
    ],
    "MID": [
        # Progression & dribbling (heavily weighted — defines midfielder style)
        "progressive_carries",
        "progressive_passing_distance",
        "pass_progressive_passes",
        "passes_into_final_third",
        # Playmaking
        "assists_per_90",
        "xag_per_90",
        "key_passes",
        "passes_into_penalty_area",
        "crosses_into_penalty_area",
        # Involvement & efficiency
        "passes_completion_pct_total",
        "goals_per_90",
        "shots_total_per_90",
        "npxg_per_90",
        # Defending
        "tackles_plus_interceptions",
        "interceptions",
    ],
    "DEF": [
        "tackles_total",
        "tackles_won",
        "tackles_def_3rd",
        "tackles_mid_3rd",
        "dribblers_tackled_pct",
        "interceptions",
        "tackles_plus_interceptions",
        "blocks_total",
        "clearances",
        "errors_leading_to_shot",
        "passes_completion_pct_total",
        "progressive_passing_distance",
        "passes_into_final_third",
        "goals_per_90",
        "assists_per_90",
    ],
    "GK": [
        "save_pct",
        "goals_against_per_90",
        "clean_sheet_pct",
        "pk_save_pct",
        "shots_on_target_against",
    ],
}


def table_ref(table: str) -> str:
    """Fully qualified, backquoted BigQuery name of a table in the dataset."""
    return f"`{GCP_PROJECT}.{BQ_DATASET}.{table}`"


def _fetch_features(bq_client: bigquery.Client, position_group: str, season: str) -> pd.DataFrame:
    """Pull feature table rows for a given position group and season from BigQuery."""
    table = table_ref(GOALKEEPER_TABLE if position_group == "GK" else OUTFIELD_TABLE)

    sql = f"""
        SELECT *
        FROM {table}
        WHERE position_group = '{position_group}'
          AND Season = '{season}'
          AND minutes >= {MIN_MINUTES}
    """
    return bq_client.query(sql).to_dataframe()


def _zscore_matrix(df: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    """
    Z-score each feature column across the pool.
    NaN values are filled with 0 (pool mean after z-scoring).
    """
    X = df[feature_cols].copy().astype(float)
    means = X.mean()
    stds  = X.std().replace(0, 1)   # avoid divide-by-zero for constant columns
    X = (X - means) / stds
    return X.fillna(0).values


def _cosine_similarity(query_vec: np.ndarray, matrix: np.ndarray) -> np.ndarray:
    """Cosine similarity between one vector and every row in a matrix."""
    query_norm = query_vec / (np.linalg.norm(query_vec) + 1e-9)
    norms      = np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-9
    return (matrix / norms) @ query_norm


def usable_features(pool_df: pd.DataFrame, query_row, template: list[str]) -> list[str]:
    """Template features the query player has (non-null) and at least
    MIN_POOL_COVERAGE of the pool has, in template order."""
    usable = []
    for col in template:
        if col not in pool_df.columns:
            continue
        if pd.isna(query_row.get(col)):
            continue
        if pool_df[col].notna().mean() < MIN_POOL_COVERAGE:
            continue
        usable.append(col)
    return usable


def rank_similar(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    template: list[str],
    top_n: int,
) -> pd.DataFrame:
    """Steps 3-6 on a fetched pool: pick the usable features, z-score, cosine,
    drop the query player, top N. Raises NotEnoughStatsError when fewer than
    MIN_FEATURES template stats are usable. With every stat present this is
    exactly the original full-template ranking."""
    pool_reset = pool_df.reset_index(drop=True).copy()
    query_mask = (pool_reset["Player"] == player) & (pool_reset["Season"] == season)
    positional_idx = pool_reset.index[query_mask][0]

    feature_cols = usable_features(pool_reset, pool_reset.iloc[positional_idx], template)
    if len(feature_cols) < MIN_FEATURES:
        raise NotEnoughStatsError()

    # ── 4. Z-score the feature matrix ─────────────────────────────────────────
    X         = _zscore_matrix(pool_reset, feature_cols)
    query_vec = X[positional_idx]

    # ── 5. Cosine similarity against full pool ────────────────────────────────
    pool_reset["similarity_score"] = _cosine_similarity(query_vec, X)

    # ── 6. Exclude the query player themselves, sort, return top N ────────────
    return (
        pool_reset[~query_mask]
        .sort_values("similarity_score", ascending=False)
        .head(top_n)[["Player", "Squad", "League", "Season", "position_group", "minutes", "similarity_score"]]
        .reset_index(drop=True)
    )


def find_similar_players(
    player: str,
    season: str,
    top_n: int = 10,
    bq_client: bigquery.Client | None = None,
) -> pd.DataFrame:
    """
    Find the top_n most similar players to a given player in a given season.

    Parameters
    ----------
    player : str
        Exact player name as it appears in the database.
    season : str
        Season string, e.g. "2022-2023".
    top_n : int
        Number of similar players to return (default 10).
    bq_client : bigquery.Client, optional
        Reuse an existing client. If None, a new one is created.

    Returns
    -------
    pd.DataFrame
        Columns: Player, Squad, League, Season, position_group,
                 minutes, similarity_score (0–1, descending).

    Raises
    ------
    NotEnoughStatsError
        When fewer than MIN_FEATURES template stats are usable (see
        usable_features). It is a ValueError.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=GCP_PROJECT)

    # ── 1. Find the player's position group ──────────────────────────────────
    lookup_sql = f"""
        SELECT position_group
        FROM {table_ref(OUTFIELD_TABLE)}
        WHERE Player = @player AND Season = @season
        UNION ALL
        SELECT position_group
        FROM {table_ref(GOALKEEPER_TABLE)}
        WHERE Player = @player AND Season = @season
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player", "STRING", player),
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ]
    )
    result = bq_client.query(lookup_sql, job_config=job_config).to_dataframe()
    if result.empty:
        raise ValueError(f"Player '{player}' not found for season '{season}'.")

    position_group = result.iloc[0]["position_group"]
    feature_cols   = FEATURE_TEMPLATES[position_group]

    # ── 2. Fetch full pool for that position group and season ─────────────────
    pool_df = _fetch_features(bq_client, position_group, season)

    if pool_df.empty:
        raise ValueError(f"No data found for position group '{position_group}'.")

    # ── 3. Locate the query player in the pool ────────────────────────────────
    query_mask = (pool_df["Player"] == player) & (pool_df["Season"] == season)
    if not query_mask.any():
        raise ValueError(
            f"'{player}' ({season}) is in the database but below the "
            f"{MIN_MINUTES}-minute threshold and cannot be used as a query."
        )

    return rank_similar(pool_df, player, season, feature_cols, top_n)


def get_player_stats(
    player: str,
    season: str,
    bq_client: bigquery.Client | None = None,
) -> pd.Series:
    """
    Return all feature columns for a single player-season.
    Used by the Streamlit app to display a player's stat card.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=GCP_PROJECT)

    for table in [OUTFIELD_TABLE, GOALKEEPER_TABLE]:
        sql = f"""
            SELECT *
            FROM {table_ref(table)}
            WHERE Player = @player AND Season = @season
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("player", "STRING", player),
                bigquery.ScalarQueryParameter("season", "STRING", season),
            ]
        )
        df = bq_client.query(sql, job_config=job_config).to_dataframe()
        if not df.empty:
            return df.iloc[0]

    raise ValueError(f"Player '{player}' not found for season '{season}'.")
