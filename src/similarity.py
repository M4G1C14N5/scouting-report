"""
similarity.py
=============
Player similarity model using cosine similarity on z-scored per-90 stats.

How it works
------------
1. Pull outfield_features or goalkeeper_features from BigQuery.
2. Filter to players with >= 450 minutes (serious sample size).
3. For the queried player's position group, select a position-specific
   set of features (feature template).
4. Z-score all feature columns so no single stat dominates by scale.
5. Compute cosine similarity between the queried player and every other
   player in the same position pool.
6. Return the top N most similar players with their similarity score.

Position feature templates
--------------------------
Each template is a curated list of per-90 and rate stats relevant to
that position group. Raw counting stats are excluded — they favour
players with more minutes.
"""

import numpy as np
import pandas as pd
from google.cloud import bigquery

# ── CONFIG — must match gcp_loader.py ────────────────────────────────────────
GCP_PROJECT = "scouting-466001"
BQ_DATASET  = "scouting"
# ─────────────────────────────────────────────────────────────────────────────

MIN_MINUTES = 450

# Feature templates: one list of columns per position group.
# All columns must exist in outfield_features / goalkeeper_features.
FEATURE_TEMPLATES = {
    "ATT": [
        "goals_per_90",
        "nonpen_goals_per_90",
        "xg_per_90",
        "npxg_per_90",
        "assists_per_90",
        "xag_per_90",
        "shots_total_per_90",
        "shots_on_target_per_90",
        "goals_per_shot",
        "npxg_per_shot",
        "goals_minus_xg",
        "key_passes",
        "passes_into_penalty_area",
        "progressive_carries",
        "passes_completion_pct_total",
    ],
    "MID": [
        "goals_per_90",
        "assists_per_90",
        "xag_per_90",
        "key_passes",
        "passes_completion_pct_total",
        "passes_into_final_third",
        "pass_progressive_passes",
        "progressive_carries",
        "tackles_total",
        "interceptions",
        "tackles_plus_interceptions",
        "shots_total_per_90",
        "npxg_per_90",
        "passes_into_penalty_area",
        "crosses_into_penalty_area",
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


def _fetch_features(bq_client: bigquery.Client, position_group: str) -> pd.DataFrame:
    """Pull feature table rows for a given position group from BigQuery."""
    if position_group == "GK":
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`"
    else:
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.outfield_features`"

    sql = f"""
        SELECT *
        FROM {table}
        WHERE position_group = '{position_group}'
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
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=GCP_PROJECT)

    # ── 1. Find the player's position group ──────────────────────────────────
    lookup_sql = f"""
        SELECT position_group
        FROM `{GCP_PROJECT}.{BQ_DATASET}.outfield_features`
        WHERE Player = '{player}' AND Season = '{season}'
        UNION ALL
        SELECT position_group
        FROM `{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`
        WHERE Player = '{player}' AND Season = '{season}'
        LIMIT 1
    """
    result = bq_client.query(lookup_sql).to_dataframe()
    if result.empty:
        raise ValueError(f"Player '{player}' not found for season '{season}'.")

    position_group = result.iloc[0]["position_group"]
    feature_cols   = FEATURE_TEMPLATES[position_group]

    # ── 2. Fetch full pool for that position group ────────────────────────────
    pool_df = _fetch_features(bq_client, position_group)

    if pool_df.empty:
        raise ValueError(f"No data found for position group '{position_group}'.")

    # ── 3. Locate the query player in the pool ────────────────────────────────
    query_mask = (pool_df["Player"] == player) & (pool_df["Season"] == season)
    if not query_mask.any():
        raise ValueError(
            f"'{player}' ({season}) is in the database but below the "
            f"{MIN_MINUTES}-minute threshold and cannot be used as a query."
        )

    # ── 4. Z-score the feature matrix ─────────────────────────────────────────
    X          = _zscore_matrix(pool_df, feature_cols)
    query_idx  = pool_df.index[query_mask][0]
    # Reindex because _zscore_matrix resets to positional index
    positional_idx = pool_df.reset_index(drop=True).index[
        (pool_df.reset_index(drop=True)["Player"] == player) &
        (pool_df.reset_index(drop=True)["Season"] == season)
    ][0]
    query_vec  = X[positional_idx]

    # ── 5. Cosine similarity against full pool ────────────────────────────────
    scores = _cosine_similarity(query_vec, X)

    pool_reset = pool_df.reset_index(drop=True).copy()
    pool_reset["similarity_score"] = scores

    # ── 6. Exclude the query player themselves, sort, return top N ────────────
    results = (
        pool_reset[
            ~((pool_reset["Player"] == player) & (pool_reset["Season"] == season))
        ]
        .sort_values("similarity_score", ascending=False)
        .head(top_n)[["Player", "Squad", "League", "Season", "position_group", "minutes", "similarity_score"]]
        .reset_index(drop=True)
    )

    return results


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

    for table in ["outfield_features", "goalkeeper_features"]:
        sql = f"""
            SELECT *
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{table}`
            WHERE Player = '{player}' AND Season = '{season}'
            LIMIT 1
        """
        df = bq_client.query(sql).to_dataframe()
        if not df.empty:
            return df.iloc[0]

    raise ValueError(f"Player '{player}' not found for season '{season}'.")
