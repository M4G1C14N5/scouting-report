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
   set of features (feature template). Newer sources leave some stats
   empty (no progressive passing after 2023-24, no tackles in the US
   leagues, no xG outside the Big 5 for most leagues), so keep only the
   features the queried player has and compare only with candidates who
   have every one of them. While there are fewer than 20 candidates, drop
   the feature the pool has least often and look again. Fewer than 3
   features left raises NotEnoughStatsError. See select_features.
4. Z-score the kept feature columns over the candidates and the queried
   player, so no single stat dominates by scale.
5. Compute cosine similarity between the queried player and every
   candidate.
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
MIN_CANDIDATES = 20       # drop the rarest stat while fewer players than this have them all
NOT_ENOUGH_STATS = "Not enough comparable stats for this player in this season"


class NotEnoughStatsError(ValueError):
    """The player-season shares too few template stats with its pool."""

    def __init__(self, message: str = NOT_ENOUGH_STATS) -> None:
        super().__init__(message)

# Feature templates: one list of columns per position group.
# All columns must exist in outfield_features / goalkeeper_features.
# Rows from the newer sources may leave some of them null; see select_features.
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


def player_features(pool_df: pd.DataFrame, query_row, template: list[str]) -> list[str]:
    """Template features the query player has (non-null), in template order."""
    return [c for c in template if c in pool_df.columns and not pd.isna(query_row.get(c))]


def select_features(
    pool_df: pd.DataFrame,
    query_mask: pd.Series,
    template: list[str],
) -> tuple[list[str], pd.Series]:
    """(features, candidates) for the query row marked by `query_mask`.

    features: the template features the query player has. candidates: the
    other pool rows that have every one of those features. While there are
    fewer than MIN_CANDIDATES candidates, the feature with the lowest
    coverage in the pool is dropped (the first in template order on a tie)
    and the candidates are recomputed. Dropping stops early when every other
    row is already a candidate, since no drop could add one: a small pool
    with every stat present keeps the full template.

    Raises NotEnoughStatsError when fewer than MIN_FEATURES features remain."""
    query_row = pool_df[query_mask].iloc[0]
    features = player_features(pool_df, query_row, template)
    others = ~query_mask
    while True:
        if len(features) < MIN_FEATURES:
            raise NotEnoughStatsError()
        candidates = pool_df[features].notna().all(axis=1) & others
        n = int(candidates.sum())
        if n >= MIN_CANDIDATES or n == int(others.sum()):
            return features, candidates
        rarest = pool_df[features].notna().mean().idxmin()
        features = [c for c in features if c != rarest]


def rank_similar(
    pool_df: pd.DataFrame,
    player: str,
    season: str,
    template: list[str],
    top_n: int,
) -> pd.DataFrame:
    """Steps 3-6 on a fetched pool: select the features and candidates,
    z-score over the candidates and the query player, cosine, top N. Raises
    NotEnoughStatsError (see select_features). With every stat present for
    every row this is exactly the original full-template ranking."""
    pool_reset = pool_df.reset_index(drop=True)
    query_mask = (pool_reset["Player"] == player) & (pool_reset["Season"] == season)
    feature_cols, candidates = select_features(pool_reset, query_mask, template)

    scored = pool_reset[candidates | query_mask].reset_index(drop=True).copy()
    scored_query = (scored["Player"] == player) & (scored["Season"] == season)
    positional_idx = scored.index[scored_query][0]

    # ── 4. Z-score the feature matrix ─────────────────────────────────────────
    X         = _zscore_matrix(scored, feature_cols)
    query_vec = X[positional_idx]

    # ── 5. Cosine similarity against the candidates ───────────────────────────
    scored["similarity_score"] = _cosine_similarity(query_vec, X)

    # ── 6. Exclude the query player themselves, sort, return top N ────────────
    return (
        scored[~scored_query]
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
        select_features). It is a ValueError.
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
