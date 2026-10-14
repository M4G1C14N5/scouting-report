"""Cached BigQuery access for the UI.

The SQL is the same text as the pre-redesign app.py (load_player_index and
the Percentile tab's position-pool query). Every query is cached for an
hour, which fixes the old app re-running 4-5 queries on every widget
click. The BigQuery client is passed as `_bq`: the leading underscore
tells st.cache_data not to hash it.

Call these through the module (`data.position_pool(...)`) so the tests can
monkeypatch them.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from google.cloud import bigquery

from src.similarity import (
    BQ_DATASET,
    GCP_PROJECT,
    MIN_MINUTES,
    find_similar_players,
    get_player_stats,
)

TTL = 3600
SIMILAR_MAX = 20  # the slider's max; views slice .head(top_n)


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


@st.cache_data(ttl=TTL, show_spinner="Loading players…")
def load_player_index(_bq) -> pd.DataFrame:
    sql = f"""
        SELECT Player, Season, Squad, League, Position, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.outfield_features`
        UNION ALL
        SELECT Player, Season, Squad, League, Position, position_group, minutes
        FROM `{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`
        ORDER BY Player, Season
    """
    return _bq.query(sql).to_dataframe()


@st.cache_data(ttl=TTL, show_spinner="Loading player stats…")
def player_stats(_bq, player: str, season: str) -> pd.Series:
    return get_player_stats(player, season, _bq)


@st.cache_data(ttl=TTL, show_spinner="Loading the position pool…")
def position_pool(_bq, position_group: str, season: str) -> pd.DataFrame:
    """Everyone in the group and season with MIN_MINUTES+ minutes, fetched once and
    shared by the Profile and Similar tabs."""
    if position_group == "GK":
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features`"
    else:
        table = f"`{GCP_PROJECT}.{BQ_DATASET}.outfield_features`"

    pool_sql = f"""
        SELECT *
        FROM {table}
        WHERE position_group = '{position_group}'
          AND Season = '{season}'
          AND minutes >= {MIN_MINUTES}
    """
    return _bq.query(pool_sql).to_dataframe()


@st.cache_data(ttl=TTL, show_spinner="Finding similar players…")
def similar_players(_bq, player: str, season: str, position_group: str) -> pd.DataFrame:
    """Top SIMILAR_MAX matches. position_group is part of the cache key only."""
    return find_similar_players(player=player, season=season, top_n=SIMILAR_MAX, bq_client=_bq)
