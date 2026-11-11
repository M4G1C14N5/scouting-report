"""Cached BigQuery access for the UI.

The SQL is the pre-redesign app.py's (load_player_index and the Percentile
tab's position-pool query), now reading the tables named in
src/similarity.py (OUTFIELD_TABLE, GOALKEEPER_TABLE: the outfield_all and
goalkeeper_all views by default), with Source in the index. Every query is cached for an
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
    GOALKEEPER_TABLE,
    MIN_MINUTES,
    MODE_SHARED,
    MODE_TRIAL,
    OUTFIELD_TABLE,
    get_player_stats,
    pool_sql,
    similar_in_pool,
    table_ref,
)

TTL = 3600
SIMILAR_MAX = 20  # the slider's max; views slice .head(top_n)


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


@st.cache_data(ttl=TTL, show_spinner="Loading players…")
def load_player_index(_bq) -> pd.DataFrame:
    sql = f"""
        SELECT Player, Season, Squad, League, Position, position_group, minutes, Source
        FROM {table_ref(OUTFIELD_TABLE)}
        UNION ALL
        SELECT Player, Season, Squad, League, Position, position_group, minutes, Source
        FROM {table_ref(GOALKEEPER_TABLE)}
        ORDER BY Player, Season
    """
    return _bq.query(sql).to_dataframe(create_bqstorage_client=False)


@st.cache_data(ttl=TTL, show_spinner="Loading player stats…")
def player_stats(_bq, player: str, season: str) -> pd.Series:
    return get_player_stats(player, season, _bq)


@st.cache_data(ttl=TTL, show_spinner="Loading the position pool…")
def position_pool(_bq, position_group: str, season: str, paired: bool = False) -> pd.DataFrame:
    """Everyone in the group and season with MIN_MINUTES+ minutes, fetched once and
    shared by the Profile and Similar tabs. paired=True adds the other region's
    paired season (src.similarity.paired_season), for the trial compare mode."""
    return _bq.query(pool_sql(position_group, season, paired)).to_dataframe(create_bqstorage_client=False)


@st.cache_data(ttl=TTL, show_spinner="Finding similar players…")
def similar_players(_bq, player: str, season: str, position_group: str,
                    mode: str = MODE_SHARED) -> pd.DataFrame:
    """Top SIMILAR_MAX matches in a compare mode, ranked on the cached position
    pool (with the paired season for the trial mode)."""
    pool = position_pool(_bq, position_group, season, paired=mode == MODE_TRIAL)
    return similar_in_pool(pool, player, season, mode, SIMILAR_MAX)
