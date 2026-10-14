"""data.py: same SQL as the old app.py, cached, never hits BigQuery in tests."""

from __future__ import annotations

import pandas as pd
import pytest
import streamlit as st

from src.ui import data


class FakeJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_dataframe(self) -> pd.DataFrame:
        return self._df


class FakeBQ:
    """Records every SQL string; returns a fixed frame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.calls: list[str] = []

    def query(self, sql: str, job_config=None) -> FakeJob:
        self.calls.append(sql)
        return FakeJob(self.df)


@pytest.fixture(autouse=True)
def clear_caches():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def norm(sql: str) -> str:
    return " ".join(sql.split())


def test_load_player_index_runs_todays_sql():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    out = data.load_player_index(bq)
    expected = (
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.outfield_features` "
        f"UNION ALL "
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.goalkeeper_features` "
        f"ORDER BY Player, Season"
    )
    assert norm(bq.calls[0]) == expected
    assert out["Player"].tolist() == ["A"]


def test_load_player_index_is_cached():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.load_player_index(bq)
    data.load_player_index(bq)
    assert len(bq.calls) == 1


@pytest.mark.parametrize("group, table", [("GK", "goalkeeper_features"), ("ATT", "outfield_features"),
                                          ("MID", "outfield_features"), ("DEF", "outfield_features")])
def test_position_pool_runs_todays_sql(group, table):
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.position_pool(bq, group, "2023-2024")
    expected = (
        f"SELECT * FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.{table}` "
        f"WHERE position_group = '{group}' AND Season = '2023-2024' AND minutes >= 450"
    )
    assert norm(bq.calls[0]) == expected


def test_position_pool_cache_key_is_group_and_season():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.position_pool(bq, "ATT", "2023-2024")
    data.position_pool(bq, "ATT", "2023-2024")
    data.position_pool(bq, "MID", "2023-2024")
    data.position_pool(bq, "ATT", "2022-2023")
    assert len(bq.calls) == 3


def test_player_stats_wraps_get_player_stats(monkeypatch):
    seen = []

    def fake(player, season, bq_client=None):
        seen.append((player, season, bq_client))
        return pd.Series({"Player": player, "Season": season})

    monkeypatch.setattr(data, "get_player_stats", fake)
    bq = object()
    row = data.player_stats(bq, "A", "2023-2024")
    data.player_stats(bq, "A", "2023-2024")
    assert row["Player"] == "A"
    assert seen == [("A", "2023-2024", bq)]  # second call served from cache


def test_similar_players_asks_for_the_max_once(monkeypatch):
    seen = []

    def fake(**kwargs):
        seen.append(kwargs)
        return pd.DataFrame({"Player": ["B"], "similarity_score": [0.9]})

    monkeypatch.setattr(data, "find_similar_players", fake)
    bq = object()
    data.similar_players(bq, "A", "2023-2024", "ATT")
    data.similar_players(bq, "A", "2023-2024", "ATT")
    assert seen == [{"player": "A", "season": "2023-2024", "top_n": data.SIMILAR_MAX, "bq_client": bq}]
    data.similar_players(bq, "A", "2023-2024", "MID")  # position_group is part of the cache key
    assert len(seen) == 2


def test_errors_are_not_cached(monkeypatch):
    calls = []

    def boom(**kwargs):
        calls.append(1)
        raise RuntimeError("bigquery down")

    monkeypatch.setattr(data, "find_similar_players", boom)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            data.similar_players(object(), "A", "2023-2024", "ATT")
    assert len(calls) == 2
