"""data.py: same SQL as the old app.py, cached, never hits BigQuery in tests."""

from __future__ import annotations

import pandas as pd
import pytest
import streamlit as st

from src.ui import data


class FakeJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_dataframe(self, **kwargs) -> pd.DataFrame:
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


def test_load_player_index_reads_both_views_with_source():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    out = data.load_player_index(bq)
    expected = (
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes, Source "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.outfield_all` "
        f"UNION ALL "
        f"SELECT Player, Season, Squad, League, Position, position_group, minutes, Source "
        f"FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.goalkeeper_all` "
        f"ORDER BY Player, Season"
    )
    assert norm(bq.calls[0]) == expected
    assert out["Player"].tolist() == ["A"]


def test_data_uses_the_similarity_table_config():
    assert data.OUTFIELD_TABLE == "outfield_all" and data.GOALKEEPER_TABLE == "goalkeeper_all"


def test_load_player_index_is_cached():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.load_player_index(bq)
    data.load_player_index(bq)
    assert len(bq.calls) == 1


@pytest.mark.parametrize("group, table", [("GK", "goalkeeper_all"), ("ATT", "outfield_all"),
                                          ("MID", "outfield_all"), ("DEF", "outfield_all")])
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


def test_position_pool_with_the_paired_season():
    bq = FakeBQ(pd.DataFrame({"Player": ["A"]}))
    data.position_pool(bq, "ATT", "2025-2026", paired=True)
    data.position_pool(bq, "MID", "2026", paired=True)
    assert norm(bq.calls[0]) == (
        f"SELECT * FROM `{data.GCP_PROJECT}.{data.BQ_DATASET}.outfield_all` "
        f"WHERE position_group = 'ATT' AND Season IN ('2025-2026', '2026') AND minutes >= 450")
    assert "Season IN ('2026', '2025-2026')" in norm(bq.calls[1])
    data.position_pool(bq, "ATT", "2025-2026")  # not paired: a different cache entry
    assert norm(bq.calls[2]).endswith("Season = '2025-2026' AND minutes >= 450")


def _record_pool(monkeypatch, seen):
    def fake_pool(_bq, group, season, paired=False):
        seen.append(("pool", group, season, paired))
        return pd.DataFrame({"Player": ["A"]})

    def fake_rank(pool, player, season, mode, top_n):
        seen.append(("rank", player, season, mode, top_n))
        return pd.DataFrame({"Player": ["B"], "similarity_score": [0.9]})

    monkeypatch.setattr(data, "position_pool", fake_pool)
    monkeypatch.setattr(data, "similar_in_pool", fake_rank)


def test_similar_players_ranks_the_cached_pool_for_the_max_once(monkeypatch):
    seen = []
    _record_pool(monkeypatch, seen)
    bq = object()
    data.similar_players(bq, "A", "2025-2026", "ATT", data.MODE_SHARED)
    data.similar_players(bq, "A", "2025-2026", "ATT", data.MODE_SHARED)
    assert seen == [("pool", "ATT", "2025-2026", False),
                    ("rank", "A", "2025-2026", data.MODE_SHARED, data.SIMILAR_MAX)]
    data.similar_players(bq, "A", "2025-2026", "MID", data.MODE_SHARED)  # group is in the cache key
    assert len(seen) == 4


def test_similar_players_trial_mode_fetches_the_paired_season(monkeypatch):
    seen = []
    _record_pool(monkeypatch, seen)
    data.similar_players(object(), "A", "2025-2026", "ATT", data.MODE_TRIAL)
    assert seen[0] == ("pool", "ATT", "2025-2026", True)
    assert seen[1][3] == data.MODE_TRIAL


def test_errors_are_not_cached(monkeypatch):
    calls = []

    def boom(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("bigquery down")

    monkeypatch.setattr(data, "position_pool", boom)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            data.similar_players(object(), "A", "2023-2024", "ATT")
    assert len(calls) == 2
