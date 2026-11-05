"""similarity.py: table config, the available-feature filter and the FBref path.

A fake client stands in for BigQuery; nothing here touches the network.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src import similarity
from tests import fakes

ROOT = Path(__file__).resolve().parents[1]


class FakeJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_dataframe(self) -> pd.DataFrame:
        return self._df


class FakeBQ:
    """Answers the position-group lookup and the pool query; records the SQL."""

    def __init__(self, pool: pd.DataFrame, group: str) -> None:
        self.pool = pool
        self.group = group
        self.calls: list[str] = []

    def query(self, sql: str, job_config=None) -> FakeJob:
        self.calls.append(sql)
        if "SELECT position_group" in sql:
            return FakeJob(pd.DataFrame({"position_group": [self.group]}))
        return FakeJob(self.pool)


def _pool(rows: list[dict], season: str = "2025") -> pd.DataFrame:
    base = {"Squad": "Club", "League": "MLS", "Season": season, "position_group": "ATT", "minutes": 2000}
    return pd.DataFrame([{**base, **r} for r in rows])


# --- table config -------------------------------------------------------------

def test_tables_default_to_the_combined_views():
    assert similarity.OUTFIELD_TABLE == "outfield_all"
    assert similarity.GOALKEEPER_TABLE == "goalkeeper_all"


def test_tables_can_be_set_from_the_environment():
    env = {**os.environ, "BQ_OUTFIELD_TABLE": "outfield_features", "BQ_GOALKEEPER_TABLE": "gk_x"}
    out = subprocess.run(
        [sys.executable, "-c",
         "from src import similarity as s; print(s.OUTFIELD_TABLE, s.GOALKEEPER_TABLE)"],
        cwd=ROOT, env=env, capture_output=True, text=True, check=True,
    )
    assert out.stdout.split() == ["outfield_features", "gk_x"]


def test_find_similar_players_queries_only_the_configured_views():
    pool = fakes.outfield_frame()
    pool = pool[pool["Season"] == fakes.LATEST].reset_index(drop=True)
    bq = FakeBQ(pool, "ATT")
    similarity.find_similar_players("Mohamed Salah", fakes.LATEST, top_n=3, bq_client=bq)
    sql = "\n".join(bq.calls)
    assert f"{similarity.BQ_DATASET}.outfield_all`" in sql
    assert f"{similarity.BQ_DATASET}.goalkeeper_all`" in sql  # the lookup checks both views
    assert "outfield_features`" not in sql and "goalkeeper_features`" not in sql


def test_get_player_stats_reads_the_views():
    row = pd.DataFrame({"Player": ["A"], "Season": ["2025"], "position_group": ["GK"]})
    bq = FakeBQ(row, "GK")
    assert similarity.get_player_stats("A", "2025", bq)["Player"] == "A"
    assert "outfield_all`" in bq.calls[0]


# --- available-feature filter -------------------------------------------------

def test_usable_features_drop_what_the_player_lacks():
    pool = _pool([
        {"Player": "Q", "a": 1.0, "b": np.nan, "c": 3.0},
        {"Player": "R", "a": 2.0, "b": 1.0, "c": 1.0},
    ])
    assert similarity.usable_features(pool, pool.iloc[0], ["a", "b", "c"]) == ["a", "c"]


def test_usable_features_need_half_the_pool():
    pool = _pool([
        {"Player": "Q", "a": 1.0, "b": 1.0, "c": 1.0},
        {"Player": "R", "a": 2.0, "b": 2.0, "c": np.nan},
        {"Player": "S", "a": 3.0, "b": np.nan, "c": np.nan},
        {"Player": "T", "a": 4.0, "b": np.nan, "c": np.nan},
    ])
    # a: 4/4, b: 2/4 (exactly half, kept), c: 1/4 (dropped)
    assert similarity.usable_features(pool, pool.iloc[0], ["a", "b", "c"]) == ["a", "b"]


def test_usable_features_skip_columns_the_table_does_not_have():
    pool = _pool([{"Player": "Q", "a": 1.0}, {"Player": "R", "a": 2.0}])
    assert similarity.usable_features(pool, pool.iloc[0], ["a", "missing"]) == ["a"]


def test_missing_stats_change_the_ranking_to_the_shared_ones():
    # Q has no "b". On a and c, R is Q's twin; S only looks close through b,
    # which Q lacks, so R must come first.
    pool = _pool([
        {"Player": "Q", "a": 1.0, "b": np.nan, "c": 1.0, "d": 1.0},
        {"Player": "R", "a": 1.0, "b": -5.0, "c": 1.0, "d": 1.0},
        {"Player": "S", "a": -1.0, "b": 9.0, "c": -1.0, "d": -1.0},
        {"Player": "T", "a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0},
    ])
    out = similarity.rank_similar(pool, "Q", "2025", ["a", "b", "c", "d"], top_n=3)
    assert out["Player"].tolist()[0] == "R"
    assert out.iloc[0]["similarity_score"] == pytest.approx(1.0, abs=1e-6)


def test_fewer_than_three_shared_stats_raise_a_plain_error():
    pool = _pool([
        {"Player": "Q", "a": 1.0, "b": 2.0, "c": np.nan, "d": np.nan},
        {"Player": "R", "a": 2.0, "b": 1.0, "c": 1.0, "d": 1.0},
    ])
    with pytest.raises(similarity.NotEnoughStatsError) as err:
        similarity.rank_similar(pool, "Q", "2025", ["a", "b", "c", "d"], top_n=3)
    assert isinstance(err.value, ValueError)
    assert str(err.value) == "Not enough comparable stats for this player in this season"
    assert similarity.NOT_ENOUGH_STATS == str(err.value)


def test_find_similar_players_raises_not_enough_stats_through_bigquery_path():
    template = similarity.FEATURE_TEMPLATES["DEF"]
    rows = []
    for name in ("Q", "R", "S"):
        rec = {c: np.nan for c in template}
        rec.update(Player=name, position_group="DEF", goals_per_90=0.1, assists_per_90=0.2)
        rows.append(rec)
    bq = FakeBQ(_pool(rows), "DEF")
    with pytest.raises(similarity.NotEnoughStatsError):
        similarity.find_similar_players("Q", "2025", bq_client=bq)


# --- FBref behaviour unchanged ------------------------------------------------

def _old_algorithm(pool: pd.DataFrame, player: str, season: str, cols: list[str], top_n: int) -> pd.DataFrame:
    """The pre-refresh steps 4-6 of find_similar_players, verbatim in effect."""
    X = similarity._zscore_matrix(pool, cols)
    reset = pool.reset_index(drop=True)
    idx = reset.index[(reset["Player"] == player) & (reset["Season"] == season)][0]
    scores = similarity._cosine_similarity(X[idx], X)
    reset = reset.copy()
    reset["similarity_score"] = scores
    return (reset[~((reset["Player"] == player) & (reset["Season"] == season))]
            .sort_values("similarity_score", ascending=False)
            .head(top_n)[["Player", "Squad", "League", "Season", "position_group", "minutes", "similarity_score"]]
            .reset_index(drop=True))


@pytest.mark.parametrize("player, group", [("Mohamed Salah", "ATT"), ("Alisson", "GK")])
def test_full_fbref_rows_rank_exactly_as_before(player, group):
    pool = fakes.position_pool(group, fakes.LATEST)
    cols = similarity.FEATURE_TEMPLATES[group]
    assert pool[cols].notna().all().all()  # every template stat present, as in FBref
    bq = FakeBQ(pool, group)
    got = similarity.find_similar_players(player, fakes.LATEST, top_n=10, bq_client=bq)
    pd.testing.assert_frame_equal(got, _old_algorithm(pool, player, fakes.LATEST, cols, 10))
