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
from tests.schema_utils import newer_source_columns

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


# --- feature and candidate selection -----------------------------------------

def test_player_features_keep_what_the_player_has_in_template_order():
    pool = _pool([
        {"Player": "Q", "a": 1.0, "b": np.nan, "c": 3.0},
        {"Player": "R", "a": 2.0, "b": 1.0, "c": 1.0},
    ])
    assert similarity.player_features(pool, pool.iloc[0], ["c", "b", "a", "missing"]) == ["c", "a"]


XG = ["xg_per_90", "npxg_per_90", "xag_per_90", "npxg_plus_xag_per_90"]
NO_XG = ["goals_per_90", "goals_per_shot", "assists_per_90", "key_passes", "shots_total_per_90",
         "passes_completion_pct_total"]


def _season_pool(big_five: int, championship: int) -> pd.DataFrame:
    """A 2025-26 ATT pool as the views return it: every template column, the
    FBref-only ones null for all, xG present for the Big 5 rows only."""
    rng = np.random.default_rng(3)
    template = similarity.FEATURE_TEMPLATES["ATT"]
    rows = []
    for league, n in (("Premier League", big_five), ("Championship", championship)):
        for i in range(n):
            rec = {c: np.nan for c in template}
            rec.update({c: float(rng.uniform(0.1, 3.0)) for c in NO_XG})
            if league == "Premier League":
                rec.update({c: float(rng.uniform(0.1, 1.0)) for c in XG})
            rec.update(Player=f"{league} {i}", League=league, Squad=f"Club {i}", position_group="ATT")
            rows.append(rec)
    return _pool(rows, season="2025-2026")


def test_big_five_query_keeps_xg_and_is_compared_only_with_big_five_rows():
    pool = _season_pool(big_five=30, championship=30)
    query = pool[pool["Player"] == "Premier League 0"].index[0]
    features, candidates = similarity.select_features(
        pool, pool["Player"] == "Premier League 0", similarity.FEATURE_TEMPLATES["ATT"])
    assert set(XG) <= set(features)
    assert set(pool.loc[candidates, "League"]) == {"Premier League"}
    assert not candidates[query]
    out = similarity.rank_similar(pool, "Premier League 0", "2025-2026",
                                  similarity.FEATURE_TEMPLATES["ATT"], top_n=40)
    assert len(out) == 29 and set(out["League"]) == {"Premier League"}


def test_championship_query_uses_the_non_xg_stats_and_matches_both_leagues():
    pool = _season_pool(big_five=30, championship=30)
    features, candidates = similarity.select_features(
        pool, pool["Player"] == "Championship 0", similarity.FEATURE_TEMPLATES["ATT"])
    assert not set(XG) & set(features)
    assert set(features) == set(NO_XG)
    out = similarity.rank_similar(pool, "Championship 0", "2025-2026",
                                  similarity.FEATURE_TEMPLATES["ATT"], top_n=60)
    assert len(out) == 59 and set(out["League"]) == {"Premier League", "Championship"}


def test_too_few_candidates_drop_the_lowest_coverage_feature_until_twenty():
    # Only 10 Big 5 rows have xG: a Big 5 query would have 9 candidates, so the
    # xG stats (the lowest coverage) go one by one and the Championship joins.
    pool = _season_pool(big_five=10, championship=30)
    features, candidates = similarity.select_features(
        pool, pool["Player"] == "Premier League 0", similarity.FEATURE_TEMPLATES["ATT"])
    assert set(features) == set(NO_XG)
    assert int(candidates.sum()) == 39


def test_dropping_stops_as_soon_as_there_are_twenty_candidates():
    pool = _pool([{"Player": f"P{i}", "a": 1.0 + i, "b": 2.0 * i, "c": 0.5 * i,
                   "d": (1.0 if i < 22 else np.nan), "e": (1.0 + i if i < 15 else np.nan)}
                  for i in range(30)])
    features, candidates = similarity.select_features(pool, pool["Player"] == "P0", ["a", "b", "c", "d", "e"])
    assert features == ["a", "b", "c", "d"]  # e (15/30) dropped; d (22/30) leaves 21 candidates
    assert int(candidates.sum()) == 21


def test_dropping_below_three_features_raises():
    pool = _pool([{"Player": f"P{i}", "a": 1.0 + i, "b": (1.0 if i < 12 else np.nan),
                   "c": (1.0 if i < 8 else np.nan)} for i in range(30)])
    with pytest.raises(similarity.NotEnoughStatsError):
        similarity.select_features(pool, pool["Player"] == "P0", ["a", "b", "c"])


def _fbref_part(group: str) -> list[str]:
    """The template features FBref rows have (the newer-source ones are null)."""
    table = "goalkeeper_features" if group == "GK" else "outfield_features"
    newer = newer_source_columns(table)
    return [c for c in similarity.FEATURE_TEMPLATES[group] if c not in newer]


def test_small_pool_with_every_stat_keeps_every_feature():
    # Under 20 others, but dropping a stat could not add a candidate: keep them all.
    pool = fakes.position_pool("ATT", fakes.LATEST)
    features, candidates = similarity.select_features(
        pool, pool["Player"] == "Mohamed Salah", similarity.FEATURE_TEMPLATES["ATT"])
    assert features == _fbref_part("ATT")
    assert int(candidates.sum()) == len(pool) - 1


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


def test_full_large_pool_ranks_exactly_as_before():
    rng = np.random.default_rng(5)
    cols = similarity.FEATURE_TEMPLATES["MID"]
    pool = _pool([{"Player": f"M{i}", "position_group": "MID", **{c: float(rng.normal()) for c in cols}}
                  for i in range(60)], season="2021-2022")
    got = similarity.rank_similar(pool, "M7", "2021-2022", cols, top_n=20)
    pd.testing.assert_frame_equal(got, _old_algorithm(pool, "M7", "2021-2022", cols, 20))


@pytest.mark.parametrize("player, group", [("Mohamed Salah", "ATT"), ("Alisson", "GK")])
def test_full_fbref_rows_rank_exactly_as_before(player, group):
    pool = fakes.position_pool(group, fakes.LATEST)
    cols = _fbref_part(group)
    assert pool[cols].notna().all().all()  # every FBref template stat present, as in FBref
    bq = FakeBQ(pool, group)
    got = similarity.find_similar_players(player, fakes.LATEST, top_n=10, bq_client=bq)
    pd.testing.assert_frame_equal(got, _old_algorithm(pool, player, fakes.LATEST, cols, 10))


# --- newer-source features ----------------------------------------------------

NEWER_FEATURES = {
    "ATT": {"dribbles_completed", "dribble_success_pct", "xg_chain", "fouls_drawn",
            "duels_won_pct", "goals_added", "ga_shooting", "ga_receiving"},
    "MID": {"xg_chain", "xg_buildup", "dribbles_completed", "duels_won_pct",
            "passes_completed_over_expected_p100", "goals_added", "ga_passing",
            "ga_interrupting"},
    "DEF": {"duels_won_pct", "dribbled_past", "fouls_committed", "xg_buildup",
            "passes_completed_over_expected_p100", "goals_added", "ga_passing",
            "ga_interrupting", "avg_pass_distance"},
    "GK": {"match_rating"},
}


def test_templates_add_the_newer_source_features_after_the_fbref_ones():
    for group, newer in NEWER_FEATURES.items():
        template = similarity.FEATURE_TEMPLATES[group]
        assert set(template) - set(_fbref_part(group)) == newer, group
        assert template[-len(newer):] == [c for c in template if c in newer], group
        assert len(template) == len(set(template)), group


def test_newer_season_totals_are_compared_per_90_and_the_fbref_ones_as_before():
    assert similarity.PER90_FEATURES == {
        "dribbles_completed", "xg_chain", "xg_buildup", "fouls_drawn", "fouls_committed",
        "dribbled_past", "goals_added", "ga_shooting", "ga_receiving", "ga_passing",
        "ga_interrupting"}
    # the original model compared these season totals as they are: unchanged
    assert similarity.SEASON_TOTAL_FEATURES == {
        "key_passes", "passes_into_final_third", "passes_into_penalty_area",
        "crosses_into_penalty_area", "progressive_carries", "progressive_passing_distance",
        "pass_progressive_passes", "tackles_total", "tackles_won", "tackles_def_3rd",
        "tackles_mid_3rd", "interceptions", "tackles_plus_interceptions", "blocks_total",
        "clearances", "errors_leading_to_shot", "shots_on_target_against"}


def test_with_per90_divides_only_the_newer_totals_by_90s():
    pool = _pool([{"Player": "Q", "minutes": 900, "dribbles_completed": 10.0,
                   "goals_added": 1.5, "key_passes": 20.0, "duels_won_pct": 55.0}])
    out = similarity.with_per90(pool, ["dribbles_completed", "goals_added", "key_passes",
                                       "duels_won_pct", "not_in_pool"])
    row = out.iloc[0]
    assert row["dribbles_completed"] == pytest.approx(1.0)
    assert row["goals_added"] == pytest.approx(0.15)
    assert row["key_passes"] == 20.0 and row["duels_won_pct"] == 55.0
    assert pool.iloc[0]["dribbles_completed"] == 10.0  # the input is not changed


def test_newer_counts_rank_per_90_not_by_season_total():
    # R has Q's per-90 rates over twice the minutes; S has Q's season totals.
    rows = [
        {"Player": "Q", "minutes": 900, "dribbles_completed": 10.0, "fouls_drawn": 20.0,
         "duels_won_pct": 50.0},
        {"Player": "R", "minutes": 1800, "dribbles_completed": 20.0, "fouls_drawn": 40.0,
         "duels_won_pct": 50.0},
        {"Player": "S", "minutes": 1800, "dribbles_completed": 10.0, "fouls_drawn": 20.0,
         "duels_won_pct": 50.0},
        {"Player": "T", "minutes": 1800, "dribbles_completed": 60.0, "fouls_drawn": 10.0,
         "duels_won_pct": 40.0},
        {"Player": "U", "minutes": 900, "dribbles_completed": 2.0, "fouls_drawn": 30.0,
         "duels_won_pct": 60.0},
    ]
    out = similarity.rank_similar(_pool(rows), "Q", "2025",
                                  ["dribbles_completed", "fouls_drawn", "duels_won_pct"], top_n=4)
    assert out["Player"].tolist()[0] == "R"
    assert out.iloc[0]["similarity_score"] == pytest.approx(1.0, abs=1e-6)


def _us_and_europe_pool() -> pd.DataFrame:
    """2026-style US rows (ASA: xG and goals added, no duels or dribbles) and
    2025-26-style League Two rows (API match data: duels and dribbles, no xG)."""
    rng = np.random.default_rng(9)
    template = similarity.FEATURE_TEMPLATES["ATT"]
    asa = ["goals_per_90", "xg_per_90", "goals_per_shot", "assists_per_90", "xag_per_90",
           "key_passes", "shots_total_per_90", "passes_completion_pct_total", "goals_added",
           "ga_shooting", "ga_receiving"]
    api = ["goals_per_90", "goals_per_shot", "assists_per_90", "key_passes",
           "shots_total_per_90", "passes_completion_pct_total", "dribbles_completed",
           "dribble_success_pct", "fouls_drawn", "duels_won_pct"]
    rows = []
    for league, have in (("USL League One", asa), ("League Two", api)):
        for i in range(25):
            rec = {c: np.nan for c in template}
            rec.update({c: float(rng.uniform(0.1, 3.0)) for c in have})
            rec.update(Player=f"{league} {i}", League=league, Squad=f"Club {i}",
                       minutes=float(rng.integers(450, 3000)))
            rows.append(rec)
    return _pool(rows)


@pytest.mark.parametrize("league, expected", [
    ("USL League One", {"goals_per_90", "xg_per_90", "goals_per_shot", "assists_per_90",
                        "xag_per_90", "key_passes", "shots_total_per_90",
                        "passes_completion_pct_total", "goals_added", "ga_shooting",
                        "ga_receiving"}),
    ("League Two", {"goals_per_90", "goals_per_shot", "assists_per_90", "key_passes",
                    "shots_total_per_90", "passes_completion_pct_total", "dribbles_completed",
                    "dribble_success_pct", "fouls_drawn", "duels_won_pct"}),
])
def test_each_era_uses_its_own_stats(league, expected):
    pool = _us_and_europe_pool()
    query = pool["Player"] == f"{league} 0"
    features, candidates = similarity.select_features(
        pool, query, similarity.FEATURE_TEMPLATES["ATT"])
    assert set(features) == expected and len(features) >= 8
    assert set(pool.loc[candidates, "League"]) == {league}
    out = similarity.rank_similar(pool, f"{league} 0", "2025",
                                  similarity.FEATURE_TEMPLATES["ATT"], top_n=30)
    assert len(out) == 24 and set(out["League"]) == {league}
