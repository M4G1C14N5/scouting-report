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
from src.ui import metrics
from tests import fakes
from tests.schema_utils import newer_source_columns

ROOT = Path(__file__).resolve().parents[1]


class FakeJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_dataframe(self, **kwargs) -> pd.DataFrame:
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


# --- ranking -------------------------------------------------------------------

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


def test_full_large_pool_with_equal_minutes_ranks_exactly_as_before():
    # every row has the same minutes, so per 90 is a constant scale: z-scores unchanged
    rng = np.random.default_rng(5)
    cols = similarity.FEATURE_TEMPLATES["MID"]
    pool = _pool([{"Player": f"M{i}", "position_group": "MID", **{c: float(rng.normal()) for c in cols}}
                  for i in range(60)], season="2021-2022")
    got = similarity.rank_similar(pool, "M7", "2021-2022", cols, top_n=20)
    pd.testing.assert_frame_equal(got, _old_algorithm(pool, "M7", "2021-2022", cols, 20))


def _per_90_by_hand(pool: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Season totals (count, total, tally) divided by minutes / 90; rates as they are."""
    out = pool.copy()
    for c in cols:
        if metrics.KIND[c] in ("count", "total", "tally"):
            out[c] = out[c] / (out["minutes"] / 90)
    return out


@pytest.mark.parametrize("player, group", [("Mohamed Salah", "ATT"), ("Alisson", "GK")])
def test_full_fbref_rows_rank_on_per_90_totals(player, group):
    # The original algorithm on the FBref template stats, with every season total
    # (key passes, tackles, saves...) turned into a per-90 rate first.
    pool = fakes.position_pool(group, fakes.LATEST)
    cols = _fbref_part(group)
    assert pool[cols].notna().all().all()  # every FBref template stat present, as in FBref
    bq = FakeBQ(pool, group)
    got = similarity.find_similar_players(player, fakes.LATEST, top_n=10, bq_client=bq)
    expected = _old_algorithm(_per_90_by_hand(pool, cols), player, fakes.LATEST, cols, 10)
    expected["minutes"] = expected["minutes"].astype(got["minutes"].dtype)
    pd.testing.assert_frame_equal(got, expected)


def test_more_minutes_no_longer_look_like_a_different_player():
    # FBref style: R has Q's key passes, carries and tackles per 90 over twice the
    # minutes, S has Q's season totals. Per 90, R is Q's twin.
    rows = [
        {"Player": "Q", "minutes": 900, "key_passes": 10.0, "progressive_carries": 20.0,
         "tackles_total": 15.0, "passes_completion_pct_total": 80.0},
        {"Player": "R", "minutes": 1800, "key_passes": 20.0, "progressive_carries": 40.0,
         "tackles_total": 30.0, "passes_completion_pct_total": 80.0},
        {"Player": "S", "minutes": 1800, "key_passes": 10.0, "progressive_carries": 20.0,
         "tackles_total": 15.0, "passes_completion_pct_total": 80.0},
        {"Player": "T", "minutes": 2700, "key_passes": 60.0, "progressive_carries": 15.0,
         "tackles_total": 90.0, "passes_completion_pct_total": 70.0},
        {"Player": "U", "minutes": 900, "key_passes": 2.0, "progressive_carries": 40.0,
         "tackles_total": 5.0, "passes_completion_pct_total": 88.0},
    ]
    template = ["key_passes", "progressive_carries", "tackles_total",
                "passes_completion_pct_total"]
    out = similarity.rank_similar(_pool(rows), "Q", "2025", template, top_n=4)
    assert out["Player"].tolist()[0] == "R"
    assert out.iloc[0]["similarity_score"] == pytest.approx(1.0, abs=1e-6)


# --- newer-source features ----------------------------------------------------

NEWER_FEATURES = {
    "ATT": {"dribbles_completed", "dribble_success_pct", "xg_chain", "fouls_drawn",
            "duels_won_pct", "ga_shooting", "ga_receiving"},
    "MID": {"xg_chain", "xg_buildup", "dribbles_completed", "duels_won_pct",
            "passes_completed_over_expected_p100", "ga_passing",
            "ga_interrupting"},
    "DEF": {"duels_won_pct", "dribbled_past", "fouls_committed", "xg_buildup",
            "passes_completed_over_expected_p100", "ga_passing",
            "ga_interrupting", "avg_pass_distance", "avg_vertical_pass_distance"},
    "GK": {"match_rating"},
}


def test_templates_add_the_newer_source_features_after_the_fbref_ones():
    for group, newer in NEWER_FEATURES.items():
        template = similarity.FEATURE_TEMPLATES[group]
        assert set(template) - set(_fbref_part(group)) == newer, group
        assert template[-len(newer):] == [c for c in template if c in newer], group
        assert len(template) == len(set(template)), group


def test_every_season_total_in_a_template_is_compared_per_90():
    in_templates = {c for t in similarity._all_templates() for c in t}
    assert similarity.PER90_FEATURES == {
        c for c in in_templates if metrics.KIND[c] in ("count", "total", "tally")}
    assert {
        # FBref totals, compared as totals before
        "key_passes", "passes_into_final_third", "passes_into_penalty_area",
        "crosses_into_penalty_area", "progressive_carries", "progressive_passing_distance",
        "pass_progressive_passes", "tackles_total", "tackles_won", "tackles_def_3rd",
        "tackles_mid_3rd", "interceptions", "tackles_plus_interceptions", "blocks_total",
        "clearances", "errors_leading_to_shot", "saves",
        # newer-source totals
        "dribbles_completed", "xg_chain", "xg_buildup", "fouls_drawn", "fouls_committed",
        "dribbled_past", "ga_shooting", "ga_receiving", "ga_passing",
        "ga_interrupting"} <= similarity.PER90_FEATURES
    # rates, percentages, distances and stored per-90 columns stay as they are
    assert not {"goals_per_90", "save_pct", "duels_won_pct", "goals_per_shot", "match_rating",
                "avg_pass_distance", "passes_completed_over_expected_p100"} & (
        similarity.PER90_FEATURES)


def test_goalkeeper_template_has_saves_and_match_rating():
    template = similarity.FEATURE_TEMPLATES["GK"]
    assert template == ["save_pct", "goals_against_per_90", "clean_sheet_pct", "pk_save_pct",
                        "saves", "match_rating"]

def test_with_per90_divides_the_season_totals_by_90s():
    pool = _pool([{"Player": "Q", "minutes": 900, "dribbles_completed": 10.0,
                   "ga_shooting": 1.5, "key_passes": 20.0, "duels_won_pct": 55.0}])
    out = similarity.with_per90(pool, ["dribbles_completed", "ga_shooting", "key_passes",
                                       "duels_won_pct", "not_in_pool"])
    row = out.iloc[0]
    assert row["dribbles_completed"] == pytest.approx(1.0)
    assert row["ga_shooting"] == pytest.approx(0.15)
    assert row["key_passes"] == pytest.approx(2.0)  # FBref totals too
    assert row["duels_won_pct"] == 55.0  # rates stay as they are
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
           "key_passes", "shots_total_per_90", "passes_completion_pct_total",
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
                        "passes_completion_pct_total", "ga_shooting",
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


def test_goals_added_counts_once_through_its_parts():
    # the total is the sum of the parts: using both would weigh it twice
    for group, template in similarity.FEATURE_TEMPLATES.items():
        assert not {"goals_added", "goals_added_above_avg"} & set(template), group
    assert {"ga_shooting", "ga_receiving"} <= set(similarity.FEATURE_TEMPLATES["ATT"])
    assert {"ga_passing", "ga_interrupting"} <= set(similarity.FEATURE_TEMPLATES["MID"])
    assert {"ga_passing", "ga_interrupting"} <= set(similarity.FEATURE_TEMPLATES["DEF"])
    # still a Profile bar
    for group in ("ATT", "MID", "DEF"):
        assert "goals_added" in metrics.profile_keys(group), group


# --- compare modes on fixed family templates -------------------------------------

SHARED, BIG5, TRIAL = similarity.MODE_SHARED, similarity.MODE_BIG_FIVE, similarity.MODE_TRIAL


def test_mode_names():
    assert SHARED == "All leagues, shared stats"
    assert BIG5 == "Big five, all stats"
    assert TRIAL == "Europe and US together (trial)"


@pytest.mark.parametrize("season, paired", [
    # paired by the year the season ends: US Y with the European (Y-1)-Y
    ("2026", "2025-2026"), ("2025", "2024-2025"), ("2024", "2023-2024"),
    ("2025-2026", "2026"), ("2024-2025", "2025"), ("not a season", None),
])
def test_paired_season(season, paired):
    assert similarity.paired_season(season) == paired


@pytest.mark.parametrize("family, group, big_five, modes", [
    ("europe", "ATT", True, [SHARED, BIG5, TRIAL]),
    ("europe", "DEF", False, [SHARED, TRIAL]),
    ("us", "MID", False, [SHARED, TRIAL]),
    ("europe", "GK", True, [SHARED]),   # no big-five or trial mode for keepers
    ("fbref", "ATT", False, [SHARED]),  # FBref seasons keep their template, no trial
    ("fbref", "GK", False, [SHARED]),
])
def test_modes_offered(family, group, big_five, modes):
    assert similarity.modes_for(family, group, big_five) == modes


def test_mode_templates_are_fixed_family_sets():
    for group in ("ATT", "MID", "DEF"):
        # dribble success % is 0/0 for a player with no dribbles: compared through
        # dribbles attempted and completed instead (it stays a Profile and Stats bar)
        europe = [k for k in metrics.EUROPE_OUTFIELD if k != "dribble_success_pct"]
        assert similarity.mode_template(SHARED, "europe", group) == europe
        assert similarity.mode_template(BIG5, "europe", group) == [*europe, *metrics.BIG_FIVE_EXTRAS]
        assert "dribbles_attempted" in europe and "dribble_success_pct" in metrics.EUROPE_OUTFIELD
        assert similarity.mode_template(TRIAL, "europe", group) == list(metrics.SHARED_SET)
        assert similarity.mode_template(TRIAL, "us", group) == list(metrics.SHARED_SET)
        # the goals added total is the sum of its six parts: compared once, through the parts
        assert similarity.mode_template(SHARED, "us", group) == [
            k for k in metrics.US_OUTFIELD if k != "goals_added"]
        assert similarity.mode_template(SHARED, "fbref", group) == similarity.FEATURE_TEMPLATES[group]
    assert similarity.mode_template(SHARED, "europe", "GK") == list(metrics.EUROPE_GOALKEEPER)
    for template in (similarity.mode_template(m, f, "ATT") for m, f in
                     ((SHARED, "europe"), (SHARED, "us"), (BIG5, "europe"), (TRIAL, "us"))):
        assert not metrics.PATCHY & set(template)


def test_family_template_totals_are_compared_per_90():
    assert {"passes_attempted_total", "duels_total", "dribbles_attempted", "blocks_total",
            "yellow_cards", "ga_dribbling", "ga_fouling"} <= similarity.PER90_FEATURES
    pool = _pool([{"Player": "Q", "minutes": 900, "passes_attempted_total": 450.0,
                   "yellow_cards": 3.0, "match_rating": 7.1}])
    out = similarity.with_per90(pool, ["passes_attempted_total", "yellow_cards", "match_rating"])
    assert out.iloc[0]["passes_attempted_total"] == pytest.approx(45.0)
    assert out.iloc[0]["yellow_cards"] == pytest.approx(0.3)
    assert out.iloc[0]["match_rating"] == 7.1


def _europe_pool(paired: bool = False) -> pd.DataFrame:
    return fakes.new_position_pool("ATT", fakes.EUROPE_SEASON, paired=paired)


def test_shared_mode_compares_across_every_european_league_on_the_europe_set():
    out = similarity.similar_in_pool(_europe_pool(), "Prem Forward 3", fakes.EUROPE_SEASON,
                                     SHARED, top_n=50)
    assert set(out["League"]) == {"Premier League", "La Liga", "League Two"}
    assert "Gappy Forward" not in set(out["Player"])  # lacks a template stat: never a candidate
    assert "No Dribbles Forward" in set(out["Player"])  # 0/0 dribble success % does not exclude
    assert len(out) == 20  # 22 European forwards, minus the query and Gappy Forward
    assert list(out.columns) == ["Player", "Squad", "League", "Season", "position_group",
                                 "minutes", "similarity_score"]


def test_shared_mode_never_mixes_in_the_other_region():
    out = similarity.similar_in_pool(_europe_pool(paired=True), "Two Forward 0",
                                     fakes.EUROPE_SEASON, SHARED, top_n=50)
    assert set(out["Season"]) == {fakes.EUROPE_SEASON}


def test_big_five_mode_uses_big_five_players_only():
    out = similarity.similar_in_pool(_europe_pool(), "Prem Forward 3", fakes.EUROPE_SEASON,
                                     BIG5, top_n=50)
    assert set(out["League"]) == {"Premier League", "La Liga"}
    assert len(out) == 11


def test_big_five_mode_is_not_offered_outside_the_big_five():
    with pytest.raises(ValueError):
        similarity.similar_in_pool(_europe_pool(), "Two Forward 0", fakes.EUROPE_SEASON, BIG5)


def test_trial_mode_mixes_europe_and_the_paired_us_season():
    out = similarity.similar_in_pool(_europe_pool(paired=True), "Two Forward 0",
                                     fakes.EUROPE_SEASON, TRIAL, top_n=50)
    assert set(out["Season"]) == {fakes.EUROPE_SEASON, fakes.US_SEASON}
    assert "Gappy Forward" in set(out["Player"])  # dribbled past is not a shared stat
    us_pool = fakes.new_position_pool("ATT", fakes.US_SEASON, paired=True)
    out = similarity.similar_in_pool(us_pool, "US Forward 0", fakes.US_SEASON, TRIAL, top_n=50)
    assert set(out["Season"]) == {fakes.EUROPE_SEASON, fakes.US_SEASON}
    assert len(out) == 7 + 22


def test_a_player_without_dribbles_is_compared_not_blocked():
    out = similarity.similar_in_pool(_europe_pool(), fakes.NO_DRIBBLES, fakes.EUROPE_SEASON,
                                     SHARED, top_n=50)
    assert len(out) == 20


def test_trial_mode_without_a_paired_season_says_so():
    msg = "No paired European season with matching stats for this season."
    assert similarity.NO_PAIRED_SEASON == msg
    pool = fakes.new_position_pool("ATT", fakes.US_EARLY, paired=True)  # 2023-24 not loaded
    with pytest.raises(similarity.NoPairedSeasonError) as err:
        similarity.similar_in_pool(pool, "Early US Forward 0", fakes.US_EARLY, TRIAL, top_n=50)
    assert str(err.value) == msg
    # 2023-24 is an FBref season: its rows are no trial partner either
    fbref = fakes.position_pool("ATT", fakes.LATEST)
    assert set(fbref["Season"]) == {"2023-2024"}
    with pytest.raises(similarity.NoPairedSeasonError):
        similarity.similar_in_pool(pd.concat([pool, fbref], ignore_index=True), "Early US Forward 0",
                                   fakes.US_EARLY, TRIAL, top_n=50)
    # the shared mode still works for him
    assert len(similarity.similar_in_pool(pool, "Early US Forward 0", fakes.US_EARLY, SHARED)) == 5


def test_trial_note_says_how_seasons_are_paired():
    assert similarity.TRIAL_NOTE.startswith(
        "Trial: compares European and US players on the seven stats both regions have")
    assert "US 2026 with the European 2025-26" in similarity.TRIAL_NOTE


def test_trial_mode_is_not_offered_for_keepers_or_fbref():
    gk = fakes.new_position_pool("GK", fakes.EUROPE_SEASON)
    with pytest.raises(ValueError):
        similarity.similar_in_pool(gk, "Prem Keeper 0", fakes.EUROPE_SEASON, TRIAL)
    fbref = fakes.position_pool("ATT", fakes.LATEST)
    with pytest.raises(ValueError):
        similarity.similar_in_pool(fbref, "Mohamed Salah", fakes.LATEST, TRIAL)


def test_incomplete_query_player_gets_the_missing_stats_named():
    with pytest.raises(similarity.IncompleteDataError) as err:
        similarity.similar_in_pool(_europe_pool(), "Gappy Forward", fakes.EUROPE_SEASON, SHARED)
    assert str(err.value) == ("This player's data is incomplete for this comparison "
                              "(missing: Dribbled Past). Try another mode.")
    assert err.value.missing == ["dribbled_past"]


def test_fixed_templates_never_drop_a_feature():
    # Q lacks nothing, but only 3 others have every stat: no dropping to find 20.
    template = ["goals_per_90", "key_passes", "duels_won_pct"]
    rows = [{"Player": f"P{i}", "Source": "API-Football", "goals_per_90": 0.1 * i,
             "key_passes": 5.0 + i, "duels_won_pct": (50.0 + i if i < 4 else np.nan)}
            for i in range(30)]
    pool = _pool(rows, season="2025-2026")
    out = similarity.rank_fixed(pool, "P0", "2025-2026", template, top_n=50)
    assert sorted(out["Player"]) == ["P1", "P2", "P3"]


def test_europe_keeper_compares_on_the_keeper_set():
    gk = fakes.new_position_pool("GK", fakes.EUROPE_SEASON)
    out = similarity.similar_in_pool(gk, "Two Keeper 0", fakes.EUROPE_SEASON, SHARED, top_n=50)
    assert len(out) == 6 and set(out["position_group"]) == {"GK"}


@pytest.mark.parametrize("player, group", [("Mohamed Salah", "ATT"), ("Alisson", "GK")])
def test_fbref_shared_mode_is_the_unchanged_fbref_ranking(player, group):
    pool = fakes.position_pool(group, fakes.LATEST)
    got = similarity.similar_in_pool(pool, player, fakes.LATEST, SHARED, top_n=10)
    expected = similarity.rank_similar(pool, player, fakes.LATEST,
                                       similarity.FEATURE_TEMPLATES[group], 10)
    pd.testing.assert_frame_equal(got, expected)


def test_find_similar_players_in_trial_mode_fetches_the_paired_season():
    pool = _europe_pool(paired=True)
    bq = FakeBQ(pool, "ATT")
    out = similarity.find_similar_players("Two Forward 0", fakes.EUROPE_SEASON, top_n=50,
                                          bq_client=bq, mode=TRIAL)
    assert "Season IN ('2025-2026', '2026')" in " ".join(bq.calls[-1].split())
    assert fakes.US_SEASON in set(out["Season"])
