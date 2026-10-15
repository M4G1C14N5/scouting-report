from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ui import build


def test_fmt_value_uses_the_metric_kind():
    assert build.fmt_value("minutes", 2536) == "2,536"
    assert build.fmt_value("std_xg", 19.24) == "19.2"
    assert build.fmt_value("goals_per_90", 0.678) == "0.68"
    assert build.fmt_value("save_pct", 71.24) == "71.2%"
    assert build.fmt_value("std_goals", None) == "—"


def _def_pool() -> pd.DataFrame:
    return pd.DataFrame({
        "Player": ["A", "B", "C", "D"],
        "Season": ["2023-2024"] * 4,
        "tackles_total": [10, 20, 30, 40],
        "errors_leading_to_shot": [0, 1, 2, 3],
        "tackles_won": [5, 6, np.nan, 8],
        "goals_per_90": [0.1, 0.2, 0.3, 0.4],
    })


def test_profile_rows_inverts_lower_is_better_stats():
    pool = _def_pool()
    rows = build.profile_rows(pool, pool.iloc[3], "DEF")
    defending = {r.key: r for r in rows["Defending"]}
    assert defending["tackles_total"].pct == 87.5
    assert defending["errors_leading_to_shot"].pct == 12.5  # most errors = worst
    assert defending["tackles_total"].label == "Tackles"
    assert defending["tackles_total"].value == 40.0


def test_profile_rows_skip_missing_values_columns_and_empty_categories():
    pool = _def_pool()
    rows = build.profile_rows(pool, pool.iloc[2], "DEF")  # C has NaN tackles_won
    keys = [r.key for r in rows["Defending"]]
    assert "tackles_won" not in keys
    assert "Passing" not in rows  # no passing columns in the pool
    assert [r.key for r in rows["Shooting"]] == ["goals_per_90"]


def test_profile_rows_follow_metric_order():
    pool = _def_pool()
    keys = [r.key for r in build.profile_rows(pool, pool.iloc[0], "DEF")["Defending"]]
    assert keys == ["tackles_total", "tackles_won", "errors_leading_to_shot"]


def test_profile_rows_unknown_group_is_empty():
    assert build.profile_rows(_def_pool(), _def_pool().iloc[0], "XYZ") == {}


def test_profile_rows_skip_a_metric_when_the_pool_has_no_values_for_it():
    # percentile_of returns None for an all-NaN pool column: the row is skipped, no crash
    pool = _def_pool().assign(tackles_won=np.nan)
    player = pool.iloc[0].copy()
    player["tackles_won"] = 7.0  # the player has a value but there is nothing to rank against
    keys = [r.key for r in build.profile_rows(pool, player, "DEF")["Defending"]]
    assert "tackles_won" not in keys
    assert "tackles_total" in keys


def test_profile_rows_never_hand_none_or_nan_to_callers():
    pool = _def_pool()
    for i in range(len(pool)):
        for rows in build.profile_rows(pool, pool.iloc[i], "DEF").values():
            for r in rows:
                assert r.pct is not None and not np.isnan(r.pct)
                assert r.value is not None and not np.isnan(r.value)


def test_compare_lists_keeps_shared_metrics_in_a_order():
    a = {"Shooting": [build.PctRow("g", "Goals", 80.0, 1.0), build.PctRow("x", "xG", 60.0, 1.0)]}
    b = {"Shooting": [build.PctRow("x", "xG", 20.0, 1.0)]}
    assert build.compare_lists(a, b) == (["xG"], [60.0], [20.0])


def test_stat_table_pairs_totals_with_stored_and_computed_per90():
    row = pd.Series({
        "nineties_played": 20.0, "std_goals": 10, "goals_per_90": 0.5, "std_xg": 8.04,
        "xg_per_90": 0.4, "shots_total": 50, "shots_on_target_pct": 41.27,
        "npxg_plus_xag_per_90": 0.71, "penalty_kicks_made": 3,
    })
    table = build.stat_table(row, "Shooting")
    assert tuple(table.columns) == build.STAT_TABLE_COLUMNS
    by = table.set_index("Stat")
    assert by.loc["Goals", "Total"] == 10 and by.loc["Goals", "Per 90"] == 0.5
    assert by.loc["xG", "Total"] == 8.0 and by.loc["xG", "Per 90"] == 0.4
    assert by.loc["Shots", "Per 90"] == 2.5          # computed 50 / 20
    assert by.loc["Shot Accuracy %", "Total"] == 41.3
    assert pd.isna(by.loc["Shot Accuracy %", "Per 90"])  # rates have no per 90
    assert pd.isna(by.loc["Penalties Made", "Per 90"])   # tallies have no per 90
    assert pd.isna(by.loc["npxG + xAG / 90", "Total"])
    assert by.loc["npxG + xAG / 90", "Per 90"] == 0.71
    assert "Goals / 90" not in by.index  # merged into the Goals row
    assert table["Total"].dtype == "float64" and table["Per 90"].dtype == "float64"
    assert list(table["Stat"])[:2] == ["Goals", "Penalties Made"]  # metric order, missing rows dropped


def test_stat_table_for_a_category_the_row_lacks_is_empty():
    gk_row = pd.Series({"nineties_played": 30.0, "wins": 10, "saves": 90})
    empty = build.stat_table(gk_row, "Shooting")
    assert empty.empty and tuple(empty.columns) == build.STAT_TABLE_COLUMNS
    gk = build.stat_table(gk_row, "Goalkeeping").set_index("Stat")
    assert gk.loc["Saves", "Per 90"] == 3.0
    assert pd.isna(gk.loc["Wins", "Per 90"])


def test_kpi_tiles_outfield_and_goalkeeper():
    out = build.kpi_tiles(pd.Series({"minutes": 2536, "nineties_played": 28.18, "std_goals": 18,
                                     "std_assists": 10, "std_xg": 19.24, "xag": 9.95}), "ATT")
    assert [(lbl, txt) for lbl, txt, _ in out] == [
        ("Minutes", "2,536"), ("90s", "28.2"), ("Goals", "18"), ("Assists", "10"),
        ("xG", "19.2"), ("xAG", "9.9")]
    assert out[4][2].startswith("Expected goals")
    gk = build.kpi_tiles(pd.Series({"minutes": 2700, "nineties_played": 30.0, "save_pct": 71.24,
                                    "goals_against_per_90": 1.0333, "clean_sheet_pct": 33.3,
                                    "saves": 90}), "GK")
    assert [lbl for lbl, _, _ in gk] == ["Minutes", "90s", "Save %", "GA / 90",
                                         "CS %", "Saves / 90"]
    assert gk[2][1] == "71.2%" and gk[3][1] == "1.03" and gk[5][1] == "3.00"
    assert gk[5][2] == "Saves divided by 90s played."


def test_kpi_tiles_missing_values_show_a_dash():
    tiles = build.kpi_tiles(pd.Series({"minutes": 100}), "ATT")
    assert tiles[2][1] == "—"


def test_similar_table_renames_for_display():
    sim = pd.DataFrame({
        "Player": ["B"], "Squad": ["Club B"], "League": ["Serie A"], "Season": ["2023-2024"],
        "position_group": ["ATT"], "minutes": [2000], "similarity_score": [0.91234],
    })
    out = build.similar_table(sim)
    assert tuple(out.columns) == build.SIMILAR_COLUMNS
    assert out.iloc[0].tolist() == ["B", "Club B", "Serie A", "2023-2024", "Forward", 2000, 0.912]
