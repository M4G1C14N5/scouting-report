from __future__ import annotations

from tests.schema_utils import (IDENTITY_COLUMNS, feature_columns, newer_source_columns,
                                numeric_columns)


def test_outfield_columns_are_parsed_in_order():
    cols = feature_columns("outfield_features")
    assert len(cols) == 107
    assert cols[:9] == ["Player", "Nation", "Position", "Squad", "League", "Age", "Born",
                        "Season", "position_group"]
    assert "pass_progressive_passes" in cols  # an `AS` alias
    assert cols[67] == "errors_leading_to_shot"  # the last FBref column
    assert cols[-8:] == ["offsides", "photo_url", "role_st", "role_amw", "role_cm", "role_fb",
                         "role_cb", "main_role"]


def test_goalkeeper_columns_are_parsed_in_order():
    cols = feature_columns("goalkeeper_features")
    assert len(cols) == 29
    assert "position_group" in cols  # 'GK' AS position_group
    assert cols[24] == "pk_save_pct"
    assert cols[-4:] == ["appearances", "starts", "match_rating", "photo_url"]


def test_numeric_columns_drop_identity():
    out = numeric_columns("outfield_features")
    gk = numeric_columns("goalkeeper_features")
    assert not set(out) & IDENTITY_COLUMNS
    assert len(out) == 91
    assert len(gk) == 19
    assert {"minutes", "nineties_played"} <= set(gk)


def test_newer_source_columns_are_the_null_casts_with_their_types():
    out = newer_source_columns("outfield_features")
    assert len(out) == 39 and out["photo_url"] == "STRING" and out["main_role"] == "STRING"
    assert out["role_amw"] == "FLOAT64"
    assert out["duels_total"] == "INT64" and out["match_rating"] == "FLOAT64"
    assert list(out) == feature_columns("outfield_features")[68:]
    assert newer_source_columns("goalkeeper_features") == {
        "appearances": "INT64", "starts": "INT64", "match_rating": "FLOAT64", "photo_url": "STRING"}
