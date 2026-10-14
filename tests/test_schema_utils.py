from __future__ import annotations

from tests.schema_utils import IDENTITY_COLUMNS, feature_columns, numeric_columns


def test_outfield_columns_are_parsed_in_order():
    cols = feature_columns("outfield_features")
    assert len(cols) == 68
    assert cols[:9] == ["Player", "Nation", "Position", "Squad", "League", "Age", "Born",
                        "Season", "position_group"]
    assert "pass_progressive_passes" in cols  # an `AS` alias
    assert cols[-1] == "errors_leading_to_shot"


def test_goalkeeper_columns_are_parsed_in_order():
    cols = feature_columns("goalkeeper_features")
    assert len(cols) == 25
    assert "position_group" in cols  # 'GK' AS position_group
    assert cols[-1] == "pk_save_pct"


def test_numeric_columns_drop_identity():
    out = numeric_columns("outfield_features")
    gk = numeric_columns("goalkeeper_features")
    assert not set(out) & IDENTITY_COLUMNS
    assert len(out) == 59
    assert len(gk) == 16
    assert {"minutes", "nineties_played"} <= set(gk)
