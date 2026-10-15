"""metrics.py must agree with schema.sql: the guard for the future data-source swap."""

from __future__ import annotations

from src.similarity import FEATURE_TEMPLATES
from src.ui import metrics
from tests.schema_utils import numeric_columns

OUTFIELD = set(numeric_columns("outfield_features"))
GOALKEEPER = set(numeric_columns("goalkeeper_features"))


def _columns_for(group: str) -> set[str]:
    return GOALKEEPER if group == "GK" else OUTFIELD


def test_every_feature_column_has_a_label_category_and_kind():
    every = OUTFIELD | GOALKEEPER
    assert set(metrics.STAT_LABELS) == every
    assert set(metrics.STAT_CATEGORIES) == every
    assert set(metrics.KIND) == every


def test_categories_are_the_spec_seven_in_display_order():
    assert metrics.CATEGORY_ORDER == ("Shooting", "Passing", "Defending", "Progression",
                                      "Discipline", "Playing time", "Goalkeeping")
    assert set(metrics.STAT_CATEGORIES.values()) == set(metrics.CATEGORY_ORDER)


def test_kinds_are_known():
    assert set(metrics.KIND.values()) <= set(metrics.KIND_FORMAT)


def test_profile_metrics_exist_in_the_right_feature_table():
    for group, categories in metrics.PROFILE_METRICS.items():
        for category, keys in categories.items():
            missing = [k for k in keys if k not in _columns_for(group)]
            assert not missing, f"{group}/{category}: not in schema.sql: {missing}"


def test_profile_metrics_cover_the_similarity_templates():
    for group, template in FEATURE_TEMPLATES.items():
        assert set(template) <= set(metrics.profile_keys(group)), group


def test_profile_metric_counts_and_no_duplicates():
    counts = {g: len(metrics.profile_keys(g)) for g in metrics.PROFILE_METRICS}
    assert counts == {"ATT": 18, "MID": 16, "DEF": 15, "GK": 7}
    for group in metrics.PROFILE_METRICS:
        keys = metrics.profile_keys(group)
        assert len(keys) == len(set(keys)), group


def test_profile_categories_are_known_categories():
    for categories in metrics.PROFILE_METRICS.values():
        assert set(categories) <= set(metrics.CATEGORY_ORDER)


def test_lower_is_better_keys_are_real_columns():
    assert metrics.LOWER_IS_BETTER <= OUTFIELD | GOALKEEPER
    assert metrics.LOWER_IS_BETTER == {
        "errors_leading_to_shot", "yellow_cards", "red_cards", "goals_against",
        "goals_against_per_90", "losses", "penalty_kicks_allowed",
    }


def test_per90_pairs_point_at_per90_columns_in_the_same_category():
    for total, per90 in metrics.PER90_PAIRS.items():
        assert metrics.KIND[per90] == "per90"
        assert metrics.KIND[total] != "per90"
        assert metrics.STAT_CATEGORIES[total] == metrics.STAT_CATEGORIES[per90]


def test_kpi_and_glossary_keys_are_real_columns():
    for group, specs in metrics.KPI_TILES.items():
        cols = GOALKEEPER if group == "GK" else OUTFIELD
        assert [s.key for s in specs if s.key not in cols] == [], group
    assert set(metrics.GLOSSARY) <= set(metrics.STAT_LABELS)


def test_duplicate_label_is_resolved():
    labels = list(metrics.STAT_LABELS.values())
    assert len(labels) == len(set(labels))


def test_helpers():
    assert metrics.label("std_goals") == "Goals"
    assert metrics.label("unknown_col") == "unknown_col"
    assert metrics.fmt_of("minutes") == "int"
    assert metrics.fmt_of("std_xg") == "dec1"
    assert metrics.fmt_of("save_pct") == "pct"
    assert metrics.keys_in_category("Discipline") == ["yellow_cards", "red_cards"]
    assert metrics.stats_categories("GK") == ["Goalkeeping", "Playing time"]
    assert metrics.stats_categories("ATT") == ["Shooting", "Passing", "Defending", "Progression",
                                               "Discipline", "Playing time"]


def test_duplicate_stat_columns_are_hidden_from_the_stats_tab():
    # xag (standard table) and expected_assisted_goals (passing table) are the same number
    assert "expected_assisted_goals" in metrics.STAT_LABELS
    assert "expected_assisted_goals" not in metrics.keys_in_category("Passing")
    assert "xag" in metrics.keys_in_category("Passing")
