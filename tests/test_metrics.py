"""metrics.py must agree with schema.sql: the guard for the future data-source swap."""

from __future__ import annotations

from src.similarity import FEATURE_TEMPLATES
from src.ui import metrics
from tests.schema_utils import newer_source_columns, numeric_columns

OUTFIELD = set(numeric_columns("outfield_features"))
GOALKEEPER = set(numeric_columns("goalkeeper_features"))


def _columns_for(group: str) -> set[str]:
    return GOALKEEPER if group == "GK" else OUTFIELD


def test_every_feature_column_has_a_label_category_and_kind():
    every = OUTFIELD | GOALKEEPER
    assert set(metrics.STAT_LABELS) == every
    assert set(metrics.STAT_CATEGORIES) == every
    assert set(metrics.KIND) == every


def test_categories_in_display_order():
    assert metrics.CATEGORY_ORDER == ("Shooting", "Chance creation", "Passing",
                                      "Possession and dribbling", "Defending", "Duels",
                                      "Goals added", "Discipline", "Playing time", "Goalkeeping")
    assert set(metrics.STAT_CATEGORIES.values()) == set(metrics.CATEGORY_ORDER)


def test_metrics_are_listed_category_by_category_in_display_order():
    order = [metrics.CATEGORY_ORDER.index(c) for c in metrics.STAT_CATEGORIES.values()]
    assert order == sorted(order)


# Newer-source columns (API-Football, Understat, American Soccer Analysis): kind and category.
NEWER = {
    "appearances": ("tally", "Playing time"),
    "starts": ("tally", "Playing time"),
    "match_rating": ("ratio", "Playing time"),
    "penalties_missed": ("tally", "Shooting"),
    "xg_chain": ("total", "Chance creation"),
    "xg_buildup": ("total", "Chance creation"),
    "xpass_completion_pct": ("pct", "Passing"),
    "passes_completed_over_expected": ("total", "Passing"),
    "passes_completed_over_expected_p100": ("ratio", "Passing"),
    "avg_pass_distance": ("distance", "Passing"),
    "avg_vertical_pass_distance": ("distance", "Passing"),
    "dribbles_attempted": ("count", "Possession and dribbling"),
    "dribbles_completed": ("count", "Possession and dribbling"),
    "dribble_success_pct": ("pct", "Possession and dribbling"),
    "fouls_drawn": ("count", "Possession and dribbling"),
    "penalties_won": ("tally", "Possession and dribbling"),
    "share_team_touches_pct": ("pct", "Possession and dribbling"),
    "dribbled_past": ("count", "Defending"),
    "duels_total": ("count", "Duels"),
    "duels_won": ("count", "Duels"),
    "duels_won_pct": ("pct", "Duels"),
    "goals_added": ("total", "Goals added"),
    "goals_added_above_avg": ("total", "Goals added"),
    "ga_shooting": ("total", "Goals added"),
    "ga_passing": ("total", "Goals added"),
    "ga_receiving": ("total", "Goals added"),
    "ga_dribbling": ("total", "Goals added"),
    "ga_interrupting": ("total", "Goals added"),
    "ga_fouling": ("total", "Goals added"),
    "fouls_committed": ("count", "Discipline"),
    "penalties_conceded": ("tally", "Discipline"),
    "offsides": ("count", "Discipline"),
}


def test_newer_source_metrics_kinds_and_categories():
    newer = set(newer_source_columns("outfield_features")) | set(
        newer_source_columns("goalkeeper_features"))
    assert newer == set(NEWER)
    for key, (kind, category) in NEWER.items():
        assert (metrics.KIND[key], metrics.STAT_CATEGORIES[key]) == (kind, category), key


def test_goals_added_labels_name_the_action():
    assert metrics.label("ga_dribbling") == "Goals Added: Dribbling"
    assert metrics.label("ga_interrupting") == "Goals Added: Defending"
    assert metrics.label("xg_chain") == "xG Chain"


def test_labels_are_short_and_have_no_long_dashes():
    long_dashes = (chr(0x2013), chr(0x2014))
    for key, text in metrics.STAT_LABELS.items():
        assert not any(d in text for d in long_dashes), key
        if key in NEWER:
            assert len(text) <= 28, (key, text)


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
    assert counts == {"ATT": 26, "MID": 24, "DEF": 25, "GK": 8}
    for group in metrics.PROFILE_METRICS:
        keys = metrics.profile_keys(group)
        assert len(keys) == len(set(keys)), group


def test_profiles_carry_the_newer_source_metrics():
    newer = {
        "ATT": {"dribbles_completed", "dribble_success_pct", "xg_chain", "fouls_drawn",
                "duels_won_pct", "goals_added", "ga_shooting", "ga_receiving"},
        "MID": {"xg_chain", "xg_buildup", "dribbles_completed", "duels_won_pct",
                "passes_completed_over_expected_p100", "goals_added", "ga_passing",
                "ga_interrupting"},
        "DEF": {"duels_won_pct", "dribbled_past", "fouls_committed", "xg_buildup",
                "passes_completed_over_expected_p100", "ga_interrupting", "ga_passing",
                "goals_added", "avg_pass_distance",
                "avg_vertical_pass_distance"},
        "GK": {"match_rating"},
    }
    for group, keys in newer.items():
        assert set(metrics.profile_keys(group)) & set(NEWER) == keys, group
    assert {"dribbled_past", "fouls_committed"} <= metrics.LOWER_IS_BETTER


def test_profile_categories_are_known_categories():
    for categories in metrics.PROFILE_METRICS.values():
        assert set(categories) <= set(metrics.CATEGORY_ORDER)


def test_profile_cards_follow_the_category_order_and_each_metric_its_category():
    # the Stats tab's card order (goalkeepers: Goalkeeping first)
    for group, categories in metrics.PROFILE_METRICS.items():
        order = [metrics.stats_categories(group).index(c) for c in categories]
        assert order == sorted(order), group
        for category, keys in categories.items():
            wrong = [k for k in keys if metrics.STAT_CATEGORIES[k] != category]
            assert not wrong, (group, category, wrong)


def test_lower_is_better_keys_are_real_columns():
    assert metrics.LOWER_IS_BETTER <= OUTFIELD | GOALKEEPER
    assert metrics.LOWER_IS_BETTER == {
        "errors_leading_to_shot", "yellow_cards", "red_cards", "goals_against",
        "goals_against_per_90", "losses", "penalty_kicks_allowed",
        "dribbled_past", "fouls_committed", "penalties_conceded", "penalties_missed",
        "offsides",
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
    assert metrics.keys_in_category("Discipline") == ["yellow_cards", "red_cards",
                                                      "fouls_committed", "penalties_conceded",
                                                      "offsides"]
    assert metrics.stats_categories("GK") == ["Goalkeeping", "Playing time"]
    assert metrics.stats_categories("ATT") == ["Shooting", "Chance creation", "Passing",
                                               "Possession and dribbling", "Defending", "Duels",
                                               "Goals added", "Discipline", "Playing time"]


def test_duplicate_stat_columns_are_hidden_from_the_stats_tab():
    # xag (standard table) and expected_assisted_goals (passing table) are the same number
    assert "expected_assisted_goals" in metrics.STAT_LABELS
    assert "expected_assisted_goals" not in metrics.keys_in_category("Chance creation")
    assert "xag" in metrics.keys_in_category("Chance creation")
