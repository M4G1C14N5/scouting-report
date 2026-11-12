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
    assert newer - {"photo_url"} == set(NEWER)
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


# --- data families -------------------------------------------------------------

import pytest  # noqa: E402


@pytest.mark.parametrize("source, family", [
    ("FBref", "fbref"),
    (None, "fbref"),
    (float("nan"), "fbref"),
    ("", "fbref"),
    ("American Soccer Analysis", "us"),
    ("API-Football", "europe"),
    ("API-Football + Understat", "europe"),
    ("API-Football match data", "europe"),
])
def test_family_comes_from_the_source(source, family):
    assert metrics.family_of(source) == family


def test_big_five_is_a_european_row_in_one_of_the_five_leagues():
    assert metrics.BIG_FIVE == ("Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1")
    assert metrics.is_big_five("API-Football + Understat", "Premier League")
    assert metrics.is_big_five("API-Football", "Ligue 1")  # no Understat match: still big five
    assert not metrics.is_big_five("API-Football match data", "League Two")
    assert not metrics.is_big_five("FBref", "Premier League")  # FBref seasons are not a family
    assert not metrics.is_big_five("American Soccer Analysis", "MLS")


EUROPE_SET = {
    "goals_per_90", "assists_per_90", "shots_total_per_90", "shots_on_target_per_90",
    "key_passes", "passes_attempted_total", "passes_completion_pct_total", "tackles_total",
    "interceptions", "blocks_total", "duels_total", "duels_won_pct", "dribbles_attempted",
    "dribbles_completed", "dribble_success_pct", "dribbled_past", "fouls_drawn",
    "fouls_committed", "yellow_cards", "match_rating",
}
BIG_FIVE_SET = {"xg_per_90", "npxg_per_90", "xag_per_90", "xg_chain", "xg_buildup"}
US_SET = {
    "goals_per_90", "assists_per_90", "shots_total_per_90", "shots_on_target_per_90",
    "key_passes", "passes_attempted_total", "passes_completion_pct_total", "xg_per_90",
    "xag_per_90", "goals_added", "ga_shooting", "ga_passing", "ga_receiving", "ga_dribbling",
    "ga_interrupting", "ga_fouling", "passes_completed_over_expected_p100",
    "xpass_completion_pct", "share_team_touches_pct", "avg_pass_distance",
    "avg_vertical_pass_distance",
}
EUROPE_GK_SET = {"save_pct", "goals_against_per_90", "clean_sheet_pct", "saves", "match_rating"}
SHARED_SET = {"goals_per_90", "assists_per_90", "shots_total_per_90", "shots_on_target_per_90",
              "key_passes", "passes_attempted_total", "passes_completion_pct_total"}


@pytest.mark.parametrize("group", ["ATT", "MID", "DEF"])
def test_family_sets_are_the_same_for_every_outfield_position(group):
    assert set(metrics.family_set("europe", group)) == EUROPE_SET
    assert set(metrics.family_set("us", group)) == US_SET
    assert len(metrics.family_set("europe", group)) == 20
    assert len(metrics.family_set("us", group)) == 21


def test_big_five_extras_keeper_set_and_shared_set():
    assert set(metrics.BIG_FIVE_EXTRAS) == BIG_FIVE_SET
    assert set(metrics.family_set("europe", "GK")) == EUROPE_GK_SET
    assert metrics.family_set("us", "GK") == ()  # no US keepers are loaded
    assert set(metrics.SHARED_SET) == SHARED_SET
    # the shared set is what both regions have
    assert SHARED_SET <= EUROPE_SET and SHARED_SET <= US_SET


def test_family_metrics_are_real_columns_of_the_right_table():
    for family in ("europe", "us"):
        for group in ("ATT", "MID", "DEF", "GK"):
            missing = set(metrics.family_set(family, group)) - _columns_for(group)
            assert not missing, (family, group, missing)
    assert set(metrics.BIG_FIVE_EXTRAS) <= OUTFIELD


def test_patchy_stats_are_in_no_family_set():
    assert metrics.PATCHY == {"offsides", "penalties_won", "penalties_conceded", "penalties_missed"}
    sets = [metrics.family_set(f, g) for f in ("europe", "us") for g in ("ATT", "MID", "DEF", "GK")]
    sets += [metrics.BIG_FIVE_EXTRAS, metrics.SHARED_SET]
    for family in ("europe", "us"):
        for group in ("ATT", "GK"):
            sets += [k for keys in metrics.stats_layout(family, group).values() for k in [keys]]
    for keys in sets:
        assert not metrics.PATCHY & set(keys), keys


def test_family_profile_layout_groups_the_set_by_category_in_card_order():
    layout = metrics.profile_layout("europe", "ATT")
    assert list(layout) == ["Shooting", "Chance creation", "Passing", "Possession and dribbling",
                            "Defending", "Duels", "Discipline", "Playing time"]
    assert {k for keys in layout.values() for k in keys} == EUROPE_SET
    for category, keys in layout.items():
        assert all(metrics.STAT_CATEGORIES[k] == category for k in keys), category
    assert metrics.profile_layout("europe", "MID") == layout == metrics.profile_layout("europe", "DEF")
    us = metrics.profile_layout("us", "DEF")
    assert list(us) == ["Shooting", "Chance creation", "Passing", "Possession and dribbling",
                        "Goals added"]
    assert {k for keys in us.values() for k in keys} == US_SET
    gk = metrics.profile_layout("europe", "GK")
    assert list(gk) == ["Goalkeeping", "Playing time"]
    assert {k for keys in gk.values() for k in keys} == EUROPE_GK_SET


@pytest.mark.parametrize("group", ["ATT", "MID", "DEF", "GK"])
def test_fbref_profile_layout_is_unchanged(group):
    assert metrics.profile_layout("fbref", group) == metrics.PROFILE_METRICS[group]


def test_stats_layout_uses_season_totals_beside_their_per_90():
    layout = metrics.stats_layout("europe", "ATT")
    keys = [k for ks in layout.values() for k in ks]
    # stored per-90 columns become their total (the Stats tab shows both side by side)
    assert {"std_goals", "std_assists", "shots_total", "shots_on_target"} <= set(keys)
    assert not {"goals_per_90", "assists_per_90"} & set(keys)
    assert layout["Playing time"] == ("minutes", "nineties_played", "appearances", "match_rating")
    us = [k for ks in metrics.stats_layout("us", "ATT").values() for k in ks]
    assert {"std_xg", "xag", "goals_added", "ga_fouling"} <= set(us)
    assert "match_rating" not in us
    gk = metrics.stats_layout("europe", "GK")
    assert gk["Goalkeeping"] == ("save_pct", "goals_against", "clean_sheet_pct", "saves")
    assert metrics.stats_keys(metrics.BIG_FIVE_EXTRAS) == ("std_xg", "npxg", "xag", "xg_chain",
                                                           "xg_buildup")
