"""Metric configuration: labels, categories, number formats, per-position
profile lists and the lower-is-better set.

THE place to edit when the data source changes (the planned API-Football
swap). Every key is a column of outfield_features / goalkeeper_features in
schema.sql; tests/test_metrics.py fails if one is not.

STAT_LABELS / STAT_CATEGORIES started as the dicts in the old app.py. All
73 keys and their labels are kept, except one: std_progressive_passes is
relabelled, because it shared "Progressive Passes" with
pass_progressive_passes. Categories were normalised to the seven shown in
the Stats tab.
"""

from __future__ import annotations

from typing import NamedTuple

POSITION_NAMES = {"ATT": "Forward", "MID": "Midfielder", "DEF": "Defender", "GK": "Goalkeeper"}
POSITION_PLURALS = {"ATT": "forwards", "MID": "midfielders", "DEF": "defenders", "GK": "goalkeepers"}

CATEGORY_ORDER = ("Shooting", "Passing", "Defending", "Progression", "Discipline",
                  "Playing time", "Goalkeeping")

# (column, label, category, kind) in display order. Kinds:
#   count    integer season total, per 90 computed when no stored per-90 column
#   tally    integer season total with no meaningful per 90 (wins, penalties)
#   total    decimal season total (xG family), per 90 computed
#   per90    stored per-90 rate (shown in the Per 90 column)
#   pct      percentage, a season rate
#   ratio    other season rate (per shot)
#   distance average distance (yards)
#   minutes / nineties  playing time
_METRICS: tuple[tuple[str, str, str, str], ...] = (
    # Shooting
    ("std_goals", "Goals", "Shooting", "count"),
    ("nonpen_goals", "Non-Pen Goals", "Shooting", "count"),
    ("penalty_kicks_made", "Penalties Made", "Shooting", "tally"),
    ("std_xg", "xG", "Shooting", "total"),
    ("npxg", "npxG", "Shooting", "total"),
    ("goals_minus_xg", "Goals - xG", "Shooting", "total"),
    ("nonpen_goals_minus_npxg", "Non-Pen Goals - npxG", "Shooting", "total"),
    ("shots_total", "Shots", "Shooting", "count"),
    ("shots_on_target", "Shots on Target", "Shooting", "count"),
    ("shots_on_target_pct", "Shot Accuracy %", "Shooting", "pct"),
    ("goals_per_shot", "Goals per Shot", "Shooting", "ratio"),
    ("goals_per_shot_on_target", "Goals per SoT", "Shooting", "ratio"),
    ("npxg_per_shot", "npxG per Shot", "Shooting", "ratio"),
    ("avg_shot_distance", "Avg Shot Distance", "Shooting", "distance"),
    ("goals_per_90", "Goals / 90", "Shooting", "per90"),
    ("nonpen_goals_per_90", "Non-Pen Goals / 90", "Shooting", "per90"),
    ("xg_per_90", "xG / 90", "Shooting", "per90"),
    ("npxg_per_90", "npxG / 90", "Shooting", "per90"),
    ("shots_total_per_90", "Shots / 90", "Shooting", "per90"),
    ("shots_on_target_per_90", "Shots on Target / 90", "Shooting", "per90"),
    ("npxg_plus_xag_per_90", "npxG + xAG / 90", "Shooting", "per90"),
    # Passing
    ("std_assists", "Assists", "Passing", "count"),
    ("xag", "xAG", "Passing", "total"),
    ("expected_assisted_goals", "Expected Assisted Goals", "Passing", "total"),
    ("expected_assists", "Expected Assists", "Passing", "total"),
    ("key_passes", "Key Passes", "Passing", "count"),
    ("passes_into_final_third", "Passes into Final Third", "Passing", "count"),
    ("passes_into_penalty_area", "Passes into Box", "Passing", "count"),
    ("crosses_into_penalty_area", "Crosses into Box", "Passing", "count"),
    ("passes_completed_total", "Passes Completed", "Passing", "count"),
    ("passes_attempted_total", "Passes Attempted", "Passing", "count"),
    ("passes_completion_pct_total", "Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_short", "Short Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_medium", "Medium Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_long", "Long Pass Completion %", "Passing", "pct"),
    ("total_passing_distance", "Total Pass Distance", "Passing", "count"),
    ("assists_per_90", "Assists / 90", "Passing", "per90"),
    ("xag_per_90", "xAG / 90", "Passing", "per90"),
    # Defending
    ("tackles_total", "Tackles", "Defending", "count"),
    ("tackles_won", "Tackles Won", "Defending", "count"),
    ("tackles_def_3rd", "Tackles (Def 3rd)", "Defending", "count"),
    ("tackles_mid_3rd", "Tackles (Mid 3rd)", "Defending", "count"),
    ("tackles_att_3rd", "Tackles (Att 3rd)", "Defending", "count"),
    ("dribblers_tackled", "Dribblers Tackled", "Defending", "count"),
    ("dribblers_tackled_pct", "Dribbler Tackle %", "Defending", "pct"),
    ("interceptions", "Interceptions", "Defending", "count"),
    ("tackles_plus_interceptions", "Tackles + Interceptions", "Defending", "count"),
    ("blocks_total", "Blocks", "Defending", "count"),
    ("clearances", "Clearances", "Defending", "count"),
    ("errors_leading_to_shot", "Errors Leading to Shot", "Defending", "count"),
    # Progression
    ("progressive_carries", "Progressive Carries", "Progression", "count"),
    ("std_progressive_passes", "Progressive Passes (standard data)", "Progression", "count"),
    ("pass_progressive_passes", "Progressive Passes", "Progression", "count"),
    ("progressive_passing_distance", "Progressive Pass Distance", "Progression", "count"),
    ("progressive_passes_received", "Progressive Passes Received", "Progression", "count"),
    # Discipline
    ("yellow_cards", "Yellow Cards", "Discipline", "count"),
    ("red_cards", "Red Cards", "Discipline", "count"),
    # Playing time
    ("minutes", "Minutes", "Playing time", "minutes"),
    ("nineties_played", "90s", "Playing time", "nineties"),
    # Goalkeeping
    ("goals_against", "Goals Against", "Goalkeeping", "count"),
    ("goals_against_per_90", "Goals Against / 90", "Goalkeeping", "per90"),
    ("shots_on_target_against", "Shots on Target Against", "Goalkeeping", "count"),
    ("saves", "Saves", "Goalkeeping", "count"),
    ("save_pct", "Save %", "Goalkeeping", "pct"),
    ("clean_sheets", "Clean Sheets", "Goalkeeping", "tally"),
    ("clean_sheet_pct", "Clean Sheet %", "Goalkeeping", "pct"),
    ("wins", "Wins", "Goalkeeping", "tally"),
    ("draws", "Draws", "Goalkeeping", "tally"),
    ("losses", "Losses", "Goalkeeping", "tally"),
    ("penalty_kicks_faced", "Penalties Faced", "Goalkeeping", "tally"),
    ("penalty_kicks_allowed", "Penalties Allowed", "Goalkeeping", "tally"),
    ("penalty_kicks_saved", "Penalties Saved", "Goalkeeping", "tally"),
    ("pk_save_pct", "Penalty Save %", "Goalkeeping", "pct"),
)

STAT_LABELS: dict[str, str] = {key: label for key, label, _, _ in _METRICS}
STAT_CATEGORIES: dict[str, str] = {key: category for key, _, category, _ in _METRICS}
KIND: dict[str, str] = {key: kind for key, _, _, kind in _METRICS}

KIND_FORMAT = {
    "count": "int", "tally": "int", "minutes": "int",
    "total": "dec1", "nineties": "dec1", "distance": "dec1",
    "per90": "dec2", "ratio": "dec2",
    "pct": "pct",
}
TOTAL_DECIMALS = {"int": 0, "dec1": 1, "dec2": 2, "pct": 1}
PER90_COMPUTED_KINDS = frozenset({"count", "total"})

# Season total -> the stored per-90 column shown beside it in the Stats tab.
PER90_PAIRS = {
    "std_goals": "goals_per_90",
    "nonpen_goals": "nonpen_goals_per_90",
    "std_xg": "xg_per_90",
    "npxg": "npxg_per_90",
    "shots_total": "shots_total_per_90",
    "shots_on_target": "shots_on_target_per_90",
    "std_assists": "assists_per_90",
    "xag": "xag_per_90",
    "goals_against": "goals_against_per_90",
}

# Profile tab: FEATURE_TEMPLATES (src/similarity.py) plus the SQL percentile
# views (schema.sql), grouped by category. Category order = card order.
PROFILE_METRICS: dict[str, dict[str, tuple[str, ...]]] = {
    "ATT": {
        "Shooting": ("goals_per_90", "nonpen_goals_per_90", "xg_per_90", "npxg_per_90",
                     "npxg_plus_xag_per_90", "shots_total_per_90", "shots_on_target_per_90",
                     "goals_per_shot", "npxg_per_shot"),
        "Passing": ("assists_per_90", "xag_per_90", "key_passes", "passes_into_penalty_area",
                    "crosses_into_penalty_area", "passes_completion_pct_total"),
        "Progression": ("progressive_carries", "progressive_passing_distance",
                        "passes_into_final_third"),
    },
    "MID": {
        "Progression": ("progressive_carries", "progressive_passing_distance",
                        "pass_progressive_passes", "passes_into_final_third"),
        "Passing": ("assists_per_90", "xag_per_90", "key_passes", "passes_into_penalty_area",
                    "crosses_into_penalty_area", "passes_completion_pct_total"),
        "Shooting": ("goals_per_90", "npxg_per_90", "shots_total_per_90"),
        "Defending": ("tackles_total", "interceptions", "tackles_plus_interceptions"),
    },
    "DEF": {
        "Defending": ("tackles_total", "tackles_won", "tackles_def_3rd", "tackles_mid_3rd",
                      "dribblers_tackled_pct", "interceptions", "tackles_plus_interceptions",
                      "blocks_total", "clearances", "errors_leading_to_shot"),
        "Passing": ("passes_completion_pct_total", "assists_per_90"),
        "Progression": ("progressive_passing_distance", "passes_into_final_third"),
        "Shooting": ("goals_per_90",),
    },
    "GK": {
        "Goalkeeping": ("save_pct", "goals_against_per_90", "clean_sheet_pct", "clean_sheets",
                        "pk_save_pct", "saves", "shots_on_target_against"),
    },
}

# Percentiles for these are inverted, so a longer bar always means better.
LOWER_IS_BETTER = frozenset({
    "errors_leading_to_shot", "yellow_cards", "red_cards",
    "goals_against", "goals_against_per_90", "losses", "penalty_kicks_allowed",
})


class KpiSpec(NamedTuple):
    label: str
    key: str
    per90: bool = False  # True: show key / nineties_played


KPI_TILES: dict[str, tuple[KpiSpec, ...]] = {
    "outfield": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Goals", "std_goals"), KpiSpec("Assists", "std_assists"),
        KpiSpec("xG", "std_xg"), KpiSpec("xAG", "xag"),
    ),
    "GK": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Save %", "save_pct"), KpiSpec("GA / 90", "goals_against_per_90"),
        KpiSpec("CS %", "clean_sheet_pct"), KpiSpec("Saves / 90", "saves", per90=True),
    ),
}

# Plain-English help for jargon (the basketball glossary.py pattern), shown as
# a hover/tap tip on KPI labels.
GLOSSARY = {
    "nineties_played": "Minutes played divided by 90: how many full matches his minutes add up to.",
    "std_xg": ("Expected goals: the summed chance quality of every shot he took. "
               "Goals above xG suggests strong finishing, or luck."),
    "xag": "Expected assisted goals: the xG of the shots his passes set up, scored or not.",
    "save_pct": "Share of shots on target faced that he saved.",
    "goals_against_per_90": "Goals conceded per 90 minutes he played. Lower is better.",
    "clean_sheet_pct": "Share of his matches in which his team kept a clean sheet.",
}


def label(key: str) -> str:
    return STAT_LABELS.get(key, key)


def fmt_of(key: str) -> str:
    return KIND_FORMAT[KIND.get(key, "ratio")]


# Same quantity, loaded from two FBref tables (standard and passing): the Stats
# tab shows the first and hides the copy.
DUPLICATE_OF = {"expected_assisted_goals": "xag"}


def keys_in_category(category: str) -> list[str]:
    return [key for key, cat in STAT_CATEGORIES.items()
            if cat == category and key not in DUPLICATE_OF]


def stats_categories(group: str) -> list[str]:
    """Stats-tab cards for a position group, in display order."""
    if group == "GK":
        return ["Goalkeeping", "Playing time"]
    return [c for c in CATEGORY_ORDER if c != "Goalkeeping"]


def profile_keys(group: str) -> list[str]:
    return [key for keys in PROFILE_METRICS.get(group, {}).values() for key in keys]
