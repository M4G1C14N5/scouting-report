"""Metric configuration: labels, categories, number formats, per-position
profile lists and the lower-is-better set.

THE place to edit when the data sources change. Every key is a column of
outfield_features / goalkeeper_features in schema.sql; tests/test_metrics.py
fails if one is not.

STAT_LABELS / STAT_CATEGORIES started as the dicts in the old app.py. All 73
FBref keys and their labels are kept, except one: std_progressive_passes is
relabelled, because it shared "Progressive Passes" with
pass_progressive_passes. The newer sources (API-Football, Understat,
American Soccer Analysis) add the metrics schema.sql null-casts for FBref
rows. Both eras share one set of categories, so a card holds whichever of
its metrics a season has. To drop the FBref-only metrics later, delete
their rows here and their columns in schema.sql.
"""

from __future__ import annotations

from typing import NamedTuple

POSITION_NAMES = {"ATT": "Forward", "MID": "Midfielder", "DEF": "Defender", "GK": "Goalkeeper"}
POSITION_PLURALS = {"ATT": "forwards", "MID": "midfielders", "DEF": "defenders", "GK": "goalkeepers"}

# Card order in the Stats and Profile tabs.
CATEGORY_ORDER = ("Shooting", "Chance creation", "Passing", "Possession and dribbling",
                  "Defending", "Duels", "Goals added", "Discipline", "Playing time",
                  "Goalkeeping")

# (column, label, category, kind) in display order, category by category. Kinds:
#   count    integer season total, per 90 computed when no stored per-90 column
#   tally    integer season total with no meaningful per 90 (wins, penalties)
#   total    decimal season total (xG family, goals added), per 90 computed
#   per90    stored per-90 rate (shown in the Per 90 column)
#   pct      percentage, a season rate
#   ratio    other season rate (per shot, per 100 passes, match rating)
#   distance average distance (yards)
#   minutes / nineties  playing time
_METRICS: tuple[tuple[str, str, str, str], ...] = (
    # Shooting
    ("std_goals", "Goals", "Shooting", "count"),
    ("nonpen_goals", "Non-Pen Goals", "Shooting", "count"),
    ("penalty_kicks_made", "Penalties Made", "Shooting", "tally"),
    ("penalties_missed", "Penalties Missed", "Shooting", "tally"),
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
    # Chance creation
    ("std_assists", "Assists", "Chance creation", "count"),
    ("xag", "xAG", "Chance creation", "total"),
    ("expected_assisted_goals", "Expected Assisted Goals", "Chance creation", "total"),
    ("expected_assists", "Expected Assists", "Chance creation", "total"),
    ("key_passes", "Key Passes", "Chance creation", "count"),
    ("passes_into_penalty_area", "Passes into Box", "Chance creation", "count"),
    ("crosses_into_penalty_area", "Crosses into Box", "Chance creation", "count"),
    ("xg_chain", "xG Chain", "Chance creation", "total"),
    ("xg_buildup", "xG Buildup", "Chance creation", "total"),
    ("assists_per_90", "Assists / 90", "Chance creation", "per90"),
    ("xag_per_90", "xAG / 90", "Chance creation", "per90"),
    # Passing
    ("passes_completed_total", "Passes Completed", "Passing", "count"),
    ("passes_attempted_total", "Passes Attempted", "Passing", "count"),
    ("passes_completion_pct_total", "Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_short", "Short Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_medium", "Medium Pass Completion %", "Passing", "pct"),
    ("passes_completion_pct_long", "Long Pass Completion %", "Passing", "pct"),
    ("xpass_completion_pct", "Expected Pass Completion %", "Passing", "pct"),
    ("passes_completed_over_expected", "Passes over Expected", "Passing", "total"),
    ("passes_completed_over_expected_p100", "Passes over Expected / 100", "Passing", "ratio"),
    ("passes_into_final_third", "Passes into Final Third", "Passing", "count"),
    ("pass_progressive_passes", "Progressive Passes", "Passing", "count"),
    ("std_progressive_passes", "Progressive Passes (standard data)", "Passing", "count"),
    ("progressive_passing_distance", "Progressive Pass Distance", "Passing", "count"),
    ("total_passing_distance", "Total Pass Distance", "Passing", "count"),
    ("avg_pass_distance", "Avg Pass Distance", "Passing", "distance"),
    ("avg_vertical_pass_distance", "Avg Vertical Pass Distance", "Passing", "distance"),
    # Possession and dribbling
    ("progressive_carries", "Progressive Carries", "Possession and dribbling", "count"),
    ("progressive_passes_received", "Progressive Passes Received", "Possession and dribbling",
     "count"),
    ("dribbles_attempted", "Dribbles Attempted", "Possession and dribbling", "count"),
    ("dribbles_completed", "Dribbles Completed", "Possession and dribbling", "count"),
    ("dribble_success_pct", "Dribble Success %", "Possession and dribbling", "pct"),
    ("fouls_drawn", "Fouls Drawn", "Possession and dribbling", "count"),
    ("penalties_won", "Penalties Won", "Possession and dribbling", "tally"),
    ("share_team_touches_pct", "Share of Team Touches %", "Possession and dribbling", "pct"),
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
    ("dribbled_past", "Dribbled Past", "Defending", "count"),
    ("errors_leading_to_shot", "Errors Leading to Shot", "Defending", "count"),
    # Duels
    ("duels_total", "Duels", "Duels", "count"),
    ("duels_won", "Duels Won", "Duels", "count"),
    ("duels_won_pct", "Duels Won %", "Duels", "pct"),
    # Goals added (American Soccer Analysis): how much a player's actions changed
    # his team's chances of scoring and conceding, in goals
    ("goals_added", "Goals Added", "Goals added", "total"),
    ("goals_added_above_avg", "Goals Added Above Avg", "Goals added", "total"),
    ("ga_shooting", "Goals Added: Shooting", "Goals added", "total"),
    ("ga_passing", "Goals Added: Passing", "Goals added", "total"),
    ("ga_receiving", "Goals Added: Receiving", "Goals added", "total"),
    ("ga_dribbling", "Goals Added: Dribbling", "Goals added", "total"),
    ("ga_interrupting", "Goals Added: Defending", "Goals added", "total"),
    ("ga_fouling", "Goals Added: Fouling", "Goals added", "total"),
    # Discipline
    ("yellow_cards", "Yellow Cards", "Discipline", "count"),
    ("red_cards", "Red Cards", "Discipline", "count"),
    ("fouls_committed", "Fouls Committed", "Discipline", "count"),
    ("penalties_conceded", "Penalties Conceded", "Discipline", "tally"),
    ("offsides", "Offsides", "Discipline", "count"),
    # Playing time
    ("minutes", "Minutes", "Playing time", "minutes"),
    ("nineties_played", "90s", "Playing time", "nineties"),
    ("appearances", "Appearances", "Playing time", "tally"),
    ("starts", "Starts", "Playing time", "tally"),
    ("match_rating", "Match Rating", "Playing time", "ratio"),
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
# views (schema.sql), grouped by category, in CATEGORY_ORDER (card order).
# FBref seasons fill the FBref metrics and the newer sources their own; the
# Profile tab hides a metric most of the chosen pool lacks, so each era shows
# the bars it has.
PROFILE_METRICS: dict[str, dict[str, tuple[str, ...]]] = {
    "ATT": {
        "Shooting": ("goals_per_90", "nonpen_goals_per_90", "xg_per_90", "npxg_per_90",
                     "npxg_plus_xag_per_90", "shots_total_per_90", "shots_on_target_per_90",
                     "goals_per_shot", "npxg_per_shot"),
        "Chance creation": ("assists_per_90", "xag_per_90", "key_passes",
                            "passes_into_penalty_area", "crosses_into_penalty_area", "xg_chain"),
        "Passing": ("passes_completion_pct_total", "passes_into_final_third",
                    "progressive_passing_distance"),
        "Possession and dribbling": ("progressive_carries", "dribbles_completed",
                                     "dribble_success_pct", "fouls_drawn"),
        "Duels": ("duels_won_pct",),
        "Goals added": ("goals_added", "ga_shooting", "ga_receiving"),
    },
    "MID": {
        "Shooting": ("goals_per_90", "npxg_per_90", "shots_total_per_90"),
        "Chance creation": ("assists_per_90", "xag_per_90", "key_passes",
                            "passes_into_penalty_area", "crosses_into_penalty_area", "xg_chain",
                            "xg_buildup"),
        "Passing": ("passes_completion_pct_total", "pass_progressive_passes",
                    "progressive_passing_distance", "passes_into_final_third",
                    "passes_completed_over_expected_p100"),
        "Possession and dribbling": ("progressive_carries", "dribbles_completed"),
        "Defending": ("tackles_total", "interceptions", "tackles_plus_interceptions"),
        "Duels": ("duels_won_pct",),
        "Goals added": ("goals_added", "ga_passing", "ga_interrupting"),
    },
    "DEF": {
        "Shooting": ("goals_per_90",),
        "Chance creation": ("assists_per_90", "xg_buildup"),
        "Passing": ("passes_completion_pct_total", "progressive_passing_distance",
                    "passes_into_final_third", "passes_completed_over_expected_p100",
                    "avg_pass_distance", "avg_vertical_pass_distance"),
        "Defending": ("tackles_total", "tackles_won", "tackles_def_3rd", "tackles_mid_3rd",
                      "dribblers_tackled_pct", "interceptions", "tackles_plus_interceptions",
                      "blocks_total", "clearances", "errors_leading_to_shot", "dribbled_past"),
        "Duels": ("duels_won_pct",),
        "Goals added": ("goals_added", "ga_interrupting", "ga_passing"),
        "Discipline": ("fouls_committed",),
    },
    "GK": {
        "Goalkeeping": ("save_pct", "goals_against_per_90", "clean_sheet_pct", "clean_sheets",
                        "pk_save_pct", "saves", "shots_on_target_against"),
        "Playing time": ("match_rating",),
    },
}

# Percentiles for these are inverted, so a longer bar always means better.
LOWER_IS_BETTER = frozenset({
    "errors_leading_to_shot", "yellow_cards", "red_cards",
    "goals_against", "goals_against_per_90", "losses", "penalty_kicks_allowed",
    "dribbled_past", "fouls_committed", "penalties_conceded", "penalties_missed", "offsides",
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
    # Newer seasons: only stats the whole family has (most of Europe has no xG).
    "europe": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Goals", "std_goals"), KpiSpec("Assists", "std_assists"),
        KpiSpec("Shots", "shots_total"), KpiSpec("Match Rating", "match_rating"),
    ),
    "us": (
        KpiSpec("Minutes", "minutes"), KpiSpec("90s", "nineties_played"),
        KpiSpec("Goals", "std_goals"), KpiSpec("Assists", "std_assists"),
        KpiSpec("xG", "std_xg"), KpiSpec("xA", "xag"),
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
    "match_rating": "Average API-Football match rating over his appearances (out of 10).",
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


# --- Data families ---------------------------------------------------------------
# Every player is analysed on one fixed stat set, picked by where his row comes
# from (the Source column the views add). The owner's rule: the same metrics for
# everyone in a family, or none, since a stat only some players have breaks the
# comparison. A metric a player lacks shows "n/a" instead of disappearing.
#   fbref   FBref seasons (2017-18 to 2023-24): PROFILE_METRICS, as before
#   europe  any "API-Football..." source: EUROPE_OUTFIELD / EUROPE_GOALKEEPER
#   us      American Soccer Analysis: US_OUTFIELD (no US keepers are loaded)
# Big-five European players also get BIG_FIVE_EXTRAS (Understat), ranked only
# against big-five players.
FBREF, EUROPE, US = "fbref", "europe", "us"
US_SOURCE = "American Soccer Analysis"
EUROPE_SOURCE_PREFIX = "API-Football"
BIG_FIVE = ("Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1")

# Stored per-90 columns where they exist, otherwise season totals the views
# rank per 90 (metric kind count or total).
EUROPE_OUTFIELD = (
    "goals_per_90", "shots_total_per_90", "shots_on_target_per_90",
    "assists_per_90", "key_passes",
    "passes_attempted_total", "passes_completion_pct_total",
    "dribbles_attempted", "dribbles_completed", "dribble_success_pct", "fouls_drawn",
    "tackles_total", "interceptions", "blocks_total", "dribbled_past",
    "duels_total", "duels_won_pct",
    "fouls_committed", "yellow_cards",
    "match_rating",
)
BIG_FIVE_EXTRAS = ("xg_per_90", "npxg_per_90", "xag_per_90", "xg_chain", "xg_buildup")
US_OUTFIELD = (
    "goals_per_90", "xg_per_90", "shots_total_per_90", "shots_on_target_per_90",
    "assists_per_90", "xag_per_90", "key_passes",
    "passes_attempted_total", "passes_completion_pct_total", "xpass_completion_pct",
    "passes_completed_over_expected_p100", "avg_pass_distance", "avg_vertical_pass_distance",
    "share_team_touches_pct",
    "goals_added", "ga_shooting", "ga_passing", "ga_receiving", "ga_dribbling",
    "ga_interrupting", "ga_fouling",
)
EUROPE_GOALKEEPER = ("save_pct", "goals_against_per_90", "clean_sheet_pct", "saves", "match_rating")
# What both regions have: the trial Europe-and-US comparison uses only these.
SHARED_SET = ("goals_per_90", "shots_total_per_90", "shots_on_target_per_90", "assists_per_90",
              "key_passes", "passes_attempted_total", "passes_completion_pct_total")
# Role mix (newer seasons, outfield): share of minutes per role, from formation
# slots in Europe and ASA positions in the US (src/soccer_data_loader.py).
ROLE_COLUMNS = ("role_st", "role_amw", "role_cm", "role_fb", "role_cb")
ROLE_SHORT = {"ST": "ST", "AMW": "AM/W", "CM": "CM", "FB": "FB", "CB": "CB"}
ROLE_NAMES = {"ST": "Striker", "AMW": "Attacking mid / winger", "CM": "Central / defensive mid",
              "FB": "Fullback / wing-back", "CB": "Centre-back"}
# Too patchy in the newer sources to compare on: kept in the data and BigQuery,
# never shown or compared for the newer seasons.
PATCHY = frozenset({"offsides", "penalties_won", "penalties_conceded", "penalties_missed"})
# Stats tab only: playing time every family has, for context.
PLAYING_TIME_CONTEXT = ("minutes", "nineties_played", "appearances")

# Stored per-90 column -> its season total (the Stats tab shows both on one row).
TOTAL_OF = {per90: total for total, per90 in PER90_PAIRS.items()}


def family_of(source: object) -> str:
    """fbref, europe or us, from a row's Source. Anything else (a missing
    Source, as in the FBref-only tables) is fbref."""
    if not isinstance(source, str):
        return FBREF
    text = source.strip()
    if text == US_SOURCE:
        return US
    if text.startswith(EUROPE_SOURCE_PREFIX):
        return EUROPE
    return FBREF


def is_big_five(source: object, league: object) -> bool:
    return family_of(source) == EUROPE and league in BIG_FIVE


def family_set(family: str, group: str) -> tuple[str, ...]:
    """The fixed stat set of a family and position group, in display order."""
    if family == EUROPE:
        return EUROPE_GOALKEEPER if group == "GK" else EUROPE_OUTFIELD
    if family == US:
        return () if group == "GK" else US_OUTFIELD
    return tuple(profile_keys(group))


def by_category(keys, group: str) -> dict[str, tuple[str, ...]]:
    """`keys` grouped into cards, in the Stats tab's card order."""
    out: dict[str, tuple[str, ...]] = {}
    for category in stats_categories(group):
        in_card = tuple(k for k in keys if STAT_CATEGORIES.get(k) == category)
        if in_card:
            out[category] = in_card
    return out


def profile_layout(family: str, group: str) -> dict[str, tuple[str, ...]]:
    """Profile cards: today's PROFILE_METRICS for FBref, the family set otherwise."""
    if family == FBREF:
        return PROFILE_METRICS.get(group, {})
    return by_category(family_set(family, group), group)


def stats_keys(keys) -> tuple[str, ...]:
    """Stored per-90 columns swapped for their season total (shown beside them)."""
    return tuple(TOTAL_OF.get(k, k) for k in keys)


def stats_layout(family: str, group: str) -> dict[str, tuple[str, ...]]:
    """Stats-tab cards for a family player: the family set plus minutes, 90s
    and appearances."""
    return by_category((*PLAYING_TIME_CONTEXT, *stats_keys(family_set(family, group))), group)
