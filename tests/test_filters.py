from __future__ import annotations

import pandas as pd

from src.ui import filters

COLUMNS = ["Player", "Season", "Squad", "League", "Position", "position_group", "minutes"]
INDEX = pd.DataFrame(
    [
        ("Mohamed Salah", "2023-2024", "Liverpool", "Premier League", "FW", "ATT", 2536),
        ("Mohamed Salah", "2022-2023", "Liverpool", "Premier League", "FW", "ATT", 3290),
        ("Harry Kane", "2023-2024", "Bayern Munich", "Bundesliga", "FW", "ATT", 2617),
        ("Harry Kane", "2022-2023", "Tottenham", "Premier League", "FW", "ATT", 3406),
        ("Alisson", "2023-2024", "Liverpool", "Premier League", "GK", "GK", 2520),
        ("ederson", "2023-2024", "Manchester City", "Premier League", "GK", "GK", 2970),
    ],
    columns=COLUMNS,
)


def test_seasons_desc_leagues_clubs():
    assert filters.seasons_desc(INDEX) == ["2023-2024", "2022-2023"]
    assert filters.leagues(INDEX) == ["Bundesliga", "Premier League"]
    assert filters.clubs(INDEX, None) == ["Bayern Munich", "Liverpool", "Manchester City", "Tottenham"]
    assert filters.clubs(INDEX, "Bundesliga") == ["Bayern Munich"]


def test_filter_index_combines_filters():
    assert len(filters.filter_index(INDEX)) == 6
    assert len(filters.filter_index(INDEX, season="2023-2024")) == 4
    out = filters.filter_index(INDEX, season="2022-2023", league="Premier League", club="Tottenham")
    assert out["Player"].tolist() == ["Harry Kane"]
    assert filters.filter_index(INDEX, season="2022-2023", league="Bundesliga").empty
    assert list(out.index) == [0]  # index reset


def test_search_options_one_row_per_player_latest_season_sorted_casefold():
    opts = filters.search_options(INDEX)
    assert opts["Player"].tolist() == ["Alisson", "ederson", "Harry Kane", "Mohamed Salah"]
    kane = opts[opts["Player"] == "Harry Kane"].iloc[0]
    assert kane["Season"] == "2023-2024" and kane["Squad"] == "Bayern Munich"


def test_search_options_of_empty_frame_is_empty():
    assert filters.search_options(INDEX.iloc[0:0]).empty


def test_option_label():
    assert filters.option_label("Mohamed Salah", "Liverpool", "Premier League") == \
        "Mohamed Salah — Liverpool, Premier League"


def test_player_seasons_ascending():
    assert filters.player_seasons(INDEX, "Mohamed Salah") == ["2022-2023", "2023-2024"]
    assert filters.player_seasons(INDEX, "Nobody") == []


def test_pick_example_prefers_listed_names_then_most_minutes():
    assert filters.pick_example(INDEX, ("Kevin De Bruyne", "Alisson")) == "Alisson"
    assert filters.pick_example(INDEX, ("Kevin De Bruyne",)) == "Harry Kane"  # 3406 minutes
    assert filters.pick_example(INDEX.iloc[0:0], ("Alisson",)) is None


# A player who moved from MLS to Europe. The two label kinds interleave by
# leading year, the European season (which ends later) first on a tie:
# 2026, 2025-2026, 2025, 2024-2025, 2024.
MIXED = pd.DataFrame(
    [
        ("Mover", "2024", "Club A", "MLS", "FW", "ATT", 2500),
        ("Mover", "2025", "Club A", "MLS", "FW", "ATT", 3000),
        ("Mover", "2024-2025", "Club B", "Championship", "FW", "ATT", 300),
        ("Mover", "2025-2026", "Club B", "Championship", "FW", "ATT", 2000),
        ("Other", "2026", "Club C", "USL Championship", "MF", "MID", 900),
        ("Other", "2023-2024", "Club D", "Ligue 1", "MF", "MID", 2900),
    ],
    columns=COLUMNS,
)


def test_seasons_desc_puts_the_newest_season_first_across_both_kinds():
    assert filters.seasons_desc(MIXED) == ["2026", "2025-2026", "2025", "2024-2025", "2024", "2023-2024"]


def test_player_seasons_ascending_across_both_kinds():
    assert filters.player_seasons(MIXED, "Mover") == ["2024", "2024-2025", "2025", "2025-2026"]


def test_search_options_pick_the_newest_season_across_both_kinds():
    opts = filters.search_options(MIXED).set_index("Player")
    assert opts.loc["Mover", "Season"] == "2025-2026"  # not "2025" (more minutes, older)
    assert opts.loc["Other", "Season"] == "2026"


def test_default_season_is_the_newest_european_season():
    assert filters.default_season(["2026", "2025-2026", "2025", "2024-2025"]) == "2025-2026"
    assert filters.default_season(filters.seasons_desc(MIXED)) == "2025-2026"


def test_default_season_falls_back_to_the_newest_season():
    assert filters.default_season(["2026", "2025", "2024"]) == "2026"
    assert filters.default_season([]) is None


def test_plain_alias_makes_letters_without_an_accent_fold_searchable():
    assert filters.plain_alias("Martin Ødegaard") == "Odegaard"
    assert filters.plain_alias("Kylian Mbappé") == ""  # the search box folds é itself
    assert filters.plain_alias("Jan Bednarek Łukasz Ąb") == "Lukasz"
    assert filters.option_label("Martin Ødegaard", "Arsenal", "Premier League") ==         "Martin Ødegaard (Odegaard) — Arsenal, Premier League"
    assert filters.option_label("Bukayo Saka", "Arsenal", "Premier League") ==         "Bukayo Saka — Arsenal, Premier League"


def test_preferred_season_falls_back_to_the_latest_season_with_enough_minutes():
    minutes = {"2025-2026": 300.0, "2024-2025": 2400.0, "2023-2024": 3000.0, "2026": 100.0}
    assert filters.preferred_season(minutes, "2025-2026", 450) == "2024-2025"  # too few in 2025/26
    assert filters.preferred_season(minutes, "2023-2024", 450) == "2023-2024"  # enough there
    assert filters.preferred_season(minutes, None, 450) == "2024-2025"
    assert filters.preferred_season({"2025-2026": 100.0}, None, 450) == "2025-2026"  # none has enough
    assert filters.preferred_season({}, None, 450) is None
