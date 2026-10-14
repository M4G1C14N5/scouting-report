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
