"""The soccer-data loader: mapping, SQL and the CLI. Never touches BigQuery."""

from __future__ import annotations

import math
import os
import re
import runpy
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from src import soccer_data_loader as loader
from tests.schema_utils import feature_columns

ROOT = Path(__file__).resolve().parents[1]

MART_COLUMNS = [
    "source", "source_player_id", "league_id", "league_code", "league_name", "country",
    "season", "season_label", "player_name", "birth_date", "nationality", "team_id", "team",
    "teams", "stint_count", "position_group", "position_detail", "minutes", "appearances",
    "starts", "goals", "assists", "penalties_scored", "shots", "shots_on_target", "key_passes",
    "passes", "pass_completion_pct", "passes_completed_over_expected_p100",
    "share_team_touches", "tackles", "interceptions", "blocks", "duels", "duels_won",
    "dribbles_attempted", "dribbles_won", "fouls_drawn", "fouls_committed", "yellow_cards",
    "red_cards", "saves", "goals_conceded", "penalties_saved", "rating", "xg", "npxg", "xa",
    "goals_added_raw", "goals_added_above_avg", "ga_dribbling", "ga_fouling",
    "ga_interrupting", "ga_passing", "ga_receiving", "ga_shooting",
]


def _row(**values) -> dict:
    row = {c: None for c in MART_COLUMNS}
    row.update(values)
    return row


def _born(text: str | None):
    return pd.Timestamp(text) if text else None


ROWS = [
    # ASA striker: no penalties, npxg or defensive stats
    _row(source="asa", league_code="MLS", season_label="2025", player_name="Carlos Striker",
         birth_date=_born("2000-03-02"), nationality="USA", team="Austin FC",
         position_group="ATT", minutes=1800, goals=10, assists=4, shots=50, shots_on_target=20,
         key_passes=30, passes=600, pass_completion_pct=75.5, xg=8.0, xa=3.0),
    # API-Football + Understat midfielder
    _row(source="apif", league_code="ENG1", season_label="2025-2026",
         player_name="Midfield Maestro", birth_date=_born("2001-08-01"), nationality="England",
         team="Arsenal", position_group="MID", minutes=2700, goals=6, assists=8,
         penalties_scored=2, shots=30, shots_on_target=12, key_passes=60, passes=2000,
         tackles=50, interceptions=30, blocks=10, yellow_cards=5, red_cards=0,
         saves=0, goals_conceded=0, xg=4.5, npxg=3.0, xa=6.0),
    # apif_matches League Two defender: no birth date, nationality or xG; zero shots
    _row(source="apif_matches", league_code="ENG4", season_label="2024-2025",
         player_name="Lower Defender", team="Walsall", position_group="DEF", minutes=900,
         goals=0, assists=1, penalties_scored=0, shots=0, shots_on_target=0, key_passes=3,
         passes=400, tackles=30, interceptions=20, blocks=5, yellow_cards=2, red_cards=1),
    # API-Football keeper
    _row(source="apif", league_code="ESP1", season_label="2024-2025", player_name="Keeper One",
         birth_date=_born("1995-01-15"), nationality="Spain", team="Getafe",
         position_group="GK", minutes=1800, saves=60, goals_conceded=20, penalties_saved=1,
         goals=0, assists=0, shots=0),
    # ASA keeper: dropped (ASA has no keeper stats)
    _row(source="asa", league_code="MLS", season_label="2024", player_name="Asa Keeper",
         team="LA Galaxy", position_group="GK", minutes=2000, goals=0, xg=0.0),
    # Null position: dropped
    _row(source="apif", league_code="ENG1", season_label="2025-2026",
         player_name="Nobody Known", team="Burnley", position_group=None, minutes=500),
    # Same name, same season, different clubs (one outfield, one keeper)
    _row(source="apif", league_code="ENG1", season_label="2025-2026", player_name="J. Smith",
         team="Chelsea", position_group="ATT", minutes=1000, goals=5, shots=20,
         shots_on_target=10, penalties_scored=1),
    _row(source="apif", league_code="SCO1", season_label="2025-2026", player_name="J. Smith",
         team="Celtic", position_group="GK", minutes=3000, saves=0, goals_conceded=0),
    # Same name, same club name, different leagues: needs the league too
    _row(source="apif", league_code="POR1", season_label="2025-2026", player_name="R. Costa",
         team="Sporting", position_group="DEF", minutes=1200),
    _row(source="apif", league_code="BEL1", season_label="2025-2026", player_name="R. Costa",
         team="Sporting", position_group="DEF", minutes=1300),
]


# read_players adds these from the Understat link and the API-Football profiles
LINK_COLUMNS = ["understat_name", "profile_first_name"]


def players(rows: list[dict] | None = None) -> pd.DataFrame:
    return pd.DataFrame(ROWS if rows is None else rows, columns=MART_COLUMNS + LINK_COLUMNS)


@pytest.fixture(scope="module")
def frames():
    report: dict = {}
    outfield, goalkeeper = loader.build_frames(players(), report=report)
    return outfield, goalkeeper, report


def _one(frame: pd.DataFrame, player: str) -> pd.Series:
    hit = frame[frame["Player"] == player]
    assert len(hit) == 1, (player, list(frame["Player"]))
    return hit.iloc[0]


def _null(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


# --- constants ---------------------------------------------------------------

def test_leagues_and_seasons():
    assert loader.LEAGUES == {
        "ENG1": "Premier League", "ESP1": "La Liga", "GER1": "Bundesliga", "ITA1": "Serie A",
        "FRA1": "Ligue 1", "ENG2": "Championship", "ENG3": "League One", "ENG4": "League Two",
        "POR1": "Primeira Liga", "NED1": "Eredivisie", "BEL1": "Belgian Pro League",
        "AUT1": "Austrian Bundesliga", "SUI1": "Swiss Super League", "DEN1": "Danish Superliga",
        "SCO1": "Scottish Premiership", "MLS": "MLS", "USLC": "USL Championship",
        "USL1": "USL League One", "MLSNP": "MLS NEXT Pro",
    }
    assert loader.SEASONS == {"2024-2025", "2025-2026", "2024", "2025", "2026"}


def test_column_lists_match_schema_sql():
    assert loader.OUTFIELD_COLUMNS == feature_columns("outfield_features")
    assert loader.GOALKEEPER_COLUMNS == feature_columns("goalkeeper_features")


# --- warehouse ---------------------------------------------------------------

def test_warehouse_path_precedence(monkeypatch, tmp_path):
    monkeypatch.delenv("SOCCER_DATA_WAREHOUSE", raising=False)
    monkeypatch.delenv("SOCCER_DATA_ROOT", raising=False)
    assert loader.warehouse_path() == Path.home() / "soccer-data" / "warehouse.duckdb"
    monkeypatch.setenv("SOCCER_DATA_ROOT", str(tmp_path))
    assert loader.warehouse_path() == tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("SOCCER_DATA_WAREHOUSE", str(tmp_path / "w.duckdb"))
    assert loader.warehouse_path() == tmp_path / "w.duckdb"


def _make_warehouse(path: Path, rows: list[dict]) -> None:
    """A tiny warehouse.duckdb: player_season_stats plus the name-lookup tables."""
    frame = pd.DataFrame(rows, columns=MART_COLUMNS)
    con = duckdb.connect(str(path))
    con.register("src_frame", frame)
    con.execute("CREATE TABLE player_season_stats AS SELECT * FROM src_frame")
    con.execute("CREATE TABLE league_info (apif_league_id INTEGER, league_code VARCHAR, "
                "understat_league VARCHAR)")
    con.execute("INSERT INTO league_info VALUES (39, 'ENG1', 'EPL'), (140, 'ESP1', 'La_liga')")
    con.execute("CREATE TABLE int_understat__apif_match (understat_player_id BIGINT, "
                "apif_player_id BIGINT, league_id INTEGER, season BIGINT)")
    con.execute("INSERT INTO int_understat__apif_match VALUES (1250, 306, 39, 2025), "
                "(1250, 306, 39, 2024)")
    con.execute("CREATE TABLE stg_understat__player_seasons (understat_player_id BIGINT, "
                "player_name VARCHAR, league VARCHAR, season BIGINT)")
    con.execute("INSERT INTO stg_understat__player_seasons VALUES "
                "(1250, 'Mohamed Salah', 'EPL', 2025), (1250, 'Mo Salah', 'EPL', 2024), "
                "(1250, 'Wrong League', 'La_liga', 2025)")
    con.execute("CREATE TABLE stg_apif__players (player_id BIGINT, player_name VARCHAR, "
                "first_name VARCHAR, last_name VARCHAR)")
    con.execute("INSERT INTO stg_apif__players VALUES (306, 'M. Salah', 'Mohamed', 'Salah'), "
                "(1100, 'E. Haaland', 'Erling Braut', 'Haaland'), "
                "(900, 'M. Dmitrović', 'Marko', 'Dmitrović')")
    con.close()


def test_read_players_filters_league_season_and_minutes(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    extra = [
        _row(source="apif", league_code="ARG1", season_label="2025", player_name="Other League",
             position_group="MID", minutes=2000),
        _row(source="apif", league_code="ENG1", season_label="2023-2024",
             player_name="Old Season", position_group="MID", minutes=2000),
        _row(source="apif", league_code="ENG1", season_label="2025-2026",
             player_name="Few Minutes", position_group="MID", minutes=89),
        _row(source="apif", league_code="ENG1", season_label="2025-2026",
             player_name="Just Enough", position_group="MID", minutes=90),
    ]
    linked = [
        _row(source="apif", source_player_id="306", league_id=39, season=2025,
             league_code="ENG1", season_label="2025-2026", player_name="M. Salah",
             team="Liverpool", position_group="ATT", minutes=2500),
        _row(source="apif", source_player_id="1100", league_id=39, season=2025,
             league_code="ENG1", season_label="2025-2026", player_name="E. Haaland",
             team="Manchester City", position_group="ATT", minutes=2400),
    ]
    _make_warehouse(path, ROWS + extra + linked)

    got = loader.read_players(path)
    names = set(got["player_name"])
    assert "Just Enough" in names
    assert not names & {"Other League", "Old Season", "Few Minutes"}
    assert len(got) == len(ROWS) + 1 + len(linked)
    salah = got[got["player_name"] == "M. Salah"].iloc[0]
    assert salah["understat_name"] == "Mohamed Salah"
    assert salah["profile_first_name"] == "Mohamed"
    haaland = got[got["player_name"] == "E. Haaland"].iloc[0]
    assert pd.isna(haaland["understat_name"])
    assert haaland["profile_first_name"] == "Erling Braut"
    assert got.loc[got["source"] == "asa", "profile_first_name"].isna().all()


# --- build_frames ------------------------------------------------------------

def test_frames_have_exactly_the_hub_columns_plus_source(frames):
    outfield, goalkeeper, _ = frames
    assert list(outfield.columns) == feature_columns("outfield_features") + ["Source"]
    assert list(goalkeeper.columns) == feature_columns("goalkeeper_features") + ["Source"]


def test_identity_league_season_position(frames):
    outfield, goalkeeper, _ = frames
    asa = _one(outfield, "Carlos Striker")
    assert (asa["League"], asa["Season"], asa["Squad"], asa["Nation"]) == (
        "MLS", "2025", "Austin FC", "USA")
    assert (asa["Position"], asa["position_group"]) == ("FW", "ATT")
    mid = _one(outfield, "Midfield Maestro")
    assert (mid["League"], mid["Season"], mid["Position"]) == ("Premier League", "2025-2026", "MF")
    dfn = _one(outfield, "Lower Defender")
    assert (dfn["League"], dfn["Season"], dfn["Position"]) == ("League Two", "2024-2025", "DF")
    gk = _one(goalkeeper, "Keeper One")
    assert (gk["League"], gk["Position"], gk["position_group"]) == ("La Liga", "GK", "GK")
    assert set(outfield["position_group"]) <= {"ATT", "MID", "DEF"}
    assert set(goalkeeper["position_group"]) == {"GK"}


def test_age_on_season_start_and_born(frames):
    outfield, goalkeeper, _ = frames
    # US season starts 1 March 2025; born 2 March 2000 -> still 24
    asa = _one(outfield, "Carlos Striker")
    assert asa["Age"] == 24.0 and asa["Born"] == 2000.0
    # European season starts 1 August 2025; born 1 August 2001 -> turns 24 that day
    mid = _one(outfield, "Midfield Maestro")
    assert mid["Age"] == 24.0 and mid["Born"] == 2001.0
    # 1 August 2024, born 15 January 1995 -> 29
    assert _one(goalkeeper, "Keeper One")["Age"] == 29.0
    dfn = _one(outfield, "Lower Defender")
    assert _null(dfn["Age"]) and _null(dfn["Born"]) and _null(dfn["Nation"])


def test_outfield_maths(frames):
    outfield, _, _ = frames
    mid = _one(outfield, "Midfield Maestro")
    assert mid["minutes"] == 2700 and mid["nineties_played"] == pytest.approx(30.0)
    assert mid["std_goals"] == 6 and mid["std_assists"] == 8
    assert mid["penalty_kicks_made"] == 2 and mid["nonpen_goals"] == 4
    assert mid["std_xg"] == 4.5 and mid["npxg"] == 3.0
    assert mid["xag"] == 6.0 and mid["expected_assisted_goals"] == 6.0
    assert mid["shots_total"] == 30 and mid["shots_on_target"] == 12
    assert mid["shots_on_target_pct"] == pytest.approx(40.0)
    assert mid["goals_per_shot"] == pytest.approx(0.2)
    assert mid["goals_per_shot_on_target"] == pytest.approx(0.5)
    assert mid["npxg_per_shot"] == pytest.approx(0.1)
    assert mid["goals_minus_xg"] == pytest.approx(1.5)
    assert mid["nonpen_goals_minus_npxg"] == pytest.approx(1.0)
    assert mid["goals_per_90"] == pytest.approx(0.2)
    assert mid["nonpen_goals_per_90"] == pytest.approx(4 / 30)
    assert mid["xg_per_90"] == pytest.approx(0.15)
    assert mid["npxg_per_90"] == pytest.approx(0.1)
    assert mid["shots_total_per_90"] == pytest.approx(1.0)
    assert mid["shots_on_target_per_90"] == pytest.approx(0.4)
    assert mid["assists_per_90"] == pytest.approx(8 / 30)
    assert mid["xag_per_90"] == pytest.approx(0.2)
    assert mid["npxg_plus_xag_per_90"] == pytest.approx(0.3)
    assert mid["key_passes"] == 60 and mid["passes_attempted_total"] == 2000
    assert mid["tackles_total"] == 50 and mid["interceptions"] == 30
    assert mid["tackles_plus_interceptions"] == 80 and mid["blocks_total"] == 10
    assert mid["yellow_cards"] == 5 and mid["red_cards"] == 0
    # apif has no pass completion: stays null
    assert _null(mid["passes_completion_pct_total"])


def test_division_by_zero_gives_null(frames):
    outfield, _, _ = frames
    dfn = _one(outfield, "Lower Defender")
    assert dfn["shots_total"] == 0
    for col in ("shots_on_target_pct", "goals_per_shot", "goals_per_shot_on_target",
                "npxg_per_shot"):
        assert _null(dfn[col]), col
    assert dfn["goals_per_90"] == 0.0  # 0 goals over 10 nineties is a real zero


def test_null_stays_null_for_asa(frames):
    outfield, _, _ = frames
    asa = _one(outfield, "Carlos Striker")
    for col in ("tackles_total", "interceptions", "tackles_plus_interceptions", "blocks_total",
                "yellow_cards", "red_cards", "penalty_kicks_made", "nonpen_goals",
                "nonpen_goals_per_90", "npxg", "npxg_per_90", "npxg_per_shot",
                "nonpen_goals_minus_npxg", "npxg_plus_xag_per_90"):
        assert _null(asa[col]), col
    assert asa["passes_completion_pct_total"] == 75.5
    assert asa["std_xg"] == 8.0 and asa["xg_per_90"] == pytest.approx(0.4)
    assert asa["goals_minus_xg"] == pytest.approx(2.0)


def test_unmapped_metrics_are_null(frames):
    outfield, goalkeeper, _ = frames
    mapped = {
        "minutes", "nineties_played", "std_goals", "std_assists", "nonpen_goals",
        "penalty_kicks_made", "yellow_cards", "red_cards", "std_xg", "npxg", "xag",
        "goals_per_90", "assists_per_90", "nonpen_goals_per_90", "xg_per_90", "xag_per_90",
        "npxg_per_90", "npxg_plus_xag_per_90", "shots_total", "shots_on_target",
        "shots_on_target_pct", "shots_total_per_90", "shots_on_target_per_90",
        "goals_per_shot", "goals_per_shot_on_target", "npxg_per_shot", "goals_minus_xg",
        "nonpen_goals_minus_npxg", "passes_attempted_total", "passes_completion_pct_total",
        "expected_assisted_goals", "key_passes", "tackles_total", "blocks_total",
        "interceptions", "tackles_plus_interceptions",
        "goals_against", "goals_against_per_90", "shots_on_target_against", "saves",
        "save_pct", "penalty_kicks_saved",
    }
    for frame in (outfield, goalkeeper):
        for col in frame.columns:
            if col in mapped or col in loader.IDENTITY_COLUMNS or col == "Source":
                continue
            assert frame[col].isna().all(), col


def test_keeper_mapping(frames):
    _, goalkeeper, _ = frames
    gk = _one(goalkeeper, "Keeper One")
    assert gk["goals_against"] == 20 and gk["saves"] == 60
    assert gk["shots_on_target_against"] == 80
    assert gk["save_pct"] == pytest.approx(75.0)
    assert gk["goals_against_per_90"] == pytest.approx(1.0)
    assert gk["penalty_kicks_saved"] == 1
    assert gk["nineties_played"] == pytest.approx(20.0)
    # nothing faced: save % is null, not 0 or 100
    assert _null(_one(goalkeeper, "J. Smith (Celtic)")["save_pct"])


def test_dropped_rows_are_counted(frames):
    outfield, goalkeeper, report = frames
    names = set(outfield["Player"]) | set(goalkeeper["Player"])
    assert "Asa Keeper" not in names and "Nobody Known" not in names
    assert report["dropped_null_position"] == 1
    assert report["dropped_asa_keepers"] == 1
    assert len(outfield) == 6 and len(goalkeeper) == 2


def test_source_strings(frames):
    outfield, goalkeeper, _ = frames
    assert _one(outfield, "Carlos Striker")["Source"] == "American Soccer Analysis"
    assert _one(outfield, "Midfield Maestro")["Source"] == "API-Football + Understat"
    assert _one(outfield, "J. Smith (Chelsea)")["Source"] == "API-Football"
    assert _one(outfield, "Lower Defender")["Source"] == "API-Football match data"
    assert _one(goalkeeper, "Keeper One")["Source"] == "API-Football"


def test_collisions_are_renamed(frames):
    outfield, goalkeeper, report = frames
    names = set(outfield["Player"]) | set(goalkeeper["Player"])
    assert "J. Smith" not in names and "R. Costa" not in names
    assert "J. Smith (Chelsea)" in set(outfield["Player"])
    assert "J. Smith (Celtic)" in set(goalkeeper["Player"])
    assert {"R. Costa (Sporting, Primeira Liga)",
            "R. Costa (Sporting, Belgian Pro League)"} <= set(outfield["Player"])
    both = pd.concat([outfield[["Player", "Season"]], goalkeeper[["Player", "Season"]]])
    assert not both.duplicated().any()
    assert report["renamed"] == 4
    assert len(report["renamed_examples"]) >= 3


def test_same_name_same_club_falls_back_to_birth_year_then_a_number():
    # Real data has these: two "A. Koné" at Reims in one season, both born 2005.
    common = dict(source="apif", league_code="FRA1", season_label="2024-2025", team="Reims",
                  position_group="DEF")
    rows = [
        _row(player_name="B. Twin", birth_date=_born("2004-07-11"), minutes=270, **common),
        _row(player_name="B. Twin", birth_date=_born("2005-12-09"), minutes=1409, **common),
        _row(player_name="A. Same", birth_date=_born("2005-04-22"), minutes=131, **common),
        _row(player_name="A. Same", birth_date=_born("2005-05-14"), minutes=1430, **common),
        _row(player_name="Z. None", minutes=360, **common),
        _row(player_name="Z. None", minutes=3240, **common),
    ]
    report: dict = {}
    outfield, _ = loader.build_frames(pd.DataFrame(rows, columns=MART_COLUMNS), report=report)
    by_minutes = dict(zip(outfield["minutes"], outfield["Player"]))
    assert by_minutes[270] == "B. Twin (Reims, Ligue 1, born 2004)"
    assert by_minutes[1409] == "B. Twin (Reims, Ligue 1, born 2005)"
    # same birth year: numbered by minutes, most minutes first
    assert by_minutes[1430] == "A. Same (Reims, Ligue 1, #1)"
    assert by_minutes[131] == "A. Same (Reims, Ligue 1, #2)"
    assert by_minutes[3240] == "Z. None (Reims, Ligue 1, #1)"
    assert by_minutes[360] == "Z. None (Reims, Ligue 1, #2)"
    assert report["renamed"] == 6


# --- display names -------------------------------------------------------------

def test_display_names_profile_first_then_understat_only_for_abbreviated():
    common = dict(league_code="ENG1", season_label="2025-2026", position_group="ATT")
    rows = [
        # Understat equals the profile expansion bar accents: the profile wins
        _row(source="apif", player_name="M. Salah", team="Liverpool", minutes=2500,
             understat_name="Mohamed Salah", profile_first_name="Mohamed", **common),
        _row(source="apif", player_name="M. Dmitrović", team="Espanyol", minutes=3000,
             understat_name="Marko Dmitrovic", profile_first_name="Marko", **common),
        # Understat shares the surname's last word: Understat wins
        _row(source="apif", player_name="P. Foden", team="Manchester City", minutes=2300,
             understat_name="Phil Foden", profile_first_name="Philip", **common),
        # Understat's surname differs: the profile expansion wins
        _row(source="apif", player_name="K. Mbappé", team="Real Madrid", minutes=2600,
             league_code="ESP1", season_label="2025-2026", position_group="ATT",
             understat_name="Kylian Mbappe-Lottin", profile_first_name="Kylian"),
        _row(source="apif", player_name="B. De Cordova-Reid", team="Leicester", minutes=1600,
             understat_name="Bobby Reid", profile_first_name="Bobby Armani", **common),
        _row(source="apif", player_name="E. Haaland", team="Manchester City", minutes=2400,
             profile_first_name="Erling Braut", **common),
        _row(source="apif", player_name="E. Mendy", team="Al Ahli", minutes=1800,
             profile_first_name="Édouard Osoque", **common),  # accent-insensitive
        _row(source="apif_matches", player_name="J. Doe", team="Walsall", minutes=900,
             profile_first_name="john", league_code="ENG4", season_label="2025-2026",
             position_group="DEF"),  # case-insensitive
        # Understat only when (b) does not apply (HTML entities decoded)
        _row(source="apif", player_name="H. Maguire", team="Manchester United", minutes=2000,
             understat_name="Harry Maguire", **common),
        _row(source="apif", player_name="N. Kanté", team="Chelsea", minutes=2000,
             understat_name="N&#039;Golo Kanté", **common),
        _row(source="apif", player_name="A. Masuaku", team="Lens", minutes=1700,
             understat_name="Arthur Masuaku", profile_first_name="Kiki", **common),
        # never changed: already-full names, even when linked to Understat
        _row(source="apif", player_name="Dani Raba", team="Leganés", minutes=1900,
             understat_name="Raba", profile_first_name="Daniel", **common),
        _row(source="apif", player_name="Vinícius Júnior", team="Real Madrid", minutes=2700,
             understat_name="Vinicius Junior", profile_first_name="Vinícius José", **common),
        _row(source="apif", player_name="Bruno Fernandes", team="Manchester United",
             minutes=2700, profile_first_name="Bruno Miguel", **common),
        # kept: abbreviated with no usable first name and no Understat link
        _row(source="apif", player_name="K. Mismatch", team="Burnley", minutes=1500,
             profile_first_name="Jacky", **common),
        _row(source="apif", player_name="P. Noprofile", team="Fulham", minutes=1200, **common),
        # ASA names never change
        _row(source="asa", league_code="MLS", season_label="2025", player_name="A. Asa",
             team="Austin FC", position_group="MID", minutes=1500,
             understat_name="Should Not", profile_first_name="Alan"),
    ]
    report: dict = {}
    outfield, _ = loader.build_frames(players(rows), report=report)
    names = set(outfield["Player"])
    assert {"Mohamed Salah", "Marko Dmitrović", "Phil Foden", "Kylian Mbappé",
            "Bobby De Cordova-Reid", "Erling Haaland", "Édouard Mendy", "john Doe",
            "Harry Maguire", "N'Golo Kanté", "Arthur Masuaku", "Dani Raba",
            "Vinícius Júnior", "Bruno Fernandes", "K. Mismatch", "P. Noprofile",
            "A. Asa"} == names
    assert report["names_initial"] == 7
    assert report["names_understat"] == 4
    assert len(report["name_examples"]) == 11
    assert "M. Salah -> Mohamed Salah" in report["name_examples"][0]


def test_api_football_names_are_unescaped_and_trimmed_before_matching():
    common = dict(source="apif", league_code="ENG1", season_label="2025-2026",
                  position_group="DEF")
    rows = [
        _row(player_name="D. O&apos;Shea", team="Ipswich", minutes=2500,
             profile_first_name=" Dara ", **common),
        _row(player_name="Jake O&apos;Brien", team="Everton", minutes=2400, **common),
        _row(player_name="Ilyas  Ansah ", team="Union Berlin", minutes=1800, **common),
        _row(player_name="&apos;Q. Odd", team="Burnley", minutes=900, **common),
        _row(player_name="N. Kanté", team="Chelsea", minutes=2000,
             understat_name="N&#039;Golo Kanté", **common),
        # ASA names are left exactly as they are
        _row(source="asa", league_code="MLS", season_label="2025", player_name="A. O&apos;Asa",
             team="Austin FC", position_group="MID", minutes=1500),
    ]
    report: dict = {}
    outfield, _ = loader.build_frames(players(rows), report=report)
    names = set(outfield["Player"])
    assert {"Dara O'Shea", "Jake O'Brien", "Ilyas Ansah", "'Q. Odd", "N'Golo Kanté",
            "A. O&apos;Asa"} == names
    assert not any("&apos;" in n or "&#" in n or " " in n
                   for n in names if n != "A. O&apos;Asa")
    assert report["names_cleaned"] == 4


def test_names_are_fixed_before_collision_renaming():
    common = dict(source="apif", league_code="ENG1", season_label="2025-2026",
                  position_group="MID")
    rows = [
        _row(player_name="J. Smith", team="Chelsea", minutes=1000, profile_first_name="James",
             **common),
        _row(player_name="J. Smith", team="Everton", minutes=1100, profile_first_name="John",
             **common),
    ]
    report: dict = {}
    outfield, _ = loader.build_frames(players(rows), report=report)
    assert set(outfield["Player"]) == {"James Smith", "John Smith"}
    assert report["renamed"] == 0


# --- plausibility --------------------------------------------------------------

def test_implausible_per_90_rows_are_dropped():
    europe = dict(source="apif", league_code="ESP1", season_label="2024-2025")
    rows = [
        # full-season keeper totals on partial minutes
        _row(player_name="G. Full", team="Valencia", position_group="GK", minutes=270,
             goals_conceded=48, saves=93, **europe),
        _row(player_name="S. Saves", team="Getafe", position_group="GK", minutes=360,
             goals_conceded=2, saves=37, **europe),          # 9.25 saves per 90
        _row(player_name="K. Fine", team="Girona", position_group="GK", minutes=360,
             goals_conceded=16, saves=36, **europe),         # 4 GA and 9 saves per 90: kept
        _row(player_name="S. Shots", team="Betis", position_group="ATT", minutes=270,
             shots=31, goals=1, key_passes=1, **europe),     # 10.3 shots per 90
        _row(player_name="G. Goals", team="Betis", position_group="ATT", minutes=270,
             shots=15, goals=10, **europe),                  # 3.3 goals per 90
        _row(player_name="K. Passes", team="Sevilla", position_group="MID", minutes=450,
             key_passes=41, **europe),                       # 8.2 key passes per 90
        _row(player_name="O. Edge", team="Sevilla", position_group="MID", minutes=270,
             shots=30, goals=9, key_passes=24, **europe),    # exactly on every limit: kept
        # under 270 minutes the checks do not apply: short real stints stay
        _row(player_name="D. Short", team="Leicester", position_group="GK", minutes=134,
             goals_conceded=6, saves=4, **europe),           # 4.03 GA per 90
        _row(player_name="E. Burst", team="Vejle", position_group="ATT", minutes=269,
             shots=40, goals=12, key_passes=30, **europe),
        _row(source="asa", league_code="MLS", season_label="2025", player_name="A. Nulls",
             team="Austin FC", position_group="DEF", minutes=900, goals=0, shots=2),
    ]
    report: dict = {}
    outfield, goalkeeper = loader.build_frames(players(rows), report=report)
    assert set(goalkeeper["Player"]) == {"K. Fine", "D. Short"}
    assert set(outfield["Player"]) == {"O. Edge", "E. Burst", "A. Nulls"}
    assert report["dropped_implausible"] == 5
    assert report["implausible_by_league_season"] == {("La Liga", "2024-2025"): 5}
    assert len(report["implausible_examples"]) == 5
    assert any("G. Full" in e for e in report["implausible_examples"])


def test_plausibility_thresholds_are_constants():
    assert loader.MAX_GK_GOALS_AGAINST_PER_90 == 4
    assert loader.MAX_GK_SAVES_PER_90 == 9
    assert loader.MAX_SHOTS_PER_90 == 10
    assert loader.MAX_GOALS_PER_90 == 3
    assert loader.MAX_KEY_PASSES_PER_90 == 8
    assert loader.MIN_MINUTES_FOR_PLAUSIBILITY == 270


# --- view SQL ----------------------------------------------------------------

FBREF_COLUMNS = [("Player", "STRING"), ("Age", "INTEGER"), ("minutes", "INT64"),
                 ("save_pct", "FLOAT"), ("Season", "STRING")]


def test_view_sql_casts_v2_columns_and_names_only_its_tables():
    sql = loader.view_sql("goalkeeper_features", FBREF_COLUMNS)
    assert re.search(r"CREATE OR REPLACE VIEW `[\w-]+\.scouting\.goalkeeper_all`", sql)
    first, second = sql.split("UNION ALL")
    assert "'FBref' AS Source" in first
    assert "CAST(Age AS INT64) AS Age" in second
    assert "CAST(minutes AS INT64) AS minutes" in second
    assert "CAST(save_pct AS FLOAT64) AS save_pct" in second
    assert "CAST(Player AS STRING) AS Player" in second
    assert re.search(r"\bSource\b", second)
    tables = re.findall(r"FROM\s+`([^`]+)`", sql)
    assert [t.split(".")[-1] for t in tables] == ["goalkeeper_features", "goalkeeper_features_v2"]
    referenced = {t.split(".")[-1] for t in re.findall(r"`([^`]+)`", sql)}
    assert referenced == {"goalkeeper_all", "goalkeeper_features", "goalkeeper_features_v2"}


# --- load (fake client) ------------------------------------------------------

class FakeJob:
    def result(self):
        return self


class FakeClient:
    def __init__(self, schemas: dict[str, list[tuple[str, str]]]):
        self.schemas = schemas
        self.loads: list[tuple[str, pd.DataFrame, object]] = []
        self.queries: list[str] = []

    def get_table(self, ref: str):
        name = ref.split(".")[-1]
        fields = [SimpleNamespace(name=n, field_type=t) for n, t in self.schemas[name]]
        return SimpleNamespace(schema=fields)

    def load_table_from_dataframe(self, frame, ref, job_config=None):
        self.loads.append((ref, frame, job_config))
        return FakeJob()

    def query(self, sql):
        self.queries.append(sql)
        return FakeJob()


def _live_schema(table: str) -> list[tuple[str, str]]:
    ints = {"Age", "Born", "minutes", "std_goals", "goals_against", "saves"}
    out = []
    for col in feature_columns(table):
        if col in loader.IDENTITY_COLUMNS and col not in ints:
            out.append((col, "STRING"))
        else:
            out.append((col, "INTEGER" if col in ints else "FLOAT"))
    return out


def test_load_writes_only_v2_tables_and_views(frames):
    outfield, goalkeeper, _ = frames
    client = FakeClient({t: _live_schema(t) for t in ("outfield_features", "goalkeeper_features")})
    counts = loader.load(outfield, goalkeeper, client)
    assert counts == {"outfield_features_v2": len(outfield),
                      "goalkeeper_features_v2": len(goalkeeper)}

    written = [ref.split(".")[-1] for ref, _, _ in client.loads]
    assert written == ["outfield_features_v2", "goalkeeper_features_v2"]
    for ref, frame, config in client.loads:
        assert config.write_disposition == "WRITE_TRUNCATE"
        names = [f.name for f in config.schema]
        assert names[-1] == "Source" and config.schema[-1].field_type == "STRING"
        assert names == list(frame.columns)
        # integer columns arrive as nullable integers, not floats
        assert str(frame["Age"].dtype) == "Int64"
    assert len(client.queries) == 2
    for sql in client.queries:
        assert sql.startswith("CREATE OR REPLACE VIEW")
        assert re.search(r"\.(outfield|goalkeeper)_all`", sql)


# --- CLI ---------------------------------------------------------------------

def test_cli_prints_non_cp1252_names_on_a_cp1252_console(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    dmitrovic = _row(source="apif", source_player_id="900", league_id=140, season=2025,
                     league_code="ESP1", season_label="2025-2026", player_name="M. Dmitrović",
                     team="Espanyol", position_group="GK", minutes=3000, saves=90,
                     goals_conceded=40)
    _make_warehouse(path, ROWS + [dmitrovic])
    env = {**os.environ, "PYTHONIOENCODING": "cp1252"}
    env.pop("PYTHONUTF8", None)
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "load_soccer_data.py"), "--dry-run",
         "--warehouse", str(path)],
        capture_output=True, env=env, cwd=ROOT, timeout=120,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    out = result.stdout.decode("utf-8")
    assert "Marko Dmitrović" in out
    assert "Dry run" in out


def test_cli_dry_run_never_builds_a_bigquery_client(monkeypatch, capsys):
    from google.cloud import bigquery

    def boom(*args, **kwargs):
        raise AssertionError("dry run must not construct a BigQuery client")

    monkeypatch.setattr(bigquery, "Client", boom)
    monkeypatch.setattr(loader, "read_players", lambda path: players())
    monkeypatch.setattr(sys, "argv", ["load_soccer_data.py", "--dry-run"])
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(ROOT / "scripts" / "load_soccer_data.py"), run_name="__main__")
    assert exit_info.value.code in (0, None)
    out = capsys.readouterr().out
    assert "Premier League" in out and "2025-2026" in out
    assert "dry run" in out.lower()
