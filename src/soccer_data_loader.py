"""Load newer seasons from the soccer-data warehouse into BigQuery.

The FBref tables (`outfield_features`, `goalkeeper_features`, 2017-18 to
2023-24) stay untouched. This module reads soccer-data's
`player_season_stats` mart (DuckDB, opened read-only), maps each row onto the
same column contract (schema.sql), and writes `outfield_features_v2` /
`goalkeeper_features_v2` plus two views, `outfield_all` / `goalkeeper_all`,
that union the FBref rows and the new rows with a `Source` column.

Metrics a source does not provide stay null: never zero.

CLI: scripts/load_soccer_data.py [--dry-run]
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery

from src.ui.metrics import KIND, PER90_PAIRS

GCP_PROJECT = os.environ.get("GCP_PROJECT", "scouting-466001")
BQ_DATASET = os.environ.get("BQ_DATASET", "scouting")

# soccer-data league_code -> Hub League string (big five match the FBref strings)
LEAGUES: dict[str, str] = {
    "ENG1": "Premier League",
    "ESP1": "La Liga",
    "GER1": "Bundesliga",
    "ITA1": "Serie A",
    "FRA1": "Ligue 1",
    "ENG2": "Championship",
    "ENG3": "League One",
    "ENG4": "League Two",
    "POR1": "Primeira Liga",
    "NED1": "Eredivisie",
    "BEL1": "Belgian Pro League",
    "AUT1": "Austrian Bundesliga",
    "SUI1": "Swiss Super League",
    "DEN1": "Danish Superliga",
    "SCO1": "Scottish Premiership",
    "MLS": "MLS",
    "USLC": "USL Championship",
    "USL1": "USL League One",
    "MLSNP": "MLS NEXT Pro",
}

# European seasons YYYY-YYYY, US calendar seasons YYYY. None overlap FBref's.
SEASONS = {"2024-2025", "2025-2026", "2024", "2025", "2026"}

MIN_MINUTES = 90

# Plausibility limits per 90 minutes. soccer-data's API-Football catalog has rows
# whose counting stats are full-season but whose minutes are partial. A row with
# at least MIN_MINUTES_FOR_PLAUSIBILITY minutes and above any limit for its
# position is dropped; shorter stints are left alone, since a substitute keeper
# can really concede 6 in 134 minutes.
MIN_MINUTES_FOR_PLAUSIBILITY = 180
MAX_GK_GOALS_AGAINST_PER_90 = 4
MAX_GK_SAVES_PER_90 = 9
MAX_SHOTS_PER_90 = 10
MAX_GOALS_PER_90 = 3
MAX_KEY_PASSES_PER_90 = 8

# FBref table -> the v2 table and the union view built from it. These are the
# only BigQuery objects this module ever writes.
V2_TABLES = {"outfield_features": "outfield_features_v2",
             "goalkeeper_features": "goalkeeper_features_v2"}
VIEWS = {"outfield_features": "outfield_all", "goalkeeper_features": "goalkeeper_all"}

POSITIONS = {"ATT": "FW", "MID": "MF", "DEF": "DF", "GK": "GK"}
KEEPER_SOURCES = {"apif", "apif_matches"}  # ASA has no keeper stats

# soccer-data nationality (country name, API-Football / ASA spelling) -> the
# FIFA-style trigram FBref shows in `Nation` (ENG, SCO, WAL, NIR, KSA, RSA, KVX...).
# Covers every nationality in player_season_stats (a test checks the warehouse).
NATION_CODES: dict[str, str] = {
    "Afghanistan": "AFG", "Albania": "ALB", "Algeria": "ALG", "Andorra": "AND",
    "Angola": "ANG", "Antigua & Barbuda": "ATG", "Antigua and Barbuda": "ATG",
    "Argentina": "ARG", "Armenia": "ARM", "Aruba": "ARU", "Australia": "AUS",
    "Austria": "AUT", "Azerbaijan": "AZE", "Barbados": "BRB", "Belarus": "BLR",
    "Belgium": "BEL", "Belize": "BLZ", "Benin": "BEN", "Bermuda": "BER", "Bolivia": "BOL",
    "Bosnia and Herzegovina": "BIH", "Brazil": "BRA", "British Virgin Islands": "VGB",
    "Bulgaria": "BUL", "Burkina Faso": "BFA", "Burundi": "BDI", "Cameroon": "CMR",
    "Canada": "CAN", "Cape Verde": "CPV", "Cape Verde Islands": "CPV",
    "Republic of Cape Verde": "CPV", "Cayman Islands": "CAY",
    "Central African Republic": "CTA", "Chad": "CHA", "Chile": "CHI", "China": "CHN",
    "China PR": "CHN", "Chinese Taipei": "TPE", "Colombia": "COL", "Comoros": "COM",
    "Congo": "CGO", "Congo DR": "COD", "DR Congo": "COD", "Costa Rica": "CRC",
    "Croatia": "CRO", "Cuba": "CUB", "Curaçao": "CUW", "Cyprus": "CYP",
    "Czech Republic": "CZE", "Czechia": "CZE", "Côte d'Ivoire": "CIV", "Ivory Coast": "CIV",
    "Denmark": "DEN", "Dominica": "DMA", "Dominican Republic": "DOM", "Ecuador": "ECU",
    "Egypt": "EGY", "El Salvador": "SLV", "England": "ENG", "Equatorial Guinea": "EQG",
    "Eritrea": "ERI", "Estonia": "EST", "Ethiopia": "ETH", "Faroe Islands": "FRO",
    "Finland": "FIN", "France": "FRA", "French Guiana": "GUF", "Gabon": "GAB",
    "Gambia": "GAM", "Georgia": "GEO", "Germany": "GER", "Ghana": "GHA", "Greece": "GRE",
    "Grenada": "GRN", "Guadeloupe": "GLP", "Guam": "GUM", "Guatemala": "GUA",
    "Guinea": "GUI", "Guinea-Bissau": "GNB", "Guyana": "GUY", "Haiti": "HAI",
    "Honduras": "HON", "Hong Kong": "HKG", "Hungary": "HUN", "Iceland": "ISL",
    "India": "IND", "Indonesia": "IDN", "Iran": "IRN", "Iraq": "IRQ", "Ireland": "IRL",
    "Republic of Ireland": "IRL", "Israel": "ISR", "Italy": "ITA", "Jamaica": "JAM",
    "Japan": "JPN", "Jordan": "JOR", "Kazakhstan": "KAZ", "Kenya": "KEN",
    "Korea DPR": "PRK", "Korea Republic": "KOR", "South Korea": "KOR", "Kosovo": "KVX", "Lebanon": "LBN",
    "Laos": "LAO", "Latvia": "LVA", "Lesotho": "LES", "Liberia": "LBR", "Libya": "LBY",
    "Liechtenstein": "LIE", "Lithuania": "LTU", "Luxembourg": "LUX", "Macedonia": "MKD",
    "North Macedonia": "MKD", "Madagascar": "MAD", "Malawi": "MWI", "Malaysia": "MAS",
    "Mali": "MLI", "Malta": "MLT", "Martinique": "MTQ", "Mauritania": "MTN",
    "Mauritius": "MRI", "Mexico": "MEX", "Moldova": "MDA", "Montenegro": "MNE",
    "Montserrat": "MSR", "Morocco": "MAR", "Mozambique": "MOZ", "Namibia": "NAM",
    "Netherlands": "NED", "New Caledonia": "NCL", "New Zealand": "NZL", "Nicaragua": "NCA",
    "Niger": "NIG", "Nigeria": "NGA", "Northern Ireland": "NIR", "Norway": "NOR",
    "Pakistan": "PAK", "Palestine": "PLE", "Panama": "PAN", "Paraguay": "PAR",
    "Peru": "PER", "Philippines": "PHI", "Poland": "POL", "Portugal": "POR",
    "Puerto Rico": "PUR", "Qatar": "QAT", "Romania": "ROU", "Russia": "RUS",
    "Rwanda": "RWA", "Saudi Arabia": "KSA", "Scotland": "SCO", "Senegal": "SEN",
    "Serbia": "SRB", "Seychelles": "SEY", "Sierra Leone": "SLE", "Slovakia": "SVK",
    "Slovenia": "SVN", "Somalia": "SOM", "Sri Lanka": "SRI", "South Africa": "RSA", "South Sudan": "SSD",
    "Spain": "ESP", "St. Kitts and Nevis": "SKN", "St. Lucia": "LCA",
    "St. Vincent and the Grenadines": "VIN", "Suriname": "SUR", "Sweden": "SWE",
    "Switzerland": "SUI", "Syria": "SYR", "São Tomé e Príncipe": "STP", "Tanzania": "TAN",
    "Thailand": "THA", "Timor-Leste": "TLS", "Togo": "TOG", "Trinidad & Tobago": "TRI",
    "Trinidad and Tobago": "TRI", "Tunisia": "TUN", "Turkey": "TUR", "Türkiye": "TUR",
    "Turks and Caicos Islands": "TCA", "US Virgin Islands": "VIR", "USA": "USA",
    "Uganda": "UGA", "Ukraine": "UKR", "Uruguay": "URU", "Uzbekistan": "UZB",
    "Venezuela": "VEN", "Vietnam": "VIE", "Wales": "WAL", "Yemen": "YEM", "Zambia": "ZAM",
    "Zimbabwe": "ZIM",
}
# Names kept as they are: no FIFA trigram, and none in the FBref data to copy.
NATION_FALLBACK = frozenset({"Bonaire", "Great Britain"})

IDENTITY_COLUMNS = frozenset(
    {"Player", "Nation", "Position", "Squad", "League", "Age", "Born", "Season", "position_group"}
)

# Column order of schema.sql's feature tables (tests/test_soccer_data_loader.py
# keeps these in sync with the file).
OUTFIELD_COLUMNS = [
    "Player", "Nation", "Position", "Squad", "League", "Age", "Born", "Season",
    "position_group", "minutes", "nineties_played", "std_goals", "std_assists",
    "nonpen_goals", "penalty_kicks_made", "yellow_cards", "red_cards", "std_xg", "npxg",
    "xag", "progressive_carries", "std_progressive_passes", "progressive_passes_received",
    "goals_per_90", "assists_per_90", "nonpen_goals_per_90", "xg_per_90", "xag_per_90",
    "npxg_per_90", "npxg_plus_xag_per_90", "shots_total", "shots_on_target",
    "shots_on_target_pct", "shots_total_per_90", "shots_on_target_per_90", "goals_per_shot",
    "goals_per_shot_on_target", "avg_shot_distance", "npxg_per_shot", "goals_minus_xg",
    "nonpen_goals_minus_npxg", "passes_completed_total", "passes_attempted_total",
    "passes_completion_pct_total", "total_passing_distance", "progressive_passing_distance",
    "passes_completion_pct_short", "passes_completion_pct_medium",
    "passes_completion_pct_long", "expected_assisted_goals", "expected_assists",
    "key_passes", "passes_into_final_third", "passes_into_penalty_area",
    "crosses_into_penalty_area", "pass_progressive_passes", "tackles_total", "tackles_won",
    "tackles_def_3rd", "tackles_mid_3rd", "tackles_att_3rd", "dribblers_tackled",
    "dribblers_tackled_pct", "blocks_total", "interceptions", "tackles_plus_interceptions",
    "clearances", "errors_leading_to_shot",
    # newer sources only (null for FBref)
    "appearances", "starts", "match_rating", "penalties_missed", "xg_chain", "xg_buildup",
    "xpass_completion_pct", "passes_completed_over_expected",
    "passes_completed_over_expected_p100", "avg_pass_distance", "avg_vertical_pass_distance",
    "dribbles_attempted", "dribbles_completed", "dribble_success_pct", "fouls_drawn",
    "penalties_won", "share_team_touches_pct", "dribbled_past", "duels_total", "duels_won",
    "duels_won_pct", "goals_added", "goals_added_above_avg", "ga_shooting", "ga_passing",
    "ga_receiving", "ga_dribbling", "ga_interrupting", "ga_fouling", "fouls_committed",
    "penalties_conceded", "offsides", "photo_url",
]
GOALKEEPER_COLUMNS = [
    "Player", "Nation", "Position", "Squad", "League", "Age", "Born", "Season",
    "position_group", "minutes", "nineties_played", "goals_against", "goals_against_per_90",
    "shots_on_target_against", "saves", "save_pct", "wins", "draws", "losses",
    "clean_sheets", "clean_sheet_pct", "penalty_kicks_faced", "penalty_kicks_allowed",
    "penalty_kicks_saved", "pk_save_pct",
    # newer sources only (null for FBref)
    "appearances", "starts", "match_rating", "photo_url",
]

_MART_NUMBERS = ("minutes", "goals", "assists", "penalties_scored", "shots", "shots_on_target",
                 "key_passes", "passes", "pass_completion_pct", "tackles", "interceptions",
                 "blocks", "yellow_cards", "red_cards", "saves", "goals_conceded",
                 "penalties_saved", "xg", "npxg", "xa",
                 # keeper results from match detail (apif / apif_matches only)
                 "clean_sheets", "gk_starts", "wins", "draws", "losses",
                 # API-Football
                 "appearances", "starts", "rating", "duels", "duels_won",
                 "dribbles_attempted", "dribbles_won", "dribbled_past", "fouls_drawn",
                 "fouls_committed", "penalties_won", "penalties_conceded", "penalties_missed",
                 "offsides",
                 # Understat (big five)
                 "xg_chain", "xg_buildup",
                 # American Soccer Analysis
                 "goals_added_raw", "goals_added_above_avg", "ga_dribbling", "ga_passing",
                 "ga_receiving", "ga_shooting", "ga_interrupting", "ga_fouling",
                 "passes_completed_over_expected", "passes_completed_over_expected_p100",
                 "xpass_completion_pct", "share_team_touches", "avg_pass_distance_yds",
                 "avg_vertical_distance_yds")

_SQL_TYPES = {"INTEGER": "INT64", "INT64": "INT64", "FLOAT": "FLOAT64", "FLOAT64": "FLOAT64",
              "BOOLEAN": "BOOL", "BOOL": "BOOL", "STRING": "STRING"}
_IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")


# --- warehouse -----------------------------------------------------------------

def warehouse_path() -> Path:
    """SOCCER_DATA_WAREHOUSE, else $SOCCER_DATA_ROOT/warehouse.duckdb, else ~/soccer-data."""
    explicit = os.environ.get("SOCCER_DATA_WAREHOUSE")
    if explicit:
        return Path(explicit)
    root = os.environ.get("SOCCER_DATA_ROOT")
    if root:
        return Path(root) / "warehouse.duckdb"
    return Path.home() / "soccer-data" / "warehouse.duckdb"


# ASA players have no photo. One is borrowed from API-Football when exactly one
# API-Football player shares the birth date and the full name or the surname
# (accents and case folded). About three US players in four get one.
ASA_PHOTO_SQL = (
    "SELECT a.source_player_id, max(pr.photo_url) AS photo_url"
    " FROM (SELECT DISTINCT source_player_id, lower(strip_accents(player_name)) AS nm,"
    "        CAST(birth_date AS DATE) AS bd"
    "       FROM player_season_stats WHERE source = 'asa' AND birth_date IS NOT NULL) a"
    " JOIN (SELECT player_id, photo_url, CAST(birth_date AS DATE) AS bd,"
    "        lower(strip_accents(player_name)) AS nm,"
    "        lower(strip_accents(coalesce(first_name, '') || ' ' || coalesce(last_name, ''))) AS full_nm"
    "       FROM stg_apif__players WHERE photo_url IS NOT NULL AND birth_date IS NOT NULL) pr"
    "   ON pr.bd = a.bd AND (a.nm IN (pr.nm, pr.full_nm)"
    "        OR split_part(a.nm, ' ', -1) = split_part(pr.full_nm, ' ', -1))"
    " GROUP BY a.source_player_id"
    " HAVING count(DISTINCT pr.player_id) = 1"
)


def read_players(path: Path | str) -> pd.DataFrame:
    """Rows of player_season_stats in LEAGUES and SEASONS with at least 90 minutes.

    API-Football rows also get `understat_name` (the Understat player name when
    soccer-data links the player for that league-season) and `profile_first_name`
    (the API-Football profile's first name), used for display names."""
    import duckdb  # dev-only dependency: the app never imports this module

    leagues, seasons = sorted(LEAGUES), sorted(SEASONS)
    where = (
        f" WHERE p.league_code IN ({', '.join('?' * len(leagues))})"
        f" AND p.season_label IN ({', '.join('?' * len(seasons))})"
        " AND p.minutes >= ?"
    )
    params = [*leagues, *seasons, MIN_MINUTES]
    apif = "p.source IN ('apif', 'apif_matches')"
    sql = (
        "SELECT p.*, us.player_name AS understat_name, pr.first_name AS profile_first_name,"
        " pr.nationality AS profile_nationality,"
        " COALESCE(pr.photo_url, ap.photo_url) AS photo_url"
        " FROM player_season_stats p"
        " LEFT JOIN league_info li ON li.apif_league_id = p.league_id"
        f" LEFT JOIN int_understat__apif_match m ON {apif}"
        "   AND m.apif_player_id = TRY_CAST(p.source_player_id AS BIGINT)"
        "   AND m.league_id = p.league_id AND m.season = p.season"
        " LEFT JOIN stg_understat__player_seasons us"
        "   ON us.understat_player_id = m.understat_player_id"
        "   AND us.season = m.season AND us.league = li.understat_league"
        f" LEFT JOIN stg_apif__players pr ON {apif}"
        "   AND pr.player_id = TRY_CAST(p.source_player_id AS BIGINT)"
        f" LEFT JOIN ({ASA_PHOTO_SQL}) ap ON p.source = 'asa'"
        "   AND ap.source_player_id = p.source_player_id"
        + where +
        " ORDER BY p.league_code, p.season_label, p.player_name, p.team"
    )
    con = duckdb.connect(str(path), read_only=True)
    try:
        players = con.execute(sql, params).df()
        expected = con.execute("SELECT count(*) FROM player_season_stats p" + where,
                               params).fetchone()[0]
    finally:
        con.close()
    if len(players) != expected:
        raise ValueError(f"name lookups duplicated rows: {len(players)} != {expected}")
    return players


# --- mapping -------------------------------------------------------------------

def _div(num: pd.Series, den: pd.Series) -> pd.Series:
    """num / den, null when either is null or den is zero."""
    return num / den.where(den != 0)


def _age_at_season_start(born: pd.Series, season_label: pd.Series) -> pd.Series:
    """Whole years on 1 August (European YYYY-YYYY) or 1 March (US YYYY)."""
    born = pd.to_datetime(born)
    year = season_label.str[:4].astype(int)
    month = np.where(season_label.str.contains("-"), 8, 3)
    before_birthday = (born.dt.month > month) | ((born.dt.month == month) & (born.dt.day > 1))
    age = year - born.dt.year - before_birthday.astype(int)
    return age.where(born.notna()).astype(float)


def _source_label(source: pd.Series, xg: pd.Series) -> pd.Series:
    unknown = set(source) - {"asa", "apif", "apif_matches"}
    if unknown:
        raise ValueError(f"unknown source(s) in player_season_stats: {sorted(unknown)}")
    label = np.select(
        [source == "asa", (source == "apif") & xg.notna(), source == "apif"],
        ["American Soccer Analysis", "API-Football + Understat", "API-Football"],
        default="API-Football match data",
    )
    return pd.Series(label, index=source.index, dtype=object)


def _identity(p: pd.DataFrame) -> dict[str, pd.Series]:
    def text(col: str) -> pd.Series:
        return p[col].astype(object).where(p[col].notna(), None)

    minutes = p["minutes"]
    return {
        "Player": text("player_name"),
        "Nation": text("nationality"),
        "Position": p["position_group"].map(POSITIONS),
        "Squad": text("team"),
        "League": p["league_code"].map(LEAGUES),
        "Age": _age_at_season_start(p["birth_date"], p["season_label"]),
        "Born": pd.to_datetime(p["birth_date"]).dt.year.astype(float),
        "Season": p["season_label"].astype(object),
        "position_group": p["position_group"].astype(object),
        "minutes": minutes,
        "nineties_played": minutes / 90,
        "photo_url": text("photo_url") if "photo_url" in p else pd.Series(None, index=p.index, dtype=object),
    }


def _frame(values: dict[str, pd.Series], columns: list[str], index, source: pd.Series):
    data = {col: values[col] if col in values else pd.Series(np.nan, index=index)
            for col in columns}
    data["Source"] = source
    return pd.DataFrame(data, index=index).reset_index(drop=True)


def _playing_time(p: pd.DataFrame) -> dict[str, pd.Series]:
    """Appearances, starts and the average API-Football match rating (2 decimals)."""
    return {"appearances": p["appearances"], "starts": p["starts"],
            "match_rating": p["rating"].round(2)}


def _newer_outfield(p: pd.DataFrame) -> dict[str, pd.Series]:
    """Metrics only API-Football, Understat or ASA provide (null for the others)."""
    return {
        **_playing_time(p),
        "duels_total": p["duels"],
        "duels_won": p["duels_won"],
        "duels_won_pct": 100 * _div(p["duels_won"], p["duels"]),
        "dribbles_attempted": p["dribbles_attempted"],
        "dribbles_completed": p["dribbles_won"],
        "dribble_success_pct": 100 * _div(p["dribbles_won"], p["dribbles_attempted"]),
        "dribbled_past": p["dribbled_past"],
        "fouls_drawn": p["fouls_drawn"],
        "fouls_committed": p["fouls_committed"],
        "penalties_won": p["penalties_won"],
        "penalties_conceded": p["penalties_conceded"],
        "penalties_missed": p["penalties_missed"],
        "offsides": p["offsides"],
        "xg_chain": p["xg_chain"],
        "xg_buildup": p["xg_buildup"],
        "goals_added": p["goals_added_raw"],
        "goals_added_above_avg": p["goals_added_above_avg"],
        **{f"ga_{action}": p[f"ga_{action}"] for action in
           ("dribbling", "passing", "receiving", "shooting", "interrupting", "fouling")},
        "passes_completed_over_expected": p["passes_completed_over_expected"],
        "passes_completed_over_expected_p100": p["passes_completed_over_expected_p100"],
        "xpass_completion_pct": p["xpass_completion_pct"],
        "share_team_touches_pct": 100 * p["share_team_touches"],
        "avg_pass_distance": p["avg_pass_distance_yds"],
        "avg_vertical_pass_distance": p["avg_vertical_distance_yds"],
    }


def _outfield(p: pd.DataFrame) -> pd.DataFrame:
    n = {col: p[col] for col in _MART_NUMBERS}
    v = _identity(p)
    v.update(_newer_outfield(p))
    nineties = v["nineties_played"]
    nonpen = n["goals"] - n["penalties_scored"]
    v.update({
        "std_goals": n["goals"],
        "std_assists": n["assists"],
        "penalty_kicks_made": n["penalties_scored"],
        "nonpen_goals": nonpen,
        "yellow_cards": n["yellow_cards"],
        "red_cards": n["red_cards"],
        "std_xg": n["xg"],
        "npxg": n["npxg"],
        "xag": n["xa"],
        "expected_assisted_goals": n["xa"],
        "shots_total": n["shots"],
        "shots_on_target": n["shots_on_target"],
        "shots_on_target_pct": 100 * _div(n["shots_on_target"], n["shots"]),
        "goals_per_shot": _div(n["goals"], n["shots"]),
        "goals_per_shot_on_target": _div(n["goals"], n["shots_on_target"]),
        "npxg_per_shot": _div(n["npxg"], n["shots"]),
        "goals_minus_xg": n["goals"] - n["xg"],
        "nonpen_goals_minus_npxg": nonpen - n["npxg"],
        "key_passes": n["key_passes"],
        "passes_attempted_total": n["passes"],
        "passes_completion_pct_total": n["pass_completion_pct"],
        "tackles_total": n["tackles"],
        "interceptions": n["interceptions"],
        "tackles_plus_interceptions": n["tackles"] + n["interceptions"],
        "blocks_total": n["blocks"],
    })
    for total, per90 in PER90_PAIRS.items():
        if per90 in OUTFIELD_COLUMNS:
            v[per90] = _div(v[total], nineties)
    v["npxg_plus_xag_per_90"] = _div(n["npxg"] + n["xa"], nineties)
    return _frame(v, OUTFIELD_COLUMNS, p.index, _source_label(p["source"], p["xg"]))


def _goalkeeper(p: pd.DataFrame) -> pd.DataFrame:
    v = _identity(p)
    v.update(_playing_time(p))
    saves, conceded = p["saves"], p["goals_conceded"]
    faced = saves + conceded
    v.update({
        "goals_against": conceded,
        "goals_against_per_90": _div(conceded, v["nineties_played"]),
        "saves": saves,
        "shots_on_target_against": faced,
        "save_pct": 100 * _div(saves, faced),
        "penalty_kicks_saved": p["penalties_saved"],
        "clean_sheets": p["clean_sheets"],
        "clean_sheet_pct": 100 * _div(p["clean_sheets"], p["gk_starts"]),
        "wins": p["wins"],
        "draws": p["draws"],
        "losses": p["losses"],
    })
    return _frame(v, GOALKEEPER_COLUMNS, p.index, _source_label(p["source"], p["xg"]))


_INITIAL_NAME = re.compile(r"^([^\W\d_])\.\s+(\S.*)$")


def _fold(text: str) -> str:
    """Accent- and case-insensitive form."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold()


def _clean(value) -> str | None:
    """HTML entities decoded ("O&apos;Shea" -> "O'Shea"), whitespace runs (including
    non-breaking spaces) collapsed and trimmed; None when empty or null."""
    if value is None or pd.isna(value):
        return None
    return " ".join(html.unescape(str(value)).split()) or None


def _last_word(text: str) -> str:
    return _fold(text.split()[-1])


def _display_name(source: str, name: str, understat, first_name) -> tuple[str, str | None]:
    """(name, rule) for one row. Only abbreviated API-Football names ("M. Salah")
    change; full names and ASA names never do. With P the profile expansion
    (first-name first token + surname, when that token starts with the initial)
    and U the linked Understat name:
      U equals P ignoring accents and case -> P (keeps accents: "Marko Dmitrović")
      U ends with the surname's last word  -> U ("Phil Foden")
      otherwise                            -> P ("Kylian Mbappé"), else U, else unchanged.
    `name`, `understat` and `first_name` must already be cleaned."""
    if source not in ("apif", "apif_matches"):
        return name, None
    match = _INITIAL_NAME.match(name or "")
    if not match:
        return name, None
    initial, surname = match.groups()
    expanded = None
    if first_name:
        token = first_name.split()[0]
        if _fold(token).startswith(_fold(initial)):
            expanded = f"{token} {surname}"
    if expanded and understat and _fold(understat) == _fold(expanded):
        return expanded, "initial"
    if understat and _last_word(understat) == _last_word(surname):
        return understat, "understat"
    if expanded:
        return expanded, "initial"
    if understat:
        return understat, "understat"
    return name, None


def _clean_api_football_names(p: pd.DataFrame, report: dict) -> None:
    """Clean every API-Football player_name and the link columns in place, before
    any matching or output. ASA names are left exactly as they are."""
    apif = p["source"].isin(("apif", "apif_matches"))
    cleaned = p["player_name"].where(~apif, p["player_name"].map(_clean))
    report["names_cleaned"] = int((apif & (cleaned != p["player_name"])).sum())
    p["player_name"] = cleaned
    for col in ("understat_name", "profile_first_name"):
        if col in p:
            p[col] = p[col].map(_clean)


def _apply_nation_codes(p: pd.DataFrame, report: dict) -> None:
    """Country names -> FBref trigrams in place. Fallback and unknown names keep the
    name and are counted in the report; nulls stay null."""
    names = p["nationality"]
    present = names.notna()
    fallback = present & names.isin(NATION_FALLBACK)
    unmapped = present & ~names.isin(NATION_CODES) & ~fallback
    report["nations_fallback"] = names[fallback].value_counts().to_dict()
    report["nations_unmapped"] = names[unmapped].value_counts().to_dict()
    p["nationality"] = names.map(NATION_CODES).where(names.isin(NATION_CODES), names)


def _apply_display_names(p: pd.DataFrame, report: dict) -> None:
    def text(col: str) -> list[str | None]:
        # older callers lack the link columns; nulls may arrive as NaN
        values = p[col] if col in p else [None] * len(p)
        return [v if isinstance(v, str) else None for v in values]

    understat, first = text("understat_name"), text("profile_first_name")
    results = [_display_name(s, n, u, f) for s, n, u, f
               in zip(p["source"], p["player_name"], understat, first)]
    new = pd.Series([r[0] for r in results], index=p.index, dtype=object)
    rule = pd.Series([r[1] for r in results], index=p.index, dtype=object)
    changed = new != p["player_name"]
    report["names_understat"] = int((changed & (rule == "understat")).sum())
    report["names_initial"] = int((changed & (rule == "initial")).sum())
    report["name_examples"] = [
        f"{old} -> {name} ({how}, {league} {season})"
        for old, name, how, league, season in zip(
            p.loc[changed, "player_name"], new[changed], rule[changed],
            p.loc[changed, "league_code"], p.loc[changed, "season_label"])
    ]
    p["player_name"] = new


def _implausible(p: pd.DataFrame) -> pd.Series:
    """Rows with MIN_MINUTES_FOR_PLAUSIBILITY+ minutes whose per-90 rates are
    impossible for their position (a null stat never fails)."""
    nineties = p["minutes"] / 90

    def over(col: str, limit: float) -> pd.Series:
        return (_div(p[col], nineties) > limit).fillna(False)

    keeper = p["position_group"] == "GK"
    keeper_bad = over("goals_conceded", MAX_GK_GOALS_AGAINST_PER_90) | over(
        "saves", MAX_GK_SAVES_PER_90)
    outfield_bad = (over("shots", MAX_SHOTS_PER_90) | over("goals", MAX_GOALS_PER_90)
                    | over("key_passes", MAX_KEY_PASSES_PER_90))
    checked = p["minutes"] >= MIN_MINUTES_FOR_PLAUSIBILITY
    return checked & ((keeper & keeper_bad) | (~keeper & outfield_bad))


def _make_unique(outfield: pd.DataFrame, goalkeeper: pd.DataFrame, report: dict) -> None:
    """Rename (Player, Season) repeats across both frames, in place, one level at a
    time until unique: "Name (Squad)", "Name (Squad, League)", then (different
    players sharing an abbreviated name at one club) "Name (Squad, League, born
    YYYY)", then "Name (Squad, League, #N)" numbered by minutes, most first."""
    cols = ["Player", "Season", "Squad", "League", "Born", "minutes"]
    both = pd.concat([outfield[cols], goalkeeper[cols]], ignore_index=True)
    base, season = both["Player"], both["Season"]
    squad = both["Squad"].fillna("unknown club")
    stem = base + " (" + squad + ", " + both["League"].fillna("unknown league")

    def clashes(names: pd.Series) -> pd.Series:
        return pd.DataFrame({"P": names, "S": season}).duplicated(keep=False)

    names = base.copy()
    clash = clashes(names)
    names[clash] = base + " (" + squad + ")"
    to_league = clash & clashes(names)
    names[to_league] = stem + ")"
    to_born = to_league & clashes(names) & both["Born"].notna()
    names[to_born] = stem + ", born " + both["Born"].astype("Int64").astype(str) + ")"
    numbered = to_league & clashes(names)
    if numbered.any():
        group = pd.DataFrame({"P": names, "S": season, "m": both["minutes"]})[numbered]
        rank = group.groupby(["P", "S"])["m"].rank(method="first", ascending=False)
        names[numbered] = stem[numbered] + ", #" + rank.astype(int).astype(str) + ")"

    final = pd.DataFrame({"Player": names, "Season": season})
    repeats = final[final.duplicated(keep=False)]
    if not repeats.empty:
        raise ValueError("(Player, Season) still repeats after renaming:\n"
                         + repeats.head(10).to_string())

    renamed = both[clash].assign(New=names[clash])
    report["renamed"] = int(clash.sum())
    report["renamed_to_league"] = int(to_league.sum())
    report["renamed_to_birth_year"] = int((to_born & ~numbered).sum())
    report["renamed_numbered"] = int(numbered.sum())
    report["renamed_examples"] = [f"{r.Player} -> {r.New} ({r.Season})"
                                  for r in renamed.itertuples()]
    outfield["Player"] = names.iloc[:len(outfield)].to_numpy()
    goalkeeper["Player"] = names.iloc[len(outfield):].to_numpy()


def build_frames(players: pd.DataFrame,
                 report: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map player_season_stats rows onto (outfield, goalkeeper) Hub frames.

    `report`, when given, receives the dropped-row and rename counts."""
    report = {} if report is None else report
    p = players.copy()
    for col in _MART_NUMBERS:
        # a mart built before a column existed gives nulls, not a crash
        values = p[col] if col in p else np.nan
        p[col] = pd.to_numeric(pd.Series(values, index=p.index), errors="coerce").astype(float)
    p["season_label"] = p["season_label"].astype(str)
    _clean_api_football_names(p, report)
    if "profile_nationality" in p:
        # match-data rows (League One/Two) carry no nationality: take the profile's
        report["nations_from_profile"] = int((p["nationality"].isna() & p["profile_nationality"].notna()).sum())
        p["nationality"] = p["nationality"].where(p["nationality"].notna(), p["profile_nationality"])
    _apply_nation_codes(p, report)

    null_position = p["position_group"].isna()
    unknown = set(p.loc[~null_position, "position_group"]) - set(POSITIONS)
    if unknown:
        raise ValueError(f"unknown position_group(s): {sorted(unknown)}")
    is_gk = p["position_group"] == "GK"
    keeper_ok = p["source"].isin(KEEPER_SOURCES)
    report["dropped_null_position"] = int(null_position.sum())
    report["dropped_asa_keepers"] = int((is_gk & ~keeper_ok).sum())

    kept = ~null_position & ~(is_gk & ~keeper_ok)
    bad = kept & _implausible(p)
    dropped = p[bad]
    report["dropped_implausible"] = int(bad.sum())
    report["implausible_by_league_season"] = {
        (LEAGUES.get(league, league), season): int(n)
        for (league, season), n in dropped.groupby(["league_code", "season_label"]).size().items()
    }
    report["implausible_examples"] = [
        f"{r.player_name} ({r.team}, {LEAGUES.get(r.league_code, r.league_code)} "
        f"{r.season_label}): {r.minutes:.0f} min, {r.goals_conceded:.0f} GA, {r.saves:.0f} saves, "
        f"{r.shots:.0f} shots, {r.goals:.0f} goals, {r.key_passes:.0f} key passes"
        for r in dropped.sort_values("minutes").head(5).itertuples()
    ]

    p = p[kept & ~bad].copy()
    _apply_display_names(p, report)
    is_gk = p["position_group"] == "GK"
    outfield = _outfield(p[~is_gk])
    goalkeeper = _goalkeeper(p[is_gk])
    _make_unique(outfield, goalkeeper, report)
    return outfield, goalkeeper


# --- BigQuery --------------------------------------------------------------------

def _sql_type(field_type: str) -> str:
    return _SQL_TYPES.get(field_type.upper(), field_type.upper())


# Non-metric columns the FBref tables lack, with their BigQuery types.
TEXT_COLUMNS = {"photo_url": "STRING"}


def bq_type(column: str) -> str:
    """BigQuery type of a Hub column the FBref tables lack: TEXT_COLUMNS as
    listed; metrics INT64 for whole-number kinds (count, tally, minutes),
    FLOAT64 otherwise."""
    if column in TEXT_COLUMNS:
        return TEXT_COLUMNS[column]
    kind = KIND.get(column)
    if kind is None:
        raise ValueError(f"{column} is not a metric column: the FBref table must have it")
    return "INT64" if kind in ("count", "tally", "minutes") else "FLOAT64"


def view_sql(table: str, fbref_columns: list[tuple[str, str]],
             new_columns: list[tuple[str, str]] = (),
             project: str = GCP_PROJECT, dataset: str = BQ_DATASET) -> str:
    """CREATE OR REPLACE VIEW <table>'s union view: FBref rows plus v2 rows cast
    to the FBref column types, each with a Source. `new_columns` (name, type) are
    the v2 columns the FBref table lacks: NULL on the FBref side."""
    view, v2 = VIEWS[table], V2_TABLES[table]
    every = [*fbref_columns, *new_columns]
    bad = [name for name, _ in every if not _IDENTIFIER.match(name)]
    if bad:
        raise ValueError(f"column names need quoting: {bad}")
    old = ",\n    ".join([name for name, _ in fbref_columns]
                         + [f"CAST(NULL AS {_sql_type(kind)}) AS {name}"
                            for name, kind in new_columns])
    new = ",\n    ".join(f"CAST({name} AS {_sql_type(kind)}) AS {name}"
                         for name, kind in every)
    return (
        f"CREATE OR REPLACE VIEW `{project}.{dataset}.{view}` AS\n"
        f"SELECT\n    {old},\n    'FBref' AS Source\n"
        f"FROM `{project}.{dataset}.{table}`\n"
        "UNION ALL\n"
        f"SELECT\n    {new},\n    Source\n"
        f"FROM `{project}.{dataset}.{v2}`\n"
    )


def _coerce(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> pd.DataFrame:
    """Order and type `frame` to a BigQuery schema (nullable ints stay nullable)."""
    names = [name for name, _ in columns]
    missing = [n for n in names if n not in frame.columns]
    extra = [c for c in frame.columns if c not in names]
    if missing or extra:
        raise ValueError(f"frame does not match the BigQuery schema: missing {missing}, "
                         f"extra {extra}")
    out = pd.DataFrame(index=frame.index)
    for name, kind in columns:
        col = frame[name]
        kind = _sql_type(kind)
        if kind == "INT64":
            values = pd.to_numeric(col, errors="coerce").astype(float)
            fractional = values.notna() & (values != values.round())
            if fractional.any():
                raise ValueError(f"{name} is INT64 in BigQuery but has fractional values")
            out[name] = values.astype("Int64")
        elif kind == "FLOAT64":
            out[name] = pd.to_numeric(col, errors="coerce").astype(float)
        elif kind == "STRING":
            out[name] = col.astype(object).where(col.notna(), None)
        else:
            out[name] = col
    return out.reset_index(drop=True)


def load(outfield: pd.DataFrame, goalkeeper: pd.DataFrame, client,
         project: str = GCP_PROJECT, dataset: str = BQ_DATASET) -> dict[str, int]:
    """WRITE_TRUNCATE both v2 tables, then create both union views. The v2 schema
    is the FBref table's live columns and types, then the Hub columns the FBref
    table lacks (newer sources only, typed by bq_type), then Source STRING.
    Returns rows written per v2 table."""
    counts: dict[str, int] = {}
    live_columns: dict[str, tuple[list[tuple[str, str]], list[tuple[str, str]]]] = {}
    hub = {"outfield_features": OUTFIELD_COLUMNS, "goalkeeper_features": GOALKEEPER_COLUMNS}
    for table, frame in (("outfield_features", outfield), ("goalkeeper_features", goalkeeper)):
        live = [(f.name, f.field_type)
                for f in client.get_table(f"{project}.{dataset}.{table}").schema]
        names = {name for name, _ in live}
        new = [(col, bq_type(col)) for col in hub[table] if col not in names]
        live_columns[table] = (live, new)
        columns = live + new + [("Source", "STRING")]
        config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(name, kind) for name, kind in columns],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        data = _coerce(frame, columns)
        target = f"{project}.{dataset}.{V2_TABLES[table]}"
        client.load_table_from_dataframe(data, target, job_config=config).result()
        counts[V2_TABLES[table]] = len(data)
    for table, (live, new) in live_columns.items():
        client.query(view_sql(table, live, new, project, dataset)).result()
    return counts


# --- CLI ---------------------------------------------------------------------------

def _counts(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "  (no rows)"
    table = pd.crosstab(frame["League"], frame["Season"], margins=True, margins_name="Total")
    return table.to_string()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load soccer-data's player_season_stats into the Scouting Hub's "
                    "BigQuery v2 tables and union views.")
    parser.add_argument("--dry-run", action="store_true",
                        help="read and map the warehouse, print counts, write nothing")
    parser.add_argument("--warehouse", type=Path, default=None,
                        help="warehouse.duckdb path (default: SOCCER_DATA_WAREHOUSE, "
                             "SOCCER_DATA_ROOT, ~/soccer-data)")
    args = parser.parse_args(argv)
    # Player names are not all cp1252 (Windows consoles): never crash printing them.
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(encoding="utf-8", errors="replace")

    path = args.warehouse or warehouse_path()
    print(f"Warehouse: {path}")
    players = read_players(path)
    report: dict = {}
    outfield, goalkeeper = build_frames(players, report=report)

    print(f"\nRead {len(players)} rows (>= {MIN_MINUTES} minutes).")
    print(f"Dropped: {report['dropped_null_position']} with no position, "
          f"{report['dropped_asa_keepers']} ASA goalkeepers, "
          f"{report['dropped_implausible']} with implausible per-90 rates.")
    for (league, season), n in sorted(report["implausible_by_league_season"].items()):
        print(f"  implausible: {league} {season}: {n}")
    for example in report["implausible_examples"]:
        print(f"  {example}")
    print(f"Nations kept as names: fallback {report['nations_fallback']}, "
          f"unmapped {report['nations_unmapped']}.")
    print(f"Display names: {report['names_cleaned']} API-Football names cleaned "
          f"(entities, spaces); {report['names_initial']} expanded from the profile first "
          f"name, {report['names_understat']} taken from Understat.")
    for rule in ("(initial,", "(understat,"):
        for example in [e for e in report["name_examples"] if rule in e][:5]:
            print(f"  {example}")
    print(f"Renamed {report['renamed']} colliding rows "
          f"({report['renamed_to_league']} needed the league, "
          f"{report['renamed_to_birth_year']} the birth year, "
          f"{report['renamed_numbered']} a number).")
    for example in report["renamed_examples"][:5]:
        print(f"  {example}")
    deep = [e for e in report["renamed_examples"] if ", born " in e or ", #" in e]
    for example in deep:
        print(f"  {example}")
    print(f"\nOutfield rows: {len(outfield)}")
    print(_counts(outfield))
    print(f"\nGoalkeeper rows: {len(goalkeeper)}")
    print(_counts(goalkeeper))
    print("\nSource:")
    print(pd.concat([outfield["Source"], goalkeeper["Source"]]).value_counts().to_string())

    if args.dry_run:
        print("\nDry run: nothing written to BigQuery.")
        return 0

    client = bigquery.Client(project=GCP_PROJECT)
    counts = load(outfield, goalkeeper, client)
    for table, rows in counts.items():
        print(f"Loaded {GCP_PROJECT}.{BQ_DATASET}.{table}: {rows} rows")
    print(f"Views: {', '.join(VIEWS.values())}")
    return 0
