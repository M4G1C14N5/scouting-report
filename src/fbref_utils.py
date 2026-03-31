import pandas as pd
import re
from io import StringIO
from pathlib import Path
from difflib import get_close_matches

def load_fbref_csv(path, header_identifier="Rk", raw=False):
    """
    Load an FBref CSV and clean repeated header rows.

    Workflow:
    - If raw=True: return the DataFrame exactly as read (for debugging/inspection).
    - If raw=False: 
        * Find the first row containing header_identifier (e.g., "Rk").
        * Set that row as the header.
        * Drop all repeated header rows inside the dataset.
        * Reset the index so it starts at 0,1,2...
        * Clear or rename the index name (so it never shows up as '26').

    Parameters
    ----------
    path : str
        Path to the CSV file.
    header_identifier : str, optional
        Value in the first column that marks the true header row (default="Rk").
    raw : bool, optional
        If True, returns the unmodified raw DataFrame. Default=False.

    Returns
    -------
    pd.DataFrame
        Cleaned (or raw) DataFrame.
    """
    # Always read with no header (since FBref headers are messy)
    df_raw = pd.read_csv(path, header=None)

    if raw:
        return df_raw

    # Find the header row index
    header_row_idx = df_raw[df_raw.iloc[:, 0] == header_identifier].index
    if header_row_idx.empty:
        raise ValueError(f"No header row found with first column == '{header_identifier}' in {path}")
    header_row_idx = header_row_idx[0]

    # Extract new header
    new_header = df_raw.iloc[header_row_idx]

    # Data starts after header row
    df = df_raw.iloc[header_row_idx+1:].copy()
    df.columns = new_header

    # Drop repeated headers inside the data
    df = df[df.iloc[:, 0] != header_identifier]

    # --- New Step: Reset index & clean index name ---
    df = df.reset_index(drop=True)
    df.index.name = None   # or set to "index" if you prefer

    return df


def common_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply common cleaning steps across all FBref stat datasets:
    1. Drop the index artifact column (first col, often labeled '26').
    2. Drop the 'Rk' column if present (FBref row counter).
    3. Clean Nation column: keep only the capitalized 3-letter code (e.g., 'es ESP' -> 'ESP').
    4. Rename 'Pos' -> 'Position' if present.
    5. Rename 'Comp' -> 'League', keeping only the league name (e.g., 'es La Liga' -> 'La Liga').
    6. Rename season column (e.g., '2017-2018') to 'Season'.
    7. Convert Age/Born to numeric.
    8. Cast Nation, League, Squad, Position to category dtype.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe after loading with proper headers.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe with standardized columns and dtypes.
    """
    # 1. Drop first column (index artifact)
    df = df.drop(df.columns[0], axis=1)

    # 2. Drop 'Rk' if present
    if "Rk" in df.columns:
        df = df.drop(columns=["Rk"])

    #added afterwards which is why order is thrown off. Let's call it 2.5
    if "Matches" in df.columns:
        df = df.drop(columns=["Matches"])

    # 3. Clean Nation column
    if "Nation" in df.columns:
        df["Nation"] = df["Nation"].astype(str).str.extract(r"([A-Z]{3})")

    # 4. Rename 'Pos' -> 'Position'
    if "Pos" in df.columns:
        df = df.rename(columns={"Pos": "Position"})

    # 5. Clean Comp column (rename -> League)
    if "Comp" in df.columns:
        df = df.rename(columns={"Comp": "League"})
        df["League"] = df["League"].astype(str).str.extract(r"([A-Z][\w\s]+)")

    # 6. Rename season column
    season_col = [col for col in df.columns if re.match(r"^\d{4}-\d{4}$", str(col))]
    if season_col:
        df = df.rename(columns={season_col[0]: "Season"})

    # 7. Convert Age and Born
    if "Age" in df.columns:
        df["Age"] = pd.to_numeric(df["Age"], errors="coerce").astype("Int64")
    if "Born" in df.columns:
        df["Born"] = pd.to_numeric(df["Born"], errors="coerce").astype("Int64")

    # 8. Cast categorical columns
    for col in ["Nation", "League", "Squad", "Position", "Season"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


def clean_defending(df: pd.DataFrame) -> pd.DataFrame:
    """
    Defending-specific cleaning:
    - Resolve duplicate column names.
    - Rename into BigQuery-safe snake_case.
    - Convert to appropriate numeric dtypes.
    """

    # 1. Assign unique names to duplicates BEFORE renaming
    df.columns = [
        "Player", "Nation", "Position", "Squad", "League", "Age", "Born", "90s",
        "Tkl_tackles", "TklW", "Def 3rd", "Mid 3rd", "Att 3rd",
        "Tkl_challenges", "Att_challenges", "Tkl%_challenges", "Lost",
        "Blocks", "Sh_blocks", "Pass_blocks",
        "Int", "Tkl+Int", "Clr", "Err", "Season"
    ]

    # 2. Rename to BigQuery-safe snake_case
    rename_map = {
        # Tackles
        "Tkl_tackles": "tackles_total",
        "TklW": "tackles_won",
        "Def 3rd": "tackles_def_3rd",
        "Mid 3rd": "tackles_mid_3rd",
        "Att 3rd": "tackles_att_3rd",

        # Challenges
        "Tkl_challenges": "dribblers_tackled",
        "Att_challenges": "dribbles_challenged",
        "Tkl%_challenges": "dribblers_tackled_pct",
        "Lost": "challenges_lost",

        # Blocks
        "Blocks": "blocks_total",
        "Sh_blocks": "blocks_shots",
        "Pass_blocks": "blocks_passes",

        # Other
        "Int": "interceptions",
        "Tkl+Int": "tackles_plus_interceptions",
        "Clr": "clearances",
        "Err": "errors_leading_to_shot",
    }
    df = df.rename(columns=rename_map)

    # 3. Type conversions
    int_cols = [
        "tackles_total", "tackles_won", "tackles_def_3rd", "tackles_mid_3rd", "tackles_att_3rd",
        "dribblers_tackled", "dribbles_challenged", "challenges_lost",
        "blocks_total", "blocks_shots", "blocks_passes",
        "interceptions", "tackles_interceptions", "clearances", "errors_leading_to_shot"
    ]
    float_cols = ["dribblers_tackled_pct"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    return df


def clean_passing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Passing-specific cleaning:
    - Disambiguate duplicate columns (Cmp, Att, Cmp%) by section (total, short, medium, long).
    - Rename columns into BigQuery-safe snake_case.
    - Convert to appropriate numeric dtypes.

    Parameters
    ----------
    df : pd.DataFrame
        Passing dataframe after common cleaning.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe ready for BigQuery/analysis.
    """

    # 1. Assign unique names to duplicates BEFORE renaming
    df.columns = [
        "Player", "Nation", "Position", "Squad", "League", "Age", "Born", "90s",
        "Cmp_total", "Att_total", "Cmp_pct_total", "TotDist", "PrgDist",
        "Cmp_short", "Att_short", "Cmp_pct_short",
        "Cmp_medium", "Att_medium", "Cmp_pct_medium",
        "Cmp_long", "Att_long", "Cmp_pct_long",
        "Ast", "xAG", "xA", "A-xAG", "KP", "final_third", "PPA", "CrsPA", "PrgP", "Season"
    ]

    # 2. BigQuery-safe renaming (snake_case, no symbols)
    rename_map = {
        # Total
        "Cmp_total": "passes_completed_total",
        "Att_total": "passes_attempted_total",
        "Cmp_pct_total": "passes_completion_pct_total",
        "TotDist": "total_passing_distance",
        "PrgDist": "progressive_passing_distance",

        # Short
        "Cmp_short": "passes_completed_short",
        "Att_short": "passes_attempted_short",
        "Cmp_pct_short": "passes_completion_pct_short",

        # Medium
        "Cmp_medium": "passes_completed_medium",
        "Att_medium": "passes_attempted_medium",
        "Cmp_pct_medium": "passes_completion_pct_medium",

        # Long
        "Cmp_long": "passes_completed_long",
        "Att_long": "passes_attempted_long",
        "Cmp_pct_long": "passes_completion_pct_long",

        # Other
        "Ast": "assists",
        "xAG": "expected_assisted_goals", #rename this
        "xA": "expected_assists",
        "A-xAG": "assists_minus_expected_assisted_goals",
        "KP": "key_passes",
        "final_third": "passes_into_final_third",
        "PPA": "passes_into_penalty_area",
        "CrsPA": "crosses_into_penalty_area",
        "PrgP": "progressive_passes"
    }
    df = df.rename(columns=rename_map)

    # 3. Convert to numeric types
    int_cols = [
        "passes_completed_total", "passes_attempted_total",
        "passes_completed_short", "passes_attempted_short",
        "passes_completed_medium", "passes_attempted_medium",
        "passes_completed_long", "passes_attempted_long",
        "assists", "key_passes", "passes_into_final_third",
        "passes_into_penalty_area", "crosses_into_penalty_area",
        "progressive_passes"
    ]
    float_cols = [
        "passes_completion_pct_total", "total_passing_distance", "progressive_passing_distance",
        "passes_completion_pct_short", "passes_completion_pct_medium", "passes_completion_pct_long",
        "expected_assisted_goals", "expected_assists", "assists_minus_expected_assisted_goals"
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    return df


def clean_shooting(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shooting-specific cleaning:
    - Rename columns to BigQuery-safe snake_case.
    - Convert to appropriate numeric dtypes.
    """
    rename_map = {
        "Gls": "goals",
        "Sh": "shots_total",
        "SoT": "shots_on_target",
        "SoT%": "shots_on_target_pct",
        "Sh/90": "shots_total_per_90",
        "SoT/90": "shots_on_target_per_90",
        "G/Sh": "goals_per_shot",
        "G/SoT": "goals_per_shot_on_target",
        "Dist": "avg_shot_distance",
        "FK": "shots_from_free_kicks",
        "PK": "penalty_kicks_made",
        "PKatt": "penalty_kicks_attempted",
        "xG": "xg",
        "npxG": "npxg",
        "npxG/Sh": "npxg_per_shot",
        "G-xG": "goals_minus_xg",
        "np:G-xG": "nonpen_goals_minus_npxg",
        # Season already cleaned in common_cleaning
    }
    df = df.rename(columns=rename_map)

    # Convert to numeric (safe coercion)
    int_cols = [
        "goals", "shots_total", "shots_on_target",
        "shots_from_free_kicks", "penalty_kicks_made",
        "penalty_kicks_attempted"
    ]
    float_cols = [
        "shots_on_target_pct", "shots_total_per_90", "shots_on_target_per_90",
        "goals_per_shot", "goals_per_shot_on_target", "avg_shot_distance",
        "xg", "npxg", "npxg_per_shot", "goals_minus_xg", "nonpen_goals_minus_npxg"
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    return df


def clean_team_stats_complete(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean squad-level standard stats (team stats):
    - Reset header row to proper columns (fixes Unnamed scraping issue).
    - Drop 'Rk' column.
    - Rename with descriptive, BigQuery-safe snake_case.
    - Convert numeric columns to appropriate dtypes.
    - Leave 'Season' untouched.
    """

    # 1. Force correct column order (ignore scraped junk headers)
    df.columns = [
        "Rk", "Squad", "Competition", "Players_used", "Average_age", "Possession",
        "Matches_played", "Starts", "Minutes", "Nineties_played",
        "Goals", "Assists", "Goals_plus_assists", "Nonpen_goals", "Penalty_kicks_made", "Penalty_kicks_attempted", "Yellow_cards", "Red_cards",
        "Expected_goals", "Nonpen_expected_goals", "Expected_assisted_goals", "Nonpen_expected_goals_plus_expected_assists",
        "Progressive_carries", "Progressive_passes",
        "Goals_per90", "Assists_per90", "Goals_plus_assists_per90",
        "Nonpen_goals_per90", "xG_plus_A-PK_per90", "xG_per90", "xAG_per90", "xG_plus_xAG_per90", "npxG_per90", "npxG_plus_xAG_per90",
        "Season"
    ]

    # 2. Drop rank column
    df = df.drop(columns=["Rk"])

    # 3. Rename to BigQuery-safe descriptive snake_case
    rename_map = {
        "Squad": "squad",
        "Competition": "league",
        "Players_used": "players_used",
        "Average_age": "average_age",
        "Possession": "possession_pct",
        "Matches_played": "matches_played",
        "Starts": "matches_started",
        "Minutes": "minutes_played",
        "Nineties_played": "nineties_played",
        "Goals": "goals_total",
        "Assists": "assists_total",
        "Goals_plus_assists": "goals_plus_assists_total",
        "Nonpen_goals": "nonpen_goals_total",
        "Penalty_kicks_made": "penalty_kicks_made_total",
        "Penalty_kicks_attempted": "penalty_kicks_attempted_total",
        "Yellow_cards": "yellow_cards",
        "Red_cards": "red_cards",
        "Expected_goals": "expected_goals_total",
        "Nonpen_expected_goals": "nonpen_expected_goals_total",
        "Expected_assisted_goals": "expected_assisted_goals_total",
        "Nonpen_expected_goals_plus_expected_assists": "nonpen_expected_goals_plus_expected_assists_total",
        "Progressive_carries": "progressive_carries",
        "Progressive_passes": "progressive_passes",
        "Goals_per90": "goals_per_90",
        "Assists_per90": "assists_per_90",
        "Goals_plus_assists_per90": "goals_plus_assists_per_90",
        "Nonpen_goals_per90": "nonpen_goals_per_90",
        "xG_plus_A-PK_per90": "goals_plus_assists_minus_penalties_per_90",
        "xG_per90": "expected_goals_per_90",
        "xAG_per90": "expected_assisted_goals_per_90",
        "xG_plus_xAG_per90": "expected_goals_plus_expected_assists_per_90",
        "npxG_per90": "nonpen_expected_goals_per_90",
        "npxG_plus_xAG_per90": "nonpen_expected_goals_plus_expected_assists_per_90",
        "Season": "season"
    }

    
    df = df.rename(columns=rename_map)

    if "league" in df.columns:
        df["league"] = (
            df["league"]
            .astype(str)
            .str.extract(r"([A-Z][\w\s]+)", expand=False)
            .str.strip()
        )

    # 4. Type conversions
    int_cols = [
        "players_used", "matches_played", "matches_started", "minutes_played",
        "goals_total", "assists_total", "goals_plus_assists_total",
        "nonpen_goals_total", "penalty_kicks_made_total", "penalty_kicks_attempted_total",
        "yellow_cards", "red_cards"
    ]

    float_cols = [
        "average_age", "possession_pct", "nineties_played",
        "expected_goals_total", "nonpen_expected_goals_total",
        "expected_assisted_goals_total", "nonpen_expected_goals_plus_expected_assists_total",
        "progressive_carries", "progressive_passes",
        "goals_per_90", "assists_per_90", "goals_plus_assists_per_90",
        "nonpen_goals_per_90",  "goals_plus_assists_minus_penalties_per_90",
        "expected_goals_per_90",
        "expected_assisted_goals_per_90",
        "expected_goals_plus_expected_assists_per_90",
        "nonpen_expected_goals_per_90",
        "nonpen_expected_goals_plus_expected_assists_per_90"
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
                    errors="coerce"
                )
                .astype("Int64")
            )

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce"
            ).astype("float")

    # 5. Cast categorical for efficiency
    for cat_col in ["league", "squad", "season"]:
        if cat_col in df.columns:
            df[cat_col] = df[cat_col].astype("category")

    return df


def clean_seasons_wages(df: pd.DataFrame) -> pd.DataFrame:
    # Work on a copy
    df = df.copy()

    # 1. Rename Competition -> League and clean league text
    if "Competition" in df.columns:
        df = df.rename(columns={"Competition": "League"})
    if "League" in df.columns:
        df["League"] = df["League"].astype(str).str.extract(r"([A-Z][\w\s]+)")

    # 2. Drop rank
    if "Rk" in df.columns:
        df = df.drop(columns=["Rk"])

    # 3. Parse wages into EUR/GBP/USD columns
    def _extract_currency_values(s):
        if pd.isna(s):
            return {"EUR": None, "GBP": None, "USD": None}

        s = str(s).replace("â‚¬", "€").replace("Â£", "£").replace("Ł", "£")

        eur_match = re.search(r"€\s*([\d,]+)", s)
        gbp_match = re.search(r"£\s*([\d,]+)", s)
        usd_match = re.search(r"\$\s*([\d,]+)", s)

        return {
            "EUR": float(eur_match.group(1).replace(",", "")) if eur_match else None,
            "GBP": float(gbp_match.group(1).replace(",", "")) if gbp_match else None,
            "USD": float(usd_match.group(1).replace(",", "")) if usd_match else None,
        }

    if "Weekly Wages" in df.columns:
        weekly = df["Weekly Wages"].apply(_extract_currency_values)
        df["weekly_wages_eur"] = weekly.apply(lambda x: x["EUR"])
        df["weekly_wages_gbp"] = weekly.apply(lambda x: x["GBP"])
        df["weekly_wages_usd"] = weekly.apply(lambda x: x["USD"])

    if "Annual Wages" in df.columns:
        annual = df["Annual Wages"].apply(_extract_currency_values)
        df["annual_wages_eur"] = annual.apply(lambda x: x["EUR"])
        df["annual_wages_gbp"] = annual.apply(lambda x: x["GBP"])
        df["annual_wages_usd"] = annual.apply(lambda x: x["USD"])

    # 4. Type conversions
    if "# of Players" in df.columns:
        df["# of Players"] = pd.to_numeric(df["# of Players"], errors="coerce").astype("Int64")

    if "% Estimated" in df.columns:
        df["pct_estimated"] = (
            df["% Estimated"].astype(str).str.replace("%", "", regex=False).str.strip()
        )
        df["pct_estimated"] = pd.to_numeric(df["pct_estimated"], errors="coerce").astype("float")

    for col in [
        "weekly_wages_eur", "weekly_wages_gbp", "weekly_wages_usd",
        "annual_wages_eur", "annual_wages_gbp", "annual_wages_usd"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    for col in ["League", "Squad", "Season"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


def clean_standard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standard stats-specific cleaning:
    - Disambiguate duplicate columns in the per-90 section (Gls, Ast, xG, etc. repeat).
    - Rename all columns into BigQuery-safe snake_case.
    - Convert to appropriate numeric dtypes.

    Expected input: standard.csv after load_fbref_csv() + common_cleaning().
    The dataframe will have 37 columns with duplicates in the per-90 section.
    """

    # 1. Assign unique names to all 37 columns before any renaming
    df.columns = [
        "Player", "Nation", "Position", "Squad", "League", "Age", "Born",
        # Playing time
        "MP", "Starts", "Min", "90s",
        # Counting stats (season totals)
        "Gls", "Ast", "G_plus_A", "G_minus_PK", "PK", "PKatt", "CrdY", "CrdR",
        # Expected stats (season totals)
        "xG", "npxG", "xAG", "npxG_plus_xAG",
        # Progressive
        "PrgC", "PrgP", "PrgR",
        # Per-90 stats (these reuse names from above, so we disambiguate)
        "Gls_per90", "Ast_per90", "G_plus_A_per90", "G_minus_PK_per90",
        "G_plus_A_minus_PK_per90", "xG_per90", "xAG_per90", "xG_plus_xAG_per90",
        "npxG_per90", "npxG_plus_xAG_per90",
        "Season",
    ]

    # 2. Rename to BigQuery-safe snake_case
    rename_map = {
        "MP": "matches_played",
        "Starts": "starts",
        "Min": "minutes",
        "90s": "nineties_played",
        "Gls": "goals",
        "Ast": "assists",
        "G_plus_A": "goals_plus_assists",
        "G_minus_PK": "nonpen_goals",
        "PK": "penalty_kicks_made",
        "PKatt": "penalty_kicks_attempted",
        "CrdY": "yellow_cards",
        "CrdR": "red_cards",
        "xG": "xg",
        "npxG": "npxg",
        "xAG": "xag",
        "npxG_plus_xAG": "npxg_plus_xag",
        "PrgC": "progressive_carries",
        "PrgP": "progressive_passes",
        "PrgR": "progressive_passes_received",
        "Gls_per90": "goals_per_90",
        "Ast_per90": "assists_per_90",
        "G_plus_A_per90": "goals_plus_assists_per_90",
        "G_minus_PK_per90": "nonpen_goals_per_90",
        "G_plus_A_minus_PK_per90": "goals_plus_assists_minus_pk_per_90",
        "xG_per90": "xg_per_90",
        "xAG_per90": "xag_per_90",
        "xG_plus_xAG_per90": "xg_plus_xag_per_90",
        "npxG_per90": "npxg_per_90",
        "npxG_plus_xAG_per90": "npxg_plus_xag_per_90",
    }
    df = df.rename(columns=rename_map)

    # 3. Type conversions
    int_cols = [
        "matches_played", "starts",
        "goals", "assists", "goals_plus_assists", "nonpen_goals",
        "penalty_kicks_made", "penalty_kicks_attempted",
        "yellow_cards", "red_cards",
        "progressive_carries", "progressive_passes", "progressive_passes_received",
    ]
    float_cols = [
        "nineties_played",
        "xg", "npxg", "xag", "npxg_plus_xag",
        "goals_per_90", "assists_per_90", "goals_plus_assists_per_90",
        "nonpen_goals_per_90", "goals_plus_assists_minus_pk_per_90",
        "xg_per_90", "xag_per_90", "xg_plus_xag_per_90",
        "npxg_per_90", "npxg_plus_xag_per_90",
    ]

    # Minutes has commas (e.g. "2,184") — strip before converting
    if "minutes" in df.columns:
        df["minutes"] = (
            pd.to_numeric(
                df["minutes"].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            ).astype("Int64")
        )

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    return df


def _load_team_stats_csv(path: str | Path) -> pd.DataFrame:
    """Load seasons_stats CSV and remove the extra top row from FBref scraping."""
    df = pd.read_csv(path, header=None)
    return df.iloc[1:].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Goalkeeper parsing and cleaning
# ---------------------------------------------------------------------------

def parse_gk_html_dir(html_dir: str | Path) -> pd.DataFrame:
    """
    Parse all goalkeeper stat HTML files from html_dir into a single DataFrame.
    Adds a Season column (extracted from filename, e.g. '2023-2024').
    Expected filenames: goalkeeping_stats_YYYY-YYYY.txt
    """
    html_dir = Path(html_dir)
    dfs = []
    for filepath in sorted(html_dir.glob("goalkeeping_stats_*.txt")):
        season_match = re.search(r"(\d{4}-\d{4})", filepath.name)
        if not season_match:
            continue
        season = season_match.group(1)
        with open(filepath, "r", encoding="utf-8") as f:
            html = f.read()
        df = pd.read_html(StringIO(html))[0]
        # Flatten multi-level column index; duplicate 'Save%' handled in clean_goalkeeping
        df.columns = [col[1] if isinstance(col, tuple) else col for col in df.columns]
        df["Season"] = season
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError(f"No goalkeeping HTML files found in {html_dir}")
    return pd.concat(dfs, ignore_index=True)


def clean_goalkeeping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw goalkeeper stats parsed from HTML by parse_gk_html_dir.
    Expects 29 columns: 28 from the FBref GK table + Season appended by the parser.
    Output columns use BigQuery-safe snake_case.
    """
    df = df.copy()

    # 1. Assign explicit names — handles the two duplicate 'Save%' columns
    df.columns = [
        "Rk", "Player", "Nation", "Pos", "Squad", "Comp", "Age", "Born",
        "MP", "Starts", "Min", "90s",
        "GA", "GA90", "SoTA", "Saves", "Save_pct",
        "W", "D", "L", "CS", "CS_pct",
        "PKatt", "PKA", "PKsv", "PKm", "PK_Save_pct",
        "Matches", "Season",
    ]

    # 2. Drop unused columns and repeated header rows
    df = df.drop(columns=["Rk", "Matches"])
    df = df[df["Player"] != "Player"].reset_index(drop=True)

    # 3. Common field cleaning
    df["Nation"] = df["Nation"].astype(str).str.extract(r"([A-Z]{3})")
    df = df.rename(columns={"Pos": "Position", "Comp": "League"})
    df["League"] = df["League"].astype(str).str.extract(r"([A-Z][\w\s]+)")
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce").astype("Int64")
    df["Born"] = pd.to_numeric(df["Born"], errors="coerce").astype("Int64")

    # 4. Rename stat columns to snake_case
    rename_map = {
        "MP": "matches_played", "Starts": "starts",
        "Min": "minutes",       "90s": "nineties_played",
        "GA": "goals_against",  "GA90": "goals_against_per_90",
        "SoTA": "shots_on_target_against", "Saves": "saves",
        "Save_pct": "save_pct", "W": "wins", "D": "draws", "L": "losses",
        "CS": "clean_sheets",   "CS_pct": "clean_sheet_pct",
        "PKatt": "penalty_kicks_faced",    "PKA": "penalty_kicks_allowed",
        "PKsv": "penalty_kicks_saved",
        "PKm": "penalty_kicks_missed_by_kicker",
        "PK_Save_pct": "pk_save_pct",
    }
    df = df.rename(columns=rename_map)

    # 5. Type conversions
    df["minutes"] = pd.to_numeric(
        df["minutes"].astype(str).str.replace(",", "", regex=False), errors="coerce"
    ).astype("Int64")

    int_cols = [
        "matches_played", "starts", "goals_against", "shots_on_target_against",
        "saves", "wins", "draws", "losses", "clean_sheets",
        "penalty_kicks_faced", "penalty_kicks_allowed",
        "penalty_kicks_saved", "penalty_kicks_missed_by_kicker",
    ]
    float_cols = ["nineties_played", "goals_against_per_90", "save_pct", "clean_sheet_pct", "pk_save_pct"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

    for col in ["Nation", "League", "Squad", "Position", "Season"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

def normalize_player_names(
    df: pd.DataFrame,
    reference_df: pd.DataFrame,
    cutoff: float = 0.75,
) -> pd.DataFrame:
    """
    Align player names in df to match canonical names in reference_df (standard stats).
    Uses fuzzy matching within (Season, Squad) groups to resolve name variants.
    Only renames when similarity >= cutoff.
    """
    key = ["Player", "Season", "Squad"]

    work = df.copy()
    ref = reference_df.copy()
    for col in key:
        if str(work[col].dtype) == "category":
            work[col] = work[col].astype(str)
        if str(ref[col].dtype) == "category":
            ref[col] = ref[col].astype(str)

    check = work.merge(ref[key].drop_duplicates(), on=key, how="left", indicator=True)
    orphan_mask = (check["_merge"] == "left_only").values

    if not orphan_mask.any():
        return df

    name_lookup: dict[str, str] = {}
    for _, row in work[orphan_mask].iterrows():
        if row["Player"] in name_lookup:
            continue
        pool = ref[
            (ref["Season"] == row["Season"]) & (ref["Squad"] == row["Squad"])
        ]["Player"].tolist()
        matches = get_close_matches(row["Player"], pool, n=1, cutoff=cutoff)
        if matches:
            name_lookup[row["Player"]] = matches[0]

    if not name_lookup:
        return df

    result = df.copy()
    is_category = str(result["Player"].dtype) == "category"
    player_series = result["Player"].astype(str).replace(name_lookup)
    result["Player"] = player_series.astype("category") if is_category else player_series
    return result


# ---------------------------------------------------------------------------
# Transfer aggregation
# ---------------------------------------------------------------------------

def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    return float(numerator) / float(denominator) if denominator != 0 else default


def _sum_numeric(group: pd.DataFrame, cols: list) -> dict:
    return {
        col: pd.to_numeric(group[col], errors="coerce").fillna(0).sum()
        for col in cols if col in group.columns
    }


def _primary_row(group: pd.DataFrame, sort_col: str = "minutes") -> pd.Series:
    minutes_num = pd.to_numeric(group[sort_col], errors="coerce").fillna(0)
    return group.iloc[int(minutes_num.argmax())]


def _identity(primary: pd.Series) -> dict:
    return {
        "Player": primary["Player"], "Nation": primary["Nation"],
        "Position": primary["Position"], "Squad": primary["Squad"],
        "League": primary["League"], "Age": primary["Age"], "Born": primary["Born"],
    }


def _agg_standard_group(group: pd.DataFrame) -> pd.Series:
    primary = _primary_row(group)
    sums = _sum_numeric(group, [
        "matches_played", "starts", "minutes",
        "goals", "assists", "goals_plus_assists", "nonpen_goals",
        "penalty_kicks_made", "penalty_kicks_attempted",
        "yellow_cards", "red_cards",
        "xg", "npxg", "xag", "npxg_plus_xag",
        "progressive_carries", "progressive_passes", "progressive_passes_received",
    ])
    n = sums["minutes"] / 90.0
    return pd.Series({
        **_identity(primary), **sums, "Season": primary["Season"],
        "nineties_played": n,
        "goals_per_90":                       _safe_div(sums["goals"], n),
        "assists_per_90":                     _safe_div(sums["assists"], n),
        "goals_plus_assists_per_90":          _safe_div(sums["goals_plus_assists"], n),
        "nonpen_goals_per_90":                _safe_div(sums["nonpen_goals"], n),
        "goals_plus_assists_minus_pk_per_90": _safe_div(sums["goals_plus_assists"] - sums["penalty_kicks_made"], n),
        "xg_per_90":            _safe_div(sums["xg"], n),
        "xag_per_90":           _safe_div(sums["xag"], n),
        "xg_plus_xag_per_90":   _safe_div(sums["xg"] + sums["xag"], n),
        "npxg_per_90":          _safe_div(sums["npxg"], n),
        "npxg_plus_xag_per_90": _safe_div(sums["npxg"] + sums["xag"], n),
    })


def _agg_shooting_group(group: pd.DataFrame) -> pd.Series:
    primary = _primary_row(group, sort_col="90s")
    sums = _sum_numeric(group, [
        "goals", "shots_total", "shots_on_target",
        "shots_from_free_kicks", "penalty_kicks_made", "penalty_kicks_attempted",
        "xg", "npxg",
    ])
    n = pd.to_numeric(group["90s"], errors="coerce").fillna(0).sum()
    shots = pd.to_numeric(group["shots_total"], errors="coerce").fillna(0)
    dist  = pd.to_numeric(group["avg_shot_distance"], errors="coerce").fillna(0)
    avg_dist = _safe_div((shots * dist).sum(), shots.sum())
    return pd.Series({
        **_identity(primary), **sums, "Season": primary["Season"], "90s": n,
        "avg_shot_distance":        avg_dist,
        "shots_on_target_pct":      _safe_div(sums["shots_on_target"] * 100, sums["shots_total"]),
        "shots_total_per_90":       _safe_div(sums["shots_total"], n),
        "shots_on_target_per_90":   _safe_div(sums["shots_on_target"], n),
        "goals_per_shot":           _safe_div(sums["goals"], sums["shots_total"]),
        "goals_per_shot_on_target": _safe_div(sums["goals"], sums["shots_on_target"]),
        "npxg_per_shot":            _safe_div(sums["npxg"], sums["shots_total"]),
        "goals_minus_xg":           sums["goals"] - sums["xg"],
        "nonpen_goals_minus_npxg":  sums["goals"] - sums["penalty_kicks_made"] - sums["npxg"],
    })


def _agg_passing_group(group: pd.DataFrame) -> pd.Series:
    primary = _primary_row(group, sort_col="90s")
    sums = _sum_numeric(group, [
        "passes_completed_total", "passes_attempted_total",
        "total_passing_distance", "progressive_passing_distance",
        "passes_completed_short", "passes_attempted_short",
        "passes_completed_medium", "passes_attempted_medium",
        "passes_completed_long", "passes_attempted_long",
        "assists", "expected_assisted_goals", "expected_assists",
        "assists_minus_expected_assisted_goals",
        "key_passes", "passes_into_final_third",
        "passes_into_penalty_area", "crosses_into_penalty_area",
        "progressive_passes",
    ])
    n = pd.to_numeric(group["90s"], errors="coerce").fillna(0).sum()
    return pd.Series({
        **_identity(primary), **sums, "Season": primary["Season"], "90s": n,
        "passes_completion_pct_total":  _safe_div(sums["passes_completed_total"]  * 100, sums["passes_attempted_total"]),
        "passes_completion_pct_short":  _safe_div(sums["passes_completed_short"]  * 100, sums["passes_attempted_short"]),
        "passes_completion_pct_medium": _safe_div(sums["passes_completed_medium"] * 100, sums["passes_attempted_medium"]),
        "passes_completion_pct_long":   _safe_div(sums["passes_completed_long"]   * 100, sums["passes_attempted_long"]),
    })


def _agg_defending_group(group: pd.DataFrame) -> pd.Series:
    primary = _primary_row(group, sort_col="90s")
    sums = _sum_numeric(group, [
        "tackles_total", "tackles_won",
        "tackles_def_3rd", "tackles_mid_3rd", "tackles_att_3rd",
        "dribblers_tackled", "dribbles_challenged", "challenges_lost",
        "blocks_total", "blocks_shots", "blocks_passes",
        "interceptions", "clearances", "errors_leading_to_shot",
    ])
    n = pd.to_numeric(group["90s"], errors="coerce").fillna(0).sum()
    return pd.Series({
        **_identity(primary), **sums, "Season": primary["Season"], "90s": n,
        "dribblers_tackled_pct":      _safe_div(sums["dribblers_tackled"] * 100, sums["dribbles_challenged"]),
        "tackles_plus_interceptions": sums["tackles_total"] + sums["interceptions"],
    })


def _agg_goalkeeping_group(group: pd.DataFrame) -> pd.Series:
    primary = _primary_row(group)
    sums = _sum_numeric(group, [
        "matches_played", "starts", "goals_against", "shots_on_target_against",
        "saves", "wins", "draws", "losses", "clean_sheets",
        "penalty_kicks_faced", "penalty_kicks_allowed",
        "penalty_kicks_saved", "penalty_kicks_missed_by_kicker",
    ])
    minutes  = pd.to_numeric(group["minutes"],        errors="coerce").fillna(0).sum()
    n        = pd.to_numeric(group["nineties_played"], errors="coerce").fillna(0).sum()
    return pd.Series({
        **_identity(primary), **sums, "Season": primary["Season"],
        "minutes": minutes, "nineties_played": n,
        "goals_against_per_90": _safe_div(sums["goals_against"], n),
        "save_pct":         _safe_div(sums["saves"] * 100, sums["shots_on_target_against"]),
        "clean_sheet_pct":  _safe_div(sums["clean_sheets"] * 100, sums["matches_played"]),
        "pk_save_pct":      _safe_div(sums["penalty_kicks_saved"] * 100, sums["penalty_kicks_faced"]),
    })


_AGG_FNS = {
    "standard":    _agg_standard_group,
    "shooting":    _agg_shooting_group,
    "passing":     _agg_passing_group,
    "defending":   _agg_defending_group,
    "goalkeeping": _agg_goalkeeping_group,
}


def aggregate_transfer_spells(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Merge per-club-spell rows into one row per (Player, Season) for transferred players.
    Counting stats are summed; rates/percentages are recalculated from aggregated totals.
    Identity columns (Squad, League, Position) come from the spell with the most minutes.
    """
    if table_name not in _AGG_FNS:
        raise ValueError(f"Unsupported table_name '{table_name}'. Use: {list(_AGG_FNS)}")

    agg_fn = _AGG_FNS[table_name]

    work = df.copy()
    cat_cols = [c for c in work.columns if str(work[c].dtype) == "category"]
    for col in cat_cols:
        work[col] = work[col].astype(str)

    counts    = work.groupby(["Player", "Season"]).size().reset_index(name="_n")
    multi_keys = counts[counts["_n"] > 1][["Player", "Season"]]

    if multi_keys.empty:
        return df

    flag = work.merge(multi_keys, on=["Player", "Season"], how="left", indicator=True)
    single_df = work[flag["_merge"].values == "left_only"].copy()
    multi_df  = work[flag["_merge"].values == "both"].copy()

    agg_rows = []
    for (player, season), group in multi_df.groupby(["Player", "Season"]):
        row = agg_fn(group.reset_index(drop=True))
        row["Player"] = player
        row["Season"] = season
        agg_rows.append(row)

    aggregated = pd.DataFrame(agg_rows)
    result = pd.concat([single_df, aggregated], ignore_index=True)

    for col in cat_cols:
        if col in result.columns:
            result[col] = result[col].astype("category")

    return result.sort_values(["Season", "Player"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

def clean_dataset_by_name(
    dataset_name: str,
    input_dir: str | Path,
    html_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Clean one dataset by name.
    Supported: standard, shooting, passing, defending, goalkeeping,
               seasons_stats, seasons_wages.
    goalkeeping reads HTML from html_dir (defaults to input_dir/../data_html).
    """
    name = dataset_name.lower().strip()
    input_dir = Path(input_dir)

    if name == "goalkeeping":
        gk_html_dir = Path(html_dir) if html_dir else input_dir.parent / "data_html"
        return clean_goalkeeping(parse_gk_html_dir(gk_html_dir))

    csv_path = input_dir / f"{name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Expected CSV not found: {csv_path}")
    if name == "shooting":
        return clean_shooting(common_cleaning(load_fbref_csv(csv_path)))
    if name == "passing":
        return clean_passing(common_cleaning(load_fbref_csv(csv_path)))
    if name == "defending":
        return clean_defending(common_cleaning(load_fbref_csv(csv_path)))
    if name == "standard":
        return clean_standard(common_cleaning(load_fbref_csv(csv_path)))
    if name == "seasons_stats":
        return clean_team_stats_complete(_load_team_stats_csv(csv_path))
    if name == "seasons_wages":
        return clean_seasons_wages(pd.read_csv(csv_path))
    raise ValueError(f"Unsupported dataset name: {dataset_name}")


def clean_all_datasets(
    input_dir: str | Path,
    output_dir: str | Path,
    html_dir: str | Path | None = None,
    dataset_order: list[str] | None = None,
    normalize_names: bool = True,
    aggregate_transfers: bool = True,
) -> dict[str, str]:
    """
    Full cleaning pipeline for all FBref datasets.

    Player tables (standard, shooting, passing, defending, goalkeeping):
      1. Clean raw data.
      2. Normalise player names against the standard table.
      3. Aggregate mid-season transfer spells into one row per player-season.

    Team tables (seasons_stats, seasons_wages): clean only.
    """
    player_tables = ["standard", "shooting", "passing", "defending", "goalkeeping"]
    team_tables   = ["seasons_stats", "seasons_wages"]
    default_order = player_tables + team_tables
    datasets = dataset_order or default_order

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cleaned: dict[str, pd.DataFrame] = {}
    for dataset in datasets:
        print(f"  Cleaning {dataset}...")
        cleaned[dataset] = clean_dataset_by_name(dataset, input_dir=input_dir, html_dir=html_dir)

    if normalize_names and "standard" in cleaned:
        standard_ref = cleaned["standard"]
        for name in player_tables:
            if name in cleaned and name != "standard":
                print(f"  Normalising names in {name}...")
                cleaned[name] = normalize_player_names(cleaned[name], standard_ref, cutoff=0.60)

    if aggregate_transfers:
        for name in player_tables:
            if name in cleaned:
                print(f"  Aggregating transfers in {name}...")
                cleaned[name] = aggregate_transfer_spells(cleaned[name], name)

    written_files: dict[str, str] = {}
    for dataset, df in cleaned.items():
        out_path = output_dir / f"{dataset}_cleaned.xlsx"
        df.to_excel(out_path, index=False)
        written_files[dataset] = str(out_path)

    return written_files
