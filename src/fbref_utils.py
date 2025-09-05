import pandas as pd
import re

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
