"""
run_cleaning.py
---------------
Run the full cleaning pipeline for all FBref datasets.

Pipeline:
  Player tables (standard, shooting, passing, defending, goalkeeping):
    1. Clean raw data (CSV or HTML).
    2. Normalize player names against the standard table.
    3. Aggregate mid-season transfer spells into one row per player-season.

  Team tables (seasons_stats, seasons_wages):
    Clean raw CSV only.

Output: cleaned Excel files saved to cleaned_data/
"""

from pathlib import Path
from src.fbref_utils import clean_all_datasets

BASE_DIR   = Path(__file__).parent
INPUT_DIR  = BASE_DIR / "uncleaned_data_csv"
HTML_DIR   = BASE_DIR / "data_html"
OUTPUT_DIR = BASE_DIR / "cleaned_data"


def main():
    print("Starting cleaning pipeline...")
    print(f"  CSV input : {INPUT_DIR}")
    print(f"  HTML input: {HTML_DIR}")
    print(f"  Output    : {OUTPUT_DIR}\n")

    results = clean_all_datasets(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        html_dir=HTML_DIR,
        normalize_names=True,
        aggregate_transfers=True,
    )

    print("\nDone! Files written:")
    for dataset, path in results.items():
        print(f"  [{dataset}] -> {path}")


if __name__ == "__main__":
    main()
