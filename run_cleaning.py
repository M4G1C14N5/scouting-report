"""
run_cleaning.py
---------------
Run the full cleaning pipeline for all FBref datasets.

Pipeline:
  - shooting, passing, defending, standard -> common_cleaning() -> specific cleaner
  - seasons_stats, seasons_wages           -> unique cleaner only

Output: cleaned Excel files saved to ../cleaned_data/
"""

from pathlib import Path
from src.fbref_utils import clean_all_datasets

BASE_DIR   = Path(__file__).parent
INPUT_DIR  = BASE_DIR / "uncleaned_data_csv"
OUTPUT_DIR = BASE_DIR / "cleaned_data"


def main():
    print("Starting cleaning pipeline...")
    print(f"  Input : {INPUT_DIR}")
    print(f"  Output: {OUTPUT_DIR}\n")

    results = clean_all_datasets(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)

    print("Done! Files written:")
    for dataset, path in results.items():
        print(f"  [{dataset}] -> {path}")


if __name__ == "__main__":
    main()
