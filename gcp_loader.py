"""
gcp_loader.py
=============
Uploads cleaned Excel files to GCS then loads them into BigQuery.

Usage
-----
Before running, set your GCP project in two places:
    GCP_PROJECT  = "your-project-id"
    GCS_BUCKET   = "your-bucket-name"

Then run:
    python gcp_loader.py

What it does
------------
1. Reads each cleaned .xlsx file from cleaned_data/
2. Uploads it as a parquet file to GCS  (gs://bucket/scouting/table.parquet)
3. Loads the parquet from GCS into BigQuery (scouting.table)
4. After all raw tables are loaded, runs the two feature table queries
   (outfield_features and goalkeeper_features) so they stay in sync.

Requirements
------------
    pip install google-cloud-bigquery google-cloud-storage pyarrow pandas openpyxl
"""

from pathlib import Path
import pandas as pd
from google.cloud import bigquery, storage

# ── CONFIG ── change these two lines ─────────────────────────────────────────
GCP_PROJECT = "scouting-466001"       # e.g. "scouting-report-123456"
GCS_BUCKET  = "magician-scouting-project"      # e.g. "scouting-report-data"
# ─────────────────────────────────────────────────────────────────────────────

BQ_DATASET  = "scouting"
GCS_PREFIX  = "scouting"
CLEANED_DIR = Path(__file__).parent / "cleaned_data"

# Map filename → BigQuery table name
TABLE_MAP = {
    "standard_cleaned.xlsx":    "standard",
    "shooting_cleaned.xlsx":    "shooting",
    "passing_cleaned.xlsx":     "passing",
    "defending_cleaned.xlsx":   "defending",
    "goalkeeping_cleaned.xlsx": "goalkeeping",
}


def upload_to_gcs(df: pd.DataFrame, table_name: str, bucket) -> str:
    """Upload a DataFrame as parquet to GCS. Returns the GCS URI."""
    blob_path = f"{GCS_PREFIX}/{table_name}.parquet"
    blob = bucket.blob(blob_path)

    parquet_bytes = df.to_parquet(index=False)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
    print(f"  Uploaded → {gcs_uri}")
    return gcs_uri


def load_into_bigquery(gcs_uri: str, table_name: str, bq_client: bigquery.Client):
    """Load a parquet file from GCS into a BigQuery table (overwrite)."""
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite
        autodetect=True,
    )

    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # wait for completion
    print(f"  Loaded  → {table_ref}  ({bq_client.get_table(table_ref).num_rows} rows)")


def rebuild_feature_tables(bq_client: bigquery.Client):
    """
    Re-run the CREATE OR REPLACE TABLE queries for outfield_features
    and goalkeeper_features so they reflect the latest raw data.
    """
    print("\nRebuilding feature tables...")

    outfield_sql = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.outfield_features` AS
    SELECT
        s.Player,
        s.Nation,
        SPLIT(s.Position, ',')[OFFSET(0)]   AS Position,
        s.Squad,
        s.League,
        s.Age,
        s.Born,
        s.Season,
        CASE
            WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('FW') THEN 'ATT'
            WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('MF') THEN 'MID'
            WHEN SPLIT(s.Position, ',')[OFFSET(0)] IN ('DF') THEN 'DEF'
            ELSE 'MID'
        END AS position_group,
        s.minutes,
        s.nineties_played,
        s.goals                             AS std_goals,
        s.assists                           AS std_assists,
        s.nonpen_goals,
        s.penalty_kicks_made,
        s.yellow_cards,
        s.red_cards,
        s.xg                                AS std_xg,
        s.npxg,
        s.xag,
        s.progressive_carries,
        s.progressive_passes                AS std_progressive_passes,
        s.progressive_passes_received,
        s.goals_per_90,
        s.assists_per_90,
        s.nonpen_goals_per_90,
        s.xg_per_90,
        s.xag_per_90,
        s.npxg_per_90,
        s.npxg_plus_xag_per_90,
        sh.shots_total,
        sh.shots_on_target,
        sh.shots_on_target_pct,
        sh.shots_total_per_90,
        sh.shots_on_target_per_90,
        sh.goals_per_shot,
        sh.goals_per_shot_on_target,
        sh.avg_shot_distance,
        sh.npxg_per_shot,
        sh.goals_minus_xg,
        sh.nonpen_goals_minus_npxg,
        p.passes_completed_total,
        p.passes_attempted_total,
        p.passes_completion_pct_total,
        p.total_passing_distance,
        p.progressive_passing_distance,
        p.passes_completion_pct_short,
        p.passes_completion_pct_medium,
        p.passes_completion_pct_long,
        p.expected_assisted_goals,
        p.expected_assists,
        p.key_passes,
        p.passes_into_final_third,
        p.passes_into_penalty_area,
        p.crosses_into_penalty_area,
        p.progressive_passes                AS pass_progressive_passes,
        d.tackles_total,
        d.tackles_won,
        d.tackles_def_3rd,
        d.tackles_mid_3rd,
        d.tackles_att_3rd,
        d.dribblers_tackled,
        d.dribblers_tackled_pct,
        d.blocks_total,
        d.interceptions,
        d.tackles_plus_interceptions,
        d.clearances,
        d.errors_leading_to_shot
    FROM `{GCP_PROJECT}.{BQ_DATASET}.standard` s
    LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.shooting`  sh ON s.Player = sh.Player AND s.Season = sh.Season
    LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.passing`   p  ON s.Player = p.Player  AND s.Season = p.Season
    LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.defending` d  ON s.Player = d.Player  AND s.Season = d.Season
    WHERE SPLIT(s.Position, ',')[OFFSET(0)] != 'GK'
      AND s.minutes >= 90
    """

    goalkeeper_sql = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.goalkeeper_features` AS
    SELECT
        s.Player,
        s.Nation,
        s.Position,
        s.Squad,
        s.League,
        s.Age,
        s.Born,
        s.Season,
        'GK' AS position_group,
        s.minutes,
        s.nineties_played,
        g.goals_against,
        g.goals_against_per_90,
        g.shots_on_target_against,
        g.saves,
        g.save_pct,
        g.wins,
        g.draws,
        g.losses,
        g.clean_sheets,
        g.clean_sheet_pct,
        g.penalty_kicks_faced,
        g.penalty_kicks_allowed,
        g.penalty_kicks_saved,
        g.pk_save_pct
    FROM `{GCP_PROJECT}.{BQ_DATASET}.standard` s
    INNER JOIN `{GCP_PROJECT}.{BQ_DATASET}.goalkeeping` g ON s.Player = g.Player AND s.Season = g.Season
    WHERE SPLIT(s.Position, ',')[OFFSET(0)] = 'GK'
      AND s.minutes >= 90
    """

    for name, sql in [("outfield_features", outfield_sql), ("goalkeeper_features", goalkeeper_sql)]:
        job = bq_client.query(sql)
        job.result()
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
        print(f"  Rebuilt → {table_ref}  ({bq_client.get_table(table_ref).num_rows} rows)")


def ensure_dataset_exists(bq_client: bigquery.Client):
    """Create the BigQuery dataset if it doesn't exist yet."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset ready: {GCP_PROJECT}.{BQ_DATASET}")


def ensure_bucket_exists(storage_client: storage.Client):
    """Create the GCS bucket if it doesn't exist yet."""
    bucket = storage_client.bucket(GCS_BUCKET)
    if not bucket.exists():
        bucket = storage_client.create_bucket(GCS_BUCKET, location="US")
        print(f"Bucket created: gs://{GCS_BUCKET}")
    else:
        print(f"Bucket ready:   gs://{GCS_BUCKET}")
    return bucket


def main():
    print("=== Scouting Report — GCP Loader ===\n")

    bq_client      = bigquery.Client(project=GCP_PROJECT)
    storage_client = storage.Client(project=GCP_PROJECT)

    ensure_dataset_exists(bq_client)
    bucket = ensure_bucket_exists(storage_client)

    for filename, table_name in TABLE_MAP.items():
        path = CLEANED_DIR / filename
        if not path.exists():
            print(f"  SKIP {filename} — file not found")
            continue

        print(f"\n[{table_name}]")
        df = pd.read_excel(path)
        print(f"  Read    → {len(df)} rows, {len(df.columns)} columns")

        gcs_uri = upload_to_gcs(df, table_name, bucket)
        load_into_bigquery(gcs_uri, table_name, bq_client)

    rebuild_feature_tables(bq_client)

    print("\nDone. All tables loaded and feature tables rebuilt.")


if __name__ == "__main__":
    main()
