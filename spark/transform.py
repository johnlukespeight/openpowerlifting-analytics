"""
Phase 3 & 4 — Spark Transformation + BigQuery Load
Reads the raw CSV from GCS, cleans it, writes Parquet to the processed
bucket, then loads it into a partitioned/clustered BigQuery table.

Usage:
    spark-submit \
        --jars spark/jars/gcs-connector-hadoop3-latest.jar,spark/jars/spark-bigquery-with-dependencies_2.13-0.41.0.jar \
        spark/transform.py

Required environment variables (set in .env):
    GCS_RAW_BUCKET                — raw GCS bucket name
    GCS_PROCESSED_BUCKET          — processed GCS bucket name
    GCP_PROJECT_ID                — GCP project ID
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON key
"""

import datetime
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

load_dotenv()

# ── Environment variables ────────────────────────────────────────────────────
RAW_BUCKET        = os.environ["GCS_RAW_BUCKET"]
PROCESSED_BUCKET  = os.environ["GCS_PROCESSED_BUCKET"]
PROJECT_ID        = os.environ["GCP_PROJECT_ID"]
KEY_FILE          = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# Default to today's dated CSV; override with INPUT_DATE env var if needed
input_date  = os.environ.get("INPUT_DATE", datetime.date.today().isoformat())
INPUT_PATH  = f"gs://{RAW_BUCKET}/raw/openpowerlifting-{input_date}.csv"
OUTPUT_PATH = f"gs://{PROCESSED_BUCKET}/parquet/"
BQ_TABLE    = f"{PROJECT_ID}.powerlifting.raw_lifts"

# Columns that should be numeric (stored as strings in the raw CSV)
NUMERIC_COLS = [
    "Best3SquatKg",
    "Best3BenchKg",
    "Best3DeadliftKg",
    "TotalKg",
    "BodyweightKg",
    "Dots",
    "Wilks",
    "Age",
]


def build_spark_session(key_file: str) -> SparkSession:
    """Create a SparkSession configured for GCS and BigQuery access."""
    spark = (
        SparkSession.builder
        .appName("opl-transform")
        # Tell the GCS connector where to find credentials
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_file)
        # Required so GCS paths (gs://) are handled by the connector
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # BigQuery connector needs to know the GCP project
        .config("spark.hadoop.fs.gs.project.id", PROJECT_ID)
        # Reduce GCS upload chunk size from default 64MB to 8MB to avoid OOM
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "8388608")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark INFO logs
    return spark


def transform(spark: SparkSession):
    """Read, clean, and write the OpenPowerlifting dataset."""

    # ── 1. Read raw CSV ──────────────────────────────────────────────────────
    print(f"Reading CSV from {INPUT_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # read everything as string first
        .csv(INPUT_PATH)
    )
    print(f"Raw row count: {df.count():,}")

    # ── 2. Cast numeric columns ──────────────────────────────────────────────
    for col in NUMERIC_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # ── 3. Filter out disqualified / non-starters ────────────────────────────
    # Place can be DQ (disqualified), NS (no show), DD (doping disqualification)
    df = df.filter(
        F.col("TotalKg").isNotNull() &
        (~F.col("Place").isin("DQ", "NS", "DD"))
    )
    print(f"Row count after filtering: {df.count():,}")

    # ── 4. Add `year` column (integer) ───────────────────────────────────────
    # Date column is formatted YYYY-MM-DD; extract the first 4 characters
    df = df.withColumn("year", F.substring("Date", 1, 4).cast("integer"))

    # ── 5. Add `lift_date` column (DateType) ─────────────────────────────────
    df = df.withColumn("lift_date", F.to_date(F.col("Date"), "yyyy-MM-dd"))

    # ── 6. Normalise Equipment (trim whitespace, title-case) ─────────────────
    df = df.withColumn("Equipment", F.initcap(F.trim(F.col("Equipment"))))

    # ── 7. Write Parquet to processed bucket (Phase 3) ───────────────────────
    # Partitioned by year only — keeping Federation as a data column so BigQuery
    # can ingest it (Spark removes partition columns from the Parquet file schema)
    print(f"Writing Parquet to {OUTPUT_PATH}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(OUTPUT_PATH)
    )
    print("Parquet write complete.")

    # ── 8. Load into BigQuery (Phase 4) ─────────────────────────────────────
    # Use the Python BigQuery client to load the Parquet files from GCS.
    # This avoids the Spark-BQ connector which is incompatible with Spark 4.x.
    load_parquet_to_bigquery(OUTPUT_PATH, PROJECT_ID, KEY_FILE)


def load_parquet_to_bigquery(parquet_gcs_path: str, project_id: str, key_file: str):
    """Load Parquet files from GCS into BigQuery using the BQ Python client."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    print(f"Loading into BigQuery table: {BQ_TABLE}")

    credentials = service_account.Credentials.from_service_account_file(key_file)
    client = bigquery.Client(project=project_id, credentials=credentials)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite
        # Partition by lift_date (DAY) and cluster by Federation, Sex, Equipment
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,  # DAY exceeds 4000 partition limit (~60 years of data)
            field="lift_date",
        ),
        clustering_fields=["Sex", "Equipment"],  # Federation is a Spark partition key (not in file schema)
        autodetect=True,
    )

    # Load all parquet files under the output path using a wildcard.
    # BQ supports * which matches any sequence of characters including /
    uri = f"{parquet_gcs_path}*"
    load_job = client.load_table_from_uri(uri, BQ_TABLE, job_config=job_config)
    load_job.result()  # wait for the job to complete

    table = client.get_table(BQ_TABLE)
    print(f"BigQuery load complete: {BQ_TABLE} ({table.num_rows:,} rows)")


if __name__ == "__main__":
    spark = build_spark_session(KEY_FILE)
    try:
        transform(spark)
    finally:
        spark.stop()
