"""
Standalone script to load processed Parquet files from GCS into BigQuery.
Run this if the Spark job completed Parquet write but failed on BQ load.

Usage:
    python spark/load_bq.py
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

PROJECT_ID       = os.environ["GCP_PROJECT_ID"]
PROCESSED_BUCKET = os.environ["GCS_PROCESSED_BUCKET"]
KEY_FILE         = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
BQ_TABLE         = f"{PROJECT_ID}.powerlifting.raw_lifts"
PARQUET_URI      = f"gs://{PROCESSED_BUCKET}/parquet/*"

print(f"Loading {PARQUET_URI} into {BQ_TABLE} ...")

credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,  # DAY exceeds 4000 partition limit (~60 years of data)
        field="lift_date",
    ),
    clustering_fields=["Sex", "Equipment"],  # Federation is a Spark partition key (not in file schema)
    autodetect=True,
)

load_job = client.load_table_from_uri(PARQUET_URI, BQ_TABLE, job_config=job_config)
load_job.result()

table = client.get_table(BQ_TABLE)
print(f"Done. {BQ_TABLE} has {table.num_rows:,} rows.")
