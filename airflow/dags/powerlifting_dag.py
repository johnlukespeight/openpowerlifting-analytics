"""
Powerlifting Pipeline DAG
=========================
Orchestrates the full weekly batch ELT:

  ingest → spark_transform → dbt_run → dbt_test

Each task runs as a BashOperator so the same scripts used during
development are called unchanged inside the Airflow container.
"""

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR     = f"{PROJECT_DIR}/dbt"
DBT_BIN     = "dbt"   # dbt-bigquery is pip-installed in the Airflow image

with DAG(
    dag_id="powerlifting_pipeline",
    description="Weekly batch ELT: ingest OPL CSV → Spark → BigQuery → dbt",
    schedule="@weekly",
    start_date=datetime.datetime(2026, 1, 7),
    catchup=False,
    tags=["powerlifting", "elt"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:

    # ── 1. Download latest OPL CSV and upload to GCS ─────────────────────────
    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python ingestion/download_upload.py"
        ),
    )

    # ── 2. Spark: clean CSV → Parquet on GCS, load into BigQuery ────────────
    # INPUT_DATE is set to today's date (the date the ingestion just uploaded)
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "INPUT_DATE=$(date +%Y-%m-%d) "
            "spark-submit "
            "  --driver-memory 2g "
            f"  --jars {PROJECT_DIR}/spark/jars/gcs-connector-hadoop3-latest.jar "
            f"  {PROJECT_DIR}/spark/transform.py"
        ),
        # Spark jobs can be slow — allow up to 30 minutes
        execution_timeout=datetime.timedelta(minutes=30),
    )

    # ── 3. dbt: build staging + intermediate + mart models ───────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    # ── 4. dbt: run data-quality tests ───────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    # ── Pipeline dependency chain ─────────────────────────────────────────────
    ingest >> spark_transform >> dbt_run >> dbt_test
