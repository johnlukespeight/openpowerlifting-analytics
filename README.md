# Powerlifting Analytics Pipeline

An end-to-end batch ELT project on GCP built from the [OpenPowerlifting](https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html) dataset. The pipeline downloads the latest competition results, lands raw data in Google Cloud Storage, transforms it with PySpark, loads it into BigQuery, models marts with dbt, validates quality with Bruin, and serves analytics in Streamlit.

## Architecture

```text
OpenPowerlifting ZIP/CSV
        |
        v
Python ingestion
        |
        v
GCS raw bucket
        |
        v
PySpark cleaning + Parquet
        |
        v
GCS processed bucket
        |
        v
BigQuery raw_lifts
        |
        v
dbt staging -> intermediate -> marts
        |
        v
Bruin checks + Streamlit dashboard

Optional orchestration: Apache Airflow (Docker Compose)
Optional infrastructure: Terraform
```

## Dataset

| Property | Detail |
|---|---|
| Source | OpenPowerlifting bulk CSV |
| URL | `https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip` |
| Size | ~3.8M competition records |
| Fields | Athlete, federation, sex, equipment, bodyweight, squat, bench, deadlift, total, Dots, Wilks, date |

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud | Google Cloud Platform |
| Infrastructure | Terraform |
| Ingestion | Python |
| Processing | PySpark |
| Warehouse | BigQuery |
| Transformations | dbt + `dbt-bigquery` |
| Quality | Bruin |
| Orchestration | Airflow |
| Dashboard | Streamlit + Plotly |

## Project Structure

```text
powerlifting-pipeline/
├── README.md
├── .env.example
├── .bruin.yml.example
├── terraform/
├── ingestion/
├── spark/
├── dbt/
├── bruin/
├── airflow/
└── dashboard/
```

## Quickstart

This is the shortest path to a submission-ready local setup.

### 1. Prerequisites

- GCP project with billing enabled
- `gcloud` authenticated with application default credentials
- Terraform
- Python 3.11+
- Java 17
- Docker Desktop
- `dbt-bigquery`
- Bruin CLI

Example installs:

```bash
gcloud auth application-default login
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-bigquery
brew install openjdk@17 bruin-data/tap/bruin
```

### 2. Provision GCP infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Set project_id before applying
terraform init
terraform apply
cd ..
```

Terraform creates:

- a raw GCS bucket
- a processed GCS bucket
- a `powerlifting` BigQuery dataset
- a service account for the pipeline

### 3. Configure environment variables

```bash
cp .env.example .env
```

Update `.env` with your values:

- `GCP_PROJECT_ID`
- `GCS_RAW_BUCKET`
- `GCS_PROCESSED_BUCKET`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `BQ_PROJECT`
- `BQ_DATASET`

### 4. Run ingestion

```bash
pip install -r ingestion/requirements.txt
python ingestion/download_upload.py
```

This downloads the latest OpenPowerlifting ZIP, extracts the CSV, and uploads it to `gs://<raw-bucket>/raw/openpowerlifting-YYYY-MM-DD.csv`.

### 5. Run Spark transformation and BigQuery load

Download the GCS connector JAR:

```bash
mkdir -p spark/jars
curl -L -o spark/jars/gcs-connector-hadoop3-latest.jar \
  https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
```

Run the transform:

```bash
pip install -r spark/requirements.txt
INPUT_DATE=$(date +%Y-%m-%d) spark-submit \
  --driver-memory 4g \
  --jars spark/jars/gcs-connector-hadoop3-latest.jar \
  spark/transform.py
```

This writes partitioned Parquet to the processed bucket and truncates/reloads `powerlifting.raw_lifts` in BigQuery.

### 6. Build marts with dbt

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
dbt run --profiles-dir dbt --project-dir dbt
dbt test --profiles-dir dbt --project-dir dbt
```

### 7. Run quality checks with Bruin

```bash
cp .bruin.yml.example .bruin.yml
# Set project_id and service_account_file in .bruin.yml
bruin run bruin/
```

### 8. Launch the dashboard

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

The dashboard reads `GCP_PROJECT_ID` or `BQ_PROJECT`, `BQ_DATASET`, and `GOOGLE_APPLICATION_CREDENTIALS` from `.env`.

## Optional: Airflow

Airflow is included for local orchestration, but it is not required to demo the project.

```bash
cd airflow
cp .env.example .env
# Set AIRFLOW_GCP_KEY_PATH and the bucket/project values
docker compose up --build -d
```

Open `http://localhost:8080` and log in with:

- username: `airflow`
- password: `airflow`

The DAG name is `powerlifting_pipeline`.

## BigQuery Tables

| Table | Purpose |
|---|---|
| `powerlifting.raw_lifts` | Cleaned raw competition records loaded from Spark |
| `powerlifting.stg_powerlifting` | Renamed staging model |
| `powerlifting.int_completed_lifts` | Filtered intermediate model for complete lifts |
| `powerlifting.mart_athlete_records` | Athlete personal records and leaderboard metrics |
| `powerlifting.mart_federation_stats` | Federation-level annual trends |
| `powerlifting.mart_weight_class_trends` | Weight-class performance trends over time |

## Submission Notes

If you are submitting this project for review, the cleanest demo flow is:

1. `terraform apply`
2. `python ingestion/download_upload.py`
3. `spark-submit spark/transform.py`
4. `dbt run && dbt test`
5. `bruin run bruin/`
6. `streamlit run dashboard/app.py`

That path exercises the full pipeline without requiring Airflow.

## Reproducibility

- Secrets stay out of Git through `.gitignore`
- The ingestion step writes a date-stamped raw object
- Spark writes Parquet with `overwrite`
- BigQuery raw load uses `WRITE_TRUNCATE`
- dbt models are idempotent
- Airflow has `catchup=False`
