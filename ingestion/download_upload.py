"""
Phase 2 — Ingestion Script
Downloads the OpenPowerlifting CSV (inside a ZIP) and uploads it to GCS.

Usage:
    python ingestion/download_upload.py

Required environment variables (set in .env):
    GCS_RAW_BUCKET                — name of the raw GCS bucket
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON key
"""

import datetime
import io
import logging
import os
import zipfile

import requests
from dotenv import load_dotenv
from google.cloud import storage

# Load .env from the project root (one directory above this file)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OPL_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
# Log a progress line every time this many bytes have been downloaded
PROGRESS_EVERY_BYTES = 50 * 1024 * 1024  # 50 MB


def download_zip(url: str) -> io.BytesIO:
    """Stream-download the ZIP archive and return it as an in-memory buffer."""
    log.info("Starting download: %s", url)
    buffer = io.BytesIO()
    downloaded = 0
    next_log_at = PROGRESS_EVERY_BYTES

    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=8192):
            buffer.write(chunk)
            downloaded += len(chunk)
            if downloaded >= next_log_at:
                log.info("  Downloaded %.0f MB so far…", downloaded / 1024 / 1024)
                next_log_at += PROGRESS_EVERY_BYTES

    log.info("Download complete — %.0f MB total", downloaded / 1024 / 1024)
    buffer.seek(0)  # rewind so zipfile can read from the start
    return buffer


def extract_csv(zip_buffer: io.BytesIO) -> tuple[io.BytesIO, str]:
    """Extract the single CSV file from the ZIP buffer.

    Returns:
        (csv_buffer, csv_filename) — the CSV as a BytesIO and its original name.
    """
    with zipfile.ZipFile(zip_buffer) as zf:
        # Find the CSV member (the archive contains exactly one CSV)
        csv_names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_names:
            raise ValueError("No CSV file found inside the ZIP archive.")
        csv_name = csv_names[0]
        log.info("Extracting '%s' from ZIP…", csv_name)
        csv_bytes = zf.read(csv_name)

    log.info("Extracted %.0f MB CSV", len(csv_bytes) / 1024 / 1024)
    return io.BytesIO(csv_bytes), csv_name


def upload_to_gcs(csv_buffer: io.BytesIO, bucket_name: str) -> str:
    """Upload the CSV buffer to GCS under raw/openpowerlifting-YYYY-MM-DD.csv.

    Returns:
        The full GCS URI of the uploaded object.
    """
    today = datetime.date.today().isoformat()  # e.g. "2026-04-02"
    destination_blob = f"raw/openpowerlifting-{today}.csv"

    log.info("Uploading to gs://%s/%s …", bucket_name, destination_blob)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    csv_buffer.seek(0)
    blob.upload_from_file(csv_buffer, content_type="text/csv")

    gcs_uri = f"gs://{bucket_name}/{destination_blob}"
    log.info("Upload complete: %s", gcs_uri)
    return gcs_uri


def download_and_upload() -> str:
    """Orchestrate the full ingest: download → extract → upload.

    Returns the GCS URI of the uploaded file.
    This function can be imported and called by the Airflow DAG in Phase 6.
    """
    bucket_name = os.environ["GCS_RAW_BUCKET"]

    zip_buffer = download_zip(OPL_URL)
    csv_buffer, _ = extract_csv(zip_buffer)
    gcs_uri = upload_to_gcs(csv_buffer, bucket_name)
    return gcs_uri


if __name__ == "__main__":
    uri = download_and_upload()
    print(f"\nIngestion finished. File available at:\n  {uri}")
