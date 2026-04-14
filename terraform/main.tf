terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------
# GCS — Raw bucket
# Stores the original CSV downloaded from OpenPowerlifting.
# -----------------------------------------------------------------------
resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-opl-raw"
  location      = var.location
  force_destroy = true # allows `terraform destroy` to delete non-empty bucket

  # Automatically delete objects after 90 days to keep storage costs low
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true
}

# -----------------------------------------------------------------------
# GCS — Processed bucket
# Stores Parquet files written by the Spark transformation job.
# -----------------------------------------------------------------------
resource "google_storage_bucket" "processed" {
  name          = "${var.project_id}-opl-processed"
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true
}

# -----------------------------------------------------------------------
# BigQuery — Dataset
# All pipeline tables (raw_lifts, dbt marts) live in this dataset.
# -----------------------------------------------------------------------
resource "google_bigquery_dataset" "powerlifting" {
  dataset_id  = "powerlifting"
  location    = var.location
  description = "OpenPowerlifting analytics pipeline dataset"
}

# -----------------------------------------------------------------------
# Service Account
# Used by ingestion script, Spark job, and dbt to access GCP resources.
# -----------------------------------------------------------------------
resource "google_service_account" "pipeline" {
  account_id   = var.service_account_id
  display_name = "Powerlifting Pipeline Service Account"
  description  = "Programmatic access for ingestion, Spark, and dbt"
}

# BigQuery Admin — allows creating/writing tables and running jobs
resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Storage Admin — allows reading/writing objects AND bucket metadata (required by GCS connector)
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
