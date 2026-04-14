variable "project_id" {
  description = "Your GCP project ID (find it in the GCP Console dashboard)"
  type        = string
}

variable "region" {
  description = "GCP region for Compute/Dataproc resources"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "GCS and BigQuery multi-region location"
  type        = string
  default     = "US"
}

variable "service_account_id" {
  description = "ID for the pipeline service account (letters, numbers, hyphens only)"
  type        = string
  default     = "powerlifting-pipeline-sa"
}
