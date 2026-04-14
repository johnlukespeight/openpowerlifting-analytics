output "raw_bucket_name" {
  description = "Name of the GCS raw bucket — use this as GCS_RAW_BUCKET in your .env"
  value       = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  description = "Name of the GCS processed bucket — use this as GCS_PROCESSED_BUCKET in your .env"
  value       = google_storage_bucket.processed.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.powerlifting.dataset_id
}

output "service_account_email" {
  description = "Service account email — reference this when creating/downloading a JSON key"
  value       = google_service_account.pipeline.email
}
