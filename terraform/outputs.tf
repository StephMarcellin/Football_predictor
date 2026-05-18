output "bucket_name" {
  description = "Nom du bucket GCS Bronze"
  value       = google_storage_bucket.bronze.name
}

output "bucket_url" {
  description = "URL du bucket GCS Bronze"
  value       = "gs://${google_storage_bucket.bronze.name}"
}

output "service_account_email" {
  description = "Email du service account pipeline"
  value       = google_service_account.pipeline.email
}