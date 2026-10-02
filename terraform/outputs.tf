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


output "wif_provider" {
  description = "Identifiant complet du provider WIF (à mettre dans le workflow GitHub)"
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "ci_service_account_email" {
  description = "Service account emprunté par la CI"
  value       = google_service_account.ci.email
}