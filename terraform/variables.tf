variable "project_id" {
  description = "ID du projet GCP"
  type        = string
}

variable "region" {
  description = "Région GCP pour les ressources"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "Nom du bucket GCS pour la couche Bronze"
  type        = string
}