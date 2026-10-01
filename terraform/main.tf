# ── Provider GCP ─────────────────────────────────────────────────────────────
# Le provider est le plugin Terraform qui sait parler à GCP.
# Terraform le télécharge automatiquement lors du "terraform init".
terraform {
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

# ── Bucket GCS — couche Bronze ────────────────────────────────────────────────
# C'est là qu'on stockera les fichiers Parquet produits par 01_ingest.py
# et 01b_odds.py au lieu de les garder uniquement en local.
resource "google_storage_bucket" "bronze" {
  name          = var.bucket_name
  location      = var.region
    force_destroy = false   # terraform destroy refuse de supprimer un bucket non vide

  # Versioning : un objet écrasé ou supprimé est conservé comme version "non courante"
  versioning {
    enabled = true
  }

  # Règle 1 — sauvegardes : passage en Coldline après 30 jours
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["backup/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Règle 2 — ménage : on supprime une version non courante dès qu'il en existe 3 plus récentes
  lifecycle_rule {
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# ── Service Account ───────────────────────────────────────────────────────────
# Un service account est une identité GCP pour les applications — pas pour les humains.
# Le pipeline utilise ce compte pour lire/écrire dans le bucket GCS.
# C'est plus sécurisé que d'utiliser ton compte personnel.
resource "google_service_account" "pipeline" {
  account_id   = "pipeline-3etoiles"
  display_name = "Service Account Pipeline 3-Étoiles"
  description  = "Utilisé par le pipeline pour accéder au bucket Bronze"
}

# ── Permissions ───────────────────────────────────────────────────────────────
# On donne au service account uniquement les droits dont il a besoin :
# lire et écrire dans le bucket Bronze — pas plus.
# C'est le principe du moindre privilège (least privilege).
resource "google_storage_bucket_iam_member" "pipeline_bronze" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}