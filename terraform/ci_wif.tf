# ══════════════════════════════════════════════════════════════════════════════
# ci_wif.tf — Authentification de la CI GitHub Actions sans clé (WIF)
# ══════════════════════════════════════════════════════════════════════════════
#
# WIF = Workload Identity Federation
#   Workload  : un programme sans humain derrière (ici, un job GitHub Actions)
#   Identity  : qui il est, et donc ce qu'il a le droit de faire
#   Federation: GCP fait confiance à une identité prouvée par GitHub,
#               sans qu'aucun secret ne soit partagé entre les deux
#
# Pourquoi ce fichier existe :
#   Avant, la CI utilisait une clé JSON permanente (secret GitHub GCP_SA_KEY).
#   Si elle fuyait, elle donnait accès au bucket indéfiniment.
#   Désormais, à chaque run :
#     1. GitHub délivre au job un jeton OIDC signé ("je suis un job du repo X")
#     2. GCP vérifie la signature et la condition (seul mon repo est accepté)
#     3. GCP délivre un jeton du service account ci-3etoiles, valable ~1 h
#   Plus aucun secret stocké, rien à révoquer ni à faire tourner.
#
# Contenu :
#   A. APIs nécessaires (IAM, IAM Credentials, STS)
#   B. Service account ci-3etoiles, lecture seule sur le bucket (moindre privilège)
#   C. Pool + provider OIDC GitHub, restreint à var.github_repo
#   D. Droit pour les jobs de mon repo d'emprunter ci-3etoiles
#
# Utilisé par : .github/workflows/dbt-ci.yml (action google-github-actions/auth)
# Décision    : ADR-011_Workload_Identity_Federation (Obsidian)
# ══════════════════════════════════════════════════════════════════════════════

# ── APIs nécessaires à la fédération d'identité ──────────────────────────────
resource "google_project_service" "wif_apis" {
  for_each = toset([
    "iam.googleapis.com",            # pools et providers d'identité
    "iamcredentials.googleapis.com", # délivre les jetons courts du service account
    "sts.googleapis.com",            # échange jeton GitHub -> jeton GCP
  ])
  service            = each.value
  disable_on_destroy = false
}

# ── Service account dédié à la CI (lecture seule) ────────────────────────────
resource "google_service_account" "ci" {
  account_id   = "ci-3etoiles"
  display_name = "Service Account CI GitHub Actions"
  description  = "Emprunté par GitHub Actions via WIF — lecture seule du bucket"
}

resource "google_storage_bucket_iam_member" "ci_reader" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ci.email}"
}

# ── Pool : registre des identités externes acceptées ─────────────────────────
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions"
  description               = "Identités des workflows GitHub Actions"

  depends_on = [google_project_service.wif_apis]
}

# ── Provider : confiance dans les jetons OIDC de GitHub, limitée à mon repo ──
resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  attribute_condition = "assertion.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# ── Droit d'emprunt : les jobs de mon repo peuvent agir en tant que ci-3etoiles ──
resource "google_service_account_iam_member" "ci_wif_user" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}