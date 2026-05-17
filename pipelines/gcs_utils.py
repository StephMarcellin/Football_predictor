"""
gcs_utils.py — Utilitaires pour l'upload vers Google Cloud Storage.

Utilisé par 01_ingest.py et 01b_odds.py pour persister les fichiers
Parquet Bronze sur GCS après leur création locale.
"""
import os
from pathlib import Path

from google.cloud import storage
from loguru import logger


def get_gcs_client() -> storage.Client:
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        # Rendre le chemin absolu par rapport à la racine du projet
        credentials_path = Path(credentials_path)
        if not credentials_path.is_absolute():
            root = Path(__file__).resolve().parent.parent
            credentials_path = root / credentials_path
        return storage.Client.from_service_account_json(str(credentials_path))
    return storage.Client()


def upload_to_gcs(local_path: Path, bucket_name: str, gcs_prefix: str = "bronze") -> str:
    """
    Upload un fichier local vers GCS.

    Args:
        local_path   : chemin local du fichier à uploader
        bucket_name  : nom du bucket GCS (ex: "3etoiles-bronze")
        gcs_prefix   : préfixe du chemin dans le bucket (ex: "bronze")

    Returns:
        URL GCS du fichier uploadé (ex: "gs://3etoiles-bronze/bronze/fbref/...")

    Le chemin dans le bucket reproduit la structure locale à partir de "data/raw/" :
        data/raw/fbref/parquet/2024-2025.parquet
        → gs://3etoiles-bronze/bronze/fbref/parquet/2024-2025.parquet
    """
    print(f"DEBUG upload_to_gcs appelé : {local_path} → {bucket_name}")
    try:
        client  = get_gcs_client()
        bucket  = client.bucket(bucket_name)

        # On reconstruit le chemin relatif à partir de "data/raw/"
        # pour reproduire la même structure dans GCS
        parts        = local_path.parts
        raw_idx      = next(i for i, p in enumerate(parts) if p == "raw")
        relative_path = "/".join(parts[raw_idx + 1:])
        blob_name    = f"{gcs_prefix}/{relative_path}"

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(local_path))

        gcs_url = f"gs://{bucket_name}/{blob_name}"
        logger.success(f"GCS upload : {local_path.name} → {gcs_url}")
        return gcs_url

    except Exception as e:
        logger.warning(f"GCS upload échoué pour {local_path.name} : {e}")
        return ""


def upload_directory_to_gcs(local_dir: Path, bucket_name: str, gcs_prefix: str = "bronze") -> list[str]:
    """
    Upload tous les fichiers Parquet d'un dossier vers GCS.

    Args:
        local_dir   : dossier local contenant les fichiers Parquet
        bucket_name : nom du bucket GCS
        gcs_prefix  : préfixe du chemin dans le bucket

    Returns:
        Liste des URLs GCS des fichiers uploadés
    """
    if not local_dir.exists():
        logger.warning(f"Dossier inexistant : {local_dir}")
        return []

    parquet_files = list(local_dir.glob("**/*.parquet"))
    if not parquet_files:
        logger.warning(f"Aucun fichier Parquet trouvé dans {local_dir}")
        return []

    logger.info(f"Upload GCS : {len(parquet_files)} fichiers depuis {local_dir}")
    urls = []
    for f in parquet_files:
        url = upload_to_gcs(f, bucket_name, gcs_prefix)
        if url:
            urls.append(url)

    logger.info(f"Upload GCS terminé : {len(urls)}/{len(parquet_files)} fichiers")
    return urls