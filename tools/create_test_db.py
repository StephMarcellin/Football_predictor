"""
create_test_db.py
-----------------
Extrait une saison test (Premier League 2023-2024) depuis football.duckdb
et crée football_test.duckdb pour la CI GitHub Actions.

Usage :
    python tools/create_test_db.py
"""

import sys
from pathlib import Path

import duckdb
from loguru import logger

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------
# ROOT_DIR = racine du projet (parent de tools/)
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PROD   = ROOT_DIR / "db" / "football.duckdb"
DB_TEST   = ROOT_DIR / "db" / "football_test.duckdb"

# Filtre fixture
LEAGUE   = "Premier League"
SEASON   = "2023-2024"

# ---------------------------------------------------------------------------
# Tables avec filtre direct league_source + season
# ---------------------------------------------------------------------------
# Ces tables ont les deux colonnes, on filtre directement
DIRECT_TABLES = [
    "fbref_keeper",
    "fbref_misc",
    "fbref_schedule",
    "fbref_shooting",
    "odds",
    "understat_schedule",
    "whoscored_team_season",
    "stg_whoscored_events",
    "stg_whoscored_match_index",
    "stg_whoscored_match_details",
]


def create_test_db() -> None:
    logger.info(f"Base source      : {DB_PROD}")
    logger.info(f"Base test cible  : {DB_TEST}")
    logger.info(f"Fixture          : {LEAGUE} / {SEASON}")

    # Supprime la base test si elle existe déjà (re-run propre)
    if DB_TEST.exists():
        DB_TEST.unlink()
        logger.info("Base test existante supprimée.")

    # Ouvre la prod en lecture seule
    prod = duckdb.connect(str(DB_PROD), read_only=True)

    # Crée la base test (lecture/écriture)
    test = duckdb.connect(str(DB_TEST))

    # Attache la prod dans la base test pour les COPY directes
    test.execute(f"ATTACH '{DB_PROD}' AS prod (READ_ONLY)")

    # -----------------------------------------------------------------------
    # 1. Tables avec filtre direct
    # -----------------------------------------------------------------------
    for table in DIRECT_TABLES:
        logger.info(f"  Copie {table}...")
        test.execute(f"""
            CREATE SCHEMA IF NOT EXISTS silver;

            CREATE TABLE silver.{table} AS
            SELECT *
            FROM prod.silver.{table}
            WHERE league_source = '{LEAGUE}'
              AND season        = '{SEASON}'
        """)
        count = test.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        logger.info(f"    → {count} lignes")

    # -----------------------------------------------------------------------
    # 2. understat_stats — pas de league_source, join via understat_schedule
    # -----------------------------------------------------------------------
    logger.info("  Copie understat_stats (via join match_id)...")

    # Le INNER JOIN se fait sur silver.understat_schedule, qui est filtrée sur les bons matchs déjà.
    test.execute("""
        CREATE TABLE silver.understat_stats AS
        SELECT s.*
        FROM prod.silver.understat_stats s
        -- On récupère uniquement les match_id de la fixture
        INNER JOIN silver.understat_schedule us
            ON s.match_id = us.match_id
            
    """)
    count = test.execute("SELECT COUNT(*) FROM silver.understat_stats").fetchone()[0]
    logger.info(f"    → {count} lignes")

    # -----------------------------------------------------------------------
    # 3. Seeds — tables de référence (pas de filtre, petites tables)
    # -----------------------------------------------------------------------
    logger.info("  Copie des tables referentiel (seeds)...")
    test.execute("CREATE SCHEMA IF NOT EXISTS referentiel")
    for seed_table in ["competition_mapping", "team_mapping", "transfermarkt_clubs"]:
        test.execute(f"""
            CREATE TABLE referentiel.{seed_table} AS
            SELECT * FROM prod.referentiel.{seed_table}
        """)
        count = test.execute(f"SELECT COUNT(*) FROM referentiel.{seed_table}").fetchone()[0]
        logger.info(f"    → referentiel.{seed_table} : {count} lignes")

    prod.close()
    test.close()

    # Taille finale
    size_mb = DB_TEST.stat().st_size / 1_048_576
    logger.success(f"Base test créée : {DB_TEST} ({size_mb:.1f} Mo)")

    # Vérification de taille — alerte si > 200 Mo (trop lourd pour la CI)
    if size_mb > 200:
        logger.warning(f"Base test volumineuse ({size_mb:.1f} Mo) — envisager de réduire la fixture")
    else:
        logger.info(f"Taille acceptable pour la CI ({size_mb:.1f} Mo < 200 Mo)")


# ---------------------------------------------------------------------------
# Upload GCS
# ---------------------------------------------------------------------------
def upload_to_gcs() -> None:
    import os
    sys.path.insert(0, str(ROOT_DIR / "pipelines"))
    from gcs_utils import get_gcs_client
    

    bucket_name = os.getenv("GCS_BUCKET_NAME", "3etoiles-bronze")
    blob_name   = "ci/football_test.duckdb"

    logger.info(f"Upload {DB_TEST.name} → gs://{bucket_name}/{blob_name}")
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    blob.upload_from_filename(str(DB_TEST))
    logger.success(f"Upload terminé : gs://{bucket_name}/{blob_name}")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    create_test_db()
    upload_to_gcs()