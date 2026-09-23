import duckdb
from loguru import logger
from pathlib import Path

# Chemins des bases
ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
PROD_DB_PATH = ROOT_DIR / "db" / "football.duckdb"
DEV_DB_PATH = ROOT_DIR / "db" / "football_dev.duckdb"
SEASON_TO_KEEP = "2023-2024"               # La saison restreinte pour dev/test

def create_dev_database():
    logger.info("=== Création de la base de test réduite (DEV) ===")
    
    con = duckdb.connect(PROD_DB_PATH)
    con.execute(f"ATTACH '{DEV_DB_PATH}' AS dev_db;")
    
    # 1. Copie intégrale du schéma REFERENTIEL
    con.execute("CREATE SCHEMA IF NOT EXISTS dev_db.referentiel;")
    ref_tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'referentiel'
    """).fetchall()
    
    logger.info(f"Tables référentiel trouvées : {len(ref_tables)}")
    for (table,) in ref_tables:
        con.execute(f"DROP TABLE IF EXISTS dev_db.referentiel.{table};")
        con.execute(f"""
            CREATE TABLE dev_db.referentiel.{table} AS 
            SELECT * FROM referentiel.{table};
        """)
        count = con.execute(f"SELECT COUNT(*) FROM dev_db.referentiel.{table}").fetchone()[0]
        logger.success(f"  -> Table dev_db.referentiel.{table} copiée ({count:,} lignes)")

    # 2. Copie filtrée du schéma SILVER
    con.execute("CREATE SCHEMA IF NOT EXISTS dev_db.silver;")
    silver_tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'silver'
    """).fetchall()
    
    logger.info(f"Tables silver trouvées : {len(silver_tables)}")
    for (table,) in silver_tables:
        con.execute(f"DROP TABLE IF EXISTS dev_db.silver.{table};")
        
        columns = [c[1] for c in con.execute(f"PRAGMA table_info('silver.{table}')").fetchall()]
        season_col = next((c for c in columns if c.lower() in ['season', 'saison']), None)

        print(season_col)
        
        if season_col:
            query = f"""
                CREATE TABLE dev_db.silver.{table} AS 
                SELECT * FROM silver.{table} 
                WHERE {season_col} = '{SEASON_TO_KEEP}';
            """
        else:
            query = f"""
                CREATE TABLE dev_db.silver.{table} AS 
                SELECT * FROM silver.{table};
            """

        print(query)    
        con.execute(query)
        count = con.execute(f"SELECT COUNT(*) FROM dev_db.silver.{table}").fetchone()[0]
        logger.success(f"  -> Table dev_db.silver.{table} créée ({count:,} lignes)")

    con.close()
    logger.success("=== Base DEV prête avec referentiel + silver ! ===")

if __name__ == "__main__":
    create_dev_database()