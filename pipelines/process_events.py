"""process_events.py — Bronze -> Silver, EVENTS WhoScored : normalise
stg_whoscored_match_index et enregistre les matchs dans match_registry.
Socle : process_common."""

from process_common import *  # noqa: F401,F403

def process_whoscored_match_index(con: duckdb.DuckDBPyConnection) -> None:
    """Normalise silver.stg_whoscored_match_index (ecrit par le scraper events)
    et enregistre les matchs WhoScored dans match_registry (grain events)."""
    # ── Normalisation stg_whoscored_match_index ────────────────────────────────
    try:
        n = con.execute("SELECT COUNT(*) FROM silver.stg_whoscored_match_index").fetchone()[0]
        if n == 0:
            logger.info("  WhoScored match index : table vide, ignorée")
        else:
            logger.info(f"  WhoScored match index : normalisation de {n:,} lignes")
            df_idx = con.execute("SELECT * FROM silver.stg_whoscored_match_index").pl()

            # Normalisation compétition
            df_idx = normalize_competition_col(df_idx, "league_source", "whoscored_match_index")

            # Normalisation équipes
            df_idx = normalize_team_col(df_idx, "home_team_name", "whoscored_match_index", conn=con)
            df_idx = normalize_team_col(df_idx, "away_team_name", "whoscored_match_index", conn=con)

            # Standardisation saison
            df_idx = standardize_season(df_idx)

            upsert_match_registry(con, df_idx, date_col="match_date", home_col="home_team_name", away_col="away_team_name")

            # Réécriture
            _write_to_duckdb(con, df_idx, "stg_whoscored_match_index", "whoscored_match_index")

    except Exception as e:
        logger.warning(f"  stg_whoscored_match_index absent ou erreur : {e}")


SOURCE_PROCESSORS = {"whoscored_events": process_whoscored_match_index}


def main(source: str = None) -> None:
    logger.info("=== Process EVENTS (Bronze -> Silver) ===")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate")
    _init_team_mapping(con); _init_team_mapping_ids(con)
    _init_competition_mapping(con); _init_transfermarkt(con); _init_match_registry(con)
    for src in list(SOURCE_PROCESSORS.keys()):
        logger.info(f"-- Source : {src} --")
        try:
            SOURCE_PROCESSORS[src](con)
        except Exception as e:
            logger.error(f"  Erreur sur {src} : {e}", exc_info=True)
    con.close()
    logger.success("=== Process EVENTS termine ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process EVENTS Bronze -> Silver")
    args = parser.parse_args()
    main()
