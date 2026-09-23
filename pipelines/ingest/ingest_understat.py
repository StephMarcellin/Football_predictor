"""ingest_understat.py — Bronze -> Silver, Understat (schedule/stats).
Alimente match_registry. Socle : process_common.
Anciennement process_team_stats.py::process_understat (scindé, source unique).

Dépendance d'ordre : nécessite silver.fbref_schedule déjà écrite (jointure de
date) → ce script doit tourner APRÈS ingest_fbref.py."""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from process_common import *  # noqa: F401,F403

RESET_TABLES = ["understat_schedule", "understat_stats"]


def process_understat(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/understat/parquet/{schedule,stats}/*.parquet
    → silver.understat_schedule + silver.understat_stats
    """
    prq_root = RAW_DIR / "understat" / "parquet"
    if not prq_root.exists():
        logger.info("  Understat : dossier Parquet absent, ignoré")
        return

    subtypes = sorted([d.name for d in prq_root.iterdir() if d.is_dir()])
    logger.info(f"  Understat : {subtypes}")

    for subtype in subtypes:
        files = sorted((prq_root / subtype).glob("*.parquet"))
        if not files:
            continue

        logger.info(f"  ── Understat/{subtype} : {len(files)} fichier(s)")
        df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
        logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

        # Normalisation compétition (colonne league_source)
        if "league_source" in df.columns:
            df = normalize_competition_col(df, "league_source", f"understat/{subtype}")

        # Normalisation équipes
        df = normalize_team_col(df, "home_team", f"understat/{subtype}", conn=con)
        df = normalize_team_col(df, "away_team", f"understat/{subtype}", conn=con)

        # Standardisation saison : int 2021 → "2021-2022"
        df = standardize_season(df)

        # Standardisation date (Understat : "2017-08-11 19:45:00")
        df = standardize_date(df)

        df = drop_unused_cols(df)
        df = apply_cat_c_rejection(df, f"understat/{subtype}")
        df = cast_numeric_cols(df)
        df = apply_cat_a_zerofill(df, f"understat/{subtype}")
        df = apply_cat_d_outliers(df, f"understat/{subtype}")
        df = remove_duplicates(df, ["match_id", "home_team", "away_team"], f"understat/{subtype}")

        if all(c in df.columns for c in ["home_team", "away_team", "league_source", "season"]):
            # Récupérer la date depuis fbref_schedule
            try:
                fbref_dates = con.execute("""
                    SELECT DISTINCT team as home_team, opponent as away_team,
                        league_source, season, date
                    FROM silver.fbref_schedule
                    WHERE venue = 'Home'
                """).pl()

                df_with_date = df.join(
                    fbref_dates,
                    on=["home_team", "away_team", "league_source", "season"],
                    how="left"
                )

                df_with_date = df_with_date.filter(pl.col("date").is_not_null())

                upsert_match_registry(
                    con, df_with_date,
                    date_col="date",
                    home_col="home_team",
                    away_col="away_team",
                )
            except Exception as e:
                logger.warning(f"  upsert_match_registry understat : {e}")

        _write_to_duckdb(con, df, f"understat_{subtype}", f"understat/{subtype}")


def main(reset: bool = False) -> None:
    logger.info("=== Ingest UNDERSTAT (Bronze -> Silver) ===")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate")

    _bootstrap_team_mapping(con)
    _init_team_mapping(con)
    _init_team_mapping_ids(con)
    _init_competition_mapping(con)
    _init_transfermarkt(con)
    _init_match_registry(con)

    if reset:
        for t in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS silver.{t}")
        logger.info(f"  {len(RESET_TABLES)} table(s) supprimee(s) (--reset)")

    try:
        process_understat(con)
    except Exception as e:
        logger.error(f"  Erreur sur understat : {e}", exc_info=True)

    _flush_team_mapping(con)
    con.close()
    logger.success("=== Ingest UNDERSTAT termine ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest UNDERSTAT Bronze -> Silver")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    main(reset=args.reset)
