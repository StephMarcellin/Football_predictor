"""ingest_fbref.py — Bronze -> Silver, FBref (schedule/shooting/keeper/misc).
Alimente match_registry. Socle : process_common.
Anciennement process_team_stats.py::process_fbref (scindé, source unique)."""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from process_common import *  # noqa: F401,F403

RESET_TABLES = ["fbref_keeper", "fbref_misc", "fbref_schedule", "fbref_shooting"]


def process_fbref(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/fbref/parquet/{cat}/*.parquet → silver.fbref_{cat}.

    Ordre :
      1. Renommage data-stat → noms canoniques
      2. Normalisation compétition → ajoute league_source canonique + comp_category
         (Big5 / D2 / Cup / Europe / Other) — aucune ligne supprimée
      3. Normalisation équipes → fuzzy Transfermarkt (Big5/D2) puis ADU/CE/Minor Club
      4. Pipeline de validation standard
    """
    prq_root = RAW_DIR / "fbref" / "parquet"
    if not prq_root.exists():
        logger.info("  FBref : dossier Parquet absent, ignoré")
        return

    categories = sorted([d.name for d in prq_root.iterdir() if d.is_dir()])
    logger.info(f"  FBref : {len(categories)} catégorie(s) → {categories}")

    for cat in categories:
        files = sorted((prq_root / cat).glob("*.parquet"))
        if not files:
            continue

        logger.info(f"  ── FBref/{cat} : {len(files)} fichier(s)")
        df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
        logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

        # 1. Renommage (data-stat → noms canoniques)
        rename = {k: v for k, v in FBREF_RENAME.items() if k in df.columns}
        if rename:
            df = df.rename(rename)

        # 2. Normalisation compétition → league_source canonique + comp_category
        #    Aucun filtre — on conserve Coupes, D2 et Europe
        if "league_source" in df.columns:
            df = normalize_competition_col(df, "league_source", f"fbref/{cat}")

        # 3. Normalisation équipes (après filtrage — moins de noms à traiter)
        df = normalize_team_col(df, "team", f"fbref/{cat}", conn=con)
        df = normalize_team_col(df, "opponent", f"fbref/{cat}", conn=con)

        # 4. Pipeline de validation
        df = standardize_date(df)
        df = standardize_season(df)
        df = drop_unused_cols(df)
        # df = apply_cat_c_rejection(df, f"fbref/{cat}")
        df = encode_result_1n2(df)
        # df = cast_numeric_cols(df)
        # df = apply_cat_a_zerofill(df, f"fbref/{cat}")
        # df = apply_cat_d_outliers(df, f"fbref/{cat}")
        df = remove_duplicates(df, ["team", "opponent", "date", "league_source"], f"fbref/{cat}")

        df_home = df.filter(pl.col("venue") == "Home")
        upsert_match_registry(con, df_home, date_col="date", home_col="team", away_col="opponent")

        df_away = df.filter(pl.col("venue") == "Away")
        upsert_match_registry(con, df_away, date_col="date", home_col="opponent", away_col="team")

        df_neutral = df.filter(pl.col("venue") == "Neutral")

        # Convention : home = équipe alphabétiquement première
        upsert_match_registry(
            con,
            df_neutral.with_columns([
                pl.when(pl.col("team") < pl.col("opponent"))
                .then(pl.col("team")).otherwise(pl.col("opponent")).alias("home_team_neutral"),
                pl.when(pl.col("team") < pl.col("opponent"))
                .then(pl.col("opponent")).otherwise(pl.col("team")).alias("away_team_neutral"),
            ]),
            date_col="date",
            home_col="home_team_neutral",
            away_col="away_team_neutral",
        )

        _write_to_duckdb(con, df, f"fbref_{cat}", f"fbref/{cat}")


def main(reset: bool = False) -> None:
    logger.info("=== Ingest FBREF (Bronze -> Silver) ===")
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
        process_fbref(con)
    except Exception as e:
        logger.error(f"  Erreur sur fbref : {e}", exc_info=True)

    _flush_team_mapping(con)
    con.close()
    logger.success("=== Ingest FBREF termine ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FBREF Bronze -> Silver")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    main(reset=args.reset)
