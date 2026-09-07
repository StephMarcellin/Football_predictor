"""process_team_stats.py — Bronze -> Silver, STATS (non-events) : FBref,
Understat, WhoScored team-season. Alimente match_registry. Socle : process_common."""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from process_common import *  # noqa: F401,F403

def process_fbref(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/fbref/parquet/{cat}/*.parquet → silver.fbref_{cat}.

    Ordre :
      1. Renommage data-stat → noms canoniques
      2. Normalisation compétition → ajoute league_source canonique + comp_category
         (Big5 / D2 / Cup / Europe / Other) — aucune ligne supprimée
      3. Normalisation équipes → Minor Club si absent du mapping
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
        df = normalize_team_col(df, "team", f"fbref/{cat}", conn = con)
        df = normalize_team_col(df, "opponent", f"fbref/{cat}", conn = con)

        # 4. Pipeline de validation
        df = standardize_date(df)
        df = standardize_season(df)
        df = drop_unused_cols(df)
        df = apply_cat_c_rejection(df, f"fbref/{cat}")
        df = encode_result_1n2(df)
        df = cast_numeric_cols(df)
        df = apply_cat_a_zerofill(df, f"fbref/{cat}")
        df = apply_cat_d_outliers(df, f"fbref/{cat}")
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
        df = normalize_team_col(df, "home_team", f"understat/{subtype}", conn = con)
        df = normalize_team_col(df, "away_team", f"understat/{subtype}", conn = con)

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

def process_whoscored_team_season(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/whoscored/parquet/*.parquet → silver.whoscored_team_season
    Grain : 1 ligne = 1 équipe × saison (stats agrégées).
    """
    prq_root = RAW_DIR / "whoscored" / "parquet"
    if not prq_root.exists():
        logger.info("  WhoScored : dossier Parquet absent, ignoré")
        return

    files = sorted(prq_root.glob("*.parquet"))
    if not files:
        logger.info("  WhoScored : aucun Parquet trouvé")
        return

    logger.info(f"  WhoScored : {len(files)} fichier(s) → grain team-season")
    df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
    logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

    # Normalisation compétition
    if "league_source" in df.columns:
        df = normalize_competition_col(df, "league_source", "whoscored")

    # Normalisation équipes
    df = normalize_team_col(df, "team", "whoscored", conn = con)

    # Standardisation saison
    df = standardize_season(df)

    # Cat C
    df = df.drop_nulls(subset=[c for c in ["team", "season", "league_source"] if c in df.columns])

    # Cast ws_* → Float64
    ws_cols = [c for c in df.columns if c.startswith("ws_")]
    if ws_cols:
        df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ws_cols])

    df = apply_cat_a_zerofill(df, "whoscored")
    df = apply_cat_d_outliers(df, "whoscored")
    df = remove_duplicates(df, ["team", "season", "league_source"], "whoscored")

    

    _write_to_duckdb(con, df, "whoscored_team_season", "whoscored")



SOURCE_PROCESSORS = {
    "fbref": process_fbref,
    "understat": process_understat,
    "whoscored_team_season": process_whoscored_team_season,
}
RESET_TABLES = ["fbref_keeper","fbref_misc","fbref_schedule","fbref_shooting",
                "understat_schedule","understat_stats","whoscored_team_season"]


def main(source: str = None, reset: bool = False) -> None:
    logger.info("=== Process TEAM STATS (Bronze -> Silver) ===")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate")
    _init_team_mapping(con); _init_team_mapping_ids(con)
    _init_competition_mapping(con); _init_transfermarkt(con); _init_match_registry(con)
    if reset:
        for t in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS silver.{t}")
        logger.info(f"  {len(RESET_TABLES)} table(s) supprimee(s) (--reset)")
    sources = [source] if source else list(SOURCE_PROCESSORS.keys())
    for src in sources:
        logger.info(f"-- Source : {src} --")
        try:
            SOURCE_PROCESSORS[src](con)
        except Exception as e:
            logger.error(f"  Erreur sur {src} : {e}", exc_info=True)
    con.close()
    logger.success("=== Process TEAM STATS termine ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process TEAM STATS Bronze -> Silver")
    parser.add_argument("--source", default=None, choices=list(SOURCE_PROCESSORS.keys()))
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    main(source=args.source, reset=args.reset)
