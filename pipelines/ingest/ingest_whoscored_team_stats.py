"""ingest_whoscored_team_stats.py — Bronze -> Silver, WhoScored (grain
équipe-saison). Socle : process_common.
Anciennement process_team_stats.py::process_whoscored_team_season (scindé,
source unique)."""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from process_common import *  # noqa: F401,F403

RESET_TABLES = ["whoscored_team_season"]


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
    df = normalize_team_col(df, "team", "whoscored", conn=con)

    # Standardisation saison
    df = standardize_season(df)

    # Cat C
    # df = df.drop_nulls(subset=[c for c in ["team", "season", "league_source"] if c in df.columns])

    # Cast ws_* → Float64
    # ws_cols = [c for c in df.columns if c.startswith("ws_")]
    # if ws_cols:
    #     df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ws_cols])

    # df = apply_cat_a_zerofill(df, "whoscored")
    # df = apply_cat_d_outliers(df, "whoscored")
    df = remove_duplicates(df, ["team", "season", "league_source"], "whoscored")

    _write_to_duckdb(con, df, "whoscored_team_season", "whoscored")


def main(reset: bool = False) -> None:
    logger.info("=== Ingest WHOSCORED TEAM STATS (Bronze -> Silver) ===")
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
        process_whoscored_team_season(con)
    except Exception as e:
        logger.error(f"  Erreur sur whoscored_team_stats : {e}", exc_info=True)

    _flush_team_mapping(con)
    con.close()
    logger.success("=== Ingest WHOSCORED TEAM STATS termine ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest WHOSCORED TEAM STATS Bronze -> Silver")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    main(reset=args.reset)
