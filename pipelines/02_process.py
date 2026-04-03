"""
Pipeline 02 — Process (Parquet FBref → DuckDB consolide.*)
============================================================
Lit les fichiers Parquet produits par 01_ingest.py,
nettoie et normalise, puis écrit dans DuckDB schéma consolide.*.

Différences avec l'ancien pipeline CSV :
  - Source : Parquet au lieu de DuckDB brut.*
  - Les noms data-stat FBref sont déjà propres → pas de mapping L0/L1
  - Seul un mapping de standardisation léger est appliqué
    (ex: goals_for → gf, start_time → time)
  - Les types sont déjà corrects depuis le Parquet → typage minimal

Usage :
    python pipelines/02_process.py
    python pipelines/02_process.py --category shooting
    python pipelines/02_process.py --reset   # recrée toutes les tables consolide
"""

import sys
import argparse
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
import yaml
from config_columns import FLOAT_SUFFIXES, META_COLS

# ── Config ────────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH  = CFG["paths"]["db"]
PRQ_DIR  = Path(CFG["paths"]["raw_data"]) / "fbref" / "parquet"

# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/process.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# ── Mapping de standardisation ────────────────────────────────────────────────
# Les noms data-stat FBref sont déjà sémantiques.
# Ce mapping ne fait que standardiser quelques noms pour cohérence
# avec le reste du pipeline (features, modèle).
# Format : {nom_data_stat: nom_canonique}

RENAME_MAP = {
    # Colonnes meta communes
    "goals_for":    "gf",
    "goals_against":"ga",
    "start_time":   "time",
    "dayofweek":    "day",
    "comp":         "league_source",
    # Shooting
    "goals":                    "standard_gls",
    "shots":                    "standard_sh",
    "shots_on_target":          "standard_sot",
    "shots_on_target_pct":      "standard_sot_pct",
    "goals_per_shot":           "standard_g_sh",
    "goals_per_shot_on_target": "standard_g_sot",
    "pens_made":                "standard_pk",
    "pens_att":                 "standard_pkatt",
    # Keeper
    "gk_shots_on_target_against": "sota",
    "gk_goals_against":           "ga_keeper",
    "gk_saves":                   "saves",
    "gk_save_pct":                "save_pct",
    "gk_clean_sheets":            "cs",
    "gk_pens_att":                "pk_att",
    "gk_pens_allowed":            "pk_allowed",
    "gk_pens_saved":              "pk_saved",
    "gk_pens_missed":             "pk_missed",
    # Misc
    "cards_yellow":     "crdy",
    "cards_red":        "crdr",
    "cards_yellow_red": "crdy2",
    "fouls":            "fls",
    "fouled":           "fld",
    "offsides":         "off",
    "interceptions":    "int",
    "tackles_won":      "tklw",
    "pens_won":         "pkwon",
    "pens_conceded":    "pkcon",
    "own_goals":        "og",
    # Schedule
    "possession":   "poss",
    # Colonnes à supprimer (pas utiles pour le ML)
    # "match_report", "notes" → gardés pour traçabilité
}

# Colonnes à supprimer complètement
COLS_TO_DROP = {"stat_category", "source"}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def is_float_col(col: str) -> bool:
    return any(col.endswith(s) or s in col for s in FLOAT_SUFFIXES)


# ── Étape 1 : Lecture des Parquet ─────────────────────────────────────────────

def load_parquet(category: str) -> pd.DataFrame:
    """
    Lit tous les fichiers Parquet d'une catégorie et les concatène.
    PRQ_DIR/{category}/*.parquet
    """
    cat_dir = PRQ_DIR / category
    if not cat_dir.exists():
        logger.warning(f"  Dossier absent : {cat_dir}")
        return pd.DataFrame()

    files = sorted(cat_dir.glob("*.parquet"))
    if not files:
        logger.warning(f"  Aucun Parquet dans : {cat_dir}")
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df  = pd.concat(dfs, ignore_index=True)
    logger.info(f"  {category} : {len(files)} fichiers → {len(df):,} lignes")
    return df


# ── Étape 2 : Renommage de standardisation ────────────────────────────────────

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Applique RENAME_MAP sur les colonnes présentes."""
    rename = {k: v for k, v in RENAME_MAP.items() if k in df.columns}
    return df.rename(columns=rename)


# ── Étape 3 : Nettoyage meta ──────────────────────────────────────────────────

def clean_meta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyages sur les colonnes meta communes.
    La date est déjà parsée par 01_ingest — on s'assure juste du type.
    """
    # Date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()

    # Résultat W/D/L → H/D/A (du point de vue de l'équipe)
    if "result" in df.columns and "venue" in df.columns:
        def encode_result(row):
            r = str(row["result"]).strip().upper()
            v = str(row["venue"]).strip().lower()
            if r == "W": return "H" if v == "home" else "A"
            if r == "L": return "A" if v == "home" else "H"
            if r == "D": return "D"
            return None
        df["result_1n2"] = df.apply(encode_result, axis=1)

    # GF/GA : déjà propres depuis data-stat, mais on s'assure du type Int64
    for col in ["gf", "ga"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Normaliser team et opponent en minuscules
    for col in ["team", "opponent"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.lower()

    # Normaliser league_source
    if "league_source" in df.columns:
        df["league_source"] = df["league_source"].str.strip()

    # Supprimer les colonnes internes d'ingestion
    drop = [c for c in COLS_TO_DROP if c in df.columns]
    if drop:
        df = df.drop(columns=drop)

    return df


# ── Étape 4 : Typage des colonnes stats ───────────────────────────────────────

def cast_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Type les colonnes stats en float ou Int64.
    Les colonnes meta (identifiées par META_COLS) sont ignorées.
    """
    for col in df.columns:
        if col in META_COLS or col == "result_1n2":
            continue
        if df[col].dtype == object:
            if is_float_col(col):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


# ── Étape 5 : Valeurs manquantes ──────────────────────────────────────────────

def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Comptages → 0
    Pourcentages/distances → médiane par league + season
    """
    for col in df.columns:
        if col in META_COLS or col == "result_1n2":
            continue
        if df[col].isna().any():
            if is_float_col(col):
                # Médiane par league + season pour les ratios
                if "league_source" in df.columns and "season" in df.columns:
                    df[col] = df.groupby(["league_source", "season"])[col].transform(
                        lambda x: x.fillna(x.median())
                    )
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna(0)
    return df


# ── Étape 6 : Doublons ────────────────────────────────────────────────────────

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [c for c in ["team", "opponent", "date", "league_source"] if c in df.columns]
    if len(key_cols) < 3:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="first")
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"    {removed} doublons supprimés")
    return df


# ── Traitement d'une catégorie ────────────────────────────────────────────────

def process_category(con: duckdb.DuckDBPyConnection, category: str) -> bool:
    logger.info(f"── Traitement : {category} ──────────────────────────────────")

    df = load_parquet(category)
    if df.empty:
        return False

    df = rename_columns(df)
    df = clean_meta(df)
    df = cast_stats(df)
    df = handle_missing(df)
    df = remove_duplicates(df)

    nan_total = df.isnull().sum().sum()
    logger.info(f"    Lignes : {len(df):,} | Colonnes : {len(df.columns)} | NaN : {nan_total:,}")

    con.execute(f"DROP TABLE IF EXISTS consolide.{category}")
    con.execute(f"CREATE TABLE consolide.{category} AS SELECT * FROM df")
    n = con.execute(f"SELECT COUNT(*) FROM consolide.{category}").fetchone()[0]
    logger.success(f"    consolide.{category} : {n:,} lignes ✅")
    return True


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Process Parquet → DuckDB consolide.*")
    parser.add_argument("--category", default=None,
                        help="Traiter une seule catégorie (ex: shooting)")
    parser.add_argument("--reset", action="store_true",
                        help="Supprime toutes les tables consolide.* avant de recharger")
    args = parser.parse_args()

    logger.info("=== Démarrage process ===")
    logger.info(f"  Source Parquet : {PRQ_DIR}")

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS consolide")

    if args.reset:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'consolide'"
        ).fetchall()]
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS consolide.{t}")
        logger.info(f"  {len(tables)} tables consolide.* supprimées (--reset)")

    # Catégories = sous-dossiers Parquet présents
    if args.category:
        categories = [args.category]
    else:
        if not PRQ_DIR.exists():
            logger.error(f"Dossier Parquet introuvable : {PRQ_DIR}")
            logger.info("Lance d'abord 01_ingest.py")
            raise SystemExit(1)
        categories = sorted([d.name for d in PRQ_DIR.iterdir() if d.is_dir()])
        logger.info(f"  Catégories trouvées : {categories}")

    for cat in categories:
        try:
            process_category(con, cat)
        except Exception as e:
            logger.error(f"  Erreur sur {cat} : {e}")

    # Résumé final
    logger.info("── Résumé consolide ────────────────────────────────────")
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'consolide' ORDER BY 1"
    ).fetchall()]
    for t in tables:
        n = con.execute(f"SELECT COUNT(*) FROM consolide.{t}").fetchone()[0]
        logger.info(f"  consolide.{t:<25} {n:>7,} lignes")

    con.close()
    logger.success("=== Process terminé ===")


if __name__ == "__main__":
    main()