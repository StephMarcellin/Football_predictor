#!/usr/bin/env python3
"""
check_spark_outputs.py — Porte d'entrée entre Spark et dbt
===========================================================
Vérifie que les sorties Parquet du job Spark existent, sont lisibles et non
vides, AVANT que dbt ne construise les vues qui les exposent.

Pourquoi cette étape : une vue DuckDB ne valide rien. Sur un Parquet absent
ou vide, `CREATE VIEW` réussit — et l'erreur ne surgit qu'au premier modèle
aval, sous une forme qui ne désigne pas la vraie cause. Ce script transforme
une panne silencieuse en échec explicite, au bon endroit.

Étape `critical=True` dans Prefect : elle bloque le pipeline.

Usage :
    python pipelines/spark/check_spark_outputs.py
    python pipelines/spark/check_spark_outputs.py --min-rows 1000
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import yaml
from dotenv import load_dotenv
from loguru import logger

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
load_dotenv(ROOT_DIR / ".env")

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

# Les trois tables produites par spark_events.py, dans l'ordre de la chaîne.
SPARK_TABLES = ["int_whoscored_events", "events_qual", "int_event_enriched"]


def resolve_out_dir() -> Path:
    """
    Résout le dossier de sortie Spark, et vérifie que .env et config.yaml
    désignent bien le même endroit.

    Les modèles dbt lisent SPARK_OUT_DIR (variable d'environnement), le job
    Spark écrit dans config.yaml → spark.paths.output. Si les deux divergent,
    dbt construirait des vues sur un dossier que Spark n'alimente pas — et
    tout semblerait fonctionner jusqu'à ce que les chiffres soient faux.
    """
    from_cfg = (ROOT_DIR / CFG["spark"]["paths"]["output"]).resolve()

    from_env = os.environ.get("SPARK_OUT_DIR")
    if not from_env:
        raise RuntimeError(
            "SPARK_OUT_DIR absente de l'environnement.\n"
            "Ajoute dans .env :\n"
            f"    SPARK_OUT_DIR={from_cfg.as_posix()}"
        )

    if Path(from_env).resolve() != from_cfg:
        raise RuntimeError(
            f"Incohérence de configuration :\n"
            f"    .env        SPARK_OUT_DIR = {Path(from_env).resolve()}\n"
            f"    config.yaml spark.paths.output = {from_cfg}\n"
            f"dbt lirait un dossier que Spark n'alimente pas."
        )

    return from_cfg


def check_table(con, out_dir: Path, name: str, min_rows: int) -> dict:
    """
    Contrôle une sortie Spark : présence, lisibilité, volume, partitions.

    Le COUNT(*) sur du Parquet est peu coûteux : DuckDB lit les métadonnées
    des row groups plutôt que les données elles-mêmes.

    Returns:
        dict avec les métriques, pour le récapitulatif final.
    """
    path = out_dir / name
    if not path.exists():
        raise FileNotFoundError(
            f"Sortie Spark absente : {path}\n"
            f"Lance d'abord : python pipelines/spark/spark_events.py"
        )

    files = list(path.rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"{path} existe mais ne contient aucun .parquet")

    glob = f"{path.as_posix()}/**/*.parquet"

    # La lecture elle-même valide que les fichiers ne sont pas corrompus :
    # un Parquet tronqué lève ici, pas trois modèles plus loin.
    n_rows, n_seasons = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT season)
        FROM read_parquet('{glob}', hive_partitioning = 1)
    """).fetchone()

    n_cols = len(con.execute(f"""
        SELECT * FROM read_parquet('{glob}', hive_partitioning = 1) LIMIT 0
    """).description)

    total_mo = sum(f.stat().st_size for f in files) / 1e6

    if n_rows < min_rows:
        raise RuntimeError(
            f"{name} : {n_rows:,} lignes, seuil minimum {min_rows:,}.\n"
            f"Le job Spark a probablement échoué ou tourné sur un périmètre vide."
        )

    logger.success(
        f"{name:<22} {n_rows:>12,} lignes  {n_cols:>3} col  "
        f"{n_seasons} saison(s)  {len(files):>3} fichiers  {total_mo:>7,.0f} Mo"
    )
    return {"table": name, "rows": n_rows, "cols": n_cols,
            "seasons": n_seasons, "files": len(files), "mo": total_mo}


def warn_small_files(results: list[dict]) -> None:
    """
    Signale le small files problem.

    Spark répartit le travail par fichier. Des fichiers de quelques Mo font
    que le coût d'ouverture dépasse le coût de lecture. Cible usuelle :
    100–200 Mo par fichier.
    """
    for r in results:
        if r["files"] > 1 and r["mo"] / r["files"] < 20:
            logger.warning(
                f"{r['table']} : {r['files']} fichiers de "
                f"{r['mo'] / r['files']:,.0f} Mo en moyenne — "
                f"baisse spark.shuffle_partitions dans config.yaml"
            )


def main(min_rows: int = 1) -> int:
    out_dir = resolve_out_dir()
    logger.info(f"Sorties Spark : {out_dir}")

    # Connexion en mémoire : on ne lit que des fichiers Parquet, aucune raison
    # d'ouvrir la base du projet ni de prendre un verrou dessus.
    con = duckdb.connect(":memory:")
    try:
        results = [check_table(con, out_dir, t, min_rows) for t in SPARK_TABLES]
    finally:
        con.close()

    warn_small_files(results)
    logger.success("Sorties Spark valides — dbt peut construire les vues.")
    return 0


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/check_spark_outputs.log", level="DEBUG", rotation="5 MB",
               retention=10, encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--min-rows", type=int, default=1,
                    help="échoue si une sortie a moins de N lignes")
    args = ap.parse_args()

    sys.exit(main(min_rows=args.min_rows))