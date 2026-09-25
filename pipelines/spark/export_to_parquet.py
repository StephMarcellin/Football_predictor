#!/usr/bin/env python3
"""
export_to_parquet.py — Frontière DuckDB → Spark
================================================
Exporte les deux tables source du job Spark vers du Parquet zstd.

Pourquoi cette étape existe : DuckDB n'expose pas de lecture parallèle
exploitable par Spark (moteur mono-processus, écrivain unique). Parquet est
le format d'échange — colonnaire, compressé, splittable, auto-descriptif.

PRINCIPE : cet export ne fait AUCUNE transformation.
    DuckDB stocke, Spark transforme.
La jointure d'identité (ws_match_id → match_id) et le remapping des team_id
sont faits dans spark_events.py, en broadcast join. C'est la décision
d'architecture : si l'export joignait, DuckDB aurait déjà fait le travail
qu'on cherche à lui retirer.

Ce que l'export produit (data/spark_in/) :
    events/season=<saison>/*.parquet   silver.stg_whoscored_events, brut
    match_index/match_index.parquet    intermediate.int_whoscored_match_index

Le partitionnement par saison est possible sans jointure : stg_whoscored_events
porte déjà season, league_source et scraped_at.

Prérequis d'exécution :
    run_scrapping (load_archive)  →  run_ingest  →  dbt run --select int_whoscored_match_index

Usage :
    python pipelines/spark/export_to_parquet.py --inventory
    python pipelines/spark/export_to_parquet.py --season 2023-2024
    python pipelines/spark/export_to_parquet.py --clean --strict
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import duckdb
import yaml
from loguru import logger

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]
IN_DIR  = ROOT_DIR / CFG["spark"]["paths"]["input"]

# Tables dont dépend l'export. Vérifiées avant toute écriture.
REQUIRED = [
    ("silver",       "stg_whoscored_events"),
    ("intermediate", "int_whoscored_match_index"),
]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Inventaire et garde-fous
#
# Mesurer avant d'agir, et échouer tôt avec un message qui dit quoi faire.
# ══════════════════════════════════════════════════════════════════════════════

def inventory(con) -> None:
    """
    Tables de la base, triées par taille décroissante.

    duckdb_tables() est une fonction système. `estimated_size` vient des
    statistiques maintenues à l'écriture : instantané, là où un COUNT(*)
    sur des dizaines de millions de lignes coûterait des minutes.
    NULLS LAST : une table jamais analysée a un estimated_size NULL et
    remonterait en tête du tri sans ce modificateur.
    """
    rows = con.execute("""
        SELECT schema_name, table_name, estimated_size, column_count
        FROM duckdb_tables()
        ORDER BY estimated_size DESC NULLS LAST
        LIMIT 30
    """).fetchall()

    if not rows:
        logger.warning("Aucune table dans la base.")
        return

    logger.info(f"{'SCHEMA':<16}{'TABLE':<38}{'LIGNES (est.)':>16}{'COLS':>6}")
    logger.info("-" * 76)
    for schema, table, size, ncols in rows:
        size_txt = f"{size:,}" if size is not None else "?"
        logger.info(f"{schema:<16}{table:<38}{size_txt:>16}{ncols:>6}")


def check_required(con) -> None:
    """
    Échoue si une table source manque, plutôt que de laisser une erreur SQL
    obscure survenir au milieu de l'export.

    Le set de tuples rend le test d'appartenance en O(1) — réflexe à garder
    dès qu'on teste l'appartenance dans une boucle.
    """
    existing = {
        (s, t) for s, t in con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables()"
        ).fetchall()
    }
    missing = [f"{s}.{t}" for s, t in REQUIRED if (s, t) not in existing]
    if missing:
        raise RuntimeError(
            "Tables source absentes : " + ", ".join(missing) +
            "\nOrdre requis : run_scrapping (load_archive) → run_ingest → "
            "dbt run --select int_whoscored_match_index"
        )


def report_index_grain(con, strict: bool = False) -> None:
    """
    Mesure les doublons de ws_match_id dans l'index et en chiffre l'impact.

    LE concept : le GRAIN d'une table, c'est ce à quoi correspond une ligne.
    Celui de l'index est « un match ». S'il est violé, la jointure multiplie :
    joindre A à B sur une clé présente k fois dans B produit k lignes par
    ligne de A. Un match dupliqué = tous ses événements dupliqués, sans
    aucune erreur levée. Des chiffres faux, silencieusement.

    N'échoue PAS par défaut : ce test est en severity=warn dans schema.yml,
    les doublons historiques (cascade A-003) sont connus et le pipeline dbt
    actuel vit avec. Bloquer d'office serait un durcissement non demandé.
    --strict permet d'exiger la garantie avant une campagne de validation.
    """
    total, distinct = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT str_ws_match_id)
        FROM intermediate.int_whoscored_match_index
    """).fetchone()

    if total == distinct:
        logger.success(f"Grain de l'index : {total:,} ws_match_id uniques.")
        return

    n_dupes = total - distinct

    # Requête coûteuse (GROUP BY complet) : payée seulement en cas de problème.
    impact = con.execute("""
        SELECT COALESCE(SUM(n_events), 0)
        FROM (
            SELECT COUNT(*) AS n_events
            FROM silver.stg_whoscored_events e
            JOIN (SELECT str_ws_match_id
                  FROM intermediate.int_whoscored_match_index
                  GROUP BY str_ws_match_id HAVING COUNT(*) > 1) d
              ON e.ws_match_id = d.str_ws_match_id
            GROUP BY e.ws_match_id
        )
    """).fetchone()[0]

    msg = (
        f"int_whoscored_match_index : {total:,} lignes pour {distinct:,} "
        f"ws_match_id distincts → {n_dupes:,} doublons.\n"
        f"Impact : {impact:,} lignes d'événements seraient dupliquées "
        f"à la jointure Spark."
    )
    if strict:
        raise RuntimeError(msg + "\n--strict activé : export interrompu.")
    logger.warning(msg)
    logger.warning("Export poursuivi (parité avec le pipeline dbt actuel).")


def report_match_id_coverage(con) -> None:
    """
    Taux de résolution de l'index.

    Le job Spark fait un INNER JOIN : un match sans match_id résolu verra tous
    ses événements écartés. On chiffre ici pour ne pas découvrir l'écart en
    comparant les comptages de validation.
    """
    total, resolus, teams_ok = con.execute("""
        SELECT COUNT(*),
               COUNT(str_match_id),
               COUNT(*) FILTER (WHERE str_team_id IS NOT NULL
                                  AND str_opponent_id IS NOT NULL)
        FROM intermediate.int_whoscored_match_index
    """).fetchone()
    logger.info(
        f"Index : {total:,} matchs — "
        f"match_id résolu {resolus:,} ({resolus / total:.1%}), "
        f"équipes résolues {teams_ok:,} ({teams_ok / total:.1%})"
    )


def known_seasons(con) -> list[str]:
    """Saisons réellement présentes dans l'index, triées."""
    return [r[0] for r in con.execute("""
        SELECT DISTINCT str_season
        FROM intermediate.int_whoscored_match_index
        WHERE str_season IS NOT NULL
        ORDER BY str_season
    """).fetchall()]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Export
#
# Aucune transformation. Deux COPY, c'est tout.
# ══════════════════════════════════════════════════════════════════════════════

def export_events(con, season: str | None) -> None:
    """
    Écrit data/spark_in/events/season=<saison>/*.parquet

    Options du COPY :
      COPY (requête) TO   écrit le résultat d'une requête, pas d'une table
      FORMAT PARQUET      sinon DuckDB écrirait du CSV
      COMPRESSION zstd    meilleur ratio que snappy — on n'a qu'un seul disque
      PARTITION_BY season arborescence hive `season=…/` ; la colonne n'est PLUS
                          stockée dans les fichiers, elle est portée par le nom
                          de dossier et reconstruite par Spark à la lecture
      OVERWRITE_OR_IGNORE rejouer l'export ne lève pas d'erreur (idempotence)

    as_posix() : les backslashes Windows dans une chaîne SQL seraient lus comme
    des séquences d'échappement. Même contrainte que le file:/// de MLflow.

    La saison est interpolée dans le SQL (COPY n'accepte pas de paramètre lié),
    donc elle est validée en amont contre la liste fermée des saisons de la base.
    """
    out = IN_DIR / "events"
    out.mkdir(parents=True, exist_ok=True)

    where = f"WHERE season = '{season}'" if season else ""

    logger.info(f"Export events{' (saison ' + season + ')' if season else ''} → {out}")
    t0 = time.time()
    con.execute(f"""
        COPY (SELECT * FROM silver.stg_whoscored_events {where})
        TO '{out.as_posix()}'
        (FORMAT PARQUET, COMPRESSION zstd, PARTITION_BY (season),
         OVERWRITE_OR_IGNORE true)
    """)
    logger.success(f"events écrit en {time.time() - t0:,.0f}s")

def export_team_mapping(con) -> None:
    """
    Écrit data/spark_in/team_mapping/team_mapping.parquet

    Petite table : un seul fichier, non partitionné. Spark la chargera
    intégralement et la diffusera à toutes ses tâches (broadcast join),
    ce qui évite tout shuffle sur la jointure d'identité.
    """
    out = IN_DIR / "team_mapping"
    out.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM referentiel.team_mapping)
        TO '{(out / "team_mapping.parquet").as_posix()}'
        (FORMAT PARQUET, COMPRESSION zstd)
    """)
    n = con.execute(
        "SELECT COUNT(*) FROM referentiel.team_mapping"
    ).fetchone()[0]
    logger.success(f"team_mapping écrit : {n:,} équipes → {out}")


def export_match_index(con) -> None:
    """
    Écrit data/spark_in/match_index/match_index.parquet

    Petite table : un seul fichier, non partitionné. Spark la chargera
    intégralement et la diffusera à toutes ses tâches (broadcast join),
    ce qui évite tout shuffle sur la jointure d'identité.

    SELECT * délibéré : la table tire une partie de ses colonnes d'un `s.*`
    dans le modèle dbt. Les nommer ici obligerait à maintenir deux listes
    synchronisées ; Spark lira le schéma dans le Parquet. Les colonnes sortent
    sous leurs noms refondus (str_match_id, str_ws_match_id, dt_scraped_at…) :
    spark_events.resolve_identity les lit sous ces noms.
    """
    out = IN_DIR / "match_index"
    out.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM intermediate.int_whoscored_match_index)
        TO '{(out / "match_index.parquet").as_posix()}'
        (FORMAT PARQUET, COMPRESSION zstd)
    """)
    n = con.execute(
        "SELECT COUNT(*) FROM intermediate.int_whoscored_match_index"
    ).fetchone()[0]
    logger.success(f"match_index écrit : {n:,} matchs → {out}")


def report_size(path: Path) -> None:
    """
    Taille et nombre de fichiers.

    Le nombre compte autant que la taille : Spark répartit le travail par
    fichier (puis par row group). Un fichier de 10 Go = une seule tâche et
    cinq cœurs qui attendent. 50 000 fichiers de 200 Ko = le coût d'ouverture
    qui dépasse le coût de lecture (small files problem). Cible usuelle :
    100–200 Mo par fichier.
    """
    if not path.exists():
        return
    files = list(path.rglob("*.parquet"))
    if not files:
        return
    total = sum(f.stat().st_size for f in files)
    logger.info(
        f"{path.name:<14} : {len(files):>4} fichiers, {total / 1e9:>6,.2f} Go "
        f"({total / len(files) / 1e6:,.0f} Mo/fichier en moyenne)"
    )


# ══════════════════════════════════════════════════════════════════════════════

def main(season: str | None = None, inventory_only: bool = False,
         clean: bool = False, strict: bool = False) -> int:
    if not DB_PATH.exists():
        logger.error(f"Base introuvable : {DB_PATH}")
        return 1

    logger.info(f"Base : {DB_PATH}  ({DB_PATH.stat().st_size / 1e9:,.1f} Go)")

    # read_only : aucune faute de frappe ne peut abîmer la base, et on prend un
    # verrou partagé au lieu d'un verrou exclusif.
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("SET memory_limit='6GB'")           # on laisse de la RAM à Spark
    # preserve_insertion_order=false : DuckDB garantit par défaut l'ordre de
    # lecture en sortie, ce qui l'oblige à tamponner les résultats des threads
    # parallèles pour les réordonner. L'ordre des lignes dans un Parquet que
    # Spark va redistribuer n'a aucune importance : on renonce à la garantie,
    # on libère la mémoire.
    con.execute("SET preserve_insertion_order=false")

    try:
        inventory(con)
        if inventory_only:
            return 0

        check_required(con)
        report_index_grain(con, strict=strict)
        report_match_id_coverage(con)

        if season:
            available = known_seasons(con)
            if season not in available:
                raise ValueError(
                    f"Saison '{season}' absente de la base.\n"
                    f"Saisons disponibles : {available}"
                )

        if clean and IN_DIR.exists():
            logger.warning(f"Suppression de {IN_DIR}")
            shutil.rmtree(IN_DIR)

        export_match_index(con)
        export_events(con, season)
    finally:
        con.close()

    report_size(IN_DIR / "events")
    report_size(IN_DIR / "match_index")
    return 0


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/export_to_parquet.log", level="DEBUG", rotation="5 MB",
               retention=10, encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", action="store_true",
                    help="liste les tables et s'arrête (aucune écriture)")
    ap.add_argument("--season", default=None,
                    help="n'exporte qu'une saison, ex: 2023-2024")
    ap.add_argument("--clean", action="store_true",
                    help="vide data/spark_in/ avant export")
    ap.add_argument("--strict", action="store_true",
                    help="échoue si l'index contient des ws_match_id dupliqués")
    args = ap.parse_args()

    sys.exit(main(season=args.season, inventory_only=args.inventory,
                  clean=args.clean, strict=args.strict))