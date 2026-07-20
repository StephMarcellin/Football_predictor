"""
load_whoscored_archive.py — Charge les JSON WhoScored archivés dans DuckDB
==========================================================================
Deuxième étape du découplage fetch / parse.

Le scraper lancé en mode --raw-only écrit UNIQUEMENT des fichiers gzip
({ws_id}.json.gz) sous data/raw/whoscored/, sans jamais toucher DuckDB.
Ce script relit ces archives et remplit la base :

    silver.stg_whoscored_events        (parse_events + upsert_events)
    silver.stg_whoscored_match_index   (upsert_match_index)
    silver.stg_whoscored_urls          (is_scraped = TRUE via mark_scraped)

À lancer quand DBeaver est FERMÉ : c'est ce script-ci qui pose le verrou
d'écriture DuckDB, pas le scraping. Le scraping (raw-only) et le chargement
sont ainsi totalement séparés dans le temps.

Idempotent : recharger un match remplace proprement ses events (DELETE + INSERT).
Comme la lecture se fait sur disque local (pas de réseau), c'est rapide.

USAGE
    python pipelines/scrapping/load_whoscored_archive.py
    python pipelines/scrapping/load_whoscored_archive.py --limit 100
    python pipelines/scrapping/load_whoscored_archive.py --season 2023-2024
    python pipelines/scrapping/load_whoscored_archive.py --skip-existing
"""

import gzip
import json
import argparse

import duckdb
from loguru import logger

# On réutilise les fonctions et constantes du scraper : même schéma, même logique
# de parsing et d'upsert → aucune duplication de code.
from scrape_whoscored_details import (
    RAW_DIR, DB_PATH, init_db,
    parse_events, upsert_events, upsert_match_index, mark_scraped,
)

SUFFIX = ".json.gz"


def load_url_meta() -> dict:
    """
    Récupère (league_source, season) par ws_match_id depuis stg_whoscored_urls.

    Ces deux champs ne sont PAS présents dans matchCentreData (ce sont des
    métadonnées côté projet, issues du scraping des URLs). On les rattache donc
    via la table des URLs, qui en est la source de vérité.
    """
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    rows = conn.execute(
        "SELECT ws_match_id, league_source, season FROM silver.stg_whoscored_urls"
    ).fetchall()
    conn.close()
    return {str(r[0]): (r[1], r[2]) for r in rows}


def already_loaded() -> set:
    """ws_match_id déjà marqués is_scraped=TRUE (pour --skip-existing)."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    rows = conn.execute(
        "SELECT ws_match_id FROM silver.stg_whoscored_urls WHERE is_scraped = TRUE"
    ).fetchall()
    conn.close()
    return {str(r[0]) for r in rows}


def run_load(limit=None, season_filter=None, skip_existing=False) -> dict:
    """
    Parcourt les archives gzip et les charge en base.

    limit         : s'arrêter après N matchs chargés avec succès (test).
    season_filter : ne charger qu'une saison (ex: "2023-2024").
    skip_existing : ignorer les matchs déjà is_scraped=TRUE.
    """
    init_db()  # garantit tables + migration des colonnes events enrichies
    meta = load_url_meta()
    done = already_loaded() if skip_existing else set()

    root = RAW_DIR / season_filter if season_filter else RAW_DIR
    files = sorted(root.rglob(f"*{SUFFIX}"))
    summary = {"ok": 0, "failed": 0, "skipped": 0, "total": len(files)}
    logger.info(f"  {len(files)} archive(s) à charger depuis {root}")

    for n, f in enumerate(files, 1):
        ws_id = f.name[:-len(SUFFIX)]

        if skip_existing and ws_id in done:
            summary["skipped"] += 1
            continue

        league, season = meta.get(ws_id, (None, None))
        if league is None:
            logger.warning(
                f"  {ws_id} absent de stg_whoscored_urls — league/season inconnus"
            )

        try:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                data = json.load(fh)
            events, match_index = parse_events(data, ws_id, league, season)
            if events and upsert_events(events) and upsert_match_index(match_index):
                mark_scraped(ws_id)
                summary["ok"] += 1
            else:
                summary["failed"] += 1
                logger.warning(f"  Échec chargement {ws_id}")
        except Exception as e:
            summary["failed"] += 1
            logger.error(f"  Erreur chargement {ws_id} : {e}")

        if limit and summary["ok"] >= limit:
            logger.info(f"  Limite de {limit} atteinte — arrêt")
            break
        if n % 500 == 0:
            logger.info(f"  ... {n}/{len(files)} traités")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Charge les archives JSON WhoScored (gzip) dans DuckDB"
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre max de matchs à charger (test)")
    parser.add_argument("--season", default=None,
                        help="Ne charger qu'une saison (ex: 2023-2024)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Ignorer les matchs déjà is_scraped=TRUE")
    args = parser.parse_args()

    logger.info("=== Chargement des archives WhoScored → DuckDB ===")
    summary = run_load(
        limit=args.limit,
        season_filter=args.season,
        skip_existing=args.skip_existing,
    )
    logger.success(
        f"=== Terminé — {summary['ok']}/{summary['total']} chargés | "
        f"{summary['failed']} échecs | {summary['skipped']} ignorés ==="
    )


if __name__ == "__main__":
    main()
