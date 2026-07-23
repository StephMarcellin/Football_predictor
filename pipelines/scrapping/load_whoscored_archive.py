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
from whoscored_entities import (
    collect_player_names, collect_formations_ref,
    upsert_players_ref, upsert_formations_ref,
    upsert_match_facts,
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


def slots_filled() -> set:
    """
    ws_match_id dont formation_slots est déjà rempli (pour --missing-slots-only).
    Permet de reprendre un backfill interrompu sans retraiter les matchs déjà faits.
    """
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(
            "SELECT DISTINCT ws_match_id FROM silver.stg_whoscored_formations "
            "WHERE formation_slots IS NOT NULL"
        ).fetchall()
        conn.close()
        return {str(r[0]) for r in rows}
    except Exception:
        return set()


def run_load(limit=None, season_filter=None, skip_existing=False,
             missing_slots_only=False) -> dict:
    """
    Parcourt les archives gzip et les charge en base.

    limit              : s'arrêter après N matchs chargés avec succès (test).
    season_filter      : ne charger qu'une saison (ex: "2023-2024").
    skip_existing      : ignorer le re-parse events des matchs déjà is_scraped=TRUE
                         (les faits, dont formation_slots, sont écrits quand même).
    missing_slots_only : ne traiter que les archives dont formation_slots manque
                         encore (reprise d'un backfill interrompu).
    """
    init_db()  # garantit tables + migration des colonnes events enrichies
    meta = load_url_meta()
    done = already_loaded() if skip_existing else set()

    root = RAW_DIR / season_filter if season_filter else RAW_DIR
    files = sorted(root.rglob(f"*{SUFFIX}"))

    if missing_slots_only:
        filled = slots_filled()
        before = len(files)
        files = [f for f in files if f.name[:-len(SUFFIX)] not in filled]
        logger.info(
            f"  --missing-slots-only : {before - len(files)} archive(s) déjà "
            f"remplie(s) ignorée(s), {len(files)} à traiter"
        )
    summary = {"ok": 0, "failed": 0, "skipped": 0, "total": len(files)}
    logger.info(f"  {len(files)} archive(s) à charger depuis {root}")

    # Accumulateurs des dimensions (dédupliquées en mémoire sur tout le run).
    # On les écrit une seule fois en fin de boucle : les dims sont petites.
    players_acc: dict = {}
    formations_acc: dict = {}

    for n, f in enumerate(files, 1):
        ws_id = f.name[:-len(SUFFIX)]

        # Ouvrir l'archive UNE fois. La collecte des dimensions se fait pour
        # chaque archive, indépendamment du skip events (un match déjà chargé
        # côté events peut ne jamais avoir alimenté les dims).
        try:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:
            summary["failed"] += 1
            logger.error(f"  Erreur lecture archive {ws_id} : {e}")
            continue

        collect_player_names(data, players_acc)
        collect_formations_ref(data, formations_acc)

        # Tables de fait par match (joueurs, formations, stats équipe, méta).
        # Écrites pour chaque archive, indépendamment du skip events : un match
        # déjà chargé côté events peut ne jamais avoir alimenté ces tables.
        try:
            upsert_match_facts(data, ws_id)
        except Exception as e:
            logger.warning(f"  Faits non écrits pour {ws_id} : {e}")

        # ── Events : on saute si déjà chargé (skip_existing) ──────────────────
        if skip_existing and ws_id in done:
            summary["skipped"] += 1
            continue

        league, season = meta.get(ws_id, (None, None))
        if league is None:
            logger.warning(
                f"  {ws_id} absent de stg_whoscored_urls — league/season inconnus"
            )

        try:
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
        if n % 100 == 0:
            logger.info(f"  ... {n}/{len(files)} traités")

    # ── Écriture des dimensions accumulées (une passe) ────────────────────────
    upsert_players_ref(players_acc)
    upsert_formations_ref(formations_acc)

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
                        help="Ignorer le re-parse events des matchs déjà is_scraped=TRUE")
    parser.add_argument("--missing-slots-only", action="store_true",
                        help="Ne traiter que les archives dont formation_slots manque (résumable)")
    args = parser.parse_args()

    logger.info("=== Chargement des archives WhoScored → DuckDB ===")
    summary = run_load(
        limit=args.limit,
        season_filter=args.season,
        skip_existing=args.skip_existing,
        missing_slots_only=args.missing_slots_only,
    )
    logger.success(
        f"=== Terminé — {summary['ok']}/{summary['total']} chargés | "
        f"{summary['failed']} échecs | {summary['skipped']} ignorés ==="
    )


if __name__ == "__main__":
    main()
