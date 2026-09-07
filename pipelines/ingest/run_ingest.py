"""
run_ingest.py — Orchestrateur de l'ingest (Bronze/CSV → Silver).

Enchaîne, dans l'ordre :
    ingest             → 01_ingest.py           (HTML/CSV → Parquet Bronze)
    odds               → 01b_odds.py            (CSV cotes → silver.odds)
    process_events     → process_events.py      (index WhoScored → match_registry + silver)
    process_team_stats → process_team_stats.py  (Parquet fbref/understat/whoscored → silver)

Suppose que le scraping + load_archive (scrapping/run_scrapping) a déjà déposé la
matière brute. À lancer AVANT le pipeline dbt (run_pipeline --tables).

Usage :
    python pipelines/ingest/run_ingest.py                     # tout, dans l'ordre
    python pipelines/ingest/run_ingest.py --step odds         # une seule étape
    python pipelines/ingest/run_ingest.py --dry-run           # liste sans exécuter
"""
import argparse
import importlib.util
import time
from pathlib import Path

from loguru import logger

# Racine ancrée sur config.yaml → robuste à la profondeur du dossier.
ROOT_DIR   = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
INGEST_DIR = ROOT_DIR / "pipelines" / "ingest"


def _load(name: str, filename: str):
    """Charge un script d'ingest par chemin (les noms à chiffres ne sont pas
    importables directement)."""
    spec = importlib.util.spec_from_file_location(name, INGEST_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_steps() -> dict:
    """Étapes ordonnées : nom -> (fonction, kwargs)."""
    ingest = _load("ingest_01",              "01_ingest.py")
    odds   = _load("odds_01b",               "01b_odds.py")
    pe     = _load("process_events_mod",     "process_events.py")
    pts    = _load("process_team_stats_mod", "process_team_stats.py")
    return {
        "ingest":             (ingest.main, {}),   # source=None → toutes les sources
        "odds":               (odds.main,   {}),
        "process_events":     (pe.main,     {}),
        "process_team_stats": (pts.main,    {}),
    }


def main(step: str = None, dry_run: bool = False):
    steps = build_steps()
    names = [step] if step else list(steps)

    logger.info("=" * 55)
    logger.info(f"INGEST — étapes : {' → '.join(names)}")
    if dry_run:
        logger.info("MODE DRY-RUN — aucune exécution")
    logger.info("=" * 55)

    for n in names:
        fn, kwargs = steps[n]
        if dry_run:
            logger.info(f"  [DRY-RUN] {n}")
            continue
        t0 = time.time()
        logger.info(f"  ▶ {n}")
        fn(**kwargs)
        logger.info(f"  ✓ {n}  ({time.time() - t0:.1f}s)")

    logger.info("Ingest terminé.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Orchestrateur ingest (Bronze/CSV → Silver).")
    p.add_argument("--step",
                   choices=["ingest", "odds", "process_events", "process_team_stats"],
                   help="Exécute une seule étape")
    p.add_argument("--dry-run", action="store_true", help="Liste les étapes sans exécuter")
    a = p.parse_args()
    main(step=a.step, dry_run=a.dry_run)
