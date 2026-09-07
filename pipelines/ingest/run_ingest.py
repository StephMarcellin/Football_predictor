#!/usr/bin/env python3
"""
run_ingest.py — Orchestrateur PHASE 2 : Ingest (Bronze/CSV → Silver)
=====================================================================
Enchaîne, dans l'ordre :
    ingest             → 01_ingest.py           (HTML/CSV → Parquet Bronze)
    odds               → 01b_odds.py            (CSV cotes → silver.odds)
    process_events     → process_events.py      (index WhoScored → match_registry + silver)
    process_team_stats → process_team_stats.py  (Parquet fbref/understat/whoscored → silver)
    validate_silver    → Great Expectations      (data quality checks sur silver)

Suppose que le scraping + load_archive (scrapping/run_scrapping) a déjà déposé la
matière brute. À lancer AVANT le feature engineering (run_features_engineering).

Usage :
    python pipelines/ingest/run_ingest.py                     # tout, dans l'ordre
    python pipelines/ingest/run_ingest.py --step odds         # une seule étape
    python pipelines/ingest/run_ingest.py --from process_events  # reprend depuis
    python pipelines/ingest/run_ingest.py --dry-run           # liste sans exécuter
    python pipelines/ingest/run_ingest.py --serve             # scheduler Prefect (bloquant)

Scheduling Prefect :
    1. Serveur Prefect dans un terminal :   prefect server start
    2. Scheduler dans un autre terminal :   python pipelines/ingest/run_ingest.py --serve
    3. Déclenchement automatique selon ingest.cron dans config.yaml.
    4. UI : http://localhost:4200

    Paramètres dans config.yaml (section ingest) :
        cron                — expression cron du scheduling
        deployment_name     — nom affiché dans l'UI Prefect
        retries             — tentatives automatiques par étape
        retry_delay_seconds — délai entre tentatives
"""

from __future__ import annotations

# --- bootstrap : rend les modules partagés (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------

from dotenv import load_dotenv
load_dotenv()

import argparse
import sys
from pathlib import Path

from prefect import flow
from loguru import logger

from orchestrator_common import (
    ROOT_DIR,
    load_config,
    import_from_path,
    make_run_step_task,
    execute_steps,
    print_summary,
)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── Logger fichier ───────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/ingest.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [INGEST] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# Chemins des scripts d'ingest
# ══════════════════════════════════════════════════════════════════════════════

INGEST_DIR = ROOT_DIR / "pipelines" / "ingest"


# ══════════════════════════════════════════════════════════════════════════════
# Étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps() -> dict:
    """
    Construit le dictionnaire ordonné des étapes d'ingest.

    Chaque étape a :
        fn       : la fonction à appeler
        kwargs   : les arguments à passer
        critical : si True, un échec stoppe la phase (fail-fast)

    Logique de criticité :
        - ingest (01_ingest) : CRITIQUE — si les données brutes ne sont pas
          chargées, le reste n'a rien à traiter.
        - odds (01b_odds) : NON-CRITIQUE — les cotes manquantes n'empêchent
          pas la normalisation des matchs. Le backtest aura des trous, c'est tout.
        - process_events : CRITIQUE — alimente match_registry et les events
          silver, base de tout le reste.
        - process_team_stats : CRITIQUE — alimente les stats silver (fbref,
          understat, whoscored).
        - validate_silver : CRITIQUE — Great Expectations vérifie la qualité
          des tables silver avant de passer au feature engineering.
    """
    ingest = import_from_path("ingest_01",              INGEST_DIR / "01_ingest.py")
    odds   = import_from_path("odds_01b",               INGEST_DIR / "01b_odds.py")
    pe     = import_from_path("process_events_mod",     INGEST_DIR / "process_events.py")
    pts    = import_from_path("process_team_stats_mod", INGEST_DIR / "process_team_stats.py")

    # Import conditionnel de la validation Silver
    try:
        from validation.run_validation import run_validate_silver
        has_validation = True
    except ImportError:
        has_validation = False
        logger.warning("validation.run_validation non trouvé — validate_silver désactivé")

    steps = {
        "ingest": {
            "fn":       ingest.main,
            "kwargs":   {},
            "critical": True,
        },
        "odds": {
            "fn":       odds.main,
            "kwargs":   {},
            "critical": False,
        },
        "process_events": {
            "fn":       pe.main,
            "kwargs":   {},
            "critical": True,
        },
        "process_team_stats": {
            "fn":       pts.main,
            "kwargs":   {},
            "critical": True,
        },
    }

    if has_validation:
        steps["validate_silver"] = {
            "fn":       run_validate_silver,
            "kwargs":   {},
            "critical": True,
        }

    return steps


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Ingest Bronze→Silver", log_prints=False)
def run_ingest_flow(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """Flow Prefect de la phase 2 (ingest)."""
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="INGEST BRONZE → SILVER",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Flow planifié (pour --serve)
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Ingest Bronze→Silver", log_prints=True)
def scheduled_ingest() -> list[dict]:
    """
    Version sans arguments de l'ingest, pour le scheduling Prefect.

    Définie au niveau MODULE (pas dans main) pour éviter les problèmes
    de pickling loguru (même raison que dans run_scrapping.py).
    """
    cfg = load_config()
    ing_cfg = cfg.get("ingest", {})
    run_step_task = make_run_step_task(
        retries=ing_cfg.get("retries", 2),
        retry_delay_seconds=ing_cfg.get("retry_delay_seconds", 30),
    )
    return run_ingest_flow(build_steps(), dry_run=False, run_step_task=run_step_task)


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = ["ingest", "odds", "process_events", "process_team_stats", "validate_silver"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 2 — Ingest (Bronze/CSV → Silver)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Exemples :
        python run_ingest.py                              # tout, dans l'ordre
        python run_ingest.py --step odds                  # une seule étape
        python run_ingest.py --from process_events        # reprend depuis
        python run_ingest.py --dry-run                    # simule sans exécuter
        python run_ingest.py --serve                      # scheduler Prefect (bloquant)
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step", choices=STEP_NAMES,
                       help="Exécute une seule étape")
    group.add_argument("--from", dest="from_step", choices=STEP_NAMES,
                       metavar="STEP", help="Exécute depuis cette étape jusqu'à la fin")

    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")
    parser.add_argument("--serve", action="store_true",
                        help="Démarre le scheduler Prefect (bloquant). "
                             "Déclenchement selon ingest.cron dans config.yaml. "
                             "Prérequis : prefect server start.")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        print("\nÉtapes disponibles (dans l'ordre) :")
        for i, name in enumerate(STEP_NAMES, 1):
            print(f"  {i}. {name}")
        print()
        return

    cfg = load_config()
    ing_cfg = cfg.get("ingest", {})

    run_step_task = make_run_step_task(
        retries=ing_cfg.get("retries", 2),
        retry_delay_seconds=ing_cfg.get("retry_delay_seconds", 30),
    )

    # ── Mode --serve : scheduler Prefect (bloquant) ──────────────────────────
    if args.serve:
        cron = ing_cfg.get("cron", "0 8 * * *")
        deployment_name = ing_cfg.get("deployment_name", "ingest-bronze-silver")
        timezone = ing_cfg.get("timezone", "Europe/Paris")

        logger.info("Démarrage du scheduler Prefect (ingest)")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}  ({timezone})")
        logger.info("  (Ctrl+C pour arrêter)")

        from prefect.schedules import Cron as CronSchedule
        scheduled_ingest.serve(
            name=deployment_name,
            schedule=CronSchedule(cron, timezone=timezone),
        )
        return

    # ── Mode normal : exécution immédiate ────────────────────────────────────
    all_steps = build_steps()

    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    elif args.from_step:
        # Filtrer les étapes disponibles (validate_silver peut manquer)
        available = [n for n in STEP_NAMES if n in all_steps]
        idx = available.index(args.from_step)
        steps_to_run = {n: all_steps[n] for n in available[idx:]}
    else:
        steps_to_run = all_steps

    results = run_ingest_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ INGEST")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
