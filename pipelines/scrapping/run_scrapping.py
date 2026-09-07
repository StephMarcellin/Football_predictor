"""
run_scrapping.py — Orchestrateur PHASE 1 : Scraping nocturne WhoScored (Prefect)
=================================================================================
Flow séparé du pipeline principal car sa cadence diffère :
le scraping tourne la nuit (23h), le pipeline le lundi midi.

Un seul flow, deux tâches enchaînées :
    1. scrape_raw    → scrape en mode --raw-only (AUCUNE écriture DuckDB, DBeaver
                       reste libre), borné par max_runtime pour s'arrêter avant
                       le matin. Reprise sur disque d'une nuit à l'autre.
    2. load_archive  → charge les JSON archivés dans DuckDB (events + index).
                       S'exécute juste après le scrape, DBeaver encore fermé.

Usage :
    python run_scrapping.py                 # scrape_raw puis load_archive
    python run_scrapping.py --step scrape_raw     # une seule étape
    python run_scrapping.py --step load_archive
    python run_scrapping.py --dry-run       # liste les étapes sans exécuter
    python run_scrapping.py --serve         # scheduler Prefect (bloquant)

Scheduling Prefect :
    1. Serveur Prefect dans un terminal :   prefect server start
    2. Scheduler dans un autre terminal :   python run_scrapping.py --serve
    3. Déclenchement automatique selon scraping.cron dans config.yaml.
    4. UI : http://localhost:4200

    Paramètres dans config.yaml (section scraping) :
        cron                — expression cron du scheduling
        deployment_name     — nom affiché dans l'UI Prefect
        max_runtime_min     — durée max du scrape (arrêt propre, reprise ensuite)
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
from prefect.schedules import Cron
from loguru import logger

from orchestrator_common import (
    ROOT_DIR,
    load_config,
    make_run_step_task,
    execute_steps,
    print_summary,
)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── Logger fichier (ajouté au runtime, pas à l'import) ──────────────────────
# Prefect sérialise (cloudpickle) le flow pour l'exécuter en sous-processus.
# Un fichier de log ouvert n'est pas picklable → on l'ouvre seulement quand
# le flow tourne, via _ensure_file_log().
Path("logs").mkdir(exist_ok=True)
_FILE_LOG_ADDED = False


def _ensure_file_log() -> None:
    """Ajoute le sink fichier logs/scrapping.log, une seule fois par processus."""
    global _FILE_LOG_ADDED
    if _FILE_LOG_ADDED:
        return
    logger.add(
        "logs/scrapping.log",
        level="INFO",
        encoding="utf-8",
        rotation="5 MB",
        retention=10,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [SCRAP] {message}",
    )
    _FILE_LOG_ADDED = True


# ══════════════════════════════════════════════════════════════════════════════
# Définition des étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict) -> dict:
    """
    Construit le dictionnaire ordonné des étapes du scraping.

    Import différé : on n'importe les fonctions de scraping qu'ici (et pas en
    tête de module) pour ne charger seleniumbase & co qu'au moment de l'exécution.
    """
    scrap_dir = ROOT_DIR / "pipelines" / "scrapping" / "events"
    if scrap_dir.exists() and str(scrap_dir) not in sys.path:
        sys.path.insert(0, str(scrap_dir))

    from scrape_whoscored_details import run_scraping
    from load_whoscored_archive import run_load

    scr = cfg.get("scraping", {})
    max_runtime_min = scr.get("max_runtime_min", 480)

    def scrape_raw():
        """Scrape en raw-only (aucune écriture DuckDB), borné par max_runtime."""
        run_scraping(raw_only=True, max_runtime_min=max_runtime_min)

    def load_archive():
        """Charge les JSON archivés dans DuckDB (events + index + mark_scraped)."""
        run_load()

    return {
        "scrape_raw":   {"fn": scrape_raw,   "kwargs": {}, "critical": False},
        "load_archive": {"fn": load_archive, "kwargs": {}, "critical": False},
    }


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Scraping WhoScored", log_prints=False)
def run_scrapping_flow(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """
    Flow Prefect du scraping. Pas de fail-fast : les deux étapes sont
    non-critiques (si scrape_raw échoue, on tente quand même load_archive).
    """
    _ensure_file_log()
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="SCRAPING WHOSCORED",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Flow planifié (pour --serve)
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Scraping WhoScored", log_prints=True)
def scheduled_scrapping() -> list[dict]:
    """
    Version sans arguments du scraping, pour le scheduling Prefect.

    Définie au niveau MODULE (pas dans main). Prefect la charge par son
    entrypoint (fichier:fonction) lors d'un run planifié, au lieu de la
    sérialiser « par valeur ». Sinon le pickling embarque le fichier de log
    loguru → PicklingError.
    """
    cfg = load_config()
    scr_cfg = cfg.get("scraping", {})
    run_step_task = make_run_step_task(
        retries=scr_cfg.get("retries", 1),
        retry_delay_seconds=scr_cfg.get("retry_delay_seconds", 60),
    )
    return run_scrapping_flow(build_steps(cfg), dry_run=False, run_step_task=run_step_task)


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = ["scrape_raw", "load_archive"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 1 — Scraping nocturne WhoScored",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Exemples :
        python run_scrapping.py                    # scrape_raw puis load_archive
        python run_scrapping.py --step scrape_raw  # scrape seul
        python run_scrapping.py --step load_archive# chargement seul
        python run_scrapping.py --dry-run          # simule sans exécuter
        python run_scrapping.py --serve            # scheduler Prefect (bloquant)
        """,
    )
    parser.add_argument("--step", choices=STEP_NAMES, help="Exécute une seule étape")
    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")
    parser.add_argument("--serve", action="store_true",
                        help="Démarre le scheduler Prefect (bloquant). Déclenchement "
                             "selon scraping.cron. Prérequis : prefect server start.")
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
    scr_cfg = cfg.get("scraping", {})

    run_step_task = make_run_step_task(
        retries=scr_cfg.get("retries", 1),
        retry_delay_seconds=scr_cfg.get("retry_delay_seconds", 60),
    )

    # ── Mode --serve : scheduler Prefect (bloquant) ──────────────────────────
    if args.serve:
        cron = scr_cfg.get("cron", "0 23 * * *")
        deployment_name = scr_cfg.get("deployment_name", "scraping-whoscored-nuit")
        timezone = scr_cfg.get("timezone", "Europe/Paris")

        logger.info("Démarrage du scheduler Prefect (scraping)")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}  ({timezone})")
        logger.info(f"  Fenêtre     : {scr_cfg.get('max_runtime_min', 480)} min max")
        logger.info("  (Ctrl+C pour arrêter le scheduler)")

        scheduled_scrapping.serve(
            name=deployment_name,
            schedule=Cron(cron, timezone=timezone),
        )
        return

    # ── Mode normal : exécution immédiate ────────────────────────────────────
    all_steps = build_steps(cfg)

    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    else:
        steps_to_run = all_steps

    results = run_scrapping_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ SCRAPING")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
