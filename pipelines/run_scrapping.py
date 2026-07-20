"""
run_scrapping.py — Orchestrateur de scraping nocturne WhoScored (Prefect)
==========================================================================
Flow séparé du pipeline principal (run_pipeline.py) car sa cadence diffère :
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
from dotenv import load_dotenv
load_dotenv()

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from prefect import flow, task
from prefect.client.schemas.objects import State as PrefectState
from prefect.results import ResultRecord
from prefect.states import Failed
from prefect.cache_policies import NO_CACHE
from prefect.schedules import Cron

import yaml
from loguru import logger

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Racine du projet (dossier qui contient config.yaml, pipelines/, etc.) ──────
ROOT_DIR = Path(__file__).resolve().parent.parent


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Configuration
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    """Charge config.yaml depuis la racine du projet."""
    config_path = ROOT_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml introuvable : {config_path}")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Logger dédié à l'orchestrateur de scraping ────────────────────────────────
# Le sink CONSOLE (stderr) de loguru est actif par défaut.
# Le sink FICHIER est ajouté à l'EXÉCUTION (via _ensure_file_log), pas à l'import.
# Raison : Prefect sérialise (cloudpickle) le flow pour l'exécuter en sous-processus.
# Un fichier de log ouvert n'est pas picklable → si on l'ouvrait à l'import, le
# processus --serve planterait au moment de sérialiser le flow ("Cannot pickle
# files ... : a"). En l'ouvrant seulement quand le flow tourne, on l'évite.
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
# SECTION 2 — Exécution protégée d'une étape (calquée sur run_pipeline.py)
# ══════════════════════════════════════════════════════════════════════════════

def make_run_step_task(retries: int, retry_delay_seconds: int):
    """
    Fabrique la tâche Prefect run_step avec les paramètres de résilience lus
    depuis config.yaml (scraping.retries / scraping.retry_delay_seconds).

    Même logique que run_pipeline : on force le cwd à ROOT_DIR (les scripts de
    scraping utilisent des chemins relatifs comme db/football.duckdb et
    data/raw/whoscored/), on mesure la durée, et on capture toute exception.
    """
    @task(
        task_run_name="{step_name}",
        log_prints=False,
        cache_policy=NO_CACHE,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )
    def run_step(step_name: str, fn: callable, **kwargs) -> dict:
        logger.info(f"▶ Démarrage : {step_name}")
        start = time.perf_counter()

        saved_cwd = os.getcwd()
        saved_path = sys.path.copy()
        os.chdir(ROOT_DIR)

        try:
            fn(**kwargs)
            duration = time.perf_counter() - start
            logger.success(f"✓ {step_name} terminé en {duration:.1f}s")
            return {"name": step_name, "status": "OK", "duration": duration, "error": None}
        except Exception as e:
            duration = time.perf_counter() - start
            logger.error(f"✗ {step_name} a échoué après {duration:.1f}s : {e}")
            result = {"name": step_name, "status": "FAILED", "duration": duration, "error": str(e)}
            return Failed(data=result, message=str(e))
        finally:
            os.chdir(saved_cwd)
            sys.path = saved_path

    return run_step


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Définition des étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict) -> dict:
    """
    Construit le dictionnaire ordonné des étapes du scraping.

    Import différé : on n'importe les fonctions de scraping qu'ici (et pas en
    tête de module) pour ne charger seleniumbase & co qu'au moment de l'exécution.
    Les scripts sont dans pipelines/scrapping/ → on ajoute ce dossier au sys.path.
    """
    scrap_dir = ROOT_DIR / "pipelines" / "scrapping"
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
# SECTION 4 — Exécution du flow
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Scraping WhoScored", log_prints=False)
def run_scrapping(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """
    Exécute les étapes dans l'ordre.

    Pas de fail-fast : si scrape_raw échoue (ex. ban en cours de nuit), on tente
    quand même load_archive pour charger en base ce qui a déjà été archivé.
    Les deux étapes sont donc non-critiques.
    """
    _ensure_file_log()   # sink fichier activé au runtime (voir _ensure_file_log)
    results = []

    logger.info("=" * 60)
    logger.info(f"  SCRAPING WHOSCORED — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Étapes : {' → '.join(steps.keys())}")
    if dry_run:
        logger.info("  MODE DRY-RUN — aucune exécution réelle")
    logger.info("=" * 60)

    for step_name, step_cfg in steps.items():
        if dry_run:
            logger.info(f"  [DRY-RUN] {step_name} | critical={step_cfg['critical']}")
            results.append({"name": step_name, "status": "DRY-RUN", "duration": 0.0, "error": None})
            continue

        result = run_step_task(step_name, step_cfg["fn"], **step_cfg["kwargs"])
        if isinstance(result, PrefectState):
            result = result.data
        if isinstance(result, ResultRecord):
            result = result.result
        results.append(result)

    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Résumé
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(results: list[dict]) -> None:
    """Affiche un tableau récapitulatif de l'exécution."""
    total_duration = sum(r["duration"] for r in results)
    icons = {"OK": "✓", "FAILED": "✗", "SKIPPED": "⊘", "DRY-RUN": "○"}

    print("\n" + "=" * 60)
    print("  RÉSUMÉ SCRAPING")
    print("=" * 60)
    for r in results:
        icon = icons.get(r["status"], "?")
        duration = f"{r['duration']:.1f}s" if r["duration"] > 0 else "—"
        print(f"  {icon} {r['name']:<14} {r['status']:<10} {duration:>8}")
        if r["error"] and r["status"] == "FAILED":
            err_short = r["error"][:80] + "..." if len(r["error"]) > 80 else r["error"]
            print(f"    └─ {err_short}")
    print("-" * 60)
    print(f"  TOTAL {total_duration:.1f}s")
    print("=" * 60 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5b — Flow planifié (pour --serve)
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Scraping WhoScored", log_prints=True)
def scheduled_scrapping() -> list[dict]:
    """
    Version sans arguments du scraping, pour le scheduling Prefect.

    IMPORTANT : définie AU NIVEAU MODULE (pas dans main). Prefect la charge par
    son entrypoint (fichier:fonction) lors d'un run planifié, au lieu de la
    sérialiser « par valeur ». Sinon le pickling embarque le fichier de log
    loguru (ouvert en append) → PicklingError. Elle reconstruit tout en interne.
    """
    cfg = load_config()
    scr_cfg = cfg.get("scraping", {})
    run_step_task = make_run_step_task(
        retries=scr_cfg.get("retries", 1),
        retry_delay_seconds=scr_cfg.get("retry_delay_seconds", 60),
    )
    return run_scrapping(build_steps(cfg), dry_run=False, run_step_task=run_step_task)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = ["scrape_raw", "load_archive"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur de scraping nocturne WhoScored",
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
        cron            = scr_cfg.get("cron", "0 23 * * *")
        deployment_name = scr_cfg.get("deployment_name", "scraping-whoscored-nuit")
        timezone        = scr_cfg.get("timezone", "Europe/Paris")

        logger.info("Démarrage du scheduler Prefect (scraping)")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}  ({timezone})")
        logger.info(f"  Fenêtre     : {scr_cfg.get('max_runtime_min', 480)} min max")
        logger.info("  (Ctrl+C pour arrêter le scheduler)")

        # schedule=Cron(..., timezone=...) : sans fuseau explicite, Prefect
        # interpréterait le cron en UTC (23h UTC = 1h du matin en France l'été).
        # scheduled_scrapping est module-level → chargée par entrypoint, pas picklée.
        scheduled_scrapping.serve(
            name=deployment_name,
            schedule=Cron(cron, timezone=timezone),
        )
        return  # jamais atteint (serve bloque)

    # ── Mode normal : exécution immédiate ────────────────────────────────────
    all_steps = build_steps(cfg)

    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    else:
        steps_to_run = all_steps

    results = run_scrapping(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results)

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
