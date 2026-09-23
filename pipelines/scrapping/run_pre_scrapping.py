"""
run_pre_scrapping.py — Orchestrateur PHASE 0.5 : Indexation quotidienne des URLs
================================================================================
Rafraîchit la liste des matchs WhoScored à scraper en indexant les URLs de
la saison courante. À lancer AVANT run_scrapping.py (qui scrape les détails).

Étape unique :
    scrape_matches → scrape_whoscored_match.run() (indexation des URLs dans
                     silver.stg_whoscored_urls avec is_scraped=False pour les
                     nouveaux matchs)

Comportement par saison (défini dans config.yaml section scraping) :
    - Saisons historiques : indexe TOUS les mois (comportement inchangé)
    - Saison courante     : indexe uniquement les mois entre
                            current_season_parameters.min_month et aujourd'hui
    - Vérification `is_month_indexed` toujours active → skip des mois déjà OK

Usage :
    python run_pre_scrapping.py                    # indexation complète
    python run_pre_scrapping.py --dry-run          # simule sans exécuter
    python run_pre_scrapping.py --list             # affiche l'étape

Config config.yaml (section scraping) :
    retries               — tentatives automatiques
    retry_delay_seconds   — délai entre tentatives
    headless              — Chrome sans interface graphique (défaut : True)
    current_season_parameters:
        season            — saison courante (ex : "2025-2026")
        min_month         — 1er mois à indexer pour cette saison (défaut : 8 = août)
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
    """Ajoute le sink fichier logs/pre_scrapping.log, une seule fois par processus."""
    global _FILE_LOG_ADDED
    if _FILE_LOG_ADDED:
        return
    logger.add(
        "logs/pre_scrapping.log",
        level="INFO",
        encoding="utf-8",
        rotation="5 MB",
        retention=10,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [PRE-SCRAP] {message}",
    )
    _FILE_LOG_ADDED = True


# ══════════════════════════════════════════════════════════════════════════════
# Définition des étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict) -> dict:
    """
    Construit le dictionnaire ordonné des étapes de pre-scraping.

    Import différé : on n'importe scrape_whoscored_match qu'ici (et pas en
    tête de module) pour ne charger seleniumbase & duckdb qu'au moment de
    l'exécution — évite aussi de faire tourner le chargement `SCRAP_CFG`
    à l'import de ce module (le config.yaml doit avoir été lu quand on y arrive).
    """
    scrap_dir = ROOT_DIR / "pipelines" / "scrapping" / "events"
    if scrap_dir.exists() and str(scrap_dir) not in sys.path:
        sys.path.insert(0, str(scrap_dir))

    from scrape_whoscored_match import run as run_scrape_match

    scr      = cfg.get("scraping", {})
    headless = bool(scr.get("headless", True))

    def scrape_matches():
        """
        Indexe les URLs des matchs WhoScored.

        `current=True` : active le filtrage [min_month → today] sur la saison
        courante (config.yaml scraping.current_season_parameters). Les saisons
        historiques restent scrapées complètement.

        `is_month_indexed=True` skippe les mois déjà en base — comportement
        indépendant de `current`.
        """
        run_scrape_match(headless=headless, current=True)

    return {
        "scrape_matches": {"fn": scrape_matches, "kwargs": {"current"}, "critical": True},
    }


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Pre-scraping WhoScored (URLs)", log_prints=False)
def run_pre_scrapping_flow(steps: dict, dry_run: bool = False, run_step_task=None) -> list:
    """Flow Prefect du pre-scraping — indexation des URLs de matchs."""
    _ensure_file_log()
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="PRE-SCRAPING WHOSCORED (URLs)",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = ["scrape_matches"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 0.5 — Indexation des URLs WhoScored",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Exemples :
        python run_pre_scrapping.py              # indexation complète
        python run_pre_scrapping.py --dry-run    # simule
        python run_pre_scrapping.py --list       # affiche l'étape
        """,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")
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

    all_steps = build_steps(cfg)
    results = run_pre_scrapping_flow(all_steps, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ PRE-SCRAPING")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
