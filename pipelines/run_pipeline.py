"""
run_pipeline.py — Orchestrateur Pipeline Projet 3-Étoiles
==========================================================
Enchaîne les étapes Gold → Modélisation → Prédiction → Backtest
en important directement les fonctions main() de chaque script.

Usage :
    python run_pipeline.py                        # pipeline complet
    python run_pipeline.py --step features        # étape unique
    python run_pipeline.py --from train           # reprend depuis train
    python run_pipeline.py --dry-run              # simule sans exécuter
    python run_pipeline.py --list                 # liste les étapes disponibles
    python run_pipeline.py --serve                # démarre le scheduler Prefect (bloquant)

Scheduling Prefect :
    1. Démarrer le serveur Prefect dans un terminal :
           prefect server start
    2. Dans un autre terminal, lancer le scheduler :
           python run_pipeline.py --serve
    3. Le pipeline se déclenchera automatiquement selon le cron défini
       dans config.yaml (pipeline.cron).
    4. Suivre l'exécution dans l'UI : http://localhost:4200

    Paramètres modifiables dans config.yaml (section pipeline) :
        cron              — expression cron du scheduling
        deployment_name   — nom affiché dans l'UI Prefect
        retries           — tentatives automatiques par étape
        retry_delay_seconds — délai entre tentatives
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import subprocess

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

import yaml
from loguru import logger

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Résolution de la racine du projet ────────────────────────────────────────
# ROOT_DIR = le dossier qui contient run_pipeline.py, config.yaml, pipelines/, etc.
ROOT_DIR = Path(__file__).resolve().parent.parent


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Configuration
# Lit config.yaml une seule fois. Tous les paramètres passent par ici.
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    """Charge config.yaml depuis la racine du projet."""
    config_path = ROOT_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml introuvable : {config_path}")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1b — Validation dbt
# ══════════════════════════════════════════════════════════════════════════════

def run_dbt_test() -> None:
    """
    Lance dbt test depuis dbt_project/.
    Valide la qualité de stg_backbone avant le feature engineering.
    Lève une exception si un test ERROR échoue (warnings ignorés).
    """

    dbt_dir  = ROOT_DIR / "dbt_project"
    log_path = ROOT_DIR / "logs" / "dbt_test_last.log"
    if not dbt_dir.exists():
        raise FileNotFoundError(f"dbt_project/ introuvable : {dbt_dir}")

    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", str(Path.home() / ".dbt")],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
    logger.info(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt test a échoué :\n{result.stderr[-1000:]}")
    
def run_dbt_seed(refresh: bool = False) -> None:
    """
    Lance dbt seed depuis dbt_project/.
    Initialise les tables de données.
    Lève une exception si l'initialisation échoue.
    """

    dbt_dir  = ROOT_DIR / "dbt_project"
    log_path = ROOT_DIR / "logs" / "dbt_seed_last.log"
    if not dbt_dir.exists():
        raise FileNotFoundError(f"dbt_project/ introuvable : {dbt_dir}")

    cmd = ["dbt", "seed"]
    if refresh:
        cmd += ["--full-refresh"]
    result = subprocess.run(
        cmd,
        cwd=dbt_dir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
    logger.info(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt seed a échoué :\n{result.stderr[-1000:]}")

def run_dbt_run(select: str = None) -> None:
    """
    Lance dbt run depuis dbt_project/.
    Exécute les modèles dbt.
    Lève une exception si l'exécution échoue.
    """
    dbt_dir  = ROOT_DIR / "dbt_project"
    log_path = ROOT_DIR / "logs" / "dbt_run_last.log"
    if not dbt_dir.exists():
        raise FileNotFoundError(f"dbt_project/ introuvable : {dbt_dir}")

    cmd = ["dbt", "run", "--profiles-dir", str(Path.home() / ".dbt")]
    if select:
        cmd += ["--select", select]
    result = subprocess.run(
        cmd,
        cwd=dbt_dir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
    logger.info(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt run a échoué :\n{result.stderr[-1000:]}")
        
# ── Logger orchestrateur ──────────────────────────────────────────────────────
# Un log dédié à l'orchestrateur, séparé des logs des scripts individuels.
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/pipeline.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [PIPELINE] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Exécution protégée d'une étape
#
# ══════════════════════════════════════════════════════════════════════════════
def make_run_step_task(retries: int, retry_delay_seconds: int):
    """
    Fabrique la tâche Prefect run_step avec les paramètres de résilience
    lus depuis config.yaml (pipeline.retries / pipeline.retry_delay_seconds).

    Pourquoi une factory ?
    Le décorateur @task ne peut pas recevoir de valeurs dynamiques au moment
    de la définition du module. En encapsulant la création dans une fonction,
    on peut passer retries et retry_delay_seconds issus du YAML.
    """
    @task(
        log_prints=True,
        cache_policy=NO_CACHE,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )
    def run_step(step_name: str, fn: callable, **kwargs) -> dict:
        """
        Exécute une fonction de pipeline dans un contexte protégé.

        - Sauvegarde et restaure os.getcwd() et sys.path
        - Force le répertoire courant à ROOT_DIR avant l'appel
        - Mesure la durée d'exécution
        - Capture toute exception sans faire crasher l'orchestrateur
        - Réessaie automatiquement en cas d'échec (retries défini dans config.yaml)

        Retourne un dict :
            {
                "name":     str,           # nom de l'étape
                "status":   "OK" | "FAILED",
                "duration": float,         # secondes
                "error":    str | None,    # message d'erreur si échec
            }
        """
        logger.info(f"▶ Démarrage : {step_name}")
        start = time.perf_counter()

        # Sauvegarde du contexte
        saved_cwd = os.getcwd()
        saved_path = sys.path.copy()

        # On s'assure d'être à la racine du projet (nécessaire pour 05_predict.py
        # qui utilise des chemins relatifs comme "db/football.duckdb")
        os.chdir(ROOT_DIR)

        try:
            fn(**kwargs)
            duration = time.perf_counter() - start
            logger.success(f"✓ {step_name} terminé en {duration:.1f}s")
            return {"name": step_name, "status": "OK", "duration": duration, "error": None}

        except Exception as e:
            duration = time.perf_counter() - start
            logger.error(f"✗ {step_name} a échoué après {duration:.1f}s : {e}")
            return {"name": step_name, "status": "FAILED", "duration": duration, "error": str(e)}

        finally:
            # Restauration du contexte — toujours exécuté, même si exception
            os.chdir(saved_cwd)
            sys.path = saved_path

    return run_step


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Définition des étapes
#
# Chaque étape est un dict avec :
#   fn       : la fonction à appeler (importée depuis le script)
#   kwargs   : les arguments à passer, lus depuis config.yaml
#   critical : si True, un échec stoppe tout le pipeline (fail-fast)
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict, full_refresh: bool = False) -> dict:
    """
    Construit le dictionnaire ordonné des étapes du pipeline.
    Les imports sont réalisés ici pour les différer au maximum.

    Retourne un dict ordonné (Python 3.7+ garantit l'ordre d'insertion) :
        { step_name: {"fn": callable, "kwargs": dict, "critical": bool} }
    """

    # ── Import différé des modules ────────────────────────────────────────────
    # On ajoute pipelines/ au sys.path pour que les imports fonctionnent
    # quelle que soit la position de run_pipeline.py dans le projet.
    pipelines_dir = ROOT_DIR / "pipelines"
    if pipelines_dir.exists() and str(pipelines_dir) not in sys.path:
        sys.path.insert(0, str(pipelines_dir))

    # Import de chaque script comme module Python
    # Si un script est introuvable, on lève une erreur claire
    try:
        mod_01 = _import_from_path("ingest_01",   ROOT_DIR / "pipelines" / "01_ingest.py")
        mod_01b = _import_from_path("odds_01b",   ROOT_DIR / "pipelines" / "01b_odds.py")
        mod_02 = _import_from_path("process_02",  ROOT_DIR / "pipelines" / "02_process.py")
        mod_04 = _import_from_path("",    ROOT_DIR / "pipelines" / "04_train.py")
        mod_04 = _import_from_path("train_04",    ROOT_DIR / "pipelines" / "04_train.py")
        mod_05 = _import_from_path("predict_05",  ROOT_DIR / "pipelines" / "05_predict.py")
        mod_06 = _import_from_path("backtest_06", ROOT_DIR / "pipelines" / "06_backtest.py")

    except FileNotFoundError as e:
        logger.error(f"Script introuvable : {e}")
        logger.error("Vérifiez que run_pipeline.py est à la racine du projet")
        raise

    # ── Paramètres lus depuis config.yaml ────────────────────────────────────
    train_cfg    = cfg.get("train", {})
    backtest_cfg = cfg.get("backtest", {})

    return {
        "ingest": {
            "fn":       mod_01.main,
            "kwargs":   {},
            "critical": True,
        },
        "odds": {
            "fn":       mod_01b.main,
            "kwargs":   {},
            "critical": True,
        },
        "process": {
            "fn":       mod_02.main,
            "kwargs":   {},
            "critical": True,
        },
        "dbt_seed": {
            "fn":       run_dbt_seed,
            "kwargs":   {"refresh": full_refresh}   ,  # Par défaut, dbt seed ne fait pas de full-refresh. Passer refresh=True pour forcer. 
            "critical": True,
        },
        "dbt_run": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "backbone features_rolling features_whoscored features_draw features_final"},
            "critical": True,
        },

        "dbt_test": {
            "fn":       run_dbt_test,
            "kwargs":   {},
            "critical": True,
        },

        "train": {
            "fn":     mod_04.main,
            "kwargs": {
                "step":     2,      # Stage 1 + Stage 2 (complet)
                "use_shap": True,
                "n_trials": train_cfg.get("bayes_n_iter", 50),
            },
            "critical": True,
        },
        "predict": {
            "fn":       mod_05.main,
            "kwargs":   {"upcoming": True},   # main() de 05 parse ses propres args — pas de kwargs ici
            "critical": True,
        },
        "backtest": {
            "fn":     mod_06.main,
            "kwargs": {
                "seasons":        None,   # None = utilise BACKTEST_SEASONS_DEFAULT du script
                "edge_min":       backtest_cfg.get("EDGE_MIN",       0.04),
                "confidence_min": backtest_cfg.get("CONFIDENCE_MIN", 0.45),
                "bankroll_init":  backtest_cfg.get("BANKROLL_INIT",  1000.0),
            },
            "critical": False,  # Un backtest qui plante ne bloque pas les prédictions du jour
        },
    }


def _import_from_path(module_name: str, path: Path):
    """
    Importe un module Python depuis un chemin absolu.
    Nécessaire car les scripts sont nommés avec des chiffres (03_, 04_...)
    ce qui les rend non-importables via import standard.
    """
    import importlib.util

    if not path.exists():
        raise FileNotFoundError(path)

    spec   = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Exécution du pipeline
# ══════════════════════════════════════════════════════════════════════════════
@flow(name="Pipeline 3-Étoiles", log_prints=True)
def run_pipeline(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """
    Exécute les étapes dans l'ordre, avec fail-fast sur les étapes critiques.

    Args:
        steps:          Sous-ensemble ordonné des étapes à exécuter (depuis build_steps).
        dry_run:        Si True, liste les étapes sans les exécuter.
        run_step_task:  La tâche Prefect à utiliser (issue de make_run_step_task).
                        Permet d'injecter les paramètres retries/retry_delay depuis config.

    Retourne la liste des résultats d'exécution.
    """
    results = []

    logger.info("=" * 60)
    logger.info(f"  PIPELINE 3-ÉTOILES — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Étapes : {' → '.join(steps.keys())}")
    if dry_run:
        logger.info("  MODE DRY-RUN — aucune exécution réelle")
    logger.info("=" * 60)

    for step_name, step_cfg in steps.items():

        if dry_run:
            logger.info(f"  [DRY-RUN] {step_name} | critical={step_cfg['critical']} | kwargs={step_cfg['kwargs']}")
            results.append({"name": step_name, "status": "DRY-RUN", "duration": 0.0, "error": None})
            continue

        result = run_step_task(step_name, step_cfg["fn"], **step_cfg["kwargs"])
        results.append(result)

        # Fail-fast : on stoppe si l'étape est critique et a échoué
        if result["status"] == "FAILED" and step_cfg["critical"]:
            logger.error(f"Étape critique '{step_name}' en échec — pipeline arrêté.")
            logger.error(f"Erreur : {result['error']}")
            # On marque les étapes non-exécutées comme SKIPPED
            executed_names = {r["name"] for r in results}
            for remaining_name in steps:
                if remaining_name not in executed_names:
                    results.append({
                        "name":     remaining_name,
                        "status":   "SKIPPED",
                        "duration": 0.0,
                        "error":    f"Pipeline arrêté après échec de '{step_name}'",
                    })
            break

    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Résumé d'exécution
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(results: list[dict]) -> None:
    """Affiche un tableau récapitulatif de l'exécution du pipeline."""

    total_duration = sum(r["duration"] for r in results)

    # Icônes de statut
    icons = {"OK": "✓", "FAILED": "✗", "SKIPPED": "⊘", "DRY-RUN": "○"}

    print("\n" + "=" * 60)
    print("  RÉSUMÉ D'EXÉCUTION")
    print("=" * 60)
    print(f"  {'Étape':<12} {'Statut':<10} {'Durée':>8}")
    print("-" * 60)

    for r in results:
        icon     = icons.get(r["status"], "?")
        duration = f"{r['duration']:.1f}s" if r["duration"] > 0 else "—"
        print(f"  {icon} {r['name']:<10} {r['status']:<10} {duration:>8}")

        if r["error"] and r["status"] == "FAILED":
            # Tronquer le message d'erreur pour la lisibilité console
            err_short = r["error"][:80] + "..." if len(r["error"]) > 80 else r["error"]
            print(f"    └─ {err_short}")

    print("-" * 60)
    print(f"  {'TOTAL':<12} {'':<10} {total_duration:.1f}s")
    print("=" * 60)

    # Statut global
    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        print(f"\n  ✗ Pipeline terminé avec {len(failed)} erreur(s)\n")
    elif all(r["status"] in ("OK", "DRY-RUN") for r in results):
        print("\n  ✓ Pipeline terminé avec succès\n")
    else:
        print("\n  ⊘ Pipeline interrompu (étapes ignorées)\n")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = ["dbt_seed","ingest","odds","process","dbt_run","dbt_test", "train", "predict", "backtest"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur Pipeline 3-Étoiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python run_pipeline.py                        # pipeline complet
  python run_pipeline.py --step ingest          # exécute l'étape d'ingestion
  python run_pipeline.py --step odds            # exécute l'étape de calcul des cotes
  python run_pipeline.py --step process         # exécute l'étape de traitement des données
  python run_pipeline.py --step dbt_seed        # seed les données
  python run_pipeline.py --step dbt_run         # exécute les modèles dbt
  python run_pipeline.py --step dbt_test        # exécute les tests dbt
  python run_pipeline.py --from train           # reprend depuis train
  python run_pipeline.py --from predict         # reprend depuis predict
  python run_pipeline.py --from backtest        # reprend depuis backtest
  python run_pipeline.py --dry-run              # simule sans exécuter
  python run_pipeline.py --list                 # liste les étapes
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--step",
        choices=STEP_NAMES,
        help="Exécute une seule étape",
    )
    group.add_argument(
        "--from",
        dest="from_step",
        choices=STEP_NAMES,
        metavar="STEP",
        help="Exécute depuis cette étape jusqu'à la fin",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Liste les étapes et leurs paramètres sans les exécuter",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Affiche les étapes disponibles et quitte",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help=(
            "Démarre le scheduler Prefect (mode bloquant). "
            "Le pipeline se déclenchera selon pipeline.cron dans config.yaml. "
            "Prérequis : prefect server start dans un terminal séparé."
        ),
    )
    parser.add_argument(
    "--full-refresh",
    action="store_true",
    help="Force dbt seed --full-refresh (recrée les tables depuis zéro)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # --list : affiche les étapes et quitte
    if args.list:
        print("\nÉtapes disponibles (dans l'ordre d'exécution) :")
        for i, name in enumerate(STEP_NAMES, 1):
            print(f"  {i}. {name}")
        print()
        return

    # Chargement de la configuration — source unique de vérité
    cfg = load_config()
    pipeline_cfg = cfg.get("pipeline", {})

    # ── Construction de la tâche Prefect avec les paramètres de résilience ────
    # Les valeurs retries et retry_delay_seconds viennent de config.yaml.
    # Si la section pipeline: est absente, on utilise des valeurs sûres par défaut.
    run_step_task = make_run_step_task(
        retries=pipeline_cfg.get("retries", 2),
        retry_delay_seconds=pipeline_cfg.get("retry_delay_seconds", 30),
    )

    # ── Mode --serve : démarre le scheduler Prefect (bloquant) ───────────────
    # Ce mode ne lance pas le pipeline immédiatement.
    # Il enregistre un déploiement Prefect et attend les déclenchements cron.
    if args.serve:
        cron            = pipeline_cfg.get("cron", "0 12 * * 1")
        deployment_name = pipeline_cfg.get("deployment_name", "pipeline-lundi-midi")

        logger.info(f"Démarrage du scheduler Prefect")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}")
        logger.info(f"  Retries     : {pipeline_cfg.get('retries', 2)} × {pipeline_cfg.get('retry_delay_seconds', 30)}s")
        logger.info(f"  UI          : http://localhost:4200")
        logger.info("  (Ctrl+C pour arrêter le scheduler)")

        # On crée une version sans arguments du flow pour le scheduling.
        # Prefect ne peut pas passer steps/dry_run lors d'un déclenchement cron,
        # donc on encapsule l'appel complet ici.
        @flow(name="Pipeline 3-Étoiles", log_prints=True)
        def scheduled_pipeline():
            """Version sans arguments du pipeline, pour le scheduling Prefect."""
            all_steps = build_steps(cfg)
            return run_pipeline(all_steps, dry_run=False, run_step_task=run_step_task)

        # .serve() bloque le processus et attend les déclenchements cron.
        # Prefect server doit tourner sur localhost:4200.
        scheduled_pipeline.serve(
            name=deployment_name,
            cron=cron,
        )
        return  # jamais atteint (serve bloque), mais par clarté

    # ── Mode normal : exécution immédiate ─────────────────────────────────────
    # Construction des étapes (imports différés)
    all_steps = build_steps(cfg,full_refresh=args.full_refresh)

    # Filtrage selon les arguments CLI
    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    elif args.from_step:
        idx_start = STEP_NAMES.index(args.from_step)
        steps_to_run = {
            name: all_steps[name]
            for name in STEP_NAMES[idx_start:]
        }
    else:
        steps_to_run = all_steps

    # Exécution
    results = run_pipeline(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)

    # Résumé
    print_summary(results)

    # Code de sortie : 1 si au moins une étape a échoué (utile pour les outils CI)
    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()