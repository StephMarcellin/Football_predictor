"""
orchestrator_common.py — Plomberie partagée entre orchestrateurs de phase
==========================================================================
Chaque orchestrateur (run_scrapping, run_ingest, run_features_engineering,
run_ml) importe depuis ce module au lieu de dupliquer le code.

Contenu :
    ROOT_DIR              — racine du projet (remonte jusqu'à config.yaml)
    load_config()         — charge config.yaml
    import_from_path()    — import dynamique (scripts nommés 01_, 03_, etc.)
    make_run_step_task()  — factory Prefect @task avec retries + protection cwd
    execute_steps()       — boucle d'exécution fail-fast (PAS un @flow)
    print_summary()       — tableau récapitulatif

Les helpers dbt (run_dbt_run, run_dbt_test, etc.) sont dans dbt_helpers.py.

Pourquoi execute_steps() n'est PAS un @flow Prefect ?
    Chaque orchestrateur a besoin de son propre nom de flow dans l'UI Prefect
    (« Scraping WhoScored », « Ingest Bronze→Silver », etc.). Le décorateur @flow
    fixe le nom au moment de la décoration. Donc chaque orchestrateur crée son
    propre @flow et appelle execute_steps() dedans.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from loguru import logger

from prefect import task
from prefect.client.schemas.objects import State as PrefectState
from prefect.results import ResultRecord
from prefect.states import Failed
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger


# ══════════════════════════════════════════════════════════════════════════════
# Racine du projet
# ══════════════════════════════════════════════════════════════════════════════
# Remonte l'arborescence depuis CE fichier jusqu'à trouver config.yaml.
# Fonctionne quel que soit l'endroit d'où le script est lancé.
ROOT_DIR = next(
    p for p in Path(__file__).resolve().parents
    if (p / "config.yaml").exists()
)


# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    """Charge config.yaml depuis la racine du projet."""
    config_path = ROOT_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml introuvable : {config_path}")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ══════════════════════════════════════════════════════════════════════════════
# Import dynamique
# ══════════════════════════════════════════════════════════════════════════════

def import_from_path(module_name: str, path: Path):
    """
    Importe un module Python depuis un chemin absolu.

    Nécessaire car certains scripts sont nommés avec des chiffres (01_, 03_...)
    ce qui les rend non-importables via import standard.

    Args:
        module_name: nom arbitraire pour le module (ex: "ingest_01")
        path:        chemin absolu vers le .py

    Returns:
        Le module importé (on peut ensuite appeler module.main(), etc.)

    Raises:
        FileNotFoundError: si le fichier n'existe pas
    """
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ══════════════════════════════════════════════════════════════════════════════
# Factory de tâche Prefect
# ══════════════════════════════════════════════════════════════════════════════

def make_run_step_task(retries: int, retry_delay_seconds: int):
    """
    Fabrique une tâche Prefect « run_step » avec les paramètres de résilience
    lus depuis config.yaml.

    Pourquoi une factory ?
        Le décorateur @task ne peut pas recevoir de valeurs dynamiques au moment
        de la définition du module. En encapsulant la création dans une fonction,
        on peut passer retries et retry_delay_seconds issus du YAML.

    Ce que fait run_step :
        - Sauvegarde et restaure os.getcwd() et sys.path (isolation entre étapes)
        - Force le cwd à ROOT_DIR (les scripts utilisent des chemins relatifs)
        - Passerelle loguru → Prefect (les logs des scripts apparaissent dans l'UI)
        - Mesure la durée d'exécution
        - Capture les exceptions → retourne Failed() au lieu de crasher

    Args:
        retries:             nombre de tentatives automatiques sur échec
        retry_delay_seconds: délai entre tentatives

    Returns:
        La tâche Prefect prête à l'emploi : run_step(step_name, fn, **kwargs)
    """
    @task(
        task_run_name="{step_name}",
        log_prints=False,
        cache_policy=NO_CACHE,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )
    def run_step(step_name: str, fn: callable, **kwargs) -> dict:
        """
        Exécute une fonction de pipeline dans un contexte protégé.

        Retourne :
            {"name": str, "status": "OK"|"FAILED", "duration": float, "error": str|None}
        """
        logger.info(f"▶ Démarrage : {step_name}")
        start = time.perf_counter()

        # Sauvegarde du contexte
        saved_cwd = os.getcwd()
        saved_path = sys.path.copy()
        os.chdir(ROOT_DIR)

        # Passerelle loguru → Prefect : chaque log des scripts remonte dans l'UI
        prefect_logger = get_run_logger()
        sink_id = logger.add(
            lambda msg: prefect_logger.log(
                msg.record["level"].no, msg.record["message"]
            ),
            level="INFO",
            format="{message}",
        )

        try:
            fn(**kwargs)
            duration = time.perf_counter() - start
            logger.success(f"✓ {step_name} terminé en {duration:.1f}s")
            return {
                "name": step_name,
                "status": "OK",
                "duration": duration,
                "error": None,
            }

        except Exception as e:
            duration = time.perf_counter() - start
            logger.error(f"✗ {step_name} a échoué après {duration:.1f}s : {e}")
            result = {
                "name": step_name,
                "status": "FAILED",
                "duration": duration,
                "error": str(e),
            }
            return Failed(data=result, message=str(e))

        finally:
            logger.remove(sink_id)
            os.chdir(saved_cwd)
            sys.path = saved_path

    return run_step


# ══════════════════════════════════════════════════════════════════════════════
# Boucle d'exécution (commune à tous les orchestrateurs)
# ══════════════════════════════════════════════════════════════════════════════

def execute_steps(
    steps: dict,
    run_step_task,
    dry_run: bool = False,
    phase_name: str = "PIPELINE",
) -> list[dict]:
    """
    Exécute les étapes dans l'ordre, avec fail-fast sur les étapes critiques.

    N'EST PAS un @flow Prefect — chaque orchestrateur crée son propre @flow
    (avec son propre nom) et appelle cette fonction dedans.

    Args:
        steps:          dict ordonné {name: {"fn": callable, "kwargs": dict, "critical": bool}}
        run_step_task:  tâche Prefect issue de make_run_step_task()
        dry_run:        si True, liste les étapes sans exécuter
        phase_name:     nom affiché dans les logs (ex: "INGEST", "SCRAPING")

    Returns:
        Liste de dicts résultats [{name, status, duration, error}, ...]

    Raises:
        RuntimeError: si une étape critique échoue (après avoir marqué les restantes SKIPPED)
    """
    results = []

    logger.info("=" * 60)
    logger.info(f"  {phase_name} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Étapes : {' → '.join(steps.keys())}")
    if dry_run:
        logger.info("  MODE DRY-RUN — aucune exécution réelle")
    logger.info("=" * 60)

    critical_failure = None

    for step_name, step_cfg in steps.items():
        if dry_run:
            logger.info(
                f"  [DRY-RUN] {step_name} | critical={step_cfg['critical']} "
                f"| kwargs={step_cfg['kwargs']}"
            )
            results.append({
                "name": step_name,
                "status": "DRY-RUN",
                "duration": 0.0,
                "error": None,
            })
            continue

        # Exécution de l'étape via la tâche Prefect
        result = run_step_task(step_name, step_cfg["fn"], **step_cfg["kwargs"])

        # Unwrap : Prefect encapsule parfois le retour dans un State ou ResultRecord
        if isinstance(result, PrefectState):
            result = result.data
        if isinstance(result, ResultRecord):
            result = result.result

        results.append(result)

        # Fail-fast : on stoppe si l'étape est critique et a échoué
        if result["status"] == "FAILED" and step_cfg["critical"]:
            logger.error(f"Étape critique '{step_name}' en échec — phase arrêtée.")
            logger.error(f"Erreur : {result['error']}")

            # Marquer les étapes non-exécutées comme SKIPPED
            executed_names = {r["name"] for r in results}
            for remaining_name in steps:
                if remaining_name not in executed_names:
                    results.append({
                        "name": remaining_name,
                        "status": "SKIPPED",
                        "duration": 0.0,
                        "error": f"Phase arrêtée après échec de '{step_name}'",
                    })
            critical_failure = step_name
            break

    if critical_failure:
        raise RuntimeError(
            f"Étape critique '{critical_failure}' en échec : {result['error']}"
        )

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Résumé d'exécution
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(results: list[dict], title: str = "RÉSUMÉ D'EXÉCUTION") -> None:
    """Affiche un tableau récapitulatif de l'exécution d'une phase."""
    total_duration = sum(r["duration"] for r in results)
    icons = {"OK": "✓", "FAILED": "✗", "SKIPPED": "⊘", "DRY-RUN": "○"}

    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print(f"  {'Étape':<25} {'Statut':<10} {'Durée':>8}")
    print("-" * 60)

    for r in results:
        icon = icons.get(r["status"], "?")
        duration = f"{r['duration']:.1f}s" if r["duration"] > 0 else "—"
        print(f"  {icon} {r['name']:<23} {r['status']:<10} {duration:>8}")

        if r["error"] and r["status"] == "FAILED":
            err_short = (
                r["error"][:80] + "..." if len(r["error"]) > 80 else r["error"]
            )
            print(f"    └─ {err_short}")

    print("-" * 60)
    print(f"  {'TOTAL':<25} {'':<10} {total_duration:.1f}s")
    print("=" * 60)

    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        print(f"\n  ✗ Phase terminée avec {len(failed)} erreur(s)\n")
    elif all(r["status"] in ("OK", "DRY-RUN") for r in results):
        print("\n  ✓ Phase terminée avec succès\n")
    else:
        print("\n  ⊘ Phase interrompue (étapes ignorées)\n")
