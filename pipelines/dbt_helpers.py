"""
dbt_helpers.py — Fonctions utilitaires pour piloter dbt depuis Python
======================================================================
Utilisé par les orchestrateurs qui ont besoin de lancer dbt run, dbt test,
dbt seed depuis Python (run_features_engineering, run_pipeline, run_ml).

Séparé de orchestrator_common.py pour ne pas mélanger la plomberie Prefect
(générique à toutes les phases) et la logique dbt (spécifique aux phases
qui transforment des tables).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from loguru import logger

from orchestrator_common import ROOT_DIR


# ══════════════════════════════════════════════════════════════════════════════
# dbt seed
# ══════════════════════════════════════════════════════════════════════════════

def run_dbt_seed(refresh: bool = False) -> None:
    """Lance dbt seed depuis dbt_project/. Lève RuntimeError si échec."""
    dbt_dir = ROOT_DIR / "dbt_project"
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


# ══════════════════════════════════════════════════════════════════════════════
# dbt run
# ══════════════════════════════════════════════════════════════════════════════

def run_dbt_run(
    select: str = None, exclude: str = None, full_refresh: bool = False
) -> None:
    """Lance dbt run depuis dbt_project/. Lève RuntimeError si échec."""
    dbt_dir = ROOT_DIR / "dbt_project"
    log_path = ROOT_DIR / "logs" / "dbt_run_last.log"
    if not dbt_dir.exists():
        raise FileNotFoundError(f"dbt_project/ introuvable : {dbt_dir}")

    cmd = ["dbt", "run", "--profiles-dir", str(Path.home() / ".dbt")]
    if select:
        cmd += ["--select", select]
    if exclude:
        cmd += ["--exclude", exclude]
    if full_refresh:
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
        raise RuntimeError(f"dbt run a échoué :\n{result.stderr[-1000:]}")


# ══════════════════════════════════════════════════════════════════════════════
# dbt test
# ══════════════════════════════════════════════════════════════════════════════

def run_dbt_test() -> None:
    """Lance dbt test depuis dbt_project/.

    Lève RuntimeError uniquement si returncode == 2 (erreur de configuration).
    Les échecs de test (returncode 1) ne lèvent PAS — c'est check_dbt_test_results()
    qui décide si les erreurs sont bloquantes.
    """
    dbt_dir = ROOT_DIR / "dbt_project"
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

    if result.returncode == 2:
        raise RuntimeError(
            f"dbt test n'a pas pu s'exécuter (erreur de configuration) :\n"
            f"{result.stderr[-1000:]}"
        )


def check_dbt_test_results() -> None:
    """Parse le log dbt test et lève RuntimeError si au moins 1 ERROR détecté.

    Les WARNs sont ignorés — seuls les ERRORs bloquent le pipeline.
    Doit être appelé APRÈS run_dbt_test().
    """
    log_path = ROOT_DIR / "logs" / "dbt_test_last.log"
    content = log_path.read_text(encoding="utf-8")

    match = re.search(r"Completed with (\d+) error", content)
    if match:
        n_errors = int(match.group(1))
        if n_errors > 0:
            error_lines = [
                l for l in content.splitlines() if "Failure in test" in l
            ]
            raise RuntimeError(
                f"dbt test : {n_errors} erreur(s) détectée(s)\n"
                + "\n".join(error_lines)
            )
