"""
spark_helpers.py — Lancement des jobs Spark depuis l'orchestrateur
===================================================================
Pendant de dbt_helpers.py : la plomberie propre à Spark, séparée de la
plomberie Prefect (orchestrator_common) et de la plomberie dbt.

Pourquoi un subprocess plutôt qu'un import + main() :

1. La JVM ne redémarre pas. Un SparkContext arrêté ne peut pas être recréé
   proprement dans le même processus — un retry Prefect planterait sur le
   second essai.
2. PYSPARK_SUBMIT_ARGS doit être posé AVANT l'import de pyspark. Dans un
   processus long-vivant où pyspark a peut-être déjà été importé, on n'a
   aucune garantie sur cet ordre.
3. La mémoire. Un driver Spark à 8 Go dans le processus qui orchestre aussi
   dbt et LightGBM est une bombe à retardement.

C'est le même raisonnement, et le même contrat, que run_phase_subprocess()
dans run_pipeline_main.py : streaming ligne par ligne, RuntimeError si code
retour ≠ 0 → capté par run_step() → retries Prefect.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from loguru import logger

from orchestrator_common import ROOT_DIR

SPARK_DIR = ROOT_DIR / "pipelines" / "spark"


def run_spark_job(script_name: str, extra_args: list[str] | None = None) -> None:
    """
    Lance un script du dossier pipelines/spark/ comme processus indépendant.

    Args:
        script_name : nom du fichier, ex. "spark_events.py"
        extra_args  : arguments passés au script, ex. ["--season", "2023-2024"]

    Le streaming ligne par ligne (Popen + bufsize=1) est indispensable :
    sans lui, aucun retour visuel pendant toute la durée du job.
    """
    script = SPARK_DIR / script_name
    if not script.exists():
        raise FileNotFoundError(f"Script Spark introuvable : {script}")

    cmd = [sys.executable, str(script), *(extra_args or [])]
    logger.info(f"→ Subprocess Spark : {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # un seul flux, l'ordre des messages est préservé
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        # raw=True : la ligne fille a déjà son horodatage loguru, on ne re-préfixe pas.
        logger.opt(raw=True).info(line if line.endswith("\n") else line + "\n")

    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"{script_name} a échoué (code retour {code}).")