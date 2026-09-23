"""
run_validation.py — Point d'entrée unique pour les validations Great Expectations
==================================================================================
Deux modes d'utilisation :

1. Import Python (depuis Prefect / dbt_helpers) :
       from validation.run_validation import run_validate_silver
       run_validate_silver()

2. CLI (depuis shell / cron / makefile / dbt run-operation) :
       python pipelines/validation/run_validation.py --silver
       python pipelines/validation/run_validation.py --intermediate
       python pipelines/validation/run_validation.py --gold
       python pipelines/validation/run_validation.py --all

Chaque fonction délègue à un wrapper ge_*.py, lui-même délègue au helper
ge_suites_runner.run_ge_suites_for_layer(...) qui parcourt le dossier
tests/great_expectations/<layer>/*.yml et exécute chaque suite via le runner
pandas-natif partagé avec les bridges pytest.

Fail-fast : RuntimeError levée dès qu'une couche a au moins une expectation
severity=error en violation. En mode --all, la cascade s'arrête à la première
couche qui casse (silver avant intermediate avant gold).

Code de sortie CLI :
    0 : toutes les validations demandées ont passé
    1 : au moins une couche a levé RuntimeError (ou FileNotFoundError)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Rend le fichier utilisable AUSSI en CLI directe
# (`python pipelines/validation/run_validation.py --silver`).
# On ajoute deux niveaux à sys.path :
#   - pipelines/ pour que `from validation.xxx` résolve les wrappers
#   - la racine du repo pour que le helper ge_suites_runner puisse résoudre
#     `from tests.great_expectations.runner import run_suite`
# En mode import (depuis run_features_engineering.py situé dans pipelines/),
# le hack est idempotent : pipelines/ est déjà dans sys.path via le contexte
# parent, et la racine du repo aussi si le script parent a été lancé depuis
# la racine (cas standard `python pipelines/run_features_engineering.py`).
_HERE          = Path(__file__).resolve()
_PIPELINES_DIR = _HERE.parent.parent            # pipelines/
_REPO_ROOT     = _PIPELINES_DIR.parent          # racine du repo
for _p in (_REPO_ROOT, _PIPELINES_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

from validation.ge_silver import validate_silver
from validation.ge_intermediate import validate_intermediate
from validation.ge_gold import validate_gold


# ══════════════════════════════════════════════════════════════════════════════
# Fonctions publiques — appelées par les orchestrateurs Prefect
# ══════════════════════════════════════════════════════════════════════════════

def run_validate_silver() -> None:
    """Valide la couche Silver. Lève RuntimeError si violation."""
    validate_silver()


def run_validate_intermediate() -> None:
    """Valide la couche Intermediate. Lève RuntimeError si violation."""
    validate_intermediate()


def run_validate_gold() -> None:
    """Valide la couche Gold. Lève RuntimeError si violation."""
    validate_gold()


def run_validate_all() -> None:
    """
    Valide silver → intermediate → gold en cascade.
    Fail-fast : la cascade s'arrête à la première couche qui plante.
    """
    validate_silver()
    validate_intermediate()
    validate_gold()


# ══════════════════════════════════════════════════════════════════════════════
# CLI — argparse au niveau `if __name__ == "__main__"` (charte projet)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Validations Great Expectations par couche du warehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python pipelines/validation/run_validation.py --silver
  python pipelines/validation/run_validation.py --intermediate
  python pipelines/validation/run_validation.py --gold
  python pipelines/validation/run_validation.py --all      # silver → intermediate → gold
""",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--silver", action="store_true",
        help="Valide la couche silver (tests/great_expectations/silver/*.yml)",
    )
    group.add_argument(
        "--intermediate", action="store_true",
        help="Valide la couche intermediate (tests/great_expectations/intermediate/*.yml)",
    )
    group.add_argument(
        "--gold", action="store_true",
        help="Valide la couche gold (tests/great_expectations/gold/*.yml)",
    )
    group.add_argument(
        "--all", action="store_true",
        help="Valide silver → intermediate → gold en cascade (fail-fast)",
    )

    args = parser.parse_args()

    try:
        if args.silver:
            run_validate_silver()
        elif args.intermediate:
            run_validate_intermediate()
        elif args.gold:
            run_validate_gold()
        elif args.all:
            run_validate_all()
    except (RuntimeError, FileNotFoundError) as e:
        logger.error(f"✗ Validation échouée : {e}")
        sys.exit(1)

    sys.exit(0)
