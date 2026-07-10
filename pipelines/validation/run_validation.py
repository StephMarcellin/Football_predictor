"""
run_validation.py — Point d'entrée unique pour les validations Great Expectations
==================================================================================
Appelé par run_pipeline.py comme étape Prefect.
Deux fonctions exposées :
    - run_validate_silver() : à brancher entre process et dbt_run
    - run_validate_gold()   : à brancher entre dbt_test et train
"""

from __future__ import annotations

from validation.ge_silver import validate_silver
from validation.ge_gold import validate_gold


def run_validate_silver() -> None:
    """Valide la couche Silver. Lève RuntimeError si violation — stoppera le pipeline."""
    validate_silver()


def run_validate_gold() -> None:
    """Valide la couche Gold. Lève RuntimeError si violation — stoppera le pipeline."""
    validate_gold()

def run_validate_intermediate() -> None:
    """Valide la couche intermediate. Lève RuntimeError si violation — stoppera le pipeline."""
    validate_intermediate()