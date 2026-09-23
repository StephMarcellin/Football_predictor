"""
Bridge pytest → suite Great Expectations pour intermediate.int_fbref_shooting.

Ce test ne valide pas de logique Python — int_fbref_shooting est un modèle dbt
SQL pur. Il rend simplement la suite GE visible dans le rapport pytest
(utile pour la CI).

Selon la charte : dbt=structure, GE=signal, pytest=logique code.
Cette table est produite par du SQL pur → aucun test de logique métier ici.
"""
from pathlib import Path
import pytest

from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations"
    / "intermediate"
    / "int_fbref_shooting.yml"
)


def test_ge_suite_int_fbref_shooting():
    """Exécute la suite GE et échoue si une expectation `error` échoue."""
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")

    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
