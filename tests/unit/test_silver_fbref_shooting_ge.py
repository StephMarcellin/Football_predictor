"""
Bridge pytest → suite Great Expectations pour silver.fbref_shooting.

Ce test ne valide pas de logique Python — silver.fbref_shooting est produit par
02_process.py hors dbt. Il rend simplement la suite GE visible dans le rapport
pytest (utile pour la CI). C'est la première couche testable après scraping.
"""
from pathlib import Path
import pytest

from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations"
    / "silver"
    / "fbref_shooting.yml"
)


def test_ge_suite_silver_fbref_shooting():
    """Exécute la suite GE et échoue si une expectation `error` échoue."""
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")

    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
