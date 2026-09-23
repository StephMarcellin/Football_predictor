"""
Bridge pytest → suite Great Expectations pour intermediate.backbone_big5.

Ce test ne valide pas de logique Python — backbone est un modèle dbt
SQL pur. Il rend simplement la suite GE visible dans le rapport pytest.

Selon la charte : dbt=structure, GE=signal, pytest=logique code.
"""
from pathlib import Path
import pytest

from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations"
    / "intermediate"
    / "backbone_big5.yml"
)


def test_ge_suite_backbone_big5():
    """Exécute la suite GE et échoue si une expectation `error` échoue."""
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")

    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
