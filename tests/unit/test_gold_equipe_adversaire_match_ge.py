"""
Bridge pytest → suite Great Expectations pour gold.equipe_adversaire_match.

Auto-généré 2026-09-09.
"""
from pathlib import Path
import pytest

from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations"
    / "gold"
    / "equipe_adversaire_match.yml"
)


def test_ge_suite_gold_equipe_adversaire_match():
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")
    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
