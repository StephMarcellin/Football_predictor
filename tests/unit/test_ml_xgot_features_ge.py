"""
Bridge pytest → suite Great Expectations pour machine_learning.xgot_features.

Auto-généré 2026-09-09.
"""
from pathlib import Path
import pytest

from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations"
    / "machine_learning"
    / "xgot_features.yml"
)


def test_ge_suite_machine_learning_xgot_features():
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")
    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
