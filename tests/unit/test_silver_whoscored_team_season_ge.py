"""Bridge pytest → suite GE pour silver.whoscored_team_season."""
from pathlib import Path
import pytest
from tests.great_expectations.runner import run_suite

SUITE_PATH = (
    Path(__file__).resolve().parents[1]
    / "great_expectations" / "silver" / "whoscored_team_season.yml"
)


def test_ge_suite_silver_whoscored_team_season():
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")
    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
