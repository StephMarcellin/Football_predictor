"""Bridge pytest → intermediate.events_qual (push-down light 2026-09-09)."""
from pathlib import Path
import pytest
from tests.great_expectations.runner import run_suite

SUITE_PATH = (Path(__file__).resolve().parents[1] / "great_expectations"
              / "intermediate" / "events_qual.yml")

def test_ge_suite_intermediate_events_qual():
    if not SUITE_PATH.exists():
        pytest.skip(f"Suite YAML absente : {SUITE_PATH}")
    result = run_suite(SUITE_PATH)
    assert result.success, "\n" + result.report()
