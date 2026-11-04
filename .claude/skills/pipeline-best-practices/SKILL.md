---
name: pipeline-best-practices
description: Production standards for Python/SQL/dbt/ML (MLOps) code in Projet 3-Étoiles — idempotence, config, logging, memory, incremental design, anti-leakage ML, MLflow. Apply when writing or reviewing pipeline code.
---

# Pipeline best practices

## Python
- Type hints; `pathlib` with `ROOT_DIR` (from `orchestrator_common`); no hardcoded absolute paths; config in `config.yaml`, secrets in `.env`.
- loguru logging, no `print` in pipeline code; fail loudly with context, never swallow exceptions.
- Idempotent steps: re-running yields the same state (`ADD COLUMN IF NOT EXISTS`, `CREATE OR REPLACE`, keyed upserts). Expose `--reset-cols` / `--coverage-only` where relevant.
- `argparse` under `__main__` only; `main()` takes parameters. Polars for transforms; DuckDB for storage/SQL.
- Heavy steps run in subprocess (isolation of argv/exit/cwd). Prefect task retries via `orchestrator_common`.
- Pin dependencies in `requirements.txt`; no unpinned installs.

## SQL / dbt
- CTE-per-concern; final SELECT reads CTEs; Jinja macros for repeats; `SELECT * EXCLUDE`.
- Explicit grain + `unique_key` on incremental models; know the anti-join limit (corrections need full-refresh).
- Memory: split by season (`target_season`), `threads` low, push the heaviest transforms to Spark with Parquet re-injection as tables.
- Prefix conventions (columns and models) per CLAUDE.md. Document every model/column in `schema.yml`.
- Tests: dbt = structure, GE = signal, pytest = Python logic (`data-test-generator`).

## ML (LightGBM / MLflow)
- Time-ordered splits only; no random split across seasons; no leakage (CLAUDE.md data rules).
- Seeds: average over 5 seeds for model 1; calibrate probabilities (isotonic); track params/metrics/artifacts in MLflow; log data version (mart + season range).
- Odds are a value reference, never a feature (V2 deferred). Backtest from `gold_odds_pinnacle`.
- Train via `run_ml.py` steps, not legacy scripts. Predictions: `machine_learning.predictions_*` (`run_predict.py --write`) and `prediction.match_predicted` (`pipelines/prediction/predict_matches.py`).

## Ops
- Dev first (`--target dev`, one season), prod after validation.
- Orchestrator step must exist for any model the pipeline must keep fresh.
- Log durations and row counts per step to spot regressions.
