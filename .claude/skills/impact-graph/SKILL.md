---
name: impact-graph
description: Dependency-graph inspection protocol (Python → DuckDB → dbt → Prefect) to assess global impact BEFORE modifying any table, model, script or config in Projet 3-Étoiles.
---

# Impact graph protocol

Goal: know every producer, consumer, test and orchestrator step of the thing you change, before editing. Use native tools (no plugin required). Read-only on prod DB (`read_only=True`).

## Step 1 — Locate the node
Identify layer: Python script / silver table / dbt model / mart / ML artifact / config.

## Step 2 — dbt graph (from `dbt_project/`)
- `dbt parse` if `target/manifest.json` may be stale.
- Ancestors + descendants: `dbt ls --select +<model>+ --output name`. Descendants only: `<model>+`.
- Tests on the node: `dbt ls --select <model> --resource-type test`. Macros used: grep `macros/`.
- Columns: grep `schema.yml` blocks, `sources.yml`, and downstream SQL for the column name.

## Step 3 — Python ↔ DuckDB edges
- Readers/writers of a table: grep `schema.table` and bare table name in `pipelines/`, `app/`, `tools/`, `tests/`, `config.yaml`, `config/models.yml`.
- Python-written tables read by dbt are declared in `sources.yml` (silver, intermediate, machine_learning, prediction). Hybrid chains (xT, PSxG, Spark) hide the order from dbt: check them explicitly.
- GE suites: `tests/great_expectations/<schema>/<table>*.yml` + bridge `tests/unit/test_<table>_ge.py`.

## Step 4 — Orchestrator (Prefect)
- Which step builds it: grep the model/tag in `pipelines/run_features_engineering.py` (`TABLES_UPDATE`, `TABLES_YEARLY`), `run_ml.py` (`STEP_NAMES`), `run_ingest.py`, `run_predict.py`, `dbt_helpers.py` selectors.
- A model in no step is never built by the pipeline (marts are intentionally outside).
- Cadence: daily / weekly / yearly (`run_pipeline_main.py`).

## Step 5 — Rebuild cost and propagation
- Incremental anti-join: a correction on existing rows needs `--full-refresh` of the model and its incremental descendants. Flag it.
- Big models (events, qualifiers, possession chains ~60M events): hours, memory risk → season-by-season or Spark. Estimate before running.
- Anything on `db/football.duckdb` that is destructive or slow → PMO approval.

## Step 6 — Output (feeds the Phase 1 plan, French)
| Élément | Détail |
|---|---|
| Amont | models / tables / scripts |
| Aval | models / marts / ML trainers / app |
| Tests | dbt, GE, pytest concernés |
| Étape Prefect | step + cadence |
| Rebuild | nécessaire ? full-refresh ? durée estimée |
| Risque | leakage, grain, cassures de colonnes |
Mark anything not confirmed by a command as "À vérifier".
