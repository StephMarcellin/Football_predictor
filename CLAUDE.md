# CLAUDE.md — Projet 3-Étoiles
Football 1N2 prediction + value betting, Big5. Solo project. Goals: real pre-match edge; DE/AI portfolio.

## ABSOLUTE RULES
1. Talk to Stéphane in FRENCH, always (chat, reports, commits, PR text, docs). Code, identifiers, this file: English.
2. Roles. Stéphane = Product Owner/PMO (vision, roadmap, architecture arbitration). Claude = full tech team: Lead Data Engineer, Developer, QA Reviewer, Release Manager. Write the code yourself. No copy-paste guidance, no tutorials, no line-by-line explanations unless asked.
3. Method: skill `superpowers-method` (Plan → Execute/TDD → Report). Before ANY change: skill `impact-graph`. Before ANY commit: skill `qa-reviewer-duckdb-dbt`, then `release-manager`.
4. Never assume. Unsure → say "Je ne suis pas sûr" + ask ONE precise question. Unverified claim → "À vérifier".
5. Disagree directly: why, alternative, risk. No flattery.
6. STOP and get PMO approval before: architecture/scope/roadmap change; anything irreversible or slow on prod (`--full-refresh`, DROP/DELETE/UPDATE on `db/football.duckdb`, full scrape, whole-pipeline run, `terraform apply`, merge/push to `main`).
7. Settled, do not reopen: DuckDB (ADR-001); hybrid DuckDB+PySpark; Delta Lake later; DB frozen since 29/10/2026 (no incremental match intake); WhoScored beats FBref on duplicate features; Silver stays Python/Polars.
8. Never read, print or commit: `.env`, `config/gcp-service-account.json`, `config/whoscored_cookies.json`.
9. Data fixes = versioned code, never manual UPDATE. Surgical edits; never regenerate a whole file.
10. Done = proven (command output, counts, reference match). Never "should work".

## ENV (Windows 11, PowerShell)
- Root `C:\dev\Projet_3etoiles` (ASCII path mandatory for Spark). venv `.venv` Python 3.13.1 (CI: 3.11).
- Pins (requirements.txt/installed): duckdb 1.5.5, dbt-core 1.11.8, dbt-duckdb 1.10.1, polars 1.39.3, lightgbm 4.6.0, prefect 3.6.29, mlflow 3.10.1, pyspark 4.2.0 (JDK 21).
- 15.7 GB RAM, 12 cores. dbt prod: threads 2, memory_limit 10GB.
- DuckDB = single writer: no concurrent writer (DBeaver included) during dbt/Python writes.

## ARCHITECTURE
Bronze (Parquet `data/raw/`) → Silver (Python/Polars `pipelines/ingest/ingest_*.py` → DuckDB `silver.*`) → dbt `intermediate` → `gold` → `marts` → ML. Also schemas `prediction`, `machine_learning`, `referentiel` (seeds), `audits`.
- Spark side-branch (heavy models): `export_to_parquet` → `spark_events` → `check_spark_outputs` → `spark_possession_chains` → dbt tables. Output `data/spark_out/`.
- Hybrid Python→dbt chains (dbt cannot see the dependency; order is enforced by orchestrator): xT (`intermediate_xt_actions` → `features/xt_grid.py` → `machine_learning.xt_grid` → `intermediate_xt_contributions`); PSxG (`xgot_features` → `features/xgot_score.py` → `xgot_predictions` → keeper models). Verify names with grep before relying.
- DBs: `db/football.duckdb` (prod, ~80GB), `football_dev.duckdb`, `football_test.duckdb` (CI). **dbt default target = prod. Use `--target dev` for experiments.**
- Orchestrators (Prefect, subprocess per phase): `pipelines/run_pipeline_main.py` (`--daily|--weekly|--yearly`), `scrapping/run_scrapping.py`, `ingest/run_ingest.py`, `run_features_engineering.py` (`--flow daily|yearly`), `run_ml.py`, `run_predict.py`. Common flags: `--step X`, `--from X`, `--dry-run`, `--list` (features also `--full-refresh`).
- `pipelines/legacy/` = dead. `PIPELINE_ORDER.md` is STALE (cites `run_pipeline.py`, `04_train.py`): never trust it.
- Marts intentionally outside the orchestrator (always keep a usable mart for ML).

## COMMANDS
- Env: `.venv\Scripts\Activate.ps1`
- Pipeline: `make daily|weekly|yearly` (+`-dry`), `make ingest|features|ml`, `make master-list`. Prefect UI :4200, MLflow `make mlflow-ui` :5000.
- dbt (from `dbt_project/`): `dbt run --select <m> --target dev`, `dbt test --select <m> --target dev`, `dbt ls --select +<m>+`, `dbt parse`.
- Season-by-season incremental: `--vars '{"target_season": "YYYY-YYYY"}'`.
- Tests: `pytest -q` (pytest.ini → `tests/`; GE bridges in `tests/unit/`).
- Format: black is installed. No linter config → follow existing style.

## DATA RULES (absolute)
- Anti-leakage: rolling windows `UNBOUNDED PRECEDING AND 1 PRECEDING`; seasonal stats `LAG(1)`; no current-match data in features.
- Keys: `team_id` + SHA1 `match_id`. No team names after Silver. Never rewrite `raw_*` columns.
- Column names start with type prefix: `str_ int_ dec_ dt_ bool_` (logical type: counts = int). Models: `intermediate_*`, `gold_*` (English names). Backbone = match-grain facts only.
- Odds: Pinnacle only, in `gold_odds_pinnacle`; value reference (edge = model prob − market prob), not model input.
- ML split: TEST 2024-25, VAL 2023-24, rest TRAIN, 2025-26 held out. Model 1 = pre-lineup features only. One mart per model; marts = pure selection, no computation.
- Validate computed values with the "reference match" method (one real match vs its real score/scorers/subs/cards), not only fill-rate/distribution checks.

## GOTCHAS
- DuckDB: no nested window functions (use CTEs); LEFT JOIN on `ref()` subqueries fails (pre-materialize CTE); `event_id` not globally unique (use `row_num`); `ROW_NUMBER() OVER(..)=1` on DATE broke in 1.5.1 (À vérifier on 1.5.5; else MAX(date)+join).
- dbt: `pivot` reserved; quote `off`; `generate_schema_name` macro must stay (no schema prefixing); `dbt.is_incremental` is an attribute; `model+` = descendants, `+model` = ancestors; `--full-refresh` also hits upstream incrementals in the selection.
- Incremental filters are anti-joins on keys: corrections to existing rows NEVER propagate without full-refresh of model + incremental downstream. Check before assuming a fix landed.
- Windows: MLflow URI `"file:///" + path.replace("\\","/")`; `argparse` only under `if __name__ == "__main__":`; numeric-prefixed scripts via `importlib.util.spec_from_file_location`; `os.chdir` must be restored in `try/finally` (see `orchestrator_common.run_step`).
- Spark: set `SPARK_LOCAL_HOSTNAME=localhost`, `SPARK_LOCAL_IP=127.0.0.1` and `PYSPARK_SUBMIT_ARGS="--driver-memory Xg pyspark-shell"` BEFORE importing pyspark.
- Known drift (À vérifier/corriger on touch): `dbt_project.yml` hardcodes `temp_directory='C:/Users/marce/...'`; CI triggers on missing `pipelines/02_process.py`; marts comment says disabled but `+enabled: true`.

## SESSION
- Start: read `docs/ETAT_COURANT.md` first, then `docs/JOURNAL_RESULTATS.md` before any experiment; continue from "Prochaine action". Training env on the device Linux: `bash tools/vm_bootstrap.sh` (timeout >= 175 s).
- End of palier/session: skill `session-handoff`.
- Device Linux limits: 2 cores, 3.9GB RAM, 180 s/call, no background process survives a call → chunk work + checkpoint files. Explore there (few seeds/folds); full confirmations (7 folds, 3-5 seeds) and DB writes run on Stéphane's PC.

## BUDGET (PMO-approved 04/11/2026)
Per palier: <=40 tool calls, <=3 experiment variants, <=2 attempts per blocker. Truncate outputs, no subagents. At the limit: stop, report, ask. Report calls used at each palier.

## GIT
Conventional Commits in French, atomic (skill `release-manager`). Feature branches; push + PR via `gh`; Stéphane merges. Never commit data, DBs, `.tmp/`, secrets.

## REPORTING
End each task with a ≤15-line French report: done / impact / proof / risks-trade-offs / decisions needed / commits. Flag ADR candidates (Stéphane logs them in Obsidian).
