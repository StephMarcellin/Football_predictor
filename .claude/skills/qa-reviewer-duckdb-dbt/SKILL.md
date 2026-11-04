---
name: qa-reviewer-duckdb-dbt
description: QA review checklist for Python, DuckDB SQL and dbt changes on Projet 3-Étoiles (leakage, grain, naming, DuckDB 1.5 pitfalls, Windows, tests). Run on every diff before commit.
---

# QA reviewer — DuckDB / dbt / Python

Review the actual diff (`git diff` / `git diff --cached`). Verify with queries and commands, not by reading only. Output a verdict per item: PASS / FAIL / À VÉRIFIER. Report in French.

## 1. Data logic
- Grain: declared grain unique (`SELECT COUNT(*) - COUNT(DISTINCT key)`); join keys do not fan out (compare row counts before/after each join).
- Anti-leakage: windows end at `1 PRECEDING`; seasonal stats use `LAG(1)`; no current-match column reaches a feature or mart; `h2h`/rolling features excluded current match.
- Keys: `team_id` + SHA1 `match_id`; no team names post-Silver; `raw_*` columns untouched.
- WhoScored `team_id` in events is a WhoScored id, not canonical: translation via match index still required.
- Incremental models: filter is an anti-join → does the change need full-refresh of model + incremental downstream? State it explicitly.
- Computed values: one real match checked end to end (score, scorers, assists, subs, cards).

## 2. Naming / structure
- Column prefixes at start: `str_ int_ dec_ dt_ bool_`; logical type (counts = int).
- Model prefixes: `intermediate_`, `gold_` (English), `mart_`.
- CTE-per-concern, final SELECT reads CTEs only; `SELECT * EXCLUDE (...)` over long lists; Jinja macro for repeated patterns.
- No team/season/path hardcoding; no hardcoded absolute paths.

## 3. DuckDB / dbt pitfalls
- No nested window functions. No LEFT JOIN on `ref()` subquery (use CTE). `event_id` not global → `row_num`.
- `ROW_NUMBER() OVER (...) = 1` with DATE: failed in 1.5.1, À vérifier on 1.5.5 (else MAX(date)+join).
- Reserved words: `pivot` (rename CTE), quote `off`.
- `generate_schema_name` macro untouched. `dbt.is_incremental` is an attribute.
- Memory: heavy models split by season (`target_season` var) or moved to Spark; no unbounded self-join on events (~60M rows).

## 4. Tests (charter: dbt = structure, GE = signal, pytest = Python logic)
- New/changed model has: grain test, not_null on keys, relationships, accepted_values, physical constraints. Use skill `data-test-generator`.
- Run: `dbt test --select <model> --target dev` and `pytest -q` for touched Python. Quote the result.
- Never delete an existing test without a written reason.

## 5. Python / Windows
- `argparse` only under `if __name__ == "__main__":`. Numeric-prefixed scripts imported via `importlib`.
- `os.chdir` restored in `try/finally`. MLflow URI `file:///` + forward slashes.
- Logging via loguru with clear messages; exceptions not swallowed silently.
- No secrets in code/logs; `.env` and credential files untouched.
- Spark: env vars set before `import pyspark`; driver memory via `PYSPARK_SUBMIT_ARGS`.

## 6. Boundaries
- Silver logic stays Python/Polars (not dbt). dbt scope = intermediate and above.
- Corrections via versioned code, not manual UPDATE.
- Settled decisions respected (CLAUDE.md rule 7).

## Output
`Verdict: GO / NO-GO` + list of FAIL / À VÉRIFIER with file:line and the fix applied or proposed.
