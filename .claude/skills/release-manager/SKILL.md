---
name: release-manager
description: Autonomous Git release management for Projet 3-Étoiles — atomic Conventional Commits in French (feat/fix/refactor/docs…), safe staging, feature branches, push and PR via gh. Use whenever code, SQL, config or docs are ready to commit.
---

# Release manager

## Commit format
`<type>(<scope>): <sujet en français, minuscule, impératif ou participe, sans point>` (≤72 chars). Optional French body: why, not how.
- Types: `feat` (new capability/model/feature), `fix` (bug/data defect), `refactor` (no behavior change), `docs`, `test`, `chore`, `perf`, `ci`, `build`.
- Scopes seen in history: `dbt`, `ml`, `ingest`, `spark`, `ci`, `terraform`, `prediction`, `odds`.
- Example: `fix(dbt): corrige le grain de intermediate_team_match_h2h`.
- End every message with the attribution lines supplied by the harness (Co-Authored-By …). Do not invent them.

## Atomicity
- One logical change per commit; it must build/test on its own. Split mixed work by file or hunk (`git add <path>`, `git add -p` is interactive and unavailable → split by path, or use `git apply --cached` with a prepared patch).
- Order: refactor/fix before feat; code before docs. Tests ship with the code they cover.
- Never `git add -A` / `git add .`. Stage explicit paths.

## Pre-commit gate (all must hold)
1. `git diff --cached --stat` reviewed; nothing unrelated.
2. No secrets (`.env`, `config/*.json`), no DBs (`*.duckdb`, `*.tmp`), no `.tmp/`, `data/`, `mlruns/`, `*.parquet`, no file >5 MB.
3. Skill `qa-reviewer-duckdb-dbt` verdict GO; tests quoted in the report.
4. Working branch is not `main`.

## Branches, push, PR
- Name: `feature/<name>` or `fix/<name>`. Create before work starts. Never commit on `main`.
- Push the branch, then `gh pr create` with French title + body (Résumé / Impact / Tests / Risques). PR body ends with the attribution line supplied by the harness.
- Stéphane merges. Never merge, never force-push, never rewrite pushed history, never `--no-verify`.
- Auth: fine-grained token in `.tmp/gh_token` (gitignored, placed by Stéphane via Notepad, expires ~30 days). Push: `git -c http.extraHeader="Authorization: Basic $(printf 'x-access-token:%s' "$TOK" | base64 -w0)" push -u origin <branche>`. PR: REST `POST /repos/StephMarcellin/Football_predictor/pulls` with python urllib (`gh` is not installed on the device).
- NEVER print the token, never `ls` a folder that may hold it (use `wc -c`/`test -s` on the exact path), never pass it on a visible command line without masking output, never paste it in chat. If a token is exposed: tell Stéphane to revoke it.
- If push fails (auth, network), stop and report; do not retry blindly.

## Hygiene
- Fix `.gitignore` when an artifact type leaks (`.tmp/` must be ignored).
- Report commits as `hash — message` in the final French report.
