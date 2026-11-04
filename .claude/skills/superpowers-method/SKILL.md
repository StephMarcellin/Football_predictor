---
name: superpowers-method
description: Mandatory 3-phase workflow for every task on Projet 3-Étoiles — (1) short plan to the PMO, (2) TDD execution with empirical validation, (3) concise report. Use at the start of any feature, fix, refactor or investigation.
---

# Superpowers method

Stéphane = PMO. Claude = tech team. All messages to him in FRENCH.

## Phase 1 — Frame (≤10 lines, French)
Run skill `impact-graph` first. Then state:
- Goal (1 line) and what is out of scope.
- Impact: layers/models/scripts/orchestrator steps touched, rebuild cost.
- Test plan: the failing check you will write first.
- Risks; open questions.
Gate: proceed WITHOUT waiting if the change is reversible, local, and inside an already-agreed scope. STOP and wait for PMO approval if it touches architecture/roadmap, prod data, a settled decision (see CLAUDE.md rule 7), or has an unclear requirement. Ambiguity → ask ONE precise question, do not guess.

## Phase 2 — Execute (TDD + validation)
1. Red: write the check that fails first (dbt test / GE suite via `data-test-generator` / pytest / SQL assertion).
2. Green: smallest change that passes. Surgical edits, no whole-file rewrites.
3. Run on dev data (`--target dev`, one season) before anything touching prod.
4. Empirical validation: row counts vs before, grain uniqueness, NULL rates, distributions, anti-leakage check, and one "reference match" check for computed values.
5. Run skill `qa-reviewer-duckdb-dbt` on the diff. Fix findings.
6. Commit atomically via skill `release-manager`.
Debugging: reproduce → one hypothesis → evidence (query/log) → fix. Never patch on a guess. Never claim success without command output.

## Phase 3 — Report (≤15 lines, French)
- Fait: what changed.
- Impact: layers/models affected, rebuild needed (yes/no, cost).
- Preuves: commands run and key numbers.
- Risques / compromis.
- Décisions attendues du PMO.
- Commits / PR.
- Candidats ADR (for Stéphane's Obsidian).
No recap of steps taken, no teaching.
