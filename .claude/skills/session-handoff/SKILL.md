---
name: session-handoff
description: End-of-palier / end-of-session ritual for Projet 3-Étoiles — updates docs/ETAT_COURANT.md and docs/JOURNAL_RESULTATS.md, commits, mirrors to the Project docs, lists what to re-grant next session. Use when a palier ends, the budget limit is hit, or the PMO says the session is closing.
---

# Session handoff

Goal: the next session resumes in one sentence ("lis docs/ETAT_COURANT.md et continue") with zero re-briefing. Write in French, terse.

## Steps
1. `docs/ETAT_COURANT.md`: update palier statuses, decisions taken, "À vérifier" (close or add), "Prochaine action" (one concrete next step), "Budget consommé" (tool calls used per palier), date.
2. `docs/JOURNAL_RESULTATS.md`: append one line per finished experiment (date · what · numbers with IC95 · decision). Never leave a result only in chat.
3. Heavy outputs stay in `reports/` (untracked); reference their filename in the journal.
4. Commit atomically via skill `release-manager` on the thematic branch; push; give the PR link if `gh` is unavailable.
5. Mirror `ETAT_COURANT.md` to the Project docs (`claude/etat_courant.md`) with `project_write` so other surfaces see it.
6. Final French message: done / next action / decisions awaiting the PMO / what to re-grant next session (folder access, delete permission) and the command `bash tools/vm_bootstrap.sh`.

## Rules
- No narrative recap, no teaching. Facts, numbers, next step.
- Never record a number that was not produced by a command in this or a logged earlier session.
