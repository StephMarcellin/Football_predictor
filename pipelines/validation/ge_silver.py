"""
ge_silver.py — Validation Great Expectations de la couche Silver
================================================================
Délègue à ge_suites_runner.run_ge_suites_for_layer("silver"), qui parcourt
tous les fichiers tests/great_expectations/silver/*.yml et exécute chaque
suite via le runner pandas-natif partagé avec les bridges pytest.

Interface publique inchangée par rapport à la version legacy : la fonction
validate_silver() est appelée depuis pipelines/validation/run_validation.py
puis relayée au flow Prefect.

Suites couvertes (au 2026-09-09) :
    - fbref_keeper, fbref_shooting, fbref_misc, fbref_schedule
    - understat_schedule, understat_stats
    - odds
    - whoscored_team_season
    - stg_whoscored_* (formations_ref, players_ref, match_index, match_meta,
      urls, team_match, formations, player_match, events × 3 push-down)

Historique : cette fonction lançait auparavant 9 expectations en Python via
la lib great_expectations (in-memory context, DataFrames pandas hardcodés).
Remplacée le 2026-09-09 par un appel au runner YAML-driven — un seul chemin
de validation partagé avec la CI pytest. Le code legacy est dans git.
"""

from __future__ import annotations

from validation.ge_suites_runner import run_ge_suites_for_layer


def validate_silver() -> None:
    """Lance toutes les suites tests/great_expectations/silver/*.yml.

    Raises:
        RuntimeError : si au moins une expectation severity=error a échoué.
        FileNotFoundError : si le dossier de suites n'existe pas.
    """
    run_ge_suites_for_layer("silver")
