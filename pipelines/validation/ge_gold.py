"""
ge_gold.py — Validation Great Expectations de la couche Gold
============================================================
Délègue à ge_suites_runner.run_ge_suites_for_layer("gold"), qui parcourt
tous les fichiers tests/great_expectations/gold/*.yml et exécute chaque
suite via le runner pandas-natif partagé avec les bridges pytest.

Interface publique inchangée par rapport à la version legacy :
validate_gold() est appelée depuis run_validation.py puis relayée
au flow Prefect. Un échec ici doit empêcher l'entraînement du modèle —
c'est la dernière ligne de défense avant que le pipeline ML ne
consomme des features potentiellement corrompues.

Suites couvertes (au 2026-09-09) — 14 modèles gold :
    - gold_team_match, gold_team_match_h2h, gold_team_match_press_matchup,
      gold_team_match_corridor_matchup, gold_team_match_keeper, gold_team_match_lineup_strength
    - gold_keeper_season_lag, gold_player_match_scorer, gold_player_match_rolling_profile, gold_player_zone_season_lag
    - gold_team_match_corners_rolling, gold_team_match_freekicks_rolling
    - gold_team_corridor_profile, gold_corridor_matchup

Historique : la version legacy validait uniquement `gold.features_final`
avec ~30 expectations hardcodées via la lib great_expectations. Remplacée
le 2026-09-09 par un appel au runner YAML-driven — désormais 14 modèles
couverts, un seul chemin partagé avec la CI pytest. Le code legacy est
dans git.
"""

from __future__ import annotations

from validation.ge_suites_runner import run_ge_suites_for_layer


def validate_gold() -> None:
    """Lance toutes les suites tests/great_expectations/gold/*.yml.

    Raises:
        RuntimeError : si au moins une expectation severity=error a échoué.
        FileNotFoundError : si le dossier de suites n'existe pas.
    """
    run_ge_suites_for_layer("gold")
