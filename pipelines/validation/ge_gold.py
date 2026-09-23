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
    - equipe_match, equipe_adversaire_match, equipe_confrontation_match,
      equipe_confrontation_zone, equipe_gardien_match, equipe_lineup_match
    - gardien_saison, joueur_match, joueur_saison, joueur_zone_saison
    - rolling_corners, rolling_freekicks
    - team_corridor_profile, zone_confrontation_match

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
