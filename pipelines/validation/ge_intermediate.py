"""
ge_intermediate.py — Validation Great Expectations de la couche Intermediate
=============================================================================
Délègue à ge_suites_runner.run_ge_suites_for_layer("intermediate"), qui
parcourt tous les fichiers tests/great_expectations/intermediate/*.yml et
exécute chaque suite via le runner pandas-natif partagé avec les bridges
pytest.

Interface publique inchangée par rapport à la version legacy :
validate_intermediate() est appelée depuis run_validation.py puis relayée
au flow Prefect.

Suites couvertes (au 2026-09-09) — 47 tables sous tests/great_expectations/intermediate/ :
    - int_fbref_* (keeper, shooting, misc, schedule)
    - int_understat_* (schedule, stats)
    - intermediate_odds
    - int_whoscored_* (14 tables : events, formations, lineup, match_index,
      match_meta, player_match, players, team_match, team_season, xt, etc.)
    - backbone_all / backbone_big5
    - intermediate_keeper_shots_faced / intermediate_keeper_season_psxg
    - intermediate_shot_creating_actions / intermediate_shots / intermediate_take_on_carries
    - intermediate_player_match_event_stats + réseau joueurs (network, passes, duels, xg_chain, ...)
    - intermediate_corner_profiles / intermediate_freekick_profiles / intermediate_team_match_tactical
    - intermediate_team_match_h2h, intermediate_threat_conceded_credits, etc.

Historique : la version legacy validait uniquement `intermediate_player_match_event_stats` avec
~25 expectations hardcodées via la lib great_expectations. Remplacée le
2026-09-09 par un appel au runner YAML-driven — désormais 47 suites couvertes,
un seul chemin partagé avec la CI pytest. Le code legacy est dans git.
"""

from __future__ import annotations

from validation.ge_suites_runner import run_ge_suites_for_layer


def validate_intermediate() -> None:
    """Lance toutes les suites tests/great_expectations/intermediate/*.yml.

    Raises:
        RuntimeError : si au moins une expectation severity=error a échoué.
        FileNotFoundError : si le dossier de suites n'existe pas.
    """
    run_ge_suites_for_layer("intermediate")
