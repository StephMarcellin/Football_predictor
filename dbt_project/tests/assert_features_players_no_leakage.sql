-- Test anti-leakage de gold.features_players
-- Une première apparition de la saison (n_matchs_window = 0) ne doit avoir
-- AUCUNE feature rolling renseignée : la fenêtre exclut le match courant, donc
-- sans match précédent toutes les moyennes _5 doivent être NULL.
-- Toute ligne renvoyée = fuite d'information depuis le match courant.

SELECT
    match_id,
    team_id,
    player_id,
    n_matchs_window
FROM {{ ref('features_players') }}
WHERE n_matchs_window = 0
  AND (
        avg_xg_chain_5            IS NOT NULL
     OR avg_xg_contribution_5     IS NOT NULL
     OR avg_progressive_passes_5  IS NOT NULL
     OR avg_key_passes_5          IS NOT NULL
     OR avg_shots_5               IS NOT NULL
     OR avg_pass_share_5          IS NOT NULL
     OR avg_betweenness_5         IS NOT NULL
     OR avg_creative_rate_5       IS NOT NULL
     OR avg_aerial_win_rate_5     IS NOT NULL
     OR avg_tackle_win_rate_5     IS NOT NULL
     OR avg_defensive_actions_5   IS NOT NULL
     OR avg_clearances_5          IS NOT NULL
     OR avg_zone_x_5              IS NOT NULL
     OR avg_zone_y_5              IS NOT NULL
     OR avg_touches_5             IS NOT NULL
  )
