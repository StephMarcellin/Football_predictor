-- Tables residuelles laissees par dbt apres --full-refresh (14/10/2026).
-- dbt renomme l'ancienne table en __dbt_backup puis la supprime ; la
-- suppression n'a pas eu lieu. Aucune n'est referencee par un modele.

DROP TABLE IF EXISTS "gold"."gold_corridor_matchup__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_keeper_season_lag__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_player_match_rolling_profile__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_player_zone_season_lag__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_team_match__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_team_match_corners_rolling__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_team_match_freekicks_rolling__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_team_match_h2h__dbt_backup";
DROP TABLE IF EXISTS "gold"."gold_team_match_press_matchup__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_completed_passes__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_corner_profiles__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_duel_pairs__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_event_values__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_fouls_drawn__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_freekick_profiles__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_pass_blocks__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_pass_network_edges__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_pass_network_player_betweenness__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_pass_network_player_centrality__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_pass_network_team_match__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_penalties__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_player_match_event_stats__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_player_zone_transitions__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_possession_chains__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_shot_creating_actions__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_shots__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_substitutions__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_take_on_carries__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_team_match_backbone__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_team_match_h2h__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_team_match_tactical__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_threat_conceded_credits__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_whoscored_team_match_minute__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_whoscored_team_match_totals__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_xg_chain_credits__dbt_backup";
DROP TABLE IF EXISTS "intermediate"."intermediate_xt_actions__dbt_backup";
