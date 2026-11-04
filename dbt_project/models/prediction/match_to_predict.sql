{{
    config(
        materialized='incremental',
        schema='prediction',
        alias='match_to_predict',
        full_refresh=false
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- prediction.match_to_predict — table d'ENTRÉE : les matchs que le modèle doit prédire.
-- Remplie à la main (INSERT dans DBeaver) ou par script ; dbt ne fait que CRÉER la table.
--
-- Pourquoi « incremental » avec un SELECT qui ne renvoie aucune ligne :
--   • au premier `dbt run`, la table est créée (types fixés par les CAST ci-dessous) ;
--   • aux runs suivants, dbt n'ajoute que les lignes du SELECT, donc RIEN : les lignes saisies
--     à la main sont conservées (une matérialisation `table` les effacerait à chaque run) ;
--   • full_refresh=false : même `dbt run --full-refresh` ne supprime pas la table.
-- Pour la vider volontairement : DELETE FROM prediction.match_to_predict (SQL direct).
--
-- Format = intermediate_fbref_schedule : UNE ligne PAR ÉQUIPE et par match (2 lignes/match).
--   dt_date           : date du match
--   str_team_name     : nom de l'équipe de la ligne (nom de club OU alias de team_mapping)
--   str_opponent_name : nom de son adversaire
--   str_venue         : 'Home' | 'Away' | 'Neutral' (du point de vue de str_team_name)
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    CAST(NULL AS DATE)    AS dt_date,
    CAST(NULL AS VARCHAR) AS str_team_name,
    CAST(NULL AS VARCHAR) AS str_opponent_name,
    CAST(NULL AS VARCHAR) AS str_venue
WHERE FALSE
