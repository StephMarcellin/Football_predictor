{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_team_match'
    )
}}

-- Stats d'équipe officielles WhoScored par match, identités normalisées.
-- Même patron : conversion team_id WhoScored → canonique + match_id unifié via
-- int_whoscored_match_index. On conserve stats_json brut (35 métriques par
-- minute) : l'extraction des totaux se fera plus tard, avec une logique
-- cumulatif/incrémental vérifiée.

WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_team_match') }}
),

match_index AS (
    SELECT
        ws_match_id,
        match_id,
        ws_home_team_id,
        ws_away_team_id,
        team_id     AS home_team_id,
        opponent_id AS away_team_id
    FROM {{ ref('int_whoscored_match_index') }}
)

SELECT
    idx.match_id,

    CASE
        WHEN t.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN t.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    t.* EXCLUDE (ws_match_id, team_id)
FROM source t
LEFT JOIN match_index idx ON t.ws_match_id = idx.ws_match_id
