{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_formations'
    )
}}

-- Timeline tactique par équipe par match, identités normalisées.
-- Même patron que int_whoscored_events / int_whoscored_player_match : on traduit
-- l'id d'équipe WhoScored vers l'id canonique et on récupère le match_id unifié
-- via int_whoscored_match_index. formation_id reste tel quel (clé vers
-- silver.stg_whoscored_formations_ref pour le libellé).

WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_formations') }}
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
        WHEN f.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN f.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    f.* EXCLUDE (ws_match_id, team_id)
FROM source f
LEFT JOIN match_index idx ON f.ws_match_id = idx.ws_match_id
