{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_events'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_events') }}
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
        WHEN e.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN e.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,
    e.* EXCLUDE (ws_match_id, team_id)
FROM source e
LEFT JOIN match_index idx ON e.ws_match_id = idx.ws_match_id