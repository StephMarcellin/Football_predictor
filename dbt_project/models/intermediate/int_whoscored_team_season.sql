{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_team_season'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'whoscored_team_season') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ ref('team_mapping') }}
    WHERE team_id IS NOT NULL
)

SELECT
    tm.team_id,
    s.* EXCLUDE (team)
FROM source s
LEFT JOIN team_mapping tm ON s.team = tm.club_name