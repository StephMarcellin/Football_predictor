{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_odds'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'odds') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ ref('team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
)

SELECT
    r.match_id,
    tm_team.team_id,
    tm_opp.team_id AS opponent_id,
    s.* EXCLUDE (home_team, away_team)
FROM source s

LEFT JOIN team_mapping tm_team ON s.home_team     = tm_team.club_name
LEFT JOIN team_mapping tm_opp  ON s.away_team = tm_opp.club_name
LEFT JOIN registry r
    ON  s.date          = r.match_date
    AND s.league_source = r.league_source
    AND s.season        = r.season
