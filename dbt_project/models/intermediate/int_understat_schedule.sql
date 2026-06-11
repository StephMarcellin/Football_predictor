{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_understat_schedule'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'understat_schedule') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ ref('team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
)

SELECT
    s.* EXCLUDE (home_team, away_team, raw_home_team, raw_away_team, match_id),
    s.match_id as us_match_id,
    r.match_id,
    tm_team.team_id,
    tm_opp.team_id AS opponent_id,
    
FROM source s

LEFT JOIN team_mapping tm_team ON s.home_team = tm_team.club_name
LEFT JOIN team_mapping tm_opp  ON s.away_team = tm_opp.club_name
LEFT JOIN registry r
    ON  r.league_source = s.league_source
    AND r.season        = s.season
    AND tm_team.team_id = r.home_team_id
    AND tm_opp.team_id = r.away_team_id