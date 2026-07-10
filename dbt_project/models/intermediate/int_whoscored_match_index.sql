{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_match_index'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_match_index') }}
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
    s.* EXCLUDE (home_team_name, away_team_name, home_team_id, away_team_id, raw_home_team_name, raw_away_team_name),
    s.home_team_id AS ws_home_team_id,
    s.away_team_id AS ws_away_team_id
FROM source s

LEFT JOIN team_mapping tm_team ON s.home_team_name = tm_team.club_name
LEFT JOIN team_mapping tm_opp  ON s.away_team_name = tm_opp.club_name
LEFT JOIN registry r
    ON  s.match_date    = r.match_date
    AND s.league_source = r.league_source
    AND s.season        = r.season
    AND tm_team.team_id = r.home_team_id
    AND tm_opp.team_id  = r.away_team_id