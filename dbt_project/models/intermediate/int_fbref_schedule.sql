{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_schedule'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_schedule') }}
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
    s.* EXCLUDE (team, opponent, raw_team, raw_opponent)
FROM source s

LEFT JOIN team_mapping tm_team ON s.team     = tm_team.club_name
LEFT JOIN team_mapping tm_opp  ON s.opponent = tm_opp.club_name
LEFT JOIN registry r
    ON  s.date          = r.match_date
    AND s.league_source = r.league_source
    AND s.season        = r.season
    AND (
        (s.venue = 'Home' AND tm_team.team_id = r.home_team_id)
        OR
        (s.venue = 'Away' AND tm_team.team_id = r.away_team_id)
        OR
        (s.venue = 'Neutral' AND (tm_team.team_id = r.home_team_id OR tm_team.team_id = r.away_team_id))
    )