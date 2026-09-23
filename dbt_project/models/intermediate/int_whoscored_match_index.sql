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

-- team_mapping AS (
--     SELECT DISTINCT club_name, team_id
--     FROM {{ source('referentiel', 'team_mapping') }}
-- ),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
)

SELECT
    r.match_id,
    s.ws_match_id,
    s.match_date,
    s.home_team_id as team_id,
    s.away_team_id as opponent_id,
    s.ws_home_team_id,
    s.ws_away_team_id,
    s.league_source,
    s.season,
    s.scraped_at,
    s.comp_category,

FROM source s

LEFT JOIN registry r
    ON  s.match_date   = r.match_date
    AND s.home_team_id = r.home_team_id
    AND s.away_team_id = r.away_team_id