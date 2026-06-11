{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_understat_stats'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'understat_stats') }}
),

us_schedule AS (
    SELECT * FROM {{ ref('int_understat_schedule') }}
)


SELECT
    s.* EXCLUDE(match_id),
    s.match_id as us_match_id,
    us_schedule.match_id
FROM source s
LEFT JOIN us_schedule
     ON s.match_id = us_schedule.us_match_id
