{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_understat_stats'
    )
}}

-- Grain : 1 ligne par match joué (point de vue domicile/extérieur en colonnes).
-- Silver déjà typé : les TRY_CAST alignent sur la convention de nommage.

WITH source AS (
    SELECT * FROM {{ source('silver', 'understat_stats') }}
),

typed AS (
    SELECT
        TRY_CAST(match_id AS VARCHAR)                         AS str_us_match_id,
        TRY_CAST(home_xpts AS DOUBLE)                         AS dec_home_xpts,
        TRY_CAST(away_xpts AS DOUBLE)                         AS dec_away_xpts,
        TRY_CAST(home_np_xg AS DOUBLE)                        AS dec_home_np_xg,
        TRY_CAST(away_np_xg AS DOUBLE)                        AS dec_away_np_xg,
        TRY_CAST(home_np_xg_diff AS DOUBLE)                   AS dec_home_np_xg_diff,
        TRY_CAST(away_np_xg_diff AS DOUBLE)                   AS dec_away_np_xg_diff,
        TRY_CAST(home_ppda AS DOUBLE)                         AS dec_home_ppda,
        TRY_CAST(away_ppda AS DOUBLE)                         AS dec_away_ppda,
        TRY_CAST(home_deep AS INTEGER)                        AS int_home_deep,
        TRY_CAST(away_deep AS INTEGER)                        AS int_away_deep,
        TRY_CAST(season AS VARCHAR)                           AS str_season,
        TRY_CAST(league_source AS VARCHAR)                    AS str_league_source,
        TRY_CAST(source AS VARCHAR)                           AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '') AS TIMESTAMP)   AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                    AS str_comp_category
    FROM source
),

us_schedule AS (
    SELECT str_us_match_id, str_match_id
    FROM {{ ref('int_understat_schedule') }}
),

-- Rattachement au match_id unifié via le calendrier Understat (1-1 sur str_us_match_id,
-- unique côté schedule → pas de fan-out possible).
enriched AS (
    SELECT
        sch.str_match_id,
        s.*
    FROM typed s
    LEFT JOIN us_schedule sch
        ON s.str_us_match_id = sch.str_us_match_id
)

SELECT * FROM enriched