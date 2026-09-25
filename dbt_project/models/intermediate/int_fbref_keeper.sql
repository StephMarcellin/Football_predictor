{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_keeper'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_keeper') }}
),

typed AS (
    SELECT
        TRY_CAST(date AS DATE)                                AS dt_date,
        TRY_CAST(time AS VARCHAR)                             AS str_time,
        TRY_CAST(league_source AS VARCHAR)                    AS str_league_source,
        TRY_CAST(day AS VARCHAR)                              AS str_day,
        TRY_CAST(venue AS VARCHAR)                            AS str_venue,
        TRY_CAST(result AS VARCHAR)                           AS str_result,
        TRY_CAST(NULLIF(TRIM(gf), '')         AS INTEGER)     AS int_gf,
        TRY_CAST(NULLIF(TRIM(ga), '')         AS INTEGER)     AS int_ga,
        TRY_CAST(opponent AS VARCHAR)                         AS str_opponent,
        TRY_CAST(NULLIF(TRIM(sota), '')       AS INTEGER)     AS int_sota,
        TRY_CAST(NULLIF(TRIM(ga_keeper), '')  AS INTEGER)     AS int_ga_keeper,
        TRY_CAST(NULLIF(TRIM(saves), '')      AS INTEGER)     AS int_saves,
        TRY_CAST(NULLIF(TRIM(save_pct), '')   AS DOUBLE)      AS dec_save_pct,
        TRY_CAST(NULLIF(TRIM(cs), '')         AS INTEGER)     AS int_cs,
        TRY_CAST(NULLIF(TRIM(pk_att), '')     AS INTEGER)     AS int_pk_att,
        TRY_CAST(NULLIF(TRIM(pk_allowed), '') AS INTEGER)     AS int_pk_allowed,
        TRY_CAST(NULLIF(TRIM(pk_saved), '')   AS INTEGER)     AS int_pk_saved,
        TRY_CAST(NULLIF(TRIM(pk_missed), '')  AS INTEGER)     AS int_pk_missed,
        TRY_CAST(team AS VARCHAR)                             AS str_team,
        TRY_CAST(season AS VARCHAR)                           AS str_season,
        TRY_CAST(source AS VARCHAR)                           AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '') AS TIMESTAMP)   AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                    AS str_comp_category,
        TRY_CAST(raw_team AS VARCHAR)                         AS str_raw_team,
        TRY_CAST(raw_opponent AS VARCHAR)                     AS str_raw_opponent,
        TRY_CAST(result_1n2 AS VARCHAR)                       AS str_result_1n2
    FROM source
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping')}}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- 1. Assemblage brut et résolution des identifiants
enriched AS (
    SELECT
        r.match_id      AS str_match_id,
        TRY_CAST(tm_team.team_id AS VARCHAR) AS str_team_id,
        TRY_CAST(tm_opp.team_id  AS VARCHAR) AS str_opponent_id,
        s.* EXCLUDE (str_team, str_opponent, str_raw_team, str_raw_opponent, int_cs)
    FROM typed s
    LEFT JOIN team_mapping tm_team ON s.str_team     = tm_team.club_name
    LEFT JOIN team_mapping tm_opp  ON s.str_opponent = tm_opp.club_name
    LEFT JOIN registry r
        ON  s.dt_date           = r.match_date
        AND s.str_league_source = r.league_source
        AND s.str_season        = r.season
        AND (
            (s.str_venue = 'Home'    AND tm_team.team_id = r.home_team_id AND tm_opp.team_id = r.away_team_id)
            OR
            (s.str_venue = 'Away'    AND tm_team.team_id = r.away_team_id AND tm_opp.team_id = r.home_team_id)
            OR
            (s.str_venue = 'Neutral' AND (
                (tm_team.team_id = r.home_team_id AND tm_opp.team_id = r.away_team_id)
                OR
                (tm_team.team_id = r.away_team_id AND tm_opp.team_id = r.home_team_id)
            ))
        )
),

resolved AS (
    SELECT
        *,
        COALESCE(int_saves, 0) AS resolved_saves,
        CASE
            WHEN int_ga_keeper IS NOT NULL THEN GREATEST(int_ga_keeper, 0)
            WHEN int_sota IS NOT NULL THEN GREATEST(int_sota - COALESCE(int_saves, 0), 0)
            ELSE NULL
        END AS resolved_ga_keeper
    FROM enriched
),

resolved2 AS (
    SELECT
        *,
        CASE
            WHEN int_sota IS NOT NULL THEN GREATEST(int_sota, COALESCE(resolved_ga_keeper, 0) + resolved_saves)
            WHEN resolved_ga_keeper IS NOT NULL THEN resolved_ga_keeper + resolved_saves
            ELSE NULL
        END AS resolved_sota
    FROM resolved
),

-- 2. Couche de garde-fous et de correction basée sur le schéma exact
corrected AS (
    SELECT
        * EXCLUDE (
            int_gf, int_ga, int_ga_keeper, int_sota, int_saves, dec_save_pct,
            int_pk_att, int_pk_allowed, int_pk_saved, int_pk_missed,
            resolved_saves, resolved_ga_keeper, resolved_sota
        ),

        GREATEST(int_gf, 0) AS int_gf,
        GREATEST(int_ga, 0) AS int_ga,

        resolved_ga_keeper AS int_ga_keeper,
        resolved_saves     AS int_saves,
        resolved_sota      AS int_sota,

        CASE
            WHEN resolved_sota IS NULL THEN NULL
            WHEN resolved_sota > 0
            THEN ROUND((resolved_sota - COALESCE(resolved_ga_keeper, 0))::FLOAT / resolved_sota, 3)
            ELSE NULL
        END AS dec_save_pct,

        GREATEST(int_pk_allowed, 0) AS int_pk_allowed,
        GREATEST(int_pk_saved, 0)   AS int_pk_saved,
        GREATEST(int_pk_missed, 0)  AS int_pk_missed,
        GREATEST(
            GREATEST(int_pk_att, 0),
            GREATEST(int_pk_allowed, 0) + GREATEST(int_pk_saved, 0) + GREATEST(int_pk_missed, 0)
        ) AS int_pk_att,

        CASE 
            WHEN resolved_ga_keeper IS NULL THEN NULL
            WHEN resolved_ga_keeper = 0 THEN 1 
            ELSE 0 
        END AS int_cs
    FROM resolved2
)

SELECT * FROM corrected