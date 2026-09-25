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
        TRY_CAST(NULLIF(TRIM(poss), '')       AS INTEGER)      AS int_poss,
        TRY_CAST(formation AS VARCHAR)                        AS str_formation,
        TRY_CAST(opp_formation AS VARCHAR)                    AS str_opp_formation,
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
    FROM {{ source('referentiel', 'team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- 1. Assemblage et résolution des identifiants
--    Jointure contrainte sur l'adversaire : 0 fan-out (90 946 lignes in = out, vs 91 228 avant).
enriched AS (
    SELECT
        r.match_id                           AS str_match_id,
        TRY_CAST(tm_team.team_id AS VARCHAR) AS str_team_id,
        TRY_CAST(tm_opp.team_id  AS VARCHAR) AS str_opponent_id,
        s.* EXCLUDE (str_team, str_opponent, str_raw_team, str_raw_opponent)
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

-- 2. Possession à 100 % = donnée manquante (19 lignes, matchs de coupe contre des clubs
--    non couverts, adversaire sans ligne). Une équipe ne peut pas avoir 100 % de possession.
corrected AS (
    SELECT
        * EXCLUDE (int_poss),
        CASE WHEN int_poss >= 100 THEN NULL ELSE int_poss END AS int_poss
    FROM enriched
)

SELECT * FROM corrected