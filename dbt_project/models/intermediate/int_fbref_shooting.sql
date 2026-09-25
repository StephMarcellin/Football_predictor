{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_shooting'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_shooting') }}
),

typed AS (
    SELECT
        TRY_CAST(date AS DATE)                                     AS dt_date,
        TRY_CAST(time AS VARCHAR)                                  AS str_time,
        TRY_CAST(league_source AS VARCHAR)                         AS str_league_source,
        TRY_CAST(day AS VARCHAR)                                   AS str_day,
        TRY_CAST(venue AS VARCHAR)                                 AS str_venue,
        TRY_CAST(result AS VARCHAR)                                AS str_result,
        TRY_CAST(NULLIF(TRIM(gf), '')               AS INTEGER)    AS int_gf,
        TRY_CAST(NULLIF(TRIM(ga), '')               AS INTEGER)    AS int_ga,
        TRY_CAST(opponent AS VARCHAR)                              AS str_opponent,
        TRY_CAST(NULLIF(TRIM(standard_gls), '')     AS INTEGER)    AS int_standard_gls,
        TRY_CAST(NULLIF(TRIM(standard_sh), '')      AS INTEGER)    AS int_standard_sh,
        TRY_CAST(NULLIF(TRIM(standard_sot), '')     AS INTEGER)    AS int_standard_sot,
        TRY_CAST(NULLIF(TRIM(standard_sot_pct), '') AS DOUBLE)     AS dec_standard_sot_pct,
        TRY_CAST(NULLIF(TRIM(standard_g_sh), '')    AS DOUBLE)     AS dec_standard_g_sh,
        TRY_CAST(NULLIF(TRIM(standard_g_sot), '')   AS DOUBLE)     AS dec_standard_g_sot,
        TRY_CAST(NULLIF(TRIM(standard_pk), '')      AS INTEGER)    AS int_standard_pk,
        TRY_CAST(NULLIF(TRIM(standard_pkatt), '')   AS INTEGER)    AS int_standard_pkatt,
        TRY_CAST(team AS VARCHAR)                                  AS str_team,
        TRY_CAST(season AS VARCHAR)                                AS str_season,
        TRY_CAST(source AS VARCHAR)                                AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '')       AS TIMESTAMP)  AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                         AS str_comp_category,
        TRY_CAST(raw_team AS VARCHAR)                              AS str_raw_team,
        TRY_CAST(raw_opponent AS VARCHAR)                          AS str_raw_opponent,
        TRY_CAST(result_1n2 AS VARCHAR)                            AS str_result_1n2
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
--    Jointure contrainte sur l'adversaire : 0 fan-out (86 450 lignes in = out).
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

-- 2. Faux zéros de tirs → NULL
--    sh = 0 est une donnée manquante codée 0 (563 lignes) : 394 ont des buts hors penalty
--    (impossible sans tir), et 0 tir est plus fréquent que 1 tir (121 vs 111 hors bloc D2 2017-18).
--    sot est neutralisé dans le même cas. Si sh est déjà NULL, sot est conservé.
cleaned AS (
    SELECT
        * EXCLUDE (int_standard_sh, int_standard_sot),
        NULLIF(int_standard_sh, 0) AS int_standard_sh,
        CASE
            WHEN int_standard_sh = 0 THEN NULL
            ELSE int_standard_sot
        END AS int_standard_sot
    FROM enriched
),

-- 3. Ratios recalculés, définition uniforme toutes saisons
--    Buts hors penalty (Sh et SoT FBref excluent les penalties), échelle 0-1.
--    Les ratios FBref bruts ne sont pas homogènes (G/Sh inclut les penalties en 2014-17).
corrected AS (
    SELECT
        * EXCLUDE (dec_standard_sot_pct, dec_standard_g_sh, dec_standard_g_sot),
        CASE
            WHEN int_standard_sh > 0
            THEN int_standard_sot::DOUBLE / int_standard_sh
        END AS dec_standard_sot_pct,
        CASE
            WHEN int_standard_sh > 0
            THEN (int_standard_gls - int_standard_pk)::DOUBLE / int_standard_sh
        END AS dec_standard_g_sh,
        CASE
            WHEN int_standard_sot > 0
            THEN (int_standard_gls - int_standard_pk)::DOUBLE / int_standard_sot
        END AS dec_standard_g_sot
    FROM cleaned
)

SELECT * FROM corrected