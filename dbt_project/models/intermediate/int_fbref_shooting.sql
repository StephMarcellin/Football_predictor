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

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

enriched AS (
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
),

-- Étape 1 : Nettoyage des métriques de comptage brutes
sanitized_counts AS (
    SELECT
        * EXCLUDE (
            gf, ga, standard_gls, standard_sh, standard_sot,
            standard_pk, standard_pkatt
        ),

        GREATEST(gf, 0) AS gf,
        GREATEST(ga, 0) AS ga,
        GREATEST(standard_gls, 0) AS standard_gls,
        GREATEST(standard_sot, 0) AS standard_sot,
        GREATEST(standard_pk, 0)  AS standard_pk,

        -- Invariance Pénaltys : PKatt >= PK
        GREATEST(GREATEST(standard_pkatt, 0), GREATEST(standard_pk, 0)) AS standard_pkatt,

        -- Invariance Tirs : Shots ne peut pas être inférieur aux tirs cadrés (SoT)
        GREATEST(GREATEST(standard_sh, 0), GREATEST(standard_sot, 0)) AS standard_sh
    FROM enriched
),

-- Étape 2 : Recalcul sécurisé des ratios
corrected AS (
    SELECT
        * EXCLUDE (standard_sot_pct, standard_g_sh, standard_g_sot),

        -- % de tirs cadrés (SoT / Sh)
        CASE 
            WHEN standard_sh > 0 THEN ROUND(standard_sot::FLOAT / standard_sh, 3)
            ELSE NULL 
        END AS standard_sot_pct,

        -- Buts par tir (Gls / Sh)
        CASE 
            WHEN standard_sh > 0 THEN ROUND(standard_gls::FLOAT / standard_sh, 3)
            ELSE NULL 
        END AS standard_g_sh,

        -- Buts par tir cadré (Gls / SoT)
        CASE 
            WHEN standard_sot > 0 THEN ROUND(standard_gls::FLOAT / standard_sot, 3)
            ELSE NULL 
        END AS standard_g_sot
    FROM sanitized_counts
)

SELECT * FROM corrected