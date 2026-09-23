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
        r.match_id,
        tm_team.team_id,
        tm_opp.team_id AS opponent_id,
        s.* EXCLUDE (team, opponent, raw_team, raw_opponent, cs)
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

-- 2. Couche de garde-fous et de correction basée sur le schéma exact
corrected AS (
    SELECT
        * EXCLUDE (
            gf, ga, ga_keeper, sota, saves, save_pct, 
            pk_att, pk_allowed, pk_saved, pk_missed
        ),

        -- Non-négativité des métriques de comptage
        GREATEST(gf, 0) AS gf,
        GREATEST(ga, 0) AS ga,
        GREATEST(ga_keeper, 0) AS ga_keeper,
        GREATEST(saves, 0) AS saves,
        GREATEST(pk_allowed, 0) AS pk_allowed,
        GREATEST(pk_saved, 0) AS pk_saved,
        GREATEST(pk_missed, 0) AS pk_missed,

        -- Invariance des pénaltys : pk_att doit au moins égaler la somme des composantes observées
        GREATEST(
            GREATEST(pk_att, 0),
            GREATEST(pk_allowed, 0) + GREATEST(pk_saved, 0) + GREATEST(pk_missed, 0)
        ) AS pk_att,

        -- SoTA doit couvrir au minimum la somme des arrêtés et encaissés (saves + ga_keeper)
        GREATEST(sota, GREATEST(ga_keeper, 0) + GREATEST(saves, 0)) AS sota,

        -- Recalcul de save_pct avec la formule FBref : (SoTA - GA) / SoTA
        -- Renvoie NULL si SoTA = 0 pour préserver la gestion native des NaN dans LightGBM
        CASE 
            WHEN GREATEST(sota, GREATEST(ga_keeper, 0) + GREATEST(saves, 0)) > 0 
            THEN ROUND(
                (GREATEST(sota, GREATEST(ga_keeper, 0) + GREATEST(saves, 0)) - GREATEST(ga_keeper, 0))::FLOAT 
                / GREATEST(sota, GREATEST(ga_keeper, 0) + GREATEST(saves, 0)), 
                3
            )
            ELSE NULL 
        END AS save_pct,

        -- Dérivation stricte du Clean Sheet depuis ga_keeper
        CASE 
            WHEN ga_keeper IS NULL THEN NULL
            WHEN GREATEST(ga_keeper, 0) = 0 THEN 1 
            ELSE 0 
        END AS cs
    FROM enriched
)

SELECT * FROM corrected