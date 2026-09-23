{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_misc'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_misc') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
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

-- 2. Couche de garde-fous et de correction sur le schéma exact
corrected AS (
    SELECT
        * EXCLUDE (
            gf, ga, crdy, crdr, crdy2,
            fls, fld, off, crosses, int, tklw,
            pkwon, pkcon, og
        ),

        -- Non-négativité des métriques de comptage
        GREATEST(gf, 0) AS gf,
        GREATEST(ga, 0) AS ga,
        GREATEST(fls, 0) AS fls,
        GREATEST(fld, 0) AS fld,
        GREATEST(off, 0) AS off,
        GREATEST(crosses, 0) AS crosses,
        GREATEST(int, 0) AS int,
        GREATEST(tklw, 0) AS tklw,
        GREATEST(pkwon, 0) AS pkwon,
        GREATEST(pkcon, 0) AS pkcon,

        -- Coherence disciplinaire des cartons (crdy2 = 2nd jaune)
        GREATEST(crdy2, 0) AS crdy2,
        GREATEST(GREATEST(crdy, 0), GREATEST(crdy2, 0)) AS crdy,
        GREATEST(GREATEST(crdr, 0), GREATEST(crdy2, 0)) AS crdr,

        -- Borne supérieure des buts contre son camp
        LEAST(GREATEST(og, 0), GREATEST(ga, 0)) AS og
    FROM enriched
)

SELECT * FROM corrected