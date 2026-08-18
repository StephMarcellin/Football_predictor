{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_odds'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'odds') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ ref('team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- Jointure identifiants + passage de TOUTES les colonnes de silver.odds
joined AS (
    SELECT
        r.match_id,
        tm_team.team_id,
        tm_opp.team_id AS opponent_id,
        s.* EXCLUDE (home_team, away_team)
    FROM source s
    LEFT JOIN team_mapping tm_team ON s.home_team = tm_team.club_name
    LEFT JOIN team_mapping tm_opp  ON s.away_team = tm_opp.club_name
    LEFT JOIN registry r
        ON  s.date          = r.match_date
        AND s.league_source = r.league_source
        AND s.season        = r.season
),

-- Probabilités implicites calculées (marge retirée) sur les nouveaux marchés
enriched AS (
    SELECT
        joined.*,

        -- 1N2 clôture — Pinnacle (référence sharp)
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'home') }} AS pinnacle_prob_close_h,
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'draw') }} AS pinnacle_prob_close_d,
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'away') }} AS pinnacle_prob_close_a,

        -- 1N2 clôture — moyenne marché
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'home') }} AS market_prob_close_h,
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'draw') }} AS market_prob_close_d,
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'away') }} AS market_prob_close_a,

        -- Over/Under 2.5 — Pinnacle ouverture
        {{ implied_prob_2way('pinnacle_ou25_over',  'pinnacle_ou25_under') }} AS pinnacle_prob_over25,
        {{ implied_prob_2way('pinnacle_ou25_under', 'pinnacle_ou25_over')  }} AS pinnacle_prob_under25,

        -- Over/Under 2.5 — Pinnacle clôture
        {{ implied_prob_2way('pinnacle_close_ou25_over',  'pinnacle_close_ou25_under') }} AS pinnacle_prob_close_over25,
        {{ implied_prob_2way('pinnacle_close_ou25_under', 'pinnacle_close_ou25_over')  }} AS pinnacle_prob_close_under25

    FROM joined
)

-- Drift ouverture→clôture (proba clôture − proba ouverture) — money informé
SELECT
    enriched.*,
    (pinnacle_prob_close_h - pinnacle_prob_h) AS pinnacle_drift_h,
    (pinnacle_prob_close_d - pinnacle_prob_d) AS pinnacle_drift_d,
    (pinnacle_prob_close_a - pinnacle_prob_a) AS pinnacle_drift_a
FROM enriched
