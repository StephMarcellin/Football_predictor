{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_odds_pinnacle'
    )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- gold.gold_odds_pinnacle — grain : 1 ligne par MATCH (clé str_match_id)
--
-- Cotes Pinnacle uniquement (bookmaker de référence), en ouverture et en clôture :
--   • 1N2             : cotes + probabilités sans marge (home / draw / away)
--   • Over/Under 2.5  : cotes + probabilités sans marge (over / under)
--
-- Ces cotes NE SONT PAS des features : elles ne doivent jamais entrer dans X du
-- modèle. Elles servent au backtest (gain d'un pari) et à la comparaison modèle/marché.
-- Elles vivent donc ici, hors du backbone et des marts.
--
-- Rattachement au match : fait en amont, dans intermediate.intermediate_odds (match_id déjà
-- relié aux cotes sur date + ligue + saison + équipe domicile + équipe extérieur).
-- Ce modèle sélectionne Pinnacle, renomme et calcule les probabilités sans marge.
-- Les lignes sans match_id sont écartées.
--
-- Une cote à 0.0 dans la source signifie « absente » (3 lignes) → NULLIF(…, 0) la remet à NULL.
-- Convention home/away = équipe listée à domicile dans la source (même sens que le label
-- H/D/A du mart). Pas de dérive ouverture→clôture : se recalcule par soustraction.
-- ════════════════════════════════════════════════════════════════════════════

WITH odds AS (
    SELECT * FROM {{ ref('intermediate_odds') }}
    WHERE str_match_id IS NOT NULL
),

joined AS (
    SELECT
        o.str_match_id,
        o.dt_date,
        o.str_season,
        o.str_league_source,
        o.str_team_id                       AS str_home_team_id,
        o.str_opponent_id                   AS str_away_team_id,

        -- 1N2 ouverture
        NULLIF(o.dec_pinnacle_1x2_home, 0)                 AS dec_odds_open_home,
        NULLIF(o.dec_pinnacle_1x2_draw, 0)                 AS dec_odds_open_draw,
        NULLIF(o.dec_pinnacle_1x2_away, 0)                 AS dec_odds_open_away,
        -- 1N2 clôture
        NULLIF(o.dec_pinnacle_close_1x2_home, 0)           AS dec_odds_close_home,
        NULLIF(o.dec_pinnacle_close_1x2_draw, 0)           AS dec_odds_close_draw,
        NULLIF(o.dec_pinnacle_close_1x2_away, 0)           AS dec_odds_close_away,
        -- Over/Under 2.5 ouverture
        NULLIF(o.dec_pinnacle_ou25_over, 0)                AS dec_odds_open_over25,
        NULLIF(o.dec_pinnacle_ou25_under, 0)               AS dec_odds_open_under25,
        -- Over/Under 2.5 clôture
        NULLIF(o.dec_pinnacle_close_ou25_over, 0)          AS dec_odds_close_over25,
        NULLIF(o.dec_pinnacle_close_ou25_under, 0)         AS dec_odds_close_under25
    FROM odds o
)

SELECT
    joined.*,

    -- Probabilités sans marge — 1N2 ouverture
    {{ implied_prob_3way('dec_odds_open_home', 'dec_odds_open_draw', 'dec_odds_open_away', 'home') }} AS dec_prob_open_home,
    {{ implied_prob_3way('dec_odds_open_home', 'dec_odds_open_draw', 'dec_odds_open_away', 'draw') }} AS dec_prob_open_draw,
    {{ implied_prob_3way('dec_odds_open_home', 'dec_odds_open_draw', 'dec_odds_open_away', 'away') }} AS dec_prob_open_away,

    -- Probabilités sans marge — 1N2 clôture
    {{ implied_prob_3way('dec_odds_close_home', 'dec_odds_close_draw', 'dec_odds_close_away', 'home') }} AS dec_prob_close_home,
    {{ implied_prob_3way('dec_odds_close_home', 'dec_odds_close_draw', 'dec_odds_close_away', 'draw') }} AS dec_prob_close_draw,
    {{ implied_prob_3way('dec_odds_close_home', 'dec_odds_close_draw', 'dec_odds_close_away', 'away') }} AS dec_prob_close_away,

    -- Probabilités sans marge — Over/Under 2.5
    {{ implied_prob_2way('dec_odds_open_over25',  'dec_odds_open_under25') }}  AS dec_prob_open_over25,
    {{ implied_prob_2way('dec_odds_open_under25', 'dec_odds_open_over25')  }}  AS dec_prob_open_under25,
    {{ implied_prob_2way('dec_odds_close_over25',  'dec_odds_close_under25') }} AS dec_prob_close_over25,
    {{ implied_prob_2way('dec_odds_close_under25', 'dec_odds_close_over25')  }} AS dec_prob_close_under25
FROM joined
