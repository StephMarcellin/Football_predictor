{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_keeper_psxg'
    )
}}

-- Post-Shot Expected Goals +/- (PSxG+/-) par gardien — mesure de shot-stopping.
-- Grain : gardien × saison × championnat.
--
-- Source : int_keeper_shots (un tir cadré subi, avec son xGOT et son gardien).
-- On agrège la qualité des tirs subis (Σ xGOT) et les buts réellement encaissés :
--   psxg_plus_minus = psxg_faced − goals_conceded
--   > 0  : le gardien a arrêté PLUS que la qualité des tirs ne le prédisait (bon)
--   < 0  : il a encaissé plus que prévu
--
-- Périmètre déjà propre en amont (xgot_features / int_keeper_shots) : hors CSC,
-- hors penalty. GARDE-FOU SYMÉTRIQUE : on ne garde que les tirs des matchs où
-- l'équipe qui défend a un GK lineup (def_gk_available) — sinon les buts ne sont
-- pas attribués alors que les arrêts le sont, ce qui gonflerait le PSxG (gardiens
-- « invaincus »). Exclut ~2,3 % des tirs (surtout 2024-2025 Serie A à 0 % de
-- lineup et 2.Bundesliga) ; ces poches reviendront après re-load des formations.

WITH attributed AS (
    SELECT *
    FROM {{ ref('int_keeper_shots') }}
    WHERE keeper_id IS NOT NULL
      AND def_gk_available
)

SELECT
    keeper_id,
    season,
    league_source,

    COUNT(*)                              AS shots_faced,      -- tirs cadrés subis
    SUM(is_goal::INTEGER)                 AS goals_conceded,   -- buts encaissés (hors CSC/penalty)
    COUNT(*) - SUM(is_goal::INTEGER)      AS saves,            -- arrêts
    ROUND(SUM(xgot), 3)                   AS psxg_faced,       -- qualité cumulée des tirs subis

    -- Shot-stopping : arrêté − attendu. Positif = surperformance du gardien.
    ROUND(SUM(xgot) - SUM(is_goal::INTEGER), 3)  AS psxg_plus_minus,

    -- Taux d'arrêt brut (pour contexte ; ne tient pas compte de la difficulté)
    ROUND((COUNT(*) - SUM(is_goal::INTEGER)) * 1.0 / COUNT(*), 3) AS save_pct

FROM attributed
GROUP BY keeper_id, season, league_source
