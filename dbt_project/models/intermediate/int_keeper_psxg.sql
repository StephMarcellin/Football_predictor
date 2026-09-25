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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_keeper_shots lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_keeper_shots AS (
    SELECT
        str_match_id                                                 AS "match_id",
        int_row_num                                                  AS "row_num",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        bool_is_goal                                                 AS "is_goal",
        dec_xgot                                                     AS "xgot",
        int_def_team                                                 AS "def_team",
        CAST(str_keeper_id AS BIGINT)                                AS "keeper_id",
        str_attribution_method                                       AS "attribution_method",
        bool_def_gk_available                                        AS "def_gk_available"
    FROM {{ ref('int_keeper_shots') }}
),

mdl_body AS (
WITH attributed AS (
    SELECT *
    FROM in_int_keeper_shots
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
),

mdl_out AS (
    SELECT
        CAST(keeper_id AS VARCHAR)                                   AS str_keeper_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "shots_faced"                                                AS int_shots_faced,
        CAST(goals_conceded AS BIGINT)                               AS int_goals_conceded,
        CAST(saves AS BIGINT)                                        AS int_saves,
        "psxg_faced"                                                 AS dec_psxg_faced,
        "psxg_plus_minus"                                            AS dec_psxg_plus_minus,
        "save_pct"                                                   AS dec_save_pct
    FROM mdl_body
)

SELECT * FROM mdl_out
