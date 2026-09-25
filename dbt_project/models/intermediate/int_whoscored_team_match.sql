{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_team_match'
    )
}}

-- Stats d'équipe officielles WhoScored par match, identités normalisées.
-- Même patron : conversion team_id WhoScored → canonique + match_id unifié via
-- int_whoscored_match_index. On conserve stats_json brut (35 métriques par
-- minute) : l'extraction des totaux se fera plus tard, avec une logique
-- cumulatif/incrémental vérifiée.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_match_index lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_match_index AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_ws_match_id                                              AS "ws_match_id",
        dt_match_date                                                AS "match_date",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        CAST(str_ws_home_team_id AS INTEGER)                         AS "ws_home_team_id",
        CAST(str_ws_away_team_id AS INTEGER)                         AS "ws_away_team_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        str_comp_category                                            AS "comp_category"
    FROM {{ ref('int_whoscored_match_index') }}
),

mdl_body AS (
WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_team_match') }}
),

match_index AS (
    SELECT
        ws_match_id,
        match_id,
        ws_home_team_id,
        ws_away_team_id,
        team_id     AS home_team_id,
        opponent_id AS away_team_id
    FROM in_int_whoscored_match_index
),

source_normalized AS (
    SELECT
        idx.match_id,

        CASE
            WHEN t.team_id = idx.ws_home_team_id THEN idx.home_team_id
            WHEN t.team_id = idx.ws_away_team_id THEN idx.away_team_id
            ELSE NULL
        END AS team_id,

        t.* EXCLUDE (ws_match_id, team_id)
    FROM source t
    LEFT JOIN match_index idx ON t.ws_match_id = idx.ws_match_id
)

SELECT * FROM source_normalized
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "field"                                                      AS str_field,
        "manager_name"                                               AS str_manager_name,
        "country_name"                                               AS str_country_name,
        "average_age"                                                AS dec_average_age,
        "stats_json"                                                 AS str_stats_json
    FROM mdl_body
)

SELECT * FROM mdl_out
