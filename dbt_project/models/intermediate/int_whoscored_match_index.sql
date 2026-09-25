{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_match_index'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_match_index') }}
),

-- team_mapping AS (
--     SELECT DISTINCT club_name, team_id
--     FROM {{ source('referentiel', 'team_mapping') }}
-- ),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
)

SELECT
    r.match_id,
    s.ws_match_id,
    s.match_date,
    s.home_team_id as team_id,
    s.away_team_id as opponent_id,
    s.ws_home_team_id,
    s.ws_away_team_id,
    s.league_source,
    s.season,
    s.scraped_at,
    s.comp_category,

FROM source s

LEFT JOIN registry r
    ON  s.match_date   = r.match_date
    AND s.home_team_id = r.home_team_id
    AND s.away_team_id = r.away_team_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "ws_match_id"                                                AS str_ws_match_id,
        "match_date"                                                 AS dt_match_date,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        CAST(ws_home_team_id AS VARCHAR)                             AS str_ws_home_team_id,
        CAST(ws_away_team_id AS VARCHAR)                             AS str_ws_away_team_id,
        "league_source"                                              AS str_league_source,
        "season"                                                     AS str_season,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        "comp_category"                                              AS str_comp_category
    FROM mdl_body
)

SELECT * FROM mdl_out
