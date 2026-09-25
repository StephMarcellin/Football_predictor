{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_formations'
    )
}}

-- Timeline tactique par équipe par match, identités normalisées.
-- Même patron que int_whoscored_events / int_whoscored_player_match : on traduit
-- l'id d'équipe WhoScored vers l'id canonique et on récupère le match_id unifié
-- via int_whoscored_match_index. formation_id reste tel quel (clé vers
-- silver.stg_whoscored_formations_ref pour le libellé).

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
    SELECT * FROM {{ source('silver', 'stg_whoscored_formations') }}
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
)

SELECT
    idx.match_id,

    CASE
        WHEN f.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN f.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    GREATEST(f.start_minute, IF(f.end_minute > 150, f.start_minute, f.end_minute)) AS end_minute,
    f.* EXCLUDE (ws_match_id, team_id,end_minute)
FROM source f
LEFT JOIN match_index idx ON f.ws_match_id = idx.ws_match_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "end_minute"                                                 AS int_end_minute,
        "formation_seq"                                              AS int_formation_seq,
        CAST(formation_id AS VARCHAR)                                AS str_formation_id,
        "period"                                                     AS int_period,
        "start_minute"                                               AS int_start_minute,
        "start_minute_reg"                                           AS int_start_minute_reg,
        "end_minute_reg"                                             AS int_end_minute_reg,
        CAST(captain_player_id AS VARCHAR)                           AS str_captain_player_id,
        "player_ids"                                                 AS str_player_ids,
        "formation_slots"                                            AS str_formation_slots,
        "formation_positions"                                        AS str_formation_positions
    FROM mdl_body
)

SELECT * FROM mdl_out
