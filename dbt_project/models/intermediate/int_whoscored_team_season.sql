{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_team_season'
    )
}}

-- Grain : 1 ligne par (équipe, saison, championnat). Agrégats de SAISON COMPLÈTE :
-- toute utilisation en feature doit être décalée à la saison N-1 (cf. gold.equipe_match)
-- sous peine de fuite de données.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH source AS (
    SELECT * FROM {{ source('silver', 'whoscored_team_season') }}
),

typed AS (
    SELECT
        TRY_CAST(team AS VARCHAR)                                          AS str_team,
        TRY_CAST(season AS VARCHAR)                                        AS str_season,
        TRY_CAST(league_source AS VARCHAR)                                 AS str_league_source,
        TRY_CAST(NULLIF(TRIM(ws_away_shots_conceded_pg), '') AS DOUBLE)    AS dec_ws_away_shots_conceded_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_tackles_pg), '') AS DOUBLE)           AS dec_ws_away_tackles_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_interceptions_pg), '') AS DOUBLE)     AS dec_ws_away_interceptions_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_fouls_pg), '') AS DOUBLE)             AS dec_ws_away_fouls_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_offsides_pg), '') AS DOUBLE)          AS dec_ws_away_offsides_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_def_rating), '') AS DOUBLE)           AS dec_ws_away_def_rating,
        TRY_CAST(NULLIF(TRIM(ws_home_shots_conceded_pg), '') AS DOUBLE)    AS dec_ws_home_shots_conceded_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_tackles_pg), '') AS DOUBLE)           AS dec_ws_home_tackles_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_interceptions_pg), '') AS DOUBLE)     AS dec_ws_home_interceptions_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_fouls_pg), '') AS DOUBLE)             AS dec_ws_home_fouls_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_offsides_pg), '') AS DOUBLE)          AS dec_ws_home_offsides_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_def_rating), '') AS DOUBLE)           AS dec_ws_home_def_rating,
        TRY_CAST(NULLIF(TRIM(ws_away_shots_pg), '') AS DOUBLE)             AS dec_ws_away_shots_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_shots_ot_pg), '') AS DOUBLE)          AS dec_ws_away_shots_ot_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_dribbles_pg), '') AS DOUBLE)          AS dec_ws_away_dribbles_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_fouled_pg), '') AS DOUBLE)            AS dec_ws_away_fouled_pg,
        TRY_CAST(NULLIF(TRIM(ws_away_att_rating), '') AS DOUBLE)           AS dec_ws_away_att_rating,
        TRY_CAST(NULLIF(TRIM(ws_home_shots_pg), '') AS DOUBLE)             AS dec_ws_home_shots_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_shots_ot_pg), '') AS DOUBLE)          AS dec_ws_home_shots_ot_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_dribbles_pg), '') AS DOUBLE)          AS dec_ws_home_dribbles_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_fouled_pg), '') AS DOUBLE)            AS dec_ws_home_fouled_pg,
        TRY_CAST(NULLIF(TRIM(ws_home_att_rating), '') AS DOUBLE)           AS dec_ws_home_att_rating,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_against), '') AS DOUBLE)           AS dec_ws_away_xg_against,
        TRY_CAST(NULLIF(TRIM(ws_away_goals_against), '') AS DOUBLE)        AS dec_ws_away_goals_against,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_diff_against), '') AS DOUBLE)      AS dec_ws_away_xg_diff_against,
        TRY_CAST(NULLIF(TRIM(ws_away_shots_against), '') AS DOUBLE)        AS dec_ws_away_shots_against,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_per_shot_against), '') AS DOUBLE)  AS dec_ws_away_xg_per_shot_against,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_against_rating), '') AS DOUBLE)    AS dec_ws_away_xg_against_rating,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_for), '') AS DOUBLE)               AS dec_ws_away_xg_for,
        TRY_CAST(NULLIF(TRIM(ws_away_goals_for), '') AS DOUBLE)            AS dec_ws_away_goals_for,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_diff_for), '') AS DOUBLE)          AS dec_ws_away_xg_diff_for,
        TRY_CAST(NULLIF(TRIM(ws_away_shots_for), '') AS DOUBLE)            AS dec_ws_away_shots_for,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_per_shot_for), '') AS DOUBLE)      AS dec_ws_away_xg_per_shot_for,
        TRY_CAST(NULLIF(TRIM(ws_away_xg_for_rating), '') AS DOUBLE)        AS dec_ws_away_xg_for_rating,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_against), '') AS DOUBLE)           AS dec_ws_home_xg_against,
        TRY_CAST(NULLIF(TRIM(ws_home_goals_against), '') AS DOUBLE)        AS dec_ws_home_goals_against,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_diff_against), '') AS DOUBLE)      AS dec_ws_home_xg_diff_against,
        TRY_CAST(NULLIF(TRIM(ws_home_shots_against), '') AS DOUBLE)        AS dec_ws_home_shots_against,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_per_shot_against), '') AS DOUBLE)  AS dec_ws_home_xg_per_shot_against,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_against_rating), '') AS DOUBLE)    AS dec_ws_home_xg_against_rating,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_for), '') AS DOUBLE)               AS dec_ws_home_xg_for,
        TRY_CAST(NULLIF(TRIM(ws_home_goals_for), '') AS DOUBLE)            AS dec_ws_home_goals_for,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_diff_for), '') AS DOUBLE)          AS dec_ws_home_xg_diff_for,
        TRY_CAST(NULLIF(TRIM(ws_home_shots_for), '') AS DOUBLE)            AS dec_ws_home_shots_for,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_per_shot_for), '') AS DOUBLE)      AS dec_ws_home_xg_per_shot_for,
        TRY_CAST(NULLIF(TRIM(ws_home_xg_for_rating), '') AS DOUBLE)        AS dec_ws_home_xg_for_rating,
        TRY_CAST(source AS VARCHAR)                                        AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '') AS TIMESTAMP)                AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                                 AS str_comp_category,
        TRY_CAST(raw_team AS VARCHAR)                                      AS str_raw_team
    FROM source
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
    WHERE team_id IS NOT NULL
),

-- 1. Résolution de l'identifiant équipe (390 lignes in = out, 0 équipe non mappée)
enriched AS (
    SELECT
        TRY_CAST(tm.team_id AS VARCHAR)                                    AS str_team_id,
        s.* EXCLUDE (str_team)
    FROM typed s
    LEFT JOIN team_mapping tm ON s.str_team = tm.club_name
)

SELECT * FROM enriched
),

mdl_out AS (
    SELECT
        "str_team_id"                                                AS str_team_id,
        "str_season"                                                 AS str_season,
        "str_league_source"                                          AS str_league_source,
        "dec_ws_away_shots_conceded_pg"                              AS dec_ws_away_shots_conceded_pg,
        "dec_ws_away_tackles_pg"                                     AS dec_ws_away_tackles_pg,
        "dec_ws_away_interceptions_pg"                               AS dec_ws_away_interceptions_pg,
        "dec_ws_away_fouls_pg"                                       AS dec_ws_away_fouls_pg,
        "dec_ws_away_offsides_pg"                                    AS dec_ws_away_offsides_pg,
        "dec_ws_away_def_rating"                                     AS dec_ws_away_def_rating,
        "dec_ws_home_shots_conceded_pg"                              AS dec_ws_home_shots_conceded_pg,
        "dec_ws_home_tackles_pg"                                     AS dec_ws_home_tackles_pg,
        "dec_ws_home_interceptions_pg"                               AS dec_ws_home_interceptions_pg,
        "dec_ws_home_fouls_pg"                                       AS dec_ws_home_fouls_pg,
        "dec_ws_home_offsides_pg"                                    AS dec_ws_home_offsides_pg,
        "dec_ws_home_def_rating"                                     AS dec_ws_home_def_rating,
        "dec_ws_away_shots_pg"                                       AS dec_ws_away_shots_pg,
        "dec_ws_away_shots_ot_pg"                                    AS dec_ws_away_shots_ot_pg,
        "dec_ws_away_dribbles_pg"                                    AS dec_ws_away_dribbles_pg,
        "dec_ws_away_fouled_pg"                                      AS dec_ws_away_fouled_pg,
        "dec_ws_away_att_rating"                                     AS dec_ws_away_att_rating,
        "dec_ws_home_shots_pg"                                       AS dec_ws_home_shots_pg,
        "dec_ws_home_shots_ot_pg"                                    AS dec_ws_home_shots_ot_pg,
        "dec_ws_home_dribbles_pg"                                    AS dec_ws_home_dribbles_pg,
        "dec_ws_home_fouled_pg"                                      AS dec_ws_home_fouled_pg,
        "dec_ws_home_att_rating"                                     AS dec_ws_home_att_rating,
        "dec_ws_away_xg_against"                                     AS dec_ws_away_xg_against,
        CAST(dec_ws_away_goals_against AS INTEGER)                   AS int_ws_away_goals_against,
        "dec_ws_away_xg_diff_against"                                AS dec_ws_away_xg_diff_against,
        CAST(dec_ws_away_shots_against AS INTEGER)                   AS int_ws_away_shots_against,
        "dec_ws_away_xg_per_shot_against"                            AS dec_ws_away_xg_per_shot_against,
        "dec_ws_away_xg_against_rating"                              AS dec_ws_away_xg_against_rating,
        "dec_ws_away_xg_for"                                         AS dec_ws_away_xg_for,
        CAST(dec_ws_away_goals_for AS INTEGER)                       AS int_ws_away_goals_for,
        "dec_ws_away_xg_diff_for"                                    AS dec_ws_away_xg_diff_for,
        CAST(dec_ws_away_shots_for AS INTEGER)                       AS int_ws_away_shots_for,
        "dec_ws_away_xg_per_shot_for"                                AS dec_ws_away_xg_per_shot_for,
        "dec_ws_away_xg_for_rating"                                  AS dec_ws_away_xg_for_rating,
        "dec_ws_home_xg_against"                                     AS dec_ws_home_xg_against,
        CAST(dec_ws_home_goals_against AS INTEGER)                   AS int_ws_home_goals_against,
        "dec_ws_home_xg_diff_against"                                AS dec_ws_home_xg_diff_against,
        CAST(dec_ws_home_shots_against AS INTEGER)                   AS int_ws_home_shots_against,
        "dec_ws_home_xg_per_shot_against"                            AS dec_ws_home_xg_per_shot_against,
        "dec_ws_home_xg_against_rating"                              AS dec_ws_home_xg_against_rating,
        "dec_ws_home_xg_for"                                         AS dec_ws_home_xg_for,
        CAST(dec_ws_home_goals_for AS INTEGER)                       AS int_ws_home_goals_for,
        "dec_ws_home_xg_diff_for"                                    AS dec_ws_home_xg_diff_for,
        CAST(dec_ws_home_shots_for AS INTEGER)                       AS int_ws_home_shots_for,
        "dec_ws_home_xg_per_shot_for"                                AS dec_ws_home_xg_per_shot_for,
        "dec_ws_home_xg_for_rating"                                  AS dec_ws_home_xg_for_rating,
        "str_source"                                                 AS str_source,
        "dt_scraped_at"                                              AS dt_scraped_at,
        "str_comp_category"                                          AS str_comp_category,
        "str_raw_team"                                               AS str_raw_team
    FROM mdl_body
)

SELECT * FROM mdl_out
