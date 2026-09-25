{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_players'
    )
}}

-- Dimension joueur par saison et par équipe.
-- Grain : (player_id, season, team_id canonique). Un joueur transféré en cours
-- de saison apparaît donc sur plusieurs lignes (une par équipe) — c'est voulu.
--
-- Attributs de profil : height / weight (valeurs COURANTES au moment du scrape,
-- ~stables chez l'adulte). L'âge est volontairement EXCLU : WhoScored renvoie
-- l'âge courant, pas l'âge au match — il serait faux « par saison ».
-- Le nom vient de la table de référence, pas des faits.

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

-- int_whoscored_player_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_player_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        int_shirt_no                                                 AS "shirt_no",
        str_position                                                 AS "position",
        bool_is_first_eleven                                         AS "is_first_eleven",
        bool_is_man_of_the_match                                     AS "is_man_of_the_match",
        int_height                                                   AS "height",
        int_weight                                                   AS "weight",
        int_age                                                      AS "age",
        dec_rating                                                   AS "rating",
        str_stats_json                                               AS "stats_json",
        CAST(int_touches AS DOUBLE)                                  AS "touches",
        CAST(int_possession AS DOUBLE)                               AS "possession",
        CAST(int_passes_total AS DOUBLE)                             AS "passes_total",
        CAST(int_passes_accurate AS DOUBLE)                          AS "passes_accurate",
        CAST(int_passes_key AS DOUBLE)                               AS "passes_key",
        CAST(int_shots_total AS DOUBLE)                              AS "shots_total",
        CAST(int_shots_on_target AS DOUBLE)                          AS "shots_on_target",
        CAST(int_shots_off_target AS DOUBLE)                         AS "shots_off_target",
        CAST(int_shots_blocked AS DOUBLE)                            AS "shots_blocked",
        CAST(int_shots_on_post AS DOUBLE)                            AS "shots_on_post",
        CAST(int_dribbles_attempted AS DOUBLE)                       AS "dribbles_attempted",
        CAST(int_dribbles_won AS DOUBLE)                             AS "dribbles_won",
        CAST(int_dribbles_lost AS DOUBLE)                            AS "dribbles_lost",
        CAST(int_dribbled_past AS DOUBLE)                            AS "dribbled_past",
        CAST(int_dispossessed AS DOUBLE)                             AS "dispossessed",
        CAST(int_tackles_total AS DOUBLE)                            AS "tackles_total",
        CAST(int_tackle_successful AS DOUBLE)                        AS "tackle_successful",
        CAST(int_tackle_unsuccesful AS DOUBLE)                       AS "tackle_unsuccesful",
        CAST(int_interceptions AS DOUBLE)                            AS "interceptions",
        CAST(int_clearances AS DOUBLE)                               AS "clearances",
        CAST(int_aerials_total AS DOUBLE)                            AS "aerials_total",
        CAST(int_aerials_won AS DOUBLE)                              AS "aerials_won",
        CAST(int_offensive_aerials AS DOUBLE)                        AS "offensive_aerials",
        CAST(int_defensive_aerials AS DOUBLE)                        AS "defensive_aerials",
        CAST(int_fouls_commited AS DOUBLE)                           AS "fouls_commited",
        CAST(int_offsides_caught AS DOUBLE)                          AS "offsides_caught",
        CAST(int_errors AS DOUBLE)                                   AS "errors",
        CAST(int_corners_total AS DOUBLE)                            AS "corners_total",
        CAST(int_corners_accurate AS DOUBLE)                         AS "corners_accurate",
        CAST(int_throw_ins_total AS DOUBLE)                          AS "throw_ins_total",
        CAST(int_throw_ins_accurate AS DOUBLE)                       AS "throw_ins_accurate",
        CAST(int_total_saves AS DOUBLE)                              AS "total_saves",
        CAST(int_parried_safe AS DOUBLE)                             AS "parried_safe",
        CAST(int_parried_danger AS DOUBLE)                           AS "parried_danger",
        CAST(int_claims_high AS DOUBLE)                              AS "claims_high",
        CAST(int_collected AS DOUBLE)                                AS "collected"
    FROM {{ ref('int_whoscored_player_match') }}
),

mdl_body AS (
WITH player_match AS (
    SELECT player_id, match_id, team_id, height, weight
    FROM in_int_whoscored_player_match
    WHERE match_id IS NOT NULL
      AND team_id  IS NOT NULL
),

-- Saison rattachée au match (1 ligne par match_id).
match_season AS (
    SELECT DISTINCT match_id, season
    FROM in_int_whoscored_match_index
    WHERE match_id IS NOT NULL
),

names AS (
    SELECT player_id, player_name
    FROM {{ source('silver', 'stg_whoscored_players_ref') }}
)

SELECT
    pm.player_id,
    ms.season,
    pm.team_id,
    MAX(n.player_name) AS player_name,
    MAX(pm.height)     AS height,
    MAX(pm.weight)     AS weight,
    COUNT(*)           AS n_matchs   -- matchs joués pour cette équipe cette saison
FROM player_match pm
JOIN match_season ms ON pm.match_id = ms.match_id
LEFT JOIN names   n  ON pm.player_id = n.player_id
GROUP BY pm.player_id, ms.season, pm.team_id
),

mdl_out AS (
    SELECT
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "season"                                                     AS str_season,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "player_name"                                                AS str_player_name,
        "height"                                                     AS int_height,
        "weight"                                                     AS int_weight,
        "n_matchs"                                                   AS int_n_matchs
    FROM mdl_body
)

SELECT * FROM mdl_out
