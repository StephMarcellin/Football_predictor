{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_1n2_avant_compo_toutes_saisons — grain (match_id, team_id) — cible result_1n2 (V/N/D).
-- Variante de mart_1n2_avant_compo SANS les 62 features absentes avant 2014-15 (xG, PPDA, tirs, fautes,
-- xG/tir de la saison précédente, pressing vs relance, h2h xG) : utilisable sur TOUTES les saisons
-- d'entraînement sans lignes à moitié vides. Les CTE d'entrée ne sont pas élaguées : seule la liste
-- explicite de mdl_out fait foi (colonne absente de mdl_out = absente du mart).
-- Variante de mart_1n2 SANS aucune information dépendant de la composition (qualité du onze, formation,
-- zones des joueurs alignés, gardien titulaire) : utilisable plusieurs jours avant le match.
-- 100 % CTE. Le profil d'équipe (forme/style) est construit UNE fois dans team_profile,
-- puis réutilisé pour soi et pour l'adversaire.
--
-- Refonte nommage : intermediate_team_match_backbone, et gold_team_match sont lus sous
-- leurs nouveaux noms ; les CLÉS sont remappées vers des noms de travail (match_id,
-- team_id INTEGER…) pour joindre les modèles pas encore refondus
-- (gold_team_match_h2h, gold_team_match_press_matchup, …).
-- Les FEATURES héritent du nom de leur modèle source (ex. dec_avg_gf_rolling_3,
-- opp_dec_avg_gf_rolling_3). Seules les colonnes listées dans
-- docs/proposition_nommage_definitif.csv pour mart_1n2 sont renommées ici
-- (str_match_id, str_team_id, str_result_1n2).
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- gold_team_match_h2h lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_equipe_adversaire_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        dt_date                                                      AS "date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(int_h2h_played AS HUGEINT)                              AS "h2h_played",
        CAST(int_h2h_home_played AS HUGEINT)                         AS "h2h_home_played",
        dec_h2h_win_rate_cutoff                                      AS "h2h_win_rate_cutoff",
        dec_h2h_draw_rate_cutoff                                     AS "h2h_draw_rate_cutoff",
        dec_h2h_loss_rate_cutoff                                     AS "h2h_loss_rate_cutoff",
        dec_h2h_home_win_rate_cutoff                                 AS "h2h_home_win_rate_cutoff",
        dec_h2h_avg_gf_10                                            AS "h2h_avg_gf_10",
        dec_h2h_avg_ga_10                                            AS "h2h_avg_ga_10",
        dec_h2h_avg_xg_diff_10                                       AS "h2h_avg_xg_diff_10"
    FROM {{ ref('gold_team_match_h2h') }}
),

-- gold_team_match_press_matchup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_equipe_confrontation_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        dec_opp_press_ppda_rolling_3                                 AS "opp_press_ppda_rolling_3",
        dec_self_buildup_resistance_rolling_3                        AS "self_buildup_resistance_rolling_3",
        dec_matchup_high_press_vs_buildup_rolling_3                  AS "matchup_high_press_vs_buildup_rolling_3",
        dec_opp_press_ppda_rolling_5                                 AS "opp_press_ppda_rolling_5",
        dec_self_buildup_resistance_rolling_5                        AS "self_buildup_resistance_rolling_5",
        dec_matchup_high_press_vs_buildup_rolling_5                  AS "matchup_high_press_vs_buildup_rolling_5",
        dec_opp_press_ppda_rolling_10                                AS "opp_press_ppda_rolling_10",
        dec_self_buildup_resistance_rolling_10                       AS "self_buildup_resistance_rolling_10",
        dec_matchup_high_press_vs_buildup_rolling_10                 AS "matchup_high_press_vs_buildup_rolling_10",
        dec_self_team_xgbuildup_lag                                  AS "self_team_xgbuildup_lag",
        dec_opp_team_xgbuildup_lag                                   AS "opp_team_xgbuildup_lag"
    FROM {{ ref('gold_team_match_press_matchup') }}
),

mdl_body AS (
with

base as (
    select
        str_match_id                       as match_id,
        cast(str_team_id as bigint)       as team_id,
        cast(str_opponent_id as bigint)   as opponent_id,
        dt_date                            as date,
        str_season                         as season,
        str_result_1n2                     as result_1n2
    from {{ ref('intermediate_team_match_backbone') }}
    where str_match_id is not null and str_team_id is not null
),

-- gold_team_match sous ses nouveaux noms : clés remappées pour les jointures,
-- features conservées telles quelles (préfixes dec_/int_).
equipe_match_in as (
    select
        str_match_id                       as match_id,
        cast(str_team_id as bigint)       as team_id,
        cast(str_opponent_id as bigint)   as opponent_id,
        dt_date                            as date,
        str_season                         as season,
        str_league_source                  as league_source,
        str_venue                          as venue,
        str_comp_category                  as comp_category,
        * exclude (str_match_id, str_team_id, str_opponent_id, dt_date, str_season,
                   str_league_source, str_venue, str_comp_category)
    from {{ ref('gold_team_match') }}
),

-- Profil d'équipe : forme/style (gold_team_match) UNIQUEMENT. Pas de qualité du onze ni de formation :
-- ces informations ne sont connues qu'avec la composition (~1 h avant le match).
team_profile as (
    select em.*
    from equipe_match_in em
),

-- Le même profil, toutes colonnes préfixées opp_ (pour l'adversaire).
team_profile_opp as (
    select columns('.*') as "opp_\0"
    from team_profile
),

-- Directionnel soi↔adversaire : H2H + pressing vs relance. Grain (match, team).
directional as (
    select h2h.*,
           conf.* exclude (match_id, team_id, opponent_id)
    from in_equipe_adversaire_match h2h
    left join in_equipe_confrontation_match    conf using (match_id, team_id)
),

assembled as (
    select
        base.*,
        tp.*  exclude (match_id, team_id, opponent_id, date, season, league_source, venue),
        tpo.* exclude (opp_match_id, opp_team_id, opp_opponent_id, opp_date, opp_season,
                       opp_league_source, opp_venue, opp_int_is_home, opp_comp_category),
        d.*   exclude (match_id, team_id, opponent_id, date, season, league_source)
    from base
    left join team_profile     tp  using (match_id, team_id)
    left join team_profile_opp tpo on tpo.opp_match_id = base.match_id and tpo.opp_team_id = base.opponent_id
    left join directional      d   using (match_id, team_id)
)

-- Renommage final (docs/proposition_nommage_definitif.csv : clés + cible)
select
    match_id                        as str_match_id,
    cast(team_id as varchar)        as str_team_id,
    result_1n2                      as str_result_1n2,
    * exclude (match_id, team_id, result_1n2)
from assembled
),

mdl_out AS (
    SELECT
        "str_match_id"                                               AS str_match_id,
        "str_team_id"                                                AS str_team_id,
        "str_result_1n2"                                             AS str_result_1n2,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        "date"                                                       AS dt_date,
        "season"                                                     AS str_season,
        "comp_category"                                              AS str_comp_category,
        "int_is_home"                                                AS int_is_home,
        "int_days_since_last_game"                                   AS int_days_since_last_game,
        "dec_avg_gf_rolling_3"                                       AS dec_avg_gf_rolling_3,
        "dec_avg_ga_rolling_3"                                       AS dec_avg_ga_rolling_3,
        "dec_points_rolling_3"                                       AS dec_points_rolling_3,
        "dec_win_rate_rolling_3"                                     AS dec_win_rate_rolling_3,
        "dec_draw_rate_rolling_3"                                    AS dec_draw_rate_rolling_3,
        "dec_loss_rate_rolling_3"                                    AS dec_loss_rate_rolling_3,
        "dec_clean_sheet_rate_rolling_3"                             AS dec_clean_sheet_rate_rolling_3,
        "dec_failed_to_score_rate_rolling_3"                         AS dec_failed_to_score_rate_rolling_3,
        "dec_field_tilt_rolling_3"                                   AS dec_field_tilt_rolling_3,
        "dec_counter_attack_dna_rolling_3"                           AS dec_counter_attack_dna_rolling_3,
        "dec_attack_left_pct_rolling_3"                              AS dec_attack_left_pct_rolling_3,
        "dec_attack_center_pct_rolling_3"                            AS dec_attack_center_pct_rolling_3,
        "dec_attack_right_pct_rolling_3"                             AS dec_attack_right_pct_rolling_3,
        "dec_def_exposed_left_pct_rolling_3"                         AS dec_def_exposed_left_pct_rolling_3,
        "dec_def_exposed_center_pct_rolling_3"                       AS dec_def_exposed_center_pct_rolling_3,
        "dec_def_exposed_right_pct_rolling_3"                        AS dec_def_exposed_right_pct_rolling_3,
        "dec_cross_rate_rolling_3"                                   AS dec_cross_rate_rolling_3,
        "dec_through_ball_rate_rolling_3"                            AS dec_through_ball_rate_rolling_3,
        "dec_long_ball_rate_rolling_3"                               AS dec_long_ball_rate_rolling_3,
        "dec_shot_six_yard_pct_rolling_3"                            AS dec_shot_six_yard_pct_rolling_3,
        "dec_shot_open_play_pct_rolling_3"                           AS dec_shot_open_play_pct_rolling_3,
        "dec_shot_set_piece_pct_rolling_3"                           AS dec_shot_set_piece_pct_rolling_3,
        "dec_set_piece_reliance_rolling_3"                           AS dec_set_piece_reliance_rolling_3,
        "dec_defensive_line_height_rolling_3"                        AS dec_defensive_line_height_rolling_3,
        "dec_yellow_cards_rolling_3"                                 AS dec_yellow_cards_rolling_3,
        "dec_fouls_drawn_rolling_3"                                  AS dec_fouls_drawn_rolling_3,
        "dec_corners_for_rolling_3"                                  AS dec_corners_for_rolling_3,
        "dec_corners_against_rolling_3"                              AS dec_corners_against_rolling_3,
        "dec_shots_blocked_rolling_3"                                AS dec_shots_blocked_rolling_3,
        "dec_avg_gf_rolling_5"                                       AS dec_avg_gf_rolling_5,
        "dec_avg_ga_rolling_5"                                       AS dec_avg_ga_rolling_5,
        "dec_points_rolling_5"                                       AS dec_points_rolling_5,
        "dec_win_rate_rolling_5"                                     AS dec_win_rate_rolling_5,
        "dec_draw_rate_rolling_5"                                    AS dec_draw_rate_rolling_5,
        "dec_loss_rate_rolling_5"                                    AS dec_loss_rate_rolling_5,
        "dec_clean_sheet_rate_rolling_5"                             AS dec_clean_sheet_rate_rolling_5,
        "dec_failed_to_score_rate_rolling_5"                         AS dec_failed_to_score_rate_rolling_5,
        "dec_field_tilt_rolling_5"                                   AS dec_field_tilt_rolling_5,
        "dec_counter_attack_dna_rolling_5"                           AS dec_counter_attack_dna_rolling_5,
        "dec_attack_left_pct_rolling_5"                              AS dec_attack_left_pct_rolling_5,
        "dec_attack_center_pct_rolling_5"                            AS dec_attack_center_pct_rolling_5,
        "dec_attack_right_pct_rolling_5"                             AS dec_attack_right_pct_rolling_5,
        "dec_def_exposed_left_pct_rolling_5"                         AS dec_def_exposed_left_pct_rolling_5,
        "dec_def_exposed_center_pct_rolling_5"                       AS dec_def_exposed_center_pct_rolling_5,
        "dec_def_exposed_right_pct_rolling_5"                        AS dec_def_exposed_right_pct_rolling_5,
        "dec_cross_rate_rolling_5"                                   AS dec_cross_rate_rolling_5,
        "dec_through_ball_rate_rolling_5"                            AS dec_through_ball_rate_rolling_5,
        "dec_long_ball_rate_rolling_5"                               AS dec_long_ball_rate_rolling_5,
        "dec_shot_six_yard_pct_rolling_5"                            AS dec_shot_six_yard_pct_rolling_5,
        "dec_shot_open_play_pct_rolling_5"                           AS dec_shot_open_play_pct_rolling_5,
        "dec_shot_set_piece_pct_rolling_5"                           AS dec_shot_set_piece_pct_rolling_5,
        "dec_set_piece_reliance_rolling_5"                           AS dec_set_piece_reliance_rolling_5,
        "dec_defensive_line_height_rolling_5"                        AS dec_defensive_line_height_rolling_5,
        "dec_yellow_cards_rolling_5"                                 AS dec_yellow_cards_rolling_5,
        "dec_fouls_drawn_rolling_5"                                  AS dec_fouls_drawn_rolling_5,
        "dec_corners_for_rolling_5"                                  AS dec_corners_for_rolling_5,
        "dec_corners_against_rolling_5"                              AS dec_corners_against_rolling_5,
        "dec_shots_blocked_rolling_5"                                AS dec_shots_blocked_rolling_5,
        "dec_avg_gf_rolling_10"                                      AS dec_avg_gf_rolling_10,
        "dec_avg_ga_rolling_10"                                      AS dec_avg_ga_rolling_10,
        "dec_points_rolling_10"                                      AS dec_points_rolling_10,
        "dec_win_rate_rolling_10"                                    AS dec_win_rate_rolling_10,
        "dec_draw_rate_rolling_10"                                   AS dec_draw_rate_rolling_10,
        "dec_loss_rate_rolling_10"                                   AS dec_loss_rate_rolling_10,
        "dec_clean_sheet_rate_rolling_10"                            AS dec_clean_sheet_rate_rolling_10,
        "dec_failed_to_score_rate_rolling_10"                        AS dec_failed_to_score_rate_rolling_10,
        "dec_field_tilt_rolling_10"                                  AS dec_field_tilt_rolling_10,
        "dec_counter_attack_dna_rolling_10"                          AS dec_counter_attack_dna_rolling_10,
        "dec_attack_left_pct_rolling_10"                             AS dec_attack_left_pct_rolling_10,
        "dec_attack_center_pct_rolling_10"                           AS dec_attack_center_pct_rolling_10,
        "dec_attack_right_pct_rolling_10"                            AS dec_attack_right_pct_rolling_10,
        "dec_def_exposed_left_pct_rolling_10"                        AS dec_def_exposed_left_pct_rolling_10,
        "dec_def_exposed_center_pct_rolling_10"                      AS dec_def_exposed_center_pct_rolling_10,
        "dec_def_exposed_right_pct_rolling_10"                       AS dec_def_exposed_right_pct_rolling_10,
        "dec_cross_rate_rolling_10"                                  AS dec_cross_rate_rolling_10,
        "dec_through_ball_rate_rolling_10"                           AS dec_through_ball_rate_rolling_10,
        "dec_long_ball_rate_rolling_10"                              AS dec_long_ball_rate_rolling_10,
        "dec_shot_six_yard_pct_rolling_10"                           AS dec_shot_six_yard_pct_rolling_10,
        "dec_shot_open_play_pct_rolling_10"                          AS dec_shot_open_play_pct_rolling_10,
        "dec_shot_set_piece_pct_rolling_10"                          AS dec_shot_set_piece_pct_rolling_10,
        "dec_set_piece_reliance_rolling_10"                          AS dec_set_piece_reliance_rolling_10,
        "dec_defensive_line_height_rolling_10"                       AS dec_defensive_line_height_rolling_10,
        "dec_yellow_cards_rolling_10"                                AS dec_yellow_cards_rolling_10,
        "dec_fouls_drawn_rolling_10"                                 AS dec_fouls_drawn_rolling_10,
        "dec_corners_for_rolling_10"                                 AS dec_corners_for_rolling_10,
        "dec_corners_against_rolling_10"                             AS dec_corners_against_rolling_10,
        "dec_shots_blocked_rolling_10"                               AS dec_shots_blocked_rolling_10,
        "opp_int_days_since_last_game"                               AS int_opp_days_since_last_game,
        "opp_dec_avg_gf_rolling_3"                                   AS dec_opp_avg_gf_rolling_3,
        "opp_dec_avg_ga_rolling_3"                                   AS dec_opp_avg_ga_rolling_3,
        "opp_dec_points_rolling_3"                                   AS dec_opp_points_rolling_3,
        "opp_dec_win_rate_rolling_3"                                 AS dec_opp_win_rate_rolling_3,
        "opp_dec_draw_rate_rolling_3"                                AS dec_opp_draw_rate_rolling_3,
        "opp_dec_loss_rate_rolling_3"                                AS dec_opp_loss_rate_rolling_3,
        "opp_dec_clean_sheet_rate_rolling_3"                         AS dec_opp_clean_sheet_rate_rolling_3,
        "opp_dec_failed_to_score_rate_rolling_3"                     AS dec_opp_failed_to_score_rate_rolling_3,
        "opp_dec_field_tilt_rolling_3"                               AS dec_opp_field_tilt_rolling_3,
        "opp_dec_counter_attack_dna_rolling_3"                       AS dec_opp_counter_attack_dna_rolling_3,
        "opp_dec_attack_left_pct_rolling_3"                          AS dec_opp_attack_left_pct_rolling_3,
        "opp_dec_attack_center_pct_rolling_3"                        AS dec_opp_attack_center_pct_rolling_3,
        "opp_dec_attack_right_pct_rolling_3"                         AS dec_opp_attack_right_pct_rolling_3,
        "opp_dec_def_exposed_left_pct_rolling_3"                     AS dec_opp_def_exposed_left_pct_rolling_3,
        "opp_dec_def_exposed_center_pct_rolling_3"                   AS dec_opp_def_exposed_center_pct_rolling_3,
        "opp_dec_def_exposed_right_pct_rolling_3"                    AS dec_opp_def_exposed_right_pct_rolling_3,
        "opp_dec_cross_rate_rolling_3"                               AS dec_opp_cross_rate_rolling_3,
        "opp_dec_through_ball_rate_rolling_3"                        AS dec_opp_through_ball_rate_rolling_3,
        "opp_dec_long_ball_rate_rolling_3"                           AS dec_opp_long_ball_rate_rolling_3,
        "opp_dec_shot_six_yard_pct_rolling_3"                        AS dec_opp_shot_six_yard_pct_rolling_3,
        "opp_dec_shot_open_play_pct_rolling_3"                       AS dec_opp_shot_open_play_pct_rolling_3,
        "opp_dec_shot_set_piece_pct_rolling_3"                       AS dec_opp_shot_set_piece_pct_rolling_3,
        "opp_dec_set_piece_reliance_rolling_3"                       AS dec_opp_set_piece_reliance_rolling_3,
        "opp_dec_defensive_line_height_rolling_3"                    AS dec_opp_defensive_line_height_rolling_3,
        "opp_dec_yellow_cards_rolling_3"                             AS dec_opp_yellow_cards_rolling_3,
        "opp_dec_fouls_drawn_rolling_3"                              AS dec_opp_fouls_drawn_rolling_3,
        "opp_dec_corners_for_rolling_3"                              AS dec_opp_corners_for_rolling_3,
        "opp_dec_corners_against_rolling_3"                          AS dec_opp_corners_against_rolling_3,
        "opp_dec_shots_blocked_rolling_3"                            AS dec_opp_shots_blocked_rolling_3,
        "opp_dec_avg_gf_rolling_5"                                   AS dec_opp_avg_gf_rolling_5,
        "opp_dec_avg_ga_rolling_5"                                   AS dec_opp_avg_ga_rolling_5,
        "opp_dec_points_rolling_5"                                   AS dec_opp_points_rolling_5,
        "opp_dec_win_rate_rolling_5"                                 AS dec_opp_win_rate_rolling_5,
        "opp_dec_draw_rate_rolling_5"                                AS dec_opp_draw_rate_rolling_5,
        "opp_dec_loss_rate_rolling_5"                                AS dec_opp_loss_rate_rolling_5,
        "opp_dec_clean_sheet_rate_rolling_5"                         AS dec_opp_clean_sheet_rate_rolling_5,
        "opp_dec_failed_to_score_rate_rolling_5"                     AS dec_opp_failed_to_score_rate_rolling_5,
        "opp_dec_field_tilt_rolling_5"                               AS dec_opp_field_tilt_rolling_5,
        "opp_dec_counter_attack_dna_rolling_5"                       AS dec_opp_counter_attack_dna_rolling_5,
        "opp_dec_attack_left_pct_rolling_5"                          AS dec_opp_attack_left_pct_rolling_5,
        "opp_dec_attack_center_pct_rolling_5"                        AS dec_opp_attack_center_pct_rolling_5,
        "opp_dec_attack_right_pct_rolling_5"                         AS dec_opp_attack_right_pct_rolling_5,
        "opp_dec_def_exposed_left_pct_rolling_5"                     AS dec_opp_def_exposed_left_pct_rolling_5,
        "opp_dec_def_exposed_center_pct_rolling_5"                   AS dec_opp_def_exposed_center_pct_rolling_5,
        "opp_dec_def_exposed_right_pct_rolling_5"                    AS dec_opp_def_exposed_right_pct_rolling_5,
        "opp_dec_cross_rate_rolling_5"                               AS dec_opp_cross_rate_rolling_5,
        "opp_dec_through_ball_rate_rolling_5"                        AS dec_opp_through_ball_rate_rolling_5,
        "opp_dec_long_ball_rate_rolling_5"                           AS dec_opp_long_ball_rate_rolling_5,
        "opp_dec_shot_six_yard_pct_rolling_5"                        AS dec_opp_shot_six_yard_pct_rolling_5,
        "opp_dec_shot_open_play_pct_rolling_5"                       AS dec_opp_shot_open_play_pct_rolling_5,
        "opp_dec_shot_set_piece_pct_rolling_5"                       AS dec_opp_shot_set_piece_pct_rolling_5,
        "opp_dec_set_piece_reliance_rolling_5"                       AS dec_opp_set_piece_reliance_rolling_5,
        "opp_dec_defensive_line_height_rolling_5"                    AS dec_opp_defensive_line_height_rolling_5,
        "opp_dec_yellow_cards_rolling_5"                             AS dec_opp_yellow_cards_rolling_5,
        "opp_dec_fouls_drawn_rolling_5"                              AS dec_opp_fouls_drawn_rolling_5,
        "opp_dec_corners_for_rolling_5"                              AS dec_opp_corners_for_rolling_5,
        "opp_dec_corners_against_rolling_5"                          AS dec_opp_corners_against_rolling_5,
        "opp_dec_shots_blocked_rolling_5"                            AS dec_opp_shots_blocked_rolling_5,
        "opp_dec_avg_gf_rolling_10"                                  AS dec_opp_avg_gf_rolling_10,
        "opp_dec_avg_ga_rolling_10"                                  AS dec_opp_avg_ga_rolling_10,
        "opp_dec_points_rolling_10"                                  AS dec_opp_points_rolling_10,
        "opp_dec_win_rate_rolling_10"                                AS dec_opp_win_rate_rolling_10,
        "opp_dec_draw_rate_rolling_10"                               AS dec_opp_draw_rate_rolling_10,
        "opp_dec_loss_rate_rolling_10"                               AS dec_opp_loss_rate_rolling_10,
        "opp_dec_clean_sheet_rate_rolling_10"                        AS dec_opp_clean_sheet_rate_rolling_10,
        "opp_dec_failed_to_score_rate_rolling_10"                    AS dec_opp_failed_to_score_rate_rolling_10,
        "opp_dec_field_tilt_rolling_10"                              AS dec_opp_field_tilt_rolling_10,
        "opp_dec_counter_attack_dna_rolling_10"                      AS dec_opp_counter_attack_dna_rolling_10,
        "opp_dec_attack_left_pct_rolling_10"                         AS dec_opp_attack_left_pct_rolling_10,
        "opp_dec_attack_center_pct_rolling_10"                       AS dec_opp_attack_center_pct_rolling_10,
        "opp_dec_attack_right_pct_rolling_10"                        AS dec_opp_attack_right_pct_rolling_10,
        "opp_dec_def_exposed_left_pct_rolling_10"                    AS dec_opp_def_exposed_left_pct_rolling_10,
        "opp_dec_def_exposed_center_pct_rolling_10"                  AS dec_opp_def_exposed_center_pct_rolling_10,
        "opp_dec_def_exposed_right_pct_rolling_10"                   AS dec_opp_def_exposed_right_pct_rolling_10,
        "opp_dec_cross_rate_rolling_10"                              AS dec_opp_cross_rate_rolling_10,
        "opp_dec_through_ball_rate_rolling_10"                       AS dec_opp_through_ball_rate_rolling_10,
        "opp_dec_long_ball_rate_rolling_10"                          AS dec_opp_long_ball_rate_rolling_10,
        "opp_dec_shot_six_yard_pct_rolling_10"                       AS dec_opp_shot_six_yard_pct_rolling_10,
        "opp_dec_shot_open_play_pct_rolling_10"                      AS dec_opp_shot_open_play_pct_rolling_10,
        "opp_dec_shot_set_piece_pct_rolling_10"                      AS dec_opp_shot_set_piece_pct_rolling_10,
        "opp_dec_set_piece_reliance_rolling_10"                      AS dec_opp_set_piece_reliance_rolling_10,
        "opp_dec_defensive_line_height_rolling_10"                   AS dec_opp_defensive_line_height_rolling_10,
        "opp_dec_yellow_cards_rolling_10"                            AS dec_opp_yellow_cards_rolling_10,
        "opp_dec_fouls_drawn_rolling_10"                             AS dec_opp_fouls_drawn_rolling_10,
        "opp_dec_corners_for_rolling_10"                             AS dec_opp_corners_for_rolling_10,
        "opp_dec_corners_against_rolling_10"                         AS dec_opp_corners_against_rolling_10,
        "opp_dec_shots_blocked_rolling_10"                           AS dec_opp_shots_blocked_rolling_10,
        CAST(h2h_played AS BIGINT)                                   AS int_h2h_played,
        CAST(h2h_home_played AS BIGINT)                              AS int_h2h_home_played,
        "h2h_win_rate_cutoff"                                        AS dec_h2h_win_rate_cutoff,
        "h2h_draw_rate_cutoff"                                       AS dec_h2h_draw_rate_cutoff,
        "h2h_loss_rate_cutoff"                                       AS dec_h2h_loss_rate_cutoff,
        "h2h_home_win_rate_cutoff"                                   AS dec_h2h_home_win_rate_cutoff,
        "h2h_avg_gf_10"                                              AS dec_h2h_avg_gf_10,
        "h2h_avg_ga_10"                                              AS dec_h2h_avg_ga_10,
        "self_team_xgbuildup_lag"                                    AS dec_self_team_xgbuildup_lag,
        "opp_team_xgbuildup_lag"                                     AS dec_opp_team_xgbuildup_lag
    FROM mdl_body
)

SELECT mo.*, el.* EXCLUDE (str_match_id, str_team_id)
FROM mdl_out mo
LEFT JOIN {{ ref('gold_team_match_elo') }} el
    ON el.str_match_id = mo.str_match_id AND el.str_team_id = mo.str_team_id
