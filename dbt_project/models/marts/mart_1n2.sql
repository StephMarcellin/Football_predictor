{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_1n2 — grain (match_id, team_id) — cible result_1n2 (V/N/D).
-- 100 % CTE. Le profil d'équipe (forme/style + qualité du onze) est construit
-- UNE fois dans team_profile, puis réutilisé pour soi et pour l'adversaire.
-- (Couche zonale imputée + gardien : étape B.)
--
-- Refonte nommage : backbone, equipe_match et equipe_gardien_match sont lus sous
-- leurs nouveaux noms ; les CLÉS sont remappées vers des noms de travail (match_id,
-- team_id INTEGER…) pour joindre les modèles pas encore refondus
-- (equipe_lineup_match, int_lineup_formation, equipe_adversaire_match, …).
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

-- equipe_adversaire_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
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
    FROM {{ ref('equipe_adversaire_match') }}
),

-- equipe_confrontation_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
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
    FROM {{ ref('equipe_confrontation_match') }}
),

-- equipe_confrontation_zone lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_equipe_confrontation_zone AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        dec_off_danger_gauche                                        AS "off_danger_gauche",
        dec_off_danger_axe                                           AS "off_danger_axe",
        dec_off_danger_droit                                         AS "off_danger_droit",
        dec_off_cross_gauche                                         AS "off_cross_gauche",
        dec_off_cross_droit                                          AS "off_cross_droit",
        dec_off_dribble_gauche                                       AS "off_dribble_gauche",
        dec_off_dribble_droit                                        AS "off_dribble_droit",
        dec_off_central_axe                                          AS "off_central_axe",
        dec_def_danger_gauche                                        AS "def_danger_gauche",
        dec_def_danger_axe                                           AS "def_danger_axe",
        dec_def_danger_droit                                         AS "def_danger_droit",
        dec_def_cross_gauche                                         AS "def_cross_gauche",
        dec_def_cross_droit                                          AS "def_cross_droit",
        dec_def_dribble_gauche                                       AS "def_dribble_gauche",
        dec_def_dribble_droit                                        AS "def_dribble_droit",
        dec_def_central_axe                                          AS "def_central_axe"
    FROM {{ ref('equipe_confrontation_zone') }}
),

-- equipe_gardien_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_equipe_gardien_match AS (
    SELECT
        str_match_id                                                 AS "str_match_id",
        str_team_id                                                  AS "str_team_id",
        dec_keeper_psxg_plus_minus_lag                               AS "keeper_psxg_plus_minus_lag",
        dec_keeper_psxg_per_shot_lag                                 AS "keeper_psxg_per_shot_lag",
        dec_keeper_save_pct_lag                                      AS "keeper_save_pct_lag",
        CAST(int_keeper_shots_faced_lag AS HUGEINT)                  AS "keeper_shots_faced_lag"
    FROM {{ ref('equipe_gardien_match') }}
),

-- equipe_lineup_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_equipe_lineup_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        dec_lineup_sum_xg_per90_lag                                  AS "lineup_sum_xg_per90_lag",
        dec_lineup_avg_shots_per90_lag                               AS "lineup_avg_shots_per90_lag",
        dec_lineup_avg_def_actions_per90_lag                         AS "lineup_avg_def_actions_per90_lag",
        dec_lineup_avg_aerial_win_rate_lag                           AS "lineup_avg_aerial_win_rate_lag",
        int_n_starters_profiled                                      AS "n_starters_profiled"
    FROM {{ ref('equipe_lineup_match') }}
),

-- int_formation_matchup_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_formation_matchup_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        int_attack_overload                                          AS "attack_overload",
        int_mid_control_delta                                        AS "mid_control_delta",
        dec_back_depth_delta                                         AS "back_depth_delta",
        dec_width_delta                                              AS "width_delta",
        dec_depth_delta                                              AS "depth_delta",
        dec_axiality_delta                                           AS "axiality_delta",
        str_formation_family_self                                    AS "formation_family_self",
        str_formation_family_opp                                     AS "formation_family_opp",
        str_matchup_family                                           AS "matchup_family"
    FROM {{ ref('int_formation_matchup_match') }}
),

-- int_lineup_formation lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_lineup_formation AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        str_formation_family                                         AS "formation_family",
        int_n_gk                                                     AS "n_gk",
        int_n_def                                                    AS "n_def",
        int_n_mid                                                    AS "n_mid",
        int_n_att                                                    AS "n_att",
        int_n_wingers                                                AS "n_wingers",
        int_n_central_att                                            AS "n_central_att",
        dec_bloc_width                                               AS "bloc_width",
        dec_bloc_depth                                               AS "bloc_depth",
        dec_line_defensive_avg                                       AS "line_defensive_avg",
        dec_line_offensive_avg                                       AS "line_offensive_avg",
        dec_axiality_score                                           AS "axiality_score"
    FROM {{ ref('int_lineup_formation') }}
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
        str_result_1n2                     as result_1n2,

        -- ── Cotes / probabilités (déjà pivotées par venue dans backbone) ──────
        -- Passage quasi-direct : faits ponctuels du match, pas d'agrégat glissant.
        -- Ouverture
        dec_odds_pinnacle_team, dec_odds_pinnacle_draw, dec_odds_pinnacle_opp,
        dec_odds_avg_team, dec_odds_avg_draw, dec_odds_avg_opp,
        dec_pinnacle_prob_team, dec_pinnacle_prob_draw, dec_pinnacle_prob_opp,
        dec_market_prob_team, dec_market_prob_draw, dec_market_prob_opp,
        (dec_pinnacle_prob_team - dec_pinnacle_prob_opp) as pinnacle_edge,
        -- Clôture (les plus "sharp")
        dec_pinnacle_prob_close_team, dec_pinnacle_prob_close_draw, dec_pinnacle_prob_close_opp,
        dec_market_prob_close_team,   dec_market_prob_close_draw,   dec_market_prob_close_opp,
        (dec_pinnacle_prob_close_team - dec_pinnacle_prob_close_opp) as pinnacle_close_edge,
        -- Drift ouverture→clôture (mouvement de ligne)
        dec_pinnacle_drift_team, dec_pinnacle_drift_draw, dec_pinnacle_drift_opp,
        -- Over/Under 2.5 (profil de buts attendu par le marché)
        dec_pinnacle_prob_over25,       dec_pinnacle_prob_under25,
        dec_pinnacle_prob_close_over25, dec_pinnacle_prob_close_under25

    from {{ ref('backbone') }}
    where str_match_id is not null and str_team_id is not null
),

-- equipe_match sous ses nouveaux noms : clés remappées pour les jointures,
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
    from {{ ref('equipe_match') }}
),

equipe_gardien_match_in as (
    select
        str_match_id                       as match_id,
        cast(str_team_id as bigint)       as team_id,
        * exclude (str_match_id, str_team_id)
    from in_equipe_gardien_match
),

-- Profil d'équipe : forme/style (equipe_match) + qualité du onze (equipe_lineup_match).
team_profile as (
    select em.*,
           lu.* exclude (match_id, team_id),
           fm.* exclude (match_id, team_id, formation_id)   -- features formation Phase 4
    from equipe_match_in em
    left join in_equipe_lineup_match  lu using (match_id, team_id)
    left join in_int_lineup_formation fm using (match_id, team_id)
),

-- Le même profil, toutes colonnes préfixées opp_ (pour l'adversaire).
team_profile_opp as (
    select columns('.*') as "opp_\0"
    from team_profile
),

-- Directionnel soi↔adversaire : H2H + pressing vs relance. Grain (match, team).
directional as (
    select h2h.*,
           conf.* exclude (match_id, team_id, opponent_id),
           fmu.*  exclude (match_id, team_id, opponent_id)   -- matchup formation Phase 4B
    from in_equipe_adversaire_match h2h
    left join in_equipe_confrontation_match    conf using (match_id, team_id)
    left join in_int_formation_matchup_match   fmu  using (match_id, team_id)
),

assembled as (
    select
        base.*,
        tp.*  exclude (match_id, team_id, opponent_id, date, season, league_source, venue),
        tpo.* exclude (opp_match_id, opp_team_id, opp_opponent_id, opp_date, opp_season,
                       opp_league_source, opp_venue, opp_int_is_home, opp_comp_category),
        d.*   exclude (match_id, team_id, opponent_id, date, season, league_source),
        zcz.* exclude (match_id, team_id),
        kg.*  exclude (match_id, team_id)
    from base
    left join team_profile     tp  using (match_id, team_id)
    left join team_profile_opp tpo on tpo.opp_match_id = base.match_id and tpo.opp_team_id = base.opponent_id
    left join directional      d   using (match_id, team_id)
    left join in_equipe_confrontation_zone zcz using (match_id, team_id)
    left join equipe_gardien_match_in                kg  using (match_id, team_id)
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
        "dec_odds_pinnacle_team"                                     AS dec_odds_pinnacle_team,
        "dec_odds_pinnacle_draw"                                     AS dec_odds_pinnacle_draw,
        "dec_odds_pinnacle_opp"                                      AS dec_odds_pinnacle_opp,
        "dec_odds_avg_team"                                          AS dec_odds_avg_team,
        "dec_odds_avg_draw"                                          AS dec_odds_avg_draw,
        "dec_odds_avg_opp"                                           AS dec_odds_avg_opp,
        "dec_pinnacle_prob_team"                                     AS dec_pinnacle_prob_team,
        "dec_pinnacle_prob_draw"                                     AS dec_pinnacle_prob_draw,
        "dec_pinnacle_prob_opp"                                      AS dec_pinnacle_prob_opp,
        "dec_market_prob_team"                                       AS dec_market_prob_team,
        "dec_market_prob_draw"                                       AS dec_market_prob_draw,
        "dec_market_prob_opp"                                        AS dec_market_prob_opp,
        "pinnacle_edge"                                              AS dec_pinnacle_edge,
        "dec_pinnacle_prob_close_team"                               AS dec_pinnacle_prob_close_team,
        "dec_pinnacle_prob_close_draw"                               AS dec_pinnacle_prob_close_draw,
        "dec_pinnacle_prob_close_opp"                                AS dec_pinnacle_prob_close_opp,
        "dec_market_prob_close_team"                                 AS dec_market_prob_close_team,
        "dec_market_prob_close_draw"                                 AS dec_market_prob_close_draw,
        "dec_market_prob_close_opp"                                  AS dec_market_prob_close_opp,
        "pinnacle_close_edge"                                        AS dec_pinnacle_close_edge,
        "dec_pinnacle_drift_team"                                    AS dec_pinnacle_drift_team,
        "dec_pinnacle_drift_draw"                                    AS dec_pinnacle_drift_draw,
        "dec_pinnacle_drift_opp"                                     AS dec_pinnacle_drift_opp,
        "dec_pinnacle_prob_over25"                                   AS dec_pinnacle_prob_over25,
        "dec_pinnacle_prob_under25"                                  AS dec_pinnacle_prob_under25,
        "dec_pinnacle_prob_close_over25"                             AS dec_pinnacle_prob_close_over25,
        "dec_pinnacle_prob_close_under25"                            AS dec_pinnacle_prob_close_under25,
        "comp_category"                                              AS str_comp_category,
        "int_is_home"                                                AS int_is_home,
        "int_days_since_last_game"                                   AS int_days_since_last_game,
        "dec_avg_gf_rolling_3"                                       AS dec_avg_gf_rolling_3,
        "dec_avg_ga_rolling_3"                                       AS dec_avg_ga_rolling_3,
        "dec_avg_np_xg_rolling_3"                                    AS dec_avg_np_xg_rolling_3,
        "dec_avg_np_xg_conceded_rolling_3"                           AS dec_avg_np_xg_conceded_rolling_3,
        "dec_avg_np_xg_diff_rolling_3"                               AS dec_avg_np_xg_diff_rolling_3,
        "dec_points_rolling_3"                                       AS dec_points_rolling_3,
        "dec_win_rate_rolling_3"                                     AS dec_win_rate_rolling_3,
        "dec_draw_rate_rolling_3"                                    AS dec_draw_rate_rolling_3,
        "dec_loss_rate_rolling_3"                                    AS dec_loss_rate_rolling_3,
        "dec_clean_sheet_rate_rolling_3"                             AS dec_clean_sheet_rate_rolling_3,
        "dec_failed_to_score_rate_rolling_3"                         AS dec_failed_to_score_rate_rolling_3,
        "dec_shots_rolling_3"                                        AS dec_shots_rolling_3,
        "dec_shots_ot_rolling_3"                                     AS dec_shots_ot_rolling_3,
        "dec_ppda_rolling_3"                                         AS dec_ppda_rolling_3,
        "dec_ppda_allowed_rolling_3"                                 AS dec_ppda_allowed_rolling_3,
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
        "dec_fouls_committed_rolling_3"                              AS dec_fouls_committed_rolling_3,
        "dec_fouls_drawn_rolling_3"                                  AS dec_fouls_drawn_rolling_3,
        "dec_corners_for_rolling_3"                                  AS dec_corners_for_rolling_3,
        "dec_corners_against_rolling_3"                              AS dec_corners_against_rolling_3,
        "dec_shots_blocked_rolling_3"                                AS dec_shots_blocked_rolling_3,
        "dec_avg_gf_rolling_5"                                       AS dec_avg_gf_rolling_5,
        "dec_avg_ga_rolling_5"                                       AS dec_avg_ga_rolling_5,
        "dec_avg_np_xg_rolling_5"                                    AS dec_avg_np_xg_rolling_5,
        "dec_avg_np_xg_conceded_rolling_5"                           AS dec_avg_np_xg_conceded_rolling_5,
        "dec_avg_np_xg_diff_rolling_5"                               AS dec_avg_np_xg_diff_rolling_5,
        "dec_points_rolling_5"                                       AS dec_points_rolling_5,
        "dec_win_rate_rolling_5"                                     AS dec_win_rate_rolling_5,
        "dec_draw_rate_rolling_5"                                    AS dec_draw_rate_rolling_5,
        "dec_loss_rate_rolling_5"                                    AS dec_loss_rate_rolling_5,
        "dec_clean_sheet_rate_rolling_5"                             AS dec_clean_sheet_rate_rolling_5,
        "dec_failed_to_score_rate_rolling_5"                         AS dec_failed_to_score_rate_rolling_5,
        "dec_shots_rolling_5"                                        AS dec_shots_rolling_5,
        "dec_shots_ot_rolling_5"                                     AS dec_shots_ot_rolling_5,
        "dec_ppda_rolling_5"                                         AS dec_ppda_rolling_5,
        "dec_ppda_allowed_rolling_5"                                 AS dec_ppda_allowed_rolling_5,
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
        "dec_fouls_committed_rolling_5"                              AS dec_fouls_committed_rolling_5,
        "dec_fouls_drawn_rolling_5"                                  AS dec_fouls_drawn_rolling_5,
        "dec_corners_for_rolling_5"                                  AS dec_corners_for_rolling_5,
        "dec_corners_against_rolling_5"                              AS dec_corners_against_rolling_5,
        "dec_shots_blocked_rolling_5"                                AS dec_shots_blocked_rolling_5,
        "dec_avg_gf_rolling_10"                                      AS dec_avg_gf_rolling_10,
        "dec_avg_ga_rolling_10"                                      AS dec_avg_ga_rolling_10,
        "dec_avg_np_xg_rolling_10"                                   AS dec_avg_np_xg_rolling_10,
        "dec_avg_np_xg_conceded_rolling_10"                          AS dec_avg_np_xg_conceded_rolling_10,
        "dec_avg_np_xg_diff_rolling_10"                              AS dec_avg_np_xg_diff_rolling_10,
        "dec_points_rolling_10"                                      AS dec_points_rolling_10,
        "dec_win_rate_rolling_10"                                    AS dec_win_rate_rolling_10,
        "dec_draw_rate_rolling_10"                                   AS dec_draw_rate_rolling_10,
        "dec_loss_rate_rolling_10"                                   AS dec_loss_rate_rolling_10,
        "dec_clean_sheet_rate_rolling_10"                            AS dec_clean_sheet_rate_rolling_10,
        "dec_failed_to_score_rate_rolling_10"                        AS dec_failed_to_score_rate_rolling_10,
        "dec_shots_rolling_10"                                       AS dec_shots_rolling_10,
        "dec_shots_ot_rolling_10"                                    AS dec_shots_ot_rolling_10,
        "dec_ppda_rolling_10"                                        AS dec_ppda_rolling_10,
        "dec_ppda_allowed_rolling_10"                                AS dec_ppda_allowed_rolling_10,
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
        "dec_fouls_committed_rolling_10"                             AS dec_fouls_committed_rolling_10,
        "dec_fouls_drawn_rolling_10"                                 AS dec_fouls_drawn_rolling_10,
        "dec_corners_for_rolling_10"                                 AS dec_corners_for_rolling_10,
        "dec_corners_against_rolling_10"                             AS dec_corners_against_rolling_10,
        "dec_shots_blocked_rolling_10"                               AS dec_shots_blocked_rolling_10,
        "dec_season_xg_per_shot_for_lag"                             AS dec_season_xg_per_shot_for_lag,
        "dec_season_xg_per_shot_against_lag"                         AS dec_season_xg_per_shot_against_lag,
        "lineup_sum_xg_per90_lag"                                    AS dec_lineup_sum_xg_per90_lag,
        "lineup_avg_shots_per90_lag"                                 AS dec_lineup_avg_shots_per90_lag,
        "lineup_avg_def_actions_per90_lag"                           AS dec_lineup_avg_def_actions_per90_lag,
        "lineup_avg_aerial_win_rate_lag"                             AS dec_lineup_avg_aerial_win_rate_lag,
        "n_starters_profiled"                                        AS int_n_starters_profiled,
        "formation_family"                                           AS str_formation_family,
        "n_gk"                                                       AS int_n_gk,
        "n_def"                                                      AS int_n_def,
        "n_mid"                                                      AS int_n_mid,
        "n_att"                                                      AS int_n_att,
        "n_wingers"                                                  AS int_n_wingers,
        "n_central_att"                                              AS int_n_central_att,
        "bloc_width"                                                 AS dec_bloc_width,
        "bloc_depth"                                                 AS dec_bloc_depth,
        "line_defensive_avg"                                         AS dec_line_defensive_avg,
        "line_offensive_avg"                                         AS dec_line_offensive_avg,
        "axiality_score"                                             AS dec_axiality_score,
        "opp_int_days_since_last_game"                               AS int_opp_days_since_last_game,
        "opp_dec_avg_gf_rolling_3"                                   AS dec_opp_avg_gf_rolling_3,
        "opp_dec_avg_ga_rolling_3"                                   AS dec_opp_avg_ga_rolling_3,
        "opp_dec_avg_np_xg_rolling_3"                                AS dec_opp_avg_np_xg_rolling_3,
        "opp_dec_avg_np_xg_conceded_rolling_3"                       AS dec_opp_avg_np_xg_conceded_rolling_3,
        "opp_dec_avg_np_xg_diff_rolling_3"                           AS dec_opp_avg_np_xg_diff_rolling_3,
        "opp_dec_points_rolling_3"                                   AS dec_opp_points_rolling_3,
        "opp_dec_win_rate_rolling_3"                                 AS dec_opp_win_rate_rolling_3,
        "opp_dec_draw_rate_rolling_3"                                AS dec_opp_draw_rate_rolling_3,
        "opp_dec_loss_rate_rolling_3"                                AS dec_opp_loss_rate_rolling_3,
        "opp_dec_clean_sheet_rate_rolling_3"                         AS dec_opp_clean_sheet_rate_rolling_3,
        "opp_dec_failed_to_score_rate_rolling_3"                     AS dec_opp_failed_to_score_rate_rolling_3,
        "opp_dec_shots_rolling_3"                                    AS dec_opp_shots_rolling_3,
        "opp_dec_shots_ot_rolling_3"                                 AS dec_opp_shots_ot_rolling_3,
        "opp_dec_ppda_rolling_3"                                     AS dec_opp_ppda_rolling_3,
        "opp_dec_ppda_allowed_rolling_3"                             AS dec_opp_ppda_allowed_rolling_3,
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
        "opp_dec_fouls_committed_rolling_3"                          AS dec_opp_fouls_committed_rolling_3,
        "opp_dec_fouls_drawn_rolling_3"                              AS dec_opp_fouls_drawn_rolling_3,
        "opp_dec_corners_for_rolling_3"                              AS dec_opp_corners_for_rolling_3,
        "opp_dec_corners_against_rolling_3"                          AS dec_opp_corners_against_rolling_3,
        "opp_dec_shots_blocked_rolling_3"                            AS dec_opp_shots_blocked_rolling_3,
        "opp_dec_avg_gf_rolling_5"                                   AS dec_opp_avg_gf_rolling_5,
        "opp_dec_avg_ga_rolling_5"                                   AS dec_opp_avg_ga_rolling_5,
        "opp_dec_avg_np_xg_rolling_5"                                AS dec_opp_avg_np_xg_rolling_5,
        "opp_dec_avg_np_xg_conceded_rolling_5"                       AS dec_opp_avg_np_xg_conceded_rolling_5,
        "opp_dec_avg_np_xg_diff_rolling_5"                           AS dec_opp_avg_np_xg_diff_rolling_5,
        "opp_dec_points_rolling_5"                                   AS dec_opp_points_rolling_5,
        "opp_dec_win_rate_rolling_5"                                 AS dec_opp_win_rate_rolling_5,
        "opp_dec_draw_rate_rolling_5"                                AS dec_opp_draw_rate_rolling_5,
        "opp_dec_loss_rate_rolling_5"                                AS dec_opp_loss_rate_rolling_5,
        "opp_dec_clean_sheet_rate_rolling_5"                         AS dec_opp_clean_sheet_rate_rolling_5,
        "opp_dec_failed_to_score_rate_rolling_5"                     AS dec_opp_failed_to_score_rate_rolling_5,
        "opp_dec_shots_rolling_5"                                    AS dec_opp_shots_rolling_5,
        "opp_dec_shots_ot_rolling_5"                                 AS dec_opp_shots_ot_rolling_5,
        "opp_dec_ppda_rolling_5"                                     AS dec_opp_ppda_rolling_5,
        "opp_dec_ppda_allowed_rolling_5"                             AS dec_opp_ppda_allowed_rolling_5,
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
        "opp_dec_fouls_committed_rolling_5"                          AS dec_opp_fouls_committed_rolling_5,
        "opp_dec_fouls_drawn_rolling_5"                              AS dec_opp_fouls_drawn_rolling_5,
        "opp_dec_corners_for_rolling_5"                              AS dec_opp_corners_for_rolling_5,
        "opp_dec_corners_against_rolling_5"                          AS dec_opp_corners_against_rolling_5,
        "opp_dec_shots_blocked_rolling_5"                            AS dec_opp_shots_blocked_rolling_5,
        "opp_dec_avg_gf_rolling_10"                                  AS dec_opp_avg_gf_rolling_10,
        "opp_dec_avg_ga_rolling_10"                                  AS dec_opp_avg_ga_rolling_10,
        "opp_dec_avg_np_xg_rolling_10"                               AS dec_opp_avg_np_xg_rolling_10,
        "opp_dec_avg_np_xg_conceded_rolling_10"                      AS dec_opp_avg_np_xg_conceded_rolling_10,
        "opp_dec_avg_np_xg_diff_rolling_10"                          AS dec_opp_avg_np_xg_diff_rolling_10,
        "opp_dec_points_rolling_10"                                  AS dec_opp_points_rolling_10,
        "opp_dec_win_rate_rolling_10"                                AS dec_opp_win_rate_rolling_10,
        "opp_dec_draw_rate_rolling_10"                               AS dec_opp_draw_rate_rolling_10,
        "opp_dec_loss_rate_rolling_10"                               AS dec_opp_loss_rate_rolling_10,
        "opp_dec_clean_sheet_rate_rolling_10"                        AS dec_opp_clean_sheet_rate_rolling_10,
        "opp_dec_failed_to_score_rate_rolling_10"                    AS dec_opp_failed_to_score_rate_rolling_10,
        "opp_dec_shots_rolling_10"                                   AS dec_opp_shots_rolling_10,
        "opp_dec_shots_ot_rolling_10"                                AS dec_opp_shots_ot_rolling_10,
        "opp_dec_ppda_rolling_10"                                    AS dec_opp_ppda_rolling_10,
        "opp_dec_ppda_allowed_rolling_10"                            AS dec_opp_ppda_allowed_rolling_10,
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
        "opp_dec_fouls_committed_rolling_10"                         AS dec_opp_fouls_committed_rolling_10,
        "opp_dec_fouls_drawn_rolling_10"                             AS dec_opp_fouls_drawn_rolling_10,
        "opp_dec_corners_for_rolling_10"                             AS dec_opp_corners_for_rolling_10,
        "opp_dec_corners_against_rolling_10"                         AS dec_opp_corners_against_rolling_10,
        "opp_dec_shots_blocked_rolling_10"                           AS dec_opp_shots_blocked_rolling_10,
        "opp_dec_season_xg_per_shot_for_lag"                         AS dec_opp_season_xg_per_shot_for_lag,
        "opp_dec_season_xg_per_shot_against_lag"                     AS dec_opp_season_xg_per_shot_against_lag,
        "opp_lineup_sum_xg_per90_lag"                                AS dec_opp_lineup_sum_xg_per90_lag,
        "opp_lineup_avg_shots_per90_lag"                             AS dec_opp_lineup_avg_shots_per90_lag,
        "opp_lineup_avg_def_actions_per90_lag"                       AS dec_opp_lineup_avg_def_actions_per90_lag,
        "opp_lineup_avg_aerial_win_rate_lag"                         AS dec_opp_lineup_avg_aerial_win_rate_lag,
        "opp_n_starters_profiled"                                    AS int_opp_n_starters_profiled,
        "opp_formation_family"                                       AS str_opp_formation_family,
        "opp_n_gk"                                                   AS int_opp_n_gk,
        "opp_n_def"                                                  AS int_opp_n_def,
        "opp_n_mid"                                                  AS int_opp_n_mid,
        "opp_n_att"                                                  AS int_opp_n_att,
        "opp_n_wingers"                                              AS int_opp_n_wingers,
        "opp_n_central_att"                                          AS int_opp_n_central_att,
        "opp_bloc_width"                                             AS dec_opp_bloc_width,
        "opp_bloc_depth"                                             AS dec_opp_bloc_depth,
        "opp_line_defensive_avg"                                     AS dec_opp_line_defensive_avg,
        "opp_line_offensive_avg"                                     AS dec_opp_line_offensive_avg,
        "opp_axiality_score"                                         AS dec_opp_axiality_score,
        CAST(h2h_played AS BIGINT)                                   AS int_h2h_played,
        CAST(h2h_home_played AS BIGINT)                              AS int_h2h_home_played,
        "h2h_win_rate_cutoff"                                        AS dec_h2h_win_rate_cutoff,
        "h2h_draw_rate_cutoff"                                       AS dec_h2h_draw_rate_cutoff,
        "h2h_loss_rate_cutoff"                                       AS dec_h2h_loss_rate_cutoff,
        "h2h_home_win_rate_cutoff"                                   AS dec_h2h_home_win_rate_cutoff,
        "h2h_avg_gf_10"                                              AS dec_h2h_avg_gf_10,
        "h2h_avg_ga_10"                                              AS dec_h2h_avg_ga_10,
        "h2h_avg_xg_diff_10"                                         AS dec_h2h_avg_xg_diff_10,
        "opp_press_ppda_rolling_3"                                   AS dec_opp_press_ppda_rolling_3,
        "self_buildup_resistance_rolling_3"                          AS dec_self_buildup_resistance_rolling_3,
        "matchup_high_press_vs_buildup_rolling_3"                    AS dec_matchup_high_press_vs_buildup_rolling_3,
        "opp_press_ppda_rolling_5"                                   AS dec_opp_press_ppda_rolling_5,
        "self_buildup_resistance_rolling_5"                          AS dec_self_buildup_resistance_rolling_5,
        "matchup_high_press_vs_buildup_rolling_5"                    AS dec_matchup_high_press_vs_buildup_rolling_5,
        "opp_press_ppda_rolling_10"                                  AS dec_opp_press_ppda_rolling_10,
        "self_buildup_resistance_rolling_10"                         AS dec_self_buildup_resistance_rolling_10,
        "matchup_high_press_vs_buildup_rolling_10"                   AS dec_matchup_high_press_vs_buildup_rolling_10,
        "self_team_xgbuildup_lag"                                    AS dec_self_team_xgbuildup_lag,
        "opp_team_xgbuildup_lag"                                     AS dec_opp_team_xgbuildup_lag,
        "attack_overload"                                            AS int_attack_overload,
        "mid_control_delta"                                          AS int_mid_control_delta,
        "back_depth_delta"                                           AS dec_back_depth_delta,
        "width_delta"                                                AS dec_width_delta,
        "depth_delta"                                                AS dec_depth_delta,
        "axiality_delta"                                             AS dec_axiality_delta,
        "formation_family_self"                                      AS str_formation_family_self,
        "formation_family_opp"                                       AS str_formation_family_opp,
        "matchup_family"                                             AS str_matchup_family,
        "off_danger_gauche"                                          AS dec_off_danger_gauche,
        "off_danger_axe"                                             AS dec_off_danger_axe,
        "off_danger_droit"                                           AS dec_off_danger_droit,
        "off_cross_gauche"                                           AS dec_off_cross_gauche,
        "off_cross_droit"                                            AS dec_off_cross_droit,
        "off_dribble_gauche"                                         AS dec_off_dribble_gauche,
        "off_dribble_droit"                                          AS dec_off_dribble_droit,
        "off_central_axe"                                            AS dec_off_central_axe,
        "def_danger_gauche"                                          AS dec_def_danger_gauche,
        "def_danger_axe"                                             AS dec_def_danger_axe,
        "def_danger_droit"                                           AS dec_def_danger_droit,
        "def_cross_gauche"                                           AS dec_def_cross_gauche,
        "def_cross_droit"                                            AS dec_def_cross_droit,
        "def_dribble_gauche"                                         AS dec_def_dribble_gauche,
        "def_dribble_droit"                                          AS dec_def_dribble_droit,
        "def_central_axe"                                            AS dec_def_central_axe,
        "keeper_psxg_plus_minus_lag"                                 AS dec_keeper_psxg_plus_minus_lag,
        "keeper_psxg_per_shot_lag"                                   AS dec_keeper_psxg_per_shot_lag,
        "keeper_save_pct_lag"                                        AS dec_keeper_save_pct_lag,
        CAST(keeper_shots_faced_lag AS BIGINT)                       AS int_keeper_shots_faced_lag
    FROM mdl_body
)

SELECT * FROM mdl_out
