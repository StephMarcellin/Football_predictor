{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='joueur_saison'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.joueur_saison — grain (match_id, team_id, player_id) — snapshot as-of-date
-- Familles CDC : 4 (source des agrégats du onze), 5/6 NON-zonales, 8 (carton),
-- 10 (Buteurs).
--   PASSE 1 : player_match_stats (volumes per-90, ratios).
--   PASSE 2 : player_xg_chain (xgChain/xgBuildup), part des tirs de l'équipe,
--             rôles tireur penalty / coup franc.
--   RESTE À FAIRE : scorer_xgot_overperformance (feature 70) — nécessite de
--   relier xgot_predictions aux tirs (int_shot_placement) pour retrouver le
--   tireur ; traité dans une passe dédiée.
--
-- Fenêtre as-of-date : 38 dernières apparitions strictement antérieures
-- (ROWS 38 PRECEDING AND 1 PRECEDING, match courant exclu → anti-leakage).
-- Volumes en PER-90 (Σ stat / Σ minutes × 90) ; ratios exacts.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- freekick_profiles lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_freekick_profiles AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_freekick_taker_id AS INTEGER)                       AS "freekick_taker_id",
        int_row_num                                                  AS "row_num",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        str_fk_type                                                  AS "fk_type",
        bool_is_offside                                              AS "is_offside",
        str_fk_zone_type                                             AS "fk_zone_type",
        str_outcome                                                  AS "outcome",
        dec_chain_danger_total                                       AS "chain_danger_total",
        dec_chain_danger_momentum                                    AS "chain_danger_momentum",
        str_shot_body_part                                           AS "shot_body_part",
        CAST(str_clearance_player_id AS INTEGER)                     AS "clearance_player_id",
        str_clearance_quality                                        AS "clearance_quality",
        bool_is_headed_clearance                                     AS "is_headed_clearance",
        dec_x_m                                                      AS "x_m",
        dec_y_m                                                      AS "y_m",
        dec_distance_to_goal                                         AS "distance_to_goal",
        dec_angle                                                    AS "angle"
    FROM {{ ref('freekick_profiles') }}
),

-- int_penalties lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_penalties AS (
    SELECT
        str_match_id                                                 AS "match_id",
        int_row_num                                                  AS "row_num",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_expanded_minute                                          AS "expanded_minute",
        CAST(str_attacking_team_id AS BIGINT)                        AS "attacking_team_id",
        CAST(str_taker_player_id AS INTEGER)                         AS "taker_player_id",
        CAST(str_defending_team_id AS BIGINT)                        AS "defending_team_id",
        CAST(str_gk_player_id AS INTEGER)                            AS "gk_player_id",
        int_player_drew                                              AS "player_drew",
        int_player_conceded                                          AS "player_conceded",
        bool_is_goal                                                 AS "is_goal",
        bool_is_saved                                                AS "is_saved",
        bool_is_post                                                 AS "is_post",
        bool_is_off_target                                           AS "is_off_target",
        str_result_label                                             AS "result_label",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z"
    FROM {{ ref('int_penalties') }}
),

-- int_shot_placement lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_shot_placement AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        bool_is_own_goal                                             AS "is_own_goal",
        bool_is_goal                                                 AS "is_goal",
        bool_is_blocked                                              AS "is_blocked",
        bool_is_on_target                                            AS "is_on_target",
        dec_pre_shot_xg_proxy                                        AS "pre_shot_xg_proxy",
        dec_offset_center                                            AS "offset_center",
        dec_height                                                   AS "height",
        str_placement_col                                            AS "placement_col",
        str_placement_row                                            AS "placement_row",
        str_placement_zone                                           AS "placement_zone",
        dec_corner_dist                                              AS "corner_dist"
    FROM {{ ref('int_shot_placement') }}
),

-- player_match_stats lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_match_stats AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        int_n_actions                                                AS "n_actions",
        dec_xg_contribution                                          AS "xg_contribution",
        int_n_shots                                                  AS "n_shots",
        CAST(int_n_shots_regular_play AS HUGEINT)                    AS "n_shots_regular_play",
        CAST(int_n_shots_individual_play AS HUGEINT)                 AS "n_shots_individual_play",
        CAST(int_n_shot_assists AS HUGEINT)                          AS "n_shot_assists",
        CAST(int_n_key_passes AS HUGEINT)                            AS "n_key_passes",
        CAST(int_n_longballs AS HUGEINT)                             AS "n_longballs",
        CAST(int_n_crosses AS HUGEINT)                               AS "n_crosses",
        CAST(int_n_throughballs AS HUGEINT)                          AS "n_throughballs",
        int_n_progressive_passes                                     AS "n_progressive_passes",
        int_n_tackles                                                AS "n_tackles",
        int_n_tackles_won                                            AS "n_tackles_won",
        int_n_interceptions                                          AS "n_interceptions",
        int_n_ball_recoveries                                        AS "n_ball_recoveries",
        int_n_challenges                                             AS "n_challenges",
        int_n_clearances                                             AS "n_clearances",
        dec_defensive_zone_x                                         AS "defensive_zone_x",
        int_n_touches                                                AS "n_touches",
        dec_pct_z1_c1                                                AS "pct_z1_c1",
        dec_pct_z1_c2                                                AS "pct_z1_c2",
        dec_pct_z1_c3                                                AS "pct_z1_c3",
        dec_pct_z1_c4                                                AS "pct_z1_c4",
        dec_pct_z1_c5                                                AS "pct_z1_c5",
        dec_pct_z2_c1                                                AS "pct_z2_c1",
        dec_pct_z2_c2                                                AS "pct_z2_c2",
        dec_pct_z2_c3                                                AS "pct_z2_c3",
        dec_pct_z2_c4                                                AS "pct_z2_c4",
        dec_pct_z2_c5                                                AS "pct_z2_c5",
        dec_pct_z3_c1                                                AS "pct_z3_c1",
        dec_pct_z3_c2                                                AS "pct_z3_c2",
        dec_pct_z3_c3                                                AS "pct_z3_c3",
        dec_pct_z3_c4                                                AS "pct_z3_c4",
        dec_pct_z3_c5                                                AS "pct_z3_c5",
        dec_pct_z4_c1                                                AS "pct_z4_c1",
        dec_pct_z4_c2                                                AS "pct_z4_c2",
        dec_pct_z4_c3                                                AS "pct_z4_c3",
        dec_pct_z4_c4                                                AS "pct_z4_c4",
        dec_pct_z4_c5                                                AS "pct_z4_c5",
        dec_pct_z5_c1                                                AS "pct_z5_c1",
        dec_pct_z5_c2                                                AS "pct_z5_c2",
        dec_pct_z5_c3                                                AS "pct_z5_c3",
        dec_pct_z5_c4                                                AS "pct_z5_c4",
        dec_pct_z5_c5                                                AS "pct_z5_c5",
        int_n_aerial_duels                                           AS "n_aerial_duels",
        int_n_aerial_won                                             AS "n_aerial_won",
        dec_aerial_win_rate                                          AS "aerial_win_rate",
        int_n_aerial_offensive                                       AS "n_aerial_offensive",
        int_n_aerial_defensive                                       AS "n_aerial_defensive",
        CAST(int_n_errors_lead_to_goal AS HUGEINT)                   AS "n_errors_lead_to_goal",
        CAST(int_n_errors_lead_to_shot AS HUGEINT)                   AS "n_errors_lead_to_shot",
        int_n_yellow_cards                                           AS "n_yellow_cards",
        int_n_second_yellows                                         AS "n_second_yellows",
        int_n_red_cards                                              AS "n_red_cards",
        int_n_actions_blank                                          AS "n_actions_blank",
        int_n_progressive_passes_blank                               AS "n_progressive_passes_blank",
        int_n_defensive_actions_blank                                AS "n_defensive_actions_blank",
        int_n_shots_blank                                            AS "n_shots_blank",
        dec_blank_pct_z1_c1                                          AS "blank_pct_z1_c1",
        dec_blank_pct_z1_c2                                          AS "blank_pct_z1_c2",
        dec_blank_pct_z1_c3                                          AS "blank_pct_z1_c3",
        dec_blank_pct_z1_c4                                          AS "blank_pct_z1_c4",
        dec_blank_pct_z1_c5                                          AS "blank_pct_z1_c5",
        dec_blank_pct_z2_c1                                          AS "blank_pct_z2_c1",
        dec_blank_pct_z2_c2                                          AS "blank_pct_z2_c2",
        dec_blank_pct_z2_c3                                          AS "blank_pct_z2_c3",
        dec_blank_pct_z2_c4                                          AS "blank_pct_z2_c4",
        dec_blank_pct_z2_c5                                          AS "blank_pct_z2_c5",
        dec_blank_pct_z3_c1                                          AS "blank_pct_z3_c1",
        dec_blank_pct_z3_c2                                          AS "blank_pct_z3_c2",
        dec_blank_pct_z3_c3                                          AS "blank_pct_z3_c3",
        dec_blank_pct_z3_c4                                          AS "blank_pct_z3_c4",
        dec_blank_pct_z3_c5                                          AS "blank_pct_z3_c5",
        dec_blank_pct_z4_c1                                          AS "blank_pct_z4_c1",
        dec_blank_pct_z4_c2                                          AS "blank_pct_z4_c2",
        dec_blank_pct_z4_c3                                          AS "blank_pct_z4_c3",
        dec_blank_pct_z4_c4                                          AS "blank_pct_z4_c4",
        dec_blank_pct_z4_c5                                          AS "blank_pct_z4_c5",
        dec_blank_pct_z5_c1                                          AS "blank_pct_z5_c1",
        dec_blank_pct_z5_c2                                          AS "blank_pct_z5_c2",
        dec_blank_pct_z5_c3                                          AS "blank_pct_z5_c3",
        dec_blank_pct_z5_c4                                          AS "blank_pct_z5_c4",
        dec_blank_pct_z5_c5                                          AS "blank_pct_z5_c5",
        int_n_actions_blank_late                                     AS "n_actions_blank_late",
        int_n_progressive_passes_blank_late                          AS "n_progressive_passes_blank_late",
        int_n_defensive_actions_blank_late                           AS "n_defensive_actions_blank_late",
        int_n_shots_blank_late                                       AS "n_shots_blank_late",
        dec_blank_late_pct_z1_c1                                     AS "blank_late_pct_z1_c1",
        dec_blank_late_pct_z1_c2                                     AS "blank_late_pct_z1_c2",
        dec_blank_late_pct_z1_c3                                     AS "blank_late_pct_z1_c3",
        dec_blank_late_pct_z1_c4                                     AS "blank_late_pct_z1_c4",
        dec_blank_late_pct_z1_c5                                     AS "blank_late_pct_z1_c5",
        dec_blank_late_pct_z2_c1                                     AS "blank_late_pct_z2_c1",
        dec_blank_late_pct_z2_c2                                     AS "blank_late_pct_z2_c2",
        dec_blank_late_pct_z2_c3                                     AS "blank_late_pct_z2_c3",
        dec_blank_late_pct_z2_c4                                     AS "blank_late_pct_z2_c4",
        dec_blank_late_pct_z2_c5                                     AS "blank_late_pct_z2_c5",
        dec_blank_late_pct_z3_c1                                     AS "blank_late_pct_z3_c1",
        dec_blank_late_pct_z3_c2                                     AS "blank_late_pct_z3_c2",
        dec_blank_late_pct_z3_c3                                     AS "blank_late_pct_z3_c3",
        dec_blank_late_pct_z3_c4                                     AS "blank_late_pct_z3_c4",
        dec_blank_late_pct_z3_c5                                     AS "blank_late_pct_z3_c5",
        dec_blank_late_pct_z4_c1                                     AS "blank_late_pct_z4_c1",
        dec_blank_late_pct_z4_c2                                     AS "blank_late_pct_z4_c2",
        dec_blank_late_pct_z4_c3                                     AS "blank_late_pct_z4_c3",
        dec_blank_late_pct_z4_c4                                     AS "blank_late_pct_z4_c4",
        dec_blank_late_pct_z4_c5                                     AS "blank_late_pct_z4_c5",
        dec_blank_late_pct_z5_c1                                     AS "blank_late_pct_z5_c1",
        dec_blank_late_pct_z5_c2                                     AS "blank_late_pct_z5_c2",
        dec_blank_late_pct_z5_c3                                     AS "blank_late_pct_z5_c3",
        dec_blank_late_pct_z5_c4                                     AS "blank_late_pct_z5_c4",
        dec_blank_late_pct_z5_c5                                     AS "blank_late_pct_z5_c5",
        int_n_actions_drawing                                        AS "n_actions_drawing",
        int_n_progressive_passes_drawing                             AS "n_progressive_passes_drawing",
        int_n_defensive_actions_drawing                              AS "n_defensive_actions_drawing",
        int_n_shots_drawing                                          AS "n_shots_drawing",
        dec_drawing_pct_z1_c1                                        AS "drawing_pct_z1_c1",
        dec_drawing_pct_z1_c2                                        AS "drawing_pct_z1_c2",
        dec_drawing_pct_z1_c3                                        AS "drawing_pct_z1_c3",
        dec_drawing_pct_z1_c4                                        AS "drawing_pct_z1_c4",
        dec_drawing_pct_z1_c5                                        AS "drawing_pct_z1_c5",
        dec_drawing_pct_z2_c1                                        AS "drawing_pct_z2_c1",
        dec_drawing_pct_z2_c2                                        AS "drawing_pct_z2_c2",
        dec_drawing_pct_z2_c3                                        AS "drawing_pct_z2_c3",
        dec_drawing_pct_z2_c4                                        AS "drawing_pct_z2_c4",
        dec_drawing_pct_z2_c5                                        AS "drawing_pct_z2_c5",
        dec_drawing_pct_z3_c1                                        AS "drawing_pct_z3_c1",
        dec_drawing_pct_z3_c2                                        AS "drawing_pct_z3_c2",
        dec_drawing_pct_z3_c3                                        AS "drawing_pct_z3_c3",
        dec_drawing_pct_z3_c4                                        AS "drawing_pct_z3_c4",
        dec_drawing_pct_z3_c5                                        AS "drawing_pct_z3_c5",
        dec_drawing_pct_z4_c1                                        AS "drawing_pct_z4_c1",
        dec_drawing_pct_z4_c2                                        AS "drawing_pct_z4_c2",
        dec_drawing_pct_z4_c3                                        AS "drawing_pct_z4_c3",
        dec_drawing_pct_z4_c4                                        AS "drawing_pct_z4_c4",
        dec_drawing_pct_z4_c5                                        AS "drawing_pct_z4_c5",
        dec_drawing_pct_z5_c1                                        AS "drawing_pct_z5_c1",
        dec_drawing_pct_z5_c2                                        AS "drawing_pct_z5_c2",
        dec_drawing_pct_z5_c3                                        AS "drawing_pct_z5_c3",
        dec_drawing_pct_z5_c4                                        AS "drawing_pct_z5_c4",
        dec_drawing_pct_z5_c5                                        AS "drawing_pct_z5_c5",
        int_n_actions_drawing_late                                   AS "n_actions_drawing_late",
        int_n_progressive_passes_drawing_late                        AS "n_progressive_passes_drawing_late",
        int_n_defensive_actions_drawing_late                         AS "n_defensive_actions_drawing_late",
        int_n_shots_drawing_late                                     AS "n_shots_drawing_late",
        dec_drawing_late_pct_z1_c1                                   AS "drawing_late_pct_z1_c1",
        dec_drawing_late_pct_z1_c2                                   AS "drawing_late_pct_z1_c2",
        dec_drawing_late_pct_z1_c3                                   AS "drawing_late_pct_z1_c3",
        dec_drawing_late_pct_z1_c4                                   AS "drawing_late_pct_z1_c4",
        dec_drawing_late_pct_z1_c5                                   AS "drawing_late_pct_z1_c5",
        dec_drawing_late_pct_z2_c1                                   AS "drawing_late_pct_z2_c1",
        dec_drawing_late_pct_z2_c2                                   AS "drawing_late_pct_z2_c2",
        dec_drawing_late_pct_z2_c3                                   AS "drawing_late_pct_z2_c3",
        dec_drawing_late_pct_z2_c4                                   AS "drawing_late_pct_z2_c4",
        dec_drawing_late_pct_z2_c5                                   AS "drawing_late_pct_z2_c5",
        dec_drawing_late_pct_z3_c1                                   AS "drawing_late_pct_z3_c1",
        dec_drawing_late_pct_z3_c2                                   AS "drawing_late_pct_z3_c2",
        dec_drawing_late_pct_z3_c3                                   AS "drawing_late_pct_z3_c3",
        dec_drawing_late_pct_z3_c4                                   AS "drawing_late_pct_z3_c4",
        dec_drawing_late_pct_z3_c5                                   AS "drawing_late_pct_z3_c5",
        dec_drawing_late_pct_z4_c1                                   AS "drawing_late_pct_z4_c1",
        dec_drawing_late_pct_z4_c2                                   AS "drawing_late_pct_z4_c2",
        dec_drawing_late_pct_z4_c3                                   AS "drawing_late_pct_z4_c3",
        dec_drawing_late_pct_z4_c4                                   AS "drawing_late_pct_z4_c4",
        dec_drawing_late_pct_z4_c5                                   AS "drawing_late_pct_z4_c5",
        dec_drawing_late_pct_z5_c1                                   AS "drawing_late_pct_z5_c1",
        dec_drawing_late_pct_z5_c2                                   AS "drawing_late_pct_z5_c2",
        dec_drawing_late_pct_z5_c3                                   AS "drawing_late_pct_z5_c3",
        dec_drawing_late_pct_z5_c4                                   AS "drawing_late_pct_z5_c4",
        dec_drawing_late_pct_z5_c5                                   AS "drawing_late_pct_z5_c5",
        int_n_actions_winning                                        AS "n_actions_winning",
        int_n_progressive_passes_winning                             AS "n_progressive_passes_winning",
        int_n_defensive_actions_winning                              AS "n_defensive_actions_winning",
        int_n_shots_winning                                          AS "n_shots_winning",
        dec_winning_pct_z1_c1                                        AS "winning_pct_z1_c1",
        dec_winning_pct_z1_c2                                        AS "winning_pct_z1_c2",
        dec_winning_pct_z1_c3                                        AS "winning_pct_z1_c3",
        dec_winning_pct_z1_c4                                        AS "winning_pct_z1_c4",
        dec_winning_pct_z1_c5                                        AS "winning_pct_z1_c5",
        dec_winning_pct_z2_c1                                        AS "winning_pct_z2_c1",
        dec_winning_pct_z2_c2                                        AS "winning_pct_z2_c2",
        dec_winning_pct_z2_c3                                        AS "winning_pct_z2_c3",
        dec_winning_pct_z2_c4                                        AS "winning_pct_z2_c4",
        dec_winning_pct_z2_c5                                        AS "winning_pct_z2_c5",
        dec_winning_pct_z3_c1                                        AS "winning_pct_z3_c1",
        dec_winning_pct_z3_c2                                        AS "winning_pct_z3_c2",
        dec_winning_pct_z3_c3                                        AS "winning_pct_z3_c3",
        dec_winning_pct_z3_c4                                        AS "winning_pct_z3_c4",
        dec_winning_pct_z3_c5                                        AS "winning_pct_z3_c5",
        dec_winning_pct_z4_c1                                        AS "winning_pct_z4_c1",
        dec_winning_pct_z4_c2                                        AS "winning_pct_z4_c2",
        dec_winning_pct_z4_c3                                        AS "winning_pct_z4_c3",
        dec_winning_pct_z4_c4                                        AS "winning_pct_z4_c4",
        dec_winning_pct_z4_c5                                        AS "winning_pct_z4_c5",
        dec_winning_pct_z5_c1                                        AS "winning_pct_z5_c1",
        dec_winning_pct_z5_c2                                        AS "winning_pct_z5_c2",
        dec_winning_pct_z5_c3                                        AS "winning_pct_z5_c3",
        dec_winning_pct_z5_c4                                        AS "winning_pct_z5_c4",
        dec_winning_pct_z5_c5                                        AS "winning_pct_z5_c5",
        int_n_actions_winning_late                                   AS "n_actions_winning_late",
        int_n_progressive_passes_winning_late                        AS "n_progressive_passes_winning_late",
        int_n_defensive_actions_winning_late                         AS "n_defensive_actions_winning_late",
        int_n_shots_winning_late                                     AS "n_shots_winning_late",
        dec_winning_late_pct_z1_c1                                   AS "winning_late_pct_z1_c1",
        dec_winning_late_pct_z1_c2                                   AS "winning_late_pct_z1_c2",
        dec_winning_late_pct_z1_c3                                   AS "winning_late_pct_z1_c3",
        dec_winning_late_pct_z1_c4                                   AS "winning_late_pct_z1_c4",
        dec_winning_late_pct_z1_c5                                   AS "winning_late_pct_z1_c5",
        dec_winning_late_pct_z2_c1                                   AS "winning_late_pct_z2_c1",
        dec_winning_late_pct_z2_c2                                   AS "winning_late_pct_z2_c2",
        dec_winning_late_pct_z2_c3                                   AS "winning_late_pct_z2_c3",
        dec_winning_late_pct_z2_c4                                   AS "winning_late_pct_z2_c4",
        dec_winning_late_pct_z2_c5                                   AS "winning_late_pct_z2_c5",
        dec_winning_late_pct_z3_c1                                   AS "winning_late_pct_z3_c1",
        dec_winning_late_pct_z3_c2                                   AS "winning_late_pct_z3_c2",
        dec_winning_late_pct_z3_c3                                   AS "winning_late_pct_z3_c3",
        dec_winning_late_pct_z3_c4                                   AS "winning_late_pct_z3_c4",
        dec_winning_late_pct_z3_c5                                   AS "winning_late_pct_z3_c5",
        dec_winning_late_pct_z4_c1                                   AS "winning_late_pct_z4_c1",
        dec_winning_late_pct_z4_c2                                   AS "winning_late_pct_z4_c2",
        dec_winning_late_pct_z4_c3                                   AS "winning_late_pct_z4_c3",
        dec_winning_late_pct_z4_c4                                   AS "winning_late_pct_z4_c4",
        dec_winning_late_pct_z4_c5                                   AS "winning_late_pct_z4_c5",
        dec_winning_late_pct_z5_c1                                   AS "winning_late_pct_z5_c1",
        dec_winning_late_pct_z5_c2                                   AS "winning_late_pct_z5_c2",
        dec_winning_late_pct_z5_c3                                   AS "winning_late_pct_z5_c3",
        dec_winning_late_pct_z5_c4                                   AS "winning_late_pct_z5_c4",
        dec_winning_late_pct_z5_c5                                   AS "winning_late_pct_z5_c5",
        int_n_actions_losing                                         AS "n_actions_losing",
        int_n_progressive_passes_losing                              AS "n_progressive_passes_losing",
        int_n_defensive_actions_losing                               AS "n_defensive_actions_losing",
        int_n_shots_losing                                           AS "n_shots_losing",
        dec_losing_pct_z1_c1                                         AS "losing_pct_z1_c1",
        dec_losing_pct_z1_c2                                         AS "losing_pct_z1_c2",
        dec_losing_pct_z1_c3                                         AS "losing_pct_z1_c3",
        dec_losing_pct_z1_c4                                         AS "losing_pct_z1_c4",
        dec_losing_pct_z1_c5                                         AS "losing_pct_z1_c5",
        dec_losing_pct_z2_c1                                         AS "losing_pct_z2_c1",
        dec_losing_pct_z2_c2                                         AS "losing_pct_z2_c2",
        dec_losing_pct_z2_c3                                         AS "losing_pct_z2_c3",
        dec_losing_pct_z2_c4                                         AS "losing_pct_z2_c4",
        dec_losing_pct_z2_c5                                         AS "losing_pct_z2_c5",
        dec_losing_pct_z3_c1                                         AS "losing_pct_z3_c1",
        dec_losing_pct_z3_c2                                         AS "losing_pct_z3_c2",
        dec_losing_pct_z3_c3                                         AS "losing_pct_z3_c3",
        dec_losing_pct_z3_c4                                         AS "losing_pct_z3_c4",
        dec_losing_pct_z3_c5                                         AS "losing_pct_z3_c5",
        dec_losing_pct_z4_c1                                         AS "losing_pct_z4_c1",
        dec_losing_pct_z4_c2                                         AS "losing_pct_z4_c2",
        dec_losing_pct_z4_c3                                         AS "losing_pct_z4_c3",
        dec_losing_pct_z4_c4                                         AS "losing_pct_z4_c4",
        dec_losing_pct_z4_c5                                         AS "losing_pct_z4_c5",
        dec_losing_pct_z5_c1                                         AS "losing_pct_z5_c1",
        dec_losing_pct_z5_c2                                         AS "losing_pct_z5_c2",
        dec_losing_pct_z5_c3                                         AS "losing_pct_z5_c3",
        dec_losing_pct_z5_c4                                         AS "losing_pct_z5_c4",
        dec_losing_pct_z5_c5                                         AS "losing_pct_z5_c5",
        int_n_actions_losing_late                                    AS "n_actions_losing_late",
        int_n_progressive_passes_losing_late                         AS "n_progressive_passes_losing_late",
        int_n_defensive_actions_losing_late                          AS "n_defensive_actions_losing_late",
        int_n_shots_losing_late                                      AS "n_shots_losing_late",
        dec_losing_late_pct_z1_c1                                    AS "losing_late_pct_z1_c1",
        dec_losing_late_pct_z1_c2                                    AS "losing_late_pct_z1_c2",
        dec_losing_late_pct_z1_c3                                    AS "losing_late_pct_z1_c3",
        dec_losing_late_pct_z1_c4                                    AS "losing_late_pct_z1_c4",
        dec_losing_late_pct_z1_c5                                    AS "losing_late_pct_z1_c5",
        dec_losing_late_pct_z2_c1                                    AS "losing_late_pct_z2_c1",
        dec_losing_late_pct_z2_c2                                    AS "losing_late_pct_z2_c2",
        dec_losing_late_pct_z2_c3                                    AS "losing_late_pct_z2_c3",
        dec_losing_late_pct_z2_c4                                    AS "losing_late_pct_z2_c4",
        dec_losing_late_pct_z2_c5                                    AS "losing_late_pct_z2_c5",
        dec_losing_late_pct_z3_c1                                    AS "losing_late_pct_z3_c1",
        dec_losing_late_pct_z3_c2                                    AS "losing_late_pct_z3_c2",
        dec_losing_late_pct_z3_c3                                    AS "losing_late_pct_z3_c3",
        dec_losing_late_pct_z3_c4                                    AS "losing_late_pct_z3_c4",
        dec_losing_late_pct_z3_c5                                    AS "losing_late_pct_z3_c5",
        dec_losing_late_pct_z4_c1                                    AS "losing_late_pct_z4_c1",
        dec_losing_late_pct_z4_c2                                    AS "losing_late_pct_z4_c2",
        dec_losing_late_pct_z4_c3                                    AS "losing_late_pct_z4_c3",
        dec_losing_late_pct_z4_c4                                    AS "losing_late_pct_z4_c4",
        dec_losing_late_pct_z4_c5                                    AS "losing_late_pct_z4_c5",
        dec_losing_late_pct_z5_c1                                    AS "losing_late_pct_z5_c1",
        dec_losing_late_pct_z5_c2                                    AS "losing_late_pct_z5_c2",
        dec_losing_late_pct_z5_c3                                    AS "losing_late_pct_z5_c3",
        dec_losing_late_pct_z5_c4                                    AS "losing_late_pct_z5_c4",
        dec_losing_late_pct_z5_c5                                    AS "losing_late_pct_z5_c5",
        int_n_assists                                                AS "n_assists",
        int_n_chances_created                                        AS "n_chances_created",
        CAST(int_minutes_played AS HUGEINT)                          AS "minutes_played",
        dt_date                                                      AS "date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at"
    FROM {{ ref('player_match_stats') }}
),

-- player_xg_chain lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_xg_chain AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(str_shot_event_id AS INTEGER)                           AS "shot_event_id",
        CAST(str_shot_type_id AS INTEGER)                            AS "shot_type_id",
        int_shot_minute                                              AS "shot_minute",
        dec_xg_proxy                                                 AS "xg_proxy",
        bool_is_counter_attack                                       AS "is_counter_attack",
        int_position_in_chain                                        AS "position_in_chain",
        int_chain_length                                             AS "chain_length",
        dec_position_weight                                          AS "position_weight",
        bool_is_shooter                                              AS "is_shooter",
        bool_is_assister                                             AS "is_assister",
        dec_xgchain                                                  AS "xgchain",
        dec_xgchain_weighted                                         AS "xgchain_weighted",
        dec_xgbuildup                                                AS "xgbuildup",
        dec_xgbuildup_weighted                                       AS "xgbuildup_weighted"
    FROM {{ ref('player_xg_chain') }}
),

-- threat_conceded lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_threat_conceded AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        dec_xg_proxy                                                 AS "xg_proxy",
        bool_is_penalty                                              AS "is_penalty",
        int_position_in_chain                                        AS "position_in_chain",
        int_chain_length                                             AS "chain_length",
        dec_position_weight                                          AS "position_weight",
        bool_is_keeper                                               AS "is_keeper",
        bool_has_error_leading_to_goal                               AS "has_error_leading_to_goal",
        dec_threat_conceded                                          AS "threat_conceded",
        dec_threat_conceded_weighted                                 AS "threat_conceded_weighted"
    FROM {{ ref('threat_conceded') }}
),

mdl_body AS (
WITH

-- 1) Base joueur-match (dédoublonnée sur le scrape le plus récent).
base AS (
    SELECT
        match_id, team_id, player_id, date, season, league_source,
        minutes_played,
        xg_contribution, n_shots, n_chances_created, n_key_passes,
        n_aerial_won, n_aerial_duels, n_tackles, n_interceptions,
        n_errors_lead_to_shot, n_errors_lead_to_goal,
        n_yellow_cards, n_second_yellows
    FROM in_player_match_stats
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id, team_id, player_id ORDER BY scraped_at DESC
    ) = 1
),

-- 2) Tirs de l'équipe par match (dénominateur de la part de tirs).
team_shots AS (
    SELECT match_id, team_id, SUM(n_shots) AS team_shots_match
    FROM base GROUP BY match_id, team_id
),

-- 3) xgChain / xgBuildup du joueur par match (Σ sur ses chaînes distinctes).
xgc AS (
    SELECT match_id, chain_team_id AS team_id, player_id,
        SUM(xgchain)   AS xgchain_match,
        SUM(xgbuildup) AS xgbuildup_match
    FROM (
        SELECT DISTINCT match_id, chain_id, chain_team_id, player_id, xgchain, xgbuildup
        FROM in_player_xg_chain
    )
    GROUP BY match_id, chain_team_id, player_id
),

-- 4) Penaltys tirés par le joueur dans le match (rôle de tireur).
pen AS (
    SELECT match_id, attacking_team_id AS team_id, taker_player_id AS player_id,
        COUNT(*) AS n_pens_taken
    FROM in_int_penalties
    WHERE taker_player_id IS NOT NULL
    GROUP BY match_id, attacking_team_id, taker_player_id
),

-- 5) Coups francs tirés par le joueur dans le match (rôle de tireur).
fk AS (
    SELECT match_id, chain_team_id AS team_id, freekick_taker_id AS player_id,
        COUNT(*) AS n_fk_taken
    FROM in_freekick_profiles
    WHERE freekick_taker_id IS NOT NULL
    GROUP BY match_id, chain_team_id, freekick_taker_id
),

-- 6a) Menace concédée par le joueur dans le match (feature 48, non-zonale).
threat_conceded_match AS (
    SELECT match_id, team_id, player_id, SUM(threat_conceded) AS threat_conceded_match
    FROM in_threat_conceded
    WHERE match_id IS NOT NULL
    GROUP BY match_id, team_id, player_id
),

-- 6b) xGOT : buts vs xGOT du joueur dans le match (feature 70). xgot_predictions
--     est produit par le pipeline Python (table externe, PAS un modèle dbt) → on
--     la référence en direct ; elle doit exister au moment du run dbt. On récupère
--     le tireur via int_shot_placement (jointure sur match_id + row_num, 100%).
xgot_match AS (
    SELECT sp.match_id, sp.team_id, sp.player_id,
        SUM(CASE WHEN xg.bool_is_goal THEN 1 ELSE 0 END) AS n_goals_ot,
        SUM(xg.dec_xgot)                                 AS sum_xgot
    FROM machine_learning.xgot_predictions xg
    JOIN in_int_shot_placement sp
        ON sp.match_id = xg.str_match_id AND sp.row_num = xg.int_row_num
    WHERE sp.match_id IS NOT NULL
    GROUP BY sp.match_id, sp.team_id, sp.player_id
),

-- 7) Enrichissement au grain joueur-match (LEFT JOIN → 0 si absent).
enriched AS (
    SELECT
        b.*,
        ts.team_shots_match,
        COALESCE(x.xgchain_match, 0)   AS xgchain_match,
        COALESCE(x.xgbuildup_match, 0) AS xgbuildup_match,
        COALESCE(p.n_pens_taken, 0)    AS n_pens_taken,
        COALESCE(f.n_fk_taken, 0)      AS n_fk_taken,
        COALESCE(tc.threat_conceded_match, 0) AS threat_conceded_match,
        COALESCE(xo.n_goals_ot, 0)     AS n_goals_ot,
        COALESCE(xo.sum_xgot, 0)       AS sum_xgot
    FROM base b
    LEFT JOIN team_shots ts USING (match_id, team_id)
    LEFT JOIN xgc x         USING (match_id, team_id, player_id)
    LEFT JOIN pen p         USING (match_id, team_id, player_id)
    LEFT JOIN fk  f         USING (match_id, team_id, player_id)
    LEFT JOIN threat_conceded_match tc USING (match_id, team_id, player_id)
    LEFT JOIN xgot_match xo USING (match_id, team_id, player_id)
),

-- 7) Profil as-of-date sur les 38 dernières apparitions antérieures.
profil AS (
    SELECT
        match_id, team_id, player_id, date, season, league_source,

        {% set w %}PARTITION BY player_id ORDER BY date, match_id ROWS BETWEEN 38 PRECEDING AND 1 PRECEDING{% endset %}
        {% set mins %}NULLIF(SUM(minutes_played) OVER ({{ w }}), 0){% endset %}

        -- Confiance / volume
        COUNT(*)             OVER ({{ w }}) AS n_apps_lag,
        SUM(minutes_played)  OVER ({{ w }}) AS minutes_lag,

        -- ══ PASSE 1 ══
        -- Famille 10 / 5 (offensif) — per-90
        SUM(xg_contribution) OVER ({{ w }}) / {{ mins }} * 90 AS scorer_xg_per90_lag,
        SUM(n_shots)         OVER ({{ w }}) / {{ mins }} * 90 AS scorer_shots_per90_lag,
        SUM(n_chances_created) OVER ({{ w }}) / {{ mins }} * 90 AS off_chances_created_per90_lag,
        SUM(n_key_passes)    OVER ({{ w }}) / {{ mins }} * 90 AS off_key_passes_per90_lag,
        SUM(xg_contribution) OVER ({{ w }}) / NULLIF(SUM(n_shots) OVER ({{ w }}), 0) AS off_xg_per_shot_lag,
        -- Famille 6 (défensif non-zonal)
        SUM(n_aerial_won)    OVER ({{ w }}) / NULLIF(SUM(n_aerial_duels) OVER ({{ w }}), 0) AS def_aerial_win_rate_lag,
        SUM(n_tackles + n_interceptions)            OVER ({{ w }}) / {{ mins }} * 90 AS def_actions_per90_lag,
        SUM(n_errors_lead_to_shot + n_errors_lead_to_goal) OVER ({{ w }}) / {{ mins }} * 90 AS def_errors_per90_lag,
        -- Famille 8 (discipline)
        SUM((n_yellow_cards + n_second_yellows)::DOUBLE) OVER ({{ w }}) / {{ mins }} * 90 AS player_card_propensity_lag,

        -- ══ PASSE 2 ══
        -- Famille 5 — implication dans la construction (per-90)
        SUM(xgchain_match)   OVER ({{ w }}) / {{ mins }} * 90 AS off_xgchain_per90_lag,
        SUM(xgbuildup_match) OVER ({{ w }}) / {{ mins }} * 90 AS off_xgbuildup_per90_lag,
        -- Famille 10 — part des tirs de l'équipe (ratio), rôles CPA (compteurs)
        SUM(n_shots)     OVER ({{ w }}) / NULLIF(SUM(team_shots_match) OVER ({{ w }}), 0) AS scorer_team_shot_share_lag,
        SUM(n_pens_taken) OVER ({{ w }}) AS scorer_penalty_taker_lag,
        SUM(n_fk_taken)   OVER ({{ w }}) AS scorer_freekick_taker_lag,
        -- Famille 6 — menace concédée par 90 (feature 48, non-zonale)
        SUM(threat_conceded_match) OVER ({{ w }}) / {{ mins }} * 90 AS def_threat_conceded_per90_lag,
        -- Famille 10 — surperformance à la finition = Σ buts − Σ xGOT sur la fenêtre
        -- (positif = finisseur clinique). NULL si aucun tir cadré (division protégée).
        (SUM(n_goals_ot) OVER ({{ w }}) - SUM(sum_xgot) OVER ({{ w }}))
            / NULLIF(SUM(sum_xgot) OVER ({{ w }}), 0) AS scorer_xgot_overperformance_lag

    FROM enriched
)

SELECT *,
    -- [Famille 11, feature 79] Fiabilité du profil selon le nb d'apparitions dans
    -- la fenêtre (seuils par défaut, à calibrer). 'none' → cible d'imputation.
    CASE WHEN n_apps_lag >= 20 THEN 'high'
         WHEN n_apps_lag >= 5  THEN 'medium'
         WHEN n_apps_lag >= 1  THEN 'low'
         ELSE 'none' END AS profile_confidence_flag
FROM profil

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            dt_date                                                      AS "date",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            int_n_apps_lag                                               AS "n_apps_lag",
            CAST(int_minutes_lag AS HUGEINT)                             AS "minutes_lag",
            dec_scorer_xg_per90_lag                                      AS "scorer_xg_per90_lag",
            dec_scorer_shots_per90_lag                                   AS "scorer_shots_per90_lag",
            dec_off_chances_created_per90_lag                            AS "off_chances_created_per90_lag",
            dec_off_key_passes_per90_lag                                 AS "off_key_passes_per90_lag",
            dec_off_xg_per_shot_lag                                      AS "off_xg_per_shot_lag",
            dec_def_aerial_win_rate_lag                                  AS "def_aerial_win_rate_lag",
            dec_def_actions_per90_lag                                    AS "def_actions_per90_lag",
            dec_def_errors_per90_lag                                     AS "def_errors_per90_lag",
            dec_player_card_propensity_lag                               AS "player_card_propensity_lag",
            dec_off_xgchain_per90_lag                                    AS "off_xgchain_per90_lag",
            dec_off_xgbuildup_per90_lag                                  AS "off_xgbuildup_per90_lag",
            dec_scorer_team_shot_share_lag                               AS "scorer_team_shot_share_lag",
            CAST(int_scorer_penalty_taker_lag AS HUGEINT)                AS "scorer_penalty_taker_lag",
            CAST(int_scorer_freekick_taker_lag AS HUGEINT)               AS "scorer_freekick_taker_lag",
            dec_def_threat_conceded_per90_lag                            AS "def_threat_conceded_per90_lag",
            dec_scorer_xgot_overperformance_lag                          AS "scorer_xgot_overperformance_lag",
            str_profile_confidence_flag                                  AS "profile_confidence_flag"
        FROM {{ this }}
    ))
{% endif %}
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "date"                                                       AS dt_date,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "n_apps_lag"                                                 AS int_n_apps_lag,
        CAST(minutes_lag AS BIGINT)                                  AS int_minutes_lag,
        "scorer_xg_per90_lag"                                        AS dec_scorer_xg_per90_lag,
        "scorer_shots_per90_lag"                                     AS dec_scorer_shots_per90_lag,
        "off_chances_created_per90_lag"                              AS dec_off_chances_created_per90_lag,
        "off_key_passes_per90_lag"                                   AS dec_off_key_passes_per90_lag,
        "off_xg_per_shot_lag"                                        AS dec_off_xg_per_shot_lag,
        "def_aerial_win_rate_lag"                                    AS dec_def_aerial_win_rate_lag,
        "def_actions_per90_lag"                                      AS dec_def_actions_per90_lag,
        "def_errors_per90_lag"                                       AS dec_def_errors_per90_lag,
        "player_card_propensity_lag"                                 AS dec_player_card_propensity_lag,
        "off_xgchain_per90_lag"                                      AS dec_off_xgchain_per90_lag,
        "off_xgbuildup_per90_lag"                                    AS dec_off_xgbuildup_per90_lag,
        "scorer_team_shot_share_lag"                                 AS dec_scorer_team_shot_share_lag,
        CAST(scorer_penalty_taker_lag AS BIGINT)                     AS int_scorer_penalty_taker_lag,
        CAST(scorer_freekick_taker_lag AS BIGINT)                    AS int_scorer_freekick_taker_lag,
        "def_threat_conceded_per90_lag"                              AS dec_def_threat_conceded_per90_lag,
        "scorer_xgot_overperformance_lag"                            AS dec_scorer_xgot_overperformance_lag,
        "profile_confidence_flag"                                    AS str_profile_confidence_flag
    FROM mdl_body
)

SELECT * FROM mdl_out
