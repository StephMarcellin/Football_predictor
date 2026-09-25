{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_player_zone_season'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- int_player_zone_season — grain (player_id, zone_5x5, season)
-- Agrégat zonal (5×5) OFFENSIF par joueur et par saison JOUÉE. Base de
-- gold.joueur_zone_saison (qui n'en fait que le décalage saison N-1).
--
-- Deux métriques :
--   • avg_touch_share : part moyenne des touches dans la cellule (grille
--     pct_zX_cY de player_match_stats, remise en format tall).
--   • total_shots : tirs pris dans la cellule (int_shot_placement binné en 5×5,
--     repère de l'attaquant, CSC exclus).
--
-- Note d'implémentation : le modèle est SCINDÉ de joueur_zone_saison et
-- matérialisé en table à cause d'un bug du planner DuckDB 1.5.1 (jointure de CTE
-- agrégés + QUALIFY). Le dédoublonnage de player_match_stats se fait ici par
-- moyenne au grain match (touch_match), sans QUALIFY.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- event_values lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_event_values AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        dec_danger_position                                          AS "danger_position",
        dec_chance_creation                                          AS "chance_creation",
        dec_def_execution_quality                                    AS "def_execution_quality",
        dec_pressure_context                                         AS "pressure_context",
        dec_context_weight                                           AS "context_weight",
        dec_action_value                                             AS "action_value"
    FROM {{ ref('event_values') }}
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

-- player_network_duels lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_network_duels AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        CAST(str_duel_type_id AS INTEGER)                            AS "duel_type_id",
        str_duel_type                                                AS "duel_type",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        int_event_id_a                                               AS "event_id_a",
        int_team_id_a                                                AS "team_id_a",
        int_player_id_a                                              AS "player_id_a",
        int_outcome_a                                                AS "outcome_a",
        int_event_id_b                                               AS "event_id_b",
        int_team_id_b                                                AS "team_id_b",
        int_player_id_b                                              AS "player_id_b",
        int_outcome_b                                                AS "outcome_b"
    FROM {{ ref('player_network_duels') }}
),

-- player_passes_raw lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_passes_raw AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        str_chain_trigger                                            AS "chain_trigger",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_passer_id AS INTEGER)                               AS "passer_id",
        CAST(str_receiver_id AS INTEGER)                             AS "receiver_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        bool_is_progressive                                          AS "is_progressive",
        bool_is_creative                                             AS "is_creative",
        bool_is_buildup                                              AS "is_buildup"
    FROM {{ ref('player_passes_raw') }}
),

mdl_body AS (
WITH

-- 1) Grille de touches remise en tall : une ligne par (match, joueur, cellule).
touch_tall AS (
    {% for z in range(1, 6) %}{% for c in range(1, 6) %}
    SELECT match_id, player_id, season, 'z{{ z }}_c{{ c }}' AS zone_5x5, pct_z{{ z }}_c{{ c }} AS touch_share
    FROM in_player_match_stats
    {% if not (z == 5 and c == 5) %}UNION ALL{% endif %}
    {% endfor %}{% endfor %}
),

-- 2) Dédoublonnage : moyenne au grain (match, joueur, cellule) — écrase les
--    28 clés en double de la source sans recourir à QUALIFY.
touch_match AS (
    SELECT match_id, player_id, season, zone_5x5, AVG(touch_share) AS touch_share
    FROM touch_tall
    GROUP BY match_id, player_id, season, zone_5x5
),

-- 3) Profil de touches par (joueur, zone, saison).
touch_season AS (
    SELECT player_id, zone_5x5, season,
        AVG(touch_share)         AS avg_touch_share,
        COUNT(DISTINCT match_id) AS n_matches
    FROM touch_match
    GROUP BY player_id, zone_5x5, season
),

-- 4) Tirs par (joueur, zone, saison) — binning 5×5, repère attaquant, CSC exclus.
shots_season AS (
    SELECT player_id, zone_5x5, season, SUM(shots_in_cell) AS total_shots
    FROM (
        SELECT player_id, season,
            'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
            COUNT(*) AS shots_in_cell
        FROM in_int_shot_placement
        WHERE is_own_goal = FALSE
        GROUP BY player_id, season,
            'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
    )
    GROUP BY player_id, zone_5x5, season
),

-- 5) Duels défensifs par (joueur, zone, saison) — côté défenseur (B).
--    RETOURNEMENT vers la cage du défenseur : les coords sont dans le repère de
--    l'attaquant (A), on les inverse en (100−x, 100−y) pour que la cellule soit
--    définie par rapport au but que le défenseur protège. Validé : un duel à
--    x=90 (près du but de A) tombe en z1 (tiers défensif de B).
duels_season AS (
    SELECT player_id_b AS player_id, season,
        'z' || LEAST(FLOOR((100 - x) / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR((100 - y) / 20) + 1, 5)::INT AS zone_5x5,
        COUNT(*)        AS n_duels,
        SUM(outcome_b)  AS duels_won
    FROM in_player_network_duels
    GROUP BY player_id_b, season,
        'z' || LEAST(FLOOR((100 - x) / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR((100 - y) / 20) + 1, 5)::INT
),

-- 6) Danger offensif par (joueur, zone, saison) — feature 35, depuis event_values.
--    Actions OFFENSIVES uniquement (def_execution_quality IS NULL) → repère de
--    l'attaquant, pas de retournement. On somme danger_position par cellule.
danger_season AS (
    SELECT player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        SUM(danger_position) AS total_danger
    FROM in_event_values
    WHERE def_execution_quality IS NULL
      AND danger_position IS NOT NULL AND match_id IS NOT NULL
      AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
),

-- 7) Passes progressives (feature 41) + centres (feature 37), par
--    (joueur, cellule, saison) depuis player_passes_raw. Repère de l'attaquant.
--    • total_progressive : is_progressive (flag amont).
--    • total_crosses : DÉFINITION GÉOMÉTRIQUE d'un centre — pas de flag amont
--      fiable (is_creative pique au centre, pas sur les flancs). Un centre =
--      passe partant du dernier tiers sur un flanc (x≥66 ET (y<20 OU y>80)) vers
--      la surface centrale (end_x≥83 ET end_y∈[21,79]). Comptabilisé dans la
--      cellule d'ORIGINE (là où le centre est délivré → flancs z4/z5).
passes_season AS (
    SELECT passer_id AS player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        SUM(CASE WHEN is_progressive THEN 1 ELSE 0 END) AS total_progressive,
        SUM(CASE WHEN x >= 66 AND (y < 20 OR y > 80)
                  AND end_x >= 83 AND end_y BETWEEN 21 AND 79
                 THEN 1 ELSE 0 END) AS total_crosses
    FROM in_player_passes_raw
    WHERE match_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY passer_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
),

-- 8) Actions défensives par (joueur, cellule, saison) — feature 45/densité famille 6.
--    event_values, actions DÉFENSIVES (def_execution_quality NON NULL). Coordonnées
--    déjà dans le repère du camp du défenseur (validé : concentrées z1-z3) → PAS de
--    retournement. On dénombre les actions par cellule.
def_actions_season AS (
    SELECT player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        COUNT(*) AS total_def_actions
    FROM in_event_values
    WHERE def_execution_quality IS NOT NULL
      AND match_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
)

SELECT
    t.player_id,
    t.zone_5x5,
    t.season,
    t.avg_touch_share,
    t.n_matches,
    COALESCE(s.total_shots, 0) AS total_shots,
    COALESCE(d.n_duels, 0)     AS n_duels,
    COALESCE(d.duels_won, 0)   AS duels_won,
    COALESCE(dg.total_danger, 0) AS total_danger,
    COALESCE(ps.total_progressive, 0) AS total_progressive,
    COALESCE(ps.total_crosses, 0)     AS total_crosses,
    COALESCE(da.total_def_actions, 0) AS total_def_actions
FROM touch_season t
LEFT JOIN shots_season s
    ON  s.player_id = t.player_id
    AND s.zone_5x5  = t.zone_5x5
    AND s.season    = t.season
LEFT JOIN danger_season dg
    ON  dg.player_id = t.player_id
    AND dg.zone_5x5  = t.zone_5x5
    AND dg.season    = t.season
LEFT JOIN duels_season d
    ON  d.player_id = t.player_id
    AND d.zone_5x5  = t.zone_5x5
    AND d.season    = t.season
LEFT JOIN passes_season ps
    ON  ps.player_id = t.player_id
    AND ps.zone_5x5  = t.zone_5x5
    AND ps.season    = t.season
LEFT JOIN def_actions_season da
    ON  da.player_id = t.player_id
    AND da.zone_5x5  = t.zone_5x5
    AND da.season    = t.season
),

mdl_out AS (
    SELECT
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "zone_5x5"                                                   AS str_zone_5x5,
        "season"                                                     AS str_season,
        "avg_touch_share"                                            AS dec_avg_touch_share,
        "n_matches"                                                  AS int_n_matches,
        CAST(total_shots AS BIGINT)                                  AS int_total_shots,
        "n_duels"                                                    AS int_n_duels,
        CAST(duels_won AS BIGINT)                                    AS int_duels_won,
        "total_danger"                                               AS dec_total_danger,
        CAST(total_progressive AS BIGINT)                            AS int_total_progressive,
        CAST(total_crosses AS BIGINT)                                AS int_total_crosses,
        "total_def_actions"                                          AS int_total_def_actions
    FROM mdl_body
)

SELECT * FROM mdl_out
