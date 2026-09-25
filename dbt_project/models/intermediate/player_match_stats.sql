{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_match_stats'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- events_qual lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        int_row_num                                                  AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
        str_qual_type_name                                           AS "qual_type_name",
        str_qual_value                                               AS "qual_value"
    FROM {{ ref('events_qual') }}
),

-- int_player_minutes lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_player_minutes AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        CAST(int_minutes_played AS HUGEINT)                          AS "minutes_played"
    FROM {{ ref('int_player_minutes') }}
),

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

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
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. FILTRE INCRÉMENTAL STRICT (Sans Cross Join)
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT e.match_id
    FROM in_int_whoscored_events e
    LEFT JOIN (
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
        FROM {{ this }}
    ) t ON e.match_id = t.match_id
    WHERE t.match_id IS NULL
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_whoscored_events
),
{% endif %}

match_dates AS (
    SELECT match_id, match_date, league_source, season, scraped_at
    FROM in_int_whoscored_match_index
    WHERE match_id IN (SELECT match_id FROM new_matches)
),

match_teams AS (
    SELECT match_id, MIN(team_id) AS team_1, MAX(team_id) AS team_2
    FROM in_int_whoscored_events
    WHERE match_id IN (SELECT match_id FROM match_dates)
    GROUP BY match_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 2. PIVOT DES QUALIFICATEURS
-- Ajout de 178/179 pour éviter la sous-requête corrélée dans n_touches
-- ══════════════════════════════════════════════════════════════════════════════
qual_pivot AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 210   THEN 1 ELSE 0 END) AS is_shot_assist,
        MAX(CASE WHEN qual_type_id = 11113 THEN 1 ELSE 0 END) AS is_key_pass,
        MAX(CASE WHEN qual_type_id = 1     THEN 1 ELSE 0 END) AS is_longball,
        MAX(CASE WHEN qual_type_id = 2     THEN 1 ELSE 0 END) AS is_cross,
        MAX(CASE WHEN qual_type_id = 4     THEN 1 ELSE 0 END) AS is_throughball,
        MAX(CASE WHEN qual_type_id = 22    THEN 1 ELSE 0 END) AS is_regular_play,
        MAX(CASE WHEN qual_type_id = 215   THEN 1 ELSE 0 END) AS is_individual_play,
        MAX(CASE WHEN qual_type_id = 170   THEN 1 ELSE 0 END) AS is_leading_to_goal,
        MAX(CASE WHEN qual_type_id = 169   THEN 1 ELSE 0 END) AS is_leading_to_attempt,
        MAX(CASE WHEN qual_type_id = 286   THEN 1 ELSE 0 END) AS is_offensive_aerial,
        MAX(CASE WHEN qual_type_id = 285   THEN 1 ELSE 0 END) AS is_defensive_aerial,
        MAX(CASE WHEN qual_type_id IN (178, 179) THEN 1 ELSE 0 END) AS is_gk_touch
    FROM in_events_qual
    WHERE qual_type_id IN (210, 11113, 1, 2, 4, 22, 215, 170, 169, 285, 286, 178, 179)
      AND match_id IN (SELECT match_id FROM match_dates)
    GROUP BY match_id, row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 3. ENRICHISSEMENT DES ÉVÉNEMENTS & CALCUL VECTORISÉ DU SCORE
-- ══════════════════════════════════════════════════════════════════════════════
events_enriched AS (
    SELECT
        e.*,
        mt.team_1,
        
        -- Score cumulé via fonction de fenêtrage (0 jointure)
        SUM(CASE WHEN e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE AND e.team_id = mt.team_1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.match_id ORDER BY e.row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t1_score,
        
        SUM(CASE WHEN e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE AND e.team_id = mt.team_2 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.match_id ORDER BY e.row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t2_score,
            
        qp.is_shot_assist, qp.is_key_pass, qp.is_longball, qp.is_cross, qp.is_throughball,
        qp.is_regular_play, qp.is_individual_play, qp.is_leading_to_goal, qp.is_leading_to_attempt,
        qp.is_offensive_aerial, qp.is_defensive_aerial, qp.is_gk_touch

    FROM in_int_whoscored_events e
    INNER JOIN match_dates d ON d.match_id = e.match_id
    INNER JOIN match_teams mt ON mt.match_id = e.match_id
    LEFT JOIN qual_pivot qp ON qp.match_id = e.match_id AND qp.row_num = e.row_num
    WHERE e.player_id IS NOT NULL
),

events_with_state AS (
    SELECT *,
        CASE WHEN team_id = team_1 THEN t1_score ELSE t2_score END AS team_score,
        CASE WHEN team_id = team_1 THEN t2_score ELSE t1_score END AS opp_score
    FROM events_enriched
),

events_final AS (
    SELECT e.*,
        CASE
            WHEN e.expanded_minute >= 75 AND e.team_score = 0 AND e.opp_score = 0 THEN 'blank_late'
            WHEN e.expanded_minute >= 75 AND e.team_score = e.opp_score           THEN 'drawing_late'
            WHEN e.expanded_minute >= 75 AND e.team_score > e.opp_score           THEN 'winning_late'
            WHEN e.expanded_minute >= 75 AND e.team_score < e.opp_score           THEN 'losing_late'
            WHEN e.team_score = 0 AND e.opp_score = 0                             THEN 'blank'
            WHEN e.team_score = e.opp_score                                       THEN 'drawing'
            WHEN e.team_score > e.opp_score                                       THEN 'winning'
            ELSE 'losing'
        END AS score_state
    FROM events_with_state e
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 4. L'AGRÉGATION MAÎTRESSE (Single-Pass)
-- Regroupe base, offensive, defensive, spatial, discipline, aerial, error, score.
-- ══════════════════════════════════════════════════════════════════════════════
master_agg AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        -- ---------------- BASE ----------------
        COUNT(*) AS n_actions,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.is_shot = TRUE) > 0
            THEN COUNT(*) FILTER (WHERE e.is_shot = TRUE)
                 * (1.0 / (1.0 + SQRT(POW(100.0 - AVG(e.x) FILTER (WHERE e.is_shot = TRUE), 2) + POW( 50.0 - AVG(e.y) FILTER (WHERE e.is_shot = TRUE), 2))))
            ELSE 0.0
        END AS xg_contribution,

        -- ---------------- OFFENSIF ----------------
        COUNT(*) FILTER (WHERE e.is_shot = TRUE)                                      AS n_shots,
        SUM(CASE WHEN e.is_shot = TRUE AND COALESCE(e.is_regular_play, 0) = 1 THEN 1 ELSE 0 END)    AS n_shots_regular_play,
        SUM(CASE WHEN e.is_shot = TRUE AND COALESCE(e.is_individual_play, 0) = 1 THEN 1 ELSE 0 END) AS n_shots_individual_play,
        SUM(COALESCE(e.is_shot_assist, 0))                                            AS n_shot_assists,
        SUM(COALESCE(e.is_key_pass, 0))                                               AS n_key_passes,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_longball, 0) = 1 THEN 1 ELSE 0 END)           AS n_longballs,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_cross, 0) = 1 THEN 1 ELSE 0 END)              AS n_crosses,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_throughball, 0) = 1 THEN 1 ELSE 0 END)        AS n_throughballs,
        COUNT(*) FILTER (WHERE e.type_id = 1 AND e.outcome_id = 1 AND e.end_x IS NOT NULL AND e.x IS NOT NULL AND e.end_x > e.x + 10) AS n_progressive_passes,

        -- ---------------- DÉFENSIF ----------------
        COUNT(*) FILTER (WHERE e.type_id = 7)                    AS n_tackles,
        COUNT(*) FILTER (WHERE e.type_id = 7 AND e.outcome_id = 1) AS n_tackles_won,
        COUNT(*) FILTER (WHERE e.type_id = 8)                    AS n_interceptions,
        COUNT(*) FILTER (WHERE e.type_id = 49)                   AS n_ball_recoveries,
        COUNT(*) FILTER (WHERE e.type_id = 45)                   AS n_challenges,
        COUNT(*) FILTER (WHERE e.type_id = 12)                   AS n_clearances,
        AVG(e.x) FILTER (WHERE e.type_id IN (7, 8, 49, 45, 12))  AS defensive_zone_x,

        -- ---------------- SPATIAL ----------------
        -- La sous-requête corrélée est remplacée par la vérification de notre nouveau flag is_gk_touch
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND COALESCE(e.is_gk_touch, 0) = 0) AS n_touches,
        {{ spatial_zones() }},

        -- ---------------- AÉRIEN ----------------
        COUNT(*) FILTER (WHERE e.type_id = 44)                   AS n_aerial_duels,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND e.outcome_id = 1) AS n_aerial_won,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.type_id = 44) > 0
            THEN CAST(COUNT(*) FILTER (WHERE e.type_id = 44 AND e.outcome_id = 1) AS DOUBLE) / COUNT(*) FILTER (WHERE e.type_id = 44)
            ELSE NULL
        END                                                      AS aerial_win_rate,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND COALESCE(e.is_offensive_aerial, 0) = 1) AS n_aerial_offensive,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND COALESCE(e.is_defensive_aerial, 0) = 1) AS n_aerial_defensive,

        -- ---------------- ERREURS ----------------
        SUM(CASE WHEN e.type_id = 51 AND COALESCE(e.is_leading_to_goal, 0) = 1 THEN 1 ELSE 0 END)    AS n_errors_lead_to_goal,
        SUM(CASE WHEN e.type_id = 51 AND COALESCE(e.is_leading_to_attempt, 0) = 1 THEN 1 ELSE 0 END) AS n_errors_lead_to_shot,

        -- ---------------- DISCIPLINE ----------------
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'Yellow')       AS n_yellow_cards,
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'SecondYellow') AS n_second_yellows,
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'Red')          AS n_red_cards,

        -- ---------------- ÉTATS DE SCORE (BOUCLE) ----------------
        {% for state in ['blank', 'blank_late', 'drawing', 'drawing_late', 'winning', 'winning_late', 'losing', 'losing_late'] %}
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}') AS n_actions_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.type_id = 1 AND e.outcome_id = 1 AND e.x IS NOT NULL AND e.end_x > (e.x + 10)) AS n_progressive_passes_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.type_id IN (7, 8, 49, 45, 12)) AS n_defensive_actions_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.is_shot = TRUE) AS n_shots_{{ state }},
        {{ spatial_zones(filter_condition="e.score_state = '" + state + "' AND e.is_touch = TRUE", prefix=state + '_') }}
        {{ "," if not loop.last }}
        {% endfor %}

    FROM events_final e
    GROUP BY e.match_id, e.team_id, e.player_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 5. CRÉATION (Agrégation séparée car la clé de regroupement est related_player_id)
-- ══════════════════════════════════════════════════════════════════════════════
creation_agg AS (
    SELECT
        e.match_id,
        e.team_id,
        e.related_player_id AS player_id,
        COUNT(*) FILTER (WHERE e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE) AS n_assists,
        COUNT(*)                                                                         AS n_chances_created
    FROM in_int_whoscored_events e
    WHERE e.related_player_id IS NOT NULL
      AND e.type_id IN (13, 14, 15, 16)
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.related_player_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- 6. ASSEMBLAGE FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    m.*,
    COALESCE(c.n_assists, 0)         AS n_assists,
    COALESCE(c.n_chances_created, 0) AS n_chances_created,
    pm.minutes_played,
    d.match_date AS date,
    d.season,
    d.league_source,
    d.scraped_at

FROM master_agg m

LEFT JOIN creation_agg c
    ON c.match_id   = m.match_id
   AND c.team_id    = m.team_id
   AND c.player_id  = m.player_id

LEFT JOIN in_int_player_minutes pm
    ON pm.match_id  = m.match_id
   AND pm.team_id   = m.team_id
   AND pm.player_id = m.player_id

JOIN match_dates d
    ON d.match_id = m.match_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "n_actions"                                                  AS int_n_actions,
        "xg_contribution"                                            AS dec_xg_contribution,
        "n_shots"                                                    AS int_n_shots,
        CAST(n_shots_regular_play AS BIGINT)                         AS int_n_shots_regular_play,
        CAST(n_shots_individual_play AS BIGINT)                      AS int_n_shots_individual_play,
        CAST(n_shot_assists AS BIGINT)                               AS int_n_shot_assists,
        CAST(n_key_passes AS BIGINT)                                 AS int_n_key_passes,
        CAST(n_longballs AS BIGINT)                                  AS int_n_longballs,
        CAST(n_crosses AS BIGINT)                                    AS int_n_crosses,
        CAST(n_throughballs AS BIGINT)                               AS int_n_throughballs,
        "n_progressive_passes"                                       AS int_n_progressive_passes,
        "n_tackles"                                                  AS int_n_tackles,
        "n_tackles_won"                                              AS int_n_tackles_won,
        "n_interceptions"                                            AS int_n_interceptions,
        "n_ball_recoveries"                                          AS int_n_ball_recoveries,
        "n_challenges"                                               AS int_n_challenges,
        "n_clearances"                                               AS int_n_clearances,
        "defensive_zone_x"                                           AS dec_defensive_zone_x,
        "n_touches"                                                  AS int_n_touches,
        "pct_z1_c1"                                                  AS dec_pct_z1_c1,
        "pct_z1_c2"                                                  AS dec_pct_z1_c2,
        "pct_z1_c3"                                                  AS dec_pct_z1_c3,
        "pct_z1_c4"                                                  AS dec_pct_z1_c4,
        "pct_z1_c5"                                                  AS dec_pct_z1_c5,
        "pct_z2_c1"                                                  AS dec_pct_z2_c1,
        "pct_z2_c2"                                                  AS dec_pct_z2_c2,
        "pct_z2_c3"                                                  AS dec_pct_z2_c3,
        "pct_z2_c4"                                                  AS dec_pct_z2_c4,
        "pct_z2_c5"                                                  AS dec_pct_z2_c5,
        "pct_z3_c1"                                                  AS dec_pct_z3_c1,
        "pct_z3_c2"                                                  AS dec_pct_z3_c2,
        "pct_z3_c3"                                                  AS dec_pct_z3_c3,
        "pct_z3_c4"                                                  AS dec_pct_z3_c4,
        "pct_z3_c5"                                                  AS dec_pct_z3_c5,
        "pct_z4_c1"                                                  AS dec_pct_z4_c1,
        "pct_z4_c2"                                                  AS dec_pct_z4_c2,
        "pct_z4_c3"                                                  AS dec_pct_z4_c3,
        "pct_z4_c4"                                                  AS dec_pct_z4_c4,
        "pct_z4_c5"                                                  AS dec_pct_z4_c5,
        "pct_z5_c1"                                                  AS dec_pct_z5_c1,
        "pct_z5_c2"                                                  AS dec_pct_z5_c2,
        "pct_z5_c3"                                                  AS dec_pct_z5_c3,
        "pct_z5_c4"                                                  AS dec_pct_z5_c4,
        "pct_z5_c5"                                                  AS dec_pct_z5_c5,
        "n_aerial_duels"                                             AS int_n_aerial_duels,
        "n_aerial_won"                                               AS int_n_aerial_won,
        "aerial_win_rate"                                            AS dec_aerial_win_rate,
        "n_aerial_offensive"                                         AS int_n_aerial_offensive,
        "n_aerial_defensive"                                         AS int_n_aerial_defensive,
        CAST(n_errors_lead_to_goal AS BIGINT)                        AS int_n_errors_lead_to_goal,
        CAST(n_errors_lead_to_shot AS BIGINT)                        AS int_n_errors_lead_to_shot,
        "n_yellow_cards"                                             AS int_n_yellow_cards,
        "n_second_yellows"                                           AS int_n_second_yellows,
        "n_red_cards"                                                AS int_n_red_cards,
        "n_actions_blank"                                            AS int_n_actions_blank,
        "n_progressive_passes_blank"                                 AS int_n_progressive_passes_blank,
        "n_defensive_actions_blank"                                  AS int_n_defensive_actions_blank,
        "n_shots_blank"                                              AS int_n_shots_blank,
        "blank_pct_z1_c1"                                            AS dec_blank_pct_z1_c1,
        "blank_pct_z1_c2"                                            AS dec_blank_pct_z1_c2,
        "blank_pct_z1_c3"                                            AS dec_blank_pct_z1_c3,
        "blank_pct_z1_c4"                                            AS dec_blank_pct_z1_c4,
        "blank_pct_z1_c5"                                            AS dec_blank_pct_z1_c5,
        "blank_pct_z2_c1"                                            AS dec_blank_pct_z2_c1,
        "blank_pct_z2_c2"                                            AS dec_blank_pct_z2_c2,
        "blank_pct_z2_c3"                                            AS dec_blank_pct_z2_c3,
        "blank_pct_z2_c4"                                            AS dec_blank_pct_z2_c4,
        "blank_pct_z2_c5"                                            AS dec_blank_pct_z2_c5,
        "blank_pct_z3_c1"                                            AS dec_blank_pct_z3_c1,
        "blank_pct_z3_c2"                                            AS dec_blank_pct_z3_c2,
        "blank_pct_z3_c3"                                            AS dec_blank_pct_z3_c3,
        "blank_pct_z3_c4"                                            AS dec_blank_pct_z3_c4,
        "blank_pct_z3_c5"                                            AS dec_blank_pct_z3_c5,
        "blank_pct_z4_c1"                                            AS dec_blank_pct_z4_c1,
        "blank_pct_z4_c2"                                            AS dec_blank_pct_z4_c2,
        "blank_pct_z4_c3"                                            AS dec_blank_pct_z4_c3,
        "blank_pct_z4_c4"                                            AS dec_blank_pct_z4_c4,
        "blank_pct_z4_c5"                                            AS dec_blank_pct_z4_c5,
        "blank_pct_z5_c1"                                            AS dec_blank_pct_z5_c1,
        "blank_pct_z5_c2"                                            AS dec_blank_pct_z5_c2,
        "blank_pct_z5_c3"                                            AS dec_blank_pct_z5_c3,
        "blank_pct_z5_c4"                                            AS dec_blank_pct_z5_c4,
        "blank_pct_z5_c5"                                            AS dec_blank_pct_z5_c5,
        "n_actions_blank_late"                                       AS int_n_actions_blank_late,
        "n_progressive_passes_blank_late"                            AS int_n_progressive_passes_blank_late,
        "n_defensive_actions_blank_late"                             AS int_n_defensive_actions_blank_late,
        "n_shots_blank_late"                                         AS int_n_shots_blank_late,
        "blank_late_pct_z1_c1"                                       AS dec_blank_late_pct_z1_c1,
        "blank_late_pct_z1_c2"                                       AS dec_blank_late_pct_z1_c2,
        "blank_late_pct_z1_c3"                                       AS dec_blank_late_pct_z1_c3,
        "blank_late_pct_z1_c4"                                       AS dec_blank_late_pct_z1_c4,
        "blank_late_pct_z1_c5"                                       AS dec_blank_late_pct_z1_c5,
        "blank_late_pct_z2_c1"                                       AS dec_blank_late_pct_z2_c1,
        "blank_late_pct_z2_c2"                                       AS dec_blank_late_pct_z2_c2,
        "blank_late_pct_z2_c3"                                       AS dec_blank_late_pct_z2_c3,
        "blank_late_pct_z2_c4"                                       AS dec_blank_late_pct_z2_c4,
        "blank_late_pct_z2_c5"                                       AS dec_blank_late_pct_z2_c5,
        "blank_late_pct_z3_c1"                                       AS dec_blank_late_pct_z3_c1,
        "blank_late_pct_z3_c2"                                       AS dec_blank_late_pct_z3_c2,
        "blank_late_pct_z3_c3"                                       AS dec_blank_late_pct_z3_c3,
        "blank_late_pct_z3_c4"                                       AS dec_blank_late_pct_z3_c4,
        "blank_late_pct_z3_c5"                                       AS dec_blank_late_pct_z3_c5,
        "blank_late_pct_z4_c1"                                       AS dec_blank_late_pct_z4_c1,
        "blank_late_pct_z4_c2"                                       AS dec_blank_late_pct_z4_c2,
        "blank_late_pct_z4_c3"                                       AS dec_blank_late_pct_z4_c3,
        "blank_late_pct_z4_c4"                                       AS dec_blank_late_pct_z4_c4,
        "blank_late_pct_z4_c5"                                       AS dec_blank_late_pct_z4_c5,
        "blank_late_pct_z5_c1"                                       AS dec_blank_late_pct_z5_c1,
        "blank_late_pct_z5_c2"                                       AS dec_blank_late_pct_z5_c2,
        "blank_late_pct_z5_c3"                                       AS dec_blank_late_pct_z5_c3,
        "blank_late_pct_z5_c4"                                       AS dec_blank_late_pct_z5_c4,
        "blank_late_pct_z5_c5"                                       AS dec_blank_late_pct_z5_c5,
        "n_actions_drawing"                                          AS int_n_actions_drawing,
        "n_progressive_passes_drawing"                               AS int_n_progressive_passes_drawing,
        "n_defensive_actions_drawing"                                AS int_n_defensive_actions_drawing,
        "n_shots_drawing"                                            AS int_n_shots_drawing,
        "drawing_pct_z1_c1"                                          AS dec_drawing_pct_z1_c1,
        "drawing_pct_z1_c2"                                          AS dec_drawing_pct_z1_c2,
        "drawing_pct_z1_c3"                                          AS dec_drawing_pct_z1_c3,
        "drawing_pct_z1_c4"                                          AS dec_drawing_pct_z1_c4,
        "drawing_pct_z1_c5"                                          AS dec_drawing_pct_z1_c5,
        "drawing_pct_z2_c1"                                          AS dec_drawing_pct_z2_c1,
        "drawing_pct_z2_c2"                                          AS dec_drawing_pct_z2_c2,
        "drawing_pct_z2_c3"                                          AS dec_drawing_pct_z2_c3,
        "drawing_pct_z2_c4"                                          AS dec_drawing_pct_z2_c4,
        "drawing_pct_z2_c5"                                          AS dec_drawing_pct_z2_c5,
        "drawing_pct_z3_c1"                                          AS dec_drawing_pct_z3_c1,
        "drawing_pct_z3_c2"                                          AS dec_drawing_pct_z3_c2,
        "drawing_pct_z3_c3"                                          AS dec_drawing_pct_z3_c3,
        "drawing_pct_z3_c4"                                          AS dec_drawing_pct_z3_c4,
        "drawing_pct_z3_c5"                                          AS dec_drawing_pct_z3_c5,
        "drawing_pct_z4_c1"                                          AS dec_drawing_pct_z4_c1,
        "drawing_pct_z4_c2"                                          AS dec_drawing_pct_z4_c2,
        "drawing_pct_z4_c3"                                          AS dec_drawing_pct_z4_c3,
        "drawing_pct_z4_c4"                                          AS dec_drawing_pct_z4_c4,
        "drawing_pct_z4_c5"                                          AS dec_drawing_pct_z4_c5,
        "drawing_pct_z5_c1"                                          AS dec_drawing_pct_z5_c1,
        "drawing_pct_z5_c2"                                          AS dec_drawing_pct_z5_c2,
        "drawing_pct_z5_c3"                                          AS dec_drawing_pct_z5_c3,
        "drawing_pct_z5_c4"                                          AS dec_drawing_pct_z5_c4,
        "drawing_pct_z5_c5"                                          AS dec_drawing_pct_z5_c5,
        "n_actions_drawing_late"                                     AS int_n_actions_drawing_late,
        "n_progressive_passes_drawing_late"                          AS int_n_progressive_passes_drawing_late,
        "n_defensive_actions_drawing_late"                           AS int_n_defensive_actions_drawing_late,
        "n_shots_drawing_late"                                       AS int_n_shots_drawing_late,
        "drawing_late_pct_z1_c1"                                     AS dec_drawing_late_pct_z1_c1,
        "drawing_late_pct_z1_c2"                                     AS dec_drawing_late_pct_z1_c2,
        "drawing_late_pct_z1_c3"                                     AS dec_drawing_late_pct_z1_c3,
        "drawing_late_pct_z1_c4"                                     AS dec_drawing_late_pct_z1_c4,
        "drawing_late_pct_z1_c5"                                     AS dec_drawing_late_pct_z1_c5,
        "drawing_late_pct_z2_c1"                                     AS dec_drawing_late_pct_z2_c1,
        "drawing_late_pct_z2_c2"                                     AS dec_drawing_late_pct_z2_c2,
        "drawing_late_pct_z2_c3"                                     AS dec_drawing_late_pct_z2_c3,
        "drawing_late_pct_z2_c4"                                     AS dec_drawing_late_pct_z2_c4,
        "drawing_late_pct_z2_c5"                                     AS dec_drawing_late_pct_z2_c5,
        "drawing_late_pct_z3_c1"                                     AS dec_drawing_late_pct_z3_c1,
        "drawing_late_pct_z3_c2"                                     AS dec_drawing_late_pct_z3_c2,
        "drawing_late_pct_z3_c3"                                     AS dec_drawing_late_pct_z3_c3,
        "drawing_late_pct_z3_c4"                                     AS dec_drawing_late_pct_z3_c4,
        "drawing_late_pct_z3_c5"                                     AS dec_drawing_late_pct_z3_c5,
        "drawing_late_pct_z4_c1"                                     AS dec_drawing_late_pct_z4_c1,
        "drawing_late_pct_z4_c2"                                     AS dec_drawing_late_pct_z4_c2,
        "drawing_late_pct_z4_c3"                                     AS dec_drawing_late_pct_z4_c3,
        "drawing_late_pct_z4_c4"                                     AS dec_drawing_late_pct_z4_c4,
        "drawing_late_pct_z4_c5"                                     AS dec_drawing_late_pct_z4_c5,
        "drawing_late_pct_z5_c1"                                     AS dec_drawing_late_pct_z5_c1,
        "drawing_late_pct_z5_c2"                                     AS dec_drawing_late_pct_z5_c2,
        "drawing_late_pct_z5_c3"                                     AS dec_drawing_late_pct_z5_c3,
        "drawing_late_pct_z5_c4"                                     AS dec_drawing_late_pct_z5_c4,
        "drawing_late_pct_z5_c5"                                     AS dec_drawing_late_pct_z5_c5,
        "n_actions_winning"                                          AS int_n_actions_winning,
        "n_progressive_passes_winning"                               AS int_n_progressive_passes_winning,
        "n_defensive_actions_winning"                                AS int_n_defensive_actions_winning,
        "n_shots_winning"                                            AS int_n_shots_winning,
        "winning_pct_z1_c1"                                          AS dec_winning_pct_z1_c1,
        "winning_pct_z1_c2"                                          AS dec_winning_pct_z1_c2,
        "winning_pct_z1_c3"                                          AS dec_winning_pct_z1_c3,
        "winning_pct_z1_c4"                                          AS dec_winning_pct_z1_c4,
        "winning_pct_z1_c5"                                          AS dec_winning_pct_z1_c5,
        "winning_pct_z2_c1"                                          AS dec_winning_pct_z2_c1,
        "winning_pct_z2_c2"                                          AS dec_winning_pct_z2_c2,
        "winning_pct_z2_c3"                                          AS dec_winning_pct_z2_c3,
        "winning_pct_z2_c4"                                          AS dec_winning_pct_z2_c4,
        "winning_pct_z2_c5"                                          AS dec_winning_pct_z2_c5,
        "winning_pct_z3_c1"                                          AS dec_winning_pct_z3_c1,
        "winning_pct_z3_c2"                                          AS dec_winning_pct_z3_c2,
        "winning_pct_z3_c3"                                          AS dec_winning_pct_z3_c3,
        "winning_pct_z3_c4"                                          AS dec_winning_pct_z3_c4,
        "winning_pct_z3_c5"                                          AS dec_winning_pct_z3_c5,
        "winning_pct_z4_c1"                                          AS dec_winning_pct_z4_c1,
        "winning_pct_z4_c2"                                          AS dec_winning_pct_z4_c2,
        "winning_pct_z4_c3"                                          AS dec_winning_pct_z4_c3,
        "winning_pct_z4_c4"                                          AS dec_winning_pct_z4_c4,
        "winning_pct_z4_c5"                                          AS dec_winning_pct_z4_c5,
        "winning_pct_z5_c1"                                          AS dec_winning_pct_z5_c1,
        "winning_pct_z5_c2"                                          AS dec_winning_pct_z5_c2,
        "winning_pct_z5_c3"                                          AS dec_winning_pct_z5_c3,
        "winning_pct_z5_c4"                                          AS dec_winning_pct_z5_c4,
        "winning_pct_z5_c5"                                          AS dec_winning_pct_z5_c5,
        "n_actions_winning_late"                                     AS int_n_actions_winning_late,
        "n_progressive_passes_winning_late"                          AS int_n_progressive_passes_winning_late,
        "n_defensive_actions_winning_late"                           AS int_n_defensive_actions_winning_late,
        "n_shots_winning_late"                                       AS int_n_shots_winning_late,
        "winning_late_pct_z1_c1"                                     AS dec_winning_late_pct_z1_c1,
        "winning_late_pct_z1_c2"                                     AS dec_winning_late_pct_z1_c2,
        "winning_late_pct_z1_c3"                                     AS dec_winning_late_pct_z1_c3,
        "winning_late_pct_z1_c4"                                     AS dec_winning_late_pct_z1_c4,
        "winning_late_pct_z1_c5"                                     AS dec_winning_late_pct_z1_c5,
        "winning_late_pct_z2_c1"                                     AS dec_winning_late_pct_z2_c1,
        "winning_late_pct_z2_c2"                                     AS dec_winning_late_pct_z2_c2,
        "winning_late_pct_z2_c3"                                     AS dec_winning_late_pct_z2_c3,
        "winning_late_pct_z2_c4"                                     AS dec_winning_late_pct_z2_c4,
        "winning_late_pct_z2_c5"                                     AS dec_winning_late_pct_z2_c5,
        "winning_late_pct_z3_c1"                                     AS dec_winning_late_pct_z3_c1,
        "winning_late_pct_z3_c2"                                     AS dec_winning_late_pct_z3_c2,
        "winning_late_pct_z3_c3"                                     AS dec_winning_late_pct_z3_c3,
        "winning_late_pct_z3_c4"                                     AS dec_winning_late_pct_z3_c4,
        "winning_late_pct_z3_c5"                                     AS dec_winning_late_pct_z3_c5,
        "winning_late_pct_z4_c1"                                     AS dec_winning_late_pct_z4_c1,
        "winning_late_pct_z4_c2"                                     AS dec_winning_late_pct_z4_c2,
        "winning_late_pct_z4_c3"                                     AS dec_winning_late_pct_z4_c3,
        "winning_late_pct_z4_c4"                                     AS dec_winning_late_pct_z4_c4,
        "winning_late_pct_z4_c5"                                     AS dec_winning_late_pct_z4_c5,
        "winning_late_pct_z5_c1"                                     AS dec_winning_late_pct_z5_c1,
        "winning_late_pct_z5_c2"                                     AS dec_winning_late_pct_z5_c2,
        "winning_late_pct_z5_c3"                                     AS dec_winning_late_pct_z5_c3,
        "winning_late_pct_z5_c4"                                     AS dec_winning_late_pct_z5_c4,
        "winning_late_pct_z5_c5"                                     AS dec_winning_late_pct_z5_c5,
        "n_actions_losing"                                           AS int_n_actions_losing,
        "n_progressive_passes_losing"                                AS int_n_progressive_passes_losing,
        "n_defensive_actions_losing"                                 AS int_n_defensive_actions_losing,
        "n_shots_losing"                                             AS int_n_shots_losing,
        "losing_pct_z1_c1"                                           AS dec_losing_pct_z1_c1,
        "losing_pct_z1_c2"                                           AS dec_losing_pct_z1_c2,
        "losing_pct_z1_c3"                                           AS dec_losing_pct_z1_c3,
        "losing_pct_z1_c4"                                           AS dec_losing_pct_z1_c4,
        "losing_pct_z1_c5"                                           AS dec_losing_pct_z1_c5,
        "losing_pct_z2_c1"                                           AS dec_losing_pct_z2_c1,
        "losing_pct_z2_c2"                                           AS dec_losing_pct_z2_c2,
        "losing_pct_z2_c3"                                           AS dec_losing_pct_z2_c3,
        "losing_pct_z2_c4"                                           AS dec_losing_pct_z2_c4,
        "losing_pct_z2_c5"                                           AS dec_losing_pct_z2_c5,
        "losing_pct_z3_c1"                                           AS dec_losing_pct_z3_c1,
        "losing_pct_z3_c2"                                           AS dec_losing_pct_z3_c2,
        "losing_pct_z3_c3"                                           AS dec_losing_pct_z3_c3,
        "losing_pct_z3_c4"                                           AS dec_losing_pct_z3_c4,
        "losing_pct_z3_c5"                                           AS dec_losing_pct_z3_c5,
        "losing_pct_z4_c1"                                           AS dec_losing_pct_z4_c1,
        "losing_pct_z4_c2"                                           AS dec_losing_pct_z4_c2,
        "losing_pct_z4_c3"                                           AS dec_losing_pct_z4_c3,
        "losing_pct_z4_c4"                                           AS dec_losing_pct_z4_c4,
        "losing_pct_z4_c5"                                           AS dec_losing_pct_z4_c5,
        "losing_pct_z5_c1"                                           AS dec_losing_pct_z5_c1,
        "losing_pct_z5_c2"                                           AS dec_losing_pct_z5_c2,
        "losing_pct_z5_c3"                                           AS dec_losing_pct_z5_c3,
        "losing_pct_z5_c4"                                           AS dec_losing_pct_z5_c4,
        "losing_pct_z5_c5"                                           AS dec_losing_pct_z5_c5,
        "n_actions_losing_late"                                      AS int_n_actions_losing_late,
        "n_progressive_passes_losing_late"                           AS int_n_progressive_passes_losing_late,
        "n_defensive_actions_losing_late"                            AS int_n_defensive_actions_losing_late,
        "n_shots_losing_late"                                        AS int_n_shots_losing_late,
        "losing_late_pct_z1_c1"                                      AS dec_losing_late_pct_z1_c1,
        "losing_late_pct_z1_c2"                                      AS dec_losing_late_pct_z1_c2,
        "losing_late_pct_z1_c3"                                      AS dec_losing_late_pct_z1_c3,
        "losing_late_pct_z1_c4"                                      AS dec_losing_late_pct_z1_c4,
        "losing_late_pct_z1_c5"                                      AS dec_losing_late_pct_z1_c5,
        "losing_late_pct_z2_c1"                                      AS dec_losing_late_pct_z2_c1,
        "losing_late_pct_z2_c2"                                      AS dec_losing_late_pct_z2_c2,
        "losing_late_pct_z2_c3"                                      AS dec_losing_late_pct_z2_c3,
        "losing_late_pct_z2_c4"                                      AS dec_losing_late_pct_z2_c4,
        "losing_late_pct_z2_c5"                                      AS dec_losing_late_pct_z2_c5,
        "losing_late_pct_z3_c1"                                      AS dec_losing_late_pct_z3_c1,
        "losing_late_pct_z3_c2"                                      AS dec_losing_late_pct_z3_c2,
        "losing_late_pct_z3_c3"                                      AS dec_losing_late_pct_z3_c3,
        "losing_late_pct_z3_c4"                                      AS dec_losing_late_pct_z3_c4,
        "losing_late_pct_z3_c5"                                      AS dec_losing_late_pct_z3_c5,
        "losing_late_pct_z4_c1"                                      AS dec_losing_late_pct_z4_c1,
        "losing_late_pct_z4_c2"                                      AS dec_losing_late_pct_z4_c2,
        "losing_late_pct_z4_c3"                                      AS dec_losing_late_pct_z4_c3,
        "losing_late_pct_z4_c4"                                      AS dec_losing_late_pct_z4_c4,
        "losing_late_pct_z4_c5"                                      AS dec_losing_late_pct_z4_c5,
        "losing_late_pct_z5_c1"                                      AS dec_losing_late_pct_z5_c1,
        "losing_late_pct_z5_c2"                                      AS dec_losing_late_pct_z5_c2,
        "losing_late_pct_z5_c3"                                      AS dec_losing_late_pct_z5_c3,
        "losing_late_pct_z5_c4"                                      AS dec_losing_late_pct_z5_c4,
        "losing_late_pct_z5_c5"                                      AS dec_losing_late_pct_z5_c5,
        "n_assists"                                                  AS int_n_assists,
        "n_chances_created"                                          AS int_n_chances_created,
        CAST(minutes_played AS BIGINT)                               AS int_minutes_played,
        "date"                                                       AS dt_date,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at
    FROM mdl_body
)

SELECT * FROM mdl_out
