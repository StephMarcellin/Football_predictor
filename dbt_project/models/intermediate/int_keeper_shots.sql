{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_keeper_shots'
    )
}}

-- Attribution du gardien à chaque tir cadré subi — brique de int_keeper_psxg.
-- Grain : un tir cadré (SavedShot + Goal, hors CSC/penalty), avec son xGOT et le
-- gardien qui l'a subi.
--
-- Deux voies d'attribution (vérifiées sur le corpus) :
--   • tirs arrêtés (15) : gardien = Save miroir, relié au tir par le QUALIFIER 233
--     (related_event_id est NULL — le vrai lien passe par qual 233, comme int_penalties).
--     Save.player_id = GK lineup à 99,995 % ; les écarts = changements de gardien, où le
--     Save est plus juste. → voie PRIMAIRE.
--   • buts (16) : pas de Save miroir → gardien = GK (slot=1) de l'équipe qui défend,
--     période de formation active (start_minute max <= minute du tir). Sert aussi de
--     filet pour un arrêt sans Save.
--
-- keeper_id = COALESCE(save, lineup). Les ~2 % sans gardien (lineup manquant :
-- D2/Minor Club sous-couverts) → keeper_id NULL, exclus de l'agrégat PSxG.

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

-- int_whoscored_lineup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('int_whoscored_lineup') }}
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
WITH shots AS (
    SELECT
        p.str_match_id      AS match_id,
        p.int_row_num       AS row_num,
        p.str_season        AS season,
        p.bool_is_goal      AS is_goal,
        p.dec_xgot          AS xgot,
        sp.team_id          AS att_team,        -- équipe qui tire
        sp.expanded_minute,
        sp.type_id,
        sp.league_source
    FROM {{ source('machine_learning', 'xgot_predictions') }} p
    JOIN in_int_shot_placement sp
        ON sp.match_id = p.str_match_id AND sp.row_num = p.int_row_num
),

-- Les deux équipes canoniques du match → l'équipe qui défend = l'autre.
match_teams AS (
    SELECT match_id, team_id, opponent_id
    FROM in_int_whoscored_match_index
),
with_def AS (
    SELECT
        s.*,
        CASE
            WHEN s.att_team = mt.team_id     THEN mt.opponent_id
            WHEN s.att_team = mt.opponent_id THEN mt.team_id
        END AS def_team
    FROM shots s
    LEFT JOIN match_teams mt USING (match_id)
),

-- ── Voie 1 : Save miroir via qualifier 233 (tirs arrêtés) ─────────────────────
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS save_event_id
    FROM in_events_qual
    WHERE qual_type_id = 233
),
saves AS (
    SELECT match_id, team_id AS def_team, event_id, player_id AS save_player
    FROM in_int_whoscored_events
    WHERE type_id = 10
),
with_save AS (
    SELECT
        w.*,
        sv.save_player
    FROM with_def w
    LEFT JOIN q233 q
        ON q.match_id = w.match_id AND q.row_num = w.row_num
    LEFT JOIN saves sv
        ON  sv.match_id = w.match_id
        AND sv.event_id = q.save_event_id
        AND sv.def_team = w.def_team
),

-- ── Voie 2 : GK (slot=1) de l'équipe qui défend, période de formation active ───
gk_periods AS (
    SELECT match_id, team_id, start_minute, player_id AS gk_player
    FROM in_int_whoscored_lineup
    WHERE slot = 1
),
with_lineup AS (
    SELECT
        ws.*,
        gp.gk_player
    FROM with_save ws
    LEFT JOIN gk_periods gp
        ON  gp.match_id     = ws.match_id
        AND gp.team_id      = ws.def_team
        AND gp.start_minute <= ws.expanded_minute
    -- garde la période la plus récente débutée avant le tir (gère le temps additionnel
    -- et les changements de formation/gardien) ; garantit aussi 1 ligne par tir.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ws.match_id, ws.row_num
        ORDER BY gp.start_minute DESC
    ) = 1
),

-- Couverture : l'équipe qui défend a-t-elle un GK lineup dans ce match ?
-- Sinon ses buts ne sont pas attribuables → les tirs de ce match sont non fiables
-- pour le PSxG (arrêts crédités sans le risque de but correspondant). Sert de
-- garde-fou symétrique en aval (int_keeper_psxg).
def_has_gk AS (
    SELECT DISTINCT match_id, team_id
    FROM gk_periods
)

SELECT
    wl.match_id,
    wl.row_num,
    wl.season,
    wl.league_source,
    wl.type_id,
    wl.is_goal,
    wl.xgot,
    wl.def_team,
    COALESCE(wl.save_player, wl.gk_player) AS keeper_id,
    CASE
        WHEN wl.save_player IS NOT NULL THEN 'save'
        WHEN wl.gk_player   IS NOT NULL THEN 'lineup'
        ELSE 'none'
    END AS attribution_method,
    -- TRUE si les buts sont attribuables dans ce match (buts + arrêts symétriques)
    (dg.match_id IS NOT NULL) AS def_gk_available
FROM with_lineup wl
LEFT JOIN def_has_gk dg
    ON dg.match_id = wl.match_id AND dg.team_id = wl.def_team
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        CAST(type_id AS VARCHAR)                                     AS str_type_id,
        "is_goal"                                                    AS bool_is_goal,
        "xgot"                                                       AS dec_xgot,
        "def_team"                                                   AS int_def_team,
        CAST(keeper_id AS VARCHAR)                                   AS str_keeper_id,
        "attribution_method"                                         AS str_attribution_method,
        "def_gk_available"                                           AS bool_def_gk_available
    FROM mdl_body
)

SELECT * FROM mdl_out
