{{
    config(
        materialized='view',
        schema='machine_learning',
        alias='xgot_features'
    )
}}

-- Socle de features xGOT — logique de calcul PARTAGÉE entre l'entraînement
-- (xgot_training) et le scoring (xgot_scoring). Définir le périmètre « tir cadré
-- éligible » et les features à UN SEUL endroit interdit structurellement le
-- train/serve skew (features du scoring calculées autrement qu'à l'entraînement).
-- Grain : un tir cadré éligible. Vue → toujours à jour avec int_shot_placement.
--
-- Périmètre (identique train ET scoring) :
--   • is_on_target        : le xGOT n'existe que pour un tir cadré (SavedShot + Goal)
--   • goal_mouth_y NOT NULL: sans placement, pas de features → tir non scorable
--   • hors CSC            : un but contre son camp n'est pas un tir à modéliser
--   • hors penalty (qual 9): régime de conversion à part (~79 %), exclu du xGOT
--
-- Géométrie : coordonnées WhoScored 0-100 → mètres (x·1.05, y·0.68). But adverse
-- à x=105 m ; poteaux à y≈30.34 et 37.66 m (largeur 7.32 m), centre y=34 m.

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

mdl_body AS (
WITH base AS (
    SELECT
        sp.match_id,
        sp.row_num,
        sp.season,
        sp.is_goal,
        -- Placement dans le cadre (cœur du xGOT)
        sp.offset_center,
        sp.height,
        sp.corner_dist,
        sp.placement_col,
        sp.placement_row,
        sp.placement_zone,
        -- Localisation de la frappe
        sp.x,
        sp.y,
        sp.pre_shot_xg_proxy,
        -- Conversion en mètres pour la géométrie
        sp.x * 1.05 AS sx_m,
        sp.y * 0.68 AS sy_m
    FROM in_int_shot_placement sp
    WHERE sp.is_on_target = TRUE
      AND sp.goal_mouth_y IS NOT NULL
      AND COALESCE(sp.is_own_goal, FALSE) = FALSE
      AND NOT EXISTS (
          SELECT 1 FROM in_events_qual q
          WHERE q.match_id = sp.match_id
            AND q.row_num  = sp.row_num
            AND q.qual_type_id = 9          -- exclut les penaltys
      )
),

geo AS (
    SELECT
        *,
        -- Distance au centre du but (m)
        SQRT(POW(105 - sx_m, 2) + POW(34 - sy_m, 2))              AS shot_distance_m,
        -- Distances aux deux poteaux (pour l'angle de tir)
        SQRT(POW(105 - sx_m, 2) + POW(30.34 - sy_m, 2))          AS dist_post_a,
        SQRT(POW(105 - sx_m, 2) + POW(37.66 - sy_m, 2))          AS dist_post_b
    FROM base
)

SELECT
    match_id,
    row_num,
    season,
    is_goal,

    -- ── Features placement ────────────────────────────────────────────────────
    offset_center,
    height,
    corner_dist,
    placement_col,
    placement_row,
    placement_zone,

    -- ── Features localisation ─────────────────────────────────────────────────
    x,
    y,
    shot_distance_m,
    -- Angle de tir (loi des cosinus), borné au domaine de acos. Radians.
    ACOS(
        LEAST(1.0, GREATEST(-1.0,
            (dist_post_a * dist_post_a + dist_post_b * dist_post_b - 7.32 * 7.32)
            / (2 * dist_post_a * dist_post_b)
        ))
    )                                                            AS shot_angle_rad,

    pre_shot_xg_proxy

FROM geo
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        "is_goal"                                                    AS bool_is_goal,
        "offset_center"                                              AS dec_offset_center,
        "height"                                                     AS dec_height,
        "corner_dist"                                                AS dec_corner_dist,
        "placement_col"                                              AS str_placement_col,
        "placement_row"                                              AS str_placement_row,
        "placement_zone"                                             AS str_placement_zone,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "shot_distance_m"                                            AS dec_shot_distance_m,
        "shot_angle_rad"                                             AS dec_shot_angle_rad,
        "pre_shot_xg_proxy"                                          AS dec_pre_shot_xg_proxy
    FROM mdl_body
)

SELECT * FROM mdl_out
