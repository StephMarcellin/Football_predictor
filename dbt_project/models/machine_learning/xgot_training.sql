{{
    config(
        materialized='view',
        schema='machine_learning',
        alias='xgot_training'
    )
}}

-- Dataset d'entraînement du modèle xGOT (post-shot xG).
-- Cible : label = is_goal (le tir cadré finit-il au fond, vu son placement).
-- = socle de features (xgot_features) + label. Toute la logique de features et le
-- périmètre « tir cadré éligible » vivent dans xgot_features, partagé avec le
-- scoring → train/serve skew structurellement impossible.
-- Grain : un tir cadré éligible. Vue → toujours à jour.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- xgot_features lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_xgot_features AS (
    SELECT
        str_match_id                                                 AS "match_id",
        int_row_num                                                  AS "row_num",
        str_season                                                   AS "season",
        bool_is_goal                                                 AS "is_goal",
        dec_offset_center                                            AS "offset_center",
        dec_height                                                   AS "height",
        dec_corner_dist                                              AS "corner_dist",
        str_placement_col                                            AS "placement_col",
        str_placement_row                                            AS "placement_row",
        str_placement_zone                                           AS "placement_zone",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_shot_distance_m                                          AS "shot_distance_m",
        dec_shot_angle_rad                                           AS "shot_angle_rad",
        dec_pre_shot_xg_proxy                                        AS "pre_shot_xg_proxy"
    FROM {{ ref('xgot_features') }}
),

mdl_body AS (
SELECT
    * EXCLUDE (is_goal),
    is_goal::INTEGER AS label
FROM in_xgot_features
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
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
        "pre_shot_xg_proxy"                                          AS dec_pre_shot_xg_proxy,
        "label"                                                      AS int_label
    FROM mdl_body
)

SELECT * FROM mdl_out
