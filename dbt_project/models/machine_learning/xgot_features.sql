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
    FROM {{ ref('int_shot_placement') }} sp
    WHERE sp.is_on_target = TRUE
      AND sp.goal_mouth_y IS NOT NULL
      AND COALESCE(sp.is_own_goal, FALSE) = FALSE
      AND NOT EXISTS (
          SELECT 1 FROM {{ ref('events_qual') }} q
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
