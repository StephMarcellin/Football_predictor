{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_shot_placement'
    )
}}

-- Placement de tir au grain « un tir » — base du Post-Shot xG (xGOT).
-- Filtre les events de tir (13 MissedShots, 14 ShotOnPost, 15 SavedShot, 16 Goal)
-- depuis int_event_enriched et dérive la géométrie du placement dans le cadre du
-- but à partir de goal_mouth_y (horizontal) / goal_mouth_z (hauteur).
--
-- Cadre du but observé : poteaux à Y≈45.2 et 54.8 (centre 50), barre à Z≈38.
-- Les dérivés de placement ne valent que pour les tirs CADRÉS (SavedShot + Goal),
-- seuls concernés par le xGOT ; NULL sinon.
--
-- Le xGOT lui-même (modèle entraîné) reste en ML/gold. Ce modèle expose les
-- ENTRÉES (placement + xG pré-tir proxy) + des dérivés géométriques de difficulté.

WITH shots AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,
        e.event_id,
        e.row_num,
        e.expanded_minute,
        e.season,
        e.league_source,
        e.scraped_at,
        e.type_id,
        e.type_name,
        e.x,                       -- position terrain de la frappe
        e.y,
        e.goal_mouth_y,
        e.goal_mouth_z,
        e.blocked_x,
        e.blocked_y,
        (e.type_id = 16)          AS is_goal,
        (e.type_id IN (15, 16))   AS is_on_target   -- saved + goal
    FROM {{ ref('int_event_enriched') }} e
    WHERE e.type_id IN (13, 14, 15, 16)
    -- int_event_enriched contient ~94 doublons (match_id, row_num) sur les tirs
    -- (souci de son modèle incrémental) : on garde une ligne par tir pour
    -- garantir l'unicité de la clé du placement.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num ORDER BY e.event_id) = 1
)

SELECT
    s.*,

    -- xG pré-tir (proxy chance_creation) : baseline du xGOT
    ev.chance_creation AS pre_shot_xg_proxy,

    -- ── Dérivés de placement (tirs cadrés uniquement) ─────────────────
    -- Écart horizontal au centre (0 = plein axe, ~4.8 = près d'un poteau)
    CASE WHEN s.is_on_target THEN ABS(s.goal_mouth_y - 50.0) END        AS offset_center,
    -- Hauteur dans le but (0 = ras de terre, ~38 = sous la barre)
    CASE WHEN s.is_on_target THEN s.goal_mouth_z END                    AS height,

    -- Colonne / rangée de la grille 3×3
    CASE WHEN s.is_on_target THEN
        CASE WHEN s.goal_mouth_y <  48.4 THEN 'left'
             WHEN s.goal_mouth_y <= 51.6 THEN 'center'
             ELSE 'right' END
    END                                                                 AS placement_col,
    CASE WHEN s.is_on_target THEN
        CASE WHEN s.goal_mouth_z <  12.7 THEN 'low'
             WHEN s.goal_mouth_z <= 25.3 THEN 'mid'
             ELSE 'high' END
    END                                                                 AS placement_row,
    -- Zone 9 cases : rangée_colonne (ex: high_left = lucarne gauche)
    CASE WHEN s.is_on_target THEN
        (CASE WHEN s.goal_mouth_z <  12.7 THEN 'low'
              WHEN s.goal_mouth_z <= 25.3 THEN 'mid'  ELSE 'high'  END)
        || '_' ||
        (CASE WHEN s.goal_mouth_y <  48.4 THEN 'left'
              WHEN s.goal_mouth_y <= 51.6 THEN 'center' ELSE 'right' END)
    END                                                                 AS placement_zone,

    -- Distance normalisée à la lucarne la plus proche (0 = pleine lucarne, plus
    -- grand = plus central/bas). Coords normalisées dans le cadre pour ne pas
    -- mélanger les échelles Y (largeur ~9.6) et Z (hauteur ~38).
    CASE WHEN s.is_on_target THEN
        LEAST(
            SQRT(POW((s.goal_mouth_y - 45.2) / 9.6, 2) + POW(1 - s.goal_mouth_z / 38.0, 2)),
            SQRT(POW((54.8 - s.goal_mouth_y) / 9.6, 2) + POW(1 - s.goal_mouth_z / 38.0, 2))
        )
    END                                                                 AS corner_dist

FROM shots s
LEFT JOIN {{ ref('event_values') }} ev
    ON ev.match_id = s.match_id
   AND ev.row_num  = s.row_num
