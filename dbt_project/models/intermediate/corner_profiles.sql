{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'corner_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='corner_profiles'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE 1 — CORNER_CHAINS
-- Toutes les chaînes déclenchées par un corner.
-- On calcule le row_num du premier événement (= la livraison) par chaîne.
-- ══════════════════════════════════════════════════════════════════════════════
corner_chains AS (
    SELECT
        match_id,
        chain_id,
        chain_team_id                                          AS team_id,
        row_num,
        type_id,
        is_shot,
        x,
        y,
        MIN(row_num) OVER (PARTITION BY chain_id)              AS chain_first_row,
        COUNT(*) OVER (PARTITION BY chain_id)                  AS chain_length
    FROM {{ ref('player_possession_chains') }}
    WHERE chain_trigger = 'corner'
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE 2 — CORNER_DELIVERY
-- On récupère end_x, end_y et player_id depuis events_qual
-- uniquement sur le premier événement de chaque chaîne (la livraison).
-- Filtre sur qual_type_id=6 (CornerTaken) pour éviter l'explosion de lignes.
-- ══════════════════════════════════════════════════════════════════════════════
corner_delivery AS (
    SELECT DISTINCT
        cc.chain_id,
        cc.team_id,
        eq.player_id                                           AS corner_taker_id,
        cc.chain_first_row                                     AS corner_row_num,
        eq.y                                                   AS corner_start_y,
        eq.end_x                                               AS delivery_end_x,
        eq.end_y                                               AS delivery_end_y,
        eq.match_id,
        eq.expanded_minute
    FROM corner_chains cc
    JOIN {{ ref('events_qual') }} eq
        ON  eq.match_id    = cc.match_id
        AND eq.row_num     = cc.chain_first_row
        AND eq.qual_type_id = 6   -- CornerTaken : une ligne par livraison
    WHERE cc.row_num = cc.chain_first_row
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE 3 — CORNER_AGG
-- Agrégation des événements de chaque chaîne corner :
--   - duels aériens dans la séquence
--   - présence d'un tir (équipe tireuse)
--   - présence d'un dégagement (équipe adverse)
-- ══════════════════════════════════════════════════════════════════════════════
corner_agg AS (
    SELECT
        chain_id,
        COUNT(*) FILTER (WHERE type_id = 44)                   AS n_aerial_duels,
        MAX(CASE WHEN is_shot = TRUE
             THEN 1 ELSE 0 END)                                AS has_shot,
        MAX(CASE WHEN type_id = 12
             THEN 1 ELSE 0 END)                                AS has_clearance
    FROM corner_chains
    GROUP BY chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_ATTACKING_EVENTS
-- Relit player_possession_chains directement (pas corner_chains, qui a
-- déjà écrasé team_id par chain_team_id). On ne garde que les actions de
-- l'équipe qui a obtenu le corner (team_id = chain_team_id), et on joint
-- action_value depuis event_values pour noter chaque action.
-- ══════════════════════════════════════════════════════════════════════════════
corner_attacking_events AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.row_num,
        pc.type_id,
        pc.is_shot,
        ev.action_value
    FROM {{ ref('player_possession_chains') }} pc
    LEFT JOIN {{ ref('event_values') }} ev
        ON  ev.match_id = pc.match_id
        AND ev.row_num  = pc.row_num
    WHERE pc.chain_trigger = 'corner'
      AND pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id = pc.chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_LAST_SHOT
-- row_num du dernier tir de chaque chaîne (convention déjà utilisée pour
-- freekick_profiles : le dernier tir de la chaîne fait foi).
-- ══════════════════════════════════════════════════════════════════════════════
corner_last_shot AS (
    SELECT
        match_id,
        chain_id,
        MAX(row_num) AS last_shot_row,
        ARG_MAX(type_id, row_num) AS last_shot_type_id
    FROM corner_attacking_events
    WHERE is_shot = TRUE
    GROUP BY match_id, chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_SHOT_BODYPART
-- Tête/pied du dernier tir de la chaîne (qualifiers 15/20/72/21 sur son row_num).
-- ══════════════════════════════════════════════════════════════════════════════
corner_shot_bodypart AS (
    SELECT
        cls.match_id,
        cls.chain_id,
        MAX(CASE
            WHEN eq.qual_type_id = 15 THEN 'head'
            WHEN eq.qual_type_id = 20 THEN 'right_foot'
            WHEN eq.qual_type_id = 72 THEN 'left_foot'
            WHEN eq.qual_type_id = 21 THEN 'other'
        END) AS shot_body_part
    FROM corner_last_shot cls
    JOIN {{ ref('events_qual') }} eq
        ON  eq.match_id = cls.match_id
        AND eq.row_num  = cls.last_shot_row
        AND eq.qual_type_id IN (15, 20, 72, 21)
    GROUP BY cls.match_id, cls.chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_DEFENDING_CLEARANCE / CORNER_CLEARANCE_DETAIL
-- Premier dégagement chronologique de la chaîne (équipe défenseuse,
-- team_id != chain_team_id). Joueur, qualité (def_execution_quality),
-- et flag tête (qualifiers 15/21 uniquement — pas de distinction pied
-- disponible sur les Clearance dans ce dataset).
-- ══════════════════════════════════════════════════════════════════════════════
corner_defending_clearance AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.row_num,
        pc.team_id,
        pc.player_id,
        ev.def_execution_quality,
        ROW_NUMBER() OVER (
            PARTITION BY pc.match_id, pc.chain_id
            ORDER BY pc.row_num ASC
        ) AS rn_first
    FROM {{ ref('player_possession_chains') }} pc
    LEFT JOIN {{ ref('event_values') }} ev
        ON  ev.match_id = pc.match_id
        AND ev.row_num  = pc.row_num
    WHERE pc.chain_trigger = 'corner'
      AND pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.type_id = 12
      AND pc.team_id != pc.chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_CLEARANCE_NEXT_POSSESSION
-- Première possession certaine après le premier dégagement de la chaîne
-- (on saute les événements passifs/neutres comme CornerAwarded, Aerial...).
-- ══════════════════════════════════════════════════════════════════════════════
corner_clearance_next_possession AS (
    SELECT
        cdc.match_id,
        cdc.chain_id,
        pc2.certain_possessor                                  AS recovering_team_id,
        pc2.x                                                  AS recovery_x,
        ROW_NUMBER() OVER (
            PARTITION BY cdc.match_id, cdc.chain_id
            ORDER BY pc2.row_num ASC
        ) AS rn_recovery
    FROM corner_defending_clearance cdc
    JOIN {{ ref('player_possession_chains') }} pc2
        ON  pc2.match_id = cdc.match_id
        AND pc2.row_num  > cdc.row_num
        AND pc2.certain_possessor IS NOT NULL
    WHERE cdc.rn_first = 1
),

corner_clearance_detail AS (
    SELECT
        cdc.match_id,
        cdc.chain_id,
        MAX(cdc.player_id)                                     AS clearance_player_id,
        -- MAX sur un booléen : TRUE l'emporte si l'événement porte à la fois
        -- le qualifier Head et Other (rare, 8 cas vus en base) — priorité au
        -- signal le plus spécifique plutôt que de dupliquer la ligne.
        MAX(CASE
            WHEN eq.qual_type_id = 15 THEN TRUE
            WHEN eq.qual_type_id = 21 THEN FALSE
        END)                                                    AS is_headed_clearance,
        -- Grille de qualité : qui récupère la première possession certaine
        -- après le dégagement, et où.
        MAX(CASE
            WHEN ncp.recovering_team_id = cdc.team_id  THEN 'perfect'
            WHEN ncp.recovering_team_id IS NULL         THEN NULL
            WHEN ncp.recovery_x >= 83                   THEN 'failed'
            WHEN ncp.recovery_x >= 75                   THEN 'poor'
            ELSE                                             'good'
        END)                                                    AS clearance_quality
    FROM corner_defending_clearance cdc
    LEFT JOIN {{ ref('events_qual') }} eq
        ON  eq.match_id = cdc.match_id
        AND eq.row_num  = cdc.row_num
        AND eq.qual_type_id IN (15, 21)
    LEFT JOIN corner_clearance_next_possession ncp
        ON  ncp.match_id = cdc.match_id
        AND ncp.chain_id = cdc.chain_id
        AND ncp.rn_recovery = 1
    WHERE cdc.rn_first = 1
    GROUP BY cdc.match_id, cdc.chain_id
),

corner_danger_agg AS (
    SELECT
        cae.chain_id,
        SUM(cae.action_value)                                                 AS chain_danger_total,
        SUM(cae.action_value) FILTER (WHERE cae.row_num < cls.last_shot_row)  AS danger_before_shot,
        cls.last_shot_row,
        cls.last_shot_type_id
    FROM corner_attacking_events cae
    LEFT JOIN corner_last_shot cls
        ON  cls.match_id = cae.match_id
        AND cls.chain_id = cae.chain_id
    GROUP BY cae.chain_id, cls.last_shot_row, cls.last_shot_type_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE 4 — SELECT FINAL
-- Assemblage : delivery + agg + landing_zone calculée depuis end_x/end_y
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        cd.match_id,
        cd.team_id,
        cd.corner_taker_id,
        cd.corner_row_num,
        cd.expanded_minute,

        -- Côté du corner : déduit du y de départ, même convention que le
        -- reste du fichier (y < 50 = right).
        CASE
            WHEN cd.corner_start_y < 50 THEN 'right'
            ELSE 'left'
        END                                                    AS corner_side,

        -- Zone d'atterrissage RELATIVE au côté du corner (proche/lointain),
        -- pas en coordonnées absolues comme avant. Seuils numériques inchangés.
        CASE
            WHEN cd.delivery_end_x < 83
                THEN 'short'
            WHEN cd.delivery_end_y BETWEEN 45 AND 55
                THEN 'center'
            WHEN (cd.corner_start_y < 50  AND cd.delivery_end_y < 45)
              OR (cd.corner_start_y >= 50 AND cd.delivery_end_y > 55)
                THEN 'near_post'
            ELSE 'far_post'
        END                                                    AS landing_zone,

        ca.n_aerial_duels,

        csb.shot_body_part,

        ccd.clearance_player_id,
        ccd.clearance_quality,
        ccd.is_headed_clearance,

        -- Outcome : priorité goal > shot_saved > shot_off_target > clearance > retained
        CASE
            WHEN da.last_shot_type_id = 16         THEN 'goal'
            WHEN da.last_shot_type_id = 15          THEN 'shot_saved'
            WHEN da.last_shot_type_id IN (13, 14)   THEN 'shot_off_target'
            WHEN ca.has_clearance = 1               THEN 'clearance'
            ELSE                                         'retained'
        END                                                    AS outcome,

        -- chain_danger_total : somme du danger (action_value) généré par
        -- l'équipe attaquante sur toute la chaîne issue du corner.
        da.chain_danger_total,

        -- chain_danger_momentum : part du danger accumulée AVANT le dernier
        -- tir, rapportée au danger total de la chaîne. NULL si aucun tir.
        CASE
            WHEN da.last_shot_row IS NULL THEN NULL
            ELSE da.danger_before_shot / NULLIF(da.chain_danger_total, 0)
        END                                                    AS chain_danger_momentum

    FROM corner_delivery cd
    JOIN corner_agg ca
        ON ca.chain_id = cd.chain_id
    LEFT JOIN corner_danger_agg da
        ON da.chain_id = cd.chain_id
    LEFT JOIN corner_shot_bodypart csb
        ON csb.chain_id = cd.chain_id
    LEFT JOIN corner_clearance_detail ccd
        ON ccd.chain_id = cd.chain_id
)

SELECT * FROM final