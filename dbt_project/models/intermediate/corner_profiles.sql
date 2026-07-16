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
        MAX(row_num) AS last_shot_row
    FROM corner_attacking_events
    WHERE is_shot = TRUE
    GROUP BY match_id, chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — CORNER_DANGER_AGG
-- chain_danger_total : somme de tout le danger généré par l'équipe attaquante
-- dans la chaîne (SUM(action_value)).
-- danger_before_shot : part de ce danger accumulée avant le dernier tir,
-- utilisée ensuite pour calculer le ratio de momentum.
-- ══════════════════════════════════════════════════════════════════════════════
corner_danger_agg AS (
    SELECT
        cae.chain_id,
        SUM(cae.action_value)                                                 AS chain_danger_total,
        SUM(cae.action_value) FILTER (WHERE cae.row_num < cls.last_shot_row)  AS danger_before_shot,
        cls.last_shot_row
    FROM corner_attacking_events cae
    LEFT JOIN corner_last_shot cls
        ON  cls.match_id = cae.match_id
        AND cls.chain_id = cae.chain_id
    GROUP BY cae.chain_id, cls.last_shot_row
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

        -- Zone d'atterrissage calculée depuis les coordonnées de livraison
        CASE
            WHEN cd.delivery_end_x < 83
                THEN 'Short'
            WHEN cd.delivery_end_y < 45
                THEN 'Right'
            WHEN cd.delivery_end_y > 55
                THEN 'Left'
            ELSE 'Center'
        END                                                    AS landing_zone,

        ca.n_aerial_duels,

        -- Outcome : priorité shot > clearance > retained
        CASE
            WHEN ca.has_shot      = 1 THEN 'shot'
            WHEN ca.has_clearance = 1 THEN 'clearance'
            ELSE                           'retained'
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
)

SELECT * FROM final