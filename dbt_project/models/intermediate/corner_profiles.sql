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

        -- xg_generated : 1 si tir dans la séquence, 0 sinon
        -- (approximation — pas de valeur xG par tir dans events_qual)
        ca.has_shot                                            AS xg_generated

    FROM corner_delivery cd
    JOIN corner_agg ca
        ON ca.chain_id = cd.chain_id
)

SELECT * FROM final