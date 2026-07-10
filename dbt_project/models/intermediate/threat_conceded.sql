{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'chain_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='threat_conceded'
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
    FROM {{ ref('player_xg_chain') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_xg_chain') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_SHOTS
-- Une ligne par chaîne avec tir.
-- is_penalty : le tir terminal est-il un penalty (qual 9) ?
--   → si oui, le gardien adverse n'est pas sanctionné
-- ══════════════════════════════════════════════════════════════════════════════
chain_shots AS (
    SELECT DISTINCT ON (xgc.match_id, xgc.chain_id)
        xgc.match_id,
        xgc.chain_id,
        xgc.chain_number,
        xgc.chain_team_id,
        xgc.xg_proxy,
        xgc.season,
        xgc.league_source,
        xgc.shot_event_id,
        CASE
            WHEN eq_pen.qual_type_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                                 AS is_penalty
    FROM {{ ref('player_xg_chain') }} xgc
    LEFT JOIN {{ ref('events_qual') }} eq_pen
        ON  eq_pen.match_id     = xgc.match_id
        AND eq_pen.event_id     = xgc.shot_event_id
        AND eq_pen.qual_type_id = 9  -- Penalty
    WHERE xgc.match_id IN (SELECT match_id FROM new_matches)
    ORDER BY xgc.match_id, xgc.chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- KEEPER_ACTIONS
-- Identifie les gardiens parmi les joueurs adverses dans chaque chaîne.
-- Un gardien = joueur qui a réalisé au moins une action exclusive de gardien
-- (Save, Claim, Punch, KeeperPickup) dans cette chaîne.
-- ══════════════════════════════════════════════════════════════════════════════
keeper_actions AS (
    SELECT DISTINCT
        pc.match_id,
        pc.chain_id,
        pc.player_id
    FROM {{ ref('player_possession_chains') }} pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id  != pc.chain_team_id
      AND pc.type_id   IN (10, 11, 41, 52)  -- Save, Claim, Punch, KeeperPickup
),

-- ══════════════════════════════════════════════════════════════════════════════
-- KEEPER_ERRORS
-- Identifie les gardiens qui ont commis une Error LeadingToGoal
-- dans la chaîne. Ces gardiens peuvent être sanctionnés malgré leur statut.
-- ══════════════════════════════════════════════════════════════════════════════
keeper_errors AS (
    SELECT DISTINCT
        pc.match_id,
        pc.chain_id,
        pc.player_id
    FROM {{ ref('player_possession_chains') }} pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    JOIN {{ ref('events_qual') }} eq_err
        ON  eq_err.match_id     = pc.match_id
        AND eq_err.event_id     = pc.event_id
        AND eq_err.qual_type_id = 170  -- LeadingToGoal
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id  != pc.chain_team_id
      AND pc.type_id   = 51  -- Error
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DEFENSIVE_PLAYERS_RAW
-- Joueurs adverses présents dans les chaînes avec tir.
-- ══════════════════════════════════════════════════════════════════════════════
defensive_players_raw AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_team_id,
        pc.player_id,
        pc.team_id,
        pc.expanded_minute,
        pc.second,
        pc.row_num,
        ROW_NUMBER() OVER (
            PARTITION BY pc.match_id, pc.chain_id
            ORDER BY pc.expanded_minute, pc.second, pc.row_num
        ) AS position_in_chain,
        COUNT(*) OVER (
            PARTITION BY pc.match_id, pc.chain_id
        ) AS chain_length
    FROM {{ ref('player_possession_chains') }} pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id  != pc.chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DEFENSIVE_PLAYERS
-- Déduplique + ajoute les flags is_keeper et has_error.
-- ══════════════════════════════════════════════════════════════════════════════
defensive_players AS (
    SELECT DISTINCT ON (dp.match_id, dp.chain_id, dp.player_id)
        dp.match_id,
        dp.chain_id,
        dp.chain_team_id,
        dp.player_id,
        dp.team_id,
        dp.position_in_chain,
        dp.chain_length,
        ROUND(
            CAST(dp.position_in_chain AS DOUBLE) / dp.chain_length,
            4
        )                                                   AS position_weight,
        CASE WHEN ka.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_keeper,
        CASE WHEN ke.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_error_leading_to_goal
    FROM defensive_players_raw dp
    LEFT JOIN keeper_actions ka
        ON  ka.match_id  = dp.match_id
        AND ka.chain_id  = dp.chain_id
        AND ka.player_id = dp.player_id
    LEFT JOIN keeper_errors ke
        ON  ke.match_id  = dp.match_id
        AND ke.chain_id  = dp.chain_id
        AND ke.player_id = dp.player_id
    ORDER BY dp.match_id, dp.chain_id, dp.player_id, dp.position_in_chain DESC
),

-- ══════════════════════════════════════════════════════════════════════════════
-- THREAT_COMPUTED
-- Calcul de threat_conceded avec règles de sanction du gardien :
--
-- Règle 1 — Penalty : le gardien n'est JAMAIS sanctionné
--   → threat_conceded_weighted = 0 pour le gardien si is_penalty = TRUE
--
-- Règle 2 — Erreur du gardien : sanction pleine si Error LeadingToGoal
--   → threat_conceded_weighted normal même si is_keeper = TRUE
--
-- Règle 3 — Défense trouée : le gardien n'est PAS sanctionné par défaut
--   → threat_conceded_weighted = 0 si is_keeper = TRUE sans erreur
--
-- Pour les défenseurs non-gardiens : sanction pondérée normale.
-- ══════════════════════════════════════════════════════════════════════════════
threat_computed AS (
    SELECT
        dp.match_id,
        dp.chain_id,
        cs.chain_number,
        dp.chain_team_id,
        dp.player_id,
        dp.team_id,
        cs.season,
        cs.league_source,
        cs.xg_proxy,
        cs.is_penalty,
        dp.position_in_chain,
        dp.chain_length,
        dp.position_weight,
        dp.is_keeper,
        dp.has_error_leading_to_goal,

        -- Sanction brute
        cs.xg_proxy                                         AS threat_conceded,

        -- Sanction pondérée avec règles gardien
        CASE
            -- Règle 1 : penalty → gardien non sanctionné
            WHEN dp.is_keeper = TRUE AND cs.is_penalty = TRUE
                THEN 0.0
            -- Règle 2 : erreur du gardien → sanction pleine
            WHEN dp.is_keeper = TRUE AND dp.has_error_leading_to_goal = TRUE
                THEN ROUND(cs.xg_proxy * dp.position_weight, 4)
            -- Règle 3 : défense trouée → gardien non sanctionné
            WHEN dp.is_keeper = TRUE
                THEN 0.0
            -- Défenseur non-gardien : sanction pondérée normale
            ELSE ROUND(cs.xg_proxy * dp.position_weight, 4)
        END                                                 AS threat_conceded_weighted

    FROM defensive_players dp

    JOIN chain_shots cs
        ON  cs.match_id = dp.match_id
        AND cs.chain_id = dp.chain_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM threat_computed