{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_chain_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='threat_conceded'
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

-- player_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_possession_chains AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        int_is_rupture                                               AS "is_rupture",
        str_chain_trigger                                            AS "chain_trigger",
        int_certain_possessor                                        AS "certain_possessor",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at"
    FROM {{ ref('player_possession_chains') }}
),

-- player_xg_chain lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_xg_chain AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(str_shot_event_id AS INTEGER)                           AS "shot_event_id",
        CAST(str_shot_type_id AS INTEGER)                            AS "shot_type_id",
        int_shot_minute                                              AS "shot_minute",
        dec_xg_proxy                                                 AS "xg_proxy",
        bool_is_counter_attack                                       AS "is_counter_attack",
        int_position_in_chain                                        AS "position_in_chain",
        int_chain_length                                             AS "chain_length",
        dec_position_weight                                          AS "position_weight",
        bool_is_shooter                                              AS "is_shooter",
        bool_is_assister                                             AS "is_assister",
        dec_xgchain                                                  AS "xgchain",
        dec_xgchain_weighted                                         AS "xgchain_weighted",
        dec_xgbuildup                                                AS "xgbuildup",
        dec_xgbuildup_weighted                                       AS "xgbuildup_weighted"
    FROM {{ ref('player_xg_chain') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_xg_chain
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            str_chain_id                                                 AS "chain_id",
            CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
            CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            dec_xg_proxy                                                 AS "xg_proxy",
            bool_is_penalty                                              AS "is_penalty",
            int_position_in_chain                                        AS "position_in_chain",
            int_chain_length                                             AS "chain_length",
            dec_position_weight                                          AS "position_weight",
            bool_is_keeper                                               AS "is_keeper",
            bool_has_error_leading_to_goal                               AS "has_error_leading_to_goal",
            dec_threat_conceded                                          AS "threat_conceded",
            dec_threat_conceded_weighted                                 AS "threat_conceded_weighted"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_xg_chain
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
    FROM in_player_xg_chain xgc
    LEFT JOIN in_events_qual eq_pen
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
    FROM in_player_possession_chains pc
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
    FROM in_player_possession_chains pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    JOIN in_events_qual eq_err
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
    FROM in_player_possession_chains pc
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
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "chain_id"                                                   AS str_chain_id,
        CAST(chain_number AS BIGINT)                                 AS int_chain_number,
        CAST(chain_team_id AS VARCHAR)                               AS str_chain_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "xg_proxy"                                                   AS dec_xg_proxy,
        "is_penalty"                                                 AS bool_is_penalty,
        "position_in_chain"                                          AS int_position_in_chain,
        "chain_length"                                               AS int_chain_length,
        "position_weight"                                            AS dec_position_weight,
        "is_keeper"                                                  AS bool_is_keeper,
        "has_error_leading_to_goal"                                  AS bool_has_error_leading_to_goal,
        "threat_conceded"                                            AS dec_threat_conceded,
        "threat_conceded_weighted"                                   AS dec_threat_conceded_weighted
    FROM mdl_body
)

SELECT * FROM mdl_out
