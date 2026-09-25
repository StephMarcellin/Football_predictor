{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_chain_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_xg_chain'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- event_values lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_event_values AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        dec_danger_position                                          AS "danger_position",
        dec_chance_creation                                          AS "chance_creation",
        dec_def_execution_quality                                    AS "def_execution_quality",
        dec_pressure_context                                         AS "pressure_context",
        dec_context_weight                                           AS "context_weight",
        dec_action_value                                             AS "action_value"
    FROM {{ ref('event_values') }}
),

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

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
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
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_SHOTS
-- Une ligne par chaîne contenant au moins un tir.
-- On prend le DERNIER tir de la chaîne comme tir terminal.
-- On récupère chance_creation depuis event_values comme proxy xG.
-- ══════════════════════════════════════════════════════════════════════════════
chain_shots AS (
    SELECT DISTINCT ON (pc.match_id, pc.chain_id)
        pc.match_id,
        pc.chain_id,
        pc.chain_number,
        pc.chain_team_id,
        pc.season,
        pc.league_source,
        pc.player_id        AS shooter_player_id,
        pc.event_id         AS shot_event_id,
        pc.expanded_minute  AS shot_minute,
        pc.type_id          AS shot_type_id,
        ev.chance_creation  AS xg_proxy,
        
    FROM in_player_possession_chains pc
    JOIN in_event_values ev
        ON  ev.match_id = pc.match_id
        AND ev.row_num  = pc.row_num
    
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.is_shot   = TRUE
      AND pc.team_id   = pc.chain_team_id
      AND pc.type_id   IN (13, 14, 15, 16)
    ORDER BY pc.match_id, pc.chain_id, pc.expanded_minute DESC, pc.second DESC
),

-- ══════════════════════════════════════════════════════════════════════════════
-- ASSISTER_EVENTS
-- Récupère l'event_id du passeur décisif via RelatedEventId (qual 55) sur le tir.
-- ══════════════════════════════════════════════════════════════════════════════
assister_events AS (
    SELECT
        eq.match_id,
        cs.chain_id,
        CAST(eq.qual_value AS BIGINT) AS assister_event_id
    FROM in_events_qual eq
    JOIN chain_shots cs
        ON  cs.match_id      = eq.match_id
        AND cs.shot_event_id = eq.event_id
    WHERE eq.qual_type_id = 55
      AND eq.match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- ASSISTER_PLAYERS
-- Résout l'event_id du passeur décisif en player_id.
-- On joint player_possession_chains pour trouver quel joueur a réalisé
-- l'action dont l'event_id correspond à assister_event_id.
-- ══════════════════════════════════════════════════════════════════════════════
assister_players AS (
    SELECT
        ae.match_id,
        ae.chain_id,
        pc.player_id AS assister_player_id
    FROM assister_events ae
    JOIN in_player_possession_chains pc
        ON  pc.match_id = ae.match_id
        AND pc.chain_id = ae.chain_id
        AND pc.event_id = ae.assister_event_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- COUNTER_ATTACK_CHAINS
-- Identifie les chaînes contenant au moins un tir avec qual 23 (FastBreak).
-- is_counter_attack est une propriété de la chaîne entière, pas du tir terminal.
-- ══════════════════════════════════════════════════════════════════════════════
counter_attack_chains AS (
    SELECT DISTINCT
        pc.match_id,
        pc.chain_id
    FROM in_player_possession_chains pc
    JOIN in_events_qual eq_fb
        ON  eq_fb.match_id     = pc.match_id
        AND eq_fb.row_num      = pc.row_num
        AND eq_fb.qual_type_id = 23
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.is_shot = TRUE
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_PLAYERS_RAW
-- Tous les joueurs de chain_team_id dans chaque chaîne contenant un tir.
-- Calcul de la position chronologique et de la taille de la chaîne.
-- ══════════════════════════════════════════════════════════════════════════════
chain_players_raw AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_team_id,
        pc.player_id,
        pc.row_num,
        pc.expanded_minute,
        pc.second,
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
      AND pc.team_id   = pc.chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_PLAYERS
-- Déduplique par (match_id, chain_id, player_id) :
-- si un joueur apparaît plusieurs fois dans la chaîne,
-- on garde sa DERNIÈRE apparition (la plus proche du tir).
-- Calcul du position_weight ici.
-- ══════════════════════════════════════════════════════════════════════════════
chain_players AS (
    SELECT DISTINCT ON (match_id, chain_id, player_id)
        match_id,
        chain_id,
        chain_team_id,
        player_id,
        position_in_chain,
        chain_length,
        ROUND(
            CAST(position_in_chain AS DOUBLE) / chain_length,
            4
        ) AS position_weight
    FROM chain_players_raw
    ORDER BY match_id, chain_id, player_id, position_in_chain DESC
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_PLAYERS_FLAGGED
-- Join avec chain_shots et assister_players.
-- Calcul de tous les flags et métriques ici —
-- le SELECT final ne fait que lire cette CTE.
--
-- xgchain          : crédit uniforme = xg_proxy du tir terminal
-- xgchain_weighted : crédit pondéré par position dans la chaîne
-- xgbuildup        : xgchain pour les joueurs hors tireur/passeur décisif
-- xgbuildup_weighted : xgchain_weighted pour les joueurs hors tireur/passeur décisif
-- ══════════════════════════════════════════════════════════════════════════════
chain_players_flagged AS (
    SELECT
        cp.match_id,
        cp.chain_id,
        cs.chain_number,
        cp.chain_team_id,
        cp.player_id,
        cs.season,
        cs.league_source,
        cs.shot_event_id,
        cs.shot_type_id,
        cs.shot_minute,
        cs.xg_proxy,
        CASE
            WHEN cac.chain_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                         AS is_counter_attack,
        cp.position_in_chain,
        cp.chain_length,
        cp.position_weight,

        -- Flag tireur
        CASE
            WHEN cp.player_id = cs.shooter_player_id THEN TRUE
            ELSE FALSE
        END AS is_shooter,

        -- Flag passeur décisif
        CASE
            WHEN cp.player_id = ap.assister_player_id THEN TRUE
            ELSE FALSE
        END AS is_assister,

        -- xGChain : crédit uniforme
        cs.xg_proxy AS xgchain,

        -- xGChain pondéré par proximité au tir
        ROUND(cs.xg_proxy * cp.position_weight, 4) AS xgchain_weighted,

        -- xGBuildup : NULL pour tireur et passeur décisif
        CASE
            WHEN cp.player_id = cs.shooter_player_id  THEN NULL
            WHEN cp.player_id = ap.assister_player_id THEN NULL
            ELSE cs.xg_proxy
        END AS xgbuildup,

        -- xGBuildup pondéré : NULL pour tireur et passeur décisif
        CASE
            WHEN cp.player_id = cs.shooter_player_id  THEN NULL
            WHEN cp.player_id = ap.assister_player_id THEN NULL
            ELSE ROUND(cs.xg_proxy * cp.position_weight, 4)
        END AS xgbuildup_weighted

    FROM chain_players cp

    JOIN chain_shots cs
        ON  cs.match_id = cp.match_id
        AND cs.chain_id = cp.chain_id

    -- LEFT JOIN : toutes les chaînes n'ont pas de passeur décisif identifié
    -- (tir sans assist, or sans RelatedEventId dans les données)
    LEFT JOIN assister_players ap
        ON  ap.match_id = cp.match_id
        AND ap.chain_id = cp.chain_id

    LEFT JOIN counter_attack_chains cac
        ON  cac.match_id = cp.match_id
        AND cac.chain_id = cp.chain_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL — lecture directe de chain_players_flagged
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM chain_players_flagged
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "chain_id"                                                   AS str_chain_id,
        CAST(chain_number AS BIGINT)                                 AS int_chain_number,
        CAST(chain_team_id AS VARCHAR)                               AS str_chain_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        CAST(shot_event_id AS VARCHAR)                               AS str_shot_event_id,
        CAST(shot_type_id AS VARCHAR)                                AS str_shot_type_id,
        "shot_minute"                                                AS int_shot_minute,
        "xg_proxy"                                                   AS dec_xg_proxy,
        "is_counter_attack"                                          AS bool_is_counter_attack,
        "position_in_chain"                                          AS int_position_in_chain,
        "chain_length"                                               AS int_chain_length,
        "position_weight"                                            AS dec_position_weight,
        "is_shooter"                                                 AS bool_is_shooter,
        "is_assister"                                                AS bool_is_assister,
        "xgchain"                                                    AS dec_xgchain,
        "xgchain_weighted"                                           AS dec_xgchain_weighted,
        "xgbuildup"                                                  AS dec_xgbuildup,
        "xgbuildup_weighted"                                         AS dec_xgbuildup_weighted
    FROM mdl_body
)

SELECT * FROM mdl_out
