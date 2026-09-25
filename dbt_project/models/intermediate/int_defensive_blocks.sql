{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_defensive_blocks'
    )
}}

-- Contres défensifs au grain « un contre », AVEC le défenseur qui bloque.
-- WhoScored crédite le bloqueur via un event dédié type 74 « BlockedPass »
-- (171 020, player_id toujours renseigné). Ce sont des contres de PASSE : le
-- défenseur coupe une ligne de passe adverse. (Les contres de TIR, eux, n'ont pas
-- de bloqueur nommé chez Opta — seulement le qual 82 sur le tir ; ils restent
-- hors de ce modèle, niveau équipe uniquement.)
--
-- Appariement bloqueur ↔ passe bloquée : le type 74 pointe vers l'event bloqué via
-- le qual 233 « OppositeRelatedEvent » (99,4 %).
--
-- PIÈGE event_id : event_id n'est PAS unique par match — scopé par (match, équipe)
-- (66,8 % existent pour les 2 équipes). Le join sur event_id DOIT contraindre
-- l'équipe adverse, sinon on rattache le mauvais côté (~42 % de faux même-équipe).
--
-- Comptes de référence (backfill partiel, validés read-only) :
--   171 002 contres, 0 doublon, bloqueur 100 %, passe bloquée rattachée 99,4 %,
--   0 faux même-équipe, 98 % de passes.

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

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

mdl_body AS (
WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            int_row_num                                                  AS "row_num",
            CAST(str_event_id AS INTEGER)                                AS "event_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
            int_expanded_minute                                          AS "expanded_minute",
            CAST(str_blocking_team_id AS BIGINT)                         AS "blocking_team_id",
            CAST(str_blocker_player_id AS INTEGER)                       AS "blocker_player_id",
            CAST(str_blocked_team_id AS BIGINT)                          AS "blocked_team_id",
            CAST(str_blocked_player_id AS INTEGER)                       AS "blocked_player_id",
            str_blocked_type                                             AS "blocked_type",
            dec_block_x                                                  AS "block_x",
            dec_block_y                                                  AS "block_y"
        FROM {{ this }}
    )
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_whoscored_events
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_int_whoscored_events
),
{% endif %}

-- ── Pointeur qual 233 vers l'event bloqué ─────────────────────────────────────
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS opposite_event_id
    FROM in_events_qual
    WHERE qual_type_id = 233
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── Le contre (pivot) : event type 74, le défenseur qui bloque ────────────────
-- Dédoublonné par (match, row_num) — la base a des lignes répétées.
blocks AS (
    SELECT
        e.match_id,
        e.event_id,
        e.row_num,
        e.season,
        e.league_source,
        e.scraped_at,
        e.expanded_minute,
        e.team_id                AS blocking_team_id,
        e.player_id              AS blocker_player_id,   -- toujours renseigné
        e.x                      AS block_x,
        e.y                      AS block_y,
        q.opposite_event_id
    FROM in_int_whoscored_events e
    LEFT JOIN q233 q
        ON q.match_id = e.match_id
       AND q.row_num  = e.row_num
    WHERE e.type_id = 74
      AND e.match_id IN (SELECT match_id FROM new_matches)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num
                               ORDER BY e.event_id) = 1
),

-- ── L'event bloqué → passeur adverse ──────────────────────────────────────────
-- Dédoublonné par (match, event_id, team_id) : event_id étant scopé par équipe,
-- team_id fait partie de la clé, et le join contraindra l'équipe adverse.
blocked_ev AS (
    SELECT
        match_id,
        event_id,
        team_id      AS blocked_team_id,
        player_id    AS blocked_player_id,
        type_name    AS blocked_type
    FROM in_int_whoscored_events
    WHERE match_id IN (SELECT match_id FROM new_matches)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, event_id, team_id
                               ORDER BY row_num) = 1
)

-- ── ASSEMBLAGE ────────────────────────────────────────────────────────────────
SELECT
    b.match_id,
    b.row_num,                                   -- (match_id, row_num) = clé unique
    b.event_id,
    b.season,
    b.league_source,
    b.scraped_at,
    b.expanded_minute,

    b.blocking_team_id,
    b.blocker_player_id,                         -- le défenseur qui bloque

    be.blocked_team_id,
    be.blocked_player_id,                        -- le joueur dont la passe est bloquée
    be.blocked_type,                             -- ce qui a été bloqué (surtout Pass)

    b.block_x,
    b.block_y

FROM blocks b
LEFT JOIN blocked_ev be
    ON  be.match_id       = b.match_id
    AND be.event_id       = b.opposite_event_id
    AND be.blocked_team_id <> b.blocking_team_id   -- event_id scopé par équipe
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        "expanded_minute"                                            AS int_expanded_minute,
        CAST(blocking_team_id AS VARCHAR)                            AS str_blocking_team_id,
        CAST(blocker_player_id AS VARCHAR)                           AS str_blocker_player_id,
        CAST(blocked_team_id AS VARCHAR)                             AS str_blocked_team_id,
        CAST(blocked_player_id AS VARCHAR)                           AS str_blocked_player_id,
        "blocked_type"                                               AS str_blocked_type,
        "block_x"                                                    AS dec_block_x,
        "block_y"                                                    AS dec_block_y
    FROM mdl_body
)

SELECT * FROM mdl_out
