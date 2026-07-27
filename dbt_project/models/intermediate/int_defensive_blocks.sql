{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
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

WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM {{ this }}
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_whoscored_events') }}
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM {{ ref('int_whoscored_events') }}
),
{% endif %}

-- ── Pointeur qual 233 vers l'event bloqué ─────────────────────────────────────
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS opposite_event_id
    FROM {{ ref('events_qual') }}
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
    FROM {{ ref('int_whoscored_events') }} e
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
    FROM {{ ref('int_whoscored_events') }}
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
