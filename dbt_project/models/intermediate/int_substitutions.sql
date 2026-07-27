{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'off_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_substitutions'
    )
}}

-- Changements au grain « un remplacement » : le joueur qui SORT apparié à celui
-- qui ENTRE, avec le timing et le rang du changement dans le match.
-- Base de features gestion tactique en gold (nb changements, minutes, fraîcheur).
--
-- WhoScored logue SubstitutionOff (type 18) et SubstitutionOn (type 19), même
-- équipe, même (minute, seconde), lignes adjacentes. `related_player_id` ne lie la
-- paire que dans 27 % des cas → on apparie par RANG au sein du groupe
-- (match, équipe, minute, seconde), ce qui gère les doubles/triples changements
-- simultanés. Croisé avec related_player_id là où il existe : concordance 99,8 %.
--
-- Comptes de référence (backfill partiel, validés read-only) :
--   81 561 changements, entrant rattaché 99,9 % (les rares sans entrant = sorties
--   sans remplaçant, ex. carton rouge), 0 doublon après dédup.

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

-- ── Sorties (type 18), rang au sein du groupe simultané ───────────────────────
offs AS (
    SELECT
        match_id,
        season,
        league_source,
        scraped_at,
        team_id,
        row_num            AS off_row_num,
        player_id          AS player_off,
        related_player_id,
        expanded_minute,
        second,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, team_id, expanded_minute, second
            ORDER BY row_num
        ) AS pair_rk
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 18
      AND match_id IN (SELECT match_id FROM new_matches)
    -- dédup des lignes répétées de la base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, row_num ORDER BY event_id) = 1
),

-- ── Entrées (type 19), même rang ──────────────────────────────────────────────
ons AS (
    SELECT
        match_id,
        team_id,
        expanded_minute,
        second,
        player_id          AS player_on,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, team_id, expanded_minute, second
            ORDER BY row_num
        ) AS pair_rk
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 19
      AND match_id IN (SELECT match_id FROM new_matches)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, row_num ORDER BY event_id) = 1
)

-- ── ASSEMBLAGE ────────────────────────────────────────────────────────────────
SELECT
    o.match_id,
    o.off_row_num,                               -- (match_id, off_row_num) = clé unique
    o.season,
    o.league_source,
    o.scraped_at,
    o.team_id,
    o.expanded_minute,
    o.player_off,
    n.player_on,                                 -- NULL si sortie sans remplaçant

    -- k-ième changement de l'équipe dans le match
    ROW_NUMBER() OVER (
        PARTITION BY o.match_id, o.team_id
        ORDER BY o.expanded_minute, o.second, o.off_row_num
    ) AS sub_number

FROM offs o
LEFT JOIN ons n
    ON  n.match_id        = o.match_id
    AND n.team_id         = o.team_id
    AND n.expanded_minute = o.expanded_minute
    AND n.second          = o.second
    AND n.pair_rk         = o.pair_rk
