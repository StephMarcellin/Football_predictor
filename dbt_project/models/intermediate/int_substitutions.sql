{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_off_row_num'],
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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

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
            int_off_row_num                                              AS "off_row_num",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            int_expanded_minute                                          AS "expanded_minute",
            int_player_off                                               AS "player_off",
            int_player_on                                                AS "player_on",
            int_sub_number                                               AS "sub_number"
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
    FROM in_int_whoscored_events
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
    FROM in_int_whoscored_events
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
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "off_row_num"                                                AS int_off_row_num,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "expanded_minute"                                            AS int_expanded_minute,
        "player_off"                                                 AS int_player_off,
        "player_on"                                                  AS int_player_on,
        "sub_number"                                                 AS int_sub_number
    FROM mdl_body
)

SELECT * FROM mdl_out
