{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_fouls_drawn'
    )
}}

-- Fautes subies (coups francs obtenus) au grain « une faute subie ».
-- Le pendant des fautes commises : qui OBTIENT les fautes. Signal ML utile —
-- subir des fautes dans le tiers offensif génère des coups francs dangereux et
-- casse le pressing adverse.
--
-- WhoScored logue chaque faute en DOUBLE (vérifié : 100 %, 265 802 de chaque côté) :
--   • outcome_id = 1 → côté qui SUBIT / obtient (porte le qual 286 « Offensive »)
--   • outcome_id = 0 → côté qui COMMET (le fautif)
-- Les deux faces sont reliées par le qual 233 « OppositeRelatedEvent » (déterministe,
-- 100 %) : on rattache ainsi le fautif à chaque faute subie.
--
-- Le joueur qui subit est NULL ~5 % du temps (limite source WhoScored) → on source
-- depuis les events BRUTS (pas l'enrichi, qui drope les player_id NULL).
-- `x` est relatif à l'équipe (0 = son but, 100 = but adverse) → tiers offensif = x > 66.7.
--
-- Comptes de référence (backfill partiel, validés read-only) :
--   265 782 fautes subies, 0 doublon, fautif rattaché 99,4 %, drawer 95 %,
--   19 % en tiers offensif, 3 471 menant à penalty.

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

-- ── Qualifiers utiles portés par la faute subie ───────────────────────────────
-- 233 : pointe vers l'event_id de la faute commise (pour rattacher le fautif)
-- 56  : zone grossière (Back/Center/Left/Right) — passthrough
-- 9   : présent si la faute donne un penalty
foul_quals AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 233 THEN TRY_CAST(qual_value AS INTEGER) END) AS opposite_event_id,
        MAX(CASE WHEN qual_type_id = 56  THEN qual_value END)                      AS foul_zone,
        MAX(CASE WHEN qual_type_id = 9   THEN 1 ELSE 0 END)                        AS is_penalty
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id IN (233, 56, 9)
      AND match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, row_num
),

-- ── Fautes commises → fautif, dédoublonnées par (match, event_id, team_id) ────
-- ATTENTION : event_id n'est PAS unique par match — il est scopé par (match,
-- équipe) (66,8 % des event_id existent pour les 2 équipes). Sans porter team_id
-- ET contraindre l'équipe adverse au join, on rattache le mauvais fautif ~0,6 %.
committed AS (
    SELECT
        match_id,
        event_id,
        team_id        AS committed_by_team_id,
        MAX(player_id) AS committed_by_player_id
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 4
      AND outcome_id = 0
      AND match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, event_id, team_id
),

-- ── Fautes subies (pivot), dédoublonnées par (match, row_num) ─────────────────
drawn AS (
    SELECT
        e.match_id,
        e.event_id,
        e.row_num,
        e.season,
        e.league_source,
        e.scraped_at,
        e.expanded_minute,
        e.team_id                       AS drawing_team_id,
        e.player_id                     AS drawer_player_id,   -- ~5 % NULL (source)
        e.x,
        e.y,
        fq.opposite_event_id,
        fq.foul_zone,
        COALESCE(fq.is_penalty, 0)      AS is_penalty
    FROM {{ ref('int_whoscored_events') }} e
    LEFT JOIN foul_quals fq
        ON fq.match_id = e.match_id
       AND fq.row_num  = e.row_num
    WHERE e.type_id = 4
      AND e.outcome_id = 1
      AND e.match_id IN (SELECT match_id FROM new_matches)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num
                               ORDER BY e.event_id) = 1
)

-- ── ASSEMBLAGE ────────────────────────────────────────────────────────────────
SELECT
    d.match_id,
    d.row_num,                                   -- (match_id, row_num) = clé unique
    d.event_id,
    d.season,
    d.league_source,
    d.scraped_at,
    d.expanded_minute,

    d.drawing_team_id,
    d.drawer_player_id,
    c.committed_by_player_id,                    -- le fautif (via qual 233)

    d.x,
    d.y,
    d.foul_zone,
    CASE WHEN d.x IS NOT NULL THEN d.x > 66.7 END  AS is_attacking_third,
    (d.is_penalty = 1)                             AS leads_to_penalty

FROM drawn d
LEFT JOIN committed c
    ON  c.match_id             = d.match_id
    AND c.event_id             = d.opposite_event_id
    AND c.committed_by_team_id <> d.drawing_team_id   -- event_id scopé par équipe
