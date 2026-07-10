{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'event_id_a'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_network_duels'
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
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_event_enriched') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- DUEL_LINKS
-- Point de départ : on filtre events_qual sur qual_type_id = 233
-- pour n'avoir que les événements qui ont un miroir adverse.
-- On joint int_event_enriched sur (match_id, row_num) pour enrichir le côté A
-- avec team_id, player_id, type_id, outcome_id, x, y.
-- ══════════════════════════════════════════════════════════════════════════════
duel_links AS (
    SELECT
        eq.match_id,
        eq.row_num,
        CAST(eq.qual_value AS BIGINT)   AS opposite_event_id,
        ie.team_id,
        ie.player_id,
        ie.event_id,
        ie.type_id,
        ie.type_name,
        ie.outcome_id,
        ie.x,
        ie.y,
        ie.expanded_minute,
        ie.second,
        ie.season,
        ie.league_source
    FROM {{ ref('events_qual') }} eq
    JOIN {{ ref('int_event_enriched') }} ie
        ON  ie.match_id = eq.match_id
        AND ie.row_num  = eq.row_num
    WHERE eq.match_id    IN (SELECT match_id FROM new_matches)
      AND eq.qual_type_id = 233
      AND ie.type_id      IN (3, 4, 7, 44, 45, 50)
      AND ie.player_id    IS NOT NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DUELS_PAIRED
-- On joint duel_links sur lui-même pour ramener le côté B.
-- La jointure côté B se fait sur (match_id, team_id != team_id_a, event_id = opposite_event_id)
-- pour contraindre explicitement vers l'équipe adverse — évite l'ambiguïté
-- quand opposite_event_id existe pour les deux équipes.
-- Le filtre dl_a.row_num < dl_b.row_num déduplique : chaque duel
-- est présent deux fois dans duel_links, on n'en garde qu'une.
-- ══════════════════════════════════════════════════════════════════════════════
duels_paired AS (
    SELECT
        dl_a.match_id,
        dl_a.season,
        dl_a.league_source,
        dl_a.expanded_minute,
        dl_a.second,
        dl_a.type_id                    AS duel_type_id,
        dl_a.type_name                  AS duel_type,
        dl_a.x,
        dl_a.y,

        -- Côté A
        dl_a.event_id                   AS event_id_a,
        dl_a.team_id                    AS team_id_a,
        dl_a.player_id                  AS player_id_a,
        dl_a.outcome_id                 AS outcome_a,

        -- Côté B
        dl_b.event_id                   AS event_id_b,
        dl_b.team_id                    AS team_id_b,
        dl_b.player_id                  AS player_id_b,
        dl_b.outcome_id                 AS outcome_b

    FROM duel_links dl_a
    JOIN duel_links dl_b
        ON  dl_b.match_id  = dl_a.match_id
        AND dl_b.event_id  = dl_a.opposite_event_id
        AND dl_b.team_id  != dl_a.team_id

    WHERE dl_a.row_num < dl_b.row_num
)

SELECT * FROM duels_paired