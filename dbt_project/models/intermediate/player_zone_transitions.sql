{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'zone_from', 'zone_to',
                    'period', 'score_state', 'formation'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_zone_transitions'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

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
-- FORMATION_ANCHORS
-- FormationSet (type_id=34) assigné à anchor_row=1 pour couvrir
-- tout le match dès le début.
-- FormationChange (type_id=40) gardé à son row_num réel.
-- ══════════════════════════════════════════════════════════════════════════════
formation_anchors AS (
    SELECT
        match_id,
        team_id,
        1                               AS anchor_row,
        qual_value                      AS formation_code
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id = 130
      AND type_id = 34
      AND match_id IN (SELECT match_id FROM new_matches)

    UNION ALL

    SELECT
        match_id,
        team_id,
        row_num                         AS anchor_row,
        qual_value                      AS formation_code
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id = 130
      AND type_id = 40
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FORMATION_LAST
-- Pour chaque événement, on cherche le row_num de la dernière ancre
-- dont anchor_row <= row_num courant — forward fill via MAX.
-- ══════════════════════════════════════════════════════════════════════════════
formation_last AS (
    SELECT
        ie.match_id,
        ie.team_id,
        ie.row_num,
        MAX(fa.anchor_row)              AS last_anchor_row
    FROM {{ ref('int_event_enriched') }} ie
    LEFT JOIN formation_anchors fa
        ON  fa.match_id   = ie.match_id
        AND fa.team_id    = ie.team_id
        AND fa.anchor_row <= ie.row_num
    WHERE ie.match_id IN (SELECT match_id FROM new_matches)
    GROUP BY ie.match_id, ie.team_id, ie.row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FORMATION_INTERVALS
-- Résout la formation active pour chaque row_num via le last_anchor_row.
-- ══════════════════════════════════════════════════════════════════════════════
formation_intervals AS (
    SELECT
        fl.match_id,
        fl.team_id,
        fl.row_num,
        fa.formation_code
    FROM formation_last fl
    JOIN formation_anchors fa
        ON  fa.match_id   = fl.match_id
        AND fa.team_id    = fl.team_id
        AND fa.anchor_row = fl.last_anchor_row
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_ENRICHED
-- Passes réussies depuis player_passes_raw avec player_id,
-- period, score_state, formation active.
-- ══════════════════════════════════════════════════════════════════════════════
passes_enriched AS (
    SELECT
        pr.match_id,
        pr.team_id,
        pr.passer_id                    AS player_id,
        pr.season,
        pr.league_source,
        ie.period,
        CASE
            WHEN ie.team_score > ie.opp_score THEN 'winning'
            WHEN ie.team_score < ie.opp_score THEN 'losing'
            ELSE                                   'drawing'
        END                             AS score_state,
        fi.formation_code               AS formation,
        pr.x,
        pr.y,
        pr.end_x,
        pr.end_y
    FROM {{ ref('player_passes_raw') }} pr
    JOIN {{ ref('int_event_enriched') }} ie
        ON  ie.match_id = pr.match_id
        AND ie.row_num  = pr.row_num
    LEFT JOIN formation_intervals fi
        ON  fi.match_id = pr.match_id
        AND fi.team_id  = pr.team_id
        AND fi.row_num  = pr.row_num
    WHERE pr.match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TAKEONS_RAW
-- TakeOns réussis avec end_x/end_y via LEAD sur player_id.
-- ══════════════════════════════════════════════════════════════════════════════
takeons_raw AS (
    SELECT
        match_id,
        team_id,
        player_id,
        row_num,
        period,
        season,
        league_source,
        team_score,
        opp_score,
        x,
        y,
        LEAD(x) OVER (
            PARTITION BY match_id, player_id
            ORDER BY row_num
        )                               AS end_x,
        LEAD(y) OVER (
            PARTITION BY match_id, player_id
            ORDER BY row_num
        )                               AS end_y
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id    = 3
      AND outcome_id = 1
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TAKEONS_ENRICHED
-- TakeOns avec end_x/end_y valides + formation active.
-- ══════════════════════════════════════════════════════════════════════════════
takeons_enriched AS (
    SELECT
        tr.match_id,
        tr.team_id,
        tr.player_id,
        tr.season,
        tr.league_source,
        tr.period,
        CASE
            WHEN tr.team_score > tr.opp_score THEN 'winning'
            WHEN tr.team_score < tr.opp_score THEN 'losing'
            ELSE                                   'drawing'
        END                             AS score_state,
        fi.formation_code               AS formation,
        tr.x,
        tr.y,
        tr.end_x,
        tr.end_y
    FROM takeons_raw tr
    LEFT JOIN formation_intervals fi
        ON  fi.match_id = tr.match_id
        AND fi.team_id  = tr.team_id
        AND fi.row_num  = tr.row_num
    WHERE tr.end_x IS NOT NULL
      AND tr.end_y IS NOT NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TRANSITIONS_RAW
-- UNION ALL passes + takeons avec calcul des zones.
-- ══════════════════════════════════════════════════════════════════════════════
transitions_raw AS (
    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        CASE
            WHEN y < 33.3 THEN 'A'
            WHEN y < 66.6 THEN 'B'
            ELSE               'C'
        END ||
        CASE
            WHEN x >= 80  THEN '1'
            WHEN x >= 60  THEN '2'
            WHEN x >= 40  THEN '3'
            WHEN x >= 20  THEN '4'
            ELSE               '5'
        END                             AS zone_from,
        CASE
            WHEN end_y < 33.3 THEN 'A'
            WHEN end_y < 66.6 THEN 'B'
            ELSE                   'C'
        END ||
        CASE
            WHEN end_x >= 80  THEN '1'
            WHEN end_x >= 60  THEN '2'
            WHEN end_x >= 40  THEN '3'
            WHEN end_x >= 20  THEN '4'
            ELSE                   '5'
        END                             AS zone_to,
        CASE WHEN end_x > x + 10 THEN 1 ELSE 0 END AS is_progressive
    FROM passes_enriched

    UNION ALL

    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        CASE
            WHEN y < 33.3 THEN 'A'
            WHEN y < 66.6 THEN 'B'
            ELSE               'C'
        END ||
        CASE
            WHEN x >= 80  THEN '1'
            WHEN x >= 60  THEN '2'
            WHEN x >= 40  THEN '3'
            WHEN x >= 20  THEN '4'
            ELSE               '5'
        END                             AS zone_from,
        CASE
            WHEN end_y < 33.3 THEN 'A'
            WHEN end_y < 66.6 THEN 'B'
            ELSE                   'C'
        END ||
        CASE
            WHEN end_x >= 80  THEN '1'
            WHEN end_x >= 60  THEN '2'
            WHEN end_x >= 40  THEN '3'
            WHEN end_x >= 20  THEN '4'
            ELSE                   '5'
        END                             AS zone_to,
        CASE WHEN end_x > x + 10 THEN 1 ELSE 0 END AS is_progressive
    FROM takeons_enriched
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TRANSITIONS_AGG
-- Agrégation par (match_id, team_id, player_id, zone_from, zone_to,
--                 period, score_state, formation).
-- ══════════════════════════════════════════════════════════════════════════════
transitions_agg AS (
    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        zone_from,
        zone_to,
        COUNT(*)                                        AS n_transitions,
        ROUND(
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (
                PARTITION BY match_id, team_id, player_id,
                             period, score_state, formation
            ), 4
        )                                               AS pct_transitions,
        ROUND(
            SUM(is_progressive) * 1.0 / COUNT(*), 4
        )                                               AS progressive_rate
    FROM transitions_raw
    GROUP BY
        match_id, team_id, player_id, season, league_source,
        period, score_state, formation,
        zone_from, zone_to
)

SELECT * FROM transitions_agg