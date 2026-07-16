{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='freekick_profiles'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
),
{% endif %}

freekick_qual_flags AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 5   THEN 1 ELSE 0 END) AS is_freekick_pass,
        MAX(CASE WHEN qual_type_id = 26  THEN 1 ELSE 0 END) AS is_freekick_shot,
        MAX(CASE WHEN qual_type_id = 241 THEN 1 ELSE 0 END) AS is_indirect_freekick
    FROM {{ ref('events_qual') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND qual_type_id IN (5, 26, 241)
    GROUP BY match_id, row_num
),

freekick_direct_anchor AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_number,
        pc.chain_team_id,
        pc.player_id      AS freekick_taker_id,
        pc.row_num,
        pc.event_id,
        pc.expanded_minute,
        pc.second,
        pc.type_id,
        pc.outcome_id,
        pc.x,
        pc.y,
        'direct'   AS fk_type,
        FALSE      AS is_offside,
        ev.chance_creation AS xg_generated,
        CASE
            WHEN pc.type_id = 16          THEN 'goal'
            WHEN pc.type_id = 15          THEN 'shot_saved'
            WHEN pc.type_id IN (13, 14)   THEN 'shot_off_target'
        END AS outcome
    FROM {{ ref('player_possession_chains') }} pc
    JOIN freekick_qual_flags fqf
        ON  fqf.match_id = pc.match_id
        AND fqf.row_num  = pc.row_num
    LEFT JOIN {{ ref('event_values') }} ev
        ON  ev.match_id = pc.match_id
        AND ev.row_num  = pc.row_num
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_trigger    = 'free_kick'
      AND pc.type_id          IN (13, 14, 15, 16)
      AND fqf.is_freekick_shot = 1
),

freekick_pass_anchor  AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_number,
        pc.chain_team_id,
        pc.player_id      AS freekick_taker_id,
        pc.row_num,
        pc.event_id,
        pc.expanded_minute,
        pc.second,
        pc.type_id,
        pc.outcome_id,
        pc.x,
        pc.y,
        CASE WHEN fqf.is_indirect_freekick = 1 THEN 'indirect' ELSE 'short_pass' END AS fk_type,
        CASE WHEN pc.type_id = 2 THEN TRUE ELSE FALSE END AS is_offside

    FROM {{ ref('player_possession_chains') }} pc
    JOIN freekick_qual_flags fqf
        ON  fqf.match_id = pc.match_id
        AND fqf.row_num  = pc.row_num
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_trigger    = 'free_kick'
      AND pc.type_id          IN (1, 2)
      AND fqf.is_freekick_pass = 1
),

freekick_pass_outcome AS (
    SELECT DISTINCT ON (pc.match_id, pc.chain_id)
        pc.match_id,
        pc.chain_id,
        pc.row_num        AS shot_row_num,
        pc.event_id       AS shot_event_id,
        pc.type_id        AS shot_type_id,
        pc.outcome_id     AS shot_outcome_id,
        pc.expanded_minute AS shot_minute
    FROM {{ ref('player_possession_chains') }} pc
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_id IN (SELECT chain_id FROM freekick_pass_anchor)
      AND pc.is_shot   = TRUE
      AND pc.team_id   = pc.chain_team_id
    ORDER BY pc.match_id, pc.chain_id, pc.expanded_minute DESC, pc.second DESC
),

last_team_action AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (
            PARTITION BY pc.chain_id
            ORDER BY pc.expanded_minute DESC, pc.second DESC, pc.row_num DESC
        ) AS rn_last
    FROM {{ ref('player_possession_chains') }} pc
    WHERE pc.chain_id IN (SELECT chain_id FROM freekick_pass_anchor)
      AND pc.team_id = pc.chain_team_id
),

freekick_pass_enriched AS (
    SELECT
        fpa.match_id,
        fpa.chain_id,
        fpa.chain_number,
        fpa.chain_team_id,
        fpa.freekick_taker_id,
        fpa.row_num,
        fpa.event_id,
        fpa.expanded_minute,
        fpa.second,
        fpa.type_id,
        fpa.outcome_id,
        fpa.x,
        fpa.y,
        fpa.fk_type,
        fpa.is_offside,
        ev.chance_creation AS xg_generated,
        CASE
            WHEN fpa.is_offside = TRUE                              THEN 'offside'
            WHEN fpo.shot_type_id = 16                              THEN 'goal'
            WHEN fpo.shot_type_id = 15                              THEN 'shot_saved'
            WHEN fpo.shot_type_id IN (13, 14)                       THEN 'shot_off_target'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id = 4
                 AND lta.outcome_id = 1                             THEN 'foul_won'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id = 6
                 AND lta.outcome_id = 1                             THEN 'corner_won'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id IN (1, 3, 61)
                 AND lta.outcome_id = 1                             THEN 'open_play'
            ELSE 'turnover'
        END AS outcome

    FROM freekick_pass_anchor fpa

    LEFT JOIN freekick_pass_outcome fpo
        ON  fpo.match_id = fpa.match_id
        AND fpo.chain_id = fpa.chain_id

    LEFT JOIN last_team_action lta
        ON  lta.chain_id = fpa.chain_id
        AND lta.rn_last  = 1

    LEFT JOIN {{ ref('event_values') }} ev
        ON  ev.match_id = fpa.match_id
        AND ev.row_num   = fpo.shot_row_num
),

freekick_anchors AS (
    SELECT * FROM freekick_direct_anchor
    UNION ALL
    SELECT * FROM freekick_pass_enriched
),

freekick_geometry AS (
    SELECT
        *,
        x * 1.05 AS x_m,
        y * 0.68 AS y_m
    FROM freekick_anchors
),

freekick_geometry_distances AS (
    SELECT
        *,
        SQRT(POWER(105 - x_m, 2) + POWER(34 - y_m, 2))          AS distance_to_goal,
        SQRT(POWER(105 - x_m, 2) + POWER(30.34 - y_m, 2))       AS dist_to_post1,
        SQRT(POWER(105 - x_m, 2) + POWER(37.66 - y_m, 2))       AS dist_to_post2,
        (105 - x_m) * (105 - x_m)
            + (30.34 - y_m) * (37.66 - y_m)                     AS dot_product
    FROM freekick_geometry
)

SELECT
    * EXCLUDE (dist_to_post1, dist_to_post2, dot_product),
    ACOS(
        GREATEST(-1.0, LEAST(1.0,
            dot_product / (dist_to_post1 * dist_to_post2)
        ))
    ) AS angle
FROM freekick_geometry_distances
