{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num', 'qual_type_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='events_qual',
        incremental_strategy='delete+insert'
    )
}}

-- depends_on: {{ ref('int_whoscored_match_index') }}

WITH events_with_season AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,
        e.event_id,
        e.minute,
        e.second,
        e.expanded_minute,
        e.period,
        e.x,
        e.y,
        e.end_x,
        e.end_y,
        e.type_id,
        e.type_name,
        e.outcome_id,
        e.is_touch,
        e.is_shot,
        e.row_num,
        m.season
    FROM {{ ref('int_whoscored_events') }} e
    INNER JOIN {{ ref('int_whoscored_match_index') }} m
        ON e.match_id = m.match_id
)

SELECT
    ews.match_id,
    ews.team_id,
    ews.player_id,
    ews.event_id,
    ews.minute,
    ews.second,
    ews.expanded_minute,
    ews.period,
    ews.x,
    ews.y,
    ews.end_x,
    ews.end_y,
    ews.type_id,
    ews.type_name,
    ews.outcome_id,
    ews.is_touch,
    ews.is_shot,
    ews.row_num,
    ews.season,
    q.qual_type_id,
    q.qual_type_name,
    q.qual_value
FROM events_with_season ews
INNER JOIN {{ ref('int_whoscored_qualifiers') }} q
    ON ews.match_id = q.match_id
    AND ews.team_id = q.team_id
    AND ews.player_id = q.player_id
    AND ews.row_num = q.row_num
    AND ews.season = q.season

{% if is_incremental() %}
  AND ews.season = '{{ var("target_season", "2024-2025") }}'
  AND ews.season NOT IN (
      SELECT DISTINCT season FROM {{ this }}
  )
{% endif %}