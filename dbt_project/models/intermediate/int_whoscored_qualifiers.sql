{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num', 'qual_type_id'],
        incremental_strategy='delete+insert',
        schema='intermediate',
    )
}}

WITH events_with_season AS (
    SELECT
        e.*,
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
    ews.row_num,
    ews.season,
    TRY_CAST(json_extract_string(q.qual, '$.type.value') AS INTEGER) AS qual_type_id,
    json_extract_string(q.qual, '$.type.displayName') AS qual_type_name,
    json_extract_string(q.qual, '$.value') AS qual_value
FROM events_with_season ews,
LATERAL (
    SELECT unnest(json_extract(ews.qualifiers_json, '$[*]')::JSON[]) AS qual
) q
WHERE ews.qualifiers_json IS NOT NULL
  AND ews.qualifiers_json != '[]'
  AND ews.season = '{{ var("target_season", "2024-2025") }}'

{% if is_incremental() %}
  AND ews.season NOT IN (
      SELECT DISTINCT season FROM {{ this }}
  )
{% endif %}