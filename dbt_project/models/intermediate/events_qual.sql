{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num', 'qual_type_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='events_qual'
    )
}}

-- depends_on: {{ ref('int_whoscored_match_index') }}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

SELECT
    e.match_id, e.team_id, e.player_id, e.event_id, e.minute, e.second,
    e.expanded_minute, e.period, e.x, e.y, e.end_x, e.end_y,
    e.type_id, e.type_name, e.outcome_id, e.is_touch, e.is_shot,
    TRY_CAST(json_extract_string(q.qual, '$.type.value') AS INTEGER) AS qual_type_id,
    json_extract_string(q.qual, '$.type.displayName')                AS qual_type_name,
    json_extract_string(q.qual, '$.value')                     AS qual_value
FROM {{ ref('int_whoscored_events') }} e,
LATERAL (
    SELECT unnest(json_extract(e.qualifiers_json, '$[*]')::JSON[]) AS qual
) q
WHERE e.qualifiers_json IS NOT NULL
  AND e.qualifiers_json != '[]'

{% if is_incremental() %}
  AND e.match_id IN (
      SELECT match_id
      FROM {{ ref('int_whoscored_match_index') }}
      WHERE match_id NOT IN (
          SELECT DISTINCT match_id FROM {{ this }}
      )
  )
{% endif %}