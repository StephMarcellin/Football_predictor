{{
    config(
        materialized='incremental',
        unique_key=['ws_match_id', 'team_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_match_stats'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM {{ this }}
),
new_matches AS (
    SELECT DISTINCT ws_match_id
    FROM {{ source('silver', 'stg_whoscored_events') }}
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT ws_match_id
    FROM {{ source('silver', 'stg_whoscored_events') }}
),
{% endif %}

match_dates AS (
    SELECT ws_match_id, match_date, league_source, season, scraped_at
    FROM {{ source('silver', 'stg_whoscored_match_index') }}
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_matches)
),

player_agg AS (
    SELECT
        e.ws_match_id,
        e.team_id,
        e.player_id,
        COUNT(*)                                        AS n_actions,
        COUNT(*) FILTER (WHERE e.is_shot = TRUE)        AS n_shots,
        COUNT(DISTINCT e.row_num) FILTER (
            WHERE e.type_id = 1
              AND TRY_CAST(
                  json_extract_string(e.qualifiers_json, '$[0].type.value') AS INTEGER
              ) = 210
        )                                               AS n_key_passes,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.is_shot = TRUE) > 0
            THEN COUNT(*) FILTER (WHERE e.is_shot = TRUE)
                 * (1.0 / (1.0 + SQRT(
                     POW(100.0 - AVG(e.x) FILTER (WHERE e.is_shot = TRUE), 2)
                   + POW( 50.0 - AVG(e.y) FILTER (WHERE e.is_shot = TRUE), 2)
                 )))
            ELSE 0.0
        END                                             AS xg_contribution,
        AVG(e.x) FILTER (WHERE e.is_touch = TRUE)       AS zone_dominance
    FROM {{ source('silver', 'stg_whoscored_events') }} e
    WHERE e.player_id IS NOT NULL
      AND e.ws_match_id IN (SELECT ws_match_id FROM match_dates)
    GROUP BY e.ws_match_id, e.team_id, e.player_id
)

SELECT
    p.ws_match_id, p.team_id, p.player_id,
    d.match_date   AS date,
    d.season,
    d.league_source,
    d.scraped_at,
    p.n_actions, p.n_shots, p.n_key_passes,
    p.xg_contribution, p.zone_dominance
FROM player_agg p
JOIN match_dates d ON p.ws_match_id = d.ws_match_id