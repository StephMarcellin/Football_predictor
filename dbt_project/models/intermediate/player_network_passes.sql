{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'passer_id', 'receiver_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_network_passes'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_passes_raw') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_passes_raw') }}
),
{% endif %}

network AS (
    SELECT
        match_id,
        team_id,
        passer_id,
        receiver_id,
        season,
        league_source,

        COUNT(*)                                                                 AS n_passes,
        COUNT(*) FILTER (WHERE chain_trigger = 'counter_attack')                AS n_passes_counter_attack,
        COUNT(*) FILTER (WHERE chain_trigger = 'open_play')                     AS n_passes_open_play,
        COUNT(*) FILTER (WHERE chain_trigger = 'recovery')                      AS n_passes_recovery,
        COUNT(*) FILTER (WHERE chain_trigger IN ('corner','free_kick',
                                                  'throw_in','goal_kick'))      AS n_passes_other,

        COUNT(*) FILTER (WHERE is_progressive)                                  AS n_progressive,
        COUNT(*) FILTER (WHERE is_creative)                                     AS n_creative,
        COUNT(*) FILTER (WHERE is_buildup)                                      AS n_buildup

    FROM {{ ref('player_passes_raw') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id, passer_id, receiver_id, season, league_source
)

SELECT * FROM network