{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_passes_raw'
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
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- TEAM_EVENTS
-- On filtre sur team_id = chain_team_id pour ne garder que les événements
-- de l'équipe en possession. Le LEAD est appliqué sur cette séquence épurée :
-- receiver_id pointe toujours vers le prochain joueur de la même équipe
-- dans la chaîne, sans sauter par-dessus des événements adverses.
-- ══════════════════════════════════════════════════════════════════════════════
team_events AS (
    SELECT
        match_id,
        chain_id,
        chain_trigger,
        chain_team_id,
        team_id,
        player_id                                                   AS passer_id,
        LEAD(player_id) OVER (
            PARTITION BY match_id, chain_id
            ORDER BY expanded_minute, second, row_num
        )                                                           AS receiver_id,
        row_num,
        expanded_minute,
        second,
        type_id,
        outcome_id,
        x,
        y,
        season,
        league_source
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND team_id = chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_RAW
-- Filtre sur les passes réussies.
-- JOIN sur int_event_enriched pour récupérer end_x, end_y, is_key_pass,
-- is_shot_assist — absents de player_possession_chains.
-- La jointure se fait sur (match_id, row_num), clé unique partagée.
-- ══════════════════════════════════════════════════════════════════════════════
passes_raw AS (
    SELECT
        te.match_id,
        te.chain_id,
        te.chain_trigger,
        te.chain_team_id                                            AS team_id,
        te.passer_id,
        te.receiver_id,
        te.row_num,
        te.expanded_minute,
        te.second,
        te.x,
        te.y,
        ie.end_x,
        ie.end_y,
        ie.is_key_pass,
        ie.is_shot_assist,
        te.season,
        te.league_source
    FROM team_events te
    JOIN {{ ref('int_event_enriched') }} ie
        ON  ie.match_id = te.match_id
        AND ie.row_num  = te.row_num
    WHERE te.type_id   = 1
      AND te.outcome_id = 1
      AND te.receiver_id IS NOT NULL
      AND te.receiver_id != te.passer_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_NETWORK
-- Calcul des 3 flags qualifiant la nature de la passe.
-- is_progressive : la passe fait avancer le ballon d'au moins 10 mètres
--                  vers le but adverse (end_x > x + 10)
-- is_creative    : la passe mène directement à un tir
--                  (is_key_pass=1 ou is_shot_assist=1)
-- is_buildup     : la passe est jouée dans la moitié défensive (x < 50)
-- ══════════════════════════════════════════════════════════════════════════════
passes_network AS (
    SELECT
        match_id,
        chain_id,
        chain_trigger,
        team_id,
        passer_id,
        receiver_id,
        row_num,
        expanded_minute,
        second,
        x,
        y,
        end_x,
        end_y,
        is_key_pass,
        is_shot_assist,
        season,
        league_source,

        -- Passe progressive : avance vers le but adverse
        CASE WHEN end_x > x + 10               THEN TRUE ELSE FALSE END AS is_progressive,

        -- Passe créative : mène directement à un tir
        CASE WHEN is_key_pass = 1
              OR is_shot_assist = 1             THEN TRUE ELSE FALSE END AS is_creative,

        -- Passe de construction : dans la moitié défensive
        CASE WHEN x < 50                        THEN TRUE ELSE FALSE END AS is_buildup

    FROM passes_raw
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM passes_network