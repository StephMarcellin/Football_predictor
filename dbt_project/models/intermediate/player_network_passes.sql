{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_passer_id', 'str_receiver_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_network_passes'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- player_passes_raw lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_passes_raw AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        str_chain_trigger                                            AS "chain_trigger",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_passer_id AS INTEGER)                               AS "passer_id",
        CAST(str_receiver_id AS INTEGER)                             AS "receiver_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        bool_is_progressive                                          AS "is_progressive",
        bool_is_creative                                             AS "is_creative",
        bool_is_buildup                                              AS "is_buildup"
    FROM {{ ref('player_passes_raw') }}
),

mdl_body AS (
WITH

{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_passes_raw
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_passer_id AS INTEGER)                               AS "passer_id",
            CAST(str_receiver_id AS INTEGER)                             AS "receiver_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            int_n_passes                                                 AS "n_passes",
            int_n_passes_counter_attack                                  AS "n_passes_counter_attack",
            int_n_passes_open_play                                       AS "n_passes_open_play",
            int_n_passes_recovery                                        AS "n_passes_recovery",
            int_n_passes_other                                           AS "n_passes_other",
            int_n_progressive                                            AS "n_progressive",
            int_n_creative                                               AS "n_creative",
            int_n_buildup                                                AS "n_buildup"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_passes_raw
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

    FROM in_player_passes_raw
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id, passer_id, receiver_id, season, league_source
)

SELECT * FROM network
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(passer_id AS VARCHAR)                                   AS str_passer_id,
        CAST(receiver_id AS VARCHAR)                                 AS str_receiver_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "n_passes"                                                   AS int_n_passes,
        "n_passes_counter_attack"                                    AS int_n_passes_counter_attack,
        "n_passes_open_play"                                         AS int_n_passes_open_play,
        "n_passes_recovery"                                          AS int_n_passes_recovery,
        "n_passes_other"                                             AS int_n_passes_other,
        "n_progressive"                                              AS int_n_progressive,
        "n_creative"                                                 AS int_n_creative,
        "n_buildup"                                                  AS int_n_buildup
    FROM mdl_body
)

SELECT * FROM mdl_out
