{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id', 'int_row_num', 'str_qual_type_id'],
        incremental_strategy='delete+insert',
        schema='intermediate',
        enabled=false,
    )
}}

-- ⚠ DÉSACTIVÉ (refonte nommage, 2026-09-23) : ce modèle explose qualifiers_json
-- depuis int_whoscored_events, mais la sortie Spark de int_whoscored_events ne
-- contient plus cette colonne (retirée par spark_events.py). Il fait doublon avec
-- events_qual (même explosion, produite par Spark) et aucun modèle ni script ne
-- le lit. Réactiver = remettre qualifiers_json dans la sortie Spark.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        str_qualifiers_json                                          AS "qualifiers_json",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

-- int_whoscored_match_index lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_match_index AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_ws_match_id                                              AS "ws_match_id",
        dt_match_date                                                AS "match_date",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        CAST(str_ws_home_team_id AS INTEGER)                         AS "ws_home_team_id",
        CAST(str_ws_away_team_id AS INTEGER)                         AS "ws_away_team_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        str_comp_category                                            AS "comp_category"
    FROM {{ ref('int_whoscored_match_index') }}
),

mdl_body AS (
WITH events_with_season AS (
    SELECT
        e.*,
        m.season
    FROM in_int_whoscored_events e
    INNER JOIN in_int_whoscored_match_index m
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
      SELECT DISTINCT season FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            CAST(str_event_id AS INTEGER)                                AS "event_id",
            int_row_num                                                  AS "row_num",
            str_season                                                   AS "season",
            CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
            str_qual_type_name                                           AS "qual_type_name",
            str_qual_value                                               AS "qual_value"
        FROM {{ this }}
    )
  )
{% endif %}
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        CAST(qual_type_id AS VARCHAR)                                AS str_qual_type_id,
        "qual_type_name"                                             AS str_qual_type_name,
        "qual_value"                                                 AS str_qual_value
    FROM mdl_body
)

SELECT * FROM mdl_out
