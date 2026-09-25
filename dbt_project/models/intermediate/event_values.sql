{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='event_values'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_event_enriched lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_event_enriched AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        bool_is_touch                                                AS "is_touch",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_is_leading_to_goal                                       AS "is_leading_to_goal",
        int_is_intentional_goal_assist                               AS "is_intentional_goal_assist",
        int_is_intentional_assist                                    AS "is_intentional_assist",
        int_is_big_chance_created                                    AS "is_big_chance_created",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        int_is_leading_to_attempt                                    AS "is_leading_to_attempt",
        int_has_defensive_qual                                       AS "has_defensive_qual",
        int_has_offensive_qual                                       AS "has_offensive_qual",
        int_has_opposite_event                                       AS "has_opposite_event",
        int_team_score                                               AS "team_score",
        int_opp_score                                                AS "opp_score"
    FROM {{ ref('int_event_enriched') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL SANS SUBQUERY EN RAM
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT e.match_id
    FROM in_int_event_enriched e
    LEFT JOIN (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            CAST(str_event_id AS INTEGER)                                AS "event_id",
            int_row_num                                                  AS "row_num",
            int_expanded_minute                                          AS "expanded_minute",
            int_period                                                   AS "period",
            CAST(str_type_id AS INTEGER)                                 AS "type_id",
            str_type_name                                                AS "type_name",
            CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
            bool_is_shot                                                 AS "is_shot",
            dec_x                                                        AS "x",
            dec_y                                                        AS "y",
            dt_match_date                                                AS "match_date",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
            dec_danger_position                                          AS "danger_position",
            dec_chance_creation                                          AS "chance_creation",
            dec_def_execution_quality                                    AS "def_execution_quality",
            dec_pressure_context                                         AS "pressure_context",
            dec_context_weight                                           AS "context_weight",
            dec_action_value                                             AS "action_value"
        FROM {{ this }}
    ) t ON e.match_id = t.match_id
    WHERE t.match_id IS NULL
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- SOURCE ET CALCUL DES AXES EN UNE SEULE PASSE
-- ══════════════════════════════════════════════════════════════════════════════
calculated_axes AS (
    SELECT
        s.*,

        -- AXE 1 : danger_position
        CASE
            WHEN s.type_id IN (1, 3, 13, 14, 15, 16, 42)
                THEN s.x / 100.0
            WHEN s.type_id IN (7, 8, 12, 45, 49, 74)
                THEN (100.0 - s.x) / 100.0
            WHEN s.type_id IN (44, 4, 50) AND s.has_defensive_qual = 1
                THEN (100.0 - s.x) / 100.0
            WHEN s.type_id IN (44, 4, 50) AND s.has_offensive_qual = 1
                THEN s.x / 100.0
            ELSE NULL
        END AS danger_position,

        -- AXE 2 : chance_creation
        CASE
            WHEN s.type_id = 16                         THEN 1.00
            WHEN s.is_leading_to_goal         = 1       THEN 0.90
            WHEN s.is_intentional_goal_assist  = 1      THEN 0.85
            WHEN s.is_intentional_assist      = 1       THEN 0.80
            WHEN s.is_big_chance_created      = 1       THEN 0.75
            WHEN s.is_key_pass                = 1       THEN 0.65
            WHEN s.is_shot_assist             = 1       THEN 0.55
            WHEN s.is_leading_to_attempt      = 1       THEN 0.50
            WHEN s.type_id = 15                         THEN 0.40
            WHEN s.type_id IN (13, 14)                  THEN 0.10
            WHEN s.type_id = 3 AND s.outcome_id = 1       THEN 0.30
            WHEN s.type_id = 42                         THEN 0.25
            WHEN s.type_id = 3 AND s.outcome_id != 1      THEN 0.05
            ELSE                                           0.00
        END AS chance_creation,

        -- AXE 3 : def_execution_quality
        CASE
            WHEN s.type_id IN (7, 8, 12, 49, 74) AND s.outcome_id = 1
                THEN 1.00
            WHEN s.type_id = 45
                THEN 0.60
            WHEN s.type_id IN (7, 8, 12, 49, 74) AND s.outcome_id != 1
                THEN 0.30
            WHEN s.type_id IN (44, 4, 50) AND s.has_defensive_qual = 1 AND s.outcome_id = 1
                THEN 1.00
            WHEN s.type_id IN (44, 4, 50) AND s.has_defensive_qual = 1 AND s.outcome_id != 1
                THEN 0.30
            ELSE NULL
        END AS def_execution_quality,

        -- AXE 4 : pressure_context
        CASE
            WHEN s.type_id IN (44, 4, 45, 50, 7)
                THEN LEAST(1.0, 0.80 + CASE WHEN s.x IS NOT NULL AND s.x < 50 THEN 0.20 ELSE 0.0 END)
            WHEN s.has_opposite_event = 1
                THEN LEAST(1.0, 0.60 + CASE WHEN s.x IS NOT NULL AND s.x < 50 THEN 0.20 ELSE 0.0 END)
            ELSE
                CASE WHEN s.x IS NOT NULL AND s.x < 50 THEN 0.20 ELSE 0.0 END
        END AS pressure_context,

        -- AXE 5 : context_weight
        CASE
            WHEN s.expanded_minute >= 75 AND s.team_score < s.opp_score               THEN 1.00
            WHEN s.expanded_minute >= 75 AND s.team_score > s.opp_score               THEN 0.85
            WHEN s.expanded_minute >= 75 AND s.team_score = s.opp_score AND s.team_score > 0
                                                                                      THEN 0.75
            WHEN s.expanded_minute >= 75 AND s.team_score = 0 AND s.opp_score = 0    THEN 0.60
            WHEN s.team_score < s.opp_score                                         THEN 0.70
            WHEN s.team_score = s.opp_score AND s.team_score > 0                      THEN 0.50
            WHEN s.team_score > s.opp_score                                         THEN 0.40
            ELSE                                                                     0.30
        END AS context_weight

    FROM in_int_event_enriched s
    WHERE s.match_id IN (SELECT match_id FROM new_matches)
)

-- ══════════════════════════════════════════════════════════════════════════════
-- CALCUL DU ACTION_VALUE ET SELECT FINAL (SANS AUCUN JOIN)
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    match_id,
    team_id,
    player_id,
    event_id,
    row_num,
    expanded_minute,
    period,
    type_id,
    type_name,
    outcome_id,
    is_shot,
    x,
    y,
    match_date,
    season,
    league_source,
    scraped_at,

    -- Axes
    danger_position,
    chance_creation,
    def_execution_quality,
    pressure_context,
    context_weight,

    -- Note globale combinée
    CASE
        WHEN type_id IN (1, 3, 13, 14, 15, 16, 42)
            THEN SQRT(danger_position * chance_creation)
                 * (0.5 + 0.3 * pressure_context + 0.2 * context_weight)

        WHEN type_id IN (7, 8, 12, 45, 49, 74)
            THEN danger_position
                 * def_execution_quality
                 * (0.5 + 0.3 * pressure_context + 0.2 * context_weight)

        WHEN type_id IN (44, 4, 50) AND has_defensive_qual = 1
            THEN danger_position
                 * def_execution_quality
                 * (0.5 + 0.3 * pressure_context + 0.2 * context_weight)

        WHEN type_id IN (44, 4, 50) AND has_offensive_qual = 1
            THEN SQRT(danger_position * chance_creation)
                 * (0.5 + 0.3 * pressure_context + 0.2 * context_weight)

        ELSE NULL
    END AS action_value

FROM calculated_axes
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "row_num"                                                    AS int_row_num,
        "expanded_minute"                                            AS int_expanded_minute,
        "period"                                                     AS int_period,
        CAST(type_id AS VARCHAR)                                     AS str_type_id,
        "type_name"                                                  AS str_type_name,
        CAST(outcome_id AS VARCHAR)                                  AS str_outcome_id,
        "is_shot"                                                    AS bool_is_shot,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "match_date"                                                 AS dt_match_date,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        "danger_position"                                            AS dec_danger_position,
        "chance_creation"                                            AS dec_chance_creation,
        "def_execution_quality"                                      AS dec_def_execution_quality,
        "pressure_context"                                           AS dec_pressure_context,
        "context_weight"                                             AS dec_context_weight,
        "action_value"                                               AS dec_action_value
    FROM mdl_body
)

SELECT * FROM mdl_out
