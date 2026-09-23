{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='event_values'
    )
}}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL SANS SUBQUERY EN RAM
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT e.match_id
    FROM {{ ref('int_event_enriched') }} e
    LEFT JOIN {{ this }} t ON e.match_id = t.match_id
    WHERE t.match_id IS NULL
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_event_enriched') }}
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

    FROM {{ ref('int_event_enriched') }} s
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