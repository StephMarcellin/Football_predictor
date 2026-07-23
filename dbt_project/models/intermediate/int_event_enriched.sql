{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_event_enriched'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- Même logique que player_match_stats : on ne traite que les nouveaux matchs
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM {{ this }}
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_whoscored_events') }}
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_whoscored_events') }}
),
{% endif %}

match_dates AS (
    SELECT match_id, match_date, league_source, season, scraped_at
    FROM {{ ref('int_whoscored_match_index') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PIVOT DES QUALIFICATEURS
-- Une ligne par événement (row_num) avec les flags dont on a besoin.
-- On ne pivote que les qual_type_id utiles pour les axes de event_values.
-- ══════════════════════════════════════════════════════════════════════════════
qual_flags AS (
    SELECT
        match_id,
        row_num,

        -- Hiérarchie chance_creation (du plus fort au plus faible)
        MAX(CASE WHEN qual_type_id = 170   THEN 1 ELSE 0 END) AS is_leading_to_goal,
        MAX(CASE WHEN qual_type_id = 11111 THEN 1 ELSE 0 END) AS is_intentional_goal_assist,
        MAX(CASE WHEN qual_type_id = 154   THEN 1 ELSE 0 END) AS is_intentional_assist,
        MAX(CASE WHEN qual_type_id = 11112 THEN 1 ELSE 0 END) AS is_big_chance_created,
        MAX(CASE WHEN qual_type_id = 11113 THEN 1 ELSE 0 END) AS is_key_pass,
        MAX(CASE WHEN qual_type_id = 210   THEN 1 ELSE 0 END) AS is_shot_assist,
        MAX(CASE WHEN qual_type_id = 169   THEN 1 ELSE 0 END) AS is_leading_to_attempt,

        -- Routage des duels purs (Aerial/Foul/Dispossessed) : couverture 100%
        MAX(CASE WHEN qual_type_id = 285   THEN 1 ELSE 0 END) AS has_defensive_qual,
        MAX(CASE WHEN qual_type_id = 286   THEN 1 ELSE 0 END) AS has_offensive_qual,

        -- Pression : lien vers l'événement symétrique adverse du duel
        MAX(CASE WHEN qual_type_id = 233   THEN 1 ELSE 0 END) AS has_opposite_event

    FROM {{ ref('events_qual') }}
    WHERE match_id IN (SELECT match_id FROM match_dates)
      AND qual_type_id IN (170, 11111, 154, 11112, 11113, 210, 169, 285, 286, 233)
    GROUP BY match_id, row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- RECONSTRUCTION DU SCORE CUMULATIF
-- Buts marqués par chaque équipe, cumulés minute par minute.
-- Nécessaire pour construire context_weight dans event_values.
-- ══════════════════════════════════════════════════════════════════════════════
goals AS (
    SELECT
        match_id,
        team_id,
        expanded_minute,
        second,
        COUNT(*) OVER (
            PARTITION BY match_id, team_id
            ORDER BY expanded_minute, second
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS team_goals_so_far
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 16
      AND outcome_id = 1
      AND is_shot = TRUE
      AND match_id IN (SELECT match_id FROM match_dates)
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL — JOIN CENTRAL
-- Chaque événement reçoit ses qual_flags et le score en cours.
-- Ce modèle est matérialisé : event_values lit depuis ici sans recalculer.
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    e.match_id,
    e.team_id,
    e.player_id,
    e.event_id,
    e.row_num,
    e.expanded_minute,
    e.second,
    e.period,
    e.type_id,
    e.type_name,
    e.outcome_id,
    e.is_shot,
    e.is_touch,
    e.x,
    e.y,
    e.end_x,
    e.end_y,

    -- Champs events enrichis (propagés depuis stg_whoscored_events).
    -- Disponibles en aval pour : vrais passeurs (related_player_id),
    -- discipline (card_type), placement de tir (goal_mouth_*, blocked_*).
    e.is_own_goal,
    e.related_event_id,
    e.related_player_id,
    e.card_type,
    e.goal_mouth_y,
    e.goal_mouth_z,
    e.blocked_x,
    e.blocked_y,

    d.match_date,
    d.season,
    d.league_source,
    d.scraped_at,

    -- Qual flags (0 si l'événement n'avait aucun qualificateur utile)
    COALESCE(qf.is_leading_to_goal,         0) AS is_leading_to_goal,
    COALESCE(qf.is_intentional_goal_assist,  0) AS is_intentional_goal_assist,
    COALESCE(qf.is_intentional_assist,       0) AS is_intentional_assist,
    COALESCE(qf.is_big_chance_created,       0) AS is_big_chance_created,
    COALESCE(qf.is_key_pass,                 0) AS is_key_pass,
    COALESCE(qf.is_shot_assist,              0) AS is_shot_assist,
    COALESCE(qf.is_leading_to_attempt,       0) AS is_leading_to_attempt,
    COALESCE(qf.has_defensive_qual,          0) AS has_defensive_qual,
    COALESCE(qf.has_offensive_qual,          0) AS has_offensive_qual,
    COALESCE(qf.has_opposite_event,          0) AS has_opposite_event,

    -- Score cumulatif de l'équipe à cet instant
    COALESCE(MAX(g_team.team_goals_so_far), 0)  AS team_score,

    -- Score cumulatif de l'adversaire à cet instant
    COALESCE(MAX(g_opp.team_goals_so_far),  0)  AS opp_score

FROM {{ ref('int_whoscored_events') }} e

JOIN match_dates d
    ON d.match_id = e.match_id

LEFT JOIN qual_flags qf
    ON qf.match_id = e.match_id
   AND qf.row_num  = e.row_num

-- Dernier cumul de buts de l'équipe avant ou à cet instant
LEFT JOIN goals g_team
    ON  g_team.match_id        = e.match_id
    AND g_team.team_id         = e.team_id
    AND g_team.expanded_minute <= e.expanded_minute

-- Dernier cumul de buts de l'adversaire avant ou à cet instant
LEFT JOIN goals g_opp
    ON  g_opp.match_id        = e.match_id
    AND g_opp.team_id        != e.team_id
    AND g_opp.expanded_minute <= e.expanded_minute

WHERE e.player_id IS NOT NULL
  AND e.match_id IN (SELECT match_id FROM match_dates)

GROUP BY
    e.match_id, e.team_id, e.player_id, e.event_id, e.row_num,
    e.expanded_minute, e.second, e.period,
    e.type_id, e.type_name, e.outcome_id,
    e.is_shot, e.is_touch, e.x, e.y, e.end_x, e.end_y,
    e.is_own_goal, e.related_event_id, e.related_player_id, e.card_type,
    e.goal_mouth_y, e.goal_mouth_z, e.blocked_x, e.blocked_y,
    d.match_date, d.season, d.league_source, d.scraped_at,
    qf.is_leading_to_goal, qf.is_intentional_goal_assist,
    qf.is_intentional_assist, qf.is_big_chance_created,
    qf.is_key_pass, qf.is_shot_assist, qf.is_leading_to_attempt,
    qf.has_defensive_qual, qf.has_offensive_qual, qf.has_opposite_event