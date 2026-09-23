{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_match_stats'
    )
}}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. FILTRE INCRÉMENTAL STRICT (Sans Cross Join)
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT e.match_id
    FROM {{ ref('int_whoscored_events') }} e
    LEFT JOIN {{ this }} t ON e.match_id = t.match_id
    WHERE t.match_id IS NULL
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

match_teams AS (
    SELECT match_id, MIN(team_id) AS team_1, MAX(team_id) AS team_2
    FROM {{ ref('int_whoscored_events') }}
    WHERE match_id IN (SELECT match_id FROM match_dates)
    GROUP BY match_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 2. PIVOT DES QUALIFICATEURS
-- Ajout de 178/179 pour éviter la sous-requête corrélée dans n_touches
-- ══════════════════════════════════════════════════════════════════════════════
qual_pivot AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 210   THEN 1 ELSE 0 END) AS is_shot_assist,
        MAX(CASE WHEN qual_type_id = 11113 THEN 1 ELSE 0 END) AS is_key_pass,
        MAX(CASE WHEN qual_type_id = 1     THEN 1 ELSE 0 END) AS is_longball,
        MAX(CASE WHEN qual_type_id = 2     THEN 1 ELSE 0 END) AS is_cross,
        MAX(CASE WHEN qual_type_id = 4     THEN 1 ELSE 0 END) AS is_throughball,
        MAX(CASE WHEN qual_type_id = 22    THEN 1 ELSE 0 END) AS is_regular_play,
        MAX(CASE WHEN qual_type_id = 215   THEN 1 ELSE 0 END) AS is_individual_play,
        MAX(CASE WHEN qual_type_id = 170   THEN 1 ELSE 0 END) AS is_leading_to_goal,
        MAX(CASE WHEN qual_type_id = 169   THEN 1 ELSE 0 END) AS is_leading_to_attempt,
        MAX(CASE WHEN qual_type_id = 286   THEN 1 ELSE 0 END) AS is_offensive_aerial,
        MAX(CASE WHEN qual_type_id = 285   THEN 1 ELSE 0 END) AS is_defensive_aerial,
        MAX(CASE WHEN qual_type_id IN (178, 179) THEN 1 ELSE 0 END) AS is_gk_touch
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id IN (210, 11113, 1, 2, 4, 22, 215, 170, 169, 285, 286, 178, 179)
      AND match_id IN (SELECT match_id FROM match_dates)
    GROUP BY match_id, row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 3. ENRICHISSEMENT DES ÉVÉNEMENTS & CALCUL VECTORISÉ DU SCORE
-- ══════════════════════════════════════════════════════════════════════════════
events_enriched AS (
    SELECT
        e.*,
        mt.team_1,
        
        -- Score cumulé via fonction de fenêtrage (0 jointure)
        SUM(CASE WHEN e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE AND e.team_id = mt.team_1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.match_id ORDER BY e.row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t1_score,
        
        SUM(CASE WHEN e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE AND e.team_id = mt.team_2 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.match_id ORDER BY e.row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS t2_score,
            
        qp.is_shot_assist, qp.is_key_pass, qp.is_longball, qp.is_cross, qp.is_throughball,
        qp.is_regular_play, qp.is_individual_play, qp.is_leading_to_goal, qp.is_leading_to_attempt,
        qp.is_offensive_aerial, qp.is_defensive_aerial, qp.is_gk_touch

    FROM {{ ref('int_whoscored_events') }} e
    INNER JOIN match_dates d ON d.match_id = e.match_id
    INNER JOIN match_teams mt ON mt.match_id = e.match_id
    LEFT JOIN qual_pivot qp ON qp.match_id = e.match_id AND qp.row_num = e.row_num
    WHERE e.player_id IS NOT NULL
),

events_with_state AS (
    SELECT *,
        CASE WHEN team_id = team_1 THEN t1_score ELSE t2_score END AS team_score,
        CASE WHEN team_id = team_1 THEN t2_score ELSE t1_score END AS opp_score
    FROM events_enriched
),

events_final AS (
    SELECT e.*,
        CASE
            WHEN e.expanded_minute >= 75 AND e.team_score = 0 AND e.opp_score = 0 THEN 'blank_late'
            WHEN e.expanded_minute >= 75 AND e.team_score = e.opp_score           THEN 'drawing_late'
            WHEN e.expanded_minute >= 75 AND e.team_score > e.opp_score           THEN 'winning_late'
            WHEN e.expanded_minute >= 75 AND e.team_score < e.opp_score           THEN 'losing_late'
            WHEN e.team_score = 0 AND e.opp_score = 0                             THEN 'blank'
            WHEN e.team_score = e.opp_score                                       THEN 'drawing'
            WHEN e.team_score > e.opp_score                                       THEN 'winning'
            ELSE 'losing'
        END AS score_state
    FROM events_with_state e
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 4. L'AGRÉGATION MAÎTRESSE (Single-Pass)
-- Regroupe base, offensive, defensive, spatial, discipline, aerial, error, score.
-- ══════════════════════════════════════════════════════════════════════════════
master_agg AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        -- ---------------- BASE ----------------
        COUNT(*) AS n_actions,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.is_shot = TRUE) > 0
            THEN COUNT(*) FILTER (WHERE e.is_shot = TRUE)
                 * (1.0 / (1.0 + SQRT(POW(100.0 - AVG(e.x) FILTER (WHERE e.is_shot = TRUE), 2) + POW( 50.0 - AVG(e.y) FILTER (WHERE e.is_shot = TRUE), 2))))
            ELSE 0.0
        END AS xg_contribution,

        -- ---------------- OFFENSIF ----------------
        COUNT(*) FILTER (WHERE e.is_shot = TRUE)                                      AS n_shots,
        SUM(CASE WHEN e.is_shot = TRUE AND COALESCE(e.is_regular_play, 0) = 1 THEN 1 ELSE 0 END)    AS n_shots_regular_play,
        SUM(CASE WHEN e.is_shot = TRUE AND COALESCE(e.is_individual_play, 0) = 1 THEN 1 ELSE 0 END) AS n_shots_individual_play,
        SUM(COALESCE(e.is_shot_assist, 0))                                            AS n_shot_assists,
        SUM(COALESCE(e.is_key_pass, 0))                                               AS n_key_passes,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_longball, 0) = 1 THEN 1 ELSE 0 END)           AS n_longballs,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_cross, 0) = 1 THEN 1 ELSE 0 END)              AS n_crosses,
        SUM(CASE WHEN e.type_id = 1 AND COALESCE(e.is_throughball, 0) = 1 THEN 1 ELSE 0 END)        AS n_throughballs,
        COUNT(*) FILTER (WHERE e.type_id = 1 AND e.outcome_id = 1 AND e.end_x IS NOT NULL AND e.x IS NOT NULL AND e.end_x > e.x + 10) AS n_progressive_passes,

        -- ---------------- DÉFENSIF ----------------
        COUNT(*) FILTER (WHERE e.type_id = 7)                    AS n_tackles,
        COUNT(*) FILTER (WHERE e.type_id = 7 AND e.outcome_id = 1) AS n_tackles_won,
        COUNT(*) FILTER (WHERE e.type_id = 8)                    AS n_interceptions,
        COUNT(*) FILTER (WHERE e.type_id = 49)                   AS n_ball_recoveries,
        COUNT(*) FILTER (WHERE e.type_id = 45)                   AS n_challenges,
        COUNT(*) FILTER (WHERE e.type_id = 12)                   AS n_clearances,
        AVG(e.x) FILTER (WHERE e.type_id IN (7, 8, 49, 45, 12))  AS defensive_zone_x,

        -- ---------------- SPATIAL ----------------
        -- La sous-requête corrélée est remplacée par la vérification de notre nouveau flag is_gk_touch
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND COALESCE(e.is_gk_touch, 0) = 0) AS n_touches,
        {{ spatial_zones() }},

        -- ---------------- AÉRIEN ----------------
        COUNT(*) FILTER (WHERE e.type_id = 44)                   AS n_aerial_duels,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND e.outcome_id = 1) AS n_aerial_won,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.type_id = 44) > 0
            THEN CAST(COUNT(*) FILTER (WHERE e.type_id = 44 AND e.outcome_id = 1) AS DOUBLE) / COUNT(*) FILTER (WHERE e.type_id = 44)
            ELSE NULL
        END                                                      AS aerial_win_rate,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND COALESCE(e.is_offensive_aerial, 0) = 1) AS n_aerial_offensive,
        COUNT(*) FILTER (WHERE e.type_id = 44 AND COALESCE(e.is_defensive_aerial, 0) = 1) AS n_aerial_defensive,

        -- ---------------- ERREURS ----------------
        SUM(CASE WHEN e.type_id = 51 AND COALESCE(e.is_leading_to_goal, 0) = 1 THEN 1 ELSE 0 END)    AS n_errors_lead_to_goal,
        SUM(CASE WHEN e.type_id = 51 AND COALESCE(e.is_leading_to_attempt, 0) = 1 THEN 1 ELSE 0 END) AS n_errors_lead_to_shot,

        -- ---------------- DISCIPLINE ----------------
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'Yellow')       AS n_yellow_cards,
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'SecondYellow') AS n_second_yellows,
        COUNT(*) FILTER (WHERE e.type_id = 17 AND e.card_type = 'Red')          AS n_red_cards,

        -- ---------------- ÉTATS DE SCORE (BOUCLE) ----------------
        {% for state in ['blank', 'blank_late', 'drawing', 'drawing_late', 'winning', 'winning_late', 'losing', 'losing_late'] %}
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}') AS n_actions_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.type_id = 1 AND e.outcome_id = 1 AND e.x IS NOT NULL AND e.end_x > (e.x + 10)) AS n_progressive_passes_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.type_id IN (7, 8, 49, 45, 12)) AS n_defensive_actions_{{ state }},
        COUNT(*) FILTER (WHERE e.score_state = '{{ state }}' AND e.is_shot = TRUE) AS n_shots_{{ state }},
        {{ spatial_zones(filter_condition="e.score_state = '" + state + "' AND e.is_touch = TRUE", prefix=state + '_') }}
        {{ "," if not loop.last }}
        {% endfor %}

    FROM events_final e
    GROUP BY e.match_id, e.team_id, e.player_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- 5. CRÉATION (Agrégation séparée car la clé de regroupement est related_player_id)
-- ══════════════════════════════════════════════════════════════════════════════
creation_agg AS (
    SELECT
        e.match_id,
        e.team_id,
        e.related_player_id AS player_id,
        COUNT(*) FILTER (WHERE e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE) AS n_assists,
        COUNT(*)                                                                         AS n_chances_created
    FROM {{ ref('int_whoscored_events') }} e
    WHERE e.related_player_id IS NOT NULL
      AND e.type_id IN (13, 14, 15, 16)
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.related_player_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- 6. ASSEMBLAGE FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    m.*,
    COALESCE(c.n_assists, 0)         AS n_assists,
    COALESCE(c.n_chances_created, 0) AS n_chances_created,
    pm.minutes_played,
    d.match_date AS date,
    d.season,
    d.league_source,
    d.scraped_at

FROM master_agg m

LEFT JOIN creation_agg c
    ON c.match_id   = m.match_id
   AND c.team_id    = m.team_id
   AND c.player_id  = m.player_id

LEFT JOIN {{ ref('int_player_minutes') }} pm
    ON pm.match_id  = m.match_id
   AND pm.team_id   = m.team_id
   AND pm.player_id = m.player_id

JOIN match_dates d
    ON d.match_id = m.match_id