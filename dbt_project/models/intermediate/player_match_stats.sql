{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id'],
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

base_agg AS (
    -- Métriques générales par joueur par match
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        -- Volume total d'actions
        COUNT(*)                                               AS n_actions,

        -- xg_contribution : approximation géométrique depuis la position du tir
        CASE
            WHEN COUNT(*) FILTER (WHERE e.is_shot = TRUE) > 0
            THEN COUNT(*) FILTER (WHERE e.is_shot = TRUE)
                 * (1.0 / (1.0 + SQRT(
                     POW(100.0 - AVG(e.x) FILTER (WHERE e.is_shot = TRUE), 2)
                   + POW( 50.0 - AVG(e.y) FILTER (WHERE e.is_shot = TRUE), 2)
                 )))
            ELSE 0.0
        END                                                    AS xg_contribution,

    FROM {{ ref('int_whoscored_events') }} e
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
),


qual_pivot AS (
    -- Pivote les qualificateurs utiles : une ligne par événement
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
        MAX(CASE WHEN qual_type_id = 169   THEN 1 ELSE 0 END) AS is_leading_to_attempt
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id IN (210, 11113, 1, 2, 4, 22, 215, 170,169)
    AND match_id IN (SELECT match_id FROM match_dates)
    GROUP BY match_id, row_num
),

offensive_agg AS (
    -- Métriques offensives par joueur par match
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        -- Tirs
        COUNT(*) FILTER (WHERE e.is_shot = TRUE)               AS n_shots,
        SUM(CASE WHEN e.is_shot = TRUE
                  AND COALESCE(qp.is_regular_play, 0) = 1
                 THEN 1 ELSE 0 END)                            AS n_shots_regular_play,
        SUM(CASE WHEN e.is_shot = TRUE
                  AND COALESCE(qp.is_individual_play, 0) = 1
                 THEN 1 ELSE 0 END)                            AS n_shots_individual_play,

        -- Création de danger
        SUM(COALESCE(qp.is_shot_assist, 0))                    AS n_shot_assists,
        SUM(COALESCE(qp.is_key_pass, 0))                       AS n_key_passes,

        -- Type de passes (filtre type_id=1 pour rester sur les passes uniquement)
        SUM(CASE WHEN e.type_id = 1
                  AND COALESCE(qp.is_longball, 0) = 1
                 THEN 1 ELSE 0 END)                            AS n_longballs,
        SUM(CASE WHEN e.type_id = 1
                  AND COALESCE(qp.is_cross, 0) = 1
                 THEN 1 ELSE 0 END)                            AS n_crosses,
        SUM(CASE WHEN e.type_id = 1
                  AND COALESCE(qp.is_throughball, 0) = 1
                 THEN 1 ELSE 0 END)                            AS n_throughballs,

        -- Passes progressives : passe réussie avançant le ballon de 10+ unités
        COUNT(*) FILTER (
            WHERE e.type_id = 1
              AND e.outcome_id = 1
              AND e.end_x IS NOT NULL
              AND e.x IS NOT NULL
              AND e.end_x > e.x + 10
        )                                                      AS n_progressive_passes

    FROM {{ ref('int_whoscored_events') }} e
    LEFT JOIN qual_pivot qp
        ON qp.match_id = e.match_id
        AND qp.row_num  = e.row_num
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
),

defensive_agg AS (
    -- Métriques défensives par joueur par match
    -- Calculé séparément de player_agg pour garder la lisibilité
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        -- Tackles
        COUNT(*) FILTER (WHERE e.type_id = 7)                  AS n_tackles,
        COUNT(*) FILTER (WHERE e.type_id = 7
                           AND e.outcome_id = 1)               AS n_tackles_won,

        -- Interceptions
        COUNT(*) FILTER (WHERE e.type_id = 8)                  AS n_interceptions,

        -- Récupérations de balle libre
        COUNT(*) FILTER (WHERE e.type_id = 49)                 AS n_ball_recoveries,

        -- Challenges (toujours Unsuccessful — signal de pressing)
        COUNT(*) FILTER (WHERE e.type_id = 45)                 AS n_challenges,

        -- Dégagements
        COUNT(*) FILTER (WHERE e.type_id = 12)                 AS n_clearances,

        -- Hauteur moyenne de la ligne défensive du joueur
        -- Plus x est bas, plus le joueur défend profond
        AVG(e.x) FILTER (
            WHERE e.type_id IN (7, 8, 49, 45, 12)
        )                                                      AS defensive_zone_x

    FROM {{ ref('int_whoscored_events') }} e
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
),

spatial_agg AS (
    -- Métriques spatiales par joueur par match
    -- Terrain découpé en 25 zones (5 longueur × 5 largeur)
    -- Pourcentages de touches par zone (hors actions gardien)
    -- Inspiré des 5 couloirs de Luis Enrique
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        COUNT(*) FILTER (
            WHERE e.is_touch = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM {{ ref('events_qual') }} eq
                  WHERE eq.match_id     = e.match_id
                    AND eq.row_num      = e.row_num
                    AND eq.qual_type_id IN (178, 179)
              )
        )                                                      AS n_touches,
        {{ spatial_zones() }}

        

    FROM {{ ref('int_whoscored_events') }} e
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
),

-- Reconstruction du score cumulatif à chaque événement
-- puis calcul des métriques par état de score
goals AS (
    -- Un goal par ligne, par équipe, par match
    SELECT
        match_id,
        team_id,
        expanded_minute,
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
),

-- Score des deux équipes à chaque instant pour chaque match
match_score AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,
        e.expanded_minute,
        e.second,
        e.type_id,
        e.is_shot,
        e.outcome_id,
        e.is_touch,
        e.x,
        e.y,
        e.end_x,

        -- Buts marqués par cette équipe jusqu'à cet instant
        COALESCE(MAX(g_team.team_goals_so_far), 0) AS team_score,

        -- Buts marqués par l'adversaire jusqu'à cet instant
        COALESCE(MAX(g_opp.team_goals_so_far), 0)  AS opp_score

    FROM {{ ref('int_whoscored_events') }} e

    -- Dernière valeur cumulée de l'équipe avant ou à cet instant
    LEFT JOIN goals g_team
        ON  g_team.match_id        = e.match_id
        AND g_team.team_id         = e.team_id
        AND g_team.expanded_minute <= e.expanded_minute

    -- Dernière valeur cumulée de l'adversaire avant ou à cet instant
    LEFT JOIN goals g_opp
        ON  g_opp.match_id        = e.match_id
        AND g_opp.team_id        != e.team_id
        AND g_opp.expanded_minute <= e.expanded_minute

    WHERE e.player_id IS NOT NULL
        AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY
        e.match_id, e.team_id, e.player_id,
        e.expanded_minute, e.second,
        e.type_id, e.is_shot, e.outcome_id,
        e.is_touch, e.x, e.end_x, e.y
),

-- Assignation de l'état de score à chaque événement
events_with_state AS (
    SELECT
        match_id, team_id, player_id,
        expanded_minute, type_id, is_shot,
        outcome_id, is_touch, x, end_x, y,
        team_score, opp_score,
        CASE
            WHEN expanded_minute >= 75 AND team_score = 0 AND opp_score = 0
                THEN 'blank_late'
            WHEN expanded_minute >= 75 AND team_score = opp_score
                THEN 'drawing_late'
            WHEN expanded_minute >= 75 AND team_score > opp_score
                THEN 'winning_late'
            WHEN expanded_minute >= 75 AND team_score < opp_score
                THEN 'losing_late'
            WHEN team_score = 0 AND opp_score = 0
                THEN 'blank'
            WHEN team_score = opp_score
                THEN 'drawing'
            WHEN team_score > opp_score
                THEN 'winning'
            ELSE 'losing'
        END AS score_state
    FROM match_score
),

score_state_agg AS (
    -- Agrégation par joueur par match par état
    SELECT
        match_id, team_id, player_id,

        {% for state in ['blank', 'blank_late', 'drawing', 'drawing_late',
                         'winning', 'winning_late', 'losing', 'losing_late'] %}

        COUNT(*) FILTER (WHERE score_state = '{{ state }}')
            AS n_actions_{{ state }},

        COUNT(*) FILTER (WHERE score_state = '{{ state }}'
                           AND type_id = 1
                           AND outcome_id = 1
                           AND x IS NOT NULL
                           AND end_x > (x + 10))
            AS n_progressive_passes_{{ state }},

        COUNT(*) FILTER (WHERE score_state = '{{ state }}'
                           AND type_id IN (7, 8, 49, 45, 12))
            AS n_defensive_actions_{{ state }},

        COUNT(*) FILTER (WHERE score_state = '{{ state }}'
                           AND is_shot = TRUE)
            AS n_shots_{{ state }},

        {{ spatial_zones(
            filter_condition="score_state = '" + state + "' AND e.is_touch = TRUE",
            prefix=state + '_'
        ) }}
        {{ "," if not loop.last }}
        {% endfor %}

        FROM events_with_state e
        GROUP BY e.match_id, e.team_id, e.player_id
),

aerial_agg AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        COUNT(*) FILTER (WHERE e.type_id = 44)                 AS n_aerial_duels,
        COUNT(*) FILTER (WHERE e.type_id = 44
                           AND e.outcome_id = 1)               AS n_aerial_won,
        CASE
            WHEN COUNT(*) FILTER (WHERE e.type_id = 44) > 0
            THEN CAST(COUNT(*) FILTER (WHERE e.type_id = 44
                                         AND e.outcome_id = 1)
                      AS DOUBLE)
                 / COUNT(*) FILTER (WHERE e.type_id = 44)
            ELSE NULL
        END                                                    AS aerial_win_rate

    FROM {{ ref('int_whoscored_events') }} e
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
),

error_agg AS (
    -- Proxy erreurs défensives menant à un tir ou un but adverse
    -- LeadingToGoal (170) : action qui mène directement au but
    -- LeadingToAttempt (169) : action qui mène à un tir
    -- Combiné avec type_id=51 (Error) pour isoler les vraies erreurs
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,

        SUM(CASE WHEN e.type_id = 51
                  AND COALESCE(qp.is_leading_to_goal, 0) = 1
                 THEN 1 ELSE 0 END)    AS n_errors_lead_to_goal,

        SUM(CASE WHEN e.type_id = 51
                  AND COALESCE(qp.is_leading_to_attempt, 0) = 1
                 THEN 1 ELSE 0 END)    AS n_errors_lead_to_shot

    FROM {{ ref('int_whoscored_events') }} e
    LEFT JOIN qual_pivot qp
        ON qp.match_id = e.match_id
        AND qp.row_num  = e.row_num
    WHERE e.player_id IS NOT NULL
      AND e.match_id IN (SELECT match_id FROM match_dates)
    GROUP BY e.match_id, e.team_id, e.player_id
)

SELECT
    b.*,
    o.* EXCLUDE (match_id, team_id, player_id),
    da.* EXCLUDE (match_id, team_id, player_id),
    sa.* EXCLUDE (match_id, team_id, player_id),
    ssa.* EXCLUDE (match_id, team_id, player_id),
    aa.* EXCLUDE (match_id, team_id, player_id),
    ea.* EXCLUDE (match_id, team_id, player_id),
    d.match_date AS date,
    d.season,
    d.league_source,
    d.scraped_at
FROM base_agg b

LEFT JOIN offensive_agg o
    ON o.match_id   = b.match_id
    AND o.team_id   = b.team_id
    AND o.player_id = b.player_id

LEFT JOIN defensive_agg da
    ON da.match_id   = b.match_id
    AND da.team_id   = b.team_id
    AND da.player_id = b.player_id

LEFT JOIN spatial_agg sa
    ON sa.match_id   = b.match_id
    AND sa.team_id   = b.team_id
    AND sa.player_id = b.player_id

LEFT JOIN score_state_agg ssa    
    ON ssa.match_id   = b.match_id
    AND ssa.team_id   = b.team_id
    AND ssa.player_id = b.player_id

LEFT JOIN aerial_agg aa
    ON aa.match_id   = b.match_id
    AND aa.team_id   = b.team_id
    AND aa.player_id = b.player_id

LEFT JOIN error_agg ea
    ON ea.match_id   = b.match_id
    AND ea.team_id   = b.team_id
    AND ea.player_id = b.player_id

JOIN match_dates d
    ON d.match_id = b.match_id