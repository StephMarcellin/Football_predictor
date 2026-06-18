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

        -- Zone de dominance : position moyenne sur les touches
        -- Exclut les BallTouch de gardien (StandingSave=178, DivingSave=179)
        AVG(e.x) FILTER (
            WHERE e.is_touch = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM {{ ref('events_qual') }} eq
                  WHERE eq.match_id  = e.match_id
                    AND eq.row_num   = e.row_num
                    AND eq.qual_type_id IN (178, 179)
              )
        )                                                      AS zone_dominance

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
        MAX(CASE WHEN qual_type_id = 215   THEN 1 ELSE 0 END) AS is_individual_play
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id IN (210, 11113, 1, 2, 4, 22, 215)
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
)


SELECT
    b.*,
    o.* EXCLUDE (match_id, team_id, player_id),
    da.* EXCLUDE (match_id, team_id, player_id),
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
JOIN match_dates d
    ON d.match_id = b.match_id