{{
    config(
        materialized='incremental',
        unique_key=['ws_match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='team_features_ws'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

new_match_ids AS (
    SELECT DISTINCT ws_match_id
    FROM {{ source('silver', 'stg_whoscored_events') }}
    {% if is_incremental() %}
    WHERE ws_match_id NOT IN (
        SELECT DISTINCT ws_match_id FROM {{ this }}
    )
    {% endif %}
),

base_counts AS (
    SELECT ws_match_id, team_id,
        COUNT(*)                                                                        AS total_events,
        COUNT(*) FILTER (WHERE is_touch=TRUE)                                           AS total_touches,
        COUNT(*) FILTER (WHERE is_shot=TRUE)                                            AS total_shots,
        COUNT(*) FILTER (WHERE type_id=1)                                               AS total_passes,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=1)                             AS passes_successful,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>66)                                  AS touches_offensive_zone,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=1 AND end_x>83)                AS deep_completions,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=0 AND x>66)                    AS turnovers_high_zone,
        COUNT(*) FILTER (WHERE x>66 AND type_id IN (1,3,13,15,16))                     AS offensive_actions,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y<33.3)                      AS att_touches_left,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y>=33.3 AND y<=66.6)        AS att_touches_center,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y>66.6)                      AS att_touches_right,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33)                                  AS att_touches_total,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x<33.3)                               AS zone_def_touches,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>=33.3 AND x<=66.6)                 AS zone_mid_touches,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>66.6)                               AS zone_att_touches,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND x>94 AND y BETWEEN 36 AND 64)          AS shots_six_yard,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND x>83 AND y BETWEEN 21 AND 79
                              AND NOT (x>94 AND y BETWEEN 36 AND 64))                   AS shots_penalty_area,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND NOT(x>83 AND y BETWEEN 21 AND 79))     AS shots_out_of_box,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND type_id=16 AND outcome_id=1)           AS goals_scored
    FROM {{ source('silver', 'stg_whoscored_events') }}
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),

defensive_exposure AS (
    SELECT ws_match_id, other_team_id AS team_id,
        COUNT(*) FILTER (WHERE x>33 AND y<33.3)                AS opp_att_left,
        COUNT(*) FILTER (WHERE x>33 AND y>=33.3 AND y<=66.6)  AS opp_att_center,
        COUNT(*) FILTER (WHERE x>33 AND y>66.6)                AS opp_att_right,
        COUNT(*) FILTER (WHERE x>33)                            AS opp_att_total
    FROM (
        SELECT f.ws_match_id, f.x, f.y, f.is_touch, other.team_id AS other_team_id
        FROM {{ source('silver', 'stg_whoscored_events') }} f
        JOIN (
            SELECT ws_match_id, team_id
            FROM {{ source('silver', 'stg_whoscored_events') }}
            WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
            GROUP BY ws_match_id, team_id
        ) other ON f.ws_match_id=other.ws_match_id AND f.team_id!=other.team_id
        WHERE f.is_touch=TRUE AND f.x>33
          AND f.ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    ) att_actions
    GROUP BY ws_match_id, other_team_id
),

qualifier_features AS (
    SELECT ws_match_id, team_id,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND qual_type_id=26)                        AS shots_counter_attack,
        COUNT(DISTINCT row_num) FILTER (WHERE qual_type_id IN (5,6) AND x>50)          AS set_pieces_offensive,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=22)         AS shots_open_play,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=23)         AS shots_set_piece,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=9)          AS shots_penalty,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=2)             AS passes_cross,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=155)           AS passes_through_ball,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=1)             AS passes_long_ball
    FROM {{ ref('events_qual') }}
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),

goals_conceded AS (
    SELECT DISTINCT f.ws_match_id, other.team_id AS conceding_team_id,
        f.expanded_minute AS goal_minute
    FROM {{ source('silver', 'stg_whoscored_events') }} f
    JOIN (
        SELECT ws_match_id, team_id
        FROM {{ source('silver', 'stg_whoscored_events') }}
        WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
        GROUP BY ws_match_id, team_id
    ) other ON f.ws_match_id=other.ws_match_id AND f.team_id!=other.team_id
    WHERE f.type_id=16 AND f.outcome_id=1 AND f.is_shot=TRUE
      AND f.ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
),

momentum_windows AS (
    SELECT gc.ws_match_id, gc.conceding_team_id AS team_id,
        COUNT(e.row_num) FILTER (
            WHERE e.expanded_minute >= gc.goal_minute - 10
              AND e.expanded_minute <  gc.goal_minute
              AND e.team_id = gc.conceding_team_id
        ) AS actions_pre,
        COUNT(e.row_num) FILTER (
            WHERE e.expanded_minute >  gc.goal_minute
              AND e.expanded_minute <= gc.goal_minute + 10
              AND e.team_id = gc.conceding_team_id
        ) AS actions_post
    FROM goals_conceded gc
    JOIN {{ source('silver', 'stg_whoscored_events') }} e
        ON e.ws_match_id = gc.ws_match_id
    GROUP BY gc.ws_match_id, gc.conceding_team_id, gc.goal_minute
),

momentum_agg AS (
    SELECT ws_match_id, team_id,
        AVG(CASE WHEN actions_pre > 0
                 THEN CAST(actions_post AS DOUBLE) / actions_pre
                 ELSE NULL END) AS momentum_delta
    FROM momentum_windows
    GROUP BY ws_match_id, team_id
),

counter_attack_cte AS (
    SELECT s.ws_match_id, s.team_id,
        COUNT(DISTINCT t.t_shot) AS counter_attack_shots,
        COUNT(*) AS total_shots_ca
    FROM (
        SELECT ws_match_id, team_id, expanded_minute*60+second AS t_shot
        FROM {{ source('silver', 'stg_whoscored_events') }}
        WHERE type_id IN (13,14,15,16)
          AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    ) s
    LEFT JOIN (
        SELECT DISTINCT s2.ws_match_id, s2.team_id, s2.t_shot
        FROM (
            SELECT ws_match_id, team_id, expanded_minute*60+second AS t_shot
            FROM {{ source('silver', 'stg_whoscored_events') }}
            WHERE type_id IN (13,14,15,16)
              AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
        ) s2
        JOIN (
            SELECT ws_match_id, team_id, expanded_minute*60+second AS t_recovery
            FROM {{ source('silver', 'stg_whoscored_events') }}
            WHERE type_id=49
              AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
        ) r ON s2.ws_match_id=r.ws_match_id AND s2.team_id=r.team_id
           AND s2.t_shot > r.t_recovery AND s2.t_shot - r.t_recovery <= 15
    ) t ON s.ws_match_id=t.ws_match_id AND s.team_id=t.team_id AND s.t_shot=t.t_shot
    GROUP BY s.ws_match_id, s.team_id
),

midfield_control_cte AS (
    SELECT ws_match_id, team_id,
        CAST(COUNT(*) FILTER (
            WHERE x BETWEEN 33 AND 66 AND type_id IN (1,7,8) AND outcome_id=1
        ) AS DOUBLE)
        / NULLIF(SUM(COUNT(*) FILTER (
            WHERE x BETWEEN 33 AND 66 AND type_id IN (1,7,8) AND outcome_id=1
        )) OVER (PARTITION BY ws_match_id), 0) AS ws_midfield_control_idx
    FROM {{ source('silver', 'stg_whoscored_events') }}
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),

defensive_shape_cte AS (
    SELECT ws_match_id, team_id,
        AVG(x) AS ws_defensive_line_height,
        CASE
            WHEN COUNT(*) FILTER (WHERE y<30) > 0
             AND COUNT(*) FILTER (WHERE y>70) > 0
            THEN (CAST(COUNT(*) FILTER (WHERE y<30 AND outcome_id=1) AS DOUBLE)
                  / COUNT(*) FILTER (WHERE y<30))
               - (CAST(COUNT(*) FILTER (WHERE y>70 AND outcome_id=1) AS DOUBLE)
                  / COUNT(*) FILTER (WHERE y>70))
            ELSE NULL
        END AS ws_flank_exposure_asymm
    FROM {{ source('silver', 'stg_whoscored_events') }}
    WHERE type_id IN (7,8,12) AND x IS NOT NULL AND y IS NOT NULL
      AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
    HAVING COUNT(*) >= 5
)

SELECT
    b.ws_match_id, b.team_id,
    CASE WHEN b.total_touches>0  THEN CAST(b.touches_offensive_zone AS DOUBLE)/b.total_touches END AS ws_field_tilt_actions,
    CASE WHEN b.total_passes>0   THEN CAST(b.turnovers_high_zone AS DOUBLE)/b.total_passes    END AS ws_high_turnover_rate,
    CASE WHEN b.total_passes>0   THEN CAST(b.deep_completions AS DOUBLE)/b.total_passes        END AS ws_deep_completion_rt,
    m.momentum_delta                                                                               AS ws_momentum_delta,
    CASE WHEN b.total_shots>0    THEN CAST(COALESCE(q.shots_counter_attack,0) AS DOUBLE)/b.total_shots   END AS ws_counter_shot_rate,
    CASE WHEN b.offensive_actions>0 THEN CAST(COALESCE(q.set_pieces_offensive,0) AS DOUBLE)/b.offensive_actions END AS ws_set_piece_pressure,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_left AS DOUBLE)/b.att_touches_total   END AS ws_attack_left_pct,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_center AS DOUBLE)/b.att_touches_total END AS ws_attack_center_pct,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_right AS DOUBLE)/b.att_touches_total  END AS ws_attack_right_pct,
    CASE WHEN b.total_touches>0  THEN CAST(b.zone_def_touches AS DOUBLE)/b.total_touches END AS ws_zone_def_pct,
    CASE WHEN b.total_touches>0  THEN CAST(b.zone_mid_touches AS DOUBLE)/b.total_touches END AS ws_zone_mid_pct,
    CASE WHEN b.total_touches>0  THEN CAST(b.zone_att_touches AS DOUBLE)/b.total_touches END AS ws_zone_att_pct,
    CASE WHEN b.total_shots>0    THEN CAST(b.shots_six_yard AS DOUBLE)/b.total_shots     END AS ws_shot_six_yard_pct,
    CASE WHEN b.total_shots>0    THEN CAST(b.shots_penalty_area AS DOUBLE)/b.total_shots END AS ws_shot_penalty_pct,
    CASE WHEN b.total_shots>0    THEN CAST(b.shots_out_of_box AS DOUBLE)/b.total_shots   END AS ws_shot_oob_pct,
    CASE WHEN b.total_shots>0    THEN CAST(COALESCE(q.shots_open_play,0) AS DOUBLE)/b.total_shots    END AS ws_shot_open_play_pct,
    CASE WHEN b.total_shots>0    THEN CAST(COALESCE(q.shots_set_piece,0) AS DOUBLE)/b.total_shots    END AS ws_shot_set_piece_pct,
    CASE WHEN b.total_shots>0    THEN CAST(COALESCE(q.shots_penalty,0) AS DOUBLE)/b.total_shots      END AS ws_shot_penalty_att_pct,
    CASE WHEN b.total_shots>0    THEN CAST(b.goals_scored AS DOUBLE)/b.total_shots                   END AS ws_conversion_rate,
    CASE WHEN b.total_passes>0   THEN CAST(COALESCE(q.passes_cross,0) AS DOUBLE)/b.total_passes      END AS ws_cross_rate,
    CASE WHEN b.total_passes>0   THEN CAST(COALESCE(q.passes_through_ball,0) AS DOUBLE)/b.total_passes END AS ws_through_ball_rate,
    CASE WHEN b.total_passes>0   THEN CAST(COALESCE(q.passes_long_ball,0) AS DOUBLE)/b.total_passes  END AS ws_long_ball_rate,
    CASE WHEN b.total_passes>0   THEN 1.0 - (
        CAST(COALESCE(q.passes_cross,0) AS DOUBLE)/b.total_passes
      + CAST(COALESCE(q.passes_through_ball,0) AS DOUBLE)/b.total_passes
      + CAST(COALESCE(q.passes_long_ball,0) AS DOUBLE)/b.total_passes
    )                                                                                     END AS ws_short_pass_rate,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_left AS DOUBLE)/de.opp_att_total   END AS ws_def_exposed_left_pct,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_center AS DOUBLE)/de.opp_att_total END AS ws_def_exposed_center_pct,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_right AS DOUBLE)/de.opp_att_total  END AS ws_def_exposed_right_pct,
    CASE WHEN ca.total_shots_ca>0 THEN CAST(ca.counter_attack_shots AS DOUBLE)/ca.total_shots_ca ELSE NULL END AS ws_counter_attack_dna,
    mc.ws_midfield_control_idx,
    ds.ws_defensive_line_height,
    ds.ws_flank_exposure_asymm
FROM base_counts b
LEFT JOIN qualifier_features  q  ON b.ws_match_id=q.ws_match_id  AND b.team_id=q.team_id
LEFT JOIN momentum_agg        m  ON b.ws_match_id=m.ws_match_id  AND b.team_id=m.team_id
LEFT JOIN defensive_exposure  de ON b.ws_match_id=de.ws_match_id AND b.team_id=de.team_id
LEFT JOIN counter_attack_cte  ca ON b.ws_match_id=ca.ws_match_id AND b.team_id=ca.team_id
LEFT JOIN midfield_control_cte mc ON b.ws_match_id=mc.ws_match_id AND b.team_id=mc.team_id
LEFT JOIN defensive_shape_cte ds ON b.ws_match_id=ds.ws_match_id AND b.team_id=ds.team_id
