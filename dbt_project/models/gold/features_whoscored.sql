{{
    config(
        materialized='incremental',
        unique_key=['date', 'team', 'league_source'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_whoscored'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- NOUVEAUX MATCHS (filtre incremental)
-- ══════════════════════════════════════════════════════════════════════════════
new_match_ids AS (
    SELECT DISTINCT ws_match_id
    FROM {{ source('silver', 'stg_whoscored_match_index') }}
    {% if is_incremental() %}
    WHERE ws_match_id NOT IN (
        SELECT DISTINCT ws_match_id
        FROM {{ ref('team_features_ws') }}
        WHERE ws_match_id NOT IN (
            SELECT DISTINCT ws_match_id
            FROM intermediate.features_whoscored_matches
        )
    )
    {% endif %}
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3A — Pivot home/away depuis team_features_ws
-- ══════════════════════════════════════════════════════════════════════════════
match_meta AS (
    SELECT ws_match_id, home_team_id, away_team_id,
           home_team_name, away_team_name, match_date, season, league_source
    FROM {{ source('silver', 'stg_whoscored_match_index') }}
),

home_features AS (
    SELECT f.ws_match_id,
        f.ws_field_tilt_actions, f.ws_high_turnover_rate, f.ws_deep_completion_rt,
        f.ws_momentum_delta, f.ws_counter_shot_rate, f.ws_set_piece_pressure,
        f.ws_attack_left_pct, f.ws_attack_center_pct, f.ws_attack_right_pct,
        f.ws_zone_def_pct, f.ws_zone_mid_pct, f.ws_zone_att_pct,
        f.ws_shot_six_yard_pct, f.ws_shot_penalty_pct, f.ws_shot_oob_pct,
        f.ws_shot_open_play_pct, f.ws_shot_set_piece_pct, f.ws_shot_penalty_att_pct,
        f.ws_conversion_rate, f.ws_cross_rate, f.ws_through_ball_rate,
        f.ws_long_ball_rate, f.ws_short_pass_rate,
        f.ws_def_exposed_left_pct, f.ws_def_exposed_center_pct, f.ws_def_exposed_right_pct,
        f.ws_counter_attack_dna, f.ws_midfield_control_idx,
        f.ws_defensive_line_height, f.ws_flank_exposure_asymm
    FROM {{ ref('team_features_ws') }} f
    JOIN match_meta m ON f.ws_match_id=m.ws_match_id AND f.team_id=m.home_team_id
),

away_features AS (
    SELECT f.ws_match_id,
        f.ws_field_tilt_actions, f.ws_high_turnover_rate, f.ws_deep_completion_rt,
        f.ws_momentum_delta, f.ws_counter_shot_rate, f.ws_set_piece_pressure,
        f.ws_attack_left_pct, f.ws_attack_center_pct, f.ws_attack_right_pct,
        f.ws_zone_def_pct, f.ws_zone_mid_pct, f.ws_zone_att_pct,
        f.ws_shot_six_yard_pct, f.ws_shot_penalty_pct, f.ws_shot_oob_pct,
        f.ws_shot_open_play_pct, f.ws_shot_set_piece_pct, f.ws_shot_penalty_att_pct,
        f.ws_conversion_rate, f.ws_cross_rate, f.ws_through_ball_rate,
        f.ws_long_ball_rate, f.ws_short_pass_rate,
        f.ws_def_exposed_left_pct, f.ws_def_exposed_center_pct, f.ws_def_exposed_right_pct,
        f.ws_counter_attack_dna, f.ws_midfield_control_idx,
        f.ws_defensive_line_height, f.ws_flank_exposure_asymm
    FROM {{ ref('team_features_ws') }} f
    JOIN match_meta m ON f.ws_match_id=m.ws_match_id AND f.team_id=m.away_team_id
),

-- Pivot home/away → 1 ligne par match avec features des deux équipes
match_pivot AS (
    SELECT
        m.ws_match_id, m.match_date, m.season, m.league_source,
        m.home_team_name, m.away_team_name,
        h.ws_field_tilt_actions     AS home_field_tilt_actions,
        h.ws_momentum_delta         AS home_momentum_delta,
        h.ws_zone_att_pct           AS home_zone_att_pct,
        h.ws_counter_attack_dna     AS home_counter_attack_dna,
        h.ws_midfield_control_idx   AS home_midfield_control_idx,
        h.ws_defensive_line_height  AS home_defensive_line_height,
        h.ws_flank_exposure_asymm   AS home_flank_exposure_asymm,
        h.ws_high_turnover_rate     AS home_high_turnover_rate,
        h.ws_deep_completion_rt     AS home_deep_completion_rt,
        h.ws_counter_shot_rate      AS home_counter_shot_rate,
        h.ws_set_piece_pressure     AS home_set_piece_pressure,
        h.ws_attack_left_pct        AS home_attack_left_pct,
        h.ws_attack_center_pct      AS home_attack_center_pct,
        h.ws_attack_right_pct       AS home_attack_right_pct,
        h.ws_zone_def_pct           AS home_zone_def_pct,
        h.ws_zone_mid_pct           AS home_zone_mid_pct,
        h.ws_shot_six_yard_pct      AS home_shot_six_yard_pct,
        h.ws_shot_penalty_pct       AS home_shot_penalty_pct,
        h.ws_shot_oob_pct           AS home_shot_oob_pct,
        h.ws_shot_open_play_pct     AS home_shot_open_play_pct,
        h.ws_shot_set_piece_pct     AS home_shot_set_piece_pct,
        h.ws_shot_penalty_att_pct   AS home_shot_penalty_att_pct,
        h.ws_conversion_rate        AS home_conversion_rate,
        h.ws_cross_rate             AS home_cross_rate,
        h.ws_through_ball_rate      AS home_through_ball_rate,
        h.ws_long_ball_rate         AS home_long_ball_rate,
        h.ws_short_pass_rate        AS home_short_pass_rate,
        h.ws_def_exposed_left_pct   AS home_def_exposed_left_pct,
        h.ws_def_exposed_center_pct AS home_def_exposed_center_pct,
        h.ws_def_exposed_right_pct  AS home_def_exposed_right_pct,
        a.ws_field_tilt_actions     AS away_field_tilt_actions,
        a.ws_momentum_delta         AS away_momentum_delta,
        a.ws_zone_att_pct           AS away_zone_att_pct,
        a.ws_counter_attack_dna     AS away_counter_attack_dna,
        a.ws_midfield_control_idx   AS away_midfield_control_idx,
        a.ws_defensive_line_height  AS away_defensive_line_height,
        a.ws_flank_exposure_asymm   AS away_flank_exposure_asymm,
        a.ws_high_turnover_rate     AS away_high_turnover_rate,
        a.ws_deep_completion_rt     AS away_deep_completion_rt,
        a.ws_counter_shot_rate      AS away_counter_shot_rate,
        a.ws_set_piece_pressure     AS away_set_piece_pressure,
        a.ws_attack_left_pct        AS away_attack_left_pct,
        a.ws_attack_center_pct      AS away_attack_center_pct,
        a.ws_attack_right_pct       AS away_attack_right_pct,
        a.ws_zone_def_pct           AS away_zone_def_pct,
        a.ws_zone_mid_pct           AS away_zone_mid_pct,
        a.ws_shot_six_yard_pct      AS away_shot_six_yard_pct,
        a.ws_shot_penalty_pct       AS away_shot_penalty_pct,
        a.ws_shot_oob_pct           AS away_shot_oob_pct,
        a.ws_shot_open_play_pct     AS away_shot_open_play_pct,
        a.ws_shot_set_piece_pct     AS away_shot_set_piece_pct,
        a.ws_shot_penalty_att_pct   AS away_shot_penalty_att_pct,
        a.ws_conversion_rate        AS away_conversion_rate,
        a.ws_cross_rate             AS away_cross_rate,
        a.ws_through_ball_rate      AS away_through_ball_rate,
        a.ws_long_ball_rate         AS away_long_ball_rate,
        a.ws_short_pass_rate        AS away_short_pass_rate,
        a.ws_def_exposed_left_pct   AS away_def_exposed_left_pct,
        a.ws_def_exposed_center_pct AS away_def_exposed_center_pct,
        a.ws_def_exposed_right_pct  AS away_def_exposed_right_pct
    FROM match_meta m
    LEFT JOIN home_features h ON m.ws_match_id=h.ws_match_id
    LEFT JOIN away_features a ON m.ws_match_id=a.ws_match_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3B — Dépivotage home/away → 1 ligne par équipe par match
-- Application du team_mapping pour normaliser les noms
-- ══════════════════════════════════════════════════════════════════════════════
home_side AS (
    SELECT
        p.match_date                                                AS ws_date,
        p.season                                                    AS ws_season,
        p.league_source,
        COALESCE(tm.club_name, p.home_team_name)                   AS team_name,
        p.home_field_tilt_actions   AS ws_field_tilt_actions,
        p.home_high_turnover_rate   AS ws_high_turnover_rate,
        p.home_deep_completion_rt   AS ws_deep_completion_rt,
        p.home_momentum_delta       AS ws_momentum_delta,
        p.home_counter_shot_rate    AS ws_counter_shot_rate,
        p.home_set_piece_pressure   AS ws_set_piece_pressure,
        p.home_attack_left_pct      AS ws_attack_left_pct,
        p.home_attack_center_pct    AS ws_attack_center_pct,
        p.home_attack_right_pct     AS ws_attack_right_pct,
        p.home_zone_def_pct         AS ws_zone_def_pct,
        p.home_zone_mid_pct         AS ws_zone_mid_pct,
        p.home_zone_att_pct         AS ws_zone_att_pct,
        p.home_shot_six_yard_pct    AS ws_shot_six_yard_pct,
        p.home_shot_penalty_pct     AS ws_shot_penalty_pct,
        p.home_shot_oob_pct         AS ws_shot_oob_pct,
        p.home_shot_open_play_pct   AS ws_shot_open_play_pct,
        p.home_shot_set_piece_pct   AS ws_shot_set_piece_pct,
        p.home_shot_penalty_att_pct AS ws_shot_penalty_att_pct,
        p.home_conversion_rate      AS ws_conversion_rate,
        p.home_cross_rate           AS ws_cross_rate,
        p.home_through_ball_rate    AS ws_through_ball_rate,
        p.home_long_ball_rate       AS ws_long_ball_rate,
        p.home_short_pass_rate      AS ws_short_pass_rate,
        p.home_def_exposed_left_pct   AS ws_def_exposed_left_pct,
        p.home_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.home_def_exposed_right_pct  AS ws_def_exposed_right_pct,
        p.home_counter_attack_dna   AS ws_counter_attack_dna,
        p.home_midfield_control_idx AS ws_midfield_control_idx,
        p.home_defensive_line_height AS ws_defensive_line_height,
        p.home_flank_exposure_asymm  AS ws_flank_exposure_asymm
    FROM match_pivot p
    LEFT JOIN {{ ref('team_mapping') }} tm ON p.home_team_name = tm.alias
    WHERE p.home_team_name IS NOT NULL
),

away_side AS (
    SELECT
        p.match_date                                                AS ws_date,
        p.season                                                    AS ws_season,
        p.league_source,
        COALESCE(tm.club_name, p.away_team_name)                   AS team_name,
        p.away_field_tilt_actions   AS ws_field_tilt_actions,
        p.away_high_turnover_rate   AS ws_high_turnover_rate,
        p.away_deep_completion_rt   AS ws_deep_completion_rt,
        p.away_momentum_delta       AS ws_momentum_delta,
        p.away_counter_shot_rate    AS ws_counter_shot_rate,
        p.away_set_piece_pressure   AS ws_set_piece_pressure,
        p.away_attack_left_pct      AS ws_attack_left_pct,
        p.away_attack_center_pct    AS ws_attack_center_pct,
        p.away_attack_right_pct     AS ws_attack_right_pct,
        p.away_zone_def_pct         AS ws_zone_def_pct,
        p.away_zone_mid_pct         AS ws_zone_mid_pct,
        p.away_zone_att_pct         AS ws_zone_att_pct,
        p.away_shot_six_yard_pct    AS ws_shot_six_yard_pct,
        p.away_shot_penalty_pct     AS ws_shot_penalty_pct,
        p.away_shot_oob_pct         AS ws_shot_oob_pct,
        p.away_shot_open_play_pct   AS ws_shot_open_play_pct,
        p.away_shot_set_piece_pct   AS ws_shot_set_piece_pct,
        p.away_shot_penalty_att_pct AS ws_shot_penalty_att_pct,
        p.away_conversion_rate      AS ws_conversion_rate,
        p.away_cross_rate           AS ws_cross_rate,
        p.away_through_ball_rate    AS ws_through_ball_rate,
        p.away_long_ball_rate       AS ws_long_ball_rate,
        p.away_short_pass_rate      AS ws_short_pass_rate,
        p.away_def_exposed_left_pct   AS ws_def_exposed_left_pct,
        p.away_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.away_def_exposed_right_pct  AS ws_def_exposed_right_pct,
        p.away_counter_attack_dna   AS ws_counter_attack_dna,
        p.away_midfield_control_idx AS ws_midfield_control_idx,
        p.away_defensive_line_height AS ws_defensive_line_height,
        p.away_flank_exposure_asymm  AS ws_flank_exposure_asymm
    FROM match_pivot p
    LEFT JOIN {{ ref('team_mapping') }} tm ON p.away_team_name = tm.alias
    WHERE p.away_team_name IS NOT NULL
),

-- Historique team-centric (home + away réunis)
ws_history AS (
    SELECT * FROM home_side
    UNION ALL
    SELECT * FROM away_side
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3C — Anti-leakage LAG(1) : jointure avec backbone
-- Pour chaque match dans backbone, on prend le dernier match WhoScored
-- STRICTEMENT ANTÉRIEUR à la date du match (pas de data leakage)
-- ══════════════════════════════════════════════════════════════════════════════
backbone_base AS (
    SELECT "date", team, league_source, season
    FROM {{ ref('backbone') }}
),

latest_ws_date AS (
    SELECT
        b.date, b.team, b.league_source,
        MAX(wsh.ws_date) AS max_ws_date
    FROM backbone_base b
    JOIN ws_history wsh
        ON  b.team          = wsh.team_name
        AND b.league_source = wsh.league_source
        AND wsh.ws_date     < b.date
    GROUP BY b.date, b.team, b.league_source
),
latest_ws AS (
    SELECT
        b."date",
        b.team, b.league_source,
        wsh.ws_field_tilt_actions, wsh.ws_high_turnover_rate, wsh.ws_deep_completion_rt,
        wsh.ws_momentum_delta, wsh.ws_counter_shot_rate, wsh.ws_set_piece_pressure,
        wsh.ws_attack_left_pct, wsh.ws_attack_center_pct, wsh.ws_attack_right_pct,
        wsh.ws_zone_def_pct, wsh.ws_zone_mid_pct, wsh.ws_zone_att_pct,
        wsh.ws_shot_six_yard_pct, wsh.ws_shot_penalty_pct, wsh.ws_shot_oob_pct,
        wsh.ws_shot_open_play_pct, wsh.ws_shot_set_piece_pct, wsh.ws_shot_penalty_att_pct,
        wsh.ws_conversion_rate, wsh.ws_cross_rate, wsh.ws_through_ball_rate,
        wsh.ws_long_ball_rate, wsh.ws_short_pass_rate,
        wsh.ws_def_exposed_left_pct, wsh.ws_def_exposed_center_pct, wsh.ws_def_exposed_right_pct,
        wsh.ws_counter_attack_dna, wsh.ws_midfield_control_idx,
        wsh.ws_defensive_line_height, wsh.ws_flank_exposure_asymm,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY b.team, b."date", b.league_source
        --     ORDER BY wsh.ws_date DESC
        -- ) AS rn
    FROM latest_ws_date b
    JOIN ws_history wsh
        ON  b.team       = wsh.team_name
        AND b.league_source = wsh.league_source
        AND wsh.ws_date < b.max_ws_date   -- anti-leakage strict
        -- AND b.season     = wsh.ws_season
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3D — Squad features rolling 5 matchs
-- ══════════════════════════════════════════════════════════════════════════════
squad_current AS (
    SELECT
        pms.ws_match_id, pms.team_id, pms.player_id,
        pms."date" AS match_date,
        pms.season, pms.league_source,
        pms.n_actions, pms.xg_contribution
    FROM {{ ref('player_match_stats') }} pms
    WHERE pms.player_id IS NOT NULL
),

player_form AS (
    SELECT
        cur.ws_match_id, cur.team_id, cur.player_id,
        cur.match_date, cur.league_source,
        AVG(hist.n_actions)        AS player_avg_actions_5,
        AVG(hist.xg_contribution)  AS player_avg_xg_5,
        COUNT(hist.ws_match_id)    AS n_prev_matches
    FROM squad_current cur
    LEFT JOIN {{ ref('player_match_stats') }} hist
        ON  hist.player_id    = cur.player_id
        AND hist.league_source = cur.league_source
        AND hist."date"::DATE   < cur.match_date
        AND hist."date"::DATE  >= cur.match_date - INTERVAL '180 days'
    GROUP BY cur.ws_match_id, cur.team_id, cur.player_id, cur.match_date, cur.league_source
    HAVING COUNT(hist.ws_match_id) >= 1
),

squad_regularity AS (
    SELECT cur.ws_match_id, cur.team_id,
        COUNT(DISTINCT cur.player_id) AS squad_size,
        CASE WHEN COUNT(DISTINCT cur.player_id) > 0
             THEN CAST(COUNT(DISTINCT prev.player_id) AS DOUBLE) / COUNT(DISTINCT cur.player_id)
             ELSE NULL END AS squad_regularity
    FROM squad_current cur
    LEFT JOIN (
        SELECT player_id, team_id, CAST("date" AS DATE) AS prev_date, league_source
        FROM {{ ref('player_match_stats') }}
        WHERE player_id IS NOT NULL
    ) prev
        ON  prev.player_id    = cur.player_id
        AND prev.team_id      = cur.team_id
        AND prev.league_source = cur.league_source
        AND prev.prev_date    < cur.match_date
        AND prev.prev_date   >= cur.match_date - INTERVAL '14 days'
    GROUP BY cur.ws_match_id, cur.team_id
),

squad_top3 AS (
    SELECT ws_match_id, team_id,
        CASE WHEN MAX(total_actions) > 0
             THEN SUM(n_actions) FILTER (WHERE rk <= 3) / MAX(total_actions)
             ELSE NULL END AS squad_top3_share
    FROM (
        SELECT ws_match_id, team_id, player_id, n_actions,
            ROW_NUMBER() OVER (PARTITION BY ws_match_id, team_id ORDER BY n_actions DESC) AS rk,
            SUM(n_actions) OVER (PARTITION BY ws_match_id, team_id) AS total_actions
        FROM squad_current
    ) ranked
    GROUP BY ws_match_id, team_id
),

squad_agg AS (
    SELECT
        pf.ws_match_id, pf.team_id, pf.match_date, pf.league_source,
        AVG(pf.player_avg_actions_5) AS squad_avg_form_5,
        AVG(pf.player_avg_xg_5)      AS squad_xg_quality_5,
        sr.squad_regularity,
        t3.squad_top3_share
    FROM player_form pf
    LEFT JOIN squad_regularity sr ON pf.ws_match_id=sr.ws_match_id AND pf.team_id=sr.team_id
    LEFT JOIN squad_top3       t3 ON pf.ws_match_id=t3.ws_match_id AND pf.team_id=t3.team_id
    GROUP BY pf.ws_match_id, pf.team_id, pf.match_date, pf.league_source,
             sr.squad_regularity, t3.squad_top3_share
),

-- Résolution team_name pour le squad via team_mapping
squad_named AS (
    SELECT
        sa.*,
        COALESCE(tm.club_name,
            CASE WHEN mi.home_team_id = sa.team_id THEN mi.home_team_name
                 ELSE mi.away_team_name END
        ) AS team_name
    FROM squad_agg sa
    JOIN {{ source('silver', 'stg_whoscored_match_index') }} mi
        ON sa.ws_match_id = mi.ws_match_id
    LEFT JOIN {{ ref('team_mapping') }} tm ON tm.alias =
        CASE WHEN mi.home_team_id = sa.team_id THEN mi.home_team_name
             ELSE mi.away_team_name END
),

-- Anti-leakage squad : dernier match squad AVANT la date du match backbone
squad_for_backbone AS (
    SELECT
        b."date", b.team, b.league_source,
        sn.squad_avg_form_5, sn.squad_xg_quality_5,
        sn.squad_regularity, sn.squad_top3_share,
        ROW_NUMBER() OVER (
            PARTITION BY b.team, b.league_source, CAST(b."date" AS DATE)
            ORDER BY sn.match_date DESC
        ) AS rn
    FROM backbone_base b
    JOIN squad_named sn
        ON  sn.team_name    = b.team
        AND sn.league_source = b.league_source
        AND CAST(sn.match_date AS DATE) < CAST(b."date" AS DATE)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- ASSEMBLAGE FINAL
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        b."date", b.team, b.league_source,

        -- Features WhoScored match (anti-leakage LAG1)
        lws.ws_field_tilt_actions,
        lws.ws_high_turnover_rate,
        lws.ws_deep_completion_rt,
        lws.ws_momentum_delta,
        lws.ws_counter_shot_rate,
        lws.ws_set_piece_pressure,
        lws.ws_attack_left_pct,
        lws.ws_attack_center_pct,
        lws.ws_attack_right_pct,
        lws.ws_zone_def_pct,
        lws.ws_zone_mid_pct,
        lws.ws_zone_att_pct,
        lws.ws_shot_six_yard_pct,
        lws.ws_shot_penalty_pct,
        lws.ws_shot_oob_pct,
        lws.ws_shot_open_play_pct,
        lws.ws_shot_set_piece_pct,
        lws.ws_shot_penalty_att_pct,
        lws.ws_conversion_rate,
        lws.ws_cross_rate,
        lws.ws_through_ball_rate,
        lws.ws_long_ball_rate,
        lws.ws_short_pass_rate,
        lws.ws_def_exposed_left_pct,
        lws.ws_def_exposed_center_pct,
        lws.ws_def_exposed_right_pct,
        lws.ws_counter_attack_dna,
        lws.ws_midfield_control_idx,
        lws.ws_defensive_line_height,
        lws.ws_flank_exposure_asymm,
        CASE WHEN lws.ws_field_tilt_actions IS NOT NULL THEN 1 ELSE 0 END AS has_ws_events,

        -- Squad features (anti-leakage)
        sfb.squad_avg_form_5,
        sfb.squad_xg_quality_5,
        sfb.squad_regularity,
        sfb.squad_top3_share

    FROM backbone_base b
    LEFT JOIN latest_ws lws
        ON  b."date" = lws."date"
        AND b.team               = lws.team
        AND b.league_source      = lws.league_source
        -- AND lws.rn = 1
    LEFT JOIN squad_for_backbone sfb
        ON  CAST(b."date" AS DATE) = CAST(sfb."date" AS DATE)
        AND b.team               = sfb.team
        AND b.league_source      = sfb.league_source
        -- AND sfb.rn = 1
)

SELECT * FROM final

{% if is_incremental() %}
WHERE (date, team, league_source) NOT IN (
    SELECT date, team, league_source
    FROM {{ this }}
)
{% endif %}