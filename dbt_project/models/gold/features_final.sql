{{
    config(
        materialized='incremental',
        unique_key=['date', 'team', 'opponent', 'league_source'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_final'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

{% set w = 5 %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- BASE : features_rolling enrichi avec WhoScored et Draw
-- ══════════════════════════════════════════════════════════════════════════════
enriched AS (
    SELECT
        r.*,
        ws.ws_match_id,
        ws.ws_field_tilt_actions, ws.ws_high_turnover_rate, ws.ws_deep_completion_rt,
        ws.ws_momentum_delta, ws.ws_counter_shot_rate, ws.ws_set_piece_pressure,
        ws.ws_attack_left_pct, ws.ws_attack_center_pct, ws.ws_attack_right_pct,
        ws.ws_zone_def_pct, ws.ws_zone_mid_pct, ws.ws_zone_att_pct,
        ws.ws_counter_attack_dna, ws.ws_midfield_control_idx,
        ws.ws_defensive_line_height, ws.ws_flank_exposure_asymm,
        ws.squad_avg_form_5, ws.squad_xg_quality_5,
        ws.squad_regularity, ws.squad_top3_share, ws.has_ws_events,
        d.f1_mutual_cancel_idx, d.f2_defensive_mirror, d.f3_draw_market_dev,
        d.f4_momentum_convergence, d.f5_cs_mutual_rate,
        d.f7_off_def_mismatch, d.f7_def_off_mismatch,
        d.f8_press_dominance_ratio, d.f9_chance_quality_gap, d.f10_venue_power_adj,
        d.f11_comeback_rate, d.f12_red_card_resilience,
        d.f15_xg_yield_ratio, d.f16_def_yield_ratio,
        d.f17_shots_to_goal_eff, d.f18_sot_conversion,
        d.f19_tactical_lock_idx, d.f20_upset_composite
    FROM {{ ref('features_rolling') }} r
    LEFT JOIN {{ ref('features_whoscored') }} ws
        ON  r.date          = ws.date
        AND r.team          = ws.team
        AND r.league_source = ws.league_source
    LEFT JOIN {{ ref('features_draw') }} d
        ON  r.date          = d.date
        AND r.team          = d.team
        AND r.league_source = d.league_source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- OPPONENT STATS : stats de l'adversaire pour les différentiels
-- ══════════════════════════════════════════════════════════════════════════════
opponent_stats AS (
    SELECT
        date, team AS opp_team, league_source,
        ws_match_id AS opp_ws_match_id,
        season_att_rating           AS opp_season_att_rating,
        season_def_rating           AS opp_season_def_rating,
        ws_dribbles_pg              AS opp_ws_dribbles_pg,
        ws_fouled_pg                AS opp_ws_fouled_pg,
        ws_shots_ot_pg              AS opp_ws_shots_ot_pg,
        days_since_last_match       AS opp_rest_days,
        odds_pinnacle_team          AS opp_odds_pinnacle,
        pinnacle_prob_team          AS opp_pinnacle_prob,
        market_prob_team            AS opp_market_prob,
        draw_rate_5                 AS opp_draw_rate_5,
        form_n_defenders            AS opp_form_n_defenders,
        form_n_midfielders          AS opp_form_n_midfielders,
        form_n_attackers            AS opp_form_n_attackers,
        {% for w in [3, 5, 10] %}
        np_xg_roll_{{ w }}              AS opp_np_xg_{{ w }},
        np_xg_roll_venue_{{ w }}        AS opp_np_xg_venue_{{ w }},
        np_xg_conceded_roll_{{ w }}     AS opp_np_xg_conceded_{{ w }},
        xg_net_roll_{{ w }}             AS opp_xg_net_{{ w }},
        shot_quality_ratio_{{ w }}      AS opp_sqr_{{ w }},
        shot_accuracy_roll_{{ w }}      AS opp_shot_accuracy_{{ w }},
        ppda_roll_{{ w }}               AS opp_ppda_{{ w }},
        ppda_allowed_roll_{{ w }}       AS opp_ppda_allowed_{{ w }},
        ppda_ratio_roll_{{ w }}         AS opp_ppda_ratio_{{ w }},
        defensive_actions_roll_{{ w }}  AS opp_defensive_actions_{{ w }},
        xg_overperformance_{{ w }}      AS opp_xg_opi_{{ w }},
        save_rate_roll_{{ w }}          AS opp_save_rate_{{ w }},
        roll_save_pct_{{ w }}           AS opp_roll_save_pct_{{ w }},
        red_card_rate_roll_{{ w }}      AS opp_red_card_rate_{{ w }},
        sterility_index_{{ w }}         AS opp_sterility_index_{{ w }},
        shots_faced_per_goal_conceded_{{ w }} AS opp_shots_faced_per_goal_conceded_{{ w }},
        sterility_weighted_{{ w }}      AS opp_sterility_weighted_{{ w }},
        press_resistance_{{ w }}        AS opp_press_resistance_{{ w }},
        shield_efficiency_{{ w }}       AS opp_shield_efficiency_{{ w }},
        win_rate_roll_{{ w }}           AS opp_win_rate_{{ w }},
        points_pg_roll_{{ w }}          AS opp_points_pg_{{ w }},
        {% endfor %}
        1 AS _dummy
    FROM enriched
),

-- ══════════════════════════════════════════════════════════════════════════════
-- H2H : historique tête-à-tête sur les 10 derniers matchs
-- ══════════════════════════════════════════════════════════════════════════════
h2h_stats AS (
    SELECT
        t.date, t.team, t.opponent, t.league_source,
        AVG(CASE WHEN (h.result_1n2 = 'H' AND h.venue = 'Home') OR (h.result_1n2 = 'A' AND h.venue = 'Away') THEN 1.0 ELSE 0.0 END) AS h2h_win_rate,
        AVG(CASE WHEN h.result_1n2='D' THEN 1.0 ELSE 0.0 END) AS h2h_draw_rate,
        AVG(h.gf)                                               AS h2h_goals_scored,
        AVG(h.ga)                                               AS h2h_goals_conceded,
        AVG(COALESCE(h.np_xg - h.np_xg_conceded,
            CAST(h.gf AS DOUBLE) - CAST(h.ga AS DOUBLE)))       AS h2h_xg_diff,
        COUNT(*)                                                 AS h2h_n_matches
    FROM enriched t
    JOIN (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.team, b.opponent, b.league_source
                ORDER BY b.date DESC
            ) AS rn
        FROM {{ ref('backbone') }} b
    ) h ON  h.team          = t.team
        AND h.opponent      = t.opponent
        AND h.league_source = t.league_source
        AND h.date          < t.date
        AND h.rn            <= 10
    GROUP BY t.date, t.team, t.opponent, t.league_source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- LEAGUE DRAW RATE : taux de nul historique par ligue (saisons précédentes)
-- ══════════════════════════════════════════════════════════════════════════════
season_draw_rates AS (
    SELECT league_source, season,
        AVG(CASE WHEN result_1n2='D' THEN 1.0 ELSE 0.0 END) AS season_draw_rate
    FROM {{ ref('backbone') }}
    GROUP BY league_source, season
),

league_draw_rate AS (
    SELECT s1.league_source, s1.season,
        AVG(s2.season_draw_rate) AS league_draw_rate
    FROM season_draw_rates s1
    JOIN season_draw_rates s2
        ON  s2.league_source = s1.league_source
        AND s2.season        < s1.season
    GROUP BY s1.league_source, s1.season
),

-- ══════════════════════════════════════════════════════════════════════════════
-- ASSEMBLAGE FINAL
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        -- Identifiants
        t.date, t.team, t.opponent, t.venue, t.is_home, t.ws_match_id,
        t.season, t.league_source, t.comp_category, t.match_id, t.result_1n2,
        t.formation,

        -- Contexte match
        t.days_since_last_match, t.is_return_from_break, t.is_short_rest,
        t.season_att_rating, t.season_def_rating,
        t.ws_dribbles_pg, t.ws_fouled_pg, t.ws_shots_ot_pg,
        o.opp_season_att_rating, o.opp_season_def_rating,
        o.opp_ws_dribbles_pg, o.opp_ws_shots_ot_pg,
        o.opp_odds_pinnacle, o.opp_pinnacle_prob, o.opp_market_prob,

        -- Rolling windows équipe + adversaire + différentiels
        {% for w in [3, 5, 10] %}
        t.np_xg_roll_{{ w }},
        t.np_xg_roll_venue_{{ w }},
        t.np_xg_conceded_roll_{{ w }},
        t.xg_net_roll_{{ w }},
        t.shot_quality_ratio_{{ w }},
        t.shot_quality_ratio_venue_{{ w }},
        t.shot_accuracy_roll_{{ w }},
        t.save_rate_roll_{{ w }},
        t.poss_roll_{{ w }},
        t.poss_roll_venue_{{ w }},
        t.ppda_roll_{{ w }},
        t.ppda_allowed_roll_{{ w }},
        t.ppda_ratio_roll_{{ w }},
        t.defensive_actions_roll_{{ w }},
        t.fouls_per_tackle_roll_{{ w }},
        t.xg_overperformance_{{ w }},
        t.red_card_rate_roll_{{ w }},
        t.sterility_index_{{ w }},
        t.shots_faced_per_goal_conceded_{{ w }},
        t.sterility_weighted_{{ w }},
        t.press_resistance_{{ w }},
        t.shield_efficiency_{{ w }},
        t.roll_save_pct_{{ w }},
        t.roll_sota_{{ w }},
        t.win_rate_roll_{{ w }},
        t.points_pg_roll_{{ w }},
        o.opp_np_xg_{{ w }},
        o.opp_np_xg_venue_{{ w }},
        o.opp_np_xg_conceded_{{ w }},
        o.opp_xg_net_{{ w }},
        o.opp_sqr_{{ w }},
        o.opp_shot_accuracy_{{ w }},
        o.opp_ppda_{{ w }},
        o.opp_ppda_allowed_{{ w }},
        o.opp_ppda_ratio_{{ w }},
        o.opp_defensive_actions_{{ w }},
        o.opp_xg_opi_{{ w }},
        o.opp_save_rate_{{ w }},
        o.opp_red_card_rate_{{ w }},
        o.opp_win_rate_{{ w }},
        o.opp_points_pg_{{ w }},
        (t.xg_net_roll_{{ w }}             - o.opp_xg_net_{{ w }})             AS xg_net_diff_{{ w }},
        (t.shot_quality_ratio_{{ w }}      - o.opp_sqr_{{ w }})                AS sqr_diff_{{ w }},
        (t.ppda_roll_{{ w }}               - o.opp_ppda_{{ w }})               AS ppda_diff_{{ w }},
        (t.ppda_ratio_roll_{{ w }}         - o.opp_ppda_ratio_{{ w }})         AS ppda_ratio_diff_{{ w }},
        (t.xg_overperformance_{{ w }}      - o.opp_xg_opi_{{ w }})             AS xg_opi_diff_{{ w }},
        (t.save_rate_roll_{{ w }}          - o.opp_save_rate_{{ w }})          AS save_rate_diff_{{ w }},
        (t.defensive_actions_roll_{{ w }}  - o.opp_defensive_actions_{{ w }})  AS defensive_actions_diff_{{ w }},
        (t.roll_save_pct_{{ w }}           - o.opp_roll_save_pct_{{ w }})      AS keeper_form_diff_{{ w }},
        (t.red_card_rate_roll_{{ w }}      - o.opp_red_card_rate_{{ w }})      AS red_card_rate_diff_{{ w }},
        (o.opp_sterility_index_{{ w }}     - t.sterility_index_{{ w }})        AS sterility_diff_{{ w }},
        (t.shots_faced_per_goal_conceded_{{ w }} - o.opp_shots_faced_per_goal_conceded_{{ w }}) AS shots_faced_per_goal_conceded_diff_{{ w }},
        (t.sterility_weighted_{{ w }}      - o.opp_sterility_weighted_{{ w }}) AS sterility_weighted_diff_{{ w }},
        (t.press_resistance_{{ w }}        - o.opp_press_resistance_{{ w }})   AS press_resistance_diff_{{ w }},
        (t.shield_efficiency_{{ w }}       - o.opp_shield_efficiency_{{ w }})  AS shield_efficiency_diff_{{ w }},
        (t.win_rate_roll_{{ w }}           - o.opp_win_rate_{{ w }})           AS win_rate_diff_{{ w }},
        (t.points_pg_roll_{{ w }}          - o.opp_points_pg_{{ w }})          AS points_pg_diff_{{ w }},
        {% endfor %}

        -- Différentiels globaux (W=5)
        (t.xg_net_roll_5            - o.opp_xg_net_5)            AS xg_net_diff,
        (t.season_att_rating        - o.opp_season_def_rating)   AS tactical_advantage,
        (t.ws_dribbles_pg           - o.opp_ws_dribbles_pg)      AS ws_dribble_style_diff,
        (t.ws_fouled_pg             - o.opp_ws_fouled_pg)        AS ws_fouled_diff,
        (t.shot_quality_ratio_5     - o.opp_sqr_5)               AS sqr_diff,
        (t.ppda_roll_5              - o.opp_ppda_5)              AS ppda_diff,
        (t.ppda_ratio_roll_5        - o.opp_ppda_ratio_5)        AS ppda_ratio_diff,
        (t.xg_overperformance_5     - o.opp_xg_opi_5)            AS xg_opi_diff,
        (t.save_rate_roll_5         - o.opp_save_rate_5)         AS save_rate_diff,
        (t.defensive_actions_roll_5 - o.opp_defensive_actions_5) AS defensive_actions_diff,
        (t.roll_save_pct_5          - o.opp_roll_save_pct_5)     AS keeper_form_diff,
        (t.red_card_rate_roll_5     - o.opp_red_card_rate_5)     AS red_card_rate_diff,
        (o.opp_sterility_index_5    - t.sterility_index_5)       AS sterility_diff,
        (t.press_resistance_5       - o.opp_press_resistance_5)  AS press_resistance_diff,
        (t.shield_efficiency_5      - o.opp_shield_efficiency_5) AS shield_efficiency_diff,
        (t.days_since_last_match    - o.opp_rest_days)           AS rest_days_diff,
        (t.draw_rate_5  - o.opp_draw_rate_5)                     AS draw_rate_diff,
        (t.draw_rate_5  * o.opp_draw_rate_5)                     AS draw_affinity,
        (CAST(t.form_n_attackers   AS DOUBLE) - CAST(o.opp_form_n_defenders   AS DOUBLE)) AS form_att_vs_def_gap,
        (CAST(t.form_n_midfielders AS DOUBLE) - CAST(o.opp_form_n_midfielders AS DOUBLE)) AS form_mid_dominance,

        -- Cotes et probabilités
        t.odds_pinnacle_team, t.odds_pinnacle_draw, t.odds_pinnacle_opp,
        t.odds_avg_team, t.odds_avg_draw, t.odds_avg_opp,
        t.pinnacle_prob_team, t.pinnacle_prob_draw, t.pinnacle_prob_opp,
        t.market_prob_team, t.market_prob_draw, t.market_prob_opp,
        (t.pinnacle_prob_team - t.pinnacle_prob_opp) AS pinnacle_edge,
        (t.market_prob_team   - t.market_prob_opp)   AS market_edge,

        -- H2H
        h.h2h_win_rate, h.h2h_draw_rate,
        h.h2h_goals_scored, h.h2h_goals_conceded,
        h.h2h_xg_diff, h.h2h_n_matches,

        -- League draw rate
        ldr.league_draw_rate,

        -- Giant Killer
        CASE WHEN o.opp_season_att_rating IS NOT NULL AND o.opp_season_att_rating <> 0
             THEN t.season_att_rating / o.opp_season_att_rating ELSE NULL END AS rating_ratio_att,
        CASE WHEN o.opp_season_def_rating IS NOT NULL AND o.opp_season_def_rating <> 0
             THEN t.season_def_rating / o.opp_season_def_rating ELSE NULL END AS rating_ratio_def,
        CASE WHEN o.opp_season_att_rating IS NOT NULL AND o.opp_season_att_rating <> 0
              AND o.opp_season_def_rating IS NOT NULL AND o.opp_season_def_rating <> 0
             THEN GREATEST(
                 t.season_att_rating / o.opp_season_att_rating,
                 t.season_def_rating / o.opp_season_def_rating
             ) * t.is_home ELSE NULL END AS upset_risk_index,

        -- -- final_match_id
        -- 'fbref_' || LEFT(md5(
        --     CAST(t.date AS VARCHAR) || '|' ||
        --     LEAST(t.team, t.opponent) || '|' ||
        --     GREATEST(t.team, t.opponent) || '|' || t.league_source
        -- ), 10) AS final_match_id,

        -- WhoScored features
        t.ws_field_tilt_actions, t.ws_high_turnover_rate, t.ws_deep_completion_rt,
        t.ws_momentum_delta, t.ws_counter_shot_rate, t.ws_set_piece_pressure,
        t.ws_attack_left_pct, t.ws_attack_center_pct, t.ws_attack_right_pct,
        t.ws_zone_def_pct, t.ws_zone_mid_pct, t.ws_zone_att_pct,
        t.ws_counter_attack_dna, t.ws_midfield_control_idx,
        t.ws_defensive_line_height, t.ws_flank_exposure_asymm,
        t.squad_avg_form_5, t.squad_xg_quality_5,
        t.squad_regularity, t.squad_top3_share, t.has_ws_events,

        -- Draw features
        t.f1_mutual_cancel_idx, t.f2_defensive_mirror, t.f3_draw_market_dev,
        t.f4_momentum_convergence, t.f5_cs_mutual_rate,
        t.f7_off_def_mismatch, t.f7_def_off_mismatch,
        t.f8_press_dominance_ratio, t.f9_chance_quality_gap, t.f10_venue_power_adj,
        t.f11_comeback_rate, t.f12_red_card_resilience,
        t.f15_xg_yield_ratio, t.f16_def_yield_ratio,
        t.f17_shots_to_goal_eff, t.f18_sot_conversion,
        t.f19_tactical_lock_idx, t.f20_upset_composite,

        -- Formation features
        t.form_n_defenders, t.form_n_midfielders, t.form_n_attackers,
        t.form_familiarity_5, t.form_change_flag,
        t.home_win_rate_hist

    FROM enriched t
    LEFT JOIN opponent_stats o
        ON  t.date          = o.date
        AND t.opponent      = o.opp_team
        AND t.league_source = o.league_source
    LEFT JOIN h2h_stats h
        ON  t.date          = h.date
        AND t.team          = h.team
        AND t.opponent      = h.opponent
        AND t.league_source = h.league_source
    LEFT JOIN league_draw_rate ldr
        ON  t.league_source = ldr.league_source
        AND t.season        = ldr.season
)

SELECT * FROM final

{% if is_incremental() %}
WHERE (date::VARCHAR || '_' || team || '_' || opponent || '_' || league_source) NOT IN (
    SELECT (date::VARCHAR || '_' || team || '_' || opponent || '_' || league_source)
    FROM {{ this }}
)
{% endif %}