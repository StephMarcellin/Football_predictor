{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_draw'
    )
}}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- BASE : rolling stats depuis intermediate.backbone
-- ══════════════════════════════════════════════════════════════════════════════
base AS (
    SELECT DISTINCT match_id, team_id, opponent_id, date, league_source, venue, season
    FROM {{ ref('backbone') }}
),

rolling_stats AS (
    SELECT
        match_id,
        team_id,
        opponent_id,

        avg_gf_5,
        avg_ga_5,
        np_xg_roll_5           AS avg_xg_5,
        np_xg_roll_venue_5     AS avg_xg_venue_5,
        np_xg_conceded_roll_5  AS avg_xg_conceded_5,

        shot_quality_ratio_5   AS sqr_5,
        avg_shots_5,
        avg_sot_5,
        cs_rate_5,
        save_rate_roll_5       AS avg_save_rate_5,
        ppda_roll_5            AS avg_ppda_5,

        draw_rate_5,
        season_att_rating,
        season_def_rating,
        pinnacle_prob_draw,
        pinnacle_prob_team,

        red_card_rate_roll_5   AS red_card_rate_5,
        sterility_index_5      AS sterility_5,

    FROM {{ ref('features_rolling') }}
),

-- ══ H1 : Late Equalizer Rate ══════════════════════════════════════════════════
late_equalizer_raw AS (
    SELECT
        e.match_id, e.team_id,
        e.expanded_minute,
        SUM(CASE WHEN g2.team_id = e.team_id  THEN 1 ELSE 0 END)
            OVER (PARTITION BY e.match_id, e.team_id
                  ORDER BY e.expanded_minute) AS goals_for_cumul,
        SUM(CASE WHEN g2.team_id != e.team_id THEN 1 ELSE 0 END)
            OVER (PARTITION BY e.match_id, e.team_id
                  ORDER BY e.expanded_minute) AS goals_against_cumul
    FROM {{ ref('int_whoscored_events') }} e
    JOIN {{ ref('int_whoscored_events') }} g2
        ON g2.match_id = e.match_id
        AND g2.expanded_minute <= e.expanded_minute
        AND g2.type_id = 16 AND g2.outcome_id = 1 AND g2.is_shot = TRUE
    WHERE e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE
),

late_equalizer_match AS (
    SELECT match_id, team_id,
        MAX(CASE WHEN expanded_minute >= 70
                  AND goals_against_cumul > (goals_for_cumul - 1)
                  AND goals_for_cumul = goals_against_cumul
             THEN 1 ELSE 0 END) AS had_late_equalizer
    FROM late_equalizer_raw
    GROUP BY match_id, team_id
),

-- ══ H2 : Post Yellow Card Concede Rate ════════════════════════════════════════
yellow_cards_events AS (
    SELECT match_id, team_id, expanded_minute AS card_minute
    FROM {{ ref('events_qual') }}
    WHERE type_name = 'Card'
      AND qual_type_name = 'Yellow'
),

goals_events AS (
    SELECT match_id, team_id, expanded_minute
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 16 AND outcome_id = 1 AND is_shot = TRUE
),

post_yellow_match AS (
    SELECT yc.match_id, yc.team_id,
        MAX(CASE WHEN g.match_id IS NOT NULL THEN 1 ELSE 0 END) AS conceded_after_yellow,
        1 AS had_yellow_card
    FROM yellow_cards_events yc
    LEFT JOIN goals_events g
        ON g.match_id = yc.match_id
        AND g.team_id != yc.team_id
        AND g.expanded_minute > yc.card_minute
        AND g.expanded_minute <= yc.card_minute + 10
    GROUP BY yc.match_id, yc.team_id
),

-- ══ H3 : Post Red Card Resilience ════════════════════════════════════════════
red_cards_events AS (
    SELECT match_id, team_id, expanded_minute AS red_minute
    FROM {{ ref('events_qual') }}
    WHERE type_name = 'Card'
      AND qual_type_name IN ('Red', 'SecondYellow')
),

offensive_touches_events AS (
    SELECT match_id, team_id, expanded_minute
    FROM {{ref('int_whoscored_events') }}
    WHERE is_touch = TRUE AND x > 50
),

post_red_match AS (
    SELECT match_id, team_id,
        AVG(CASE WHEN off_before > 0
                 THEN CAST(off_after AS DOUBLE) / off_before
                 WHEN off_after > 0 THEN 1.0
                 ELSE NULL END) AS resilience_ratio
    FROM (
        SELECT
            rc.match_id, rc.team_id, rc.red_minute,
            COUNT(e.expanded_minute) FILTER (
                WHERE e.expanded_minute >= rc.red_minute - 10
                  AND e.expanded_minute <  rc.red_minute
            ) AS off_before,
            COUNT(e.expanded_minute) FILTER (
                WHERE e.expanded_minute >  rc.red_minute
                  AND e.expanded_minute <= rc.red_minute + 10
            ) AS off_after
        FROM red_cards_events rc
        JOIN offensive_touches_events e
            ON e.match_id = rc.match_id
            AND e.team_id    = rc.team_id
            AND e.expanded_minute BETWEEN rc.red_minute - 10 AND rc.red_minute + 10
        GROUP BY rc.match_id, rc.team_id, rc.red_minute
    ) sub
    GROUP BY match_id, team_id
),

red_card_stats AS (
    SELECT
        team_id,
        date,
        league_source,
        season,
        SUM(CASE WHEN red_cards > 0 AND result_1n2 = 'H' AND venue = 'Home' THEN 3.0
                 WHEN red_cards > 0 AND result_1n2 = 'A' AND venue = 'Away' THEN 3.0
                 WHEN red_cards > 0 AND result_1n2 = 'D' THEN 1.0
                 ELSE 0.0 END)
            OVER (PARTITION BY team_id, league_source, season ORDER BY date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pts_with_red_card,
        SUM(CASE WHEN red_cards > 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY team_id, league_source, season ORDER BY date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS n_matches_with_red
    FROM {{ ref('backbone') }}
),

-- ══ Pivot draw behavior home/away → team_name ════════════════════════════════
draw_behavior_pivot AS (
    SELECT
        m.match_id,
        m.team_id,
        m.match_date AS ws_date,
        m.season AS ws_season,
        m.league_source,
        COALESCE(le.had_late_equalizer, 0) AS had_late_equalizer,
        py.conceded_after_yellow,
        py.had_yellow_card,
        pr.resilience_ratio AS red_card_resilience
    FROM {{ ref('int_whoscored_match_index') }} m
    LEFT JOIN late_equalizer_match le
        ON m.match_id = le.match_id AND m.team_id = le.team_id
    LEFT JOIN post_yellow_match py
        ON m.match_id = py.match_id AND m.team_id = py.team_id
    LEFT JOIN post_red_match pr
        ON m.match_id = pr.match_id AND m.team_id = pr.team_id

    UNION ALL

    SELECT
        m.match_id,
        m.opponent_id AS team_id,
        m.match_date AS ws_date,
        m.season AS ws_season,
        m.league_source,
        COALESCE(le.had_late_equalizer, 0) AS had_late_equalizer,
        py.conceded_after_yellow,
        py.had_yellow_card,
        pr.resilience_ratio AS red_card_resilience
    FROM {{ ref('int_whoscored_match_index') }} m
    LEFT JOIN late_equalizer_match le
        ON m.match_id = le.match_id AND m.opponent_id = le.team_id
    LEFT JOIN post_yellow_match py
        ON m.match_id = py.match_id AND m.opponent_id = py.team_id
    LEFT JOIN post_red_match pr
        ON m.match_id = pr.match_id AND m.opponent_id = pr.team_id
),

-- Anti-leakage : dernier match WhoScored AVANT la date backbone
draw_behavior_latest_date AS (
    SELECT
        b.match_id,
        b.date, b.team_id, b.league_source,
        MAX(dbp.ws_date) AS max_ws_date
    FROM {{ ref('backbone') }} b
    JOIN draw_behavior_pivot dbp
        ON  dbp.team_id    = b.team_id
        AND dbp.league_source = b.league_source
        AND dbp.ws_date      < b.date
    GROUP BY b.match_id, b.date, b.team_id, b.league_source
),

draw_behavior AS (
    SELECT
        d.match_id,
        d.date, d.team_id, d.league_source,
        AVG(dbp.had_late_equalizer)    AS ws_late_equalizer_rate,
        AVG(dbp.conceded_after_yellow) AS ws_post_yellowcard_concede_rate,
        AVG(dbp.red_card_resilience)   AS ws_post_redcard_resilience
    FROM draw_behavior_latest_date d
    JOIN draw_behavior_pivot dbp
        ON  dbp.team_id    = d.team_id
        AND dbp.league_source = d.league_source
        AND dbp.ws_date      = d.max_ws_date
    GROUP BY d.match_id, d.team_id, d.date, d.league_source
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


comeback_stats AS (
    SELECT
        team_id,
        date,
        match_id,
        league_source,
        NULL::DOUBLE AS comeback_rate
    FROM {{ ref('backbone') }}
),

-- market_probs AS (
--     SELECT match_id, team_id, league_source, pinnacle_prob_draw, pinnacle_prob_team, market_prob_draw
--     FROM {{ ref('backbone') }}
-- ),

backbone_features AS (
    SELECT
        b.match_id,
        b.date, b.team_id, b.opponent_id, b.league_source, b.venue, b.season,

        r.avg_gf_5,
        r.avg_ga_5,
        r.avg_xg_5,
        r.avg_xg_conceded_5,
        r.avg_shots_5,
        r.avg_sot_5,
        r.sqr_5,
        r.cs_rate_5,
        r.avg_save_rate_5,
        r.avg_ppda_5,
        r.red_card_rate_5,
        r.sterility_5,
        r.avg_xg_venue_5,
        r.draw_rate_5,
        r.season_att_rating,
        r.season_def_rating,

        rc.pts_with_red_card,
        rc.n_matches_with_red,

        ld.league_draw_rate,
        cb.comeback_rate,

        r.pinnacle_prob_draw,
        r.pinnacle_prob_team,

        db.ws_late_equalizer_rate,
        db.ws_post_yellowcard_concede_rate,
        db.ws_post_redcard_resilience

    FROM base b
    LEFT JOIN rolling_stats    r  ON b.match_id = r.match_id AND b.team_id = r.team_id
    LEFT JOIN red_card_stats   rc ON b.team_id = rc.team_id AND b.season = rc.season AND b.league_source = rc.league_source
    LEFT JOIN league_draw_rate ld ON b.league_source = ld.league_source AND b.season = ld.season
    LEFT JOIN comeback_stats   cb ON b.match_id = cb.match_id AND b.team_id = cb.team_id
    LEFT JOIN draw_behavior    db ON b.match_id = db.match_id AND b.team_id = db.team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- WS ROLLING : late_goal_tendency, goal_timing_variance, ht_draw_rate
-- Ces features viennent de features_rolling (ws_momentum_delta, ws_zone_att_pct)
-- Les features timing WhoScored (F6/F13/F14) seront NULL sans stg_whoscored_events
-- ══════════════════════════════════════════════════════════════════════════════
ws_features AS (
    SELECT
        match_id,
        team_id,
        ws_momentum_delta,
        ws_zone_att_pct,
        -- F6/F13/F14 restent NULL jusqu'à migration whoscored.py
        CAST(NULL AS DOUBLE) AS late_goal_tendency,
        CAST(NULL AS DOUBLE) AS goal_timing_variance,
        CAST(NULL AS DOUBLE) AS ht_draw_rate
    FROM {{ ref('team_features_ws') }}
),

-- ══════════════════════════════════════════════════════════════════════════════
-- team_idVS OPP : jointure équipe × adversaire pour les features comparatives
-- ══════════════════════════════════════════════════════════════════════════════
team_vs_opp AS (
    SELECT
        t.match_id,
        t.date, t.team_id, t.opponent_id,
        t.league_source, t.venue, t.season,

        -- Features équipe
        t.avg_gf_5, t.avg_ga_5, t.avg_xg_5, t.avg_xg_conceded_5,
        t.avg_shots_5, t.avg_sot_5, t.sqr_5, t.cs_rate_5,
        t.avg_save_rate_5, t.avg_ppda_5, t.red_card_rate_5,
        t.pts_with_red_card, t.n_matches_with_red, t.sterility_5,
        t.season_att_rating, t.season_def_rating,
        t.avg_xg_venue_5, t.league_draw_rate, t.comeback_rate,
        
        t.pinnacle_prob_draw, t.pinnacle_prob_team,

        -- Features adversaire
        o.avg_gf_5              AS opp_avg_gf_5,
        o.avg_ga_5              AS opp_avg_ga_5,
        o.avg_xg_5              AS opp_avg_xg_5,
        o.avg_xg_conceded_5     AS opp_avg_xg_conceded_5,
        o.sqr_5                 AS opp_sqr_5,
        o.cs_rate_5             AS opp_cs_rate_5,
        o.avg_save_rate_5       AS opp_avg_save_rate_5,
        o.avg_ppda_5            AS opp_avg_ppda_5,
        o.sterility_5           AS opp_sterility_5,
        o.season_att_rating     AS opp_season_att_rating,
        o.season_def_rating     AS opp_season_def_rating,
        o.comeback_rate         AS opp_comeback_rate,
        o.pinnacle_prob_team   AS opp_pinnacle_prob_team,

        -- WhoScored features (équipe et adversaire)
        ws.ws_momentum_delta,
        ws.ws_zone_att_pct,
        ws_opp.ws_momentum_delta    AS opp_ws_momentum_delta,
        ws_opp.ws_zone_att_pct      AS opp_ws_zone_att_pct,

        -- H1/H2/H3
        t.ws_late_equalizer_rate,
        t.ws_post_yellowcard_concede_rate,
        t.ws_post_redcard_resilience

    FROM backbone_features t

    LEFT JOIN backbone_features o
        ON  t.date          = o.date
        AND t.opponent_id   = o.team_id
        AND t.league_source = o.league_source

    LEFT JOIN ws_features ws
        ON  ws.match_id      = t.match_id
        AND ws.team_id        = t.team_id

    LEFT JOIN ws_features ws_opp
        ON  ws_opp.match_id      = t.match_id
        AND ws_opp.team_id        = t.opponent_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- F1–F20 : calcul des 20 features draw signals
-- ══════════════════════════════════════════════════════════════════════════════
f_computed AS (
    SELECT
        tvo.match_id,
        tvo.date, tvo.team_id, tvo.league_source,
        tvo.opponent_id, tvo.venue, tvo.season,

        -- AXE 1 — Détecteurs de match bloqué (F1–F5)
        CASE WHEN (tvo.avg_save_rate_5 + tvo.opp_avg_save_rate_5) > 0
              AND tvo.sterility_5 IS NOT NULL AND tvo.opp_sterility_5 IS NOT NULL
            THEN (tvo.sterility_5 * tvo.opp_sterility_5)
                 / (tvo.avg_save_rate_5 + tvo.opp_avg_save_rate_5 + 0.01)
        END AS f1_mutual_cancel_idx,

        CASE WHEN tvo.ws_zone_att_pct IS NOT NULL AND tvo.opp_ws_zone_att_pct IS NOT NULL
            THEN 1.0 - ABS(tvo.ws_zone_att_pct - tvo.opp_ws_zone_att_pct)
        END AS f2_defensive_mirror,

        CASE WHEN tvo.pinnacle_prob_draw IS NOT NULL AND tvo.league_draw_rate IS NOT NULL
            THEN tvo.pinnacle_prob_draw - tvo.league_draw_rate
        END AS f3_draw_market_dev,

        CASE WHEN tvo.ws_momentum_delta IS NOT NULL AND tvo.opp_ws_momentum_delta IS NOT NULL
            THEN 1.0 - ABS(tvo.ws_momentum_delta - tvo.opp_ws_momentum_delta)
        END AS f4_momentum_convergence,

        CASE WHEN tvo.cs_rate_5 IS NOT NULL AND tvo.opp_cs_rate_5 IS NOT NULL
            THEN tvo.cs_rate_5 * tvo.opp_cs_rate_5
        END AS f5_cs_mutual_rate,

        -- F6 — HT Draw Tendency (depuis stg_whoscored_match_details via events)
        tvo.ws_late_equalizer_rate AS f6_ht_draw_tendency,

        -- AXE 2 — Domination relative (F7–F10)
        CASE WHEN tvo.season_att_rating IS NOT NULL AND tvo.opp_season_def_rating IS NOT NULL
            THEN tvo.season_att_rating - tvo.opp_season_def_rating
        END AS f7_off_def_mismatch,

        CASE WHEN tvo.opp_season_att_rating IS NOT NULL AND tvo.season_def_rating IS NOT NULL
            THEN tvo.opp_season_att_rating - tvo.season_def_rating
        END AS f7_def_off_mismatch,

        CASE WHEN tvo.avg_ppda_5 > 0 AND tvo.opp_avg_ppda_5 > 0
            THEN LN(tvo.opp_avg_ppda_5 / tvo.avg_ppda_5)
        END AS f8_press_dominance_ratio,

        CASE WHEN tvo.sqr_5 IS NOT NULL AND tvo.opp_sqr_5 IS NOT NULL
            THEN tvo.sqr_5 - tvo.opp_sqr_5
        END AS f9_chance_quality_gap,

        CASE WHEN tvo.avg_xg_venue_5 IS NOT NULL AND tvo.avg_xg_5 IS NOT NULL
            THEN tvo.avg_xg_venue_5 - tvo.avg_xg_5
        END AS f10_venue_power_adj,

        -- AXE 3 — Résilience & psychologie (F11–F14)
        tvo.comeback_rate AS f11_comeback_rate,

        CASE WHEN tvo.n_matches_with_red > 0
            THEN tvo.pts_with_red_card / tvo.n_matches_with_red
        END AS f12_red_card_resilience,

        -- F13/F14 — Late goal tendency et variance
        NULL::DOUBLE        AS f13_late_goal_tendency,
        NULL::DOUBLE        AS f14_goal_timing_variance,

        -- AXE 4 — Efficacité / yield (F15–F18)
        CASE WHEN tvo.avg_xg_5 > 0
            THEN tvo.avg_gf_5 / tvo.avg_xg_5
        END AS f15_xg_yield_ratio,

        CASE WHEN tvo.avg_xg_conceded_5 > 0
            THEN tvo.avg_ga_5 / tvo.avg_xg_conceded_5
        END AS f16_def_yield_ratio,

        CASE WHEN tvo.avg_shots_5 > 0
            THEN tvo.avg_gf_5 / tvo.avg_shots_5
        END AS f17_shots_to_goal_eff,

        CASE WHEN tvo.avg_sot_5 > 0
            THEN tvo.avg_gf_5 / tvo.avg_sot_5
        END AS f18_sot_conversion,

        -- AXE 5 — Composites signatures (F19–F20)
        CASE WHEN tvo.sterility_5 IS NOT NULL AND tvo.opp_sterility_5 IS NOT NULL
              AND tvo.avg_ppda_5 IS NOT NULL AND tvo.opp_avg_ppda_5 IS NOT NULL
            THEN (tvo.sterility_5 + tvo.opp_sterility_5)
                 * (1.0 / (ABS(tvo.avg_ppda_5 - tvo.opp_avg_ppda_5) + 0.5))
                 * (1.0 - ABS(COALESCE(tvo.ws_zone_att_pct, 0.33)
                              - COALESCE(tvo.opp_ws_zone_att_pct, 0.33)))
        END AS f19_tactical_lock_idx,

        CASE WHEN tvo.pinnacle_prob_team > 0
              AND tvo.avg_xg_5 > 0 AND tvo.opp_avg_xg_5 > 0
            THEN (1.0 / tvo.pinnacle_prob_team)
                 * ((tvo.opp_avg_gf_5 / NULLIF(tvo.opp_avg_xg_5, 0))
                    / NULLIF((tvo.avg_gf_5 / NULLIF(tvo.avg_xg_5, 0)), 0))
                 * COALESCE(tvo.opp_comeback_rate, 0.3)
        END AS f20_upset_composite

    FROM team_vs_opp tvo
)

SELECT * FROM f_computed

{% if is_incremental() %}
WHERE (date::VARCHAR || '_' || team_id|| '_' || league_source) NOT IN (
    SELECT (date::VARCHAR || '_' || team_id|| '_' || league_source)
    FROM {{ this }}
)
{% endif %}