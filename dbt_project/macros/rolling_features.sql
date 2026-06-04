{% macro rolling_cols(w) %}
    -- ══ ROLLING W={{ w }} ══════════════════════════════════════════════
    {% set fg %}PARTITION BY team, season, league_source ORDER BY date ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}
    {% set fv %}PARTITION BY team, season, league_source, venue ORDER BY date ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}

    AVG(np_xg)                    OVER ({{ fg }}) AS np_xg_roll_{{ w }},
    AVG(np_xg)                    OVER ({{ fv }}) AS np_xg_roll_venue_{{ w }},
    AVG(np_xg_conceded)           OVER ({{ fg }}) AS np_xg_conceded_roll_{{ w }},
    AVG(np_xg - np_xg_conceded)   OVER ({{ fg }}) AS xg_net_roll_{{ w }},
    AVG(np_xg)                    OVER ({{ fg }}) / NULLIF(AVG(shots_total) OVER ({{ fg }}), 0) AS shot_quality_ratio_{{ w }},
    AVG(np_xg)                    OVER ({{ fv }}) / NULLIF(AVG(shots_total) OVER ({{ fv }}), 0) AS shot_quality_ratio_venue_{{ w }},
    AVG(shots_on_target)          OVER ({{ fg }}) / NULLIF(AVG(shots_total) OVER ({{ fg }}), 0) AS shot_accuracy_roll_{{ w }},
    AVG(saves)                    OVER ({{ fg }}) / NULLIF(AVG(shots_on_target_faced) OVER ({{ fg }}), 0) AS save_rate_roll_{{ w }},
    AVG(save_pct)                 OVER ({{ fg }}) AS roll_save_pct_{{ w }},
    AVG(shots_on_target_faced)    OVER ({{ fg }}) AS roll_sota_{{ w }},
    AVG(possession)               OVER ({{ fg }}) AS poss_roll_{{ w }},
    AVG(possession)               OVER ({{ fv }}) AS poss_roll_venue_{{ w }},
    AVG(ppda)                     OVER ({{ fg }}) AS ppda_roll_{{ w }},
    AVG(ppda_allowed)             OVER ({{ fg }}) AS ppda_allowed_roll_{{ w }},
    AVG(ppda)                     OVER ({{ fg }}) / NULLIF(AVG(ppda_allowed) OVER ({{ fg }}), 0) AS ppda_ratio_roll_{{ w }},
    AVG(tackles_won + interceptions) OVER ({{ fg }}) AS defensive_actions_roll_{{ w }},
    AVG(fouls_committed)          OVER ({{ fg }}) / NULLIF(AVG(tackles_won) OVER ({{ fg }}), 0) AS fouls_per_tackle_roll_{{ w }},
    AVG(gf)                       OVER ({{ fg }}) / NULLIF(AVG(np_xg) OVER ({{ fg }}), 0) - 1 AS xg_overperformance_{{ w }},
    AVG(possession)               OVER ({{ fg }}) / NULLIF(AVG(np_xg) OVER ({{ fg }}) / NULLIF(AVG(shots_total) OVER ({{ fg }}), 0), 0) AS sterility_index_{{ w }},
    AVG(shots_on_target_faced)    OVER ({{ fg }}) / NULLIF(AVG(ga) OVER ({{ fg }}), 0) AS shots_faced_per_goal_conceded_{{ w }},
    (AVG(possession) OVER ({{ fg }}) / 50.0) * (AVG(possession) OVER ({{ fg }}) / NULLIF(AVG(np_xg) OVER ({{ fg }}) / NULLIF(AVG(shots_total) OVER ({{ fg }}), 0), 0)) AS sterility_weighted_{{ w }},
    AVG(possession)               OVER ({{ fg }}) / NULLIF(AVG(ppda_allowed) OVER ({{ fg }}), 0) AS press_resistance_{{ w }},
    AVG(save_pct)                 OVER ({{ fg }}) / NULLIF(1 + AVG(np_xg_conceded) OVER ({{ fg }}), 0) AS shield_efficiency_{{ w }},
    AVG(CASE WHEN (red_cards + second_yellow_cards) > 0 THEN 1.0 ELSE 0.0 END) OVER ({{ fg }}) AS red_card_rate_roll_{{ w }},
    AVG(CASE WHEN (result_1n2 = 'H' AND venue = 'Home') OR (result_1n2 = 'A' AND venue = 'Away') THEN 1.0 ELSE 0.0 END) OVER ({{ fg }}) AS win_rate_roll_{{ w }},
    AVG(CASE WHEN (result_1n2 = 'H' AND venue = 'Home') OR (result_1n2 = 'A' AND venue = 'Away') THEN 3.0
         WHEN result_1n2 = 'D' THEN 1.0
         ELSE 0.0 END) OVER ({{ fg }}) AS points_pg_roll_{{ w }},
{% endmacro %}