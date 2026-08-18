{#
    Macro roll_equipe_match(w)
    ---------------------------
    Émet toutes les colonnes ROLLING(w) au grain équipe-match, avec les NOMS
    CANONIQUES du CDC (familles 2, 3, et partie native de la 8).

    Garde-fou anti-leakage (règle non négociable du CDC) :
        ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING
    → la fenêtre s'arrête au match PRÉCÉDENT (1 PRECEDING). Le match courant
      n'est JAMAIS inclus.

    Partition : (team_id, season, league_source) → la forme est propre à une
    saison et à une compétition (on ne mélange pas la forme en L1 avec un match
    de coupe). AVG() ignore nativement les NULL → np_xg absent (coupes/Europe,
    cf. vérif bonus) est automatiquement exclu, sans imputation.
#}
{% macro roll_equipe_match(w) %}
    {% set f %}PARTITION BY team_id, season, league_source ORDER BY date, match_id ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}

    -- ══ Famille 2 — Forme & momentum (w={{ w }}) ══════════════════════════
    AVG(gf)                        OVER ({{ f }}) AS avg_gf_rolling_{{ w }},
    AVG(ga)                        OVER ({{ f }}) AS avg_ga_rolling_{{ w }},
    AVG(np_xg)                     OVER ({{ f }}) AS avg_np_xg_rolling_{{ w }},
    AVG(np_xg_conceded)            OVER ({{ f }}) AS avg_np_xg_conceded_rolling_{{ w }},
    AVG(np_xg_diff_match)          OVER ({{ f }}) AS avg_np_xg_diff_rolling_{{ w }},
    AVG(points_match)              OVER ({{ f }}) AS points_rolling_{{ w }},
    AVG(win_flag)                  OVER ({{ f }}) AS win_rate_rolling_{{ w }},
    AVG(draw_flag)                 OVER ({{ f }}) AS draw_rate_rolling_{{ w }},
    AVG(loss_flag)                 OVER ({{ f }}) AS loss_rate_rolling_{{ w }},
    AVG(clean_sheet::DOUBLE)       OVER ({{ f }}) AS clean_sheet_rate_rolling_{{ w }},
    AVG(failed_to_score_flag)      OVER ({{ f }}) AS failed_to_score_rate_rolling_{{ w }},
    AVG(shots_total)               OVER ({{ f }}) AS shots_rolling_{{ w }},
    AVG(shots_on_target)           OVER ({{ f }}) AS shots_ot_rolling_{{ w }},
    AVG(ppda)                      OVER ({{ f }}) AS ppda_rolling_{{ w }},
    AVG(ppda_allowed)              OVER ({{ f }}) AS ppda_allowed_rolling_{{ w }},

    -- ══ Famille 3 — Style & identité (roulé depuis team_features_ws) ══════
    AVG(ws_field_tilt_actions)     OVER ({{ f }}) AS field_tilt_rolling_{{ w }},
    AVG(ws_counter_attack_dna)     OVER ({{ f }}) AS counter_attack_dna_rolling_{{ w }},
    AVG(ws_attack_left_pct)        OVER ({{ f }}) AS attack_left_pct_rolling_{{ w }},
    AVG(ws_attack_center_pct)      OVER ({{ f }}) AS attack_center_pct_rolling_{{ w }},
    AVG(ws_attack_right_pct)       OVER ({{ f }}) AS attack_right_pct_rolling_{{ w }},
    AVG(ws_def_exposed_left_pct)   OVER ({{ f }}) AS def_exposed_left_pct_rolling_{{ w }},
    AVG(ws_def_exposed_center_pct) OVER ({{ f }}) AS def_exposed_center_pct_rolling_{{ w }},
    AVG(ws_def_exposed_right_pct)  OVER ({{ f }}) AS def_exposed_right_pct_rolling_{{ w }},
    AVG(ws_cross_rate)             OVER ({{ f }}) AS cross_rate_rolling_{{ w }},
    AVG(ws_through_ball_rate)      OVER ({{ f }}) AS through_ball_rate_rolling_{{ w }},
    AVG(ws_long_ball_rate)         OVER ({{ f }}) AS long_ball_rate_rolling_{{ w }},
    AVG(ws_shot_six_yard_pct)      OVER ({{ f }}) AS shot_six_yard_pct_rolling_{{ w }},
    AVG(ws_shot_open_play_pct)     OVER ({{ f }}) AS shot_open_play_pct_rolling_{{ w }},
    AVG(ws_shot_set_piece_pct)     OVER ({{ f }}) AS shot_set_piece_pct_rolling_{{ w }},
    AVG(ws_set_piece_pressure)     OVER ({{ f }}) AS set_piece_reliance_rolling_{{ w }},
    AVG(ws_defensive_line_height)  OVER ({{ f }}) AS defensive_line_height_rolling_{{ w }},

    -- ══ Famille 8 — Discipline (partie native backbone ; corners/fautes
    --    subies/tirs contrés à ajouter après agrégation des sources dédiées) ═
    AVG((yellow_cards + second_yellow_cards)::DOUBLE) OVER ({{ f }}) AS yellow_cards_rolling_{{ w }},
    AVG(fouls_committed)           OVER ({{ f }}) AS fouls_committed_rolling_{{ w }},
    AVG(fouls_drawn)               OVER ({{ f }}) AS fouls_drawn_rolling_{{ w }},
    AVG(corners_for)               OVER ({{ f }}) AS corners_for_rolling_{{ w }},
    AVG(corners_against)           OVER ({{ f }}) AS corners_against_rolling_{{ w }},
    AVG(shots_blocked)             OVER ({{ f }}) AS shots_blocked_rolling_{{ w }}
{% endmacro %}
