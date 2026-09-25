{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='equipe_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.equipe_match — grain (match_id, team_id)
-- Familles CDC : 1 (contexte pré-match), 2 (forme), 3 (style), 8 (natif backbone).
-- Familles reportées : 4 (agrégats du onze — nécessite gold.joueur_saison) ;
--   8 restante (fouls_drawn, corners_for/against, shots_blocked — nécessite
--   l'agrégation de int_fouls_drawn / corner_profiles / int_defensive_blocks).
--
-- Incrémental sans casser l'anti-leakage : les fenêtres rolling sont calculées
-- sur TOUT l'historique de la source (CTE 'prepared'), puis on ne MATÉRIALISE
-- que les nouvelles lignes (filtre final). Le contexte des fenêtres reste donc
-- toujours complet, même en run incrémental.
--
-- Convention (refonte nommage) : backbone et int_whoscored_team_season sont lus sous
-- leurs nouveaux noms et remappés vers des noms de travail courts (CTE backbone_in,
-- team_season) — la macro roll_equipe_match et la logique restent inchangées.
-- Les ids sont recastés en INTEGER pour joindre les int_* WhoScored (team_id BIGINT).
-- Renommage vers docs/proposition_nommage_definitif.csv dans la CTE finale renamed.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_fouls_drawn lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_fouls_drawn AS (
    SELECT
        str_match_id                                                 AS "match_id",
        int_row_num                                                  AS "row_num",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_expanded_minute                                          AS "expanded_minute",
        CAST(str_drawing_team_id AS BIGINT)                          AS "drawing_team_id",
        CAST(str_drawer_player_id AS INTEGER)                        AS "drawer_player_id",
        CAST(str_committed_by_player_id AS INTEGER)                  AS "committed_by_player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        str_foul_zone                                                AS "foul_zone",
        bool_is_attacking_third                                      AS "is_attacking_third",
        bool_leads_to_penalty                                        AS "leads_to_penalty"
    FROM {{ ref('int_fouls_drawn') }}
),

-- int_shot_placement lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_shot_placement AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        bool_is_own_goal                                             AS "is_own_goal",
        bool_is_goal                                                 AS "is_goal",
        bool_is_blocked                                              AS "is_blocked",
        bool_is_on_target                                            AS "is_on_target",
        dec_pre_shot_xg_proxy                                        AS "pre_shot_xg_proxy",
        dec_offset_center                                            AS "offset_center",
        dec_height                                                   AS "height",
        str_placement_col                                            AS "placement_col",
        str_placement_row                                            AS "placement_row",
        str_placement_zone                                           AS "placement_zone",
        dec_corner_dist                                              AS "corner_dist"
    FROM {{ ref('int_shot_placement') }}
),

-- int_whoscored_player_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_player_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        int_shirt_no                                                 AS "shirt_no",
        str_position                                                 AS "position",
        bool_is_first_eleven                                         AS "is_first_eleven",
        bool_is_man_of_the_match                                     AS "is_man_of_the_match",
        int_height                                                   AS "height",
        int_weight                                                   AS "weight",
        int_age                                                      AS "age",
        dec_rating                                                   AS "rating",
        str_stats_json                                               AS "stats_json",
        CAST(int_touches AS DOUBLE)                                  AS "touches",
        CAST(int_possession AS DOUBLE)                               AS "possession",
        CAST(int_passes_total AS DOUBLE)                             AS "passes_total",
        CAST(int_passes_accurate AS DOUBLE)                          AS "passes_accurate",
        CAST(int_passes_key AS DOUBLE)                               AS "passes_key",
        CAST(int_shots_total AS DOUBLE)                              AS "shots_total",
        CAST(int_shots_on_target AS DOUBLE)                          AS "shots_on_target",
        CAST(int_shots_off_target AS DOUBLE)                         AS "shots_off_target",
        CAST(int_shots_blocked AS DOUBLE)                            AS "shots_blocked",
        CAST(int_shots_on_post AS DOUBLE)                            AS "shots_on_post",
        CAST(int_dribbles_attempted AS DOUBLE)                       AS "dribbles_attempted",
        CAST(int_dribbles_won AS DOUBLE)                             AS "dribbles_won",
        CAST(int_dribbles_lost AS DOUBLE)                            AS "dribbles_lost",
        CAST(int_dribbled_past AS DOUBLE)                            AS "dribbled_past",
        CAST(int_dispossessed AS DOUBLE)                             AS "dispossessed",
        CAST(int_tackles_total AS DOUBLE)                            AS "tackles_total",
        CAST(int_tackle_successful AS DOUBLE)                        AS "tackle_successful",
        CAST(int_tackle_unsuccesful AS DOUBLE)                       AS "tackle_unsuccesful",
        CAST(int_interceptions AS DOUBLE)                            AS "interceptions",
        CAST(int_clearances AS DOUBLE)                               AS "clearances",
        CAST(int_aerials_total AS DOUBLE)                            AS "aerials_total",
        CAST(int_aerials_won AS DOUBLE)                              AS "aerials_won",
        CAST(int_offensive_aerials AS DOUBLE)                        AS "offensive_aerials",
        CAST(int_defensive_aerials AS DOUBLE)                        AS "defensive_aerials",
        CAST(int_fouls_commited AS DOUBLE)                           AS "fouls_commited",
        CAST(int_offsides_caught AS DOUBLE)                          AS "offsides_caught",
        CAST(int_errors AS DOUBLE)                                   AS "errors",
        CAST(int_corners_total AS DOUBLE)                            AS "corners_total",
        CAST(int_corners_accurate AS DOUBLE)                         AS "corners_accurate",
        CAST(int_throw_ins_total AS DOUBLE)                          AS "throw_ins_total",
        CAST(int_throw_ins_accurate AS DOUBLE)                       AS "throw_ins_accurate",
        CAST(int_total_saves AS DOUBLE)                              AS "total_saves",
        CAST(int_parried_safe AS DOUBLE)                             AS "parried_safe",
        CAST(int_parried_danger AS DOUBLE)                           AS "parried_danger",
        CAST(int_claims_high AS DOUBLE)                              AS "claims_high",
        CAST(int_collected AS DOUBLE)                                AS "collected"
    FROM {{ ref('int_whoscored_player_match') }}
),

-- int_whoscored_team_season lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_team_season AS (
    SELECT
        str_team_id                                                  AS "str_team_id",
        str_season                                                   AS "str_season",
        str_league_source                                            AS "str_league_source",
        dec_ws_away_shots_conceded_pg                                AS "dec_ws_away_shots_conceded_pg",
        dec_ws_away_tackles_pg                                       AS "dec_ws_away_tackles_pg",
        dec_ws_away_interceptions_pg                                 AS "dec_ws_away_interceptions_pg",
        dec_ws_away_fouls_pg                                         AS "dec_ws_away_fouls_pg",
        dec_ws_away_offsides_pg                                      AS "dec_ws_away_offsides_pg",
        dec_ws_away_def_rating                                       AS "dec_ws_away_def_rating",
        dec_ws_home_shots_conceded_pg                                AS "dec_ws_home_shots_conceded_pg",
        dec_ws_home_tackles_pg                                       AS "dec_ws_home_tackles_pg",
        dec_ws_home_interceptions_pg                                 AS "dec_ws_home_interceptions_pg",
        dec_ws_home_fouls_pg                                         AS "dec_ws_home_fouls_pg",
        dec_ws_home_offsides_pg                                      AS "dec_ws_home_offsides_pg",
        dec_ws_home_def_rating                                       AS "dec_ws_home_def_rating",
        dec_ws_away_shots_pg                                         AS "dec_ws_away_shots_pg",
        dec_ws_away_shots_ot_pg                                      AS "dec_ws_away_shots_ot_pg",
        dec_ws_away_dribbles_pg                                      AS "dec_ws_away_dribbles_pg",
        dec_ws_away_fouled_pg                                        AS "dec_ws_away_fouled_pg",
        dec_ws_away_att_rating                                       AS "dec_ws_away_att_rating",
        dec_ws_home_shots_pg                                         AS "dec_ws_home_shots_pg",
        dec_ws_home_shots_ot_pg                                      AS "dec_ws_home_shots_ot_pg",
        dec_ws_home_dribbles_pg                                      AS "dec_ws_home_dribbles_pg",
        dec_ws_home_fouled_pg                                        AS "dec_ws_home_fouled_pg",
        dec_ws_home_att_rating                                       AS "dec_ws_home_att_rating",
        dec_ws_away_xg_against                                       AS "dec_ws_away_xg_against",
        CAST(int_ws_away_goals_against AS DOUBLE)                    AS "dec_ws_away_goals_against",
        dec_ws_away_xg_diff_against                                  AS "dec_ws_away_xg_diff_against",
        CAST(int_ws_away_shots_against AS DOUBLE)                    AS "dec_ws_away_shots_against",
        dec_ws_away_xg_per_shot_against                              AS "dec_ws_away_xg_per_shot_against",
        dec_ws_away_xg_against_rating                                AS "dec_ws_away_xg_against_rating",
        dec_ws_away_xg_for                                           AS "dec_ws_away_xg_for",
        CAST(int_ws_away_goals_for AS DOUBLE)                        AS "dec_ws_away_goals_for",
        dec_ws_away_xg_diff_for                                      AS "dec_ws_away_xg_diff_for",
        CAST(int_ws_away_shots_for AS DOUBLE)                        AS "dec_ws_away_shots_for",
        dec_ws_away_xg_per_shot_for                                  AS "dec_ws_away_xg_per_shot_for",
        dec_ws_away_xg_for_rating                                    AS "dec_ws_away_xg_for_rating",
        dec_ws_home_xg_against                                       AS "dec_ws_home_xg_against",
        CAST(int_ws_home_goals_against AS DOUBLE)                    AS "dec_ws_home_goals_against",
        dec_ws_home_xg_diff_against                                  AS "dec_ws_home_xg_diff_against",
        CAST(int_ws_home_shots_against AS DOUBLE)                    AS "dec_ws_home_shots_against",
        dec_ws_home_xg_per_shot_against                              AS "dec_ws_home_xg_per_shot_against",
        dec_ws_home_xg_against_rating                                AS "dec_ws_home_xg_against_rating",
        dec_ws_home_xg_for                                           AS "dec_ws_home_xg_for",
        CAST(int_ws_home_goals_for AS DOUBLE)                        AS "dec_ws_home_goals_for",
        dec_ws_home_xg_diff_for                                      AS "dec_ws_home_xg_diff_for",
        CAST(int_ws_home_shots_for AS DOUBLE)                        AS "dec_ws_home_shots_for",
        dec_ws_home_xg_per_shot_for                                  AS "dec_ws_home_xg_per_shot_for",
        dec_ws_home_xg_for_rating                                    AS "dec_ws_home_xg_for_rating",
        str_source                                                   AS "str_source",
        dt_scraped_at                                                AS "dt_scraped_at",
        str_comp_category                                            AS "str_comp_category",
        str_raw_team                                                 AS "str_raw_team"
    FROM {{ ref('int_whoscored_team_season') }}
),

-- team_features_ws lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_team_features_ws AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        dec_ws_field_tilt_actions                                    AS "ws_field_tilt_actions",
        dec_ws_high_turnover_rate                                    AS "ws_high_turnover_rate",
        dec_ws_deep_completion_rt                                    AS "ws_deep_completion_rt",
        dec_ws_momentum_delta                                        AS "ws_momentum_delta",
        dec_ws_counter_shot_rate                                     AS "ws_counter_shot_rate",
        dec_ws_set_piece_pressure                                    AS "ws_set_piece_pressure",
        dec_ws_attack_left_pct                                       AS "ws_attack_left_pct",
        dec_ws_attack_center_pct                                     AS "ws_attack_center_pct",
        dec_ws_attack_right_pct                                      AS "ws_attack_right_pct",
        dec_ws_zone_def_pct                                          AS "ws_zone_def_pct",
        dec_ws_zone_mid_pct                                          AS "ws_zone_mid_pct",
        dec_ws_zone_att_pct                                          AS "ws_zone_att_pct",
        dec_ws_shot_six_yard_pct                                     AS "ws_shot_six_yard_pct",
        dec_ws_shot_penalty_pct                                      AS "ws_shot_penalty_pct",
        dec_ws_shot_oob_pct                                          AS "ws_shot_oob_pct",
        dec_ws_shot_open_play_pct                                    AS "ws_shot_open_play_pct",
        dec_ws_shot_set_piece_pct                                    AS "ws_shot_set_piece_pct",
        dec_ws_shot_penalty_att_pct                                  AS "ws_shot_penalty_att_pct",
        dec_ws_conversion_rate                                       AS "ws_conversion_rate",
        dec_ws_cross_rate                                            AS "ws_cross_rate",
        dec_ws_through_ball_rate                                     AS "ws_through_ball_rate",
        dec_ws_long_ball_rate                                        AS "ws_long_ball_rate",
        dec_ws_short_pass_rate                                       AS "ws_short_pass_rate",
        dec_ws_def_exposed_left_pct                                  AS "ws_def_exposed_left_pct",
        dec_ws_def_exposed_center_pct                                AS "ws_def_exposed_center_pct",
        dec_ws_def_exposed_right_pct                                 AS "ws_def_exposed_right_pct",
        dec_ws_counter_attack_dna                                    AS "ws_counter_attack_dna",
        dec_ws_midfield_control_idx                                  AS "ws_midfield_control_idx",
        dec_ws_defensive_line_height                                 AS "ws_defensive_line_height",
        dec_ws_flank_exposure_asymm                                  AS "ws_flank_exposure_asymm"
    FROM {{ ref('team_features_ws') }}
),

mdl_body AS (
WITH

backbone_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        dt_date                            AS date,
        str_venue                          AS venue,
        str_season                         AS season,
        str_league_source                  AS league_source,
        str_comp_category                  AS comp_category,
        int_gf                             AS gf,
        int_ga                             AS ga,
        dec_np_xg                          AS np_xg,
        dec_np_xg_conceded                 AS np_xg_conceded,
        dec_np_xg_diff_match               AS np_xg_diff_match,
        int_clean_sheet                    AS clean_sheet,
        int_shots_total                    AS shots_total,
        int_shots_on_target                AS shots_on_target,
        dec_ppda                           AS ppda,
        dec_ppda_allowed                   AS ppda_allowed,
        int_yellow_cards                   AS yellow_cards,
        int_second_yellow_cards            AS second_yellow_cards,
        int_fouls_committed                AS fouls_committed
    FROM {{ ref('backbone') }}
),

-- 0) Famille 8 agrégée au grain équipe-match : fautes subies (59) + corners
--    for/against (61/62). WHERE match_id IS NOT NULL écarte la ligne orpheline
--    (match_id NULL sous laquelle 52 équipes s'agglutinent). shots_blocked (63)
--    reporté : source CDC erronée (int_defensive_blocks = blocks passe/dégagement).
fouls_drawn_match AS (
    SELECT match_id, drawing_team_id AS team_id, COUNT(*) AS fouls_drawn
    FROM in_int_fouls_drawn
    WHERE drawing_team_id IS NOT NULL AND match_id IS NOT NULL
    GROUP BY match_id, drawing_team_id
),

corners_match AS (
    SELECT match_id, team_id, SUM(corners_total) AS corners_for
    FROM in_int_whoscored_player_match
    WHERE match_id IS NOT NULL
    GROUP BY match_id, team_id
),

-- Tirs bloqués par match et par équipe TIREUSE (feature 63). shots_blocked d'une
-- équipe = tirs de son ADVERSAIRE qui ont été contrés → jointure sur opponent.
blocked_shots_match AS (
    SELECT match_id, team_id, SUM(CASE WHEN is_blocked THEN 1 ELSE 0 END) AS n_blocked
    FROM in_int_shot_placement
    WHERE match_id IS NOT NULL AND is_own_goal = FALSE
    GROUP BY match_id, team_id
),

-- 1) Base : backbone (grain équipe-match) + style WhoScored (même grain)
base AS (
    SELECT
        b.match_id, b.team_id, b.opponent_id, b.date, b.venue, b.season,
        b.league_source, b.comp_category,
        b.gf, b.ga, b.np_xg, b.np_xg_conceded, b.np_xg_diff_match,
        b.clean_sheet, b.shots_total, b.shots_on_target,
        b.ppda, b.ppda_allowed, b.yellow_cards, b.second_yellow_cards,
        b.fouls_committed,
        fd.fouls_drawn,   -- NULL si match hors couverture WhoScored (l'AVG rolling l'ignore, pas de 0 factice)
        cf.corners_for,
        ca.corners_for AS corners_against,   -- corners de l'adversaire = corners concédés
        sb.n_blocked AS shots_blocked,       -- tirs de l'adversaire contrés par cette équipe
        tw.ws_field_tilt_actions, tw.ws_counter_attack_dna,
        tw.ws_attack_left_pct, tw.ws_attack_center_pct, tw.ws_attack_right_pct,
        tw.ws_def_exposed_left_pct, tw.ws_def_exposed_center_pct, tw.ws_def_exposed_right_pct,
        tw.ws_cross_rate, tw.ws_through_ball_rate, tw.ws_long_ball_rate,
        tw.ws_shot_six_yard_pct, tw.ws_shot_open_play_pct, tw.ws_shot_set_piece_pct,
        tw.ws_set_piece_pressure, tw.ws_defensive_line_height
    FROM backbone_in b
    LEFT JOIN in_team_features_ws tw
        USING (match_id, team_id)
    LEFT JOIN fouls_drawn_match fd
        USING (match_id, team_id)
    LEFT JOIN corners_match cf
        USING (match_id, team_id)
    LEFT JOIN corners_match ca
        ON ca.match_id = b.match_id AND ca.team_id = b.opponent_id
    LEFT JOIN blocked_shots_match sb
        ON sb.match_id = b.match_id AND sb.team_id = b.opponent_id
),

-- 2) Colonnes dérivées (issue du match) préparées AVANT les fenêtres.
--    Elles ne sont utilisées qu'agrégées en rolling → pas de fuite.
--    Match non joué (gf NULL depuis la refonte d'int_fbref_schedule) → flags NULL,
--    ignorés par AVG. Avant, le score vide valait 0-0 et comptait comme un nul.
prepared AS (
    SELECT *,
        CASE WHEN gf IS NULL OR ga IS NULL THEN NULL
             WHEN gf > ga THEN 3.0 WHEN gf = ga THEN 1.0 ELSE 0.0 END AS points_match,
        CASE WHEN gf IS NULL OR ga IS NULL THEN NULL
             WHEN gf > ga THEN 1.0 ELSE 0.0 END AS win_flag,
        CASE WHEN gf IS NULL OR ga IS NULL THEN NULL
             WHEN gf = ga THEN 1.0 ELSE 0.0 END AS draw_flag,
        CASE WHEN gf IS NULL OR ga IS NULL THEN NULL
             WHEN gf < ga THEN 1.0 ELSE 0.0 END AS loss_flag,
        CASE WHEN gf IS NULL THEN NULL
             WHEN gf = 0  THEN 1.0 ELSE 0.0 END AS failed_to_score_flag
    FROM base
),

-- 3) Famille 3 — feature 28 : xG/tir de saison, SEASON-LAG (saison précédente).
--    int_whoscored_team_season est un résumé de saison complète. On fusionne
--    home+away, puis on rattache la saison N-1 par JOINTURE EXPLICITE sur la
--    saison calendaire précédente (surtout PAS un LAG par ordre de lignes : la
--    source a des trous — 2020-2021 manque — et un LAG prendrait alors une
--    saison à N-2 sans le savoir). NULL si la saison N-1 est absente : correct.
--    Couverture réelle : ~65-85 % à partir de 2022-2023, faible avant.
team_season AS (
    SELECT
        CAST(str_team_id AS BIGINT) AS team_id,
        str_season                   AS season,
        AVG((dec_ws_home_xg_per_shot_for     + dec_ws_away_xg_per_shot_for)     / 2.0) AS season_xg_per_shot_for_lag,
        AVG((dec_ws_home_xg_per_shot_against + dec_ws_away_xg_per_shot_against) / 2.0) AS season_xg_per_shot_against_lag
    FROM in_int_whoscored_team_season
    GROUP BY 1, 2
),

-- 4) Calcul des features rolling (macro CDC) + pré-match connu.
rolled AS (
    SELECT
        -- clés & contexte
        match_id, team_id, opponent_id, date, season, league_source, venue, comp_category,

        -- Famille 1 — pré-match connu (aucun calcul sur le match courant)
        CASE WHEN venue = 'Home' THEN 1 ELSE 0 END AS is_home,
        (date - LAG(date) OVER (PARTITION BY team_id ORDER BY date, match_id)) AS days_since_last_game,

        -- Familles 2, 3, 8-natif — rolling {3,5,10}
        {% for w in [3, 5, 10] %}
        {{ roll_equipe_match(w) }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM prepared
),

final AS (
    SELECT
        r.*,
        ts.season_xg_per_shot_for_lag,
        ts.season_xg_per_shot_against_lag
    FROM rolled r
    LEFT JOIN team_season ts
        ON ts.team_id = r.team_id
       -- saison N-1 calendaire : '2023-2024' → '2022-2023'
       AND ts.season = (CAST(LEFT(r.season, 4) AS INTEGER) - 1)::VARCHAR || '-' || LEFT(r.season, 4)
),

-- 5) Renommage final (docs/proposition_nommage_definitif.csv)
renamed AS (
    SELECT
        match_id                                             AS str_match_id,
        CAST(team_id AS VARCHAR)                             AS str_team_id,
        CAST(opponent_id AS VARCHAR)                         AS str_opponent_id,
        date                                                 AS dt_date,
        season                                               AS str_season,
        league_source                                        AS str_league_source,
        venue                                                AS str_venue,
        comp_category                                        AS str_comp_category,
        is_home                                              AS int_is_home,
        days_since_last_game                                 AS int_days_since_last_game,
        avg_gf_rolling_3                                     AS dec_avg_gf_rolling_3,
        avg_ga_rolling_3                                     AS dec_avg_ga_rolling_3,
        avg_np_xg_rolling_3                                  AS dec_avg_np_xg_rolling_3,
        avg_np_xg_conceded_rolling_3                         AS dec_avg_np_xg_conceded_rolling_3,
        avg_np_xg_diff_rolling_3                             AS dec_avg_np_xg_diff_rolling_3,
        points_rolling_3                                     AS dec_points_rolling_3,
        win_rate_rolling_3                                   AS dec_win_rate_rolling_3,
        draw_rate_rolling_3                                  AS dec_draw_rate_rolling_3,
        loss_rate_rolling_3                                  AS dec_loss_rate_rolling_3,
        clean_sheet_rate_rolling_3                           AS dec_clean_sheet_rate_rolling_3,
        failed_to_score_rate_rolling_3                       AS dec_failed_to_score_rate_rolling_3,
        shots_rolling_3                                      AS dec_shots_rolling_3,
        shots_ot_rolling_3                                   AS dec_shots_ot_rolling_3,
        ppda_rolling_3                                       AS dec_ppda_rolling_3,
        ppda_allowed_rolling_3                               AS dec_ppda_allowed_rolling_3,
        field_tilt_rolling_3                                 AS dec_field_tilt_rolling_3,
        counter_attack_dna_rolling_3                         AS dec_counter_attack_dna_rolling_3,
        attack_left_pct_rolling_3                            AS dec_attack_left_pct_rolling_3,
        attack_center_pct_rolling_3                          AS dec_attack_center_pct_rolling_3,
        attack_right_pct_rolling_3                           AS dec_attack_right_pct_rolling_3,
        def_exposed_left_pct_rolling_3                       AS dec_def_exposed_left_pct_rolling_3,
        def_exposed_center_pct_rolling_3                     AS dec_def_exposed_center_pct_rolling_3,
        def_exposed_right_pct_rolling_3                      AS dec_def_exposed_right_pct_rolling_3,
        cross_rate_rolling_3                                 AS dec_cross_rate_rolling_3,
        through_ball_rate_rolling_3                          AS dec_through_ball_rate_rolling_3,
        long_ball_rate_rolling_3                             AS dec_long_ball_rate_rolling_3,
        shot_six_yard_pct_rolling_3                          AS dec_shot_six_yard_pct_rolling_3,
        shot_open_play_pct_rolling_3                         AS dec_shot_open_play_pct_rolling_3,
        shot_set_piece_pct_rolling_3                         AS dec_shot_set_piece_pct_rolling_3,
        set_piece_reliance_rolling_3                         AS dec_set_piece_reliance_rolling_3,
        defensive_line_height_rolling_3                      AS dec_defensive_line_height_rolling_3,
        yellow_cards_rolling_3                               AS dec_yellow_cards_rolling_3,
        fouls_committed_rolling_3                            AS dec_fouls_committed_rolling_3,
        fouls_drawn_rolling_3                                AS dec_fouls_drawn_rolling_3,
        corners_for_rolling_3                                AS dec_corners_for_rolling_3,
        corners_against_rolling_3                            AS dec_corners_against_rolling_3,
        shots_blocked_rolling_3                              AS dec_shots_blocked_rolling_3,
        avg_gf_rolling_5                                     AS dec_avg_gf_rolling_5,
        avg_ga_rolling_5                                     AS dec_avg_ga_rolling_5,
        avg_np_xg_rolling_5                                  AS dec_avg_np_xg_rolling_5,
        avg_np_xg_conceded_rolling_5                         AS dec_avg_np_xg_conceded_rolling_5,
        avg_np_xg_diff_rolling_5                             AS dec_avg_np_xg_diff_rolling_5,
        points_rolling_5                                     AS dec_points_rolling_5,
        win_rate_rolling_5                                   AS dec_win_rate_rolling_5,
        draw_rate_rolling_5                                  AS dec_draw_rate_rolling_5,
        loss_rate_rolling_5                                  AS dec_loss_rate_rolling_5,
        clean_sheet_rate_rolling_5                           AS dec_clean_sheet_rate_rolling_5,
        failed_to_score_rate_rolling_5                       AS dec_failed_to_score_rate_rolling_5,
        shots_rolling_5                                      AS dec_shots_rolling_5,
        shots_ot_rolling_5                                   AS dec_shots_ot_rolling_5,
        ppda_rolling_5                                       AS dec_ppda_rolling_5,
        ppda_allowed_rolling_5                               AS dec_ppda_allowed_rolling_5,
        field_tilt_rolling_5                                 AS dec_field_tilt_rolling_5,
        counter_attack_dna_rolling_5                         AS dec_counter_attack_dna_rolling_5,
        attack_left_pct_rolling_5                            AS dec_attack_left_pct_rolling_5,
        attack_center_pct_rolling_5                          AS dec_attack_center_pct_rolling_5,
        attack_right_pct_rolling_5                           AS dec_attack_right_pct_rolling_5,
        def_exposed_left_pct_rolling_5                       AS dec_def_exposed_left_pct_rolling_5,
        def_exposed_center_pct_rolling_5                     AS dec_def_exposed_center_pct_rolling_5,
        def_exposed_right_pct_rolling_5                      AS dec_def_exposed_right_pct_rolling_5,
        cross_rate_rolling_5                                 AS dec_cross_rate_rolling_5,
        through_ball_rate_rolling_5                          AS dec_through_ball_rate_rolling_5,
        long_ball_rate_rolling_5                             AS dec_long_ball_rate_rolling_5,
        shot_six_yard_pct_rolling_5                          AS dec_shot_six_yard_pct_rolling_5,
        shot_open_play_pct_rolling_5                         AS dec_shot_open_play_pct_rolling_5,
        shot_set_piece_pct_rolling_5                         AS dec_shot_set_piece_pct_rolling_5,
        set_piece_reliance_rolling_5                         AS dec_set_piece_reliance_rolling_5,
        defensive_line_height_rolling_5                      AS dec_defensive_line_height_rolling_5,
        yellow_cards_rolling_5                               AS dec_yellow_cards_rolling_5,
        fouls_committed_rolling_5                            AS dec_fouls_committed_rolling_5,
        fouls_drawn_rolling_5                                AS dec_fouls_drawn_rolling_5,
        corners_for_rolling_5                                AS dec_corners_for_rolling_5,
        corners_against_rolling_5                            AS dec_corners_against_rolling_5,
        shots_blocked_rolling_5                              AS dec_shots_blocked_rolling_5,
        avg_gf_rolling_10                                    AS dec_avg_gf_rolling_10,
        avg_ga_rolling_10                                    AS dec_avg_ga_rolling_10,
        avg_np_xg_rolling_10                                 AS dec_avg_np_xg_rolling_10,
        avg_np_xg_conceded_rolling_10                        AS dec_avg_np_xg_conceded_rolling_10,
        avg_np_xg_diff_rolling_10                            AS dec_avg_np_xg_diff_rolling_10,
        points_rolling_10                                    AS dec_points_rolling_10,
        win_rate_rolling_10                                  AS dec_win_rate_rolling_10,
        draw_rate_rolling_10                                 AS dec_draw_rate_rolling_10,
        loss_rate_rolling_10                                 AS dec_loss_rate_rolling_10,
        clean_sheet_rate_rolling_10                          AS dec_clean_sheet_rate_rolling_10,
        failed_to_score_rate_rolling_10                      AS dec_failed_to_score_rate_rolling_10,
        shots_rolling_10                                     AS dec_shots_rolling_10,
        shots_ot_rolling_10                                  AS dec_shots_ot_rolling_10,
        ppda_rolling_10                                      AS dec_ppda_rolling_10,
        ppda_allowed_rolling_10                              AS dec_ppda_allowed_rolling_10,
        field_tilt_rolling_10                                AS dec_field_tilt_rolling_10,
        counter_attack_dna_rolling_10                        AS dec_counter_attack_dna_rolling_10,
        attack_left_pct_rolling_10                           AS dec_attack_left_pct_rolling_10,
        attack_center_pct_rolling_10                         AS dec_attack_center_pct_rolling_10,
        attack_right_pct_rolling_10                          AS dec_attack_right_pct_rolling_10,
        def_exposed_left_pct_rolling_10                      AS dec_def_exposed_left_pct_rolling_10,
        def_exposed_center_pct_rolling_10                    AS dec_def_exposed_center_pct_rolling_10,
        def_exposed_right_pct_rolling_10                     AS dec_def_exposed_right_pct_rolling_10,
        cross_rate_rolling_10                                AS dec_cross_rate_rolling_10,
        through_ball_rate_rolling_10                         AS dec_through_ball_rate_rolling_10,
        long_ball_rate_rolling_10                            AS dec_long_ball_rate_rolling_10,
        shot_six_yard_pct_rolling_10                         AS dec_shot_six_yard_pct_rolling_10,
        shot_open_play_pct_rolling_10                        AS dec_shot_open_play_pct_rolling_10,
        shot_set_piece_pct_rolling_10                        AS dec_shot_set_piece_pct_rolling_10,
        set_piece_reliance_rolling_10                        AS dec_set_piece_reliance_rolling_10,
        defensive_line_height_rolling_10                     AS dec_defensive_line_height_rolling_10,
        yellow_cards_rolling_10                              AS dec_yellow_cards_rolling_10,
        fouls_committed_rolling_10                           AS dec_fouls_committed_rolling_10,
        fouls_drawn_rolling_10                               AS dec_fouls_drawn_rolling_10,
        corners_for_rolling_10                               AS dec_corners_for_rolling_10,
        corners_against_rolling_10                           AS dec_corners_against_rolling_10,
        shots_blocked_rolling_10                             AS dec_shots_blocked_rolling_10,
        season_xg_per_shot_for_lag                           AS dec_season_xg_per_shot_for_lag,
        season_xg_per_shot_against_lag                       AS dec_season_xg_per_shot_against_lag
    FROM final
)

SELECT * FROM renamed

{% if is_incremental() %}
WHERE dt_date > (SELECT MAX(dt_date) FROM {{ this }})
{% endif %}
)

SELECT * FROM mdl_body
