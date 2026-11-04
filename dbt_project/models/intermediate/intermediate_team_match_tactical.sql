{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='intermediate_team_match_tactical'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- intermediate_whoscored_event_qualifiers lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        int_row_num                                                  AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
        str_qual_type_name                                           AS "qual_type_name",
        str_qual_value                                               AS "qual_value"
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
),

-- intermediate_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('intermediate_whoscored_events') }}
),

mdl_body AS (
WITH

-- 1. FILTRE INCRÉMENTAL STRICT (LEFT JOIN au lieu de NOT IN)
{% if is_incremental() %}
new_match_ids AS (
    SELECT DISTINCT e.match_id
    FROM in_int_whoscored_events e
    LEFT JOIN (
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
        FROM {{ this }}
    ) t ON e.match_id = t.match_id
    WHERE t.match_id IS NULL
),
{% else %}
new_match_ids AS (
    SELECT DISTINCT match_id
    FROM in_int_whoscored_events
),
{% endif %}

-- 2. MAPPING DES ÉQUIPES (Élimine les auto-jointures pour trouver l'adversaire)
match_teams AS (
    SELECT 
        match_id, 
        MIN(team_id) AS team_1, 
        MAX(team_id) AS team_2
    FROM in_int_whoscored_events
    WHERE match_id IN (SELECT match_id FROM new_match_ids)
    GROUP BY match_id
),

-- 3. PRÉPARATION VECTORISÉE DES ÉVÉNEMENTS
events_base AS (
    SELECT 
        e.*,
        e.expanded_minute * 60 + e.second AS t_sec,
        CASE WHEN e.team_id = mt.team_1 THEN mt.team_2 ELSE mt.team_1 END AS opponent_id,
        
        -- Tracking vectorisé du temps de la dernière récupération pour les contre-attaques (0 jointure)
        -- ORDER BY (t_sec, row_num) : sans row_num, les evenements de la MEME seconde
        -- sont ordonnes arbitrairement, donc une BallRecovery simultanee d'un tir peut
        -- etre ou ne pas etre dans la fenetre ROWS UNBOUNDED PRECEDING selon le build.
        -- 223 tirs sur 67 965 (0,33 %) sont concernes. row_num est l'ordre d'enregistrement
        -- WhoScored : il est deterministe ET semantiquement juste (la recuperation doit
        -- preceder le tir dans le flux, pas seulement partager sa seconde).
        MAX(CASE WHEN e.type_id = 49 THEN e.expanded_minute * 60 + e.second END) 
            OVER (PARTITION BY e.match_id, e.team_id ORDER BY e.expanded_minute * 60 + e.second, e.row_num ROWS UNBOUNDED PRECEDING) AS last_recovery_t_sec,
            
        -- Total des actions de l'équipe adverse (midfield control)
        COUNT(*) FILTER (WHERE e.x BETWEEN 33 AND 66 AND e.type_id IN (1,7,8) AND e.outcome_id = 1) 
            OVER (PARTITION BY e.match_id) AS match_total_midfield_actions
            
    FROM in_int_whoscored_events e
    INNER JOIN new_match_ids n ON e.match_id = n.match_id
    INNER JOIN match_teams mt ON e.match_id = mt.match_id
),

-- 4. AGRÉGATION PRINCIPALE (Fusion de base_counts, defensive_exposure, counter_attack, midfield, defensive_shape)
master_agg AS (
    SELECT 
        e.match_id, 
        e.team_id,
        
        -- Base Counts
        COUNT(*)                                                                        AS total_events,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE)                                       AS total_touches,
        COUNT(*) FILTER (WHERE e.is_shot = TRUE)                                        AS total_shots,
        COUNT(*) FILTER (WHERE e.type_id = 1)                                           AS total_passes,
        COUNT(*) FILTER (WHERE e.type_id = 1 AND e.outcome_id = 1)                      AS passes_successful,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 66)                          AS touches_offensive_zone,
        COUNT(*) FILTER (WHERE e.type_id = 1 AND e.outcome_id = 1 AND e.end_x > 83)     AS deep_completions,
        COUNT(*) FILTER (WHERE e.type_id = 1 AND e.outcome_id = 0 AND e.x > 66)         AS turnovers_high_zone,
        -- type 14 ShotOnPost etait omis alors que total_shots_ca utilise (13,14,15,16).
        -- 1 286 evenements sur 686 844 actions offensives (0,19 %). offensive_actions est
        -- le denominateur de ws_set_piece_pressure, consomme par mart_1n2.
        COUNT(*) FILTER (WHERE e.x > 66 AND e.type_id IN (1,3,13,14,15,16))             AS offensive_actions,
        
        -- Zones Attaque
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 33 AND e.y < 33.3)           AS att_touches_left,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 33 AND e.y >= 33.3 AND e.y <= 66.6) AS att_touches_center,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 33 AND e.y > 66.6)           AS att_touches_right,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 33)                          AS att_touches_total,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x < 33.3)                        AS zone_def_touches,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x >= 33.3 AND e.x <= 66.6)       AS zone_mid_touches,
        COUNT(*) FILTER (WHERE e.is_touch = TRUE AND e.x > 66.6)                        AS zone_att_touches,
        
        -- Tirs
        COUNT(*) FILTER (WHERE e.is_shot = TRUE AND e.x > 94 AND e.y BETWEEN 36 AND 64) AS shots_six_yard,
        COUNT(*) FILTER (WHERE e.is_shot = TRUE AND e.x > 83 AND e.y BETWEEN 21 AND 79 AND NOT (e.x > 94 AND e.y BETWEEN 36 AND 64)) AS shots_penalty_area,
        COUNT(*) FILTER (WHERE e.is_shot = TRUE AND NOT(e.x > 83 AND e.y BETWEEN 21 AND 79)) AS shots_out_of_box,
        -- Un CSC est un evenement type 16 / outcome 1 / is_shot TRUE attribue a l'equipe
        -- qui l'ENCAISSE (x moyen 5,7 = sa propre ligne de but). Sans le filtre, les 210 CSC
        -- du perimetre creditent un but a la mauvaise equipe dans ws_conversion_rate.
        COUNT(*) FILTER (WHERE e.type_id = 16 AND e.outcome_id = 1 AND e.is_shot = TRUE
                           AND COALESCE(e.is_own_goal, FALSE) = FALSE)                  AS goals_scored,
        
        -- Contre-attaques vectorisées (Remplace la double jointure)
        COUNT(*) FILTER (WHERE e.type_id IN (13,14,15,16))                              AS total_shots_ca,
        COUNT(*) FILTER (WHERE e.type_id IN (13,14,15,16) AND (e.t_sec - e.last_recovery_t_sec) <= 15) AS counter_attack_shots,
        
        -- Midfield Control
        COUNT(*) FILTER (WHERE e.x BETWEEN 33 AND 66 AND e.type_id IN (1,7,8) AND e.outcome_id = 1) AS midfield_actions,
        MAX(e.match_total_midfield_actions)                                             AS match_total_midfield_actions,
        
        -- Defensive Shape
        AVG(e.x) FILTER (WHERE e.type_id IN (7,8,12))                                   AS ws_defensive_line_height,
        COUNT(*) FILTER (WHERE e.type_id IN (7,8,12) AND e.y < 30)                      AS def_actions_left,
        COUNT(*) FILTER (WHERE e.type_id IN (7,8,12) AND e.y < 30 AND e.outcome_id = 1) AS def_actions_left_won,
        COUNT(*) FILTER (WHERE e.type_id IN (7,8,12) AND e.y > 70)                      AS def_actions_right,
        COUNT(*) FILTER (WHERE e.type_id IN (7,8,12) AND e.y > 70 AND e.outcome_id = 1) AS def_actions_right_won

    FROM events_base e
    GROUP BY e.match_id, e.team_id
),

-- 5. EXPOSITION DÉFENSIVE (Inversée par rapport à l'adversaire, sans auto-jointure)
defensive_exposure AS (
    SELECT 
        opponent_id AS team_id,
        match_id,
        -- Repere WhoScored : (x, y) -> (100 - x, 100 - y) est une ROTATION de 180 degres,
        -- pas deux miroirs independants. Prouve sur 74 604 paires de duels aeriens
        -- (x_a + x_b = 100,0 ET y_a + y_b = 100,0) et re-prouve sur le match de reference
        -- (7 paires, somme = 100,00 exactement, min = max).
        -- Consequence : le flanc GAUCHE de l'adversaire dans SON repere (y < 33,3) est
        -- physiquement MON flanc DROIT. Ces colonnes sont donc nommees dans MON repere,
        -- ce qui est ce que def_exposed_* doit exprimer et ce qui les rend coherentes
        -- avec ws_flank_exposure_asymm, deja calcule sur mes propres evenements.
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33 AND y > 66.6)                AS opp_att_my_left,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33 AND y >= 33.3 AND y <= 66.6) AS opp_att_my_center,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33 AND y < 33.3)                AS opp_att_my_right,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33)                             AS opp_att_total
    FROM events_base
    GROUP BY match_id, opponent_id
),

-- 6. QUALIFICATEURS
qualifier_features AS (
    SELECT match_id, team_id,
        -- Les 6 qualificateurs de situation PARTITIONNENT les tirs : 67 185 tirs en portent
        -- exactement un, 780 aucun, 0 en portent deux. Noms reels verifies en base :
        --   9 Penalty · 22 RegularPlay · 23 FastBreak · 24 SetPiece · 25 FromCorner
        --   26 DirectFreekick
        -- Avant correctif : 26 servait de "contre-attaque" (c'est un coup franc direct) et
        -- 23 de "coup de pied arrete" (c'est la contre-attaque) ; 24 et 25 n'etaient lus par
        -- personne, soit 15 572 tirs = 23 % du total laisses sans classe.
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot = TRUE AND qual_type_id = 23)     AS shots_counter_attack,
        COUNT(DISTINCT row_num) FILTER (WHERE qual_type_id IN (5,6) AND x > 50)         AS set_pieces_offensive,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot = TRUE AND qual_type_id = 22)     AS shots_open_play,
        -- CPA = 24 SetPiece + 25 FromCorner + 26 DirectFreekick. Le penalty garde sa
        -- colonne dediee shots_penalty, donc 22 devient exactement le complement :
        -- open_play + set_piece + counter + penalty = 1, taxonomie fermee et verifiable.
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot = TRUE AND qual_type_id IN (24,25,26)) AS shots_set_piece,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot = TRUE AND qual_type_id = 9)      AS shots_penalty,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id = 1 AND qual_type_id = 2)         AS passes_cross,
        -- 4 = Throughball (6 729 passes). Avant correctif : 155 = Chipped (248 323),
        -- soit un facteur 36,9 et une tout autre notion.
        COUNT(DISTINCT row_num) FILTER (WHERE type_id = 1 AND qual_type_id = 4)         AS passes_through_ball,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id = 1 AND qual_type_id = 1)         AS passes_long_ball,
        -- Passes portant AU MOINS un de {1 Longball, 2 Cross, 155 Chipped}. Les trois
        -- ensembles se recoupent (155 144 passes en portent 2, 30 589 en portent 3), donc
        -- la soustraction des trois taux double et triple comptait : biais de -8,8 points
        -- sur ws_short_pass_rate (0,7163 au lieu de 0,8043).
        COUNT(DISTINCT row_num) FILTER (WHERE type_id = 1 AND qual_type_id IN (1,2,155)) AS passes_non_short
    FROM in_events_qual
    WHERE match_id IN (SELECT match_id FROM new_match_ids)
    GROUP BY match_id, team_id
),

-- 7. MOMENTUM (Optimisé avec une pré-agrégation temporelle)
goals_conceded AS (
    -- Pour un but normal, l'equipe qui encaisse est l'adversaire du buteur. Pour un CSC,
    -- l'evenement est deja attribue a l'equipe qui encaisse : prendre opponent_id
    -- attribuait le but encaisse a l'equipe qui en BENEFICIE. 200 matchs, 400 lignes.
    SELECT match_id,
           CASE WHEN COALESCE(is_own_goal, FALSE) THEN team_id ELSE opponent_id END
                                                      AS conceding_team_id,
           expanded_minute AS goal_minute
    FROM events_base
    WHERE type_id = 16 AND outcome_id = 1 AND is_shot = TRUE
),

momentum_windows AS (
    SELECT gc.match_id, gc.conceding_team_id AS team_id,
        COUNT(e.row_num) FILTER (WHERE e.expanded_minute >= gc.goal_minute - 10 AND e.expanded_minute < gc.goal_minute) AS actions_pre,
        COUNT(e.row_num) FILTER (WHERE e.expanded_minute > gc.goal_minute AND e.expanded_minute <= gc.goal_minute + 10) AS actions_post
    FROM goals_conceded gc
    JOIN events_base e ON e.match_id = gc.match_id AND e.team_id = gc.conceding_team_id
    GROUP BY gc.match_id, gc.conceding_team_id, gc.goal_minute
),

momentum_agg AS (
    SELECT match_id, team_id,
        AVG(CASE WHEN actions_pre > 0 THEN CAST(actions_post AS DOUBLE) / actions_pre ELSE NULL END) AS momentum_delta
    FROM momentum_windows
    GROUP BY match_id, team_id
)

-- 8. ASSEMBLAGE FINAL
SELECT
    b.match_id, 
    b.team_id,
    CASE WHEN b.total_touches > 0  THEN CAST(b.touches_offensive_zone AS DOUBLE) / b.total_touches END AS ws_field_tilt_actions,
    CASE WHEN b.total_passes > 0   THEN CAST(b.turnovers_high_zone AS DOUBLE) / b.total_passes    END AS ws_high_turnover_rate,
    CASE WHEN b.total_passes > 0   THEN CAST(b.deep_completions AS DOUBLE) / b.total_passes       END AS ws_deep_completion_rt,
    m.momentum_delta                                                                              AS ws_momentum_delta,
    CASE WHEN b.total_shots > 0    THEN CAST(COALESCE(q.shots_counter_attack, 0) AS DOUBLE) / b.total_shots END AS ws_counter_shot_rate,
    CASE WHEN b.offensive_actions > 0 THEN CAST(COALESCE(q.set_pieces_offensive, 0) AS DOUBLE) / b.offensive_actions END AS ws_set_piece_pressure,
    CASE WHEN b.att_touches_total > 0 THEN CAST(b.att_touches_left AS DOUBLE) / b.att_touches_total   END AS ws_attack_left_pct,
    CASE WHEN b.att_touches_total > 0 THEN CAST(b.att_touches_center AS DOUBLE) / b.att_touches_total END AS ws_attack_center_pct,
    CASE WHEN b.att_touches_total > 0 THEN CAST(b.att_touches_right AS DOUBLE) / b.att_touches_total  END AS ws_attack_right_pct,
    CASE WHEN b.total_touches > 0  THEN CAST(b.zone_def_touches AS DOUBLE) / b.total_touches END AS ws_zone_def_pct,
    CASE WHEN b.total_touches > 0  THEN CAST(b.zone_mid_touches AS DOUBLE) / b.total_touches END AS ws_zone_mid_pct,
    CASE WHEN b.total_touches > 0  THEN CAST(b.zone_att_touches AS DOUBLE) / b.total_touches END AS ws_zone_att_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(b.shots_six_yard AS DOUBLE) / b.total_shots     END AS ws_shot_six_yard_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(b.shots_penalty_area AS DOUBLE) / b.total_shots END AS ws_shot_penalty_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(b.shots_out_of_box AS DOUBLE) / b.total_shots   END AS ws_shot_oob_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(COALESCE(q.shots_open_play, 0) AS DOUBLE) / b.total_shots END AS ws_shot_open_play_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(COALESCE(q.shots_set_piece, 0) AS DOUBLE) / b.total_shots END AS ws_shot_set_piece_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(COALESCE(q.shots_penalty, 0) AS DOUBLE) / b.total_shots   END AS ws_shot_penalty_att_pct,
    CASE WHEN b.total_shots > 0    THEN CAST(b.goals_scored AS DOUBLE) / b.total_shots                 END AS ws_conversion_rate,
    CASE WHEN b.total_passes > 0   THEN CAST(COALESCE(q.passes_cross, 0) AS DOUBLE) / b.total_passes   END AS ws_cross_rate,
    CASE WHEN b.total_passes > 0   THEN CAST(COALESCE(q.passes_through_ball, 0) AS DOUBLE) / b.total_passes END AS ws_through_ball_rate,
    CASE WHEN b.total_passes > 0   THEN CAST(COALESCE(q.passes_long_ball, 0) AS DOUBLE) / b.total_passes END AS ws_long_ball_rate,
    CASE WHEN b.total_passes > 0   THEN 1.0 - (CAST(COALESCE(q.passes_non_short, 0) AS DOUBLE) / b.total_passes) END AS ws_short_pass_rate,
    CASE WHEN de.opp_att_total > 0 THEN CAST(de.opp_att_my_left AS DOUBLE) / de.opp_att_total   END AS ws_def_exposed_left_pct,
    CASE WHEN de.opp_att_total > 0 THEN CAST(de.opp_att_my_center AS DOUBLE) / de.opp_att_total END AS ws_def_exposed_center_pct,
    CASE WHEN de.opp_att_total > 0 THEN CAST(de.opp_att_my_right AS DOUBLE) / de.opp_att_total  END AS ws_def_exposed_right_pct,
    CASE WHEN b.total_shots_ca > 0 THEN CAST(b.counter_attack_shots AS DOUBLE) / b.total_shots_ca ELSE NULL END AS ws_counter_attack_dna,
    CAST(b.midfield_actions AS DOUBLE) / NULLIF(b.match_total_midfield_actions, 0) AS ws_midfield_control_idx,
    b.ws_defensive_line_height,
    CASE 
        WHEN b.def_actions_left > 0 AND b.def_actions_right > 0 
        THEN (CAST(b.def_actions_left_won AS DOUBLE) / b.def_actions_left) - (CAST(b.def_actions_right_won AS DOUBLE) / b.def_actions_right) 
        ELSE NULL 
    END AS ws_flank_exposure_asymm
FROM master_agg b
LEFT JOIN qualifier_features q ON b.match_id = q.match_id AND b.team_id = q.team_id
LEFT JOIN momentum_agg       m ON b.match_id = m.match_id AND b.team_id = m.team_id
LEFT JOIN defensive_exposure de ON b.match_id = de.match_id AND b.team_id = de.team_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "ws_field_tilt_actions"                                      AS dec_ws_field_tilt_actions,
        "ws_high_turnover_rate"                                      AS dec_ws_high_turnover_rate,
        "ws_deep_completion_rt"                                      AS dec_ws_deep_completion_rt,
        "ws_momentum_delta"                                          AS dec_ws_momentum_delta,
        "ws_counter_shot_rate"                                       AS dec_ws_counter_shot_rate,
        "ws_set_piece_pressure"                                      AS dec_ws_set_piece_pressure,
        "ws_attack_left_pct"                                         AS dec_ws_attack_left_pct,
        "ws_attack_center_pct"                                       AS dec_ws_attack_center_pct,
        "ws_attack_right_pct"                                        AS dec_ws_attack_right_pct,
        "ws_zone_def_pct"                                            AS dec_ws_zone_def_pct,
        "ws_zone_mid_pct"                                            AS dec_ws_zone_mid_pct,
        "ws_zone_att_pct"                                            AS dec_ws_zone_att_pct,
        "ws_shot_six_yard_pct"                                       AS dec_ws_shot_six_yard_pct,
        "ws_shot_penalty_pct"                                        AS dec_ws_shot_penalty_pct,
        "ws_shot_oob_pct"                                            AS dec_ws_shot_oob_pct,
        "ws_shot_open_play_pct"                                      AS dec_ws_shot_open_play_pct,
        "ws_shot_set_piece_pct"                                      AS dec_ws_shot_set_piece_pct,
        "ws_shot_penalty_att_pct"                                    AS dec_ws_shot_penalty_att_pct,
        "ws_conversion_rate"                                         AS dec_ws_conversion_rate,
        "ws_cross_rate"                                              AS dec_ws_cross_rate,
        "ws_through_ball_rate"                                       AS dec_ws_through_ball_rate,
        "ws_long_ball_rate"                                          AS dec_ws_long_ball_rate,
        "ws_short_pass_rate"                                         AS dec_ws_short_pass_rate,
        "ws_def_exposed_left_pct"                                    AS dec_ws_def_exposed_left_pct,
        "ws_def_exposed_center_pct"                                  AS dec_ws_def_exposed_center_pct,
        "ws_def_exposed_right_pct"                                   AS dec_ws_def_exposed_right_pct,
        "ws_counter_attack_dna"                                      AS dec_ws_counter_attack_dna,
        "ws_midfield_control_idx"                                    AS dec_ws_midfield_control_idx,
        "ws_defensive_line_height"                                   AS dec_ws_defensive_line_height,
        "ws_flank_exposure_asymm"                                    AS dec_ws_flank_exposure_asymm
    FROM mdl_body
)

SELECT * FROM mdl_out
