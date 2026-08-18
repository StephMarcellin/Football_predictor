{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
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
-- ══════════════════════════════════════════════════════════════════════════════

WITH

-- 0) Famille 8 agrégée au grain équipe-match : fautes subies (59) + corners
--    for/against (61/62). WHERE match_id IS NOT NULL écarte la ligne orpheline
--    (match_id NULL sous laquelle 52 équipes s'agglutinent). shots_blocked (63)
--    reporté : source CDC erronée (int_defensive_blocks = blocks passe/dégagement).
fouls_drawn_match AS (
    SELECT match_id, drawing_team_id AS team_id, COUNT(*) AS fouls_drawn
    FROM {{ ref('int_fouls_drawn') }}
    WHERE drawing_team_id IS NOT NULL AND match_id IS NOT NULL
    GROUP BY match_id, drawing_team_id
),

corners_match AS (
    SELECT match_id, team_id, SUM(corners_total) AS corners_for
    FROM {{ ref('int_whoscored_player_match') }}
    WHERE match_id IS NOT NULL
    GROUP BY match_id, team_id
),

-- Tirs bloqués par match et par équipe TIREUSE (feature 63). shots_blocked d'une
-- équipe = tirs de son ADVERSAIRE qui ont été contrés → jointure sur opponent.
blocked_shots_match AS (
    SELECT match_id, team_id, SUM(CASE WHEN is_blocked THEN 1 ELSE 0 END) AS n_blocked
    FROM {{ ref('int_shot_placement') }}
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
    FROM {{ ref('backbone') }} b
    LEFT JOIN {{ ref('team_features_ws') }} tw
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
prepared AS (
    SELECT *,
        CASE WHEN gf > ga THEN 3.0 WHEN gf = ga THEN 1.0 ELSE 0.0 END AS points_match,
        CASE WHEN gf > ga THEN 1.0 ELSE 0.0 END AS win_flag,
        CASE WHEN gf = ga THEN 1.0 ELSE 0.0 END AS draw_flag,
        CASE WHEN gf < ga THEN 1.0 ELSE 0.0 END AS loss_flag,
        CASE WHEN gf = 0  THEN 1.0 ELSE 0.0 END AS failed_to_score_flag
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
    SELECT team_id, season,
        AVG((ws_home_xg_per_shot_for     + ws_away_xg_per_shot_for)     / 2.0) AS season_xg_per_shot_for_lag,
        AVG((ws_home_xg_per_shot_against + ws_away_xg_per_shot_against) / 2.0) AS season_xg_per_shot_against_lag
    FROM {{ ref('int_whoscored_team_season') }}
    GROUP BY team_id, season
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
)

SELECT
    r.*,
    ts.season_xg_per_shot_for_lag,
    ts.season_xg_per_shot_against_lag
FROM rolled r
LEFT JOIN team_season ts
    ON ts.team_id = r.team_id
   -- saison N-1 calendaire : '2023-2024' → '2022-2023'
   AND ts.season = (CAST(LEFT(r.season, 4) AS INTEGER) - 1)::VARCHAR || '-' || LEFT(r.season, 4)

{% if is_incremental() %}
WHERE r.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
