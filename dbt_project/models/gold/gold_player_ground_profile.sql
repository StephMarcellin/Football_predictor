{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_player_ground_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_player_ground_profile
-- Profil « duels au sol » d'un joueur (dribbles, tacles, dépossessions, fautes) sur une
-- FENÊTRE DE 3 SAISONS GLISSANTES.
-- Grain : (str_player_id, str_season) où str_season est la DERNIÈRE saison de la fenêtre
--         (saisons s-2, s-1, s). Une ligne n'existe que si le joueur a disputé au moins un
--         duel au sol en s.
--
-- ⚠️ FUITE : la fenêtre INCLUT la saison str_season. Pour prédire un match de la saison s,
--    un modèle doit lire la ligne str_season = s-1 (convention « lag » des autres tables).
--
-- Source : intermediate_ground_duel_players (depuis 2014-2015), toutes ligues présentes.
--
-- « L'ATTENDU » : le taux de réussite moyen de la saison du duel pour le même contexte
-- (famille, rôle, zone 5×5 du duel). Un dribble dans le tiers offensif réussit 50 % du
-- temps, dans le tiers défensif 63 % : un taux brut mélange le talent et le terrain.
-- Les colonnes dec_*_woe (win over expected) = somme des (issue − attendu) sur la fenêtre,
-- rétrécie vers 0 avec K = 30 duels : un joueur à petit volume reste proche de la moyenne.
-- Les taux bruts (dec_*_win_rate, volumes) restent disponibles à côté pour les graphiques.
--
-- AJUSTEMENT PAR L'ADVERSAIRE (dribbles uniquement) : dec_dribble_woe_adj et
-- dec_defended_woe_adj retirent l'effet de la force de l'adversaire affronté. Un tour de
-- « backfitting » : (1) pour chaque joueur-saison, effet = somme des (issue − attendu de
-- zone) rétrécie (K = 30) ; (2) pour chaque dribble, l'attendu est décalé de l'effet de
-- l'adversaire, en EXCLUANT le duel lui-même (leave-one-out) ; (3) on resomme. Descriptif :
-- les effets de la saison s utilisent des duels de la saison s.
--
-- Danger : dec_*_danger_net = écart de tirs (propres − adverses) dans les 10 s qui suivent
-- le duel, net de la moyenne du contexte (famille, rôle, zone), rétréci avec K = 30. Il
-- mêle l'issue immédiate et la suite de l'action.
--
-- Pairage : seuls les dribbles APPARIÉS à un défenseur figurent dans les taux (≈ 74 % des
-- dribbles ratés ont un tacle apparié ; les ratés sans défenseur identifié sont absents).
-- Le taux de réussite d'un dribbleur est donc biaisé à la hausse par rapport au taux brut
-- des événements TakeOn ; les volumes bruts (dribbles tentés, tacles) viennent de
-- intermediate_player_match_event_stats quand ils y sont.
--
-- xT après duel gagné : xT du geste suivant (passe ou conduite) du joueur qui vient de gagner
-- son duel (dribble réussi, tacle gagnant), seulement quand c'est lui qui enchaîne et que le
-- geste porte un xT ; rétréci vers la moyenne avec K = 30 gestes.
--
-- Fautes : parts commises / subies rapportées à l'exposition (duels défensifs / offensifs
-- au sol + fautes) et à 90 minutes ; cartons dans les 120 s après la faute (rétréci K = 20),
-- penaltys concédés / obtenus.
-- ══════════════════════════════════════════════════════════════════════════════

{% set k_duels = 30 %}
{% set k_fouls = 40 %}
{% set k_cards = 20 %}

WITH

-- xT du geste suivant, quand le joueur qui a gagné le duel enchaîne lui-même. On part de
-- la petite table des gestes suivants (≈ 700 000 lignes) pour ne pas construire une table
-- de hachage sur les ≈ 29 M de lignes de intermediate_xt_contributions. xT n'existe que
-- pour les gestes qui déplacent le ballon (passes, conduites) : le reste vaut NULL.
xt_follow AS (
    SELECT f.str_match_id, f.int_next_event_row_num AS row_num, xt.dec_xt_added
    FROM (
        SELECT DISTINCT str_match_id, int_next_event_row_num
        FROM {{ ref('intermediate_ground_duel_players') }}
        WHERE bool_is_won AND str_next_event_player_id = str_player_id
          AND str_duel_family IN ('dribble', 'dispossession') AND str_season >= '2014-2015'
    ) f
    JOIN {{ ref('intermediate_xt_contributions') }} xt
      ON xt.str_match_id = f.str_match_id AND xt.int_row_num = f.int_next_event_row_num
),

duels AS (
    SELECT
        d.str_player_id,
        d.str_opp_player_id,
        d.str_season,
        CAST(SUBSTR(d.str_season, 1, 4) AS INTEGER)                  AS y,
        d.str_duel_family                                            AS family,
        d.str_role                                                   AS role,
        d.str_zone_5x5                                               AS zone,
        d.str_zone_depth                                             AS depth,
        d.str_zone_corridor                                          AS corridor,
        CAST(d.bool_is_won AS INTEGER)                               AS won,
        CAST(d.bool_ball_recovered AS INTEGER)                       AS recovered,
        CAST(d.int_own_shots_10s - d.int_opp_shots_10s AS INTEGER)   AS shot_diff,
        CAST(COALESCE(d.bool_yellow_card_120s, FALSE) AS INTEGER)    AS yellow,
        CAST(COALESCE(d.bool_red_card_120s, FALSE) AS INTEGER)       AS red,
        CAST(COALESCE(d.bool_leads_to_penalty, FALSE) AS INTEGER)    AS penalty,
        CASE WHEN d.bool_is_won AND d.str_next_event_player_id = d.str_player_id
             THEN xf.dec_xt_added END                                AS xt
    FROM {{ ref('intermediate_ground_duel_players') }} d
    LEFT JOIN xt_follow xf
      ON xf.str_match_id = d.str_match_id AND xf.row_num = d.int_next_event_row_num
    WHERE d.str_season >= '2014-2015'
),

-- Attendu : par saison, famille, rôle, zone 5×5 du duel (issue moyenne et écart de tirs moyen)
baseline AS (
    SELECT str_season, family, role, zone,
           AVG(won)        AS p_win,
           AVG(shot_diff)  AS p_shot_diff
    FROM duels
    WHERE family IN ('dribble', 'dispossession')
    GROUP BY str_season, family, role, zone
),

-- Tour 1 : effet de zone de chaque joueur-saison, pour les dribbles
dribble_rows AS (
    SELECT d.*, b.p_win, b.p_shot_diff, (d.won - b.p_win) AS r1
    FROM duels d
    JOIN baseline b
      ON b.str_season = d.str_season AND b.family = d.family AND b.role = d.role AND b.zone = d.zone
    WHERE d.family = 'dribble'
),

effects AS (
    SELECT str_player_id, str_season, role,
           COUNT(*)  AS n,
           SUM(r1)   AS s1
    FROM dribble_rows
    GROUP BY str_player_id, str_season, role
),

-- Tour 2 : l'attendu de chaque dribble est décalé de l'effet de l'adversaire (sans le duel
-- lui-même). Le résidu de l'adversaire dans ce duel est l'opposé du nôtre (p_win des deux
-- rôles se complètent) : son effet hors duel = (s1_adv + r1) / (n_adv - 1 + K).
dribble_adj AS (
    SELECT
        r.str_player_id, r.str_season, r.y, r.role, r.depth, r.corridor,
        r.won, r.recovered, r.r1, r.shot_diff, r.p_shot_diff, r.xt,
        r.r1 + (e.s1 + r.r1) / (e.n - 1 + {{ k_duels }}) AS r_adj
    FROM dribble_rows r
    JOIN effects e
      ON e.str_player_id = r.str_opp_player_id AND e.str_season = r.str_season
     AND e.role = CASE r.role WHEN 'attaquant' THEN 'defenseur' ELSE 'attaquant' END
),

other_rows AS (
    SELECT d.*, b.p_win, b.p_shot_diff
    FROM duels d
    LEFT JOIN baseline b
      ON b.str_season = d.str_season AND b.family = d.family AND b.role = d.role AND b.zone = d.zone
    WHERE d.family <> 'dribble'
),

per_season AS (
    SELECT
        str_player_id, str_season, y,
        -- Dribbleur
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant')                       AS n_dr,
        SUM(won) FILTER (WHERE fam = 'dribble' AND role = 'attaquant')                       AS won_dr,
        COALESCE(SUM(r1) FILTER (WHERE fam = 'dribble' AND role = 'attaquant'), 0)           AS woe_dr,
        COALESCE(SUM(r_adj) FILTER (WHERE fam = 'dribble' AND role = 'attaquant'), 0)        AS woe_adj_dr,
        COALESCE(SUM(shot_diff - p_shot_diff) FILTER (WHERE fam = 'dribble' AND role = 'attaquant'), 0) AS danger_dr,
        COUNT(xt) FILTER (WHERE fam = 'dribble' AND role = 'attaquant')                      AS n_xt_dr,
        COALESCE(SUM(xt) FILTER (WHERE fam = 'dribble' AND role = 'attaquant'), 0)           AS xt_dr,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND depth = 'tiers_def') AS n_dr_def3,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND depth = 'milieu')    AS n_dr_mid3,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND depth = 'tiers_off') AS n_dr_off3,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND corridor = 'gauche') AS n_dr_left,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND corridor = 'axe')    AS n_dr_axis,
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'attaquant' AND corridor = 'droit')  AS n_dr_right,
        -- Défenseur face aux dribbles
        COUNT(*) FILTER (WHERE fam = 'dribble' AND role = 'defenseur')                       AS n_df,
        SUM(won) FILTER (WHERE fam = 'dribble' AND role = 'defenseur')                       AS won_df,
        COALESCE(SUM(r1) FILTER (WHERE fam = 'dribble' AND role = 'defenseur'), 0)           AS woe_df,
        COALESCE(SUM(r_adj) FILTER (WHERE fam = 'dribble' AND role = 'defenseur'), 0)        AS woe_adj_df,
        COALESCE(SUM(shot_diff - p_shot_diff) FILTER (WHERE fam = 'dribble' AND role = 'defenseur'), 0) AS danger_df,
        COUNT(xt) FILTER (WHERE role = 'defenseur' AND fam IN ('dribble', 'dispossession'))  AS n_xt_df,
        COALESCE(SUM(xt) FILTER (WHERE role = 'defenseur' AND fam IN ('dribble', 'dispossession')), 0) AS xt_df,
        COUNT(recovered) FILTER (WHERE fam = 'dribble' AND role = 'defenseur')               AS n_df_tackle,
        COALESCE(SUM(recovered) FILTER (WHERE fam = 'dribble' AND role = 'defenseur'), 0)    AS rec_df,
        -- Tacleur sur dépossession
        COUNT(*) FILTER (WHERE fam = 'dispossession' AND role = 'defenseur')                 AS n_tk,
        SUM(won) FILTER (WHERE fam = 'dispossession' AND role = 'defenseur')                 AS won_tk,
        COALESCE(SUM(won - p_win) FILTER (WHERE fam = 'dispossession' AND role = 'defenseur'), 0) AS woe_tk,
        COALESCE(SUM(shot_diff - p_shot_diff) FILTER (WHERE fam = 'dispossession' AND role = 'defenseur'), 0) AS danger_tk,
        -- Dépossédé
        COUNT(*) FILTER (WHERE fam = 'dispossession' AND role = 'attaquant')                 AS n_ds,
        COALESCE(SUM(shot_diff - p_shot_diff) FILTER (WHERE fam = 'dispossession' AND role = 'attaquant'), 0) AS danger_ds,
        -- Fautes
        COUNT(*) FILTER (WHERE fam = 'faute' AND role = 'auteur')                            AS n_fc,
        COUNT(*) FILTER (WHERE fam = 'faute' AND role = 'victime')                           AS n_fd,
        COUNT(*) FILTER (WHERE fam = 'faute' AND role = 'auteur' AND depth = 'tiers_off')    AS n_fc_own3,
        COUNT(*) FILTER (WHERE fam = 'faute' AND role = 'victime' AND depth = 'tiers_off')   AS n_fd_att3,
        COALESCE(SUM(yellow) FILTER (WHERE fam = 'faute' AND role = 'auteur'), 0)            AS yellow_fc,
        COALESCE(SUM(red) FILTER (WHERE fam = 'faute' AND role = 'auteur'), 0)               AS red_fc,
        COALESCE(SUM(penalty) FILTER (WHERE fam = 'faute' AND role = 'auteur'), 0)           AS pen_fc,
        COALESCE(SUM(penalty) FILTER (WHERE fam = 'faute' AND role = 'victime'), 0)          AS pen_fd
    FROM (
        SELECT str_player_id, str_season, y, 'dribble' AS fam, role, depth, corridor, won, recovered,
               r1, r_adj, shot_diff, p_shot_diff, xt, 0 AS yellow, 0 AS red, 0 AS penalty, NULL::DOUBLE AS p_win
        FROM dribble_adj
        UNION ALL
        SELECT str_player_id, str_season, y, family, role, depth, corridor, won, recovered,
               NULL, NULL, shot_diff, p_shot_diff, xt, yellow, red, penalty, p_win
        FROM other_rows
    )
    GROUP BY str_player_id, str_season, y
),

player_season AS (
    SELECT
        str_player_id, str_season,
        SUM(int_n_matchs)                          AS n_matches,
        arg_max(str_player_name, int_n_matchs)     AS player_name,
        arg_max(str_macro_position, int_n_matchs)  AS macro_position
    FROM {{ ref('intermediate_whoscored_player_team_season') }}
    GROUP BY str_player_id, str_season
),

minutes_season AS (
    SELECT str_player_id, str_season,
           SUM(int_minutes_played)  AS minutes,
           SUM(int_n_tackles)       AS tackles_raw,
           SUM(int_n_tackles_won)   AS tackles_won_raw
    FROM {{ ref('intermediate_player_match_event_stats') }}
    GROUP BY str_player_id, str_season
),

season_full AS (
    SELECT
        p.*,
        COALESCE(m.n_matches, 0)       AS n_matches,
        COALESCE(mi.minutes, 0)        AS minutes,
        COALESCE(mi.tackles_raw, 0)    AS tackles_raw,
        COALESCE(mi.tackles_won_raw, 0) AS tackles_won_raw
    FROM per_season p
    LEFT JOIN player_season m  ON m.str_player_id = p.str_player_id AND m.str_season = p.str_season
    LEFT JOIN minutes_season mi ON mi.str_player_id = p.str_player_id AND mi.str_season = p.str_season
),

-- Moyennes de référence pour le rétrécissement des parts et des taux
priors AS (
    SELECT
        SUM(n_ds) * 1.0 / NULLIF(SUM(n_dr + n_ds), 0)                          AS p_dispossessed,
        SUM(n_fc) * 1.0 / NULLIF(SUM(n_df + n_tk + n_fc), 0)                   AS p_foul_committed,
        SUM(n_fd) * 1.0 / NULLIF(SUM(n_dr + n_ds + n_fd), 0)                   AS p_foul_drawn,
        SUM(yellow_fc) * 1.0 / NULLIF(SUM(n_fc), 0)                            AS p_yellow,
        SUM(rec_df) * 1.0 / NULLIF(SUM(n_df_tackle), 0)                        AS p_recovered,
        SUM(xt_dr) / NULLIF(SUM(n_xt_dr), 0)                                   AS p_xt_dr,
        SUM(xt_df) / NULLIF(SUM(n_xt_df), 0)                                   AS p_xt_df
    FROM season_full
),

-- Fenêtre : saisons s-2, s-1, s
win AS (
    SELECT
        a.str_player_id, a.str_season,
        COUNT(*)                AS n_seasons,
        SUM(b.n_dr) AS n_dr, SUM(b.won_dr) AS won_dr, SUM(b.woe_dr) AS woe_dr,
        SUM(b.woe_adj_dr) AS woe_adj_dr, SUM(b.danger_dr) AS danger_dr,
        SUM(b.n_xt_dr) AS n_xt_dr, SUM(b.xt_dr) AS xt_dr, SUM(b.n_xt_df) AS n_xt_df, SUM(b.xt_df) AS xt_df,
        SUM(b.n_dr_def3) AS n_dr_def3, SUM(b.n_dr_mid3) AS n_dr_mid3, SUM(b.n_dr_off3) AS n_dr_off3,
        SUM(b.n_dr_left) AS n_dr_left, SUM(b.n_dr_axis) AS n_dr_axis, SUM(b.n_dr_right) AS n_dr_right,
        SUM(b.n_df) AS n_df, SUM(b.won_df) AS won_df, SUM(b.woe_df) AS woe_df,
        SUM(b.woe_adj_df) AS woe_adj_df, SUM(b.danger_df) AS danger_df,
        SUM(b.n_df_tackle) AS n_df_tackle, SUM(b.rec_df) AS rec_df,
        SUM(b.n_tk) AS n_tk, SUM(b.won_tk) AS won_tk, SUM(b.woe_tk) AS woe_tk, SUM(b.danger_tk) AS danger_tk,
        SUM(b.n_ds) AS n_ds, SUM(b.danger_ds) AS danger_ds,
        SUM(b.n_fc) AS n_fc, SUM(b.n_fd) AS n_fd,
        SUM(b.n_fc_own3) AS n_fc_own3, SUM(b.n_fd_att3) AS n_fd_att3,
        SUM(b.yellow_fc) AS yellow_fc, SUM(b.red_fc) AS red_fc,
        SUM(b.pen_fc) AS pen_fc, SUM(b.pen_fd) AS pen_fd,
        SUM(b.n_matches) AS n_matches, SUM(b.minutes) AS minutes,
        SUM(b.tackles_raw) AS tackles_raw, SUM(b.tackles_won_raw) AS tackles_won_raw
    FROM season_full a
    JOIN season_full b
      ON b.str_player_id = a.str_player_id AND b.y BETWEEN a.y - 2 AND a.y
    GROUP BY a.str_player_id, a.str_season
),

mdl_out AS (
    SELECT
        w.str_player_id,
        w.str_season,
        ps.player_name                                                        AS str_player_name,
        ps.macro_position                                                     AS str_macro_position,
        CAST(w.n_seasons AS INTEGER)                                          AS int_n_seasons_window,
        CAST(w.n_matches AS INTEGER)                                          AS int_n_matches,
        CAST(w.minutes AS INTEGER)                                            AS int_minutes,
        CAST(w.n_dr + w.n_df + w.n_tk + w.n_ds + w.n_fc + w.n_fd AS INTEGER)  AS int_n_ground_duels,

        -- ── Dribbleur (appariés) ──
        CAST(w.n_dr AS INTEGER)                                               AS int_n_dribbles,
        CAST(w.won_dr AS INTEGER)                                             AS int_n_dribbles_won,
        w.won_dr * 1.0 / NULLIF(w.n_dr, 0)                                    AS dec_dribble_win_rate,
        w.woe_dr / (w.n_dr + {{ k_duels }})                                   AS dec_dribble_woe,
        w.woe_adj_dr / (w.n_dr + {{ k_duels }})                               AS dec_dribble_woe_adj,
        w.danger_dr / (w.n_dr + {{ k_duels }})                                AS dec_dribble_danger_net,
        w.n_dr * 90.0 / NULLIF(w.minutes, 0)                                  AS dec_dribbles_per_90,
        CAST(w.n_xt_dr AS INTEGER)                                            AS int_n_dribble_followups_xt,
        (w.xt_dr + {{ k_duels }} * pr.p_xt_dr) / (w.n_xt_dr + {{ k_duels }})   AS dec_dribble_xt_after_win,
        w.n_dr_def3 * 1.0 / NULLIF(w.n_dr, 0)                                 AS dec_dribble_share_tiers_def,
        w.n_dr_mid3 * 1.0 / NULLIF(w.n_dr, 0)                                 AS dec_dribble_share_milieu,
        w.n_dr_off3 * 1.0 / NULLIF(w.n_dr, 0)                                 AS dec_dribble_share_tiers_off,
        w.n_dr_left * 1.0 / NULLIF(w.n_dr, 0)                                 AS dec_dribble_share_gauche,
        w.n_dr_axis * 1.0 / NULLIF(w.n_dr, 0)                                 AS dec_dribble_share_axe,
        w.n_dr_right * 1.0 / NULLIF(w.n_dr, 0)                                AS dec_dribble_share_droit,

        -- ── Défenseur face aux dribbles (appariés) ──
        CAST(w.n_df AS INTEGER)                                               AS int_n_dribbles_faced,
        w.won_df * 1.0 / NULLIF(w.n_df, 0)                                    AS dec_defended_win_rate,
        w.woe_df / (w.n_df + {{ k_duels }})                                   AS dec_defended_woe,
        w.woe_adj_df / (w.n_df + {{ k_duels }})                               AS dec_defended_woe_adj,
        w.danger_df / (w.n_df + {{ k_duels }})                                AS dec_defended_danger_net,
        w.n_df * 90.0 / NULLIF(w.minutes, 0)                                  AS dec_dribbles_faced_per_90,
        (w.rec_df + {{ k_duels }} * pr.p_recovered) / (w.n_df_tackle + {{ k_duels }}) AS dec_defended_recovery_rate,
        CAST(w.n_xt_df AS INTEGER)                                            AS int_n_tackle_followups_xt,
        (w.xt_df + {{ k_duels }} * pr.p_xt_df) / (w.n_xt_df + {{ k_duels }})   AS dec_tackle_xt_after_win,

        -- ── Tacleur sur dépossession ──
        CAST(w.n_tk AS INTEGER)                                               AS int_n_tackles_dispossession,
        w.won_tk * 1.0 / NULLIF(w.n_tk, 0)                                    AS dec_tackle_win_rate,
        w.woe_tk / (w.n_tk + {{ k_duels }})                                   AS dec_tackle_woe,
        w.danger_tk / (w.n_tk + {{ k_duels }})                                AS dec_tackle_danger_net,
        CAST(w.tackles_raw AS INTEGER)                                        AS int_n_tackles_raw,
        w.tackles_won_raw * 1.0 / NULLIF(w.tackles_raw, 0)                    AS dec_tackle_win_rate_raw,
        w.tackles_raw * 90.0 / NULLIF(w.minutes, 0)                           AS dec_tackles_raw_per_90,

        -- ── Dépossédé ──
        CAST(w.n_ds AS INTEGER)                                               AS int_n_dispossessed,
        (w.n_ds + {{ k_duels }} * pr.p_dispossessed) / (w.n_dr + w.n_ds + {{ k_duels }}) AS dec_dispossessed_share,
        w.n_ds * 90.0 / NULLIF(w.minutes, 0)                                  AS dec_dispossessed_per_90,
        w.danger_ds / (w.n_ds + {{ k_duels }})                                AS dec_dispossessed_danger_net,

        -- ── Fautes, cartons, penaltys ──
        CAST(w.n_fc AS INTEGER)                                               AS int_n_fouls_committed,
        CAST(w.n_fd AS INTEGER)                                               AS int_n_fouls_drawn,
        w.n_fc * 90.0 / NULLIF(w.minutes, 0)                                  AS dec_fouls_committed_per_90,
        w.n_fd * 90.0 / NULLIF(w.minutes, 0)                                  AS dec_fouls_drawn_per_90,
        (w.n_fc + {{ k_fouls }} * pr.p_foul_committed)
            / (w.n_df + w.n_tk + w.n_fc + {{ k_fouls }})                      AS dec_foul_committed_share,
        (w.n_fd + {{ k_fouls }} * pr.p_foul_drawn)
            / (w.n_dr + w.n_ds + w.n_fd + {{ k_fouls }})                      AS dec_foul_drawn_share,
        w.n_fc_own3 * 1.0 / NULLIF(w.n_fc, 0)                                 AS dec_foul_committed_share_zone_off,
        w.n_fd_att3 * 1.0 / NULLIF(w.n_fd, 0)                                 AS dec_foul_drawn_share_zone_off,
        CAST(w.yellow_fc AS INTEGER)                                          AS int_n_yellow_after_foul,
        CAST(w.red_fc AS INTEGER)                                             AS int_n_red_after_foul,
        (w.yellow_fc + {{ k_cards }} * pr.p_yellow) / (w.n_fc + {{ k_cards }}) AS dec_yellow_per_foul,
        CAST(w.pen_fc AS INTEGER)                                             AS int_n_penalties_conceded,
        CAST(w.pen_fd AS INTEGER)                                             AS int_n_penalties_drawn
    FROM win w
    CROSS JOIN priors pr
    LEFT JOIN player_season ps ON ps.str_player_id = w.str_player_id AND ps.str_season = w.str_season
)

SELECT * FROM mdl_out
