{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_player_aerial_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_player_aerial_profile
-- Profil aérien d'un joueur sur une FENÊTRE DE 3 SAISONS GLISSANTES.
-- Grain : (str_player_id, str_season) où str_season est la DERNIÈRE saison de la fenêtre
--         (saisons s-2, s-1, s). Une ligne n'existe que si le joueur a disputé des duels
--         aériens en s.
--
-- ⚠️ FUITE : la fenêtre INCLUT la saison str_season. Pour prédire un match de la saison s,
--    un modèle doit lire la ligne str_season = s-1 (convention « lag » des autres tables).
--
-- Source : intermediate_aerial_duel_players (depuis 2014-2015 : le pairage 2013-2014 est
-- trop fragile, voir docs/JOURNAL_RESULTATS.md), toutes ligues présentes.
--
-- Mesures (prototype validé le 07/11/2026 : stabilité d'une saison à l'autre) :
--   · dec_woe / _off / _def : victoire AU-DESSUS DE L'ATTENDU. L'attendu est le taux de
--     victoire moyen de la saison du duel pour (rôle, type de ballon). Rétrécie vers 0
--     avec K = 30 duels : un joueur à petit volume reste proche de la moyenne.
--     Stabilité N / N+1 : 0,73 global, 0,69 offensif, 0,56 défensif. Corrélée à la taille (0,47).
--   · dec_head_shot_rate : tirs de la tête par duel offensif (rétréci vers la moyenne,
--     K = 30). Stabilité 0,66. Compétence DISTINCTE de gagner les duels (corrélation 0,26).
--   · dec_follow_* : ce que fait le gagnant de son ballon (geste suivant, 99,9 % du temps
--     le gagnant lui-même). Part de passes / dégagements / tirs : stabilité modérée
--     (0,34 à 0,59). Précision, progression et xT de la passe de la tête : BRUIT à ces
--     volumes (stabilité 0,2) → rétrécis vers la moyenne avec K = 20 passes.
--   · dec_shot_after_win_off : part des victoires offensives suivies d'un tir de l'équipe
--     dans les 10 s (brut, non rétréci ; dépend du type de ballon et du contexte d'équipe).
--   · dec_woe_net / _off / _def : victoire au-dessus de l'attendu NET DE LA TAILLE. L'attendu est
--     le taux de victoire de la saison pour (rôle, groupe de type de ballon, écart de taille
--     entre le joueur et son adversaire par tranches de 4 cm). Un grand joueur n'est plus
--     crédité d'être grand. Rétréci avec K = 30. Mesuré le 09/11/2026 sur la table construite
--     (profils s et s+3, fenêtres SANS recouvrement, au moins 150 duels, 4 208 paires) :
--     stabilité 0,69 attaque et 0,75 défense (0,74 et 0,80 sans correction de taille).
--     Corrélation à la taille : attaque -0,20 (0,37 avant), défense 0,01 (0,54 avant).
--     Résidu négatif en attaque : la correction par tranches de 4 cm sur-corrige un peu les
--     très grands attaquants.
--     dec_height_diff_mean : écart de taille moyen avec les adversaires affrontés (cm).
--   · dec_head_shots_setpiece_per_match / _open_per_match : tirs de la tête sur corner ou
--     coup franc (qualifiers 24, 25) contre tirs de la tête en jeu ouvert, par match.
--     Mesuré le 09/11/2026 : stabilité d'une saison à l'autre 0,64 (CPA) et 0,68 (jeu ouvert),
--     corrélation entre les deux 0,18 (profils s et s+3 sur la table : 0,72 et 0,81).
-- Taille : valeur COURANTE de la fiche WhoScored (voir intermediate_whoscored_player_team_season),
-- valeurs <= 150 cm traitées comme manquantes ; MAX sur toutes les saisons du joueur.
-- xT : intermediate_xt_contributions (gain xT du déplacement), jointure sur le numéro de
-- ligne du geste suivant, passes de la tête du gagnant uniquement.
-- ══════════════════════════════════════════════════════════════════════════════

{% set k_duels = 30 %}
{% set k_passes = 20 %}

WITH

player_height AS (
    SELECT str_player_id, MAX(CASE WHEN int_height > 150 THEN int_height END) AS h
    FROM {{ ref('intermediate_whoscored_player_team_season') }}
    GROUP BY str_player_id
),

duels AS (
    SELECT
        d.str_player_id,
        d.str_team_id,
        d.str_season,
        CAST(SUBSTR(d.str_season, 1, 4) AS INTEGER)          AS y,
        d.str_match_id,
        d.str_role,
        d.str_ball_type,
        CAST(d.bool_is_won AS INTEGER)                       AS won,
        CAST(d.int_own_shots_10s > 0 AS INTEGER)             AS shot10,
        (d.bool_is_won AND d.bool_next_event_is_own_player)  AS own_follow,
        d.str_next_event_type,
        d.bool_next_event_success,
        d.dec_next_event_progress,
        d.int_next_event_row_num,
        ph.h                                                 AS own_h,
        po.h                                                 AS opp_h,
        CASE WHEN ph.h IS NULL OR po.h IS NULL THEN 99
             ELSE LEAST(GREATEST(CAST(ROUND((ph.h - po.h) / 4.0) AS INTEGER), -4), 4) END AS hbucket,
        CASE WHEN d.str_ball_type IN ('corner', 'coup_franc') THEN 'cpa'
             WHEN d.str_ball_type = 'centre' THEN 'centre'
             WHEN d.str_ball_type IN ('six_metres', 'longue_balle', 'passe_lobee', 'degagement') THEN 'long'
             ELSE 'autre' END                                AS ball_group
    FROM {{ ref('intermediate_aerial_duel_players') }} d
    LEFT JOIN player_height ph ON ph.str_player_id = d.str_player_id
    LEFT JOIN player_height po ON po.str_player_id = d.str_opp_player_id
    WHERE d.str_season >= '2014-2015'
      AND d.str_role IN ('offensif', 'defensif')
),

-- Taux de victoire attendu : par saison, rôle et type de ballon
baseline AS (
    SELECT str_season, str_role, str_ball_type, AVG(won) AS p_win
    FROM duels
    GROUP BY str_season, str_role, str_ball_type
),

-- Taux de victoire attendu NET DE LA TAILLE : saison, rôle, groupe de ballon, tranche d'écart de taille
-- (tranche 99 = taille manquante pour l'un des deux joueurs)
baseline_net AS (
    SELECT str_season, str_role, ball_group, hbucket, AVG(won) AS p_net
    FROM duels
    GROUP BY str_season, str_role, ball_group, hbucket
),

-- xT des seules passes de la tête des gagnants : on part de la petite table des gestes
-- suivants (environ 600 000 lignes) pour ne pas construire une table de hachage sur les
-- 29 M de lignes de intermediate_xt_contributions.
xt_follow AS (
    SELECT f.str_match_id, f.int_next_event_row_num AS row_num, xt.dec_xt_added
    FROM (
        SELECT DISTINCT str_match_id, int_next_event_row_num
        FROM duels
        WHERE own_follow AND str_next_event_type = 'Pass'
    ) f
    JOIN {{ ref('intermediate_xt_contributions') }} xt
      ON xt.str_match_id = f.str_match_id AND xt.int_row_num = f.int_next_event_row_num
),

duels_x AS (
    SELECT d.*, b.p_win, bn.p_net,
           CASE WHEN d.own_follow AND d.str_next_event_type = 'Pass' THEN xf.dec_xt_added END AS dec_xt_added
    FROM duels d
    JOIN baseline b
      ON b.str_season = d.str_season AND b.str_role = d.str_role AND b.str_ball_type = d.str_ball_type
    JOIN baseline_net bn
      ON bn.str_season = d.str_season AND bn.str_role = d.str_role
     AND bn.ball_group = d.ball_group AND bn.hbucket = d.hbucket
    LEFT JOIN xt_follow xf
      ON xf.str_match_id = d.str_match_id AND xf.row_num = d.int_next_event_row_num
),

per_season AS (
    SELECT
        str_player_id, str_season, y,
        COUNT(*)                                                                   AS n,
        COUNT(*) FILTER (WHERE str_role = 'offensif')                              AS n_off,
        COUNT(*) FILTER (WHERE str_role = 'defensif')                              AS n_def,
        SUM(won)                                                                   AS n_won,
        SUM(won) FILTER (WHERE str_role = 'offensif')                              AS won_off,
        SUM(won) FILTER (WHERE str_role = 'defensif')                              AS won_def,
        COALESCE(SUM(won - p_win) FILTER (WHERE str_role = 'offensif'), 0)         AS woe_off,
        COALESCE(SUM(won - p_win) FILTER (WHERE str_role = 'defensif'), 0)         AS woe_def,
        COALESCE(SUM(won - p_net) FILTER (WHERE str_role = 'offensif'), 0)         AS woe_net_off,
        COALESCE(SUM(won - p_net) FILTER (WHERE str_role = 'defensif'), 0)         AS woe_net_def,
        COUNT(own_h - opp_h)                                                       AS n_hdiff,
        COALESCE(SUM(own_h - opp_h), 0)                                            AS hdiff_sum,
        COUNT(*) FILTER (WHERE str_ball_type IN ('corner', 'coup_franc'))          AS n_setpiece,
        COUNT(*) FILTER (WHERE str_ball_type = 'centre')                           AS n_cross,
        COUNT(*) FILTER (WHERE str_ball_type IN ('six_metres', 'longue_balle', 'degagement')) AS n_long,
        COUNT(*) FILTER (WHERE own_follow)                                         AS n_follow,
        COUNT(*) FILTER (WHERE own_follow AND str_next_event_type = 'Pass')        AS n_f_pass,
        COUNT(*) FILTER (WHERE own_follow AND str_next_event_type = 'Clearance')   AS n_f_clear,
        COUNT(*) FILTER (WHERE own_follow AND str_next_event_type IN ('MissedShots', 'SavedShot', 'Goal', 'ShotOnPost')) AS n_f_shot,
        COUNT(*) FILTER (WHERE own_follow AND str_next_event_type = 'Pass' AND bool_next_event_success) AS n_f_pass_ok,
        COUNT(dec_next_event_progress) FILTER (WHERE own_follow AND str_next_event_type = 'Pass')       AS n_f_pass_prog,
        COALESCE(SUM(dec_next_event_progress) FILTER (WHERE own_follow AND str_next_event_type = 'Pass'), 0) AS f_pass_progress,
        COUNT(dec_xt_added)                                                        AS n_xt,
        COALESCE(SUM(dec_xt_added), 0)                                             AS xt_sum,
        COUNT(*) FILTER (WHERE won = 1 AND str_role = 'offensif')                  AS n_off_wins,
        COALESCE(SUM(shot10) FILTER (WHERE won = 1 AND str_role = 'offensif'), 0)  AS off_wins_shot
    FROM duels_x
    GROUP BY str_player_id, str_season, y
),

-- Tirs de la tête (qualifier 15), hors csc
head_q AS (
    SELECT str_match_id, int_row_num
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
    WHERE str_qual_type_id = '15' AND str_season >= '2014-2015'
),

-- Tirs sur coup de pied arrêté (qualifiers 24 SetPiece, 25 FromCorner)
sp_q AS (
    SELECT DISTINCT str_match_id, int_row_num
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
    WHERE str_qual_type_id IN ('24', '25') AND str_season >= '2014-2015'
),

head_shots AS (
    SELECT s.str_player_id, s.str_season,
           COUNT(*)                                                 AS head_shots,
           COUNT(*) FILTER (WHERE s.bool_is_goal)                   AS head_goals,
           COUNT(*) FILTER (WHERE sp.int_row_num IS NOT NULL)       AS head_shots_sp,
           COUNT(*) FILTER (WHERE sp.int_row_num IS NULL)           AS head_shots_open
    FROM {{ ref('intermediate_shots') }} s
    JOIN head_q q
      ON  q.str_match_id = s.str_match_id
      AND q.int_row_num  = s.int_row_num
    LEFT JOIN sp_q sp
      ON  sp.str_match_id = s.str_match_id
      AND sp.int_row_num  = s.int_row_num
    WHERE s.str_season >= '2014-2015'
      AND NOT COALESCE(s.bool_is_own_goal, FALSE)
    GROUP BY s.str_player_id, s.str_season
),

player_season AS (
    SELECT
        str_player_id, str_season,
        SUM(int_n_matchs)                                         AS n_matches,
        arg_max(str_player_name, int_n_matchs)                    AS player_name,
        arg_max(str_macro_position, int_n_matchs)                 AS macro_position,
        MAX(CASE WHEN int_height > 150 THEN int_height END)       AS height
    FROM {{ ref('intermediate_whoscored_player_team_season') }}
    GROUP BY str_player_id, str_season
),

season_full AS (
    SELECT
        p.*,
        COALESCE(h.head_shots, 0)  AS head_shots,
        COALESCE(h.head_goals, 0)  AS head_goals,
        COALESCE(h.head_shots_sp, 0)   AS head_shots_sp,
        COALESCE(h.head_shots_open, 0) AS head_shots_open,
        COALESCE(m.n_matches, 0)   AS n_matches
    FROM per_season p
    LEFT JOIN head_shots h    ON h.str_player_id = p.str_player_id AND h.str_season = p.str_season
    LEFT JOIN player_season m ON m.str_player_id = p.str_player_id AND m.str_season = p.str_season
),

-- Moyennes de référence pour le rétrécissement
priors AS (
    SELECT
        SUM(head_shots) * 1.0 / NULLIF(SUM(n_off), 0)        AS p_head_shot,
        SUM(n_f_pass_ok) * 1.0 / NULLIF(SUM(n_f_pass), 0)    AS p_pass_ok,
        SUM(f_pass_progress) / NULLIF(SUM(n_f_pass_prog), 0) AS p_progress,
        SUM(xt_sum) / NULLIF(SUM(n_xt), 0)                   AS p_xt
    FROM season_full
),

-- Fenêtre : saisons s-2, s-1, s
win AS (
    SELECT
        a.str_player_id, a.str_season,
        COUNT(*)                 AS n_seasons,
        SUM(b.n)                 AS n,
        SUM(b.n_off)             AS n_off,
        SUM(b.n_def)             AS n_def,
        SUM(b.n_won)             AS n_won,
        SUM(b.won_off)           AS won_off,
        SUM(b.won_def)           AS won_def,
        SUM(b.woe_off)           AS woe_off,
        SUM(b.woe_def)           AS woe_def,
        SUM(b.woe_net_off)       AS woe_net_off,
        SUM(b.woe_net_def)       AS woe_net_def,
        SUM(b.n_hdiff)           AS n_hdiff,
        SUM(b.hdiff_sum)         AS hdiff_sum,
        SUM(b.n_setpiece)        AS n_setpiece,
        SUM(b.n_cross)           AS n_cross,
        SUM(b.n_long)            AS n_long,
        SUM(b.n_follow)          AS n_follow,
        SUM(b.n_f_pass)          AS n_f_pass,
        SUM(b.n_f_clear)         AS n_f_clear,
        SUM(b.n_f_shot)          AS n_f_shot,
        SUM(b.n_f_pass_ok)       AS n_f_pass_ok,
        SUM(b.n_f_pass_prog)     AS n_f_pass_prog,
        SUM(b.f_pass_progress)   AS f_pass_progress,
        SUM(b.n_xt)              AS n_xt,
        SUM(b.xt_sum)            AS xt_sum,
        SUM(b.n_off_wins)        AS n_off_wins,
        SUM(b.off_wins_shot)     AS off_wins_shot,
        SUM(b.head_shots)        AS head_shots,
        SUM(b.head_goals)        AS head_goals,
        SUM(b.head_shots_sp)     AS head_shots_sp,
        SUM(b.head_shots_open)   AS head_shots_open,
        SUM(b.n_matches)         AS n_matches
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
        ps.height                                                             AS int_height,
        CAST(w.n_seasons AS INTEGER)                                          AS int_n_seasons_window,
        CAST(w.n AS INTEGER)                                                  AS int_n_duels,
        CAST(w.n_off AS INTEGER)                                              AS int_n_duels_off,
        CAST(w.n_def AS INTEGER)                                              AS int_n_duels_def,
        CAST(w.n_won AS INTEGER)                                              AS int_n_won,
        CAST(w.n_matches AS INTEGER)                                          AS int_n_matches,
        w.n * 1.0 / NULLIF(w.n_matches, 0)                                    AS dec_duels_per_match,
        w.n_won * 1.0 / w.n                                                   AS dec_win_rate,
        w.won_off * 1.0 / NULLIF(w.n_off, 0)                                  AS dec_win_rate_off,
        w.won_def * 1.0 / NULLIF(w.n_def, 0)                                  AS dec_win_rate_def,
        (w.woe_off + w.woe_def) / (w.n + {{ k_duels }})                       AS dec_woe,
        w.woe_off / (w.n_off + {{ k_duels }})                                 AS dec_woe_off,
        w.woe_def / (w.n_def + {{ k_duels }})                                 AS dec_woe_def,
        (w.woe_net_off + w.woe_net_def) / (w.n + {{ k_duels }})               AS dec_woe_net,
        w.woe_net_off / (w.n_off + {{ k_duels }})                             AS dec_woe_net_off,
        w.woe_net_def / (w.n_def + {{ k_duels }})                             AS dec_woe_net_def,
        w.hdiff_sum * 1.0 / NULLIF(w.n_hdiff, 0)                              AS dec_height_diff_mean,
        w.n_off * 1.0 / w.n                                                   AS dec_share_off,
        w.n_setpiece * 1.0 / w.n                                              AS dec_share_setpiece,
        w.n_cross * 1.0 / w.n                                                 AS dec_share_cross,
        w.n_long * 1.0 / w.n                                                  AS dec_share_long,
        CAST(w.head_shots AS INTEGER)                                         AS int_n_head_shots,
        CAST(w.head_goals AS INTEGER)                                         AS int_n_head_goals,
        (w.head_shots + {{ k_duels }} * pr.p_head_shot) / (w.n_off + {{ k_duels }}) AS dec_head_shot_rate,
        w.head_shots * 1.0 / NULLIF(w.n_matches, 0)                           AS dec_head_shots_per_match,
        CAST(w.head_shots_sp AS INTEGER)                                      AS int_n_head_shots_setpiece,
        CAST(w.head_shots_open AS INTEGER)                                    AS int_n_head_shots_open,
        w.head_shots_sp * 1.0 / NULLIF(w.n_matches, 0)                        AS dec_head_shots_setpiece_per_match,
        w.head_shots_open * 1.0 / NULLIF(w.n_matches, 0)                      AS dec_head_shots_open_per_match,
        CAST(w.n_follow AS INTEGER)                                           AS int_n_follow,
        w.n_f_pass * 1.0 / NULLIF(w.n_follow, 0)                              AS dec_follow_pass_share,
        w.n_f_clear * 1.0 / NULLIF(w.n_follow, 0)                             AS dec_follow_clear_share,
        w.n_f_shot * 1.0 / NULLIF(w.n_follow, 0)                              AS dec_follow_shot_share,
        CAST(w.n_f_pass AS INTEGER)                                           AS int_n_follow_pass,
        (w.n_f_pass_ok + {{ k_passes }} * pr.p_pass_ok) / (w.n_f_pass + {{ k_passes }})              AS dec_follow_pass_accuracy,
        (w.f_pass_progress + {{ k_passes }} * pr.p_progress) / (w.n_f_pass_prog + {{ k_passes }})    AS dec_follow_pass_progress,
        (w.xt_sum + {{ k_passes }} * pr.p_xt) / (w.n_xt + {{ k_passes }})                            AS dec_follow_xt_per_pass,
        w.off_wins_shot * 1.0 / NULLIF(w.n_off_wins, 0)                       AS dec_shot_after_win_off
    FROM win w
    CROSS JOIN priors pr
    LEFT JOIN player_season ps ON ps.str_player_id = w.str_player_id AND ps.str_season = w.str_season
)

SELECT * FROM mdl_out
