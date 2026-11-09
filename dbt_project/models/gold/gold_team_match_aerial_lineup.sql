{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_team_match_aerial_lineup'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_team_match_aerial_lineup — grain (str_match_id, str_team_id)
-- Profil aérien de la COMPOSITION DE DÉPART (11 titulaires, minute 0), seul et face à l'adversaire.
-- Dépend de la compo (connue environ une heure avant le coup d'envoi), comme
-- gold_team_match_lineup_strength.
--
-- Sans fuite : les compétences viennent de la dernière ligne de gold_player_aerial_profile dont la
-- saison de fin est s-1 ou s-2 (fenêtre de 3 saisons qui n'inclut jamais la saison du match s).
-- Un joueur sans duels aériens en s-1 ni s-2 n'a pas de profil (nouveau joueur, longue blessure) :
-- il est compté dans int_n_starters mais pas dans int_n_profiled, et ne pèse ni dans les moyennes
-- de compétence ni dans les sommes de volume. Lire int_n_profiled avant d'interpréter une somme.
--
-- Taille : valeur courante de la fiche WhoScored (MAX sur les saisons du joueur, <= 150 cm manquant).
-- Poste : poste macro habituel du joueur (poste le plus joué toutes saisons, intermediate_whoscored_
-- player_team_season), pas son poste du jour. Gardiens exclus des agrégats « joueurs de champ ».
--
-- Colonnes :
--   · dec_xi_height_mean / _top3 / _def / _att : taille moyenne des joueurs de champ, des 3 plus
--     grands, des défenseurs, des attaquants ;
--   · dec_xi_woe_net_def_def, dec_xi_woe_net_off_att, dec_xi_woe_net_mean : compétence aérienne
--     nette de la taille (voir gold_player_aerial_profile) des défenseurs en défense, des attaquants
--     en attaque, de tous les joueurs de champ ;
--   · dec_xi_duels_per_match_sum : présence aérienne attendue (somme des duels par match) ;
--   · dec_xi_head_shots_setpiece_sum / _open_sum : menace de la tête sur coup de pied arrêté / jeu ouvert ;
--   · dec_edge_* : écart avec l'adversaire. edge_att = attaquants contre défenseurs adverses,
--     edge_def = défenseurs contre attaquants adverses, en taille (cm) et en compétence nette.
--     Positif = avantage de l'équipe. Vide si l'un des deux groupes est vide ou sans profil.
-- ══════════════════════════════════════════════════════════════════════════════

WITH

xi AS (
    SELECT DISTINCT str_match_id, str_team_id, str_player_id
    FROM {{ ref('intermediate_whoscored_lineup_period') }}
    WHERE int_start_minute = 0 AND str_match_id IS NOT NULL
),

match_ctx AS (
    SELECT str_match_id, str_team_id, str_opponent_id, str_season,
           CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS y
    FROM {{ ref('gold_team_match') }}
),

player_attr AS (
    SELECT str_player_id,
           MAX(CASE WHEN int_height > 150 THEN int_height END)      AS height,
           arg_max(str_macro_position, int_n_matchs)                AS pos
    FROM {{ ref('intermediate_whoscored_player_team_season') }}
    GROUP BY str_player_id
),

xi_ctx AS (
    SELECT x.str_match_id, x.str_team_id, x.str_player_id, m.str_opponent_id, m.y
    FROM xi x
    JOIN match_ctx m ON m.str_match_id = x.str_match_id AND m.str_team_id = x.str_team_id
),

-- dernière saison de profil disponible parmi s-2 et s-1
last_profile AS (
    SELECT x.str_match_id, x.str_team_id, x.str_player_id, MAX(p.str_season) AS profile_season
    FROM xi_ctx x
    JOIN {{ ref('gold_player_aerial_profile') }} p
      ON p.str_player_id = x.str_player_id
     AND CAST(SUBSTR(p.str_season, 1, 4) AS INTEGER) BETWEEN x.y - 2 AND x.y - 1
    GROUP BY x.str_match_id, x.str_team_id, x.str_player_id
),

xi_prof AS (
    SELECT x.str_match_id, x.str_team_id, x.str_opponent_id,
           a.height, a.pos,
           (lp.profile_season IS NOT NULL)          AS profiled,
           p.dec_woe_net, p.dec_woe_net_off, p.dec_woe_net_def, p.dec_duels_per_match,
           p.dec_head_shots_setpiece_per_match, p.dec_head_shots_open_per_match
    FROM xi_ctx x
    LEFT JOIN player_attr a ON a.str_player_id = x.str_player_id
    LEFT JOIN last_profile lp
           ON lp.str_match_id = x.str_match_id AND lp.str_team_id = x.str_team_id AND lp.str_player_id = x.str_player_id
    LEFT JOIN {{ ref('gold_player_aerial_profile') }} p
           ON p.str_player_id = x.str_player_id AND p.str_season = lp.profile_season
),

own AS (
    SELECT
        str_match_id, str_team_id, ANY_VALUE(str_opponent_id)                                                AS str_opponent_id,
        COUNT(*)                                                                                             AS n_starters,
        COUNT(*) FILTER (WHERE profiled)                                                                     AS n_profiled,
        AVG(height) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                                                AS height_mean,
        list_aggregate(list_slice(list(height ORDER BY height DESC)
                       FILTER (WHERE COALESCE(pos, '?') <> 'GK' AND height IS NOT NULL), 1, 3), 'avg')       AS height_top3,
        AVG(height) FILTER (WHERE pos = 'DEF')                                                               AS height_def,
        AVG(height) FILTER (WHERE pos = 'ATT')                                                               AS height_att,
        AVG(dec_woe_net_def) FILTER (WHERE pos = 'DEF')                                                      AS woe_net_def_def,
        AVG(dec_woe_net_off) FILTER (WHERE pos = 'ATT')                                                      AS woe_net_off_att,
        AVG(dec_woe_net) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                                           AS woe_net_mean,
        SUM(dec_duels_per_match) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                                   AS duels_per_match_sum,
        SUM(dec_head_shots_setpiece_per_match) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                     AS head_sp_sum,
        SUM(dec_head_shots_open_per_match) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                         AS head_open_sum
    FROM xi_prof
    GROUP BY str_match_id, str_team_id
),

mdl_out AS (
    SELECT
        o.str_match_id,
        o.str_team_id,
        CAST(o.n_starters AS INTEGER)                    AS int_n_starters,
        CAST(o.n_profiled AS INTEGER)                    AS int_n_profiled,
        o.height_mean                                    AS dec_xi_height_mean,
        o.height_top3                                    AS dec_xi_height_top3,
        o.height_def                                     AS dec_xi_height_def,
        o.height_att                                     AS dec_xi_height_att,
        o.woe_net_def_def                                AS dec_xi_woe_net_def_def,
        o.woe_net_off_att                                AS dec_xi_woe_net_off_att,
        o.woe_net_mean                                   AS dec_xi_woe_net_mean,
        o.duels_per_match_sum                            AS dec_xi_duels_per_match_sum,
        o.head_sp_sum                                    AS dec_xi_head_shots_setpiece_sum,
        o.head_open_sum                                  AS dec_xi_head_shots_open_sum,
        o.height_mean   - q.height_mean                  AS dec_edge_height_mean,
        o.height_att    - q.height_def                   AS dec_edge_att_height,
        o.woe_net_off_att - q.woe_net_def_def            AS dec_edge_att_skill,
        o.height_def    - q.height_att                   AS dec_edge_def_height,
        o.woe_net_def_def - q.woe_net_off_att            AS dec_edge_def_skill
    FROM own o
    LEFT JOIN own q ON q.str_match_id = o.str_match_id AND q.str_team_id = o.str_opponent_id
)

SELECT * FROM mdl_out
