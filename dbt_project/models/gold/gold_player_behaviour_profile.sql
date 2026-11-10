{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_player_behaviour_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_player_behaviour_profile
-- Profil de COMPORTEMENT unifié d'un joueur sur une FENÊTRE DE 3 SAISONS GLISSANTES,
-- toutes familles d'actions réunies, normalisé pour comparer des joueurs entre eux.
-- Grain : (str_player_id, str_season) où str_season est la DERNIÈRE saison de la fenêtre
--         (s-2, s-1, s). Une ligne n'existe que si le joueur a joué au moins un match en s.
--
-- ⚠️ FUITE : la fenêtre INCLUT la saison str_season. Pour prédire un match de la saison s,
--    un modèle doit lire la ligne str_season = s-1 (convention « lag » des autres profils).
--
-- Thèmes : occupation du terrain (9 secteurs : 3 profondeurs × 3 couloirs), finition,
-- création, distribution (passes de jeu ouvert), défense, aérien, duels au sol, fautes.
--
-- NORMALISATION (les définitions d'événements dérivent d'une saison à l'autre : interceptions
-- 1,4 → 0,5 par joueur et par match entre 2009 et 2025). Chaque métrique existe en deux colonnes :
--   · dec_<m>    valeur brute RÉTRÉCIE vers la moyenne des pairs (voir plus bas), à lire dans l'unité
--                de la métrique (par 90 minutes, taux, part) ;
--   · dec_z_<m>  écart réduit aux pairs : (valeur − moyenne des pairs) / écart-type des pairs.
-- Pairs = joueurs de la MÊME saison, de la MÊME ligue et de la MÊME famille de poste
-- (GK, CB, FB, MID = DM/CM/AM, WIDE = WM/W, ST) ayant au moins 450 minutes sur la fenêtre.
-- Les z-scores sont donc comparables entre saisons ; les valeurs brutes ne le sont pas.
--
-- RÉTRÉCISSEMENT : valeur = (numérateur + K × moyenne pairs) / (dénominateur + K). Pour un
-- volume par 90 minutes, le dénominateur est en « 90 minutes » et K = 10 (900 minutes) ;
-- pour un taux, K est un nombre d'essais (voir la liste des métriques). Un joueur à petit volume
-- reste proche de la moyenne de ses pairs.
--
-- Les métriques issues des profils aérien et sol (dec_g_*, dec_a_*) existent déjà rétrécies : on
-- ne fait que les centrer et réduire. Elles manquent avant 2014-2015.
--
-- Poste : modal sur la fenêtre (str_role_fin_resolved, pondéré par les minutes).
-- Ligue : modale sur la fenêtre (pondérée par les minutes).
-- Seuils (450 minutes, K) posés par hypothèse, à valider par la stabilité inter-saisons.
-- ══════════════════════════════════════════════════════════════════════════════

{% set zone_depths = {'def': [1, 2], 'mid': [3], 'att': [4, 5]} %}
{% set zone_corr = {'left': [1, 2], 'centre': [3], 'right': [4, 5]} %}

{# (nom, colonne numérateur, colonne dénominateur ou None pour un volume par 90 min, K) #}
{% set rates = [
    ('shots', 'int_n_shots'), ('xg_contribution', 'dec_xg_contribution'),
    ('key_passes', 'int_n_key_passes'), ('chances_created', 'int_n_chances_created'),
    ('progressive_passes', 'int_n_progressive_passes'), ('crosses', 'int_n_crosses'),
    ('throughballs', 'int_n_throughballs'), ('longballs', 'int_n_longballs'),
    ('passes_open', 'int_passes_open'), ('passes_into_final_third', 'int_passes_into_final_third'),
    ('xt_created', 'dec_xt_created'), ('tackles', 'int_n_tackles'),
    ('interceptions', 'int_n_interceptions'), ('recoveries', 'int_n_ball_recoveries'),
    ('clearances', 'int_n_clearances'), ('challenges', 'int_n_challenges'),
    ('aerial_duels', 'int_n_aerial_duels'), ('touches', 'int_n_touches'),
    ('yellow_cards', 'int_n_yellow_cards'),
    ('shots_blocked', 'int_shots_blocked'), ('passes_blocked', 'int_passes_blocked'),
    ('crosses_blocked', 'int_crosses_blocked'), ('last_man_actions', 'int_last_man_actions'),
    ('def_actions', 'int_def_actions'), ('def_actions_own_box', 'int_def_actions_own_box')
] %}
{% set ratios = [
    ('pass_accuracy', 'int_passes_open_completed', 'int_passes_open', 100),
    ('pass_forward_share', 'int_passes_forward', 'int_passes_open', 100),
    ('pass_forward_accuracy', 'int_passes_forward_completed', 'int_passes_forward', 50),
    ('pass_backward_share', 'int_passes_backward', 'int_passes_open', 100),
    ('pass_long_share', 'int_passes_long', 'int_passes_open', 100),
    ('pass_long_accuracy', 'int_passes_long_completed', 'int_passes_long', 30),
    ('pass_short_share', 'int_passes_short', 'int_passes_open', 100),
    ('pass_from_own_third_share', 'int_passes_from_own_third', 'int_passes_open', 100),
    ('pass_from_attacking_third_share', 'int_passes_from_attacking_third', 'int_passes_open', 100),
    ('pass_length_mean', 'dec_pass_length_sum', 'int_passes_open', 100),
    ('xt_per_pass', 'dec_xt_created', 'int_passes_open', 100),
    ('tackle_win_rate', 'int_n_tackles_won', 'int_n_tackles', 30),
    ('aerial_win_rate', 'int_n_aerial_won', 'int_n_aerial_duels', 30),
    ('def_own_box_share', 'int_def_actions_own_box', 'int_def_actions', 30)
] %}
{# métriques déjà rétrécies, issues des profils sol (g) et aérien (a) : (nom, source, colonne) #}
{% set joined = [
    ('g_dribbles_per_90', 'g', 'dec_dribbles_per_90'), ('g_dribble_woe_adj', 'g', 'dec_dribble_woe_adj'),
    ('g_dribble_win_rate', 'g', 'dec_dribble_win_rate'), ('g_defended_woe_adj', 'g', 'dec_defended_woe_adj'),
    ('g_dispossessed_share', 'g', 'dec_dispossessed_share'),
    ('g_fouls_committed_per_90', 'g', 'dec_fouls_committed_per_90'),
    ('g_fouls_drawn_per_90', 'g', 'dec_fouls_drawn_per_90'),
    ('g_yellow_per_foul', 'g', 'dec_yellow_per_foul'),
    ('a_woe_net_off', 'a', 'dec_woe_net_off'), ('a_woe_net_def', 'a', 'dec_woe_net_def'),
    ('a_duels_per_match', 'a', 'dec_duels_per_match'), ('a_head_shots_per_match', 'a', 'dec_head_shots_per_match')
] %}

WITH

-- ── Un joueur et un match : volumes bruts, zones pondérées par les touches ──────────
pm AS (
    SELECT
        e.str_player_id,
        CAST(SUBSTR(e.str_season, 1, 4) AS INTEGER)                       AS y,
        e.str_league_source,
        CASE r.str_role_fin_resolved
            WHEN 'GK' THEN 'GK' WHEN 'CB' THEN 'CB' WHEN 'FB' THEN 'FB'
            WHEN 'DM' THEN 'MID' WHEN 'CM' THEN 'MID' WHEN 'AM' THEN 'MID'
            WHEN 'WM' THEN 'WIDE' WHEN 'W' THEN 'WIDE' WHEN 'ST' THEN 'ST' END AS macro,
        CAST(e.int_minutes_played AS DOUBLE)                              AS mins,
        COALESCE(e.int_n_shots, 0) AS int_n_shots,
        COALESCE(e.dec_xg_contribution, 0) AS dec_xg_contribution,
        COALESCE(e.int_n_key_passes, 0) AS int_n_key_passes,
        COALESCE(e.int_n_chances_created, 0) AS int_n_chances_created,
        COALESCE(e.int_n_progressive_passes, 0) AS int_n_progressive_passes,
        COALESCE(e.int_n_crosses, 0) AS int_n_crosses,
        COALESCE(e.int_n_throughballs, 0) AS int_n_throughballs,
        COALESCE(e.int_n_longballs, 0) AS int_n_longballs,
        COALESCE(e.int_n_tackles, 0) AS int_n_tackles,
        COALESCE(e.int_n_tackles_won, 0) AS int_n_tackles_won,
        COALESCE(e.int_n_interceptions, 0) AS int_n_interceptions,
        COALESCE(e.int_n_ball_recoveries, 0) AS int_n_ball_recoveries,
        COALESCE(e.int_n_clearances, 0) AS int_n_clearances,
        COALESCE(e.int_n_challenges, 0) AS int_n_challenges,
        COALESCE(e.int_n_aerial_duels, 0) AS int_n_aerial_duels,
        COALESCE(e.int_n_aerial_won, 0) AS int_n_aerial_won,
        COALESCE(e.int_n_touches, 0) AS int_n_touches,
        COALESCE(e.int_n_yellow_cards, 0) AS int_n_yellow_cards,
        COALESCE(p.int_passes_open, 0) AS int_passes_open,
        COALESCE(p.int_passes_open_completed, 0) AS int_passes_open_completed,
        COALESCE(p.int_passes_short, 0) AS int_passes_short,
        COALESCE(p.int_passes_long, 0) AS int_passes_long,
        COALESCE(p.int_passes_long_completed, 0) AS int_passes_long_completed,
        COALESCE(p.int_passes_forward, 0) AS int_passes_forward,
        COALESCE(p.int_passes_forward_completed, 0) AS int_passes_forward_completed,
        COALESCE(p.int_passes_backward, 0) AS int_passes_backward,
        COALESCE(p.int_passes_from_own_third, 0) AS int_passes_from_own_third,
        COALESCE(p.int_passes_from_attacking_third, 0) AS int_passes_from_attacking_third,
        COALESCE(p.int_passes_into_final_third, 0) AS int_passes_into_final_third,
        COALESCE(p.dec_pass_length_mean * p.int_passes_open, 0) AS dec_pass_length_sum,
        COALESCE(p.dec_xt_created, 0) AS dec_xt_created,
        COALESCE(cm.int_shots_blocked, 0) AS int_shots_blocked,
        COALESCE(cm.int_passes_blocked, 0) AS int_passes_blocked,
        COALESCE(cm.int_crosses_blocked, 0) AS int_crosses_blocked,
        COALESCE(cm.int_last_man_actions, 0) AS int_last_man_actions,
        COALESCE(cm.int_def_actions, 0) AS int_def_actions,
        COALESCE(cm.int_def_actions_own_box, 0) AS int_def_actions_own_box,
        {% for dn, dl in zone_depths.items() %}{% for cn, cl in zone_corr.items() %}
        e.int_n_touches * ({% for z in dl %}{% for c in cl %}COALESCE(e.dec_pct_z{{ z }}_c{{ c }}, 0) + {% endfor %}{% endfor %} 0) AS dec_sec_{{ dn }}_{{ cn }},
        {% endfor %}{% endfor %}
        e.int_n_touches * ({% for z in range(1, 6) %}{% for c in range(1, 6) %}COALESCE(e.dec_pct_z{{ z }}_c{{ c }}, 0) + {% endfor %}{% endfor %} 0) AS dec_sec_total
    FROM {{ ref('intermediate_player_match_event_stats') }} e
    LEFT JOIN {{ ref('intermediate_player_match_passes') }} p
      ON p.str_match_id = e.str_match_id AND p.str_team_id = e.str_team_id AND p.str_player_id = e.str_player_id
    LEFT JOIN {{ ref('intermediate_player_match_commitment') }} cm
      ON cm.str_match_id = e.str_match_id AND cm.str_team_id = e.str_team_id AND cm.str_player_id = e.str_player_id
    LEFT JOIN {{ ref('intermediate_player_match_role') }} r
      ON r.str_match_id = e.str_match_id AND r.str_team_id = e.str_team_id AND r.str_player_id = e.str_player_id
    WHERE e.int_minutes_played > 0
),

-- ── Fenêtre glissante : chaque match compte pour les saisons y, y+1 et y+2 ──────────
win AS (
    SELECT pm.*, pm.y + o.k AS ty, o.k AS lagk
    FROM pm
    CROSS JOIN (VALUES (0), (1), (2)) o(k)
    WHERE pm.y + o.k <= (SELECT MAX(y) FROM pm)
),

agg AS (
    SELECT
        str_player_id, ty,
        COUNT(*) FILTER (WHERE lagk = 0)                   AS int_n_matches_season,
        COUNT(*)                                           AS int_n_matches,
        COUNT(DISTINCT y)                                  AS int_n_seasons_window,
        SUM(mins)                                          AS mins,
        {% for n, c in rates %}SUM({{ c }}) AS s_{{ n }},
        {% endfor %}
        {% for n, a, b, k in ratios %}SUM({{ a }}) AS sn_{{ n }}, SUM({{ b }}) AS sd_{{ n }},
        {% endfor %}
        {% for dn in zone_depths %}{% for cn in zone_corr %}SUM(dec_sec_{{ dn }}_{{ cn }}) AS s_sec_{{ dn }}_{{ cn }},
        {% endfor %}{% endfor %}
        SUM(dec_sec_total)                                 AS s_sec_total
    FROM win
    GROUP BY str_player_id, ty
    HAVING COUNT(*) FILTER (WHERE lagk = 0) >= 1
),

role_mode AS (
    SELECT str_player_id, ty, ARG_MAX(macro, mins_r) AS macro
    FROM (SELECT str_player_id, ty, macro, SUM(mins) AS mins_r FROM win WHERE macro IS NOT NULL GROUP BY 1, 2, 3)
    GROUP BY 1, 2
),

league_mode AS (
    SELECT str_player_id, ty, ARG_MAX(str_league_source, mins_l) AS str_league_source
    FROM (SELECT str_player_id, ty, str_league_source, SUM(mins) AS mins_l FROM win GROUP BY 1, 2, 3)
    GROUP BY 1, 2
),

ctx AS (
    SELECT a.str_player_id, a.ty, a.int_n_matches_season, a.int_n_matches, a.int_n_seasons_window, a.mins,
           r.macro, l.str_league_source, (a.mins >= 450) AS is_peer
    FROM agg a
    JOIN role_mode r ON r.str_player_id = a.str_player_id AND r.ty = a.ty
    JOIN league_mode l ON l.str_player_id = a.str_player_id AND l.ty = a.ty
),

-- ── Format long : (joueur, saison, métrique, numérateur, dénominateur, K) ───────────
long AS (
    {% for n, c in rates %}
    SELECT str_player_id, ty, '{{ n }}' AS metric, s_{{ n }} AS num, mins / 90.0 AS den, 10.0 AS k FROM agg
    UNION ALL
    {% endfor %}
    {% for n, a, b, k in ratios %}
    SELECT str_player_id, ty, '{{ n }}', sn_{{ n }}, sd_{{ n }}, {{ k }}.0 FROM agg
    UNION ALL
    {% endfor %}
    {% for dn in zone_depths %}{% for cn in zone_corr %}
    SELECT str_player_id, ty, 'sec_{{ dn }}_{{ cn }}', s_sec_{{ dn }}_{{ cn }}, s_sec_total, 100.0 FROM agg
    UNION ALL
    {% endfor %}{% endfor %}
    {% for n, src, col in joined %}
    SELECT ag.str_player_id, ag.ty, '{{ n }}', CAST({{ src }}.{{ col }} AS DOUBLE), 1.0, 0.0
    FROM agg ag
    LEFT JOIN {{ ref('gold_player_ground_profile') if src == 'g' else ref('gold_player_aerial_profile') }} {{ src }}
      ON {{ src }}.str_player_id = ag.str_player_id AND {{ src }}.str_season = CAST(ag.ty AS VARCHAR) || '-' || CAST(ag.ty + 1 AS VARCHAR)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

-- moyenne des pairs : pondérée par le dénominateur (vraie moyenne de la population)
peer AS (
    SELECT c.ty, c.str_league_source, c.macro, l.metric,
           SUM(l.num) / NULLIF(SUM(l.den), 0) AS p
    FROM long l
    JOIN ctx c ON c.str_player_id = l.str_player_id AND c.ty = l.ty
    WHERE c.is_peer AND l.num IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

shrunk AS (
    SELECT l.str_player_id, l.ty, l.metric,
           (l.num + l.k * pr.p) / NULLIF(l.den + l.k, 0) AS v
    FROM long l
    JOIN ctx c ON c.str_player_id = l.str_player_id AND c.ty = l.ty
    JOIN peer pr ON pr.ty = c.ty AND pr.str_league_source = c.str_league_source
                AND pr.macro = c.macro AND pr.metric = l.metric
    WHERE l.num IS NOT NULL
),

spread AS (
    SELECT c.ty, c.str_league_source, c.macro, s.metric,
           AVG(s.v) AS mu, STDDEV_POP(s.v) AS sd
    FROM shrunk s
    JOIN ctx c ON c.str_player_id = s.str_player_id AND c.ty = s.ty
    WHERE c.is_peer
    GROUP BY 1, 2, 3, 4
),

scored AS (
    SELECT s.str_player_id, s.ty, s.metric, s.v,
           (s.v - sp.mu) / NULLIF(sp.sd, 0) AS z
    FROM shrunk s
    JOIN ctx c ON c.str_player_id = s.str_player_id AND c.ty = s.ty
    JOIN spread sp ON sp.ty = c.ty AND sp.str_league_source = c.str_league_source
                  AND sp.macro = c.macro AND sp.metric = s.metric
),

wide AS (
    SELECT str_player_id, ty,
        {% for n, c in rates %}MAX(v) FILTER (WHERE metric = '{{ n }}') AS dec_{{ n }}, MAX(z) FILTER (WHERE metric = '{{ n }}') AS dec_z_{{ n }},
        {% endfor %}
        {% for n, a, b, k in ratios %}MAX(v) FILTER (WHERE metric = '{{ n }}') AS dec_{{ n }}, MAX(z) FILTER (WHERE metric = '{{ n }}') AS dec_z_{{ n }},
        {% endfor %}
        {% for dn in zone_depths %}{% for cn in zone_corr %}MAX(v) FILTER (WHERE metric = 'sec_{{ dn }}_{{ cn }}') AS dec_sec_{{ dn }}_{{ cn }}, MAX(z) FILTER (WHERE metric = 'sec_{{ dn }}_{{ cn }}') AS dec_z_sec_{{ dn }}_{{ cn }},
        {% endfor %}{% endfor %}
        {% for n, src, col in joined %}MAX(v) FILTER (WHERE metric = '{{ n }}') AS dec_{{ n }}, MAX(z) FILTER (WHERE metric = '{{ n }}') AS dec_z_{{ n }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM scored
    GROUP BY str_player_id, ty
)

SELECT
    c.str_player_id,
    CAST(c.ty AS VARCHAR) || '-' || CAST(c.ty + 1 AS VARCHAR)  AS str_season,
    COALESCE(g.str_player_name, a.str_player_name)                AS str_player_name,
    c.macro                                                       AS str_macro_role,
    c.str_league_source,
    c.int_n_seasons_window,
    c.int_n_matches,
    CAST(c.mins AS INTEGER)                                       AS int_minutes,
    c.is_peer                                                     AS bool_is_peer,
    w.* EXCLUDE (str_player_id, ty)
FROM ctx c
JOIN wide w ON w.str_player_id = c.str_player_id AND w.ty = c.ty
LEFT JOIN {{ ref('gold_player_ground_profile') }} g
  ON g.str_player_id = c.str_player_id AND g.str_season = CAST(c.ty AS VARCHAR) || '-' || CAST(c.ty + 1 AS VARCHAR)
LEFT JOIN {{ ref('gold_player_aerial_profile') }} a
  ON a.str_player_id = c.str_player_id AND a.str_season = CAST(c.ty AS VARCHAR) || '-' || CAST(c.ty + 1 AS VARCHAR)
