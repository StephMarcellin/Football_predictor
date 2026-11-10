{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_referee_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_referee_profile
-- Profil d'un arbitre sur une FENÊTRE DE 3 SAISONS GLISSANTES.
-- Grain : (str_referee_id, str_season) où str_season est la DERNIÈRE saison de la fenêtre
--         (saisons s-2, s-1, s). Une ligne n'existe que si l'arbitre a dirigé au moins un
--         match en s.
--
-- ⚠️ FUITE : la fenêtre INCLUT la saison str_season. Pour prédire un match de la saison s,
--    un modèle doit lire la ligne str_season = s-1 (convention « lag » des autres tables).
--
-- L'arbitre d'un match est CONNU AVANT LE COUP D'ENVOI (désignation publiée à l'avance) :
-- le profil peut donc servir de feature d'avant-match (à vérifier : la source actuelle,
-- silver.stg_whoscored_match_meta, est scrapée après le match ; la désignation à l'avance
-- devra être collectée pour la production).
--
-- Source : stg_whoscored_match_meta (referee_id, score final), intermediate_whoscored_match_bridge
-- (pont match WhoScored ↔ match du projet, équipe à domicile), cartons des événements,
-- fautes et penaltys de intermediate_ground_duel_players. Toutes ligues présentes.
--
-- Mesures par match, puis moyennes sur la fenêtre :
--   · dec_*_per_match : moyennes brutes (jaunes, rouges, fautes, penaltys par match).
--   · dec_*_net : écart à la moyenne de la ligue et de la saison du match (une ligue siffle
--     plus de fautes qu'une autre), rétréci vers 0 avec K = 10 matchs.
--   · dec_home_goal_diff_net : avantage du domicile (buts domicile − extérieur) net de la
--     ligue-saison, rétréci. dec_home_yellow_diff_net : jaunes domicile − extérieur, net.
-- Les penaltys viennent du pairage des fautes (bool_leads_to_penalty).
-- ══════════════════════════════════════════════════════════════════════════════

{% set k_matches = 10 %}

WITH

matches AS (
    SELECT
        b.str_match_id,
        b.str_ws_match_id,
        b.str_league_source,
        b.str_season,
        CAST(SUBSTR(b.str_season, 1, 4) AS INTEGER)                        AS y,
        CAST(b.str_ws_home_team_id AS BIGINT)                              AS home_ws_team_id,
        m.referee_id                                                       AS referee_id,
        m.referee_name                                                     AS referee_name,
        CAST(SPLIT_PART(m.ft_score, ' : ', 1) AS INTEGER)                  AS goals_home,
        CAST(SPLIT_PART(m.ft_score, ' : ', 2) AS INTEGER)                  AS goals_away
    FROM {{ ref('intermediate_whoscored_match_bridge') }} b
    JOIN {{ source('silver', 'stg_whoscored_match_meta') }} m
      ON m.ws_match_id = b.str_ws_match_id
    WHERE m.referee_id IS NOT NULL
      AND b.str_season >= '2014-2015'
      AND m.ft_score LIKE '% : %'
),

cards AS (
    SELECT
        e.str_match_id,
        CAST(e.str_team_id AS BIGINT)                                      AS team_id,
        COUNT(*) FILTER (WHERE e.str_card_type = 'Yellow')                 AS yellow,
        COUNT(*) FILTER (WHERE e.str_card_type IN ('Red', 'SecondYellow')) AS red
    FROM {{ ref('intermediate_whoscored_events_enriched') }} e
    WHERE e.str_card_type IS NOT NULL
      AND e.str_season >= '2014-2015'
    GROUP BY e.str_match_id, CAST(e.str_team_id AS BIGINT)
),

fouls AS (
    SELECT
        str_match_id,
        COUNT(*)                                                           AS fouls,
        COUNT(*) FILTER (WHERE bool_leads_to_penalty)                      AS penalties
    FROM {{ ref('intermediate_ground_duel_players') }}
    WHERE str_duel_family = 'faute' AND str_role = 'auteur' AND str_season >= '2014-2015'
    GROUP BY str_match_id
),

match_stats AS (
    SELECT
        m.*,
        COALESCE(SUM(c.yellow) FILTER (WHERE c.team_id = m.home_ws_team_id), 0) AS yellow_home,
        COALESCE(SUM(c.yellow) FILTER (WHERE c.team_id <> m.home_ws_team_id), 0) AS yellow_away,
        COALESCE(SUM(c.red), 0)                                            AS red,
        COALESCE(MAX(f.fouls), 0)                                          AS fouls,
        COALESCE(MAX(f.penalties), 0)                                      AS penalties
    FROM matches m
    LEFT JOIN cards c ON c.str_match_id = m.str_match_id
    LEFT JOIN fouls f ON f.str_match_id = m.str_match_id
    GROUP BY ALL
),

-- Moyenne de la ligue et de la saison du match
league_avg AS (
    SELECT
        str_league_source, str_season,
        AVG(yellow_home + yellow_away)                  AS yellow,
        AVG(red)                                        AS red,
        AVG(fouls)                                      AS fouls,
        AVG(penalties)                                  AS penalties,
        AVG(goals_home - goals_away)                    AS goal_diff,
        AVG(yellow_home - yellow_away)                  AS yellow_diff
    FROM match_stats
    GROUP BY str_league_source, str_season
),

per_season AS (
    SELECT
        s.referee_id, s.str_season, s.y,
        COUNT(*)                                                       AS n,
        SUM(s.yellow_home + s.yellow_away)                             AS yellow,
        SUM(s.red)                                                     AS red,
        SUM(s.fouls)                                                   AS fouls,
        SUM(s.penalties)                                               AS penalties,
        SUM(s.yellow_home + s.yellow_away - l.yellow)                  AS yellow_net,
        SUM(s.red - l.red)                                             AS red_net,
        SUM(s.fouls - l.fouls)                                         AS fouls_net,
        SUM(s.penalties - l.penalties)                                 AS penalties_net,
        SUM((s.goals_home - s.goals_away) - l.goal_diff)               AS goal_diff_net,
        SUM((s.yellow_home - s.yellow_away) - l.yellow_diff)           AS yellow_diff_net,
        arg_max(s.referee_name, s.y)                                   AS referee_name,
        mode(s.str_league_source)                                      AS main_league
    FROM match_stats s
    JOIN league_avg l
      ON l.str_league_source = s.str_league_source AND l.str_season = s.str_season
    GROUP BY s.referee_id, s.str_season, s.y
),

-- Fenêtre : saisons s-2, s-1, s
win AS (
    SELECT
        a.referee_id, a.str_season,
        a.referee_name, a.main_league,
        COUNT(*)                   AS n_seasons,
        SUM(b.n)                   AS n,
        SUM(b.yellow)              AS yellow,
        SUM(b.red)                 AS red,
        SUM(b.fouls)               AS fouls,
        SUM(b.penalties)           AS penalties,
        SUM(b.yellow_net)          AS yellow_net,
        SUM(b.red_net)             AS red_net,
        SUM(b.fouls_net)           AS fouls_net,
        SUM(b.penalties_net)       AS penalties_net,
        SUM(b.goal_diff_net)       AS goal_diff_net,
        SUM(b.yellow_diff_net)     AS yellow_diff_net
    FROM per_season a
    JOIN per_season b
      ON b.referee_id = a.referee_id AND b.y BETWEEN a.y - 2 AND a.y
    GROUP BY a.referee_id, a.str_season, a.referee_name, a.main_league
),

mdl_out AS (
    SELECT
        referee_id                                                         AS str_referee_id,
        str_season,
        referee_name                                                       AS str_referee_name,
        main_league                                                        AS str_main_league,
        CAST(n_seasons AS INTEGER)                                         AS int_n_seasons_window,
        CAST(n AS INTEGER)                                                 AS int_n_matches,
        yellow * 1.0 / n                                                   AS dec_yellow_per_match,
        red * 1.0 / n                                                      AS dec_red_per_match,
        fouls * 1.0 / n                                                    AS dec_fouls_per_match,
        penalties * 1.0 / n                                                AS dec_penalties_per_match,
        yellow_net / (n + {{ k_matches }})                                 AS dec_yellow_net,
        red_net / (n + {{ k_matches }})                                    AS dec_red_net,
        fouls_net / (n + {{ k_matches }})                                  AS dec_fouls_net,
        penalties_net / (n + {{ k_matches }})                              AS dec_penalties_net,
        goal_diff_net / (n + {{ k_matches }})                              AS dec_home_goal_diff_net,
        yellow_diff_net / (n + {{ k_matches }})                            AS dec_home_yellow_diff_net
    FROM win
)

SELECT * FROM mdl_out
