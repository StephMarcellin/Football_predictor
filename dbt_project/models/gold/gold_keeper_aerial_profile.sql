{{
    config(
        materialized='table',
        schema='gold',
        alias='gold_keeper_aerial_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gold_keeper_aerial_profile — grain (str_player_id, str_season)
-- Jeu aérien du gardien sur une FENÊTRE DE 3 SAISONS GLISSANTES : str_season est la DERNIÈRE saison de
-- la fenêtre (s-2, s-1, s). ⚠️ FUITE : la fenêtre INCLUT s ; pour prédire la saison s, lire la ligne s-1.
-- Complète gold_keeper_season_lag, qui ne couvre que les arrêts (psxg, taux d'arrêts).
--
-- Source : événements WhoScored Claim (ballon capté), Punch (ballon boxé), KeeperSweeper (sortie hors de
-- la surface). CrossNotClaimed (centre non capté) est trop rare (environ 90 par saison) : ignoré.
-- Gardiens = joueurs dont le poste macro est GK dans intermediate_whoscored_player_team_season ; matchs
-- joués = int_n_matchs.
--
-- Mesures du 09/11/2026 (stabilité d'une même mesure à deux dates) :
--   · une saison à la suivante (gardiens-saisons avec au moins 10 matchs, brut) : volume 0,51, part de captés
--     0,42 (au moins 20 actions), sorties 0,49 ;
--   · sur la table construite, profils s et s+3 (fenêtres SANS recouvrement, au moins 30 matchs, 1 211 paires) :
--     volume 0,35, part de captés 0,37, sorties 0,30. Corrélation à la taille de la part de captés : 0,05.
--   Les gardiens changent de club : le volume de centres subis dépend aussi de l'équipe. Signal MODESTE.
--   · dec_aerial_actions_per_match : captés + boxés par match (0,78 en moyenne) ;
--   · dec_claim_share : part de captés parmi captés + boxés, rétrécie vers la moyenne (K = 20 actions) ;
--   · dec_sweeper_per_match : sorties hors surface par match.
-- Limite : le dénominateur « centres subis » est inconnu ; ces mesures décrivent un STYLE (volume, capter ou
-- boxer, sortir), pas un taux de réussite. Valeurs de taille : fiche WhoScored courante.
-- ══════════════════════════════════════════════════════════════════════════════

{% set k_actions = 20 %}

WITH

keepers AS (
    SELECT str_player_id, str_season,
           CAST(SUBSTR(str_season, 1, 4) AS INTEGER)                AS y,
           SUM(int_n_matchs)                                         AS n_matches,
           arg_max(str_player_name, int_n_matchs)                    AS player_name,
           MAX(CASE WHEN int_height > 150 THEN int_height END)       AS height
    FROM {{ ref('intermediate_whoscored_player_team_season') }}
    WHERE str_macro_position = 'GK' AND str_season >= '2014-2015'
    GROUP BY str_player_id, str_season
),

acts AS (
    SELECT str_player_id, str_season,
           COUNT(*) FILTER (WHERE str_type_name = 'Claim')          AS claims,
           COUNT(*) FILTER (WHERE str_type_name = 'Punch')          AS punches,
           COUNT(*) FILTER (WHERE str_type_name = 'KeeperSweeper')  AS sweeps
    FROM {{ ref('intermediate_whoscored_events') }}
    WHERE str_type_name IN ('Claim', 'Punch', 'KeeperSweeper') AND str_season >= '2014-2015'
    GROUP BY str_player_id, str_season
),

season_full AS (
    SELECT k.*, COALESCE(a.claims, 0) AS claims, COALESCE(a.punches, 0) AS punches, COALESCE(a.sweeps, 0) AS sweeps
    FROM keepers k
    LEFT JOIN acts a ON a.str_player_id = k.str_player_id AND a.str_season = k.str_season
),

priors AS (
    SELECT SUM(claims) * 1.0 / NULLIF(SUM(claims + punches), 0) AS p_claim FROM season_full
),

win AS (
    SELECT a.str_player_id, a.str_season,
           COUNT(*)            AS n_seasons,
           SUM(b.n_matches)    AS n_matches,
           SUM(b.claims)       AS claims,
           SUM(b.punches)      AS punches,
           SUM(b.sweeps)       AS sweeps
    FROM season_full a
    JOIN season_full b ON b.str_player_id = a.str_player_id AND b.y BETWEEN a.y - 2 AND a.y
    GROUP BY a.str_player_id, a.str_season
),

mdl_out AS (
    SELECT
        w.str_player_id,
        w.str_season,
        k.player_name                                                           AS str_player_name,
        k.height                                                                AS int_height,
        CAST(w.n_seasons AS INTEGER)                                            AS int_n_seasons_window,
        CAST(w.n_matches AS INTEGER)                                            AS int_n_matches,
        CAST(w.claims AS INTEGER)                                               AS int_n_claims,
        CAST(w.punches AS INTEGER)                                              AS int_n_punches,
        CAST(w.sweeps AS INTEGER)                                               AS int_n_sweeps,
        (w.claims + w.punches) * 1.0 / NULLIF(w.n_matches, 0)                   AS dec_aerial_actions_per_match,
        (w.claims + {{ k_actions }} * pr.p_claim) / (w.claims + w.punches + {{ k_actions }}) AS dec_claim_share,
        w.sweeps * 1.0 / NULLIF(w.n_matches, 0)                                 AS dec_sweeper_per_match
    FROM win w
    CROSS JOIN priors pr
    LEFT JOIN keepers k ON k.str_player_id = w.str_player_id AND k.str_season = w.str_season
)

SELECT * FROM mdl_out
