{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_players'
    )
}}

-- Dimension joueur par saison et par équipe.
-- Grain : (player_id, season, team_id canonique). Un joueur transféré en cours
-- de saison apparaît donc sur plusieurs lignes (une par équipe) — c'est voulu.
--
-- Attributs de profil : height / weight (valeurs COURANTES au moment du scrape,
-- ~stables chez l'adulte). L'âge est volontairement EXCLU : WhoScored renvoie
-- l'âge courant, pas l'âge au match — il serait faux « par saison ».
-- Le nom vient de la table de référence, pas des faits.

WITH player_match AS (
    SELECT player_id, match_id, team_id, height, weight
    FROM {{ ref('int_whoscored_player_match') }}
    WHERE match_id IS NOT NULL
      AND team_id  IS NOT NULL
),

-- Saison rattachée au match (1 ligne par match_id).
match_season AS (
    SELECT DISTINCT match_id, season
    FROM {{ ref('int_whoscored_match_index') }}
    WHERE match_id IS NOT NULL
),

names AS (
    SELECT player_id, player_name
    FROM {{ source('silver', 'stg_whoscored_players_ref') }}
)

SELECT
    pm.player_id,
    ms.season,
    pm.team_id,
    MAX(n.player_name) AS player_name,
    MAX(pm.height)     AS height,
    MAX(pm.weight)     AS weight,
    COUNT(*)           AS n_matchs   -- matchs joués pour cette équipe cette saison
FROM player_match pm
JOIN match_season ms ON pm.match_id = ms.match_id
LEFT JOIN names   n  ON pm.player_id = n.player_id
GROUP BY pm.player_id, ms.season, pm.team_id
