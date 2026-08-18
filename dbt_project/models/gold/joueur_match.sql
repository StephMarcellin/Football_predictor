{{
    config(
        materialized='table',
        schema='gold',
        alias='joueur_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.joueur_match — grain (match_id, team_id, player_id) — 7e table de grain
-- Table d'assemblage du modèle BUTEURS (une ligne = un joueur du XI dans un match).
-- Réunit les features scorer SEASON-LAG (joueur_saison) et la feature propre 75 :
--   scorer_context_vs_opponent_style = Σ_couloir [ tirs du joueur dans le couloir
--   × vulnérabilité de l'adversaire dans le couloir MIROIR (1 − def_solidity) ].
--   C'est la version personnalisée de la confrontation (famille 7) pour le tireur.
--
-- Candidats = XI de départ (compo connue ~1 h avant). Pas d'imputation de la
-- solidité adverse manquante (la famille 11 comblera) → contexte NULL/partiel
-- quand le profil adverse manque.
-- ══════════════════════════════════════════════════════════════════════════════

WITH xi AS (
    SELECT DISTINCT
        l.match_id, l.team_id, b.opponent_id, l.player_id, b.season, b.venue
    FROM {{ ref('int_whoscored_lineup') }} l
    JOIN {{ ref('backbone') }} b
        ON b.match_id = l.match_id AND b.team_id = l.team_id
    WHERE l.start_minute = 0 AND l.match_id IS NOT NULL
),

-- Tirs du joueur par couloir (repère attaquant : c1,c2=gauche, c3=axe, c4,c5=droit ;
-- cellules attaquantes z4,z5), depuis son profil zonal N-1.
player_corridor_shots AS (
    SELECT player_id, season,
        CASE WHEN CAST(substr(zone_5x5, 5, 1) AS INTEGER) IN (1, 2) THEN 'gauche'
             WHEN CAST(substr(zone_5x5, 5, 1) AS INTEGER) = 3 THEN 'axe'
             ELSE 'droit' END AS corridor,
        SUM(off_shot_volume_by_zone_lag) AS shot_vol
    FROM {{ source('machine_learning', 'zonal_profiles_imputed') }}
    WHERE CAST(substr(zone_5x5, 2, 1) AS INTEGER) IN (4, 5)
    GROUP BY 1, 2, 3
),

-- Feature 75 : croisement tirs du joueur × vulnérabilité adverse (couloir miroir).
context AS (
    SELECT x.match_id, x.team_id, x.player_id,
        SUM(pcs.shot_vol * (1 - tcp.def_solidity)) AS scorer_context_vs_opponent_style
    FROM xi x
    JOIN player_corridor_shots pcs
        ON pcs.player_id = x.player_id AND pcs.season = x.season
    LEFT JOIN {{ ref('team_corridor_profile') }} tcp
        ON tcp.match_id = x.match_id
        AND tcp.team_id = x.opponent_id
        AND tcp.corridor = CASE pcs.corridor
                              WHEN 'gauche' THEN 'droit'
                              WHEN 'droit'  THEN 'gauche'
                              ELSE 'axe'
                           END
    GROUP BY x.match_id, x.team_id, x.player_id
)

SELECT
    x.match_id,
    x.team_id,
    x.opponent_id,
    x.player_id,
    CASE WHEN x.venue = 'Home' THEN 1 ELSE 0 END AS is_home,

    -- Features Buteurs (SEASON-LAG, depuis joueur_saison)
    js.scorer_xg_per90_lag,
    js.scorer_shots_per90_lag,
    js.scorer_team_shot_share_lag,
    js.scorer_penalty_taker_lag,
    js.scorer_freekick_taker_lag,
    js.off_xg_per_shot_lag,

    -- Feature propre 75
    c.scorer_context_vs_opponent_style
FROM xi x
LEFT JOIN {{ ref('joueur_saison') }} js
    ON js.match_id = x.match_id AND js.team_id = x.team_id AND js.player_id = x.player_id
LEFT JOIN context c
    ON c.match_id = x.match_id AND c.team_id = x.team_id AND c.player_id = x.player_id
