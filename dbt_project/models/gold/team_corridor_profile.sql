{{
    config(
        materialized='table',
        schema='gold',
        alias='team_corridor_profile'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.team_corridor_profile — grain (match_id, team_id, corridor)
-- Brique de la famille 7 : agrège les profils zonaux des titulaires par couloir,
-- une fois la composition connue. Corridor ∈ {gauche, axe, droit}.
--
-- Méthode (CDC « Voie 2 » — par couloir occupé, position lineup) :
--   • XI de départ = joueurs présents dès la minute 0 (int_whoscored_lineup).
--   • Couloir du joueur via grid_horizontal (gauche <4.5, axe 4.5-5.5, droit >5.5).
--   • Profil zonal de la saison N-1 via joueur_zone_saison (join player+season).
--   • off_strength = Σ volume de tirs dans les cellules ATTAQUANTES (z4,z5) du
--     couloir ; def_solidity = taux de duels gagnés pondéré par volume, dans les
--     cellules DÉFENSIVES (z1,z2) du couloir.
-- Cellules du couloir : gauche=c1,c2 · axe=c3 · droit=c4,c5.
-- ══════════════════════════════════════════════════════════════════════════════

WITH xi AS (
    SELECT DISTINCT
        l.match_id, l.team_id, b.opponent_id, l.player_id, b.season,
        CASE WHEN l.grid_horizontal < 4.5 THEN 'gauche'
             WHEN l.grid_horizontal <= 5.5 THEN 'axe'
             ELSE 'droit' END AS corridor
    FROM {{ ref('int_whoscored_lineup') }} l
    JOIN {{ ref('backbone') }} b
        ON b.match_id = l.match_id AND b.team_id = l.team_id
    WHERE l.start_minute = 0 AND l.match_id IS NOT NULL
),

prof AS (
    SELECT
        x.match_id, x.team_id, x.opponent_id, x.corridor,
        jz.off_shot_volume_by_zone_lag AS off_vol,
        jz.off_cross_volume_by_zone_lag AS off_cross,
        jz.off_progressive_actions_by_zone_lag AS off_prog,
        jz.off_touch_share_by_zone_lag AS off_touch,
        jz.def_duel_win_rate_by_zone_lag AS def_wr,
        jz.def_actions_by_zone_lag AS def_act,
        jz.n_duels_prev,
        CAST(substr(jz.zone_5x5, 2, 1) AS INTEGER) AS z,
        CAST(substr(jz.zone_5x5, 5, 1) AS INTEGER) AS c
    FROM xi x
    JOIN {{ ref('joueur_zone_saison') }} jz
        ON jz.player_id = x.player_id AND jz.season = x.season
)

SELECT
    match_id, team_id, opponent_id, corridor,
    -- Cellules du couloir (in_corridor) : gauche c1,c2 · axe c3 · droit c4,c5
    SUM(CASE WHEN z IN (4, 5)
             AND ((corridor = 'gauche' AND c IN (1, 2)) OR (corridor = 'axe' AND c = 3) OR (corridor = 'droit' AND c IN (4, 5)))
        THEN off_vol ELSE 0 END) AS off_strength,
    -- Force de centre (feature 53) et de percée/dribble (feature 54), tiers off du couloir.
    SUM(CASE WHEN z IN (4, 5)
             AND ((corridor = 'gauche' AND c IN (1, 2)) OR (corridor = 'axe' AND c = 3) OR (corridor = 'droit' AND c IN (4, 5)))
        THEN off_cross ELSE 0 END) AS off_cross_strength,
    SUM(CASE WHEN z IN (4, 5)
             AND ((corridor = 'gauche' AND c IN (1, 2)) OR (corridor = 'axe' AND c = 3) OR (corridor = 'droit' AND c IN (4, 5)))
        THEN off_prog ELSE 0 END) AS off_dribble_strength,
    -- Contrôle de l'axe (feature 55). Présence/progression centrale de A (z3-z4, c3)
    -- et densité défensive centrale de B (z1-z3, c3). Ne portent leur sens que sur la
    -- ligne du couloir 'axe' (les joueurs latéraux n'ont quasi pas de cellules c3).
    SUM(CASE WHEN z IN (3, 4) AND c = 3 THEN off_prog  ELSE 0 END) AS off_central_progression,
    SUM(CASE WHEN z IN (3, 4) AND c = 3 THEN off_touch ELSE 0 END) AS off_central_touch,
    SUM(CASE WHEN z IN (1, 2, 3) AND c = 3 THEN def_act ELSE 0 END) AS def_central_density,
    SUM(CASE WHEN z IN (1, 2)
             AND ((corridor = 'gauche' AND c IN (1, 2)) OR (corridor = 'axe' AND c = 3) OR (corridor = 'droit' AND c IN (4, 5)))
        THEN def_wr * n_duels_prev ELSE 0 END)
      / NULLIF(SUM(CASE WHEN z IN (1, 2)
             AND ((corridor = 'gauche' AND c IN (1, 2)) OR (corridor = 'axe' AND c = 3) OR (corridor = 'droit' AND c IN (4, 5)))
        THEN n_duels_prev ELSE 0 END), 0) AS def_solidity
FROM prof
GROUP BY match_id, team_id, opponent_id, corridor
