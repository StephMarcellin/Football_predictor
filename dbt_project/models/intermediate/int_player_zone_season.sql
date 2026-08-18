{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_player_zone_season'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- int_player_zone_season — grain (player_id, zone_5x5, season)
-- Agrégat zonal (5×5) OFFENSIF par joueur et par saison JOUÉE. Base de
-- gold.joueur_zone_saison (qui n'en fait que le décalage saison N-1).
--
-- Deux métriques :
--   • avg_touch_share : part moyenne des touches dans la cellule (grille
--     pct_zX_cY de player_match_stats, remise en format tall).
--   • total_shots : tirs pris dans la cellule (int_shot_placement binné en 5×5,
--     repère de l'attaquant, CSC exclus).
--
-- Note d'implémentation : le modèle est SCINDÉ de joueur_zone_saison et
-- matérialisé en table à cause d'un bug du planner DuckDB 1.5.1 (jointure de CTE
-- agrégés + QUALIFY). Le dédoublonnage de player_match_stats se fait ici par
-- moyenne au grain match (touch_match), sans QUALIFY.
-- ══════════════════════════════════════════════════════════════════════════════

WITH

-- 1) Grille de touches remise en tall : une ligne par (match, joueur, cellule).
touch_tall AS (
    {% for z in range(1, 6) %}{% for c in range(1, 6) %}
    SELECT match_id, player_id, season, 'z{{ z }}_c{{ c }}' AS zone_5x5, pct_z{{ z }}_c{{ c }} AS touch_share
    FROM {{ ref('player_match_stats') }}
    {% if not (z == 5 and c == 5) %}UNION ALL{% endif %}
    {% endfor %}{% endfor %}
),

-- 2) Dédoublonnage : moyenne au grain (match, joueur, cellule) — écrase les
--    28 clés en double de la source sans recourir à QUALIFY.
touch_match AS (
    SELECT match_id, player_id, season, zone_5x5, AVG(touch_share) AS touch_share
    FROM touch_tall
    GROUP BY match_id, player_id, season, zone_5x5
),

-- 3) Profil de touches par (joueur, zone, saison).
touch_season AS (
    SELECT player_id, zone_5x5, season,
        AVG(touch_share)         AS avg_touch_share,
        COUNT(DISTINCT match_id) AS n_matches
    FROM touch_match
    GROUP BY player_id, zone_5x5, season
),

-- 4) Tirs par (joueur, zone, saison) — binning 5×5, repère attaquant, CSC exclus.
shots_season AS (
    SELECT player_id, zone_5x5, season, SUM(shots_in_cell) AS total_shots
    FROM (
        SELECT player_id, season,
            'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
            COUNT(*) AS shots_in_cell
        FROM {{ ref('int_shot_placement') }}
        WHERE is_own_goal = FALSE
        GROUP BY player_id, season,
            'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
    )
    GROUP BY player_id, zone_5x5, season
),

-- 5) Duels défensifs par (joueur, zone, saison) — côté défenseur (B).
--    RETOURNEMENT vers la cage du défenseur : les coords sont dans le repère de
--    l'attaquant (A), on les inverse en (100−x, 100−y) pour que la cellule soit
--    définie par rapport au but que le défenseur protège. Validé : un duel à
--    x=90 (près du but de A) tombe en z1 (tiers défensif de B).
duels_season AS (
    SELECT player_id_b AS player_id, season,
        'z' || LEAST(FLOOR((100 - x) / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR((100 - y) / 20) + 1, 5)::INT AS zone_5x5,
        COUNT(*)        AS n_duels,
        SUM(outcome_b)  AS duels_won
    FROM {{ ref('player_network_duels') }}
    GROUP BY player_id_b, season,
        'z' || LEAST(FLOOR((100 - x) / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR((100 - y) / 20) + 1, 5)::INT
),

-- 6) Danger offensif par (joueur, zone, saison) — feature 35, depuis event_values.
--    Actions OFFENSIVES uniquement (def_execution_quality IS NULL) → repère de
--    l'attaquant, pas de retournement. On somme danger_position par cellule.
danger_season AS (
    SELECT player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        SUM(danger_position) AS total_danger
    FROM {{ ref('event_values') }}
    WHERE def_execution_quality IS NULL
      AND danger_position IS NOT NULL AND match_id IS NOT NULL
      AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
),

-- 7) Passes progressives (feature 41) + centres (feature 37), par
--    (joueur, cellule, saison) depuis player_passes_raw. Repère de l'attaquant.
--    • total_progressive : is_progressive (flag amont).
--    • total_crosses : DÉFINITION GÉOMÉTRIQUE d'un centre — pas de flag amont
--      fiable (is_creative pique au centre, pas sur les flancs). Un centre =
--      passe partant du dernier tiers sur un flanc (x≥66 ET (y<20 OU y>80)) vers
--      la surface centrale (end_x≥83 ET end_y∈[21,79]). Comptabilisé dans la
--      cellule d'ORIGINE (là où le centre est délivré → flancs z4/z5).
passes_season AS (
    SELECT passer_id AS player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        SUM(CASE WHEN is_progressive THEN 1 ELSE 0 END) AS total_progressive,
        SUM(CASE WHEN x >= 66 AND (y < 20 OR y > 80)
                  AND end_x >= 83 AND end_y BETWEEN 21 AND 79
                 THEN 1 ELSE 0 END) AS total_crosses
    FROM {{ ref('player_passes_raw') }}
    WHERE match_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY passer_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
),

-- 8) Actions défensives par (joueur, cellule, saison) — feature 45/densité famille 6.
--    event_values, actions DÉFENSIVES (def_execution_quality NON NULL). Coordonnées
--    déjà dans le repère du camp du défenseur (validé : concentrées z1-z3) → PAS de
--    retournement. On dénombre les actions par cellule.
def_actions_season AS (
    SELECT player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT AS zone_5x5,
        COUNT(*) AS total_def_actions
    FROM {{ ref('event_values') }}
    WHERE def_execution_quality IS NOT NULL
      AND match_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
    GROUP BY player_id, season,
        'z' || LEAST(FLOOR(x / 20) + 1, 5)::INT || '_c' || LEAST(FLOOR(y / 20) + 1, 5)::INT
)

SELECT
    t.player_id,
    t.zone_5x5,
    t.season,
    t.avg_touch_share,
    t.n_matches,
    COALESCE(s.total_shots, 0) AS total_shots,
    COALESCE(d.n_duels, 0)     AS n_duels,
    COALESCE(d.duels_won, 0)   AS duels_won,
    COALESCE(dg.total_danger, 0) AS total_danger,
    COALESCE(ps.total_progressive, 0) AS total_progressive,
    COALESCE(ps.total_crosses, 0)     AS total_crosses,
    COALESCE(da.total_def_actions, 0) AS total_def_actions
FROM touch_season t
LEFT JOIN shots_season s
    ON  s.player_id = t.player_id
    AND s.zone_5x5  = t.zone_5x5
    AND s.season    = t.season
LEFT JOIN danger_season dg
    ON  dg.player_id = t.player_id
    AND dg.zone_5x5  = t.zone_5x5
    AND dg.season    = t.season
LEFT JOIN duels_season d
    ON  d.player_id = t.player_id
    AND d.zone_5x5  = t.zone_5x5
    AND d.season    = t.season
LEFT JOIN passes_season ps
    ON  ps.player_id = t.player_id
    AND ps.zone_5x5  = t.zone_5x5
    AND ps.season    = t.season
LEFT JOIN def_actions_season da
    ON  da.player_id = t.player_id
    AND da.zone_5x5  = t.zone_5x5
    AND da.season    = t.season
