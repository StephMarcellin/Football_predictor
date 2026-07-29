{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_xt_actions'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

-- Actions atomiques pour l'expected threat (xT), affectées à la GRILLE FINE 16×12.
-- Grain : une action de possession (déplacement, tir OU perte de balle). Alimente
-- l'estimation de la grille (xt_grid.py) et le calcul des contributions
-- (int_xt_contributions).
--
-- ⚠️ AFFECTATION (x,y) → CASE DÉFINIE ICI, UNE SEULE FOIS. Les autres pièces
-- (xt_grid.py, int_xt_contributions) LISENT col/row, elles ne recalculent jamais —
-- garantit que la grille estimée et les contributions parlent de la même case.
--   col = quantile de x sur 16 (0 = son but, 15 = but adverse)
--   row = quantile de y sur 12 (0..11, 6 ≈ axe central)
--
-- Déplacements = passes réussies (player_passes_raw) + dribbles TakeOn réussis
--                (int_event_enriched, destination = prochaine touche du joueur).
-- Tirs = int_shot_placement, HORS CSC (is_own_goal).
-- Pertes = fin de possession (passe/dribble ratés, contrôle manqué, Dispossessed,
--          OffsidePass) depuis int_event_enriched ; pas de destination.

WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM {{ ref('int_event_enriched') }}
),
{% endif %}

-- ── Déplacements : passes + dribbles ──────────────────────────────────────────
-- Dribbles : touche SUIVANTE du joueur, calculee sur TOUS ses events.
-- La window balaie tous les evenements du joueur (pas seulement les TakeOns) ;
-- LEAD pointe donc sur sa vraie prochaine touche. On filtre les TakeOn au-dessus.
dribbles_next_touch AS (
    SELECT
        match_id, season, league_source, team_id, player_id, row_num, x, y,
        type_id, outcome_id, expanded_minute, second,
        LEAD(x)               OVER w AS next_x,
        LEAD(y)               OVER w AS next_y,
        LEAD(expanded_minute) OVER w AS next_min,
        LEAD(second)          OVER w AS next_sec
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    WINDOW w AS (PARTITION BY match_id, player_id ORDER BY expanded_minute, second, row_num)
),

moves AS (
    -- Passes réussies (déjà x/y/end_x/end_y)
    SELECT
        match_id, season, league_source, team_id,
        passer_id                       AS player_id,
        row_num, x, y, end_x, end_y,
        'move'                          AS action_kind,
        FALSE                           AS is_shot,
        CAST(NULL AS BOOLEAN)           AS is_goal
    FROM {{ ref('player_passes_raw') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)

    UNION ALL

    -- Dribbles (TakeOn reussis) : destination = prochaine touche du joueur (<= 10 s)
    SELECT
        match_id, season, league_source, team_id, player_id, row_num, x, y,
        CASE WHEN (next_min * 60 + next_sec) - (expanded_minute * 60 + second) <= 10
             THEN next_x END          AS end_x,
        CASE WHEN (next_min * 60 + next_sec) - (expanded_minute * 60 + second) <= 10
             THEN next_y END          AS end_y,
        'move'                        AS action_kind,
        FALSE                         AS is_shot,
        CAST(NULL AS BOOLEAN)         AS is_goal
    FROM dribbles_next_touch
    WHERE type_id = 3 AND outcome_id = 1
),

-- ── Tirs (hors CSC) ───────────────────────────────────────────────────────────
shots AS (
    SELECT
        match_id, season, league_source, team_id, player_id, row_num, x, y,
        CAST(NULL AS DOUBLE)            AS end_x,
        CAST(NULL AS DOUBLE)            AS end_y,
        'shot'                          AS action_kind,
        TRUE                            AS is_shot,
        is_goal
    FROM {{ ref('int_shot_placement') }}
    WHERE type_id IN (13, 14, 15, 16)
      AND COALESCE(is_own_goal, FALSE) = FALSE
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── Pertes de balle (fin de possession attribuée au porteur) ──────────────────
-- Passe ratée (1/0), dribble raté (3/0), contrôle manqué (61/0), Dispossessed (50),
-- OffsidePass (2). Pas de destination (la possession s'arrête) -> end_x/end_y NULL.
turnovers AS (
    SELECT
        match_id, season, league_source, team_id, player_id, row_num, x, y,
        CAST(NULL AS DOUBLE)  AS end_x,
        CAST(NULL AS DOUBLE)  AS end_y,
        'turnover'            AS action_kind,
        FALSE                 AS is_shot,
        CAST(NULL AS BOOLEAN) AS is_goal
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND (
           (type_id = 1  AND outcome_id = 0)   -- passe ratée
        OR (type_id = 3  AND outcome_id = 0)   -- dribble raté
        OR (type_id = 61 AND outcome_id = 0)   -- contrôle manqué (BallTouch)
        OR  type_id = 50                       -- Dispossessed
        OR  type_id = 2                        -- OffsidePass
      )
),

actions AS (
    SELECT * FROM moves
    UNION ALL
    SELECT * FROM shots
    UNION ALL
    SELECT * FROM turnovers
)

-- ── AFFECTATION À LA GRILLE (source unique de vérité) ─────────────────────────
SELECT
    match_id,
    row_num,                                     -- (match_id, row_num) = clé unique
    season,
    league_source,
    team_id,
    player_id,
    action_kind,
    is_shot,
    is_goal,

    -- Case de départ (origine de l'action)
    CASE WHEN x IS NOT NULL
         THEN GREATEST(0, LEAST(15, CAST(x / 100.0 * 16 AS INTEGER))) END  AS col_from,
    CASE WHEN y IS NOT NULL
         THEN GREATEST(0, LEAST(11, CAST(y / 100.0 * 12 AS INTEGER))) END  AS row_from,

    -- Case d'arrivée (déplacements ; NULL pour les tirs et dribbles sans suite)
    CASE WHEN end_x IS NOT NULL
         THEN GREATEST(0, LEAST(15, CAST(end_x / 100.0 * 16 AS INTEGER))) END AS col_to,
    CASE WHEN end_y IS NOT NULL
         THEN GREATEST(0, LEAST(11, CAST(end_y / 100.0 * 12 AS INTEGER))) END AS row_to

FROM actions
-- int_event_enriched a des (match_id, row_num) non uniques (~4560 : bug incrémental
-- amont, dont des collisions row_num entre events différents). On garde une action
-- par (match, row_num) — priorité : tir > déplacement > perte (le tir porte le
-- signal de récompense ; sinon on préfère garder une action qui conserve la balle).
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, row_num
    ORDER BY CASE action_kind WHEN 'shot' THEN 0 WHEN 'move' THEN 1 ELSE 2 END,
             player_id, x, y
) = 1
