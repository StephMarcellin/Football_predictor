{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_progressive_carries'
    )
}}


-- Conduites (dribbles) au grain « un dribble réussi ».
-- WhoScored/Opta ne loggue AUCUN event « carry » : le seul événement de conduite
-- balle au pied est le TakeOn (type 3) — un joueur élimine un adversaire en dribble.
-- On pivote donc sur les TakeOn RÉUSSIS côté offensif (outcome 1 + qual 286).
--
-- Le TakeOn est un événement ponctuel (pas de end_x, ni qual Length/Angle) : la
-- progression se mesure du dribble jusqu'à la PROCHAINE touche du même joueur DANS
-- LA MÊME CHAÎNE de possession (borne = ne pas capter une touche 2 min plus tard).
--
-- Progression = réduction de distance au centre du but adverse, en MÈTRES.
-- Coordonnées WhoScored 0-100 → terrain 105 m × 68 m : x_m = x*1.05, y_m = y*0.68,
-- but adverse au centre (105, 34). is_progressive = gain ≥ 5 m vers le but.
--
-- Comptes de référence (échantillon 300 matchs, validés read-only) :
--   ~7 dribbles réussis / équipe / match, progression moyenne +4,8 m,
--   ~2,8 dribbles progressifs (≥5 m) / équipe / match.

WITH

-- ── FILTRE INCRÉMENTAL (même patron que player_possession_chains) ──────────────
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM {{ ref('player_possession_chains') }}
),
{% endif %}

-- ── Événements de possession, ordonnés dans la chaîne, avec la prochaine touche
--    du MÊME joueur dans la même chaîne (LEAD sur la partition joueur) ──────────
pc AS (
    SELECT
        c.match_id,
        c.chain_id,
        c.season,
        c.league_source,
        c.scraped_at,
        c.event_id,
        c.row_num,
        c.team_id,
        c.player_id,
        c.type_id,
        c.outcome_id,
        c.expanded_minute,
        c.x,
        c.y,
        -- Prochaine touche du même joueur dans la même chaîne = fin de la conduite
        LEAD(c.x) OVER w AS next_x,
        LEAD(c.y) OVER w AS next_y,
        -- Écart de temps (s) jusqu'à cette touche : borne anti-bruit. Si la touche
        -- suivante arrive > 5 s après (chaîne longue, re-touche tardive), ce n'est
        -- pas une continuation de la conduite → on ne mesurera pas la progression.
        LEAD(c.expanded_minute * 60 + c.second) OVER w
            - (c.expanded_minute * 60 + c.second)               AS next_gap_s
    FROM {{ ref('player_possession_chains') }} c
    WHERE c.match_id IN (SELECT match_id FROM new_matches)
    WINDOW w AS (
        PARTITION BY c.match_id, c.chain_id, c.player_id
        ORDER BY c.expanded_minute, c.second, c.row_num
    )
),

-- ── Flag « côté offensif » (qual 286) : on veut le dribbleur, pas le défenseur ─
offensive_side AS (
    SELECT DISTINCT match_id, row_num
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id = 286
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── Dribbles réussis (pivot) ──────────────────────────────────────────────────
dribbles AS (
    SELECT
        pc.match_id,
        pc.row_num,
        pc.event_id,
        pc.season,
        pc.league_source,
        pc.scraped_at,
        pc.team_id,
        pc.player_id,
        pc.expanded_minute,
        pc.x            AS dribble_x,
        pc.y            AS dribble_y,
        -- Touche suivante seulement si elle suit dans ≤ 5 s (continuation réelle)
        CASE WHEN pc.next_gap_s <= 5 THEN pc.next_x END AS next_x,
        CASE WHEN pc.next_gap_s <= 5 THEN pc.next_y END AS next_y
    FROM pc
    JOIN offensive_side o
        ON o.match_id = pc.match_id
       AND o.row_num  = pc.row_num
    WHERE pc.type_id    = 3    -- TakeOn
      AND pc.outcome_id = 1    -- réussi
)

-- ── ASSEMBLAGE : progression en mètres vers le but ────────────────────────────
SELECT
    match_id,
    row_num,                                     -- (match_id, row_num) = clé unique
    event_id,
    season,
    league_source,
    scraped_at,
    team_id,
    player_id,
    expanded_minute,
    dribble_x,
    dribble_y,
    next_x,
    next_y,

    -- Réduction de distance au but (m) entre le dribble et la touche suivante
    SQRT(POW(105 - dribble_x * 1.05, 2) + POW(34 - dribble_y * 0.68, 2))
      - SQRT(POW(105 - next_x    * 1.05, 2) + POW(34 - next_y    * 0.68, 2))
                                                 AS progress_m,

    -- Conduite progressive = gain ≥ 5 m vers le but (FALSE si pas de suite)
    COALESCE(
        SQRT(POW(105 - dribble_x * 1.05, 2) + POW(34 - dribble_y * 0.68, 2))
          - SQRT(POW(105 - next_x * 1.05, 2) + POW(34 - next_y * 0.68, 2)) >= 5,
        FALSE
    )                                            AS is_progressive

FROM dribbles
