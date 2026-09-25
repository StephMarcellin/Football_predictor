{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_xt_actions'
    )
}}

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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_event_enriched lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_event_enriched AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        bool_is_touch                                                AS "is_touch",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_is_leading_to_goal                                       AS "is_leading_to_goal",
        int_is_intentional_goal_assist                               AS "is_intentional_goal_assist",
        int_is_intentional_assist                                    AS "is_intentional_assist",
        int_is_big_chance_created                                    AS "is_big_chance_created",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        int_is_leading_to_attempt                                    AS "is_leading_to_attempt",
        int_has_defensive_qual                                       AS "has_defensive_qual",
        int_has_offensive_qual                                       AS "has_offensive_qual",
        int_has_opposite_event                                       AS "has_opposite_event",
        int_team_score                                               AS "team_score",
        int_opp_score                                                AS "opp_score"
    FROM {{ ref('int_event_enriched') }}
),

-- int_shot_placement lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_shot_placement AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        bool_is_own_goal                                             AS "is_own_goal",
        bool_is_goal                                                 AS "is_goal",
        bool_is_blocked                                              AS "is_blocked",
        bool_is_on_target                                            AS "is_on_target",
        dec_pre_shot_xg_proxy                                        AS "pre_shot_xg_proxy",
        dec_offset_center                                            AS "offset_center",
        dec_height                                                   AS "height",
        str_placement_col                                            AS "placement_col",
        str_placement_row                                            AS "placement_row",
        str_placement_zone                                           AS "placement_zone",
        dec_corner_dist                                              AS "corner_dist"
    FROM {{ ref('int_shot_placement') }}
),

-- player_passes_raw lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_passes_raw AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        str_chain_trigger                                            AS "chain_trigger",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_passer_id AS INTEGER)                               AS "passer_id",
        CAST(str_receiver_id AS INTEGER)                             AS "receiver_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        bool_is_progressive                                          AS "is_progressive",
        bool_is_creative                                             AS "is_creative",
        bool_is_buildup                                              AS "is_buildup"
    FROM {{ ref('player_passes_raw') }}
),

mdl_body AS (
WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            int_row_num                                                  AS "row_num",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            str_action_kind                                              AS "action_kind",
            bool_is_shot                                                 AS "is_shot",
            bool_is_goal                                                 AS "is_goal",
            int_col_from                                                 AS "col_from",
            int_row_from                                                 AS "row_from",
            int_col_to                                                   AS "col_to",
            int_row_to                                                   AS "row_to"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_int_event_enriched
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
    FROM in_int_event_enriched
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
    FROM in_player_passes_raw
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
    FROM in_int_shot_placement
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
    FROM in_int_event_enriched
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
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "action_kind"                                                AS str_action_kind,
        "is_shot"                                                    AS bool_is_shot,
        "is_goal"                                                    AS bool_is_goal,
        "col_from"                                                   AS int_col_from,
        "row_from"                                                   AS int_row_from,
        "col_to"                                                     AS int_col_to,
        "row_to"                                                     AS int_row_to
    FROM mdl_body
)

SELECT * FROM mdl_out
