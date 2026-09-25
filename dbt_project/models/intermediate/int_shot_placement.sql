{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_shot_placement'
    )
}}

-- Placement de tir au grain « un tir » — base du Post-Shot xG (xGOT).
-- Filtre les events de tir (13 MissedShots, 14 ShotOnPost, 15 SavedShot, 16 Goal)
-- depuis int_event_enriched et dérive la géométrie du placement dans le cadre du
-- but à partir de goal_mouth_y (horizontal) / goal_mouth_z (hauteur).
--
-- Cadre du but observé : poteaux à Y≈45.2 et 54.8 (centre 50), barre à Z≈38.
-- Les dérivés de placement ne valent que pour les tirs CADRÉS (SavedShot + Goal),
-- seuls concernés par le xGOT ; NULL sinon.
--
-- Le xGOT lui-même (modèle entraîné) reste en ML/gold. Ce modèle expose les
-- ENTRÉES (placement + xG pré-tir proxy) + des dérivés géométriques de difficulté.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- event_values lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_event_values AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        dec_danger_position                                          AS "danger_position",
        dec_chance_creation                                          AS "chance_creation",
        dec_def_execution_quality                                    AS "def_execution_quality",
        dec_pressure_context                                         AS "pressure_context",
        dec_context_weight                                           AS "context_weight",
        dec_action_value                                             AS "action_value"
    FROM {{ ref('event_values') }}
),

-- events_qual lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        int_row_num                                                  AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
        str_qual_type_name                                           AS "qual_type_name",
        str_qual_value                                               AS "qual_value"
    FROM {{ ref('events_qual') }}
),

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

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

mdl_body AS (
WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
-- Même patron que int_event_enriched : on ne traite que les matchs dont le
-- scraped_at dépasse le dernier déjà présent. Grain « un tir » = append-only
-- (les tirs passés ne changent pas) → incrémental correct.
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM (
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
        FROM {{ this }}
    )
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_whoscored_events
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_int_whoscored_events
),
{% endif %}

-- Tirs contrés par un défenseur (qualifier 82) : jamais cadrés, pas de placement.
-- On les repère pour les EXCLURE du périmètre xGOT.
blocked AS (
    SELECT DISTINCT match_id, row_num
    FROM in_events_qual
    WHERE qual_type_id = 82
),

shots AS (
    SELECT
        e.match_id,
        e.team_id,
        e.player_id,
        e.event_id,
        e.row_num,
        e.expanded_minute,
        e.season,
        e.league_source,
        e.scraped_at,
        e.type_id,
        e.type_name,
        e.x,                       -- position terrain de la frappe
        e.y,
        e.goal_mouth_y,
        e.goal_mouth_z,
        e.blocked_x,
        e.blocked_y,
        e.is_own_goal,                             -- distingue les CSC (type 16 c.s.c.)
        (e.type_id = 16)          AS is_goal,
        (b.match_id IS NOT NULL)  AS is_blocked,     -- contré par un défenseur
        (e.type_id IN (15, 16) AND b.match_id IS NULL)
                                  AS is_on_target     -- saved + goal, HORS contre
    FROM in_int_event_enriched e
    LEFT JOIN blocked b
        ON b.match_id = e.match_id AND b.row_num = e.row_num
    WHERE e.type_id IN (13, 14, 15, 16)
      AND e.match_id IN (SELECT match_id FROM new_matches)
    -- int_event_enriched contient ~94 doublons (match_id, row_num) sur les tirs
    -- (souci de son modèle incrémental) : on garde une ligne par tir pour
    -- garantir l'unicité de la clé du placement.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num ORDER BY e.event_id) = 1
)

SELECT
    s.*,

    -- xG pré-tir (proxy chance_creation) : baseline du xGOT
    ev.chance_creation AS pre_shot_xg_proxy,

    -- ── Dérivés de placement (tirs cadrés uniquement) ─────────────────
    -- Écart horizontal au centre (0 = plein axe, ~4.8 = près d'un poteau)
    CASE WHEN s.is_on_target THEN ABS(s.goal_mouth_y - 50.0) END        AS offset_center,
    -- Hauteur dans le but (0 = ras de terre, ~38 = sous la barre)
    CASE WHEN s.is_on_target THEN s.goal_mouth_z END                    AS height,

    -- Colonne / rangée de la grille 3×3
    CASE WHEN s.is_on_target THEN
        CASE WHEN s.goal_mouth_y <  48.4 THEN 'left'
             WHEN s.goal_mouth_y <= 51.6 THEN 'center'
             ELSE 'right' END
    END                                                                 AS placement_col,
    CASE WHEN s.is_on_target THEN
        CASE WHEN s.goal_mouth_z <  12.7 THEN 'low'
             WHEN s.goal_mouth_z <= 25.3 THEN 'mid'
             ELSE 'high' END
    END                                                                 AS placement_row,
    -- Zone 9 cases : rangée_colonne (ex: high_left = lucarne gauche)
    CASE WHEN s.is_on_target THEN
        (CASE WHEN s.goal_mouth_z <  12.7 THEN 'low'
              WHEN s.goal_mouth_z <= 25.3 THEN 'mid'  ELSE 'high'  END)
        || '_' ||
        (CASE WHEN s.goal_mouth_y <  48.4 THEN 'left'
              WHEN s.goal_mouth_y <= 51.6 THEN 'center' ELSE 'right' END)
    END                                                                 AS placement_zone,

    -- Distance normalisée à la lucarne la plus proche (0 = pleine lucarne, plus
    -- grand = plus central/bas). Coords normalisées dans le cadre pour ne pas
    -- mélanger les échelles Y (largeur ~9.6) et Z (hauteur ~38).
    CASE WHEN s.is_on_target THEN
        LEAST(
            SQRT(POW((s.goal_mouth_y - 45.2) / 9.6, 2) + POW(1 - s.goal_mouth_z / 38.0, 2)),
            SQRT(POW((54.8 - s.goal_mouth_y) / 9.6, 2) + POW(1 - s.goal_mouth_z / 38.0, 2))
        )
    END                                                                 AS corner_dist

FROM shots s
LEFT JOIN in_event_values ev
    ON ev.match_id = s.match_id
   AND ev.row_num  = s.row_num
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "row_num"                                                    AS int_row_num,
        "expanded_minute"                                            AS int_expanded_minute,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        CAST(type_id AS VARCHAR)                                     AS str_type_id,
        "type_name"                                                  AS str_type_name,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "goal_mouth_y"                                               AS dec_goal_mouth_y,
        "goal_mouth_z"                                               AS dec_goal_mouth_z,
        "blocked_x"                                                  AS dec_blocked_x,
        "blocked_y"                                                  AS dec_blocked_y,
        "is_own_goal"                                                AS bool_is_own_goal,
        "is_goal"                                                    AS bool_is_goal,
        "is_blocked"                                                 AS bool_is_blocked,
        "is_on_target"                                               AS bool_is_on_target,
        "pre_shot_xg_proxy"                                          AS dec_pre_shot_xg_proxy,
        "offset_center"                                              AS dec_offset_center,
        "height"                                                     AS dec_height,
        "placement_col"                                              AS str_placement_col,
        "placement_row"                                              AS str_placement_row,
        "placement_zone"                                             AS str_placement_zone,
        "corner_dist"                                                AS dec_corner_dist
    FROM mdl_body
)

SELECT * FROM mdl_out
