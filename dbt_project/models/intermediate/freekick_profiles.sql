{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='freekick_profiles'
    )
}}

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

-- player_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_possession_chains AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
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
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        int_is_rupture                                               AS "is_rupture",
        str_chain_trigger                                            AS "chain_trigger",
        int_certain_possessor                                        AS "certain_possessor",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at"
    FROM {{ ref('player_possession_chains') }}
),

mdl_body AS (
WITH

{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            str_chain_id                                                 AS "chain_id",
            CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
            CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
            CAST(str_freekick_taker_id AS INTEGER)                       AS "freekick_taker_id",
            int_row_num                                                  AS "row_num",
            CAST(str_event_id AS INTEGER)                                AS "event_id",
            int_expanded_minute                                          AS "expanded_minute",
            int_second                                                   AS "second",
            CAST(str_type_id AS INTEGER)                                 AS "type_id",
            CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
            dec_x                                                        AS "x",
            dec_y                                                        AS "y",
            str_fk_type                                                  AS "fk_type",
            bool_is_offside                                              AS "is_offside",
            str_fk_zone_type                                             AS "fk_zone_type",
            str_outcome                                                  AS "outcome",
            dec_chain_danger_total                                       AS "chain_danger_total",
            dec_chain_danger_momentum                                    AS "chain_danger_momentum",
            str_shot_body_part                                           AS "shot_body_part",
            CAST(str_clearance_player_id AS INTEGER)                     AS "clearance_player_id",
            str_clearance_quality                                        AS "clearance_quality",
            bool_is_headed_clearance                                     AS "is_headed_clearance",
            dec_x_m                                                      AS "x_m",
            dec_y_m                                                      AS "y_m",
            dec_distance_to_goal                                         AS "distance_to_goal",
            dec_angle                                                    AS "angle"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
),
{% endif %}

freekick_qual_flags AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 5   THEN 1 ELSE 0 END) AS is_freekick_pass,
        MAX(CASE WHEN qual_type_id = 26  THEN 1 ELSE 0 END) AS is_freekick_shot,
        MAX(CASE WHEN qual_type_id = 241 THEN 1 ELSE 0 END) AS is_indirect_freekick,
        MAX(CASE WHEN qual_type_id = 2   THEN 1 ELSE 0 END) AS is_cross
    FROM in_events_qual
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND qual_type_id IN (5, 26, 241, 2)
    GROUP BY match_id, row_num
),

freekick_attacking_events AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.row_num,
        pc.is_shot,
        ev.action_value
    FROM in_player_possession_chains pc
    LEFT JOIN in_event_values ev
        ON  ev.match_id = pc.match_id
        AND ev.row_num  = pc.row_num
    WHERE pc.chain_trigger = 'free_kick'
      AND pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id = pc.chain_team_id
),

freekick_last_shot AS (
    SELECT
        match_id,
        chain_id,
        MAX(row_num) AS last_shot_row
    FROM freekick_attacking_events
    WHERE is_shot = TRUE
    GROUP BY match_id, chain_id
),

freekick_danger_agg AS (
    SELECT
        cae.chain_id,
        SUM(cae.action_value)                                                 AS chain_danger_total,
        SUM(cae.action_value) FILTER (WHERE cae.row_num < cls.last_shot_row)  AS danger_before_shot,
        cls.last_shot_row
    FROM freekick_attacking_events cae
    LEFT JOIN freekick_last_shot cls
        ON  cls.match_id = cae.match_id
        AND cls.chain_id = cae.chain_id
    GROUP BY cae.chain_id, cls.last_shot_row
),

freekick_direct_anchor AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_number,
        pc.chain_team_id,
        pc.player_id      AS freekick_taker_id,
        pc.row_num,
        pc.event_id,
        pc.expanded_minute,
        pc.second,
        pc.type_id,
        pc.outcome_id,
        pc.x,
        pc.y,
        'direct'   AS fk_type,
        FALSE      AS is_offside,
        'direct_shot' AS fk_zone_type,
        -- ev.chance_creation AS xg_generated,
        CASE
            WHEN pc.type_id = 16          THEN 'goal'
            WHEN pc.type_id = 15          THEN 'shot_saved'
            WHEN pc.type_id IN (13, 14)   THEN 'shot_off_target'
        END AS outcome
    FROM in_player_possession_chains pc
    JOIN freekick_qual_flags fqf
        ON  fqf.match_id = pc.match_id
        AND fqf.row_num  = pc.row_num
    -- LEFT JOIN in_event_values ev
    --     ON  ev.match_id = pc.match_id
    --     AND ev.row_num  = pc.row_num
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_trigger    = 'free_kick'
      AND pc.type_id          IN (13, 14, 15, 16)
      AND fqf.is_freekick_shot = 1
),

freekick_pass_anchor  AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_number,
        pc.chain_team_id,
        pc.player_id      AS freekick_taker_id,
        pc.row_num,
        pc.event_id,
        pc.expanded_minute,
        pc.second,
        pc.type_id,
        pc.outcome_id,
        pc.x,
        pc.y,
        CASE WHEN fqf.is_indirect_freekick = 1 THEN 'indirect' ELSE 'short_pass' END AS fk_type,
        CASE
            WHEN pc.x < 17        THEN 'own_box'
            WHEN fqf.is_cross = 1 THEN 'crossed'
            ELSE                        'too_far'
        END AS fk_zone_type,
        CASE WHEN pc.type_id = 2 THEN TRUE ELSE FALSE END AS is_offside

    FROM in_player_possession_chains pc
    JOIN freekick_qual_flags fqf
        ON  fqf.match_id = pc.match_id
        AND fqf.row_num  = pc.row_num
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_trigger    = 'free_kick'
      AND pc.type_id          IN (1, 2)
      AND fqf.is_freekick_pass = 1
),

freekick_pass_outcome AS (
    SELECT DISTINCT ON (pc.match_id, pc.chain_id)
        pc.match_id,
        pc.chain_id,
        pc.row_num        AS shot_row_num,
        pc.event_id       AS shot_event_id,
        pc.type_id        AS shot_type_id,
        pc.outcome_id     AS shot_outcome_id,
        pc.expanded_minute AS shot_minute
    FROM in_player_possession_chains pc
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.chain_id IN (SELECT chain_id FROM freekick_pass_anchor)
      AND pc.is_shot   = TRUE
      AND pc.team_id   = pc.chain_team_id
    ORDER BY pc.match_id, pc.chain_id, pc.expanded_minute DESC, pc.second DESC
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — FREEKICK_SHOT_BODYPART
-- Tête/pied du tir résultant (réutilise shot_row_num de freekick_pass_outcome).
-- ══════════════════════════════════════════════════════════════════════════════
freekick_shot_bodypart AS (
    SELECT
        fpo.match_id,
        fpo.chain_id,
        MAX(CASE
            WHEN eq.qual_type_id = 15 THEN 'head'
            WHEN eq.qual_type_id = 20 THEN 'right_foot'
            WHEN eq.qual_type_id = 72 THEN 'left_foot'
            WHEN eq.qual_type_id = 21 THEN 'other'
        END) AS shot_body_part
    FROM freekick_pass_outcome fpo
    JOIN in_events_qual eq
        ON  eq.match_id = fpo.match_id
        AND eq.row_num  = fpo.shot_row_num
        AND eq.qual_type_id IN (15, 20, 72, 21)
    WHERE fpo.shot_row_num IS NOT NULL
    GROUP BY fpo.match_id, fpo.chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CTE — FREEKICK_DEFENDING_CLEARANCE / NEXT_POSSESSION / CLEARANCE_DETAIL
-- Même mécanique que corner_profiles : premier dégagement défensif de la
-- chaîne, puis grille de qualité selon qui récupère la 1ère possession
-- certaine ensuite, et où.
-- ══════════════════════════════════════════════════════════════════════════════
freekick_defending_clearance AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.row_num,
        pc.team_id,
        pc.player_id,
        ROW_NUMBER() OVER (
            PARTITION BY pc.match_id, pc.chain_id
            ORDER BY pc.row_num ASC
        ) AS rn_first
    FROM in_player_possession_chains pc
    WHERE pc.chain_trigger = 'free_kick'
      AND pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.type_id = 12
      AND pc.team_id != pc.chain_team_id
),

freekick_clearance_next_possession AS (
    SELECT
        cdc.match_id,
        cdc.chain_id,
        pc2.certain_possessor AS recovering_team_id,
        pc2.x AS recovery_x,
        ROW_NUMBER() OVER (
            PARTITION BY cdc.match_id, cdc.chain_id
            ORDER BY pc2.row_num ASC
        ) AS rn_recovery
    FROM freekick_defending_clearance cdc
    JOIN in_player_possession_chains pc2
        ON  pc2.match_id = cdc.match_id
        AND pc2.row_num  > cdc.row_num
        AND pc2.certain_possessor IS NOT NULL
    WHERE cdc.rn_first = 1
),

freekick_clearance_detail AS (
    SELECT
        cdc.match_id,
        cdc.chain_id,
        MAX(cdc.player_id) AS clearance_player_id,
        MAX(CASE
            WHEN eq.qual_type_id = 15 THEN TRUE
            WHEN eq.qual_type_id = 21 THEN FALSE
        END) AS is_headed_clearance,
        MAX(CASE
            WHEN ncp.recovering_team_id = cdc.team_id THEN 'perfect'
            WHEN ncp.recovering_team_id IS NULL         THEN NULL
            WHEN ncp.recovery_x >= 83                   THEN 'failed'
            WHEN ncp.recovery_x >= 75                   THEN 'poor'
            ELSE                                             'good'
        END) AS clearance_quality
    FROM freekick_defending_clearance cdc
    LEFT JOIN in_events_qual eq
        ON  eq.match_id = cdc.match_id
        AND eq.row_num  = cdc.row_num
        AND eq.qual_type_id IN (15, 21)
    LEFT JOIN freekick_clearance_next_possession ncp
        ON  ncp.match_id = cdc.match_id
        AND ncp.chain_id = cdc.chain_id
        AND ncp.rn_recovery = 1
    WHERE cdc.rn_first = 1
    GROUP BY cdc.match_id, cdc.chain_id
),

last_team_action AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (
            PARTITION BY pc.chain_id
            ORDER BY pc.expanded_minute DESC, pc.second DESC, pc.row_num DESC
        ) AS rn_last
    FROM in_player_possession_chains pc
    WHERE pc.chain_id IN (SELECT chain_id FROM freekick_pass_anchor)
      AND pc.team_id = pc.chain_team_id
),

freekick_pass_enriched AS (
    SELECT
        fpa.match_id,
        fpa.chain_id,
        fpa.chain_number,
        fpa.chain_team_id,
        fpa.freekick_taker_id,
        fpa.row_num,
        fpa.event_id,
        fpa.expanded_minute,
        fpa.second,
        fpa.type_id,
        fpa.outcome_id,
        fpa.x,
        fpa.y,
        fpa.fk_type,
        fpa.is_offside,
        fpa.fk_zone_type,
        -- ev.chance_creation AS xg_generated,
        CASE
            WHEN fpa.is_offside = TRUE                              THEN 'offside'
            WHEN fpo.shot_type_id = 16                              THEN 'goal'
            WHEN fpo.shot_type_id = 15                              THEN 'shot_saved'
            WHEN fpo.shot_type_id IN (13, 14)                       THEN 'shot_off_target'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id = 4
                 AND lta.outcome_id = 1                             THEN 'foul_won'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id = 6
                 AND lta.outcome_id = 1                             THEN 'corner_won'
            WHEN fpo.shot_row_num IS NULL AND lta.type_id IN (1, 3, 61)
                 AND lta.outcome_id = 1                             THEN 'open_play'
            ELSE 'turnover'
        END AS outcome

    FROM freekick_pass_anchor fpa

    LEFT JOIN freekick_pass_outcome fpo
        ON  fpo.match_id = fpa.match_id
        AND fpo.chain_id = fpa.chain_id

    LEFT JOIN last_team_action lta
        ON  lta.chain_id = fpa.chain_id
        AND lta.rn_last  = 1

    -- LEFT JOIN in_event_values ev
    --     ON  ev.match_id = fpa.match_id
    --     AND ev.row_num   = fpo.shot_row_num
),

freekick_anchors_raw AS (
    SELECT * FROM freekick_direct_anchor
    UNION ALL
    SELECT * FROM freekick_pass_enriched
),

freekick_anchors AS (
    SELECT
        far.*,
        da.chain_danger_total,
        CASE
            WHEN da.last_shot_row IS NULL THEN NULL
            ELSE da.danger_before_shot / NULLIF(da.chain_danger_total, 0)
        END AS chain_danger_momentum,
        sbp.shot_body_part,
        ccd.clearance_player_id,
        ccd.clearance_quality,
        ccd.is_headed_clearance
    FROM freekick_anchors_raw far
    LEFT JOIN freekick_danger_agg da
        ON da.chain_id = far.chain_id
    LEFT JOIN freekick_shot_bodypart sbp
        ON  sbp.chain_id = far.chain_id
        AND far.fk_zone_type = 'crossed'
    LEFT JOIN freekick_clearance_detail ccd
        ON  ccd.chain_id = far.chain_id
        AND far.fk_zone_type = 'crossed'
),

freekick_geometry AS (
    SELECT
        *,
        x * 1.05 AS x_m,
        y * 0.68 AS y_m
    FROM freekick_anchors
),

freekick_geometry_distances AS (
    SELECT
        *,
        SQRT(POWER(105 - x_m, 2) + POWER(34 - y_m, 2))          AS distance_to_goal,
        SQRT(POWER(105 - x_m, 2) + POWER(30.34 - y_m, 2))       AS dist_to_post1,
        SQRT(POWER(105 - x_m, 2) + POWER(37.66 - y_m, 2))       AS dist_to_post2,
        (105 - x_m) * (105 - x_m)
            + (30.34 - y_m) * (37.66 - y_m)                     AS dot_product
    FROM freekick_geometry
)

SELECT
    * EXCLUDE (dist_to_post1, dist_to_post2, dot_product),
    ACOS(
        GREATEST(-1.0, LEAST(1.0,
            dot_product / (dist_to_post1 * dist_to_post2)
        ))
    ) AS angle
FROM freekick_geometry_distances
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "chain_id"                                                   AS str_chain_id,
        CAST(chain_number AS BIGINT)                                 AS int_chain_number,
        CAST(chain_team_id AS VARCHAR)                               AS str_chain_team_id,
        CAST(freekick_taker_id AS VARCHAR)                           AS str_freekick_taker_id,
        "row_num"                                                    AS int_row_num,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "expanded_minute"                                            AS int_expanded_minute,
        "second"                                                     AS int_second,
        CAST(type_id AS VARCHAR)                                     AS str_type_id,
        CAST(outcome_id AS VARCHAR)                                  AS str_outcome_id,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "fk_type"                                                    AS str_fk_type,
        "is_offside"                                                 AS bool_is_offside,
        "fk_zone_type"                                               AS str_fk_zone_type,
        "outcome"                                                    AS str_outcome,
        "chain_danger_total"                                         AS dec_chain_danger_total,
        "chain_danger_momentum"                                      AS dec_chain_danger_momentum,
        "shot_body_part"                                             AS str_shot_body_part,
        CAST(clearance_player_id AS VARCHAR)                         AS str_clearance_player_id,
        "clearance_quality"                                          AS str_clearance_quality,
        "is_headed_clearance"                                        AS bool_is_headed_clearance,
        "x_m"                                                        AS dec_x_m,
        "y_m"                                                        AS dec_y_m,
        "distance_to_goal"                                           AS dec_distance_to_goal,
        "angle"                                                      AS dec_angle
    FROM mdl_body
)

SELECT * FROM mdl_out
