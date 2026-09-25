{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id', 'str_zone_from', 'str_zone_to',
                    'int_period', 'str_score_state', 'str_formation'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_zone_transitions'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

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

{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            int_period                                                   AS "period",
            str_score_state                                              AS "score_state",
            str_formation                                                AS "formation",
            str_zone_from                                                AS "zone_from",
            str_zone_to                                                  AS "zone_to",
            int_n_transitions                                            AS "n_transitions",
            dec_pct_transitions                                          AS "pct_transitions",
            dec_progressive_rate                                         AS "progressive_rate"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- FORMATION_ANCHORS
-- FormationSet (type_id=34) assigné à anchor_row=1 pour couvrir
-- tout le match dès le début.
-- FormationChange (type_id=40) gardé à son row_num réel.
-- ══════════════════════════════════════════════════════════════════════════════
formation_anchors AS (
    SELECT
        match_id,
        team_id,
        1                               AS anchor_row,
        qual_value                      AS formation_code
    FROM in_events_qual
    WHERE qual_type_id = 130
      AND type_id = 34
      AND match_id IN (SELECT match_id FROM new_matches)

    UNION ALL

    SELECT
        match_id,
        team_id,
        row_num                         AS anchor_row,
        qual_value                      AS formation_code
    FROM in_events_qual
    WHERE qual_type_id = 130
      AND type_id = 40
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FORMATION_LAST
-- Pour chaque événement, on cherche le row_num de la dernière ancre
-- dont anchor_row <= row_num courant — forward fill via MAX.
-- ══════════════════════════════════════════════════════════════════════════════
formation_last AS (
    SELECT
        ie.match_id,
        ie.team_id,
        ie.row_num,
        MAX(fa.anchor_row)              AS last_anchor_row
    FROM in_int_event_enriched ie
    LEFT JOIN formation_anchors fa
        ON  fa.match_id   = ie.match_id
        AND fa.team_id    = ie.team_id
        AND fa.anchor_row <= ie.row_num
    WHERE ie.match_id IN (SELECT match_id FROM new_matches)
    GROUP BY ie.match_id, ie.team_id, ie.row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FORMATION_INTERVALS
-- Résout la formation active pour chaque row_num via le last_anchor_row.
-- ══════════════════════════════════════════════════════════════════════════════
formation_intervals AS (
    SELECT
        fl.match_id,
        fl.team_id,
        fl.row_num,
        fa.formation_code
    FROM formation_last fl
    JOIN formation_anchors fa
        ON  fa.match_id   = fl.match_id
        AND fa.team_id    = fl.team_id
        AND fa.anchor_row = fl.last_anchor_row
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_ENRICHED
-- Passes réussies depuis player_passes_raw avec player_id,
-- period, score_state, formation active.
-- ══════════════════════════════════════════════════════════════════════════════
passes_enriched AS (
    SELECT
        pr.match_id,
        pr.team_id,
        pr.passer_id                    AS player_id,
        pr.season,
        pr.league_source,
        ie.period,
        CASE
            WHEN ie.team_score > ie.opp_score THEN 'winning'
            WHEN ie.team_score < ie.opp_score THEN 'losing'
            ELSE                                   'drawing'
        END                             AS score_state,
        fi.formation_code               AS formation,
        pr.x,
        pr.y,
        pr.end_x,
        pr.end_y
    FROM in_player_passes_raw pr
    JOIN in_int_event_enriched ie
        ON  ie.match_id = pr.match_id
        AND ie.row_num  = pr.row_num
    LEFT JOIN formation_intervals fi
        ON  fi.match_id = pr.match_id
        AND fi.team_id  = pr.team_id
        AND fi.row_num  = pr.row_num
    WHERE pr.match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TAKEONS_RAW
-- TakeOns réussis avec end_x/end_y via LEAD sur player_id.
-- ══════════════════════════════════════════════════════════════════════════════
takeons_raw AS (
    SELECT
        match_id,
        team_id,
        player_id,
        row_num,
        period,
        season,
        league_source,
        team_score,
        opp_score,
        x,
        y,
        LEAD(x) OVER (
            PARTITION BY match_id, player_id
            ORDER BY row_num
        )                               AS end_x,
        LEAD(y) OVER (
            PARTITION BY match_id, player_id
            ORDER BY row_num
        )                               AS end_y
    FROM in_int_event_enriched
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id    = 3
      AND outcome_id = 1
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TAKEONS_ENRICHED
-- TakeOns avec end_x/end_y valides + formation active.
-- ══════════════════════════════════════════════════════════════════════════════
takeons_enriched AS (
    SELECT
        tr.match_id,
        tr.team_id,
        tr.player_id,
        tr.season,
        tr.league_source,
        tr.period,
        CASE
            WHEN tr.team_score > tr.opp_score THEN 'winning'
            WHEN tr.team_score < tr.opp_score THEN 'losing'
            ELSE                                   'drawing'
        END                             AS score_state,
        fi.formation_code               AS formation,
        tr.x,
        tr.y,
        tr.end_x,
        tr.end_y
    FROM takeons_raw tr
    LEFT JOIN formation_intervals fi
        ON  fi.match_id = tr.match_id
        AND fi.team_id  = tr.team_id
        AND fi.row_num  = tr.row_num
    WHERE tr.end_x IS NOT NULL
      AND tr.end_y IS NOT NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TRANSITIONS_RAW
-- UNION ALL passes + takeons avec calcul des zones.
-- ══════════════════════════════════════════════════════════════════════════════
transitions_raw AS (
    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        CASE
            WHEN y < 33.3 THEN 'A'
            WHEN y < 66.6 THEN 'B'
            ELSE               'C'
        END ||
        CASE
            WHEN x >= 80  THEN '1'
            WHEN x >= 60  THEN '2'
            WHEN x >= 40  THEN '3'
            WHEN x >= 20  THEN '4'
            ELSE               '5'
        END                             AS zone_from,
        CASE
            WHEN end_y < 33.3 THEN 'A'
            WHEN end_y < 66.6 THEN 'B'
            ELSE                   'C'
        END ||
        CASE
            WHEN end_x >= 80  THEN '1'
            WHEN end_x >= 60  THEN '2'
            WHEN end_x >= 40  THEN '3'
            WHEN end_x >= 20  THEN '4'
            ELSE                   '5'
        END                             AS zone_to,
        CASE WHEN end_x > x + 10 THEN 1 ELSE 0 END AS is_progressive
    FROM passes_enriched

    UNION ALL

    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        CASE
            WHEN y < 33.3 THEN 'A'
            WHEN y < 66.6 THEN 'B'
            ELSE               'C'
        END ||
        CASE
            WHEN x >= 80  THEN '1'
            WHEN x >= 60  THEN '2'
            WHEN x >= 40  THEN '3'
            WHEN x >= 20  THEN '4'
            ELSE               '5'
        END                             AS zone_from,
        CASE
            WHEN end_y < 33.3 THEN 'A'
            WHEN end_y < 66.6 THEN 'B'
            ELSE                   'C'
        END ||
        CASE
            WHEN end_x >= 80  THEN '1'
            WHEN end_x >= 60  THEN '2'
            WHEN end_x >= 40  THEN '3'
            WHEN end_x >= 20  THEN '4'
            ELSE                   '5'
        END                             AS zone_to,
        CASE WHEN end_x > x + 10 THEN 1 ELSE 0 END AS is_progressive
    FROM takeons_enriched
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TRANSITIONS_AGG
-- Agrégation par (match_id, team_id, player_id, zone_from, zone_to,
--                 period, score_state, formation).
-- ══════════════════════════════════════════════════════════════════════════════
transitions_agg AS (
    SELECT
        match_id,
        team_id,
        player_id,
        season,
        league_source,
        period,
        score_state,
        formation,
        zone_from,
        zone_to,
        COUNT(*)                                        AS n_transitions,
        ROUND(
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (
                PARTITION BY match_id, team_id, player_id,
                             period, score_state, formation
            ), 4
        )                                               AS pct_transitions,
        ROUND(
            SUM(is_progressive) * 1.0 / COUNT(*), 4
        )                                               AS progressive_rate
    FROM transitions_raw
    GROUP BY
        match_id, team_id, player_id, season, league_source,
        period, score_state, formation,
        zone_from, zone_to
)

SELECT * FROM transitions_agg
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "period"                                                     AS int_period,
        "score_state"                                                AS str_score_state,
        "formation"                                                  AS str_formation,
        "zone_from"                                                  AS str_zone_from,
        "zone_to"                                                    AS str_zone_to,
        "n_transitions"                                              AS int_n_transitions,
        "pct_transitions"                                            AS dec_pct_transitions,
        "progressive_rate"                                           AS dec_progressive_rate
    FROM mdl_body
)

SELECT * FROM mdl_out
