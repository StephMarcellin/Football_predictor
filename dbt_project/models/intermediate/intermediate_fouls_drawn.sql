{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='intermediate_fouls_drawn'
    )
}}

WITH

-- intermediate_whoscored_event_qualifiers lu sous ses noms refondus
in_events_qual AS (
    SELECT
        str_match_id                                             AS "match_id",
        CAST(str_team_id AS BIGINT)                              AS "team_id",
        CAST(str_player_id AS INTEGER)                           AS "player_id",
        CAST(str_event_id AS INTEGER)                            AS "event_id",
        int_minute                                               AS "minute",
        int_second                                               AS "second",
        int_expanded_minute                                      AS "expanded_minute",
        int_period                                               AS "period",
        dec_x                                                    AS "x",
        dec_y                                                    AS "y",
        dec_end_x                                                AS "end_x",
        dec_end_y                                                AS "end_y",
        CAST(str_type_id AS INTEGER)                             AS "type_id",
        str_type_name                                            AS "type_name",
        CAST(str_outcome_id AS INTEGER)                          AS "outcome_id",
        bool_is_touch                                            AS "is_touch",
        bool_is_shot                                             AS "is_shot",
        int_row_num                                              AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                        AS "qual_type_id",
        str_qual_type_name                                       AS "qual_type_name",
        str_qual_value                                           AS "qual_value"
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
),

-- intermediate_whoscored_events lu sous ses noms refondus
in_int_whoscored_events AS (
    SELECT
        str_match_id                                             AS "match_id",
        CAST(str_team_id AS BIGINT)                              AS "team_id",
        CAST(str_event_id AS INTEGER)                            AS "event_id",
        str_league_source                                        AS "league_source",
        str_season                                               AS "season",
        int_minute                                               AS "minute",
        int_second                                               AS "second",
        int_expanded_minute                                      AS "expanded_minute",
        int_period                                               AS "period",
        CAST(str_player_id AS INTEGER)                           AS "player_id",
        dec_x                                                    AS "x",
        dec_y                                                    AS "y",
        dec_end_x                                                AS "end_x",
        dec_end_y                                                AS "end_y",
        CAST(str_type_id AS INTEGER)                             AS "type_id",
        str_type_name                                            AS "type_name",
        CAST(str_outcome_id AS INTEGER)                          AS "outcome_id",
        str_outcome_name                                         AS "outcome_name",
        bool_is_touch                                            AS "is_touch",
        bool_is_shot                                             AS "is_shot",
        CAST(dt_scraped_at AS VARCHAR)                           AS "scraped_at",
        int_row_num                                              AS "row_num",
        bool_is_goal                                             AS "is_goal",
        bool_is_own_goal                                         AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                    AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                   AS "related_player_id",
        str_card_type                                            AS "card_type",
        dec_goal_mouth_y                                         AS "goal_mouth_y",
        dec_goal_mouth_z                                         AS "goal_mouth_z",
        dec_blocked_x                                            AS "blocked_x",
        dec_blocked_y                                            AS "blocked_y"
    FROM {{ ref('intermediate_whoscored_events') }}
),

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM (
        SELECT CAST(dt_scraped_at AS VARCHAR) AS "scraped_at"
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

-- ── Qualifiers utiles portés par la faute subie ───────────────────────────────
foul_quals AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 233 THEN TRY_CAST(qual_value AS INTEGER) END) AS opposite_event_id,
        MAX(CASE WHEN qual_type_id = 56  THEN qual_value END)                      AS foul_zone,
        MAX(CASE WHEN qual_type_id = 9   THEN 1 ELSE 0 END)                        AS is_penalty,
        MAX(player_id)                                                             AS drawer_player_id,
        MAX(team_id)                                                               AS drawing_team_id
    FROM in_events_qual
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id = 4
      AND outcome_id = 1
    GROUP BY match_id, row_num
),

-- ── Fautes commises → fautif, dédoublonnées par (match, event_id, team_id) ────
committed AS (
    SELECT
        match_id,
        event_id,
        team_id        AS committed_by_team_id,
        MAX(player_id) AS committed_by_player_id
    FROM in_int_whoscored_events
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id = 4
      AND outcome_id = 0
    GROUP BY match_id, event_id, team_id
),

-- ── Fautes subies (pivot), dédoublonnées par (match, row_num) ─────────────────
drawn AS (
    SELECT
        fq.match_id,
        fq.row_num,
        fq.opposite_event_id,
        fq.foul_zone,
        COALESCE(fq.is_penalty, 0)      AS is_penalty,
        fq.drawer_player_id,
        fq.drawing_team_id,

        e.event_id,
        e.season,
        e.league_source,
        e.scraped_at,
        e.expanded_minute,
        e.x,
        e.y
    FROM in_int_whoscored_events e
    LEFT JOIN foul_quals fq
        ON fq.match_id = e.match_id
       AND fq.row_num  = e.row_num
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num
                               ORDER BY e.event_id) = 1
),

-- ── ASSEMBLAGE ET RENOMMAGE FINAL ─────────────────────────────────────────────
mdl_out AS (
    SELECT
        d.match_id                                                   AS str_match_id,
        d.row_num                                                    AS int_row_num,
        CAST(d.event_id AS VARCHAR)                                  AS str_event_id,
        d.season                                                     AS str_season,
        d.league_source                                              AS str_league_source,
        TRY_CAST(d.scraped_at AS TIMESTAMP)                          AS dt_scraped_at,
        d.expanded_minute                                            AS int_expanded_minute,
        CAST(d.drawing_team_id AS VARCHAR)                           AS str_drawing_team_id,
        CAST(d.drawer_player_id AS VARCHAR)                          AS str_drawer_player_id,
        CAST(c.committed_by_player_id AS VARCHAR)                    AS str_committed_by_player_id,
        d.x                                                          AS dec_x,
        d.y                                                          AS dec_y,
        d.foul_zone                                                  AS str_foul_zone,
        CASE WHEN d.x IS NOT NULL THEN d.x > 66.7 END                AS bool_is_attacking_third,
        (d.is_penalty = 1)                                           AS bool_leads_to_penalty
    FROM drawn d
    LEFT JOIN committed c
        ON  c.match_id             = d.match_id
        AND c.event_id             = d.opposite_event_id
        AND c.committed_by_team_id <> d.drawing_team_id
    WHERE d.drawer_player_id IS NOT NULL
)

SELECT * FROM mdl_out