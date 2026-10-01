{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='intermediate_penalties'
    )
}}

WITH

-- 1. Qualifiers (qual 9 = Penalty, qual 233 = OppositeRelatedEvent, 20 = Right Foot, 72 = Left Foot)
in_events_qual AS (
    SELECT
        str_match_id                                                 AS match_id,
        int_row_num                                                  AS row_num,
        CAST(str_qual_type_id AS INTEGER)                            AS qual_type_id,
        str_qual_value                                               AS qual_value
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
),

-- 2. Événements bruts
in_events AS (
    SELECT
        str_match_id                                                 AS match_id,
        CAST(str_team_id AS BIGINT)                                  AS team_id,
        CAST(str_player_id AS INTEGER)                               AS player_id,
        CAST(str_event_id AS INTEGER)                                AS event_id,
        int_row_num                                                  AS row_num,
        int_expanded_minute                                          AS expanded_minute,
        CAST(str_type_id AS INTEGER)                                 AS type_id,
        CAST(str_outcome_id AS INTEGER)                              AS outcome_id,
        dec_goal_mouth_y                                             AS goal_mouth_y,
        dec_goal_mouth_z                                             AS goal_mouth_z,
        str_season                                                   AS season,
        str_league_source                                            AS league_source,
        CAST(dt_scraped_at AS VARCHAR)                               AS scraped_at
    FROM {{ ref('intermediate_whoscored_events') }}
),

-- Filtre incrémental
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(CAST(dt_scraped_at AS VARCHAR)) AS last_scraped 
    FROM {{ this }}
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_events
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_events
),
{% endif %}

-- Identifiants des événements portant le qualifier « Penalty » (qual 9)
pen_ids AS (
    SELECT DISTINCT match_id, row_num
    FROM in_events_qual
    WHERE qual_type_id = 9
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- Pied du tireur (qual 20 = Right foot, qual 72 = Left foot)
pen_feet AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE
            WHEN qual_type_id = 20 THEN 'right'
            WHEN qual_type_id = 72 THEN 'left'
        END) AS taker_foot
    FROM in_events_qual
    WHERE qual_type_id IN (20, 72)
      AND match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, row_num
),

-- Lien OppositeRelatedEvent (qual 233) — Dédoublonné strict
q233 AS (
    SELECT 
        match_id, 
        row_num, 
        TRY_CAST(qual_value AS INTEGER) AS opposite_event_id
    FROM in_events_qual
    WHERE qual_type_id = 233
      AND match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, row_num, TRY_CAST(qual_value AS INTEGER)
),

-- PIVOT : Les tirs de penalty (types 13, 14, 15, 16 + qual 9)
pen_shots AS (
    SELECT
        e.match_id,
        e.event_id,
        e.season,
        e.league_source,
        e.scraped_at,
        e.row_num,
        e.expanded_minute,
        e.team_id                       AS attacking_team_id,
        e.player_id                     AS taker_player_id,
        pf.taker_foot,
        e.goal_mouth_y,
        e.goal_mouth_z,
        (e.type_id = 16)                AS is_goal,
        (e.type_id = 15)                AS is_saved,
        (e.type_id = 14)                AS is_post,
        (e.type_id = 13)                AS is_off_target
    FROM in_events e
    JOIN pen_ids p
        ON p.match_id = e.match_id
       AND p.row_num  = e.row_num
    LEFT JOIN pen_feet pf
        ON pf.match_id = e.match_id
       AND pf.row_num  = e.row_num
    WHERE e.type_id IN (13, 14, 15, 16)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num ORDER BY e.event_id) = 1
),

shots AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, attacking_team_id ORDER BY row_num) AS pen_seq
    FROM pen_shots
),

-- Face au penalty (type 58) — Dédoublonné sur (match_id, shot_event_id)
faced AS (
    SELECT
        e.match_id,
        q.opposite_event_id  AS shot_event_id,
        e.team_id            AS defending_team_id,
        e.player_id          AS gk_player_id
    FROM in_events e
    JOIN q233 q
        ON q.match_id = e.match_id
       AND q.row_num  = e.row_num
    WHERE e.type_id = 58
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, q.opposite_event_id ORDER BY e.row_num) = 1
),

-- Fautes obtenues / concédées — Dédoublonnées sur (match_id, won_team, pen_seq)
foul_pairs AS (
    SELECT
        o.match_id,
        o.won_team,
        o.pen_seq,
        o.player_drew,
        d.player_conceded,
        d.conceded_team            -- équipe du fautif, pour sa clé joueur_équipe_saison
    FROM (
        SELECT e.match_id, e.event_id,
               e.team_id AS won_team, e.player_id AS player_drew,
               ROW_NUMBER() OVER (PARTITION BY e.match_id, e.team_id ORDER BY e.row_num) AS pen_seq
        FROM in_events e
        JOIN pen_ids p ON p.match_id = e.match_id AND p.row_num = e.row_num
        WHERE e.type_id = 4 AND e.outcome_id = 1
    ) o
    LEFT JOIN (
        SELECT e.match_id, e.team_id AS conceded_team, e.player_id AS player_conceded,
               q.opposite_event_id
        FROM in_events e
        JOIN pen_ids p ON p.match_id = e.match_id AND p.row_num = e.row_num
        JOIN q233 q     ON q.match_id = e.match_id AND q.row_num = e.row_num
        WHERE e.type_id = 4 AND e.outcome_id = 0
    ) d
        ON  d.match_id          = o.match_id
        AND d.opposite_event_id = o.event_id
        AND d.conceded_team    <> o.won_team
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.match_id, o.won_team, o.pen_seq ORDER BY o.event_id) = 1
)

-- Sortie typée
SELECT
    s.match_id                                                   AS str_match_id,
    s.row_num                                                    AS int_row_num,
    s.season                                                     AS str_season,
    s.league_source                                              AS str_league_source,
    TRY_CAST(s.scraped_at AS TIMESTAMP)                          AS dt_scraped_at,
    s.expanded_minute                                            AS int_expanded_minute,
    CAST(s.attacking_team_id AS VARCHAR)                         AS str_attacking_team_id,
    CAST(s.taker_player_id AS VARCHAR)                           AS str_taker_player_id,
    -- Clés '<player_id>_<team_id>_<saison>' → intermediate_whoscored_player_team_season.
    -- Chaque joueur est associé à SON équipe. || : composante NULL → clé NULL.
    CAST(s.taker_player_id AS VARCHAR) || '_' || CAST(s.attacking_team_id AS VARCHAR) || '_' || s.season
                                                                 AS str_taker_player_id_team_season_key,
    s.taker_foot                                                 AS str_taker_foot,
    CAST(f.defending_team_id AS VARCHAR)                         AS str_defending_team_id,
    CAST(f.gk_player_id AS VARCHAR)                              AS str_gk_player_id,
    CAST(f.gk_player_id AS VARCHAR) || '_' || CAST(f.defending_team_id AS VARCHAR) || '_' || s.season
                                                                 AS str_gk_player_id_team_season_key,
    fp.player_drew                                               AS int_player_drew,
    fp.player_conceded                                           AS int_player_conceded,
    CAST(fp.player_drew AS VARCHAR) || '_' || CAST(fp.won_team AS VARCHAR) || '_' || s.season
                                                                 AS str_player_drew_team_season_key,
    CAST(fp.player_conceded AS VARCHAR) || '_' || CAST(fp.conceded_team AS VARCHAR) || '_' || s.season
                                                                 AS str_player_conceded_team_season_key,
    s.is_goal                                                    AS bool_is_goal,
    s.is_saved                                                   AS bool_is_saved,
    s.is_post                                                    AS bool_is_post,
    s.is_off_target                                              AS bool_is_off_target,
    CASE
        WHEN s.is_goal       THEN 'scored'
        WHEN s.is_saved      THEN 'saved'
        WHEN s.is_post       THEN 'post'
        ELSE 'off_target'
    END                                                          AS str_result_label,
    s.goal_mouth_y                                               AS dec_goal_mouth_y,
    s.goal_mouth_z                                               AS dec_goal_mouth_z
FROM shots s
LEFT JOIN faced f
    ON  f.match_id          = s.match_id
    AND f.shot_event_id     = s.event_id
    AND f.defending_team_id <> s.attacking_team_id
LEFT JOIN foul_pairs fp
    ON  fp.match_id = s.match_id
    AND fp.won_team = s.attacking_team_id
    AND fp.pen_seq  = s.pen_seq