{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_player_match_finishing'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_player_match_finishing
-- Grain : UNE LIGNE PAR JOUEUR ET PAR MATCH (joueurs ayant tiré ou tiré un coup de pied arrêté).
--
-- Répond à : « comment ce joueur tire-t-il ? » — volume, cadrage, buts, lieu (surface ou loin), tête, grosses
-- occasions, distance moyenne ; et « tire-t-il les coups de pied arrêtés ? » — corners, coups francs, penaltys.
--
-- Périmètre des tirs : événements Goal, SavedShot, MissedShots, ShotOnPost du joueur ; penaltys (qualifier Penalty)
-- et csc (qualifier OwnGoal) écartés des tirs de jeu. Un tir bloqué est un SavedShot avec le qualifier Blocked
-- (le tireur est crédité du tir, il n'est pas cadré). Cadré = Goal ou SavedShot non bloqué.
-- Dans la surface : x >= 84 et 20 <= y <= 80 (repère de l'équipe, 0-100 ; surface 16,5 m x 40,3 m).
-- Distance au but : mètres, terrain 105 x 68, but en (100, 50). Seuils posés par hypothèse.
--
-- Coups de pied arrêtés tirés : Pass portant CornerTaken (corners), FreekickTaken (coups francs joués), tirs portant
-- DirectFreekick (coups francs directs tirés), tirs portant Penalty (penaltys tirés, séances de tirs au but comprises
-- si la source les code ainsi : à vérifier).
-- ══════════════════════════════════════════════════════════════════════════════

WITH

{% if var('only_season', none) %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_whoscored_events') }}
    WHERE str_match_id IN ({{ season_match_ids(this) }})
    {%- if is_incremental() %}
      AND str_match_id NOT IN (SELECT DISTINCT str_match_id FROM {{ this }})
    {%- endif %}
),
{% elif is_incremental() %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_whoscored_events') }}
    WHERE str_match_id NOT IN (SELECT DISTINCT str_match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_whoscored_events') }}
),
{% endif %}

shots AS (
    SELECT DISTINCT e.str_match_id, e.str_team_id, e.str_player_id, e.int_row_num, e.str_type_name, e.dec_x, e.dec_y
    FROM {{ ref('intermediate_whoscored_events') }} e
    WHERE e.str_type_name IN ('Goal', 'SavedShot', 'MissedShots', 'ShotOnPost')
      AND e.str_player_id IS NOT NULL
      {%- if var('only_season', none) %}
      AND e.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND e.str_match_id IN (SELECT str_match_id FROM new_matches)
),

qual AS (
    SELECT q.str_match_id, q.int_row_num, q.str_type_name,
        BOOL_OR(q.str_qual_type_name = 'Blocked')        AS is_blocked,
        BOOL_OR(q.str_qual_type_name = 'Head')           AS is_head,
        BOOL_OR(q.str_qual_type_name = 'BigChance')      AS is_big_chance,
        BOOL_OR(q.str_qual_type_name = 'Penalty')        AS is_penalty,
        BOOL_OR(q.str_qual_type_name = 'OwnGoal')        AS is_own_goal,
        BOOL_OR(q.str_qual_type_name = 'DirectFreekick') AS is_direct_freekick,
        BOOL_OR(q.str_qual_type_name = 'CornerTaken')    AS is_corner_taken,
        BOOL_OR(q.str_qual_type_name = 'FreekickTaken')  AS is_freekick_taken
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }} q
    WHERE q.str_qual_type_name IN ('Blocked', 'Head', 'BigChance', 'Penalty', 'OwnGoal', 'DirectFreekick',
                                   'CornerTaken', 'FreekickTaken')
      {%- if var('only_season', none) %}
      AND q.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND q.str_match_id IN (SELECT str_match_id FROM new_matches)
    GROUP BY 1, 2, 3
),

shot_flags AS (
    SELECT s.*,
        COALESCE(q.is_blocked, FALSE)        AS is_blocked,
        COALESCE(q.is_head, FALSE)           AS is_head,
        COALESCE(q.is_big_chance, FALSE)     AS is_big_chance,
        COALESCE(q.is_penalty, FALSE)        AS is_penalty,
        COALESCE(q.is_own_goal, FALSE)       AS is_own_goal,
        COALESCE(q.is_direct_freekick, FALSE) AS is_direct_freekick,
        SQRT(POWER((100 - s.dec_x) * 1.05, 2) + POWER((50 - s.dec_y) * 0.68, 2)) AS dist_m
    FROM shots s
    LEFT JOIN qual q ON q.str_match_id = s.str_match_id AND q.int_row_num = s.int_row_num AND q.str_type_name = s.str_type_name
),

shot_agg AS (
    SELECT str_match_id, str_team_id, str_player_id,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal)                                           AS int_shots_open,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal
                          AND (str_type_name = 'Goal' OR (str_type_name = 'SavedShot' AND NOT is_blocked))) AS int_shots_on_target,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND str_type_name = 'Goal')                AS int_goals_open,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND is_blocked)                            AS int_shots_blocked_by_def,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND is_head)                               AS int_shots_head,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND dec_x >= 84 AND dec_y BETWEEN 20 AND 80) AS int_shots_in_box,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND NOT (dec_x >= 84 AND dec_y BETWEEN 20 AND 80)) AS int_shots_outside_box,
        COUNT(*) FILTER (WHERE NOT is_penalty AND NOT is_own_goal AND is_big_chance)                         AS int_shots_big_chance,
        AVG(dist_m) FILTER (WHERE NOT is_penalty AND NOT is_own_goal)                                        AS dec_shot_dist_mean,
        COUNT(*) FILTER (WHERE is_direct_freekick AND NOT is_own_goal)                                       AS int_freekicks_direct_shots,
        COUNT(*) FILTER (WHERE is_penalty AND NOT is_own_goal)                                               AS int_penalties_taken
    FROM shot_flags
    GROUP BY 1, 2, 3
),

set_piece AS (
    SELECT p.str_match_id, p.str_team_id, p.str_player_id,
        COUNT(*) FILTER (WHERE q.is_corner_taken)   AS int_corners_taken,
        COUNT(*) FILTER (WHERE q.is_freekick_taken) AS int_freekicks_played
    FROM (
        SELECT DISTINCT e.str_match_id, e.str_team_id, e.str_player_id, e.int_row_num
        FROM {{ ref('intermediate_whoscored_events') }} e
        WHERE e.str_type_name = 'Pass' AND e.str_player_id IS NOT NULL
          {%- if var('only_season', none) %}
          AND e.str_season = '{{ var("only_season") }}'
          {%- endif %}
          AND e.str_match_id IN (SELECT str_match_id FROM new_matches)
    ) p
    JOIN qual q ON q.str_match_id = p.str_match_id AND q.int_row_num = p.int_row_num AND q.str_type_name = 'Pass'
    WHERE q.is_corner_taken OR q.is_freekick_taken
    GROUP BY 1, 2, 3
)

SELECT
    COALESCE(s.str_match_id, c.str_match_id)   AS str_match_id,
    COALESCE(s.str_team_id, c.str_team_id)     AS str_team_id,
    COALESCE(s.str_player_id, c.str_player_id) AS str_player_id,
    COALESCE(s.int_shots_open, 0)              AS int_shots_open,
    COALESCE(s.int_shots_on_target, 0)         AS int_shots_on_target,
    COALESCE(s.int_goals_open, 0)              AS int_goals_open,
    COALESCE(s.int_shots_blocked_by_def, 0)    AS int_shots_blocked_by_def,
    COALESCE(s.int_shots_head, 0)              AS int_shots_head,
    COALESCE(s.int_shots_in_box, 0)            AS int_shots_in_box,
    COALESCE(s.int_shots_outside_box, 0)       AS int_shots_outside_box,
    COALESCE(s.int_shots_big_chance, 0)        AS int_shots_big_chance,
    s.dec_shot_dist_mean,
    COALESCE(s.int_freekicks_direct_shots, 0)  AS int_freekicks_direct_shots,
    COALESCE(s.int_penalties_taken, 0)         AS int_penalties_taken,
    COALESCE(c.int_corners_taken, 0)           AS int_corners_taken,
    COALESCE(c.int_freekicks_played, 0)        AS int_freekicks_played
FROM shot_agg s
FULL OUTER JOIN set_piece c USING (str_match_id, str_team_id, str_player_id)
