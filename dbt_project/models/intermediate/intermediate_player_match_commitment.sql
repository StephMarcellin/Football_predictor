{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_player_match_commitment'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_player_match_commitment
-- Grain : UNE LIGNE PAR JOUEUR ET PAR MATCH (joueurs ayant au moins une action défensive de sacrifice).
--
-- Répond à : « ce joueur se sacrifie-t-il pour son équipe ? » — tirs bloqués, passes et centres bloqués,
-- actions de dernier homme, actions défensives dans sa propre surface.
--
-- Sources (WhoScored) :
--   · tir bloqué par un joueur de champ : événement Save portant le qualifier OutfielderBlock
--     (18 301 en 2023-24, toutes ligues) ; le joueur de l'événement est le bloqueur ;
--   · passe bloquée : événement BlockedPass (41 749 en 2023-24) ;
--   · centre bloqué : événement Clearance portant le qualifier BlockedCross (7 660) ;
--   · dernier homme : qualifier LastMan sur Interception, Save ou Tackle (1 754 en 2023-24, soit moins
--     d'une action par joueur et par saison : trait à vérifier en stabilité avant usage) ;
--   · actions défensives : Tackle, Interception, Clearance, BlockedPass et tirs bloqués ; dans la surface
--     propre = x <= 17 et 20 <= y <= 80 (repère de l'équipe, surface de réparation 16,5 m x 40,3 m sur un
--     terrain 105 x 68, converti en 0-100). Seuils posés par hypothèse.
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

def_events AS (
    SELECT e.str_match_id, e.str_team_id, e.str_player_id, e.int_row_num, e.str_type_name, e.dec_x, e.dec_y
    FROM {{ ref('intermediate_whoscored_events') }} e
    WHERE e.str_type_name IN ('Tackle', 'Interception', 'Clearance', 'BlockedPass')
      AND e.str_player_id IS NOT NULL
      {%- if var('only_season', none) %}
      AND e.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND e.str_match_id IN (SELECT str_match_id FROM new_matches)
),

qual AS (
    SELECT DISTINCT q.str_match_id, q.str_team_id, q.str_player_id, q.int_row_num, q.str_type_name,
                    q.str_qual_type_name, q.dec_x, q.dec_y
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }} q
    WHERE q.str_qual_type_name IN ('OutfielderBlock', 'BlockedCross', 'LastMan')
      AND q.str_player_id IS NOT NULL
      {%- if var('only_season', none) %}
      AND q.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND q.str_match_id IN (SELECT str_match_id FROM new_matches)
),

-- une ligne par action défensive (tir bloqué compris), sans doublon d'événement
actions AS (
    SELECT str_match_id, str_team_id, str_player_id, int_row_num, dec_x, dec_y, TRUE AS is_def_action
    FROM def_events
    UNION
    SELECT str_match_id, str_team_id, str_player_id, int_row_num, dec_x, dec_y, TRUE
    FROM qual WHERE str_qual_type_name = 'OutfielderBlock'
),

action_agg AS (
    SELECT str_match_id, str_team_id, str_player_id,
        COUNT(*)                                                                         AS int_def_actions,
        COUNT(*) FILTER (WHERE dec_x <= 17 AND dec_y BETWEEN 20 AND 80)                  AS int_def_actions_own_box
    FROM actions
    GROUP BY 1, 2, 3
),

sacrifice_agg AS (
    SELECT str_match_id, str_team_id, str_player_id,
        COUNT(*) FILTER (WHERE str_qual_type_name = 'OutfielderBlock')                   AS int_shots_blocked,
        COUNT(*) FILTER (WHERE str_qual_type_name = 'BlockedCross')                      AS int_crosses_blocked,
        COUNT(*) FILTER (WHERE str_qual_type_name = 'LastMan')                           AS int_last_man_actions
    FROM qual
    GROUP BY 1, 2, 3
),

passes_blocked AS (
    SELECT str_match_id, str_team_id, str_player_id, COUNT(*) AS int_passes_blocked
    FROM def_events WHERE str_type_name = 'BlockedPass'
    GROUP BY 1, 2, 3
)

SELECT
    a.str_match_id,
    a.str_team_id,
    a.str_player_id,
    a.int_def_actions,
    a.int_def_actions_own_box,
    COALESCE(s.int_shots_blocked, 0)    AS int_shots_blocked,
    COALESCE(p.int_passes_blocked, 0)   AS int_passes_blocked,
    COALESCE(s.int_crosses_blocked, 0)  AS int_crosses_blocked,
    COALESCE(s.int_last_man_actions, 0) AS int_last_man_actions
FROM action_agg a
LEFT JOIN sacrifice_agg s USING (str_match_id, str_team_id, str_player_id)
LEFT JOIN passes_blocked p USING (str_match_id, str_team_id, str_player_id)
