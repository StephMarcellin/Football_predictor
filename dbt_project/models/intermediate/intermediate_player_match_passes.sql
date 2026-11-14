{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_player_match_passes'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_player_match_passes
-- Grain : UNE LIGNE PAR JOUEUR ET PAR MATCH (joueurs ayant tenté au moins une passe).
--
-- Répond à : « comment ce joueur distribue-t-il le ballon ? » — volume, précision,
-- longueur, direction, zone de départ, entrée dans le dernier tiers, xT créé.
--
-- Périmètre : passes de jeu ouvert. Les passes de remise en jeu (touche, corner, coup franc,
-- coup franc indirect, six mètres, relance main du gardien) sont comptées à part
-- (int_passes_set_piece) et exclues des taux : elles décrivent le contexte, pas le joueur.
--
-- Repère : coordonnées WhoScored 0-100, repère de l'équipe (x croît vers le but adverse).
-- Longueur en mètres sur un terrain 105 × 68 m. Passe vers l'avant : gain en x ≥ 5 m ;
-- vers l'arrière : perte en x ≥ 5 m ; sinon latérale. Courte < 15 m, longue ≥ 30 m.
-- Tiers de départ : x < 33,3 / 33,3 à 66,7 / ≥ 66,7. Seuils posés par hypothèse, à valider.
--
-- xT : somme de intermediate_xt_contributions sur les passes réussies du joueur
-- (dec_xt_created) ; les déplacements qui ne sont pas des passes (dribbles) sont
-- comptés à part (dec_xt_other_moves).
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

passes AS (
    SELECT
        e.str_match_id,
        e.str_team_id,
        e.str_player_id,
        e.int_row_num,
        (e.str_outcome_id = '1')                          AS is_completed,
        e.dec_x,
        (e.dec_end_x - e.dec_x) * 1.05                    AS dx_m,
        SQRT(POWER((e.dec_end_x - e.dec_x) * 1.05, 2)
           + POWER((e.dec_end_y - e.dec_y) * 0.68, 2))    AS length_m,
        e.dec_end_x
    FROM {{ ref('intermediate_whoscored_events') }} e
    WHERE e.str_type_name = 'Pass'
      {%- if var('only_season', none) %}
      AND e.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND e.str_player_id IS NOT NULL
      AND e.dec_end_x IS NOT NULL AND e.dec_end_y IS NOT NULL
      AND e.str_match_id IN (SELECT str_match_id FROM new_matches)
),

set_piece AS (
    SELECT DISTINCT q.str_match_id, q.int_row_num
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }} q
    WHERE q.str_type_name = 'Pass'
      AND q.str_qual_type_name IN ('ThrowIn', 'FreekickTaken', 'CornerTaken', 'GoalKick',
                                   'KeeperThrow', 'IndirectFreekickTaken')
      {%- if var('only_season', none) %}
      AND q.str_season = '{{ var("only_season") }}'
      {%- endif %}
      AND q.str_match_id IN (SELECT str_match_id FROM new_matches)
),

xt AS (
    SELECT str_match_id, int_row_num, dec_xt_added
    FROM {{ ref('intermediate_xt_contributions') }}
    WHERE str_match_id IN (SELECT str_match_id FROM new_matches)
      {%- if var('only_season', none) %}
      AND str_season = '{{ var("only_season") }}'
      {%- endif %}
),

flagged AS (
    SELECT
        p.*,
        (s.int_row_num IS NOT NULL) AS is_set_piece
    FROM passes p
    LEFT JOIN set_piece s
      ON s.str_match_id = p.str_match_id AND s.int_row_num = p.int_row_num
),

pass_agg AS (
    SELECT
        str_match_id, str_team_id, str_player_id,
        COUNT(*) FILTER (WHERE is_set_piece)                                         AS int_passes_set_piece,
        COUNT(*) FILTER (WHERE NOT is_set_piece)                                     AS int_passes_open,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND is_completed)                    AS int_passes_open_completed,
        AVG(length_m) FILTER (WHERE NOT is_set_piece)                                AS dec_pass_length_mean,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND length_m < 15)                   AS int_passes_short,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND length_m >= 30)                  AS int_passes_long,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND length_m >= 30 AND is_completed) AS int_passes_long_completed,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dx_m >= 5)                       AS int_passes_forward,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dx_m >= 5 AND is_completed)      AS int_passes_forward_completed,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dx_m <= -5)                      AS int_passes_backward,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dec_x < 33.3)                    AS int_passes_from_own_third,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dec_x >= 33.3 AND dec_x < 66.7)  AS int_passes_from_middle_third,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND dec_x >= 66.7)                   AS int_passes_from_attacking_third,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND is_completed
                          AND dec_x < 66.7 AND dec_end_x >= 66.7)                    AS int_passes_into_final_third,
        COUNT(*) FILTER (WHERE NOT is_set_piece AND NOT is_completed)                AS int_passes_open_lost
    FROM flagged
    GROUP BY 1, 2, 3
),

xt_agg AS (
    SELECT
        x.str_match_id,
        ev.str_team_id,
        ev.str_player_id,
        SUM(x.dec_xt_added)                                  AS dec_xt_created,
        SUM(GREATEST(x.dec_xt_added, 0))                     AS dec_xt_created_positive
    FROM xt x
    JOIN passes ev ON ev.str_match_id = x.str_match_id AND ev.int_row_num = x.int_row_num
    GROUP BY 1, 2, 3
)

SELECT
    a.str_match_id,
    a.str_team_id,
    a.str_player_id,
    a.int_passes_open,
    a.int_passes_open_completed,
    a.int_passes_open_lost,
    a.int_passes_set_piece,
    a.dec_pass_length_mean,
    a.int_passes_short,
    a.int_passes_long,
    a.int_passes_long_completed,
    a.int_passes_forward,
    a.int_passes_forward_completed,
    a.int_passes_backward,
    a.int_passes_from_own_third,
    a.int_passes_from_middle_third,
    a.int_passes_from_attacking_third,
    a.int_passes_into_final_third,
    COALESCE(x.dec_xt_created, 0)                            AS dec_xt_created,
    COALESCE(x.dec_xt_created_positive, 0)                   AS dec_xt_created_positive
FROM pass_agg a
LEFT JOIN xt_agg x
  ON x.str_match_id = a.str_match_id AND x.str_team_id = a.str_team_id AND x.str_player_id = a.str_player_id
