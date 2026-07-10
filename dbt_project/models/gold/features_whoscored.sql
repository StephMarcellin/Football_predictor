{{
    config(
        materialized='incremental',
        unique_key=['date', 'team', 'league_source'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_whoscored'
    )
}}

{% set ws_cols = [
    'ws_field_tilt_actions', 'ws_high_turnover_rate', 'ws_deep_completion_rt',
    'ws_momentum_delta', 'ws_counter_shot_rate', 'ws_set_piece_pressure',
    'ws_attack_left_pct', 'ws_attack_center_pct', 'ws_attack_right_pct',
    'ws_zone_def_pct', 'ws_zone_mid_pct', 'ws_zone_att_pct',
    'ws_shot_six_yard_pct', 'ws_shot_penalty_pct', 'ws_shot_oob_pct',
    'ws_shot_open_play_pct', 'ws_shot_set_piece_pct', 'ws_shot_penalty_att_pct',
    'ws_conversion_rate', 'ws_cross_rate', 'ws_through_ball_rate',
    'ws_long_ball_rate', 'ws_short_pass_rate',
    'ws_def_exposed_left_pct', 'ws_def_exposed_center_pct', 'ws_def_exposed_right_pct',
    'ws_counter_attack_dna', 'ws_midfield_control_idx',
    'ws_defensive_line_height', 'ws_flank_exposure_asymm'
] %}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- NOUVEAUX MATCHS (filtre incremental)
-- ══════════════════════════════════════════════════════════════════════════════
new_match_ids AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_whoscored_match_index') }}
    {% if is_incremental() %}
    WHERE match_id NOT IN (
        SELECT DISTINCT match_id FROM {{ this }}
    )
    {% endif %}
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3A — Pivot home/away depuis team_features_ws
-- ══════════════════════════════════════════════════════════════════════════════
ws_history AS (
    SELECT
        f.match_id,
        f.team_id,
        m.match_date,
        m.season,
        m.league_source,
        {% for col in ws_cols %}
        f.{{ col }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('team_features_ws') }} f
    JOIN {{ ref('int_whoscored_match_index') }} m
        ON f.match_id = m.match_id
    WHERE f.match_id IN (SELECT match_id FROM new_match_ids)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3B — Anti-leakage LAG(1) : jointure avec backbone
-- Pour chaque match dans backbone, on prend le dernier match WhoScored
-- STRICTEMENT ANTÉRIEUR à la date du match (pas de data leakage)
-- ══════════════════════════════════════════════════════════════════════════════
backbone_base AS (
    SELECT date, team_id, league_source, season, match_id
    FROM {{ ref('backbone') }}
),
latest_ws AS (
    SELECT
        *,
        LAG(match_id) OVER (
            PARTITION BY team_id, league_source, season
            ORDER BY match_date
        ) AS prev_match_id
    FROM ws_history
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSE 3D — Squad features rolling 5 matchs
-- ══════════════════════════════════════════════════════════════════════════════
squad_current AS (
    SELECT
        pms.match_id, pms.team_id, pms.player_id,
        pms.date AS match_date,
        pms.season, pms.league_source,
        pms.n_actions, pms.xg_contribution
    FROM {{ ref('player_match_stats') }} pms
    WHERE pms.player_id IS NOT NULL
),

player_form AS (
    SELECT
        cur.match_id, cur.team_id, cur.player_id,
        cur.match_date, cur.league_source,cur.season,
        AVG(hist.n_actions)        AS player_avg_actions_5,
        AVG(hist.xg_contribution)  AS player_avg_xg_5,
        COUNT(hist.match_id)    AS n_prev_matches
    FROM squad_current cur
    LEFT JOIN intermediate.player_match_stats hist
        ON  hist.player_id    = cur.player_id
        AND hist.league_source = cur.league_source
        AND hist.date   < cur.match_date
        AND hist.date  >= cur.match_date - INTERVAL '180 days'
    GROUP BY cur.match_id, cur.team_id, cur.player_id, cur.match_date, cur.league_source, cur.season
    HAVING COUNT(hist.match_id) >= 1
),

squad_regularity AS (
    SELECT cur.match_id, cur.team_id,
        COUNT(DISTINCT cur.player_id) AS squad_size,
        CASE WHEN COUNT(DISTINCT cur.player_id) > 0
             THEN CAST(COUNT(DISTINCT prev.player_id) AS DOUBLE) / COUNT(DISTINCT cur.player_id)
             ELSE NULL END AS squad_reg_rate
    FROM squad_current cur
    LEFT JOIN (
        SELECT player_id, team_id, date AS prev_date, league_source
        FROM intermediate.player_match_stats
        WHERE player_id IS NOT NULL
    ) prev
        ON  prev.player_id    = cur.player_id
        AND prev.team_id      = cur.team_id
        AND prev.league_source = cur.league_source
        AND prev.prev_date    < cur.match_date
        AND prev.prev_date   >= cur.match_date - INTERVAL '14 days'
    GROUP BY cur.match_id, cur.team_id
),

squad_top3 AS (
    SELECT match_id, team_id,
        CASE WHEN MAX(total_actions) > 0
             THEN SUM(n_actions) FILTER (WHERE rk <= 3) / MAX(total_actions)
             ELSE NULL END AS squad_top3_share
    FROM (
        SELECT match_id, team_id, player_id, n_actions,
            ROW_NUMBER() OVER (PARTITION BY match_id, team_id ORDER BY n_actions DESC) AS rk,
            SUM(n_actions) OVER (PARTITION BY match_id, team_id) AS total_actions
        FROM squad_current
    ) ranked
    GROUP BY match_id, team_id
),

squad_agg AS (
    SELECT
        pf.match_id, pf.team_id, pf.match_date, pf.league_source, pf.season,
        AVG(pf.player_avg_actions_5) AS squad_avg_form_5,
        AVG(pf.player_avg_xg_5)      AS squad_xg_quality_5,
        sr.squad_reg_rate,
        t3.squad_top3_share
    FROM player_form pf
    LEFT JOIN squad_regularity sr ON pf.match_id=sr.match_id AND pf.team_id=sr.team_id
    LEFT JOIN squad_top3       t3 ON pf.match_id=t3.match_id AND pf.team_id=t3.team_id
    GROUP BY pf.match_id, pf.team_id, pf.match_date, pf.league_source, pf.season,
             sr.squad_reg_rate, t3.squad_top3_share
),

squad_for_backbone AS (
    SELECT
        sq.match_id,
        sq.team_id,
        LAG(sq.match_id) OVER (
            PARTITION BY sq.team_id, sq.league_source, sq.season
            ORDER BY sq.match_date
        ) AS prev_match_id,
        sa.squad_avg_form_5,
        sa.squad_xg_quality_5,
        sa.squad_reg_rate,
        sa.squad_top3_share
    FROM squad_agg sa
    JOIN {{ ref('int_whoscored_match_index') }} sq
        ON sa.match_id = sq.match_id
        AND sa.team_id = sq.team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- ASSEMBLAGE FINAL
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        b.date, b.team_id, b.league_source,b.match_id,

        -- Features WhoScored match (anti-leakage LAG1)
        {% for col in ws_cols %}
        lws.{{ col }},
        {% endfor %}
        CASE WHEN lws.ws_field_tilt_actions IS NOT NULL THEN 1 ELSE 0 END AS has_ws_events,

        -- Squad features (anti-leakage)
        sfb.squad_avg_form_5,
        sfb.squad_xg_quality_5,
        sfb.squad_reg_rate as squad_regularity,
        sfb.squad_top3_share


    FROM backbone_base b

    LEFT JOIN latest_ws lws
	    ON  lws.match_id = b.match_id
	    AND lws.team_id   = b.team_id

	LEFT JOIN squad_for_backbone sfb
	    ON  sfb.match_id = b.match_id
	    AND sfb.team_id   = b.team_id
)

SELECT * FROM final

{% if is_incremental() %}
WHERE (date, team, league_source) NOT IN (
    SELECT date, team, league_source
    FROM {{ this }}
)
{% endif %}