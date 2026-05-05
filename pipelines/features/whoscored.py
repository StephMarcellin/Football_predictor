"""
features/whoscored.py — WhoScored Events → Gold Features
=========================================================
ARCHITECTURE INCRÉMENTALE + CORRECTIONS
─────────────────────────────────────────
  Passe 0 — player_match_stats  : INSERT incrémental sur ws_match_id
  Passe 1 — stg_events_qual     : INSERT incrémental (table Gold persistante, 32min → 0sec)
  Passe 2 — stg_team_features   : INSERT incrémental sur ws_match_id
  Passe 3 — Pivot + LAG(1)      : UPDATE idempotent (WHERE ws_field_tilt_actions IS NULL)
  Squad    — squad features      : UPDATE idempotent (WHERE squad_avg_form_5 IS NULL)
                                   FIX: CAST(ft.date AS DATE) pour éviter DATE != VARCHAR
  Diff     — différentiels       : UPDATE idempotent (WHERE ws_turnover_zone_diff IS NULL)
                                   FIX: virgule manquante avant ws_counter_attack_diff

Appelable :
  python -m features.whoscored
  python -m features.whoscored --reset-cols
  python -m features.whoscored --coverage-only
  python -m features.whoscored --force-recompute
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger

from .columns import (
    NEW_COLS_WS, DIFF_COLS_WS,
    NEW_COLS_SQUAD, DIFF_COLS_SQUAD,
)

# ── Config ────────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).resolve().parent.parent.parent)

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["db"])

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_match_details.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [whoscored] {message}",
)

WINDOW = CFG.get("features", {}).get("form_window", 5)

_RAW_MAPPING: dict = CFG.get("team_mapping", {})
TEAM_MAPPING_ROWS: list = [
    (str(raw), str(canonical))
    for raw, canonical in _RAW_MAPPING.items()
    if raw and canonical
]


# ── SQL : Passe 0 — player_match_stats ───────────────────────────────────────

SQL_CREATE_PLAYER_MATCH_STATS = """
CREATE TABLE IF NOT EXISTS gold.player_match_stats (
    ws_match_id VARCHAR, team_id INTEGER, player_id INTEGER,
    date DATE, season VARCHAR, league_source VARCHAR,
    n_actions INTEGER, n_shots INTEGER, n_key_passes INTEGER,
    xg_contribution DOUBLE, zone_dominance DOUBLE
)
"""

SQL_INSERT_PLAYER_MATCH_STATS = """
INSERT INTO gold.player_match_stats
WITH
match_dates AS (
    SELECT ws_match_id, match_date, league_source, season
    FROM silver.stg_whoscored_match_index
    WHERE ws_match_id NOT IN (SELECT DISTINCT ws_match_id FROM gold.player_match_stats)
),
player_agg AS (
    SELECT e.ws_match_id, e.team_id, e.player_id,
        COUNT(*) AS n_actions,
        COUNT(*) FILTER (WHERE e.is_shot=TRUE) AS n_shots,
        COUNT(DISTINCT e.row_num) FILTER (
            WHERE e.type_id=1
              AND TRY_CAST(
                  json_extract_string(e.qualifiers_json, '$[0].type.value') AS INTEGER
              ) = 210
        ) AS n_key_passes,
        CASE WHEN COUNT(*) FILTER (WHERE e.is_shot=TRUE)>0
             THEN COUNT(*) FILTER (WHERE e.is_shot=TRUE)
                  * (1.0 / (1.0 + SQRT(
                      POW(100.0 - AVG(e.x) FILTER (WHERE e.is_shot=TRUE), 2)
                    + POW( 50.0 - AVG(e.y) FILTER (WHERE e.is_shot=TRUE), 2)
                  )))
             ELSE 0.0 END AS xg_contribution,
        AVG(e.x) FILTER (WHERE e.is_touch=TRUE) AS zone_dominance
    FROM silver.stg_whoscored_events e
    WHERE e.player_id IS NOT NULL
      AND e.ws_match_id IN (SELECT ws_match_id FROM match_dates)
    GROUP BY e.ws_match_id, e.team_id, e.player_id
)
SELECT p.ws_match_id, p.team_id, p.player_id,
    d.match_date AS date, d.season, d.league_source,
    p.n_actions, p.n_shots, p.n_key_passes, p.xg_contribution, p.zone_dominance
FROM player_agg p
JOIN match_dates d ON p.ws_match_id=d.ws_match_id
"""


# ── SQL : Passe 1 — stg_events_qual (table Gold persistante) ─────────────────

SQL_CREATE_EVENTS_QUAL = """
CREATE TABLE IF NOT EXISTS gold.stg_events_qual (
    ws_match_id VARCHAR, team_id INTEGER, player_id INTEGER,
    minute INTEGER, second INTEGER, expanded_minute INTEGER, period INTEGER,
    x DOUBLE, y DOUBLE, end_x DOUBLE, end_y DOUBLE,
    type_id INTEGER, type_name VARCHAR, outcome_id INTEGER,
    is_touch BOOLEAN, is_shot BOOLEAN, row_num INTEGER,
    qual_type_id INTEGER, qual_type_name VARCHAR, qual_value VARCHAR
)
"""

SQL_INSERT_EVENTS_QUAL = """
INSERT INTO gold.stg_events_qual
SELECT
    e.ws_match_id, e.team_id, e.player_id, e.minute, e.second,
    e.expanded_minute, e.period, e.x, e.y, e.end_x, e.end_y,
    e.type_id, e.type_name, e.outcome_id, e.is_touch, e.is_shot, e.row_num,
    TRY_CAST(json_extract_string(q.qual, '$.type.value') AS INTEGER) AS qual_type_id,
    json_extract_string(q.qual, '$.type.displayName') AS qual_type_name,
    json_extract_string(q.qual, '$.value.value')       AS qual_value
FROM silver.stg_whoscored_events e,
     LATERAL (
         SELECT unnest(json_extract(e.qualifiers_json, '$[*]')::JSON[]) AS qual
     ) q
WHERE e.qualifiers_json IS NOT NULL
  AND e.qualifiers_json != '[]'
  AND e.ws_match_id NOT IN (SELECT DISTINCT ws_match_id FROM gold.stg_events_qual)
"""


# ── SQL : Passe 2 — stg_team_features (table Gold persistante) ───────────────

SQL_CREATE_TEAM_FEATURES = """
CREATE TABLE IF NOT EXISTS gold.stg_team_features (
    ws_match_id VARCHAR, team_id INTEGER,
    ws_field_tilt_actions DOUBLE, ws_high_turnover_rate DOUBLE,
    ws_deep_completion_rt DOUBLE, ws_momentum_delta DOUBLE,
    ws_counter_shot_rate DOUBLE, ws_set_piece_pressure DOUBLE,
    ws_attack_left_pct DOUBLE, ws_attack_center_pct DOUBLE, ws_attack_right_pct DOUBLE,
    ws_zone_def_pct DOUBLE, ws_zone_mid_pct DOUBLE, ws_zone_att_pct DOUBLE,
    ws_shot_six_yard_pct DOUBLE, ws_shot_penalty_pct DOUBLE, ws_shot_oob_pct DOUBLE,
    ws_shot_open_play_pct DOUBLE, ws_shot_set_piece_pct DOUBLE, ws_shot_penalty_att_pct DOUBLE,
    ws_conversion_rate DOUBLE, ws_cross_rate DOUBLE, ws_through_ball_rate DOUBLE,
    ws_long_ball_rate DOUBLE, ws_short_pass_rate DOUBLE,
    ws_def_exposed_left_pct DOUBLE, ws_def_exposed_center_pct DOUBLE, ws_def_exposed_right_pct DOUBLE,
    ws_counter_attack_dna DOUBLE, ws_midfield_control_idx DOUBLE,
    ws_defensive_line_height DOUBLE, ws_flank_exposure_asymm DOUBLE
)
"""

SQL_INSERT_TEAM_FEATURES = """
INSERT INTO gold.stg_team_features
WITH
new_match_ids AS (
    SELECT DISTINCT ws_match_id FROM silver.stg_whoscored_events
    WHERE ws_match_id NOT IN (SELECT DISTINCT ws_match_id FROM gold.stg_team_features)
),
base_counts AS (
    SELECT ws_match_id, team_id,
        COUNT(*) AS total_events,
        COUNT(*) FILTER (WHERE is_touch=TRUE) AS total_touches,
        COUNT(*) FILTER (WHERE is_shot=TRUE) AS total_shots,
        COUNT(*) FILTER (WHERE type_id=1) AS total_passes,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=1) AS passes_successful,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>66) AS touches_offensive_zone,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=1 AND end_x>83) AS deep_completions,
        COUNT(*) FILTER (WHERE type_id=1 AND outcome_id=0 AND x>66) AS turnovers_high_zone,
        COUNT(*) FILTER (WHERE x>66 AND type_id IN (1,3,13,15,16)) AS offensive_actions,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y<33.3) AS att_touches_left,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y>=33.3 AND y<=66.6) AS att_touches_center,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33 AND y>66.6) AS att_touches_right,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>33) AS att_touches_total,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x<33.3) AS zone_def_touches,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>=33.3 AND x<=66.6) AS zone_mid_touches,
        COUNT(*) FILTER (WHERE is_touch=TRUE AND x>66.6) AS zone_att_touches,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND x>94 AND y BETWEEN 36 AND 64) AS shots_six_yard,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND x>83 AND y BETWEEN 21 AND 79
                              AND NOT (x>94 AND y BETWEEN 36 AND 64)) AS shots_penalty_area,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND NOT(x>83 AND y BETWEEN 21 AND 79)) AS shots_out_of_box,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND type_id=16 AND outcome_id=1) AS goals_scored
    FROM silver.stg_whoscored_events
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),
defensive_exposure AS (
    SELECT ws_match_id, other_team_id AS team_id,
        COUNT(*) FILTER (WHERE x>33 AND y<33.3) AS opp_att_left,
        COUNT(*) FILTER (WHERE x>33 AND y>=33.3 AND y<=66.6) AS opp_att_center,
        COUNT(*) FILTER (WHERE x>33 AND y>66.6) AS opp_att_right,
        COUNT(*) FILTER (WHERE x>33) AS opp_att_total
    FROM (
        SELECT f.ws_match_id, f.x, f.y, f.is_touch, other.team_id AS other_team_id
        FROM silver.stg_whoscored_events f
        JOIN (SELECT ws_match_id, team_id FROM silver.stg_whoscored_events
              WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
              GROUP BY ws_match_id, team_id) other
            ON f.ws_match_id=other.ws_match_id AND f.team_id!=other.team_id
        WHERE f.is_touch=TRUE AND f.x>33
          AND f.ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    ) att_actions
    GROUP BY ws_match_id, other_team_id
),
qualifier_features AS (
    SELECT ws_match_id, team_id,
        COUNT(*) FILTER (WHERE is_shot=TRUE AND qual_type_id=26) AS shots_counter_attack,
        COUNT(DISTINCT row_num) FILTER (WHERE qual_type_id IN (5,6) AND x>50) AS set_pieces_offensive,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=22) AS shots_open_play,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=23) AS shots_set_piece,
        COUNT(DISTINCT row_num) FILTER (WHERE is_shot=TRUE AND qual_type_id=9) AS shots_penalty,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=2) AS passes_cross,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=155) AS passes_through_ball,
        COUNT(DISTINCT row_num) FILTER (WHERE type_id=1 AND qual_type_id=1) AS passes_long_ball
    FROM gold.stg_events_qual
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),
goals_conceded AS (
    SELECT DISTINCT f.ws_match_id, other.team_id AS conceding_team_id, f.expanded_minute AS goal_minute
    FROM silver.stg_whoscored_events f
    JOIN (SELECT ws_match_id, team_id FROM silver.stg_whoscored_events
          WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
          GROUP BY ws_match_id, team_id) other
        ON f.ws_match_id=other.ws_match_id AND f.team_id!=other.team_id
    WHERE f.type_id=16 AND f.outcome_id=1 AND f.is_shot=TRUE
      AND f.ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
),
momentum_windows AS (
    SELECT gc.ws_match_id, gc.conceding_team_id AS team_id,
        COUNT(e.row_num) FILTER (WHERE e.expanded_minute>=gc.goal_minute-10
                                  AND e.expanded_minute<gc.goal_minute
                                  AND e.team_id=gc.conceding_team_id) AS actions_pre,
        COUNT(e.row_num) FILTER (WHERE e.expanded_minute>gc.goal_minute
                                  AND e.expanded_minute<=gc.goal_minute+10
                                  AND e.team_id=gc.conceding_team_id) AS actions_post
    FROM goals_conceded gc
    JOIN silver.stg_whoscored_events e ON e.ws_match_id=gc.ws_match_id
    GROUP BY gc.ws_match_id, gc.conceding_team_id, gc.goal_minute
),
momentum_agg AS (
    SELECT ws_match_id, team_id,
        AVG(CASE WHEN actions_pre>0 THEN CAST(actions_post AS DOUBLE)/actions_pre ELSE NULL END) AS momentum_delta
    FROM momentum_windows GROUP BY ws_match_id, team_id
),
counter_attack_cte AS (
    SELECT s.ws_match_id, s.team_id,
        COUNT(DISTINCT t.t_shot) AS counter_attack_shots,
        COUNT(*) AS total_shots_ca
    FROM (
        SELECT ws_match_id, team_id, expanded_minute*60+second AS t_shot
        FROM silver.stg_whoscored_events
        WHERE type_id IN (13,14,15,16)
          AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    ) s
    LEFT JOIN (
        SELECT DISTINCT s2.ws_match_id, s2.team_id, s2.t_shot
        FROM (
            SELECT ws_match_id, team_id, expanded_minute*60+second AS t_shot
            FROM silver.stg_whoscored_events
            WHERE type_id IN (13,14,15,16)
              AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
        ) s2
        JOIN (
            SELECT ws_match_id, team_id, expanded_minute*60+second AS t_recovery
            FROM silver.stg_whoscored_events WHERE type_id=49
              AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
        ) r ON s2.ws_match_id=r.ws_match_id AND s2.team_id=r.team_id
           AND s2.t_shot>r.t_recovery AND s2.t_shot-r.t_recovery<=15
    ) t ON s.ws_match_id=t.ws_match_id AND s.team_id=t.team_id AND s.t_shot=t.t_shot
    GROUP BY s.ws_match_id, s.team_id
),
midfield_control_cte AS (
    SELECT ws_match_id, team_id,
        CAST(COUNT(*) FILTER (WHERE x BETWEEN 33 AND 66 AND type_id IN (1,7,8) AND outcome_id=1) AS DOUBLE)
        / NULLIF(SUM(COUNT(*) FILTER (WHERE x BETWEEN 33 AND 66 AND type_id IN (1,7,8) AND outcome_id=1))
                 OVER (PARTITION BY ws_match_id), 0) AS ws_midfield_control_idx
    FROM silver.stg_whoscored_events
    WHERE ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id
),
defensive_shape_cte AS (
    SELECT ws_match_id, team_id,
        AVG(x) AS ws_defensive_line_height,
        CASE WHEN COUNT(*) FILTER (WHERE y<30)>0 AND COUNT(*) FILTER (WHERE y>70)>0
             THEN (CAST(COUNT(*) FILTER (WHERE y<30 AND outcome_id=1) AS DOUBLE)/COUNT(*) FILTER (WHERE y<30))
                - (CAST(COUNT(*) FILTER (WHERE y>70 AND outcome_id=1) AS DOUBLE)/COUNT(*) FILTER (WHERE y>70))
             ELSE NULL END AS ws_flank_exposure_asymm
    FROM silver.stg_whoscored_events
    WHERE type_id IN (7,8,12) AND x IS NOT NULL AND y IS NOT NULL
      AND ws_match_id IN (SELECT ws_match_id FROM new_match_ids)
    GROUP BY ws_match_id, team_id HAVING COUNT(*)>=5
)
SELECT
    b.ws_match_id, b.team_id,
    CASE WHEN b.total_touches>0 THEN CAST(b.touches_offensive_zone AS DOUBLE)/b.total_touches END AS ws_field_tilt_actions,
    CASE WHEN b.total_passes>0  THEN CAST(b.turnovers_high_zone AS DOUBLE)/b.total_passes END AS ws_high_turnover_rate,
    CASE WHEN b.total_passes>0  THEN CAST(b.deep_completions AS DOUBLE)/b.total_passes END AS ws_deep_completion_rt,
    m.momentum_delta AS ws_momentum_delta,
    CASE WHEN b.total_shots>0   THEN CAST(COALESCE(q.shots_counter_attack,0) AS DOUBLE)/b.total_shots END AS ws_counter_shot_rate,
    CASE WHEN b.offensive_actions>0 THEN CAST(COALESCE(q.set_pieces_offensive,0) AS DOUBLE)/b.offensive_actions END AS ws_set_piece_pressure,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_left AS DOUBLE)/b.att_touches_total END AS ws_attack_left_pct,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_center AS DOUBLE)/b.att_touches_total END AS ws_attack_center_pct,
    CASE WHEN b.att_touches_total>0 THEN CAST(b.att_touches_right AS DOUBLE)/b.att_touches_total END AS ws_attack_right_pct,
    CASE WHEN b.total_touches>0 THEN CAST(b.zone_def_touches AS DOUBLE)/b.total_touches END AS ws_zone_def_pct,
    CASE WHEN b.total_touches>0 THEN CAST(b.zone_mid_touches AS DOUBLE)/b.total_touches END AS ws_zone_mid_pct,
    CASE WHEN b.total_touches>0 THEN CAST(b.zone_att_touches AS DOUBLE)/b.total_touches END AS ws_zone_att_pct,
    CASE WHEN b.total_shots>0 THEN CAST(b.shots_six_yard AS DOUBLE)/b.total_shots END AS ws_shot_six_yard_pct,
    CASE WHEN b.total_shots>0 THEN CAST(b.shots_penalty_area AS DOUBLE)/b.total_shots END AS ws_shot_penalty_pct,
    CASE WHEN b.total_shots>0 THEN CAST(b.shots_out_of_box AS DOUBLE)/b.total_shots END AS ws_shot_oob_pct,
    CASE WHEN b.total_shots>0 THEN CAST(COALESCE(q.shots_open_play,0) AS DOUBLE)/b.total_shots END AS ws_shot_open_play_pct,
    CASE WHEN b.total_shots>0 THEN CAST(COALESCE(q.shots_set_piece,0) AS DOUBLE)/b.total_shots END AS ws_shot_set_piece_pct,
    CASE WHEN b.total_shots>0 THEN CAST(COALESCE(q.shots_penalty,0) AS DOUBLE)/b.total_shots END AS ws_shot_penalty_att_pct,
    CASE WHEN b.total_shots>0 THEN CAST(b.goals_scored AS DOUBLE)/b.total_shots END AS ws_conversion_rate,
    CASE WHEN b.total_passes>0 THEN CAST(COALESCE(q.passes_cross,0) AS DOUBLE)/b.total_passes END AS ws_cross_rate,
    CASE WHEN b.total_passes>0 THEN CAST(COALESCE(q.passes_through_ball,0) AS DOUBLE)/b.total_passes END AS ws_through_ball_rate,
    CASE WHEN b.total_passes>0 THEN CAST(COALESCE(q.passes_long_ball,0) AS DOUBLE)/b.total_passes END AS ws_long_ball_rate,
    CASE WHEN b.total_passes>0 THEN 1.0-(
        CAST(COALESCE(q.passes_cross,0) AS DOUBLE)/b.total_passes
      + CAST(COALESCE(q.passes_through_ball,0) AS DOUBLE)/b.total_passes
      + CAST(COALESCE(q.passes_long_ball,0) AS DOUBLE)/b.total_passes
    ) END AS ws_short_pass_rate,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_left AS DOUBLE)/de.opp_att_total END AS ws_def_exposed_left_pct,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_center AS DOUBLE)/de.opp_att_total END AS ws_def_exposed_center_pct,
    CASE WHEN de.opp_att_total>0 THEN CAST(de.opp_att_right AS DOUBLE)/de.opp_att_total END AS ws_def_exposed_right_pct,
    CASE WHEN ca.total_shots_ca>0 THEN CAST(ca.counter_attack_shots AS DOUBLE)/ca.total_shots_ca ELSE NULL END AS ws_counter_attack_dna,
    mc.ws_midfield_control_idx,
    ds.ws_defensive_line_height,
    ds.ws_flank_exposure_asymm
FROM base_counts b
LEFT JOIN qualifier_features q  ON b.ws_match_id=q.ws_match_id AND b.team_id=q.team_id
LEFT JOIN momentum_agg m        ON b.ws_match_id=m.ws_match_id AND b.team_id=m.team_id
LEFT JOIN defensive_exposure de ON b.ws_match_id=de.ws_match_id AND b.team_id=de.team_id
LEFT JOIN counter_attack_cte ca ON b.ws_match_id=ca.ws_match_id AND b.team_id=ca.team_id
LEFT JOIN midfield_control_cte mc ON b.ws_match_id=mc.ws_match_id AND b.team_id=mc.team_id
LEFT JOIN defensive_shape_cte  ds ON b.ws_match_id=ds.ws_match_id AND b.team_id=ds.team_id
"""


# ── SQL : Passe 3 ─────────────────────────────────────────────────────────────

SQL_PIVOT_HOME_AWAY = """
CREATE OR REPLACE TEMP TABLE tmp_pivot AS
WITH
match_meta AS (
    SELECT ws_match_id, home_team_id, away_team_id, season
    FROM silver.stg_whoscored_match_index
),
home_features AS (
    SELECT f.ws_match_id, f.ws_field_tilt_actions, f.ws_high_turnover_rate, f.ws_deep_completion_rt,
        f.ws_momentum_delta, f.ws_counter_shot_rate, f.ws_set_piece_pressure,
        f.ws_attack_left_pct, f.ws_attack_center_pct, f.ws_attack_right_pct,
        f.ws_zone_def_pct, f.ws_zone_mid_pct, f.ws_zone_att_pct,
        f.ws_shot_six_yard_pct, f.ws_shot_penalty_pct, f.ws_shot_oob_pct,
        f.ws_shot_open_play_pct, f.ws_shot_set_piece_pct, f.ws_shot_penalty_att_pct,
        f.ws_conversion_rate, f.ws_cross_rate, f.ws_through_ball_rate,
        f.ws_long_ball_rate, f.ws_short_pass_rate,
        f.ws_def_exposed_left_pct, f.ws_def_exposed_center_pct, f.ws_def_exposed_right_pct,
        f.ws_counter_attack_dna, f.ws_midfield_control_idx,
        f.ws_defensive_line_height, f.ws_flank_exposure_asymm
    FROM gold.stg_team_features f
    JOIN match_meta m ON f.ws_match_id=m.ws_match_id AND f.team_id=m.home_team_id
),
away_features AS (
    SELECT f.ws_match_id, f.ws_field_tilt_actions, f.ws_high_turnover_rate, f.ws_deep_completion_rt,
        f.ws_momentum_delta, f.ws_counter_shot_rate, f.ws_set_piece_pressure,
        f.ws_attack_left_pct, f.ws_attack_center_pct, f.ws_attack_right_pct,
        f.ws_zone_def_pct, f.ws_zone_mid_pct, f.ws_zone_att_pct,
        f.ws_shot_six_yard_pct, f.ws_shot_penalty_pct, f.ws_shot_oob_pct,
        f.ws_shot_open_play_pct, f.ws_shot_set_piece_pct, f.ws_shot_penalty_att_pct,
        f.ws_conversion_rate, f.ws_cross_rate, f.ws_through_ball_rate,
        f.ws_long_ball_rate, f.ws_short_pass_rate,
        f.ws_def_exposed_left_pct, f.ws_def_exposed_center_pct, f.ws_def_exposed_right_pct,
        f.ws_counter_attack_dna, f.ws_midfield_control_idx,
        f.ws_defensive_line_height, f.ws_flank_exposure_asymm
    FROM gold.stg_team_features f
    JOIN match_meta m ON f.ws_match_id=m.ws_match_id AND f.team_id=m.away_team_id
)
SELECT m.ws_match_id, m.season, mi.match_date, mi.home_team_name, mi.away_team_name,
    h.ws_field_tilt_actions AS home_field_tilt_actions, h.ws_high_turnover_rate AS home_high_turnover_rate,
    h.ws_deep_completion_rt AS home_deep_completion_rt, h.ws_momentum_delta AS home_momentum_delta,
    h.ws_counter_shot_rate AS home_counter_shot_rate, h.ws_set_piece_pressure AS home_set_piece_pressure,
    h.ws_attack_left_pct AS home_attack_left_pct, h.ws_attack_center_pct AS home_attack_center_pct,
    h.ws_attack_right_pct AS home_attack_right_pct, h.ws_zone_def_pct AS home_zone_def_pct,
    h.ws_zone_mid_pct AS home_zone_mid_pct, h.ws_zone_att_pct AS home_zone_att_pct,
    h.ws_shot_six_yard_pct AS home_shot_six_yard_pct, h.ws_shot_penalty_pct AS home_shot_penalty_pct,
    h.ws_shot_oob_pct AS home_shot_oob_pct, h.ws_shot_open_play_pct AS home_shot_open_play_pct,
    h.ws_shot_set_piece_pct AS home_shot_set_piece_pct, h.ws_shot_penalty_att_pct AS home_shot_penalty_att_pct,
    h.ws_conversion_rate AS home_conversion_rate, h.ws_cross_rate AS home_cross_rate,
    h.ws_through_ball_rate AS home_through_ball_rate, h.ws_long_ball_rate AS home_long_ball_rate,
    h.ws_short_pass_rate AS home_short_pass_rate,
    h.ws_def_exposed_left_pct AS home_def_exposed_left_pct,
    h.ws_def_exposed_center_pct AS home_def_exposed_center_pct,
    h.ws_def_exposed_right_pct AS home_def_exposed_right_pct,
    h.ws_counter_attack_dna AS home_counter_attack_dna, h.ws_midfield_control_idx AS home_midfield_control_idx,
    h.ws_defensive_line_height AS home_defensive_line_height, h.ws_flank_exposure_asymm AS home_flank_exposure_asymm,
    a.ws_field_tilt_actions AS away_field_tilt_actions, a.ws_high_turnover_rate AS away_high_turnover_rate,
    a.ws_deep_completion_rt AS away_deep_completion_rt, a.ws_momentum_delta AS away_momentum_delta,
    a.ws_counter_shot_rate AS away_counter_shot_rate, a.ws_set_piece_pressure AS away_set_piece_pressure,
    a.ws_attack_left_pct AS away_attack_left_pct, a.ws_attack_center_pct AS away_attack_center_pct,
    a.ws_attack_right_pct AS away_attack_right_pct, a.ws_zone_def_pct AS away_zone_def_pct,
    a.ws_zone_mid_pct AS away_zone_mid_pct, a.ws_zone_att_pct AS away_zone_att_pct,
    a.ws_shot_six_yard_pct AS away_shot_six_yard_pct, a.ws_shot_penalty_pct AS away_shot_penalty_pct,
    a.ws_shot_oob_pct AS away_shot_oob_pct, a.ws_shot_open_play_pct AS away_shot_open_play_pct,
    a.ws_shot_set_piece_pct AS away_shot_set_piece_pct, a.ws_shot_penalty_att_pct AS away_shot_penalty_att_pct,
    a.ws_conversion_rate AS away_conversion_rate, a.ws_cross_rate AS away_cross_rate,
    a.ws_through_ball_rate AS away_through_ball_rate, a.ws_long_ball_rate AS away_long_ball_rate,
    a.ws_short_pass_rate AS away_short_pass_rate,
    a.ws_def_exposed_left_pct AS away_def_exposed_left_pct,
    a.ws_def_exposed_center_pct AS away_def_exposed_center_pct,
    a.ws_def_exposed_right_pct AS away_def_exposed_right_pct,
    a.ws_counter_attack_dna AS away_counter_attack_dna, a.ws_midfield_control_idx AS away_midfield_control_idx,
    a.ws_defensive_line_height AS away_defensive_line_height, a.ws_flank_exposure_asymm AS away_flank_exposure_asymm
FROM match_meta m
JOIN silver.stg_whoscored_match_index mi ON m.ws_match_id=mi.ws_match_id
LEFT JOIN home_features h ON m.ws_match_id=h.ws_match_id
LEFT JOIN away_features a ON m.ws_match_id=a.ws_match_id
"""

SQL_JOIN_TRAINING = """
CREATE OR REPLACE TEMP TABLE tmp_ws_team_history AS
WITH
home_side AS (
    SELECT p.match_date AS ws_date, p.season AS ws_season,
        COALESCE(tm.canonical_name, p.home_team_name) AS team_name,
        p.home_field_tilt_actions AS ws_field_tilt_actions, p.home_high_turnover_rate AS ws_high_turnover_rate,
        p.home_deep_completion_rt AS ws_deep_completion_rt, p.home_momentum_delta AS ws_momentum_delta,
        p.home_counter_shot_rate AS ws_counter_shot_rate, p.home_set_piece_pressure AS ws_set_piece_pressure,
        p.home_attack_left_pct AS ws_attack_left_pct, p.home_attack_center_pct AS ws_attack_center_pct,
        p.home_attack_right_pct AS ws_attack_right_pct, p.home_zone_def_pct AS ws_zone_def_pct,
        p.home_zone_mid_pct AS ws_zone_mid_pct, p.home_zone_att_pct AS ws_zone_att_pct,
        p.home_shot_six_yard_pct AS ws_shot_six_yard_pct, p.home_shot_penalty_pct AS ws_shot_penalty_pct,
        p.home_shot_oob_pct AS ws_shot_oob_pct, p.home_shot_open_play_pct AS ws_shot_open_play_pct,
        p.home_shot_set_piece_pct AS ws_shot_set_piece_pct, p.home_shot_penalty_att_pct AS ws_shot_penalty_att_pct,
        p.home_conversion_rate AS ws_conversion_rate, p.home_cross_rate AS ws_cross_rate,
        p.home_through_ball_rate AS ws_through_ball_rate, p.home_long_ball_rate AS ws_long_ball_rate,
        p.home_short_pass_rate AS ws_short_pass_rate,
        p.home_def_exposed_left_pct AS ws_def_exposed_left_pct,
        p.home_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.home_def_exposed_right_pct AS ws_def_exposed_right_pct,
        p.home_counter_attack_dna AS ws_counter_attack_dna, p.home_midfield_control_idx AS ws_midfield_control_idx,
        p.home_defensive_line_height AS ws_defensive_line_height, p.home_flank_exposure_asymm AS ws_flank_exposure_asymm
    FROM tmp_pivot p LEFT JOIN tmp_team_mapping tm ON p.home_team_name=tm.raw_name
    WHERE p.home_team_name IS NOT NULL
),
away_side AS (
    SELECT p.match_date AS ws_date, p.season AS ws_season,
        COALESCE(tm.canonical_name, p.away_team_name) AS team_name,
        p.away_field_tilt_actions AS ws_field_tilt_actions, p.away_high_turnover_rate AS ws_high_turnover_rate,
        p.away_deep_completion_rt AS ws_deep_completion_rt, p.away_momentum_delta AS ws_momentum_delta,
        p.away_counter_shot_rate AS ws_counter_shot_rate, p.away_set_piece_pressure AS ws_set_piece_pressure,
        p.away_attack_left_pct AS ws_attack_left_pct, p.away_attack_center_pct AS ws_attack_center_pct,
        p.away_attack_right_pct AS ws_attack_right_pct, p.away_zone_def_pct AS ws_zone_def_pct,
        p.away_zone_mid_pct AS ws_zone_mid_pct, p.away_zone_att_pct AS ws_zone_att_pct,
        p.away_shot_six_yard_pct AS ws_shot_six_yard_pct, p.away_shot_penalty_pct AS ws_shot_penalty_pct,
        p.away_shot_oob_pct AS ws_shot_oob_pct, p.away_shot_open_play_pct AS ws_shot_open_play_pct,
        p.away_shot_set_piece_pct AS ws_shot_set_piece_pct, p.away_shot_penalty_att_pct AS ws_shot_penalty_att_pct,
        p.away_conversion_rate AS ws_conversion_rate, p.away_cross_rate AS ws_cross_rate,
        p.away_through_ball_rate AS ws_through_ball_rate, p.away_long_ball_rate AS ws_long_ball_rate,
        p.away_short_pass_rate AS ws_short_pass_rate,
        p.away_def_exposed_left_pct AS ws_def_exposed_left_pct,
        p.away_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.away_def_exposed_right_pct AS ws_def_exposed_right_pct,
        p.away_counter_attack_dna AS ws_counter_attack_dna, p.away_midfield_control_idx AS ws_midfield_control_idx,
        p.away_defensive_line_height AS ws_defensive_line_height, p.away_flank_exposure_asymm AS ws_flank_exposure_asymm
    FROM tmp_pivot p LEFT JOIN tmp_team_mapping tm ON p.away_team_name=tm.raw_name
    WHERE p.away_team_name IS NOT NULL
)
SELECT * FROM home_side UNION ALL SELECT * FROM away_side
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def add_columns_if_not_exist(conn: duckdb.DuckDBPyConnection) -> None:
    for col_name, col_type in list(NEW_COLS_WS) + list(NEW_COLS_SQUAD):
        try:
            conn.execute(f"ALTER TABLE gold.features_training ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
        except Exception as e:
            logger.warning(f"  ALTER features_training.{col_name} : {e}")
    for col_name, col_type in list(DIFF_COLS_WS) + list(DIFF_COLS_SQUAD):
        try:
            conn.execute(f"ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
        except Exception as e:
            logger.warning(f"  ALTER features_final.{col_name} : {e}")


def reset_columns(conn: duckdb.DuckDBPyConnection) -> None:
    logger.warning("  Reset des colonnes ws_* + squad_* → NULL...")
    for col_name, _ in list(NEW_COLS_WS) + list(NEW_COLS_SQUAD):
        try:
            conn.execute(f"UPDATE gold.features_training SET {col_name}=NULL")
        except Exception:
            pass
    for col_name, _ in list(DIFF_COLS_WS) + list(DIFF_COLS_SQUAD):
        try:
            conn.execute(f"UPDATE gold.features_final SET {col_name}=NULL")
        except Exception:
            pass


def inject_team_mapping(conn: duckdb.DuckDBPyConnection) -> None:
    if not TEAM_MAPPING_ROWS:
        conn.execute("CREATE OR REPLACE TEMP TABLE tmp_team_mapping (raw_name VARCHAR, canonical_name VARCHAR)")
        logger.warning("  team_mapping vide — jointure sur nom brut")
        return
    df = pd.DataFrame(TEAM_MAPPING_ROWS, columns=["raw_name", "canonical_name"])
    conn.register("_df_team_mapping", df)
    conn.execute("CREATE OR REPLACE TEMP TABLE tmp_team_mapping AS SELECT raw_name, canonical_name FROM _df_team_mapping")
    conn.unregister("_df_team_mapping")
    n = conn.execute("SELECT COUNT(*) FROM tmp_team_mapping").fetchone()[0]
    logger.info(f"  tmp_team_mapping : {n:,} entrées")


# ── Passes ────────────────────────────────────────────────────────────────────

def run_passe0(conn: duckdb.DuckDBPyConnection) -> int:
    logger.info("Passe 0 — player_match_stats (incrémental)...")
    conn.execute(SQL_CREATE_PLAYER_MATCH_STATS)
    n_existing = conn.execute("SELECT COUNT(DISTINCT ws_match_id) FROM gold.player_match_stats").fetchone()[0]
    n_total    = conn.execute("SELECT COUNT(*) FROM silver.stg_whoscored_match_index").fetchone()[0]
    n_new = n_total - n_existing
    if n_new > 0:
        logger.info(f"  {n_new:,} nouveaux matchs...")
        conn.execute(SQL_INSERT_PLAYER_MATCH_STATS)
    n = conn.execute("SELECT COUNT(*) FROM gold.player_match_stats").fetchone()[0]
    logger.info(f"  gold.player_match_stats : {n:,} lignes ({n_existing:,} existants, {n_new:,} nouveaux)")
    return n


def run_passe1_incremental(conn: duckdb.DuckDBPyConnection, force_recompute: bool = False) -> int:
    """Table Gold persistante — clé ws_match_id. Premier run: 32 min. Relances: 0 sec."""
    conn.execute(SQL_CREATE_EVENTS_QUAL)
    if force_recompute:
        logger.warning("  --force-recompute : vidage gold.stg_events_qual...")
        conn.execute("DELETE FROM gold.stg_events_qual")
    n_existing = conn.execute("SELECT COUNT(DISTINCT ws_match_id) FROM gold.stg_events_qual").fetchone()[0]
    n_total    = conn.execute("SELECT COUNT(DISTINCT ws_match_id) FROM silver.stg_whoscored_events").fetchone()[0]
    n_new = n_total - n_existing
    if n_new == 0:
        n_qual = conn.execute("SELECT COUNT(*) FROM gold.stg_events_qual").fetchone()[0]
        logger.info(f"  Passe 1 skippée — gold.stg_events_qual à jour ({n_existing:,} matchs, {n_qual:,} lignes)")
        return n_qual
    logger.info(f"  Passe 1 — {n_new:,} nouveaux matchs (existants : {n_existing:,})...")
    conn.execute(SQL_INSERT_EVENTS_QUAL)
    n_qual = conn.execute("SELECT COUNT(*) FROM gold.stg_events_qual").fetchone()[0]
    logger.info(f"  gold.stg_events_qual total : {n_qual:,} lignes")
    return n_qual


def run_passe2_incremental(conn: duckdb.DuckDBPyConnection) -> int:
    conn.execute(SQL_CREATE_TEAM_FEATURES)
    n_existing = conn.execute("SELECT COUNT(DISTINCT ws_match_id) FROM gold.stg_team_features").fetchone()[0]
    n_total    = conn.execute("SELECT COUNT(DISTINCT ws_match_id) FROM silver.stg_whoscored_events").fetchone()[0]
    n_new = n_total - n_existing
    if n_new == 0:
        n = conn.execute("SELECT COUNT(*) FROM gold.stg_team_features").fetchone()[0]
        logger.info(f"  Passe 2 skippée — gold.stg_team_features à jour ({n_existing:,} matchs)")
        return n
    logger.info(f"  Passe 2 — {n_new:,} nouveaux matchs...")
    conn.execute(SQL_INSERT_TEAM_FEATURES)
    n = conn.execute("SELECT COUNT(*) FROM gold.stg_team_features").fetchone()[0]
    logger.info(f"  gold.stg_team_features total : {n:,} lignes ({n//2} matchs)")
    return n


def run_passe3(conn: duckdb.DuckDBPyConnection) -> int:
    logger.info("Passe 3 — Pivot home/away + anti-leakage LAG(1) (idempotent)...")
    conn.execute(SQL_PIVOT_HOME_AWAY)
    n_pivot = conn.execute("SELECT COUNT(*) FROM tmp_pivot").fetchone()[0]
    logger.info(f"  {n_pivot:,} matchs dans tmp_pivot")
    inject_team_mapping(conn)
    conn.execute(SQL_JOIN_TRAINING)
    n_hist = conn.execute("SELECT COUNT(*) FROM tmp_ws_team_history").fetchone()[0]
    logger.info(f"  {n_hist:,} lignes dans l'historique team-centric")

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_latest_ws AS
        SELECT ft.team AS team, ft.date AS ft_date, wsh.ws_date,
            wsh.ws_field_tilt_actions, wsh.ws_high_turnover_rate, wsh.ws_deep_completion_rt,
            wsh.ws_momentum_delta, wsh.ws_counter_shot_rate, wsh.ws_set_piece_pressure,
            wsh.ws_attack_left_pct, wsh.ws_attack_center_pct, wsh.ws_attack_right_pct,
            wsh.ws_zone_def_pct, wsh.ws_zone_mid_pct, wsh.ws_zone_att_pct,
            wsh.ws_shot_six_yard_pct, wsh.ws_shot_penalty_pct, wsh.ws_shot_oob_pct,
            wsh.ws_shot_open_play_pct, wsh.ws_shot_set_piece_pct, wsh.ws_shot_penalty_att_pct,
            wsh.ws_conversion_rate, wsh.ws_cross_rate, wsh.ws_through_ball_rate,
            wsh.ws_long_ball_rate, wsh.ws_short_pass_rate,
            wsh.ws_def_exposed_left_pct, wsh.ws_def_exposed_center_pct, wsh.ws_def_exposed_right_pct,
            wsh.ws_counter_attack_dna, wsh.ws_midfield_control_idx,
            wsh.ws_defensive_line_height, wsh.ws_flank_exposure_asymm
        FROM gold.features_training ft
        JOIN tmp_ws_team_history wsh
            ON ft.team=wsh.team_name AND wsh.ws_date < ft.date AND ft.season=wsh.ws_season
        WHERE ft.ws_field_tilt_actions IS NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ft.team, CAST(ft.date AS DATE) ORDER BY wsh.ws_date DESC) = 1
    """)
    n_joinable = conn.execute("SELECT COUNT(*) FROM tmp_latest_ws").fetchone()[0]
    logger.info(f"  {n_joinable:,} lignes joignables (anti-leakage)")

    conn.execute("""
        UPDATE gold.features_training AS ft SET
            ws_field_tilt_actions=lws.ws_field_tilt_actions, ws_high_turnover_rate=lws.ws_high_turnover_rate,
            ws_deep_completion_rt=lws.ws_deep_completion_rt, ws_momentum_delta=lws.ws_momentum_delta,
            ws_counter_shot_rate=lws.ws_counter_shot_rate, ws_set_piece_pressure=lws.ws_set_piece_pressure,
            ws_attack_left_pct=lws.ws_attack_left_pct, ws_attack_center_pct=lws.ws_attack_center_pct,
            ws_attack_right_pct=lws.ws_attack_right_pct, ws_zone_def_pct=lws.ws_zone_def_pct,
            ws_zone_mid_pct=lws.ws_zone_mid_pct, ws_zone_att_pct=lws.ws_zone_att_pct,
            ws_shot_six_yard_pct=lws.ws_shot_six_yard_pct, ws_shot_penalty_pct=lws.ws_shot_penalty_pct,
            ws_shot_oob_pct=lws.ws_shot_oob_pct, ws_shot_open_play_pct=lws.ws_shot_open_play_pct,
            ws_shot_set_piece_pct=lws.ws_shot_set_piece_pct, ws_shot_penalty_att_pct=lws.ws_shot_penalty_att_pct,
            ws_conversion_rate=lws.ws_conversion_rate, ws_cross_rate=lws.ws_cross_rate,
            ws_through_ball_rate=lws.ws_through_ball_rate, ws_long_ball_rate=lws.ws_long_ball_rate,
            ws_short_pass_rate=lws.ws_short_pass_rate,
            ws_def_exposed_left_pct=lws.ws_def_exposed_left_pct,
            ws_def_exposed_center_pct=lws.ws_def_exposed_center_pct,
            ws_def_exposed_right_pct=lws.ws_def_exposed_right_pct,
            ws_counter_attack_dna=lws.ws_counter_attack_dna, ws_midfield_control_idx=lws.ws_midfield_control_idx,
            ws_defensive_line_height=lws.ws_defensive_line_height, ws_flank_exposure_asymm=lws.ws_flank_exposure_asymm
        FROM tmp_latest_ws lws
        WHERE ft.team=lws.team AND CAST(ft.date AS DATE)=lws.ft_date
    """)

    conn.execute("""
        UPDATE gold.features_training SET
            has_ws_events = CASE WHEN ws_field_tilt_actions IS NOT NULL THEN 1 ELSE 0 END
        WHERE has_ws_events IS NULL
    """)
    n_updated = conn.execute("SELECT COUNT(*) FROM gold.features_training WHERE ws_field_tilt_actions IS NOT NULL").fetchone()[0]
    logger.info(f"  {n_updated:,} lignes enrichies dans features_training")
    return n_updated


def run_squad_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Squad features rolling 5 matchs — idempotent + FIX DATE != VARCHAR."""
    logger.info("  Squad features (Groupe 1) — rolling 5 matchs...")

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_current AS
        SELECT pms.ws_match_id, pms.team_id, pms.player_id,
            CAST(pms.date AS DATE) AS match_date, pms.season, pms.league_source,
            pms.n_actions, pms.xg_contribution
        FROM gold.player_match_stats pms WHERE pms.player_id IS NOT NULL
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_player_form AS
        WITH raw AS (
            SELECT cur.ws_match_id, cur.team_id, cur.player_id, cur.match_date, cur.league_source,
                AVG(hist.n_actions) AS player_avg_actions_5,
                AVG(hist.xg_contribution) AS player_avg_xg_5,
                COUNT(hist.ws_match_id) AS n_prev_matches
            FROM tmp_squad_current cur
            LEFT JOIN gold.player_match_stats hist
                ON hist.player_id=cur.player_id
               AND hist.league_source=cur.league_source
               AND hist.date::DATE < cur.match_date
               AND hist.date::DATE >= cur.match_date - INTERVAL '180 days'
            GROUP BY cur.ws_match_id, cur.team_id, cur.player_id, cur.match_date, cur.league_source
            HAVING COUNT(hist.ws_match_id) >= 1
        ),
        deduped AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ws_match_id, team_id, player_id ORDER BY match_date
            ) AS rn
            FROM raw
        )
        SELECT ws_match_id, team_id, player_id, match_date, league_source,
            player_avg_actions_5, player_avg_xg_5, n_prev_matches
        FROM deduped WHERE rn = 1
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_regularity AS
        SELECT cur.ws_match_id, cur.team_id,
            COUNT(DISTINCT cur.player_id) AS squad_size,
            COUNT(DISTINCT prev.player_id) AS players_in_prev,
            CASE WHEN COUNT(DISTINCT cur.player_id)>0
                 THEN CAST(COUNT(DISTINCT prev.player_id) AS DOUBLE)/COUNT(DISTINCT cur.player_id)
                 ELSE NULL END AS squad_regularity
        FROM tmp_squad_current cur
        LEFT JOIN (
            SELECT player_id, team_id, CAST(date AS DATE) AS prev_date, league_source
            FROM gold.player_match_stats WHERE player_id IS NOT NULL
        ) prev
            ON prev.player_id=cur.player_id AND prev.team_id=cur.team_id
           AND prev.league_source=cur.league_source
           AND prev.prev_date < cur.match_date AND prev.prev_date >= cur.match_date - INTERVAL '14 days'
        GROUP BY cur.ws_match_id, cur.team_id
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_top3 AS
        WITH ranked AS (
            SELECT ws_match_id, team_id, player_id, n_actions,
                ROW_NUMBER() OVER (PARTITION BY ws_match_id, team_id ORDER BY n_actions DESC) AS rk,
                SUM(n_actions) OVER (PARTITION BY ws_match_id, team_id) AS total_actions
            FROM tmp_squad_current
        )
        SELECT ws_match_id, team_id,
            CASE WHEN MAX(total_actions)>0 THEN SUM(n_actions) FILTER (WHERE rk<=3)/MAX(total_actions) ELSE NULL END AS squad_top3_share
        FROM ranked GROUP BY ws_match_id, team_id
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_agg AS
        SELECT pf.ws_match_id, pf.team_id, pf.match_date, pf.league_source,
            AVG(pf.player_avg_actions_5) AS squad_avg_form_5,
            AVG(pf.player_avg_xg_5) AS squad_xg_quality_5,
            sr.squad_regularity, t3.squad_top3_share
        FROM tmp_player_form pf
        LEFT JOIN tmp_squad_regularity sr ON pf.ws_match_id=sr.ws_match_id AND pf.team_id=sr.team_id
        LEFT JOIN tmp_squad_top3 t3 ON pf.ws_match_id=t3.ws_match_id AND pf.team_id=t3.team_id
        GROUP BY pf.ws_match_id, pf.team_id, pf.match_date, pf.league_source, sr.squad_regularity, t3.squad_top3_share
    """)

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_named AS
        SELECT sa.*,
            COALESCE(tm.canonical_name,
                CASE WHEN mi.home_team_id=sa.team_id THEN mi.home_team_name ELSE mi.away_team_name END
            ) AS team_name
        FROM tmp_squad_agg sa
        JOIN silver.stg_whoscored_match_index mi ON sa.ws_match_id=mi.ws_match_id
        LEFT JOIN tmp_team_mapping tm ON tm.raw_name=
            CASE WHEN mi.home_team_id=sa.team_id THEN mi.home_team_name ELSE mi.away_team_name END
    """)

    # FIX CRITIQUE : CAST(ft.date AS DATE) dans la QUALIFY pour éviter DATE != VARCHAR
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_squad_for_update AS
        SELECT ft.team, ft.date, ft.league_source,
            sn.squad_avg_form_5, sn.squad_xg_quality_5,
            sn.squad_regularity, sn.squad_top3_share
        FROM gold.features_training ft
        JOIN (
            SELECT team_name, league_source, match_date,
                squad_avg_form_5, squad_xg_quality_5, squad_regularity, squad_top3_share,
                ROW_NUMBER() OVER (
                    PARTITION BY team_name, league_source, match_date
                    ORDER BY match_date DESC
                ) AS rn
            FROM tmp_squad_named
        ) sn
            ON sn.team_name=ft.team AND sn.league_source=ft.league_source
           AND sn.match_date < ft.date
        WHERE ft.squad_avg_form_5 IS NULL AND sn.rn = 1
    """)

    n_squad = conn.execute("SELECT COUNT(*) FROM tmp_squad_for_update").fetchone()[0]
    logger.info(f"  {n_squad:,} lignes squad joignables")

    conn.execute("""
            UPDATE gold.features_training AS ft SET
                squad_avg_form_5=su.squad_avg_form_5,
                squad_xg_quality_5=su.squad_xg_quality_5,
                squad_regularity=su.squad_regularity,
                squad_top3_share=su.squad_top3_share
            FROM tmp_squad_for_update su
            WHERE ft.team=su.team AND ft.date=su.date AND ft.league_source=su.league_source
        """)

    n_ok = conn.execute("SELECT COUNT(*) FROM gold.features_training WHERE squad_avg_form_5 IS NOT NULL").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
    logger.info(f"  squad_avg_form_5 : {n_ok:,}/{total:,} ({n_ok/total*100:.1f}%)")


def build_differential_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Différentiels ws_* — FIX virgule manquante + idempotent WHERE IS NULL."""
    logger.info("  Calcul différentiels ws_* dans gold.features_final (idempotent)...")
    conn.execute("""
        UPDATE gold.features_final AS ff SET
            ws_turnover_zone_diff   = ft_team.ws_high_turnover_rate   - ft_opp.ws_high_turnover_rate,
            ws_deep_pass_diff       = ft_team.ws_deep_completion_rt   - ft_opp.ws_deep_completion_rt,
            ws_momentum_diff        = ft_team.ws_momentum_delta       - ft_opp.ws_momentum_delta,
            ws_counter_threat_diff  = ft_team.ws_counter_shot_rate    - ft_opp.ws_counter_shot_rate,
            ws_attack_width_diff    = ft_team.ws_attack_center_pct    - ft_opp.ws_attack_center_pct,
            ws_zone_att_diff        = ft_team.ws_zone_att_pct         - ft_opp.ws_zone_att_pct,
            ws_shot_zone_diff       = ft_team.ws_shot_penalty_pct     - ft_opp.ws_shot_penalty_pct,
            ws_conversion_diff      = ft_team.ws_conversion_rate      - ft_opp.ws_conversion_rate,
            ws_cross_diff           = ft_team.ws_cross_rate           - ft_opp.ws_cross_rate,
            ws_long_ball_diff       = ft_team.ws_long_ball_rate       - ft_opp.ws_long_ball_rate,
            ws_left_matchup_adv     = ft_team.ws_attack_left_pct      - ft_opp.ws_def_exposed_right_pct,
            ws_right_matchup_adv    = ft_team.ws_attack_right_pct     - ft_opp.ws_def_exposed_left_pct,
            ws_center_matchup_adv   = ft_team.ws_attack_center_pct    - ft_opp.ws_def_exposed_center_pct,
            ws_left_exploit_score   = ft_team.ws_attack_left_pct      * ft_opp.ws_def_exposed_left_pct,
            ws_center_exploit_score = ft_team.ws_attack_center_pct    * ft_opp.ws_def_exposed_center_pct,
            ws_right_exploit_score  = ft_team.ws_attack_right_pct     * ft_opp.ws_def_exposed_right_pct,
            ws_structural_matchup   = (
                ft_team.ws_attack_left_pct   * ft_opp.ws_def_exposed_left_pct
              + ft_team.ws_attack_center_pct * ft_opp.ws_def_exposed_center_pct
              + ft_team.ws_attack_right_pct  * ft_opp.ws_def_exposed_right_pct
            ) / NULLIF(
                ft_team.ws_attack_left_pct + ft_team.ws_attack_center_pct + ft_team.ws_attack_right_pct
            , 0),
            ws_counter_attack_diff  = ft_team.ws_counter_attack_dna   - ft_opp.ws_counter_attack_dna,
            ws_def_line_diff        = ft_team.ws_defensive_line_height - ft_opp.ws_defensive_line_height,
            ws_flank_asymm_diff     = ft_team.ws_flank_exposure_asymm  - ft_opp.ws_flank_exposure_asymm,
            squad_quality_gap       = ft_team.squad_avg_form_5         - ft_opp.squad_avg_form_5,
            squad_xg_matchup        = ft_team.squad_xg_quality_5 / NULLIF(ft_opp.squad_xg_quality_5, 0)
        FROM gold.features_training ft_team
        JOIN gold.features_training ft_opp
            ON ft_team.date=ft_opp.date AND ft_team.opponent=ft_opp.team
           AND ft_team.league_source=ft_opp.league_source
        WHERE ff.date=ft_team.date AND ff.team=ft_team.team AND ff.league_source=ft_team.league_source
          AND ff.ws_turnover_zone_diff IS NULL
    """)


def print_coverage_report(conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("═══ Rapport de couverture — whoscored ═══")
    total = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
    logger.info(f"  gold.features_training : {total:,} lignes")
    for col_name, _ in list(NEW_COLS_WS) + list(NEW_COLS_SQUAD):
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_training WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct = n_ok / total * 100 if total else 0
            status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
            logger.info(f"  {status} {col_name:<35} : {n_ok:>7,}/{total:,} ({pct:.1f}%)")
        except Exception as e:
            logger.warning(f"  {col_name} : erreur coverage ({e})")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(
    reset_cols: bool = False,
    coverage_only: bool = False,
    force_recompute: bool = False,
) -> None:
    logger.info("═══ Pipeline whoscored — Events → Gold Features ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    _tmp_dir = Path(tempfile.gettempdir()) / "duckdb_03b_tmp"
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(f"SET temp_directory='{_tmp_dir.as_posix()}'")

    try:
        n_events   = conn.execute("SELECT COUNT(*) FROM silver.stg_whoscored_events").fetchone()[0]
        n_index    = conn.execute("SELECT COUNT(*) FROM silver.stg_whoscored_match_index").fetchone()[0]
        n_training = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
        logger.info(f"  stg_whoscored_events      : {n_events:,}")
        logger.info(f"  stg_whoscored_match_index : {n_index:,}")
        logger.info(f"  gold.features_training    : {n_training:,}")
    except Exception as e:
        logger.error(f"  Prérequis manquant : {e}")
        conn.close()
        raise

    if n_events == 0:
        logger.warning("  Aucun événement WhoScored — pipeline ignoré")
        conn.close()
        return

    if coverage_only:
        print_coverage_report(conn)
        conn.close()
        return

    add_columns_if_not_exist(conn)
    if reset_cols:
        reset_columns(conn)

    run_passe0(conn)
    run_passe1_incremental(conn, force_recompute=force_recompute)
    run_passe2_incremental(conn)
    run_passe3(conn)

    try:
        run_squad_features(conn)
    except Exception as e:
        logger.warning(f"  Squad features ignorées : {e}")

    try:
        build_differential_features(conn)
    except Exception as e:
        logger.warning(f"  Différentiels features_final ignorés : {e}")

    print_coverage_report(conn)
    conn.close()
    logger.success("═══ Pipeline whoscored terminé ═══")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WhoScored Events → Gold Features")
    parser.add_argument("--reset-cols",      action="store_true")
    parser.add_argument("--coverage-only",   action="store_true")
    parser.add_argument("--force-recompute", action="store_true",
                        help="Force recalcul Passe 1 (si silver.stg_whoscored_events a changé)")
    args = parser.parse_args()
    run_pipeline(
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
        force_recompute=args.force_recompute,
    )