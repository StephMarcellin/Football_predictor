"""
features/draw.py — Draw Behavior + Draw Signals (F1–F20)
=========================================================
Fusion de 03c_features_draw_behavior.py et 03c_features_draw_signals.py.

Deux sous-pipelines exécutés séquentiellement dans run_pipeline() :

  Sous-pipeline A — Draw Behavior (Bloc H)
  ─────────────────────────────────────────
  H1 — ws_late_equalizer_rate          : égalisateurs tardifs (>70min) quand menés
  H2 — ws_post_yellowcard_concede_rate : fragilité après carton jaune
  H3 — ws_post_redcard_resilience      : résilience en infériorité numérique

  Sous-pipeline B — Draw Signals (F1–F20)
  ─────────────────────────────────────────
  AXE 1  — Détecteurs de match bloqué (F1–F6)
  AXE 2  — Domination relative (F7–F10)
  AXE 3  — Résilience & psychologie (F11–F14)
  AXE 4  — Efficacité / yield (F15–F18)
  AXE 5  — Composites signatures (F19–F20)

ANTI-LEAKAGE :
  Toutes les features utilisent les matchs strictement antérieurs à la date
  du match à prédire (h.date < t.date ou wsh.ws_date < ft.date).

Colonnes référencées depuis features/columns.py :
  NEW_COLS_DRAW_BEHAVIOR, NEW_COLS_DRAW_SIGNALS, DIFF_COLS_DRAW

Appelable :
  python -m features.draw                           # run complet (A+B)
  python -m features.draw --step behavior           # Bloc H uniquement
  python -m features.draw --step signals            # F1–F20 uniquement
  python -m features.draw --reset-cols
  python -m features.draw --coverage-only
  python -m features.draw --window 10
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
    NEW_COLS_DRAW_BEHAVIOR,
    NEW_COLS_DRAW_SIGNALS,
    DIFF_COLS_DRAW,
)

# ── Config ────────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).resolve().parent.parent.parent)

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["db"])

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_draw.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [draw] {message}",
)

WINDOW     = CFG.get("features", {}).get("form_window", 5)
H2H_WINDOW = CFG.get("features", {}).get("h2h_window", 10)

_RAW_MAPPING: dict = CFG.get("team_mapping", {})
TEAM_MAPPING_ROWS: list = [
    (str(raw), str(canonical))
    for raw, canonical in _RAW_MAPPING.items()
    if raw and canonical
]


# ══════════════════════════════════════════════════════════════════════════════
# PARTIE A — DRAW BEHAVIOR (Bloc H : H1, H2, H3)
# ══════════════════════════════════════════════════════════════════════════════

SQL_EVENTS_FLAT_H = """
CREATE OR REPLACE TEMP TABLE tmp_events_flat AS
SELECT ws_match_id, team_id, minute, second, expanded_minute, period,
       x, y, type_id, type_name, outcome_id, is_touch, is_shot,
       qualifiers_json, row_num
FROM silver.stg_whoscored_events
;
"""

SQL_DRAW_FEATURES_STEPS = [
"""
CREATE OR REPLACE TEMP TABLE tmp_late_equalizer AS
WITH
goals_raw AS (
    SELECT ws_match_id, team_id, minute, expanded_minute, 1 AS is_goal
    FROM tmp_events_flat
    WHERE is_shot=TRUE AND type_id=16 AND outcome_id=1
),
score_timeline AS (
    SELECT
        g.ws_match_id, g.team_id, g.expanded_minute,
        SUM(CASE WHEN g2.team_id=g.team_id  THEN g2.is_goal ELSE 0 END) AS goals_for_cumul,
        SUM(CASE WHEN g2.team_id!=g.team_id THEN g2.is_goal ELSE 0 END) AS goals_against_cumul
    FROM goals_raw g
    JOIN goals_raw g2 ON g2.ws_match_id=g.ws_match_id AND g2.expanded_minute<=g.expanded_minute
    GROUP BY g.ws_match_id, g.team_id, g.expanded_minute
),
late_equalizer_raw AS (
    SELECT ws_match_id, team_id,
        CASE WHEN expanded_minute>=70
              AND goals_against_cumul > (goals_for_cumul-1)
              AND goals_for_cumul     =  goals_against_cumul
        THEN 1 ELSE 0 END AS is_late_equalizer
    FROM score_timeline
)
SELECT ws_match_id, team_id, MAX(is_late_equalizer) AS had_late_equalizer
FROM late_equalizer_raw GROUP BY ws_match_id, team_id
""",
"""
CREATE OR REPLACE TEMP TABLE tmp_post_yellow AS
WITH
goals_raw AS (
    SELECT ws_match_id, team_id, expanded_minute, 1 AS is_goal
    FROM tmp_events_flat
    WHERE is_shot=TRUE AND type_id=16 AND outcome_id=1
),
yellow_cards AS (
    SELECT ws_match_id, team_id, expanded_minute AS card_expanded_minute
    FROM tmp_events_flat
    WHERE type_name='Card'
      AND json_extract_string(json_extract(qualifiers_json, '$[0]'), '$.type.displayName')='Yellow'
),
goals_conceded_after_yellow AS (
    SELECT
        yc.ws_match_id, yc.team_id, yc.card_expanded_minute,
        MAX(CASE WHEN g.ws_match_id IS NOT NULL THEN 1 ELSE 0 END) AS conceded_after_yellow
    FROM yellow_cards yc
    LEFT JOIN goals_raw g
        ON g.ws_match_id=yc.ws_match_id AND g.team_id!=yc.team_id
        AND g.expanded_minute > yc.card_expanded_minute
        AND g.expanded_minute <= yc.card_expanded_minute + 10
    GROUP BY yc.ws_match_id, yc.team_id, yc.card_expanded_minute
)
SELECT ws_match_id, team_id,
    MAX(conceded_after_yellow) AS conceded_after_any_yellow,
    1 AS had_yellow_card
FROM goals_conceded_after_yellow GROUP BY ws_match_id, team_id
""",
"""
CREATE OR REPLACE TEMP TABLE tmp_post_red AS
WITH
red_cards AS (
    SELECT ws_match_id, team_id, expanded_minute AS red_minute
    FROM tmp_events_flat
    WHERE type_name='Card'
      AND json_extract_string(json_extract(qualifiers_json, '$[0]'), '$.type.displayName') IN ('Red','SecondYellow')
),
offensive_touches AS (
    SELECT ws_match_id, team_id, expanded_minute
    FROM tmp_events_flat WHERE is_touch=TRUE AND x>50
),
offensive_actions_window AS (
    SELECT rc.ws_match_id, rc.team_id, rc.red_minute,
        COUNT(CASE WHEN e.expanded_minute>=rc.red_minute-10 AND e.expanded_minute<rc.red_minute  THEN 1 END) AS off_actions_before,
        COUNT(CASE WHEN e.expanded_minute>rc.red_minute     AND e.expanded_minute<=rc.red_minute+10 THEN 1 END) AS off_actions_after
    FROM red_cards rc
    INNER JOIN offensive_touches e
        ON e.ws_match_id=rc.ws_match_id AND e.team_id=rc.team_id
        AND e.expanded_minute BETWEEN rc.red_minute-10 AND rc.red_minute+10
    GROUP BY rc.ws_match_id, rc.team_id, rc.red_minute
)
SELECT ws_match_id, team_id,
    AVG(CASE WHEN off_actions_before>0 THEN CAST(off_actions_after AS DOUBLE)/off_actions_before
             WHEN off_actions_after>0  THEN 1.0 ELSE NULL END) AS resilience_ratio
FROM offensive_actions_window GROUP BY ws_match_id, team_id
""",
"""
CREATE OR REPLACE TEMP TABLE tmp_draw_features AS
SELECT
    al.ws_match_id, al.team_id,
    COALESCE(le.had_late_equalizer, 0) AS had_late_equalizer,
    py.conceded_after_any_yellow       AS conceded_after_yellow,
    py.had_yellow_card                 AS had_yellow_card,
    pr.resilience_ratio                AS red_card_resilience
FROM (SELECT DISTINCT ws_match_id, team_id FROM tmp_events_flat) al
LEFT JOIN tmp_late_equalizer le ON al.ws_match_id=le.ws_match_id AND al.team_id=le.team_id
LEFT JOIN tmp_post_yellow    py ON al.ws_match_id=py.ws_match_id AND al.team_id=py.team_id
LEFT JOIN tmp_post_red       pr ON al.ws_match_id=pr.ws_match_id AND al.team_id=pr.team_id
"""
]

SQL_PIVOT_DRAW = """
CREATE OR REPLACE TEMP TABLE tmp_pivot_draw AS
WITH match_meta AS (
    SELECT ws_match_id, home_team_id, away_team_id, match_date, home_team_name, away_team_name, season
    FROM silver.stg_whoscored_match_index
)
SELECT
    m.ws_match_id, m.season, m.match_date, m.home_team_name, m.away_team_name,
    h.had_late_equalizer    AS home_had_late_equalizer,
    h.conceded_after_yellow AS home_conceded_after_yellow,
    h.had_yellow_card       AS home_had_yellow_card,
    h.red_card_resilience   AS home_red_card_resilience,
    a.had_late_equalizer    AS away_had_late_equalizer,
    a.conceded_after_yellow AS away_conceded_after_yellow,
    a.had_yellow_card       AS away_had_yellow_card,
    a.red_card_resilience   AS away_red_card_resilience
FROM match_meta m
LEFT JOIN tmp_draw_features h ON m.ws_match_id=h.ws_match_id AND m.home_team_id=h.team_id
LEFT JOIN tmp_draw_features a ON m.ws_match_id=a.ws_match_id AND m.away_team_id=a.team_id
;
"""

SQL_TEAM_HISTORY_DRAW = """
CREATE OR REPLACE TEMP TABLE tmp_draw_team_history AS
WITH
home_side AS (
    SELECT p.match_date AS ws_date, p.season AS ws_season,
        COALESCE(tm.canonical_name, p.home_team_name) AS team_name,
        p.home_had_late_equalizer AS had_late_equalizer,
        p.home_conceded_after_yellow AS conceded_after_yellow,
        p.home_had_yellow_card AS had_yellow_card,
        p.home_red_card_resilience AS red_card_resilience
    FROM tmp_pivot_draw p LEFT JOIN tmp_team_mapping tm ON p.home_team_name=tm.raw_name
    WHERE p.home_team_name IS NOT NULL
),
away_side AS (
    SELECT p.match_date, p.season,
        COALESCE(tm.canonical_name, p.away_team_name) AS team_name,
        p.away_had_late_equalizer, p.away_conceded_after_yellow,
        p.away_had_yellow_card, p.away_red_card_resilience AS red_card_resilience
    FROM tmp_pivot_draw p LEFT JOIN tmp_team_mapping tm ON p.away_team_name=tm.raw_name
    WHERE p.away_team_name IS NOT NULL
)
SELECT * FROM home_side UNION ALL SELECT * FROM away_side
;
"""


# ══════════════════════════════════════════════════════════════════════════════
# PARTIE B — DRAW SIGNALS (F1–F20) — SQL inline (garder les f-strings de l'original)
# ══════════════════════════════════════════════════════════════════════════════

def _build_sql_backbone_features(window: int) -> str:
    return f"""
CREATE OR REPLACE TEMP TABLE tmp_backbone_features AS
WITH
base AS (SELECT DISTINCT date, team, opponent, league_source, venue, season FROM gold.stg_backbone),
rolling_stats AS (
    SELECT t.date, t.team, t.league_source, t.venue, t.opponent,
        AVG(h.gf)   AS avg_gf_5, AVG(h.ga)   AS avg_ga_5,
        AVG(h.np_xg) AS avg_xg_5, AVG(h.np_xg_conceded) AS avg_xg_conceded_5,
        AVG(h.shots_total) AS avg_shots_5, AVG(h.shots_on_target) AS avg_sot_5,
        AVG(CASE WHEN h.shots_total>0 THEN h.np_xg/CAST(h.shots_total AS DOUBLE) END) AS sqr_5,
        AVG(CAST(h.clean_sheet AS DOUBLE)) AS cs_rate_5,
        AVG(h.save_pct) AS avg_save_rate_5,
        AVG(h.ppda) AS avg_ppda_5,
        AVG(CAST(h.red_cards AS DOUBLE)) AS avg_red_cards_5,
        AVG(CASE WHEN h.red_cards>0 THEN 1.0 ELSE 0.0 END) AS red_card_rate_5,
        SUM(CASE WHEN h.red_cards>0 AND h.result_1n2='W' THEN 3.0
                 WHEN h.red_cards>0 AND h.result_1n2='D' THEN 1.0 ELSE 0.0 END) AS pts_with_red_card,
        SUM(CASE WHEN h.red_cards>0 THEN 1.0 ELSE 0.0 END) AS n_matches_with_red,
        AVG(CASE WHEN h.shots_total>0 THEN 1.0-(h.gf/CAST(h.shots_total AS DOUBLE)) END) AS sterility_5,
        COUNT(*) FILTER (WHERE h.result_1n2='W') AS wins_5,
        COUNT(*) FILTER (WHERE h.result_1n2='D') AS draws_5,
        COUNT(*) AS n_matches_5,
        MAX(h.season_att_rating) AS season_att_rating,
        MAX(h.season_def_rating) AS season_def_rating
    FROM base t
    JOIN gold.stg_backbone h
        ON h.team=t.team AND h.league_source=t.league_source AND h.date < t.date
    WHERE h.date >= (t.date - INTERVAL '{window * 7} days') AND h.np_xg IS NOT NULL
    GROUP BY t.date, t.team, t.league_source, t.venue, t.opponent
),
rolling_venue AS (
    SELECT t.date, t.team, t.league_source, t.venue,
        AVG(h.np_xg) AS avg_xg_venue_5
    FROM base t
    JOIN gold.stg_backbone h
        ON h.team=t.team AND h.league_source=t.league_source AND h.venue=t.venue AND h.date < t.date
    WHERE h.date >= (t.date - INTERVAL '{window * 14} days') AND h.np_xg IS NOT NULL
    GROUP BY t.date, t.team, t.league_source, t.venue
),
league_draw_rate AS (
    SELECT t.date, t.team, t.league_source, t.season,
        AVG(CASE WHEN h.result_1n2='D' THEN 1.0 ELSE 0.0 END) AS league_draw_rate
    FROM base t
    JOIN gold.stg_backbone h ON h.league_source=t.league_source AND h.season=t.season AND h.date < t.date
    GROUP BY t.date, t.team, t.league_source, t.season
),
comeback_stats AS (
    SELECT t.date, t.team, t.league_source,
        AVG(CASE WHEN h.ga>0 AND h.result_1n2 IN ('W','D') THEN 1.0 ELSE 0.0 END) AS comeback_rate
    FROM base t
    JOIN gold.stg_backbone h ON h.team=t.team AND h.league_source=t.league_source AND h.date < t.date
    WHERE h.date >= (t.date - INTERVAL '{window * 7} days')
    GROUP BY t.date, t.team, t.league_source
),
market_probs AS (
    SELECT date, team, league_source, pinnacle_prob_draw, pinnacle_prob_team, market_prob_draw
    FROM gold.stg_backbone
)
SELECT b.date, b.team, b.opponent, b.league_source, b.venue, b.season,
    r.avg_gf_5, r.avg_ga_5, r.avg_xg_5, r.avg_xg_conceded_5, r.avg_shots_5, r.avg_sot_5,
    r.sqr_5, r.cs_rate_5, r.avg_save_rate_5, r.avg_ppda_5, r.red_card_rate_5,
    r.pts_with_red_card, r.n_matches_with_red, r.sterility_5,
    r.wins_5, r.draws_5, r.n_matches_5, r.season_att_rating, r.season_def_rating,
    v.avg_xg_venue_5, ld.league_draw_rate, cb.comeback_rate,
    mp.pinnacle_prob_draw, mp.pinnacle_prob_team, mp.market_prob_draw
FROM base b
LEFT JOIN rolling_stats r ON b.date=r.date AND b.team=r.team AND b.league_source=r.league_source
LEFT JOIN rolling_venue v  ON b.date=v.date AND b.team=v.team AND b.league_source=v.league_source AND b.venue=v.venue
LEFT JOIN league_draw_rate ld ON b.date=ld.date AND b.team=ld.team AND b.league_source=ld.league_source
LEFT JOIN comeback_stats cb    ON b.date=cb.date AND b.team=cb.team AND b.league_source=cb.league_source
LEFT JOIN market_probs mp      ON b.date=mp.date AND b.team=mp.team AND b.league_source=mp.league_source
;
"""


SQL_WS_TIMING_FEATURES = """
CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
WITH
goals_scored AS (
    SELECT ws_match_id, team_id, expanded_minute,
        CASE WHEN expanded_minute>75 THEN 1 ELSE 0 END AS is_late_goal
    FROM silver.stg_whoscored_events
    WHERE type_id=16 AND outcome_id=1 AND is_shot=TRUE
),
goal_timing AS (
    SELECT ws_match_id, team_id,
        COUNT(*) AS total_goals,
        AVG(CAST(is_late_goal AS DOUBLE)) AS late_goal_pct,
        STDDEV(CAST(expanded_minute AS DOUBLE)) AS goal_minute_stddev
    FROM goals_scored GROUP BY ws_match_id, team_id
),
ht_situation AS (
    SELECT mi.ws_match_id, mi.home_team_id, mi.away_team_id,
        CASE WHEN md.ht_score_home=md.ht_score_away THEN 1 ELSE 0 END AS ht_draw
    FROM silver.stg_whoscored_match_index mi
    LEFT JOIN silver.stg_whoscored_match_details md ON mi.ws_match_id=md.ws_match_id
    WHERE md.ht_score_home IS NOT NULL
)
SELECT gt.ws_match_id, gt.team_id, gt.total_goals, gt.late_goal_pct,
       gt.goal_minute_stddev, ht.ht_draw
FROM goal_timing gt
LEFT JOIN ht_situation ht
    ON gt.ws_match_id=ht.ws_match_id
    AND (gt.team_id=ht.home_team_id OR gt.team_id=ht.away_team_id)
;
"""
def _build_sql_ws_rolling(window: int) -> str:
    return f"""
CREATE OR REPLACE TEMP TABLE tmp_ws_rolling AS
WITH
-- Résolution des noms d'équipe depuis le mapping (matérialisée pour éviter subquery dans JOIN)
home_resolved AS (
    SELECT
        mi.ws_match_id,
        mi.match_date,
        mi.home_team_id,
        mi.away_team_id,
        COALESCE(tm_h.canonical_name, mi.home_team_name) AS home_canonical,
        COALESCE(tm_a.canonical_name, mi.away_team_name) AS away_canonical
    FROM silver.stg_whoscored_match_index mi
    LEFT JOIN tmp_team_mapping_c tm_h ON mi.home_team_name = tm_h.raw_name
    LEFT JOIN tmp_team_mapping_c tm_a ON mi.away_team_name = tm_a.raw_name
),
-- Vue team-centric : une ligne par (match, team)
ws_per_team AS (
    SELECT
        hr.ws_match_id,
        hr.match_date,
        hr.home_canonical AS team,
        wst.late_goal_pct,
        wst.goal_minute_stddev,
        wst.ht_draw
    FROM home_resolved hr
    LEFT JOIN tmp_ws_timing wst
        ON wst.ws_match_id = hr.ws_match_id
        AND wst.team_id    = hr.home_team_id

    UNION ALL

    SELECT
        hr.ws_match_id,
        hr.match_date,
        hr.away_canonical AS team,
        wst.late_goal_pct,
        wst.goal_minute_stddev,
        wst.ht_draw
    FROM home_resolved hr
    LEFT JOIN tmp_ws_timing wst
        ON wst.ws_match_id = hr.ws_match_id
        AND wst.team_id    = hr.away_team_id
)
-- Rolling sur les W derniers matchs WhoScored par équipe
SELECT
    t.date,
    t.team,
    t.league_source,
    AVG(wpt.late_goal_pct)           AS late_goal_tendency,
    AVG(wpt.goal_minute_stddev)      AS goal_timing_variance,
    AVG(CAST(wpt.ht_draw AS DOUBLE)) AS ht_draw_rate
FROM (SELECT DISTINCT date, team, league_source FROM gold.stg_backbone) t
JOIN ws_per_team wpt
    ON  wpt.team       = t.team
    AND wpt.match_date < t.date
GROUP BY t.date, t.team, t.league_source
;
"""
def _build_sql_compute_features(window: int) -> str:
    return f"""
CREATE OR REPLACE TEMP TABLE tmp_f_computed AS
WITH
team_vs_opp AS (
    SELECT
        t.date, t.team, t.league_source, t.venue,
        t.avg_gf_5, t.avg_ga_5, t.avg_xg_5, t.avg_xg_conceded_5,
        t.avg_shots_5, t.avg_sot_5, t.sqr_5, t.cs_rate_5,
        t.avg_save_rate_5, t.avg_ppda_5, t.red_card_rate_5,
        t.pts_with_red_card, t.n_matches_with_red, t.sterility_5,
        t.n_matches_5, t.season_att_rating, t.season_def_rating,
        t.avg_xg_venue_5, t.league_draw_rate, t.comeback_rate,
        t.pinnacle_prob_draw, t.pinnacle_prob_team,
        o.avg_gf_5 AS opp_avg_gf_5, o.avg_xg_5 AS opp_avg_xg_5,
        o.avg_xg_conceded_5 AS opp_avg_xg_conceded_5, o.sqr_5 AS opp_sqr_5,
        o.cs_rate_5 AS opp_cs_rate_5, o.avg_save_rate_5 AS opp_avg_save_rate_5,
        o.avg_ppda_5 AS opp_avg_ppda_5, o.sterility_5 AS opp_sterility_5,
        o.season_att_rating AS opp_season_att_rating,
        o.season_def_rating AS opp_season_def_rating,
        o.comeback_rate AS opp_comeback_rate,
        o.pinnacle_prob_team AS opp_pinnacle_prob_team,
        ft.ws_momentum_delta, ft_opp.ws_momentum_delta AS opp_ws_momentum_delta,
        ft.ws_zone_att_pct,   ft_opp.ws_zone_att_pct  AS opp_ws_zone_att_pct
    FROM tmp_backbone_features t
    LEFT JOIN tmp_backbone_features o
        ON t.date=o.date AND t.opponent=o.team AND t.league_source=o.league_source
    LEFT JOIN gold.features_training ft
        ON ft.date=t.date AND ft.team=t.team AND ft.league_source=t.league_source
    LEFT JOIN gold.features_training ft_opp
        ON ft_opp.date=t.date AND ft_opp.team=t.opponent AND ft_opp.league_source=t.league_source
)

SELECT
    tvo.date, tvo.team, tvo.league_source,
    CASE WHEN (tvo.avg_save_rate_5+tvo.opp_avg_save_rate_5)>0
          AND tvo.sterility_5 IS NOT NULL AND tvo.opp_sterility_5 IS NOT NULL
        THEN (tvo.sterility_5*tvo.opp_sterility_5)/(tvo.avg_save_rate_5+tvo.opp_avg_save_rate_5+0.01)
    END AS f1_mutual_cancel_idx,
    CASE WHEN tvo.ws_zone_att_pct IS NOT NULL AND tvo.opp_ws_zone_att_pct IS NOT NULL
        THEN 1.0 - ABS(tvo.ws_zone_att_pct - tvo.opp_ws_zone_att_pct)
    END AS f2_defensive_mirror,
    CASE WHEN tvo.pinnacle_prob_draw IS NOT NULL AND tvo.league_draw_rate IS NOT NULL
        THEN tvo.pinnacle_prob_draw - tvo.league_draw_rate
    END AS f3_draw_market_dev,
    CASE WHEN tvo.ws_momentum_delta IS NOT NULL AND tvo.opp_ws_momentum_delta IS NOT NULL
        THEN 1.0 - ABS(tvo.ws_momentum_delta - tvo.opp_ws_momentum_delta)
    END AS f4_momentum_convergence,
    CASE WHEN tvo.cs_rate_5 IS NOT NULL AND tvo.opp_cs_rate_5 IS NOT NULL
        THEN tvo.cs_rate_5 * tvo.opp_cs_rate_5
    END AS f5_cs_mutual_rate,
    wr.ht_draw_rate AS f6_ht_draw_tendency,
    CASE WHEN tvo.season_att_rating IS NOT NULL AND tvo.opp_season_def_rating IS NOT NULL
        THEN tvo.season_att_rating - tvo.opp_season_def_rating END AS f7_off_def_mismatch,
    CASE WHEN tvo.opp_season_att_rating IS NOT NULL AND tvo.season_def_rating IS NOT NULL
        THEN tvo.opp_season_att_rating - tvo.season_def_rating END AS f7_def_off_mismatch,
    CASE WHEN tvo.avg_ppda_5>0 AND tvo.opp_avg_ppda_5>0
        THEN LN(tvo.opp_avg_ppda_5/tvo.avg_ppda_5) END AS f8_press_dominance_ratio,
    CASE WHEN tvo.sqr_5 IS NOT NULL AND tvo.opp_sqr_5 IS NOT NULL
        THEN tvo.sqr_5 - tvo.opp_sqr_5 END AS f9_chance_quality_gap,
    CASE WHEN tvo.avg_xg_venue_5 IS NOT NULL AND tvo.avg_xg_5 IS NOT NULL
        THEN tvo.avg_xg_venue_5 - tvo.avg_xg_5 END AS f10_venue_power_adj,
    tvo.comeback_rate AS f11_comeback_rate,
    CASE WHEN tvo.n_matches_with_red>0
        THEN tvo.pts_with_red_card/tvo.n_matches_with_red END AS f12_red_card_resilience,
    wr.late_goal_tendency AS f13_late_goal_tendency,
    wr.goal_timing_variance AS f14_goal_timing_variance,
    CASE WHEN tvo.avg_xg_5>0 THEN tvo.avg_gf_5/tvo.avg_xg_5 END AS f15_xg_yield_ratio,
    CASE WHEN tvo.avg_xg_conceded_5>0 THEN tvo.avg_ga_5/tvo.avg_xg_conceded_5 END AS f16_def_yield_ratio,
    CASE WHEN tvo.avg_shots_5>0 THEN tvo.avg_gf_5/tvo.avg_shots_5 END AS f17_shots_to_goal_eff,
    CASE WHEN tvo.avg_sot_5>0  THEN tvo.avg_gf_5/tvo.avg_sot_5   END AS f18_sot_conversion,
    CASE WHEN tvo.sterility_5 IS NOT NULL AND tvo.opp_sterility_5 IS NOT NULL
          AND tvo.avg_ppda_5 IS NOT NULL AND tvo.opp_avg_ppda_5 IS NOT NULL
        THEN (tvo.sterility_5+tvo.opp_sterility_5)
             * (1.0/(ABS(tvo.avg_ppda_5-tvo.opp_avg_ppda_5)+0.5))
             * (1.0-ABS(COALESCE(tvo.ws_zone_att_pct,0.33)-COALESCE(tvo.opp_ws_zone_att_pct,0.33)))
    END AS f19_tactical_lock_idx,
    CASE WHEN tvo.pinnacle_prob_team>0 AND tvo.avg_xg_5>0 AND tvo.opp_avg_xg_5>0
        THEN (1.0/tvo.pinnacle_prob_team)
             * ((tvo.opp_avg_gf_5/NULLIF(tvo.opp_avg_xg_5,0))
                / NULLIF((tvo.avg_gf_5/NULLIF(tvo.avg_xg_5,0)),0))
             * COALESCE(tvo.opp_comeback_rate,0.3)
    END AS f20_upset_composite
FROM team_vs_opp tvo
LEFT JOIN tmp_ws_rolling wr
    ON  wr.date          = tvo.date
    AND wr.team          = tvo.team
    AND wr.league_source = tvo.league_source
;
"""


SQL_UPDATE_TRAINING = """
UPDATE gold.features_training AS ft SET
    f1_mutual_cancel_idx    = fc.f1_mutual_cancel_idx,
    f2_defensive_mirror     = fc.f2_defensive_mirror,
    f3_draw_market_dev      = fc.f3_draw_market_dev,
    f4_momentum_convergence = fc.f4_momentum_convergence,
    f5_cs_mutual_rate       = fc.f5_cs_mutual_rate,
    f6_ht_draw_tendency     = fc.f6_ht_draw_tendency,
    f7_off_def_mismatch     = fc.f7_off_def_mismatch,
    f7_def_off_mismatch     = fc.f7_def_off_mismatch,
    f8_press_dominance_ratio= fc.f8_press_dominance_ratio,
    f9_chance_quality_gap   = fc.f9_chance_quality_gap,
    f10_venue_power_adj     = fc.f10_venue_power_adj,
    f11_comeback_rate       = fc.f11_comeback_rate,
    f12_red_card_resilience = fc.f12_red_card_resilience,
    f13_late_goal_tendency  = fc.f13_late_goal_tendency,
    f14_goal_timing_variance= fc.f14_goal_timing_variance,
    f15_xg_yield_ratio      = fc.f15_xg_yield_ratio,
    f16_def_yield_ratio     = fc.f16_def_yield_ratio,
    f17_shots_to_goal_eff   = fc.f17_shots_to_goal_eff,
    f18_sot_conversion      = fc.f18_sot_conversion,
    f19_tactical_lock_idx   = fc.f19_tactical_lock_idx,
    f20_upset_composite     = fc.f20_upset_composite
FROM tmp_f_computed fc
WHERE ft.date=fc.date AND ft.team=fc.team AND ft.league_source=fc.league_source
;
"""

SQL_UPDATE_FINAL_DIFFS = """
UPDATE gold.features_final AS ff SET
    f1_mutual_cancel_diff   = ft_team.f1_mutual_cancel_idx  - ft_opp.f1_mutual_cancel_idx,
    f7_mismatch_diff        = ft_team.f7_off_def_mismatch   - ft_opp.f7_off_def_mismatch,
    f8_press_dominance_diff = ft_team.f8_press_dominance_ratio - ft_opp.f8_press_dominance_ratio,
    f9_chance_quality_diff  = ft_team.f9_chance_quality_gap  - ft_opp.f9_chance_quality_gap,
    f10_venue_power_diff    = ft_team.f10_venue_power_adj    - ft_opp.f10_venue_power_adj,
    f11_comeback_diff       = ft_team.f11_comeback_rate      - ft_opp.f11_comeback_rate,
    f13_late_goal_diff      = ft_team.f13_late_goal_tendency - ft_opp.f13_late_goal_tendency,
    f15_xg_yield_diff       = ft_team.f15_xg_yield_ratio     - ft_opp.f15_xg_yield_ratio,
    f16_def_yield_diff      = ft_team.f16_def_yield_ratio    - ft_opp.f16_def_yield_ratio,
    f19_tactical_lock_diff  = ft_team.f19_tactical_lock_idx  - ft_opp.f19_tactical_lock_idx,
    f20_upset_diff          = ft_team.f20_upset_composite    - ft_opp.f20_upset_composite
FROM gold.features_training ft_team
JOIN gold.features_training ft_opp
    ON ft_team.date=ft_opp.date AND ft_team.opponent=ft_opp.team AND ft_team.league_source=ft_opp.league_source
WHERE ff.date=ft_team.date AND ff.team=ft_team.team AND ff.league_source=ft_team.league_source
;
"""


# ── Helpers partagés ──────────────────────────────────────────────────────────

def _inject_team_mapping(conn: duckdb.DuckDBPyConnection,
                          table_name: str = "tmp_team_mapping") -> None:
    """Injecte le team_mapping sous le nom de table spécifié."""
    rows = TEAM_MAPPING_ROWS
    df   = pd.DataFrame(rows if rows else [("__none__", "__none__")],
                        columns=["raw_name", "canonical_name"])
    conn.register("_df_tm_shared", df)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT raw_name, canonical_name FROM _df_tm_shared
    """)
    conn.unregister("_df_tm_shared")
    n = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"  {table_name} : {n:,} entrées")


def _add_columns(conn: duckdb.DuckDBPyConnection) -> None:
    all_training = list(NEW_COLS_DRAW_BEHAVIOR) + list(NEW_COLS_DRAW_SIGNALS)
    for col_name, col_type in all_training:
        try:
            conn.execute(f"ALTER TABLE gold.features_training ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
        except Exception as e:
            logger.warning(f"  ALTER features_training.{col_name} : {e}")
    for col_name, col_type in DIFF_COLS_DRAW:
        try:
            conn.execute(f"ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
        except Exception as e:
            logger.warning(f"  ALTER features_final.{col_name} : {e}")


def _reset_columns(conn: duckdb.DuckDBPyConnection) -> None:
    all_training = list(NEW_COLS_DRAW_BEHAVIOR) + list(NEW_COLS_DRAW_SIGNALS)
    for col_name, _ in all_training:
        try:
            conn.execute(f"UPDATE gold.features_training SET {col_name}=NULL")
        except Exception:
            pass
    for col_name, _ in DIFF_COLS_DRAW:
        try:
            conn.execute(f"UPDATE gold.features_final SET {col_name}=NULL")
        except Exception:
            pass


def _print_coverage(conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("═══ Rapport de couverture — draw ═══")
    total = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
    all_cols = list(NEW_COLS_DRAW_BEHAVIOR) + list(NEW_COLS_DRAW_SIGNALS)
    for col_name, _ in all_cols:
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_training WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct    = n_ok / total * 100 if total else 0
            status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
            logger.info(f"  {status} {col_name:<40} : {n_ok:>7,}/{total:,} ({pct:.1f}%)")
        except Exception as e:
            logger.warning(f"  {col_name} : erreur ({e})")


# ── Sous-pipeline A : Behavior ────────────────────────────────────────────────

def run_behavior(conn: duckdb.DuckDBPyConnection, window: int = WINDOW) -> int:
    """
    Calcule les 3 features Draw Behavior (H1/H2/H3) et met à jour
    gold.features_training.
    """
    logger.info("--- Sous-pipeline A : Draw Behavior (H1/H2/H3) ---")

    # Réutilise tmp_events_flat si disponible (run après whoscored.py)
    try:
        n_flat = conn.execute("SELECT COUNT(*) FROM tmp_events_flat").fetchone()[0]
        logger.info(f"  tmp_events_flat déjà disponible : {n_flat:,} événements")
    except Exception:
        logger.info("  Passe 1 — Création de tmp_events_flat...")
        conn.execute(SQL_EVENTS_FLAT_H)
        n_flat = conn.execute("SELECT COUNT(*) FROM tmp_events_flat").fetchone()[0]
        logger.info(f"  {n_flat:,} événements chargés")

    for sql in SQL_DRAW_FEATURES_STEPS:
        conn.execute(sql)
    n_df = conn.execute("SELECT COUNT(*) FROM tmp_draw_features").fetchone()[0]
    logger.info(f"  {n_df:,} lignes dans tmp_draw_features")

    conn.execute(SQL_PIVOT_DRAW)

    # Réutilise tmp_team_mapping si disponible (run après whoscored.py)
    try:
        conn.execute("SELECT COUNT(*) FROM tmp_team_mapping")
        logger.info("  tmp_team_mapping déjà disponible")
    except Exception:
        _inject_team_mapping(conn, "tmp_team_mapping")

    conn.execute(SQL_TEAM_HISTORY_DRAW)
    n_hist = conn.execute("SELECT COUNT(*) FROM tmp_draw_team_history").fetchone()[0]
    logger.info(f"  {n_hist:,} lignes dans tmp_draw_team_history")

    # Rolling moyenne + UPDATE
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_draw_rolling AS
        WITH ranked AS (
            SELECT ft.team AS team, ft.date AS ft_date, wsh.ws_date,
                wsh.had_late_equalizer, wsh.conceded_after_yellow,
                wsh.had_yellow_card, wsh.red_card_resilience,
                ROW_NUMBER() OVER (PARTITION BY ft.team, ft.date ORDER BY wsh.ws_date DESC) AS rn
            FROM gold.features_training ft
            JOIN tmp_draw_team_history wsh
                ON ft.team=wsh.team_name AND wsh.ws_date < ft.date AND ft.season=wsh.ws_season
        )
        SELECT team, ft_date,
            AVG(had_late_equalizer) FILTER (WHERE rn<={window}) AS ws_late_equalizer_rate,
            AVG(conceded_after_yellow) FILTER (WHERE rn<={window} AND had_yellow_card=1)
                AS ws_post_yellowcard_concede_rate,
            AVG(red_card_resilience) FILTER (WHERE rn<={window} AND red_card_resilience IS NOT NULL)
                AS ws_post_redcard_resilience
        FROM ranked WHERE rn<={window}
        GROUP BY team, ft_date
    """)

    conn.execute("""
        UPDATE gold.features_training AS ft SET
            ws_late_equalizer_rate          = dr.ws_late_equalizer_rate,
            ws_post_yellowcard_concede_rate = dr.ws_post_yellowcard_concede_rate,
            ws_post_redcard_resilience      = dr.ws_post_redcard_resilience
        FROM tmp_draw_rolling dr
        WHERE ft.team=dr.team AND ft.date=dr.ft_date
    """)

    n_updated = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training WHERE ws_late_equalizer_rate IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"  {n_updated:,} lignes mises à jour (features Behavior)")
    return n_updated


# ── Sous-pipeline B : Signals (F1–F20) ───────────────────────────────────────

def run_signals(conn: duckdb.DuckDBPyConnection, window: int = WINDOW) -> int:
    """
    Calcule les 20 features Draw Signals (F1–F20) et met à jour
    gold.features_training + gold.features_final (diffs).
    """
    logger.info("--- Sous-pipeline B : Draw Signals (F1–F20) ---")

    n_backbone = conn.execute("SELECT COUNT(*) FROM gold.stg_backbone").fetchone()[0]
    if n_backbone == 0:
        logger.warning("  gold.stg_backbone vide — sous-pipeline B ignoré")
        return 0

    # Team mapping dédié (nommé tmp_team_mapping_c pour éviter les conflits)
    _inject_team_mapping(conn, "tmp_team_mapping_c")

    # Passe 1 : agrégations backbone rolling
    logger.info("  Passe 1 — Agrégations rolling stg_backbone...")
    conn.execute(_build_sql_backbone_features(window))
    n_bf = conn.execute("SELECT COUNT(*) FROM tmp_backbone_features").fetchone()[0]
    logger.info(f"  {n_bf:,} lignes dans tmp_backbone_features")

    # Passe 2 : features WhoScored timing (F6, F13, F14)
    try:
        n_ws = conn.execute("SELECT COUNT(*) FROM silver.stg_whoscored_events").fetchone()[0]
        if n_ws > 0:
            logger.info("  Passe 2 — Agrégations WhoScored timing (F6/F13/F14)...")
            conn.execute(SQL_WS_TIMING_FEATURES)
        else:
            logger.warning("  stg_whoscored_events vide — F6/F13/F14 seront NULL")
            conn.execute("""
                CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
                SELECT CAST(NULL AS VARCHAR) AS ws_match_id, CAST(NULL AS INTEGER) AS team_id,
                       CAST(NULL AS INTEGER) AS total_goals, CAST(NULL AS DOUBLE) AS late_goal_pct,
                       CAST(NULL AS DOUBLE) AS goal_minute_stddev, CAST(NULL AS INTEGER) AS ht_draw
                WHERE FALSE
            """)
    except Exception as e:
        logger.warning(f"  WhoScored events inaccessible : {e}")
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
            SELECT CAST(NULL AS VARCHAR) AS ws_match_id, CAST(NULL AS INTEGER) AS team_id,
                   CAST(NULL AS INTEGER) AS total_goals, CAST(NULL AS DOUBLE) AS late_goal_pct,
                   CAST(NULL AS DOUBLE) AS goal_minute_stddev, CAST(NULL AS INTEGER) AS ht_draw
            WHERE FALSE
        """)

    # Passe 3 : calcul F1–F20
    logger.info("  Passe 3 — Calcul des features F1–F20...")
    conn.execute(_build_sql_ws_rolling(window))
    conn.execute(_build_sql_compute_features(window))
    n_fc = conn.execute("SELECT COUNT(*) FROM tmp_f_computed").fetchone()[0]
    logger.info(f"  {n_fc:,} lignes dans tmp_f_computed")

    conn.execute(SQL_UPDATE_TRAINING)
    n_updated = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training WHERE f1_mutual_cancel_idx IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"  {n_updated:,} lignes enrichies (features Signals)")

    try:
        conn.execute(SQL_UPDATE_FINAL_DIFFS)
        n_diff = conn.execute(
            "SELECT COUNT(*) FROM gold.features_final WHERE f7_mismatch_diff IS NOT NULL"
        ).fetchone()[0]
        logger.info(f"  {n_diff:,} lignes enrichies dans features_final (diffs F)")
    except Exception as e:
        logger.warning(f"  Différentiels features_final ignorés : {e}")

    return n_updated


# ── Pipeline principal ────────────────────────────────────────────────────────

def run_pipeline(
    reset_cols:    bool = False,
    coverage_only: bool = False,
    window:        int  = WINDOW,
    step:          str  = "all",  # "all" | "behavior" | "signals"
) -> None:
    logger.info("═══ Pipeline draw — Draw Behavior + Draw Signals ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    _tmp_dir = Path(tempfile.gettempdir()) / "duckdb_draw_tmp"
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(f"SET temp_directory='{_tmp_dir.as_posix()}'")

    try:
        n_events  = conn.execute("SELECT COUNT(*) FROM silver.stg_whoscored_events").fetchone()[0]
        n_train   = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
        n_backbone = conn.execute("SELECT COUNT(*) FROM gold.stg_backbone").fetchone()[0]
        logger.info(f"  stg_whoscored_events   : {n_events:,}")
        logger.info(f"  gold.features_training : {n_train:,}")
        logger.info(f"  gold.stg_backbone      : {n_backbone:,}")
    except Exception as e:
        logger.error(f"  Prérequis manquant : {e}")
        conn.close()
        raise

    if coverage_only:
        _print_coverage(conn)
        conn.close()
        return

    _add_columns(conn)
    if reset_cols:
        _reset_columns(conn)

    if step in ("all", "behavior") and n_events > 0:
        run_behavior(conn, window)
    elif step == "behavior" and n_events == 0:
        logger.warning("  Aucun événement WhoScored — Behavior ignoré")

    if step in ("all", "signals"):
        run_signals(conn, window)

    _print_coverage(conn)
    conn.close()
    logger.success("═══ Pipeline draw terminé ═══")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Draw Behavior + Draw Signals")
    parser.add_argument("--step",          default="all",
                        choices=["all", "behavior", "signals"])
    parser.add_argument("--reset-cols",    action="store_true")
    parser.add_argument("--coverage-only", action="store_true")
    parser.add_argument("--window",        type=int, default=WINDOW)
    args = parser.parse_args()
    run_pipeline(
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
        window=args.window,
        step=args.step,
    )