"""
Pipeline 03c — Feature Engineering : Signaux Nul / Victoire / Failles
======================================================================
Génère les features avancées définies dans le catalogue "Projet 3-Étoiles"
pour améliorer la détection des Matchs Nuls (N), Victoires Domicile (1)
et Extérieur (2).

ARCHITECTURE 3 PASSES (identique à 03b)
─────────────────────────────────────────
  Passe 1 — Agrégations Gold  : lectures depuis gold.stg_backbone (rolling)
  Passe 2 — Agrégations WS    : nouvelles métriques depuis stg_whoscored_events
  Passe 3 — Différentiels     : UPDATE gold.features_final avec les diffs

CATALOGUE DES FEATURES (F1–F20)
────────────────────────────────
  ── AXE 1 : Détecteurs de "Match Bloqué" (signal Nul) ──────────────────────
  F1  — mutual_cancellation_index    : sterility_team × sterility_opp / save_rate_sum
  F2  — defensive_mirror_score       : alignement axe attaque ≈ axe défense adverse
  F3  — draw_market_prior_deviation  : pinnacle_prob_draw - league_draw_rate
  F4  — momentum_convergence         : |momentum_delta_team - momentum_delta_opp|
  F5  — clean_sheet_mutual_rate      : cs_rate_5_team × cs_rate_5_opp
  F6  — half_time_draw_tendency      : % matchs récents égalité à 45' ET nul final

  ── AXE 2 : Domination Relative (signal Victoire 1 / 2) ────────────────────
  F7  — offensive_defensive_mismatch : season_att_team - season_def_opp (et inverse)
  F8  — press_dominance_ratio        : log(opp_ppda / team_ppda)
  F9  — chance_quality_gap           : shot_quality_ratio_5_team - shot_quality_ratio_5_opp
  F10 — venue_power_adjusted         : xG_venue_5 différentiel (home vs away performance)

  ── AXE 3 : Résilience & Psychologie du Score ──────────────────────────────
  F11 — comeback_rate                : % matchs récents avec retour au score
  F12 — red_card_resilience          : points gagnés après carton rouge récent
  F13 — late_goal_tendency           : % buts marqués après la 75e minute
  F14 — goal_timing_variance         : écart-type des minutes de buts marqués

  ── AXE 4 : Efficacité / Yield ─────────────────────────────────────────────
  F15 — xg_yield_ratio               : rolling_gf_5 / np_xg_roll_5 (surperformance)
  F16 — defensive_yield_ratio        : rolling_ga_5 / np_xg_conceded_roll_5
  F17 — shots_to_goal_efficiency     : gf_5 / shots_5 (conversion brute)
  F18 — sot_conversion_gap           : gf_5 / shots_on_target_5 (gardien vs finisseur)

  ── AXE 5 : Features Composites Signatures ─────────────────────────────────
  F19 — tactical_lock_index          : stérilité × équilibre pressing × égalité territoriale
  F20 — upset_probability_composite  : (1/prob_team) × yield_adverse × comeback_opp

DIFFÉRENTIELS dans gold.features_final
────────────────────────────────────────
  Chaque feature est déclinée en différentiel team - opp dans features_final
  pour maximiser le signal LGBM (un seul split = toute l'asymétrie).

ANTI-LEAKAGE
─────────────
  ⚠️  Toutes les features rolling utilisent uniquement les matchs STRICTEMENT
  ANTÉRIEURS à la date du match courant (h.date < t.date).
  Les features WhoScored (F6, F13, F14) utilisent le match N-1 via LAG(1).

Usage :
    python pipelines/03c_features_draw_signals.py
    python pipelines/03c_features_draw_signals.py --reset-cols
    python pipelines/03c_features_draw_signals.py --coverage-only
"""

import argparse
from pathlib import Path
import tempfile

import duckdb
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
import os
os.chdir(Path(__file__).resolve().parent.parent)

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["db"])

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_draw_signals.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

WINDOW     = CFG.get("features", {}).get("form_window", 5)
H2H_WINDOW = CFG.get("features", {}).get("h2h_window", 10)

# ── Mapping team_mapping (identique à 03b) ────────────────────────────────────
_RAW_MAPPING: dict = CFG.get("team_mapping", {})
TEAM_MAPPING_ROWS: list = [
    (str(raw), str(canonical))
    for raw, canonical in _RAW_MAPPING.items()
    if raw and canonical
]

# ── WhoScored type_id & qualifiers (référence 03b) ───────────────────────────
# type_id 16 + outcome_id 1 + is_shot = but marqué
# type_id 7  = tackle, 8 = interception, 12 = clearance
# qualifier 26 = counter_attack

# ─────────────────────────────────────────────────────────────────────────────
# COLONNES AJOUTÉES À gold.features_training
# ─────────────────────────────────────────────────────────────────────────────
NEW_COLS = [
    # ── AXE 1 — Détecteurs Nul ───────────────────────────────────────────────
    ("f1_mutual_cancel_idx",       "DOUBLE"),  # stérilité croisée × save_rate
    ("f2_defensive_mirror",        "DOUBLE"),  # alignement axe attaque vs défense adverse
    ("f3_draw_market_dev",         "DOUBLE"),  # pinnacle_draw - league_draw_rate
    ("f4_momentum_convergence",    "DOUBLE"),  # |momentum_delta_team - momentum_delta_opp|
    ("f5_cs_mutual_rate",          "DOUBLE"),  # clean_sheet_rate_5 × cs_rate_5_opp
    ("f6_ht_draw_tendency",        "DOUBLE"),  # % matchs récents : égalité mi-temps ET nul final

    # ── AXE 2 — Domination Relative ──────────────────────────────────────────
    ("f7_off_def_mismatch",        "DOUBLE"),  # season_att_team - season_def_opp
    ("f7_def_off_mismatch",        "DOUBLE"),  # season_att_opp  - season_def_team (vulnérabilité)
    ("f8_press_dominance_ratio",   "DOUBLE"),  # log(opp_ppda / team_ppda)
    ("f9_chance_quality_gap",      "DOUBLE"),  # sqr_5_team - sqr_5_opp
    ("f10_venue_power_adj",        "DOUBLE"),  # xG_venue_5 - xG_global_5 (prime/malus de lieu)

    # ── AXE 3 — Résilience & Psychologie ─────────────────────────────────────
    ("f11_comeback_rate",          "DOUBLE"),  # % matchs récents avec retour au score
    ("f12_red_card_resilience",    "DOUBLE"),  # pts gagnés / matchs avec carton rouge récent
    ("f13_late_goal_tendency",     "DOUBLE"),  # % buts après la 75e (source WhoScored events)
    ("f14_goal_timing_variance",   "DOUBLE"),  # écart-type minute des buts marqués (WS events)

    # ── AXE 4 — Yield / Efficacité ───────────────────────────────────────────
    ("f15_xg_yield_ratio",         "DOUBLE"),  # gf_5 / np_xg_5 (surperformance offensive)
    ("f16_def_yield_ratio",        "DOUBLE"),  # ga_5 / np_xg_conceded_5 (surperformance défensive)
    ("f17_shots_to_goal_eff",      "DOUBLE"),  # gf_5 / shots_total_5
    ("f18_sot_conversion",         "DOUBLE"),  # gf_5 / shots_on_target_5

    # ── AXE 5 — Composites Signatures ────────────────────────────────────────
    ("f19_tactical_lock_idx",      "DOUBLE"),  # triple verrou : stérilité × pressing × territoire
    ("f20_upset_composite",        "DOUBLE"),  # (1/prob_team) × yield_adverse × comeback_opp
]

# ── Colonnes différentielles ajoutées à gold.features_final ──────────────────
DIFF_COLS = [
    ("f1_mutual_cancel_diff",      "DOUBLE"),  # > 0 = équipe plus susceptible de subir un blocage
    ("f7_mismatch_diff",           "DOUBLE"),  # off_mismatch_team - off_mismatch_opp (domination nette)
    ("f8_press_dominance_diff",    "DOUBLE"),  # pressing supérieur vs adversaire
    ("f9_chance_quality_diff",     "DOUBLE"),  # qualité de chance supérieure
    ("f10_venue_power_diff",       "DOUBLE"),  # prime de lieu relative
    ("f11_comeback_diff",          "DOUBLE"),  # résilience mentale relative
    ("f13_late_goal_diff",         "DOUBLE"),  # danger tardif relatif
    ("f15_xg_yield_diff",          "DOUBLE"),  # surperformance xG relative (régression attendue)
    ("f16_def_yield_diff",         "DOUBLE"),  # surperformance défensive relative
    ("f19_tactical_lock_diff",     "DOUBLE"),  # qui "verrouille" le plus
    ("f20_upset_diff",             "DOUBLE"),  # potentiel upset relatif
]


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 1 — Features Gold (depuis stg_backbone, rolling)
# Calcule les features F1–F20 qui dépendent de l'historique gold.stg_backbone
# ─────────────────────────────────────────────────────────────────────────────

SQL_BACKBONE_FEATURES = f"""
CREATE OR REPLACE TEMP TABLE tmp_backbone_features AS

WITH

-- ══════════════════════════════════════════════════════════════════════════
-- BASE : historique match par match depuis stg_backbone
-- Grain : une ligne par (date, team, league_source)
-- Anti-leakage : toutes les jointures utilisent h.date < t.date
-- ══════════════════════════════════════════════════════════════════════════
base AS (
    SELECT DISTINCT
        date,
        team,
        opponent,
        league_source,
        venue,
        season
    FROM gold.stg_backbone
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1A — Rolling stats offensives/défensives sur fenêtre W={WINDOW}
-- ══════════════════════════════════════════════════════════════════════════
rolling_stats AS (
    SELECT
        t.date,
        t.team,
        t.league_source,
        t.venue,
        t.opponent,

        -- Buts pour/contre rolling
        AVG(h.gf)                                         AS avg_gf_5,
        AVG(h.ga)                                         AS avg_ga_5,

        -- xG rolling
        AVG(h.np_xg)                                      AS avg_xg_5,
        AVG(h.np_xg_conceded)                             AS avg_xg_conceded_5,

        -- Tirs rolling
        AVG(h.shots_total)                                AS avg_shots_5,
        AVG(h.shots_on_target)                            AS avg_sot_5,

        -- Shot Quality Ratio rolling (= np_xg / shots = xG moyen par tir)
        AVG(
            CASE WHEN h.shots_total > 0
            THEN h.np_xg / CAST(h.shots_total AS DOUBLE)
            END
        )                                                 AS sqr_5,

        -- Clean sheet rate rolling
        AVG(CAST(h.clean_sheet AS DOUBLE))                AS cs_rate_5,

        -- Save rate rolling
        AVG(h.save_pct)                                   AS avg_save_rate_5,

        -- PPDA rolling (pressing intensity)
        AVG(h.ppda)                                       AS avg_ppda_5,

        -- Cartons rouges rolling
        AVG(CAST(h.red_cards AS DOUBLE))                  AS avg_red_cards_5,
        -- Flag : a eu un carton rouge lors du match
        AVG(CASE WHEN h.red_cards > 0 THEN 1.0 ELSE 0.0 END)
                                                          AS red_card_rate_5,
        -- Points gagnés lors des matchs avec carton rouge
        SUM(CASE
            WHEN h.red_cards > 0 AND h.result_1n2 = 'W' THEN 3.0
            WHEN h.red_cards > 0 AND h.result_1n2 = 'D' THEN 1.0
            ELSE 0.0
        END)                                              AS pts_with_red_card,
        SUM(CASE WHEN h.red_cards > 0 THEN 1.0 ELSE 0.0 END)
                                                          AS n_matches_with_red,

        -- Stérilité offensive (= faible taux de conversion)
        AVG(
            CASE WHEN h.shots_total > 0
            THEN 1.0 - (h.gf / CAST(h.shots_total AS DOUBLE))
            END
        )                                                 AS sterility_5,

        -- Résultats récents (pour come-back et résilience)
        COUNT(*) FILTER (WHERE h.result_1n2 = 'W')       AS wins_5,
        COUNT(*) FILTER (WHERE h.result_1n2 = 'D')       AS draws_5,
        COUNT(*)                                          AS n_matches_5,

        -- Saisonnier (rating)
        MAX(h.season_att_rating)                          AS season_att_rating,
        MAX(h.season_def_rating)                          AS season_def_rating

    FROM base t
    JOIN gold.stg_backbone h
        ON  h.team          = t.team
        AND h.league_source = t.league_source
        AND h.date          < t.date           -- anti-leakage strict
    WHERE h.date >= (t.date - INTERVAL '{WINDOW * 7} days')
      AND h.np_xg IS NOT NULL                  -- on ne compte que les matchs avec xG
    GROUP BY t.date, t.team, t.league_source, t.venue, t.opponent
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1B — xG venue-aware rolling (prime/malus domicile vs extérieur)
-- Capture si une équipe est plus/moins dangereuse dans sa venue actuelle
-- ══════════════════════════════════════════════════════════════════════════
rolling_venue AS (
    SELECT
        t.date,
        t.team,
        t.league_source,
        t.venue,
        AVG(h.np_xg)                                      AS avg_xg_venue_5
    FROM base t
    JOIN gold.stg_backbone h
        ON  h.team          = t.team
        AND h.league_source = t.league_source
        AND h.venue         = t.venue          -- même venue uniquement
        AND h.date          < t.date
    WHERE h.date >= (t.date - INTERVAL '{WINDOW * 14} days')  -- fenêtre élargie (venue = moins de matchs)
      AND h.np_xg IS NOT NULL
    GROUP BY t.date, t.team, t.league_source, t.venue
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1C — Taux de nul de la ligue (rolling par saison)
-- Nécessaire pour F3 : pinnacle_draw - league_draw_rate
-- ══════════════════════════════════════════════════════════════════════════
league_draw_rate AS (
    SELECT
        t.date,
        t.team,
        t.league_source,
        t.season,
        AVG(CASE WHEN h.result_1n2 = 'D' THEN 1.0 ELSE 0.0 END)
                                                          AS league_draw_rate
    FROM base t
    JOIN gold.stg_backbone h
        ON  h.league_source = t.league_source
        AND h.season        = t.season
        AND h.date          < t.date
    GROUP BY t.date, t.team, t.league_source, t.season
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1D — Come-back rate : % matchs récents avec retour au score
-- Définition : équipe menait ou était à égalité, puis a été menée,
-- et a terminé nul ou gagnante (ou était menée puis a rattrapé)
-- Proxy via : (résultat final W ou D) ET (ga > 0 = a concédé un but)
-- = l'équipe a montré une capacité à résister sous pression
-- ══════════════════════════════════════════════════════════════════════════
comeback_stats AS (
    SELECT
        t.date,
        t.team,
        t.league_source,
        -- Proxy come-back : matchs où l'équipe a concédé ET obtenu au moins 1 point
        AVG(CASE
            WHEN h.ga > 0 AND h.result_1n2 IN ('W', 'D') THEN 1.0
            ELSE 0.0
        END)                                              AS comeback_rate
    FROM base t
    JOIN gold.stg_backbone h
        ON  h.team          = t.team
        AND h.league_source = t.league_source
        AND h.date          < t.date
    WHERE h.date >= (t.date - INTERVAL '{WINDOW * 7} days')
    GROUP BY t.date, t.team, t.league_source
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1E — Odds & proba marché (depuis gold.stg_backbone)
-- Nécessaire pour F3, F20
-- ══════════════════════════════════════════════════════════════════════════
market_probs AS (
    SELECT
        date,
        team,
        league_source,
        pinnacle_prob_draw,
        pinnacle_prob_team,
        market_prob_draw
    FROM gold.stg_backbone
)

-- ══════════════════════════════════════════════════════════════════════════
-- ASSEMBLAGE FINAL (grain : date × team × league_source)
-- ══════════════════════════════════════════════════════════════════════════
SELECT
    b.date,
    b.team,
    b.opponent,
    b.league_source,
    b.venue,
    b.season,

    -- ── Features rolling (base) ──────────────────────────────────────────
    r.avg_gf_5,
    r.avg_ga_5,
    r.avg_xg_5,
    r.avg_xg_conceded_5,
    r.avg_shots_5,
    r.avg_sot_5,
    r.sqr_5,
    r.cs_rate_5,
    r.avg_save_rate_5,
    r.avg_ppda_5,
    r.red_card_rate_5,
    r.pts_with_red_card,
    r.n_matches_with_red,
    r.sterility_5,
    r.wins_5,
    r.draws_5,
    r.n_matches_5,
    r.season_att_rating,
    r.season_def_rating,

    -- ── Venue-aware ──────────────────────────────────────────────────────
    v.avg_xg_venue_5,

    -- ── Ligue ────────────────────────────────────────────────────────────
    ld.league_draw_rate,

    -- ── Come-back ────────────────────────────────────────────────────────
    cb.comeback_rate,

    -- ── Marché ───────────────────────────────────────────────────────────
    mp.pinnacle_prob_draw,
    mp.pinnacle_prob_team,
    mp.market_prob_draw

FROM base b
LEFT JOIN rolling_stats  r  ON  b.date = r.date
                            AND b.team = r.team
                            AND b.league_source = r.league_source
LEFT JOIN rolling_venue  v  ON  b.date = v.date
                            AND b.team = v.team
                            AND b.league_source = v.league_source
                            AND b.venue = v.venue
LEFT JOIN league_draw_rate ld ON b.date = ld.date
                             AND b.team = ld.team
                             AND b.league_source = ld.league_source
LEFT JOIN comeback_stats cb   ON b.date = cb.date
                             AND b.team = cb.team
                             AND b.league_source = cb.league_source
LEFT JOIN market_probs   mp   ON b.date = mp.date
                             AND b.team = mp.team
                             AND b.league_source = mp.league_source
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 2 — Features WhoScored Events (F6, F13, F14)
# Ces features requièrent les events WhoScored bruts (minutes de buts, etc.)
# ─────────────────────────────────────────────────────────────────────────────

SQL_WS_TIMING_FEATURES = """
CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
WITH

-- ══════════════════════════════════════════════════════════════════════════
-- Buts marqués par (ws_match_id, team_id) avec leur minute
-- type_id=16, outcome_id=1, is_shot=TRUE = but
-- ══════════════════════════════════════════════════════════════════════════
goals_scored AS (
    SELECT
        ws_match_id,
        team_id,
        expanded_minute,
        -- Flag : but tardif (après 75e minute)
        CASE WHEN expanded_minute > 75 THEN 1 ELSE 0 END  AS is_late_goal
    FROM silver.stg_whoscored_events
    WHERE type_id    = 16
      AND outcome_id = 1
      AND is_shot    = TRUE
),

-- ══════════════════════════════════════════════════════════════════════════
-- F13 — Late Goal Tendency : % buts après la 75e
-- et F14 — Goal Timing Variance : écart-type de la minute des buts
-- ══════════════════════════════════════════════════════════════════════════
goal_timing AS (
    SELECT
        ws_match_id,
        team_id,
        COUNT(*)                                           AS total_goals,
        -- F13 : proportion de buts tardifs
        AVG(CAST(is_late_goal AS DOUBLE))                 AS late_goal_pct,
        -- F14 : variance temporelle des buts (écart-type des minutes)
        STDDEV(CAST(expanded_minute AS DOUBLE))           AS goal_minute_stddev
    FROM goals_scored
    GROUP BY ws_match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════
-- F6 — Half-Time Draw Tendency
-- Identifier les matchs où il y avait égalité à la mi-temps
-- Source : stg_whoscored_match_details (ht_score_home, ht_score_away)
-- On calcule le % de matchs récents : égalité HT + nul final
-- Ce calcul est fait ici par ws_match_id pour rolling ultérieur
-- ══════════════════════════════════════════════════════════════════════════
ht_situation AS (
    SELECT
        mi.ws_match_id,
        mi.home_team_id,
        mi.away_team_id,
        -- Égalité à la mi-temps ?
        CASE WHEN md.ht_score_home = md.ht_score_away THEN 1 ELSE 0 END
                                                          AS ht_draw
    FROM silver.stg_whoscored_match_index mi
    LEFT JOIN silver.stg_whoscored_match_details md
        ON mi.ws_match_id = md.ws_match_id
    WHERE md.ht_score_home IS NOT NULL
)

-- Assemblage par (ws_match_id, team_id)
SELECT
    gt.ws_match_id,
    gt.team_id,
    gt.total_goals,
    gt.late_goal_pct,
    gt.goal_minute_stddev,
    -- HT draw flag (même valeur pour home et away)
    ht.ht_draw
FROM goal_timing gt
LEFT JOIN ht_situation ht
    ON gt.ws_match_id = ht.ws_match_id
    AND (gt.team_id = ht.home_team_id OR gt.team_id = ht.away_team_id)
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 3 — Calcul des features finales F1–F20 + UPDATE gold.features_training
# On joint tmp_backbone_features (team) avec tmp_backbone_features (opponent)
# pour avoir les valeurs croisées nécessaires aux features composites.
# ─────────────────────────────────────────────────────────────────────────────

SQL_COMPUTE_FEATURES = f"""
CREATE OR REPLACE TEMP TABLE tmp_f_computed AS
WITH

-- ══════════════════════════════════════════════════════════════════════════
-- Auto-jointure : team × opponent sur (date, league_source)
-- Permet de calculer les différentiels et composites sans quitter SQL
-- ══════════════════════════════════════════════════════════════════════════
team_vs_opp AS (
    SELECT
        t.date,
        t.team,
        t.league_source,
        t.venue,

        -- ── Valeurs équipe ────────────────────────────────────────────────
        t.avg_gf_5,
        t.avg_ga_5,
        t.avg_xg_5,
        t.avg_xg_conceded_5,
        t.avg_shots_5,
        t.avg_sot_5,
        t.sqr_5,
        t.cs_rate_5,
        t.avg_save_rate_5,
        t.avg_ppda_5,
        t.red_card_rate_5,
        t.pts_with_red_card,
        t.n_matches_with_red,
        t.sterility_5,
        t.n_matches_5,
        t.season_att_rating,
        t.season_def_rating,
        t.avg_xg_venue_5,
        t.league_draw_rate,
        t.comeback_rate,
        t.pinnacle_prob_draw,
        t.pinnacle_prob_team,

        -- ── Valeurs adversaire ────────────────────────────────────────────
        o.avg_gf_5                   AS opp_avg_gf_5,
        o.avg_xg_5                   AS opp_avg_xg_5,
        o.avg_xg_conceded_5          AS opp_avg_xg_conceded_5,
        o.sqr_5                      AS opp_sqr_5,
        o.cs_rate_5                  AS opp_cs_rate_5,
        o.avg_save_rate_5            AS opp_avg_save_rate_5,
        o.avg_ppda_5                 AS opp_avg_ppda_5,
        o.sterility_5                AS opp_sterility_5,
        o.season_att_rating          AS opp_season_att_rating,
        o.season_def_rating          AS opp_season_def_rating,
        o.comeback_rate              AS opp_comeback_rate,
        o.pinnacle_prob_team         AS opp_pinnacle_prob_team,

        -- WhoScored features existantes (depuis features_training)
        ft.ws_momentum_delta,
        ft_opp.ws_momentum_delta     AS opp_ws_momentum_delta,
        ft.ws_zone_att_pct,
        ft_opp.ws_zone_att_pct       AS opp_ws_zone_att_pct

    FROM tmp_backbone_features t
    -- Jointure adversaire (même match = même (date, league_source), opponent inversé)
    LEFT JOIN tmp_backbone_features o
        ON  t.date          = o.date
        AND t.opponent      = o.team
        AND t.league_source = o.league_source
    -- Jointure features_training pour récupérer les ws_* existantes
    LEFT JOIN gold.features_training ft
        ON  ft.date          = t.date
        AND ft.team          = t.team
        AND ft.league_source = t.league_source
    LEFT JOIN gold.features_training ft_opp
        ON  ft_opp.date          = t.date
        AND ft_opp.team          = t.opponent
        AND ft_opp.league_source = t.league_source
),

-- ══════════════════════════════════════════════════════════════════════════
-- Jointure WhoScored timing (F13, F14, F6)
-- Rolling sur les derniers matchs WhoScored de l'équipe
-- ══════════════════════════════════════════════════════════════════════════
ws_rolling AS (
    SELECT
        t.date,
        t.team,
        t.league_source,

        -- F13 — Late Goal Tendency (rolling {WINDOW} derniers matchs WS)
        AVG(wst.late_goal_pct)                            AS late_goal_tendency,

        -- F14 — Goal Timing Variance
        AVG(wst.goal_minute_stddev)                       AS goal_timing_variance,

        -- F6 — HT Draw Tendency : % matchs avec égalité HT
        -- (on ne peut pas filtrer "ET nul final" sans leakage ici)
        -- → on prend seulement le flag HT comme signal prospectif
        AVG(CAST(wst.ht_draw AS DOUBLE))                  AS ht_draw_rate

    FROM (SELECT DISTINCT date, team, league_source FROM gold.stg_backbone) t
    JOIN silver.stg_whoscored_match_index mi
        ON  mi.match_date < t.date
        AND (
            (COALESCE((SELECT canonical_name FROM tmp_team_mapping_c
                       WHERE raw_name = mi.home_team_name LIMIT 1),
                       mi.home_team_name) = t.team)
         OR
            (COALESCE((SELECT canonical_name FROM tmp_team_mapping_c
                       WHERE raw_name = mi.away_team_name LIMIT 1),
                       mi.away_team_name) = t.team)
        )
    LEFT JOIN tmp_ws_timing wst
        ON wst.ws_match_id = mi.ws_match_id
        AND (
            (mi.home_team_id = wst.team_id AND
             COALESCE((SELECT canonical_name FROM tmp_team_mapping_c
                       WHERE raw_name = mi.home_team_name LIMIT 1),
                       mi.home_team_name) = t.team)
         OR
            (mi.away_team_id = wst.team_id AND
             COALESCE((SELECT canonical_name FROM tmp_team_mapping_c
                       WHERE raw_name = mi.away_team_name LIMIT 1),
                       mi.away_team_name) = t.team)
        )
    GROUP BY t.date, t.team, t.league_source
)

-- ══════════════════════════════════════════════════════════════════════════
-- CALCUL FINAL DES FEATURES F1–F20
-- ══════════════════════════════════════════════════════════════════════════
SELECT
    tvo.date,
    tvo.team,
    tvo.league_source,

    -- ─────────────────────────────────────────────────────────────────────
    -- AXE 1 — Détecteurs de Match Bloqué
    -- ─────────────────────────────────────────────────────────────────────

    -- F1 — Mutual Cancellation Index
    -- = (stérilité_équipe × stérilité_adversaire) / (save_rate_team + save_rate_opp + ε)
    -- Signal : deux équipes incapables de marquer + gardiens en forme = 0-0 probable
    -- ↑ = risque nul élevé (double blocage offensif + filet bien gardé)
    CASE
        WHEN (tvo.avg_save_rate_5 + tvo.opp_avg_save_rate_5) > 0
         AND tvo.sterility_5 IS NOT NULL
         AND tvo.opp_sterility_5 IS NOT NULL
        THEN (tvo.sterility_5 * tvo.opp_sterility_5)
           / (tvo.avg_save_rate_5 + tvo.opp_avg_save_rate_5 + 0.01)
    END                                                   AS f1_mutual_cancel_idx,

    -- F2 — Defensive Mirror Score
    -- = 1 - |attack_center_team - zone_mid_opp| normalisé
    -- Quand l'équipe attaque là où l'adversaire est le plus présent défensivement
    -- → les deux équipes se neutralisent tactiquement dans l'axe
    -- Utilise ws_zone_mid_pct (présence au milieu adverse) comme proxy défensif
    CASE
        WHEN tvo.ws_zone_att_pct IS NOT NULL
         AND tvo.opp_ws_zone_att_pct IS NOT NULL
        THEN 1.0 - ABS(tvo.ws_zone_att_pct - tvo.opp_ws_zone_att_pct)
    END                                                   AS f2_defensive_mirror,

    -- F3 — Draw Market Prior Deviation
    -- = pinnacle_prob_draw - league_draw_rate
    -- > 0 = marché surestime le nul dans cette ligue (value bet possible côté 1 ou 2)
    -- < 0 = marché sous-estime le nul → valeur sur le X
    CASE
        WHEN tvo.pinnacle_prob_draw IS NOT NULL
         AND tvo.league_draw_rate IS NOT NULL
        THEN tvo.pinnacle_prob_draw - tvo.league_draw_rate
    END                                                   AS f3_draw_market_dev,

    -- F4 — Momentum Convergence
    -- = |momentum_delta_team - momentum_delta_opp| (inversé : 0 = convergence totale)
    -- Faible différentiel = les deux équipes réagissent identiquement aux buts encaissés
    -- → aucune n'a pris l'ascendant psychologique → signal nul
    CASE
        WHEN tvo.ws_momentum_delta IS NOT NULL
         AND tvo.opp_ws_momentum_delta IS NOT NULL
        THEN 1.0 - ABS(tvo.ws_momentum_delta - tvo.opp_ws_momentum_delta)
    END                                                   AS f4_momentum_convergence,

    -- F5 — Clean Sheet Mutual Rate
    -- = cs_rate_5_team × cs_rate_5_opp
    -- Produit des taux de clean sheets récents des deux équipes
    -- ↑ = probabilité structurelle de 0-0 ou 1-1 → signal fort de nul
    CASE
        WHEN tvo.cs_rate_5 IS NOT NULL
         AND tvo.opp_cs_rate_5 IS NOT NULL
        THEN tvo.cs_rate_5 * tvo.opp_cs_rate_5
    END                                                   AS f5_cs_mutual_rate,

    -- F6 — Half-Time Draw Tendency (source WhoScored events)
    -- = % matchs récents avec score nul à la mi-temps
    -- Équipes qui arrivent souvent à égalité à la pause ont un ADN "bloqueur"
    wr.ht_draw_rate                                       AS f6_ht_draw_tendency,

    -- ─────────────────────────────────────────────────────────────────────
    -- AXE 2 — Domination Relative
    -- ─────────────────────────────────────────────────────────────────────

    -- F7a — Offensive vs Defensive Mismatch (avantage offensif)
    -- = season_att_team - season_def_opp
    -- > 0 = l'attaque de l'équipe surclasse la défense adverse → victoire attendue
    CASE
        WHEN tvo.season_att_rating IS NOT NULL
         AND tvo.opp_season_def_rating IS NOT NULL
        THEN tvo.season_att_rating - tvo.opp_season_def_rating
    END                                                   AS f7_off_def_mismatch,

    -- F7b — Défense vs Attaque adverse (vulnérabilité)
    -- = season_att_opp - season_def_team
    -- > 0 = l'attaque adverse surclasse notre défense → on est en danger
    CASE
        WHEN tvo.opp_season_att_rating IS NOT NULL
         AND tvo.season_def_rating IS NOT NULL
        THEN tvo.opp_season_att_rating - tvo.season_def_rating
    END                                                   AS f7_def_off_mismatch,

    -- F8 — Press Dominance Ratio
    -- = log(opp_ppda / team_ppda)
    -- PPDA faible = pressing intensif ; ratio log > 0 = pression supérieure
    -- Signal : équipe qui presse bien vs équipe qui subit → domination de transition
    CASE
        WHEN tvo.avg_ppda_5 > 0
         AND tvo.opp_avg_ppda_5 > 0
        THEN LN(tvo.opp_avg_ppda_5 / tvo.avg_ppda_5)
    END                                                   AS f8_press_dominance_ratio,

    -- F9 — Chance Quality Gap
    -- = sqr_5_team - sqr_5_opp (Shot Quality Ratio = np_xg / tirs)
    -- > 0 = l'équipe crée des occasions de meilleure qualité
    -- Signal fort de victoire, indépendant du volume de tirs
    CASE
        WHEN tvo.sqr_5 IS NOT NULL
         AND tvo.opp_sqr_5 IS NOT NULL
        THEN tvo.sqr_5 - tvo.opp_sqr_5
    END                                                   AS f9_chance_quality_gap,

    -- F10 — Venue Power Adjusted
    -- = avg_xg_venue_5 - avg_xg_5 (écart venue vs global)
    -- > 0 = équipe surperforme à domicile / en déplacement selon sa venue actuelle
    -- Signal : certaines équipes sont bien plus dangereuses dans leur venue (fort effet domicile)
    CASE
        WHEN tvo.avg_xg_venue_5 IS NOT NULL
         AND tvo.avg_xg_5 IS NOT NULL
        THEN tvo.avg_xg_venue_5 - tvo.avg_xg_5
    END                                                   AS f10_venue_power_adj,

    -- ─────────────────────────────────────────────────────────────────────
    -- AXE 3 — Résilience & Psychologie du Score
    -- ─────────────────────────────────────────────────────────────────────

    -- F11 — Come-back Rate
    -- = % matchs récents où l'équipe a concédé ET pris au moins 1 point
    -- Signal : résilience mentale — crucial pour les matchs nuls et les upsets
    tvo.comeback_rate                                     AS f11_comeback_rate,

    -- F12 — Red Card Resilience
    -- = pts gagnés lors des matchs avec carton rouge / n matchs avec carton rouge
    -- Une équipe bien organisée défensivement peut tenir à 10 → signal nul fort
    CASE
        WHEN tvo.n_matches_with_red > 0
        THEN tvo.pts_with_red_card / tvo.n_matches_with_red
    END                                                   AS f12_red_card_resilience,

    -- F13 — Late Goal Tendency (source WhoScored events)
    -- = % de buts marqués après la 75e minute
    -- Équipes qui marquent tard = danger persistant jusqu'au coup de sifflet final
    -- Signal upset : si l'outsider marque souvent tard = risque de renversement
    wr.late_goal_tendency                                 AS f13_late_goal_tendency,

    -- F14 — Goal Timing Variance (source WhoScored events)
    -- = écart-type des minutes de buts marqués sur les N derniers matchs
    -- Haute variance = équipe imprévisible (marque n'importe quand)
    -- Faible variance = équipe à pattern temporel (ex : toujours en fin de match)
    wr.goal_timing_variance                               AS f14_goal_timing_variance,

    -- ─────────────────────────────────────────────────────────────────────
    -- AXE 4 — Efficacité / Yield
    -- ─────────────────────────────────────────────────────────────────────

    -- F15 — xG Yield Ratio (surperformance offensive)
    -- = avg_gf_5 / avg_xg_5
    -- > 1.0 = équipe marque plus que son xG → chanceuse → régression attendue
    -- < 0.8 = équipe sous-performe → explosion imminente possible
    -- Signal nul : si les DEUX équipes yielden > 1 → buts probables → pas 0-0
    -- Signal upset : si team yield > 1 ET opp yield < 0.8 → upset probable
    CASE
        WHEN tvo.avg_xg_5 > 0
        THEN tvo.avg_gf_5 / tvo.avg_xg_5
    END                                                   AS f15_xg_yield_ratio,

    -- F16 — Defensive Yield Ratio (surperformance défensive)
    -- = avg_ga_5 / avg_xg_conceded_5
    -- < 1.0 = équipe concède moins que son xGA → gardien performant ou chance → régression
    -- > 1.2 = équipe concède plus que son xGA → défense vulnérable malgré les stats
    CASE
        WHEN tvo.avg_xg_conceded_5 > 0
        THEN tvo.avg_ga_5 / tvo.avg_xg_conceded_5
    END                                                   AS f16_def_yield_ratio,

    -- F17 — Shots to Goal Efficiency
    -- = avg_gf_5 / avg_shots_5 (conversion brute sans xG)
    -- Utile pour les équipes/ligues avec xG peu fiable
    -- Différentiel élevé = équipe clinique → signal victoire
    CASE
        WHEN tvo.avg_shots_5 > 0
        THEN tvo.avg_gf_5 / tvo.avg_shots_5
    END                                                   AS f17_shots_to_goal_eff,

    -- F18 — Shot on Target Conversion
    -- = avg_gf_5 / avg_sot_5 (buts par tir cadré)
    -- Plus fin que F17 : élimine les tirs hors cadre
    -- Signal fort : équipe avec high sot_conversion = finishing de qualité
    CASE
        WHEN tvo.avg_sot_5 > 0
        THEN tvo.avg_gf_5 / tvo.avg_sot_5
    END                                                   AS f18_sot_conversion,

    -- ─────────────────────────────────────────────────────────────────────
    -- AXE 5 — Features Composites Signatures
    -- ─────────────────────────────────────────────────────────────────────

    -- F19 — Tactical Lock Index (triple verrou)
    -- = (stérilité_team + stérilité_opp) × (1 / (|ppda_diff| + ε)) × (1 - |zone_att_diff|)
    -- Combine : deux équipes stériles + pressing équivalent + domination territoriale nulle
    -- ↑ = triple verrou → nul le plus probable du catalogue
    CASE
        WHEN tvo.sterility_5 IS NOT NULL
         AND tvo.opp_sterility_5 IS NOT NULL
         AND tvo.avg_ppda_5 IS NOT NULL
         AND tvo.opp_avg_ppda_5 IS NOT NULL
        THEN
            (tvo.sterility_5 + tvo.opp_sterility_5)
            * (1.0 / (ABS(tvo.avg_ppda_5 - tvo.opp_avg_ppda_5) + 0.5))
            * (1.0 - ABS(COALESCE(tvo.ws_zone_att_pct, 0.33)
                       - COALESCE(tvo.opp_ws_zone_att_pct, 0.33)))
    END                                                   AS f19_tactical_lock_idx,

    -- F20 — Upset Probability Composite
    -- = (1 / pinnacle_prob_team) × (opp_xg_yield / team_xg_yield) × opp_comeback_rate
    -- Décomposition :
    --   (1/prob_team) = bookmaker voit cette équipe comme outsider
    --   opp_yield > team_yield = adversaire surperforme son xG → régression attendue
    --   opp_comeback_rate élevé = adversaire tient même sous pression
    -- ↑ = constellation d'upset : outsider face à une équipe qui va régresser
    CASE
        WHEN tvo.pinnacle_prob_team > 0
         AND tvo.avg_xg_5 > 0
         AND tvo.opp_avg_xg_5 > 0
        THEN
            (1.0 / tvo.pinnacle_prob_team)
            * ((tvo.opp_avg_gf_5 / NULLIF(tvo.opp_avg_xg_5, 0))
               / NULLIF((tvo.avg_gf_5 / NULLIF(tvo.avg_xg_5, 0)), 0))
            * COALESCE(tvo.opp_comeback_rate, 0.3)
    END                                                   AS f20_upset_composite

FROM team_vs_opp tvo
LEFT JOIN ws_rolling wr
    ON  wr.date          = tvo.date
    AND wr.team          = tvo.team
    AND wr.league_source = tvo.league_source
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# UPDATE gold.features_training
# ─────────────────────────────────────────────────────────────────────────────

SQL_UPDATE_TRAINING = """
UPDATE gold.features_training AS ft
SET
    -- AXE 1
    f1_mutual_cancel_idx       = fc.f1_mutual_cancel_idx,
    f2_defensive_mirror        = fc.f2_defensive_mirror,
    f3_draw_market_dev         = fc.f3_draw_market_dev,
    f4_momentum_convergence    = fc.f4_momentum_convergence,
    f5_cs_mutual_rate          = fc.f5_cs_mutual_rate,
    f6_ht_draw_tendency        = fc.f6_ht_draw_tendency,
    -- AXE 2
    f7_off_def_mismatch        = fc.f7_off_def_mismatch,
    f7_def_off_mismatch        = fc.f7_def_off_mismatch,
    f8_press_dominance_ratio   = fc.f8_press_dominance_ratio,
    f9_chance_quality_gap      = fc.f9_chance_quality_gap,
    f10_venue_power_adj        = fc.f10_venue_power_adj,
    -- AXE 3
    f11_comeback_rate          = fc.f11_comeback_rate,
    f12_red_card_resilience    = fc.f12_red_card_resilience,
    f13_late_goal_tendency     = fc.f13_late_goal_tendency,
    f14_goal_timing_variance   = fc.f14_goal_timing_variance,
    -- AXE 4
    f15_xg_yield_ratio         = fc.f15_xg_yield_ratio,
    f16_def_yield_ratio        = fc.f16_def_yield_ratio,
    f17_shots_to_goal_eff      = fc.f17_shots_to_goal_eff,
    f18_sot_conversion         = fc.f18_sot_conversion,
    -- AXE 5
    f19_tactical_lock_idx      = fc.f19_tactical_lock_idx,
    f20_upset_composite        = fc.f20_upset_composite
FROM tmp_f_computed fc
WHERE ft.date          = fc.date
  AND ft.team          = fc.team
  AND ft.league_source = fc.league_source
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# DIFFÉRENTIELS dans gold.features_final
# ─────────────────────────────────────────────────────────────────────────────

SQL_UPDATE_FINAL_DIFFS = """
UPDATE gold.features_final AS ff
SET
    -- F1 diff : mutuelle stérilité team vs opp (les deux peuvent être verrouillées)
    f1_mutual_cancel_diff    = ft_team.f1_mutual_cancel_idx
                             - ft_opp.f1_mutual_cancel_idx,

    -- F7 diff : mismatch offensif net (team_att - opp_def) - (opp_att - team_def)
    f7_mismatch_diff         = ft_team.f7_off_def_mismatch
                             - ft_opp.f7_off_def_mismatch,

    -- F8 diff : pressing supérieur net
    f8_press_dominance_diff  = ft_team.f8_press_dominance_ratio
                             - ft_opp.f8_press_dominance_ratio,

    -- F9 diff : qualité de chance supérieure
    f9_chance_quality_diff   = ft_team.f9_chance_quality_gap
                             - ft_opp.f9_chance_quality_gap,

    -- F10 diff : prime de lieu relative
    f10_venue_power_diff     = ft_team.f10_venue_power_adj
                             - ft_opp.f10_venue_power_adj,

    -- F11 diff : résilience mentale relative
    f11_comeback_diff        = ft_team.f11_comeback_rate
                             - ft_opp.f11_comeback_rate,

    -- F13 diff : danger tardif relatif
    -- > 0 = cette équipe est plus dangereuse en fin de match
    f13_late_goal_diff       = ft_team.f13_late_goal_tendency
                             - ft_opp.f13_late_goal_tendency,

    -- F15 diff : surperformance xG relative
    -- > 0 = cette équipe surperforme plus → régression attendue = risque
    -- < 0 = cette équipe sous-performe → explosion attendue = opportunité
    f15_xg_yield_diff        = ft_team.f15_xg_yield_ratio
                             - ft_opp.f15_xg_yield_ratio,

    -- F16 diff : surperformance défensive relative
    -- < 0 = cette équipe concède moins que son xGA → gardien performant (risque régression)
    f16_def_yield_diff       = ft_team.f16_def_yield_ratio
                             - ft_opp.f16_def_yield_ratio,

    -- F19 diff : qui verrouille le plus le match
    -- > 0 = cette équipe impose plus le "lock" tactique
    f19_tactical_lock_diff   = ft_team.f19_tactical_lock_idx
                             - ft_opp.f19_tactical_lock_idx,

    -- F20 diff : potentiel upset relatif
    -- > 0 = cette équipe est plus en position d'upset que l'adversaire
    f20_upset_diff           = ft_team.f20_upset_composite
                             - ft_opp.f20_upset_composite

FROM gold.features_training ft_team
JOIN gold.features_training ft_opp
    ON  ft_team.date          = ft_opp.date
    AND ft_team.opponent      = ft_opp.team
    AND ft_team.league_source = ft_opp.league_source
WHERE ff.date          = ft_team.date
  AND ff.team          = ft_team.team
  AND ff.league_source = ft_team.league_source
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def inject_team_mapping(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Injecte le team_mapping dans une table temporaire DuckDB.
    Nommée tmp_team_mapping_c pour éviter les conflits avec 03b.
    """
    import pandas as pd
    if not TEAM_MAPPING_ROWS:
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE tmp_team_mapping_c (
                raw_name       VARCHAR,
                canonical_name VARCHAR
            )
        """)
        logger.warning("  team_mapping vide — jointure WhoScored sur nom brut")
        return

    df_mapping = pd.DataFrame(TEAM_MAPPING_ROWS, columns=["raw_name", "canonical_name"])
    conn.register("_df_tm_c", df_mapping)
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_team_mapping_c AS
        SELECT raw_name, canonical_name FROM _df_tm_c
    """)
    conn.unregister("_df_tm_c")
    n = conn.execute("SELECT COUNT(*) FROM tmp_team_mapping_c").fetchone()[0]
    logger.info(f"  tmp_team_mapping_c : {n:,} entrées")


def add_columns_if_not_exist(conn: duckdb.DuckDBPyConnection) -> None:
    """Ajoute les colonnes F1–F20 à features_training et les diffs à features_final."""
    logger.info("  Ajout des colonnes F1–F20 (ADD COLUMN IF NOT EXISTS)...")
    for col_name, col_type in NEW_COLS:
        try:
            conn.execute(f"""
                ALTER TABLE gold.features_training
                ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            """)
        except Exception as e:
            logger.warning(f"    features_training {col_name}: {e}")

    for col_name, col_type in DIFF_COLS:
        try:
            conn.execute(f"""
                ALTER TABLE gold.features_final
                ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            """)
        except Exception as e:
            logger.warning(f"    features_final {col_name}: {e}")


def reset_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Remet à NULL toutes les colonnes F1–F20 et leurs diffs."""
    logger.warning("  Reset des colonnes F1–F20...")
    for col_name, _ in NEW_COLS:
        try:
            conn.execute(f"UPDATE gold.features_training SET {col_name} = NULL")
        except Exception:
            pass
    for col_name, _ in DIFF_COLS:
        try:
            conn.execute(f"UPDATE gold.features_final SET {col_name} = NULL")
        except Exception:
            pass


def print_coverage_report(conn: duckdb.DuckDBPyConnection) -> None:
    """Rapport de couverture sur les colonnes F1–F20."""
    logger.info("═══ Rapport de couverture — 03c ═══")

    total = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training"
    ).fetchone()[0]
    logger.info(f"  gold.features_training : {total:,} lignes")

    for col_name, _ in NEW_COLS:
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_training WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct = n_ok / total * 100 if total else 0
            status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
            logger.info(f"  {status} {col_name:<35} : {n_ok:>7,}/{total:,} ({pct:.1f}%)")
        except Exception as e:
            logger.warning(f"  {col_name} : erreur ({e})")

    try:
        total_ff = conn.execute("SELECT COUNT(*) FROM gold.features_final").fetchone()[0]
        logger.info(f"\n  gold.features_final : {total_ff:,} lignes")
        for col_name, _ in DIFF_COLS:
            try:
                n_ok = conn.execute(
                    f"SELECT COUNT(*) FROM gold.features_final WHERE {col_name} IS NOT NULL"
                ).fetchone()[0]
                pct = n_ok / total_ff * 100 if total_ff else 0
                status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
                logger.info(f"  {status} {col_name:<35} : {n_ok:>7,}/{total_ff:,} ({pct:.1f}%)")
            except Exception as e:
                logger.warning(f"  {col_name} : erreur ({e})")
    except Exception:
        logger.warning("  features_final absent ou inaccessible")

    # Sanity check : valeurs moyennes
    logger.info("\n  Sanity check — valeurs moyennes (features_training) :")
    for col_name, _ in NEW_COLS:
        try:
            stats = conn.execute(f"""
                SELECT AVG({col_name}), MIN({col_name}), MAX({col_name})
                FROM gold.features_training
                WHERE {col_name} IS NOT NULL
            """).fetchone()
            if stats and stats[0] is not None:
                logger.info(
                    f"    {col_name:<35} mean={stats[0]:.4f}  "
                    f"min={stats[1]:.4f}  max={stats[2]:.4f}"
                )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(reset_cols: bool = False, coverage_only: bool = False) -> None:
    """Pipeline complet 03c."""
    logger.info("═══ Pipeline 03c — Features Nul/Victoire/Failles (F1–F20) ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    _tmp_dir = Path(tempfile.gettempdir()) / "duckdb_03c_tmp"
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(f"SET temp_directory='{_tmp_dir.as_posix()}'")

    # ── Vérification prérequis ────────────────────────────────────────────────
    try:
        n_backbone = conn.execute(
            "SELECT COUNT(*) FROM gold.stg_backbone"
        ).fetchone()[0]
        logger.info(f"  gold.stg_backbone : {n_backbone:,} lignes disponibles")

        n_training = conn.execute(
            "SELECT COUNT(*) FROM gold.features_training"
        ).fetchone()[0]
        logger.info(f"  gold.features_training : {n_training:,} lignes (cible)")

    except Exception as e:
        logger.error(f"  Prérequis manquant : {e}")
        conn.close()
        raise

    if n_backbone == 0:
        logger.warning("  gold.stg_backbone vide — pipeline 03c ignoré")
        conn.close()
        return

    # ── Coverage only ─────────────────────────────────────────────────────────
    if coverage_only:
        print_coverage_report(conn)
        conn.close()
        return

    # ── Ajout colonnes ────────────────────────────────────────────────────────
    add_columns_if_not_exist(conn)

    if reset_cols:
        reset_columns(conn)

    # ── Team mapping ──────────────────────────────────────────────────────────
    inject_team_mapping(conn)

    # ── Passe 1 : agrégations backbone rolling ────────────────────────────────
    logger.info("Passe 1 — Agrégations rolling gold.stg_backbone...")
    conn.execute(SQL_BACKBONE_FEATURES)
    n_bf = conn.execute("SELECT COUNT(*) FROM tmp_backbone_features").fetchone()[0]
    logger.info(f"  {n_bf:,} lignes dans tmp_backbone_features")

    # ── Passe 2 : features WhoScored events (F6, F13, F14) ────────────────────
    try:
        n_ws_events = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        if n_ws_events > 0:
            logger.info("Passe 2 — Agrégations WhoScored events (F6, F13, F14)...")
            conn.execute(SQL_WS_TIMING_FEATURES)
            n_wst = conn.execute("SELECT COUNT(*) FROM tmp_ws_timing").fetchone()[0]
            logger.info(f"  {n_wst:,} lignes dans tmp_ws_timing")
        else:
            logger.warning(
                "  silver.stg_whoscored_events vide — F6/F13/F14 seront NULL"
            )
            # Créer table vide pour éviter les erreurs dans SQL_COMPUTE_FEATURES
            conn.execute("""
                CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
                SELECT
                    CAST(NULL AS VARCHAR)  AS ws_match_id,
                    CAST(NULL AS INTEGER)  AS team_id,
                    CAST(NULL AS INTEGER)  AS total_goals,
                    CAST(NULL AS DOUBLE)   AS late_goal_pct,
                    CAST(NULL AS DOUBLE)   AS goal_minute_stddev,
                    CAST(NULL AS INTEGER)  AS ht_draw
                WHERE FALSE
            """)
    except Exception as e:
        logger.warning(f"  WhoScored events inaccessible : {e} — F6/F13/F14 seront NULL")
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE tmp_ws_timing AS
            SELECT
                CAST(NULL AS VARCHAR)  AS ws_match_id,
                CAST(NULL AS INTEGER)  AS team_id,
                CAST(NULL AS INTEGER)  AS total_goals,
                CAST(NULL AS DOUBLE)   AS late_goal_pct,
                CAST(NULL AS DOUBLE)   AS goal_minute_stddev,
                CAST(NULL AS INTEGER)  AS ht_draw
            WHERE FALSE
        """)

    # ── Passe 3 : calcul final des features F1–F20 ───────────────────────────
    logger.info("Passe 3 — Calcul des features F1–F20 (composites + diffs)...")
    conn.execute(SQL_COMPUTE_FEATURES)
    n_fc = conn.execute("SELECT COUNT(*) FROM tmp_f_computed").fetchone()[0]
    logger.info(f"  {n_fc:,} lignes dans tmp_f_computed")

    # ── UPDATE gold.features_training ────────────────────────────────────────
    logger.info("  UPDATE gold.features_training...")
    conn.execute(SQL_UPDATE_TRAINING)
    n_updated = conn.execute("""
        SELECT COUNT(*) FROM gold.features_training
        WHERE f1_mutual_cancel_idx IS NOT NULL
    """).fetchone()[0]
    logger.info(f"  {n_updated:,} lignes enrichies dans features_training")

    # ── Différentiels gold.features_final ────────────────────────────────────
    try:
        conn.execute(SQL_UPDATE_FINAL_DIFFS)
        n_diff = conn.execute(
            "SELECT COUNT(*) FROM gold.features_final WHERE f7_mismatch_diff IS NOT NULL"
        ).fetchone()[0]
        logger.info(f"  {n_diff:,} lignes enrichies dans features_final (diffs)")
    except Exception as e:
        logger.warning(f"  Différentiels features_final ignorés : {e}")

    # ── Rapport final ─────────────────────────────────────────────────────────
    print_coverage_report(conn)

    conn.close()
    logger.success("═══ Pipeline 03c terminé ═══")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering Nul/Victoire/Failles (F1–F20) — 03c"
    )
    parser.add_argument(
        "--reset-cols",
        action="store_true",
        help="Remet à NULL toutes les colonnes F1–F20 avant recalcul",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Affiche uniquement le rapport de couverture sans recalculer",
    )
    args = parser.parse_args()

    run_pipeline(
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
    )
