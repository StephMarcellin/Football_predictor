"""
features/sandbox.py — Features Candidates (Mode Manuel / Read-Only)
====================================================================
Ex-03c_suggested_features.py, déplacé dans le package features/.

Bac à sable validé pour les nouvelles features candidates issues de l'Agent
Analyste. Chaque feature doit être validée manuellement avant intégration.

MODE : toujours read-only (pas d'écriture en DB).

CATALOGUE DES FEATURES CANDIDATES
────────────────────────────────────
  H1 — draw_prob_ratio (feature méta Stage 2) → calculé dans 04_train.py
  H2 — draw_rate_5 + draw_affinity            → à intégrer dans rolling.py
  H3 — has_ws_events                          → déjà implémentée (whoscored.py)
  H4 — home_win_rate_hist                     → à intégrer dans rolling.py

  ── Sprint 1 — Quick Wins (WhoScored events) ──────────────────────────────
  S3 — ws_counter_attack_dna    : % actions offensives avec qualifier COUNTER_ATTACK (26)
  M1 — ws_midfield_control_idx  : % duels de milieu remportés (zone x 33–66) vs total des deux équipes
  D1 — ws_defensive_line_height : moyenne pondérée de x sur actions défensives (tacles/intercept/dégagements)
  D3 — ws_flank_exposure_asymm  : asymétrie succès défensif gauche (y<30) vs droite (y>70)

  Sources SQL :
    S3 → tmp_events_qual  (qual_type_id=26 sur actions offensives type_id ∈ {1,3,13,15,16})
    M1 → tmp_events_flat  (actions réussies en zone x 33–66 par équipe / total des deux équipes)
    D1 → tmp_events_flat  (type_id ∈ {7,8,12} = tacles, interceptions, dégagements)
    D3 → tmp_events_flat  (actions défensives par couloir y<30 vs y>70)

  Anti-leakage : features calculées sur le match en cours, appliquées au match
  suivant via le mécanisme LAG(1) existant dans whoscored.py (Passe 3).
  Couverture : conditionnelle à has_ws_events=1.

Appelable :
  python -m features.sandbox              # validation complète + guide d'intégration
  python -m features.sandbox --dry-run    # mode silencieux (défaut)
  python -m features.sandbox --sprint1    # valide uniquement S3/M1/D1/D3
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).resolve().parent.parent.parent)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"].get("duckdb", CFG["paths"].get("db", "data/projet_3etoiles.duckdb"))

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_sandbox.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [sandbox] {message}",
)

TEST_SEASON = CFG.get("train", {}).get("test_season", "2024-2025")


# ══════════════════════════════════════════════════════════════════════════════
# SQL CANDIDATS — Features H1–H4 (existantes)
# ══════════════════════════════════════════════════════════════════════════════

SQL_DRAW_RATE_ROLLING = f"""
-- H2 : draw_rate rolling 5 matchs — cible : features/rolling.py Bloc 2
-- Source : gold.stg_backbone (result_1n2, team, date)
-- ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING : anti-leakage strict

WITH draw_rate_cte AS (
    SELECT
        match_id, team, date, venue, season, league_source, result_1n2,
        AVG(CASE WHEN result_1n2='D' THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS draw_rate_5,
        COUNT(*) OVER (
            PARTITION BY team ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS draw_rate_5_n
    FROM gold.stg_backbone WHERE comp_category='Big5'
)
SELECT match_id, team, date, draw_rate_5, draw_rate_5_n
FROM draw_rate_cte ORDER BY team, date
"""

SQL_DRAW_AFFINITY_DIFF = """
-- Différentiel draw_affinity — à ajouter dans le Bloc 3 de rolling.py
    (t.draw_rate_5 + o.draw_rate_5) / 2.0  AS draw_affinity,
    t.draw_rate_5 - o.draw_rate_5           AS draw_rate_diff,
"""

SQL_HOME_WIN_RATE = f"""
-- H4 : win rate domicile historique par équipe — cible : features/rolling.py Bloc 1
-- TEST_SEASON exclue pour éviter tout leakage
WITH home_perf AS (
    SELECT team,
        AVG(CASE WHEN result_1n2='H' THEN 1.0 ELSE 0.0 END) AS home_win_rate_hist,
        COUNT(*) AS home_matches_hist
    FROM gold.stg_backbone
    WHERE venue='Home' AND comp_category='Big5' AND season != '{TEST_SEASON}'
    GROUP BY team HAVING COUNT(*) >= 10
)
SELECT * FROM home_perf ORDER BY home_win_rate_hist DESC
"""

SQL_DRAW_PROB_RATIO = """
-- H1 : draw_prob_ratio — feature méta pour 04_train.py Stage 2
-- À calculer APRÈS les prédictions Stage 1 (prob_D_model dispo)
meta_df["draw_prob_ratio"] = (
    meta_df["prob_D"]
    / meta_df["market_prob_d"].clip(lower=0.05)
)
meta_df["draw_prob_ratio"] = meta_df["draw_prob_ratio"].clip(upper=5.0)
"""


# ══════════════════════════════════════════════════════════════════════════════
# SQL CANDIDATS — Sprint 1 : S3, M1, D1, D3
# ══════════════════════════════════════════════════════════════════════════════
#
# Architecture :
#   Ces 4 SQL sont des SELECT de validation (read-only).
#   Ils calculent les features directement depuis silver.stg_whoscored_events
#   pour auditer le signal, le taux de NULL et la distribution.
#   Lors de l'intégration dans whoscored.py, ils s'insèrent dans la CTE
#   `assembled` de SQL_TEAM_FEATURES (Passe 2).
# ──────────────────────────────────────────────────────────────────────────────

# ── S3 : counter_attack_DNA ───────────────────────────────────────────────────
# Signal métier : une équipe contre-attaquante se caractérise par des transitions
# rapides récupération → tir. On mesure le % de tirs précédés d'un BallRecovery
# de la même équipe dans le même match en ≤ 15 secondes.
#
# Seuil 15s : confirmé par la distribution empirique des intervalles
# BallRecovery → tir :
#   0–5s  :  5 258  ← transitions immédiates
#   5–10s : 22 864  ← pic — vraies contre-attaques
#   10–15s: 25 298  ← pic
#   15–20s: 18 871  ← décroissance
#   25s+  : plateau ← jeu positionnel, lien causal rompu
#
# Qualifier FastBreak (qual=23) abandonné : 13 631 events sur 17M (trop sparse)
# et ne couvre pas les transitions non taggées par WhoScored.
#
# Dénominateur : total des tirs de l'équipe dans le match
# type_id tirs confirmés via ref.ws_event_types :
#   13 = MissedShots, 14 = ShotOnPost, 15 = SavedShot, 16 = Goal
#
# Compatibilité sandbox : self-join sur ws_match_id + team_id, pas de UNNEST.

SQL_S3_COUNTER_ATTACK_DNA = """
-- S3 : ws_counter_attack_dna — validation read-only
-- Source : silver.stg_whoscored_events (self-join temporelle)
-- Grain : 1 ligne = 1 match × 1 équipe
-- Leakage : AUCUN — séquence intra-match

WITH shot_events AS (
    SELECT
        ws_match_id, team_id,
        expanded_minute * 60 + second AS t_shot
    FROM silver.stg_whoscored_events
    WHERE type_id IN (13, 14, 15, 16)  -- MissedShots, ShotOnPost, SavedShot, Goal
),
recovery_events AS (
    SELECT
        ws_match_id, team_id,
        expanded_minute * 60 + second AS t_recovery
    FROM silver.stg_whoscored_events
    WHERE type_id = 49  -- BallRecovery
),
transition_shots AS (
    -- Tirs précédés d'un BallRecovery de la même équipe en <= 15 secondes
    SELECT DISTINCT
        s.ws_match_id,
        s.team_id,
        s.t_shot
    FROM shot_events s
    JOIN recovery_events r
        ON  s.ws_match_id = r.ws_match_id
        AND s.team_id     = r.team_id
        AND s.t_shot      > r.t_recovery
        AND s.t_shot     - r.t_recovery <= 15
),
counts_per_match AS (
    SELECT
        s.ws_match_id, s.team_id,
        COUNT(*)                   AS total_shots,
        COUNT(DISTINCT t.t_shot)   AS counter_attack_shots
    FROM shot_events s
    LEFT JOIN transition_shots t
        ON  s.ws_match_id = t.ws_match_id
        AND s.team_id     = t.team_id
        AND s.t_shot      = t.t_shot
    GROUP BY s.ws_match_id, s.team_id
)
SELECT
    ws_match_id, team_id,
    total_shots,
    counter_attack_shots,
    CASE WHEN total_shots > 0
         THEN CAST(counter_attack_shots AS DOUBLE) / total_shots
         ELSE NULL
    END AS ws_counter_attack_dna
FROM counts_per_match
ORDER BY ws_counter_attack_dna DESC NULLS LAST
"""

# ── M1 : midfield_control_index ───────────────────────────────────────────────
# Signal métier : l'équipe qui gagne les duels en zone médiane (33≤x≤66) contrôle
# le tempo. Un indice > 0.58 = domination du milieu.
# Le dénominateur inclut les DEUX équipes pour le même match → ratio compétitif.
# Type_id ∈ {1=passe, 7=tacle, 8=interception} + outcome_id=1 = action réussie.

SQL_M1_MIDFIELD_CONTROL = """
-- M1 : ws_midfield_control_idx — validation read-only
-- Source : silver.stg_whoscored_events (tmp_events_flat, pas de qualifiers nécessaires)
-- Grain : 1 ligne = 1 match × 1 équipe
-- Leakage : AUCUN — calculé sur les events du match courant

WITH midfield_actions AS (
    SELECT
        ws_match_id, team_id,
        COUNT(*) FILTER (
            WHERE x BETWEEN 33 AND 66
              AND type_id IN (1, 7, 8)
              AND outcome_id = 1             -- action réussie
        ) AS midfield_success
    FROM silver.stg_whoscored_events
    GROUP BY ws_match_id, team_id
),
match_totals AS (
    -- Total des deux équipes par match (dénominateur compétitif)
    SELECT
        ws_match_id,
        SUM(midfield_success) AS match_midfield_total
    FROM midfield_actions
    GROUP BY ws_match_id
),
feature_per_match AS (
    SELECT
        ma.ws_match_id, ma.team_id,
        ma.midfield_success,
        mt.match_midfield_total,
        CASE WHEN mt.match_midfield_total > 0
             THEN CAST(ma.midfield_success AS DOUBLE) / mt.match_midfield_total
             ELSE NULL
        END AS ws_midfield_control_idx
    FROM midfield_actions ma
    JOIN match_totals mt ON ma.ws_match_id = mt.ws_match_id
)
SELECT
    ws_match_id, team_id,
    ws_midfield_control_idx,
    midfield_success,
    match_midfield_total
FROM feature_per_match
ORDER BY ws_midfield_control_idx DESC NULLS LAST
"""

# ── D1 : defensive_line_height ────────────────────────────────────────────────
# Signal métier : la moyenne de x sur les actions défensives indique à quelle
# hauteur l'équipe défend. Coordonnées WhoScored déjà orientées par équipe
# (x=100 = but adverse, x=0 = but propre) — confirmé empiriquement.
# Distribution observée : mean=27, std=5.6, max=55.9
# Seuils calibrés sur les données Big5 :
#   x < 22 = bloc bas (défense très profonde)
#   22-32  = zone normale
#   x > 32 = ligne haute (pression haute, vulnérable dans le dos)
# type_id : 7=Tackle, 8=Interception, 12=Clearance

SQL_D1_DEFENSIVE_LINE_HEIGHT = """
-- D1 : ws_defensive_line_height — validation read-only
-- Source : silver.stg_whoscored_events
-- Grain : 1 ligne = 1 match × 1 équipe
-- Leakage : AUCUN — position spatiale descriptive du match courant

WITH defensive_actions AS (
    SELECT
        ws_match_id, team_id, x,
        type_id
    FROM silver.stg_whoscored_events
    WHERE type_id IN (7, 8, 12)   -- tacles(7), interceptions(8), dégagements(12)
      AND x IS NOT NULL
),
feature_per_match AS (
    SELECT
        ws_match_id, team_id,
        COUNT(*)      AS n_defensive_actions,
        AVG(x)        AS ws_defensive_line_height,
        MIN(x)        AS def_line_min_x,
        MAX(x)        AS def_line_max_x,
        STDDEV(x)     AS def_line_stddev_x    -- dispersion = organisation vs désordre
    FROM defensive_actions
    GROUP BY ws_match_id, team_id
    HAVING COUNT(*) >= 5   -- seuil minimal : au moins 5 actions défensives
)
SELECT
    ws_match_id, team_id,
    ws_defensive_line_height,
    n_defensive_actions,
    def_line_min_x,
    def_line_max_x,
    def_line_stddev_x
FROM feature_per_match
ORDER BY ws_defensive_line_height DESC NULLS LAST
"""

# ── D3 : flank_exposure_asymmetry ────────────────────────────────────────────
# Signal métier : une asymétrie défensive de flanc > 0.12 signale un côté
# systématiquement plus faible. Croisé avec ws_attack_left/right_pct adverse
# → détecte un mismatch de couloir exploitable.
# Gauche = y < 30, Droite = y > 70 (flancs stricts, hors axe central)
# Taux de réussite = outcome_id=1 / total des actions défensives par couloir.

SQL_D3_FLANK_EXPOSURE_ASYMMETRY = """
-- D3 : ws_flank_exposure_asymm — validation read-only
-- Source : silver.stg_whoscored_events
-- Grain : 1 ligne = 1 match × 1 équipe
-- Leakage : AUCUN — ratio défensif intra-match

WITH flank_defense AS (
    SELECT
        ws_match_id, team_id,
        -- Flanc gauche (y < 30)
        COUNT(*) FILTER (WHERE y < 30 AND type_id IN (7, 8, 12))                  AS left_def_total,
        COUNT(*) FILTER (WHERE y < 30 AND type_id IN (7, 8, 12) AND outcome_id=1) AS left_def_success,
        -- Flanc droit (y > 70)
        COUNT(*) FILTER (WHERE y > 70 AND type_id IN (7, 8, 12))                  AS right_def_total,
        COUNT(*) FILTER (WHERE y > 70 AND type_id IN (7, 8, 12) AND outcome_id=1) AS right_def_success
    FROM silver.stg_whoscored_events
    WHERE y IS NOT NULL
    GROUP BY ws_match_id, team_id
    HAVING COUNT(*) FILTER (WHERE y < 30 AND type_id IN (7, 8, 12))
         + COUNT(*) FILTER (WHERE y > 70 AND type_id IN (7, 8, 12)) >= 4
),
feature_per_match AS (
    SELECT
        ws_match_id, team_id,
        left_def_total,
        right_def_total,
        -- Taux de réussite par flanc (NULL si pas d'actions sur ce flanc)
        CASE WHEN left_def_total  > 0
             THEN CAST(left_def_success  AS DOUBLE) / left_def_total
             ELSE NULL END AS left_success_rate,
        CASE WHEN right_def_total > 0
             THEN CAST(right_def_success AS DOUBLE) / right_def_total
             ELSE NULL END AS right_success_rate,
        -- Asymétrie : positif = meilleur à droite, négatif = meilleur à gauche
        -- Valeur absolue > 0.12 = flanc structurellement vulnérable (seuil catalogue)
        CASE WHEN left_def_total > 0 AND right_def_total > 0
             THEN (CAST(left_def_success  AS DOUBLE) / left_def_total)
                - (CAST(right_def_success AS DOUBLE) / right_def_total)
             ELSE NULL END AS ws_flank_exposure_asymm
    FROM flank_defense
)
SELECT
    ws_match_id, team_id,
    ws_flank_exposure_asymm,
    left_success_rate,
    right_success_rate,
    left_def_total,
    right_def_total
FROM feature_per_match
ORDER BY ABS(ws_flank_exposure_asymm) DESC NULLS LAST
"""


# ══════════════════════════════════════════════════════════════════════════════
# GUIDE D'INTÉGRATION
# ══════════════════════════════════════════════════════════════════════════════

INTEGRATION_GUIDE = f"""
╔══════════════════════════════════════════════════════════════╗
║          GUIDE D'INTÉGRATION — Features candidates          ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  H2 — draw_rate_5 + draw_affinity                           ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/rolling.py                             ║
║  Bloc     : Bloc 2 (Rolling) — CTE rolling_features         ║
║  Colonnes : draw_rate_5, draw_affinity, draw_rate_diff      ║
║  Source   : gold.stg_backbone (result_1n2)                  ║
║                                                              ║
║  H4 — home_win_rate_hist                                    ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/rolling.py                             ║
║  Bloc     : Bloc 1 (Staging) — CTE home_perf               ║
║  Colonne  : home_win_rate_hist                              ║
║  Garde-fou: season != '{TEST_SEASON}' (anti-leakage)          ║
║                                                              ║
║  H1 — draw_prob_ratio (feature méta)                        ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : 04_train.py                                     ║
║  Bloc     : Stage 2 — construction meta_df                  ║
║  Source   : prob_D (Stage 1) / market_prob_d (silver.odds)  ║
║                                                              ║
║  H3 — has_ws_events → DÉJÀ IMPLÉMENTÉE ✅                   ║
║  (features/whoscored.py — gold.features_training)           ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║       SPRINT 1 — Quick Wins (cible : whoscored.py)          ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  S3 — ws_counter_attack_dna                                 ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/whoscored.py + features/columns.py     ║
║  Étape    : Passe 2 — CTE `qualifier_features` (assembled)  ║
║  Colonne  : ws_counter_attack_dna  DOUBLE                   ║
║  Source   : tmp_events_qual, qual_type_id=26                ║
║  Diff     : ws_counter_attack_diff = team - opp             ║
║  columns.py → NEW_COLS_WS + DIFF_COLS_WS                    ║
║                                                              ║
║  M1 — ws_midfield_control_idx                               ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/whoscored.py + features/columns.py     ║
║  Étape    : Passe 2 — nouvelle CTE `midfield_control`        ║
║             (nécessite une self-join sur le match_total)     ║
║  Colonne  : ws_midfield_control_idx  DOUBLE                 ║
║  Source   : tmp_events_flat, x BETWEEN 33 AND 66            ║
║  NOTE     : ratio compétitif (vs deux équipes) → pas de     ║
║             diff utile (déjà relatif). Garder tel quel.      ║
║                                                              ║
║  D1 — ws_defensive_line_height                              ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/whoscored.py + features/columns.py     ║
║  Étape    : Passe 2 — nouvelle CTE `defensive_shape`         ║
║  Colonne  : ws_defensive_line_height  DOUBLE                ║
║  Source   : tmp_events_flat, type_id IN (7, 8, 12)          ║
║  Diff     : ws_def_line_diff = team - opp                   ║
║             (>0 = équipe défend plus haut = pression)        ║
║                                                              ║
║  D3 — ws_flank_exposure_asymm                               ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : features/whoscored.py + features/columns.py     ║
║  Étape    : Passe 2 — CTE `defensive_exposure` étendue      ║
║  Colonne  : ws_flank_exposure_asymm  DOUBLE                 ║
║  Source   : tmp_events_flat, y < 30 et y > 70               ║
║  Diff     : ws_flank_asymm_diff = |team| - |opp|            ║
║             (>0 = team plus asymétrique = plus exploitable)  ║
║                                                              ║
║  SÉQUENCE D'INTÉGRATION RECOMMANDÉE :                       ║
║   1. Valider taux de NULL via validate_sprint1()            ║
║   2. Ajouter colonnes dans columns.py (NEW_COLS_WS +        ║
║      DIFF_COLS_WS)                                          ║
║   3. Ajouter CTEs dans SQL_TEAM_FEATURES (whoscored.py)     ║
║   4. Étendre SQL_PIVOT_HOME_AWAY + SQL_JOIN_TRAINING        ║
║   5. Étendre UPDATE dans run_passe3()                       ║
║   6. Étendre build_differential_features()                  ║
║   7. Vérifier Features_names.txt                            ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION — Features H1–H4 (existantes)
# ══════════════════════════════════════════════════════════════════════════════

def validate_legacy_features(conn: duckdb.DuckDBPyConnection) -> dict:
    """Valide les SQL candidats H1–H4 en read-only. Aucune écriture."""
    results = {}

    # H2 : draw_rate_5
    logger.info("  [H2] Validation draw_rate_5 (rolling 5 matchs)")
    try:
        df = conn.execute(SQL_DRAW_RATE_ROLLING).df()
        total    = len(df)
        coverage = (df["draw_rate_5"].notna().sum() / total) if total else 0
        mean_dr  = df["draw_rate_5"].mean()
        low_n    = (df["draw_rate_5_n"] < 3).mean()
        results["H2_draw_rate_5"] = {
            "status": "✅ OK",
            "coverage": f"{coverage:.1%}",
            "mean_draw_rate": f"{mean_dr:.3f}",
            "low_sample_pct": f"{low_n:.1%} des lignes avec < 3 matchs dans la fenêtre",
        }
        logger.success(f"    H2 ✅ — couverture: {coverage:.1%} | draw_rate moyen: {mean_dr:.3f}")
    except Exception as e:
        results["H2_draw_rate_5"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H2 ❌ — {e}")

    # H4 : home_win_rate_hist
    logger.info(f"  [H4] Validation home_win_rate_hist (excl. {TEST_SEASON})")
    try:
        df = conn.execute(SQL_HOME_WIN_RATE).df()
        results["H4_home_win_rate_hist"] = {
            "status": "✅ OK",
            "n_teams": len(df),
            "win_rate_max":  f"{df['home_win_rate_hist'].max():.1%}",
            "win_rate_min":  f"{df['home_win_rate_hist'].min():.1%}",
            "win_rate_mean": f"{df['home_win_rate_hist'].mean():.1%}",
        }
        logger.success(
            f"    H4 ✅ — {len(df)} équipes | "
            f"{df['home_win_rate_hist'].min():.1%} → {df['home_win_rate_hist'].max():.1%}"
        )
    except Exception as e:
        results["H4_home_win_rate_hist"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H4 ❌ — {e}")

    # H1 : draw_prob_ratio (vérif market_prob_d)
    logger.info("  [H1] Validation draw_prob_ratio (silver.odds.market_prob_d)")
    try:
        df = conn.execute("""
            SELECT COUNT(*) AS total, COUNT(market_prob_d) AS non_null,
                   AVG(market_prob_d) AS mean_market_prob_d
            FROM silver.odds WHERE market_prob_d IS NOT NULL
        """).df()
        non_null = int(df["non_null"].iloc[0])
        mean_val = float(df["mean_market_prob_d"].iloc[0])
        results["H1_draw_prob_ratio"] = {
            "status": "✅ OK (colonne disponible)",
            "non_null_rows": non_null,
            "mean_market_prob_d": f"{mean_val:.3f}",
            "note": "Calcul effectué dans 04_train.py Stage 2",
        }
        logger.success(f"    H1 ✅ — market_prob_d : {non_null} lignes | mean: {mean_val:.3f}")
    except Exception as e:
        results["H1_draw_prob_ratio"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H1 ❌ — {e}")

    # H3 : has_ws_events (déjà implémentée)
    logger.info("  [H3] Vérification has_ws_events (déjà dans gold)")
    try:
        df = conn.execute("""
            SELECT COUNT(*) AS total, SUM(has_ws_events) AS with_ws,
                   AVG(has_ws_events) AS coverage_pct
            FROM gold.features_training WHERE has_ws_events IS NOT NULL
        """).df()
        coverage = float(df["coverage_pct"].iloc[0])
        with_ws  = int(df["with_ws"].iloc[0])
        results["H3_has_ws_events"] = {
            "status": "✅ DÉJÀ IMPLÉMENTÉE (gold.features_training)",
            "coverage": f"{coverage:.1%} ({with_ws} matchs couverts)",
        }
        logger.success(f"    H3 ✅ — has_ws_events présente | coverage: {coverage:.1%}")
    except Exception as e:
        results["H3_has_ws_events"] = {"status": f"⚠️ Table inaccessible : {e}"}
        logger.warning(f"    H3 ⚠️ — {e}")

    return results


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION — Sprint 1 : S3, M1, D1, D3
# ══════════════════════════════════════════════════════════════════════════════

def _null_audit(df: pd.DataFrame, feat_col: str) -> dict:
    """Retourne les métriques de couverture et de distribution d'une feature."""
    total   = len(df)
    n_ok    = df[feat_col].notna().sum()
    coverage = n_ok / total if total else 0.0
    status  = "✅" if coverage > 0.70 else ("⚠️ " if coverage > 0.30 else "❌")
    metrics = {
        "status"  : status,
        "coverage": f"{coverage:.1%} ({n_ok}/{total} matchs-équipes)",
    }
    if n_ok > 0:
        metrics["mean"]   = f"{df[feat_col].mean():.4f}"
        metrics["median"] = f"{df[feat_col].median():.4f}"
        metrics["std"]    = f"{df[feat_col].std():.4f}"
        metrics["min"]    = f"{df[feat_col].min():.4f}"
        metrics["max"]    = f"{df[feat_col].max():.4f}"
    return metrics


def validate_sprint1(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Valide les 4 features Sprint 1 en mode read-only.
    Chaque validation calcule la feature depuis silver, mesure le taux de NULL,
    et vérifie la distribution pour détecter les valeurs aberrantes.
    """
    results = {}

    # ── Prérequis : vérifier que silver.stg_whoscored_events existe ───────────
    try:
        n_events = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        logger.info(f"  silver.stg_whoscored_events : {n_events:,} événements disponibles")
        if n_events == 0:
            logger.warning("  ⚠️ Aucun événement WhoScored — Sprint 1 ignoré")
            return {"sprint1_prereq": {"status": "⚠️ SKIP — table vide"}}
    except Exception as e:
        logger.error(f"  ❌ silver.stg_whoscored_events inaccessible : {e}")
        return {"sprint1_prereq": {"status": f"❌ TABLE MANQUANTE : {e}"}}

    # ── S3 : counter_attack_DNA ───────────────────────────────────────────────
    logger.info("  [S3] Validation ws_counter_attack_dna (CONTAINS '\"value\":26,' — sans UNNEST)")
    try:
        df = conn.execute(SQL_S3_COUNTER_ATTACK_DNA).df()
        metrics = _null_audit(df, "ws_counter_attack_dna")

        # Vérification signal : une équipe counter-attaquante est > 15%
        if df["ws_counter_attack_dna"].notna().sum() > 0:
            n_high = (df["ws_counter_attack_dna"] > 0.30).sum()
            metrics["n_high_counter_attack"] = (
                f"{n_high} matchs-équipes avec ADN contre > 30% des tirs"
            )
            n_zero = (df["counter_attack_shots"] == 0).sum()
            metrics["n_zero_counter"] = (
                f"{n_zero} matchs-équipes sans tir en transition rapide"
            )
            metrics["total_counter_shots"] = (
                f"{int(df['counter_attack_shots'].sum()):,} tirs issus d'une transition <= 15s"
            )

        results["S3_counter_attack_dna"] = metrics
        logger.success(f"    S3 {metrics['status']} — {metrics['coverage']}")
    except Exception as e:
        results["S3_counter_attack_dna"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    S3 ❌ — {e}")

    # ── M1 : midfield_control_index ───────────────────────────────────────────
    logger.info("  [M1] Validation ws_midfield_control_idx (zone x 33–66)")
    try:
        df = conn.execute(SQL_M1_MIDFIELD_CONTROL).df()
        metrics = _null_audit(df, "ws_midfield_control_idx")

        # Sanity check : la moyenne doit être proche de 0.50 (ratio compétitif)
        if df["ws_midfield_control_idx"].notna().sum() > 0:
            mean_val = df["ws_midfield_control_idx"].mean()
            if not (0.40 < mean_val < 0.60):
                metrics["warning"] = (
                    f"⚠️ Moyenne = {mean_val:.3f} — attendu ~0.50 "
                    "(ratio compétitif, les deux équipes doivent être équilibrées en moyenne)"
                )
            else:
                metrics["sanity_check"] = f"✅ Moyenne = {mean_val:.3f} ≈ 0.50 (ratio cohérent)"

            # Distribution : détecter les cas de domination totale (> 0.80)
            n_dominant = (df["ws_midfield_control_idx"] > 0.80).sum()
            metrics["n_dominant_midfield"] = (
                f"{n_dominant} matchs avec domination milieu > 80%"
            )

        results["M1_midfield_control_idx"] = metrics
        logger.success(f"    M1 {metrics['status']} — {metrics['coverage']}")
    except Exception as e:
        results["M1_midfield_control_idx"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    M1 ❌ — {e}")

    # ── D1 : defensive_line_height ────────────────────────────────────────────
    logger.info("  [D1] Validation ws_defensive_line_height (type_id 7/8/12)")
    try:
        df = conn.execute(SQL_D1_DEFENSIVE_LINE_HEIGHT).df()
        metrics = _null_audit(df, "ws_defensive_line_height")

        if df["ws_defensive_line_height"].notna().sum() > 0:
            # Segmentation tactique : < 40 = bloc bas, 40–60 = moyen, > 60 = ligne haute
            n_low  = (df["ws_defensive_line_height"] < 22).sum()
            n_mid  = ((df["ws_defensive_line_height"] >= 22) & (df["ws_defensive_line_height"] <= 32)).sum()
            n_high = (df["ws_defensive_line_height"] > 32).sum()
            metrics["distribution_tactique"] = (
                f"Bloc bas (<22) : {n_low} | Normal (22-32) : {n_mid} | Ligne haute (>32) : {n_high}"
            )
            # Note : coordonnées déjà orientées par WhoScored (x=100 = but adverse)
            # mean=27 confirme que les équipes défendent dans leur propre moitié (x<50)

            # Vérifier la dispersion : std trop faible = manque de variance = feature peu utile
            overall_std = df["ws_defensive_line_height"].std()
            if overall_std < 3.0:
                metrics["warning"] = (
                    f"⚠️ Std = {overall_std:.2f} — faible variance, "
                    "feature potentiellement peu discriminante"
                )

        results["D1_defensive_line_height"] = metrics
        logger.success(f"    D1 {metrics['status']} — {metrics['coverage']}")
    except Exception as e:
        results["D1_defensive_line_height"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    D1 ❌ — {e}")

    # ── D3 : flank_exposure_asymmetry ────────────────────────────────────────
    logger.info("  [D3] Validation ws_flank_exposure_asymm (y<30 vs y>70)")
    try:
        df = conn.execute(SQL_D3_FLANK_EXPOSURE_ASYMMETRY).df()
        metrics = _null_audit(df, "ws_flank_exposure_asymm")

        if df["ws_flank_exposure_asymm"].notna().sum() > 0:
            # Seuil catalogue : asymétrie > 0.12 = flanc vulnérable
            n_asym = (df["ws_flank_exposure_asymm"].abs() > 0.12).sum()
            n_right_weak = (df["ws_flank_exposure_asymm"] > 0.12).sum()
            n_left_weak  = (df["ws_flank_exposure_asymm"] < -0.12).sum()
            metrics["n_asymmetric_matches"] = (
                f"{n_asym} matchs avec asymétrie > 0.12 "
                f"(flanc droit faible : {n_right_weak}, flanc gauche faible : {n_left_weak})"
            )

            # Warning si trop peu d'actions sur les flancs (taux de NULL élevé)
            n_low_sample = ((df["left_def_total"] < 2) | (df["right_def_total"] < 2)).sum()
            if n_low_sample > len(df) * 0.20:
                metrics["warning"] = (
                    f"⚠️ {n_low_sample} matchs avec < 2 actions sur un flanc — "
                    "envisager de baisser le seuil HAVING ou d'agréger sur N matchs"
                )

        results["D3_flank_exposure_asymm"] = metrics
        logger.success(f"    D3 {metrics['status']} — {metrics['coverage']}")
    except Exception as e:
        results["D3_flank_exposure_asymm"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    D3 ❌ — {e}")

    # ── Requête de validation NULL dans gold (si déjà intégrées) ─────────────
    # Cette requête est fournie à titre documentaire pour l'audit post-intégration.
    logger.info("  [AUDIT] Requête de validation NULL post-intégration :")
    logger.info("""
    -- À exécuter après intégration dans whoscored.py :
    SELECT
        COUNT(*)                                     AS total,
        COUNT(ws_counter_attack_dna)                 AS s3_ok,
        COUNT(ws_midfield_control_idx)               AS m1_ok,
        COUNT(ws_defensive_line_height)              AS d1_ok,
        COUNT(ws_flank_exposure_asymm)               AS d3_ok,
        AVG(CASE WHEN ws_counter_attack_dna    IS NOT NULL THEN 1.0 ELSE 0.0 END) AS s3_pct,
        AVG(CASE WHEN ws_midfield_control_idx  IS NOT NULL THEN 1.0 ELSE 0.0 END) AS m1_pct,
        AVG(CASE WHEN ws_defensive_line_height IS NOT NULL THEN 1.0 ELSE 0.0 END) AS d1_pct,
        AVG(CASE WHEN ws_flank_exposure_asymm  IS NOT NULL THEN 1.0 ELSE 0.0 END) AS d3_pct
    FROM gold.features_training
    WHERE has_ws_events = 1
    """)

    return results


# ══════════════════════════════════════════════════════════════════════════════
# RAPPORT
# ══════════════════════════════════════════════════════════════════════════════

def print_validation_report(results: dict, section: str = "") -> None:
    header = f"══════ Rapport de validation — {section} ══════" if section else \
             "══════ Rapport de validation features candidates ══════"
    logger.info(f"\n{header}")
    for feat, info in results.items():
        logger.info(f"  {feat}")
        for k, v in info.items():
            logger.info(f"    {k}: {v}")
    logger.info("═" * 54)


# ── Entry points ──────────────────────────────────────────────────────────────

def validate_features(conn: duckdb.DuckDBPyConnection) -> dict:
    """Valide l'ensemble des features candidates (H1–H4 + Sprint 1)."""
    results = {}
    results.update(validate_legacy_features(conn))
    results.update(validate_sprint1(conn))
    return results


def run(context: dict | None = None, dry_run: bool = True) -> dict:
    """Entrypoint — validation read-only des features candidates."""
    if context is None:
        context = {}
    logger.info(f"Validation des features candidates (sandbox) — dry_run={dry_run}")
    conn    = duckdb.connect(str(DB_PATH), read_only=True)
    results = validate_features(conn)
    conn.close()
    print_validation_report(results)
    if dry_run:
        print(INTEGRATION_GUIDE)
    context["feature_validation_results"] = results
    return context


def run_sprint1_only(context: dict | None = None) -> dict:
    """Entrypoint restreint — valide uniquement les features Sprint 1 (S3/M1/D1/D3)."""
    if context is None:
        context = {}
    logger.info("Validation Sprint 1 uniquement (S3, M1, D1, D3)")
    conn    = duckdb.connect(str(DB_PATH), read_only=True)
    results = validate_sprint1(conn)
    conn.close()
    print_validation_report(results, section="Sprint 1")
    print(INTEGRATION_GUIDE)
    context["sprint1_validation_results"] = results
    return context


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Features candidates — validation read-only")
    parser.add_argument("--dry-run",  action="store_true", default=True,
                        help="Mode silencieux avec guide d'intégration (défaut)")
    parser.add_argument("--sprint1",  action="store_true",
                        help="Valide uniquement les features Sprint 1 (S3/M1/D1/D3)")
    args = parser.parse_args()

    if args.sprint1:
        run_sprint1_only()
    else:
        run(dry_run=args.dry_run)
