"""
Pipeline 03c — Features Suggérées (Mode Manuel)
================================================
Bac à sable validé pour les nouvelles features candidates issues de l'Agent
Analyste (agents/auditor.py). Chaque feature doit être validée par Stéphane
avant intégration dans 03_features.py ou 03b_features_match_details.py.

RÈGLES :
  - Toutes les requêtes s'appuient sur gold.stg_backbone (source rolling)
    et non silver.fbref_match_stats (table brute non normalisée).
  - has_ws_events est DÉJÀ dans gold.features_training (ligne 386 Features_names.txt)
    → feature H3 supprimée du scope.
  - draw_rate_5 et home_win_rate_hist sont des NOUVELLES features (absentes des tables gold).

Usage :
    python pipelines/03c_suggested_features.py --dry-run   # test sans écriture (défaut)
    python pipelines/03c_suggested_features.py             # calcul + affichage stats
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_suggestions.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [03c] {message}",
)

# TEST_SEASON doit correspondre à la valeur dans 04_train.py
TEST_SEASON = CFG.get("train", {}).get("test_season", "2024-2025")


# ══════════════════════════════════════════════════════════════════════════════
# CATALOGUE DES FEATURES CANDIDATES
# ══════════════════════════════════════════════════════════════════════════════
#
# H1 — DRAW PROBABILITY RATIO (feature méta Stage 2)
# ─────────────────────────────────────────────────────
# Hypothèse : Le modèle Stage 1 sous-estime les nuls car il n'a pas de signal
#             sur son propre désaccord avec le marché pour le draw.
# Feature   : draw_prob_ratio = prob_D_model / implied_prob_D_market
#             > 1 = modèle croit plus au nul que le marché (signal sur-confiance)
#             < 1 = marché croit plus au nul
# Cible     : Stage 2 uniquement (calculé depuis prédictions Stage 1 + silver.odds)
# Table     : silver.odds (market_prob_d)
# Statut    : CANDIDAT
#
# H2 — DRAW_RATE_5 + DRAW_AFFINITY (rolling 5 matchs)
# ──────────────────────────────────────────────────────
# Hypothèse : Deux équipes qui tirent souvent ont une probabilité accrue de nul.
#             Ce signal n'est pas capturé par xG ni ppda.
# Feature   : draw_rate_5 = AVG(result == 'D') OVER 5 derniers matchs
#             draw_affinity = (draw_rate_5_team + draw_rate_5_opp) / 2
# Table     : gold.stg_backbone (a result_1n2 et la clé team/date)
# Statut    : CANDIDAT
#
# H3 — HAS_WS_EVENTS → DÉJÀ PRÉSENTE
# ──────────────────────────────────────────────────────
# Confirmé dans Features_names.txt ligne 386 :
#   gold.features_training.has_ws_events
# → Feature supprimée du scope 03c (rien à faire).
#
# H4 — HOME_WIN_RATE_HIST (par équipe, toutes saisons hors test)
# ──────────────────────────────────────────────────────
# Hypothèse : L'avantage domicile varie fortement par équipe.
#             Une feature par équipe capture cet effet non linéaire.
# Feature   : home_win_rate_hist = win_rate home historique (excl. TEST_SEASON)
# Table     : gold.stg_backbone
# Statut    : CANDIDAT
# ══════════════════════════════════════════════════════════════════════════════


# ── SQL : draw_rate_5 ─────────────────────────────────────────────────────────
# À intégrer dans 03_features.py, Bloc 2 (Rolling), CTE "rolling_features"

SQL_DRAW_RATE_ROLLING = f"""
-- H2 : draw_rate rolling 5 matchs — à intégrer dans 03_features.py Bloc 2
-- Source : gold.stg_backbone (a result_1n2, team, date)
-- NOTE : ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING exclut le match courant (anti-leakage)

WITH draw_rate_cte AS (
    SELECT
        match_id,
        team,
        date,
        venue,
        season,
        league_source,
        result_1n2,
        AVG(
            CASE WHEN result_1n2 = 'D' THEN 1.0 ELSE 0.0 END
        ) OVER (
            PARTITION BY team
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS draw_rate_5,
        COUNT(*) OVER (
            PARTITION BY team
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS draw_rate_5_n   -- nb de matchs dans la fenêtre (pour filtrer les débuts de carrière)
    FROM gold.stg_backbone
    WHERE comp_category = 'Big5'
)
SELECT
    match_id,
    team,
    date,
    draw_rate_5,
    draw_rate_5_n
FROM draw_rate_cte
ORDER BY team, date
"""

# Différentiel (à ajouter dans la jointure team × opponent du Bloc 3 de 03_features.py)
SQL_DRAW_AFFINITY_DIFF = """
-- Différentiel draw_affinity — Bloc 3 (Match-up) de 03_features.py
-- t = équipe, o = adversaire (après la jointure home/away)

    (t.draw_rate_5 + o.draw_rate_5) / 2.0  AS draw_affinity,
    t.draw_rate_5 - o.draw_rate_5           AS draw_rate_diff,
"""


# ── SQL : home_win_rate_hist ──────────────────────────────────────────────────
# À intégrer dans 03_features.py Bloc 1 (Staging), comme feature saison-équipe

SQL_HOME_WIN_RATE = f"""
-- H4 : win rate domicile historique par équipe — Bloc 1 de 03_features.py
-- Source : gold.stg_backbone
-- TEST_SEASON exclue pour éviter tout leakage vers les données de test

WITH home_perf AS (
    SELECT
        team,
        AVG(CASE WHEN result_1n2 = 'H' THEN 1.0 ELSE 0.0 END) AS home_win_rate_hist,
        COUNT(*) AS home_matches_hist
    FROM gold.stg_backbone
    WHERE venue = 'Home'
      AND comp_category = 'Big5'
      AND season != '{TEST_SEASON}'
    GROUP BY team
    HAVING COUNT(*) >= 10   -- éviter les petits échantillons (promus récents)
)
SELECT * FROM home_perf
ORDER BY home_win_rate_hist DESC
"""

# ── SQL : draw_prob_ratio (feature méta) ──────────────────────────────────────
# À calculer dans 04_train.py Stage 2, APRÈS les prédictions Stage 1

SQL_DRAW_PROB_RATIO = """
-- H1 : draw_prob_ratio — feature méta pour Stage 2 de 04_train.py
-- Calculé après Stage 1 (prob_D_model dispo) + jointure silver.odds
-- NB : market_prob_d = probabilité implicite marché Average (col silver.odds)

    meta_df["draw_prob_ratio"] = (
        meta_df["prob_D"]                          # sortie Stage 1
        / meta_df["market_prob_d"].clip(lower=0.05)  # éviter division par 0
    )
    meta_df["draw_prob_ratio"] = meta_df["draw_prob_ratio"].clip(upper=5.0)  # cap outliers
"""


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION DES SQL CANDIDATS
# ══════════════════════════════════════════════════════════════════════════════

def validate_features(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Teste chaque SQL candidat contre la DB pour valider syntaxe et couverture.
    Toujours en read-only — aucune écriture.
    """
    results = {}

    # ── H2 : draw_rate_5 ─────────────────────────────────────────────────────
    logger.info("  [H2] Validation draw_rate_5 (gold.stg_backbone)")
    try:
        df = conn.execute("""
            SELECT
                team,
                date,
                result_1n2,
                AVG(
                    CASE WHEN result_1n2 = 'D' THEN 1.0 ELSE 0.0 END
                ) OVER (
                    PARTITION BY team
                    ORDER BY date
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS draw_rate_5,
                COUNT(*) OVER (
                    PARTITION BY team
                    ORDER BY date
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS draw_rate_5_n
            FROM gold.stg_backbone
            WHERE comp_category = 'Big5'
            LIMIT 2000
        """).df()

        coverage  = df["draw_rate_5"].notna().mean()
        mean_dr   = df["draw_rate_5"].mean()
        low_n_pct = (df["draw_rate_5_n"] < 3).mean()

        results["H2_draw_rate_5"] = {
            "status":        "✅ OK",
            "coverage":      f"{coverage:.1%}",
            "mean_draw_rate": f"{mean_dr:.3f}",
            "low_sample_pct": f"{low_n_pct:.1%} des lignes avec < 3 matchs dans la fenêtre",
        }
        logger.success(
            f"    H2 ✅ — couverture: {coverage:.1%} | draw_rate moyen: {mean_dr:.3f} | "
            f"fenêtre courte (<3): {low_n_pct:.1%}"
        )
    except Exception as e:
        results["H2_draw_rate_5"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H2 ❌ — {e}")

    # ── H4 : home_win_rate_hist ───────────────────────────────────────────────
    logger.info(f"  [H4] Validation home_win_rate_hist (excl. {TEST_SEASON})")
    try:
        df = conn.execute(f"""
            SELECT
                team,
                AVG(CASE WHEN result_1n2 = 'H' THEN 1.0 ELSE 0.0 END) AS home_win_rate_hist,
                COUNT(*) AS n
            FROM gold.stg_backbone
            WHERE venue = 'Home'
              AND comp_category = 'Big5'
              AND season != '{TEST_SEASON}'
            GROUP BY team
            HAVING COUNT(*) >= 10
            ORDER BY home_win_rate_hist DESC
        """).df()

        results["H4_home_win_rate_hist"] = {
            "status":       "✅ OK",
            "n_teams":      len(df),
            "win_rate_max":  f"{df['home_win_rate_hist'].max():.1%}",
            "win_rate_min":  f"{df['home_win_rate_hist'].min():.1%}",
            "win_rate_mean": f"{df['home_win_rate_hist'].mean():.1%}",
            "top_3":         df.head(3)[["team", "home_win_rate_hist", "n"]].to_dict("records"),
            "bottom_3":      df.tail(3)[["team", "home_win_rate_hist", "n"]].to_dict("records"),
        }
        logger.success(
            f"    H4 ✅ — {len(df)} équipes | "
            f"win rate home : {df['home_win_rate_hist'].min():.1%} → {df['home_win_rate_hist'].max():.1%}"
        )
        logger.debug(f"    Top 3 : {df.head(3)[['team', 'home_win_rate_hist']].to_dict('records')}")
    except Exception as e:
        results["H4_home_win_rate_hist"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H4 ❌ — {e}")

    # ── H1 : draw_prob_ratio (vérification silver.odds) ──────────────────────
    logger.info("  [H1] Validation draw_prob_ratio (silver.odds.market_prob_d)")
    try:
        df = conn.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(market_prob_d) AS non_null,
                AVG(market_prob_d) AS mean_market_prob_d,
                MIN(market_prob_d) AS min_val,
                MAX(market_prob_d) AS max_val
            FROM silver.odds
            WHERE market_prob_d IS NOT NULL
        """).df()

        non_null = int(df["non_null"].iloc[0])
        mean_val = float(df["mean_market_prob_d"].iloc[0])
        results["H1_draw_prob_ratio"] = {
            "status":             "✅ OK (colonne disponible)",
            "non_null_rows":      non_null,
            "mean_market_prob_d": f"{mean_val:.3f}",
            "note":               "Calcul effectué dans 04_train.py Stage 2 (feature méta, hors scope 03c)",
        }
        logger.success(f"    H1 ✅ — market_prob_d disponible : {non_null} lignes | mean: {mean_val:.3f}")
    except Exception as e:
        results["H1_draw_prob_ratio"] = {"status": f"❌ ERREUR : {e}"}
        logger.error(f"    H1 ❌ — {e}")

    # ── Confirmation H3 déjà implémentée ─────────────────────────────────────
    logger.info("  [H3] Vérification has_ws_events (déjà dans gold)")
    try:
        df = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(has_ws_events) AS with_ws,
                AVG(has_ws_events) AS coverage_pct
            FROM gold.features_training
            WHERE has_ws_events IS NOT NULL
        """).df()
        coverage = float(df["coverage_pct"].iloc[0])
        with_ws  = int(df["with_ws"].iloc[0])
        results["H3_has_ws_events"] = {
            "status":   "✅ DÉJÀ IMPLÉMENTÉE (gold.features_training)",
            "coverage": f"{coverage:.1%} ({with_ws} matchs couverts)",
            "note":     "Aucune action requise — feature présente depuis 03b",
        }
        logger.success(f"    H3 ✅ — has_ws_events déjà présente | coverage: {coverage:.1%}")
    except Exception as e:
        results["H3_has_ws_events"] = {"status": f"⚠️ Table inaccessible : {e}"}
        logger.warning(f"    H3 ⚠️ — {e}")

    return results


def print_validation_report(results: dict):
    logger.info("\n══════ Rapport de validation features candidates ══════")
    for feat, info in results.items():
        logger.info(f"  {feat}")
        for k, v in info.items():
            prefix = "    "
            if k == "top_3" and isinstance(v, list):
                for row in v:
                    logger.debug(f"{prefix}  {row}")
            elif k == "bottom_3" and isinstance(v, list):
                for row in v:
                    logger.debug(f"{prefix}  {row}")
            else:
                logger.info(f"{prefix}{k}: {v}")
    logger.info("═" * 54)


# ══════════════════════════════════════════════════════════════════════════════
# GUIDE D'INTÉGRATION (affiché en dry-run)
# ══════════════════════════════════════════════════════════════════════════════

INTEGRATION_GUIDE = """
╔══════════════════════════════════════════════════════════════╗
║          GUIDE D'INTÉGRATION — Features candidates          ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  H2 — draw_rate_5 + draw_affinity                           ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : pipelines/03_features.py                        ║
║  Bloc     : Bloc 2 (Rolling) — CTE rolling_features         ║
║  Colonnes : draw_rate_5, draw_affinity, draw_rate_diff      ║
║  Source   : gold.stg_backbone (result_1n2)                  ║
║                                                              ║
║  H4 — home_win_rate_hist                                    ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : pipelines/03_features.py                        ║
║  Bloc     : Bloc 1 (Staging) — CTE home_perf               ║
║  Colonne  : home_win_rate_hist                              ║
║  Source   : gold.stg_backbone                               ║
║  Garde-fou: season != TEST_SEASON (anti-leakage)            ║
║                                                              ║
║  H1 — draw_prob_ratio (feature méta)                        ║
║  ─────────────────────────────────────────────────────────  ║
║  Fichier  : pipelines/04_train.py                           ║
║  Bloc     : Stage 2 — construction meta_df                  ║
║  Source   : prob_D (Stage 1) / market_prob_d (silver.odds)  ║
║                                                              ║
║  H3 — has_ws_events → DÉJÀ IMPLÉMENTÉE ✅                   ║
║  Aucune action requise.                                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run(context: dict | None = None, dry_run: bool = True) -> dict:
    """Entrypoint appelé par l'orchestrateur (Agent Développeur)."""
    if context is None:
        context = {}

    logger.info(f"Validation des features candidates (03c) — dry_run={dry_run}")
    conn    = duckdb.connect(str(DB_PATH), read_only=True)
    results = validate_features(conn)
    conn.close()

    print_validation_report(results)

    if dry_run:
        print(INTEGRATION_GUIDE)

    context["suggested_features_path"]    = str(Path(__file__))
    context["feature_validation_results"] = results
    return context


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="03c — Features suggérées (Mode Manuel)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Validation SQL sans écriture en DB (défaut: True)",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)