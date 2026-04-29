"""
Diagnostic — Signal brut du modèle (mise fixe)
===============================================
Teste si le modèle a un signal réel, INDÉPENDAMMENT de la stratégie
edge/Kelly. Si le modèle est bon, ce test doit être rentable.

Usage :
    python diagnostic_model_signal.py
    python diagnostic_model_signal.py --predictions models/predictions_test.csv
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

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODELS_DIR = ROOT_DIR / "models"


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def load_data(pred_path: Path) -> pd.DataFrame:
    """Charge les prédictions depuis le CSV (qui contient déjà actual_result)
    et enrichit avec les cotes depuis DuckDB pour les tests de ROI."""

    df = pd.read_csv(pred_path)
    logger.info(f"  Prédictions chargées : {len(df)} lignes depuis {pred_path.name}")
    logger.info(f"  Colonnes : {list(df.columns)}")

    # Normalisation des colonnes
    rename = {
        "prob_home": "prob_H",
        "prob_draw": "prob_D",
        "prob_away": "prob_A",
        "pred":      "predicted_result",
        "league":    "league_source",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Prédiction = classe avec proba max (si absente)
    if "predicted_result" not in df.columns:
        df["predicted_result"] = df[["prob_H", "prob_D", "prob_A"]].idxmax(axis=1).str[-1]

    # Enrichissement avec les cotes depuis DuckDB (pour ROI réel)
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        df_odds = conn.execute("""
            SELECT
                f.final_match_id,
                o.odds_avg_h    AS odd_H,
                o.odds_avg_d    AS odd_D,
                o.odds_avg_a    AS odd_A,
                o.market_prob_h AS implied_H,
                o.market_prob_d AS implied_D,
                o.market_prob_a AS implied_A,
                o.season
            FROM silver.odds o
            LEFT JOIN gold.features_final f
                ON  o.date::DATE    = f.date::DATE
                AND o.home_team     = f.team
                AND o.league_source = f.league_source
                AND o.season        = f.season
                AND f.venue         = 'Home'
            WHERE o.odds_avg_h IS NOT NULL
        """).df()
        conn.close()
        df = df.merge(df_odds, on="final_match_id", how="left")
        logger.info(f"  Cotes jointes : {df['odd_H'].notna().sum()} matchs avec cotes")
    except Exception as e:
        logger.warning(f"  Impossible de charger les cotes DuckDB : {e}")
        logger.warning("  Tests ROI seront basés sur résultats uniquement (sans cotes réelles)")

    logger.info(f"  Dataset final : {len(df)} matchs")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TEST 1 — MISE FIXE SUR LA PRÉDICTION DU MODÈLE
# ══════════════════════════════════════════════════════════════════════════════

def test_flat_stake(df: pd.DataFrame) -> None:
    """
    Parie 1 unité sur le résultat prédit par le modèle, sans filtre.
    C'est le test de base : le modèle bat-il le marché ?
    """
    sep = "─" * 55

    def get_odd(row, outcome):
        return row.get(f"odd_{outcome}", None)

    df = df.copy()
    df["correct"]    = df["predicted_result"] == df["actual_result"]
    df["odd_played"] = df.apply(lambda r: get_odd(r, r["predicted_result"]), axis=1)
    df               = df.dropna(subset=["odd_played"])

    df["profit"] = df.apply(
        lambda r: r["odd_played"] - 1 if r["correct"] else -1.0, axis=1
    )

    n      = len(df)
    wr     = df["correct"].mean()
    roi    = df["profit"].sum() / n
    profit = df["profit"].sum()

    logger.info(f"\n{sep}")
    logger.info("  TEST 1 — Mise fixe sur prédiction du modèle")
    logger.info(sep)
    logger.info(f"  Matchs          : {n}")
    logger.info(f"  Taux de réussite: {wr:.2%}")
    logger.info(f"  Profit total    : {profit:+.2f} u")
    logger.info(f"  ROI mise fixe   : {roi:+.2%}")
    logger.info(f"  Cote moyenne    : {df['odd_played'].mean():.2f}")

    # Par outcome
    logger.info(f"\n  Par résultat prédit :")
    for out in ["H", "D", "A"]:
        sub = df[df["predicted_result"] == out]
        if len(sub) == 0:
            continue
        logger.info(
            f"    {out} : {len(sub):4d} paris | "
            f"WR {sub['correct'].mean():.2%} | "
            f"ROI {sub['profit'].sum()/len(sub):+.2%} | "
            f"cote moy {sub['odd_played'].mean():.2f}"
        )

    # Par ligue
    if "league_source" in df.columns:
        logger.info(f"\n  Par ligue :")
        for league, grp in df.groupby("league_source"):
            logger.info(
                f"    {league:<20} : {len(grp):4d} paris | "
                f"WR {grp['correct'].mean():.2%} | "
                f"ROI {grp['profit'].sum()/len(grp):+.2%}"
            )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 2 — MISE FIXE UNIQUEMENT QUAND LE MODÈLE EST CONFIANT
# ══════════════════════════════════════════════════════════════════════════════

def test_confidence_filter(df: pd.DataFrame) -> None:
    """
    Même test mais avec différents seuils de confiance.
    Montre à partir de quel seuil le modèle a vraiment un signal.
    """
    sep = "─" * 55
    logger.info(f"\n{sep}")
    logger.info("  TEST 2 — Mise fixe par seuil de confiance")
    logger.info(sep)

    df = df.copy()
    df["max_prob"]       = df[["prob_H", "prob_D", "prob_A"]].max(axis=1)
    df["predicted_result"] = df[["prob_H", "prob_D", "prob_A"]].idxmax(axis=1).str[-1]
    df["correct"]        = df["predicted_result"] == df["actual_result"]
    df["odd_played"]     = df.apply(
        lambda r: r.get(f"odd_{r['predicted_result']}"), axis=1
    )
    df = df.dropna(subset=["odd_played"])
    df["profit"] = df.apply(
        lambda r: r["odd_played"] - 1 if r["correct"] else -1.0, axis=1
    )

    thresholds = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    logger.info(f"  {'Seuil':>6} | {'N':>5} | {'WR':>7} | {'ROI':>8} | {'Profit':>8}")
    logger.info(f"  {'─'*6}-+-{'─'*5}-+-{'─'*7}-+-{'─'*8}-+-{'─'*8}")
    for t in thresholds:
        sub = df[df["max_prob"] >= t]
        if len(sub) < 10:
            continue
        roi    = sub["profit"].sum() / len(sub)
        profit = sub["profit"].sum()
        logger.info(
            f"  {t:>6.0%} | {len(sub):>5} | "
            f"{sub['correct'].mean():>7.2%} | {roi:>+8.2%} | {profit:>+8.2f}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# TEST 3 — COMPARAISON MODÈLE VS MARCHÉ (PROBA IMPLICITE)
# ══════════════════════════════════════════════════════════════════════════════

def test_model_vs_market(df: pd.DataFrame) -> None:
    """
    Compare la proba du modèle vs la proba implicite du marché
    sur le résultat réel. Un bon modèle doit avoir une proba
    plus élevée que le marché sur les bons résultats.
    """
    sep = "─" * 55
    logger.info(f"\n{sep}")
    logger.info("  TEST 3 — Modèle vs Marché (calibration comparée)")
    logger.info(sep)

    df = df.copy()

    def get_model_prob(row):
        return row.get(f"prob_{row['actual_result']}", None)

    def get_market_prob(row):
        return row.get(f"implied_{row['actual_result']}", None)

    df["model_prob_correct"]  = df.apply(get_model_prob, axis=1)
    df["market_prob_correct"] = df.apply(get_market_prob, axis=1)
    df = df.dropna(subset=["model_prob_correct", "market_prob_correct"])

    diff = df["model_prob_correct"] - df["market_prob_correct"]
    logger.info(f"  Matchs analysés    : {len(df)}")
    logger.info(f"  Proba modèle moy   : {df['model_prob_correct'].mean():.4f}")
    logger.info(f"  Proba marché moy   : {df['market_prob_correct'].mean():.4f}")
    logger.info(f"  Différence moyenne : {diff.mean():+.4f}")
    logger.info(f"  % modèle > marché  : {(diff > 0).mean():.2%}")
    logger.info("")
    logger.info("  Interprétation :")
    if diff.mean() > 0.01:
        logger.info("  ✅ Le modèle assigne en moyenne plus de proba au bon résultat")
        logger.info("     que le marché → signal potentiel exploitable")
    elif diff.mean() > -0.01:
        logger.info("  ⚠️  Le modèle est équivalent au marché → pas d'edge réel")
    else:
        logger.info("  ❌ Le marché est meilleur que le modèle → retravailler les features")

    # Par ligue
    if "league_source" in df.columns:
        logger.info(f"\n  Par ligue (diff modèle - marché sur résultat réel) :")
        for league, grp in df.groupby("league_source"):
            d = (grp["model_prob_correct"] - grp["market_prob_correct"]).mean()
            flag = "✅" if d > 0.01 else ("⚠️ " if d > -0.01 else "❌")
            logger.info(f"    {flag} {league:<20} : {d:+.4f}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(pred_path: Path) -> None:
    logger.info("=" * 55)
    logger.info("  DIAGNOSTIC SIGNAL MODÈLE — Mise fixe")
    logger.info("=" * 55)

    df = load_data(pred_path)

    test_flat_stake(df)
    test_confidence_filter(df)
    test_model_vs_market(df)

    logger.info("\n" + "=" * 55)
    logger.info("  Lecture des résultats :")
    logger.info("  TEST 1 ROI > 0  → modèle profitable en isolation")
    logger.info("  TEST 2          → trouve le bon seuil de confiance")
    logger.info("  TEST 3 diff > 0 → modèle meilleur que le marché")
    logger.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--predictions",
        type=Path,
        default=MODELS_DIR / "predictions_test.csv",
        help="Chemin vers le CSV de prédictions (défaut: models/predictions_test.csv)",
    )
    args = parser.parse_args()
    main(args.predictions)