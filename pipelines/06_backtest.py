"""
Pipeline 06 — Backtest
======================
Simule une stratégie de paris value bet sur les prédictions du modèle.
Utilise les cotes Average marché comme proxy Winamax.

Stratégie :
  - Sélection : value bet (edge > EDGE_MIN) ET confidence > CONFIDENCE_MIN
  - Mise      : Half Kelly sur bankroll courante
  - Cotes     : Average marché (proxy Winamax)

Usage :
    python pipelines/06_backtest.py
    python pipelines/06_backtest.py --seasons 2023-2024 2024-2025
    python pipelines/06_backtest.py --edge-min 0.04 --confidence-min 0.45
"""

import argparse
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR   = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODEL_PATH = ROOT_DIR / "models" / "football_stacking_v1.joblib"
OUTPUT_DIR = ROOT_DIR / "models"

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/backtest.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# ── Paramètres stratégie ──────────────────────────────────────────────────────

BANKROLL_INIT  = 1000.0   # bankroll initiale en unités
EDGE_MIN       = 0.04     # edge minimum pour parier (prob_model - prob_implied)
CONFIDENCE_MIN = 0.45     # confiance minimum du modèle
KELLY_FRACTION = 0.5      # Half Kelly
MAX_MISE_PCT   = 0.05     # mise max = 5% de la bankroll (protection)
MIN_MISE       = 1.0      # mise minimale en unités

# Saisons propres pour le backtest (hors train set)
BACKTEST_SEASONS_DEFAULT = ["2023-2024", "2024-2025"]


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT DONNÉES ET PRÉDICTIONS
# ══════════════════════════════════════════════════════════════════════════════

def load_predictions(seasons: list[str]) -> pd.DataFrame:
    season_to_file = {
        "2023-2024": OUTPUT_DIR / "predictions_val.csv",
        "2024-2025": OUTPUT_DIR / "predictions_2425.csv",
    }

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    seasons_df = conn.execute("""
        SELECT DISTINCT final_match_id, season
        FROM gold.features_final
        WHERE comp_category = 'Big5' AND venue = 'Home'
    """).df()
    conn.close()

    dfs = []
    for season in seasons:
        path = season_to_file.get(season)
        if not path or not path.exists():
            logger.warning(f"  Fichier manquant pour {season}")
            continue

        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])

        # Normaliser les colonnes selon la source
        # predictions_val.csv (04_train.py) a un format différent
        if "prob_home" in df.columns:
            df = df.rename(columns={
                "prob_home": "prob_H",
                "prob_draw": "prob_D",
                "prob_away": "prob_A",
                "pred":      "predicted_result",
            })

        # Garder uniquement les colonnes nécessaires
        keep_cols = ["final_match_id", "date", "home_team", "away_team",
                     "prob_H", "prob_D", "prob_A", "predicted_result"]
        df = df[[c for c in keep_cols if c in df.columns]]

        # Ajouter season si absente
        if "season" not in df.columns:
            df = df.merge(seasons_df, on="final_match_id", how="left")

        df = df[df["season"] == season].copy()
        dfs.append(df)
        logger.info(f"  {season} : {len(df)} matchs depuis {path.name}")

    if not dfs:
        raise FileNotFoundError("Aucun fichier de prédictions trouvé")

    result = pd.concat(dfs, ignore_index=True)
    logger.info(f"  Total : {len(result)} matchs")
    return result


def load_odds_and_results(seasons: list[str]) -> pd.DataFrame:
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    seasons_str = ", ".join(f"'{s}'" for s in seasons)

    df = conn.execute(f"""
        SELECT
            o.date::DATE            AS date,
            o.season,
            o.league_source,
            o.home_team,
            o.away_team,
            o.result_fdc            AS actual_result,

            -- Cotes Average marché — noms directs depuis silver.odds
            o.odds_avg_h            AS odd_H,
            o.odds_avg_d            AS odd_D,
            o.odds_avg_a            AS odd_A,

            -- Probabilités implicites Average
            o.market_prob_h         AS implied_prob_H,
            o.market_prob_d         AS implied_prob_D,
            o.market_prob_a         AS implied_prob_A,

            f.final_match_id

        FROM silver.odds o
        LEFT JOIN gold.features_final f
            ON  o.date::DATE    = f.date::DATE
            AND o.home_team     = f.team
            AND o.league_source = f.league_source
            AND o.season        = f.season
            AND f.venue         = 'Home'
        WHERE o.season IN ({seasons_str})
          AND o.odds_avg_h IS NOT NULL
          AND o.result_fdc IS NOT NULL
    """).df()

    conn.close()
    logger.info(f"  Cotes + résultats chargés : {len(df)} matchs")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# KELLY CRITERION
# ══════════════════════════════════════════════════════════════════════════════

def kelly_stake(prob: float, odd: float,
                bankroll: float,
                fraction: float = KELLY_FRACTION,
                max_pct: float  = MAX_MISE_PCT,
                min_mise: float = MIN_MISE) -> float:
    """
    Calcule la mise Half Kelly.

    f* = (p * odd - 1) / (odd - 1)
    mise = bankroll * f* * fraction

    Protections :
      - f* <= 0 → pas de mise (edge négatif)
      - mise cappée à max_pct * bankroll
      - mise minimale = min_mise
    """
    if odd <= 1 or prob <= 0:
        return 0.0

    f_full = (prob * odd - 1) / (odd - 1)

    if f_full <= 0:
        return 0.0

    mise = bankroll * f_full * fraction
    mise = min(mise, bankroll * max_pct)   # cap à 5% bankroll
    mise = max(mise, min_mise)             # mise minimale

    return round(mise, 2)


# ══════════════════════════════════════════════════════════════════════════════
# SÉLECTION DES PARIS
# ══════════════════════════════════════════════════════════════════════════════

def select_bets(df_pred: pd.DataFrame,
                df_odds: pd.DataFrame,
                edge_min: float       = EDGE_MIN,
                confidence_min: float = CONFIDENCE_MIN) -> pd.DataFrame:
    """
    Joint prédictions et cotes, calcule l'edge et filtre les value bets.

    Un pari est sélectionné si :
      1. prob_model > implied_prob + edge_min  (value bet)
      2. prob_model > confidence_min           (confiance suffisante)
    """
    # Jointure prédictions × cotes
    odds_cols = ["final_match_id", "actual_result",
                 "odd_H", "odd_D", "odd_A",
                 "implied_prob_H", "implied_prob_D", "implied_prob_A"]
    
    df = df_pred.merge(
        df_odds[odds_cols],
        on="final_match_id",
        how="inner"
    )

    logger.info(f"  Après jointure : {len(df)} matchs avec prédictions et cotes")

    if df.empty:
        return df

    # Calcul edge par outcome
    bets = []
    for outcome in ["H", "D", "A"]:
        prob_col     = f"prob_{outcome}"
        implied_col  = f"implied_prob_{outcome}"
        odd_col      = f"odd_{outcome}"

        mask_value = (
            df[prob_col].notna() &
            df[implied_col].notna() &
            df[odd_col].notna() &
            (df[prob_col] - df[implied_col] > edge_min) &
            (df[prob_col] > confidence_min) &
            (df[odd_col] > 1.0) &
            df["actual_result"].notna()   # ← directement sans passer par actual_result_col()
        )

        # Règle spéciale Draw — seuil de confiance plus élevé
        if outcome == "D":
            mask_value = mask_value & (df[prob_col] > 0.38)
            draw_candidates = df[mask_value & df[prob_col].notna()]
            logger.debug(f"  Draw candidates avant filtre 0.38 : {len(draw_candidates)}")
            logger.debug(f"  prob_D min/max : {draw_candidates[prob_col].min():.3f} / {draw_candidates[prob_col].max():.3f}")
            mask_value = mask_value & (df[prob_col] > 0.38)
            logger.debug(f"  Draw après filtre 0.38 : {mask_value.sum()}")

        sub = df[mask_value].copy()
        sub["bet_outcome"]   = outcome
        sub["prob_model"]    = sub[prob_col]
        sub["implied_prob"]  = sub[implied_col]
        sub["odd"]           = sub[odd_col]
        sub["edge"]          = (sub[prob_col] - sub[implied_col]).round(4)
        bets.append(sub)

    if not bets:
        return pd.DataFrame()

    df_bets = pd.concat(bets, ignore_index=True)

    # Si plusieurs outcomes sélectionnés pour le même match → garder le meilleur edge
    df_bets = df_bets.sort_values("edge", ascending=False)
    df_bets = df_bets.drop_duplicates(subset=["final_match_id"], keep="first")
    df_bets = df_bets.sort_values("date").reset_index(drop=True)

    logger.info(f"  Value bets sélectionnés : {len(df_bets)}")
    logger.info(f"  Répartition : {df_bets['bet_outcome'].value_counts().to_dict()}")
    return df_bets


def actual_result_col(df):
    """Nom de la colonne résultat réel selon le merge."""
    for col in ["actual_result", "result_fdc"]:
        if col in df.columns:
            return col
    raise KeyError("Colonne résultat réel introuvable")


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION BANKROLL
# ══════════════════════════════════════════════════════════════════════════════

def simulate_bankroll(df_bets: pd.DataFrame,
                      bankroll_init: float = BANKROLL_INIT) -> pd.DataFrame:
    """
    Simule l'évolution de la bankroll match par match.
    Applique le Half Kelly sur la bankroll courante.
    """
    bankroll = bankroll_init
    rows     = []

    for _, row in df_bets.iterrows():
        # Réduire Kelly sur les nuls — Quarter Kelly
        fraction = KELLY_FRACTION * 0.5 if row["bet_outcome"] == "D" else KELLY_FRACTION

        mise = kelly_stake(row["prob_model"], row["odd"], bankroll, fraction=fraction)

        if mise <= 0 or bankroll < MIN_MISE:
            continue

        won = (row["bet_outcome"] == row["actual_result"])
        profit = mise * (row["odd"] - 1) if won else -mise

        bankroll += profit

        rows.append({
            "date":         row["date"],
            "season":       row.get("season", row.get("season_x", "")),
            "league":       row.get("league_source", row.get("league_source_x", "")),
            "home_team":    row.get("home_team", row.get("home_team_x", "")),
            "away_team":    row.get("away_team", row.get("away_team_x", "")),
            "bet_outcome":  row["bet_outcome"],
            "prob_model":   round(row["prob_model"], 4),
            "implied_prob": round(row["implied_prob"], 4),
            "edge":         round(row["edge"], 4),
            "odd":          round(row["odd"], 2),
            "mise":         round(mise, 2),
            "won":          int(won),
            "profit":       round(profit, 2),
            "bankroll":     round(bankroll, 2),
            "actual_result": row["actual_result"],
        })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# MÉTRIQUES
# ══════════════════════════════════════════════════════════════════════════════

def compute_metrics(df_sim: pd.DataFrame,
                    bankroll_init: float = BANKROLL_INIT) -> dict:
    """Calcule les métriques de performance de la stratégie."""
    if df_sim.empty:
        return {}

    total_bets    = len(df_sim)
    total_staked  = df_sim["mise"].sum()
    total_profit  = df_sim["profit"].sum()
    win_rate      = df_sim["won"].mean()
    roi           = total_profit / total_staked if total_staked > 0 else 0
    yield_pct     = total_profit / (total_bets * df_sim["mise"].mean()) if total_bets > 0 else 0
    final_bankroll = df_sim["bankroll"].iloc[-1]

    # Drawdown maximum
    peak      = df_sim["bankroll"].cummax()
    drawdown  = (df_sim["bankroll"] - peak) / peak
    max_dd    = drawdown.min()

    # Série perdante max
    losing_streak = 0
    max_streak    = 0
    for won in df_sim["won"]:
        if won == 0:
            losing_streak += 1
            max_streak = max(max_streak, losing_streak)
        else:
            losing_streak = 0

    # Edge moyen réalisé
    avg_edge = df_sim["edge"].mean()

    return {
        "total_bets":       total_bets,
        "win_rate":         round(win_rate, 4),
        "total_staked":     round(total_staked, 2),
        "total_profit":     round(total_profit, 2),
        "roi":              round(roi, 4),
        "yield_pct":        round(yield_pct, 4),
        "bankroll_init":    bankroll_init,
        "bankroll_final":   round(final_bankroll, 2),
        "bankroll_growth":  round((final_bankroll - bankroll_init) / bankroll_init, 4),
        "max_drawdown":     round(max_dd, 4),
        "max_losing_streak": max_streak,
        "avg_edge":         round(avg_edge, 4),
        "avg_odd":          round(df_sim["odd"].mean(), 2),
        "avg_mise":         round(df_sim["mise"].mean(), 2),
    }


def print_metrics(metrics: dict, label: str = ""):
    logger.info(f"\n── Résultats backtest {label} {'─'*(40-len(label))}")
    logger.info(f"  Paris joués        : {metrics['total_bets']}")
    logger.info(f"  Taux de victoire   : {metrics['win_rate']:.2%}")
    logger.info(f"  Total misé         : {metrics['total_staked']:.2f} u")
    logger.info(f"  Profit total       : {metrics['total_profit']:+.2f} u")
    logger.info(f"  ROI                : {metrics['roi']:+.2%}")
    logger.info(f"  Bankroll init      : {metrics['bankroll_init']:.2f} u")
    logger.info(f"  Bankroll finale    : {metrics['bankroll_final']:.2f} u")
    logger.info(f"  Croissance         : {metrics['bankroll_growth']:+.2%}")
    logger.info(f"  Drawdown max       : {metrics['max_drawdown']:.2%}")
    logger.info(f"  Série perdante max : {metrics['max_losing_streak']}")
    logger.info(f"  Edge moyen         : {metrics['avg_edge']:.2%}")
    logger.info(f"  Cote moyenne       : {metrics['avg_odd']:.2f}")


# ══════════════════════════════════════════════════════════════════════════════
# ANALYSE PAR SOUS-GROUPES
# ══════════════════════════════════════════════════════════════════════════════

def analyze_breakdown(df_sim: pd.DataFrame):
    """Analyse ROI par ligue, par outcome, et par tranche d'edge."""

    # Par outcome
    logger.info("\n── ROI par outcome ──────────────────────────────────────")
    for outcome in ["H", "D", "A"]:
        sub = df_sim[df_sim["bet_outcome"] == outcome]
        if sub.empty:
            continue
        roi = sub["profit"].sum() / sub["mise"].sum() if sub["mise"].sum() > 0 else 0
        logger.info(f"  {outcome} : {len(sub):3d} paris | WR {sub['won'].mean():.2%} | ROI {roi:+.2%}")

    # Par ligue
    logger.info("\n── ROI par ligue ────────────────────────────────────────")
    for league in sorted(df_sim["league"].unique()):
        sub = df_sim[df_sim["league"] == league]
        roi = sub["profit"].sum() / sub["mise"].sum() if sub["mise"].sum() > 0 else 0
        logger.info(f"  {league:<20} : {len(sub):3d} paris | WR {sub['won'].mean():.2%} | ROI {roi:+.2%}")

    # Par tranche d'edge
    logger.info("\n── ROI par tranche d'edge ───────────────────────────────")
    bins   = [0.04, 0.06, 0.08, 0.10, 0.15, 1.0]
    labels = ["4-6%", "6-8%", "8-10%", "10-15%", ">15%"]
    df_sim["edge_bin"] = pd.cut(df_sim["edge"], bins=bins, labels=labels)
    for label in labels:
        sub = df_sim[df_sim["edge_bin"] == label]
        if sub.empty:
            continue
        roi = sub["profit"].sum() / sub["mise"].sum() if sub["mise"].sum() > 0 else 0
        logger.info(f"  Edge {label:<8} : {len(sub):3d} paris | WR {sub['won'].mean():.2%} | ROI {roi:+.2%}")

    # Par saison
    logger.info("\n── ROI par saison ───────────────────────────────────────")
    for season in sorted(df_sim["season"].unique()):
        sub = df_sim[df_sim["season"] == season]
        roi = sub["profit"].sum() / sub["mise"].sum() if sub["mise"].sum() > 0 else 0
        logger.info(f"  {season} : {len(sub):3d} paris | WR {sub['won'].mean():.2%} | ROI {roi:+.2%}")


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main(seasons: list[str]       = None,
         edge_min: float          = EDGE_MIN,
         confidence_min: float    = CONFIDENCE_MIN,
         bankroll_init: float     = BANKROLL_INIT):

    if seasons is None:
        seasons = BACKTEST_SEASONS_DEFAULT

    logger.info("=== Démarrage backtest ===")
    logger.info(f"  Saisons        : {seasons}")
    logger.info(f"  Edge minimum   : {edge_min:.2%}")
    logger.info(f"  Confidence min : {confidence_min:.2%}")
    logger.info(f"  Kelly fraction : {KELLY_FRACTION}")
    logger.info(f"  Bankroll init  : {bankroll_init:.2f} u")

    # ── Données ───────────────────────────────────────────────────────────────
    df_pred = load_predictions(seasons)
    df_odds = load_odds_and_results(seasons)

    if df_pred.empty or df_odds.empty:
        logger.error("Données insuffisantes pour le backtest")
        return

    # ── Sélection des paris ───────────────────────────────────────────────────
    df_bets = select_bets(df_pred, df_odds, edge_min, confidence_min)

    if df_bets.empty:
        logger.warning("Aucun value bet sélectionné avec ces paramètres")
        return

    # ── Simulation bankroll ───────────────────────────────────────────────────
    df_sim = simulate_bankroll(df_bets, bankroll_init)

    if df_sim.empty:
        logger.warning("Simulation vide")
        return

    # ── Métriques globales ────────────────────────────────────────────────────
    metrics = compute_metrics(df_sim, bankroll_init)
    print_metrics(metrics, label=" × ".join(seasons))

    # ── Analyse par sous-groupes ──────────────────────────────────────────────
    analyze_breakdown(df_sim)

    # ── Export ────────────────────────────────────────────────────────────────
    out_path = OUTPUT_DIR / "backtest_results.csv"
    df_sim.to_csv(out_path, index=False)
    logger.success(f"  Résultats sauvegardés : {out_path} ({len(df_sim)} paris)")

    # Résumé rapide console
    print(f"\n{'='*50}")
    print(f"  BACKTEST {' + '.join(seasons)}")
    print(f"  {metrics['total_bets']} paris | ROI {metrics['roi']:+.2%} | Bankroll {metrics['bankroll_init']:.0f} → {metrics['bankroll_final']:.0f}")
    print(f"{'='*50}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons",        nargs="+", default=None)
    parser.add_argument("--edge-min",       type=float, default=EDGE_MIN)
    parser.add_argument("--confidence-min", type=float, default=CONFIDENCE_MIN)
    parser.add_argument("--bankroll",       type=float, default=BANKROLL_INIT)
    args = parser.parse_args()

    main(
        seasons        = args.seasons,
        edge_min       = args.edge_min,
        confidence_min = args.confidence_min,
        bankroll_init  = args.bankroll,
    )