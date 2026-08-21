"""
backtest_1n2.py — Backtest value du modèle 1N2 contre le marché.

Charge models/resultat_1n2.joblib (calibré), prédit sur la saison, calcule
l'edge (proba_modèle − proba_marché), sélectionne les value bets, mise en Kelly
fractionnaire, règle contre le résultat réel et sort ROI / bankroll / CLV.
Balaie marché (Pinnacle vs moyen) × seuil d'edge pour repérer une éventuelle
poche d'edge — ou confirmer proprement qu'il n'y en a pas.

Lancement :  python pipelines/backtest_1n2.py [--season 2024-2025]
"""
# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import argparse

import joblib
import numpy as np
import pandas as pd

import ml_common as mc

# (issue, proba modèle, proba pinnacle, proba moyenne, proba clôture pinnacle, cote)
OUTCOMES = [
    ("H", "p_H", "pinnacle_prob_team", "market_prob_team", "pinnacle_prob_close_team", "odds_pinnacle_team"),
    ("D", "p_D", "pinnacle_prob_draw", "market_prob_draw", "pinnacle_prob_close_draw", "odds_pinnacle_draw"),
    ("A", "p_A", "pinnacle_prob_opp",  "market_prob_opp",  "pinnacle_prob_close_opp",  "odds_pinnacle_opp"),
]


def build_bets(cfg, season):
    """Une ligne par (match, issue) : proba modèle, proba marché, cote, clôture,
    résultat. On prépare X sur TOUT le mart (encodage catégoriel cohérent avec
    l'entraînement) avant de filtrer sur la saison."""
    df = mc.load_mart(cfg, "mart_1n2")
    df = df[df["result_1n2"].notna()].copy()
    bundle = joblib.load(mc.MODELS_DIR / "resultat_1n2.joblib")
    model, feats = bundle["model"], bundle["features"]

    X = mc.prepare_x(df, "result_1n2", None).reindex(columns=feats, fill_value=0)
    proba = model.predict_proba(X)
    df["p_H"], df["p_D"], df["p_A"] = proba[:, 0], proba[:, 1], proba[:, 2]

    df = df[(df["season"] == season) & (df["is_home"] == True)
            & df["odds_pinnacle_team"].notna()]
    parts = []
    for out, pcol, pin, avg, pinc, odd in OUTCOMES:
        parts.append(pd.DataFrame({
            "match_id": df["match_id"].values, "date": df["date"].values, "outcome": out,
            "p_model": df[pcol].values, "p_pinnacle": df[pin].values, "p_avg": df[avg].values,
            "p_close": df[pinc].values, "odds": df[odd].values,
            "won": (df["result_1n2"] == out).astype(int).values,
        }))
    bets = pd.concat(parts, ignore_index=True)
    bets["clv_vs_pinnacle"] = bets["p_close"] - bets["p_pinnacle"]   # >0 = clôture vers nous
    bets["clv_vs_avg"] = bets["p_close"] - bets["p_avg"]
    return bets


def run(bets, market, edge_min, conf_min, kelly_frac, bankroll_init):
    """Sélectionne les value bets contre `market` et simule. Retourne les métriques."""
    pm = bets["p_" + market]
    sel = ((bets["p_model"] - pm) > edge_min) & (bets["p_model"] > conf_min) \
        & (bets["odds"] > 1) & pm.notna()
    b = bets[sel].sort_values("date")
    if len(b) == 0:
        return dict(n=0, hit=0.0, roi=0.0, bankroll=bankroll_init, clv=0.0)

    # ROI à mise plate (1 unité par pari) — signal d'edge indépendant de l'ordre.
    pnl_flat = np.where(b["won"].values == 1, b["odds"].values - 1, -1.0)
    roi = 100 * pnl_flat.sum() / len(b)

    # Bankroll séquentielle en Kelly fractionnaire (par date).
    f = np.clip((b["p_model"].values * b["odds"].values - 1) / (b["odds"].values - 1), 0, 1) * kelly_frac
    bankroll = bankroll_init
    for won, odd, frac in zip(b["won"].values, b["odds"].values, f):
        stake = bankroll * frac
        bankroll += (odd - 1) * stake if won else -stake

    # CLV : la clôture Pinnacle a-t-elle bougé vers nos paris ? (>0 = on a pris de la valeur)
    clv = 100 * (b["p_close"] - b["p_" + market]).mean()
    return dict(n=len(b), hit=100 * b["won"].mean(), roi=roi, bankroll=bankroll, clv=clv)


def main(season):
    cfg, _ = mc.load_configs()
    bt = cfg.get("backtest", {})
    conf = bt.get("CONFIDENCE_MIN", 0.0)
    kelly = bt.get("KELLY_FRACTION", 0.25)
    bank0 = bt.get("BANKROLL_INIT", 1000.0)

    bets = build_bets(cfg, season)
    print(f"Saison {season} : {bets['match_id'].nunique():,} matchs, "
          f"{len(bets):,} paris candidats  (conf≥{conf}, Kelly×{kelly})\n")
    print(f"{'marché':10}{'edge≥':>7}{'nBets':>7}{'hit%':>7}{'ROI%':>8}{'CLV%':>7}{'bankroll':>10}")
    for market in ["pinnacle", "avg"]:
        for edge in [0.03, 0.05, 0.08, 0.12]:
            r = run(bets, market, edge, conf, kelly, bank0)
            print(f"{market:10}{edge:>7}{r['n']:>7}{r['hit']:>7.1f}"
                  f"{r['roi']:>8.1f}{r['clv']:>7.2f}{r['bankroll']:>10.0f}")

    out = mc.ROOT_DIR / "reports" / f"backtest_1n2_{season}.csv"
    out.parent.mkdir(exist_ok=True)
    bets.to_csv(out, index=False)
    print(f"\nParis détaillés exportés : {out}")


if __name__ == "__main__":
    cfg, _ = mc.load_configs()
    parser = argparse.ArgumentParser(description="Backtest value 1N2.")
    parser.add_argument("--season", default=cfg["train"]["TEST_SEASON"],
                        help="Saison à backtester (défaut : TEST_SEASON de config).")
    args = parser.parse_args()
    main(args.season)
