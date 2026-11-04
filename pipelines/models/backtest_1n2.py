"""
backtest_1n2.py — Backtest value du modèle 1N2 contre le marché.

Charge models/<clé>.joblib (calibré, défaut resultat_1n2), prédit sur la saison, calcule
l'edge (proba_modèle − proba_marché), sélectionne les value bets, mise en Kelly
fractionnaire, règle contre le résultat réel et sort ROI / bankroll / CLV.
Balaie le seuil d'edge (marché : Pinnacle uniquement) pour repérer une éventuelle
poche d'edge — ou confirmer proprement qu'il n'y en a pas.
Les cotes viennent de gold.gold_odds_pinnacle (1 ligne par match), jointes par match_id.

Lancement :  python pipelines/models/backtest_1n2.py [--season 2024-2025] [--model resultat_1n2_avant_compo]
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

MODEL_KEY = "resultat_1n2"                 # clé PAR DÉFAUT dans config/models.yml

# (issue, proba modèle, proba Pinnacle ouverture, proba Pinnacle clôture, cote d'ouverture)
# Colonnes de gold.gold_odds_pinnacle ; home/draw/away = équipe listée à domicile.
OUTCOMES = [
    ("H", "p_H", "dec_prob_open_home", "dec_prob_close_home", "dec_odds_open_home"),
    ("D", "p_D", "dec_prob_open_draw", "dec_prob_close_draw", "dec_odds_open_draw"),
    ("A", "p_A", "dec_prob_open_away", "dec_prob_close_away", "dec_odds_open_away"),
]
ODDS_COLS = ["str_match_id", "str_home_team_id"] + [c for o in OUTCOMES for c in o[2:]]


def build_bets(cfg, season, model_key=MODEL_KEY):
    """Une ligne par (match, issue) : proba modèle, proba marché, cote, clôture,
    résultat. On prépare X sur TOUT le mart (encodage catégoriel cohérent avec
    l'entraînement) avant de filtrer sur la saison."""
    _, models = mc.load_configs()
    df = mc.load_mart(cfg, models[model_key]["mart"])
    df = df[df["str_result_1n2"].notna()].copy()
    bundle = joblib.load(mc.MODELS_DIR / f"{model_key}.joblib")
    model, feats = bundle["model"], bundle["features"]

    X = mc.align_features(mc.prepare_x(df, "str_result_1n2", None), feats)
    proba = mc.floor_proba(model.predict_proba(X))   # plancher 0,5 % : pas de probabilité nulle
    df["p_H"], df["p_D"], df["p_A"] = proba[:, 0], proba[:, 1], proba[:, 2]

    # Une prédiction par match : la ligne de l'équipe « domicile » des cotes (celle que le
    # calendrier liste en premier, str_home_team_id). Valable aussi sur terrain neutre,
    # où aucune ligne n'a int_is_home = 1.
    df = df[df["str_season"] == season]
    # Cotes : jointure par match_id sur gold_odds_pinnacle (1 ligne par match).
    odds = mc.load_odds(cfg)[ODDS_COLS]
    n_total = df["str_match_id"].nunique()
    df = df.merge(odds, on="str_match_id", how="inner")
    df = df[df["str_team_id"].astype(str) == df["str_home_team_id"].astype(str)]
    n_matchs = n_total
    df = df[df["dec_odds_open_home"].notna()]
    print(f"{season} : {n_matchs:,} matchs, {len(df):,} avec cotes Pinnacle (ligne de l'équipe domicile des cotes)")
    parts = []
    for out, pcol, pin, pinc, odd in OUTCOMES:
        parts.append(pd.DataFrame({
            "str_match_id": df["str_match_id"].values, "dt_date": df["dt_date"].values, "outcome": out,
            "p_model": df[pcol].values, "p_pinnacle": df[pin].values,
            "p_close": df[pinc].values, "odds": df[odd].values,
            "won": (df["str_result_1n2"] == out).astype(int).values,
        }))
    bets = pd.concat(parts, ignore_index=True)
    bets["clv_vs_pinnacle"] = bets["p_close"] - bets["p_pinnacle"]   # >0 = clôture vers nous
    return bets


def run(bets, market, edge_min, conf_min, kelly_frac, bankroll_init):
    """Sélectionne les value bets contre `market` et simule. Retourne les métriques."""
    pm = bets["p_" + market]
    sel = ((bets["p_model"] - pm) > edge_min) & (bets["p_model"] > conf_min) \
        & (bets["odds"] > 1) & pm.notna()
    b = bets[sel].sort_values("dt_date")
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


def main(season, model_key=MODEL_KEY):
    cfg, models = mc.load_configs()
    if model_key not in models:
        raise SystemExit(f"Modèle inconnu '{model_key}' (clés de config/models.yml : {', '.join(models)})")
    bt = cfg.get("backtest", {})
    conf = bt.get("CONFIDENCE_MIN", 0.0)
    kelly = bt.get("KELLY_FRACTION", 0.25)
    bank0 = bt.get("BANKROLL_INIT", 1000.0)

    bets = build_bets(cfg, season, model_key)
    print(f"Saison {season} : {bets['str_match_id'].nunique():,} matchs, "
          f"{len(bets):,} paris candidats  (conf≥{conf}, Kelly×{kelly})\n")
    print(f"{'marché':10}{'edge≥':>7}{'nBets':>7}{'hit%':>7}{'ROI%':>8}{'CLV%':>7}{'bankroll':>10}")
    for market in ["pinnacle"]:
        for edge in [0.03, 0.05, 0.08, 0.12]:
            r = run(bets, market, edge, conf, kelly, bank0)
            print(f"{market:10}{edge:>7}{r['n']:>7}{r['hit']:>7.1f}"
                  f"{r['roi']:>8.1f}{r['clv']:>7.2f}{r['bankroll']:>10.0f}")

    tag = "1n2" if model_key == MODEL_KEY else model_key
    out = mc.ROOT_DIR / "reports" / f"backtest_{tag}_{season}.csv"
    out.parent.mkdir(exist_ok=True)
    bets.to_csv(out, index=False)
    print(f"\nParis détaillés exportés : {out}")


if __name__ == "__main__":
    cfg, _ = mc.load_configs()
    parser = argparse.ArgumentParser(description="Backtest value 1N2.")
    parser.add_argument("--season", default=cfg["train"]["TEST_SEASON"],
                        help="Saison à backtester (défaut : TEST_SEASON de config).")
    parser.add_argument("--model", default=MODEL_KEY,
                        help="clé du modèle dans config/models.yml (défaut : resultat_1n2)")
    args = parser.parse_args()
    main(args.season, model_key=args.model)
