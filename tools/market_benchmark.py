"""Écart du modèle 1N2 au marché (Pinnacle ouverture/clôture), par saison et poolé.

Entrée : un CSV rolling_eval (colonnes str_match_id, str_season, str_comp_category, y, p_H, p_D, p_A).
Odds : gold.gold_odds_pinnacle, lecture seule — jamais des features.
Périmètre : Big5, matchs ayant des probas d'ouverture ET de clôture.
Métriques : précision, RPS, log-loss (plus bas = mieux, sauf précision).

Usage : python tools/market_benchmark.py reports/rolling_eval_<model>_<tag>.csv
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import ml_common as mc  # noqa: E402


def rps(P, y):
    """Ranked Probability Score 1N2 (ordre H<D<A), moyenne par match."""
    cp = np.cumsum(P, axis=1)[:, :2]
    co = np.cumsum(np.eye(3)[y], axis=1)[:, :2]
    return ((cp - co) ** 2).sum(axis=1) / 2


def logloss(P, y):
    return -np.log(np.clip(P[np.arange(len(y)), y], 1e-15, None))


def score(P, y):
    return dict(acc=(P.argmax(1) == y).mean(), rps=rps(P, y).mean(), ll=logloss(P, y).mean())


def main(path):
    cfg, _ = mc.load_configs()
    res = pd.read_csv(path)
    res = res[res["str_comp_category"] == "Big5"]
    odds = mc.load_odds(cfg)
    cols = [f"dec_prob_{t}_{o}" for t in ("open", "close") for o in ("home", "draw", "away")]
    df = res.merge(odds[["str_match_id"] + cols], on="str_match_id", how="left")
    n_all = len(df)
    df = df.dropna(subset=cols)
    print(f"Big5 : {n_all:,} matchs, {n_all - len(df):,} exclus (sans cotes), {len(df):,} comparés")
    sources = {
        "modele": df[["p_H", "p_D", "p_A"]].values,
        "open": df[cols[:3]].values,
        "close": df[cols[3:]].values,
    }
    rows = []
    for season, g in [("POOLE", df)] + list(df.groupby("str_season")):
        y = g["y"].values.astype(int)
        r = {"saison": season, "n": len(g)}
        for name, P in sources.items():
            s = score(P[g.index.map(df.index.get_loc)], y)
            r.update({f"{name}_{k}": v for k, v in s.items()})
        r["ecart_rps_vs_close"] = r["modele_rps"] - r["close_rps"]
        r["ecart_ll_vs_close"] = r["modele_ll"] - r["close_ll"]
        rows.append(r)
    out = pd.DataFrame(rows).round(4)
    print(out.to_string(index=False))
    dest = Path(path).with_name(Path(path).stem.replace("rolling_eval", "market_gap") + ".csv")
    out.to_csv(dest, index=False)
    print(f"-> {dest}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("rolling_csv")
    main(ap.parse_args().rolling_csv)
