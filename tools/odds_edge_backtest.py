"""
odds_edge_backtest.py — palier 5. Sans modèle : parier quand la cote d'un bookmaker x dépasse la cote équitable de Pinnacle
(démarginée) d'au moins `seuil`. Données : data/raw/bets/*.csv (football-data.co.uk), 5 ligues, 2017-18 -> 2025-26.
Sélection SANS anticipation : Pinnacle ouverture (PSH/PSD/PSA) contre la cote d'ouverture du bookmaker, au même instant de collecte.
Mise fixe 1. ROI = profit / nb de paris. IC95 par bootstrap sur les paris.
Variantes (3 au plus) :  V1 démarginage proportionnel ; V2 démarginage par puissance (corrige le biais favori/outsider) ;
                         V3 meilleure cote du marché (Max*) face à Pinnacle ouverture, démarginage puissance (plafond irréaliste).
Règle de décision fixée AVANT la mesure : ROI poolé avec borne basse IC95 > 0 ET ROI > 0 sur >= 7 saisons sur 9.
Nombre de combinaisons testées affiché : à lire avant de conclure (comparaisons multiples).
Usage : python tools/odds_edge_backtest.py
"""
import glob
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BOOKS = ["B365", "WH", "BW", "IW", "VC", "LB"]
THRESH = [0.0, 0.02, 0.04, 0.06]


def load():
    out = []
    for f in sorted(glob.glob(str(ROOT / "data/raw/bets/*.csv"))):
        m = re.match(r"(.*)_(\d{4})\.csv", Path(f).name)
        d = pd.read_csv(f, encoding="latin-1", low_memory=False)
        d = d[d["FTR"].isin(["H", "D", "A"])].copy()
        d["ligue"], d["saison"] = m.group(1), m.group(2)
        if "MaxH" not in d.columns and "BbMxH" in d.columns:
            for o in "HDA":
                d[f"Max{o}"] = d[f"BbMx{o}"]
        out.append(d)
    return pd.concat(out, ignore_index=True)


def devig(o, method):
    """o : (n,3) cotes ; retourne les probabilités équitables (n,3)."""
    inv = 1.0 / o
    if method == "prop":
        return inv / inv.sum(1, keepdims=True)
    lo, hi = np.ones(len(o)), np.full(len(o), 3.0)   # puissance k tel que somme(inv^k)=1
    for _ in range(40):
        k = (lo + hi) / 2
        s = (inv ** k[:, None]).sum(1)
        lo, hi = np.where(s > 1, k, lo), np.where(s > 1, hi, k)
    return inv ** ((lo + hi) / 2)[:, None]


def run(df, book, method, thr, rng):
    cols = [f"{book}{o}" for o in "HDA"]
    if not all(c in df.columns for c in cols) or not all(c in df.columns for c in ("PSH", "PSD", "PSA")):
        return None
    d = df.dropna(subset=cols + ["PSH", "PSD", "PSA"]).copy()
    d = d[(d[cols] > 1).all(axis=1) & (d[["PSH", "PSD", "PSA"]] > 1).all(axis=1)]
    if len(d) == 0:
        return None
    p = devig(d[["PSH", "PSD", "PSA"]].values, method)
    o = d[cols].values
    won = np.stack([(d["FTR"] == r).values for r in "HDA"], axis=1)
    bet = (o * p - 1) > thr
    profit = np.where(bet, np.where(won, o - 1, -1.0), 0.0)
    sea = np.repeat(d["saison"].values[:, None], 3, axis=1)[bet]
    pr, odds = profit[bet], o[bet]
    if len(pr) < 30:
        return dict(n=len(pr), roi=np.nan, lo=np.nan, hi=np.nan, pos="-", cote=np.nan, matchs=len(d))
    b = [pr[rng.integers(0, len(pr), len(pr))].mean() for _ in range(400)]
    seas = sorted(set(sea))
    pos = sum(pr[sea == s].mean() > 0 for s in seas if (sea == s).sum() >= 10)
    return dict(n=len(pr), roi=pr.mean(), lo=np.percentile(b, 2.5), hi=np.percentile(b, 97.5),
                pos=f"{pos}/{len(seas)}", cote=odds.mean(), matchs=len(d))


def main():
    df = load()
    rng = np.random.default_rng(0)
    print(f"{len(df):,} matchs, saisons {sorted(df.saison.unique())[0]}..{sorted(df.saison.unique())[-1]}")
    res, ncomb = [], 0
    for var, books, method in (("V1 prop", BOOKS, "prop"), ("V2 puiss", BOOKS, "power"), ("V3 Max", ["Max"], "power")):
        for bk in books:
            for t in THRESH:
                r = run(df, bk, method, t, rng)
                ncomb += 1
                if r:
                    res.append(dict(variante=var, book=bk, seuil=t, **r))
    R = pd.DataFrame(res)
    R.to_csv(ROOT / "reports" / "odds_edge_backtest.csv", index=False)
    print(f"combinaisons testées : {ncomb}")
    pd.set_option("display.width", 200)
    print(R.round(4).to_string(index=False))


if __name__ == "__main__":
    main()
