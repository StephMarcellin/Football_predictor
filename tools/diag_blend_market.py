"""Diagnostic palier 2 : un mélange linéaire modèle/marché bat-il le marché seul ? (aucun réentraînement)
Usage : python tools/diag_blend_market.py reports/rolling_eval_<modele>_<tag>.csv
Mélange P = w*modèle + (1-w)*Pinnacle ouverture ; w balayé de 0 à 1. Sur 1 seul paramètre le sur-ajustement est faible,
mais w est choisi sur toutes les saisons : lire la colonne 'saisons gagnantes' avant de conclure."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines")); sys.path.insert(0, str(ROOT / "tools"))
import ml_common as mc
from market_benchmark import logloss, rps

cfg, _ = mc.load_configs()
res = pd.read_csv(sys.argv[1]); res = res[res.str_comp_category == "Big5"]
o = mc.load_odds(cfg); oc = [f"dec_prob_open_{x}" for x in ("home", "draw", "away")]
df = res.merge(o[["str_match_id"] + oc], on="str_match_id").dropna(subset=oc).reset_index(drop=True)
y = df.y.values.astype(int); M = df[["p_H", "p_D", "p_A"]].values; O = df[oc].values
rng = np.random.default_rng(0); rows = []
for w in np.round(np.arange(0, 1.01, 0.1), 1):
    P = w * M + (1 - w) * O
    ll = logloss(P, y); d = logloss(O, y) - ll
    sg = sum(d[df.str_season.values == s].mean() > 0 for s in df.str_season.unique())
    bs = [d[rng.integers(0, len(d), len(d))].mean() for _ in range(500)]
    rows.append(dict(w_modele=w, ll=ll.mean(), rps=rps(P, y).mean(), gain_ll_vs_open=d.mean(),
                     ic95_lo=np.percentile(bs, 2.5), ic95_hi=np.percentile(bs, 97.5), saisons_gagnantes=f"{sg}/7"))
print(f"n={len(df):,} matchs Big5 avec cotes d'ouverture")
print(pd.DataFrame(rows).round(4).to_string(index=False))
