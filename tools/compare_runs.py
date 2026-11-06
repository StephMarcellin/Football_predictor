"""Compare deux CSV rolling_eval sur les mêmes matchs Big5 : gain de log-loss (base - nouveau), IC95 bootstrap, détail par saison.
Règle go/no-go : IC95 exclut 0 ET gain > 0 sur >= 5 saisons sur 7.   Usage : python tools/compare_runs.py <base.csv> <nouveau.csv>"""
import sys
import numpy as np
import pandas as pd

a, b = pd.read_csv(sys.argv[1]), pd.read_csv(sys.argv[2])
m = a.merge(b, on="str_match_id", suffixes=("_a", "_b"))
m = m[m["str_comp_category_a"] == "Big5"]
d = (m["ll_model_a"] - m["ll_model_b"]).values
rng = np.random.default_rng(0)
bs = [d[rng.integers(0, len(d), len(d))].mean() for _ in range(1000)]
per = {s: float(d[(m["str_season_a"] == s).values].mean()) for s in sorted(m["str_season_a"].unique())}
ok = sum(v > 0 for v in per.values())
print(f"n={len(d):,} | LL base {m.ll_model_a.mean():.4f} -> nouveau {m.ll_model_b.mean():.4f} | gain {d.mean():+.4f} IC95 [{np.percentile(bs, 2.5):+.4f}, {np.percentile(bs, 97.5):+.4f}] | saisons gagnantes {ok}/{len(per)}")
print("par saison:", {k: round(v, 4) for k, v in per.items()})
print("DECISION:", "GO" if (np.percentile(bs, 2.5) > 0 and ok >= 5) else "NO-GO")
