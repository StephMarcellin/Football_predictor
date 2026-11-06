"""
goals_eval.py — du λ de buts aux probabilités 1N2 : Poisson indépendant vs Dixon-Coles (rho ajusté sur la validation de chaque pli),
comparaison au 1N2 direct, mélange, et test d'information face à Pinnacle ouverture. Big5, matchs avec cotes d'ouverture.

  python tools/goals_eval.py --tag G1 --n-seeds 3 --direct reports/rolling_eval_resultat_1n2_toutes_saisons_E_bagging.csv
Règle go/no-go (CLAUDE.md) : IC95 du gain exclut 0 ET gain sur >= 5/7 saisons. Poids de mélange choisi sur 2019-20..2022-23, évalué sur 2023-24..2025-26.
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "tools"))
import ml_common as mc  # noqa: E402
from market_benchmark import logloss, rps  # noqa: E402

G = 11  # buts 0..10


def tau(h, a, lh, la, rho):
    t = np.ones_like(lh)
    t = np.where((h == 0) & (a == 0), 1 - lh * la * rho, t)
    t = np.where((h == 0) & (a == 1), 1 + lh * rho, t)
    t = np.where((h == 1) & (a == 0), 1 + la * rho, t)
    t = np.where((h == 1) & (a == 1), 1 - rho, t)
    return t


def score_matrix(lh, la, rho):
    """(n, G, G) probabilités de score, normalisées ; rho=0 = Poisson indépendant."""
    k = np.arange(G)
    M = poisson.pmf(k[None, :, None], lh[:, None, None]) * poisson.pmf(k[None, None, :], la[:, None, None])
    if rho != 0:
        for hh, aa in ((0, 0), (0, 1), (1, 0), (1, 1)):
            M[:, hh, aa] *= tau(np.array(hh), np.array(aa), lh, la, rho)
    return M / M.sum(axis=(1, 2), keepdims=True)


def p1n2(M):
    i, j = np.indices((G, G))
    return np.stack([M[:, i > j].sum(1), M[:, i == j].sum(1), M[:, i < j].sum(1)], axis=1)


def p_over25(M):
    i, j = np.indices((G, G))
    return M[:, (i + j) >= 3].sum(1)


def fit_rho(lh, la, gh, ga):
    best, bl = 0.0, -1e18
    for rho in np.round(np.arange(-0.20, 0.201, 0.02), 2):
        t = tau(gh, ga, lh, la, rho)
        if (t <= 0).any():
            continue
        ll = np.log(t).sum()          # le terme de Poisson ne dépend pas de rho ; la normalisation est ~1
        if ll > bl:
            best, bl = rho, ll
    return float(best)


def pair(df):
    h = df[df.int_is_home == 1].set_index("str_match_id")[["lam", "goals"]].add_suffix("_h")
    a = df[df.int_is_home == 0].set_index("str_match_id")[["lam", "goals"]].add_suffix("_a")
    return h.join(a, how="inner").reset_index()


def boot(d, rng, n=1000):
    b = [d[rng.integers(0, len(d), len(d))].mean() for _ in range(n)]
    return np.percentile(b, 2.5), np.percentile(b, 97.5)


def main(tag, n_seeds, direct_csv):
    cfg, _ = mc.load_configs()
    d = ROOT / "reports" / "chunks" / tag
    rows = []
    for s in sorted({p.name.split("_s")[0] for p in d.glob("*_s0.csv")}):
        fs = [pd.read_csv(d / f"{s}_s{k}.csv") for k in range(n_seeds)]
        base = fs[0].copy()
        base["lam"] = np.mean([f["lam"].values for f in fs], axis=0)
        va, te = pair(base[base.split == "val"]), pair(base[base.split == "test"])
        rho = fit_rho(va.lam_h.values, va.lam_a.values, va.goals_h.values, va.goals_a.values)
        te["str_season"] = s
        te["rho"] = rho
        for name, r in (("poi", 0.0), ("dc", rho)):
            M = score_matrix(te.lam_h.values, te.lam_a.values, r)
            P = p1n2(M)
            te[[f"{name}_H", f"{name}_D", f"{name}_A"]] = P
            if name == "dc":
                te["dc_over25"] = p_over25(M)
        rows.append(te)
        print(f"  {s} : {len(te)} matchs, rho={rho:+.2f}, λ moyen {te.lam_h.mean():.3f}/{te.lam_a.mean():.3f} vs buts {te.goals_h.mean():.3f}/{te.goals_a.mean():.3f}")
    T = pd.concat(rows, ignore_index=True)
    dr = pd.read_csv(direct_csv)[["str_match_id", "str_season", "str_comp_category", "y", "p_H", "p_D", "p_A"]]
    odds = mc.load_odds(cfg)
    oc = [f"dec_prob_open_{x}" for x in ("home", "draw", "away")] + ["dec_prob_open_over25"]
    df = dr[dr.str_comp_category == "Big5"].merge(T.drop(columns="str_season"), on="str_match_id") \
        .merge(odds[["str_match_id"] + oc], on="str_match_id").dropna(subset=oc).reset_index(drop=True)
    y = df.y.values.astype(int)
    Pd, Pp, Pc, Po = (df[["p_H", "p_D", "p_A"]].values, df[["poi_H", "poi_D", "poi_A"]].values,
                      df[["dc_H", "dc_D", "dc_A"]].values, df[oc[:3]].values)
    seas = df.str_season.values
    print(f"\n=== {len(df):,} matchs Big5 avec cotes ===")
    tab = []
    for n, P in (("direct E", Pd), ("Poisson indép.", Pp), ("Dixon-Coles", Pc), ("Pinnacle ouv.", Po)):
        tab.append(dict(source=n, acc=(P.argmax(1) == y).mean(), rps=rps(P, y).mean(), ll=logloss(P, y).mean()))
    print(pd.DataFrame(tab).round(4).to_string(index=False))

    rng = np.random.default_rng(0)
    early = np.isin(seas, ["2019-2020", "2020-2021", "2021-2022", "2022-2023"])
    late = ~early
    ws = np.round(np.arange(0, 1.01, 0.1), 1)
    for title, A, B, ref, refname in (("Mélange direct E + Dixon-Coles, référence = direct E", Pd, Pc, Pd, "direct"),
                                      ("Mélange Dixon-Coles + Pinnacle ouv., référence = Pinnacle", Pc, Po, Po, "Pinnacle")):
        # P = w*A + (1-w)*B pour le 1er ; pour le 2e, w = poids du modèle de buts
        if refname == "direct":
            f = lambda w: w * B + (1 - w) * A      # w = poids Dixon-Coles
        else:
            f = lambda w: w * A + (1 - w) * B      # w = poids Dixon-Coles
        llref = logloss(ref, y)
        res = []
        for w in ws:
            ll = logloss(f(w), y)
            g = llref - ll
            res.append((w, g[early].mean(), g[late].mean(), g.mean()))
        wbest = min(res, key=lambda r: -r[1])[0]       # meilleur gain sur 2019-23
        g = llref - logloss(f(wbest), y)
        lo, hi = boot(g[late], rng)
        won = sum(g[seas == s].mean() > 0 for s in np.unique(seas[late]))
        print(f"\n--- {title}")
        print(pd.DataFrame(res, columns=["w_buts", "gain_ll_2019-23", "gain_ll_2023-26", "gain_ll_7ans"]).round(4).to_string(index=False))
        print(f"w retenu (sur 2019-23) = {wbest} -> gain hors échantillon 2023-26 : {g[late].mean():+.4f} IC95 [{lo:+.4f}, {hi:+.4f}], saisons gagnantes {won}/3")
        gg = llref - logloss(f(wbest), y)
        print(f"   sur 7 saisons (w fixé) : gain {gg.mean():+.4f}, saisons gagnantes {sum(gg[seas == s].mean() > 0 for s in np.unique(seas))}/7")

    # Plus/moins 2,5 buts
    yo = ((df.goals_h + df.goals_a) >= 3).astype(int).values
    po, pm = np.clip(df.dc_over25.values, 1e-6, 1 - 1e-6), df["dec_prob_open_over25"].values
    bl = lambda p: -(yo * np.log(p) + (1 - yo) * np.log(1 - p))
    print(f"\n--- Over 2,5 : LL Dixon-Coles {bl(po).mean():.4f} | Pinnacle ouv. {bl(pm).mean():.4f} | "
          f"mélange 50/50 {bl(0.5 * po + 0.5 * pm).mean():.4f} | base {bl(np.full(len(yo), yo.mean())).mean():.4f}")
    df.to_csv(ROOT / "reports" / f"goals_eval_{tag}.csv", index=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--n-seeds", type=int, default=3)
    ap.add_argument("--direct", required=True)
    a = ap.parse_args()
    main(a.tag, a.n_seeds, a.direct)
