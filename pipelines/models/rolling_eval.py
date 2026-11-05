"""
rolling_eval.py — Évaluation 1N2 en fenêtre croissante (rolling-origin).

Pour chaque saison de test S : entraînement sur les saisons < S-1, validation (arrêt anticipé +
calibration) sur S-1, test sur S. Mêmes paramètres que train_1n2.py. Sert à MESURER (plus de matchs,
stabilité dans le temps) : le modèle de production reste entraîné par train_1n2.py.

Sorties : reports/rolling_eval_<modèle>.csv (une ligne par match testé : id, saison, compétition,
résultat, P(H), P(D), P(A) après plancher) + résumé par saison dans la console.
Seuls les matchs ayant une ligne domicile (int_is_home = 1) sont évalués (terrain neutre exclu).

Lancement :
  python pipelines/models/rolling_eval.py
  python pipelines/models/rolling_eval.py --model resultat_1n2_toutes_saisons --first-test 2019-2020 --last-test 2025-2026
"""
# --- bootstrap : rend les modules partagés (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import argparse
import ast

import numpy as np
import pandas as pd

import ml_common as mc

MODEL_KEY = "resultat_1n2_toutes_saisons"
LABEL_MAP = {"H": 0, "D": 1, "A": 2}
# Mêmes paramètres que train_1n2.py (à garder alignés si on les change là-bas).
PARAMS = dict(objective="multiclass", num_class=3, n_estimators=600, learning_rate=0.03,
              num_leaves=31, subsample=0.8, subsample_freq=1, colsample_bytree=0.8, random_state=42, verbose=-1)


def run_fold(df, X, y, test_season, seasons, n_seeds, params):
    """Entraîne sur seasons[:i-1], valide sur seasons[i-1], prédit test_season.
    Retourne (DataFrame des prédictions, nombre moyen d'arbres retenus par l'arrêt anticipé)."""
    i = seasons.index(test_season)
    tr = df["str_season"].isin(seasons[:i - 1])
    va = df["str_season"] == seasons[i - 1]
    sel = (df["str_season"] == test_season) & (df["int_is_home"] == 1)

    members, iters = [], []
    for k in range(n_seeds):
        m = mc.train_lgbm(X, y, tr, va, {**params, "random_state": params["random_state"] + k})
        iters.append(m.best_iteration_)
        members.append(mc.calibrate(m, X, y, va))
    model = members[0] if n_seeds == 1 else mc.SeedEnsemble(members)

    P = mc.floor_proba(model.predict_proba(X[sel]))
    base = np.bincount(y[tr & (df["int_is_home"] == 1)], minlength=3)
    base = base / base.sum()                                   # fréquences H/D/A du jeu d'entraînement
    out = df.loc[sel, ["str_match_id", "str_season", "str_comp_category"]].copy()
    out["y"] = y[sel].values
    out["p_H"], out["p_D"], out["p_A"] = P[:, 0], P[:, 1], P[:, 2]
    out["ll_model"] = -np.log(P[np.arange(len(P)), out["y"].values])
    out["ll_base"] = -np.log(base[out["y"].values])
    return out, float(np.mean(iters))


def summarize(res):
    """Log-loss / Brier / précision par saison et pooled, pour Big5 et pour tout, avec IC95 bootstrap du gain."""
    rng = np.random.default_rng(0)
    P = res[["p_H", "p_D", "p_A"]].values
    res = res.assign(brier=((P - np.eye(3)[res["y"].values]) ** 2).mean(axis=1),
                     ok=(P.argmax(1) == res["y"].values))
    for scope, sub in [("Big5", res[res["str_comp_category"] == "Big5"]), ("Tout", res)]:
        print(f"\n=== {scope} ===")
        rows = []
        for s, g in sub.groupby("str_season"):
            rows.append(dict(saison=s, n=len(g), logloss=g.ll_model.mean(), base=g.ll_base.mean(),
                             gain=g.ll_base.mean() - g.ll_model.mean(), brier=g.brier.mean(), precision=g.ok.mean()))
        print(pd.DataFrame(rows).round(4).to_string(index=False))
        d = (sub.ll_base - sub.ll_model).values
        bs = [d[rng.integers(0, len(d), len(d))].mean() for _ in range(1000)]
        print(f"POOLED n={len(d):,} : logloss {sub.ll_model.mean():.4f} | base {sub.ll_base.mean():.4f} | "
              f"gain {d.mean():+.4f} IC95 [{np.percentile(bs, 2.5):+.4f}, {np.percentile(bs, 97.5):+.4f}]")


def parse_overrides(items):
    """['num_leaves=8', 'reg_lambda=10'] → {'num_leaves': 8, 'reg_lambda': 10} (valeurs lues comme littéraux Python)."""
    out = {}
    for it in items or []:
        k, v = it.split("=", 1)
        out[k.strip()] = ast.literal_eval(v.strip())
    return out


def main(model_key=MODEL_KEY, first_test="2019-2020", last_test="2025-2026", n_seeds=None,
         overrides=None, tag=""):
    cfg, models = mc.load_configs()
    if model_key not in models:
        raise SystemExit(f"Modèle inconnu '{model_key}' (clés de config/models.yml : {', '.join(models)})")
    spec = models[model_key]
    n_seeds = n_seeds or spec.get("n_seeds", 1)
    params = {**PARAMS, **(overrides or {})}

    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].reset_index(drop=True)
    y = df[spec["target"]].map(LABEL_MAP).astype(int)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))
    seasons = sorted(df["str_season"].unique())
    tests = [s for s in seasons if first_test <= s <= last_test]
    print(f"[{model_key}] {len(df):,} lignes, {X.shape[1]} features, {n_seeds} graine(s) | saisons testées : {tests}")
    print(f"  paramètres : {params}")

    parts = []
    for s in tests:
        print(f"  fold test={s} (validation={seasons[seasons.index(s) - 1]}) ...", flush=True)
        part, mean_it = run_fold(df, X, y, s, seasons, n_seeds, params)
        print(f"    arbres retenus (moyenne des graines) : {mean_it:.0f} / {params['n_estimators']}")
        parts.append(part)
    res = pd.concat(parts, ignore_index=True)
    summarize(res)

    out = mc.ROOT_DIR / "reports" / f"rolling_eval_{model_key}{('_' + tag) if tag else ''}.csv"
    out.parent.mkdir(exist_ok=True)
    res.to_csv(out, index=False)
    print(f"\nSauvegardé : {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Évaluation 1N2 en fenêtre croissante.")
    parser.add_argument("--model", default=MODEL_KEY, help="clé du modèle dans config/models.yml")
    parser.add_argument("--first-test", default="2019-2020")
    parser.add_argument("--last-test", default="2025-2026")
    parser.add_argument("--n-seeds", type=int, default=None, help="défaut : n_seeds du modèle dans models.yml")
    parser.add_argument("--param", action="append", default=[], metavar="CLE=VALEUR",
                        help="surcharge un paramètre LightGBM (répétable), ex. --param num_leaves=8")
    parser.add_argument("--tag", default="", help="suffixe du CSV de sortie (ex. B_leaves8)")
    args = parser.parse_args()
    main(args.model, args.first_test, args.last_test, args.n_seeds, parse_overrides(args.param), args.tag)
