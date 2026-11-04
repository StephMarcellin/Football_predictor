"""
rolling_chunk.py — rolling_eval découpé en tâches (un pli x une graine) pour l'appareil Linux
(180 s par appel, aucun processus persistant). Mêmes données, mêmes paramètres et même logique que
pipelines/models/rolling_eval.py ; l'agrégation moyenne les graines AVANT le plancher, comme SeedEnsemble.

Une tâche :   python tools/rolling_chunk.py --test-season 2024-2025 --seed-index 0 --tag A_vm
Agrégation :  python tools/rolling_chunk.py --aggregate --first-test 2024-2025 --last-test 2024-2025 --n-seeds 3 --tag A_vm
Sorties : reports/chunks/<tag>/<saison>_s<k>.csv puis reports/rolling_eval_<modèle>_<tag>.csv
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "pipelines" / "models"))

import numpy as np
import pandas as pd

import ml_common as mc
import rolling_eval as re_


def prepare(model_key):
    cfg, models = mc.load_configs()
    spec = models[model_key]
    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].reset_index(drop=True)
    y = df[spec["target"]].map(re_.LABEL_MAP).astype(int)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))
    return spec, df, X, y, sorted(df["str_season"].unique())


def run_task(model_key, test_season, k, overrides, tag):
    spec, df, X, y, seasons = prepare(model_key)
    params = {**re_.PARAMS, **(overrides or {})}
    i = seasons.index(test_season)
    tr = df["str_season"].isin(seasons[:i - 1])
    va = df["str_season"] == seasons[i - 1]
    sel = (df["str_season"] == test_season) & (df["int_is_home"] == 1)
    m = mc.train_lgbm(X, y, tr, va, {**params, "random_state": params["random_state"] + k})
    cal = mc.calibrate(m, X, y, va)
    P = cal.predict_proba(X[sel])                       # brut : plancher appliqué à l'agrégation
    base = np.bincount(y[tr & (df["int_is_home"] == 1)], minlength=3)
    base = base / base.sum()
    out = df.loc[sel, ["str_match_id", "str_season", "str_comp_category"]].copy()
    out["y"] = y[sel].values
    out["p_H"], out["p_D"], out["p_A"] = P[:, 0], P[:, 1], P[:, 2]
    out["ll_base"] = -np.log(base[out["y"].values])
    out["best_iter"] = m.best_iteration_
    d = ROOT / "reports" / "chunks" / tag
    d.mkdir(parents=True, exist_ok=True)
    out.to_csv(d / f"{test_season}_s{k}.csv", index=False)
    print(f"OK {test_season} graine {k} : {m.best_iteration_} arbres, {len(out)} matchs -> {d.name}/{test_season}_s{k}.csv")


def aggregate(model_key, first, last, n_seeds, tag):
    spec, df, X, y, seasons = prepare(model_key)
    tests = [s for s in seasons if first <= s <= last]
    d = ROOT / "reports" / "chunks" / tag
    parts = []
    for s in tests:
        seeds = [pd.read_csv(d / f"{s}_s{k}.csv") for k in range(n_seeds)]
        ids = seeds[0]["str_match_id"]
        assert all((x["str_match_id"] == ids).all() for x in seeds), "ordre des matchs différent entre graines"
        P = np.mean([x[["p_H", "p_D", "p_A"]].values for x in seeds], axis=0)
        P = mc.floor_proba(P)
        out = seeds[0][["str_match_id", "str_season", "str_comp_category", "y", "ll_base"]].copy()
        out["p_H"], out["p_D"], out["p_A"] = P[:, 0], P[:, 1], P[:, 2]
        out["ll_model"] = -np.log(P[np.arange(len(P)), out["y"].values])
        parts.append(out)
    res = pd.concat(parts, ignore_index=True)
    re_.summarize(res)
    path = ROOT / "reports" / f"rolling_eval_{model_key}_{tag}.csv"
    res[["str_match_id", "str_season", "str_comp_category", "y", "p_H", "p_D", "p_A", "ll_model", "ll_base"]].to_csv(path, index=False)
    print(f"Sauvegardé : {path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="rolling_eval par tâches (pli x graine).")
    ap.add_argument("--model", default=re_.MODEL_KEY)
    ap.add_argument("--test-season")
    ap.add_argument("--seed-index", type=int, default=0)
    ap.add_argument("--param", action="append", default=[], metavar="CLE=VALEUR")
    ap.add_argument("--tag", required=True)
    ap.add_argument("--aggregate", action="store_true")
    ap.add_argument("--first-test", default="2019-2020")
    ap.add_argument("--last-test", default="2025-2026")
    ap.add_argument("--n-seeds", type=int, default=3)
    a = ap.parse_args()
    if a.aggregate:
        aggregate(a.model, a.first_test, a.last_test, a.n_seeds, a.tag)
    else:
        run_task(a.model, a.test_season, a.seed_index, re_.parse_overrides(a.param), a.tag)
