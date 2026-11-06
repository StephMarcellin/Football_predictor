"""
goals_chunk.py — évaluation glissante du modèle de buts (Poisson LightGBM, λ = buts attendus d'une équipe dans un match).
Un pli (saison test S) x une graine = une tâche, pour l'appareil Linux (180 s/appel). Mêmes plis que rolling_eval.py :
entraînement < S-1, validation S-1, test S. Écrit les λ de la validation ET du test (la validation sert à ajuster rho).

  python tools/goals_chunk.py --test-season 2024-2025 --seed-index 0 --features shared --tag G1
--features shared : seulement les colonnes aussi présentes dans mart_1n2_avant_compo_toutes_saisons (même information que le 1N2 direct).
--features all    : toutes les colonnes de mart_goals (compo, xG, PPDA inclus).
Sorties : reports/chunks/<tag>/<saison>_s<k>.csv (str_match_id, str_season, int_is_home, goals, lam, split)
"""
import argparse
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import ml_common as mc  # noqa: E402

PARAMS = dict(objective="poisson", n_estimators=800, learning_rate=0.03, num_leaves=31, subsample=0.8,
              subsample_freq=1, colsample_bytree=0.8, random_state=42, verbose=-1)


def prepare(features, extra=None):
    cfg, models = mc.load_configs()
    spec = models["buts_equipe"]
    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].reset_index(drop=True)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))
    if features == "shared":
        ref = models["resultat_1n2_toutes_saisons"]
        ref_cols = set(mc.prepare_x(mc.load_mart(cfg, ref["mart"]), ref["target"], ref.get("exclude")).columns)
        X = X[[c for c in X.columns if c in ref_cols]]
    for path in (extra or []):
        # Features externes calculées avant chaque match (tools/elo_features.py), jointes sur (match, équipe).
        E = pd.read_csv(path, dtype={"str_match_id": str, "str_team_id": str})
        cols = [c for c in E.columns if c not in ("str_match_id", "str_team_id")]
        keys = df[["str_match_id", "str_team_id"]].astype(str)
        Mx = keys.merge(E, how="left", on=["str_match_id", "str_team_id"])
        assert len(Mx) == len(df), "jointure non univoque"
        for c in cols:
            X[c] = Mx[c].values
    return df, X, df[spec["target"]].astype(float), sorted(df["str_season"].unique())


def run(season, k, features, tag, extra=None):
    df, X, y, seasons = prepare(features, extra)
    i = seasons.index(season)
    tr = df["str_season"].isin(seasons[:i - 1]).values
    va = (df["str_season"] == seasons[i - 1]).values
    te = (df["str_season"] == season).values
    m = lgb.LGBMRegressor(**{**PARAMS, "random_state": PARAMS["random_state"] + k})
    m.fit(X[tr], y[tr], eval_set=[(X[va], y[va])], eval_metric="poisson", callbacks=[lgb.early_stopping(50, verbose=False)])
    out = []
    for name, msk in (("val", va), ("test", te)):
        o = df.loc[msk, ["str_match_id", "str_season", "int_is_home"]].copy()
        o["goals"] = y[msk].values
        o["lam"] = m.predict(X[msk])
        o["split"] = name
        out.append(o)
    d = ROOT / "reports" / "chunks" / tag
    d.mkdir(parents=True, exist_ok=True)
    pd.concat(out).to_csv(d / f"{season}_s{k}.csv", index=False)
    print(f"OK {season} graine {k} : {X.shape[1]} features, {m.best_iteration_} arbres, train={int(tr.sum()):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test-season", required=True)
    ap.add_argument("--seed-index", type=int, default=0)
    ap.add_argument("--features", choices=["shared", "all"], default="shared")
    ap.add_argument("--extra", nargs="+", default=None, help="CSV de features externes (Elo/classement)")
    ap.add_argument("--tag", required=True)
    a = ap.parse_args()
    run(a.test_season, a.seed_index, a.features, a.tag, a.extra)
