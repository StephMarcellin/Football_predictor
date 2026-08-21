"""
train_goals.py — Modèle de buts par équipe (régression Poisson LightGBM).

Prédit λ = E[buts marqués] pour une équipe dans un match (mart_goals → team_goals).
LightGBM objective='poisson' : la loi de comptage correcte pour des buts. La sortie
λ par équipe alimente ensuite la dérivation des marchés (poisson_markets.py) :
1X2, over/under, BTTS, score exact.

Lancement :  python pipelines/train_goals.py
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
import lightgbm as lgb
import mlflow
import numpy as np

import ml_common as mc

MODEL_KEY = "buts_equipe"              # clé dans config/models.yml


def poisson_deviance(y, mu):
    """Déviance de Poisson moyenne — la métrique naturelle d'un modèle de comptage.
    2·Σ[ y·log(y/mu) − (y − mu) ], avec la convention y·log(y)=0 pour y=0.
    On ne calcule le log QUE là où y>0 (sinon log(0)=−inf → warnings numpy)."""
    y = np.asarray(y, dtype=float)
    mu = np.clip(np.asarray(mu, dtype=float), 1e-8, None)
    term = np.zeros_like(y)
    pos = y > 0
    term[pos] = y[pos] * np.log(y[pos] / mu[pos])
    return float(2.0 * np.sum(term - (y - mu)) / len(y))


def evaluate(model, X, y, mask, name):
    """Déviance de Poisson + RMSE + biais moyen (sur/sous-estimation des buts)."""
    mu = model.predict(X[mask])
    yv = y[mask].values
    m = {
        "poisson_deviance": poisson_deviance(yv, mu),
        "rmse":             float(np.sqrt(np.mean((yv - mu) ** 2))),
        "bias":             float(mu.mean() - yv.mean()),
    }
    print(f"  {name:5} poisson_dev {m['poisson_deviance']:.4f} | rmse {m['rmse']:.4f} "
          f"| biais {m['bias']:+.4f} | n={int(mask.sum()):,}")
    return m


def main(n_estimators=800, learning_rate=0.03):
    cfg, models = mc.load_configs()
    spec = models[MODEL_KEY]

    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].copy()          # écarte les matchs sans résultat
    y = df[spec["target"]].astype(float)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))   # cotes exclues (MARKET_PREFIXES)
    tr, va, te = mc.temporal_split(df, cfg)

    params = dict(objective="poisson", n_estimators=n_estimators, learning_rate=learning_rate,
                  num_leaves=31, subsample=0.8, colsample_bytree=0.8,
                  random_state=42, verbose=-1)

    print(f"[GOALS] train={int(tr.sum()):,}  val={int(va.sum()):,}  "
          f"test={int(te.sum()):,}  | {X.shape[1]} features")
    model = lgb.LGBMRegressor(**params)
    model.fit(X[tr], y[tr], eval_set=[(X[va], y[va])],
              eval_metric="poisson", callbacks=[lgb.early_stopping(50, verbose=False)])

    print("Évaluation :")
    val_m = evaluate(model, X, y, va, "VAL")
    test_m = evaluate(model, X, y, te, "TEST")

    out = mc.MODELS_DIR / "buts_equipe.joblib"
    joblib.dump({"model": model, "features": list(X.columns)}, out)
    print(f"Modèle sauvegardé : {out}")

    try:
        mc.setup_mlflow(cfg)
        mlflow.set_experiment("marts_" + MODEL_KEY)
        with mlflow.start_run():
            mlflow.log_params({**params, "n_features": X.shape[1]})
            mlflow.log_metrics({f"val_{k}": v for k, v in val_m.items()})
            mlflow.log_metrics({f"test_{k}": v for k, v in test_m.items()})
        print("MLflow : run loggé.")
    except Exception as e:
        print(f"MLflow non loggé (best-effort) : {e}")

    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trainer buts (Poisson LightGBM).")
    parser.add_argument("--n-estimators", type=int, default=800)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    args = parser.parse_args()
    main(n_estimators=args.n_estimators, learning_rate=args.learning_rate)
