"""
train_players.py — Trainer binaire générique pour les modèles JOUEUR.

Un seul entraîneur pour buteurs (mart_scorers → scored) et passeurs
(mart_assists → assisted) : deux classifications binaires DÉSÉQUILIBRÉES de même
structure. Piloté par config/models.yml (clé --model).

LightGBM binaire + scale_pos_weight (classe positive rare), évalué en log-loss /
AUC / PR-AUC / Brier (jamais l'accuracy, trompeuse sur cible déséquilibrée), avec
analyse SHAP exportée (PNG + ranking) pour élaguer les features.

Lancement :
  python pipelines/models/train_players.py --model buteurs
  python pipelines/models/train_players.py --model passeurs
"""
import argparse

import joblib
import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")                    # backend sans écran (export PNG)
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import shap
from sklearn.metrics import (log_loss, roc_auc_score,
                             average_precision_score, brier_score_loss)

import ml_common as mc


def evaluate(model, X, y, mask, name):
    """Métriques adaptées à une cible déséquilibrée (pas d'accuracy)."""
    p = model.predict_proba(X[mask])[:, 1]
    yv = y[mask].values
    m = {
        "logloss":   log_loss(yv, p, labels=[0, 1]),
        "auc":       roc_auc_score(yv, p),
        "pr_auc":    average_precision_score(yv, p),   # référence = base rate
        "brier":     brier_score_loss(yv, p),
        "base_rate": float(yv.mean()),
    }
    print(f"  {name:5} logloss {m['logloss']:.4f} | auc {m['auc']:.4f} | "
          f"pr_auc {m['pr_auc']:.4f} | brier {m['brier']:.4f} | "
          f"base {m['base_rate']:.3f} | n={int(mask.sum()):,}")
    return m


def shap_report(model, X, mask, key, max_display=25):
    """SHAP summary (bar) → PNG + top features par |SHAP| moyen. Retourne le ranking."""
    Xs = X[mask].sample(min(5000, int(mask.sum())), random_state=42)
    sv = shap.TreeExplainer(model).shap_values(Xs)
    if isinstance(sv, list):             # ancienne API : [classe 0, classe 1]
        sv = sv[1]
    imp = np.abs(sv).mean(axis=0)
    ranked = sorted(zip(Xs.columns, imp.astype(float)), key=lambda t: -t[1])

    out_dir = mc.ROOT_DIR / "reports" / "shap"
    out_dir.mkdir(parents=True, exist_ok=True)
    shap.summary_plot(sv, Xs, plot_type="bar", max_display=max_display, show=False)
    png = out_dir / f"shap_{key}.png"
    plt.tight_layout(); plt.savefig(png, dpi=120); plt.close()

    print(f"\nSHAP → {png}\nTop features (|SHAP| moyen) :")
    for name, val in ranked[:15]:
        print(f"  {val:8.4f}  {name}")
    return ranked, png


def main(model_key="buteurs", n_estimators=600, learning_rate=0.03):
    cfg, models = mc.load_configs()
    spec = models[model_key]

    df = mc.load_mart(cfg, spec["mart"])
    y = df[spec["target"]].astype(int)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))   # ids + cotes exclus
    tr, va, te = mc.temporal_split(df, cfg)

    pos = int(y[tr].sum())
    spw = (int(tr.sum()) - pos) / max(pos, 1)     # neg/pos → rééquilibre la classe rare
    params = dict(objective="binary", n_estimators=n_estimators, learning_rate=learning_rate,
                  num_leaves=31, subsample=0.8, colsample_bytree=0.8,
                  scale_pos_weight=spw, random_state=42, verbose=-1)

    print(f"[{model_key}] train={int(tr.sum()):,} val={int(va.sum()):,} test={int(te.sum()):,} "
          f"| {X.shape[1]} features | base rate {y[tr].mean():.3f} | scale_pos_weight {spw:.2f}")
    model = lgb.LGBMClassifier(**params)
    model.fit(X[tr], y[tr], eval_set=[(X[va], y[va])], eval_metric="auc",
              callbacks=[lgb.early_stopping(50, verbose=False)])

    print("Évaluation :")
    val_m = evaluate(model, X, y, va, "VAL")
    test_m = evaluate(model, X, y, te, "TEST")

    ranked, png = shap_report(model, X, va, model_key)

    out = mc.MODELS_DIR / f"{model_key}.joblib"
    joblib.dump({"model": model, "features": list(X.columns), "shap_ranking": ranked}, out)
    print(f"\nModèle sauvegardé : {out}")

    try:
        mc.setup_mlflow(cfg)
        mlflow.set_experiment("marts_" + model_key)
        with mlflow.start_run():
            mlflow.log_params({**params, "n_features": X.shape[1]})
            mlflow.log_metrics({f"val_{k}": v for k, v in val_m.items()})
            mlflow.log_metrics({f"test_{k}": v for k, v in test_m.items()})
            mlflow.log_artifact(str(png))
        print("MLflow : run loggé.")
    except Exception as e:
        print(f"MLflow non loggé (best-effort) : {e}")

    return model


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Trainer binaire joueur (buteurs / passeurs).")
    p.add_argument("--model", choices=["buteurs", "passeurs"], required=True)
    p.add_argument("--n-estimators", type=int, default=600)
    p.add_argument("--learning-rate", type=float, default=0.03)
    a = p.parse_args()
    main(model_key=a.model, n_estimators=a.n_estimators, learning_rate=a.learning_rate)
