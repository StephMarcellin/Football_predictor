"""
train_1n2.py — Entraînement du modèle 1N2 (mart_1n2 → P(H / D / A)).

Lit config/models.yml (mart, cible, exclusions), entraîne un LightGBM
multiclasse, évalue sur val/test, sauvegarde le modèle et logge dans MLflow
(best-effort : un remote indisponible ne bloque pas l'entraînement).

Lancement :  python pipelines/models/train_1n2.py [--model resultat_1n2_avant_compo]

Le modèle entraîné est sauvegardé sous models/<clé>.joblib (clé de config/models.yml) :
plusieurs modèles 1N2 (complet, avant compos) coexistent sans s'écraser.
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
import mlflow

import ml_common as mc

MODEL_KEY = "resultat_1n2"                 # clé PAR DÉFAUT dans config/models.yml
LABEL_MAP = {"H": 0, "D": 1, "A": 2}       # H=domicile, D=nul, A=extérieur


def main(n_estimators=600, learning_rate=0.03, model_key=MODEL_KEY, n_seeds=None):
    cfg, models = mc.load_configs()
    if model_key not in models:
        raise SystemExit(f"Modèle inconnu '{model_key}' (clés de config/models.yml : {', '.join(models)})")
    spec = models[model_key]
    n_seeds = n_seeds or spec.get("n_seeds", 1)         # CLI > models.yml > 1

    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].copy()          # écarte les matchs sans résultat

    y = df[spec["target"]].map(LABEL_MAP).astype(int)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))
    tr, va, te = mc.temporal_split(df, cfg)

    params = dict(objective="multiclass", num_class=3,
                  n_estimators=n_estimators, learning_rate=learning_rate,
                  num_leaves=31, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                  random_state=42, verbose=-1)

    print(f"[{model_key}] train={int(tr.sum()):,}  val={int(va.sum()):,}  "
          f"test={int(te.sum()):,}  | {X.shape[1]} features")
    members = []
    for i in range(n_seeds):                       # une graine par membre : random_state = 42, 43, ...
        m = mc.train_lgbm(X, y, tr, va, {**params, "random_state": params["random_state"] + i})
        members.append(mc.calibrate(m, X, y, va))  # calibration sigmoïde sur la validation (cf. ml_common.calibrate)
        if n_seeds > 1:
            print(f"  graine {i + 1}/{n_seeds} entraînée")
    model = members[0] if n_seeds == 1 else mc.SeedEnsemble(members)

    print("Évaluation :")
    val_m = mc.evaluate(model, X, y, va, "VAL")
    test_m = mc.evaluate(model, X, y, te, "TEST")
    if "str_comp_category" in df.columns:          # le Big5 est la cible du value betting : on le suit à part
        big5_m = mc.evaluate(model, X, y, te & (df["str_comp_category"] == "Big5"), "T-Big5")
    else:
        big5_m = {}

    out = mc.MODELS_DIR / f"{model_key}.joblib"
    joblib.dump({"model": model, "features": list(X.columns),
                 "label_map": LABEL_MAP}, out)
    print(f"Modèle sauvegardé : {out}")

    try:
        # mlflow.set_tracking_uri(cfg["mlflow"]["tracking_uri"])
        mc.setup_mlflow(cfg)
        mlflow.set_experiment("marts_" + model_key)
        with mlflow.start_run():
            mlflow.log_params({**params, "n_features": X.shape[1], "n_seeds": n_seeds})
            mlflow.log_metrics({f"val_{k}": v for k, v in val_m.items()})
            mlflow.log_metrics({f"test_{k}": v for k, v in test_m.items()})
            mlflow.log_metrics({f"test_big5_{k}": v for k, v in big5_m.items()})
        print("MLflow : run loggé.")
    except Exception as e:
        print(f"MLflow non loggé (best-effort) : {e}")

    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trainer 1N2 (mart_1n2 ou variante).")
    parser.add_argument("--model", default=MODEL_KEY,
                        help="clé du modèle dans config/models.yml (défaut : resultat_1n2)")
    parser.add_argument("--n-estimators", type=int, default=600)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--n-seeds", type=int, default=None,
                        help="nombre de graines moyennées (défaut : n_seeds du modèle dans models.yml, sinon 1)")
    args = parser.parse_args()
    main(n_estimators=args.n_estimators, learning_rate=args.learning_rate, model_key=args.model, n_seeds=args.n_seeds)
