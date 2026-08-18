"""
train_1n2.py — Entraînement du modèle 1N2 (mart_1n2 → P(H / D / A)).

Lit config/models.yml (mart, cible, exclusions), entraîne un LightGBM
multiclasse, évalue sur val/test, sauvegarde le modèle et logge dans MLflow
(best-effort : un remote indisponible ne bloque pas l'entraînement).

Lancement :  python pipelines/train_1n2.py
"""
import argparse

import joblib
import mlflow

import ml_common as mc

MODEL_KEY = "resultat_1n2"                 # clé dans config/models.yml
LABEL_MAP = {"H": 0, "D": 1, "A": 2}       # H=domicile, D=nul, A=extérieur


def main(n_estimators=600, learning_rate=0.03):
    cfg, models = mc.load_configs()
    spec = models[MODEL_KEY]

    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].copy()          # écarte les matchs sans résultat

    y = df[spec["target"]].map(LABEL_MAP).astype(int)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude"))
    tr, va, te = mc.temporal_split(df, cfg)

    params = dict(objective="multiclass", num_class=3,
                  n_estimators=n_estimators, learning_rate=learning_rate,
                  num_leaves=31, subsample=0.8, colsample_bytree=0.8,
                  random_state=42, verbose=-1)

    print(f"[1N2] train={int(tr.sum()):,}  val={int(va.sum()):,}  "
          f"test={int(te.sum()):,}  | {X.shape[1]} features")
    model = mc.train_lgbm(X, y, tr, va, params)
    model = mc.calibrate(model, X, y, va)          # calibration isotone sur la validation

    print("Évaluation :")
    val_m = mc.evaluate(model, X, y, va, "VAL")
    test_m = mc.evaluate(model, X, y, te, "TEST")

    out = mc.MODELS_DIR / "resultat_1n2.joblib"
    joblib.dump({"model": model, "features": list(X.columns),
                 "label_map": LABEL_MAP}, out)
    print(f"Modèle sauvegardé : {out}")

    try:
        mlflow.set_tracking_uri(cfg["mlflow"]["tracking_uri"])
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
    parser = argparse.ArgumentParser(description="Trainer 1N2 (mart_1n2).")
    parser.add_argument("--n-estimators", type=int, default=600)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    args = parser.parse_args()
    main(n_estimators=args.n_estimators, learning_rate=args.learning_rate)
