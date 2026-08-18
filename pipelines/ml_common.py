"""
ml_common.py — Plomberie partagée des trainers (un mart → un modèle).

Chargée par train_1n2.py, train_goals.py, etc. Regroupe : chargement conf +
mart, split temporel par saison, préparation X/y (écarte identifiants + cible +
exclusions config, encode booléens/objets), entraînement LightGBM avec early
stopping, évaluation, et helpers MLflow/sauvegarde.
"""
from pathlib import Path

import yaml
import duckdb
import numpy as np
import lightgbm as lgb
from sklearn.metrics import log_loss, accuracy_score, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV

ROOT_DIR = Path(__file__).resolve().parent.parent
MODELS_DIR = ROOT_DIR / "models"
MODELS_DIR.mkdir(exist_ok=True)

# Identifiants / clés : jamais des features.
ID_COLS = ["match_id", "team_id", "opponent_id", "player_id",
           "date", "season", "league_source"]

# Cotes / marché : jamais des features (value betting = proba_modèle vs marché).
# Présentes dans les marts pour le backtest, mais bannies de l'entraînement.
MARKET_PREFIXES = ("odds", "pinnacle", "market")


def load_configs():
    """Charge config.yaml (global) et config/models.yml (roster des modèles)."""
    with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    with open(ROOT_DIR / "config" / "models.yml", encoding="utf-8") as f:
        models = yaml.safe_load(f)["models"]
    return cfg, models


def load_mart(cfg, mart):
    """Lit le mart en read-only (aucune écriture sur la DB)."""
    con = duckdb.connect(str(ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    df = con.sql(f"select * from marts.{mart}").df()
    con.close()
    return df


def prepare_x(df, target, exclude):
    """Retourne X : df moins identifiants, cible et exclusions (config).
    Booléens → int8, colonnes objet → codes catégoriels (LightGBM veut du
    numérique)."""
    drop = set(ID_COLS + [target] + list(exclude or []))
    keep = [c for c in df.columns
            if c not in drop and not c.startswith(MARKET_PREFIXES)]
    X = df[keep].copy()
    for c in X.columns:
        if X[c].dtype == "bool":
            X[c] = X[c].astype("int8")
        elif X[c].dtype == "object":
            X[c] = X[c].astype("category").cat.codes
    return X


def temporal_split(df, cfg):
    """Masques train / val / test par saison (config.yaml → train:)."""
    tr = df["season"].isin(cfg["train"]["TRAIN_SEASONS"])
    va = df["season"].isin(cfg["train"]["VAL_SEASONS"])
    te = df["season"] == cfg["train"]["TEST_SEASON"]
    return tr, va, te


def train_lgbm(X, y, tr, va, params):
    """Fit LightGBM avec early stopping sur la validation."""
    model = lgb.LGBMClassifier(**params)
    model.fit(X[tr], y[tr], eval_set=[(X[va], y[va])],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    return model


def calibrate(model, X, y, mask, method="isotonic"):
    """Calibre un modèle déjà entraîné sur le jeu de validation (prefit).
    Indispensable au value betting : l'edge n'a de sens que si les probas sont
    justes. Gère l'API sklearn ancienne (cv='prefit') et récente (FrozenEstimator)."""
    try:
        from sklearn.frozen import FrozenEstimator
        return CalibratedClassifierCV(FrozenEstimator(model), method=method).fit(X[mask], y[mask])
    except ImportError:
        return CalibratedClassifierCV(model, method=method, cv="prefit").fit(X[mask], y[mask])


def evaluate(model, X, y, mask, name):
    """Logloss + accuracy (+ AUC si binaire). Affiche et retourne un dict."""
    labels = np.sort(np.unique(y))
    proba = model.predict_proba(X[mask])
    metrics = {
        "logloss": log_loss(y[mask], proba, labels=labels),
        "accuracy": accuracy_score(y[mask], proba.argmax(1)),
    }
    if len(labels) == 2:
        metrics["auc"] = roc_auc_score(y[mask], proba[:, 1])
    line = " | ".join(f"{k} {v:.4f}" for k, v in metrics.items())
    print(f"  {name:5} {line} | n={int(mask.sum()):,}")
    return metrics
