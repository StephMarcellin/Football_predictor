"""
ml_common.py — Plomberie partagée des trainers (un mart → un modèle).

Chargée par train_1n2.py, train_goals.py, etc. Regroupe : chargement conf +
mart, split temporel par saison, préparation X/y (écarte identifiants + cible +
exclusions config, encode booléens/objets), entraînement LightGBM avec early
stopping, évaluation, et helpers MLflow/sauvegarde.
"""
from pathlib import Path
import os
from dotenv import load_dotenv

import yaml
import duckdb
import numpy as np
import lightgbm as lgb
from sklearn.metrics import log_loss, accuracy_score, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
MODELS_DIR = ROOT_DIR / "models"
MODELS_DIR.mkdir(exist_ok=True)

# Identifiants / clés : jamais des features.
# Noms refondus (préfixe de type en tête), tels qu'exposés par les marts dbt.
ID_COLS = ["str_match_id", "str_team_id", "str_opponent_id", "str_player_id",
           "dt_date", "str_season", "str_league_source"]

# Cotes / marché : jamais des features (value betting = proba_modèle vs marché).
# Présentes dans les marts pour le backtest, mais bannies de l'entraînement.
# Depuis la refonte du nommage dbt, ces colonnes portent un préfixe de type
# (dec_odds_…, dec_pinnacle_…) : on le retire avant de tester le préfixe métier
# (sinon les cotes redeviendraient des features, silencieusement).
MARKET_PREFIXES = ("odds", "pinnacle", "market")
TYPE_PREFIXES = ("str_", "int_", "dec_", "dt_", "bool_")


def _is_market_col(col):
    base = col
    for p in TYPE_PREFIXES:
        if base.startswith(p):
            base = base[len(p):]
            break
    return base.startswith(MARKET_PREFIXES)


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


def load_odds(cfg):
    """Cotes Pinnacle (table gold.gold_odds_pinnacle), 1 ligne par match, read-only.
    Servent au backtest et à la comparaison au marché — jamais des features."""
    con = duckdb.connect(str(ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    df = con.sql("select * from gold.gold_odds_pinnacle").df()
    con.close()
    return df


def prepare_x(df, target, exclude):
    """Retourne X : df moins identifiants, cible et exclusions (config).
    Booléens → int8, colonnes objet → dtype pandas `category` (catégorielles
    natives LightGBM). Aucun code entier n'est fixé ici : le modèle mémorise
    les modalités vues à l'entraînement et réaligne celles du lot à prédire,
    donc le résultat ne dépend pas des modalités présentes dans `df`."""
    drop = set(ID_COLS + [target] + list(exclude or []))
    keep = [c for c in df.columns
            if c not in drop and not _is_market_col(c)]
    X = df[keep].copy()
    for c in X.columns:
        if X[c].dtype == "bool":
            X[c] = X[c].astype("int8")
        elif X[c].dtype == "object":
            X[c] = X[c].astype("category")
    return X


def align_features(X, feats):
    """Réaligne X sur les features, dans l'ordre, du modèle à appliquer.

    Lève une erreur si une feature du modèle est absente de X, au lieu de la remplacer
    par 0 en silence (un modèle qui prédit avec des features inventées donne des
    probabilités fausses sans aucun signal). Les colonnes de X inconnues du modèle sont
    ignorées (le mart peut avoir gagné des colonnes depuis l'entraînement)."""
    missing = [c for c in feats if c not in X.columns]
    if missing:
        raise ValueError(
            f"{len(missing)} feature(s) du modèle absente(s) du jeu à prédire : "
            f"{missing[:10]}{' …' if len(missing) > 10 else ''}. "
            "Réentraîne le modèle ou corrige le mart.")
    return X[list(feats)]


def temporal_split(df, cfg):
    """Masques train / val / test par saison (config.yaml → train:)."""
    tr = df["str_season"].isin(cfg["train"]["TRAIN_SEASONS"])
    va = df["str_season"].isin(cfg["train"]["VAL_SEASONS"])
    te = df["str_season"] == cfg["train"]["TEST_SEASON"]
    return tr, va, te


def train_lgbm(X, y, tr, va, params):
    """Fit LightGBM avec early stopping sur la validation."""
    model = lgb.LGBMClassifier(**params)
    model.fit(X[tr], y[tr], eval_set=[(X[va], y[va])],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    return model


def calibrate(model, X, y, mask, method="sigmoid"):
    """Calibre un modèle déjà entraîné sur le jeu de validation (prefit).
    Indispensable au value betting : l'edge n'a de sens que si les probas sont
    justes. Gère l'API sklearn ancienne (cv='prefit') et récente (FrozenEstimator)."""
    try:
        from sklearn.frozen import FrozenEstimator
        return CalibratedClassifierCV(FrozenEstimator(model), method=method).fit(X[mask], y[mask])
    except ImportError:
        return CalibratedClassifierCV(model, method=method, cv="prefit").fit(X[mask], y[mask])


class SeedEnsemble:
    """Moyenne des probabilités de plusieurs modèles calibrés entraînés avec des graines différentes.

    Un LightGBM seul varie d'environ ±0,02 de logloss d'une graine à l'autre ; la moyenne de
    plusieurs graines supprime l'essentiel de ce bruit. L'objet se comporte comme un modèle
    scikit-learn pour le reste du code (classes_, predict_proba, predict)."""

    def __init__(self, models):
        self.models = list(models)
        self.classes_ = self.models[0].classes_

    def predict_proba(self, X):
        return np.mean([m.predict_proba(X) for m in self.models], axis=0)

    def predict(self, X):
        return self.classes_[self.predict_proba(X).argmax(axis=1)]


PROBA_FLOOR = 0.005   # aucune issue n'est jamais annoncée à moins de 0,5 % (garde-fou, non optimisé)


def floor_proba(P, floor=PROBA_FLOOR):
    """Applique un plancher aux probabilités (n_matchs × 3) puis renormalise chaque ligne à 1."""
    P = np.maximum(P, floor)
    return P / P.sum(axis=1, keepdims=True)


def evaluate(model, X, y, mask, name):
    """Logloss + accuracy (+ AUC si binaire). Affiche et retourne un dict."""
    labels = np.sort(np.unique(y))
    proba = model.predict_proba(X[mask])
    if proba.shape[1] == 3:                  # 1N2 : plancher de probabilité (cf. floor_proba)
        proba = floor_proba(proba)
    metrics = {
        "logloss": log_loss(y[mask], proba, labels=labels),
        "accuracy": accuracy_score(y[mask], proba.argmax(1)),
    }
    if len(labels) == 2:
        metrics["auc"] = roc_auc_score(y[mask], proba[:, 1])
    line = " | ".join(f"{k} {v:.4f}" for k, v in metrics.items())
    print(f"  {name:5} {line} | n={int(mask.sum()):,}")
    return metrics

def setup_mlflow(cfg):
    """Configure MLflow depuis config.yaml et retourne l'URI effective.

    - Charge .env (sans quoi DAGSHUB_* n'est pas dans l'environnement quand on
      lance le script en direct).
    - Si l'URI est un remote dagshub, traduit DAGSHUB_* → MLFLOW_TRACKING_* :
      c'est ce couple que le client HTTP de MLflow lit pour s'authentifier.
      Sans cette traduction, la requête part sans auth → 403.
    - Pose le tracking_uri. Import mlflow local : on ne le charge que si un
      trainer en a besoin.

    À appeler dans un try/except côté trainer (best-effort : un remote KO ne
    doit pas bloquer l'entraînement, le modèle est déjà sauvé sur disque).
    """
    import mlflow
    load_dotenv(ROOT_DIR / ".env")
    uri = cfg["mlflow"]["tracking_uri"]
    if "dagshub" in str(uri):
        os.environ["MLFLOW_TRACKING_USERNAME"] = os.getenv("DAGSHUB_USERNAME", "")
        os.environ["MLFLOW_TRACKING_PASSWORD"] = os.getenv("DAGSHUB_TOKEN", "")
    mlflow.set_tracking_uri(uri)
    return uri