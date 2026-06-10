"""
Pipeline 04 — Train (Two-Stage Stacking)
==========================================
Architecture :
  Stage 1A : LightGBM sur perspective HOME (features équipe domicile)
  Stage 1B : LightGBM sur perspective AWAY (features équipe extérieur)
  Stage 1C : Logistic Regression baseline (signal linéaire)
  Stage 2  : LogReg méta sur [P_1A, P_1B, P_1C] → P(H/D/A) final

Consolidation : pivot home/away → 1 ligne par match
Évaluation    : Log Loss, Brier Score, Calibration, Précision upsets

Usage :
    python pipelines/04_train.py
    python pipelines/04_train.py --no-shap
    python pipelines/04_train.py --step 1   # Stage 1 uniquement
"""

import argparse
from sys import prefix
import warnings
import joblib
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")   # pas de display requis — sauvegarde fichier uniquement
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

import duckdb
import numpy as np
import pandas as pd
import yaml
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
from loguru import logger

from sklearn.linear_model import LogisticRegression
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.calibration import IsotonicRegression, calibration_curve
from sklearn.metrics import (
    accuracy_score, log_loss, classification_report,
    brier_score_loss, mean_absolute_error
)
from sklearn.model_selection import StratifiedKFold

import lightgbm as lgb

warnings.filterwarnings("ignore")


# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODELS_DIR = ROOT_DIR / CFG["paths"].get("models", "models")
MODELS_DIR.mkdir(exist_ok=True)

DIAG_DIR   = MODELS_DIR / "diagnostics"
DIAG_DIR.mkdir(exist_ok=True)

MLFLOW_URI = CFG["mlflow"]["tracking_uri"]
TARGET     = "result_1n2"

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/train.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

TRAIN_SEASONS = CFG["train"]["TRAIN_SEASONS"]
VAL_SEASONS = CFG["train"]["VAL_SEASONS"]
TEST_SEASON = CFG["train"]["TEST_SEASON"]

# Colonnes identifiants — jamais dans le modèle
ID_COLS = [
    "date", "team", "opponent", "venue", "season", "league_source",
    "comp_category","match_id", "result_1n2",
    # Cotes exclues des Stage 1 — entrée directe dans Stage 2 uniquement
    "odds_pinnacle_team", "odds_pinnacle_draw", "odds_pinnacle_opp",
    "odds_avg_team", "odds_avg_draw", "odds_avg_opp",
    "pinnacle_prob_team", "pinnacle_prob_draw", "pinnacle_prob_opp",
    "market_prob_team", "market_prob_draw", "market_prob_opp",
    "pinnacle_edge", "market_edge",
    "opp_odds_pinnacle", "opp_pinnacle_prob", "opp_market_prob",
]

# Features à winsoriser
COLS_TO_WINSORIZE = [
    "sterility_index_5", 
    "sterility_diff",
    "press_resistance_5", 
    "press_resistance_diff",
    "shield_efficiency_5", 
    "shield_efficiency_diff",
    "xg_overperformance_5", 
    "xg_opi_diff",
    "fouls_per_tackle_roll_5",
    "ws_momentum_delta",
    "ws_momentum_diff",
]

# Features propres à chaque perspective
# Stage 1A = features de l'équipe qui joue à domicile
# Stage 1B = features de l'équipe qui joue à l'extérieur
# On les sépare lors du pivot — le reste est partagé
HOME_SPECIFIC = [
    "np_xg_roll_venue_5", "shot_quality_ratio_venue_5",
    "poss_roll_venue_5", "is_home",
]
AWAY_SPECIFIC = [c.replace("home", "away") for c in HOME_SPECIFIC
                 if "home" in c]


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 1 — Chargement et pivot home/away
# ══════════════════════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    """Charge gold.features_final filtré Big5."""
    logger.info("Chargement gold.features_final...")
    conn = duckdb.connect(DB_PATH)
    df = conn.execute("""
        SELECT *
        FROM gold.features_final
        WHERE comp_category = 'Big5'
    """).df()
    conn.close()

    df["date"] = pd.to_datetime(df["date"])
    logger.info(f"  {len(df):,} lignes × {len(df.columns)} colonnes")
    logger.info(f"  Distribution : {df[TARGET].value_counts().to_dict()}")
    return df


def pivot_to_match(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivote les données de 2 lignes par match (home + away)
    vers 1 ligne par match.

    Principe :
      - Ligne Home : features de l'équipe domicile
      - Ligne Away : features de l'équipe extérieur
      - On joint sur match_id en séparant home_ et away_

    Le résultat contient :
      - Toutes les features Home préfixées h_
      - Toutes les features Away préfixées a_
      - result_match : H/D/A du point de vue domicile
    """
    logger.info("Pivot home/away → 1 ligne par match...")

    id_cols_pivot = ["match_id", "date", "season",
                     "league_source", "comp_category"]
    feature_cols  = [c for c in df.columns
                     if c not in ID_COLS + id_cols_pivot
                     and c not in ["team", "opponent"]]

    # Séparer home et away
    home = df[df["venue"] == "Home"].copy()
    away = df[df["venue"] == "Away"].copy()

    # Renommer les features
    home_renamed = home[id_cols_pivot + ["team", "opponent", TARGET] + feature_cols].copy()
    away_renamed = away[id_cols_pivot + ["team", "opponent"] + feature_cols].copy()

    print(f" Colonnes avant pivot : {len(feature_cols)}")
    print(f" Colonnes home: {home_renamed.columns.tolist()}")

    home_renamed = home_renamed.rename(
        columns={c: f"h_{c}" for c in feature_cols}
    ).rename(columns={"team": "home_team", "opponent": "away_team",
                       TARGET: "result_match"})

    away_renamed = away_renamed.rename(
        columns={c: f"a_{c}" for c in feature_cols}
    ).rename(columns={"team": "away_team_check", "opponent": "home_team_check"})

    print(f"  Home features : {len(feature_cols)} → {len([c for c in home_renamed.columns if c.startswith('h_')])}")
    print(f"  Away features : {len(feature_cols)} → {len([c for c in away_renamed.columns if c.startswith('a_')])}")

    print(f" Colonnes après pivot : {len(feature_cols)}")
    print(f" Colonnes home: {home_renamed.columns.tolist()}")
    print(f"Nb feature_cols : {len(feature_cols)}")
    print(f"match_id dans feature_cols : {'match_id' in feature_cols}")

    # Jointure sur match_id
    merged = home_renamed.merge(
        away_renamed[["match_id"] +
                     [f"a_{c}" for c in feature_cols]],
        on="match_id",
        how="inner",
    )

    logger.info(f"  {len(merged):,} matchs consolidés")
    logger.info(f"  Distribution : {merged['result_match'].value_counts().to_dict()}")
    return merged


def winsorize(df: pd.DataFrame,
              caps: dict = None,
              fit: bool = True) -> tuple[pd.DataFrame, dict]:
    """Capper au percentile 1-99% — caps calculés sur train si fit=True."""
    cols_present = [c for c in COLS_TO_WINSORIZE if c in df.columns]

    # Ajouter les versions h_ et a_ après pivot
    cols_h = [f"h_{c}" for c in COLS_TO_WINSORIZE
              if f"h_{c}" in df.columns]
    cols_a = [f"a_{c}" for c in COLS_TO_WINSORIZE
              if f"a_{c}" in df.columns]
    all_cols = cols_present + cols_h + cols_a

    if fit:
        caps = {}
        for col in all_cols:
            caps[col] = {
                "lower": df[col].quantile(0.01),
                "upper": df[col].quantile(0.99),
            }

    for col, bounds in (caps or {}).items():
        if col in df.columns:
            df[col] = df[col].clip(bounds["lower"], bounds["upper"])

    return df, caps


def split_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split temporel Train / Val / Test."""
    train = df[df["season"].isin(TRAIN_SEASONS)].copy()
    val   = df[df["season"].isin(VAL_SEASONS)].copy()
    test  = df[df["season"] == TEST_SEASON].copy()
    logger.info(f"  Train : {len(train):,} | Val : {len(val):,} | Test : {len(test):,}")

    for name, split in [("Train", train), ("Val", val)]:
        if "result_match" in split.columns:
            dist = split["result_match"].value_counts(normalize=True).round(3).to_dict()
            logger.info(f"  Distribution {name} (post-pivot) : {dist}")
    return train, val, test


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 2 — Préparation des features par perspective
# ══════════════════════════════════════════════════════════════════════════════

def prepare_perspective(df_match: pd.DataFrame,
                        perspective: str) -> tuple[pd.DataFrame, pd.Series]:
    """
    Extrait les features pour une perspective donnée (home/away).
    perspective = 'home' → préfixe h_
    perspective = 'away' → préfixe a_
    """
    prefix = "h_" if perspective == "home" else "a_"
    feat_cols = [c for c in df_match.columns if c.startswith(prefix)]

    X = df_match[feat_cols].copy()
    X.columns = [c[len(prefix):] for c in X.columns]  # strip prefix
    # Exclure les colonnes non-numériques (ex: formation — encodage prévu en V2)
    X = X.select_dtypes(include=[np.number])

    y_raw = df_match["result_match"]
    if perspective == "away":
        # Du point de vue Away : H → A, A → H, D → D
        y_raw = y_raw.map({"H": "A", "A": "H", "D": "D"})

    return X, y_raw


def prepare_combined(df_match: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Features combinées home + away pour le Stage 2."""
    h_cols = [c for c in df_match.columns if c.startswith("h_")]
    a_cols = [c for c in df_match.columns if c.startswith("a_")]
    X = df_match[h_cols + a_cols].copy()
    X = X.select_dtypes(include=[np.number])
    y = df_match["result_match"]
    return X, y


def build_preprocessor() -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
    ])


def encode_labels(y_train, y_val) -> tuple:
    le = LabelEncoder()
    return (le.fit_transform(y_train),
            le.transform(y_val),
            le)

def encode_test_labels(y_test, le: LabelEncoder) -> np.ndarray:
    """Encode le test set avec un LabelEncoder déjà fitté sur train."""
    return le.transform(y_test)


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 3 — Stage 1 : modèles par perspective
# ══════════════════════════════════════════════════════════════════════════════
def tune_lgbm_perspective(
    X_train, y_train, X_val, y_val,
    le, name: str, n_trials: int = 50
) -> lgb.LGBMClassifier:
    """
    Optimisation Optuna des hyperparamètres LightGBM.
    Objectif : minimiser le LogLoss sur le val set.
    """
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    n_classes = len(le.classes_)

    def objective(trial):
        params = {
            "objective":        "multiclass",
            "num_class":        n_classes,
            "metric":           "multi_logloss",
            "n_estimators":     1000,
            "class_weight":     "balanced",
            "learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
            "max_depth":        trial.suggest_int("max_depth", 3, 7),
            "num_leaves":       trial.suggest_int("num_leaves", 15, 127),
            "min_child_samples":trial.suggest_int("min_child_samples", 10, 80),
            "subsample":        trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 0.9),
            "reg_alpha":        trial.suggest_float("reg_alpha", 0.01, 10.0, log=True),
            "reg_lambda":       trial.suggest_float("reg_lambda", 0.01, 10.0, log=True),
            "random_state":     42,
            "n_jobs":           -1,
            "verbose":          -1,
        }

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(-1),
            ],
        )
        proba = model.predict_proba(X_val)
        return log_loss(y_val, proba)

    logger.info(f"  Optuna [{name}] — {n_trials} trials...")
    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(
            seed=42 if "Home" in name else 43
        ),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    logger.info(f"  Best LogLoss : {study.best_value:.4f}")
    logger.info(f"  Best params  : {study.best_params}")

    # Réentraîner avec les meilleurs paramètres
    best_params = study.best_params
    best_model  = lgb.LGBMClassifier(
        objective="multiclass",
        num_class=n_classes,
        metric="multi_logloss",
        n_estimators=1000,
        is_unbalance=True,
        **best_params,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    best_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[
            lgb.early_stopping(50, verbose=False),
            lgb.log_evaluation(-1),
        ],
    )

    proba = best_model.predict_proba(X_val)
    ll    = log_loss(y_val, proba)
    acc   = accuracy_score(y_val, np.argmax(proba, axis=1))
    logger.info(f"    {name} tuné — LogLoss: {ll:.4f} | Acc: {acc:.4f} | BestIter: {best_model.best_iteration_}")

    return best_model

def train_lgbm_perspective(X_train, y_train, X_val, y_val,
                            le, name: str) -> lgb.LGBMClassifier:
    """Entraîne un LightGBM multiclasse pour une perspective."""
    logger.info(f"  Stage 1 — {name}...")
    model = lgb.LGBMClassifier(
        objective="multiclass",
        num_class=len(le.classes_),
        metric="multi_logloss",
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=4,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.6,
        reg_alpha=1.0,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[
            lgb.early_stopping(50, verbose=False),
            lgb.log_evaluation(-1),
        ],
    )
    proba = model.predict_proba(X_val)
    ll    = log_loss(y_val, proba)
    acc   = accuracy_score(y_val, np.argmax(proba, axis=1))
    logger.info(f"    {name} — LogLoss: {ll:.4f} | Acc: {acc:.4f} | BestIter: {model.best_iteration_}")
    return model


def train_lr_baseline(X_train, y_train, X_val, y_val,
                       le) -> LogisticRegression:
    """Baseline LR sur features combinées."""
    logger.info("  Stage 1C — LR Baseline...")
    lr = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        C=1.0,
        random_state=42,
        n_jobs=-1,
    )
    lr.fit(X_train, y_train)
    proba = lr.predict_proba(X_val)
    ll    = log_loss(y_val, proba)
    acc   = accuracy_score(y_val, np.argmax(proba, axis=1))
    logger.info(f"    LR Baseline — LogLoss: {ll:.4f} | Acc: {acc:.4f}")
    return lr


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 4 — Stage 2 : méta-modèle
# ══════════════════════════════════════════════════════════════════════════════

def build_meta_features(models: dict,
                         preprocessors: dict,
                         df_match: pd.DataFrame,
                         le_home, le_away, le_combined) -> np.ndarray:
    """
    Construit la matrice méta à partir des probabilités Stage 1.
    Chaque modèle contribue 3 colonnes (P_H, P_D, P_A).
    """
    parts = []

    # Stage 1A — perspective Home
    X_h, _ = prepare_perspective(df_match, "home")
    X_h_s  = preprocessors["home"].transform(X_h)
    p_home  = models["lgbm_home"].predict_proba(X_h_s)
    # Réordonner vers H/D/A du point de vue match (home team)
    p_home  = _reorder_proba(p_home, le_home, ["H", "D", "A"])
    parts.append(p_home)

    # Stage 1B — perspective Away (inverser H↔A)
    X_a, _ = prepare_perspective(df_match, "away")
    X_a_s  = preprocessors["away"].transform(X_a)
    p_away  = models["lgbm_away"].predict_proba(X_a_s)
    # Du point de vue away : classe H = victoire away = A du point de vue match
    p_away  = _reorder_proba(p_away, le_away, ["A", "D", "H"])
    parts.append(p_away)

    # Stage 1C — LR Baseline sur combined
    X_c, _ = prepare_combined(df_match)
    X_c_s  = preprocessors["combined"].transform(X_c)
    p_lr    = models["lr_baseline"].predict_proba(X_c_s)
    p_lr    = _reorder_proba(p_lr, le_combined, ["H", "D", "A"])
    parts.append(p_lr)

    # Stage 1D — Signal marché Pinnacle
    # Réordonner H/D/A selon le point de vue du match (home_team perspective)
    # pinnacle_prob_team = P(victoire équipe) = P(H) du point de vue match
    odds_cols = ["pinnacle_prob_team", "pinnacle_prob_draw", "pinnacle_prob_opp"]
    if all(c in df_match.columns for c in odds_cols):
        p_market = df_match[odds_cols].values.astype(float)
        # Remplacer les NaN par 1/3 (prior uniforme si cotes absentes)
        p_market = np.where(np.isnan(p_market), 1/3, p_market)
        parts.append(p_market)  # shape (n, 3) → 3 colonnes méta supplémentaires
    else:
        # Fallback si colonnes absentes
        parts.append(np.full((len(df_match), 3), 1/3))

    # Stage 1E — Features contextuelles Draw
    # h2h_draw_rate : % historique de nuls entre ces deux équipes
    # h2h_n_matches : fiabilité du signal (peu de matchs = signal faible)
    # market_prob_draw : le marché encode déjà une info sur la probabilité de nul
    draw_cols = ["h_h2h_draw_rate", "h_h2h_n_matches", "h_market_prob_draw", "h_league_draw_rate",
                    "h_sterility_weighted_10",
                    "h_shots_faced_per_goal_conceded_5"]
    draw_feats_list = []
    for c in draw_cols:
        if c in df_match.columns:
            arr = df_match[c].to_numpy(dtype=float, na_value=0.0)
            draw_feats_list.append(arr)

    if draw_feats_list:
        draw_feats = np.column_stack(draw_feats_list)
        parts.append(draw_feats)

    return np.hstack(parts)  # shape (n, 12+)


def _reorder_proba(proba: np.ndarray,
                   le: LabelEncoder,
                   target_order: list) -> np.ndarray:
    """Réordonne les colonnes de proba selon target_order."""
    current = list(le.classes_)
    idx     = [current.index(c) if c in current else 0
               for c in target_order]
    return proba[:, idx]


def train_meta(X_meta_train, y_train,
               X_meta_val, y_val, le) -> LogisticRegression:
    """Stage 2 : LogReg méta — configuration neutre, LogLoss minimal."""
    logger.info("── Stage 2 : Méta-modèle LogReg ─────────────────────────")
    meta = LogisticRegression(
        max_iter=2000,
        C=0.1,
        class_weight=None,   # Neutre — pas de biais artificiel
        random_state=42,
    )
    meta.fit(X_meta_train, y_train)

    proba = meta.predict_proba(X_meta_val)
    ll    = log_loss(y_val, proba)
    acc   = accuracy_score(y_val, np.argmax(proba, axis=1))
    logger.info(f"  Stage 2 — LogLoss: {ll:.4f} | Acc: {acc:.4f}")
    return meta


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 5 — Calibration
# ══════════════════════════════════════════════════════════════════════════════

def calibrate(model, X_val, y_val_enc, le) -> tuple:
    """
    Calibration isotonique par classe.
    Le val set est splitté en deux moitiés temporelles :
      - première moitié  → fit des calibrateurs (cal split)
      - deuxième moitié  → évaluation finale non biaisée (eval split)
    """
    logger.info("── Calibration isotonique ───────────────────────────────")

    n        = len(y_val_enc)
    split    = n // 2
    cal_idx  = np.arange(0, split)
    eval_idx = np.arange(split, n)

    logger.info(f"  Cal split  : {len(cal_idx)} matchs")
    logger.info(f"  Eval split : {len(eval_idx)} matchs")

    if hasattr(X_val, "iloc"):
        X_cal  = X_val.iloc[cal_idx]
        X_eval = X_val.iloc[eval_idx]
    else:
        X_cal  = X_val[cal_idx]
        X_eval = X_val[eval_idx]

    y_cal  = y_val_enc[cal_idx]
    y_eval = y_val_enc[eval_idx]

    probs_cal = model.predict_proba(X_cal)
    n_cl      = probs_cal.shape[1]

    calibrators = []
    for c in range(n_cl):
        cal = IsotonicRegression(out_of_bounds="clip")
        cal.fit(probs_cal[:, c], (y_cal == c).astype(int))
        calibrators.append(cal)

    def predict_calibrated(X):
        raw = model.predict_proba(X)
        cal_probs = np.column_stack([
            calibrators[c].transform(raw[:, c]) for c in range(n_cl)
        ])
        row_sums = cal_probs.sum(axis=1, keepdims=True)
        cal_probs = cal_probs / np.where(row_sums == 0, 1, row_sums)
        return np.clip(cal_probs, 0.01, 0.99)

    return predict_calibrated, calibrators, X_eval, y_eval, eval_idx

def optimize_draw_threshold(calibrators, meta_model, X_val, y_val_enc, le,
                             thresholds=None) -> float:
    """
    Cherche le seuil P(D) optimal sur le val set (après calibration).
    Si P(D) > threshold → prédit D, sinon argmax(H, A).
    
    Objectif : maximiser le F1-score Draw (précision × recall équilibrés).
    Retourne le seuil optimal.
    """
    if thresholds is None:
        thresholds = np.arange(0.25, 0.55, 0.01)

    idx_D = list(le.classes_).index("D")
    idx_H = list(le.classes_).index("H")
    idx_A = list(le.classes_).index("A")

    raw_proba = meta_model.predict_proba(X_val)
    n_cl = raw_proba.shape[1]
    cal_proba = np.column_stack([
        calibrators[i].predict(raw_proba[:, i])   # ← index entier, pas nom de classe
        for i in range(n_cl)
    ])
    # Renormaliser (isotonique ne garantit pas la somme = 1)
    row_sums = cal_proba.sum(axis=1, keepdims=True)
    cal_proba = cal_proba / np.where(row_sums == 0, 1, row_sums)
    cal_proba = np.clip(cal_proba, 0.01, 0.99)

    results = []
    for t in thresholds:
        # Règle de décision : D si P(D) > t, sinon max(H, A)
        pred = np.where(
            cal_proba[:, idx_D] > t,
            idx_D,
            np.where(cal_proba[:, idx_H] > cal_proba[:, idx_A], idx_H, idx_A)
        )
        # F1 Draw uniquement
        draw_mask_true = (y_val_enc == idx_D)
        draw_mask_pred = (pred == idx_D)
        tp = (draw_mask_true & draw_mask_pred).sum()
        precision = tp / draw_mask_pred.sum() if draw_mask_pred.sum() > 0 else 0
        recall    = tp / draw_mask_true.sum() if draw_mask_true.sum() > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        results.append((t, f1, precision, recall))

    # Seuil optimal = F1 Draw max
    best = max(results, key=lambda x: x[1])
    logger.info(f"\n── Optimisation seuil Draw ──────────────────────────────")
    logger.info(f"  {'Seuil':>6} | {'F1-D':>6} | {'Prec':>6} | {'Recall':>6}")
    logger.info(f"  {'-'*34}")
    for t, f1, prec, rec in results:
        marker = " ◄ BEST" if t == best[0] else ""
        logger.info(f"  {t:>6.2f} | {f1:>6.4f} | {prec:>6.4f} | {rec:>6.4f}{marker}")
    logger.info(f"  Seuil optimal Draw : {best[0]:.2f} "
                f"(F1={best[1]:.4f} | Prec={best[2]:.4f} | Recall={best[3]:.4f})")
    return best[0]
# ══════════════════════════════════════════════════════════════════════════════
# BLOC 6 — Évaluation
# ══════════════════════════════════════════════════════════════════════════════
def predict_with_draw_boost(y_proba, le, draw_threshold: float = 0.28) -> np.ndarray:
    """
    Décision finale avec boost Draw.
    Si P(D) > draw_threshold ET P(D) > P(H)*0.7 ET P(D) > P(A)*0.7 → prédit Draw.
    Sinon, argmax standard.
    """
    idx_D = list(le.classes_).index("D")
    idx_H = list(le.classes_).index("H")
    idx_A = list(le.classes_).index("A")

    p_d = y_proba[:, idx_D]
    p_h = y_proba[:, idx_H]
    p_a = y_proba[:, idx_A]

    draw_mask = (p_d > draw_threshold) & (p_d > p_h * 0.7) & (p_d > p_a * 0.7)

    y_pred = np.argmax(y_proba, axis=1).copy()
    y_pred[draw_mask] = idx_D
    return y_pred

def evaluate_full(y_true_enc, y_proba, le, label: str,draw_threshold: float = 0.28) -> dict:
    """Log Loss + Accuracy + Brier Score + rapport par classe."""
    y_pred = predict_with_draw_boost(y_proba, le, draw_threshold= draw_threshold)
    y_true_dec = le.inverse_transform(y_true_enc)
    y_pred_dec = le.inverse_transform(y_pred)

    acc  = accuracy_score(y_true_enc, y_pred)
    ll   = log_loss(y_true_enc, y_proba)

    # Brier Score multiclasse (moyenne sur les classes)
    brier = np.mean([
        brier_score_loss(
            (y_true_enc == c).astype(int),
            y_proba[:, c]
        )
        for c in range(len(le.classes_))
    ])

    logger.info(f"\n── [{label}] ─────────────────────────────────────────────")
    logger.info(f"  Accuracy    : {acc:.4f}")
    logger.info(f"  Log Loss    : {ll:.4f}")
    logger.info(f"  Brier Score : {brier:.4f}")
    logger.info("\n" + classification_report(
        y_true_dec, y_pred_dec,
        target_names=list(le.classes_),
        zero_division=0,
    ))

    # Calibration curve par classe
    for c, cls in enumerate(le.classes_):
        fraction, mean_pred = calibration_curve(
            (y_true_enc == c).astype(int),
            y_proba[:, c],
            n_bins=10,
        )
        mae_cal = mean_absolute_error(fraction, mean_pred)
        logger.info(f"  Calibration MAE [{cls}] : {mae_cal:.4f}")

    return {"accuracy": acc, "log_loss": ll, "brier_score": brier}


def analyze_upsets(y_proba, y_true_enc, le,
                    df_match: pd.DataFrame,draw_threshold: float = 0.28) -> pd.DataFrame:
    """
    Précision sur les upsets : matchs où le modèle détecte
    une victoire extérieure improbable.
    Un upset = P(Away) > P(Home) + seuil
    """
    logger.info("── Analyse Upsets ───────────────────────────────────────")
    classes = list(le.classes_)
    idx_H   = classes.index("H")
    idx_A   = classes.index("A")
    idx_D   = classes.index("D")

    df_out = pd.DataFrame({
        "match_id": df_match["match_id"].values,
        "home_team":      df_match["home_team"].values,
        "away_team":      df_match["away_team"].values,
        "date":           df_match["date"].values,
        "league":         df_match["league_source"].values,
        "season":         df_match["season"].values,
        "prob_home":      y_proba[:, idx_H],
        "prob_draw":      y_proba[:, idx_D],
        "prob_away":      y_proba[:, idx_A],
        "actual_result":  le.inverse_transform(y_true_enc),
        "pred":           le.inverse_transform(predict_with_draw_boost(y_proba, le, draw_threshold=draw_threshold)),
    })

    df_out["correct"]    = (df_out["pred"] == df_out["actual_result"]).astype(int)
    df_out["confidence"] = df_out[["prob_home", "prob_draw", "prob_away"]].max(axis=1)

    # Matchs upsets détectés (modèle favorise Away)
    df_upset = df_out[df_out["pred"] == "A"].copy()
    logger.info(f"\n  Upsets détectés (pred=Away) : {len(df_upset)}")
    if len(df_upset) > 0:
        prec = df_upset["correct"].mean()
        logger.info(f"  Précision sur upsets : {prec:.2%}")

    # Précision par seuil de confiance
    logger.info(f"\n  {'Seuil':>8} | {'N':>6} | {'Acc':>8}")
    logger.info(f"  {'-'*28}")
    for t in [0.40, 0.45, 0.50, 0.55, 0.60]:
        mask = df_out["confidence"] > t
        n    = mask.sum()
        if n > 0:
            acc = df_out.loc[mask, "correct"].mean()
            logger.info(f"  > {t:.2f}   | {n:>6d} | {acc:>8.2%}")

    return df_out

def compute_rps(y_true_str: pd.Series,
                prob_h: np.ndarray,
                prob_d: np.ndarray,
                prob_a: np.ndarray) -> float:
    """
    Ranked Probability Score multiclasse pour H/D/A.
    Ordre naturel : H=0, D=1, A=2
    RPS plus bas = meilleur modèle.
    """
    classes = ["H", "D", "A"]
    n = len(y_true_str)
    rps_total = 0.0

    for i, true_cls in enumerate(y_true_str):
        # Vecteur one-hot de la vraie classe
        o = np.array([1.0 if c == true_cls else 0.0 for c in classes])
        # Vecteur des probabilités prédites
        p = np.array([prob_h[i], prob_d[i], prob_a[i]])
        # RPS = somme des carrés des CDF cumulées
        rps_total += np.sum((np.cumsum(p) - np.cumsum(o)) ** 2)

    return rps_total / n


def log_detailed_metrics(df_preds: pd.DataFrame, prefix: str) -> None:
    """
    Calcule et logge dans MLflow les métriques détaillées par league,
    par saison et par classe.

    Paramètres :
        df_preds : DataFrame avec colonnes league, season, actual_result,
                   pred, prob_home, prob_draw, prob_away
        prefix   : "val", "test" ou "train"
    """
    metrics = {}
    classes = ["H", "D", "A"]

    prob_matrix = df_preds[["prob_home", "prob_draw", "prob_away"]].values

    # ── F1 / précision / rappel par classe — global ───────────────────────────
    from sklearn.metrics import precision_recall_fscore_support
    prec, rec, f1, _ = precision_recall_fscore_support(
        df_preds["actual_result"],
        df_preds["pred"],
        labels=classes,
        zero_division=0,
    )
    for i, cls in enumerate(classes):
        metrics[f"{prefix}_f1_{cls}"]        = float(f1[i])
        metrics[f"{prefix}_precision_{cls}"] = float(prec[i])
        metrics[f"{prefix}_recall_{cls}"]    = float(rec[i])

    # ── RPS global ────────────────────────────────────────────────────────────
    rps = compute_rps(
        df_preds["actual_result"],
        df_preds["prob_home"].values,
        df_preds["prob_draw"].values,
        df_preds["prob_away"].values,
    )
    metrics[f"{prefix}_rps"] = float(rps)

    # ── ECE global ────────────────────────────────────────────────────────────
    # On moyenne l'ECE sur les 3 classes (one-vs-rest)
    ece_total = 0.0
    for i, cls in enumerate(classes):
        y_binary   = (df_preds["actual_result"] == cls).astype(int).values
        y_prob     = prob_matrix[:, i]
        frac, mean = calibration_curve(y_binary, y_prob, n_bins=10)
        ece_total += float(mean_absolute_error(frac, mean))
    metrics[f"{prefix}_ece"] = ece_total / 3

    # ── Overconfidence rate ───────────────────────────────────────────────────
    # % de prédictions avec confiance > 80% qui sont fausses
    high_conf_mask = df_preds[["prob_home", "prob_draw", "prob_away"]].max(axis=1) > 0.80
    if high_conf_mask.sum() > 0:
        wrong_high_conf = (
            df_preds.loc[high_conf_mask, "pred"]
            != df_preds.loc[high_conf_mask, "actual_result"]
        ).mean()
        metrics[f"{prefix}_overconfidence_rate"] = float(wrong_high_conf)

    # ── Métriques par league ──────────────────────────────────────────────────
    for league, grp in df_preds.groupby("league"):
        league_key = league.lower().replace(" ", "_")
        acc = accuracy_score(grp["actual_result"], grp["pred"])
        ll  = log_loss(grp["actual_result"], grp[["prob_home", "prob_draw", "prob_away"]].values,
                       labels=classes)
        metrics[f"{prefix}_acc_{league_key}"]      = float(acc)
        metrics[f"{prefix}_log_loss_{league_key}"] = float(ll)

    # ── Métriques par saison ──────────────────────────────────────────────────
    for season, grp in df_preds.groupby("season"):
        season_key = season.replace("-", "_")
        acc = accuracy_score(grp["actual_result"], grp["pred"])
        metrics[f"{prefix}_acc_{season_key}"] = float(acc)

    mlflow.log_metrics(metrics)
    logger.info(f"  [{prefix}] {len(metrics)} métriques loggées dans MLflow")

# ══════════════════════════════════════════════════════════════════════════════
# BLOC 7 — SHAP
# ══════════════════════════════════════════════════════════════════════════════

def analyze_shap(model, X_val, feature_names: list, le,
                  label: str = ""):
    """Top 20 features par classe via SHAP TreeExplainer."""
    logger.info(f"── SHAP [{label}] ─────────────────────────────────────")
    try:
        import shap
    except ImportError:
        logger.warning("  pip install shap")
        return

    explainer = shap.TreeExplainer(model)
    shap_vals = explainer.shap_values(X_val)

    for i, cls in enumerate(le.classes_):
        sv       = shap_vals[i] if isinstance(shap_vals, list) else shap_vals[:, :, i]
        mean_abs = np.abs(sv).mean(axis=0)
        top_idx  = np.argsort(mean_abs)[::-1][:20]
        logger.info(f"\n  Top 20 → classe '{cls}' :")
        for rank, idx in enumerate(top_idx, 1):
            name = feature_names[idx] if idx < len(feature_names) else f"feat_{idx}"
            logger.info(f"    {rank:2d}. {name:<45} {mean_abs[idx]:.4f}")


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 8 — DIAGNOSTIC VISUEL (Draw analysis)
# ══════════════════════════════════════════════════════════════════════════════

def plot_diagnostics(y_true_enc: np.ndarray,
                     proba_cal:  np.ndarray,
                     le,
                     label:      str = "v1",
                     draw_threshold: float = 0.28) -> None:
    """
    Génère une planche de 4 diagnostics focalisés sur le Draw et sauvegarde
    dans models/diagnostics/diagnostic_{label}.png

    Graphiques :
      1. Reliability Diagram (H / D / A)
         Probabilité prédite (axe X) vs fréquence réelle (axe Y).
         Une courbe parfaitement calibrée = diagonale.
         Signal recherché : si la courbe Draw est systématiquement
         sous la diagonale → le modèle sous-estime P(D).

      2. Distribution de P(Draw) par résultat réel
         KDE des probabilités prédites pour le Draw,
         coloré par résultat réel (H / D / A).
         Signal : les vrais nuls doivent former un mode à droite.
         Le trait vertical = draw_threshold actuel.

      3. Matrice de Confusion normalisée (recall)
         Chaque ligne = résultat réel, chaque colonne = prédiction.
         Valeurs = recall (% de chaque classe bien prédit).
         Signal : vers quelle classe les nuls sont-ils "volés" ?

      4. Distribution comparée P(H) / P(D) / P(A)
         Violin plot des 3 distributions de probabilité.
         Signal : si P(D) est très concentré à gauche (<0.30)
         → le modèle n'a jamais confiance dans les nuls.
    """
    classes  = list(le.classes_)   # ['A', 'D', 'H'] ordre LabelEncoder
    idx      = {c: i for i, c in enumerate(classes)}
    n_cls    = len(classes)
    colors   = {"H": "#2196F3", "D": "#FF9800", "A": "#F44336"}
    y_pred   = predict_with_draw_boost(proba_cal, le, draw_threshold)

    # ── Figure layout ─────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(18, 14))
    fig.patch.set_facecolor("#0F1117")
    gs  = gridspec.GridSpec(2, 2, figure=fig, hspace=0.38, wspace=0.32)
    ax_rel  = fig.add_subplot(gs[0, 0])   # Reliability
    ax_kde  = fig.add_subplot(gs[0, 1])   # KDE P(Draw)
    ax_cm   = fig.add_subplot(gs[1, 0])   # Confusion matrix
    ax_vio  = fig.add_subplot(gs[1, 1])   # Violin distributions

    _style_ax = lambda ax: (
        ax.set_facecolor("#1A1D27"),
        ax.tick_params(colors="white", labelsize=9),
        [s.set_edgecolor("#444") for s in ax.spines.values()],
        ax.xaxis.label.set_color("white"),
        ax.yaxis.label.set_color("white"),
        ax.title.set_color("white"),
    )

    # ── 1. Reliability Diagram ────────────────────────────────────────────────
    from sklearn.calibration import calibration_curve

    ax_rel.plot([0, 1], [0, 1], "w--", lw=1, alpha=0.5, label="Parfait")
    for cls in ["H", "D", "A"]:
        c     = idx[cls]
        frac, mean_pred = calibration_curve(
            (y_true_enc == c).astype(int),
            proba_cal[:, c],
            n_bins=10,
        )
        ax_rel.plot(mean_pred, frac,
                    "o-", color=colors[cls], lw=2, ms=5,
                    label=f"{cls}  (MAE={np.mean(np.abs(frac - mean_pred)):.3f})")

    ax_rel.set_title("① Reliability Diagram — Calibration par classe", fontsize=11, pad=10)
    ax_rel.set_xlabel("Probabilité prédite")
    ax_rel.set_ylabel("Fréquence réelle")
    ax_rel.legend(fontsize=8, facecolor="#1A1D27", labelcolor="white",
                  edgecolor="#444")
    ax_rel.set_xlim(0, 1); ax_rel.set_ylim(0, 1)
    _style_ax(ax_rel)

    # ── 2. KDE P(Draw) par résultat réel ─────────────────────────────────────
    y_true_dec = le.inverse_transform(y_true_enc)
    p_draw     = proba_cal[:, idx["D"]]

    for cls in ["H", "D", "A"]:
        mask = y_true_dec == cls
        if mask.sum() > 10:
            sns.kdeplot(p_draw[mask], ax=ax_kde,
                        color=colors[cls], fill=True, alpha=0.25,
                        linewidth=2, label=f"Réel={cls} (n={mask.sum()})")

    ax_kde.axvline(draw_threshold, color="white", lw=1.5,
                   ls="--", label=f"Seuil draw_boost={draw_threshold}")
    ax_kde.set_title("② Distribution P(Draw) par résultat réel", fontsize=11, pad=10)
    ax_kde.set_xlabel("P(Draw) prédit")
    ax_kde.set_ylabel("Densité")
    ax_kde.legend(fontsize=8, facecolor="#1A1D27", labelcolor="white",
                  edgecolor="#444")
    ax_kde.set_xlim(0, 1)
    _style_ax(ax_kde)

    # ── 3. Matrice de Confusion normalisée (recall) ───────────────────────────
    from sklearn.metrics import confusion_matrix

    # Ordre d'affichage : H, D, A
    display_order = ["H", "D", "A"]
    enc_order     = [idx[c] for c in display_order]

    # Convertir en labels décodés pour confusion_matrix
    y_true_dec_arr = np.array(le.inverse_transform(y_true_enc))
    y_pred_dec_arr = np.array(le.inverse_transform(y_pred))

    cm = confusion_matrix(y_true_dec_arr, y_pred_dec_arr,
                          labels=display_order, normalize="true")

    sns.heatmap(cm, annot=True, fmt=".2f", ax=ax_cm,
                xticklabels=display_order, yticklabels=display_order,
                cmap="YlOrRd", linewidths=0.5, linecolor="#333",
                annot_kws={"size": 12, "weight": "bold"},
                cbar_kws={"shrink": 0.8})

    ax_cm.set_title("③ Matrice de Confusion normalisée (recall)", fontsize=11, pad=10)
    ax_cm.set_xlabel("Prédit")
    ax_cm.set_ylabel("Réel")
    # Colorier en orange la ligne Draw pour attirer l'œil
    ax_cm.get_yticklabels()[1].set_color(colors["D"])
    ax_cm.get_yticklabels()[1].set_fontweight("bold")
    _style_ax(ax_cm)
    ax_cm.tick_params(colors="white")

    # ── 4. Violin — distributions P(H), P(D), P(A) ───────────────────────────
    df_vio = pd.DataFrame({
        "prob":  np.concatenate([proba_cal[:, idx[c]] for c in ["H", "D", "A"]]),
        "classe": np.repeat(["P(H)", "P(D)", "P(A)"], len(y_true_enc)),
    })
    palette = {"P(H)": colors["H"], "P(D)": colors["D"], "P(A)": colors["A"]}
    sns.violinplot(data=df_vio, x="classe", y="prob", ax=ax_vio,
                   palette=palette, inner="quartile",
                   linewidth=1.2, cut=0)

    ax_vio.axhline(draw_threshold, color="white", lw=1.2, ls="--",
                   label=f"Seuil={draw_threshold}")
    ax_vio.axhline(1/3, color="#aaa", lw=0.8, ls=":",
                   label="Prior uniforme (1/3)")
    ax_vio.set_title("④ Distribution des probabilités prédites", fontsize=11, pad=10)
    ax_vio.set_xlabel("Classe")
    ax_vio.set_ylabel("Probabilité prédite")
    ax_vio.legend(fontsize=8, facecolor="#1A1D27", labelcolor="white",
                  edgecolor="#444")
    ax_vio.set_ylim(0, 1)
    _style_ax(ax_vio)

    # ── Titre global ──────────────────────────────────────────────────────────
    n_draw_true  = (y_true_dec == "D").sum()
    n_draw_pred  = (le.inverse_transform(y_pred) == "D").sum()
    recall_draw  = cm[1, 1]   # ligne D, colonne D dans l'ordre H/D/A

    fig.suptitle(
        f"Diagnostic Modèle [{label}]  —  "
        f"Draws réels : {n_draw_true}  |  "
        f"Draws prédits : {n_draw_pred}  |  "
        f"Recall Draw : {recall_draw:.1%}",
        fontsize=13, color="white", y=0.98, fontweight="bold"
    )

    # ── Sauvegarde ────────────────────────────────────────────────────────────
    out_path = DIAG_DIR / f"diagnostic_{label}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)

    logger.success(f"  Diagnostic sauvegardé : {out_path}")
    logger.info(f"  Draws réels={n_draw_true} | Draws prédits={n_draw_pred} "
                f"| Recall Draw={recall_draw:.1%}")


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main(step: int = 2, use_shap: bool = True, n_trials: int = 50):
    logger.info("=== Démarrage train Two-Stage Stacking ===")

    df       = load_data()
    df_match = pivot_to_match(df)

    # ── Split Train / Val / Test ──────────────────────────────────────────────
    df_train, df_val, df_test = split_data(df_match)

    # Winsorisation — caps calculés sur train uniquement, appliqués sur val et test
    df_train, caps = winsorize(df_train, fit=True)
    df_val,   _    = winsorize(df_val,  caps=caps, fit=False)
    df_test,  _    = winsorize(df_test, caps=caps, fit=False)

    # ── Préparation features ──────────────────────────────────────────────────
    X_h_tr,   y_h_tr   = prepare_perspective(df_train, "home")
    X_h_val,  y_h_val  = prepare_perspective(df_val,   "home")

    X_a_tr,   y_a_tr   = prepare_perspective(df_train, "away")
    X_a_val,  y_a_val  = prepare_perspective(df_val,   "away")

    X_c_tr,   y_c_tr   = prepare_combined(df_train)
    X_c_val,  y_c_val  = prepare_combined(df_val)

    # ── Encodage cible ────────────────────────────────────────────────────────
    # Le LabelEncoder est fitté sur train, le val est transformé — le test aussi
    y_h_tr_enc, y_h_val_enc, le_home     = encode_labels(y_h_tr, y_h_val)
    y_a_tr_enc, y_a_val_enc, le_away     = encode_labels(y_a_tr, y_a_val)
    y_c_tr_enc, y_c_val_enc, le_combined = encode_labels(y_c_tr, y_c_val)

    # Test labels — encodés avec le LabelEncoder fitté, jamais utilisés pendant le train
    _, y_c_test_raw = prepare_combined(df_test)
    y_c_test_enc    = encode_test_labels(y_c_test_raw, le_combined)

    # ── Preprocessing ─────────────────────────────────────────────────────────
    pp_home = build_preprocessor()
    pp_away = build_preprocessor()
    pp_comb = build_preprocessor()

    X_h_tr_s  = pp_home.fit_transform(X_h_tr)
    X_h_val_s = pp_home.transform(X_h_val)

    X_a_tr_s  = pp_away.fit_transform(X_a_tr)
    X_a_val_s = pp_away.transform(X_a_val)

    X_c_tr_s  = pp_comb.fit_transform(X_c_tr)
    X_c_val_s = pp_comb.transform(X_c_val)

    preprocessors = {"home": pp_home, "away": pp_away, "combined": pp_comb}

    # ── MLflow ────────────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    if "dagshub" in str(MLFLOW_URI):
        os.environ["MLFLOW_TRACKING_USERNAME"] = os.getenv("DAGSHUB_USERNAME", "")
        os.environ["MLFLOW_TRACKING_PASSWORD"] = os.getenv("DAGSHUB_TOKEN", "")
    mlflow.set_experiment("football_1N2_stacking")

    with mlflow.start_run(run_name="TwoStage_Stacking_v1"):
        mlflow.log_params({
            "train_seasons": str(TRAIN_SEASONS),
            "val_season":    VAL_SEASONS,
            "test_season":   TEST_SEASON,
            "train_matches": len(df_train),
            "val_matches":   len(df_val),
        })

        # ── Stage 1 ───────────────────────────────────────────────────────────
        logger.info("══ Stage 1 ══════════════════════════════════════════════")

        lgbm_home = tune_lgbm_perspective(
            X_h_tr_s, y_h_tr_enc, X_h_val_s, y_h_val_enc,
            le_home, "LGBM Home", n_trials=n_trials
        )
        lgbm_away = tune_lgbm_perspective(
            X_a_tr_s, y_a_tr_enc, X_a_val_s, y_a_val_enc,
            le_away, "LGBM Away", n_trials=n_trials
        )
        lr_base = train_lr_baseline(
            X_c_tr_s, y_c_tr_enc, X_c_val_s, y_c_val_enc, le_combined
        )

        models = {
            "lgbm_home":  lgbm_home,
            "lgbm_away":  lgbm_away,
            "lr_baseline": lr_base,
        }

        if step == 1:
            logger.info("Step 1 uniquement — arrêt après Stage 1")
            return

        # ── Stage 2 ───────────────────────────────────────────────────────────
        logger.info("══ Stage 2 ══════════════════════════════════════════════")

        X_meta_tr  = build_meta_features(
            models, preprocessors, df_train,
            le_home, le_away, le_combined
        )
        X_meta_val = build_meta_features(
            models, preprocessors, df_val,
            le_home, le_away, le_combined
        )

        meta_model = train_meta(
            X_meta_tr, y_c_tr_enc,
            X_meta_val, y_c_val_enc,
            le_combined
        )

        # ── Calibration ───────────────────────────────────────────────────────
        predict_cal, calibrators, X_meta_eval, y_eval_enc, eval_idx = calibrate(
            meta_model, X_meta_val, y_c_val_enc, le_combined
        )
        df_eval = df_val.iloc[eval_idx].copy()

        draw_threshold = optimize_draw_threshold(
            calibrators, meta_model,
            X_meta_eval, y_eval_enc, le_combined
        )

        # ── Évaluation finale sur eval split uniquement ───────────────────────
        proba_cal = predict_cal(X_meta_eval)
        metrics   = evaluate_full(
            y_eval_enc, proba_cal, le_combined, "Stage 2 Calibré (eval split)",draw_threshold=draw_threshold
        )
        mlflow.log_metrics(metrics)
        mlflow.log_param("draw_threshold", draw_threshold)

        # Évaluation non calibrée pour comparaison
        proba_meta_raw = meta_model.predict_proba(X_meta_eval)
        metrics_raw    = evaluate_full(
            y_eval_enc, proba_meta_raw, le_combined, "Stage 2 Non-calibré (eval split)",draw_threshold=draw_threshold
        )
        mlflow.log_metrics({f"raw_{k}": v for k, v in metrics_raw.items()})

        # ── Analyse upsets ────────────────────────────────────────────────────
        df_results = analyze_upsets(
            proba_cal, y_eval_enc, le_combined, df_eval,draw_threshold=draw_threshold
        )

        # Export résultats
        results_path = MODELS_DIR / "predictions_val.csv"
        log_detailed_metrics(df_results, prefix="val")
        df_results.to_csv(results_path, index=False)
        mlflow.log_artifact(str(results_path))
        logger.info(f"  Prédictions sauvegardées : {results_path}")

        # ── Diagnostic visuel — Val set ───────────────────────────────────────
        plot_diagnostics(
            y_eval_enc, proba_cal, le_combined,
            label=f"v1_val",
            draw_threshold=draw_threshold,
        )
        mlflow.log_artifact(str(DIAG_DIR / "diagnostic_v1_val.png"))

        # ── Évaluation sur Test set (OOS pur — 2024-2025) ────────────────────
        if len(df_test) > 0:
            X_meta_test = build_meta_features(
                models, preprocessors, df_test,
                le_home, le_away, le_combined
            )
            proba_test_cal = predict_cal(X_meta_test)
            metrics_test   = evaluate_full(
                y_c_test_enc, proba_test_cal, le_combined, "Stage 2 Calibré TEST (OOS)",
                draw_threshold=draw_threshold
            )
            mlflow.log_metrics({f"test_{k}": v for k, v in metrics_test.items()})

            # Export prédictions test — utilisées par 06_backtest.py
            df_test_results = analyze_upsets(
                proba_test_cal, y_c_test_enc, le_combined, df_test, draw_threshold=draw_threshold
            )
            test_path = MODELS_DIR / "predictions_test.csv"
            log_detailed_metrics(df_test_results, prefix="test")
            df_test_results.to_csv(test_path, index=False)
            mlflow.log_artifact(str(test_path))
            logger.info(f"  Prédictions test sauvegardées : {test_path}")

            # ── Diagnostic visuel — Test OOS ──────────────────────────────────
            plot_diagnostics(
                y_c_test_enc, proba_test_cal, le_combined,
                label=f"v1_test_oos",
                draw_threshold=draw_threshold,
            )
            mlflow.log_artifact(str(DIAG_DIR / "diagnostic_v1_test_oos.png"))

        # ── SHAP ──────────────────────────────────────────────────────────────
        if use_shap:
            analyze_shap(
                lgbm_home, X_h_val_s,
                list(X_h_val.columns), le_home, "LGBM Home"
            )
            analyze_shap(
                lgbm_away, X_a_val_s,
                list(X_a_val.columns), le_away, "LGBM Away"
            )

        # ── Sauvegarde artefacts ───────────────────────────────────────────────
        artifacts = {
            "preprocessors":   preprocessors,
            "label_encoders": {
                "home":     le_home,
                "away":     le_away,
                "combined": le_combined,
            },
            "models":        models,
            "meta_model":    meta_model,
            "calibrators":   calibrators,
            "winsorize_caps": caps,
            "feature_names": {
                "home":     list(pp_home.get_feature_names_out(X_h_tr.columns)),
                "away":     list(pp_away.get_feature_names_out(X_a_tr.columns)),
                "combined": list(pp_comb.get_feature_names_out(X_c_tr.columns)),
            },
            "draw_threshold": draw_threshold,
        }
        model_path = MODELS_DIR / "football_stacking_v1.joblib"
        logger.info(f"  feature_names home : {len(list(X_h_tr.columns))} colonnes")
        joblib.dump(artifacts, model_path)
        mlflow.log_artifact(str(model_path))
        logger.success(f"  Modèle sauvegardé : {model_path}")

    logger.success("=== Train terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step",    type=int, default=2)
    parser.add_argument("--no-shap", action="store_true")
    parser.add_argument("--n-trials", type=int, default=50,
                    help="Nombre de trials Optuna par modèle")
    args = parser.parse_args()
    main(step=args.step, use_shap=not args.no_shap, n_trials=args.n_trials)