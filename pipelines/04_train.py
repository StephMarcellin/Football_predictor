"""
Pipeline 04 — Train
====================
Entraîne un modèle XGBoost multiclasse (H/D/A) sur features.ml_dataset.

Modes disponibles (config.yaml) :
  use_autoencoder : false → XGBoost direct sur les features brutes
  use_autoencoder : true  → Réduction dimensionnelle via auto-encodeur
                            avant XGBoost
  use_stacking    : true  → Stacking XGBoost + RandomForest → LogReg

Toutes les expériences sont loggées dans MLflow.

BLOC 1 — Charge features.ml_dataset, filtre sur les Matchweeks, vérifie la distribution H/D/A
BLOC 2 — Split chronologique propre sur date : saisons passées + début saison test → train, milieu → val, fin → test
BLOC 3 — Preprocessing : imputation médiane + RobustScaler + VarianceThreshold sur les numériques, OneHotEncoder sur rank_tier et kickoff_category, passthrough sur les colonnes rebellious
BLOC 4 — Auto-encodeur TensorFlow (activé via use_autoencoder: true dans config.yaml). Architecture reprise de ton notebook avec détection automatique des bypass features
BLOC 5 — XGBoost multi:softprob + BayesSearchCV sur GroupTimeSeriesSplit. Option --no-bayes pour aller vite
BLOC 6 — Stacking XGB + RF → LogReg (activé via use_stacking: true)
BLOC 7 — Calibration isotonique par classe (H, D, A séparément)
BLOC 8 — Évaluation complète + analyse paris avec delta prob_H - prob_A sur plusieurs seuils
BLOC 9 — MLflow : params, métriques, artifact du modèle



Usage :
    python pipelines/04_train.py
    python pipelines/04_train.py --no-bayes    # skip BayesSearchCV
"""

import os
import sys
import warnings
import argparse
import joblib
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yaml
import mlflow
import mlflow.sklearn
import mlflow.xgboost
from loguru import logger

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import VarianceThreshold
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, classification_report, confusion_matrix, log_loss
)
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    LabelEncoder, OneHotEncoder, RobustScaler
)
from sklearn.calibration import CalibratedClassifierCV, calibration_curve

from xgboost import XGBClassifier

warnings.filterwarnings("ignore")


# ── Config ────────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH          = CFG["paths"]["db"]
TRAIN_CFG        = CFG.get("train", {})
USE_AE           = TRAIN_CFG.get("use_autoencoder", False)
USE_STACKING     = TRAIN_CFG.get("use_stacking", False)
TEST_SEASON      = TRAIN_CFG.get("test_season", "2024-2025")
VAL_SPLIT_PCT    = TRAIN_CFG.get("val_split_pct", 0.5)
BOTTLENECK_DIM   = TRAIN_CFG.get("bottleneck_dim", 32)
BAYES_N_ITER     = TRAIN_CFG.get("bayes_n_iter", 50)
TARGET           = TRAIN_CFG.get("target", "result_1n2")
MLFLOW_URI       = CFG.get("mlflow", {}).get("tracking_uri", "mlruns")
MODELS_DIR       = Path(CFG["paths"].get("models", "models"))
MODELS_DIR.mkdir(exist_ok=True)

# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/train.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# Colonnes à exclure du modèle (identifiants, pas du signal)
DROP_COLS = [
    "team", "opponent", "date", "round", "day", "venue",
    "league", "season", "season_raw", "season_norm",
    "source_file", "game", "notes", "time", "match_report",
    "attendance", "captain", "formation", "opp_formation", "referee",
    "result",            # version W/D/L — on garde result_1n2
    "gf", "ga",          # buts du match = data leakage
    "is_matchweek",      # utilisé pour le filtrage uniquement
]

# Colonnes catégorielles à encoder
CAT_COLS = ["rank_tier", "kickoff_category"]

# Colonnes rebellious (contexte pur, pas de scaling)
REBELLIOUS_COLS = ["team_rank_pre", "pts_cum_pre", "gd_cum_pre",
                   "h2h_win_pct", "h2h_avg_gd", "h2h_avg_points",
                   "month", "home_days_since_last", "away_days_since_last",
                   "home_matches_14d", "away_matches_14d"]


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 1 — Chargement
# ══════════════════════════════════════════════════════════════════════════════

def load_data(con):
    logger.info("Chargement de features.ml_dataset...")
    df = con.execute("SELECT * FROM features.ml_dataset").df()
    df["date"] = pd.to_datetime(df["date"])

    # Filtrer sur les Matchweeks uniquement
    n_before = len(df)
    df = df[df["is_matchweek"] == 1].copy()
    logger.info(f"  {n_before:,} lignes → {len(df):,} Matchweeks")
    logger.info(f"  Colonnes : {len(df.columns)}")

    # Vérifier la cible
    if TARGET not in df.columns:
        raise ValueError(f"Colonne cible '{TARGET}' absente")

    dist = df[TARGET].value_counts().to_dict()
    logger.info(f"  Distribution cible : {dist}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 2 — Split chronologique
# ══════════════════════════════════════════════════════════════════════════════

def chronological_split(df):
    """
    Train  : toutes les saisons sauf la dernière
             + première moitié de la saison test
    Val    : milieu de la saison test (pour BayesSearchCV)
    Test   : fin de la saison test (évaluation finale)
    """
    logger.info(f"Split chronologique (saison test : {TEST_SEASON})...")

    mask_past    = df["season"] != TEST_SEASON
    mask_test_s  = df["season"] == TEST_SEASON

    df_test_season = df[mask_test_s].sort_values("date")
    n              = len(df_test_season)
    split_train    = int(n * (1 - VAL_SPLIT_PCT))
    split_val      = split_train + int(n * VAL_SPLIT_PCT / 2)

    idx_train_extra = df_test_season.index[:split_train]
    idx_val         = df_test_season.index[split_train:split_val]
    idx_test        = df_test_season.index[split_val:]

    df_train = pd.concat([df[mask_past], df.loc[idx_train_extra]])
    df_val   = df.loc[idx_val]
    df_test  = df.loc[idx_test]

    logger.info(f"  Train : {len(df_train):,} matchs")
    logger.info(f"  Val   : {len(df_val):,} matchs")
    logger.info(f"  Test  : {len(df_test):,} matchs")
    return df_train, df_val, df_test


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 3 — Preprocessing
# ══════════════════════════════════════════════════════════════════════════════

def prepare_xy(df):
    """Sépare X et y, retire les colonnes à exclure."""
    y = df[TARGET].copy()
    drop = [c for c in DROP_COLS + [TARGET] if c in df.columns]
    X = df.drop(columns=drop)
    return X, y


def build_preprocessor(X_train):
    """
    Construit le ColumnTransformer :
    - num   : imputation médiane + RobustScaler + VarianceThreshold
    - cat   : imputation mode + OneHotEncoder
    - raw   : passthrough (colonnes rebellious)
    """
    present_cat  = [c for c in CAT_COLS if c in X_train.columns]
    present_raw  = [c for c in REBELLIOUS_COLS if c in X_train.columns]
    num_cols     = [c for c in X_train.select_dtypes(include="number").columns
                    if c not in present_cat + present_raw]

    logger.info(f"  Numériques  : {len(num_cols)}")
    logger.info(f"  Catégories  : {present_cat}")
    logger.info(f"  Passthrough : {len(present_raw)}")

    num_pipe = Pipeline([
        ("imputer",  SimpleImputer(strategy="median")),
        ("variance", VarianceThreshold(threshold=0)),
        ("scaler",   RobustScaler()),
    ])
    cat_pipe = Pipeline([
        ("imputer",  SimpleImputer(strategy="most_frequent")),
        ("encoder",  OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer([
        ("num", num_pipe,       num_cols),
        ("cat", cat_pipe,       present_cat),
        ("raw", "passthrough",  present_raw),
    ], remainder="drop")

    return preprocessor, num_cols, present_cat, present_raw


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 4 — Auto-encodeur (optionnel)
# ══════════════════════════════════════════════════════════════════════════════

def build_autoencoder(input_dim, bottleneck_dim):
    """
    Auto-encodeur profond : input_dim → 512 → 256 → 128 → bottleneck → ...
    Repris de ton notebook avec quelques améliorations mineures.
    """
    import tensorflow as tf
    from tensorflow.keras.models import Model
    from tensorflow.keras.layers import (
        Input, Dense, Dropout, LeakyReLU, BatchNormalization
    )
    from tensorflow.keras.optimizers import Adam

    DROPOUT = 0.2
    SLOPE   = 0.01

    inp = Input(shape=(input_dim,), name="input_features")
    x   = inp

    # Encodeur
    for i, units in enumerate([512, 256, 128], 1):
        x = Dense(units, kernel_initializer="he_normal", name=f"enc_{i}")(x)
        x = BatchNormalization()(x)
        x = LeakyReLU(negative_slope=SLOPE)(x)
        x = Dropout(DROPOUT)(x)

    bottleneck = Dense(bottleneck_dim, activation="linear",
                       name="bottleneck")(x)
    bottleneck = BatchNormalization()(bottleneck)

    # Décodeur
    x = bottleneck
    for i, units in enumerate([128, 256, 512], 1):
        x = Dense(units, kernel_initializer="he_normal", name=f"dec_{i}")(x)
        x = BatchNormalization()(x)
        x = LeakyReLU(negative_slope=SLOPE)(x)
        x = Dropout(DROPOUT)(x)

    out = Dense(input_dim, activation="linear", name="output")(x)

    ae      = Model(inp, out,        name="autoencoder")
    encoder = Model(inp, bottleneck, name="encoder")
    ae.compile(optimizer=Adam(learning_rate=0.001), loss="mse")

    return ae, encoder


def apply_autoencoder(X_train_s, X_val_s, X_test_s, n_num):
    """
    Entraîne l'AE sur la partie numérique de X_train_s,
    puis encode les trois splits.
    Retourne les arrays encodés + indices des bypass features.
    """
    from tensorflow.keras.callbacks import EarlyStopping
    from sklearn.metrics import mean_squared_error

    logger.info("  Entraînement auto-encodeur...")
    X_ae_train = X_train_s[:, :n_num]
    X_ae_val   = X_val_s[:, :n_num]
    X_ae_test  = X_test_s[:, :n_num]

    ae, encoder = build_autoencoder(n_num, BOTTLENECK_DIM)

    ae.fit(
        X_ae_train, X_ae_train,
        validation_data=(X_ae_val, X_ae_val),
        epochs=50, batch_size=64, shuffle=True, verbose=0,
        callbacks=[EarlyStopping(monitor="val_loss", patience=10,
                                 restore_best_weights=True)]
    )

    # Diagnostic reconstruction
    recon      = ae.predict(X_ae_test, verbose=0)
    mse_feat   = np.mean((X_ae_test - recon) ** 2, axis=0)
    threshold  = mse_feat.mean() + 2 * mse_feat.std()
    bypass_idx = np.where(mse_feat > threshold)[0]
    logger.info(f"  Bottleneck : {BOTTLENECK_DIM} dims | Bypass : {len(bypass_idx)} features")

    # Encoder les données
    from sklearn.preprocessing import StandardScaler
    sc = StandardScaler()
    lat_train = sc.fit_transform(encoder.predict(X_ae_train, verbose=0))
    lat_val   = sc.transform(encoder.predict(X_ae_val,   verbose=0))
    lat_test  = sc.transform(encoder.predict(X_ae_test,  verbose=0))

    # Fusion : latent + bypass + reste (cat + raw)
    rest_train = X_train_s[:, n_num:]
    rest_val   = X_val_s[:, n_num:]
    rest_test  = X_test_s[:, n_num:]

    X_final_train = np.hstack([lat_train, X_ae_train[:, bypass_idx], rest_train])
    X_final_val   = np.hstack([lat_val,   X_ae_val[:, bypass_idx],   rest_val])
    X_final_test  = np.hstack([lat_test,  X_ae_test[:, bypass_idx],  rest_test])

    logger.info(f"  Dims finales : {X_final_train.shape[1]}")
    return X_final_train, X_final_val, X_final_test, encoder


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 5 — XGBoost multiclasse + BayesSearchCV
# ══════════════════════════════════════════════════════════════════════════════

def encode_target(y_train, y_val, y_test):
    """Encode H/D/A en entiers pour XGBoost."""
    le = LabelEncoder()
    y_train_enc = le.fit_transform(y_train)
    y_val_enc   = le.transform(y_val)
    y_test_enc  = le.transform(y_test)
    logger.info(f"  Classes : {dict(enumerate(le.classes_))}")
    return y_train_enc, y_val_enc, y_test_enc, le


def train_xgboost(X_train, y_train, X_val, y_val, use_bayes=True):
    """
    Entraîne XGBoost multiclasse.
    Si use_bayes=True : optimisation bayésienne des hyperparamètres.
    Sinon : hyperparamètres par défaut raisonnables.
    """
    n_classes = len(np.unique(y_train))

    xgb_base = XGBClassifier(
        objective="multi:softprob",
        num_class=n_classes,
        eval_metric="mlogloss",
        n_jobs=-1,
        early_stopping_rounds=50,
        random_state=42,
    )

    if not use_bayes:
        logger.info("  XGBoost avec hyperparamètres par défaut...")
        xgb_base.set_params(
            learning_rate=0.05,
            n_estimators=500,
            max_depth=4,
            subsample=0.8,
            colsample_bytree=0.6,
            reg_alpha=1.0,
            reg_lambda=1.0,
        )
        xgb_base.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
        return xgb_base, xgb_base.get_params()

    logger.info(f"  BayesSearchCV ({BAYES_N_ITER} itérations)...")
    try:
        from skopt import BayesSearchCV
        from skopt.space import Integer, Real
        from mlxtend.evaluate import GroupTimeSeriesSplit
    except ImportError:
        logger.warning("  skopt/mlxtend non disponible, fallback hyperparams par défaut")
        return train_xgboost(X_train, y_train, X_val, y_val, use_bayes=False)

    tscv = GroupTimeSeriesSplit(
        n_splits=3,
        test_size=len(X_train) // 4
    )
    groups = np.arange(len(X_train))

    param_space = {
        "learning_rate":    Real(0.01, 0.1, prior="log-uniform"),
        "n_estimators":     Integer(300, 800),
        "max_depth":        Integer(3, 5),
        "reg_alpha":        Real(0.1, 10, prior="log-uniform"),
        "reg_lambda":       Real(0.1, 10, prior="log-uniform"),
        "subsample":        Real(0.7, 1.0, prior="uniform"),
        "colsample_bytree": Real(0.4, 0.8, prior="uniform"),
    }

    search = BayesSearchCV(
        xgb_base,
        search_spaces=param_space,
        n_iter=BAYES_N_ITER,
        cv=tscv,
        scoring="neg_log_loss",
        random_state=42,
        verbose=0,
        n_jobs=-1,
    )
    search.fit(
        X_train, y_train,
        groups=groups,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )
    logger.info(f"  Best score : {search.best_score_:.4f}")
    logger.info(f"  Best params : {search.best_params_}")
    return search.best_estimator_, search.best_params_


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 6 — Stacking (optionnel)
# ══════════════════════════════════════════════════════════════════════════════

def train_stacking(xgb_model, X_train, y_train, X_test):
    """
    Stacking simple : XGBoost + RandomForest → LogisticRegression.
    Utilise les prédictions OOF pour entraîner le méta-modèle.
    """
    logger.info("  Stacking XGBoost + RandomForest...")
    rf = RandomForestClassifier(n_estimators=100, max_depth=4,
                                n_jobs=-1, random_state=42)
    n_classes = len(np.unique(y_train))
    kf = GroupKFold(n_splits=3)
    groups = np.arange(len(X_train))

    oof_xgb = np.zeros((len(X_train), n_classes))
    oof_rf  = np.zeros((len(X_train), n_classes))

    for tr_idx, val_idx in kf.split(X_train, y_train, groups):
        X_f_tr, X_f_val = X_train[tr_idx], X_train[val_idx]
        y_f_tr          = y_train[tr_idx]
        xgb_model.fit(X_f_tr, y_f_tr)
        rf.fit(X_f_tr, y_f_tr)
        oof_xgb[val_idx] = xgb_model.predict_proba(X_f_val)
        oof_rf[val_idx]  = rf.predict_proba(X_f_val)

    X_meta = np.hstack([oof_xgb, oof_rf])
    meta   = LogisticRegression(max_iter=1000, random_state=42)
    meta.fit(X_meta, y_train)

    # Entraînement final sur tout le train
    xgb_model.fit(X_train, y_train)
    rf.fit(X_train, y_train)

    def predict_stack(X):
        p_xgb = xgb_model.predict_proba(X)
        p_rf  = rf.predict_proba(X)
        return meta.predict_proba(np.hstack([p_xgb, p_rf]))

    logger.info(f"  Poids méta : XGB={meta.coef_[0][:n_classes].mean():.3f}")
    return predict_stack


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 7 — Calibration isotonique
# ══════════════════════════════════════════════════════════════════════════════

def calibrate_model(model, X_val, y_val):
    """Calibration isotonique des probabilités sur le set de validation."""
    logger.info("  Calibration isotonique...")
    probs  = model.predict_proba(X_val)
    n_cl   = probs.shape[1]
    calibrators = []
    from sklearn.calibration import IsotonicRegression
    for c in range(n_cl):
        cal = IsotonicRegression(out_of_bounds="clip")
        cal.fit(probs[:, c], (y_val == c).astype(int))
        calibrators.append(cal)

    def predict_calibrated(X):
        raw = model.predict_proba(X)
        cal_probs = np.column_stack([
            calibrators[c].transform(raw[:, c]) for c in range(n_cl)
        ])
        # Renormaliser pour que les probas somment à 1
        row_sums = cal_probs.sum(axis=1, keepdims=True)
        return cal_probs / np.where(row_sums == 0, 1, row_sums)

    return predict_calibrated, calibrators


# ══════════════════════════════════════════════════════════════════════════════
# BLOC 8 — Évaluation
# ══════════════════════════════════════════════════════════════════════════════

def evaluate(y_true, y_pred, y_proba, label_encoder, split_name="Test"):
    """Rapport complet : classification, confusion, log loss."""
    class_names = list(label_encoder.classes_)
    acc  = accuracy_score(y_true, y_pred)
    loss = log_loss(y_true, y_proba)

    logger.info(f"── Évaluation {split_name} ──────────────────────────────")
    logger.info(f"  Accuracy  : {acc:.4f}")
    logger.info(f"  Log Loss  : {loss:.4f}")
    logger.info("\n" + classification_report(
        y_true, y_pred, target_names=class_names, zero_division=0
    ))

    cm = confusion_matrix(y_true, y_pred)
    logger.info(f"  Confusion matrix :\n{cm}")

    return {"accuracy": acc, "log_loss": loss}


def analyze_bets(y_proba, le, df_test, threshold=0.10):
    """
    Analyse paris style delta prob_H - prob_A.
    Retourne un DataFrame avec prob_H, prob_A, prob_D, delta et résultat réel.
    """
    classes = list(le.classes_)
    idx_H   = classes.index("H") if "H" in classes else None
    idx_A   = classes.index("A") if "A" in classes else None
    idx_D   = classes.index("D") if "D" in classes else None

    if idx_H is None or idx_A is None:
        logger.warning("  Classes H/A non trouvées pour l'analyse paris")
        return pd.DataFrame()

    df_bets = pd.DataFrame({
        "team":     df_test["team"].values,
        "opponent": df_test["opponent"].values,
        "date":     df_test["date"].values,
        "league":   df_test["league_source"].values,
        "prob_H":   y_proba[:, idx_H],
        "prob_D":   y_proba[:, idx_D] if idx_D is not None else 0,
        "prob_A":   y_proba[:, idx_A],
        "result":   df_test[TARGET].values,
    })

    df_bets["delta"]     = df_bets["prob_H"] - df_bets["prob_A"]
    df_bets["pred"]      = np.select(
        [df_bets["delta"] > threshold, df_bets["delta"] < -threshold],
        ["H", "A"], default="D"
    )
    df_bets["correct"]   = (df_bets["pred"] == df_bets["result"]).astype(int)

    logger.info("── Analyse paris ────────────────────────────────────────")
    for t in [0.05, 0.10, 0.15, 0.20]:
        mask = df_bets["delta"].abs() > t
        n    = mask.sum()
        if n > 0:
            acc = df_bets.loc[mask, "correct"].mean()
            logger.info(f"  Delta > {t:.2f} : {n:3d} paris | Précision = {acc:.2%}")

    return df_bets


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main(use_bayes=True):
    logger.info("=== Démarrage train ===")
    logger.info(f"  Mode AE       : {USE_AE}")
    logger.info(f"  Mode Stacking : {USE_STACKING}")
    logger.info(f"  BayesSearchCV : {use_bayes}")

    con = duckdb.connect(DB_PATH)

    # ── Données ───────────────────────────────────────────────────────────────
    df = load_data(con)
    con.close()

    df_train, df_val, df_test = chronological_split(df)

    X_train, y_train = prepare_xy(df_train)
    X_val,   y_val   = prepare_xy(df_val)
    X_test,  y_test  = prepare_xy(df_test)

    # ── Preprocessing ─────────────────────────────────────────────────────────
    logger.info("Preprocessing...")
    preprocessor, num_cols, cat_cols, raw_cols = build_preprocessor(X_train)

    X_train_s = preprocessor.fit_transform(X_train)
    X_val_s   = preprocessor.transform(X_val)
    X_test_s  = preprocessor.transform(X_test)

    # Nombre de features numériques après VarianceThreshold
    n_num = preprocessor.named_transformers_["num"].transform(
        X_train[num_cols].fillna(0)
    ).shape[1]

    # ── Auto-encodeur (optionnel) ─────────────────────────────────────────────
    if USE_AE:
        logger.info("Auto-encodeur...")
        X_train_s, X_val_s, X_test_s, ae_encoder = apply_autoencoder(
            X_train_s, X_val_s, X_test_s, n_num
        )

    # ── Encodage cible ────────────────────────────────────────────────────────
    y_train_enc, y_val_enc, y_test_enc, le = encode_target(
        y_train, y_val, y_test
    )

    # ── MLflow ────────────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("football_1N2")

    run_name = f"XGB_{'AE_' if USE_AE else ''}{'Stack_' if USE_STACKING else ''}multiclass"

    with mlflow.start_run(run_name=run_name):
        mlflow.log_param("use_autoencoder", USE_AE)
        mlflow.log_param("use_stacking",    USE_STACKING)
        mlflow.log_param("test_season",     TEST_SEASON)
        mlflow.log_param("train_size",      len(X_train_s))
        mlflow.log_param("val_size",        len(X_val_s))
        mlflow.log_param("test_size",       len(X_test_s))
        mlflow.log_param("n_features",      X_train_s.shape[1])

        # ── XGBoost ───────────────────────────────────────────────────────────
        logger.info("Entraînement XGBoost...")
        xgb_model, best_params = train_xgboost(
            X_train_s, y_train_enc,
            X_val_s,   y_val_enc,
            use_bayes=use_bayes
        )
        mlflow.log_params({f"xgb_{k}": v for k, v in best_params.items()
                           if isinstance(v, (int, float, str))})

        # ── Stacking (optionnel) ───────────────────────────────────────────────
        if USE_STACKING:
            logger.info("Stacking...")
            predict_fn = train_stacking(xgb_model, X_train_s, y_train_enc, X_test_s)
        else:
            predict_fn = xgb_model.predict_proba

        # ── Calibration ───────────────────────────────────────────────────────
        logger.info("Calibration...")

        class _ProbWrapper:
            """Wrapper pour que calibrate_model accepte notre predict_fn."""
            def predict_proba(self, X):
                return predict_fn(X)

        wrapper = _ProbWrapper()
        predict_calibrated, calibrators = calibrate_model(
            wrapper, X_val_s, y_val_enc
        )

        # ── Évaluation ────────────────────────────────────────────────────────
        logger.info("Évaluation...")

        # Sur le set de validation
        proba_val   = predict_calibrated(X_val_s)
        pred_val    = le.inverse_transform(np.argmax(proba_val, axis=1))
        metrics_val = evaluate(y_val, pred_val, proba_val, le, "Val")
        mlflow.log_metrics({f"val_{k}": v for k, v in metrics_val.items()})

        # Sur le set de test
        proba_test   = predict_calibrated(X_test_s)
        pred_test    = le.inverse_transform(np.argmax(proba_test, axis=1))
        metrics_test = evaluate(y_test, pred_test, proba_test, le, "Test")
        mlflow.log_metrics({f"test_{k}": v for k, v in metrics_test.items()})

        # Analyse paris
        analyze_bets(proba_test, le, df_test)

        # ── Sauvegarde ────────────────────────────────────────────────────────
        logger.info("Sauvegarde du modèle...")
        artifacts = {
            "preprocessor": preprocessor,
            "label_encoder": le,
            "xgb_model":    xgb_model,
            "calibrators":  calibrators,
        }
        if USE_AE:
            artifacts["ae_encoder"] = ae_encoder
        if USE_STACKING:
            artifacts["predict_fn"] = predict_fn

        model_path = MODELS_DIR / f"{run_name}.joblib"
        joblib.dump(artifacts, model_path)
        mlflow.log_artifact(str(model_path))
        logger.success(f"  Modèle sauvegardé : {model_path}")

    logger.success("=== Train terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-bayes", action="store_true",
                        help="Désactiver BayesSearchCV (plus rapide)")
    args = parser.parse_args()
    main(use_bayes=not args.no_bayes)