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
import warnings
import joblib
from pathlib import Path

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

DB_PATH    = CFG["paths"]["duckdb"]
MODELS_DIR = Path(CFG["paths"].get("models", "models"))
MODELS_DIR.mkdir(exist_ok=True)
MLFLOW_URI = CFG.get("mlflow", {}).get("tracking_uri", "mlruns")
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

TRAIN_SEASONS = ["2017-2018",
  "2018-2019",
  "2019-2020",
  "2020-2021",
  "2021-2022",
  "2022-2023"
]
VAL_SEASON    = "2023-2024"

# Colonnes identifiants — jamais dans le modèle
ID_COLS = [
    "date", "team", "opponent", "venue", "season", "league_source",
    "comp_category", "match_id", "final_match_id", "result_1n2",
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
    "sterility_index_5", "sterility_diff",
    "press_resistance_5", "press_resistance_diff",
    "shield_efficiency_5", "shield_efficiency_diff",
    "xg_overperformance_5", "xg_opi_diff",
    "fouls_per_tackle_roll_5",
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
          AND result_1n2    IS NOT NULL
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
      - On joint sur final_match_id en séparant home_ et away_

    Le résultat contient :
      - Toutes les features Home préfixées h_
      - Toutes les features Away préfixées a_
      - result_match : H/D/A du point de vue domicile
    """
    logger.info("Pivot home/away → 1 ligne par match...")

    id_cols_pivot = ["final_match_id", "date", "season",
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

    home_renamed = home_renamed.rename(
        columns={c: f"h_{c}" for c in feature_cols}
    ).rename(columns={"team": "home_team", "opponent": "away_team",
                       TARGET: "result_match"})

    away_renamed = away_renamed.rename(
        columns={c: f"a_{c}" for c in feature_cols}
    ).rename(columns={"team": "away_team_check", "opponent": "home_team_check"})

    # Jointure sur final_match_id
    merged = home_renamed.merge(
        away_renamed[["final_match_id"] +
                     [f"a_{c}" for c in feature_cols]],
        on="final_match_id",
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


def split_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split temporel par saison."""
    train = df[df["season"].isin(TRAIN_SEASONS)].copy()
    val   = df[df["season"] == VAL_SEASON].copy()
    logger.info(f"  Train : {len(train):,} | Val : {len(val):,}")
    return train, val


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
            "is_unbalance":     True,
            "learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
            "max_depth":        trial.suggest_int("max_depth", 3, 6),
            "num_leaves":       trial.suggest_int("num_leaves", 15, 63),
            "min_child_samples":trial.suggest_int("min_child_samples", 10, 50),
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

    return np.hstack(parts)  # shape (n, 12)


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
    """Stage 2 : LogReg méta sur les probabilités Stage 1."""
    logger.info("── Stage 2 : Méta-modèle LogReg ─────────────────────────")
    meta = LogisticRegression(
    max_iter=2000,
    C=0.1,
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
    """Calibration isotonique par classe sur le val set."""
    logger.info("── Calibration isotonique ───────────────────────────────")
    probs = model.predict_proba(X_val)
    n_cl  = probs.shape[1]

    calibrators = []
    for c in range(n_cl):
        cal = IsotonicRegression(out_of_bounds="clip")
        cal.fit(probs[:, c], (y_val_enc == c).astype(int))
        calibrators.append(cal)

    def predict_calibrated(X):
        raw = model.predict_proba(X)
        cal_probs = np.column_stack([
            calibrators[c].transform(raw[:, c]) for c in range(n_cl)
        ])
        row_sums = cal_probs.sum(axis=1, keepdims=True)
        return cal_probs / np.where(row_sums == 0, 1, row_sums)

    return predict_calibrated, calibrators


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

def evaluate_full(y_true_enc, y_proba, le, label: str) -> dict:
    """Log Loss + Accuracy + Brier Score + rapport par classe."""
    y_pred = predict_with_draw_boost(y_proba, le)
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
                    df_match: pd.DataFrame) -> pd.DataFrame:
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
        "final_match_id": df_match["final_match_id"].values,
        "home_team":      df_match["home_team"].values,
        "away_team":      df_match["away_team"].values,
        "date":           df_match["date"].values,
        "league":         df_match["league_source"].values,
        "prob_home":      y_proba[:, idx_H],
        "prob_draw":      y_proba[:, idx_D],
        "prob_away":      y_proba[:, idx_A],
        "actual_result":  le.inverse_transform(y_true_enc),
        "pred":           le.inverse_transform(np.argmax(y_proba, axis=1)),
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
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main(step: int = 2, use_shap: bool = True, n_trials: int = 50):
    logger.info("=== Démarrage train Two-Stage Stacking ===")

    # ── Données ───────────────────────────────────────────────────────────────
    df      = load_data()
    df_match = pivot_to_match(df)

    df_train, df_val = split_data(df_match)

    # Winsorisation — caps sur train uniquement
    df_train, caps = winsorize(df_train, fit=True)
    df_val,   _    = winsorize(df_val,   caps=caps, fit=False)

    # ── Préparation features ──────────────────────────────────────────────────

    # Perspective Home
    X_h_tr, y_h_tr = prepare_perspective(df_train, "home")
    X_h_val, y_h_val = prepare_perspective(df_val, "home")

    # Perspective Away
    X_a_tr, y_a_tr = prepare_perspective(df_train, "away")
    X_a_val, y_a_val = prepare_perspective(df_val, "away")

    # Combined (pour LR baseline + Stage 2)
    X_c_tr, y_c_tr = prepare_combined(df_train)
    X_c_val, y_c_val = prepare_combined(df_val)

    # ── Encodage cible ────────────────────────────────────────────────────────
    y_h_tr_enc, y_h_val_enc, le_home     = encode_labels(y_h_tr, y_h_val)
    y_a_tr_enc, y_a_val_enc, le_away     = encode_labels(y_a_tr, y_a_val)
    y_c_tr_enc, y_c_val_enc, le_combined = encode_labels(y_c_tr, y_c_val)

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
    mlflow.set_experiment("football_1N2_stacking")

    with mlflow.start_run(run_name="TwoStage_Stacking_v1"):
        mlflow.log_params({
            "train_seasons": str(TRAIN_SEASONS),
            "val_season":    VAL_SEASON,
            "train_matches": len(df_train),
            "val_matches":   len(df_val),
        })

        # ── Stage 1 ───────────────────────────────────────────────────────────
        logger.info("══ Stage 1 ══════════════════════════════════════════════")

        lgbm_home = tune_lgbm_perspective(
            X_h_tr_s, y_h_tr_enc, X_h_val_s, y_h_val_enc,
            le_home, "LGBM Home", n_trials=args.n_trials
        )
        lgbm_away = tune_lgbm_perspective(
            X_a_tr_s, y_a_tr_enc, X_a_val_s, y_a_val_enc,
            le_away, "LGBM Away", n_trials=args.n_trials
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
        predict_cal, calibrators = calibrate(
            meta_model, X_meta_val, y_c_val_enc, le_combined
        )

        # ── Évaluation finale ─────────────────────────────────────────────────
        proba_cal = predict_cal(X_meta_val)
        metrics   = evaluate_full(
            y_c_val_enc, proba_cal, le_combined, "Stage 2 Calibré"
        )
        mlflow.log_metrics(metrics)

        # Évaluation Stage 1 seul pour comparaison
        proba_meta_raw = meta_model.predict_proba(X_meta_val)
        metrics_raw    = evaluate_full(
            y_c_val_enc, proba_meta_raw, le_combined, "Stage 2 Non-calibré"
        )
        mlflow.log_metrics({f"raw_{k}": v for k, v in metrics_raw.items()})

        # ── Analyse upsets ────────────────────────────────────────────────────
        df_results = analyze_upsets(
            proba_cal, y_c_val_enc, le_combined, df_val
        )

        # Export résultats
        results_path = MODELS_DIR / "predictions_val.csv"
        df_results.to_csv(results_path, index=False)
        mlflow.log_artifact(str(results_path))
        logger.info(f"  Prédictions sauvegardées : {results_path}")

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
                "home":     list(X_h_tr.columns),
                "away":     list(X_a_tr.columns),
                "combined": list(X_c_tr.columns),
            },
        }
        model_path = MODELS_DIR / "football_stacking_v1.joblib"
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