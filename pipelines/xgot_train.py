"""
xGOT — entraînement du modèle Post-Shot xG
==========================================
Modèle AUXILIAIRE (pré-modèle) : prédit P(but | tir cadré + placement), i.e. le
xGOT. Sa sortie alimentera int_keeper_psxg (shot-stopping du gardien).

Données  : machine_learning.xgot_training (vue dbt, features + label).
Modèle   : LightGBM + calibration isotonique (mêmes briques que 04_train.py).
Split    : saisons (config.yaml → TRAIN/VAL/TEST_SEASONS), anti-fuite temporelle.

DEUX BARRIÈRES DE SÉCURITÉ (cf. discussion gouvernance) :
  1. Barrière de COUVERTURE (avant entraînement) : on refuse d'entraîner si les
     saisons train+val n'ont pas une couverture placement suffisante ET équilibrée
     entre buts/arrêts. Protège contre un entraînement sur données incomplètes.
  2. Barrière de CALIBRATION (avant promotion) : on ne marque pas le modèle comme
     bon si sa calibration hors-échantillon dépasse un seuil de Brier/logloss.

Usage :
    python pipelines/xgot_train.py
    python pipelines/xgot_train.py --dry-run        # barrière + rapport, sans train
"""

import sys
import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

import duckdb
import numpy as np
import pandas as pd
import yaml
import joblib
import mlflow
import mlflow.lightgbm
from loguru import logger

import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss, brier_score_loss
from sklearn.calibration import calibration_curve

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODELS_DIR = ROOT_DIR / CFG["paths"].get("models", "models")
DIAG_DIR   = MODELS_DIR / "diagnostics"
MLFLOW_URI = CFG["mlflow"]["tracking_uri"]

TRAIN_SEASONS = CFG["train"]["TRAIN_SEASONS"]
VAL_SEASONS   = CFG["train"]["VAL_SEASONS"]
TEST_SEASON   = CFG["train"]["TEST_SEASON"]

# Seuils des barrières (paramètres, pas des constantes magiques)
MIN_COVERAGE      = 0.95   # couverture placement minimale par saison train+val
MAX_CLASS_GAP     = 0.10   # écart max de couverture entre buts et arrêts
MAX_BRIER_OOS     = 0.16   # Brier hors-échantillon max pour valider (base ~0.16 à 20%)

FEATURES_NUM = [
    "offset_center", "height", "corner_dist",
    "x", "y", "shot_distance_m", "shot_angle_rad",
]
FEATURE_CAT = "placement_zone"   # 9 zones du cadre → one-hot


# ── Barrière 1 : couverture des données ───────────────────────────────────────
def coverage_barrier(con, seasons) -> bool:
    """
    Vérifie que chaque saison de `seasons` a une couverture placement >= MIN_COVERAGE
    ET un écart de couverture buts/arrêts <= MAX_CLASS_GAP. Retourne True si OK.
    Interroge int_shot_placement (source de vérité de la couverture).
    """
    q = """
        SELECT season,
            CASE WHEN is_goal THEN 'but' ELSE 'arret' END AS classe,
            AVG((goal_mouth_y IS NOT NULL)::INT) AS cov
        FROM intermediate.int_shot_placement
        WHERE is_on_target AND season IN ({})
        GROUP BY 1, 2
    """.format(",".join("?" * len(seasons)))
    cov = con.execute(q, seasons).df()

    ok = True
    for season in seasons:
        s = cov[cov.season == season]
        cov_but = float(s[s.classe == "but"]["cov"].iloc[0]) if (s.classe == "but").any() else 0.0
        cov_ar  = float(s[s.classe == "arret"]["cov"].iloc[0]) if (s.classe == "arret").any() else 0.0
        gap = abs(cov_but - cov_ar)
        status = "OK" if (cov_but >= MIN_COVERAGE and cov_ar >= MIN_COVERAGE and gap <= MAX_CLASS_GAP) else "KO"
        if status == "KO":
            ok = False
        logger.info(f"[couverture] {season} : buts={cov_but:.1%} arrets={cov_ar:.1%} gap={gap:.1%} → {status}")
    return ok


def build_X(df: pd.DataFrame) -> pd.DataFrame:
    """Matrice de features X : 7 numériques + one-hot de placement_zone.
    Partagée par l'entraînement (prepare) ET le scoring (xgot_score.py) →
    features identiques des deux côtés (anti train/serve skew)."""
    X = df[FEATURES_NUM].copy()
    zone = pd.get_dummies(df[FEATURE_CAT], prefix="zone")
    return pd.concat([X, zone], axis=1)


def prepare(df: pd.DataFrame):
    """Matrice X (via build_X) et cible y."""
    X = build_X(df)
    y = df["label"].astype(int).values
    return X, y


# ── Barrière 2 : calibration ──────────────────────────────────────────────────
def calibration_ok(y_true, p_pred) -> bool:
    brier = brier_score_loss(y_true, p_pred)
    logger.info(f"[calibration] Brier hors-échantillon = {brier:.4f} (seuil {MAX_BRIER_OOS})")
    return brier <= MAX_BRIER_OOS


def plot_calibration(y_true, p_pred, path):
    frac_pos, mean_pred = calibration_curve(y_true, p_pred, n_bins=10, strategy="quantile")
    plt.figure(figsize=(5, 5))
    plt.plot([0, 1], [0, 1], "--", color="grey", label="parfait")
    plt.plot(mean_pred, frac_pos, "o-", label="xGOT")
    plt.xlabel("xGOT prédit"); plt.ylabel("taux de but observé")
    plt.title("Calibration xGOT (hors-échantillon)"); plt.legend()
    plt.tight_layout(); plt.savefig(path, dpi=120); plt.close()


# ── Entraînement ──────────────────────────────────────────────────────────────
def run(dry_run: bool = False):
    MODELS_DIR.mkdir(exist_ok=True)
    DIAG_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Barrière 1 : couverture sur les saisons train + val
    logger.info("=== Barrière de couverture ===")
    if not coverage_barrier(con, TRAIN_SEASONS + VAL_SEASONS):
        logger.error("Couverture insuffisante/biaisée → entraînement ANNULÉ. "
                     "Attendre que le re-scrape complète les saisons concernées.")
        con.close()
        sys.exit(1)
    logger.success("Couverture OK.")

    if dry_run:
        logger.info("--dry-run : barrière passée, on s'arrête avant l'entraînement.")
        con.close()
        return

    # Chargement du dataset
    df = con.execute("SELECT * FROM machine_learning.xgot_training").df()
    con.close()

    train = df[df.season.isin(TRAIN_SEASONS)]
    val   = df[df.season.isin(VAL_SEASONS)]
    test  = df[df.season == TEST_SEASON]
    logger.info(f"train={len(train)}  val={len(val)}  test={len(test)}")

    X_tr, y_tr = prepare(train)
    X_va, y_va = prepare(val)
    X_te, y_te = prepare(test)
    # Aligne les colonnes one-hot (zones absentes d'un split)
    X_va = X_va.reindex(columns=X_tr.columns, fill_value=0)
    X_te = X_te.reindex(columns=X_tr.columns, fill_value=0)

    if "dagshub" in str(MLFLOW_URI):
        os.environ["MLFLOW_TRACKING_USERNAME"] = os.getenv("DAGSHUB_USERNAME", "")
        os.environ["MLFLOW_TRACKING_PASSWORD"] = os.getenv("DAGSHUB_TOKEN", "")
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("xgot")

    with mlflow.start_run(run_name="xgot_lgbm_isotonic"):
        mlflow.log_params({
            "min_coverage": MIN_COVERAGE, "n_train": len(train), "n_val": len(val),
            "features": ",".join(FEATURES_NUM) + f",{FEATURE_CAT}",
        })

        # LightGBM (probas brutes)
        clf = lgb.LGBMClassifier(
            n_estimators=400, learning_rate=0.03, num_leaves=31,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
        )
        clf.fit(X_tr, y_tr)

        # Calibration isotonique sur la validation
        p_va_raw = clf.predict_proba(X_va)[:, 1]
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(p_va_raw, y_va)

        # Évaluation hors-échantillon (test)
        p_te = iso.predict(clf.predict_proba(X_te)[:, 1])
        base_rate = y_tr.mean()
        metrics = {
            "logloss": log_loss(y_te, p_te),
            "logloss_baseline": log_loss(y_te, np.full_like(y_te, base_rate, dtype=float)),
            "brier": brier_score_loss(y_te, p_te),
            "sum_xgot": float(p_te.sum()), "buts_reels": int(y_te.sum()),
        }
        mlflow.log_metrics(metrics)
        for k, v in metrics.items():
            logger.info(f"[metric] {k} = {v}")

        # Diagnostics + barrière 2
        cal_path = DIAG_DIR / "xgot_calibration.png"
        plot_calibration(y_te, p_te, cal_path)
        mlflow.log_artifact(str(cal_path))

        promotable = (
            calibration_ok(y_te, p_te)
            and metrics["logloss"] < metrics["logloss_baseline"]   # bat la baseline
        )
        mlflow.log_param("promotable", promotable)

        if promotable:
            joblib.dump({"model": clf, "calibrator": iso, "columns": list(X_tr.columns)},
                        MODELS_DIR / "xgot.joblib")
            mlflow.lightgbm.log_model(clf, "xgot_lgbm")
            logger.success("xGOT validé et sauvegardé (models/xgot.joblib).")
        else:
            logger.warning("xGOT NON promu (calibration/baseline insuffisantes). "
                          "Modèle non sauvegardé — l'aval n'est pas alimenté.")


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/xgot_train.log", level="DEBUG", rotation="5 MB", retention=10,
               encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    parser = argparse.ArgumentParser(description="Entraînement du modèle xGOT")
    parser.add_argument("--dry-run", action="store_true",
                        help="Vérifie la barrière de couverture puis s'arrête (sans entraîner)")
    args = parser.parse_args()

    run(dry_run=args.dry_run)
