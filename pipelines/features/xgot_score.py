"""
xgot_score.py — Scoring du modèle xGOT
======================================
Applique models/xgot.joblib à TOUS les tirs cadrés éligibles (vue
machine_learning.xgot_features) et écrit le xGOT par tir dans la table
machine_learning.xgot_predictions.

C'est le « predict » du modèle auxiliaire xGOT — pendant de 05_predict.py pour le
modèle 1N2 — consommé ensuite par int_keeper_psxg (shot-stopping du gardien).

Anti train/serve skew : les features sont construites via build_X importé de
xgot_train (donc identiques à l'entraînement), puis alignées sur les colonnes
mémorisées dans le .joblib.

Usage :
    python pipelines/xgot_score.py
"""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import sys
from pathlib import Path

from dotenv import load_dotenv
import duckdb
import joblib
import yaml
from loguru import logger

# Même construction de features qu'à l'entraînement (fonction partagée).
from xgot_train import build_X


# ── Config (même patron que xgot_train.py) ────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODELS_DIR = ROOT_DIR / CFG["paths"].get("models", "models")
MODEL_PATH = MODELS_DIR / "xgot.joblib"

PRED_SCHEMA = "machine_learning"
PRED_TABLE  = "xgot_predictions"


def main():
    # 1. Charger le modèle promu : modèle LightGBM + calibrateur + colonnes d'entraînement
    if not MODEL_PATH.exists():
        logger.error(f"{MODEL_PATH} introuvable — lance d'abord xgot_train.py.")
        sys.exit(1)
    art = joblib.load(MODEL_PATH)
    model, calibrator, columns = art["model"], art["calibrator"], art["columns"]
    logger.info(f"Modèle chargé ({len(columns)} colonnes de features).")

    # 2. Charger les tirs à scorer (lecture seule)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(f"SELECT * FROM {PRED_SCHEMA}.xgot_features").df()
    con.close()
    logger.info(f"{len(df):,} tirs cadrés éligibles à scorer.")

    # 3. Features identiques à l'entraînement, alignées sur les colonnes du modèle.
    #    reindex : ajoute à 0 une zone absente, ignore une colonne en trop → shape garantie.
    X = build_X(df).reindex(columns=columns, fill_value=0)

    # 4. xGOT = proba LightGBM brute → calibration isotonique (mêmes briques qu'au train)
    p_raw = model.predict_proba(X)[:, 1]
    xgot  = calibrator.predict(p_raw)

    # 5. Résultat au grain (match_id, row_num) — clé de jointure pour int_keeper_psxg
    out = df[["match_id", "row_num", "season", "is_goal"]].copy()
    out["xgot"] = xgot

    # 6. Écrire la table (idempotent : CREATE OR REPLACE, connexion en écriture)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {PRED_SCHEMA}")
    con.register("out_df", out)
    con.execute(f"CREATE OR REPLACE TABLE {PRED_SCHEMA}.{PRED_TABLE} AS SELECT * FROM out_df")
    n = con.execute(f"SELECT COUNT(*) FROM {PRED_SCHEMA}.{PRED_TABLE}").fetchone()[0]
    con.close()

    logger.success(
        f"{PRED_SCHEMA}.{PRED_TABLE} écrite : {n:,} tirs — "
        f"Σ xGOT = {xgot.sum():.1f} pour {int(out['is_goal'].sum())} buts réels."
    )


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/xgot_score.log", level="DEBUG", rotation="5 MB", retention=10,
               encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")
    main()
