"""
predict_matches.py — Prédit les matchs de prediction.match_resolved et alimente prediction.match_predicted.

Flux : prediction.match_to_predict (saisi) → prediction.match_resolved (vue dbt : noms → match_id)
       → CE SCRIPT : lit les lignes du mart pour les matchs `resolu`, prédit, écrit match_predicted.

Pour chaque match résolu, on prédit sur la ligne de l'équipe À DOMICILE (comme le backtest) : le label du
mart est fixé côté domicile (H = l'équipe à domicile gagne), donc P(H), P(D), P(A) se lisent directement.
Les matchs non résolus sont aussi écrits (probabilités vides + motif), pour voir tout ce qui a été demandé.

Table de sortie prediction.match_predicted (créée si absente) — une ligne par match demandé et par
(modèle, version de modèle) : relancer avec le même modèle remplace ses lignes, un nouveau modèle ajoute les siennes.

Lancement (depuis la racine du projet, base libre — aucune autre connexion en écriture) :
  python pipelines/prediction/predict_matches.py
  python pipelines/prediction/predict_matches.py --model resultat_1n2
"""
# --- bootstrap : rend les modules partagés (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
_P = str(_Path(__file__).resolve().parents[1])
if _P not in _sys.path:
    _sys.path.insert(0, _P)
# ----------------------------------------------------------------------------
import argparse
from datetime import datetime

import duckdb
import joblib
import pandas as pd

import ml_common as mc

MODEL_KEY = "resultat_1n2_toutes_saisons"      # modèle 1 : avant compo, toutes saisons

DDL = """
CREATE SCHEMA IF NOT EXISTS prediction;
CREATE TABLE IF NOT EXISTS prediction.match_predicted (
    dt_date                DATE,
    str_home_team_name     VARCHAR,
    str_away_team_name     VARCHAR,
    str_match_id           VARCHAR,
    str_home_team_id       VARCHAR,
    str_away_team_id       VARCHAR,
    str_status             VARCHAR,
    str_message            VARCHAR,
    str_model_key          VARCHAR,
    str_model_version      VARCHAR,
    dt_predicted_at        TIMESTAMP,
    dec_p_home             DOUBLE,
    dec_p_draw             DOUBLE,
    dec_p_away             DOUBLE,
    str_predicted_outcome  VARCHAR
)
"""
COLS = ["dt_date", "str_home_team_name", "str_away_team_name", "str_match_id", "str_home_team_id",
        "str_away_team_id", "str_status", "str_message", "str_model_key", "str_model_version",
        "dt_predicted_at", "dec_p_home", "dec_p_draw", "dec_p_away", "str_predicted_outcome"]


def model_version(model_key):
    """Version = date de modification du fichier .joblib (change à chaque réentraînement)."""
    ts = (mc.MODELS_DIR / f"{model_key}.joblib").stat().st_mtime
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def read_resolved(db_path):
    """Lit prediction.match_resolved (vue dbt) en lecture seule."""
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute("select * from prediction.match_resolved").df()
    con.close()
    return df


def predict_resolved(cfg, models, model_key, resolved):
    """Probabilités H/D/A pour les matchs au statut `resolu` (ligne domicile du mart).
    Retourne resolved enrichi des colonnes de probabilités ; le statut devient `ligne_mart_absente`
    si le mart n'a pas la ligne domicile du match."""
    spec = models[model_key]
    payload = joblib.load(mc.MODELS_DIR / f"{model_key}.joblib")
    model, feats, label_map = payload["model"], payload["features"], payload["label_map"]

    mart = mc.load_mart(cfg, spec["mart"])
    # X est préparé sur TOUT le mart (encodage catégoriel cohérent avec l'entraînement), puis filtré.
    X_all = mc.align_features(mc.prepare_x(mart, spec["target"], None), feats)
    keys = mart[["str_match_id", "str_team_id"]].reset_index(drop=True)
    X_all = X_all.reset_index(drop=True)

    ok = resolved[resolved["str_status"] == "resolu"][["str_match_id", "str_home_team_id"]]
    rows = keys.reset_index().merge(
        ok, left_on=["str_match_id", "str_team_id"], right_on=["str_match_id", "str_home_team_id"])
    proba = model.predict_proba(X_all.iloc[rows["index"].values])
    proba = mc.floor_proba(proba)            # plancher 0,5 % : pas de probabilité nulle
    out = pd.DataFrame({
        "str_match_id": rows["str_match_id"].values,
        "dec_p_home": proba[:, label_map["H"]],
        "dec_p_draw": proba[:, label_map["D"]],
        "dec_p_away": proba[:, label_map["A"]],
    })
    out["str_predicted_outcome"] = out[["dec_p_home", "dec_p_draw", "dec_p_away"]].values.argmax(axis=1)
    out["str_predicted_outcome"] = out["str_predicted_outcome"].map({0: "H", 1: "D", 2: "A"})

    res = resolved.merge(out, on="str_match_id", how="left")
    miss = (res["str_status"] == "resolu") & res["dec_p_home"].isna()
    res.loc[miss, "str_status"] = "ligne_mart_absente"
    res.loc[miss, "str_message"] = "Match résolu mais ligne domicile absente du mart (features non construites)"
    return res


def write_predictions(db_path, df, model_key, version):
    """Écrit df dans prediction.match_predicted : remplace les lignes du même (modèle, version) pour les
    mêmes matchs demandés (date + équipes), puis insère."""
    df = df.copy()
    df["str_model_key"], df["str_model_version"] = model_key, version
    df["dt_predicted_at"] = pd.Timestamp.now().floor("s")
    df = df[COLS]
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    con.register("batch", df)
    con.execute("""
        DELETE FROM prediction.match_predicted p
        WHERE p.str_model_key = ? AND p.str_model_version = ?
          AND EXISTS (SELECT 1 FROM batch b
                      WHERE b.dt_date = p.dt_date
                        AND b.str_home_team_name = p.str_home_team_name
                        AND b.str_away_team_name = p.str_away_team_name)
    """, [model_key, version])
    con.execute("INSERT INTO prediction.match_predicted SELECT * FROM batch")
    n = con.execute("select count(*) from prediction.match_predicted where str_model_key = ? and str_model_version = ?",
                    [model_key, version]).fetchone()[0]
    con.close()
    return n


def main(model_key=MODEL_KEY, out_db=None):
    cfg, models = mc.load_configs()
    if model_key not in models:
        raise SystemExit(f"Modèle inconnu '{model_key}' (clés de config/models.yml : {', '.join(models)})")
    db_path = mc.ROOT_DIR / cfg["paths"]["duckdb"]
    resolved = read_resolved(db_path)
    print(f"{len(resolved):,} matchs demandés : {resolved['str_status'].value_counts().to_dict()}")
    res = predict_resolved(cfg, models, model_key, resolved)
    version = model_version(model_key)
    n = write_predictions(out_db or db_path, res, model_key, version)
    ok = res["dec_p_home"].notna().sum()
    print(f"[{model_key} v{version}] {ok:,} matchs prédits ; prediction.match_predicted contient {n:,} lignes pour ce modèle.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prédit prediction.match_resolved → prediction.match_predicted.")
    parser.add_argument("--model", default=MODEL_KEY,
                        help="clé du modèle dans config/models.yml (défaut : resultat_1n2_toutes_saisons)")
    parser.add_argument("--out-db", default=None,
                        help="(tests) base DuckDB où écrire, au lieu de la base du projet")
    args = parser.parse_args()
    main(args.model, args.out_db)
