"""
predict_1n2.py — Prédiction du modèle 1N2 (P(H / D / A)).

Charge models/resultat_1n2.joblib et prédit EXACTEMENT les matchs demandés
(--match-ids). Reconstruit X via ml_common.prepare_x (mêmes features, cotes
exclues) et écrit les probabilités P(H/D/A).

Note grain : mart_1n2 est au grain (match_id, team_id) → chaque match donne
2 lignes (une par équipe, sa perspective). team_id / opponent_id désambiguïsent.

Sortie :
  - toujours       : reports/predictions_1n2.csv
  - avec --write   : table machine_learning.predictions_1n2 (CREATE OR REPLACE)

Lancement :
  python pipelines/predict_1n2.py --match-ids 093f1d...,5bb4dc...
  python pipelines/predict_1n2.py --match-ids 093f1d... --write
"""
# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import argparse
from datetime import datetime, timezone

import duckdb
import joblib

import ml_common as mc

MODEL_KEY = "resultat_1n2"
ID_OUT = ["match_id", "team_id", "opponent_id", "league_source", "date", "season"]


def load_model():
    """Dépickle le payload de train_1n2 : modèle calibré, features ordonnées,
    label_map {'H':0,'D':1,'A':2}."""
    payload = joblib.load(mc.MODELS_DIR / "resultat_1n2.joblib")
    return payload["model"], payload["features"], payload["label_map"]

def read_ids_file(path):
    """Lit les match_id d'un fichier : CSV (colonne match_id) ou TXT (1 id/ligne)."""
    from pathlib import Path
    p = Path(path)
    if not p.is_absolute():
        p = mc.ROOT_DIR / p
    lines = p.read_text(encoding="utf-8").splitlines()
    if not lines:
        return []
    header = [h.strip().lower() for h in lines[0].split(",")]
    if "match_id" in header:                       # CSV avec en-tête → on prend la bonne colonne
        idx = header.index("match_id")
        return [ln.split(",")[idx].strip() for ln in lines[1:] if ln.strip()]
    return [ln.strip() for ln in lines if ln.strip()]   # sinon 1 id par ligne

def build_predict_frame(cfg, spec, match_ids=None):
    """Lignes à prédire. Lit le mart en read-only et rattache league_source
    (absent du mart) depuis backbone. Si match_ids est fourni → uniquement ces
    matchs (tels quels) ; sinon → tous les matchs sans résultat."""
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    df = con.execute(f"""
        select m.*, b.league_source
        from marts.{spec['mart']} m
        left join intermediate.backbone b using (match_id, team_id)
    """).df()
    con.close()

    if match_ids:
        df = df[df["match_id"].isin(match_ids)].copy()
    else:
        df = df[df[spec["target"]].isna()].copy()
    return df


def predict(model, feats, label_map, pred, spec):
    """X reconstruit comme au fit, réaligné sur l'ordre du modèle, puis probas
    → df identifiants + P(H/D/A) + label argmax."""
    X = mc.prepare_x(pred, spec["target"], spec.get("exclude"))
    X = X.reindex(columns=feats, fill_value=0)       # même ordre/colonnes qu'au fit
    proba = model.predict_proba(X)

    inv = {v: k for k, v in label_map.items()}       # {0:'H', 1:'D', 2:'A'}
    out = pred[ID_OUT].reset_index(drop=True).copy()
    for i, cls in enumerate(model.classes_):         # classes_ = [0 1 2] → colonne i
        out[f"prob_{inv[cls]}"] = proba[:, i]

    prob_cols = [f"prob_{inv[c]}" for c in model.classes_]
    out["pred_1n2"] = out[prob_cols].idxmax(axis=1).str.replace("prob_", "", regex=False)
    out["predicted_at"] = datetime.now(timezone.utc)
    return out


def write_table(cfg, out, table="machine_learning.predictions_1n2"):
    """Écrit dans DuckDB (CREATE OR REPLACE). Connexion en écriture → aucune
    autre session SQL ouverte (DuckDB = single writer)."""
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=False)
    con.register("tmp_pred_1n2", out)
    con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM tmp_pred_1n2")
    con.unregister("tmp_pred_1n2")
    con.close()


def main(match_ids=None, write=False):
    cfg, models = mc.load_configs()
    spec = models[MODEL_KEY]

    pred = build_predict_frame(cfg, spec, match_ids)
    if pred.empty:
        print("Aucun match à prédire (ids introuvables ou aucun match sans résultat).")
        return None
    print(f"[predict 1N2] {pred['match_id'].nunique()} match(s), {len(pred)} lignes à prédire.")

    model, feats, label_map = load_model()
    out = predict(model, feats, label_map, pred, spec)

    csv_path = mc.ROOT_DIR / "reports" / "predictions_1n2.csv"
    csv_path.parent.mkdir(exist_ok=True)
    out.to_csv(csv_path, index=False)
    print(f"CSV écrit : {csv_path}")

    if write:
        write_table(cfg, out)
        print("Table écrite : machine_learning.predictions_1n2")
    else:
        print("Dry-run : table DuckDB non écrite (ajoute --write pour la produire).")

    print(out.to_string(index=False))
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prédiction 1N2 (mart_1n2 → P(H/D/A)).")
    parser.add_argument("--match-ids", required=True,
                        help="ids de matchs séparés par des virgules")
    parser.add_argument("--write", action="store_true",
                        help="écrit la table machine_learning.predictions_1n2")
    args = parser.parse_args()
    ids = [s.strip() for s in args.match_ids.split(",") if s.strip()]
    main(match_ids=ids, write=args.write)