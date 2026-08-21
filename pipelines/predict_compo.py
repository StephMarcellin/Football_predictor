"""
predict_compo.py — Prédit un match à partir d'un fichier compo (formation + noms).
Chemin de service complet : fichier compo → features de compo recalculées →
vecteur de 256 features → P(H/D/A).

Lancement : python pipelines/predict_compo.py data/compo_b5c065.yaml
"""
import argparse

import duckdb
import joblib

import ml_common as mc
import serve_features as sf


def main(compo_path):
    cfg, models = mc.load_configs()
    spec = models["resultat_1n2"]
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    mid, compo, xi_pos = sf.load_compo(con, compo_path)
    payload = joblib.load(mc.MODELS_DIR / "resultat_1n2.joblib")
    out = sf.predict_served(con, mid, compo, xi_pos, payload, spec)
    con.close()
    print(f"match {mid[:12]} — prédiction depuis compo :")
    print(out.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prédiction à partir d'un fichier compo.")
    parser.add_argument("compo", help="chemin du fichier compo YAML")
    args = parser.parse_args()
    main(args.compo)
