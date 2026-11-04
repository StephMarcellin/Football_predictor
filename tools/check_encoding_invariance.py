"""
check_encoding_invariance.py — Garde-fou train/serve : la prédiction d'une ligne du mart
ne doit PAS dépendre du lot dans lequel elle est prédite.

Pourquoi : l'encodage des colonnes texte (famille de formation, catégorie de compétition…)
a déjà dépendu du lot de prédiction (cat.codes calculé sur le lot). Ce test entraîne un
mini-modèle 1N2 (rapide) avec le VRAI code (ml_common.prepare_x / train_lgbm / calibrate),
puis compare la prédiction d'une ligne seule, d'un lot et du match de référence à celle
obtenue dans le jeu complet. Écart attendu : 0.

Lancement :
    python tools/check_encoding_invariance.py [--tol 1e-9] [--n-estimators 150]

Code retour : 0 = OK, 1 = écart supérieur à la tolérance. Lecture seule sur la DB
(la DB ne doit pas être ouverte en écriture ailleurs). Ne sauvegarde aucun modèle.
"""
import argparse
import sys
import tempfile
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "pipelines"))
import ml_common as mc  # noqa: E402  (après l'ajout au sys.path)

TARGET = "str_result_1n2"
LABEL_MAP = {"H": 0, "D": 1, "A": 2}
REF_MATCH = "4490faecfa0de031a96e4174c48ae80297bf0190"   # Liverpool–Man City, 01/12/2024


def build_model(df, cfg, n_estimators):
    """Mini-entraînement avec le code réel. Retourne (modèle calibré, X complet, y, masque val)."""
    d = df[df[TARGET].notna()].copy()
    y = d[TARGET].map(LABEL_MAP).astype(int)
    X = mc.prepare_x(d, TARGET, [])
    tr, va, _ = mc.temporal_split(d, cfg)
    tr = tr & (d["str_season"] >= "2017-2018")          # plus rapide : le test n'évalue pas la qualité
    params = dict(objective="multiclass", num_class=3, n_estimators=n_estimators,
                  learning_rate=0.1, num_leaves=31, subsample=0.8, colsample_bytree=0.8,
                  random_state=42, verbose=-1, n_jobs=2)
    model = mc.train_lgbm(X, y, tr, va, params)
    model = mc.calibrate(model, X, y, va)
    return model, d, X, va


def main(tol, n_estimators):
    cfg, _ = mc.load_configs()
    df = mc.load_mart(cfg, "mart_1n2")
    model, d, X, va = build_model(df, cfg, n_estimators)
    feats = list(X.columns)
    full = pd.DataFrame(model.predict_proba(X), index=d.index)     # référence : jeu complet

    def predict_like_serving(sub):
        """Même chemin que predict_1n2 : prepare_x sur le seul lot, puis réalignement."""
        Xs = mc.prepare_x(sub, TARGET, []).reindex(columns=feats, fill_value=0)
        return model.predict_proba(Xs)

    def gap(sub):
        return float(np.abs(predict_like_serving(sub) - full.loc[sub.index].to_numpy()).max())

    val_rows = d[va]
    cat_cols = [c for c in X.columns if str(X[c].dtype) == "category"]
    checks = []
    ref = d[d["str_match_id"] == REF_MATCH]
    if len(ref):
        checks.append(("match de référence prédit seul", gap(ref)))
    checks.append(("lot de 300 lignes", gap(val_rows.sample(300, random_state=0))))
    singles = val_rows.sample(60, random_state=1)
    checks.append(("60 lignes prédites une par une", max(gap(singles.iloc[[i]]) for i in range(60))))
    cup = val_rows[val_rows["str_comp_category"] != "Big5"].head(1)
    if len(cup):
        checks.append(("1 match hors Big5 prédit seul", gap(cup)))

    # Matchs à venir (sans résultat) : doivent se prédire sans erreur, probas qui somment à 1.
    fut = df[df[TARGET].isna()].head(12)
    if len(fut):
        p = predict_like_serving(fut)
        checks.append(("12 matchs à venir : écart de somme à 1", float(np.abs(p.sum(axis=1) - 1).max())))

    # Aller-retour disque : le modèle rechargé doit prédire pareil.
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "m.joblib"
        joblib.dump({"model": model, "features": feats}, f)
        m2 = joblib.load(f)["model"]
        sub = ref if len(ref) else singles.iloc[[0]]
        Xs = mc.prepare_x(sub, TARGET, []).reindex(columns=feats, fill_value=0)
        checks.append(("après dump/load joblib", float(np.abs(m2.predict_proba(Xs) - full.loc[sub.index].to_numpy()).max())))

    print(f"Colonnes catégorielles natives : {len(cat_cols)} {cat_cols}")
    worst = 0.0
    for name, g in checks:
        worst = max(worst, g)
        print(f"  {'OK ' if g <= tol else 'KO '} {name:42} écart max = {g:.2e}")
    ok = worst <= tol and len(cat_cols) > 0
    if not cat_cols:
        print("KO : aucune colonne catégorielle détectée — le test ne prouve rien.")
    print("RÉSULTAT :", "OK" if ok else "ÉCHEC")
    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invariance de la prédiction au lot (encodage catégoriel).")
    parser.add_argument("--tol", type=float, default=1e-9, help="écart maximal toléré (défaut 1e-9)")
    parser.add_argument("--n-estimators", type=int, default=150, help="arbres du mini-modèle (défaut 150)")
    args = parser.parse_args()
    sys.exit(main(args.tol, args.n_estimators))
