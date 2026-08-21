"""
eval_goals.py — Comparatif 1X2 : dérivé des buts vs direct vs ensemble.

Charge models/buts_equipe.joblib (train_goals) et models/resultat_1n2.joblib
(train_1n2). Le 1X2 dérivé vient de λ_home/λ_away → matrice de scores Poisson +
correction Dixon-Coles. rho est estimé sur la VALIDATION (min log-loss), puis figé
pour le TEST. Le 1X2 direct est la moyenne des 2 perspectives (domicile/extérieur)
du modèle multiclasse.

Lancement :  python pipelines/eval_goals.py
"""
import duckdb
import joblib
import numpy as np
from sklearn.metrics import log_loss, accuracy_score

import ml_common as mc
from poisson_markets import markets_from_lambdas

LAB = {"H": 0, "D": 1, "A": 2}


def _pairs_lambda(cfg, seasons):
    """{match_id: (lam_home, lam_away, result_1n2)} — buts attendus par côté."""
    spec = mc.load_configs()[1]["buts_equipe"]
    pay = joblib.load(mc.MODELS_DIR / "buts_equipe.joblib")
    df = mc.load_mart(cfg, spec["mart"])
    df = df[df[spec["target"]].notna()].copy()
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=pay["features"], fill_value=0)
    df = df[["match_id", "team_id", "season"]].copy()
    df["mu"] = pay["model"].predict(X)

    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    bb = con.execute("select match_id, team_id, venue, result_1n2 "
                     "from intermediate.backbone").df()
    con.close()
    df = df[df["season"].isin(seasons)].merge(bb, on=["match_id", "team_id"])
    H = df[df.venue == "Home"].set_index("match_id")
    A = df[df.venue == "Away"].set_index("match_id")
    out = {}
    for m in H.index.intersection(A.index):
        r = H.loc[m, "result_1n2"]
        if r in LAB:
            out[m] = (float(H.loc[m, "mu"]), float(A.loc[m, "mu"]), r)
    return out


def _direct_probs(cfg, seasons):
    """{match_id: [P(H), P(D), P(A)]} — moyenne des 2 perspectives du modèle direct."""
    spec = mc.load_configs()[1]["resultat_1n2"]
    pay = joblib.load(mc.MODELS_DIR / "resultat_1n2.joblib")
    df = mc.load_mart(cfg, spec["mart"])
    df = df[df["season"].isin(seasons)].copy()
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=pay["features"], fill_value=0)
    proba = pay["model"].predict_proba(X)
    if float(proba.std(axis=0).mean()) < 1e-6:
        raise RuntimeError("Probas uniformes : calibration dégénérée — vérifie la "
                           "version de scikit-learn vs celle d'entraînement.")
    inv = {v: k for k, v in pay["label_map"].items()}          # 0→H, 1→D, 2→A
    df = df[["match_id"]].copy()
    for k, cls in enumerate(pay["model"].classes_):
        df[inv[cls]] = proba[:, k]
    g = df.groupby("match_id")[["H", "D", "A"]].mean()
    return {m: g.loc[m, ["H", "D", "A"]].values for m in g.index}


def _score(pairs, rho):
    P, Y = [], []
    for m, (lh, la, r) in pairs.items():
        mk = markets_from_lambdas(lh, la, rho=rho)
        P.append([mk["prob_H"], mk["prob_D"], mk["prob_A"]]); Y.append(LAB[r])
    P, Y = np.array(P), np.array(Y)
    return log_loss(Y, P, labels=[0, 1, 2]), accuracy_score(Y, P.argmax(1)), P, Y


def main():
    cfg = mc.load_configs()[0]
    val_seasons = cfg["train"]["VAL_SEASONS"]
    test_season = [cfg["train"]["TEST_SEASON"]]

    # 1) rho Dixon-Coles estimé sur la validation
    val_pairs = _pairs_lambda(cfg, val_seasons)
    grid = [0.0, -0.02, -0.04, -0.06, -0.08, -0.10, -0.12]
    rho = min(grid, key=lambda r: _score(val_pairs, r)[0])
    print(f"rho Dixon-Coles retenu (min logloss VAL) : {rho:+.2f}")

    # 2) évaluation sur le test
    test_pairs = _pairs_lambda(cfg, test_season)
    ll_der, acc_der, Pder, Y = _score(test_pairs, rho)

    direct = _direct_probs(cfg, test_season)
    common = [m for m in test_pairs if m in direct]
    idx = [i for i, m in enumerate(test_pairs) if m in direct]
    Pdir = np.array([direct[m] for m in common], dtype=float)
    Ycommon = Y[idx]; Pder_c = Pder[idx]
    Pblend = 0.5 * Pder_c + 0.5 * Pdir

    print(f"\nComparatif 1X2 — saison {test_season[0]} (n={len(common)} matchs)\n")
    for name, P in [("Dérivé (buts→Poisson+DC)", Pder_c),
                    ("Direct (predict_1n2)", Pdir),
                    ("Ensemble 50/50", Pblend)]:
        ll = log_loss(Ycommon, P, labels=[0, 1, 2])
        acc = accuracy_score(Ycommon, P.argmax(1))
        print(f"  {name:28} logloss={ll:.4f}  acc={acc:.3f}")


if __name__ == "__main__":
    main()
