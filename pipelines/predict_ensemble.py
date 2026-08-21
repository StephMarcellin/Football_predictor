"""
predict_ensemble.py — Prédiction 1X2 au niveau MATCH par ensemble.

Combine deux vues du même match (cf. eval_goals : l'ensemble bat les deux) :
  • DÉRIVÉ : modèle de buts (buts_equipe.joblib) → λ_home/λ_away → matrice de
             scores Poisson + correction Dixon-Coles → 1X2 (poisson_markets).
  • DIRECT : modèle multiclasse (resultat_1n2.joblib), moyenné sur les 2
             perspectives (domicile/extérieur) → 1X2 au niveau match.

Sortie = alpha·dérivé + (1−alpha)·direct, plus over/under et BTTS (issus du
modèle de buts). alpha et rho viennent de config.yaml (predict.ensemble_alpha,
predict.dixon_coles_rho), avec des défauts sûrs (0.5 / −0.08).

Lancement :
  python pipelines/predict_ensemble.py --season 2024-2025
  python pipelines/predict_ensemble.py --match-ids <id1>,<id2> --write
"""
import argparse

import duckdb
import joblib
import pandas as pd

import ml_common as mc
from poisson_markets import markets_from_lambdas


def _filter(df, match_ids, season):
    if match_ids:
        return df[df["match_id"].isin(match_ids)].copy()
    if season:
        return df[df["season"] == season].copy()
    return df


def derived_probs(cfg, con, match_ids=None, season=None, rho=None):
    """1X2 dérivé + marchés de buts, par match, depuis le modèle de buts."""
    spec = mc.load_configs()[1]["buts_equipe"]
    pay = joblib.load(mc.MODELS_DIR / "buts_equipe.joblib")
    df = _filter(mc.load_mart(cfg, spec["mart"]), match_ids, season)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=pay["features"], fill_value=0)
    df = df[["match_id", "team_id"]].copy()
    df["mu"] = pay["model"].predict(X)
    bb = con.execute("select match_id, team_id, venue from intermediate.backbone").df()
    df = df.merge(bb, on=["match_id", "team_id"])
    H = df[df.venue == "Home"].set_index("match_id")["mu"]
    A = df[df.venue == "Away"].set_index("match_id")["mu"]
    if rho is None:
        rho = cfg.get("predict", {}).get("dixon_coles_rho", -0.08)
    rows = []
    for m in H.index.intersection(A.index):
        mk = markets_from_lambdas(float(H[m]), float(A[m]), rho=rho)
        rows.append({"match_id": m, "H": mk["prob_H"], "D": mk["prob_D"], "A": mk["prob_A"],
                     "prob_over": mk["prob_over"], "prob_under": mk["prob_under"],
                     "prob_btts": mk["prob_btts"], "exp_goals": mk["exp_goals"]})
    return pd.DataFrame(rows).set_index("match_id")


def direct_probs(cfg, match_ids=None, season=None):
    """1X2 direct par match : moyenne des 2 perspectives du modèle multiclasse."""
    spec = mc.load_configs()[1]["resultat_1n2"]
    pay = joblib.load(mc.MODELS_DIR / "resultat_1n2.joblib")
    df = _filter(mc.load_mart(cfg, spec["mart"]), match_ids, season)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=pay["features"], fill_value=0)
    proba = pay["model"].predict_proba(X)
    if float(proba.std(axis=0).mean()) < 1e-6:
        raise RuntimeError("Probas uniformes : calibration dégénérée — version sklearn ?")
    inv = {v: k for k, v in pay["label_map"].items()}
    out = df[["match_id"]].copy()
    for k, cls in enumerate(pay["model"].classes_):
        out[inv[cls]] = proba[:, k]
    return out.groupby("match_id")[["H", "D", "A"]].mean()


def predict_ensemble(cfg, match_ids=None, season=None):
    """Blend des deux vues → 1X2 match-level + marchés de buts."""
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    der = derived_probs(cfg, con, match_ids, season)
    con.close()
    dir_ = direct_probs(cfg, match_ids, season)

    alpha = cfg.get("predict", {}).get("ensemble_alpha", 0.5)
    common = der.index.intersection(dir_.index)
    out = pd.DataFrame(index=common)
    for c in ["H", "D", "A"]:
        out[f"prob_{c}"] = alpha * der.loc[common, c] + (1 - alpha) * dir_.loc[common, c]
    for c in ["prob_over", "prob_under", "prob_btts", "exp_goals"]:
        out[c] = der.loc[common, c]
    out["pred_1n2"] = out[["prob_H", "prob_D", "prob_A"]].idxmax(1).str.replace("prob_", "", regex=False)
    return out.reset_index(names="match_id")


def main(match_ids=None, season=None, write=False):
    cfg = mc.load_configs()[0]
    out = predict_ensemble(cfg, match_ids, season)

    csv = mc.ROOT_DIR / "reports" / "predictions_ensemble_1n2.csv"
    csv.parent.mkdir(exist_ok=True)
    out.to_csv(csv, index=False)
    print(f"{len(out)} matchs → {csv}")
    print(out.head(10).to_string(index=False))

    if write:
        con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=False)
        con.register("tmp_ens", out)
        con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
        con.execute("CREATE OR REPLACE TABLE machine_learning.predictions_ensemble_1n2 "
                    "AS SELECT * FROM tmp_ens")
        con.unregister("tmp_ens"); con.close()
        print("Table écrite : machine_learning.predictions_ensemble_1n2")
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Prédiction 1X2 ensemble (buts + direct).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--season", help="ex: 2024-2025")
    g.add_argument("--match-ids", help="ids séparés par des virgules")
    p.add_argument("--write", action="store_true",
                   help="écrit machine_learning.predictions_ensemble_1n2")
    a = p.parse_args()
    ids = [s.strip() for s in a.match_ids.split(",") if s.strip()] if a.match_ids else None
    main(match_ids=ids, season=a.season, write=a.write)
