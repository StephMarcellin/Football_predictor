"""
predict_markets.py — Vue détaillée des marchés d'un match depuis le modèle de buts.

Pour chaque match : buts attendus par équipe, distribution de buts (0..6+) des
deux côtés, 1X2, over/under de toutes les lignes, clean sheets, BTTS, scores exacts
les plus probables. Tout est dérivé de λ (buts_equipe.joblib) via la matrice Poisson
+ Dixon-Coles (poisson_markets.full_markets).

Lancement :
  python pipelines/predict_markets.py --match-ids <id1>,<id2>
  python pipelines/predict_markets.py --season 2024-2025 --csv
"""
# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import argparse

import duckdb
import joblib
import pandas as pd

import ml_common as mc
from poisson_markets import full_markets

_KS = ["0", "1", "2", "3", "4", "5", "6+"]


def _lambdas(cfg, con, match_ids, season):
    """{match_id: (lam_home, lam_away)} depuis le modèle de buts."""
    spec = mc.load_configs()[1]["buts_equipe"]
    pay = joblib.load(mc.MODELS_DIR / "buts_equipe.joblib")
    df = mc.load_mart(cfg, spec["mart"])
    if match_ids:
        df = df[df["match_id"].isin(match_ids)].copy()
    elif season:
        df = df[df["season"] == season].copy()
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=pay["features"], fill_value=0)
    df = df[["match_id", "team_id"]].copy()
    df["mu"] = pay["model"].predict(X)
    bb = con.execute("select match_id, team_id, venue from intermediate.backbone").df()
    df = df.merge(bb, on=["match_id", "team_id"])
    H = df[df.venue == "Home"].set_index("match_id")["mu"]
    A = df[df.venue == "Away"].set_index("match_id")["mu"]
    return {m: (float(H[m]), float(A[m])) for m in H.index.intersection(A.index)}


def _print_match(mid, fm):
    r = fm["result"]; ou = fm["over_under"]["2.5"]
    print(f"\n═══ {mid[:12]} — buts attendus {fm['exp_goals_home']:.2f} - {fm['exp_goals_away']:.2f} ═══")
    print(f"  Résultat   :  H {r['H']:.1%}   N {r['D']:.1%}   A {r['A']:.1%}")
    print("  Buts dom.  :  " + "  ".join(f"{k}:{fm['goals_home'][k]:.1%}" for k in _KS))
    print("  Buts ext.  :  " + "  ".join(f"{k}:{fm['goals_away'][k]:.1%}" for k in _KS))
    print(f"  O/U 2.5    :  over {ou['over']:.1%}   under {ou['under']:.1%}")
    print(f"  BTTS       :  oui {fm['btts']['yes']:.1%}   non {fm['btts']['no']:.1%}")
    print(f"  Clean sheet:  dom {fm['clean_sheet_home']:.1%}   ext {fm['clean_sheet_away']:.1%}")
    print("  Scores     :  " + ", ".join(f"{s['score']} {s['prob']:.1%}" for s in fm["top_scores"]))


def _flatten(mid, fm):
    row = {"match_id": mid,
           "exp_goals_home": fm["exp_goals_home"], "exp_goals_away": fm["exp_goals_away"],
           "prob_H": fm["result"]["H"], "prob_D": fm["result"]["D"], "prob_A": fm["result"]["A"],
           "clean_sheet_home": fm["clean_sheet_home"], "clean_sheet_away": fm["clean_sheet_away"],
           "btts_yes": fm["btts"]["yes"]}
    for k, v in fm["goals_home"].items():
        row[f"home_{k}"] = v
    for k, v in fm["goals_away"].items():
        row[f"away_{k}"] = v
    for L, d in fm["over_under"].items():
        row[f"over_{L}"] = d["over"]
    return row


def main(match_ids=None, season=None, csv=False):
    cfg = mc.load_configs()[0]
    rho = cfg.get("predict", {}).get("dixon_coles_rho", -0.08)
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    lambdas = _lambdas(cfg, con, match_ids, season)
    con.close()

    rows = []
    for mid, (lh, la) in lambdas.items():
        fm = full_markets(lh, la, rho=rho)
        rows.append(_flatten(mid, fm))
        if not csv:
            _print_match(mid, fm)

    if csv:
        out = pd.DataFrame(rows)
        path = mc.ROOT_DIR / "reports" / "markets_detail.csv"
        path.parent.mkdir(exist_ok=True)
        out.to_csv(path, index=False)
        print(f"{len(out)} matchs → {path}")
    return rows


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Vue détaillée des marchés (modèle de buts).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--season", help="ex: 2024-2025")
    g.add_argument("--match-ids", help="ids séparés par des virgules")
    p.add_argument("--csv", action="store_true",
                   help="écrit reports/markets_detail.csv au lieu d'afficher")
    a = p.parse_args()
    ids = [s.strip() for s in a.match_ids.split(",") if s.strip()] if a.match_ids else None
    main(match_ids=ids, season=a.season, csv=a.csv)
