"""
elo_tune.py — palier 9, point 1. Choisit K, avantage du terrain et régression de l'Elo SANS regarder les saisons de test
(2019-20 et après) : l'Elo sert d'unique variable d'une régression logistique multinomiale ajustée sur les matchs Big5 jusqu'à 2015-16,
évaluée en log-loss sur 2016-17 à 2018-19. Recherche par coordonnées figée avant mesure : K, puis avantage du terrain, puis régression.
Reprise : l'état est dans reports/chunks/elo_tune.json (limite de 180 s par appel de l'appareil). Usage : python tools/elo_tune.py
"""
import json
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "pipelines" / "features"))
import ml_common as mc  # noqa: E402
import team_elo  # noqa: E402

STATE = ROOT / "reports" / "chunks" / "elo_tune.json"
TRAIN_END, VAL = "2015-2016", ("2016-2017", "2017-2018", "2018-2019")
STAGES = [("K", [10.0, 15.0, 20.0, 30.0, 40.0]), ("HFA", [40.0, 55.0, 70.0, 85.0, 100.0]), ("REGRESS", [0.10, 0.25, 0.40])]


def load_rows():
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    rows = con.sql("""select str_match_id, str_team_id, str_opponent_id, dt_date, str_venue, str_season, str_league_source,
                      str_comp_category, int_gf, int_ga from intermediate.intermediate_team_match_backbone""").df()
    con.close()
    return rows


def evaluate(rows, K, HFA, REG):
    team_elo.K, team_elo.HFA, team_elo.REGRESS = K, HFA, REG
    F = team_elo.compute(rows)[2]
    h = rows[(rows.str_venue == "Home") & (rows.str_comp_category == "Big5") & rows.int_gf.notna()]
    d = h.merge(F[["str_match_id", "str_team_id", "dec_elo_diff"]], on=["str_match_id", "str_team_id"])
    d["y"] = np.where(d.int_gf > d.int_ga, 0, np.where(d.int_gf == d.int_ga, 1, 2))
    tr, va = d[d.str_season <= TRAIN_END], d[d.str_season.isin(VAL)]
    m = LogisticRegression(max_iter=500).fit(tr[["dec_elo_diff"]] / 100, tr.y)
    P = m.predict_proba(va[["dec_elo_diff"]] / 100)
    return float(-np.log(P[np.arange(len(va)), va.y.values]).mean()), len(va)


def main():
    t0 = time.time()
    state = json.loads(STATE.read_text()) if STATE.exists() else {"results": {}}
    res = state["results"]
    rows = load_rows()
    best = dict(K=20.0, HFA=70.0, REGRESS=0.25)
    for name, values in STAGES:
        for v in values:
            p = {**best, name: v}
            key = f"{p['K']}|{p['HFA']}|{p['REGRESS']}"
            if key not in res:
                if time.time() - t0 > 110:
                    STATE.write_text(json.dumps(state))
                    print("PAUSE : relancer la même commande")
                    return
                res[key] = evaluate(rows, **{"K": p["K"], "HFA": p["HFA"], "REG": p["REGRESS"]})
                print(f"  K={p['K']:>4} HFA={p['HFA']:>5} régression={p['REGRESS']:.2f} -> log-loss {res[key][0]:.5f} (n={res[key][1]})", flush=True)
        cand = {k: v[0] for k, v in res.items() if all(float(k.split('|')[i]) == best[n] for i, n in enumerate(['K', 'HFA', 'REGRESS']) if n != name)}
        best[name] = float(min(cand, key=cand.get).split("|")[["K", "HFA", "REGRESS"].index(name)])
        print(f"== meilleur {name} = {best[name]}", flush=True)
    STATE.write_text(json.dumps(state))
    base = res["20.0|70.0|0.25"][0]
    fin = res[f"{best['K']}|{best['HFA']}|{best['REGRESS']}"][0]
    print(f"FINAL : K={best['K']} HFA={best['HFA']} régression={best['REGRESS']} | log-loss {fin:.5f} contre {base:.5f} (paramètres actuels) : {base - fin:+.5f}")
    state["best"] = best
    STATE.write_text(json.dumps(state))


if __name__ == "__main__":
    main()
