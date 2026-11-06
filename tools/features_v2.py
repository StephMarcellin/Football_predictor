"""
features_v2.py — palier 7. Trois groupes de features calculés AVANT chaque match, par (match, équipe) :
  A : mémoire longue et forme par rapport à l'attendu Elo (écart résultat - attendu sur 5/10/20, points par match 20/38, différence de buts 38,
      buts marqués/encaissés pondérés par ancienneté, demi-vie 10)
  B : forme selon le lieu (10 derniers matchs à domicile pour qui joue à domicile, à l'extérieur pour qui joue à l'extérieur)
  C : calendrier (matchs sur 7/14/30 jours, toutes compétitions) et enjeu (écart de points au leader, à la 4e place, à la zone de relégation ; matchs restants)
Aucune fuite : shift(1) avant tout cumul ; classement figé avant les mises à jour de la journée.
Prérequis : reports/chunks/elo_features.csv (tools/elo_features.py). Sorties : reports/chunks/v2A.csv, v2B.csv, v2C.csv
"""
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "tools"))
import ml_common as mc  # noqa: E402
from elo_features import build, LEAGUE_CATS  # noqa: E402

KEY = ["str_match_id", "str_team_id"]


def roll(df, col, n, minp, by="str_team_id"):
    return df.groupby(by, sort=False)[col].transform(lambda s: s.shift(1).rolling(n, min_periods=minp).mean())


def with_opp(df, cols):
    """Ajoute dec_opp_<col> : la valeur de la même colonne pour l'adversaire du même match (NaN s'il n'a pas de ligne)."""
    o = df[["str_match_id", "str_team_id"] + cols].rename(columns={"str_team_id": "str_opponent_id", **{c: "dec_opp_" + c[4:] if c.startswith("dec_") else "opp_" + c for c in cols}})
    return df.merge(o, on=["str_match_id", "str_opponent_id"], how="left")


def main():
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    rows = con.sql("""select str_match_id, str_team_id, str_opponent_id, dt_date, str_venue, str_season, str_league_source,
                      str_comp_category, int_gf, int_ga from intermediate.intermediate_team_match_backbone""").df()
    con.close()
    for c in ("int_gf", "int_ga"):
        rows[c] = rows[c].astype("float64")
    rows["dt_date"] = pd.to_datetime(rows["dt_date"])
    E = pd.read_csv(ROOT / "reports/chunks/elo_features.csv", usecols=KEY + ["dec_elo_win_exp"], dtype={"str_match_id": str, "str_team_id": str})
    rows = rows.merge(E, on=KEY, how="left").sort_values(["str_team_id", "dt_date", "str_match_id"]).reset_index(drop=True)
    gf, ga = rows["int_gf"], rows["int_ga"]
    rows["S"] = np.where(gf.isna(), np.nan, np.where(gf > ga, 1.0, np.where(gf == ga, 0.5, 0.0)))
    rows["pts"] = np.where(gf.isna(), np.nan, np.where(gf > ga, 3.0, np.where(gf == ga, 1.0, 0.0)))
    rows["gd"] = gf - ga
    rows["resid"] = rows["S"] - rows["dec_elo_win_exp"]

    # --- A
    A = rows[KEY + ["str_opponent_id"]].copy()
    for n in (5, 10, 20):
        A[f"dec_pae_{n}"] = roll(rows, "resid", n, max(3, n // 2))
    A["dec_ppg_20"], A["dec_ppg_38"] = roll(rows, "pts", 20, 10), roll(rows, "pts", 38, 15)
    A["dec_gd_pg_38"] = roll(rows, "gd", 38, 15)
    for c, name in (("int_gf", "dec_ewm_gf"), ("int_ga", "dec_ewm_ga")):
        A[name] = rows.groupby("str_team_id", sort=False)[c].transform(lambda s: s.shift(1).ewm(halflife=10, min_periods=3).mean())
    colsA = [c for c in A.columns if c.startswith("dec_")]
    A = with_opp(A, colsA)
    # --- B
    r2 = rows[rows["str_venue"].isin(["Home", "Away"])]
    B = rows[KEY + ["str_opponent_id"]].copy()
    for src, name in (("pts", "dec_v_ppg_10"), ("gd", "dec_v_gd_10"), ("int_gf", "dec_v_gf_10"), ("int_ga", "dec_v_ga_10")):
        v = r2.groupby(["str_team_id", "str_venue"], sort=False)[src].transform(lambda s: s.shift(1).rolling(10, min_periods=5).mean())
        B[name] = v.reindex(B.index)
    colsB = [c for c in B.columns if c.startswith("dec_")]
    B = with_opp(B, colsB)
    # --- C : calendrier
    C = rows[KEY + ["str_opponent_id"]].copy()
    d = rows["dt_date"].values.astype("datetime64[D]")
    for days in (7, 14, 30):
        out = np.zeros(len(rows))
        for team, idx in rows.groupby("str_team_id", sort=False).indices.items():
            dd = d[idx]
            out[idx] = np.searchsorted(dd, dd, "left") - np.searchsorted(dd, dd - np.timedelta64(days, "D"), "left")
        C[f"int_matches_{days}d"] = out
    colsC = [c for c in C.columns if c.startswith("int_matches")]
    o = C[["str_match_id", "str_team_id"] + colsC].rename(columns={"str_team_id": "str_opponent_id", **{c: "int_opp_" + c[4:] for c in colsC}})
    C = C.merge(o, on=["str_match_id", "str_opponent_id"], how="left")

    # --- C : enjeu de classement (boucle chronologique, état figé avant la journée)
    M = build(rows.assign(dt_date=rows["dt_date"].dt.date))
    size = rows[rows["str_comp_category"].isin(LEAGUE_CATS)].groupby(["str_league_source", "str_season"])["str_team_id"].nunique()
    table = defaultdict(lambda: defaultdict(lambda: [0, 0]))               # (ligue, saison) -> équipe -> [pts, joués]
    stakes = []
    for date, day in M.groupby("date", sort=True):
        upd = []
        for m in day.itertuples():
            if m.cat not in LEAGUE_CATS:
                continue
            t = table[(m.league, m.season)]
            L = int(size.get((m.league, m.season), 20))
            pts_sorted = sorted((v[0] for v in t.values()), reverse=True)
            top = pts_sorted[0] if pts_sorted else np.nan
            top4 = pts_sorted[3] if len(pts_sorted) >= 4 else np.nan
            rel = pts_sorted[L - 3] if (L >= 6 and len(pts_sorted) > L - 3) else np.nan
            for team in (m.h, m.a):
                p, j = t[team] if team in t else (0, 0)
                stakes.append((m.mid, team, p - top, p - top4, p - rel, 2 * (L - 1) - j))
            if not np.isnan(m.gh):
                upd.append(m)
        for m in upd:
            t = table[(m.league, m.season)]
            for team, f, c in ((m.h, m.gh, m.ga), (m.a, m.ga, m.gh)):
                t[team][0] += 3 if f > c else 1 if f == c else 0
                t[team][1] += 1
    S = pd.DataFrame(stakes, columns=KEY + ["dec_gap_top", "dec_gap_top4", "dec_gap_rel", "int_matches_left"])
    C = C.merge(S, on=KEY, how="left")

    keep = lambda D, cols: D[KEY + cols].drop_duplicates(KEY)
    outs = {"v2A": keep(A, colsA + ["dec_opp_" + c[4:] for c in colsA]),
            "v2B": keep(B, colsB + ["dec_opp_" + c[4:] for c in colsB]),
            "v2C": keep(C, colsC + ["int_opp_" + c[4:] for c in colsC] + ["dec_gap_top", "dec_gap_top4", "dec_gap_rel", "int_matches_left"])}
    for k, D in outs.items():
        D.to_csv(ROOT / "reports" / "chunks" / f"{k}.csv", index=False)
        print(f"{k}: {len(D):,} lignes, {D.shape[1] - 2} colonnes, part de NaN moyenne {D.iloc[:, 2:].isna().mean().mean():.3f}")
    print("C enjeu, exemple de plages :", S[["dec_gap_top", "dec_gap_top4", "dec_gap_rel"]].describe().loc[["min", "50%", "max"]].round(1).to_dict("list"))


if __name__ == "__main__":
    main()
