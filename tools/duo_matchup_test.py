"""Test des DUOS et des face-à-face au niveau du joueur et du match, sur trois cibles.

Unité : un attaquant (WIDE ou ST) dans un onze de départ, un match, au moins 60 minutes. Cibles par 90 minutes :
  xt        = xT créé par ses passes (intermediate_player_match_passes)  [cible principale, dense]
  shots     = tirs                                                        [confirmation]
  xg        = xg_contribution                                             [confirmation]
Variables (profils z de la saison s-1) en quatre groupes :
  A    : l'attaquant lui-même
  team : coéquipiers (arrière du même flanc, autres attaquants, milieux)
  opp  : adversaires en face (arrière du flanc miroir, défenseurs centraux)
  ctx  : domicile, flanc, poste
Modèles (LightGBM, mêmes variables) : additif (A, team et opp ne se croisent pas) ; A x opp autorisé ; A x team autorisé ;
libre. Gain = baisse de l'erreur quadratique contre l'additif. Origine glissante 2019-2020 à 2025-2026 (Big5), apprentissage
depuis 2016-17. IC95 par rééchantillonnage des matchs. Un pli par appel : --target T --test-season Y ; bilan : --aggregate.

Usage : python tools/duo_matchup_test.py --behaviour "read_parquet('b.parquet')" --passes "read_parquet('passes_20*.parquet')"
"""
import argparse
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BIG5 = ["Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1"]
MIRROR = {"L": "R", "R": "L", "C": "C"}
FEAT = {
    "A": ["shots", "key_passes", "xt_created", "g_dribbles_per_90", "crosses", "pass_forward_share", "aerial_win_rate",
          "g_dribble_woe_adj", "xg_contribution"],
    "fb": ["xt_created", "crosses", "pass_forward_share", "g_dribbles_per_90"],
    "att": ["shots", "xt_created", "key_passes", "aerial_win_rate", "xg_contribution"],
    "mid": ["xt_created", "progressive_passes", "pass_accuracy", "pass_forward_share"],
    "ofb": ["g_defended_woe_adj", "tackles", "interceptions", "aerial_win_rate"],
    "ocb": ["g_defended_woe_adj", "aerial_win_rate", "tackles", "interceptions", "clearances"],
}
ALLZ = sorted({f for v in FEAT.values() for f in v})
TARGETS = {"xt": "dec_xt_created", "shots": "int_n_shots", "xg": "dec_xg_contribution"}


def load(behaviour, passes, db):
    con = duckdb.connect(str(db), read_only=True)
    z = ", ".join(f"p.dec_z_{n} AS z_{n}" for n in ALLZ)
    sql = f"""
    WITH mc AS (SELECT str_match_id, str_team_id, str_opponent_id, int_is_home, str_league_source,
                       CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS y FROM gold.gold_team_match),
    xi AS (SELECT DISTINCT l.str_match_id, l.str_team_id, l.str_player_id, m.str_opponent_id, m.int_is_home, m.str_league_source, m.y
           FROM intermediate.intermediate_whoscored_lineup_period l
           JOIN mc m ON m.str_match_id = l.str_match_id AND m.str_team_id = l.str_team_id
           WHERE l.int_start_minute = 0 AND l.str_match_id IS NOT NULL AND m.y >= 2016),
    prof AS (SELECT *, CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS py,
                    (dec_sec_def_right + dec_sec_mid_right + dec_sec_att_right
                   - dec_sec_def_left - dec_sec_mid_left - dec_sec_att_left) AS lat FROM {behaviour}),
    lastp AS (SELECT x.str_match_id, x.str_team_id, x.str_player_id, MAX(p.py) AS py
              FROM xi x JOIN prof p ON p.str_player_id = x.str_player_id AND p.py = x.y - 1 GROUP BY ALL)
    SELECT x.str_match_id, x.str_team_id, x.str_opponent_id, x.str_player_id, x.int_is_home, x.str_league_source, x.y,
           p.str_macro_role AS macro, CASE WHEN p.lat > 0.10 THEN 'R' WHEN p.lat < -0.10 THEN 'L' ELSE 'C' END AS side, {z},
           m.int_minutes_played AS mins, COALESCE(e.int_n_shots, 0) AS int_n_shots,
           COALESCE(e.dec_xg_contribution, 0) AS dec_xg_contribution, COALESCE(ps.dec_xt_created, 0) AS dec_xt_created
    FROM xi x
    JOIN lastp lp ON lp.str_match_id = x.str_match_id AND lp.str_team_id = x.str_team_id AND lp.str_player_id = x.str_player_id
    JOIN prof p ON p.str_player_id = x.str_player_id AND p.py = lp.py
    LEFT JOIN intermediate.intermediate_player_match_minutes m
      ON m.str_match_id = x.str_match_id AND m.str_team_id = x.str_team_id AND m.str_player_id = x.str_player_id
    LEFT JOIN intermediate.intermediate_player_match_event_stats e
      ON e.str_match_id = x.str_match_id AND e.str_team_id = x.str_team_id AND e.str_player_id = x.str_player_id
    LEFT JOIN {passes} ps
      ON ps.str_match_id = x.str_match_id AND ps.str_team_id = x.str_team_id AND ps.str_player_id = x.str_player_id
    """
    df = con.sql(sql).df()
    con.close()
    return df


def build(df):
    """Une ligne par attaquant (WIDE, ST) titulaire, avec les groupes de variables A, team, opp, ctx."""
    key = ["str_match_id", "str_team_id"]
    zc = [f"z_{n}" for n in ALLZ]
    a = df[df.macro.isin(["WIDE", "ST"]) & (df.mins >= 60)].copy()
    out = a[key + ["str_opponent_id", "str_player_id", "y", "str_league_source", "int_is_home", "side", "macro", "mins",
                   "int_n_shots", "dec_xg_contribution", "dec_xt_created"]].copy()
    for n in FEAT["A"]:
        out[f"A_{n}"] = a[f"z_{n}"].values
    # coéquipiers : arrière du même flanc (moyenne des arrières de ce côté), milieux, autres attaquants (moyenne hors soi)
    fb = df[df.macro == "FB"].groupby(key + ["side"])[[f"z_{n}" for n in FEAT["fb"]]].mean().add_prefix("fb_").reset_index()
    out = out.merge(fb.rename(columns={f"fb_z_{n}": f"fb_{n}" for n in FEAT["fb"]}), on=key + ["side"], how="left")
    out.loc[out.side == "C", [f"fb_{n}" for n in FEAT["fb"]]] = np.nan
    mid = df[df.macro == "MID"].groupby(key)[[f"z_{n}" for n in FEAT["mid"]]].mean()
    mid.columns = [f"mid_{n}" for n in FEAT["mid"]]
    out = out.merge(mid.reset_index(), on=key, how="left")
    att = df[df.macro.isin(["WIDE", "ST"])]
    s = att.groupby(key)[[f"z_{n}" for n in FEAT["att"]]].agg("sum")
    c = att.groupby(key)[[f"z_{FEAT['att'][0]}"]].agg("count").iloc[:, 0].rename("n_att")
    tmp = out.merge(s.add_prefix("s_").reset_index(), on=key, how="left").merge(c.reset_index(), on=key, how="left")
    for n in FEAT["att"]:
        own = a[f"z_{n}"].values
        out[f"att_{n}"] = ((tmp[f"s_z_{n}"].values - own) / np.maximum(tmp["n_att"].values - 1, 1))
    out.loc[tmp["n_att"].values <= 1, [f"att_{n}" for n in FEAT["att"]]] = np.nan
    # adversaires : arrière du flanc miroir, défenseurs centraux
    ofb = df[df.macro == "FB"].groupby(key + ["side"])[[f"z_{n}" for n in FEAT["ofb"]]].mean()
    ofb.columns = [f"ofb_{n}" for n in FEAT["ofb"]]
    ofb = ofb.reset_index().rename(columns={"str_team_id": "str_opponent_id", "side": "side_opp"})
    out["side_opp"] = out["side"].map(MIRROR)
    out = out.merge(ofb, on=["str_match_id", "str_opponent_id", "side_opp"], how="left")
    out.loc[out.side == "C", [f"ofb_{n}" for n in FEAT["ofb"]]] = np.nan
    ocb = df[df.macro == "CB"].groupby(key)[[f"z_{n}" for n in FEAT["ocb"]]].mean()
    ocb.columns = [f"ocb_{n}" for n in FEAT["ocb"]]
    ocb = ocb.reset_index().rename(columns={"str_team_id": "str_opponent_id"})
    out = out.merge(ocb, on=["str_match_id", "str_opponent_id"], how="left")
    out["side_c"] = out["side"].map({"L": 0, "C": 1, "R": 2})
    out["macro_c"] = (out["macro"] == "ST").astype(int)
    return out


def groups(df):
    g = {"A": [f"A_{n}" for n in FEAT["A"]],
         "team": [f"{p}_{n}" for p in ("fb", "att", "mid") for n in FEAT[p]],
         "opp": [f"{p}_{n}" for p in ("ofb", "ocb") for n in FEAT[p]],
         "ctx": ["int_is_home", "side_c", "macro_c"]}
    return g


def fit_predict(Xtr, ytr, Xte, constraints=None, seed=0):
    p = dict(n_estimators=300, learning_rate=0.03, num_leaves=15, min_child_samples=100, subsample=0.8, subsample_freq=1,
             colsample_bytree=0.8, reg_lambda=5.0, random_state=seed, verbose=-1, n_jobs=2)
    if constraints is not None:
        p["interaction_constraints"] = constraints
    return lgb.LGBMRegressor(**p).fit(Xtr, ytr).predict(Xte)


def run_fold(d, target, ty, out_dir):
    part = Path(out_dir) / f"duo_matchup_{target}_{ty}.parquet"
    if part.exists():
        return
    g = groups(d)
    feats = g["A"] + g["team"] + g["opp"] + g["ctx"]
    ix = {c: i for i, c in enumerate(feats)}
    I = lambda *ks: [ix[c] for k in ks for c in g[k]]
    variants = {"add": [I("A", "ctx"), I("team", "ctx"), I("opp", "ctx")],
                "Aopp": [I("A", "opp", "ctx"), I("team", "ctx")],
                "Ateam": [I("A", "team", "ctx"), I("opp", "ctx")],
                "free": None}
    y = d[TARGETS[target]] * 90.0 / d["mins"]
    tr, te = d[d.y < ty], d[(d.y == ty) & d.str_league_source.isin(BIG5)].copy()
    te["y_true"] = y.loc[te.index]
    base = g["A"] + g["ctx"]
    te["p_base"] = fit_predict(tr[base], y.loc[tr.index], te[base])
    for k, c in variants.items():
        te["p_" + k] = fit_predict(tr[feats], y.loc[tr.index], te[feats], c)
    te[["str_match_id", "str_team_id", "str_player_id", "y", "y_true"] + [c for c in te.columns if c.startswith("p_")]].to_parquet(part)
    print(f"pli {target} {ty} écrit : {len(te):,} lignes de test, {len(tr):,} d'apprentissage")


def aggregate(target, out_dir):
    files = sorted(Path(out_dir).glob(f"duo_matchup_{target}_*.parquet"))
    r = pd.concat([pd.read_parquet(f) for f in files])
    var = ((r.y_true - r.y_true.mean()) ** 2).mean()
    se = {k: (r.y_true - r["p_" + k]) ** 2 for k in ("base", "add", "Aopp", "Ateam", "free")}
    print(f"[{target}] {len(r):,} lignes (attaquant, match), Big5 {r.y.min()}-{r.y.max()}")
    print("R2 : " + " | ".join(f"{k} {1 - v.mean() / var:.4f}" for k, v in se.items()))
    for nm, a, b in (("équipe + adversaires additifs contre A seul", "base", "add"), ("A x adversaires", "add", "Aopp"),
                     ("A x coéquipiers (duo)", "add", "Ateam"), ("tout libre", "add", "free")):
        gain = se[a] - se[b]
        per = gain.groupby(r.str_match_id).sum().values
        rng = np.random.default_rng(0)
        bs = np.array([per[rng.integers(0, len(per), len(per))].sum() / len(r) for _ in range(1000)])
        by = gain.groupby(r.y).mean()
        print(f"  {nm} : gain relatif {gain.mean() / se[a].mean() * 100:+.2f} % IC95 [{np.percentile(bs, 2.5) / se[a].mean() * 100:+.2f} ; "
              f"{np.percentile(bs, 97.5) / se[a].mean() * 100:+.2f}] % ; saisons gagnantes {(by > 0).sum()} / {len(by)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Duos et face-à-face au niveau de l'attaquant")
    ap.add_argument("--behaviour", default="gold.gold_player_behaviour_profile")
    ap.add_argument("--passes", default="intermediate.intermediate_player_match_passes")
    ap.add_argument("--target", choices=list(TARGETS), default="xt")
    ap.add_argument("--test-season", type=int, default=None)
    ap.add_argument("--aggregate", action="store_true")
    ap.add_argument("--cache", default=None, help="parquet de la table construite (évite de la reconstruire à chaque pli)")
    ap.add_argument("--out-dir", default=None)
    a = ap.parse_args()
    import yaml
    out_dir = Path(a.out_dir or ROOT / "reports" / "chunks")
    out_dir.mkdir(parents=True, exist_ok=True)
    if a.aggregate:
        aggregate(a.target, out_dir)
    else:
        cache = Path(a.cache) if a.cache else None
        if cache and cache.exists():
            d = pd.read_parquet(cache)
        else:
            db = ROOT / yaml.safe_load(open(ROOT / "config.yaml", encoding="utf-8"))["paths"]["duckdb"]
            d = build(load(a.behaviour, a.passes, db))
            if cache:
                d.to_parquet(cache)
        d = d.reset_index(drop=True)
        for ty in ([a.test_season] if a.test_season else range(2019, 2026)):
            run_fold(d, a.target, ty, out_dir)
