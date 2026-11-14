"""Test d'interaction attaquant x défenseur sur les DUELS (dribble au sol ou duel aérien).

Question : le profil du défenseur change-t-il l'effet du profil de l'attaquant (un défenseur qui éteint un dribbleur mais pas
un autre style ; un défenseur qui domine un grand attaquant de tête mais pas un petit), au-delà de la somme des deux ?
Méthode : deux LightGBM avec les mêmes variables ; l'un avec contraintes d'interaction (attaquant + contexte d'un côté,
défenseur + contexte de l'autre : effets additifs entre les deux joueurs), l'autre libre. Écart de log-loss = ce que
l'interaction apporte. Profils = gold_player_behaviour_profile de la saison s-1 (jamais la saison du duel) ; z-scores contre
les pairs ; taille = gold_player_aerial_profile (constante du joueur). Test : Big5, saisons 2019-2020 à 2025-2026 en origine
glissante ; apprentissage = saisons précédentes depuis 2016-17. IC95 par rééchantillonnage des matchs.

Usage : python tools/duel_matchup_test.py --family dribble --duels "read_parquet('ground_20*.parquet')" --behaviour "read_parquet('b.parquet')"
        python tools/duel_matchup_test.py --family aerial  --duels "read_parquet('aerial_20*.parquet')"  --behaviour "read_parquet('b.parquet')"
"""
import argparse
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BIG5 = ["Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1"]

FAMILIES = {
    "dribble": dict(
        where="d.str_duel_family = 'dribble' AND d.str_role = 'attaquant'",
        ctx=["dec_x_duel", "dec_y_duel"],
        att=["g_dribbles_per_90", "g_dribble_woe_adj", "shots", "key_passes", "xt_created", "xg_contribution",
             "pass_short_share", "pass_forward_share", "sec_att_left", "sec_att_centre", "sec_att_right", "touches"],
        dfn=["g_defended_woe_adj", "tackles", "interceptions", "clearances", "aerial_win_rate", "aerial_duels",
             "g_fouls_committed_per_90", "recoveries", "sec_def_left", "sec_def_centre", "sec_def_right"],
        height=False),
    "aerial": dict(
        where="d.str_role = 'offensif'",
        ctx=["dec_x", "dec_y", "ball_cross", "ball_corner", "ball_freekick", "ball_longball", "ball_headpass", "ball_goalkick", "ball_throwin"],
        att=["aerial_win_rate", "aerial_duels", "a_woe_net_off", "a_head_shots_per_match", "a_duels_per_match", "shots",
             "clearances", "touches"],
        dfn=["aerial_win_rate", "aerial_duels", "a_woe_net_def", "a_duels_per_match", "clearances", "interceptions",
             "tackles"],
        height=True),
}


def load(duels, behaviour, family, db):
    cfg = FAMILIES[family]
    con = duckdb.connect(str(db), read_only=True)
    z = lambda p, names: ", ".join(f"{p}.dec_z_{n} AS {p}_{n}" for n in names)
    ctx = ", ".join(f"CAST(d.bool_{c} AS INTEGER) AS {c}" if c.startswith("ball_") else f"d.{c}" for c in cfg["ctx"])
    lag = "CAST(CAST(SUBSTR(d.str_season, 1, 4) AS INTEGER) - 1 AS VARCHAR) || '-' || SUBSTR(d.str_season, 1, 4)"
    h_cols = ", ha.h AS a_height, hf.h AS f_height, ha.h - hf.h AS height_diff" if cfg["height"] else ""
    h_join = ("LEFT JOIN (SELECT str_player_id, MAX(int_height) AS h FROM gold.gold_player_aerial_profile GROUP BY 1) ha "
              "ON ha.str_player_id = d.str_player_id "
              "LEFT JOIN (SELECT str_player_id, MAX(int_height) AS h FROM gold.gold_player_aerial_profile GROUP BY 1) hf "
              "ON hf.str_player_id = d.str_opp_player_id") if cfg["height"] else ""
    sql = f"""
    SELECT d.str_match_id, d.str_season, d.str_league_source, CAST(d.bool_is_won AS INTEGER) AS won, {ctx},
           {z('a', cfg['att'])}, {z('f', cfg['dfn'])} {h_cols}
    FROM {duels} d
    LEFT JOIN {behaviour} a ON a.str_player_id = d.str_player_id AND a.str_season = {lag}
    LEFT JOIN {behaviour} f ON f.str_player_id = d.str_opp_player_id AND f.str_season = {lag}
    {h_join}
    WHERE {cfg['where']} AND d.str_season >= '2016-2017' AND a.str_player_id IS NOT NULL AND f.str_player_id IS NOT NULL
    """
    df = con.sql(sql).df()
    con.close()
    df["y"] = df["str_season"].str[:4].astype(int)
    return df


def fit_predict(Xtr, ytr, Xte, constraints=None, seed=0):
    params = dict(n_estimators=400, learning_rate=0.03, num_leaves=15, min_child_samples=200, subsample=0.8,
                  subsample_freq=1, colsample_bytree=0.8, reg_lambda=5.0, random_state=seed, verbose=-1, n_jobs=2)
    if constraints is not None:
        params["interaction_constraints"] = constraints
    return lgb.LGBMClassifier(**params).fit(Xtr, ytr).predict_proba(Xte)[:, 1]


def logloss(y, p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def main(duels, behaviour, family, db=None, test_season=None, max_train=None, out_dir=None):
    import yaml
    cfg = FAMILIES[family]
    if db is None:
        db = ROOT / yaml.safe_load(open(ROOT / "config.yaml", encoding="utf-8"))["paths"]["duckdb"]
    df = load(duels, behaviour, family, db)
    att = [f"a_{n}" for n in cfg["att"]] + (["a_height"] if cfg["height"] else [])
    dfn = [f"f_{n}" for n in cfg["dfn"]] + (["f_height"] if cfg["height"] else [])
    ctx = list(cfg["ctx"])
    extra = ["height_diff"] if cfg["height"] else []     # écart de taille : interaction par construction, hors contraintes
    feats = att + dfn + ctx
    idx = {c: i for i, c in enumerate(feats)}
    cons = [[idx[c] for c in att + ctx], [idx[c] for c in dfn + ctx]]
    print(f"[{family}] {len(df):,} duels (attaquant et défenseur profilés), taux de réussite {df.won.mean():.3f}")
    out_dir = Path(out_dir or ROOT / "reports" / "chunks")
    out_dir.mkdir(parents=True, exist_ok=True)
    for ty in ([test_season] if test_season else range(2019, 2026)):
        part = out_dir / f"duel_matchup_{family}_{ty}.parquet"
        if part.exists():
            continue
        tr, te = df[df.y < ty], df[(df.y == ty) & df.str_league_source.isin(BIG5)].copy()
        if max_train and len(tr) > max_train:
            tr = tr.sample(max_train, random_state=0)
        te["p_ctx"] = fit_predict(tr[ctx], tr.won, te[ctx])
        te["p_add"] = fit_predict(tr[feats], tr.won, te[feats], cons)
        te["p_free"] = fit_predict(tr[feats], tr.won, te[feats])
        if extra:
            te["p_diff"] = fit_predict(tr[feats + extra], tr.won, te[feats + extra])
        te.to_parquet(part)
        print(f"pli {ty} écrit : {len(te):,} duels de test, {len(tr):,} d'apprentissage")
    if test_season:
        return None
    return aggregate(family, out_dir)


def aggregate(family, out_dir):
    cfg = FAMILIES[family]
    extra = ["height_diff"] if cfg["height"] else []
    files = sorted(Path(out_dir).glob(f"duel_matchup_{family}_*.parquet"))
    r = pd.concat([pd.read_parquet(f) for f in files])
    for k in ["p_ctx", "p_add", "p_free"] + (["p_diff"] if extra else []):
        r["ll_" + k[2:]] = logloss(r.won.values, r[k].values)
    print(f"Big5 2019-2020 à 2025-2026 : {len(r):,} duels")
    print("log-loss : " + " | ".join(f"{c[3:]} {r[c].mean():.5f}" for c in r.columns if c.startswith("ll_")))
    comps = [("profils additifs contre contexte seul", "ll_ctx", "ll_add"),
             ("INTERACTION (libre contre additif)", "ll_add", "ll_free")]
    if extra:
        comps.append(("écart de taille ajouté (contre additif)", "ll_add", "ll_diff"))
    for nm, a, b in comps:
        gain = (r[a] - r[b])
        g = gain.groupby(r.str_match_id).sum().values
        rng = np.random.default_rng(0)
        bs = np.array([g[rng.integers(0, len(g), len(g))].sum() / len(r) for _ in range(1000)])
        by = gain.groupby(r.y).mean()
        print(f"{nm} : gain {gain.mean():+.5f} IC95 [{np.percentile(bs, 2.5):+.5f} ; {np.percentile(bs, 97.5):+.5f}] ; "
              f"saisons gagnantes {(by > 0).sum()} / {len(by)}")
    return r


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Interaction attaquant x défenseur sur les duels")
    ap.add_argument("--family", choices=list(FAMILIES), default="dribble")
    ap.add_argument("--duels", default=None)
    ap.add_argument("--behaviour", default="gold.gold_player_behaviour_profile")
    ap.add_argument("--test-season", type=int, default=None, help="un seul pli (année de début de la saison testée)")
    ap.add_argument("--aggregate", action="store_true", help="bilan des plis déjà écrits")
    ap.add_argument("--max-train", type=int, default=500000)
    ap.add_argument("--out-dir", default=None)
    a = ap.parse_args()
    default = {"dribble": "intermediate.intermediate_ground_duel_players", "aerial": "intermediate.intermediate_aerial_duel_players"}
    if a.aggregate:
        aggregate(a.family, a.out_dir or ROOT / "reports" / "chunks")
    else:
        main(a.duels or default[a.family], a.behaviour, a.family, test_season=a.test_season,
             max_train=a.max_train, out_dir=a.out_dir)
