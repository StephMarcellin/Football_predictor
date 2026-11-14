"""Validation sur cible de SECTEUR : la composition des joueurs prédit-elle la menace créée sur un flanc, mieux que
l'historique des équipes seul ?

Cible : xT positif créé par l'équipe attaquante dans la moitié adverse (col_to >= 8 sur 16), par flanc (L : rows 0-4,
C : rows 5-6, R : rows 7-11 de la grille xT), un match et une équipe.
Référence (base) : moyenne glissante des 20 matchs précédents de l'équipe sur ce flanc (attaque), de l'adversaire sur le
flanc qui lui fait face (défense : menace concédée), totaux sur les trois flancs, domicile.
Ajout (comp) : cellules de la compo de départ (tools/lineup_cells.py) : attaque du flanc, défense propre du flanc,
milieu, et en face la défense du flanc miroir et le milieu adverse.
Protocole : origine glissante, test = saison 2019-2020 à 2025-2026 (Big5), apprentissage = tous les matchs d'avant la
saison testée. LightGBM peu profond. Gain = baisse de l'erreur quadratique ; IC95 par rééchantillonnage des matchs.
GO si IC95 exclut 0 ET gain sur >= 5 saisons sur 7.

Usage : python tools/sector_validation.py --target flank_target.parquet --cells lineup_cells.parquet
"""
import argparse
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BIG5 = ["Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1"]
FLANKS = ["L", "C", "R"]
MIRROR = {"L": "R", "C": "C", "R": "L"}
ROLL, MIN_PRIOR = 20, 8
ATT_D = ["shots", "xg_contribution", "key_passes", "xt_created", "g_dribbles_per_90", "crosses"]
DEF_D = ["tackles", "interceptions", "aerial_win_rate", "clearances", "g_defended_woe_adj"]
BUILD_D = ["xt_created", "pass_forward_share", "progressive_passes", "pass_accuracy"]


def load_context(db):
    con = duckdb.connect(str(db), read_only=True)
    ctx = con.sql("""SELECT str_match_id, str_team_id, str_opponent_id, dt_date, str_season, str_league_source, int_is_home
                     FROM gold.gold_team_match""").df()
    con.close()
    return ctx


def long_target(target, ctx):
    t = target.melt(id_vars=["str_match_id", "str_team_id"], value_vars=["xt_left", "xt_centre", "xt_right"],
                    var_name="flank", value_name="y")
    t["flank"] = t["flank"].map({"xt_left": "L", "xt_centre": "C", "xt_right": "R"})
    t["y"] = t["y"].fillna(0.0)
    return t.merge(ctx, on=["str_match_id", "str_team_id"], how="inner")


def add_priors(d):
    """Moyennes glissantes (20 matchs précédents), par équipe et flanc, dans le repère de l'équipe :
    att_prior = menace créée sur le flanc ; def_prior = menace subie sur le flanc. Puis, pour la ligne (A, f), la
    défense de l'adversaire B qui lui fait face : def_prior de B sur son flanc miroir de f."""
    d = d.sort_values(["dt_date", "str_match_id"]).copy()
    # menace subie par A sur son flanc f = menace créée par l'adversaire sur MIRROR[f]
    sub = d[["str_match_id", "str_team_id", "flank", "y"]].copy()
    sub["flank"] = sub["flank"].map(MIRROR)
    sub = sub.rename(columns={"str_team_id": "str_opponent_id", "y": "conceded"})
    d = d.merge(sub, on=["str_match_id", "str_opponent_id", "flank"], how="left")
    d = d.sort_values(["dt_date", "str_match_id"])
    for col, out in (("y", "att_prior"), ("conceded", "def_prior")):
        d[out] = d.groupby(["str_team_id", "flank"], sort=False)[col].transform(
            lambda x: x.shift(1).rolling(ROLL, min_periods=MIN_PRIOR).mean())
    d["att_total"] = d.groupby(["str_match_id", "str_team_id"])["att_prior"].transform("sum")
    d["def_total"] = d.groupby(["str_match_id", "str_team_id"])["def_prior"].transform("sum")
    opp = d[["str_match_id", "str_team_id", "flank", "def_prior", "def_total"]].rename(
        columns={"str_team_id": "str_opponent_id", "flank": "flank_opp", "def_prior": "opp_def_prior",
                 "def_total": "opp_def_total"})
    d["flank_opp"] = d["flank"].map(MIRROR)
    return d.merge(opp, on=["str_match_id", "str_opponent_id", "flank_opp"], how="left")


def cell_wide(cells):
    """Une ligne par (match, équipe) ; colonnes '<mesure>||<ligne>|<flanc>'."""
    cols = ["int_n_players"] + [x for x in cells.columns if x.startswith("dec_cell_")]
    p = cells.pivot_table(index=["str_match_id", "str_team_id"], columns=["str_line", "str_flank"], values=cols)
    p.columns = [f"{value}||{line}|{flank}" for value, line, flank in p.columns]
    return p.reset_index()


def comp_features(d, wide):
    """Pour chaque ligne (équipe A, flanc f) : cellules propres et adverses mises en regard."""
    w = wide.set_index(["str_match_id", "str_team_id"])
    keys_own = pd.MultiIndex.from_frame(d[["str_match_id", "str_team_id"]])
    keys_opp = pd.MultiIndex.from_frame(d[["str_match_id", "str_opponent_id"]])
    out = pd.DataFrame(index=d.index)

    def col(name, keys, line, flank):
        c = f"{name}||{line}|{flank}"
        s = w[c] if c in w.columns else pd.Series(np.nan, index=w.index)
        return s.reindex(keys).values

    for flank in FLANKS:
        m = (d["flank"] == flank).values
        mir = MIRROR[flank]
        # menace créée par A sur son flanc f ; la défense qui lui fait face est le flanc miroir de l'adversaire
        feats = {}
        for x in ATT_D:
            feats[f"att_{x}"] = col(f"dec_cell_{x}", keys_own, "ATT", flank)
        feats["att_n"] = col("int_n_players", keys_own, "ATT", flank)
        for x in BUILD_D:
            feats[f"defown_{x}"] = col(f"dec_cell_{x}", keys_own, "DEF", flank)
            feats[f"mid_{x}"] = _nanmean_rows(np.vstack([col(f"dec_cell_{x}", keys_own, "MID", f) for f in FLANKS]))
        feats["defown_n"] = col("int_n_players", keys_own, "DEF", flank)
        for x in DEF_D:
            feats[f"opp_def_{x}"] = col(f"dec_cell_{x}", keys_opp, "DEF", mir)
            feats[f"opp_mid_{x}"] = _nanmean_rows(np.vstack([col(f"dec_cell_{x}", keys_opp, "MID", f) for f in FLANKS]))
        feats["opp_def_n"] = col("int_n_players", keys_opp, "DEF", mir)
        for k, v in feats.items():
            if k not in out:
                out[k] = np.nan
            out.loc[m, k] = v[m]
    return out


def _nanmean_rows(a):
    """Moyenne sur les lignes en ignorant les NaN ; NaN si toute la colonne est vide (sans avertissement)."""
    n = (~np.isnan(a)).sum(axis=0)
    return np.where(n > 0, np.nansum(a, axis=0) / np.maximum(n, 1), np.nan)


def fit_predict(Xtr, ytr, Xte, seed=0):
    m = lgb.LGBMRegressor(n_estimators=300, learning_rate=0.03, num_leaves=15, min_child_samples=100,
                          subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=5.0,
                          random_state=seed, verbose=-1, n_jobs=2)
    m.fit(Xtr, ytr)
    return m.predict(Xte)


def main(target_path, cells_path, db=None, out=None):
    import yaml
    cfg = yaml.safe_load(open(ROOT / "config.yaml", encoding="utf-8"))
    ctx = load_context(db or ROOT / cfg["paths"]["duckdb"])
    d = add_priors(long_target(pd.read_parquet(target_path), ctx))
    d = d[d["str_season"] >= "2014-2015"].copy()
    comp = comp_features(d, cell_wide(pd.read_parquet(cells_path)))
    d = pd.concat([d, comp], axis=1)
    d["flank_c"] = d["flank"].map({"L": 0, "C": 1, "R": 2})
    base = ["att_prior", "opp_def_prior", "att_total", "opp_def_total", "int_is_home", "flank_c"]
    compf = list(comp.columns)
    # écart de la compo du jour à la compo habituelle de l'équipe (20 matchs précédents) : ce que l'historique ne sait pas
    d = d.sort_values(["dt_date", "str_match_id"])
    dev = pd.DataFrame({f"dev_{c}": d[c] - d.groupby(["str_team_id", "flank"], sort=False)[c].transform(
        lambda x: x.shift(1).rolling(ROLL, min_periods=MIN_PRIOR).mean()) for c in compf}, index=d.index)
    # écart de l'adversaire à sa défense habituelle, déjà contenu dans les colonnes opp_* de l'équipe adverse :
    # on garde les écarts propres (att_, defown_, mid_) et on calcule ceux de l'adversaire par jointure miroir
    d = pd.concat([d, dev], axis=1)
    devf = list(dev.columns)
    d = d.dropna(subset=["att_prior", "opp_def_prior"]).copy()
    d["y_year"] = d["str_season"].str[:4].astype(int)
    rows = []
    for ty in range(2019, 2026):
        tr, te = d[d.y_year < ty], d[(d.y_year == ty) & d.str_league_source.isin(BIG5)].copy()
        te["p_base"] = fit_predict(tr[base], tr["y"], te[base])
        te["p_comp"] = fit_predict(tr[base + compf], tr["y"], te[base + compf])
        te["p_dev"] = fit_predict(tr[base + devf], tr["y"], te[base + devf])
        rows.append(te[["str_match_id", "str_team_id", "flank", "y", "p_base", "p_comp", "p_dev", "y_year"]])
    r = pd.concat(rows)
    r["se_base"], r["se_comp"], r["se_dev"] = (r.y - r.p_base) ** 2, (r.y - r.p_comp) ** 2, (r.y - r.p_dev) ** 2
    r["gain"] = r.se_base - r.se_comp
    r["gain_dev"] = r.se_base - r.se_dev
    var = ((r.y - r.y.mean()) ** 2).mean()
    print(f"n = {len(r):,} lignes (match, équipe, flanc), Big5 2019-2020 à 2025-2026")
    print(f"R2 base {1 - r.se_base.mean() / var:.4f} | R2 base+compo {1 - r.se_comp.mean() / var:.4f} | "
          f"gain relatif d'erreur quadratique {r.gain.mean() / r.se_base.mean() * 100:+.2f} %")
    per_match = r.groupby("str_match_id")["gain"].sum().values
    rng = np.random.default_rng(0)
    bs = np.array([per_match[rng.integers(0, len(per_match), len(per_match))].mean() for _ in range(2000)])
    print(f"gain moyen par match (somme sur équipes et flancs) {per_match.mean():+.5f}  IC95 [{np.percentile(bs, 2.5):+.5f} ; {np.percentile(bs, 97.5):+.5f}]")
    by = r.groupby("y_year").agg(se_base=("se_base", "mean"), se_comp=("se_comp", "mean"))
    by["gain_rel_%"] = (1 - by.se_comp / by.se_base) * 100
    print(by.round(5).to_string())
    print("saisons gagnantes :", int((by["gain_rel_%"] > 0).sum()), "/", len(by))
    pm_dev = r.groupby("str_match_id")["gain_dev"].sum().values
    bsd = np.array([pm_dev[rng.integers(0, len(pm_dev), len(pm_dev))].mean() for _ in range(2000)])
    byd = r.groupby("y_year").agg(b=("se_base", "mean"), d=("se_dev", "mean"))
    print(f"VARIANTE écarts à la compo habituelle : R2 {1 - r.se_dev.mean() / var:.4f} ; gain relatif {r.gain_dev.mean() / r.se_base.mean() * 100:+.2f} % ; "
          f"par match {pm_dev.mean():+.5f} IC95 [{np.percentile(bsd, 2.5):+.5f} ; {np.percentile(bsd, 97.5):+.5f}] ; "
          f"saisons gagnantes {int(((1 - byd.d / byd.b) > 0).sum())} / {len(byd)}")
    fl = r.groupby("flank")[["se_base", "se_comp"]].mean()
    print((1 - fl.se_comp / fl.se_base).mul(100).round(2).to_dict())
    if out:
        r.to_parquet(out)
    return r


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Validation sectorielle de la composition (menace par flanc)")
    ap.add_argument("--target", required=True)
    ap.add_argument("--cells", required=True)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    main(a.target, a.cells, out=a.out)
