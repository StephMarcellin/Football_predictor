"""Modèle de confrontation aérienne -> machine_learning.aerial_duel_matchup (+ coefficients).

Pour chaque duel aérien (vu du côté de l'ATTAQUANT : une ligne par duel), estime la probabilité que
l'attaquant gagne, d'après :
  · le contexte du duel (type de ballon, zone, minute, couloir),
  · l'écart de taille attaquant - défenseur,
  · la compétence NETTE DE LA TAILLE de l'attaquant (dec_woe_net_off) et du défenseur adverse
    (dec_woe_net_def), lues sur la ligne de la saison PRÉCÉDENTE de gold.gold_player_aerial_profile
    (la fenêtre de ce profil inclut sa propre saison : lire s-1 pour un duel de la saison s évite
    toute fuite).

Étape Python APRÈS dbt_gold_base : le profil doit être reconstruit avant.

Mesure du 09/11/2026 (prototype sur la compétence brute, pas nette ; entraînement 2017-18 à
2021-22, test 2022-23 à 2025-26) : le contexte seul n'apporte presque rien (+0,0013 de log-loss),
l'écart de taille +0,0212, le profil +0,0255, profil + taille +0,0323 (AUC 0,650). Le poids, la
taille au carré et les interactions taille x type de ballon n'ajoutent rien (<= +0,0005).
Le step recalcule ces mesures sur les nouvelles variables et les écrit dans les journaux.

Limites : la table écrite contient des probabilités EN ÉCHANTILLON pour les saisons d'entraînement
(2017-18 et suivantes) ; pour un usage comme variable d'un modèle de match, utiliser le profil
(déjà décalé d'une saison) ou refaire un calcul hors échantillon. La taille est la valeur courante
de la fiche WhoScored. Joueurs sans profil la saison précédente : compétence mise à 0 avec un
indicateur. Variables d'échelle : écart de taille en 10 cm, compétences en 10 points de taux.

Usage :  python pipelines/features/aerial_matchup.py
         python pipelines/features/aerial_matchup.py --dry-run   (calcule et contrôle, n'écrit rien)
"""
import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yaml
from loguru import logger
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]

PROFILE = "gold.gold_player_aerial_profile"
OUT_SCHEMA, OUT_TABLE, COEF_TABLE = "machine_learning", "aerial_duel_matchup", "aerial_matchup_coefs"

FIRST_SEASON = "2015-2016"        # premières saisons dont le pairage est fiable (2013-14 exclue)
FIRST_TRAIN_SEASON = "2017-2018"  # avant, le profil de la saison précédente est trop mince
LAST_TRAIN_SEASON = "2021-2022"   # séparation entraînement / test pour la mesure hors échantillon
C = 10.0
KEY = ["str_match_id", "str_team_id", "int_event_id", "int_opp_event_id"]
CONTEXT_CATS = ["str_ball_type", "x_bin", "str_zone", "mn_bin", "str_cross_lane"]
FULL_NUM = ["woe_att", "woe_opp", "has_prior_att", "has_prior_opp", "hdiff", "h_missing"]

SQL_DUELS = f"""
WITH ph AS (
    SELECT str_player_id, MAX(CASE WHEN int_height > 150 THEN int_height END) AS h
    FROM intermediate.intermediate_whoscored_player_team_season
    GROUP BY str_player_id
)
SELECT d.str_match_id, d.str_team_id, d.int_event_id, d.int_opp_event_id, d.str_season,
       d.str_player_id, d.str_opp_player_id, d.str_ball_type, d.dec_x, d.int_expanded_minute,
       d.str_zone, d.str_cross_lane, CAST(d.bool_is_won AS INTEGER) AS won,
       a.h AS own_h, b.h AS opp_h
FROM intermediate.intermediate_aerial_duel_players d
LEFT JOIN ph a ON a.str_player_id = d.str_player_id
LEFT JOIN ph b ON b.str_player_id = d.str_opp_player_id
WHERE d.str_role = 'offensif' AND d.str_season >= '{FIRST_SEASON}'
"""


def previous_season(season):
    y = int(season[:4])
    return f"{y - 1}-{y}"


def build_features(duels, profile):
    """Table de variables : contexte discret + écart de taille + compétences nettes de s-1."""
    d = duels.copy()
    d["prev"] = d["str_season"].map(previous_season)
    p = profile[["str_player_id", "str_season", "dec_woe_net_off", "dec_woe_net_def"]]
    d = d.merge(p[["str_player_id", "str_season", "dec_woe_net_off"]]
                .rename(columns={"str_season": "prev", "dec_woe_net_off": "woe_att"}),
                on=["str_player_id", "prev"], how="left")
    d = d.merge(p[["str_player_id", "str_season", "dec_woe_net_def"]]
                .rename(columns={"str_player_id": "str_opp_player_id", "str_season": "prev",
                                 "dec_woe_net_def": "woe_opp"}),
                on=["str_opp_player_id", "prev"], how="left")
    d["has_prior_att"] = d["woe_att"].notna().astype(float)
    d["has_prior_opp"] = d["woe_opp"].notna().astype(float)
    d["woe_att"] = d["woe_att"].fillna(0.0) * 10
    d["woe_opp"] = d["woe_opp"].fillna(0.0) * 10
    d["h_missing"] = (d["own_h"].isna() | d["opp_h"].isna()).astype(float)
    d["hdiff"] = ((d["own_h"] - d["opp_h"]) / 10).fillna(0.0)
    d["x_bin"] = (d["dec_x"] // 10).clip(4, 9).fillna(4).astype(int).astype(str)
    d["mn_bin"] = pd.cut(d["int_expanded_minute"], [-1, 15, 30, 45, 60, 75, 400], labels=False).astype("Int64").astype(str)
    for c in ("str_ball_type", "str_zone", "str_cross_lane"):
        d[c] = d[c].fillna("na").astype(str)
    return d.drop(columns=["prev"]).reset_index(drop=True)


def design(df, num, columns=None):
    X = pd.concat([pd.get_dummies(df[c], prefix=c, dtype=np.float32) for c in CONTEXT_CATS]
                  + [df[num].astype(np.float32)], axis=1)
    if columns is not None:
        X = X.reindex(columns=columns, fill_value=0.0)
    return X


def _fit(df, num):
    X = design(df, num)
    m = LogisticRegression(C=C, max_iter=500).fit(X, df["won"])
    return m, list(X.columns)


def evaluate(f):
    """Mesure hors échantillon : entraînement jusqu'à LAST_TRAIN_SEASON, test après. None si pas de test."""
    tr = f[(f["str_season"] >= FIRST_TRAIN_SEASON) & (f["str_season"] <= LAST_TRAIN_SEASON)]
    te = f[f["str_season"] > LAST_TRAIN_SEASON]
    if len(tr) < 1000 or len(te) < 1000:
        return None
    res = {"n_train": len(tr), "n_test": len(te)}
    for name, num in [("contexte", []), ("taille", ["hdiff", "h_missing"]), ("complet", FULL_NUM)]:
        m, cols = _fit(tr, num)
        p = m.predict_proba(design(te, num, cols))[:, 1]
        res[name] = (log_loss(te["won"], p), roc_auc_score(te["won"], p))
    return res


def fit_predict(f):
    """Ajuste le modèle complet sur les saisons >= FIRST_TRAIN_SEASON et prédit toutes les lignes.
    Retourne (table de sortie, table des coefficients, mesures hors échantillon)."""
    tr = f[f["str_season"] >= FIRST_TRAIN_SEASON]
    if len(tr) < 1000:
        raise RuntimeError(f"Trop peu de duels pour ajuster le modèle ({len(tr)}).")
    metrics = evaluate(f)
    m, cols = _fit(tr, FULL_NUM)
    p = m.predict_proba(design(f, FULL_NUM, cols))[:, 1]
    out = f[KEY + ["str_season", "str_player_id", "str_opp_player_id"]].copy()
    out["bool_attacker_won"] = f["won"].astype(bool)
    out["dec_p_attacker_win"] = p
    out["dec_height_diff_cm"] = np.where(f["h_missing"] > 0, np.nan, f["hdiff"] * 10)
    out["bool_is_fitted_season"] = (f["str_season"] >= FIRST_TRAIN_SEASON).values
    coefs = pd.DataFrame({"str_feature": ["intercept"] + cols,
                          "dec_coef": [float(m.intercept_[0])] + [float(x) for x in m.coef_[0]]})
    return out.reset_index(drop=True), coefs, metrics


def log_metrics(metrics):
    if metrics is None:
        logger.warning("Pas assez de duels pour la mesure hors échantillon.")
        return
    base = metrics["contexte"][0]
    logger.info(f"Hors échantillon ({metrics['n_train']:,} duels d'entraînement, {metrics['n_test']:,} de test) :")
    for name in ("contexte", "taille", "complet"):
        ll, auc = metrics[name]
        logger.info(f"  {name:9s} log-loss {ll:.5f}  gain vs contexte {base - ll:+.5f}  AUC {auc:.3f}")


def main(write=True):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    duels = con.sql(SQL_DUELS).df()
    profile = con.sql(f"SELECT str_player_id, str_season, dec_woe_net_off, dec_woe_net_def FROM {PROFILE}").df()
    con.close()
    f = build_features(duels, profile)
    del duels
    if f.duplicated(KEY).any():
        raise RuntimeError(f"{OUT_TABLE} : clé {KEY} non unique.")
    out, coefs, metrics = fit_predict(f)
    logger.info(f"{len(out):,} duels notés, saisons {out['str_season'].min()} à {out['str_season'].max()}.")
    log_metrics(metrics)
    for r in coefs[coefs["str_feature"].isin(["hdiff", "woe_att", "woe_opp"])].itertuples():
        logger.info(f"  coefficient {r.str_feature} : {r.dec_coef:+.3f}")
    if not write:
        logger.info("--dry-run : aucune écriture.")
        return out
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA}")
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_TABLE} AS SELECT * FROM out")
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{COEF_TABLE} AS SELECT * FROM coefs")
    n = con.execute(f"SELECT count(*) FROM {OUT_SCHEMA}.{OUT_TABLE}").fetchone()[0]
    con.close()
    logger.info(f"{OUT_SCHEMA}.{OUT_TABLE} écrite : {n:,} lignes ; {OUT_SCHEMA}.{COEF_TABLE} : {len(coefs)} coefficients.")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Modèle de confrontation aérienne -> machine_learning.aerial_duel_matchup")
    ap.add_argument("--dry-run", action="store_true", help="calcule et contrôle sans écrire dans la base")
    a = ap.parse_args()
    main(write=not a.dry_run)
