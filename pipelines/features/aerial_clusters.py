"""Clusters de joueurs selon leur profil aérien -> machine_learning.aerial_player_clusters.

Lit gold.gold_player_aerial_profile (profil sur 3 saisons glissantes), forme 6 groupes par
k-means sur des variables standardisées, écrit machine_learning.aerial_player_clusters
(CREATE OR REPLACE). Étape Python APRÈS dbt_gold_base : aucun modèle dbt ne la lit pour
l'instant.

Variables : victoire au-dessus de l'attendu en attaque et en défense, part de duels offensifs,
duels par match, tirs de la tête par duel offensif, part de dégagements, taille. La position
n'entre PAS dans les variables : elle sert à lire les groupes.

Limites (prototype du 07/11/2026) : silhouette de 0,18 à 0,26 : les joueurs forment un
continuum, les groupes sont une typologie lisible et non des catégories étanches.
Le numéro de groupe est stabilisé (tri par victoire au-dessus de l'attendu décroissante) ;
l'étiquette est attribuée par règles de rang sur les centres des groupes, pas apprise.

Usage :  python pipelines/features/aerial_clusters.py
         python pipelines/features/aerial_clusters.py --dry-run   (calcule et contrôle, n'écrit rien)
"""
import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yaml
from loguru import logger
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]

SRC = "gold.gold_player_aerial_profile"
OUT_SCHEMA, OUT_TABLE = "machine_learning", "aerial_player_clusters"

MIN_DUELS = 80                  # en dessous, le profil est trop bruité
FIRST_SEASON = "2016-2017"      # avant, la fenêtre de 3 saisons est incomplète
N_CLUSTERS = 6
SEED = 0
FEATURES = ["dec_woe_off", "dec_woe_def", "dec_share_off", "dec_duels_per_match",
            "dec_head_shot_rate", "dec_follow_clear_share", "int_height"]
KEY = ["str_player_id", "str_season"]


def label_clusters(cent):
    """Étiquettes des groupes d'après leurs centres (unités d'origine). Règles de RANG entre les
    groupes d'une même famille, pas de seuil fixe : les centres bougent quand les données
    changent, le rang reste stable. Famille « attaquant » : part de duels offensifs > 0,55."""
    lab = {}
    att = [i for i in cent.index if cent.loc[i, "dec_share_off"] > 0.55]
    dfn = [i for i in cent.index if i not in att]
    if att:
        top = max(att, key=lambda i: cent.loc[i, "dec_duels_per_match"])
        lab[top] = "avant-centre de présence"
        rest = [i for i in att if i != top]
        woe = {i: cent.loc[i, "dec_woe_off"] + cent.loc[i, "dec_woe_def"] for i in rest}
        if len(rest) >= 2:
            worst = min(rest, key=woe.get)
            for i in rest:
                lab[i] = "attaquant qui perd ses duels" if i == worst else "avant-centre moyen"
        else:
            for i in rest:
                lab[i] = "attaquant qui perd ses duels" if woe[i] < -0.05 else "avant-centre moyen"
    if dfn:
        dom = max(dfn, key=lambda i: cent.loc[i, "dec_woe_def"])
        rest = [i for i in dfn if i != dom]
        if cent.loc[dom, "dec_woe_def"] > 0.05:
            lab[dom] = "défenseur dominant"
        else:
            rest, clr_pool = dfn, dfn
        if rest:
            clr = max(rest, key=lambda i: cent.loc[i, "dec_follow_clear_share"])
            for i in rest:
                lab[i] = ("défenseur qui dégage" if i == clr and (len(rest) >= 2 or cent.loc[i, "dec_follow_clear_share"] > 0.2)
                          else "faible présence aérienne")
    return [lab[i] for i in cent.index]


def fit_assign(df, n_clusters=N_CLUSTERS, seed=SEED):
    """Forme les groupes et les attribue. Retourne (table de sortie, centres en unités d'origine)."""
    d = df[(df["int_n_duels"] >= MIN_DUELS) & (df["str_season"] >= FIRST_SEASON)].dropna(subset=FEATURES).copy()
    if len(d) < n_clusters * 20:
        raise RuntimeError(f"Trop peu de profils ({len(d)}) pour {n_clusters} groupes.")
    scaler = StandardScaler().fit(d[FEATURES])
    X = scaler.transform(d[FEATURES])
    km = KMeans(n_clusters, n_init=20, random_state=seed).fit(X)
    cent = pd.DataFrame(scaler.inverse_transform(km.cluster_centers_), columns=FEATURES)
    order = (cent["dec_woe_off"] + cent["dec_woe_def"]).sort_values(ascending=False).index
    rank = {int(old): new for new, old in enumerate(order)}
    labels = label_clusters(cent)
    dup = {x for x in labels if labels.count(x) > 1}
    labels = [f"{x} ({rank[i]})" if x in dup else x for i, x in enumerate(labels)]
    d["int_cluster"] = [rank[int(i)] for i in km.labels_]
    d["str_cluster_label"] = [labels[int(i)] for i in km.labels_]
    d["dec_dist_to_centroid"] = np.linalg.norm(X - km.cluster_centers_[km.labels_], axis=1)
    out = d[KEY + ["int_cluster", "str_cluster_label", "dec_dist_to_centroid", "int_n_duels"]].reset_index(drop=True)
    cent["int_cluster"] = [rank[i] for i in range(n_clusters)]
    cent["str_cluster_label"] = labels
    cent["int_n_profiles"] = np.bincount(km.labels_, minlength=n_clusters)
    return out, cent.sort_values("int_cluster").reset_index(drop=True)


def main(write=True):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    cols = ", ".join(KEY + ["int_n_duels"] + FEATURES)
    df = con.sql(f"SELECT {cols} FROM {SRC}").df()
    con.close()
    out, cent = fit_assign(df)
    if out.duplicated(KEY).any():
        raise RuntimeError(f"{OUT_TABLE} : clé (str_player_id, str_season) non unique.")
    logger.info(f"{len(out):,} profils classés (au moins {MIN_DUELS} duels, depuis {FIRST_SEASON}).")
    for r in cent.itertuples():
        logger.info(f"  groupe {r.int_cluster} : {r.str_cluster_label} ({r.int_n_profiles:,} profils)")
    if not write:
        logger.info("--dry-run : aucune écriture.")
        return out
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA}")
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_TABLE} AS SELECT * FROM out")
    n = con.execute(f"SELECT count(*) FROM {OUT_SCHEMA}.{OUT_TABLE}").fetchone()[0]
    con.close()
    logger.info(f"{OUT_SCHEMA}.{OUT_TABLE} écrite : {n:,} lignes.")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Clusters de profils aériens -> machine_learning.aerial_player_clusters")
    ap.add_argument("--dry-run", action="store_true", help="calcule et contrôle sans écrire dans la base")
    a = ap.parse_args()
    main(write=not a.dry_run)
