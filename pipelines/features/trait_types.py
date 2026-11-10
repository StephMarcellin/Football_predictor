"""Types de joueurs par TRAITS de jeu (toutes positions mêlées) -> machine_learning.player_trait_types.

Idée du PMO : décrire COMMENT un joueur joue (jeu de tête, sacrifice défensif, duels au sol, création, distribution),
pas où il joue. Un défenseur et un attaquant forts de la tête peuvent donc partager le même type ; le poste et les
coordonnées restent une information séparée.

Méthode :
  · une famille de traits = un jeu de z-scores de gold.gold_player_behaviour_profile, SANS variable de position
    (ni secteurs d'occupation, ni flanc, ni parts de passes par tiers) ;
  · z-scores relatifs au poste (famille de poste, saison, ligue), voir le profil : un type exprime donc « fort de la
    tête pour son poste », le poste étant connu à côté. Sans cela, les types retrouveraient le poste ;
  · un k-means par famille, sur tous les joueurs de champ ensemble (gardiens exclus), centres figés sur les fenêtres
    terminant de 2016-2017 à 2018-2019 (aucune information des saisons à prédire à partir de 2019-2020) ;
  · k = 3 par famille : la persistance (kappa entre fenêtres sans recouvrement s / s+3, 6 484 paires) est de 0,32 (duels au
    sol) à 0,38 (création, distribution) à k = 3 et baisse avec k. Les types sont des étiquettes lisibles, pas des variables
    plus informatives que les z-scores continus : sur les duels aériens, les types gardent 90 % du gain des z-scores
    (+0,0296 contre +0,0329 de log-loss).
  · seuls les joueurs avec au moins 450 minutes sur la fenêtre (bool_is_peer) reçoivent des types.

Contrôle : l'information mutuelle normalisée entre type et poste est de 0,001 à 0,003 (0 = type indépendant du poste).
Le dernier homme (LastMan) est écarté : stabilité 0,08 en z-score, bruit.

Sorties : player_trait_types (un type par famille, distance au centre) et player_trait_type_labels (description
automatique de chaque type par ses écarts les plus marqués : description de données, pas une étiquette validée).

Usage :  python pipelines/features/trait_types.py [--dry-run] [--source "read_parquet('b.parquet')"]
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

SRC_DEFAULT = "gold.gold_player_behaviour_profile"
OUT_SCHEMA, OUT_TYPES, OUT_LABELS = "machine_learning", "player_trait_types", "player_trait_type_labels"
FIT_FIRST_YEAR, FIT_LAST_YEAR = 2016, 2018
N_TYPES = 3
SEED = 0
KEY = ["str_player_id", "str_season"]

FAMILIES = {
    "aerien": ["aerial_duels", "aerial_win_rate", "a_woe_net_off", "a_woe_net_def", "a_head_shots_per_match"],
    "sacrifice": ["shots_blocked", "passes_blocked", "crosses_blocked", "def_actions_own_box", "def_own_box_share",
                  "clearances", "tackles", "interceptions"],
    "duel_sol": ["g_dribbles_per_90", "g_dribble_woe_adj", "g_defended_woe_adj", "g_dispossessed_share",
                 "g_fouls_committed_per_90", "g_fouls_drawn_per_90", "g_yellow_per_foul", "tackle_win_rate",
                 "challenges", "recoveries"],
    "creation": ["key_passes", "chances_created", "xt_created", "crosses", "throughballs", "xg_contribution", "shots"],
    "distribution": ["passes_open", "pass_accuracy", "pass_forward_share", "pass_long_share", "pass_short_share",
                     "pass_length_mean", "progressive_passes", "longballs", "xt_per_pass", "passes_into_final_third",
                     "touches"],
}


def describe_center(center, cols, n_hi=3, n_lo=2):
    s = pd.Series(center, index=[c[len("dec_z_"):] for c in cols]).sort_values()
    hi = ", ".join(f"{i} {v:+.1f}" for i, v in s.tail(n_hi)[::-1].items())
    lo = ", ".join(f"{i} {v:+.1f}" for i, v in s.head(n_lo).items())
    return f"haut : {hi} | bas : {lo}"


def fit_family(df, fam, cols, n_types=N_TYPES, seed=SEED):
    """k-means sur les lignes d'apprentissage, assignation de toutes les lignes ; numéro de type stabilisé par la somme
    croissante des z du centre (type 0 = le plus faible sur la famille)."""
    fit = df[df["bool_fit"]]
    km = KMeans(n_types, n_init=10, random_state=seed).fit(fit[cols].fillna(0.0).values)
    order = np.argsort(km.cluster_centers_.sum(axis=1))
    rank = np.empty(n_types, dtype=int)
    rank[order] = np.arange(n_types)
    X = df[cols].fillna(0.0).values
    dist = np.linalg.norm(X[:, None, :] - km.cluster_centers_[None, :, :], axis=2)
    t = rank[dist.argmin(axis=1)]
    labels = pd.DataFrame({"str_family": fam, "int_type": rank,
                           "str_description": [describe_center(km.cluster_centers_[i], cols) for i in range(n_types)]})
    return t, dist.min(axis=1), labels


def build(df):
    """df : gold_player_behaviour_profile. Retourne (types, labels)."""
    df = df[df["bool_is_peer"] & df["str_macro_role"].notna() & (df["str_macro_role"] != "GK")].copy()
    df["bool_fit"] = df["str_season"].str[:4].astype(int).between(FIT_FIRST_YEAR, FIT_LAST_YEAR)
    out = df[KEY + ["str_macro_role"]].copy()
    labels = []
    for fam, feats in FAMILIES.items():
        cols = [f"dec_z_{f}" for f in feats]
        t, dist, lab = fit_family(df, fam, cols)
        out[f"int_type_{fam}"], out[f"dec_dist_{fam}"] = t, dist
        labels.append(lab)
    return out.reset_index(drop=True), pd.concat(labels, ignore_index=True)


def main(source=SRC_DEFAULT, write=True):
    import duckdb
    import yaml
    from loguru import logger
    root = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
    with open(root / "config.yaml", encoding="utf-8") as f:
        db_path = root / yaml.safe_load(f)["paths"]["duckdb"]
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(f"SELECT * FROM {source}").df()
    con.close()
    types, labels = build(df)
    logger.info(f"{len(types)} joueurs-saisons typés sur {len(FAMILIES)} familles")
    if not write:
        logger.info("dry-run : rien n'est écrit")
        return types, labels
    con = duckdb.connect(str(db_path))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA}")
    con.register("types_df", types)
    con.register("labels_df", labels)
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_TYPES} AS SELECT * FROM types_df")
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_LABELS} AS SELECT * FROM labels_df")
    con.close()
    logger.info(f"écrit {OUT_SCHEMA}.{OUT_TYPES} et {OUT_SCHEMA}.{OUT_LABELS}")
    return types, labels


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Types de joueurs par traits -> machine_learning.player_trait_types")
    ap.add_argument("--source", default=SRC_DEFAULT)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(source=args.source, write=not args.dry_run)
