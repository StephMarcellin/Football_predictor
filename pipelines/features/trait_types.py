"""Types de joueurs par TRAITS de jeu (toutes positions mêlées) -> machine_learning.player_trait_types.

Deux niveaux : un type par famille de capacités (8 familles) et un type GLOBAL sur toutes les capacités réunies.

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
  · type global : k-means à 8 types sur les 56 z-scores de toutes les familles (chaque famille pèse autant, z écrêtés à ±4) ;
    kappa de persistance 0,32 (0,42 à 4 types, 0,25 à 16), information mutuelle avec le poste 0,03 ;
  · seuls les joueurs avec au moins 450 minutes sur la fenêtre (bool_is_peer) reçoivent des types.

Contrôle : l'information mutuelle normalisée entre type et poste est de 0,001 à 0,003 (0 = type indépendant du poste).
Écartés (stabilité s / s+3 des z-scores) : dernier homme 0,08 ; erreurs menant à un tir ou à un but 0,06 à 0,09 ;
bruit. Finition : le style (distance, lieu, tête) est stable (0,5 à 0,7), la réussite (conversion 0,21, cadrage 0,22) non.

VERSION v2 (17/11/2026, défaut) : types plus nets, version v1 conservée (`--version v1`) pour reproduire les expériences antérieures.
  · les 7 variables dont la persistance s / s+3 est inférieure à 0,35 sont écartées du k-means des familles (UNSTABLE) : taux de tacles
    gagnés 0,09, cartons par faute 0,20, taux de tirs cadrés 0,21, conversion 0,23, duels défensifs ajustés 0,30, centres bloqués
    0,31, dribbles ajustés 0,32. Silhouette à k = 3 : duels au sol 0,10 -> 0,17, finition 0,22 -> 0,31, sacrifice 0,20 -> 0,25 ;
  · type global : k-means à 5 types sur les 8 aptitudes (AXES, moyenne de z de variables orientées « plus = davantage ») au lieu
    de 8 types sur 56 variables : silhouette 0,05 -> 0,16 et kappa 0,32 -> 0,37 (la dimension plus faible aide : à lire avec prudence) ;
  · dec_margin_<famille> = distance au centre le plus proche / distance au deuxième centre. Un joueur est « au cœur » d'un type si ce
    rapport est inférieur à CORE_RATIO (0,8) : 61 % des joueurs pour le type global, avec une silhouette de 0,27 et un kappa de 0,54.
    Les autres sont des profils intermédiaires, gardés dans la table mais à ne pas lire comme des représentants du type.

Sorties : player_trait_types (un type par famille, distance au centre, rapport de marge) et player_trait_type_labels (description
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
N_GLOBAL = 5            # v2 : 5 types sur les 8 aptitudes (v1 : N_GLOBAL_V1 = 8 sur 56 variables)
N_GLOBAL_V1 = 8
Z_CLIP = 4.0
VERSION_DEFAULT = "v2"
MIN_RELIABILITY = 0.35  # persistance s / s+3 minimale d'une variable pour entrer dans les types v2 et dans les points forts de la fiche
CORE_RATIO = 0.8        # « au cœur » d'un type : distance au centre le plus proche < 0,8 x distance au deuxième centre
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
    "finition": ["shots_in_box", "shots_outside_box", "shot_dist_mean", "shot_head_share", "shot_big_chance_share",
                 "shots_on_target", "goal_conversion", "shot_on_target_rate"],
    "coups_arretes": ["corners_taken", "freekicks_played", "freekicks_direct_shots", "penalties_taken"],
    "reseau": ["net_pass_share", "net_creative_rate", "net_betweenness"],
}


# Noms lisibles des types : (famille, numéro) -> (nom, variable-ancre). L'ancre est une variable qui DOIT figurer dans la description
# du type ; si les numéros changent après un nouvel entraînement, l'ancre ne correspond plus et l'application signale « à revalider ».
# Noms établis le 17/11/2026 d'après les centres figés (profils en z) et les joueurs les plus proches du centre.
TYPE_NAMES = {
    ("aerien", 0): ("Faible dans les airs", "aerial_win_rate"),
    ("aerien", 1): ("Gagnant aérien sélectif", "aerial_win_rate"),
    ("aerien", 2): ("Dominant aérien", "a_head_shots_per_match"),
    ("sacrifice", 0): ("Peu d'actions devant son but", "def_actions_own_box"),
    ("sacrifice", 1): ("Récupérateur (tacles, interceptions)", "tackles"),
    ("sacrifice", 2): ("Défenseur de sa surface", "def_actions_own_box"),
    ("duel_sol", 0): ("Évite les duels au sol", "g_dribbles_per_90"),
    ("duel_sol", 1): ("Dribbleur provocateur", "g_dribbles_per_90"),
    ("duel_sol", 2): ("Combatif en duel défensif", "challenges"),
    ("creation", 0): ("Peu offensif", "shots"),
    ("creation", 1): ("Menace de tir", "xg_contribution"),
    ("creation", 2): ("Créateur", "xt_created"),
    ("distribution", 0): ("Passeur court", "pass_short_share"),
    ("distribution", 1): ("Passeur long et direct", "pass_long_share"),
    ("distribution", 2): ("Passeur de volume", "passes_open"),
    ("finition", 0): ("Tireur rare", "shots_on_target"),
    ("finition", 1): ("Tireur de loin", "shots_outside_box"),
    ("finition", 2): ("Finisseur de surface", "shot_big_chance_share"),
    ("coups_arretes", 0): ("Ne tire pas les coups de pied arrêtés", "corners_taken"),
    ("coups_arretes", 1): ("Tireur de penaltys", "penalties_taken"),
    ("coups_arretes", 2): ("Tireur de corners et coups francs", "corners_taken"),
    ("reseau", 0): ("Périphérique dans le réseau de passes", "net_betweenness"),
    ("reseau", 1): ("Passeur créatif peu central", "net_creative_rate"),
    ("reseau", 2): ("Pivot du réseau de passes", "net_betweenness"),
    ("global", 0): ("Discret : peu de circulation et de centralité", "apt_centralite"),
    ("global", 1): ("Constructeur et récupérateur, tire peu", "apt_centralite"),
    ("global", 2): ("Attaquant créatif (tir, création, coups de pied arrêtés)", "apt_tir"),
    ("global", 3): ("Finisseur aérien (jeu de tête et tir)", "apt_aerien"),
    ("global", 4): ("Meneur de jeu (circulation, création, coups de pied arrêtés)", "apt_circulation"),
}


# Noms des 8 types globaux de la version v1 (56 variables), pour relire d'anciennes tables.
TYPE_NAMES_GLOBAL_V1 = {
    0: ("Joueur en retrait (peu de touches)", "net_betweenness"),
    1: ("Passeur court et sûr", "pass_accuracy"),
    2: ("Replié devant sa surface", "def_actions_own_box"),
    3: ("Centreur créateur", "xt_per_pass"),
    4: ("Menace aérienne dans la surface", "a_head_shots_per_match"),
    5: ("Meneur de circulation (gros volume de passes)", "passes_open"),
    6: ("Attaquant à gros volume de tirs", "shots_on_target"),
    7: ("Offensif complet (création, tir, coups de pied arrêtés)", "corners_taken"),
}


def type_name(family: str, type_id, description: str | None = None) -> str:
    """Nom lisible d'un type. Si la description est fournie et ne contient pas la variable-ancre, le nom est suffixé « (à revalider) »."""
    try:
        if family == "global" and description is not None and "apt_" not in description:
            name, anchor = TYPE_NAMES_GLOBAL_V1[int(type_id)]  # table v1 : description en variables, pas en aptitudes
        else:
            name, anchor = TYPE_NAMES[(family, int(type_id))]
    except (KeyError, TypeError, ValueError):
        return f"Type {type_id}"
    if description is not None and anchor not in description:
        return f"{name} (à revalider)"
    return name


# Variables écartées des types v2 : persistance s / s+3 du z (corrélation d'une fenêtre à la suivante, fenêtres sans saison commune)
# inférieure à MIN_RELIABILITY. Valeurs mesurées le 17/11/2026 sur 20 112 paires de joueurs-saisons.
UNSTABLE = {"tackle_win_rate": 0.09, "g_yellow_per_foul": 0.20, "shot_on_target_rate": 0.21, "goal_conversion": 0.23,
            "g_defended_woe_adj": 0.30, "crosses_blocked": 0.31, "g_dribble_woe_adj": 0.32}

# 8 aptitudes : (libellé, variables). Toutes les variables sont orientées « plus = davantage de cette aptitude ».
AXES = {
    "tir": ("Tir", ["shots", "shots_on_target", "shots_in_box", "xg_contribution"]),
    "creation": ("Création", ["key_passes", "chances_created", "xt_created", "throughballs"]),
    "circulation": ("Circulation", ["passes_open", "touches", "progressive_passes", "passes_into_final_third"]),
    "centralite": ("Centralité", ["net_pass_share", "net_betweenness"]),
    "recuperation": ("Récupération", ["tackles", "interceptions", "recoveries", "clearances"]),
    "aerien": ("Jeu aérien", ["aerial_duels", "aerial_win_rate", "a_head_shots_per_match"]),
    "duels": ("Dribbles et duels", ["g_dribbles_per_90", "challenges"]),
    "coups_arretes": ("Coups de pied arrêtés", ["corners_taken", "freekicks_played", "penalties_taken"]),
}


def family_features(fam, version=VERSION_DEFAULT):
    """Variables d'une famille retenues pour le k-means (v2 : sans les variables instables)."""
    return [f for f in FAMILIES[fam] if version == "v1" or f not in UNSTABLE]


def aptitude_features(df):
    """Matrice des 8 aptitudes (moyenne des z écrêtés de leurs variables) et noms de colonnes apt_<axe>."""
    cols = [f"apt_{k}" for k in AXES]
    X = np.column_stack([df[[f"dec_z_{n}" for n in ns]].fillna(0.0).clip(-Z_CLIP, Z_CLIP).mean(axis=1)
                         for _, ns in AXES.values()])
    return X, cols


def cluster_matrix(df, fam, version=VERSION_DEFAULT):
    """Espace dans lequel les types de `fam` ont été formés : (matrice, noms de colonnes). Sert aux contrôles de netteté et aux cartes."""
    if fam == "global":
        if version == "v1":
            X, cols, _ = global_features(df)
            return np.asarray(X), cols
        return aptitude_features(df)
    cols = [f"dec_z_{f}" for f in family_features(fam, version)]
    return df[cols].fillna(0.0).clip(-Z_CLIP, Z_CLIP).values, cols


def describe_center(center, cols, n_hi=3, n_lo=2):
    s = pd.Series(center, index=[c[len("dec_z_"):] if c.startswith("dec_z_") else c for c in cols]).sort_values()
    hi = ", ".join(f"{i} {v:+.1f}" for i, v in s.tail(n_hi)[::-1].items())
    lo = ", ".join(f"{i} {v:+.1f}" for i, v in s.head(n_lo).items())
    return f"haut : {hi} | bas : {lo}"


def _margin(dist):
    """Distance au centre le plus proche / distance au deuxième centre (0 = au cœur du type, 1 = entre deux types)."""
    d = np.sort(dist, axis=1)
    return d[:, 0] / np.maximum(d[:, 1], 1e-12)


def fit_family(df, fam, cols, n_types=N_TYPES, seed=SEED):
    """k-means sur les lignes d'apprentissage, assignation de toutes les lignes ; numéro de type stabilisé par la somme
    croissante des z du centre (type 0 = le plus faible sur la famille). Retourne (types, distance au centre, marge, labels)."""
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
    return t, dist.min(axis=1), _margin(dist), labels


def global_features(df):
    """Matrice du type global : z-scores des 8 familles, écrêtés à +-Z_CLIP, chaque famille pesant autant (poids
    1 / racine du nombre de variables). Sert au k-means et à la recherche de joueurs similaires (même espace)."""
    feats = [(f"dec_z_{n}", 1.0 / np.sqrt(len(ns))) for ns in FAMILIES.values() for n in ns]
    cols, w = [c for c, _ in feats], np.array([x for _, x in feats])
    return df[cols].fillna(0.0).clip(-Z_CLIP, Z_CLIP).values * w, cols, w


def fit_global(df, n_types=None, seed=SEED, version=VERSION_DEFAULT):
    """Type global. v2 : k-means sur les 8 aptitudes. v1 : toutes les variables des familles, chaque famille pesant autant
    (poids 1/racine du nombre de variables), z écrêtés à +-Z_CLIP. Types numérotés par taille décroissante sur les lignes
    d'apprentissage. Retourne (types, distance au centre, marge, labels)."""
    if version == "v1":
        n_types = n_types or N_GLOBAL_V1
        X, cols, w = global_features(df)
    else:
        n_types = n_types or N_GLOBAL
        (X, cols), w = aptitude_features(df), np.ones(len(AXES))
    fit = df["bool_fit"].values
    km = KMeans(n_types, n_init=10, random_state=seed).fit(X[fit])
    dist_all = np.linalg.norm(X[:, None, :] - km.cluster_centers_[None, :, :], axis=2)
    raw = dist_all.argmin(axis=1)
    order = np.argsort(-np.bincount(raw[fit], minlength=n_types))
    rank = np.empty(n_types, dtype=int)
    rank[order] = np.arange(n_types)
    centers = km.cluster_centers_ / w
    n_hi, n_lo = (5, 3) if version == "v1" else (4, 4)
    labels = pd.DataFrame({"str_family": "global", "int_type": rank,
                           "str_description": [describe_center(centers[i], cols, n_hi=n_hi, n_lo=n_lo) for i in range(n_types)]})
    return rank[raw], dist_all.min(axis=1), _margin(dist_all), labels


def build(df, version=VERSION_DEFAULT):
    """df : gold_player_behaviour_profile. Retourne (types, labels). version : « v2 » (défaut) ou « v1 » (expériences antérieures)."""
    df = df[df["bool_is_peer"] & df["str_macro_role"].notna() & (df["str_macro_role"] != "GK")].copy()
    df["bool_fit"] = df["str_season"].str[:4].astype(int).between(FIT_FIRST_YEAR, FIT_LAST_YEAR)
    out = df[KEY + ["str_macro_role"]].copy()
    labels = []
    for fam in FAMILIES:
        cols = [f"dec_z_{f}" for f in family_features(fam, version)]
        t, dist, margin, lab = fit_family(df, fam, cols)
        out[f"int_type_{fam}"], out[f"dec_dist_{fam}"] = t, dist
        if version != "v1":
            out[f"dec_margin_{fam}"] = margin
        labels.append(lab)
    t, dist, margin, lab = fit_global(df, version=version)
    out["int_type_global"], out["dec_dist_global"] = t, dist
    if version != "v1":
        out["dec_margin_global"] = margin
    labels.append(lab)
    return out.reset_index(drop=True), pd.concat(labels, ignore_index=True)


def main(source=SRC_DEFAULT, write=True, version=VERSION_DEFAULT):
    import duckdb
    import yaml
    from loguru import logger
    root = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
    with open(root / "config.yaml", encoding="utf-8") as f:
        db_path = root / yaml.safe_load(f)["paths"]["duckdb"]
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(f"SELECT * FROM {source}").df()
    con.close()
    types, labels = build(df, version=version)
    logger.info(f"{len(types)} joueurs-saisons typés sur {len(FAMILIES)} familles et en type global (version {version})")
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
    ap.add_argument("--version", choices=["v1", "v2"], default=VERSION_DEFAULT,
                    help="v2 (défaut) : variables stables, type global sur 8 aptitudes, marge ; v1 : version d'origine")
    args = ap.parse_args()
    main(source=args.source, write=not args.dry_run, version=args.version)
