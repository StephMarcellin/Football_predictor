"""Types de joueurs par comportement -> machine_learning.player_behaviour_types.

Lit gold.gold_player_behaviour_profile (profil sur 3 saisons glissantes, valeurs rétrécies et z-scores
contre les pairs de la même saison, ligue et famille de poste) et classe chaque joueur dans un TYPE au
sein de sa famille de poste (GK, CB, FB, MID, WIDE, ST), plus des coordonnées continues (composantes
principales). Étape Python APRÈS dbt_gold_base ; aucun modèle dbt ne la lit pour l'instant.

Choix de méthode (mesurés le 10/11/2026, voir docs/JOURNAL_RESULTATS.md) :
  · Les secteurs gauche / droite sont convertis en « flanc principal / flanc opposé » d'après la latéralité
    du joueur : sinon un arrière gauche et un arrière droit forment deux types, ce qui mesure le côté et non le
    style. La latéralité est conservée à part (dec_lateral_index, str_side).
  · Centres figés sur les fenêtres terminant de 2016-2017 à 2018-2019 : aucune information des saisons à
    prédire à partir de 2019-2020 (la ligne s-1 d'une saison 2019-2020 se termine en 2018-2019). Les z-scores
    rendent les saisons comparables, le centre figé ne dérive donc pas avec les définitions d'événements.
  · ACP gardant 90 % de la variance, puis k-means. Nombre de types par famille : 4 (FB, MID, WIDE, ST),
    3 (CB, GK). Persistance mesurée entre fenêtres sans recouvrement (s et s+3) : kappa de 0,47 (FB), 0,58 (MID),
    0,57 (WIDE), 0,51 (ST), 0,37 (CB), 0,31 (GK). Les types de CB et de GK tiennent peu : privilégier les
    composantes continues (corrélation entre fenêtres sans recouvrement de 0,6 à 0,8 sur les 2 ou 3 premières).
  · Seuls les joueurs ayant au moins 450 minutes sur la fenêtre (bool_is_peer) reçoivent un type.

Les types sont décrits automatiquement par leurs écarts les plus marqués (str_type_description) : ce sont des
descriptions de données, pas des étiquettes validées.

Usage :  python pipelines/features/behaviour_types.py
         python pipelines/features/behaviour_types.py --dry-run
         python pipelines/features/behaviour_types.py --source "read_parquet('x.parquet')" --dry-run
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

SRC_DEFAULT = "gold.gold_player_behaviour_profile"
OUT_SCHEMA, OUT_TABLE = "machine_learning", "player_behaviour_types"

FIT_FIRST_YEAR, FIT_LAST_YEAR = 2016, 2018      # fenêtres terminant dans [2016-2017, 2018-2019]
N_TYPES = {"FB": 4, "MID": 4, "WIDE": 4, "ST": 4, "CB": 3, "GK": 3}
N_PC_OUT = 5
VARIANCE = 0.90
SEED = 0
SIDE_THRESHOLD = 0.10       # hypothèse : latéralité au-delà de 0,10 = côté marqué
DEPTHS = ("def", "mid", "att")
KEY = ["str_player_id", "str_season"]


def add_mirrored_sectors(df):
    """Latéralité (part droite - part gauche, toutes profondeurs) et secteurs en flanc principal / opposé."""
    df = df.copy()
    df["dec_lateral_index"] = sum(df[f"dec_sec_{d}_right"] - df[f"dec_sec_{d}_left"] for d in DEPTHS)
    right = df["dec_lateral_index"] >= 0
    for d in DEPTHS:
        left_z, right_z = df[f"dec_z_sec_{d}_left"], df[f"dec_z_sec_{d}_right"]
        df[f"dec_z_sec_{d}_main"] = np.where(right, right_z, left_z)
        df[f"dec_z_sec_{d}_off"] = np.where(right, left_z, right_z)
    df["str_side"] = np.where(df["dec_lateral_index"] > SIDE_THRESHOLD, "R",
                              np.where(df["dec_lateral_index"] < -SIDE_THRESHOLD, "L", "C"))
    return df


def feature_columns(df, role):
    """Z-scores utilisés pour une famille de poste : tout sauf les secteurs gauche / droite bruts ; pas de
    secteur ni de duel au sol pour les gardiens."""
    cols = [c for c in df.columns if c.startswith("dec_z_") and not c.endswith(("_left", "_right"))]
    if role == "GK":
        cols = [c for c in cols if not (c.startswith("dec_z_g_") or c.startswith("dec_z_sec_"))]
    return cols


def describe_center(center, cols, n_hi=3, n_lo=2):
    s = pd.Series(center, index=[c[len("dec_z_"):] for c in cols]).sort_values()
    hi = ", ".join(f"{i} {v:+.1f}" for i, v in s.tail(n_hi)[::-1].items())
    lo = ", ".join(f"{i} {v:+.1f}" for i, v in s.head(n_lo).items())
    return f"haut : {hi} | bas : {lo}"


def fit_assign(df, role, n_types, seed=SEED):
    """Ajuste ACP + k-means sur les lignes d'apprentissage de la famille, assigne toute la famille.
    Retourne (lignes de la famille avec type, distance, composantes ; descriptions des types)."""
    cols = feature_columns(df, role)
    fit = df[df["bool_fit"]]
    X = fit[cols].fillna(0.0).values
    pca = PCA(n_components=VARIANCE, random_state=seed).fit(X)
    km = KMeans(n_types, n_init=10, random_state=seed).fit(pca.transform(X))
    # numéro de type stabilisé : tri par première composante croissante du centre
    order = np.argsort(km.cluster_centers_[:, 0])
    rank = np.empty(n_types, dtype=int)
    rank[order] = np.arange(n_types)
    P = pca.transform(df[cols].fillna(0.0).values)
    dist = np.linalg.norm(P[:, None, :] - km.cluster_centers_[None, :, :], axis=2)
    out = df[KEY + ["str_side", "dec_lateral_index"]].copy()
    out["str_macro_role"] = role
    out["int_type"] = rank[dist.argmin(axis=1)]
    srt = np.sort(dist, axis=1)
    out["dec_dist_to_center"] = srt[:, 0]
    out["dec_margin_to_second"] = srt[:, 1] - srt[:, 0]
    for i in range(N_PC_OUT):
        out[f"dec_pc{i + 1}"] = P[:, i] if i < P.shape[1] else np.nan
    centers_z = pca.inverse_transform(km.cluster_centers_)
    desc = {int(rank[t]): describe_center(centers_z[t], cols) for t in range(n_types)}
    out["str_type_description"] = out["int_type"].map(desc)
    out["str_type"] = out["str_macro_role"] + "_" + out["int_type"].astype(str)
    if role == "GK":                      # la latéralité n'a pas de sens pour un gardien
        out["str_side"], out["dec_lateral_index"] = None, np.nan
    return out


def build(df):
    """df : gold_player_behaviour_profile. Retourne la table des types (joueurs avec >= 450 minutes)."""
    df = df[df["bool_is_peer"] & df["str_macro_role"].notna()].copy()
    df["y"] = df["str_season"].str[:4].astype(int)
    df["bool_fit"] = df["y"].between(FIT_FIRST_YEAR, FIT_LAST_YEAR)
    df = add_mirrored_sectors(df)
    parts = []
    for role, k in N_TYPES.items():
        sub = df[df["str_macro_role"] == role]
        if sub["bool_fit"].sum() < 20 * k:
            continue
        parts.append(fit_assign(sub, role, k))
    return pd.concat(parts, ignore_index=True)


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
    out = build(df)
    logger.info(f"{len(out)} joueurs-saisons typés ; types par famille : "
                f"{out.groupby('str_macro_role')['int_type'].nunique().to_dict()}")
    if not write:
        logger.info("dry-run : rien n'est écrit")
        return out
    con = duckdb.connect(str(db_path))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA}")
    con.register("out_df", out)
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_TABLE} AS SELECT * FROM out_df")
    con.close()
    logger.info(f"écrit {OUT_SCHEMA}.{OUT_TABLE}")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Types de joueurs -> machine_learning.player_behaviour_types")
    ap.add_argument("--source", default=SRC_DEFAULT, help="relation SQL source (table ou read_parquet)")
    ap.add_argument("--dry-run", action="store_true", help="calcule et contrôle, n'écrit rien")
    args = ap.parse_args()
    main(source=args.source, write=not args.dry_run)
