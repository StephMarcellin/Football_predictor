"""
03_knn_impute.py — Famille 11 : clusters de style (76/77) + imputation KNN (78).

Étape 1 (ce fichier) : clusters de style offensif/défensif par (player_id, season).
Le cluster pré-restreint le vivier de voisins du KNN ; il n'est JAMAIS une entrée
du modèle. Ajusté sur les profils fiables (n_apps_lag >= MIN_APPS), prédit pour
tous. Gardiens exclus (label -1).
"""
# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from pathlib import Path
import argparse

import duckdb
import yaml
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import KFold

# Chemins ancrés sur __file__ (jamais sur le cwd — cf. convention run_pipeline).
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH  = Path(CFG["paths"]["duckdb"])

# ── Paramètres famille 11, chargés depuis config.yaml (rien en dur) ──────────
KNN_CFG = CFG["knn"]
OFFENSIVE_FEATURES = KNN_CFG["features"]["offensive"]
DEFENSIVE_FEATURES = KNN_CFG["features"]["defensive"]
N_CLUSTERS = KNN_CFG["n_clusters"]
MIN_APPS = KNN_CFG["min_apps"]
GK_VERTICAL_MAX = KNN_CFG["gk_vertical_max"]

def load_player_profiles(con):
    """Un profil courant par (player_id, season) : la dernière ligne as-of de la
    saison (date max), enrichie de la position moyenne du joueur (lineup).
    Retourne un DataFrame ; les NaN de features sont gérés plus tard."""
    cols = ",\n               ".join(OFFENSIVE_FEATURES + DEFENSIVE_FEATURES)
    df = con.sql(f"""
        WITH latest AS (
          SELECT * FROM (
            SELECT *, row_number() OVER (
                PARTITION BY player_id, season ORDER BY date DESC, match_id DESC) AS rn
            FROM gold.joueur_saison
          ) WHERE rn = 1
        ),
        pos AS (
          SELECT player_id, AVG(grid_vertical) AS gv, AVG(grid_horizontal) AS gh
          FROM intermediate.int_whoscored_lineup GROUP BY 1
        )
        SELECT l.player_id, l.season, l.n_apps_lag,
               {cols},
               pos.gv, pos.gh
        FROM latest l
        LEFT JOIN pos ON pos.player_id = CAST(l.player_id AS BIGINT)
    """).df()
    return df.sort_values(["player_id", "season"]).reset_index(drop=True)

def fit_style_clusters(df, features, side):
    """Ajuste KMeans sur les profils FIABLES d'un côté (offensif/défensif) puis
    prédit le cluster de TOUS les non-gardiens. Gardiens = -1.

    df       : sortie de load_player_profiles (une ligne par joueur-saison)
    features : OFFENSIVE_FEATURES ou DEFENSIVE_FEATURES
    side     : 'offensive' / 'defensive' (pour le nommage/logs)
    Retourne : (labels: pd.Series alignée sur df.index, modèle, scaler, medians)
    """
    df = df.copy()
    df["width"] = (df["gh"] - 5).abs()          # écart à l'axe = largeur de position
    cols = features + ["gv", "width"]           # profil + position (source cluster 76)

    is_gk = df["gv"].notna() & (df["gv"] <= GK_VERTICAL_MAX)
    fit_mask = (~is_gk) & (df["n_apps_lag"] >= MIN_APPS)   # profils fiables uniquement

    medians = df.loc[fit_mask, cols].median()   # médianes de référence (fiables)
    X_fit = df.loc[fit_mask, cols].fillna(medians)
    scaler = StandardScaler().fit(X_fit)
    km = KMeans(n_clusters=N_CLUSTERS, n_init=10, random_state=0).fit(
        scaler.transform(X_fit))

    labels = pd.Series(-1, index=df.index, dtype="int64")   # défaut = gardien/-1
    non_gk = ~is_gk
    X_all = df.loc[non_gk, cols].fillna(medians)            # même médianes de réf.
    labels.loc[non_gk] = km.predict(scaler.transform(X_all))
    return labels, km, scaler, medians

def to_canonical(df, labels, side):
    """Mappe les numéros KMeans (arbitraires, permutables) vers des labels
    sémantiques STABLES, par règle sur les centroïdes → insensible à la
    renumérotation. Gardiens (-1) → 'GK'."""
    cent = df.assign(_c=labels.values)
    cent = cent[cent["_c"] >= 0].groupby("_c").mean(numeric_only=True)
    if side == "offensive":
        fin = cent["scorer_shots_per90_lag"].idxmax()             # + de tirs → finisseur
        cre = cent["off_key_passes_per90_lag"].drop(fin).idxmax()  # + de passes clés → créateur
        low = [c for c in cent.index if c not in (fin, cre)][0]
        mapping = {fin: "off_finisher", cre: "off_creator", low: "off_low"}
    else:
        low = cent["gv"].idxmax()                                 # + avancé → peu défensif
        aer = cent["def_threat_conceded_per90_lag"].drop(low).idxmax()  # + de menace concédée
        rec = [c for c in cent.index if c not in (low, aer)][0]
        mapping = {low: "def_low", aer: "def_aerial", rec: "def_recuperator"}
    out = pd.Series("GK", index=df.index, dtype="object")
    m = labels >= 0
    out[m] = labels[m].map(mapping)
    return out

def characterize(df, labels, features, side):
    """Affiche la moyenne de chaque feature par cluster → lecture football."""
    tmp = df.copy()
    tmp["cluster"] = labels
    print(f"\n=== Clusters {side} (n par cluster) ===")
    print(tmp["cluster"].value_counts().sort_index().to_string())
    prof = tmp[tmp["cluster"] != "GK"].groupby("cluster")[features + ["gv", "gh"]].mean()
    print(prof.round(3).to_string())

def write_clusters(con, df, off_labels, def_labels,
                   table="machine_learning.player_style_clusters"):
    """Une ligne par (player_id, season) avec ses 2 clusters de style (-1 = GK).
    Table écrite par le pipeline Python, consommée ensuite par dbt en `source`."""
    out = df[["player_id", "season"]].copy()
    out["cluster_offensive"] = off_labels.values
    out["cluster_defensive"] = def_labels.values
    con.register("tmp_style_clusters", out)
    con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM tmp_style_clusters")
    con.unregister("tmp_style_clusters")
    print(f"\nÉcrit : {table} ({len(out):,} lignes)")
    return out

def knn_impute(X_don, y_don, cl_don, X_tgt, cl_tgt, k):
    """KNN action-spécifique avec repli cluster.
    Donneurs : X_don (coords standardisées), y_don (valeur cible), cl_don (cluster).
    Cibles   : X_tgt (coords standardisées), cl_tgt (cluster).
    Pour chaque cible : moyenne des k donneurs les plus proches DU MÊME cluster ;
    repli = moyenne cible du cluster si < k donneurs dans ce cluster."""
    gmean = y_don.mean()
    cmeans = {c: y_don[cl_don == c].mean() for c in np.unique(cl_don)}
    out = np.full(len(X_tgt), gmean)
    for c in np.unique(cl_tgt):                  # traité cluster par cluster (batch)
        te = cl_tgt == c
        tr = cl_don == c
        if tr.sum() >= k:                        # voie principale : KNN dans le cluster
            nn = NearestNeighbors(n_neighbors=k).fit(X_don[tr])
            _, j = nn.kneighbors(X_tgt[te])
            out[te] = y_don[tr][j].mean(1)
        else:                                    # repli : moyenne du cluster
            out[te] = cmeans.get(c, gmean)
    return out

def load_impute_context(con):
    """(a) coordonnées joueur fiables + clusters par (player, season),
    (b) table zonale longue. Réutilisé par la validation et l'imputation."""
    all_coords = sorted({c for cs in KNN_CFG["coordinate_sets"].values() for c in cs})
    coords_sql = ",".join("l." + c for c in all_coords)
    coords_df = con.sql(f"""
        WITH latest AS (SELECT * FROM (
          SELECT *, row_number() OVER (PARTITION BY player_id, season
                    ORDER BY date DESC, match_id DESC) rn
          FROM gold.joueur_saison) WHERE rn = 1)
        SELECT l.player_id, l.season, l.n_apps_lag, {coords_sql},
               c.cluster_offensive, c.cluster_defensive
        FROM latest l
        JOIN machine_learning.player_style_clusters c
             ON c.player_id = l.player_id AND c.season = l.season
    """).df()
    targets = [t["feature"] for t in KNN_CFG["zonal_targets"]]
    zon = con.sql(f"""SELECT player_id, season, zone_5x5, profile_confidence_flag,
                             {','.join(targets)}
                      FROM gold.joueur_zone_saison""").df()
    return coords_df, zon


def validate_target(con, coords_df, zon, feature, coords, side, k=None, folds=3):
    """RMSE de reconstruction par masquage, poolée sur les 25 cellules d'une
    cible zonale. Compare KNN / repli cluster / moyenne globale."""
    k = k or KNN_CFG["k_neighbors"]
    clcol = "cluster_offensive" if side == "offensive" else "cluster_defensive"
    base = coords_df[["player_id", "season", "n_apps_lag", clcol] + coords].dropna(subset=coords)
    sk = scl = sg = cnt = 0
    for _, zc in zon[["player_id", "season", "zone_5x5", feature]].groupby("zone_5x5"):
        d = base.merge(zc.rename(columns={feature: "tgt"}), on=["player_id", "season"])
        d = d[(d.n_apps_lag >= MIN_APPS) & (d[clcol] != "GK") & d.tgt.notna()]
        if len(d) < 200:
            continue
        X = d[coords].values.astype(float); y = d.tgt.values; cl = d[clcol].values
        for tr, te in KFold(folds, shuffle=True, random_state=0).split(X):
            s = StandardScaler().fit(X[tr])
            pk = knn_impute(s.transform(X[tr]), y[tr], cl[tr], s.transform(X[te]), cl[te], k)
            gm = y[tr].mean(); cm = {c: y[tr][cl[tr] == c].mean() for c in np.unique(cl[tr])}
            pc = np.array([cm.get(c, gm) for c in cl[te]])
            sk += ((pk - y[te]) ** 2).sum(); scl += ((pc - y[te]) ** 2).sum()
            sg += ((gm - y[te]) ** 2).sum(); cnt += len(te)
    return np.sqrt(sk / cnt), np.sqrt(scl / cnt), np.sqrt(sg / cnt), cnt

def _impute_column(m, feature, coords, side, method, k):
    """Impute une colonne zonale : garde la valeur si le profil zonal est fiable
    (flag high/medium), sinon impute par KNN (coords joueur, dans le cluster) ou
    repli cluster. Retourne (valeurs remplies, masque imputé booléen)."""
    clcol = "cluster_offensive" if side == "offensive" else "cluster_defensive"
    val = m[feature].values.astype(float).copy()
    keep = m["profile_confidence_flag"].isin(["high", "medium"]).values & ~np.isnan(val)
    out = val.copy()
    imputed = ~keep
    coords_ok = m[coords].notna().all(1).values
    reliable = m["n_apps_lag"].values >= MIN_APPS
    cl = m[clcol].values
    for cell in m["zone_5x5"].unique():
        cellm = m["zone_5x5"].values == cell
        don = cellm & keep & coords_ok & reliable & (cl != "GK")   # donneurs fiables
        tgt = cellm & imputed                                      # cellules à combler
        if tgt.sum() == 0 or don.sum() == 0:
            continue
        yv = val[don]; cld = cl[don]
        gm = yv.mean()
        cmeans = {c: yv[cld == c].mean() for c in np.unique(cld)}
        clt = cl[tgt]
        pred = np.array([cmeans.get(c, gm) for c in clt])          # défaut : repli cluster
        if method == "knn" and don.sum() >= k:
            Xd = m.loc[don, coords].values.astype(float)
            Xt = m.loc[tgt, coords].values.astype(float)
            tgt_ok = coords_ok[tgt] & reliable[tgt]                # cible plaçable ?
            sc = StandardScaler().fit(Xd); Xds = sc.transform(Xd)
            for c in np.unique(clt):
                sub = (clt == c) & tgt_ok
                dm = cld == c
                if sub.sum() > 0 and dm.sum() >= k:
                    nn = NearestNeighbors(n_neighbors=k).fit(Xds[dm])
                    _, j = nn.kneighbors(sc.transform(Xt[sub]))
                    pred[sub] = yv[dm][j].mean(1)
        out[tgt] = pred
    return out, imputed


def impute_zonal_table(con, coords_df, zon):
    """Table zonale imputée, même grain que joueur_zone_saison. Une colonne
    valeur + une colonne <feature>_imputed (traçabilité observé vs imputé)."""
    k = KNN_CFG["k_neighbors"]
    csets = KNN_CFG["coordinate_sets"]
    m = zon.merge(coords_df, on=["player_id", "season"], how="left")
    out = m[["player_id", "season", "zone_5x5"]].copy()
    for t in KNN_CFG["zonal_targets"]:
        vals, imp = _impute_column(m, t["feature"], csets[t["coords"]],
                                   t["side"], t["method"], k)
        out[t["feature"]] = vals
        out[t["feature"] + "_imputed"] = imp
    return out


def write_imputed(con, df, table="machine_learning.zonal_profiles_imputed"):
    con.register("tmp_imputed", df)
    con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM tmp_imputed")
    con.unregister("tmp_imputed")
    print(f"Écrit : {table} ({len(df):,} lignes)")

def main(write=False, validate=False, db=None):
    """Clusters de style + imputation KNN (famille 11).
    write=True écrit player_style_clusters + zonal_profiles_imputed.
    Ferme la connexion à la fin — indispensable quand l'orchestrateur enchaîne
    d'autres steps (DuckDB = un seul writer)."""
    con = duckdb.connect(db or str(DB_PATH), read_only=not write)
    df = load_player_profiles(con)
    print(f"Profils joueur-saison : {len(df):,} | avec position : {df['gv'].notna().sum():,}")

    off_int, _, _, _ = fit_style_clusters(df, OFFENSIVE_FEATURES, "offensive")
    off_canon = to_canonical(df, off_int, "offensive")
    characterize(df, off_canon, OFFENSIVE_FEATURES, "offensive")

    def_int, _, _, _ = fit_style_clusters(df, DEFENSIVE_FEATURES, "defensive")
    def_canon = to_canonical(df, def_int, "defensive")
    characterize(df, def_canon, DEFENSIVE_FEATURES, "defensive")

    if write:
        write_clusters(con, df, off_canon, def_canon)
        coords_df, zon = load_impute_context(con)
        write_imputed(con, impute_zonal_table(con, coords_df, zon))

    if validate:
        coords_df, zon = load_impute_context(con)
        csets = KNN_CFG["coordinate_sets"]
        print(f"\n{'cible':40}{'KNN':>8}{'cluster':>9}{'global':>8}{'n':>10}")
        for t in KNN_CFG["zonal_targets"]:
            rk, rc, rg, n = validate_target(con, coords_df, zon,
                                            t["feature"], csets[t["coords"]], t["side"])
            best = "KNN" if rk < rc and rk < rg else ("cluster" if rc < rg else "global")
            print(f"{t['feature']:40}{rk:8.3f}{rc:9.3f}{rg:8.3f}{n:10,}  -> {best}")

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clusters de style + KNN (famille 11).")
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--write", action="store_true",
                        help="Écrit la table (sinon read-only, diagnostic seul).")
    parser.add_argument("--validate", action="store_true",
                        help="Valide le KNN par masquage (RMSE), sans écrire.")
    args = parser.parse_args()
    main(write=args.write, validate=args.validate, db=args.db)