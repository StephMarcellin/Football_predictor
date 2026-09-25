"""
knn_diagnostic.py — Diagnostic données préalable au chantier KNN (famille 11).

Objectif : figer, sur données réelles (db/football.duckdb), les 3 paramètres
nécessaires avant de coder les features 76 (cluster offensif), 77 (cluster
défensif) et 78 (imputation KNN) :

  1. COMPLÉTION   — % non-null / distinct / zéros de chaque colonne *_lag des
                    7 espaces de similarité → repérer les dimensions trop vides.
  2. STABILITÉ    — fiabilité split-half du profil joueur : corrélation entre
                    deux profils indépendants de k matchs → seuil d'imputation.
  3. CLUSTERS     — silhouette + inertie KMeans pour k=2..8, côté offensif et
                    défensif → nombre de clusters de style (76/77).

Connexion en READ-ONLY : ce script ne modifie jamais la base.
Lancement :  python pipelines/diagnostics/knn_diagnostic.py [--db db/football.duckdb]

Note anti-leakage : les colonnes *_lag de gold.joueur_saison sont déjà décalées
dans le passé (fenêtre 38 apparitions ANTÉRIEURES, match courant exclu). Le
diagnostic de stabilité, lui, repart des valeurs par match brutes
(intermediate.player_match_stats) pour mesurer la reproductibilité d'un profil.
"""

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


# ── Espaces de similarité (référence, colonnes des 2 tables profils) ──────────
OFFENSIVE_PROFILE = [
    "dec_scorer_xg_per90_lag", "dec_scorer_shots_per90_lag", "dec_off_xg_per_shot_lag",
    "dec_scorer_team_shot_share_lag", "dec_off_chances_created_per90_lag",
    "dec_off_key_passes_per90_lag", "dec_off_xgchain_per90_lag", "dec_off_xgbuildup_per90_lag",
    "dec_scorer_xgot_overperformance_lag",
]
DEFENSIVE_PROFILE = [
    "dec_def_aerial_win_rate_lag", "dec_def_actions_per90_lag", "dec_def_errors_per90_lag",
    "dec_def_threat_conceded_per90_lag",
]


# ── 1. COMPLÉTION ─────────────────────────────────────────────────────────────
def completion(con, table, cols):
    """Pour chaque colonne : % non-null, nb distinct, % de zéros.
    Une dimension trop vide (non-null faible) ou saturée de zéros est un mauvais
    axe de similarité KNN."""
    n = con.sql(f"select count(*) from {table}").fetchone()[0]
    print(f"\n===== COMPLÉTION {table} : {n:,} lignes =====")
    print(f"{'colonne':42} {'non-null%':>9} {'distinct':>9} {'zeros%':>7}")
    for c in cols:
        nn, nd, zp = con.sql(f"""
            select 100.0*count({c})/count(*),
                   count(distinct {c}),
                   100.0*sum(case when {c}=0 then 1 else 0 end)/count(*)
            from {table}""").fetchone()
        print(f"{c:42} {nn:9.1f} {nd:9d} {zp:7.1f}")


# ── 2. STABILITÉ (fiabilité split-half) ──────────────────────────────────────
def _half_profile(sub):
    """Profil agrégé d'un lot de matchs : volumes en per-90 (Σstat/Σmin×90) et
    ratios exacts. Un profil = un vecteur de features."""
    m = sub.int_minutes_played.sum()
    d = sub.int_n_aerial_duels.sum()
    return {
        "xg90":  sub.dec_xg_contribution.sum() / m * 90,
        "sh90":  sub.int_n_shots.sum() / m * 90,
        "cc90":  sub.int_n_chances_created.sum() / m * 90,
        "kp90":  sub.int_n_key_passes.sum() / m * 90,
        "def90": (sub.int_n_tackles.sum() + sub.int_n_interceptions.sum()) / m * 90,
        "aer":   (sub.int_n_aerial_won.sum() / d) if d > 0 else np.nan,
        "thr90": sub.dec_threat.sum() / m * 90,
    }


def stability(con, ks=(3, 5, 8, 10, 12, 15, 20, 25, 30)):
    """Pour chaque taille k : on prend les joueurs ayant ≥ 2k matchs, on compare
    leur profil sur les k premiers matchs vs les k suivants. La corrélation
    inter-joueurs entre les deux moitiés = reproductibilité d'un profil de k
    matchs. Le coude (r≈0.70) donne le seuil d'imputation candidat.
    Spearman-Brown projette la fiabilité d'un profil de 2k matchs : 2r/(1+r)."""
    df = con.sql("""
        WITH base AS (
          SELECT str_player_id, str_team_id, str_match_id, dt_date, int_minutes_played,
                 dec_xg_contribution, int_n_shots, int_n_chances_created, int_n_key_passes,
                 int_n_tackles, int_n_interceptions, int_n_aerial_won, int_n_aerial_duels
          FROM intermediate.player_match_stats
          QUALIFY ROW_NUMBER() OVER (
              PARTITION BY str_match_id, str_team_id, str_player_id ORDER BY dt_scraped_at DESC) = 1
        ),
        tc AS (SELECT str_match_id, str_player_id, SUM(dec_threat_conceded) dec_tc
               FROM intermediate.threat_conceded WHERE str_match_id IS NOT NULL GROUP BY 1,2)
        SELECT b.*, COALESCE(tc.dec_tc, 0) dec_threat
        FROM base b LEFT JOIN tc USING (str_match_id, str_player_id)
        WHERE int_minutes_played > 0
    """).df().sort_values(["str_player_id", "dt_date", "str_match_id"])

    groups = {pid: sub for pid, sub in df.groupby("str_player_id", sort=False)}
    feats = ["xg90", "sh90", "cc90", "kp90", "def90", "aer", "thr90"]

    print("\n===== STABILITÉ (fiabilité split-half du profil joueur) =====")
    print(f"{'k':>3} {'nJoueurs':>8} " + " ".join(f"{f:>6}" for f in feats)
          + f" {'moy':>6} {'SB(2k)':>7}")
    for k in ks:
        A, B = [], []
        for sub in groups.values():
            if len(sub) >= 2 * k:
                A.append(_half_profile(sub.iloc[:k]))
                B.append(_half_profile(sub.iloc[k:2 * k]))
        if len(A) < 30:
            print(f"{k:>3} {len(A):>8}  (trop peu de joueurs)")
            continue
        dfa, dfb = pd.DataFrame(A), pd.DataFrame(B)
        rs = []
        for f in feats:
            m = dfa[f].notna() & dfb[f].notna()
            rs.append(np.corrcoef(dfa[f][m], dfb[f][m])[0, 1] if m.sum() > 10 else np.nan)
        mean_r = np.nanmean(rs)
        sb = 2 * mean_r / (1 + mean_r)
        print(f"{k:>3} {len(A):>8} " + " ".join(f"{r:6.2f}" for r in rs)
              + f" {mean_r:6.2f} {sb:7.2f}")


# ── 3. CLUSTERS (silhouette + inertie) ───────────────────────────────────────
def clusters(con, min_apps=10, gk_vertical_max=0.3):
    """Profil courant par joueur (dernière ligne joueur_saison) enrichi de la
    position moyenne (int_whoscored_lineup). On exclut les gardiens (position
    très basse) et on ne garde que les profils fiables (n_apps_lag ≥ seuil).
    KMeans standardisé k=2..8, silhouette + inertie, côté offensif et défensif.
    Silhouette modérée attendue : le style est un continuum."""
    df = con.sql(f"""
        WITH latest AS (
          SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY str_player_id ORDER BY dt_date DESC) rn
            FROM gold.joueur_saison) WHERE rn = 1
        ),
        pos AS (SELECT str_player_id, AVG(dec_grid_vertical) dec_gv, AVG(dec_grid_horizontal) dec_gh
                FROM intermediate.int_whoscored_lineup GROUP BY 1)
        SELECT l.*, pos.dec_gv, pos.dec_gh
        FROM latest l LEFT JOIN pos ON pos.str_player_id = l.str_player_id
        WHERE l.int_n_apps_lag >= {min_apps}
    """).df()

    gk = df.dec_gv.notna() & (df.dec_gv <= gk_vertical_max)
    d = df[~gk].copy()
    d["dec_width"] = (d.dec_gh - 5).abs()
    print(f"\n===== CLUSTERS (joueurs ≥{min_apps} apps, {gk.sum()} gardiens exclus) =====")

    def run(name, feats):
        X = d[feats].copy().fillna(d[feats].median())
        Xs = StandardScaler().fit_transform(X)
        idx = np.random.default_rng(0).choice(
            len(Xs), size=min(3000, len(Xs)), replace=False)
        print(f"\n--- {name} (n={len(Xs)}, {len(feats)} dims) ---")
        print(f"{'k':>2} {'silhouette':>11} {'inertie':>12}")
        for k in range(2, 9):
            km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(Xs)
            sil = silhouette_score(Xs[idx], km.labels_[idx])
            print(f"{k:>2} {sil:11.3f} {km.inertia_:12.0f}")

    pos_feats = ["dec_gv", "dec_width"]
    run("OFFENSIF profil seul", OFFENSIVE_PROFILE)
    run("OFFENSIF profil + position", OFFENSIVE_PROFILE + pos_feats)
    run("DÉFENSIF profil seul", DEFENSIVE_PROFILE)
    run("DÉFENSIF profil + position", DEFENSIVE_PROFILE + pos_feats)


def main():
    ap = argparse.ArgumentParser(description="Diagnostic données KNN (read-only).")
    ap.add_argument("--db", default="db/football.duckdb")
    ap.add_argument("--skip-stability", action="store_true")
    ap.add_argument("--skip-clusters", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(str(Path(args.db)), read_only=True)

    completion(con, "gold.joueur_saison", OFFENSIVE_PROFILE + DEFENSIVE_PROFILE)
    completion(con, "gold.joueur_zone_saison", [
        "dec_off_touch_share_by_zone_lag", "dec_off_shot_volume_by_zone_lag",
        "dec_off_danger_by_zone_lag", "dec_off_progressive_actions_by_zone_lag",
        "dec_off_cross_volume_by_zone_lag", "dec_def_duel_win_rate_by_zone_lag",
        "dec_def_actions_by_zone_lag"])

    if not args.skip_stability:
        stability(con)
    if not args.skip_clusters:
        clusters(con)


if __name__ == "__main__":
    main()
