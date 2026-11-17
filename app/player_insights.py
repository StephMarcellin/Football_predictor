"""
player_insights.py — Indicateurs de caractérisation d'un joueur pour l'explorateur (sans Streamlit, testable).

- percentile et valeur réelle (par 90 minutes) face aux joueurs du même poste, de la même ligue et de la même saison ;
- fiabilité de chaque variable : corrélation du z d'un joueur entre la fenêtre s et la fenêtre s+3 (fenêtres sans saison commune) ;
- variables regroupées quand elles se ressemblent (corrélation absolue >= 0,75) : un seul représentant par groupe en tête de fiche ;
- 8 aptitudes (radar) : moyenne de z de variables toutes orientées « plus = davantage » ;
- netteté de chaque type (silhouette, cohésion, séparation, rétention d'une fenêtre à la suivante) ;
- phrase de synthèse.

⚠️ Les profils de la ligne s incluent la saison s : l'affichage est descriptif, jamais à fournir tel quel à un modèle de prédiction.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform
from sklearn.metrics import silhouette_samples

import app.player_data as pdta

tt = pdta.tt

# Axes du radar : (libellé, variables), définis dans trait_types (ce sont aussi les axes du type global v2).
AXES = tt.AXES
# Sous ce seuil, la variable n'entre pas dans les points forts / faibles de la fiche ni dans les types v2.
# ⚠️ Seuil abaissé de 0,4 à 0,35 après coup (17/11/2026) : à 0,4 trop de variables utiles tombaient ; choix posé APRÈS avoir vu les
# fiabilités, donc à ne pas présenter comme un seuil fixé a priori.
MIN_RELIABILITY = tt.MIN_RELIABILITY
GROUP_THRESHOLD = 0.75         # corrélation absolue au-delà de laquelle deux variables sont « la même information »
MIN_PEERS = 20


def feature_names() -> list[str]:
    return [n for ns in tt.FAMILIES.values() for n in ns]


def family_of(name: str) -> str:
    return next(f for f, ns in tt.FAMILIES.items() if name in ns)


def _field_players(profiles: pd.DataFrame) -> pd.DataFrame:
    return profiles[(profiles.str_macro_role != "GK") & profiles.bool_is_peer]


# ── Fiabilité et regroupement des variables ──────────────────────────────────────────────────
def feature_reliability(profiles: pd.DataFrame, lag: int = 3) -> pd.Series:
    """Corrélation du z d'un joueur entre la fenêtre s et la fenêtre s+lag (3 saisons d'écart : aucune saison commune)."""
    d = _field_players(profiles)
    seasons = sorted(d.str_season.unique())
    idx = {s: i for i, s in enumerate(seasons)}
    cols = [f"dec_z_{n}" for n in feature_names()]
    a = d[["str_player_id", "str_season"] + cols].copy()
    a["k"] = a.str_season.map(idx)
    b = a.copy()
    b["k"] -= lag
    m = a.merge(b, on=["str_player_id", "k"], suffixes=("", "_next"))
    out = {}
    for n in feature_names():
        x, y = m[f"dec_z_{n}"], m[f"dec_z_{n}_next"]
        ok = x.notna() & y.notna()
        out[n] = float(np.corrcoef(x[ok], y[ok])[0, 1]) if ok.sum() >= 200 else np.nan
    return pd.Series(out)


def feature_groups(profiles: pd.DataFrame, threshold: float = GROUP_THRESHOLD, max_rows: int = 30000) -> pd.Series:
    """Numéro de groupe par variable : classification hiérarchique sur 1 - |corrélation| des z des joueurs de champ."""
    d = _field_players(profiles)
    cols = [f"dec_z_{n}" for n in feature_names()]
    if len(d) > max_rows:
        d = d.sample(max_rows, random_state=0)
    corr = d[cols].corr().fillna(0.0).values
    dist = 1.0 - np.abs(corr)
    np.fill_diagonal(dist, 0.0)
    dist = (dist + dist.T) / 2
    lab = fcluster(linkage(squareform(dist, checks=False), method="average"), t=1.0 - threshold, criterion="distance")
    return pd.Series(lab, index=feature_names())


# ── Pairs et percentiles ─────────────────────────────────────────────────────────────────────
def peers_of(profiles: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    """Pairs : même saison, même ligue, même famille de poste, au moins 450 minutes sur la fenêtre."""
    return profiles[(profiles.str_season == row["str_season"]) & (profiles.str_league_source == row["str_league_source"])
                    & (profiles.str_macro_role == row["str_macro_role"]) & profiles.bool_is_peer]


def _percentile(peer_values: pd.Series, value: float) -> float:
    v = peer_values.dropna()
    if len(v) < MIN_PEERS or value is None or pd.isna(value):
        return np.nan
    return float(100.0 * ((v < value).sum() + 0.5 * (v == value).sum()) / len(v))


def variable_table(profiles: pd.DataFrame, row: pd.Series, reliability: pd.Series, groups: pd.Series) -> pd.DataFrame:
    """Une ligne par variable : valeur (par 90 min, taux ou part), médiane des pairs, z, percentile, fiabilité, groupe."""
    peers = peers_of(profiles, row)
    rows = []
    for n in feature_names():
        z = row.get(f"dec_z_{n}")
        rows.append({"name": n, "Famille": pdta.FAMILIES_FR[family_of(n)], "Variable": pdta.feature_label(n),
                     "valeur": row.get(f"dec_{n}"), "mediane": peers[f"dec_{n}"].median() if len(peers) else np.nan,
                     "z": z, "percentile": _percentile(peers[f"dec_z_{n}"], z) if len(peers) else np.nan,
                     "fiabilite": reliability.get(n, np.nan), "groupe": int(groups.get(n, -1))})
    return pd.DataFrame(rows)


def headline(table: pd.DataFrame, k: int = 5, min_reliability: float = MIN_RELIABILITY,
             strong: float = 75.0, weak: float = 25.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Points forts et faibles : variables fiables, un seul représentant par groupe de variables semblables (le plus extrême)."""
    t = table.dropna(subset=["percentile"])
    t = t[t.fiabilite >= min_reliability].copy()
    t["ecart"] = (t.percentile - 50).abs()
    rep = t.sort_values("ecart", ascending=False).drop_duplicates("groupe")
    n_group = t.groupby("groupe").size()
    rep["proches"] = rep.groupe.map(n_group) - 1
    forts = rep[rep.percentile >= strong].sort_values("percentile", ascending=False).head(k)
    faibles = rep[rep.percentile <= weak].sort_values("percentile").head(k)
    return forts, faibles


# ── Radar : 8 aptitudes ──────────────────────────────────────────────────────────────────────
def axis_table(profiles: pd.DataFrame, row: pd.Series, reliability: pd.Series) -> pd.DataFrame:
    """Percentile de chaque aptitude : moyenne des z de ses variables, comparée à celle des pairs."""
    peers = peers_of(profiles, row)
    out = []
    for key, (label, names) in AXES.items():
        cols = [f"dec_z_{n}" for n in names]
        own = row[cols].astype(float).mean()
        pc = _percentile(peers[cols].astype(float).mean(axis=1), own) if len(peers) else np.nan
        out.append({"axe": key, "Aptitude": label, "percentile": pc, "z": own,
                    "fiabilite": float(np.nanmean([reliability.get(n, np.nan) for n in names]))})
    return pd.DataFrame(out)


# ── Netteté des types ────────────────────────────────────────────────────────────────────────
def type_quality(profiles: pd.DataFrame, sample: int = 5000, seed: int = 0, lag: int = 3) -> pd.DataFrame:
    """Une ligne par type : part, silhouette, cohésion, séparation, rétention d'une fenêtre à la suivante, netteté.

    Silhouette (entre -1 et 1) : proche de 0 = le type recouvre les autres ; cohésion = distance moyenne au centre ; séparation =
    distance au centre le plus proche (distances divisées par la racine du nombre de variables). Rétention : part des joueurs du
    type qui sont dans le même type trois saisons plus tard, comparée à la part du type (élévation)."""
    d0 = profiles[profiles.str_macro_role != "GK"].reset_index(drop=True)
    version = pdta.types_version(profiles)
    seasons = sorted(d0.str_season.unique())
    rng = np.random.default_rng(seed)
    rows = []
    for fam in list(tt.FAMILIES) + ["global"]:
        d = d0[d0[f"int_type_{fam}"].notna()].reset_index(drop=True)
        if d.empty:
            continue
        X, _ = tt.cluster_matrix(d, fam, version)  # espace réel de clustering : v2 = variables stables, 8 aptitudes pour le global
        t = d[f"int_type_{fam}"].astype(int).values
        marge = d[f"dec_margin_{fam}"].values if version == "v2" else np.full(len(d), np.nan)
        coeur = marge < tt.CORE_RATIO
        types = np.unique(t)
        cen = {k: X[t == k].mean(axis=0) for k in types}
        idx = rng.choice(len(d), min(sample, len(d)), replace=False)
        sil = silhouette_samples(X[idx], t[idx]) if len(types) > 1 else np.zeros(len(idx))
        a = pd.DataFrame({"p": d.str_player_id, "k": d.str_season.map({s: i for i, s in enumerate(seasons)}), "t": t})
        b = a.copy()
        b["k"] -= lag
        m = a.merge(b, on=["p", "k"], suffixes=("", "_next"))
        root = np.sqrt(X.shape[1])
        for k in types:
            part = float((t == k).mean())
            mk = m[m.t == k]
            ret = float((mk.t_next == k).mean()) if len(mk) >= 30 else np.nan
            sep = min((np.linalg.norm(cen[k] - cen[j]) for j in types if j != k), default=np.nan)
            rows.append({"family": fam, "type": int(k), "n": int((t == k).sum()), "part": part,
                         "silhouette": float(sil[t[idx] == k].mean()) if (t[idx] == k).any() else np.nan,
                         "part_coeur": float(coeur[t == k].mean()) if version == "v2" else np.nan,
                         "silhouette_coeur": float(sil[(t[idx] == k) & coeur[idx]].mean()) if ((t[idx] == k) & coeur[idx]).any() else np.nan,
                         "cohesion": float(np.linalg.norm(X[t == k] - cen[k], axis=1).mean() / root),
                         "separation": float(sep / root), "retention": ret, "elevation": ret / part if part > 0 else np.nan})
    q = pd.DataFrame(rows)
    q["nettete"] = pd.cut(q.silhouette, [-1, 0.12, 0.25, 1], labels=["floue", "moyenne", "nette"]).astype(str)
    return q


def sharpness(quality: pd.DataFrame, family: str, type_id) -> str:
    r = quality[(quality.family == family) & (quality.type == int(type_id))]
    return r.nettete.iloc[0] if len(r) else "n.c."


# ── Synthèse ─────────────────────────────────────────────────────────────────────────────────
def summary_sentence(row: pd.Series, role_label: str, forts: pd.DataFrame, faibles: pd.DataFrame, types: pd.DataFrame,
                     quality: pd.DataFrame, labels: pd.DataFrame) -> str:
    """Phrase de synthèse : poste, types distinctifs (rares, nets ou moyens, ressemblance non faible), points forts et faibles."""
    parts = [role_label]
    fam_by_fr = {v: k for k, v in pdta.FAMILIES_FR.items()}
    typed = []
    for r in types.itertuples():
        fam = fam_by_fr.get(r.Famille)
        if fam is None or fam == "global" or str(r.Ressemblance).startswith("faible"):
            continue
        t = row.get(f"int_type_{fam}")
        q = quality[(quality.family == fam) & (quality.type == int(t))] if t is not None and not pd.isna(t) else quality.iloc[0:0]
        if len(q) and q.part.iloc[0] < 0.35 and q.nettete.iloc[0] != "floue":
            typed.append(r.Type)
    if typed:
        parts[0] += " — " + ", ".join(typed[:3]).lower()
    s = parts[0] + "."
    low = lambda x: x[0].lower() + x[1:] if x[1:2].islower() else x
    fmt = lambda df: ", ".join(f"{low(r.Variable)} (percentile {r.percentile:.0f})" for r in df.itertuples())
    if len(forts):
        s += f" Points forts : {fmt(forts.head(3))}."
    if len(faibles):
        s += f" Points faibles : {fmt(faibles.head(3))}."
    if not len(forts) and not len(faibles):
        s += " Aucune variable fiable ne s'écarte nettement des joueurs du même poste."
    return s
