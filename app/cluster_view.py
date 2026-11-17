"""
cluster_view.py — logique de l'onglet « Clusters » de l'explorateur (sans Streamlit, testable).

Carte : les joueurs d'une saison sont projetés en 2 dimensions (ACP de l'espace dans lequel les types ont été formés). Chaque type est
entouré d'une ellipse qui contient 80 % de ses joueurs (calculée sur TOUS les joueurs du type, pas seulement ceux dont on affiche le
point). Les chevauchements d'ellipses montrent si les types sont mélangés ou distincts.

⚠️ Une projection en 2 dimensions déforme les distances : deux types qui se recouvrent sur la carte peuvent être séparés dans l'espace
complet, et inversement. La netteté (silhouette dans l'espace complet) reste la mesure de référence ; la part de variance restituée par
la carte est affichée avec elle.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.decomposition import PCA

import app.player_data as pdta
import app.player_insights as ins

tt = pdta.tt

PALETTE = ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#56B4E9", "#D55E00", "#7f7f7f", "#b8a900"]  # lisible aussi par les daltoniens
CLICK_MAX = 10          # nombre maximal de joueurs affichés PAR CLUSTER pour que le clic soit actif
ELLIPSE_SHARE = 0.8     # part des joueurs d'un type contenue dans son ellipse
DEFAULT_PER_CLUSTER = 10
MODES = {"random": "Au hasard (reproductible)", "typical": "Les plus typiques du cluster"}


def color_of(type_id: int) -> str:
    return PALETTE[int(type_id) % len(PALETTE)]


def clickable(n_per_cluster: int) -> bool:
    """Le clic n'est actif que si au plus CLICK_MAX joueurs sont affichés par cluster (lisibilité)."""
    return int(n_per_cluster) <= CLICK_MAX


# ── Données de la carte ──────────────────────────────────────────────────────────────────────
def names_for_season(catalog: pd.DataFrame, season: str) -> pd.DataFrame:
    """Un nom et un club par joueur pour la saison (le club où il a le plus joué). Joueurs sans vrai nom écartés."""
    c = catalog[catalog.season == season].sort_values("minutes", ascending=False).drop_duplicates("player_id")
    c = c[~c.player.astype(str).str.startswith("Joueur ")]
    return c[["player_id", "player", "team", "league"]].rename(columns={"league": "championnat"})


def season_points(profiles: pd.DataFrame, season: str, fam: str, role: str | None = None, league: str | None = None) -> dict | None:
    """Joueurs de champ de la saison typés dans `fam` + coordonnées 2D (ACP). None s'il y a trop peu de joueurs."""
    d = profiles[(profiles.str_season == season) & profiles.bool_is_peer & (profiles.str_macro_role != "GK")
                 & profiles[f"int_type_{fam}"].notna()]
    if role:
        d = d[d.str_macro_role == role]
    if league:
        d = d[d.str_league_source == league]
    d = d.reset_index(drop=True)
    if len(d) < 30 or d[f"int_type_{fam}"].nunique() < 2:
        return None
    version = pdta.types_version(profiles)
    # l'espace est celui du clustering ; avec un filtre de poste, les z restent relatifs au poste de chaque joueur
    X, cols = tt.cluster_matrix(d, fam, version)
    pca = PCA(2, random_state=0).fit(X)
    xy = pca.transform(X)
    d = d.assign(x=xy[:, 0], y=xy[:, 1], type=d[f"int_type_{fam}"].astype(int), dist=d[f"dec_dist_{fam}"],
                 marge=d[f"dec_margin_{fam}"] if version == "v2" else np.nan)
    d["coeur"] = d.marge < tt.CORE_RATIO
    return {"df": d, "X": X, "cols": cols, "var2d": float(pca.explained_variance_ratio_.sum()), "version": version}


def pick_players(d: pd.DataFrame, n: int, mode: str = "random", allowed_ids=None, types=None, seed: int = 0) -> pd.DataFrame:
    """n joueurs par type. « random » : tirage reproductible, emboîté (augmenter n ajoute des joueurs sans changer les premiers) ;
    « typical » : les plus proches du centre de leur type. Seuls les joueurs dont on connaît le nom (allowed_ids) sont proposés."""
    if allowed_ids is not None:
        d = d[d.str_player_id.isin(set(allowed_ids))]
    if types is not None:
        d = d[d.type.isin(list(types))]
    parts = []
    for t, g in d.groupby("type"):
        if mode == "typical":
            parts.append(g.sort_values("dist").head(n))
        else:
            rng = np.random.default_rng(seed + int(t))
            parts.append(g.assign(_r=rng.random(len(g))).sort_values("_r").head(n).drop(columns="_r"))
    return pd.concat(parts) if parts else d.iloc[0:0]


def ellipse(x, y, share: float = ELLIPSE_SHARE, n: int = 80):
    """Ellipse (de Mahalanobis) qui contient la part `share` des points réels. None s'il y a moins de 10 points."""
    x, y = np.asarray(x, float), np.asarray(y, float)
    if len(x) < 10:
        return None
    P = np.column_stack([x, y])
    mu = P.mean(axis=0)
    cov = np.cov(P.T) + 1e-9 * np.eye(2)
    inv = np.linalg.inv(cov)
    md = np.sqrt(np.einsum("ij,jk,ik->i", P - mu, inv, P - mu))
    r = float(np.quantile(md, share))
    w, v = np.linalg.eigh(cov)
    t = np.linspace(0, 2 * np.pi, n)
    pts = v @ (np.sqrt(np.clip(w, 1e-12, None))[:, None] * r * np.vstack([np.cos(t), np.sin(t)]))
    return pts[0] + mu[0], pts[1] + mu[1]


def share_in_ellipse(d: pd.DataFrame, share: float = ELLIPSE_SHARE) -> float:
    """Contrôle : part des joueurs réellement contenue dans leur propre ellipse (doit être proche de `share`)."""
    return float(np.mean([_inside(g, share) for _, g in d.groupby("type") if len(g) >= 10] or [np.nan]))


def _inside(g, share):
    P = g[["x", "y"]].values
    mu, cov = P.mean(axis=0), np.cov(P.T) + 1e-9 * np.eye(2)
    md = np.sqrt(np.einsum("ij,jk,ik->i", P - mu, np.linalg.inv(cov), P - mu))
    return float((md <= np.quantile(md, share) + 1e-9).mean())


def overlap_share(d: pd.DataFrame) -> float:
    """Part des joueurs qui, sur la carte, tombent plus près du centre d'un AUTRE type que du leur (indicateur de mélange en 2D)."""
    cen = d.groupby("type")[["x", "y"]].mean()
    dist = np.linalg.norm(d[["x", "y"]].values[:, None, :] - cen.values[None, :, :], axis=2)
    nearest = cen.index.values[dist.argmin(axis=1)]
    return float((nearest != d.type.values).mean())


# ── Figure ───────────────────────────────────────────────────────────────────────────────────
def _rgba(hex_color: str, a: float) -> str:
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{a})"


def build_figure(d: pd.DataFrame, shown: pd.DataFrame, type_names: dict, names: pd.DataFrame, picks=(), n_per: int = DEFAULT_PER_CLUSTER,
                 var2d: float | None = None):
    """Retourne (figure, trace_ids). trace_ids : {numéro de courbe: [identifiants joueurs dans l'ordre des points]}, pour retrouver le
    joueur cliqué. Les ellipses sont tracées d'abord (tous les joueurs du type), puis un nuage de points par type, puis les joueurs
    choisis pour la comparaison (cercle noir lettré A / B)."""
    nm = names.set_index("player_id")
    fig = go.Figure()
    types = sorted(shown.type.unique())
    for t in types:
        e = ellipse(*d[d.type == t][["x", "y"]].values.T)
        if e is not None:
            fig.add_trace(go.Scatter(x=e[0], y=e[1], mode="lines", fill="toself", fillcolor=_rgba(color_of(t), 0.10),
                                     line=dict(color=_rgba(color_of(t), 0.8), width=2), hoverinfo="skip", showlegend=False))
    trace_ids = {}
    for t in types:
        g = shown[shown.type == t]
        fig.add_trace(go.Scatter(
            x=g.x, y=g.y, mode="markers", name=f"{type_names.get(t, f'Type {t}')} ({int((d.type == t).sum())} joueurs)",
            marker=dict(color=color_of(t), size=12, line=dict(color="white", width=1.2)), hoverinfo="none",
            selected=dict(marker=dict(opacity=1, size=18)), unselected=dict(marker=dict(opacity=1))))
        trace_ids[len(fig.data) - 1] = list(g.str_player_id)
    if picks:
        p = d[d.str_player_id.isin(list(picks))].set_index("str_player_id").reindex([i for i in picks if i in set(d.str_player_id)])
        if len(p):
            lab = ["A", "B"][: len(p)]
            fig.add_trace(go.Scatter(x=p.x, y=p.y, mode="markers+text", text=lab, textposition="top center", hoverinfo="skip",
                                     marker=dict(symbol="circle-open", size=24, line=dict(color="black", width=3)), showlegend=False))
    fig.update_layout(height=620, margin=dict(l=10, r=10, t=30, b=10), legend=dict(orientation="h", y=-0.05),
                      xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title="", scaleanchor="y"),
                      yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=""), plot_bgcolor="white",
                      dragmode="pan")
    return fig, trace_ids


def clicked_players(points, trace_ids: dict) -> list:
    """Identifiants des joueurs correspondant aux points sélectionnés (événement Streamlit)."""
    out = []
    for p in points or []:
        ids = trace_ids.get(p.get("curve_number"))
        i = p.get("point_index", p.get("point_number"))
        if ids is not None and i is not None and 0 <= int(i) < len(ids):
            out.append(ids[int(i)])
    return out


def update_picks(picks: list, new_ids: list, keep: int = 2) -> list:
    """Ajoute les joueurs cliqués à la liste de comparaison (on garde les `keep` derniers, sans doublon)."""
    out = list(picks)
    for i in new_ids:
        if i not in out:
            out.append(i)
    return out[-keep:]


# ── Caractéristiques des clusters ────────────────────────────────────────────────────────────
def centre_matrix(d: pd.DataFrame, cols: list, X: np.ndarray, types=None) -> pd.DataFrame:
    """z moyen de chaque variable (ou aptitude) du clustering, par type, pour les joueurs de la carte."""
    M = pd.DataFrame(X, columns=[c[len("dec_z_"):] if c.startswith("dec_z_") else c for c in cols], index=d.index)
    M["type"] = d["type"].values
    M = M.groupby("type").mean()
    return M if types is None else M.loc[[t for t in types if t in M.index]]


def centre_figure(M: pd.DataFrame, type_names: dict):
    z = M.values
    lim = max(1.0, float(np.nanmax(np.abs(z))))
    fig = go.Figure(go.Heatmap(z=z, x=[pdta.feature_label(c) for c in M.columns], y=[type_names.get(t, f"Type {t}") for t in M.index],
                               zmin=-lim, zmax=lim, colorscale="RdBu_r", text=np.round(z, 1), texttemplate="%{text}",
                               colorbar=dict(title="z moyen")))
    fig.update_layout(height=120 + 60 * len(M), margin=dict(l=10, r=10, t=10, b=10), yaxis=dict(autorange="reversed"))
    return fig


def cluster_table(d: pd.DataFrame, labels: pd.DataFrame, quality: pd.DataFrame, fam: str, types) -> pd.DataFrame:
    """Une ligne par cluster affiché : nom, effectif, part au cœur, netteté, description en français."""
    rows = []
    for t in types:
        g = d[d.type == t]
        lab = labels[(labels.str_family == fam) & (labels.int_type == int(t))]
        rows.append({"Cluster": lab["name"].iloc[0] if len(lab) else f"Type {t}", "Joueurs (carte)": len(g),
                     "Au cœur du type": f"{100 * g.coeur.mean():.0f} %" if g.marge.notna().any() else "n.c.",
                     "Netteté": ins.sharpness(quality, fam, t), "Description": lab.description_fr.iloc[0] if len(lab) else ""})
    return pd.DataFrame(rows)


# ── Comparaison de deux joueurs ──────────────────────────────────────────────────────────────
def profile_row(profiles: pd.DataFrame, pid: str, season: str) -> pd.Series | None:
    r = profiles[(profiles.str_player_id == pid) & (profiles.str_season == season)]
    return r.iloc[0] if len(r) else None


def compare_variables(profiles: pd.DataFrame, ra: pd.Series, rb: pd.Series, rel: pd.Series, groups: pd.Series) -> pd.DataFrame:
    """Une ligne par variable : valeurs et percentiles des deux joueurs (chacun face aux joueurs de son poste et de sa ligue)."""
    ta = ins.variable_table(profiles, ra, rel, groups)
    tb = ins.variable_table(profiles, rb, rel, groups)
    m = ta[["name", "Famille", "Variable", "valeur", "percentile", "fiabilite"]].merge(
        tb[["name", "valeur", "percentile"]], on="name", suffixes=("_a", "_b"))
    m["ecart"] = (m.percentile_a - m.percentile_b).abs()
    return m


def compare_axes(profiles: pd.DataFrame, ra: pd.Series, rb: pd.Series, rel: pd.Series) -> pd.DataFrame:
    a = ins.axis_table(profiles, ra, rel)[["Aptitude", "percentile"]]
    b = ins.axis_table(profiles, rb, rel)[["Aptitude", "percentile"]]
    return a.merge(b, on="Aptitude", suffixes=("_a", "_b"))


def compare_radar(ax: pd.DataFrame, name_a: str, name_b: str):
    d = ax.dropna()
    th = list(d.Aptitude) + [d.Aptitude.iloc[0]]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=[50] * len(th), theta=th, mode="lines", line=dict(color="#7f7f7f", dash="dot", width=1),
                                  hoverinfo="skip", name="médiane des pairs"))
    for col, nm, c in (("percentile_a", name_a, "#0072B2"), ("percentile_b", name_b, "#D55E00")):
        fig.add_trace(go.Scatterpolar(r=list(d[col]) + [d[col].iloc[0]], theta=th, fill="toself", name=nm, line=dict(color=c),
                                      opacity=0.55, hovertemplate="%{theta}<br>percentile %{r:.0f}<extra>" + nm + "</extra>"))
    fig.update_layout(title="Aptitudes (percentile parmi les joueurs du même poste)", height=400, margin=dict(l=40, r=40, t=50, b=30),
                      polar=dict(radialaxis=dict(range=[0, 100], tickvals=[25, 50, 75])), legend=dict(orientation="h", y=-0.1))
    return fig
