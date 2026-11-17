"""
player_explorer.py — Explorateur de joueurs : saison > championnat > club > joueur, fiche complète.

Fiche : infos de base, stats de la saison, caractérisation (profil de comportement, types par famille et global, occupation du
terrain), joueurs similaires (un par championnat). Onglet « Types et contrôle » : ce que contient chaque type, avec des
joueurs exemples, pour vérifier que les profils sont crédibles.

Usage :   streamlit run app/player_explorer.py
Base :    config.yaml (paths.duckdb) ; variable d'environnement P3E_DB pour en indiquer une autre.
La base est ouverte en lecture seule, le temps de la requête, puis refermée : dbt peut tourner pendant que l'application est ouverte.
Les tables nécessaires (profil, types) viennent de dbt run gold_player_behaviour_profile puis pipelines/features/trait_types.py.
"""
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
import app.player_data as pdta  # noqa: E402
import app.player_insights as ins  # noqa: E402

st.set_page_config(page_title="3-Étoiles · Joueurs", page_icon="⚽", layout="wide")

POS, NEG, GREY = "#1f77b4", "#d62728", "#7f7f7f"
# Streamlit récent : width="stretch" ; ancien : use_container_width (supprimé dans les versions récentes)
_V = tuple(int(x) for x in st.__version__.split(".")[:2])
STRETCH = {"width": "stretch"} if _V >= (1, 52) else {"use_container_width": True}


def db_path() -> Path:
    if os.environ.get("P3E_DB"):
        return Path(os.environ["P3E_DB"])
    with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
        return ROOT_DIR / yaml.safe_load(f)["paths"]["duckdb"]


def run(fn, *args):
    """Exécute une fonction de player_data sur une connexion en lecture seule, fermée aussitôt."""
    con = duckdb.connect(str(db_path()), read_only=True)
    try:
        return fn(con, *args)
    finally:
        con.close()


def stamp() -> float:
    """Date de la base : change quand dbt la met à jour, ce qui vide le cache."""
    return os.path.getmtime(db_path())


@st.cache_data(show_spinner="Chargement du catalogue…")
def catalog(_stamp):
    return run(pdta.load_catalog)


@st.cache_data(show_spinner="Chargement des profils…")
def profiles(_stamp):
    return run(pdta.load_profiles)


@st.cache_data
def labels(_stamp):
    return run(pdta.load_type_labels)


@st.cache_data(show_spinner=False)
def info(_stamp, pid, season, team):
    return run(pdta.player_info, pid, season, team)


@st.cache_data(show_spinner=False)
def stats(_stamp, pid, season, team):
    return run(pdta.season_stats, pid, season, team)


@st.cache_data(show_spinner="Calcul de la fiabilité des variables…")
def reliability(_stamp):
    return ins.feature_reliability(profiles(_stamp)), ins.feature_groups(profiles(_stamp))


@st.cache_data(show_spinner="Calcul de la netteté des types (une fois, environ une minute)…")
def quality(_stamp):
    return ins.type_quality(profiles(_stamp))


@st.cache_data(show_spinner="Calcul de la cohérence…")
def coherence(_stamp, season):
    return pdta.neighbour_coherence(profiles(_stamp), season)


def reliability_label(v) -> str:
    return "n.c." if v is None or pd.isna(v) else ("bonne" if v >= 0.55 else ("moyenne" if v >= ins.MIN_RELIABILITY else "faible (instable)"))


def pbar(df: pd.DataFrame, title: str, height: int = None):
    """Barres de percentile (0 à 100, centre = médiane des pairs). Les variables peu fiables sont estompées."""
    d = df.dropna(subset=["percentile"]).sort_values("percentile")
    fig = go.Figure(go.Bar(
        x=d.percentile - 50, y=d.Variable, orientation="h", text=[f"{v:.0f}" for v in d.percentile], textposition="outside", cliponaxis=False,
        marker_color=[POS if v >= 50 else NEG for v in d.percentile],
        marker_opacity=[1.0 if (f >= ins.MIN_RELIABILITY) else 0.3 for f in d.fiabilite.fillna(0)],
        customdata=list(zip(d.percentile.round(0), d.valeur.round(2), d.mediane.round(2), d.fiabilite.round(2))),
        hovertemplate="%{y}<br>percentile : %{customdata[0]}<br>valeur : %{customdata[1]} (médiane des pairs %{customdata[2]})"
                      "<br>fiabilité : %{customdata[3]}<extra></extra>"))
    fig.update_layout(title=title, height=height or 70 + 28 * len(d), margin=dict(l=10, r=10, t=50, b=30),
                      xaxis=dict(range=[-58, 58], tickvals=[-50, -25, 0, 25, 50], ticktext=["0", "25", "50", "75", "100"],
                                 title="percentile (barre estompée = variable peu fiable)"))
    return fig


def radar(ax: pd.DataFrame):
    d = ax.dropna(subset=["percentile"])
    th = list(d.Aptitude) + [d.Aptitude.iloc[0]]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=[50] * len(th), theta=th, mode="lines", line=dict(color=GREY, dash="dot", width=1),
                                  hoverinfo="skip", name="médiane des pairs"))
    fig.add_trace(go.Scatterpolar(r=list(d.percentile) + [d.percentile.iloc[0]], theta=th, fill="toself", name="joueur",
                                  line=dict(color=POS), hovertemplate="%{theta}<br>percentile %{r:.0f}<extra></extra>"))
    fig.update_layout(title="Aptitudes (percentile parmi les joueurs du même poste)", height=380, showlegend=False,
                      margin=dict(l=40, r=40, t=50, b=20), polar=dict(radialaxis=dict(range=[0, 100], tickvals=[25, 50, 75])))
    return fig


def occupation(row: pd.Series):
    z = [[row[f"dec_sec_{d}_{s}"] * 100 for s, _ in pdta.SIDES] for d, _ in pdta.SECTORS]
    fig = px.imshow(z, x=[n for _, n in pdta.SIDES], y=[n for _, n in pdta.SECTORS], text_auto=".0f", aspect="auto",
                    color_continuous_scale="Blues")
    fig.update_layout(title="Occupation du terrain (% des touches, repère de l'équipe)", height=300,
                      margin=dict(l=10, r=10, t=50, b=10), coloraxis_showscale=False)
    return fig


# ── Base disponible ? ─────────────────────────────────────────────────────────────────────────
try:
    S = stamp()
    cat = catalog(S)
    prof = profiles(S)
    lab = labels(S)
except Exception as e:                       # base absente, verrouillée par une écriture, tables non construites
    st.error("Impossible de lire les données. Causes possibles : une écriture dbt est en cours sur la base, ou les tables "
             "`gold.gold_player_behaviour_profile` et `machine_learning.player_trait_types` ne sont pas construites "
             "(dbt run gold_player_behaviour_profile, puis python pipelines/features/trait_types.py).")
    st.exception(e)
    st.stop()

# ── Sélection ─────────────────────────────────────────────────────────────────────────────────
st.sidebar.title("⚽ Explorateur de joueurs")
seasons = sorted(cat.season.unique(), reverse=True)
default_season = "2025-2026" if "2025-2026" in seasons else seasons[0]
season = st.sidebar.selectbox("Saison", seasons, index=seasons.index(default_season))
cs = cat[cat.season == season]
league = st.sidebar.selectbox("Championnat", sorted(cs.league.unique()))
cl = cs[cs.league == league]
teams = cl[["team_id", "team"]].drop_duplicates().sort_values("team")
team_name = st.sidebar.selectbox("Club", teams.team.tolist())
team_id = teams[teams.team == team_name].team_id.iloc[0]
cp = cl[cl.team_id == team_id].sort_values("minutes", ascending=False)
labels_p = {f"{r.player} ({int(r.minutes)} min)": r.player_id for r in cp.itertuples()}
player_label = st.sidebar.selectbox("Joueur", list(labels_p))
pid = labels_p[player_label]
st.sidebar.markdown("---")
same_role = st.sidebar.checkbox("Joueurs similaires : même poste uniquement", value=True)
per_league = st.sidebar.slider("Joueurs similaires par championnat", 1, 3, 1)
if season >= "2026-2027":
    st.sidebar.warning("Saison en cours : peu de matchs, profils incomplets.")

tab_card, tab_types = st.tabs(["Fiche joueur", "Types et contrôle"])

# ── Fiche joueur ──────────────────────────────────────────────────────────────────────────────
with tab_card:
    inf, stt = info(S, pid, season, team_id), stats(S, pid, season, team_id)
    st.header(f"{cp[cp.player_id == pid].player.iloc[0]}")
    st.caption(f"{team_name} · {league} · saison {season}")
    c = st.columns([1, 1, 1, 2, 1.6, 1])
    c[0].metric("Âge", inf["age"] if inf["age"] else "n.c.")
    c[1].metric("Taille", f"{inf['height']} cm" if inf["height"] else "n.c.")
    c[2].metric("Poids", f"{inf['weight']} kg" if inf["weight"] else "n.c.")
    c[3].metric("Poste (détail)", pdta.ROLES_FIN_FR.get(inf["role_fin"], "n.c."))
    c[4].metric("Matchs / titularisations", f"{int(stt['matches'])} / {inf['starts'] if inf['starts'] is not None else 'n.c.'}")
    c[5].metric("Minutes", int(stt["minutes"]))
    st.caption("Nationalité et date de naissance : absentes de la base. Âge = âge maximal relevé sur la feuille de match de la saison.")

    st.subheader("Statistiques de la saison")
    tab_stats = pdta.stats_table(stt, inf["starts"])
    st.dataframe(tab_stats, hide_index=True, height=35 * (len(tab_stats) + 1) + 3, **STRETCH)
    st.caption("Championnat uniquement, pour ce club. Contribution xG : variable de la source, définition à confirmer.")

    st.subheader("Caractérisation")
    me = prof[(prof.str_player_id == pid) & (prof.str_season == season)]
    if me.empty:
        st.info("Pas de profil de comportement pour ce joueur à cette saison.")
    elif me.iloc[0].str_macro_role == "GK":
        st.info("Profil gardien non construit : les gardiens n'ont pas de caractérisation pour l'instant.")
    else:
        row = me.iloc[0]
        st.caption(f"Poste de référence : {pdta.ROLES_FIN_FR.get(inf['role_fin'], pdta.ROLES_FR.get(row.str_macro_role))} "
                   f"({pdta.ROLES_FR.get(row.str_macro_role)}). Fenêtre de 3 saisons se terminant en {season} "
                   f"({int(row.int_minutes)} minutes). Écarts calculés contre les joueurs du même poste, de la même ligue et de la même saison.")
        if not row.bool_is_peer:
            st.warning("Moins de 450 minutes sur la fenêtre : profil indicatif, pas de type attribué.")
        REL, GRP = reliability(S)
        Q = quality(S)
        tb = ins.variable_table(prof, row, REL, GRP)
        forts, faibles = ins.headline(tb)
        axes = ins.axis_table(prof, row, REL)
        types = pdta.player_types(row, lab, prof)
        types["Netteté du type"] = [ins.sharpness(Q, f, t) for f, t in zip(types.famille_cle, types.type_id)]
        role_label = pdta.ROLES_FIN_FR.get(inf["role_fin"], pdta.ROLES_FR.get(row.str_macro_role))
        st.info(ins.summary_sentence(row, role_label, forts, faibles, types, Q, lab))
        st.caption(f"Pairs : {len(ins.peers_of(prof, row))} joueurs du même poste, de la même saison et de la ligue de référence du "
                   f"profil, {row.str_league_source} (la ligue où le joueur a le plus joué sur les 3 saisons de la fenêtre, "
                   "parfois différente de celle du club actuel), avec 450 minutes ou plus. Seules les variables fiables (corrélation d'une fenêtre à la suivante "
                   f">= {ins.MIN_RELIABILITY:.1f}) entrent dans la synthèse et dans les points forts et faibles.")
        left, right = st.columns([3, 2])
        with left:
            st.plotly_chart(radar(axes), **STRETCH)
            if len(forts) or len(faibles):
                pts = pd.concat([forts, faibles])
                st.plotly_chart(pbar(pts, "Points forts et points faibles (variables fiables, une par groupe de variables proches)"),
                                **STRETCH)
        with right:
            st.plotly_chart(occupation(row), **STRETCH)
        st.markdown("**Types du joueur**")
        st.dataframe(types[["Famille", "Type", "Netteté du type", "Ressemblance"]], hide_index=True, **STRETCH)
        with st.expander("Description des types de ce joueur"):
            st.dataframe(types[["Famille", "Type", "Netteté du type", "Ressemblance", "Description"]], hide_index=True, **STRETCH)
            st.caption("Le nom résume le type ; la description donne ses variables au-dessus et en dessous de la moyenne des joueurs du même "
                       "poste (écarts en écarts-types). « Ressemblance » : proximité du joueur au centre de son type, comparée aux autres "
                       "joueurs du type ; « faible » = profil atypique, le nom du type décrit mal ce joueur. « Netteté du type » : un type "
                       "flou recouvre les autres types, son nom décrit un centre et non un groupe distinct (voir l'onglet « Types et contrôle »).")
        with st.expander("Toutes les variables (valeur, médiane des pairs, percentile, fiabilité)"):
            full = tb.assign(Fiabilité=tb.fiabilite.map(reliability_label), Valeur=tb.valeur.round(2),
                             **{"Médiane des pairs": tb.mediane.round(2), "Percentile": tb.percentile.round(0), "z": tb.z.round(2),
                                "Corrélation s / s+3": tb.fiabilite.round(2)})
            st.dataframe(full[["Famille", "Variable", "Valeur", "Médiane des pairs", "Percentile", "z", "Fiabilité", "Corrélation s / s+3"]],
                         hide_index=True, **STRETCH)
            st.caption("Valeur : par 90 minutes, taux ou part, rétrécie vers la moyenne des pairs quand les minutes sont faibles. "
                       "Les valeurs brutes ne se comparent pas d'une saison à l'autre (les définitions d'événements dérivent) : "
                       "le percentile, oui.")
        for fam, ns in pdta.tt.FAMILIES.items():
            with st.expander(f"{pdta.FAMILIES_FR[fam]} : détail"):
                st.plotly_chart(pbar(tb[tb.name.isin(ns)], pdta.FAMILIES_FR[fam]), **STRETCH)

        st.subheader("Joueurs similaires")
        sim = pdta.similar_players(prof, cat, pid, season, same_role, per_league)
        if sim.empty:
            st.info("Aucun joueur comparable.")
        else:
            show = sim.rename(columns={"league": "Championnat", "player": "Joueur", "team": "Club", "str_macro_role": "Poste",
                                       "distance": "Distance", "percentile": "Plus proche que (% des candidats)",
                                       "int_type_global": "Type global", "points_communs": "Points communs",
                                       "plus_grands_ecarts": "Plus grands écarts"}).drop(columns="str_player_id")
            show["Poste"] = show["Poste"].map(pdta.ROLES_FR)
            gl = lab[lab.str_family == "global"]
            show["Type global"] = show["Type global"].map(dict(zip(gl.int_type, gl["name"])))
            cfg = {"Joueur": st.column_config.TextColumn(width="medium"), "Club": st.column_config.TextColumn(width="medium"),
                   "Points communs": st.column_config.TextColumn(width="large"),
                   "Plus grands écarts": st.column_config.TextColumn(width="large")}
            cfg["Type global"] = st.column_config.TextColumn(width="large")
            st.dataframe(show.round({"Distance": 2, "Plus proche que (% des candidats)": 1}), hide_index=True,
                         column_config=cfg, **STRETCH)
            st.caption("Distance dans l'espace des 56 variables des 8 familles (chaque famille pèse autant). ▲ / ▼ : les deux joueurs sont "
                       "au-dessus / en dessous des joueurs de leur poste. Même saison, joueurs de champ avec au moins 450 minutes. "
                       "Le « type global » est flou (voir l'onglet « Types et contrôle ») : à lire comme un repère.")

# ── Types et contrôle ─────────────────────────────────────────────────────────────────────────
with tab_types:
    st.subheader("Que contiennent les types ?")
    fams = list(pdta.tt.FAMILIES) + ["global"]
    fam = st.selectbox("Famille", fams, format_func=lambda f: pdta.FAMILIES_FR[f])
    lb = lab[lab.str_family == fam].sort_values("int_type")
    mix = pdta.type_role_mix(prof, season, fam)
    Q = quality(S)
    qf = Q[Q.family == fam].set_index("type")
    st.caption("Netteté d'un type (silhouette, de -1 à 1) : « nette » à partir de 0,25, « moyenne » à partir de 0,12, sinon « floue ». "
               "Un type flou recouvre les autres : son nom décrit le centre du groupe, pas une famille de joueurs bien séparée. "
               "Rétention : part des joueurs du type qui sont dans le même type trois saisons plus tard (élévation = rapport à la part du type, "
               "1 = hasard).")
    if fam == "global":
        st.warning("Les 8 types globaux sont tous flous : ce sont des découpes d'un continuum de profils. À lire comme des repères, pas "
                   "comme des familles de joueurs ; préférer le radar d'aptitudes et les types par famille.")
    ov = qf.reset_index().merge(lb[["int_type", "name"]], left_on="type", right_on="int_type")
    ov = ov.assign(**{"Type": ov["name"], "Part des joueurs": (100 * ov.part).round(0).astype(int).astype(str) + " %",
                      "Netteté": ov.nettete, "Silhouette": ov.silhouette.round(2), "Cohésion": ov.cohesion.round(2),
                      "Séparation": ov.separation.round(2), "Rétention": (100 * ov.retention).round(0).astype(str) + " %",
                      "Élévation": ov.elevation.round(1)})
    st.dataframe(ov[["Type", "Part des joueurs", "Netteté", "Silhouette", "Cohésion", "Séparation", "Rétention", "Élévation"]],
                 hide_index=True, **STRETCH)
    for t in lb.int_type:
        with st.expander(f"{lb[lb.int_type == t]['name'].iloc[0]} ({pdta.FAMILIES_FR[fam]}) · netteté {ins.sharpness(Q, fam, t)}",
                         expanded=False):
            st.write(lb[lb.int_type == t].description_fr.iloc[0])
            if t in mix.index:
                m = mix.loc[t].rename(index=pdta.ROLES_FR).sort_values(ascending=False)
                st.caption("Postes dans ce type (saison choisie) : " + ", ".join(f"{k} {100 * v:.0f} %" for k, v in m.items()))
            ex = pdta.representative_players(prof, cat, season, fam, int(t))
            if len(ex):
                ex = ex.rename(columns={"player": "Joueur", "team": "Club", "str_league_source": "Championnat",
                                        "str_macro_role": "Poste", f"dec_dist_{fam}": "Distance au centre"})
                ex["Poste"] = ex["Poste"].map(pdta.ROLES_FR)
                st.dataframe(ex.round(2), hide_index=True, **STRETCH)
    st.subheader("Contrôle de cohérence")
    co = coherence(S, season)
    if co:
        st.write(f"Le joueur le plus proche (tous postes confondus) a le même poste dans **{100 * co['share_same_role']:.0f} %** des cas "
                 f"(hasard : {100 * co['chance']:.0f} %), sur {co['n']} joueurs tirés au sort.")
        st.caption("Les écarts sont calculés contre le poste : deux joueurs ne se ressemblent donc pas parce qu'ils jouent au même endroit. "
                   "Un chiffre proche du hasard voudrait dire que les profils ne contiennent rien sur le poste ; un chiffre proche de 100 % que "
                   "les types retrouvent le poste. Ni l'un ni l'autre n'est visé.")
