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


@st.cache_data(show_spinner="Calcul de la cohérence…")
def coherence(_stamp, season):
    return pdta.neighbour_coherence(profiles(_stamp), season)


def zbar(df: pd.DataFrame, title: str, height: int = None):
    d = df.sort_values("z")
    fig = go.Figure(go.Bar(x=d.z.clip(-3.5, 3.5), y=d.Variable, orientation="h",
                           marker_color=[POS if v >= 0 else NEG for v in d.z], customdata=d.z.round(2),
                           hovertemplate="%{y}<br>écart aux pairs : %{customdata}<extra></extra>"))
    fig.update_layout(title=title, height=height or 60 + 26 * len(d), margin=dict(l=10, r=10, t=50, b=30),
                      xaxis=dict(range=[-3.6, 3.6], title="écart aux joueurs du même poste (en écarts-types)"))
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
        left, right = st.columns([3, 2])
        with left:
            fs = pdta.family_scores(row).dropna(subset=["z"])
            top = pd.concat([fs.nlargest(8, "z"), fs.nsmallest(8, "z")]).drop_duplicates("name")
            st.plotly_chart(zbar(top, "Points forts et points faibles (8 plus hauts, 8 plus bas)"), **STRETCH)
        with right:
            st.plotly_chart(occupation(row), **STRETCH)
            st.dataframe(pdta.player_types(row, lab)[["Famille", "Type"]], hide_index=True, **STRETCH)
        with st.expander("Description des types de ce joueur"):
            st.dataframe(pdta.player_types(row, lab), hide_index=True, **STRETCH)
            st.caption("Le numéro d'un type n'a pas de sens en soi. La description donne ses variables les plus hautes et les plus basses "
                       "(écarts en écarts-types).")
        for fam, ns in pdta.tt.FAMILIES.items():
            with st.expander(f"{pdta.FAMILIES_FR[fam]} : détail"):
                d = fs[fs.name.isin(ns)]
                st.plotly_chart(zbar(d, pdta.FAMILIES_FR[fam]), **STRETCH)

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
            cfg = {"Joueur": st.column_config.TextColumn(width="medium"), "Club": st.column_config.TextColumn(width="medium"),
                   "Points communs": st.column_config.TextColumn(width="large"),
                   "Plus grands écarts": st.column_config.TextColumn(width="large")}
            st.dataframe(show.round({"Distance": 2, "Plus proche que (% des candidats)": 1}), hide_index=True,
                         column_config=cfg, **STRETCH)
            st.caption("Distance dans l'espace des 56 variables des 8 familles (chaque famille pèse autant). ▲ / ▼ : les deux joueurs sont "
                       "au-dessus / en dessous des joueurs de leur poste. Même saison, joueurs de champ avec au moins 450 minutes.")

# ── Types et contrôle ─────────────────────────────────────────────────────────────────────────
with tab_types:
    st.subheader("Que contiennent les types ?")
    fams = list(pdta.tt.FAMILIES) + ["global"]
    fam = st.selectbox("Famille", fams, format_func=lambda f: pdta.FAMILIES_FR[f])
    lb = lab[lab.str_family == fam].sort_values("int_type")
    mix = pdta.type_role_mix(prof, season, fam)
    for t in lb.int_type:
        with st.expander(f"Type {t} : {pdta.FAMILIES_FR[fam]}", expanded=False):
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
