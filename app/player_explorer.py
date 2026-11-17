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
import app.cluster_view as cv  # noqa: E402
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

tab_card, tab_clusters, tab_types = st.tabs(["Fiche joueur", "Clusters", "Types et contrôle"])

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

# ── Clusters ──────────────────────────────────────────────────────────────────────────────────
with tab_clusters:
    st.subheader("Les clusters sont-ils distincts ou mélangés ?")
    version = pdta.types_version(prof)
    if version == "v1":
        st.warning("La table de types est ancienne (v1) : relancer `python pipelines/features/trait_types.py` pour la version v2 "
                   "(variables stables, type global sur 8 aptitudes, indicateur « au cœur »).")
    st.caption(f"Saison choisie dans la barre latérale : **{season}**. Chaque point est un joueur (joueurs de champ, au moins 450 minutes). "
               "Chaque ellipse contient 80 % des joueurs de son cluster (tous les joueurs du cluster, pas seulement ceux dont le point est affiché).")
    c1, c2, c3, c4, c5 = st.columns([1.5, 1, 1.4, 1, 1.3])
    fam_c = c1.selectbox("Caractéristique à visualiser", list(pdta.FAMILIES_FR), index=list(pdta.FAMILIES_FR).index("global"),
                         format_func=lambda f: pdta.FAMILIES_FR[f], key="cl_fam")
    n_per = int(c2.number_input("Joueurs par cluster", min_value=1, max_value=300, value=cv.DEFAULT_PER_CLUSTER, step=1, key="cl_n",
                                help=f"Le clic sur un point n'est actif que jusqu'à {cv.CLICK_MAX} joueurs par cluster."))
    mode = c3.selectbox("Joueurs affichés", list(cv.MODES), format_func=cv.MODES.get, key="cl_mode",
                        help="« Au hasard » montre le mélange réel ; « les plus typiques » montre le cœur de chaque cluster (flatteur).")
    role_c = c4.selectbox("Poste", ["Tous", "CB", "FB", "MID", "WIDE", "ST"],
                          format_func=lambda r: r if r == "Tous" else pdta.ROLES_FR.get(r, r), key="cl_role")
    league_c = c5.selectbox("Championnat", ["Tous"] + sorted(prof[prof.str_season == season].str_league_source.dropna().unique()), key="cl_league")
    pts = cv.season_points(prof, season, fam_c, None if role_c == "Tous" else role_c, None if league_c == "Tous" else league_c)
    if pts is None:
        st.info("Pas assez de joueurs typés pour cette combinaison saison / poste / championnat.")
    else:
        D, names = pts["df"], cv.names_for_season(cat, season)
        lb_c = lab[lab.str_family == fam_c].set_index("int_type")
        tnames = {int(t): lb_c.loc[t, "name"] for t in lb_c.index}
        present = sorted(D.type.unique())
        chosen = st.multiselect("Clusters affichés", present, default=present, format_func=lambda t: tnames.get(t, f"Type {t}"), key=f"cl_types_{fam_c}")
        if not chosen:
            st.info("Choisir au moins un cluster.")
        else:
            shown = cv.pick_players(D, n_per, mode, allowed_ids=names.player_id, types=chosen)
            can_click = cv.clickable(n_per)
            ss = st.session_state
            ss.setdefault("cl_cmp", False); ss.setdefault("cl_picks", []); ss.setdefault("cl_bump", 0); ss.setdefault("cl_last", ())
            b1, b2, b3 = st.columns([1.2, 1, 3])
            if b1.button("Quitter la comparaison" if ss["cl_cmp"] else "Comparer deux joueurs", key="cl_cmp_btn", type="primary" if ss["cl_cmp"] else "secondary"):
                ss["cl_cmp"] = not ss["cl_cmp"]; ss["cl_picks"] = []; ss["cl_last"] = (); ss["cl_bump"] += 1; st.rerun()
            if ss["cl_cmp"] and b2.button("Réinitialiser", key="cl_reset"):
                ss["cl_picks"] = []; ss["cl_last"] = (); ss["cl_bump"] += 1; st.rerun()
            if not can_click:
                b3.info(f"Clic désactivé : {n_per} joueurs par cluster (maximum {cv.CLICK_MAX} pour cliquer). Réduire le nombre pour pouvoir cliquer.")
            elif ss["cl_cmp"]:
                b3.info("Mode comparaison : cliquer sur deux joueurs (A puis B). Un troisième clic remplace le plus ancien.")
            else:
                b3.caption("Cliquer sur un point pour voir le joueur et son club.")
            fig, trace_ids = cv.build_figure(D[D.type.isin(chosen)], shown, tnames, names, picks=ss["cl_picks"] if ss["cl_cmp"] else (), n_per=n_per)
            sig = f"{season}|{fam_c}|{role_c}|{league_c}|{n_per}|{mode}|{','.join(map(str, chosen))}|{ss['cl_bump']}"
            cfg = {"displayModeBar": False, "scrollZoom": False}
            clicked = []
            if can_click:
                ev = st.plotly_chart(fig, key=f"cl_map_{sig}", on_select="rerun", selection_mode="points", config=cfg, **STRETCH)
                clicked = cv.clicked_players(((ev or {}).get("selection") or {}).get("points", []), trace_ids)
            else:
                st.plotly_chart(fig, key=f"cl_map_static_{sig}", config=cfg, **STRETCH)
            st.caption(f"Carte en 2 dimensions : elle restitue {100 * pts['var2d']:.0f} % de l'information de l'espace réel des types. "
                       f"{100 * cv.overlap_share(D[D.type.isin(chosen)]):.0f} % des joueurs sont, sur la carte, plus près du centre d'un autre cluster "
                       "que du leur. Une projection déforme les distances : deux clusters qui se recouvrent ici peuvent être séparés dans l'espace réel ; "
                       "la netteté du tableau ci-dessous est la mesure de référence.")
            sel = tuple(clicked)
            if sel != ss["cl_last"]:
                ss["cl_last"] = sel
                if ss["cl_cmp"] and sel:
                    ss["cl_picks"] = cv.update_picks(ss["cl_picks"], list(sel))
                    st.rerun()
            nm_idx = names.set_index("player_id")

            def who(i):
                r = nm_idx.loc[i] if i in nm_idx.index else None
                return (r.player, r.team, r.championnat) if r is not None else (f"Joueur {i}", "club inconnu", "")

            if not ss["cl_cmp"]:
                if clicked:
                    i = clicked[0]; nm_, club_, lg_ = who(i)
                    row = cv.profile_row(prof, i, season)
                    st.markdown(f"### {nm_}")
                    st.markdown(f"**Club : {club_}**" + (f" · {lg_}" if lg_ else ""))
                    if row is not None:
                        st.caption(f"{pdta.ROLES_FR.get(row.str_macro_role, row.str_macro_role)} · {int(row.int_minutes)} minutes · cluster : "
                                   f"{tnames.get(int(row[f'int_type_{fam_c}']), '?')}"
                                   + ("" if pd.isna(row.get(f'dec_margin_{fam_c}')) else
                                      (" (au cœur du cluster)" if row[f'dec_margin_{fam_c}'] < pdta.tt.CORE_RATIO else " (profil entre deux clusters)")))
            else:
                a_, b_ = (ss["cl_picks"] + [None, None])[:2]
                st.markdown("#### Comparaison")
                if a_ is None or b_ is None:
                    st.caption("Joueur A : " + (who(a_)[0] if a_ else "à cliquer") + " · Joueur B : " + (who(b_)[0] if b_ else "à cliquer"))
                else:
                    ra, rb = cv.profile_row(prof, a_, season), cv.profile_row(prof, b_, season)
                    (na, ca, la), (nb, cb, lb_) = who(a_), who(b_)
                    h1, h2 = st.columns(2)
                    for col, (nn, cc, ll, rr, tag) in zip((h1, h2), ((na, ca, la, ra, "A"), (nb, cb, lb_, rb, "B"))):
                        col.markdown(f"**{tag} ({"bleu" if tag == "A" else "orange"}) · {nn}**  \n{cc} · {ll}  \n{pdta.ROLES_FR.get(rr.str_macro_role, rr.str_macro_role)} · {int(rr.int_minutes)} minutes")
                    rel, grp = reliability(S)
                    ax = cv.compare_axes(prof, ra, rb, rel)
                    g1, g2 = st.columns([1, 1.5])
                    g1.plotly_chart(cv.compare_radar(ax, na, nb), **STRETCH)
                    ta, tb = pdta.player_types(ra, lab, prof), pdta.player_types(rb, lab, prof)
                    tt_ = ta[["Famille", "Type", "Ressemblance"]].merge(tb[["Famille", "Type", "Ressemblance"]], on="Famille", how="outer", suffixes=(" A", " B"))
                    g2.dataframe(tt_, hide_index=True, **STRETCH)
                    cmpv = cv.compare_variables(prof, ra, rb, rel, grp)
                    fiable = cmpv[cmpv.fiabilite >= ins.MIN_RELIABILITY].sort_values("ecart", ascending=False)
                    fmt = lambda d: d.assign(**{"Variable": d.Variable, "A (valeur)": d.valeur_a.round(2), "A (percentile)": d.percentile_a.round(0),
                                                "B (valeur)": d.valeur_b.round(2), "B (percentile)": d.percentile_b.round(0),
                                                "Écart de percentile": d.ecart.round(0)})[["Famille", "Variable", "A (valeur)", "A (percentile)", "B (valeur)", "B (percentile)", "Écart de percentile"]]
                    st.markdown("**Les 12 plus gros écarts entre A et B (variables fiables)**")
                    st.dataframe(fmt(fiable.head(12)), hide_index=True, **STRETCH)
                    with st.expander("Toutes les variables"):
                        st.dataframe(fmt(cmpv.sort_values("ecart", ascending=False)), hide_index=True, **STRETCH)
                    st.caption("Les percentiles de chaque joueur sont calculés face aux joueurs de SON poste, de sa ligue et de la même saison : "
                               "deux joueurs de postes différents se comparent donc chacun à leurs pairs, pas l'un à l'autre.")

            st.markdown("#### Caractéristiques des clusters affichés")
            Qc = quality(S)
            st.table(cv.cluster_table(D, lab, Qc, fam_c, chosen).set_index("Cluster"))
            M = cv.centre_matrix(D, pts["cols"], pts["X"], chosen)
            st.plotly_chart(cv.centre_figure(M, tnames), **STRETCH)
            st.caption("z moyen des joueurs de chaque cluster sur les variables qui ont servi à former les types (0 = moyenne des joueurs du même poste ; "
                       "rouge = au-dessus, bleu = en dessous). « Au cœur du type » : part des joueurs nettement plus proches du centre de leur cluster "
                       "que du deuxième ; les autres sont des profils intermédiaires.")

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
    if pdta.types_version(prof) == "v1":
        st.warning("Table de types ancienne (v1) : relancer `python pipelines/features/trait_types.py` pour obtenir la version v2.")
    elif fam == "global":
        st.warning("Le type global reste un découpage d'un continuum de profils : à lire comme un repère. Il est plus net que dans la version "
                   "précédente (silhouette moyenne 0,16 contre 0,05 mesurée sur la version précédente), surtout pour les joueurs « au cœur » du type.")
    ov = qf.reset_index().merge(lb[["int_type", "name"]], left_on="type", right_on="int_type")
    ov = ov.assign(**{"Type": ov["name"], "Part des joueurs": (100 * ov.part).round(0).astype(int).astype(str) + " %",
                      "Netteté": ov.nettete, "Silhouette": ov.silhouette.round(2), "Cohésion": ov.cohesion.round(2),
                      "Au cœur": ov.part_coeur.map(lambda v: "n.c." if pd.isna(v) else f"{100 * v:.0f} %"),
                      "Silhouette (au cœur)": ov.silhouette_coeur.round(2),
                      "Séparation": ov.separation.round(2), "Rétention": (100 * ov.retention).round(0).astype(str) + " %",
                      "Élévation": ov.elevation.round(1)})
    st.dataframe(ov[["Type", "Part des joueurs", "Netteté", "Silhouette", "Au cœur", "Silhouette (au cœur)", "Cohésion", "Séparation", "Rétention", "Élévation"]],
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
