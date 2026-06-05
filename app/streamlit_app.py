"""
streamlit_app.py — Dashboard de santé des features du pipeline 3-Étoiles.

Usage :
    streamlit run app/streamlit_app.py
"""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

# ── Résolution des imports ────────────────────────────────────────────────────
# app/ n'est pas dans sys.path par défaut quand on lance depuis la racine.
# On ajoute la racine du projet pour pouvoir importer app.data_loader.
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from app.data_loader import get_feature_tables, get_filter_values, get_null_stats, get_coverage_by_season,get_feature_importance, get_correlations

# ── Configuration de la page ─────────────────────────────────────────────────
st.set_page_config(
    page_title="3-Étoiles · Feature Health",
    page_icon="⚽",
    layout="wide",
)

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — Sélection de la table et des filtres
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("⚽ Feature Health Dashboard")
st.sidebar.markdown("---")

# Dropdown : quelle table visualiser ?
# get_feature_tables() est appelée une fois au démarrage (pas de @cache_data
# nécessaire ici car c'est rapide et appelée une seule fois).
available_tables = get_feature_tables()
selected_table = st.sidebar.selectbox(
    "📋 Table à analyser",
    options=available_tables,
    # Affiche 'features_final' en premier si disponible, sinon le premier de la liste
    index=available_tables.index("features_final") if "features_final" in available_tables else 0,
)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 Filtres")

# Filtres dynamiques selon la table sélectionnée
# get_filter_values() retourne (ligues, saisons) disponibles pour cette table
available_leagues, available_seasons = get_filter_values(selected_table)

BIG_5 = ["Premier League", "Ligue 1", "Bundesliga", "Serie A", "La Liga"]

only_big5 = st.sidebar.checkbox("🏆 Big 5 uniquement", value=False)

# Si Big 5 coché : on force la sélection sur les 5 ligues majeures
# On intersecte avec available_leagues au cas où une ligue manque dans la table
big5_default = [l for l in BIG_5 if l in available_leagues] if only_big5 else []

selected_leagues = st.sidebar.multiselect(
    "Championnat(s)",
    options=available_leagues,
    default=big5_default,
    placeholder="Toutes les ligues",
    # Désactiver le multiselect quand Big 5 est coché pour éviter la confusion
    disabled=only_big5,
)

selected_seasons = st.sidebar.multiselect(
    "Saison(s)",
    options=available_seasons,
    default=[],          # vide = toutes les saisons
    placeholder="Toutes les saisons",
)

# ── Titre principal dynamique ────────────────────────────────────────────────
st.title(f"📊 Feature Health — `gold.{selected_table}`")

# Résumé des filtres actifs sous le titre
filter_summary = []
if selected_leagues:
    filter_summary.append(f"**Ligues :** {', '.join(selected_leagues)}")
if selected_seasons:
    filter_summary.append(f"**Saisons :** {', '.join(selected_seasons)}")
if filter_summary:
    st.caption(" · ".join(filter_summary))
else:
    st.caption("Toutes ligues · Toutes saisons")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# ONGLETS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "🔴 Data Quality",
    "📦 Couverture",
    "🏆 Feature Importance",
    "🔗 Corrélations",
])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — Data Quality (% NULL)
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Taux de valeurs NULL par feature")
    st.markdown(
        "Une feature à **0 %** est parfaitement remplie. "
        "Au-delà de **50 %**, elle est peu fiable pour l'entraînement."
    )

    # On wrappe dans @st.cache_data via une fonction locale pour bénéficier
    # du cache Streamlit avec les arguments dynamiques
    @st.cache_data
    def cached_null_stats(table, leagues_tuple, seasons_tuple):
        # Les listes ne sont pas hashables → on les reçoit en tuple pour le cache
        return get_null_stats(table, list(leagues_tuple), list(seasons_tuple))

    df_null = cached_null_stats(
        selected_table,
        tuple(selected_leagues),
        tuple(selected_seasons),
    )

    if df_null.empty:
        st.warning("Aucune donnée disponible pour ces filtres.")
    else:
        # ── Métriques résumées ───────────────────────────────────────────────
        total_features = len(df_null)
        features_ok    = (df_null["null_pct"] == 0).sum()
        features_warn  = ((df_null["null_pct"] > 0) & (df_null["null_pct"] <= 50)).sum()
        features_crit  = (df_null["null_pct"] > 50).sum()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Features totales",   total_features)
        col2.metric("✅ Complètes (0 %)",  features_ok)
        col3.metric("⚠️ Partielles (≤50 %)", features_warn)
        col4.metric("🔴 Critiques (>50 %)", features_crit)

        st.markdown("---")

        # ── Contrôles d'affichage ────────────────────────────────────────────
        col_left, col_right = st.columns([2, 1])
        with col_left:
            show_only_nulls = st.checkbox(
                "Afficher uniquement les features avec des NULLs",
                value=False,
            )
        with col_right:
            sort_order = st.radio(
                "Trier par",
                options=["% NULL décroissant", "Nom alphabétique"],
                horizontal=True,
            )

        # ── Filtrage et tri ──────────────────────────────────────────────────
        df_display = df_null.copy()
        if show_only_nulls:
            df_display = df_display[df_display["null_pct"] > 0]

        if sort_order == "% NULL décroissant":
            df_display = df_display.sort_values("null_pct", ascending=False)
        else:
            df_display = df_display.sort_values("feature_name")

        # ── Graphique bar horizontal ─────────────────────────────────────────
        # On colorie les barres selon le seuil : vert / orange / rouge
        df_display["statut"] = pd.cut(
            df_display["null_pct"],
            bins=[-1, 0, 50, 100],
            labels=["Complète", "Partielle", "Critique"],
        )

        fig = px.bar(
            df_display,
            x="null_pct",
            y="feature_name",
            orientation="h",
            color="statut",
            color_discrete_map={
                "Complète":  "#22c55e",   # vert
                "Partielle": "#f59e0b",   # orange
                "Critique":  "#ef4444",   # rouge
            },
            labels={"null_pct": "% NULL", "feature_name": "Feature", "statut": "Statut"},
            hover_data={"null_count": True, "total_count": True},
            height=max(400, len(df_display) * 18),  # hauteur adaptative
        )
        fig.update_layout(
            # Pas de categoryorder — on respecte l'ordre du DataFrame
            # Pour le tri "% NULL décroissant", on a trié ascending=False en amont,
            # donc Plotly doit afficher dans l'ordre inverse (les plus hauts en haut)
            yaxis={"autorange": "reversed"} if sort_order == "Nom alphabétique" else {"autorange": True},
            showlegend=True,
            margin={"l": 200},
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Tableau détaillé (repliable) ─────────────────────────────────────
        with st.expander("📋 Voir le tableau détaillé"):
            st.dataframe(
                df_display[["feature_name", "null_pct", "null_count", "total_count"]].rename(columns={
                    "feature_name": "Feature",
                    "null_pct":     "% NULL",
                    "null_count":   "Nb NULL",
                    "total_count":  "Total lignes",
                }),
                use_container_width=True,
                hide_index=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# ONGLETS 2, 3, 4 — Placeholders
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Couverture par saison")
    st.markdown(
        "Pourcentage de lignes **remplies** (non-NULL) par feature et par saison. "
        "🟩 100 % = aucun NULL · 🟥 0 % = colonne entièrement vide."
    )

    # Dictionnaire de groupement des features par famille
    # On détecte la famille par préfixe du nom de colonne
    FEATURE_GROUPS = {
        "xG & tirs":       lambda c: c.startswith(("np_xg_", "xg_", "shot_", "shots_")),
        "Pressing":        lambda c: c.startswith("ppda_"),
        "WhoScored":       lambda c: c.startswith("ws_"),
        "Draw signals":    lambda c: c.startswith(("f1_","f2_","f3_","f4_","f5_","f6_","f7_","f8_","f9_","f10_","f11_","f12_","f13_","f14_","f15_","f16_","f17_","f18_","f19_","f20_")),
        "Adversaire":      lambda c: c.startswith("opp_"),
        "Head-to-head":    lambda c: c.startswith("h2h_"),
        "Cotes & probas":  lambda c: c.startswith(("odds_", "pinnacle_", "market_")),
        "Squad quality":   lambda c: c.startswith("squad_"),
        "Forme & résultats": lambda c: c.startswith(("win_rate_", "points_", "roll_", "save_rate_", "red_card_", "sterility_", "press_", "shield_", "poss_", "fouls_", "defensive_", "keeper_")),
        "Contexte":        lambda c: True,  # tout le reste
    }

    def assign_group(col_name: str) -> str:
        """Retourne la famille d'une feature selon son préfixe."""
        for group, condition in FEATURE_GROUPS.items():
            if group == "Contexte":
                continue
            if condition(col_name):
                return group
        return "Contexte"

    @st.cache_data
    def cached_coverage(table, leagues_tuple):
        return get_coverage_by_season(table, list(leagues_tuple))

    df_cov = cached_coverage(selected_table, tuple(selected_leagues))

    if df_cov.empty:
        st.warning("Aucune donnée de couverture disponible pour cette table.")
    else:
        # Sélecteur de groupe de features
        all_groups = sorted(df_cov["feature_name"].apply(assign_group).unique())
        selected_group = st.selectbox(
            "Groupe de features",
            options=["Toutes"] + list(FEATURE_GROUPS.keys()),
            index=0,
        )

        df_plot = df_cov.copy()
        if selected_group != "Toutes":
            df_plot = df_plot[df_plot["feature_name"].apply(assign_group) == selected_group]

        if df_plot.empty:
            st.info(f"Aucune feature dans le groupe « {selected_group} » pour cette table.")
        else:
            fig2 = px.imshow(
                # Pivot : lignes = features, colonnes = saisons
                df_plot.pivot(index="feature_name", columns="season", values="fill_pct"),
                color_continuous_scale="RdYlGn",   # rouge → jaune → vert
                zmin=0,
                zmax=100,
                labels={"color": "% remplissage"},
                aspect="auto",
                height=max(400, df_plot["feature_name"].nunique() * 20),
            )
            fig2.update_layout(
                yaxis_title="Feature",
                coloraxis_colorbar={"title": "% rempli"},
                margin={"l": 220, "t": 80},
            )
            fig2.update_xaxes(
                title_text="Saison",
                side="top",          # axe principal en haut
                showticklabels=True,
                mirror=True,         # mirror=True reflète les étiquettes sur le côté opposé (bas)
            )
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("📋 Voir le tableau détaillé"):
            df_table = df_plot.pivot(
                index="feature_name", columns="season", values="fill_pct"
            ).reset_index()
            st.dataframe(df_table, use_container_width=True, hide_index=True)

# ******************************************************************************* #
# ONGLETS 3 — Feature Importance — LightGBM                                       #
# ******************************************************************************* #

with tab3:
    st.subheader("Feature Importance — LightGBM")
    st.markdown(
        "Importance de chaque feature selon les modèles LightGBM Stage 1. "
        "Métrique utilisée : **split** (nombre de fois qu'une feature est utilisée pour diviser un nœud)."
    )

    @st.cache_data
    def cached_importance():
        return get_feature_importance()

    df_imp = cached_importance()

    if df_imp.empty:
        st.warning("Joblib introuvable — lance d'abord `04_train.py`.")
    else:
        col_left, col_mid, col_right = st.columns([1, 1, 1])
        with col_left:
            selected_model = st.selectbox(
                "Modèle",
                options=["moyenne", "lgbm_home", "lgbm_away"],
                index=0,
            )
        with col_mid:
            importance_type = st.selectbox(
                "Métrique d'importance",
                options=["split", "gain"],
                index=0,
            )
        with col_right:
            top_n = st.slider("Top N features", min_value=10, max_value=50, value=20, step=5)

        # Filtrer sur le modèle sélectionné et garder le top N
        df_plot = (
            df_imp[df_imp["model"] == selected_model]
            .sort_values(importance_type, ascending=False)
            .head(top_n)
            .sort_values(importance_type, ascending=True)  # ascending=True pour que le plus haut soit en haut du bar horizontal
        )

        label = "Importance (splits)" if importance_type == "split" else "Importance (gain)"
        fig3 = px.bar(
            df_plot,
            x=importance_type,
            y="feature_name",
            orientation="h",
            color=importance_type,
            color_continuous_scale="Blues",
            labels={importance_type: label, "feature_name": "Feature"},
            height=max(400, top_n * 22),
        )
        
        fig3.update_layout(
            coloraxis_showscale=False,  # la couleur est redondante avec la longueur des barres
            margin={"l": 220},
        )
        st.plotly_chart(fig3, use_container_width=True)

        with st.expander("📋 Voir le tableau complet"):
            df_full = (
                df_imp[df_imp["model"] == selected_model]
                .sort_values(importance_type, ascending=False)
                .reset_index(drop=True)
            )
            df_full.index += 1  # rang commence à 1
            st.dataframe(
                df_full[["feature_name", "split", "gain"]].rename(columns={
                    "feature_name": "Feature",
                    "split":        "Splits",
                    "gain":         "Gain",
                }),
                use_container_width=True,
            )

# ******************************************************************************* #
# Fonction utilitaire locale — évite de dupliquer le code du heatmap dans les deux modes
def _render_heatmap(df_corr: pd.DataFrame):
    """Affiche la heatmap de corrélation Plotly."""
    fig4 = px.imshow(
        df_corr,
        color_continuous_scale="RdBu_r",  # rouge = corrélation positive, bleu = négative
        zmin=-1,
        zmax=1,
        labels={"color": "Pearson r"},
        aspect="auto",
        height=max(400, len(df_corr) * 25),
        text_auto=".2f",   # affiche la valeur dans chaque cellule
    )
    fig4.update_layout(margin={"l": 180, "b": 180})
    fig4.update_xaxes(tickangle=45)
    st.plotly_chart(fig4, use_container_width=True)

# Connexion locale pour les vérifications légères dans l'onglet 4
# On importe _connect depuis data_loader directement
from app.data_loader import _connect as _connect_tab4
# ******************************************************************************* #


# ******************************************************************************* #
# ONGLETS 4 — Corrélations                                                        #
# ******************************************************************************* #
with tab4:
    st.subheader("Corrélations — Pearson")
    st.markdown(
        "Corrélation entre **-1** (inverse parfaite) et **+1** (identiques). "
        "Deux features très corrélées (|r| > 0.8) sont redondantes pour le modèle."
    )

    # Dictionnaire de groupement — même logique que l'onglet 2
    # Défini ici localement pour ne pas créer de dépendance entre onglets
    GROUPS_TAB4 = {
        "xG & tirs":         lambda c: c.startswith(("np_xg_", "xg_", "shot_", "shots_")),
        "Pressing":          lambda c: c.startswith("ppda_"),
        "WhoScored":         lambda c: c.startswith("ws_"),
        "Draw signals":      lambda c: c.startswith(("f1_","f2_","f3_","f4_","f5_","f6_","f7_","f8_","f9_","f10_","f11_","f12_","f13_","f14_","f15_","f16_","f17_","f18_","f19_","f20_")),
        "Adversaire":        lambda c: c.startswith("opp_"),
        "Head-to-head":      lambda c: c.startswith("h2h_"),
        "Cotes & probas":    lambda c: c.startswith(("odds_", "pinnacle_", "market_")),
        "Squad quality":     lambda c: c.startswith("squad_"),
        "Forme & résultats": lambda c: c.startswith(("win_rate_", "points_", "roll_", "save_rate_", "red_card_", "sterility_", "press_", "shield_", "poss_", "fouls_", "defensive_", "keeper_")),
        "Contexte":          lambda c: True,
    }

    def assign_group_tab4(col_name: str) -> str:
        for group, condition in GROUPS_TAB4.items():
            if group == "Contexte":
                continue
            if condition(col_name):
                return group
        return "Contexte"

    # ── Sélection du mode ────────────────────────────────────────────────────
    mode_corr = st.radio(
        "Mode de sélection des features",
        options=["🏆 Top N par importance", "📁 Par groupe de features"],
        horizontal=True,
    )

    st.markdown("---")

    # ── Mode A : Top N par importance ────────────────────────────────────────
    if mode_corr == "🏆 Top N par importance":

        @st.cache_data
        def cached_imp_corr():
            return get_feature_importance()

        df_imp_corr = cached_imp_corr()

        if df_imp_corr.empty:
            st.warning("Joblib introuvable — lance d'abord `04_train.py`.")
        else:
            col_l, col_r = st.columns([1, 1])
            with col_l:
                model_corr = st.selectbox(
                    "Modèle de référence pour l'importance",
                    options=["moyenne", "lgbm_home", "lgbm_away"],
                    key="corr_model",   # key unique — évite le conflit avec le selectbox de l'onglet 3
                )
            with col_r:
                top_n_corr = st.slider(
                    "Top N features",
                    min_value=5, max_value=40, value=20, step=5,
                    key="corr_topn",
                )

            # On récupère les top N features du modèle sélectionné
            top_features = (
                df_imp_corr[df_imp_corr["model"] == model_corr]
                .sort_values("gain", ascending=False)
                .head(top_n_corr)["feature_name"]
                .tolist()
            )

            # On vérifie que ces features existent dans la table sélectionnée
            _, available_seasons_corr = get_filter_values(selected_table)
            with _connect_tab4() as _:
                pass  # pas besoin — on réutilise get_correlations directement

            @st.cache_data
            def cached_corr_topn(table, leagues_t, seasons_t, features_t):
                return get_correlations(table, list(leagues_t), list(seasons_t), list(features_t))

            try:
                df_corr = cached_corr_topn(
                    selected_table,
                    tuple(selected_leagues),
                    tuple(selected_seasons),
                    tuple(top_features),
                )
                _render_heatmap(df_corr)
            except Exception as e:
                st.warning(f"Impossible de calculer les corrélations : {e}")

    # ── Mode B : Par groupe ──────────────────────────────────────────────────
    else:
        selected_group_corr = st.selectbox(
            "Groupe de features",
            options=list(GROUPS_TAB4.keys()),
            key="corr_group",
        )

        # Récupérer les colonnes numériques de la table pour filtrer par groupe
        @st.cache_data
        def cached_numeric_cols(table):
            with _connect_tab4() as con:
                return [
                    row[0] for row in con.execute(f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'gold' AND table_name = '{table}'
                          AND data_type IN ('DOUBLE', 'FLOAT', 'INTEGER', 'BIGINT')
                        ORDER BY ordinal_position
                    """).fetchall()
                ]

        all_numeric = cached_numeric_cols(selected_table)
        group_features = [
            c for c in all_numeric
            if assign_group_tab4(c) == selected_group_corr
        ]

        if not group_features:
            st.info(f"Aucune feature numérique dans le groupe « {selected_group_corr} » pour cette table.")
        else:
            st.caption(f"{len(group_features)} features dans ce groupe.")

            @st.cache_data
            def cached_corr_group(table, leagues_t, seasons_t, features_t):
                return get_correlations(table, list(leagues_t), list(seasons_t), list(features_t))

            try:
                df_corr = cached_corr_group(
                    selected_table,
                    tuple(selected_leagues),
                    tuple(selected_seasons),
                    tuple(group_features),
                )
                _render_heatmap(df_corr)
            except Exception as e:
                st.warning(f"Impossible de calculer les corrélations : {e}")