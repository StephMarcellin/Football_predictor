"""
Dashboard Streamlit — Football Prediction
==========================================
Visualisation des données et prédictions 1N2 en temps réel.

Usage :
    streamlit run app/streamlit_app.py
"""

import streamlit as st
import duckdb
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import pickle
import numpy as np
from pathlib import Path
import yaml

# ── Config ──────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = CFG["paths"]["db"]
MODELS_DIR = Path(CFG["paths"]["models"])

st.set_page_config(
    page_title="⚽ Football Prediction",
    page_icon="⚽",
    layout="wide",
)

# ── Chargement données ───────────────────────────────────────────────────────
@st.cache_data
def load_matches() -> pl.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT * FROM clean_matches ORDER BY match_date DESC").pl()
    con.close()
    return df

@st.cache_resource
def load_model(name: str):
    path = MODELS_DIR / f"{name}_model.pkl"
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return pickle.load(f)

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("⚽ Football Prediction")
page = st.sidebar.radio("Navigation", ["📊 Statistiques", "🔮 Prédiction", "📈 Performances modèle"])

# ══════════════════════════════════════════════════════════════════════════════
# Page : Statistiques
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Statistiques":
    st.title("📊 Statistiques")

    try:
        df = load_matches()

        col1, col2, col3 = st.columns(3)
        col1.metric("Matchs total",    len(df))
        col2.metric("Équipes",         df["home_team"].n_unique())
        col3.metric("Compétitions",    df["competition"].n_unique())

        # Distribution des résultats
        st.subheader("Distribution des résultats 1N2")
        result_counts = df["result"].value_counts().sort("result")
        fig = px.bar(result_counts.to_pandas(), x="result", y="count",
                     color="result",
                     color_discrete_map={"H": "#2563eb", "D": "#6b7280", "A": "#dc2626"},
                     labels={"result": "Résultat", "count": "Nombre de matchs"})
        st.plotly_chart(fig, use_container_width=True)

        # Évolution par saison
        if "season" in df.columns:
            st.subheader("Matchs par saison")
            by_season = df.group_by(["season", "result"]).agg(pl.len().alias("count"))
            fig2 = px.bar(by_season.to_pandas(), x="season", y="count",
                          color="result",
                          color_discrete_map={"H": "#2563eb", "D": "#6b7280", "A": "#dc2626"})
            st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.warning(f"Données non disponibles — Lance d'abord les pipelines 01 à 03.\n\n{e}")

# ══════════════════════════════════════════════════════════════════════════════
# Page : Prédiction
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔮 Prédiction":
    st.title("🔮 Prédiction d'un match")

    model_name = st.selectbox("Modèle", ["lightgbm", "xgboost"])
    bundle = load_model(model_name)

    if bundle is None:
        st.warning("Modèle non trouvé — Lance d'abord le pipeline 04_train.py")
    else:
        model       = bundle["model"]
        le          = bundle["label_encoder"]
        feature_cols = bundle["feature_cols"]

        st.subheader("Renseigne les features du match")
        st.info("💡 Ces champs correspondent aux features calculées par le pipeline. "
                "Adapte l'interface une fois tes données réelles disponibles.")

        # Formulaire dynamique basé sur les features du modèle
        inputs = {}
        cols = st.columns(3)
        for i, feat in enumerate(feature_cols):
            with cols[i % 3]:
                inputs[feat] = st.number_input(feat, value=0.0, format="%.3f")

        if st.button("🔮 Prédire", type="primary"):
            X = np.array([[inputs[f] for f in feature_cols]])
            proba = model.predict_proba(X)[0]
            classes = le.classes_

            st.subheader("Résultats")
            cols2 = st.columns(3)
            colors = {"H": "🔵", "D": "⚪", "A": "🔴"}
            labels = {"H": "Victoire domicile", "D": "Match nul", "A": "Victoire extérieur"}

            for i, (cls, prob) in enumerate(zip(classes, proba)):
                with cols2[i]:
                    st.metric(f"{colors.get(cls,'')} {labels.get(cls, cls)}", f"{prob*100:.1f}%")

            best_idx = np.argmax(proba)
            st.success(f"Prédiction : **{labels.get(classes[best_idx], classes[best_idx])}** "
                       f"({proba[best_idx]*100:.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# Page : Performances modèle
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Performances modèle":
    st.title("📈 Performances des modèles")
    st.info("Lance `mlflow ui` dans ton terminal pour accéder au dashboard MLflow complet.")

    for name in ["lightgbm", "xgboost"]:
        bundle = load_model(name)
        if bundle:
            st.success(f"✅ {name} — modèle disponible")
        else:
            st.warning(f"⚠️ {name} — modèle non entraîné")