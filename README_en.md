# ⚽ Football Prediction Engine

Système de prédiction de résultats (1N2) sur les 5 grands championnats européens.

## Stack
- **DuckDB** — stockage analytique local
- **Polars** — traitement des données
- **LightGBM / XGBoost** — modèles ML
- **MLflow** — tracking des expériences
- **Streamlit** — dashboard web

## Installation

```bash
python -m venv .venv
source .venv/bin/activate      # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

## Utilisation

```bash
# 1. Dépose tes fichiers CSV dans data/raw/
# 2. Lance les pipelines dans l'ordre :

python pipelines/01_ingest.py   # Chargement dans DuckDB
python pipelines/02_clean.py    # Nettoyage
python pipelines/03_features.py # Feature engineering
python pipelines/04_train.py    # Entraînement ML

# 3. Lance le dashboard
streamlit run app/streamlit_app.py

# 4. (Optionnel) Suivi MLflow
mlflow ui
```

## Structure

```
football-prediction/
├── data/
│   ├── raw/          ← Tes fichiers sources ici
│   ├── processed/
│   └── features/
├── db/               ← Base DuckDB
├── pipelines/        ← ETL + ML
├── models/           ← Modèles sérialisés
├── app/              ← Dashboard Streamlit
├── notebooks/        ← Exploration
├── config.yaml       ← Configuration centrale
└── requirements.txt
```

## Format des données attendu

| Fichier | Colonnes minimales |
|---|---|
| `data/raw/matches.csv` | date, competition, season, home_team, away_team, home_goals, away_goals |
| `data/raw/players.csv` | à définir selon ta source |
| `data/raw/financials.csv` | team, season, market_value_m |
