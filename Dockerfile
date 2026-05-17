# ============================================================
#  Dockerfile — Projet 3-Étoiles
#  Image de base : python:3.11-slim (Debian minimal, pas d'Alpine
#  pour éviter les problèmes de compilation de librairies C comme
#  LightGBM et DuckDB qui nécessitent glibc)
# ============================================================

FROM python:3.11-slim

# ── Métadonnées ───────────────────────────────────────────────────────────
LABEL maintainer="Projet 3-Étoiles"
LABEL description="Pipeline de prédiction football — Gold → Backtest"

# ── Variables d'environnement ─────────────────────────────────────────────
# PYTHONUNBUFFERED=1 : désactive le buffering stdout/stderr
# → les logs Python apparaissent en temps réel dans docker logs
# Sans ça, les logs sont bufferisés et n'apparaissent qu'en fin d'exécution
ENV PYTHONUNBUFFERED=1

# PYTHONDONTWRITEBYTECODE=1 : ne pas créer les fichiers .pyc
# → image plus légère, pas de bytecode inutile dans le conteneur
ENV PYTHONDONTWRITEBYTECODE=1

# Répertoire de travail dans le conteneur
# Tous les COPY et RUN suivants s'exécutent depuis /app
WORKDIR /app

# ── Dépendances système ───────────────────────────────────────────────────
# Nécessaires pour compiler certaines librairies Python (LightGBM, lxml)
# On nettoie le cache apt après l'installation pour réduire la taille de l'image
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Dépendances Python ────────────────────────────────────────────────────
# On copie UNIQUEMENT requirements.txt en premier.
# Pourquoi ? Docker met cette couche en cache.
# Tant que requirements.txt ne change pas, pip install n'est pas réexécuté
# même si le code source change — le build est beaucoup plus rapide.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Code source ───────────────────────────────────────────────────────────
# Copié en dernier — c'est ce qui change le plus souvent.
# Le .dockerignore exclut les fichiers inutiles (mlruns, data, models, logs)
COPY . .

# ── dbt ───────────────────────────────────────────────────────────────────
# On installe les dépendances dbt (packages comme dbt_utils)
# si le dossier dbt_project existe
RUN if [ -f "dbt_project/packages.yml" ]; then \
    cd dbt_project && dbt deps; \
    fi

# ── Point d'entrée par défaut ─────────────────────────────────────────────
# Lance le pipeline complet par défaut.
# Peut être surchargé au runtime :
#   docker run projet-3etoiles python pipelines/run_pipeline.py --step train
CMD ["python", "pipelines/run_pipeline.py"]