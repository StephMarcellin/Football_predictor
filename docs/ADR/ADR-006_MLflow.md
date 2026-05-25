# ADR-006 — MLflow comme système de tracking des expériences ML

## Statut

Adopté — 15/05/2026

## Contexte

Le pipeline entraîne un modèle LightGBM two-stage et évalue une stratégie de paris. À chaque run, des dizaines de métriques sont produites : Log Loss, accuracy, ROI, win rate, drawdown, etc. Sans système de tracking, ces métriques n'existent que dans les logs — difficiles à comparer entre runs, impossibles à visualiser dans le temps.

Les besoins identifiés :

1. **Persistance des métriques** — retrouver les performances d'un run passé sans fouiller les logs
2. **Comparaison entre runs** — a-t-on progressé depuis la dernière version du modèle ?
3. **Traçabilité des paramètres** — avec quels hyperparamètres ce modèle a-t-il été entraîné ?
4. **Bus de données inter-scripts** — passer les métriques de `04_train.py` et `06_backtest.py` vers `run_pipeline.py` sans couplage fort

### Options considérées

|Option|Profil|
|---|---|
|**MLflow (open-source)**|Standard industrie, UI locale, tracking + model registry, léger|
|**Weights & Biases**|Très complet, SaaS, excellent pour la visualisation, payant en production|
|**Neptune.ai**|Similaire à W&B, orienté collaboration, payant|
|**DVC**|Orienté versioning de données et pipelines, moins centré sur les métriques|
|**Fichiers CSV/JSON**|Simple, pas d'UI, difficile à comparer entre runs|
|**Logs Loguru**|Déjà en place, lisibles mais non structurés pour la comparaison|

---

## Décision

**MLflow open-source avec tracking local** (`mlruns/` à la racine du projet) est adopté.

MLflow est utilisé pour deux usages distincts dans ce projet :

1. **Tracking d'expériences** : `04_train.py` et `06_backtest.py` loguent leurs métriques et paramètres dans MLflow après chaque run
2. **Bus de données** : `run_pipeline.py` lit MLflow pour récupérer les dernières métriques et les publier comme Artifacts Prefect

---

## Justification

### Standard industrie accessible

MLflow est présent dans la quasi-totalité des stacks ML en entreprise. Son API est simple — `mlflow.log_metric()`, `mlflow.log_param()`, `mlflow.log_artifact()` — et son UI locale (`mlflow ui`) ne nécessite aucune infrastructure externe.

### Intégration LightGBM native

MLflow propose `mlflow.lightgbm` pour logguer automatiquement les modèles LightGBM avec leurs paramètres. Dans `04_train.py`, cette intégration est déjà en place.

### Séparation des responsabilités

Chaque script écrit ses métriques indépendamment dans MLflow. L'orchestrateur lit MLflow comme source de vérité. Ce découplage évite de faire transiter des métriques via des variables globales ou des fichiers intermédiaires fragiles :

```
04_train.py    ──→  mlruns/ (log_loss, accuracy)
06_backtest.py ──→  mlruns/ (roi, win_rate, drawdown)
                                ↓
run_pipeline.py ←── mlruns/ (lecture via MlflowClient)
                                ↓
                    Prefect Artifact pipeline-metrics
```

### Comparaison avec Weights & Biases

W&B est plus puissant pour la visualisation et la collaboration. Pour un projet personnel local, MLflow est suffisant et gratuit sans limite. W&B est à considérer si le projet évolue vers une équipe ou un contexte cloud.

---

## Implémentation

### Structure dans `04_train.py`

```python
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("football_1N2_stacking")

with mlflow.start_run(run_name="TwoStage_Stacking_v1"):
    mlflow.log_params({...})    # hyperparamètres
    mlflow.log_metrics({...})   # log_loss, accuracy, etc.
    mlflow.log_artifact(...)    # modèle joblib, diagnostics PNG
```

### Structure dans `06_backtest.py`

```python
with mlflow.start_run(run_name=f"backtest_{'_'.join(seasons)}"):
    mlflow.log_params({...})    # edge_min, confidence_min, kelly_fraction
    mlflow.log_metrics({...})   # roi, win_rate, drawdown, total_bets
    mlflow.log_artifact(...)    # backtest_results.csv
```

### Lecture dans `run_pipeline.py`

```python
client = mlflow.MlflowClient()
runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    filter_string="tags.`mlflow.runName` LIKE 'TwoStage%'",
    order_by=["start_time DESC"],
    max_results=1,
)
metrics = runs[0].data.metrics
```

### URI sur Windows

MLflow sur Windows requiert le préfixe `file:///` avec des slashes Unix pour l'URI :

```python
mlflow_uri = "file:///" + str(ROOT_DIR / "mlruns").replace("\\", "/")
```

Sans ce préfixe, `MlflowClient` échoue avec une erreur "unsupported URI scheme".

---

## Conséquences

### Positives

- Métriques persistées et comparables entre runs
- UI locale sur `localhost:5000` (`make mlflow-ui`)
- Intégration native LightGBM
- Sert de bus de données vers les Prefect Artifacts
- Zéro coût, zéro infrastructure externe

### Négatives et limites

- **`mlruns/` non versionné** : le dossier est dans `.gitignore` — les métriques ne voyagent pas avec le code sur GitHub. Pour partager les résultats, il faudrait un MLflow Server distant (PostgreSQL + S3).
- **Pas de comparaison visuelle avancée** : l'UI MLflow est fonctionnelle mais moins riche que W&B pour visualiser l'évolution des métriques dans le temps.
- **Deux serveurs locaux** : MLflow (`localhost:5000`) et Prefect (`localhost:4200`) doivent tourner simultanément pour une observabilité complète — deux terminaux dédiés.

---

## Questions d'entretien anticipées

**"Qu'est-ce que MLflow et à quoi ça sert ?"**

MLflow est un outil open-source de gestion du cycle de vie ML. Il couvre quatre domaines : le tracking d'expériences (log des métriques, paramètres, artefacts), la gestion de modèles (versioning, staging, production), les projets ML (packaging reproductible), et le model registry (catalogue centralisé). Dans ce projet, on utilise principalement le tracking et le model registry local.

**"Quelle est la différence entre MLflow et Prefect dans ce projet ?"**

MLflow répond à "mon modèle est-il meilleur qu'avant ?" — il stocke les métriques ML et permet la comparaison entre runs. Prefect répond à "mon pipeline s'est-il bien exécuté ?" — il trace les statuts, durées, et logs de chaque étape. Les deux sont complémentaires : MLflow est la source de vérité des métriques, Prefect est la source de vérité de l'exécution. Dans ce projet, Prefect lit MLflow pour afficher les métriques dans ses Artifacts.

**"Qu'est-ce qu'une expérience MLflow ?"**

Une expérience est un regroupement logique de runs. Tous les runs d'entraînement et de backtest du projet sont dans l'expérience `football_1N2_stacking`. Chaque run dans cette expérience a un nom (`TwoStage_Stacking_v1`, `backtest_2023-2024_2024-2025`), des paramètres, des métriques et des artefacts. L'UI MLflow permet de filtrer, trier et comparer les runs d'une même expérience.