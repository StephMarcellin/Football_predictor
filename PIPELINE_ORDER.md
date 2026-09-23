# Ordre d'Exécution du Pipeline — Projet 3-Étoiles

**Source**: `pipelines/run_pipeline.py` (build_steps, build_table_steps, build_flow_steps)  
**Dernière mise à jour**: 2026-09-03

---

## 1. PIPELINE COMPLET (par défaut)

Ordre d'exécution : **20 étapes**

| # | Étape | Script/Fonction | Type | Critique | Rôle |
|---|-------|-----------------|------|----------|------|
| 1 | `dbt_seed` | `run_dbt_seed()` | dbt | ✓ CRITIQUE | Initialise les tables de seed (referentiel, configurations) |
| 2 | `ingest` | `pipelines/ingest/01_ingest.py` | Python | ✓ CRITIQUE | Scrape FBref, Understat, WhoScored → Bronze (Parquet local) |
| 3 | `odds` | `pipelines/ingest/01b_odds.py` | Python | ✓ CRITIQUE | Scrape cotes football-data.co.uk → DuckDB |
| 4 | `process_events` | `pipelines/ingest/process_events.py` | Python | ✓ CRITIQUE | Normalise events → Silver |
| 5 | `process_team_stats` | `pipelines/ingest/process_team_stats.py` | Python | ✓ CRITIQUE | Normalise team stats → Silver |
| 6 | `validate_silver` | `validation/run_validation.py` | Python | ✓ CRITIQUE | Valide la couche Silver |
| 7 | `dbt_run` | `run_dbt_run(select=backbone,features_*,features_final)` | dbt | ✓ CRITIQUE | Construit backbone, features_rolling, whoscored, draw, final → Gold |
| 8 | `dbt_xt_actions` | `run_dbt_run(select=+int_xt_actions)` | dbt | ✗ NON-CRITIQUE | Chaîne xT auxiliaire (dépendance Python) |
| 9 | `xt_grid` | `pipelines/features/xt_grid.py` | Python | ✗ NON-CRITIQUE | Calcule grille xT expected threat → machine_learning.xt_grid |
| 10 | `dbt_xt_contributions` | `run_dbt_run(select=int_xt_contributions)` | dbt | ✗ NON-CRITIQUE | Utilise xt_grid pour contributions |
| 11 | `dbt_xgot_features` | `run_dbt_run(select=xgot_features)` | dbt | ✗ NON-CRITIQUE | Chaîne PSxG auxiliaire (dépendance Python) |
| 12 | `xgot_score` | `pipelines/features/xgot_score.py` | Python | ✗ NON-CRITIQUE | Applique modèle xGOT → machine_learning.xgot_predictions |
| 13 | `dbt_keeper_psxg` | `run_dbt_run(select=int_keeper_shots+,int_keeper_psxg)` | dbt | ✗ NON-CRITIQUE | Calcule PSxG gardien (utilise xgot_score) |
| 14 | `dbt_test` | `run_dbt_test()` | dbt | ✗ NON-CRITIQUE | Tests de qualité dbt |
| 15 | `dbt_test_check` | `check_dbt_test_results()` | Python | ✓ CRITIQUE | Parse logs dbt, lève erreur si ERROR détecté |
| 16 | `validate_gold` | `validation/run_validation.py` | Python | ✓ CRITIQUE | Valide la couche Gold (features_final) |
| 17 | `train` | `pipelines/legacy/04_train.py` | Python | ✓ CRITIQUE | LightGBM two-stage 1N2 + calibration → MLflow |
| 18 | `predict` | `pipelines/legacy/05_predict.py` | Python | ✓ CRITIQUE | Prédictions ensemble + value bets (upcoming=True) |
| 19 | `backtest` | `pipelines/legacy/06_backtest.py` | Python | ✗ NON-CRITIQUE | Backtest Kelly criterion, ROI → MLflow |
| 20 | `agent_analysis` | `pipelines/agent_gemini.py` | Python | ✗ NON-CRITIQUE | Analyse post-run Gemini ReAct |

---

## 2. BLOCS RÉUTILISABLES

### `TABLES_UPDATE` — Rafraîchissement quotidien
```
dbt_transform → xgot_score → knn_impute → dbt_transform_downstream → dbt_test
```

**Utilisation**: `make daily` ou `python run_pipeline.py --tables`

| # | Étape | Rôle | Critique |
|---|-------|------|----------|
| 1 | `dbt_transform` | dbt run exclude='int_keeper_shots+ joueur_match+' | ✓ |
| 2 | `xgot_score` | Applique xGOT existant | ✗ |
| 3 | `knn_impute` | Imputation KNN → zonal_profiles_imputed | ✓ |
| 4 | `dbt_transform_downstream` | dbt run select='int_keeper_shots+ joueur_match+' | ✓ |
| 5 | `dbt_test` | Tests dbt | ✗ |

### `TABLES_REFIT` — Refonte des artefacts (rare)
```
xt_grid → xgot_train
```

**Utilisation**: `make rare` ou `python run_pipeline.py --refit`

| # | Étape | Rôle | Critique |
|---|-------|------|----------|
| 1 | `xt_grid` | Refit grille xT | ✗ |
| 2 | `xgot_train` | Refit modèle xGOT | ✗ |

---

## 3. FLUX COMPOSÉS (depuis build_flow_steps)

### `daily` (quotidien, **mise à jour sans réentraînement**)
```
TABLES_UPDATE + predict_ensemble
```
= `dbt_transform → xgot_score → knn_impute → dbt_transform_downstream → dbt_test → predict_ensemble`

**Rôle**: Prédictions du jour sur données fraîches (pas d'entraînement).

### `weekly` (hebdo, **réentraînement + backtest**)
```
train_1n2 → train_goals → backtest_1n2
```

**Rôle**: Réentraîner les modèles, backtest sur TEST_SEASON.

### `rare` (rare, **refonte des artefacts coûteux**)
```
TABLES_REFIT
```

**Rôle**: Recalculer grille xT et modèle xGOT (30+ min).

---

## 4. DÉPENDANCES CRITIQUES

### Fail-fast (un échec stoppe le pipeline)
```
dbt_seed 
  ↓
ingest → odds → process_events → process_team_stats
  ↓
dbt_run (backbone, features_*)
  ↓
validate_gold
  ↓
train → predict
```

### Non-critique (un échec n'arrête **pas** le pipeline)
```
xT : dbt_xt_actions → xt_grid → dbt_xt_contributions
PSxG : dbt_xgot_features → xgot_score → dbt_keeper_psxg
backtest, agent_analysis
```

---

## 5. DÉPENDANCES PYTHON→DBT IMPOSÉES

Le pipeline contient **deux chaînes hybrides** où Python write des tables consommées par dbt. dbt ignore la dépendance → ordre imposé en orchestration :

### Chaîne xT
```
dbt : int_xt_actions 
  ↓
Python : xt_grid.py → machine_learning.xt_grid (écrit)
  ↓
dbt : int_xt_contributions (lit machine_learning.xt_grid)
```

### Chaîne PSxG
```
dbt : xgot_features
  ↓
Python : xgot_score.py → machine_learning.xgot_predictions (écrit)
  ↓
dbt : int_keeper_shots, int_keeper_psxg (lisent xgot_predictions)
```

**Important**: Sans cet ordre, les modèles dbt n'auraient pas les données Python. C'est pourquoi `xt_grid` et `xgot_score` sont au cœur du pipeline même si non-critiques.

---

## 6. COUCHES MEDALLION

| Couche | Scripts | Format | Contenu |
|--------|---------|--------|---------|
| **Bronze** | 01_ingest.py, 01b_odds.py | Parquet local | Données brutes scrappées |
| **Silver** | process_events.py, process_team_stats.py | DuckDB | Données nettoyées, normalisées |
| **Gold** | dbt (run_dbt_run) | DuckDB | Features, modèles prêts ML |
| **ML** | 04_train.py, 05_predict.py, 06_backtest.py | Fichiers, MLflow | Modèles, prédictions, métriques |

---

## 7. POINT D'ENTRÉE PRINCIPAL

**Fichier**: `pipelines/run_pipeline.py`

**Fonction**: `build_steps(cfg)` → dict ordonné de 20 étapes

**Usage CLI** (depuis la racine du projet):

```bash
# Pipeline complet
python pipelines/run_pipeline.py

# Une seule étape
python pipelines/run_pipeline.py --step train

# Depuis une étape
python pipelines/run_pipeline.py --from train

# Simulation
python pipelines/run_pipeline.py --dry-run

# Lister les étapes
python pipelines/run_pipeline.py --list

# Flux composés
python pipelines/run_pipeline.py --flow daily
python pipelines/run_pipeline.py --flow weekly
python pipelines/run_pipeline.py --flow rare

# Scheduler Prefect (bloquant)
python pipelines/run_pipeline.py --serve
```

---

## 8. PARAMÈTRES RÉSILIENCE (config.yaml)

```yaml
pipeline:
  retries: 2
  retry_delay_seconds: 30
  cron: "0 12 * * 1"  # lundi 12h
  deployment_name: "pipeline-lundi-midi"
```

Chaque étape critique réessaie 2× avant de donner l'alerte.

---

## 9. INTERFACE PREFECT

**URL**: `http://localhost:4200`

**Démarrage**: `make prefect-ui` ou `prefect server start`

Affiche en temps réel:
- État d'exécution de chaque étape
- Logs en streaming
- Artifacts (tableau métriques MLflow)
- Historique des runs

---

## 10. INTERFACE MLFLOW

**URL**: `http://localhost:5000`

**Démarrage**: `make mlflow-ui` ou `mlflow ui --backend-store-uri mlruns`

Affiche:
- Runs d'entraînement (4_train.py)
- Runs de backtest (06_backtest.py)
- Hyperparamètres, métriques, artefacts

---

## RÉSUMÉ RAPIDE

**Scrappe** → **Seeds** → **Calcul tables** (dbt) → **Features Python** (xT, PSxG, KNN) → **Validation** → **Entraînement** → **Prédictions** → **Backtest**

**20 étapes, ~2h exécution complète, 7 étapes critiques qui bloquent si échec.**
