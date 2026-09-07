# Kickoff — Rendre le projet 3-Étoiles présentable en prod (entretien)

> **À coller en premier message d'un nouveau Cowork pour lancer ce chantier.**
> Objectif : rendre le pipeline **démontrable de bout en bout** en entretien.
> L'edge de prédiction n'est **pas** requis ici — on veut un projet prod crédible,
> orchestré, testé, documenté. Contexte complet :
> `docs/Passation_2026-08-17_marts_modeles.md`.

---

## Où on en est

La chaîne **scrape → tables (dbt) → KNN → marts → train → backtest** tourne déjà
de bout en bout. Il reste à câbler l'orchestration, les prédictions courantes, les
tests et la vitrine engineering.

**Artefacts existants à connaître :**
- DB : `db/football.duckdb` (~65 Go, local). Lire en `read_only=True`. DuckDB =
  un seul writer → fermer toute session SQL avant d'écrire.
- Schémas : `silver`, `intermediate`, `gold`, `marts`, `machine_learning`.
- KNN : `pipelines/03_knn_impute.py --write` → `machine_learning.player_style_clusters`
  + `machine_learning.zonal_profiles_imputed` (déclarés en source dbt).
- Marts : `marts.mart_1n2 / mart_goals / mart_scorers / mart_assists / mart_events`
  (config : `config/models.yml`).
- Modélisation : `pipelines/ml_common.py` (plomberie partagée), `pipelines/train_1n2.py`
  (LightGBM calibré → `models/resultat_1n2.joblib`), `pipelines/backtest_1n2.py`.
- Orchestrateur : `pipelines/run_pipeline.py` (dict d'étapes, `run_step` restaure le
  cwd, `run_dbt_run(select=...)`).

**Conventions du projet à respecter :** `argparse` dans `if __name__ == "__main__"`,
tout piloté par `config.yaml`, **marts = pure sélection** (aucun calcul), **cotes
jamais en features** (`MARKET_PREFIXES` dans `ml_common`), vérifier le **grain unique**
après chaque table. Pédagogie avant le code, modifications **ligne par ligne dans
VS Code**, expliquer avant d'écrire.

---

## Tâches, par ordre de priorité

### 1. Tests dbt + doc des nouveaux modèles (rapide, fondation qualité)
- Ajouter dans les `schema.yml` (gold + un `marts/schema.yml`) les tests de base sur
  les modèles créés cette session : `equipe_confrontation_zone`, `equipe_gardien_match`,
  et les 5 marts. Au minimum : `dbt_utils.unique_combination_of_columns` sur le grain,
  `not_null` sur les clés et la cible.
- **Critère de réussite** : `dbt test --select marts equipe_confrontation_zone equipe_gardien_match` vert.

### 2. MLflow — réparer le tracking (403 dagshub actuel)
- `config.yaml` pointe `mlflow.tracking_uri` vers dagshub → erreur 403 sans creds.
- Deux options : (a) poser `MLFLOW_TRACKING_USERNAME` + token dagshub en variables
  d'env ; (b) basculer sur un `mlruns/` local (URI `file:///…` slashes Unix) pour la démo.
- **Critère** : `python pipelines/train_1n2.py` logge un run visible (dagshub ou UI locale).

### 3. Script de prédiction sur les nouveaux marts
- Créer `pipelines/predict_1n2.py` (patron symétrique de `train_1n2.py`) : charge
  `models/resultat_1n2.joblib`, prédit les **matchs à venir** (sans `result_1n2`),
  écrit les probas P(H/D/A) dans une table (`machine_learning.predictions_1n2`) ou un CSV.
- Réutiliser `ml_common.prepare_x` (mêmes features, cotes exclues).
- **Critère** : une table/fichier de prédictions pour les matchs de la prochaine journée.

### 4. Orchestration — découper `run_pipeline.py` en 3 flux
- **Quotidien** (transform + predict) : scrape → dbt tables → xgot_score → KNN impute
  → dbt (consommateurs imputés + marts) → predict. **Pas d'entraînement.**
- **Hebdomadaire** (retrain) : `train_1n2` (+ futurs trainers) → backtest.
- **Rare / manuel** (refit) : grille xT, modèle xGOT.
- Insérer l'étape KNN **entre deux phases dbt**, exactement comme les chaînes
  `dbt_xt_actions → xt_grid → dbt_xt_contributions` et xgot déjà présentes dans
  `run_pipeline.py` : `run_dbt_run(select="joueur_saison joueur_zone_saison")` →
  `03_knn_impute.py --write` → `run_dbt_run(select="team_corridor_profile joueur_match <marts>")`.
- **Critère** : `make pipeline` (ou l'équivalent) enchaîne tout sans intervention,
  marts à jour, prédictions produites.

### 5. Scraping des compositions 2025-2026 (débloque la couche tactique)
- Aujourd'hui : 0 % de compos sur 2025-26 → confrontations zonales + gardien vides pour
  les prédictions courantes. Étendre le scraping des lineups à la saison en cours.
- **Critère** : couverture des compos > 0 % sur 2025-26 dans `int_whoscored_lineup`,
  features de confrontation non-NULL sur les matchs récents.

### 6. Vitrine engineering
- **GitHub Actions** : CI qui, à chaque push, lance `dbt build --select ...` sur un
  échantillon + les tests. (Concepts CI/CD demandés par Stéphane.)
- **Great Expectations** : quelques attentes de data quality sur les marts.
- **Docker** : vérifier que `docker-compose` up (prefect, mlflow) tourne pour la démo.

---

## Ordre conseillé
1 (tests) → 2 (MLflow) → 3 (predict) → 4 (orchestration) → 5 (compos) → 6 (CI/GE).
Les tâches 1-4 suffisent déjà à une démo prod convaincante ; 5-6 renforcent.

À la fin : produire la doc Obsidian des concepts vus + la passation, et lister les
commits/branches à pousser (convention `feat:`/`fix:`/`docs:`/`chore:`).
