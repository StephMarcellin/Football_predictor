# ADR-003 — Prefect comme orchestrateur de pipeline

## Statut

Adopté — mis à jour le 15/05/2026

## Contexte

Initialement, le pipeline était exécuté manuellement : chaque script (`04_train.py`, etc.) était lancé à la main dans un terminal. Cette approche était suffisante en phase d'exploration mais devenait un frein dès que l'objectif était de faire tourner le pipeline régulièrement (chaque lundi après les matchs du week-end).

Les besoins identifiés pour un système d'orchestration :

1. **Scheduling automatique** — déclencher le pipeline sans intervention manuelle
2. **Résilience** — réessayer automatiquement une étape en cas d'échec transitoire
3. **Observabilité** — savoir quelle étape a duré combien de temps, quelle a échoué
4. **Fail-fast** — arrêter la chaîne si une étape critique échoue
5. **Exécution partielle** — relancer uniquement l'étape `predict` sans tout rejouer

### Options considérées

|Option|Profil|
|---|---|
|**Prefect v2 (open-source)**|Python-first, léger, UI locale, scheduling, retries, cloud optionnel|
|**Apache Airflow**|Outil historique, DAGs statiques, scheduler permanent, lourd à opérer|
|**Luigi (Spotify)**|Léger, dependency-based, pas d'UI native, moins actif|
|**Cron natif**|Scheduling uniquement, pas de retry, pas d'observabilité|
|**Script bash**|Séquençage simple, zéro résilience, pas d'historique|
|**Dagster**|Orienté assets, fort typage, courbe d'apprentissage plus raide|

---

## Décision

**Prefect v2 (open-source, serveur local)** est adopté comme orchestrateur.

Prefect est utilisé via trois primitives : `@flow` pour le pipeline entier, `@task` pour chaque étape individuelle, et `create_markdown_artifact` pour l'observabilité enrichie. Le scheduling est géré par `.serve()` avec une expression cron issue de `config.yaml`. `run_pipeline.py` encapsule la logique d'orchestration au-dessus de Prefect.

---

## Justification

### Python-first sans friction opérationnelle

La philosophie de Prefect v2 est "n'importe quelle fonction Python devient un flow avec `@flow`". Pas de YAML de configuration, pas de DAG à déclarer séparément, pas d'opérateurs à hériter. On décore des fonctions existantes :

```python
@flow(name="Pipeline 3-Étoiles")
def run_pipeline(steps, dry_run):
    for step_name, step_cfg in steps.items():
        result = run_step_task(step_name, step_cfg["fn"])
        if result["status"] == "FAILED" and step_cfg["critical"]:
            break   # fail-fast

@task(retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE, task_run_name="{step_name}")
def run_step(step_name, fn): ...
```

### Artifacts — observabilité enrichie

Prefect Artifacts permettent d'attacher des données structurées (tableaux Markdown) directement à un run dans l'UI. Après chaque run, deux Artifacts sont publiés :

- `pipeline-metrics` : métriques MLflow (Log Loss, ROI, paris, drawdown)
- `agent-synthese` : analyse textuelle de l'agent Gemini avec recommandation

Ces Artifacts transforment l'UI Prefect d'un simple suivi de statuts en un véritable tableau de bord de monitoring. Sans quitter `localhost:4200`, on sait si le modèle s'est amélioré et si la stratégie est profitable.

### `task_run_name` dynamique

Par défaut, toutes les exécutions de `run_step` apparaissent dans l'UI comme `run_step-abc`, `run_step-d61`, etc. Le paramètre `task_run_name="{step_name}"` injecte le nom réel de l'étape au moment de l'exécution — on voit `backtest`, `train`, `agent` dans l'UI, ce qui rend le diagnostic immédiat.

### Serveur permanent et `wait_for_prefect.ps1`

Les Artifacts Prefect ne sont persistés que si un serveur Prefect est actif au moment de leur création. Sans serveur, Prefect démarre un serveur temporaire en mémoire qui s'arrête avec le flow — emportant les Artifacts.

Le Makefile démarre automatiquement le serveur avant le pipeline et attend sa disponibilité via `scripts/wait_for_prefect.ps1` :

```powershell
# wait_for_prefect.ps1 — attend que http://127.0.0.1:4200/api/health réponde
while ($elapsed -lt $max) {
    try { Invoke-WebRequest -Uri $url ...; break }
    catch { Start-Sleep -Seconds 1; $elapsed++ }
}
```

### Comparaison avec Airflow

|Dimension|Airflow|Prefect v2|
|---|---|---|
|**DAG definition**|Python déclaratif, statique|Python dynamique (généré au runtime)|
|**Scheduler**|Processus permanent (poll-based)|`.serve()` ou worker Prefect|
|**Infrastructure**|WebServer + Scheduler + Worker + DB|Processus unique + SQLite optionnel|
|**Observabilité**|UI avec logs et durées|UI + Artifacts structurés|
|**Adoption**|Très répandu en entreprise|Plus récent, croissance rapide|

> [!info] Pourquoi Airflow reste un standard à connaître Airflow est présent dans la majorité des stacks data engineering en entreprise. La bonne réponse en entretien : "J'ai utilisé Prefect dont j'ai compris les primitives (flow, task, deployment, scheduling, artifacts). Les concepts sont transposables à Airflow (DAG, Operator, Sensor, XCom)."

### Retries automatiques

Le mécanisme de retry Prefect est utile pour les étapes réseau (appels API, téléchargements). Configurable depuis `config.yaml` :

```yaml
pipeline:
  retries: 2
  retry_delay_seconds: 30
```

La factory `make_run_step_task()` injecte ces valeurs dynamiquement dans le décorateur `@task`.

> [!warning] Subtilité : retries vs gestion d'exception interne Dans `run_step`, les exceptions sont capturées dans un `try/except` et converties en `{"status": "FAILED"}`. Du point de vue de Prefect, la tâche s'est terminée normalement — les retries ne se déclenchent pas pour les erreurs métier. Les retries couvrent les pannes inattendues non capturées par `except Exception`. C'est un choix délibéré : on contrôle soi-même la logique de fail-fast.

---

## Conséquences

### Positives

- Setup en 2 minutes (`pip install prefect`, `prefect server start`)
- Historique de runs consultable dans l'UI sans fouiller les logs
- Artifacts structurés : métriques MLflow + synthèse agent visibles dans l'UI
- Noms de tâches lisibles grâce à `task_run_name`
- Scheduling configurable sans modifier le code
- Démarrage automatique du serveur via `make pipeline`

### Négatives et limites

- **Mode `.serve()` non résilient** : si le processus scheduler s'arrête (reboot machine, Ctrl+C), le scheduling s'arrête. Pour une résilience réelle, il faudrait un work pool Prefect ou un service systemd.
- **Artifacts liés au serveur** : sans `make prefect-ui` actif, les Artifacts ne sont pas persistés entre les sessions. Le `wait_for_prefect.ps1` adresse ce problème pour le Makefile mais pas pour les appels directs Python.
- **Pas d'alerting natif en local** : Prefect Cloud propose des alertes email/Slack. En local, il faudrait implémenter manuellement un webhook.
- **Dépendance à un port local** : l'agent Gemini interroge `localhost:4200` pour `get_pipeline_status()`. Si Prefect server n'est pas lancé, cette fonctionnalité est dégradée.

---

## Relation avec `run_pipeline.py`

Prefect fournit les primitives (`@flow`, `@task`, `.serve()`, `create_markdown_artifact`). `run_pipeline.py` apporte la logique métier au-dessus :

|Couche|Responsabilité|
|---|---|
|**Prefect**|Tracking des états, retries, UI, scheduling cron, Artifacts|
|**`run_pipeline.py`**|Fail-fast configurable, CLI (`--step`, `--from`, `--dry-run`), protection `os.chdir()`, import dynamique, lecture MLflow|

Cette séparation est intentionnelle : si on remplace Prefect par un autre orchestrateur demain, la logique de `run_pipeline.py` reste valide.

---

## Questions d'entretien anticipées

**"Qu'est-ce qu'un orchestrateur de pipeline data ?"**

Un orchestrateur est un système qui gère l'exécution d'un ensemble de tâches avec des dépendances : il détermine l'ordre d'exécution, relance en cas d'échec, log les durées et statuts, et peut scheduler les exécutions. La différence avec un simple script : l'orchestrateur apporte de l'observabilité (on sait ce qui s'est passé sans lire les logs) et de la résilience (retry automatique sans code manuel).

**"Quelle est la différence entre un @flow et un @task dans Prefect ?"**

Un `@flow` est l'unité de haut niveau — c'est le pipeline dans son ensemble. Il est visible dans l'UI, peut être schedulé, et possède un état global. Un `@task` est une étape atomique au sein d'un flow — il hérite des retries, peut être mis en cache, et son état est tracé indépendamment. Un flow peut appeler plusieurs tasks et d'autres flows (sous-flows).

**"Qu'est-ce qu'un Prefect Artifact ?"**

Un Artifact est une donnée structurée (tableau Markdown, lien, valeur) attachée à un run Prefect et visible dans l'UI. Il permet de transformer l'UI d'un simple suivi de statuts en tableau de bord. Dans ce projet, on publie les métriques MLflow et la synthèse de l'agent Gemini comme Artifacts après chaque run.

**"Quand choisiriez-vous Airflow plutôt que Prefect ?"**

Airflow est préférable quand l'organisation a déjà une infrastructure Airflow en place, qu'on a besoin du backfill natif, ou que l'équipe maîtrise déjà Airflow. Prefect est préférable pour les nouveaux projets où la rapidité de setup compte, les pipelines dynamiques, et les équipes qui veulent rester proches de Python pur.