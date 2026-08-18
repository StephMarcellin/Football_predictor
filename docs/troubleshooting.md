# Runbook — Troubleshooting Projet 3-Étoiles

## Comment utiliser ce document

Chaque entrée suit le format :
- **Symptôme** — ce que tu observes
- **Cause** — pourquoi ça arrive
- **Solution** — comment régler le problème

---

## DuckDB

### `at` is a reserved keyword
**Symptôme** : erreur SQL `Parser Error: syntax error at or near "at"`  
**Cause** : `at` est un mot réservé DuckDB — ne peut pas être utilisé comme nom de colonne sans guillemets  
**Solution** : renommer la colonne ou l'entourer de guillemets doubles : `"at"`

---

### `ROW_NUMBER() OVER (...) = 1` échoue avec DATE
**Symptôme** : erreur DuckDB 1.5.1 sur un filtre `ROW_NUMBER() = 1` avec une colonne DATE  
**Cause** : bug connu DuckDB 1.5.1 sur ce pattern  
**Solution** : remplacer par `MAX(date)` + double join

```sql
-- Au lieu de :
WHERE ROW_NUMBER() OVER (PARTITION BY team ORDER BY date DESC) = 1

-- Utiliser :
INNER JOIN (
    SELECT team, MAX(date) AS max_date
    FROM table
    GROUP BY team
) latest ON t.team = latest.team AND t.date = latest.max_date
```

---

### `INSERT INTO ... SELECT *` mappe par position
**Symptôme** : données insérées dans les mauvaises colonnes  
**Cause** : DuckDB mappe par position, pas par nom de colonne  
**Solution** : toujours lister les colonnes explicitement dans le `INSERT INTO`

---

### `json_extract` échoue sur DuckDB ancien
**Symptôme** : erreur sur `json_extract` avec accès imbriqué  
**Cause** : syntaxe non supportée sur les versions < 1.0  
**Solution** : décomposer en plusieurs extractions successives

---

### Non-inner joins sur subqueries échouent
**Symptôme** : erreur sur `LEFT JOIN (SELECT ...)` complexe  
**Cause** : DuckDB ne supporte pas les non-inner joins sur certaines subqueries  
**Solution** : matérialiser la subquery en `TEMP TABLE` d'abord

```sql
CREATE TEMP TABLE tmp AS SELECT ...;
LEFT JOIN tmp ON ...;
```

---

## dbt

### Schémas préfixés automatiquement (`gold_referentiel` au lieu de `referentiel`)
**Symptôme** : dbt crée `gold_referentiel` au lieu de `referentiel`  
**Cause** : comportement par défaut de dbt sur Windows qui préfixe le target schema  
**Solution** : ajouter la macro `generate_schema_name` dans `macros/`

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

---

### `pivot` mot réservé DuckDB dans dbt
**Symptôme** : erreur SQL sur un modèle dbt qui utilise un CTE nommé `pivot`  
**Cause** : `pivot` est un mot réservé DuckDB  
**Solution** : renommer le CTE (`pivot_cte`, `pivoted`, etc.)

---

### `off` interprété comme booléen dans les YAML dbt
**Symptôme** : erreur de parsing YAML sur une valeur `off`  
**Cause** : YAML interprète `off` comme `false`  
**Solution** : mettre `off` entre guillemets : `"off"`

---

### Tests dbt sur colonnes renommées
**Symptôme** : `Binder Error: Referenced column "final_match_id" not found`  
**Cause** : un test dans `schema.yml` référence une ancienne colonne qui a été renommée  
**Solution** : mettre à jour le nom de la colonne dans `schema.yml` pour correspondre au modèle réel

---

## MLflow

### URI invalide sur Windows
**Symptôme** : MLflow ne trouve pas les runs, erreur d'URI  
**Cause** : les backslashes Windows ne sont pas valides dans une URI MLflow  
**Solution** :

```python
mlflow_uri = "file:///" + str(ROOT_DIR / "mlruns").replace("\\", "/")
mlflow.set_tracking_uri(mlflow_uri)
```

---

## Prefect

### `dbt_test` apparaît comme `Completed` même en cas d'échec
**Symptôme** : Prefect affiche `Completed` pour `dbt_test` même quand dbt retourne des erreurs  
**Cause** : `run_step()` catch les exceptions et retourne un dict de status sans re-raise  
**Solution** : vérifier le champ `status` dans le dict retourné et lever une exception si `status != "success"`

---

### Prefect Artifacts disparaissent
**Symptôme** : les Artifacts ne sont pas persistés  
**Cause** : les Artifacts nécessitent un serveur Prefect actif au moment de leur création  
**Solution** : s'assurer que `make pipeline` démarre Prefect via `start /B` avant de lancer le pipeline

---

## Python / Environnement

### Variables d'environnement système overrident `.env`
**Symptôme** : `load_dotenv()` ne prend pas effet, les anciennes valeurs persistent  
**Cause** : les variables définies au niveau système Windows ont priorité sur `.env`  
**Solution** : vérifier dans PowerShell `[System.Environment]::GetEnvironmentVariable("MA_VAR", "Machine")` et supprimer la variable système, puis redémarrer la machine

---

### Variables module-level évaluées avant `load_dotenv()`
**Symptôme** : une variable définie depuis `os.getenv()` au niveau module est `None`  
**Cause** : les variables module-level sont évaluées à l'import, avant que `load_dotenv()` soit appelé dans `run_pipeline.py`  
**Solution** : déplacer la définition de la variable à l'intérieur de `main()`

---

### `SimpleImputer` silencieusement drop les colonnes 100% NULL
**Symptôme** : shape mismatch entre train `(368, 259)` et predict `(368, 236)` — 23 colonnes manquantes  
**Cause** : `SimpleImputer.transform()` supprime silencieusement les colonnes entièrement NULL  
**Solution** : sauvegarder `feature_names_post_pp` via `get_feature_names_out()` après le fit du preprocessor, et utiliser ces noms dans `05_predict.py`

---

### Import de scripts commençant par un chiffre
**Symptôme** : `ModuleNotFoundError` sur `import 04_train`  
**Cause** : Python n'autorise pas les imports directs de modules commençant par un chiffre  
**Solution** : utiliser `importlib`

```python
import importlib.util
spec = importlib.util.spec_from_file_location("train", "pipelines/04_train.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
```

---

## GCS / Google Cloud

### `upload_to_gcs` échoue avec chemin hors `data/raw/`
**Symptôme** : `StopIteration` ou mauvais chemin GCS lors de l'upload d'un fichier hors `data/raw/`  
**Cause** : `upload_to_gcs()` reconstruit le chemin GCS en cherchant `raw/` dans le path local  
**Solution** : utiliser `get_gcs_client()` directement et construire le `blob_name` manuellement

```python
client = get_gcs_client()
bucket = client.bucket(bucket_name)
blob   = bucket.blob("ci/football_test.duckdb")
blob.upload_from_filename(str(local_path))
```

---

## Docker

### `curl` absent dans l'image Prefect officielle
**Symptôme** : healthcheck Docker échoue sur le service Prefect  
**Cause** : `curl` n'est pas installé dans l'image officielle Prefect  
**Solution** : désactiver le healthcheck dans `docker-compose.yml` : `disable: true`

---

## Data quality — à investiguer (détecté 2026-08-06, construction couche Gold)

### `int_whoscored_team_season` : couverture partielle / trous par saison
**Symptôme** : la feature season-lag `season_xg_per_shot_*_lag` de `gold.equipe_match` n'a une couverture correcte (~65-85 %) qu'à partir de 2022-2023. Saisons présentes dans la source : 2019-2020, **2021-2022**, 2022-2023, 2023-2024, 2024-2025 — la saison **2020-2021 manque entièrement**, et 2017-2018 / 2018-2019 sont absentes.  
**Cause** : à confirmer. Deux pistes non tranchées : (1) 2019-2020 tronquée/décalée par le **Covid** (Ligue 1 arrêtée, autres championnats finis à l'été 2020) → agrégats de saison peut-être partiels ou mal datés ; (2) le trou complet de 2020-2021 ressemble davantage à un **problème de scraping** de la source WhoScored season.  
**Solution** : vérifier le scraper `int_whoscored_team_season` sur 2020-2021 et la datation des saisons Covid. En attendant, les features xG roulées (famille 2) portent le signal ; la feature season-lag reste NULL sans casser le modèle (imputation famille 11 / sélection).

---

### `h2h_history` : décalage d'un match (off-by-one) sur ~3 % des lignes
**Symptôme** : sur ~3 % des lignes avec `h2h_played > 0`, on a `h2h_wins + h2h_draws + h2h_losses = h2h_played − 1` (les taux dérivés dans `gold.equipe_adversaire_match` somment alors à un peu moins de 1).  
**Cause** : erreur probable dans la construction de la table intermediate `h2h_history` — une confrontation historique comptée dans `h2h_played` mais non classée en W/D/L (résultat manquant, ou décompte du premier match).  
**Solution** : à corriger dans le modèle `h2h_history` en amont (ne pas masquer dans le Gold). Candidat pour un check Great Expectations : `h2h_wins + h2h_draws + h2h_losses = h2h_played`.

---

### `backbone.clean_sheet` : colonne salie (valeurs 2, incohérences avec ga)
**Symptôme** : le test `accepted_range [0,1]` sur `clean_sheet_rate_rolling_{3,5,10}` de `gold.equipe_match` remonte des valeurs > 1 (warnings dbt).  
**Cause** : la colonne `cs` de `silver.fbref_keeper` (→ `int_fbref_keeper.cs` → `backbone.clean_sheet`) n'est pas un flag 0/1 propre. Valeurs : 0 (33069), 1 (12877), **2 (106)**. Les 106 `cs=2` ont toutes `ga_keeper=0` (vrais clean sheets mal étiquetés) ; en plus, 176 clean sheets sont étiquetés 0 et 18 « clean sheets » ont des buts encaissés. Ce n'est PAS un bug de grain (int_fbref_keeper : 0 doublon).  
**Solution** : garde-fou à la **racine**, dans `intermediate/int_fbref_keeper.sql` — `cs` est dérivé du champ autoritaire **local** `ga_keeper` (`CASE WHEN ga_keeper=0 THEN 1 ...`), qui concorde à 99,9 % avec le `ga` du schedule. Corrige les ~300 étiquettes fausses pour TOUS les consommateurs de la table, pas seulement backbone. Gold consomme sans recalculer. Ordre de reconstruction : `dbt run --select int_fbref_keeper` (table, rebuild complet), puis `dbt run --full-refresh --select backbone` (incrémental → full-refresh pour réécrire l'historique), puis les tables gold. Candidat check Great Expectations : `cs = (ga_keeper = 0)`.